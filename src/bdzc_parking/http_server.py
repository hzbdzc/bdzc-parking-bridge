"""海康消息接收 HTTP server。"""

from __future__ import annotations

import json
import logging
import mimetypes
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

from bdzc_parking.config import AppConfig
from bdzc_parking.service import ParkingBridgeService


LOGGER = logging.getLogger(__name__)
_RATE_LIMIT_STALE_SECONDS = 600.0


class BridgeHTTPServer:
    """封装 ThreadingHTTPServer 的启动和停止逻辑。"""

    def __init__(self, config: AppConfig, service: ParkingBridgeService):
        """保存监听配置和用于处理请求的桥接服务。"""
        self.config = config
        self.service = service
        self._server: _ServiceHTTPServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def is_running(self) -> bool:
        """返回 HTTP server 是否已经启动。"""
        return self._server is not None

    def start(self) -> None:
        """启动后台 HTTP server 线程。"""
        if self._server is not None:
            return
        self._server = _ServiceHTTPServer(
            (self.config.listen_host, self.config.listen_port),
            HikRequestHandler,
            self.config,
            self.service,
        )
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="hikvision-http-server",
            daemon=True,
        )
        self._thread.start()
        LOGGER.info(
            "HTTP server listening on %s:%s",
            self.config.listen_host,
            self._server.server_port,
        )

    def stop(self) -> None:
        """停止 HTTP server 并等待后台线程退出。"""
        if self._server is None:
            return
        server = self._server
        self._server = None
        server.shutdown()
        server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._thread = None
        LOGGER.info("HTTP server stopped")


class _ServiceHTTPServer(ThreadingHTTPServer):
    """带有业务服务引用的 HTTP server 子类。"""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        config: AppConfig,
        service: ParkingBridgeService,
    ):
        """初始化 HTTP server，并挂载配置和业务服务。"""
        super().__init__(server_address, handler_class)
        self.config = config
        self.service = service
        self.image_rate_limiter = ImageRateLimiter(
            config.image_rate_limit_per_minute,
            config.image_rate_limit_burst,
        )

    def is_image_request(self, path: str) -> bool:
        """判断请求路径是否命中对外图片访问前缀。"""
        prefix = self.config.external_image_path
        if not prefix:
            return False
        return path.startswith(f"{prefix}/")

    def image_name_from_path(self, path: str) -> str | None:
        """从图片访问路径中提取文件名。"""
        prefix = self.config.external_image_path
        if not prefix or not path.startswith(f"{prefix}/"):
            return None
        image_name = unquote(path.removeprefix(f"{prefix}/")).strip()
        if not image_name:
            return None
        if Path(image_name).name != image_name or "/" in image_name or "\\" in image_name:
            return None
        return image_name


class HikRequestHandler(BaseHTTPRequestHandler):
    """处理海康终端发来的 HTTP 请求。"""

    server: _ServiceHTTPServer

    def do_GET(self) -> None:
        """响应健康检查和对外图片访问请求。"""
        path = urlsplit(self.path).path
        if path == "/":
            self._send_text(200, "BDZC Parking Bridge is running")
            return
        if path == "/healthz":
            self._handle_health_request()
            return
        if path == "/status":
            self._handle_status_request()
            return
        if self.server.is_image_request(path):
            self._handle_image_request(path)
            return
        self._send_text(404, "Not Found")

    def do_POST(self) -> None:
        """接收海康 POST 上报的过车消息。"""
        self._handle_hikvision_event()

    def do_PUT(self) -> None:
        """接收海康 PUT 上报的过车消息。"""
        self._handle_hikvision_event()

    def _handle_hikvision_event(self) -> None:
        """读取请求体并交给业务服务处理。"""
        path = urlsplit(self.path).path
        if path != self.server.config.listen_path:
            self._send_text(404, "Not Found")
            return

        length = self._content_length()
        if length is None:
            return
        if length > self.server.config.max_request_bytes:
            self._send_text(413, "Payload Too Large")
            return

        try:
            self.connection.settimeout(self.server.config.request_read_timeout_seconds)
            body = self.rfile.read(length) if length > 0 else b""
        except (OSError, socket.timeout):
            LOGGER.warning("request body read timed out: %s %s", self.command, self.path)
            self._send_text(408, "Request Timeout")
            return

        if len(body) != length:
            self._send_text(400, "Bad Request")
            return

        content_type = self.headers.get("Content-Type", "")
        client_ip = self.client_address[0] if self.client_address else "unknown"
        LOGGER.info(
            "%s %s ip=%s len=%s content_type=%s",
            self.command,
            self.path,
            client_ip,
            length,
            content_type or "-",
        )
        try:
            self.server.service.handle_request(content_type, body, client_ip=client_ip)
        except Exception:
            LOGGER.exception("request handling failed")
            self._send_text(500, "Internal Server Error")
            return
        self._send_text(200, "OK")

    def _handle_image_request(self, path: str) -> None:
        """返回本地保存的过车图片。"""
        image_name = self.server.image_name_from_path(path)
        if image_name is None:
            self._send_text(404, "Not Found")
            return

        client_ip = self.client_address[0] if self.client_address else "unknown"
        if not self.server.image_rate_limiter.allow(client_ip):
            LOGGER.warning("image rate limit exceeded for %s: %s", client_ip, path)
            self._send_text(429, "Too Many Requests")
            return

        image_path = self.server.service.store.resolve_public_image_path(image_name)
        if image_path is None:
            self._send_text(404, "Not Found")
            return

        try:
            data = image_path.read_bytes()
        except OSError:
            LOGGER.exception("failed to read image file: %s", image_path)
            self._send_text(404, "Not Found")
            return

        content_type = mimetypes.guess_type(image_path.name)[0] or "application/octet-stream"
        self._send_bytes(200, data, content_type)

    def _handle_health_request(self) -> None:
        """返回进程和 SQLite 探针组成的健康检查 JSON。"""
        try:
            db_ok = self.server.service.is_database_healthy()
        except Exception:
            LOGGER.exception("health check failed")
            db_ok = False
        payload = {
            "status": "ok" if db_ok else "error",
            "server_running": True,
            "db_ok": db_ok,
            "time": datetime.now().isoformat(timespec="seconds"),
        }
        self._send_json(200 if db_ok else 503, payload)

    def _handle_status_request(self) -> None:
        """返回最小运维状态 JSON，便于探针和人工排查。"""
        try:
            payload = self.server.service.get_status_snapshot()
        except Exception:
            LOGGER.exception("status endpoint failed")
            self._send_json(
                500,
                {
                    "status": "error",
                    "server_running": True,
                    "message": "failed to build status snapshot",
                    "time": datetime.now().isoformat(timespec="seconds"),
                },
            )
            return
        self._send_json(200, payload)

    def _content_length(self) -> int | None:
        """读取并校验 Content-Length。"""
        header_value = self.headers.get("Content-Length")
        if header_value is None:
            self._send_text(400, "Missing Content-Length")
            return None
        try:
            length = int(header_value)
        except ValueError:
            self._send_text(400, "Invalid Content-Length")
            return None
        if length < 0:
            self._send_text(400, "Invalid Content-Length")
            return None
        return length

    def _send_text(self, status_code: int, body: str) -> None:
        """发送 UTF-8 纯文本响应。"""
        self._send_bytes(status_code, body.encode("utf-8"), "text/plain; charset=utf-8")

    def _send_json(self, status_code: int, payload: dict[str, object]) -> None:
        """发送 UTF-8 JSON 响应。"""
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send_bytes(status_code, body, "application/json; charset=utf-8")

    def _send_bytes(self, status_code: int, body: bytes, content_type: str) -> None:
        """发送二进制响应。"""
        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        """把 BaseHTTPRequestHandler 的访问日志接入项目 logger。"""
        LOGGER.debug(fmt, *args)


@dataclass
class _RateBucket:
    """单个客户端 IP 的令牌桶状态。"""

    tokens: float
    updated_at: float
    last_seen: float


class ImageRateLimiter:
    """对图片访问做按 IP 的内存型令牌桶限流。"""

    def __init__(self, per_minute: int, burst: int):
        self.per_minute = max(1, int(per_minute))
        self.burst = max(1, int(burst))
        self.refill_per_second = self.per_minute / 60.0
        self._buckets: dict[str, _RateBucket] = {}
        self._lock = threading.Lock()
        self._cleanup_counter = 0

    def allow(self, key: str) -> bool:
        """判断当前请求是否允许通过。"""
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _RateBucket(float(self.burst), now, now)
                self._buckets[key] = bucket
            else:
                elapsed = max(0.0, now - bucket.updated_at)
                bucket.tokens = min(float(self.burst), bucket.tokens + elapsed * self.refill_per_second)
                bucket.updated_at = now
                bucket.last_seen = now

            allowed = bucket.tokens >= 1.0
            if allowed:
                bucket.tokens -= 1.0

            self._cleanup_counter += 1
            if self._cleanup_counter >= 256:
                self._cleanup(now)
                self._cleanup_counter = 0
            return allowed

    def _cleanup(self, now: float) -> None:
        """清理长时间未访问的 IP bucket。"""
        stale_keys = [
            key
            for key, bucket in self._buckets.items()
            if now - bucket.last_seen >= _RATE_LIMIT_STALE_SECONDS
        ]
        for key in stale_keys:
            self._buckets.pop(key, None)
