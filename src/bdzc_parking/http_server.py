"""海康消息接收 HTTP server。"""

from __future__ import annotations

import json
import logging
import mimetypes
import socket
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import urllib.error
import urllib.request
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
        self._lock = threading.RLock()
        self._watchdog_stop_event = threading.Event()
        self._watchdog_thread: threading.Thread | None = None
        self._watchdog_failure_count = 0
        self._watchdog_restart_count = 0
        self._watchdog_last_probe_at = ""
        self._watchdog_last_probe_error = ""
        self._watchdog_last_restart_at = ""
        self._watchdog_next_restart_allowed_at = ""
        self._watchdog_next_restart_allowed_monotonic = 0.0

    @property
    def is_running(self) -> bool:
        """返回 HTTP server 是否已经启动。"""
        with self._lock:
            self._cleanup_stopped_server_locked()
            return self._server is not None and self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """启动后台 HTTP server 线程。"""
        with self._lock:
            self._cleanup_stopped_server_locked()
            if self._server is None:
                self._start_server_locked()
            self._start_watchdog_locked()

    def stop(self) -> None:
        """停止 HTTP server 并等待后台线程退出。"""
        self._stop_watchdog()
        self._stop_current_server()

    def get_watchdog_snapshot(self) -> dict[str, object]:
        """返回 HTTP server watchdog 的当前状态，供 /status 展示。"""
        with self._lock:
            next_restart_allowed_at = self._watchdog_next_restart_allowed_at
            if self._watchdog_next_restart_allowed_monotonic <= time.monotonic():
                next_restart_allowed_at = ""
            return {
                "enabled": self._watchdog_thread is not None and self._watchdog_thread.is_alive(),
                "failure_count": self._watchdog_failure_count,
                "restart_count": self._watchdog_restart_count,
                "last_probe_at": self._watchdog_last_probe_at,
                "last_probe_error": self._watchdog_last_probe_error,
                "last_restart_at": self._watchdog_last_restart_at,
                "next_restart_allowed_at": next_restart_allowed_at,
            }

    def _start_server_locked(self) -> None:
        """在已持有锁时创建 HTTP server 并启动接收线程。"""
        server = _ServiceHTTPServer(
            (self.config.listen_host, self.config.listen_port),
            HikRequestHandler,
            self.config,
            self.service,
            self,
        )
        thread = threading.Thread(
            target=self._serve_forever,
            args=(server,),
            name="hikvision-http-server",
            daemon=True,
        )
        self._server = server
        self._thread = thread
        try:
            thread.start()
        except Exception:
            self._server = None
            self._thread = None
            server.server_close()
            raise

        LOGGER.info(
            "HTTP server listening on %s:%s",
            self.config.listen_host,
            server.server_port,
        )

    def _serve_forever(self, server: _ServiceHTTPServer) -> None:
        """运行 HTTP server 主循环，并记录导致接收线程退出的异常。"""
        try:
            server.serve_forever()
        except Exception:
            with self._lock:
                is_active_server = self._server is server
            if is_active_server:
                LOGGER.exception("HTTP server thread crashed")
            else:
                LOGGER.debug("HTTP server thread exited during server cleanup", exc_info=True)
        finally:
            self._handle_server_thread_exit(server)

    def _handle_server_thread_exit(self, server: _ServiceHTTPServer) -> None:
        """接收线程退出时关闭仍被当前对象持有的监听 socket。"""
        with self._lock:
            if self._server is not server:
                return
            self._server = None
            self._thread = None

        LOGGER.warning("HTTP server thread exited unexpectedly; closing stale listening socket")
        try:
            server.server_close()
        except Exception:
            LOGGER.exception("failed to close stale HTTP server socket after thread exit")

    def _stop_current_server(self) -> None:
        """停止当前 HTTP server 实例，但不处理 watchdog 生命周期。"""
        with self._lock:
            server = self._server
            thread = self._thread
            self._server = None
            self._thread = None

        if server is None:
            return

        try:
            thread_alive = thread is not None and thread.is_alive()
            if thread_alive:
                shutdown_thread = threading.Thread(
                    target=server.shutdown,
                    name="hikvision-http-server-shutdown",
                    daemon=True,
                )
                try:
                    shutdown_thread.start()
                    shutdown_thread.join(timeout=5)
                    if shutdown_thread.is_alive():
                        LOGGER.warning("HTTP server shutdown timed out; closing listening socket")
                except Exception:
                    LOGGER.exception("failed to request HTTP server shutdown")
            else:
                LOGGER.warning("HTTP server thread already stopped; closing stale server socket")

            if thread is not None and thread is not threading.current_thread():
                thread.join(timeout=5)
                if thread.is_alive():
                    LOGGER.warning("HTTP server thread did not exit within timeout")
        finally:
            try:
                server.server_close()
            except Exception:
                LOGGER.exception("failed to close HTTP server socket")
        LOGGER.info("HTTP server stopped")

    def _cleanup_stopped_server_locked(self) -> None:
        """清理已经退出但还残留在对象里的 HTTP server。"""
        if self._server is None:
            self._thread = None
            return
        if self._thread is not None and self._thread.is_alive():
            return

        LOGGER.warning("HTTP server thread is not alive; cleaning up stale server state")
        try:
            self._server.server_close()
        except Exception:
            LOGGER.exception("failed to close stale HTTP server socket")
        self._server = None
        self._thread = None

    def _start_watchdog_locked(self) -> None:
        """在已持有锁时启动 HTTP server 健康守护线程。"""
        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            return
        self._watchdog_stop_event.clear()
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name="hikvision-http-watchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

    def _stop_watchdog(self) -> None:
        """停止 HTTP server 健康守护线程。"""
        with self._lock:
            thread = self._watchdog_thread
            self._watchdog_stop_event.set()

        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5)
            if thread.is_alive():
                LOGGER.warning("HTTP server watchdog thread did not exit within timeout")

        with self._lock:
            if self._watchdog_thread is thread:
                self._watchdog_thread = None

    def _watchdog_loop(self) -> None:
        """周期性从本机探测 HTTP server，连续失败后自动重启。"""
        while not self._watchdog_stop_event.wait(self.config.http_watchdog_interval_seconds):
            try:
                self._watchdog_once()
            except Exception as exc:
                error = f"watchdog error: {type(exc).__name__}: {exc}"
                LOGGER.exception("HTTP server watchdog loop failed")
                failure_count = self._record_watchdog_failure(error)
                if failure_count >= self.config.http_watchdog_failure_threshold:
                    self._restart_from_watchdog(error)

    def _watchdog_once(self) -> None:
        """执行一次 HTTP server 健康检查和必要的自动重启。"""
        if self._is_server_thread_dead():
            reason = "HTTP server thread is not alive"
            self._record_watchdog_failure(reason)
            self._restart_from_watchdog(reason)
            return

        ok, error = self._probe_http_root()
        if ok:
            self._record_watchdog_success()
            return

        failure_count = self._record_watchdog_failure(error)
        if failure_count >= self.config.http_watchdog_failure_threshold:
            self._restart_from_watchdog(error)

    def _is_server_thread_dead(self) -> bool:
        """判断 server 对象仍存在但接收线程已经退出。"""
        with self._lock:
            if self._server is None:
                return False
            return self._thread is None or not self._thread.is_alive()

    def _probe_http_root(self) -> tuple[bool, str]:
        """通过 GET / 从外部视角确认 HTTP server 是否可响应。"""
        with self._lock:
            server = self._server
            if server is None:
                return False, "HTTP server is not running"
            host = _watchdog_probe_host(self.config.listen_host)
            port = server.server_port

        url = f"http://{host}:{port}/"
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        request = urllib.request.Request(url, method="GET")
        try:
            with opener.open(request, timeout=self.config.http_watchdog_timeout_seconds) as response:
                response.read(1)
            return True, ""
        except urllib.error.HTTPError:
            return True, ""
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    def _record_watchdog_success(self) -> None:
        """记录一次成功探测，并清空连续失败计数。"""
        with self._lock:
            self._watchdog_last_probe_at = datetime.now().isoformat(timespec="seconds")
            self._watchdog_last_probe_error = ""
            self._watchdog_failure_count = 0

    def _record_watchdog_failure(self, error: str) -> int:
        """记录一次失败探测，并返回最新连续失败次数。"""
        with self._lock:
            self._watchdog_last_probe_at = datetime.now().isoformat(timespec="seconds")
            self._watchdog_last_probe_error = error
            self._watchdog_failure_count += 1
            return self._watchdog_failure_count

    def _restart_from_watchdog(self, reason: str) -> None:
        """由 watchdog 触发 HTTP server 重启，并按 cooldown 限制频率。"""
        now = time.monotonic()
        with self._lock:
            if now < self._watchdog_next_restart_allowed_monotonic:
                return
            cooldown_until = datetime.now() + timedelta(
                seconds=self.config.http_watchdog_restart_cooldown_seconds
            )
            self._watchdog_next_restart_allowed_monotonic = (
                now + self.config.http_watchdog_restart_cooldown_seconds
            )
            self._watchdog_next_restart_allowed_at = cooldown_until.isoformat(timespec="seconds")

        LOGGER.warning("HTTP server watchdog restarting server: %s", reason)
        try:
            self._stop_current_server()
        except Exception:
            LOGGER.exception("HTTP server watchdog failed while stopping stale server")
        try:
            with self._lock:
                self._start_server_locked()
                self._watchdog_failure_count = 0
                self._watchdog_last_probe_error = ""
                self._watchdog_restart_count += 1
                self._watchdog_last_restart_at = datetime.now().isoformat(timespec="seconds")
        except Exception as exc:
            with self._lock:
                self._watchdog_last_probe_error = f"restart failed: {type(exc).__name__}: {exc}"
            LOGGER.exception("HTTP server watchdog restart failed")
        else:
            LOGGER.info("HTTP server watchdog restarted server")


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
        bridge_server: BridgeHTTPServer,
    ):
        """初始化 HTTP server，并挂载配置和业务服务。"""
        super().__init__(server_address, handler_class)
        self.config = config
        self.service = service
        self.bridge_server = bridge_server
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
            payload["http_watchdog"] = self.server.bridge_server.get_watchdog_snapshot()
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


def _watchdog_probe_host(listen_host: str) -> str:
    """把监听地址转换为 watchdog 从本机访问时使用的主机名。"""
    host = str(listen_host or "").strip()
    if host in {"", "0.0.0.0", "::"}:
        return "127.0.0.1"
    if ":" in host and not host.startswith("["):
        return f"[{host}]"
    return host
