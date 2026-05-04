"""海康消息接收 HTTP server。"""

from __future__ import annotations

import asyncio
import logging
import mimetypes
import os
import socket
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import uvicorn
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from bdzc_parking.common import iso_now
from bdzc_parking.config import AppConfig
from bdzc_parking.service import ParkingBridgeService


LOGGER = logging.getLogger(__name__)
_LISTEN_HOST = "0.0.0.0"
_RATE_LIMIT_STALE_SECONDS = 600.0
_MAX_HEADER_COUNT = 100
_MAX_HEADER_BYTES = 16 * 1024
_MAX_REQUEST_PATH_CHARS = 2048
_MAX_REQUEST_BYTES = 1_048_576
_REQUEST_READ_TIMEOUT_SECONDS = 15.0
_HTTP_MAX_CONNECTIONS = 64
_HTTP_REQUEST_QUEUE_SIZE = 128
_IMAGE_RATE_LIMIT_PER_MINUTE = 60
_IMAGE_RATE_LIMIT_BURST = 20
_REQUEST_COUNTER = 0
_REQUEST_COUNTER_LOCK = threading.Lock()
_SERVER_START_TIMEOUT_SECONDS = 5.0
_SERVER_STOP_TIMEOUT_SECONDS = 5.0


@dataclass
class _UvicornRuntime:
    """单次 HTTP server 运行实例的底层对象。"""

    server: uvicorn.Server
    socket: socket.socket
    server_port: int


@dataclass
class _LifecycleState:
    """Store HTTP server lifecycle fields and render their public snapshot."""

    state: str = "stopped"
    desired_running: bool = False
    stop_requested: bool = False
    last_start_requested_at: str = ""
    last_started_at: str = ""
    last_stopped_at: str = ""
    last_failed_at: str = ""
    last_failure_reason: str = ""

    def mark_starting(self) -> None:
        """Record that the user or config requested server startup."""
        self.state = "starting"
        self.desired_running = True
        self.stop_requested = False
        self.last_start_requested_at = iso_now()

    def mark_running(self) -> None:
        """Record that Uvicorn confirmed the listening server is running."""
        self.state = "running"
        self.last_started_at = iso_now()
        self.last_failed_at = ""
        self.last_failure_reason = ""

    def mark_stopped(self) -> None:
        """Record a normal user-requested stop."""
        self.state = "stopped"
        self.last_stopped_at = iso_now()

    def record_failure(self, reason: str) -> None:
        """Record a non-user-requested failure reason for the server."""
        self.state = "failed"
        self.last_failed_at = iso_now()
        self.last_failure_reason = str(reason or "unknown failure")[:1000]

    def snapshot(self, thread_alive: bool, server_port: int | None) -> dict[str, object]:
        """Return the lifecycle JSON shape used by GUI and /status."""
        return {
            "state": self.state,
            "desired_running": self.desired_running,
            "thread_alive": thread_alive,
            "server_port": server_port,
            "last_start_requested_at": self.last_start_requested_at,
            "last_started_at": self.last_started_at,
            "last_stopped_at": self.last_stopped_at,
            "last_failed_at": self.last_failed_at,
            "last_failure_reason": self.last_failure_reason,
        }


@dataclass
class _RuntimeStats:
    """Store in-memory HTTP request counters and render their public snapshot."""

    active_requests: int = 0
    busy_response_count: int = 0
    request_exception_count: int = 0
    last_request_exception_at: str = ""
    last_request_exception: str = ""
    _lock: Any = field(default_factory=threading.Lock, repr=False)

    def begin_request(self) -> int:
        """Record a request start and return the process-local request id."""
        request_id = _next_request_id()
        with self._lock:
            self.active_requests += 1
        return request_id

    def finish_request(self) -> None:
        """Record that a request finished."""
        with self._lock:
            self.active_requests = max(0, self.active_requests - 1)

    def record_busy_response(self) -> None:
        """Record a 503 response caused by the business ingress queue being full."""
        with self._lock:
            self.busy_response_count += 1

    def record_request_exception(self, summary: str) -> None:
        """Record a request-handling exception summary for /status."""
        with self._lock:
            self.request_exception_count += 1
            self.last_request_exception_at = iso_now()
            self.last_request_exception = summary[:1000]

    def snapshot(self, lifecycle: dict[str, object]) -> dict[str, object]:
        """Return the runtime JSON shape used by /status."""
        with self._lock:
            active_requests = self.active_requests
            busy_response_count = self.busy_response_count
            exception_count = self.request_exception_count
            last_exception_at = self.last_request_exception_at
            last_exception = self.last_request_exception
        return {
            "active_requests": active_requests,
            "busy_response_count": busy_response_count,
            "request_exception_count": exception_count,
            "last_request_exception_at": last_exception_at,
            "last_request_exception": last_exception,
            "lifecycle": lifecycle,
        }


class BridgeHTTPServer:
    """封装嵌入式 Uvicorn HTTP server 的启动、停止和生命周期状态。"""

    def __init__(self, config: AppConfig, service: ParkingBridgeService):
        """保存监听配置和用于处理请求的桥接服务。"""
        self.config = config
        self.service = service
        self._runtime: _UvicornRuntime | None = None
        self._thread: threading.Thread | None = None
        self._lock = threading.RLock()
        self._lifecycle = _LifecycleState()
        self._stats = _RuntimeStats()
        self.image_rate_limiter = ImageRateLimiter(
            _IMAGE_RATE_LIMIT_PER_MINUTE,
            _IMAGE_RATE_LIMIT_BURST,
        )

    @property
    def is_running(self) -> bool:
        """返回 HTTP server 是否处于运行状态。"""
        with self._lock:
            return self._lifecycle.state == "running" and self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """同步绑定端口并启动后台 Uvicorn HTTP server 线程。"""
        with self._lock:
            if self._lifecycle.state in {"starting", "running"}:
                return
            if self._lifecycle.state == "stopping":
                raise RuntimeError("HTTP server is stopping")
            self._lifecycle.mark_starting()

        try:
            runtime = self._create_runtime()
        except Exception as exc:
            self._record_start_failure(exc)
            raise

        thread = threading.Thread(
            target=self._serve_runtime,
            args=(runtime,),
            name="hikvision-http-server",
            daemon=True,
        )
        with self._lock:
            self._runtime = runtime
            self._thread = thread

        try:
            thread.start()
            self._wait_until_started(runtime)
        except Exception:
            self._request_runtime_exit(runtime)
            raise

        LOGGER.info(
            "HTTP server listening on %s:%s pid=%s server_id=%s thread_id=%s native_thread_id=%s",
            _LISTEN_HOST,
            runtime.server_port,
            os.getpid(),
            id(runtime.server),
            thread.ident,
            thread.native_id,
        )

    def stop(self) -> None:
        """停止 HTTP server 并等待后台线程退出。"""
        with self._lock:
            self._lifecycle.desired_running = False
            if self._runtime is None:
                if self._lifecycle.state in {"starting", "running", "stopping"}:
                    self._lifecycle.mark_stopped()
                return
            runtime = self._runtime
            thread = self._thread
            self._lifecycle.stop_requested = True
            self._lifecycle.state = "stopping"

        LOGGER.info(
            "HTTP server stop requested server_id=%s thread_alive=%s",
            id(runtime.server),
            thread is not None and thread.is_alive(),
        )
        self._request_runtime_exit(runtime)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=_SERVER_STOP_TIMEOUT_SECONDS)
            if thread.is_alive():
                runtime.server.force_exit = True
                thread.join(timeout=2)
            if thread.is_alive():
                LOGGER.warning("HTTP server thread did not exit within timeout")

    def get_lifecycle_snapshot(self) -> dict[str, object]:
        """返回 HTTP server 生命周期状态，供 GUI 和 /status 展示。"""
        with self._lock:
            thread_alive = self._thread is not None and self._thread.is_alive()
            server_port = self._runtime.server_port if self._runtime is not None else None
            return self._lifecycle.snapshot(thread_alive, server_port)

    def get_runtime_snapshot(self) -> dict[str, object]:
        """返回 HTTP server 自身的连接、请求和生命周期指标。"""
        return self._stats.snapshot(self.get_lifecycle_snapshot())

    def _create_runtime(self) -> _UvicornRuntime:
        """创建 Starlette app、预绑定监听 socket 并组装 Uvicorn runtime。"""
        listen_socket = _bind_listen_socket(
            _LISTEN_HOST,
            self.config.listen_port,
            _HTTP_REQUEST_QUEUE_SIZE,
        )
        server_port = int(listen_socket.getsockname()[1])
        self.image_rate_limiter = ImageRateLimiter(
            _IMAGE_RATE_LIMIT_PER_MINUTE,
            _IMAGE_RATE_LIMIT_BURST,
        )
        app = self._build_asgi_app()
        uvicorn_config = uvicorn.Config(
            app,
            host=_LISTEN_HOST,
            port=server_port,
            log_config=None,
            access_log=False,
            server_header=False,
            log_level="warning",
            lifespan="off",
            limit_concurrency=_HTTP_MAX_CONNECTIONS + 1,
            backlog=_HTTP_REQUEST_QUEUE_SIZE,
            timeout_keep_alive=_REQUEST_READ_TIMEOUT_SECONDS,
            timeout_graceful_shutdown=5,
        )
        return _UvicornRuntime(uvicorn.Server(uvicorn_config), listen_socket, server_port)

    def _build_asgi_app(self) -> Starlette:
        """创建处理状态查询、海康上报和图片访问的 ASGI app。"""
        app = Starlette(
            routes=[
                Route("/", self._handle_get_request, methods=["GET"]),
                Route("/{path:path}", self._handle_get_request, methods=["GET"]),
                Route("/{path:path}", self._handle_hikvision_event, methods=["POST", "PUT"]),
            ]
        )
        app.add_middleware(_RequestLifecycleMiddleware, owner=self)
        return app

    def _serve_runtime(self, runtime: _UvicornRuntime) -> None:
        """运行 Uvicorn 主循环，并把非主动退出记录为故障。"""
        try:
            self._run_uvicorn(runtime)
        except BaseException as exc:
            with self._lock:
                stop_requested = self._lifecycle.stop_requested or not self._lifecycle.desired_running
            if stop_requested:
                LOGGER.debug("HTTP server exited while stopping", exc_info=(type(exc), exc, exc.__traceback__))
            else:
                reason = f"crashed: {type(exc).__name__}: {exc}"
                with self._lock:
                    if self._runtime is runtime:
                        self._record_failure_locked(reason)
                LOGGER.error(
                    "HTTP server thread crashed",
                    exc_info=(type(exc), exc, exc.__traceback__),
                )
        finally:
            self._handle_runtime_exit(runtime)

    def _run_uvicorn(self, runtime: _UvicornRuntime) -> None:
        """执行底层 Uvicorn server；单独方法便于测试异常路径。"""
        runtime.server.run(sockets=[runtime.socket])

    def _wait_until_started(self, runtime: _UvicornRuntime) -> None:
        """等待 Uvicorn 确认启动，确保 start 返回时 server 已可接收连接。"""
        deadline = time.monotonic() + _SERVER_START_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            with self._lock:
                if self._runtime is not runtime:
                    raise RuntimeError(self._lifecycle.last_failure_reason or "HTTP server stopped during startup")
                if self._lifecycle.state == "failed":
                    raise RuntimeError(self._lifecycle.last_failure_reason or "HTTP server failed during startup")
                thread_alive = self._thread is not None and self._thread.is_alive()
            if runtime.server.started:
                with self._lock:
                    if self._runtime is runtime:
                        self._lifecycle.mark_running()
                return
            if not thread_alive:
                time.sleep(0.02)
                continue
            time.sleep(0.02)

        reason = "HTTP server startup timed out"
        with self._lock:
            if self._runtime is runtime:
                self._record_failure_locked(reason)
        raise TimeoutError(reason)

    def _handle_runtime_exit(self, runtime: _UvicornRuntime) -> None:
        """清理已退出的 Uvicorn runtime，并按退出原因更新生命周期状态。"""
        try:
            runtime.socket.close()
        except OSError:
            pass

        with self._lock:
            if self._runtime is not runtime:
                return
            stop_requested = self._lifecycle.stop_requested or not self._lifecycle.desired_running
            if stop_requested:
                self._lifecycle.mark_stopped()
                LOGGER.info("HTTP server stopped")
            elif self._lifecycle.state != "failed":
                self._record_failure_locked("server returned unexpectedly")
                LOGGER.error("HTTP server returned unexpectedly server_id=%s", id(runtime.server))

            self._runtime = None
            self._thread = None
            self._lifecycle.stop_requested = False

    def _request_runtime_exit(self, runtime: _UvicornRuntime) -> None:
        """请求 Uvicorn runtime 按自身 graceful shutdown 机制退出。"""
        runtime.server.should_exit = True

    def _record_start_failure(self, exc: BaseException) -> None:
        """记录 start 阶段同步绑定或创建 runtime 的失败。"""
        reason = f"start failed: {type(exc).__name__}: {exc}"
        with self._lock:
            self._record_failure_locked(reason)
            self._runtime = None
            self._thread = None
        LOGGER.error(
            "failed to start HTTP server",
            exc_info=(type(exc), exc, exc.__traceback__),
        )

    def _record_failure_locked(self, reason: str) -> None:
        """在已持有锁时把 HTTP server 标记为故障。"""
        self._lifecycle.record_failure(reason)

    def _begin_request(self) -> int:
        """记录请求开始并返回进程内请求编号。"""
        return self._stats.begin_request()

    def _finish_request(self) -> None:
        """记录请求结束并减少活动请求数。"""
        self._stats.finish_request()

    def _record_busy_response(self) -> None:
        """记录一次业务接收队列满导致的 503 Busy 响应。"""
        self._stats.record_busy_response()

    def _record_request_exception(self, context: str, request: Request, exc: BaseException) -> None:
        """记录 HTTP 请求处理异常摘要，供 /status 排查。"""
        summary = (
            f"{context} client={_client_ip(request)} method={request.method} "
            f"path={request.url.path} {type(exc).__name__}: {exc}"
        )
        self._stats.record_request_exception(summary)

    async def _handle_get_request(self, request: Request) -> Response:
        """处理 GET 路由。"""
        path = request.url.path
        if path == "/":
            return _text_response(200, "BDZC Parking Bridge is running")
        if path == "/status":
            return await self._handle_status_request()
        if self.is_image_request(path):
            return await self._handle_image_request(request, path)
        return _text_response(404, "Not Found")

    async def _handle_hikvision_event(self, request: Request) -> Response:
        """读取海康上报请求体并交给业务服务处理。"""
        if request.url.path != self.config.listen_path:
            return _text_response(404, "Not Found")

        length = _content_length(request)
        if length is None:
            return _text_response(400, "Missing Content-Length")
        if length < 0:
            return _text_response(400, "Invalid Content-Length")
        if length > _MAX_REQUEST_BYTES:
            request.state.request_body_length = length
            return _text_response(413, "Payload Too Large")

        try:
            body = await asyncio.wait_for(
                request.body(),
                timeout=_REQUEST_READ_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            LOGGER.warning("request body read timed out: %s %s", request.method, request.url.path)
            return _text_response(408, "Request Timeout")
        except Exception as exc:
            self._record_request_exception("request_body", request, exc)
            LOGGER.warning("failed to read request body: %s %s", request.method, request.url.path, exc_info=True)
            return _text_response(400, "Bad Request")

        request.state.request_body_length = length
        if len(body) != length:
            return _text_response(400, "Bad Request")

        content_type = request.headers.get("content-type", "")
        client_ip = _client_ip(request)
        LOGGER.debug(
            "HTTP body accepted method=%s path=%s client=%s bytes=%s content_type=%s",
            request.method,
            request.url.path,
            client_ip,
            length,
            content_type or "-",
        )
        request_id = getattr(request.state, "request_id", "-")
        accepted = await run_in_threadpool(
            self.service.enqueue_http_request,
            content_type,
            body,
            client_ip,
            request_id,
        )
        if not accepted:
            self._record_busy_response()
            return _text_response(503, "Busy")
        return _text_response(200, "OK")

    async def _handle_status_request(self) -> Response:
        """返回包含健康检查的最小运维状态 JSON。"""
        try:
            service_runtime = await run_in_threadpool(self.service.get_runtime_snapshot)
            try:
                db_ok = await run_in_threadpool(self.service.is_database_healthy)
            except Exception:
                LOGGER.exception("database health probe failed")
                db_ok = False
            database_snapshot = None
            if db_ok:
                database_snapshot = await run_in_threadpool(self.service.get_status_snapshot)
            payload = self._build_status_payload(db_ok, service_runtime, database_snapshot)
        except Exception:
            LOGGER.exception("status endpoint failed")
            return _json_response(
                500,
                {
                    "status": "error",
                    "db_ok": False,
                    "message": "failed to build status snapshot",
                    "time": iso_now(),
                },
            )
        LOGGER.debug("status snapshot returned status=%s db_ok=%s", payload["status"], db_ok)
        return _json_response(200 if db_ok else 503, payload)

    def _build_status_payload(
        self,
        db_ok: bool,
        service_runtime: dict[str, object],
        database_snapshot: dict[str, object] | None,
    ) -> dict[str, object]:
        """把 service、storage 和 HTTP server 快照组合成稳定的 /status schema。"""
        database_snapshot = database_snapshot or {}
        return {
            "status": "ok" if db_ok else "error",
            "time": iso_now(),
            "db_ok": db_ok,
            "queues": service_runtime["queues"],
            "workers": service_runtime["workers"],
            "events": {
                "last_success_sent_at": str(database_snapshot.get("last_success_sent_at") or ""),
                "failed_retryable": database_snapshot.get("failed_retryable_count"),
                "dead_letter": database_snapshot.get("dead_letter_count"),
                "failure_backlog": database_snapshot.get("failure_backlog_count"),
            },
            "database": {
                "main_size_bytes": database_snapshot.get("db_main_size_bytes"),
                "total_size_bytes": database_snapshot.get("db_total_size_bytes"),
            },
            "http_server": self.get_runtime_snapshot(),
        }

    async def _handle_image_request(self, request: Request, path: str) -> Response:
        """返回本地保存的过车图片。"""
        image_name = self.image_name_from_path(path)
        if image_name is None:
            return _text_response(404, "Not Found")

        client_ip = _client_ip(request)
        if not self.image_rate_limiter.allow(client_ip):
            LOGGER.warning("image rate limit exceeded for %s: %s", client_ip, path)
            return _text_response(429, "Too Many Requests")

        image_path = await run_in_threadpool(self.service.store.resolve_public_image_path, image_name)
        if image_path is None:
            LOGGER.debug("image not found client=%s name=%s", client_ip, image_name)
            return _text_response(404, "Not Found")

        try:
            data = await run_in_threadpool(image_path.read_bytes)
        except OSError:
            LOGGER.exception("failed to read image file: %s", image_path)
            return _text_response(404, "Not Found")

        content_type = mimetypes.guess_type(image_path.name)[0] or "application/octet-stream"
        LOGGER.debug("image served client=%s name=%s bytes=%s", client_ip, image_name, len(data))
        return _bytes_response(200, data, content_type)

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


class _RequestLifecycleMiddleware(BaseHTTPMiddleware):
    """为每个 HTTP 请求提供异常边界、协议限制和访问日志。"""

    def __init__(self, app: Any, owner: BridgeHTTPServer):
        """保存外层 BridgeHTTPServer 以更新运行指标。"""
        super().__init__(app)
        self.owner = owner

    async def dispatch(self, request: Request, call_next: Callable[[Request], Any]) -> Response:
        """处理一次 HTTP 请求的生命周期。"""
        request_id = self.owner._begin_request()
        request.state.request_id = request_id
        request.state.request_body_length = 0
        started_at = time.monotonic()
        response: Response | None = None
        try:
            response = self._reject_invalid_request(request)
            if response is None:
                response = await call_next(request)
        except Exception as exc:
            self.owner._record_request_exception("request", request, exc)
            LOGGER.error(
                "HTTP request handling failed request_id=%s client=%s method=%s path=%s",
                request_id,
                _client_ip(request),
                request.method,
                request.url.path,
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            response = _text_response(500, "Internal Server Error")
        finally:
            self.owner._finish_request()

        response.headers["Connection"] = "close"
        self._log_request_summary(request, response, started_at)
        return response

    def _reject_invalid_request(self, request: Request) -> Response | None:
        """按生产运行限制拒绝异常路径和过大请求头。"""
        if len(str(request.url.path)) > _MAX_REQUEST_PATH_CHARS:
            return _text_response(414, "URI Too Long")

        header_items = list(request.headers.raw)
        if len(header_items) > _MAX_HEADER_COUNT:
            return _text_response(431, "Too Many Request Headers")

        header_bytes = sum(len(name) + len(value) + 4 for name, value in header_items)
        if header_bytes > _MAX_HEADER_BYTES:
            return _text_response(431, "Request Header Fields Too Large")
        return None

    def _log_request_summary(self, request: Request, response: Response, started_at: float) -> None:
        """记录单次请求的核心信息和耗时。"""
        if self._should_skip_request_summary(request, response):
            return
        elapsed_ms = (time.monotonic() - started_at) * 1000.0
        response_length = response.headers.get("content-length", "-")
        LOGGER.debug(
            "HTTP request request_id=%s client=%s method=%s path=%s status=%s request_bytes=%s response_bytes=%s elapsed_ms=%.1f",
            getattr(request.state, "request_id", "-"),
            _client_ip(request),
            request.method,
            request.url.path,
            response.status_code,
            getattr(request.state, "request_body_length", 0),
            response_length,
            elapsed_ms,
        )

    def _should_skip_request_summary(self, request: Request, response: Response) -> bool:
        """判断成功的轻量探针是否应跳过单次请求摘要。"""
        return (
            request.method == "GET"
            and request.url.path in {"/", "/status"}
            and response.status_code < 400
        )


@dataclass
class _RateBucket:
    """单个客户端 IP 的令牌桶状态。"""

    tokens: float
    updated_at: float
    last_seen: float


class ImageRateLimiter:
    """对图片访问做按 IP 的内存型令牌桶限流。"""

    def __init__(self, per_minute: int, burst: int):
        """初始化令牌桶容量和补充速度。"""
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


def _bind_listen_socket(host: str, port: int, backlog: int) -> socket.socket:
    """同步绑定监听 socket，让启动错误直接从 start 抛出。"""
    last_error: OSError | None = None
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    for family, socktype, proto, _canonname, sockaddr in infos:
        listen_socket = socket.socket(family, socktype, proto)
        try:
            if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
                listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            else:
                listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if family == socket.AF_INET6 and hasattr(socket, "IPV6_V6ONLY"):
                listen_socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            listen_socket.bind(sockaddr)
            listen_socket.listen(max(1, int(backlog)))
            listen_socket.set_inheritable(False)
            return listen_socket
        except OSError as exc:
            last_error = exc
            listen_socket.close()
    if last_error is not None:
        raise last_error
    raise OSError(f"no address available for {host}:{port}")


def _content_length(request: Request) -> int | None:
    """读取并校验 Content-Length。"""
    header_value = request.headers.get("content-length")
    if header_value is None:
        return None
    try:
        return int(header_value)
    except ValueError:
        return -1


def _text_response(status_code: int, body: str) -> PlainTextResponse:
    """创建 UTF-8 纯文本响应。"""
    return PlainTextResponse(body, status_code=status_code)


def _json_response(status_code: int, payload: dict[str, object]) -> JSONResponse:
    """创建 UTF-8 JSON 响应。"""
    return JSONResponse(payload, status_code=status_code)


def _bytes_response(status_code: int, body: bytes, content_type: str) -> Response:
    """创建二进制响应。"""
    return Response(body, status_code=status_code, media_type=content_type)


def _client_ip(request: Request) -> str:
    """返回当前请求客户端 IP。"""
    return request.client.host if request.client is not None else "unknown"


def _next_request_id() -> int:
    """生成进程内单调递增的 HTTP 请求编号，便于关联日志。"""
    global _REQUEST_COUNTER
    with _REQUEST_COUNTER_LOCK:
        _REQUEST_COUNTER += 1
        return _REQUEST_COUNTER
