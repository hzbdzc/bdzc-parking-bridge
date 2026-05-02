"""海康消息接收 HTTP server。"""

from __future__ import annotations

import json
import logging
import mimetypes
import os
import socket
import sys
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
from bdzc_parking.safe_logging import log_exception
from bdzc_parking.service import ParkingBridgeService


LOGGER = logging.getLogger(__name__)
_RATE_LIMIT_STALE_SECONDS = 600.0
_MAX_HEADER_COUNT = 100
_MAX_HEADER_BYTES = 16 * 1024
_MAX_REQUEST_PATH_CHARS = 2048
_REQUEST_COUNTER = 0
_REQUEST_COUNTER_LOCK = threading.Lock()
_CLIENT_DISCONNECT_ERRORS = (BrokenPipeError, ConnectionAbortedError, ConnectionResetError)


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
            "HTTP server listening on %s:%s pid=%s server_id=%s thread_id=%s native_thread_id=%s",
            self.config.listen_host,
            server.server_port,
            os.getpid(),
            id(server),
            thread.ident,
            thread.native_id,
        )

    def _serve_forever(self, server: _ServiceHTTPServer) -> None:
        """运行 HTTP server 主循环，并记录导致接收线程退出的异常。"""
        try:
            server.serve_forever()
        except Exception:
            with self._lock:
                is_active_server = self._server is server
            if is_active_server:
                log_exception(LOGGER, "HTTP server thread crashed")
            else:
                LOGGER.debug("HTTP server thread exited during server cleanup", exc_info=True)
        else:
            with self._lock:
                is_active_server = self._server is server
            if is_active_server:
                LOGGER.error(
                    "HTTP server serve_forever returned without exception server_id=%s thread=%s",
                    id(server),
                    threading.current_thread().name,
                )
            else:
                LOGGER.debug("HTTP server serve_forever returned after stop server_id=%s", id(server))
        finally:
            self._handle_server_thread_exit(server)

    def _handle_server_thread_exit(self, server: _ServiceHTTPServer) -> None:
        """接收线程退出时关闭仍被当前对象持有的监听 socket。"""
        with self._lock:
            if self._server is not server:
                return
            self._server = None
            self._thread = None

        LOGGER.warning(
            "HTTP server thread exited unexpectedly; closing stale listening socket server_id=%s",
            id(server),
        )
        try:
            server.server_close()
        except Exception:
            log_exception(LOGGER, "failed to close stale HTTP server socket after thread exit")

    def _stop_current_server(self) -> None:
        """停止当前 HTTP server 实例，但不处理 watchdog 生命周期。"""
        with self._lock:
            server = self._server
            thread = self._thread
            self._server = None
            self._thread = None

        if server is None:
            return

        LOGGER.info(
            "HTTP server stop requested server_id=%s thread_alive=%s",
            id(server),
            thread is not None and thread.is_alive(),
        )
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
                    log_exception(LOGGER, "failed to request HTTP server shutdown")
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
                log_exception(LOGGER, "failed to close HTTP server socket")
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
            log_exception(LOGGER, "failed to close stale HTTP server socket")
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
                log_exception(LOGGER, "HTTP server watchdog loop failed")
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
            log_exception(LOGGER, "HTTP server watchdog failed while stopping stale server")
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
            log_exception(LOGGER, "HTTP server watchdog restart failed")
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
        self.request_queue_size = max(1, int(config.http_request_queue_size))
        super().__init__(server_address, handler_class)
        self.config = config
        self.service = service
        self.bridge_server = bridge_server
        self.max_connections = max(1, int(config.http_max_connections))
        self._connection_slots = threading.BoundedSemaphore(self.max_connections)
        self._connection_stats_lock = threading.Lock()
        self._active_connections = 0
        self._rejected_connections = 0
        self._request_entry_exception_count = 0
        self._last_request_entry_exception_at = ""
        self._last_request_entry_exception = ""
        self.image_rate_limiter = ImageRateLimiter(
            config.image_rate_limit_per_minute,
            config.image_rate_limit_burst,
        )

    def process_request(self, request, client_address) -> None:
        """在创建请求线程前限制并发连接数，避免慢连接耗尽线程。"""
        if not self._connection_slots.acquire(blocking=False):
            with self._connection_stats_lock:
                self._rejected_connections += 1
            self._send_busy_response(request, client_address)
            self.close_request(request)
            return
        with self._connection_stats_lock:
            self._active_connections += 1
        try:
            super().process_request(request, client_address)
        except Exception as exc:
            self._release_connection_slot()
            self._record_request_entry_exception("process_request", client_address, exc)
            log_exception(
                LOGGER,
                "HTTP process_request failed before request thread client=%s",
                _client_address_text(client_address),
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            self._send_fallback_response(
                request,
                client_address,
                503,
                "Service Unavailable",
                "Service Unavailable",
            )
            self.close_request(request)

    def process_request_thread(self, request, client_address) -> None:
        """处理单个请求线程，并兜住 handler 初始化阶段的异常。"""
        try:
            self.finish_request(request, client_address)
        except _CLIENT_DISCONNECT_ERRORS as exc:
            LOGGER.debug(
                "client disconnected before HTTP handler completed: %s error=%s: %s",
                _client_address_text(client_address),
                type(exc).__name__,
                exc,
            )
        except Exception as exc:
            self._record_request_entry_exception("process_request_thread", client_address, exc)
            log_exception(
                LOGGER,
                "HTTP request thread failed before handler completed client=%s",
                _client_address_text(client_address),
                exc_info=(type(exc), exc, exc.__traceback__),
            )
            self._send_fallback_response(
                request,
                client_address,
                500,
                "Internal Server Error",
                "Internal Server Error",
            )
        finally:
            try:
                self.shutdown_request(request)
            finally:
                self._release_connection_slot()

    def _release_connection_slot(self) -> None:
        """归还一个 HTTP 连接并发槽位，并维护运行计数。"""
        with self._connection_stats_lock:
            self._active_connections = max(0, self._active_connections - 1)
        self._connection_slots.release()

    def get_runtime_snapshot(self) -> dict[str, object]:
        """返回 HTTP server 自身的连接与监听队列状态。"""
        with self._connection_stats_lock:
            active_connections = self._active_connections
            rejected_connections = self._rejected_connections
            exception_count = self._request_entry_exception_count
            last_exception_at = self._last_request_entry_exception_at
            last_exception = self._last_request_entry_exception
        return {
            "max_connections": self.max_connections,
            "active_connections": active_connections,
            "rejected_connections": rejected_connections,
            "request_queue_size": self.request_queue_size,
            "request_entry_exception_count": exception_count,
            "last_request_entry_exception_at": last_exception_at,
            "last_request_entry_exception": last_exception,
        }

    def handle_error(self, request, client_address) -> None:
        """记录 handler 线程未捕获异常，避免静默吞掉请求级错误。"""
        exc_type, exc, traceback_obj = sys.exc_info()
        if exc_type is not None and exc is not None:
            self._record_request_entry_exception("handle_error", client_address, exc)
        log_exception(
            LOGGER,
            "uncaught HTTP handler error from %s",
            _client_address_text(client_address),
            exc_info=(exc_type, exc, traceback_obj) if exc_type is not None and exc is not None else None,
        )

    def _record_request_entry_exception(self, context: str, client_address, exc: BaseException) -> None:
        """记录 HTTP 请求入口异常的计数和最近一次摘要，供 /status 排查。"""
        summary = f"{context} client={_client_address_text(client_address)} {type(exc).__name__}: {exc}"
        with self._connection_stats_lock:
            self._request_entry_exception_count += 1
            self._last_request_entry_exception_at = datetime.now().isoformat(timespec="seconds")
            self._last_request_entry_exception = summary[:1000]

    def _send_fallback_response(
        self,
        request,
        client_address,
        status_code: int,
        reason: str,
        body_text: str,
    ) -> None:
        """在 handler 尚未可用时直接向 socket 写入最小 HTTP 错误响应。"""
        body = body_text.encode("utf-8")
        response = (
            f"HTTP/1.1 {status_code} {reason}\r\n"
            "Connection: close\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            f"Content-Length: {len(body)}\r\n"
            "\r\n"
        ).encode("ascii") + body
        try:
            request.settimeout(1.0)
            request.sendall(response)
            self._drain_rejected_request(request)
        except _CLIENT_DISCONNECT_ERRORS:
            LOGGER.debug("client disconnected before fallback response: %s", _client_address_text(client_address))
        except OSError as exc:
            self._record_request_entry_exception("fallback_response", client_address, exc)
            log_exception(
                LOGGER,
                "failed to send fallback HTTP response to %s",
                _client_address_text(client_address),
                exc_info=(type(exc), exc, exc.__traceback__),
            )

    def _send_busy_response(self, request, client_address) -> None:
        """并发过高时直接返回 503，保护业务处理线程池。"""
        body = b"Busy"
        response = (
            b"HTTP/1.1 503 Service Unavailable\r\n"
            b"Connection: close\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            b"Content-Length: " + str(len(body)).encode("ascii") + b"\r\n"
            b"\r\n" + body
        )
        try:
            request.settimeout(1.0)
            request.sendall(response)
            self._drain_rejected_request(request)
        except _CLIENT_DISCONNECT_ERRORS:
            LOGGER.debug("client disconnected before busy response: %s", _client_address_text(client_address))
        except OSError:
            LOGGER.warning("failed to send busy response to %s", _client_address_text(client_address), exc_info=True)

    def _drain_rejected_request(self, request) -> None:
        """短暂清理被拒请求的已到达数据，降低 Windows 上立即关闭导致 RST 的概率。"""
        try:
            request.shutdown(socket.SHUT_WR)
        except OSError:
            return

        request.settimeout(0.05)
        remaining = 16 * 1024
        while remaining > 0:
            try:
                data = request.recv(min(4096, remaining))
            except (OSError, socket.timeout):
                break
            if not data:
                break
            remaining -= len(data)

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

    def setup(self) -> None:
        """初始化连接流，并给读写都设置超时。"""
        super().setup()
        try:
            self.connection.settimeout(self.server.config.request_read_timeout_seconds)
        except OSError:
            LOGGER.debug("failed to set HTTP connection timeout", exc_info=True)

    def handle_one_request(self) -> None:
        """为单个 HTTP 请求建立完整的异常边界和访问日志。"""
        self.request_id = _next_request_id()
        self._request_started_at = time.monotonic()
        self._request_body_length = 0
        self._response_status: int | None = None
        self._response_body_length = 0
        try:
            super().handle_one_request()
        except _CLIENT_DISCONNECT_ERRORS as exc:
            self._log_client_disconnect(exc)
            self.close_connection = True
        except socket.timeout:
            LOGGER.warning(
                "HTTP request timed out request_id=%s client=%s",
                self.request_id,
                self._client_ip(),
            )
            self.close_connection = True
        except Exception:
            log_exception(
                LOGGER,
                "HTTP handler failed request_id=%s client=%s method=%s path=%s",
                self.request_id,
                self._client_ip(),
                getattr(self, "command", "-"),
                getattr(self, "path", "-"),
            )
            self._send_text(500, "Internal Server Error")
            self.close_connection = True
        finally:
            self._log_request_summary()

    def parse_request(self) -> bool:
        """在标准库解析完成后追加生产运行所需的协议限制。"""
        if not super().parse_request():
            return False
        if len(self.path) > _MAX_REQUEST_PATH_CHARS:
            self.send_error(414, "URI Too Long")
            return False

        header_items = list(self.headers.raw_items())
        if len(header_items) > _MAX_HEADER_COUNT:
            self.send_error(431, "Too Many Request Headers")
            return False

        header_bytes = sum(len(name) + len(value) + 4 for name, value in header_items)
        if header_bytes > _MAX_HEADER_BYTES:
            self.send_error(431, "Request Header Fields Too Large")
            return False
        return True

    def do_GET(self) -> None:
        """响应健康检查和对外图片访问请求。"""
        self._dispatch_request(self._handle_get_request)

    def do_POST(self) -> None:
        """接收海康 POST 上报的过车消息。"""
        self._dispatch_request(self._handle_hikvision_event)

    def do_PUT(self) -> None:
        """接收海康 PUT 上报的过车消息。"""
        self._dispatch_request(self._handle_hikvision_event)

    def send_response(self, code: int, message: str | None = None) -> None:
        """记录响应状态码，供请求结束日志使用。"""
        self._response_status = code
        super().send_response(code, message)

    def send_error(
        self,
        code: int,
        message: str | None = None,
        explain: str | None = None,
    ) -> None:
        """发送错误响应，并把客户端断开降级记录。"""
        self.close_connection = True
        try:
            super().send_error(code, message, explain)
        except _CLIENT_DISCONNECT_ERRORS as exc:
            self._log_client_disconnect(exc)
            self.close_connection = True
        except OSError:
            LOGGER.warning(
                "failed to send error response request_id=%s status=%s client=%s",
                getattr(self, "request_id", "-"),
                code,
                self._client_ip(),
                exc_info=True,
            )
            self.close_connection = True

    def _dispatch_request(self, action) -> None:
        """执行具体路由处理，并隔离所有请求级异常。"""
        try:
            action()
        except _CLIENT_DISCONNECT_ERRORS as exc:
            self._log_client_disconnect(exc)
            self.close_connection = True
        except socket.timeout:
            LOGGER.warning(
                "HTTP request processing timed out request_id=%s client=%s method=%s path=%s",
                self.request_id,
                self._client_ip(),
                self.command,
                self.path,
            )
            self._send_text(408, "Request Timeout")
            self.close_connection = True
        except Exception:
            log_exception(
                LOGGER,
                "HTTP request handling failed request_id=%s client=%s method=%s path=%s",
                self.request_id,
                self._client_ip(),
                self.command,
                self.path,
            )
            self._send_text(500, "Internal Server Error")
            self.close_connection = True

    def _handle_get_request(self) -> None:
        """处理 GET 路由。"""
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
        if not self.server.service.enqueue_http_request(
            content_type,
            body,
            client_ip=client_ip,
            request_id=self.request_id,
        ):
            self._send_text(503, "Busy")
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
            log_exception(LOGGER, "failed to read image file: %s", image_path)
            self._send_text(404, "Not Found")
            return

        content_type = mimetypes.guess_type(image_path.name)[0] or "application/octet-stream"
        self._send_bytes(200, data, content_type)

    def _handle_health_request(self) -> None:
        """返回进程和 SQLite 探针组成的健康检查 JSON。"""
        try:
            db_ok = self.server.service.is_database_healthy()
        except Exception:
            log_exception(LOGGER, "health check failed")
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
            payload["http_server"] = self.server.get_runtime_snapshot()
        except Exception:
            log_exception(LOGGER, "status endpoint failed")
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
        self._request_body_length = length
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
        self._response_body_length = len(body)
        self.close_connection = True
        try:
            self.send_response(status_code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except _CLIENT_DISCONNECT_ERRORS as exc:
            self._log_client_disconnect(exc)
            self.close_connection = True
        except OSError:
            LOGGER.warning(
                "failed to write HTTP response request_id=%s status=%s client=%s",
                getattr(self, "request_id", "-"),
                status_code,
                self._client_ip(),
                exc_info=True,
            )
            self.close_connection = True

    def _log_client_disconnect(self, exc: BaseException) -> None:
        """把客户端主动断开记录为低噪声日志。"""
        LOGGER.debug(
            "client disconnected request_id=%s client=%s method=%s path=%s error=%s: %s",
            getattr(self, "request_id", "-"),
            self._client_ip(),
            getattr(self, "command", "-"),
            getattr(self, "path", "-"),
            type(exc).__name__,
            exc,
        )

    def _log_request_summary(self) -> None:
        """记录单次请求的核心信息和耗时。"""
        requestline = getattr(self, "requestline", "")
        if not requestline:
            return
        elapsed_ms = (time.monotonic() - self._request_started_at) * 1000.0
        LOGGER.info(
            "HTTP request request_id=%s client=%s method=%s path=%s status=%s request_bytes=%s response_bytes=%s elapsed_ms=%.1f",
            self.request_id,
            self._client_ip(),
            getattr(self, "command", "-"),
            getattr(self, "path", "-"),
            self._response_status if self._response_status is not None else "-",
            self._request_body_length,
            self._response_body_length,
            elapsed_ms,
        )

    def _client_ip(self) -> str:
        """返回当前请求客户端 IP。"""
        return self.client_address[0] if self.client_address else "unknown"

    def log_error(self, fmt: str, *args: object) -> None:
        """把 BaseHTTPRequestHandler 的协议错误按 warning 写入生产日志。"""
        LOGGER.warning(
            "HTTP protocol error request_id=%s client=%s message=%s",
            getattr(self, "request_id", "-"),
            self._client_ip(),
            _format_log_message(fmt, args),
        )

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


def _next_request_id() -> int:
    """生成进程内单调递增的 HTTP 请求编号，便于关联日志。"""
    global _REQUEST_COUNTER
    with _REQUEST_COUNTER_LOCK:
        _REQUEST_COUNTER += 1
        return _REQUEST_COUNTER


def _client_address_text(client_address: object) -> str:
    """把 socketserver 的客户端地址转换为日志文本。"""
    if isinstance(client_address, tuple) and client_address:
        host = str(client_address[0])
        port = client_address[1] if len(client_address) > 1 else ""
        return f"{host}:{port}" if port != "" else host
    return str(client_address or "unknown")


def _format_log_message(fmt: str, args: tuple[object, ...]) -> str:
    """按 logging 百分号格式尽力生成可读消息。"""
    if not args:
        return fmt
    try:
        return fmt % args
    except Exception:
        return f"{fmt} args={args!r}"
