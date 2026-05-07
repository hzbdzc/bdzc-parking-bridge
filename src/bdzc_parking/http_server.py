"""Hikvision inbound HTTP server running in a managed child process."""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import multiprocessing
import os
import queue
import socket
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import uvicorn
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from bdzc_parking.common import iso_now
from bdzc_parking.config import AppConfig
from bdzc_parking.service import ParkingBridgeService


LOGGER = logging.getLogger(__name__)
_LISTEN_HOST = "0.0.0.0"
_PROBE_HOST = "127.0.0.1"
_MAX_HEADER_COUNT = 100
_MAX_HEADER_BYTES = 16 * 1024
_MAX_REQUEST_PATH_CHARS = 2048
_MAX_REQUEST_BYTES = 1_048_576
_REQUEST_READ_TIMEOUT_SECONDS = 15.0
_HTTP_MAX_CONNECTIONS = 64
_HTTP_REQUEST_QUEUE_SIZE = 128
_IMAGE_RATE_LIMIT_PER_MINUTE = 60
_IMAGE_RATE_LIMIT_BURST = 20
_IMAGE_RATE_LIMIT_STALE_SECONDS = 600.0
_IPC_INGRESS_QUEUE_SIZE = 256
_IPC_STATUS_REQUEST_QUEUE_SIZE = 16
_IPC_STATUS_RESPONSE_QUEUE_SIZE = 16
_IPC_READY_QUEUE_SIZE = 4
_SERVER_START_TIMEOUT_SECONDS = 5.0
_SERVER_STOP_GRACE_SECONDS = 1.0
_SERVER_KILL_TIMEOUT_SECONDS = 1.0
_SHUTDOWN_EVENT_SIGNAL_TIMEOUT_SECONDS = 0.2
_HEALTH_CHECK_INTERVAL_SECONDS = 10.0
_HEALTH_CHECK_TIMEOUT_SECONDS = 3.0
_HEALTH_FAILURE_THRESHOLD = 2
_STATUS_REQUEST_TIMEOUT_SECONDS = 2.0
_INGRESS_ACK_TIMEOUT_SECONDS = 2.0
_IPC_DRAIN_ENQUEUE_TIMEOUT_SECONDS = 1.0
_REQUEST_COUNTER = 0
_REQUEST_COUNTER_LOCK = threading.Lock()


@dataclass(frozen=True)
class _ServerSettings:
    """Picklable HTTP limits passed to the child process."""

    max_header_count: int
    max_header_bytes: int
    max_request_path_chars: int
    max_request_bytes: int
    request_read_timeout_seconds: float
    max_connections: int
    request_queue_size: int
    image_rate_limit_per_minute: int
    image_rate_limit_burst: int


@dataclass(frozen=True)
class _IngressItem:
    """Raw inbound HTTP request handed from child process to parent process."""

    content_type: str
    body: bytes
    client_ip: str
    request_id: int | str
    ack_sender: Any


@dataclass
class _LifecycleState:
    """Lifecycle fields owned by the parent process."""

    state: str = "stopped"
    desired_running: bool = False
    server_port: int | None = None
    process_pid: int | None = None
    restart_count: int = 0
    last_start_requested_at: str = ""
    last_started_at: str = ""
    last_stopped_at: str = ""
    last_failed_at: str = ""
    last_failure_reason: str = ""
    state_changed_monotonic: float = field(default_factory=time.monotonic)

    def mark_starting(self) -> None:
        """Record that the parent is starting a child process."""
        self.state = "starting"
        self.desired_running = True
        self.last_start_requested_at = iso_now()
        self.state_changed_monotonic = time.monotonic()

    def mark_running(self, port: int, pid: int | None) -> None:
        """Record that the child process is accepting HTTP traffic."""
        self.state = "running"
        self.server_port = int(port)
        self.process_pid = pid
        self.last_started_at = iso_now()
        self.last_failure_reason = ""
        self.last_failed_at = ""
        self.state_changed_monotonic = time.monotonic()

    def mark_restarting(self, reason: str) -> None:
        """Record that the health guard is replacing the child process."""
        self.state = "restarting"
        self.desired_running = True
        self.last_failed_at = iso_now()
        self.last_failure_reason = str(reason)[:1000]
        self.state_changed_monotonic = time.monotonic()

    def mark_stopping(self) -> None:
        """Record that a normal stop was requested."""
        self.state = "stopping"
        self.state_changed_monotonic = time.monotonic()

    def mark_stopped(self) -> None:
        """Record that no child process is active."""
        self.state = "stopped"
        self.desired_running = False
        self.server_port = None
        self.process_pid = None
        self.last_stopped_at = iso_now()
        self.state_changed_monotonic = time.monotonic()

    def mark_failed(self, reason: str) -> None:
        """Record a failed lifecycle transition."""
        self.state = "failed"
        self.last_failed_at = iso_now()
        self.last_failure_reason = str(reason)[:1000]
        self.state_changed_monotonic = time.monotonic()

    def snapshot(self, process_alive: bool) -> dict[str, object]:
        """Return the public lifecycle snapshot used by GUI and status endpoints."""
        return {
            "state": self.state,
            "desired_running": self.desired_running,
            "thread_alive": process_alive,
            "process_alive": process_alive,
            "server_port": self.server_port,
            "process_pid": self.process_pid,
            "restart_count": self.restart_count,
            "last_start_requested_at": self.last_start_requested_at,
            "last_started_at": self.last_started_at,
            "last_stopped_at": self.last_stopped_at,
            "last_failed_at": self.last_failed_at,
            "last_failure_reason": self.last_failure_reason,
        }


@dataclass
class _RuntimeStats:
    """Child-process HTTP request counters."""

    active_requests: int = 0
    busy_response_count: int = 0
    request_exception_count: int = 0
    last_request_exception_at: str = ""
    last_request_exception: str = ""
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def begin_request(self) -> int:
        """Start one request and return its local request id."""
        request_id = _next_request_id()
        with self._lock:
            self.active_requests += 1
        return request_id

    def finish_request(self) -> None:
        """Finish one request and decrement the active counter."""
        with self._lock:
            self.active_requests = max(0, self.active_requests - 1)

    def record_busy(self) -> None:
        """Record one 503 response caused by backpressure."""
        with self._lock:
            self.busy_response_count += 1

    def record_exception(self, summary: str) -> None:
        """Record one request handling exception summary."""
        with self._lock:
            self.request_exception_count += 1
            self.last_request_exception_at = iso_now()
            self.last_request_exception = summary[:1000]

    def snapshot(self) -> dict[str, object]:
        """Return the HTTP request metrics snapshot."""
        with self._lock:
            return {
                "active_requests": self.active_requests,
                "busy_response_count": self.busy_response_count,
                "request_exception_count": self.request_exception_count,
                "last_request_exception_at": self.last_request_exception_at,
                "last_request_exception": self.last_request_exception,
            }


class BridgeHTTPServer:
    """Parent-process manager for the inbound HTTP child process."""

    def __init__(self, config: AppConfig, service: ParkingBridgeService):
        """Store configuration, service, and lifecycle state."""
        self.config = config
        self.service = service
        self._lock = threading.RLock()
        self._lifecycle = _LifecycleState()
        self._mp_context = multiprocessing.get_context("spawn")
        self._process: multiprocessing.Process | None = None
        self._shutdown_event: Any = None
        self._ingress_queue: Any = None
        self._status_request_queue: Any = None
        self._status_response_queue: Any = None
        self._ready_queue: Any = None
        self._parent_stop_event = threading.Event()
        self._ingress_thread: threading.Thread | None = None
        self._status_thread: threading.Thread | None = None
        self._health_thread: threading.Thread | None = None
        self._health_failure_count = 0
        self._last_probe_at = ""
        self._last_probe_error = ""
        self._ipc_dropped_count = 0

    @property
    def is_running(self) -> bool:
        """Return whether the tracked child process is currently running."""
        with self._lock:
            return self._lifecycle.state == "running" and _process_alive(self._process)

    def start(self) -> None:
        """Start the child Uvicorn process and parent management threads."""
        with self._lock:
            if self._lifecycle.state in {"starting", "running"} and _process_alive(self._process):
                self._ensure_parent_threads_locked()
                return
            self._lifecycle.mark_starting()
            self._parent_stop_event.clear()
            self._ensure_ipc_locked()

        try:
            process, port, pid, shutdown_event = self._start_child()
        except Exception as exc:
            with self._lock:
                self._lifecycle.mark_failed(f"start failed: {type(exc).__name__}: {exc}")
                self._process = None
                self._shutdown_event = None
            raise

        with self._lock:
            self._process = process
            self._shutdown_event = shutdown_event
            self._lifecycle.mark_running(port, pid)
            self._health_failure_count = 0
            self._last_probe_error = ""
            self._ensure_parent_threads_locked()
        LOGGER.info("HTTP server started on %s:%s child_pid=%s", _LISTEN_HOST, port, pid)

    def stop(self) -> None:
        """Stop the child process and parent management threads."""
        with self._lock:
            self._lifecycle.desired_running = False
            if self._lifecycle.state in {"starting", "running", "restarting", "failed"}:
                self._lifecycle.mark_stopping()
            process = self._process
            shutdown_event = self._shutdown_event

        self._request_child_stop(process, shutdown_event)
        self._stop_child_process(process)
        self._parent_stop_event.set()
        self._join_parent_threads()

        with self._lock:
            self._process = None
            self._lifecycle.mark_stopped()
            self._close_ipc_locked()
        LOGGER.info("HTTP server stopped")

    def refresh(self, reason: str = "configuration changed") -> None:
        """Restart the child process when the HTTP server is currently running."""
        if not self.is_running:
            LOGGER.info("HTTP server refresh skipped because it is not running: %s", reason)
            return
        LOGGER.info("HTTP server refresh requested: %s", reason)
        self.stop()
        self.start()

    def get_lifecycle_snapshot(self) -> dict[str, object]:
        """Return HTTP server lifecycle status for GUI and /status."""
        with self._lock:
            return self._lifecycle.snapshot(_process_alive(self._process))

    def get_runtime_snapshot(self) -> dict[str, object]:
        """Return parent-side HTTP manager metrics."""
        lifecycle = self.get_lifecycle_snapshot()
        with self._lock:
            return {
                "active_requests": 0,
                "busy_response_count": 0,
                "request_exception_count": 0,
                "last_request_exception_at": "",
                "last_request_exception": "",
                "lifecycle": lifecycle,
                "control": self.get_control_snapshot(lifecycle),
                "health": {
                    "failure_count": self._health_failure_count,
                    "last_probe_at": self._last_probe_at,
                    "last_probe_error": self._last_probe_error,
                },
                "ipc": {
                    "ingress_queue_length": _safe_queue_size(self._ingress_queue),
                    "status_request_queue_length": _safe_queue_size(self._status_request_queue),
                    "dropped_ingress_count": self._ipc_dropped_count,
                },
            }

    def get_control_snapshot(self, lifecycle: dict[str, object] | None = None) -> dict[str, object]:
        """Return GUI-ready display and button state."""
        if lifecycle is None:
            lifecycle = self.get_lifecycle_snapshot()
        with self._lock:
            return _build_control_snapshot(
                lifecycle,
                self._health_failure_count,
                self._last_probe_error,
                self._last_probe_at,
            )

    def _ensure_ipc_locked(self) -> None:
        """Create IPC objects if they do not already exist."""
        if self._ingress_queue is None:
            self._ingress_queue = self._mp_context.Queue(maxsize=_IPC_INGRESS_QUEUE_SIZE)
        if self._status_request_queue is None:
            self._status_request_queue = self._mp_context.Queue(maxsize=_IPC_STATUS_REQUEST_QUEUE_SIZE)
        if self._status_response_queue is None:
            self._status_response_queue = self._mp_context.Queue(maxsize=_IPC_STATUS_RESPONSE_QUEUE_SIZE)
        if self._ready_queue is None:
            self._ready_queue = self._mp_context.Queue(maxsize=_IPC_READY_QUEUE_SIZE)

    def _close_ipc_locked(self) -> None:
        """Close IPC queues and events after shutdown."""
        for queue_obj in (
            self._ingress_queue,
            self._status_request_queue,
            self._status_response_queue,
            self._ready_queue,
        ):
            _close_queue(queue_obj)
        self._shutdown_event = None
        self._ingress_queue = None
        self._status_request_queue = None
        self._status_response_queue = None
        self._ready_queue = None

    def _start_child(self) -> tuple[multiprocessing.Process, int, int | None, Any]:
        """Spawn the child process and wait for its ready notification."""
        assert self._ingress_queue is not None
        assert self._status_request_queue is not None
        assert self._status_response_queue is not None
        assert self._ready_queue is not None
        shutdown_event = self._mp_context.Event()
        _drain_queue(self._ready_queue)
        process = self._mp_context.Process(
            target=_run_http_child,
            name="bdzc-parking-http",
            args=(
                self.config,
                _settings_from_constants(),
                self._ingress_queue,
                self._status_request_queue,
                self._status_response_queue,
                self._ready_queue,
                shutdown_event,
                os.getpid(),
            ),
            daemon=True,
        )
        process.start()
        ready = self._wait_child_ready(process)
        if ready.get("status") != "started":
            raise OSError(str(ready.get("error") or "child process failed to start"))
        return process, int(ready["port"]), _coerce_int(ready.get("pid")), shutdown_event

    def _wait_child_ready(self, process: multiprocessing.Process) -> dict[str, object]:
        """Wait until the child process reports its listening port."""
        deadline = time.monotonic() + _SERVER_START_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                value = self._ready_queue.get(timeout=0.05)
            except queue.Empty:
                if not _process_alive(process):
                    raise OSError("child process exited before ready")
                continue
            if isinstance(value, dict):
                return value
        raise TimeoutError("HTTP child did not report ready in time")

    def _ensure_parent_threads_locked(self) -> None:
        """Start parent-side IPC and health workers."""
        if self._ingress_thread is None or not self._ingress_thread.is_alive():
            self._ingress_thread = threading.Thread(
                target=self._ingress_loop,
                name="http-ingress-ipc",
                daemon=True,
            )
            self._ingress_thread.start()
        if self._status_thread is None or not self._status_thread.is_alive():
            self._status_thread = threading.Thread(
                target=self._status_loop,
                name="http-status-ipc",
                daemon=True,
            )
            self._status_thread.start()
        if self._health_thread is None or not self._health_thread.is_alive():
            self._health_thread = threading.Thread(
                target=self._health_loop,
                name="http-health-guard",
                daemon=True,
            )
            self._health_thread.start()

    def _join_parent_threads(self) -> None:
        """Join parent-side threads after shutdown."""
        current = threading.current_thread()
        for thread in (self._ingress_thread, self._status_thread, self._health_thread):
            if thread is None or thread is current:
                continue
            thread.join(timeout=2)
        self._ingress_thread = None
        self._status_thread = None
        self._health_thread = None

    def _ingress_loop(self) -> None:
        """Forward raw HTTP ingress items into the current business service."""
        while not self._parent_stop_event.is_set() or not _queue_empty(self._ingress_queue):
            try:
                item = self._ingress_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            except (EOFError, OSError):
                LOGGER.exception("HTTP ingress IPC queue closed")
                break
            except Exception:
                LOGGER.exception("HTTP ingress IPC loop recovered from unexpected error")
                self._parent_stop_event.wait(0.1)
                continue
            if not isinstance(item, _IngressItem):
                LOGGER.warning("HTTP ingress IPC ignored unexpected item: %r", type(item).__name__)
                continue

            accepted = False
            try:
                accepted = self.service.enqueue_http_request(
                    item.content_type,
                    item.body,
                    item.client_ip,
                    item.request_id,
                    block=True,
                    timeout=_IPC_DRAIN_ENQUEUE_TIMEOUT_SECONDS,
                )
            except Exception:
                LOGGER.exception("failed to forward inbound HTTP request request_id=%s", item.request_id)
            finally:
                _send_ingress_ack(item.ack_sender, accepted)

            if not accepted:
                LOGGER.warning("HTTP ingress request rejected request_id=%s", item.request_id)
                with self._lock:
                    self._ipc_dropped_count += 1

    def _status_loop(self) -> None:
        """Answer child-process /status requests with parent-side service data."""
        while not self._parent_stop_event.is_set():
            try:
                request_id = str(self._status_request_queue.get(timeout=0.2) or "")
            except queue.Empty:
                continue
            except (EOFError, OSError):
                LOGGER.exception("HTTP status IPC queue closed")
                break
            except Exception:
                LOGGER.exception("HTTP status IPC loop recovered from unexpected error")
                self._parent_stop_event.wait(0.1)
                continue
            try:
                payload = self._build_status_payload(request_id)
            except Exception as exc:
                LOGGER.exception("failed to build HTTP status payload request_id=%s", request_id)
                payload = {
                    "status": "error",
                    "time": iso_now(),
                    "db_ok": False,
                    "message": f"parent status failed: {type(exc).__name__}: {exc}",
                    "http_server": self.get_runtime_snapshot(),
                    "_status_request_id": request_id,
                }
            _put_nowait_drop_oldest(self._status_response_queue, payload)

    def _build_status_payload(self, request_id: str) -> dict[str, object]:
        """Build a combined HTTP/service/database status payload."""
        runtime = self.service.get_runtime_snapshot()
        try:
            db_ok = self.service.is_database_healthy()
        except Exception:
            LOGGER.exception("database health probe failed")
            db_ok = False
        database = self.service.get_status_snapshot() if db_ok else {}
        return {
            "status": "ok" if db_ok else "error",
            "time": iso_now(),
            "db_ok": db_ok,
            "queues": runtime.get("queues", {}),
            "workers": runtime.get("workers", {}),
            "events": {
                "last_success_sent_at": str(database.get("last_success_sent_at") or ""),
                "dead_letter": database.get("dead_letter_count"),
                "failure_backlog": database.get("failure_backlog_count"),
            },
            "database": {
                "main_size_bytes": database.get("db_main_size_bytes"),
                "total_size_bytes": database.get("db_total_size_bytes"),
            },
            "http_server": self.get_runtime_snapshot(),
            "_status_request_id": request_id,
        }

    def _health_loop(self) -> None:
        """Probe child process health and restart it when needed."""
        while not self._parent_stop_event.wait(_HEALTH_CHECK_INTERVAL_SECONDS):
            try:
                self._health_check_once()
            except Exception:
                LOGGER.exception("HTTP health guard failed")

    def _health_check_once(self) -> None:
        """Run one health check and restart on process or /livez failure."""
        restart_reason = ""
        with self._lock:
            if not self._lifecycle.desired_running:
                return
            process = self._process
            port = self._lifecycle.server_port
            if process is None or not _process_alive(process):
                self._last_probe_at = iso_now()
                self._last_probe_error = "child process is not alive"
                restart_reason = "child process is not alive"
            elif port is None:
                self._last_probe_at = iso_now()
                self._last_probe_error = "child port is unknown"
                restart_reason = "child port is unknown"
        if restart_reason:
            self._restart_child(restart_reason)
            return

        assert port is not None
        ok, error = _probe_livez(port)
        with self._lock:
            self._last_probe_at = iso_now()
            if ok:
                self._health_failure_count = 0
                self._last_probe_error = ""
                return
            self._health_failure_count += 1
            self._last_probe_error = error
            if self._health_failure_count < _HEALTH_FAILURE_THRESHOLD:
                return
        self._restart_child(f"/livez failed: {error}")

    def _restart_child(self, reason: str) -> None:
        """Replace the child process after a health failure."""
        with self._lock:
            if not self._lifecycle.desired_running:
                return
            old_process = self._process
            old_shutdown_event = self._shutdown_event
            self._lifecycle.mark_restarting(reason)

        LOGGER.warning("restarting HTTP server, reason: %s", reason)
        self._request_child_stop(old_process, old_shutdown_event)
        self._stop_child_process(old_process)
        with self._lock:
            self._process = None
            self._shutdown_event = None

        try:
            process, port, pid, shutdown_event = self._start_child()
        except Exception as exc:
            with self._lock:
                self._lifecycle.mark_failed(f"restart failed: {type(exc).__name__}: {exc}")
                self._process = None
                self._shutdown_event = None
            LOGGER.exception("failed to restart HTTP child")
            return

        with self._lock:
            self._process = process
            self._shutdown_event = shutdown_event
            self._lifecycle.restart_count += 1
            self._lifecycle.mark_running(port, pid)
            self._health_failure_count = 0
            self._last_probe_error = ""
        LOGGER.info("started new HTTP server pid=%s port=%s restart_count=%s",
                    pid, port, self._lifecycle.restart_count)

    def _request_child_stop(self, process: Any, shutdown_event: Any) -> None:
        """Ask the child process to stop gracefully."""
        if shutdown_event is None or not _process_alive(process):
            return

        def signal_shutdown() -> None:
            """Set the child shutdown event without blocking the caller."""
            try:
                shutdown_event.set()
            except Exception:
                LOGGER.debug("failed to signal HTTP child shutdown", exc_info=True)

        signal_thread = threading.Thread(
            target=signal_shutdown,
            name="http-child-stop-signal",
            daemon=True,
        )
        signal_thread.start()
        signal_thread.join(_SHUTDOWN_EVENT_SIGNAL_TIMEOUT_SECONDS)
        if signal_thread.is_alive():
            LOGGER.warning("HTTP child shutdown event signal timed out")

    def _stop_child_process(self, process: Any) -> None:
        """Wait for graceful exit for one second, then kill the child."""
        if process is None:
            return
        _join_process(process, _SERVER_STOP_GRACE_SECONDS)
        if _process_alive(process):
            LOGGER.warning("HTTP child pid=%s did not stop; killing", getattr(process, "pid", None))
            kill = getattr(process, "kill", None)
            if callable(kill):
                try:
                    kill()
                except Exception:
                    LOGGER.debug("failed to kill HTTP child", exc_info=True)
            _join_process(process, _SERVER_KILL_TIMEOUT_SECONDS)
        _close_process(process)


class _ChildHTTPApp:
    """HTTP routes and request state that live inside the child process."""

    def __init__(
        self,
        config: AppConfig,
        settings: _ServerSettings,
        ingress_queue: Any,
        status_request_queue: Any,
        status_response_queue: Any,
        server_port: int,
        parent_pid: int,
    ):
        """Store child-process dependencies and runtime counters."""
        self.config = config
        self.settings = settings
        self.ingress_queue = ingress_queue
        self.status_request_queue = status_request_queue
        self.status_response_queue = status_response_queue
        self.server_port = server_port
        self.parent_pid = parent_pid
        self.process_pid = os.getpid()
        self.stats = _RuntimeStats()
        self.image_root = Path(config.db_path).parent / "images"
        self.image_limiter = _ImageRateLimiter(
            settings.image_rate_limit_per_minute,
            settings.image_rate_limit_burst,
        )

    def build_app(self) -> Starlette:
        """Build the Starlette application served by Uvicorn."""
        app = Starlette(
            routes=[
                Route("/{path:path}", self._handle_get, methods=["GET"]),
                Route("/{path:path}", self._handle_ingress, methods=["POST", "PUT"]),
            ]
        )
        app.add_middleware(_RequestLimitMiddleware, owner=self)
        return app

    async def _handle_get(self, request: Request) -> Response:
        """Route GET requests to /livez, /status, images, or 404."""
        path = request.url.path
        if path == "/livez":
            return _json_response(200, self._livez_payload())
        if path == "/status":
            return await self._handle_status()
        if self._is_image_path(path):
            return await self._handle_image(request, path)
        return _text_response(404, "Not Found")

    def _livez_payload(self) -> dict[str, object]:
        """Build an HTTP-only health payload without touching business state."""
        payload = self.stats.snapshot()
        payload.update(
            {
                "status": "ok",
                "http_ok": True,
                "time": iso_now(),
                "server_port": self.server_port,
                "process_pid": self.process_pid,
                "parent_pid": self.parent_pid,
                "parent_alive": _multiprocessing_parent_alive(),
            }
        )
        return payload

    async def _handle_status(self) -> Response:
        """Ask the parent process for business health and return it as JSON."""
        request_id = str(_next_request_id())
        try:
            self.status_request_queue.put_nowait(request_id)
        except queue.Full:
            return _json_response(503, {"status": "error", "db_ok": False, "message": "status queue is full"})
        payload = await asyncio.to_thread(self._wait_status_response, request_id)
        if payload is None:
            return _json_response(
                503,
                {
                    "status": "error",
                    "db_ok": False,
                    "message": "parent status timeout",
                    "time": iso_now(),
                    "http_server": self._runtime_payload({}),
                },
            )
        payload.pop("_status_request_id", None)
        parent_http = payload.get("http_server")
        payload["http_server"] = self._runtime_payload(parent_http if isinstance(parent_http, dict) else {})
        return _json_response(200 if payload.get("db_ok") is True else 503, payload)

    def _wait_status_response(self, request_id: str) -> dict[str, object] | None:
        """Wait for a parent status response matching this request id."""
        deadline = time.monotonic() + _STATUS_REQUEST_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                payload = self.status_response_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            except (EOFError, OSError):
                return None
            if isinstance(payload, dict) and str(payload.get("_status_request_id") or "") == request_id:
                return payload
        return None

    def _runtime_payload(self, parent_http: dict[str, object]) -> dict[str, object]:
        """Merge parent HTTP lifecycle with child HTTP request counters."""
        lifecycle = parent_http.get("lifecycle") if isinstance(parent_http, dict) else {}
        lifecycle = dict(lifecycle) if isinstance(lifecycle, dict) else {}
        lifecycle.update(
            {
                "thread_alive": True,
                "process_alive": True,
                "server_port": self.server_port,
                "process_pid": self.process_pid,
                "parent_pid": self.parent_pid,
                "parent_alive": _multiprocessing_parent_alive(),
            }
        )
        merged = dict(parent_http)
        merged.update(self.stats.snapshot())
        merged["lifecycle"] = lifecycle
        return merged

    async def _handle_ingress(self, request: Request) -> Response:
        """Validate and forward a Hikvision report to the parent process."""
        if request.url.path != self.config.listen_path:
            return _text_response(404, "Not Found")
        content_length = _content_length(request)
        if content_length is None:
            return _text_response(400, "Missing Content-Length")
        if content_length < 0:
            return _text_response(400, "Invalid Content-Length")
        if content_length > self.settings.max_request_bytes:
            request.state.request_body_length = content_length
            return _text_response(413, "Payload Too Large")
        try:
            body = await asyncio.wait_for(request.body(), timeout=self.settings.request_read_timeout_seconds)
        except TimeoutError:
            return _text_response(408, "Request Timeout")
        except Exception as exc:
            self._record_exception(request, exc)
            return _text_response(400, "Bad Request")
        request.state.request_body_length = len(body)
        if len(body) != content_length:
            return _text_response(400, "Bad Request")
        ack_receiver, ack_sender = multiprocessing.Pipe(duplex=False)
        item = _IngressItem(
            request.headers.get("content-type", ""),
            body,
            _client_ip(request),
            getattr(request.state, "request_id", "-"),
            ack_sender,
        )
        try:
            self.ingress_queue.put_nowait(item)
        except queue.Full:
            _close_connection(ack_receiver)
            _close_connection(ack_sender)
            self.stats.record_busy()
            return _text_response(503, "Busy")
        except Exception as exc:
            _close_connection(ack_receiver)
            _close_connection(ack_sender)
            self._record_exception(request, exc)
            return _text_response(503, "Busy")
        accepted = await asyncio.to_thread(_wait_ingress_ack, ack_receiver)
        _close_connection(ack_receiver)
        _close_connection(ack_sender)
        if not accepted:
            self.stats.record_busy()
            return _text_response(503, "Busy")
        return _text_response(200, "OK")

    async def _handle_image(self, request: Request, path: str) -> Response:
        """Serve a saved event image by safe file name."""
        image_name = self._image_name(path)
        if image_name is None:
            return _text_response(404, "Not Found")
        client_ip = _client_ip(request)
        if not self.image_limiter.allow(client_ip):
            return _text_response(429, "Too Many Requests")
        image_path = await asyncio.to_thread(_resolve_image_path, self.image_root, image_name)
        if image_path is None:
            return _text_response(404, "Not Found")
        try:
            data = await asyncio.to_thread(image_path.read_bytes)
        except OSError:
            return _text_response(404, "Not Found")
        return Response(data, media_type=mimetypes.guess_type(image_path.name)[0] or "application/octet-stream")

    def _is_image_path(self, path: str) -> bool:
        """Return whether this path uses the configured public image prefix."""
        prefix = self.config.external_image_path
        return bool(prefix and path.startswith(f"{prefix}/"))

    def _image_name(self, path: str) -> str | None:
        """Extract one safe image file name from a public image path."""
        prefix = self.config.external_image_path
        if not prefix or not path.startswith(f"{prefix}/"):
            return None
        name = unquote(path.removeprefix(f"{prefix}/")).strip()
        if not name or Path(name).name != name or "/" in name or "\\" in name:
            return None
        return name

    def _record_exception(self, request: Request, exc: BaseException) -> None:
        """Record one child HTTP exception in runtime counters."""
        self.stats.record_exception(
            f"{request.method} {request.url.path} client={_client_ip(request)} {type(exc).__name__}: {exc}"
        )


class _RequestLimitMiddleware(BaseHTTPMiddleware):
    """Apply request limits, simple concurrency gating, and compact access logging."""

    def __init__(self, app: Any, owner: _ChildHTTPApp):
        """Store child context and create a process-local concurrency gate."""
        super().__init__(app)
        self.owner = owner
        self._semaphore = asyncio.Semaphore(max(1, owner.settings.max_connections))

    async def dispatch(self, request: Request, call_next: Callable[[Request], Any]) -> Response:
        """Handle one HTTP request lifecycle."""
        if self._semaphore.locked():
            self.owner.stats.record_busy()
            return _text_response(503, "Busy")
        async with self._semaphore:
            request_id = self.owner.stats.begin_request()
            request.state.request_id = request_id
            request.state.request_body_length = 0
            started_at = time.monotonic()
            try:
                response = self._reject_invalid(request)
                if response is None:
                    response = await call_next(request)
            except Exception as exc:
                self.owner._record_exception(request, exc)
                response = _text_response(500, "Internal Server Error")
            finally:
                self.owner.stats.finish_request()
            response.headers["Connection"] = "close"
            self._log_request(request, response, started_at)
            return response

    def _reject_invalid(self, request: Request) -> Response | None:
        """Reject abnormal request path and header sizes."""
        if len(request.url.path) > self.owner.settings.max_request_path_chars:
            return _text_response(414, "URI Too Long")
        raw_headers = list(request.headers.raw)
        if len(raw_headers) > self.owner.settings.max_header_count:
            return _text_response(431, "Too Many Request Headers")
        header_bytes = sum(len(name) + len(value) + 4 for name, value in raw_headers)
        if header_bytes > self.owner.settings.max_header_bytes:
            return _text_response(431, "Request Header Fields Too Large")
        return None

    def _log_request(self, request: Request, response: Response, started_at: float) -> None:
        """Log non-successful probe requests for diagnostics."""
        if request.method == "GET" and request.url.path in {"/livez", "/status"} and response.status_code < 400:
            return
        LOGGER.debug(
            "HTTP request request_id=%s client=%s method=%s path=%s status=%s request_bytes=%s elapsed_ms=%.1f",
            getattr(request.state, "request_id", "-"),
            _client_ip(request),
            request.method,
            request.url.path,
            response.status_code,
            getattr(request.state, "request_body_length", 0),
            (time.monotonic() - started_at) * 1000.0,
        )


@dataclass
class _RateBucket:
    """Token-bucket state for one client IP."""

    tokens: float
    updated_at: float
    last_seen: float


class _ImageRateLimiter:
    """Simple process-local token bucket for image access."""

    def __init__(self, per_minute: int, burst: int):
        """Initialize refill rate and bucket capacity."""
        self.per_minute = max(1, int(per_minute))
        self.burst = max(1, int(burst))
        self.refill_per_second = self.per_minute / 60.0
        self._lock = threading.Lock()
        self._buckets: dict[str, _RateBucket] = {}

    def allow(self, key: str) -> bool:
        """Return whether one request from this key is allowed."""
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = _RateBucket(float(self.burst), now, now)
                self._buckets[key] = bucket
            elapsed = max(0.0, now - bucket.updated_at)
            bucket.tokens = min(float(self.burst), bucket.tokens + elapsed * self.refill_per_second)
            bucket.updated_at = now
            bucket.last_seen = now
            allowed = bucket.tokens >= 1.0
            if allowed:
                bucket.tokens -= 1.0
            self._drop_stale(now)
            return allowed

    def _drop_stale(self, now: float) -> None:
        """Drop inactive buckets to keep memory bounded."""
        for key, bucket in list(self._buckets.items()):
            if now - bucket.last_seen >= _IMAGE_RATE_LIMIT_STALE_SECONDS:
                self._buckets.pop(key, None)


def _run_http_child(
    config: AppConfig,
    settings: _ServerSettings,
    ingress_queue: Any,
    status_request_queue: Any,
    status_response_queue: Any,
    ready_queue: Any,
    shutdown_event: Any,
    parent_pid: int,
) -> None:
    """Child-process target that runs Uvicorn with a pre-bound socket."""
    listen_socket: socket.socket | None = None
    server: uvicorn.Server | None = None
    try:
        listen_socket = _bind_socket(_LISTEN_HOST, int(config.listen_port), settings.request_queue_size)
        server_port = int(listen_socket.getsockname()[1])
        child_app = _ChildHTTPApp(
            config,
            settings,
            ingress_queue,
            status_request_queue,
            status_response_queue,
            server_port,
            parent_pid,
        )
        uvicorn_config = uvicorn.Config(
            child_app.build_app(),
            host=_LISTEN_HOST,
            port=server_port,
            log_config=None,
            access_log=False,
            server_header=False,
            log_level="warning",
            lifespan="off",
            timeout_keep_alive=settings.request_read_timeout_seconds,
            timeout_graceful_shutdown=_SERVER_STOP_GRACE_SECONDS,
        )
        server = uvicorn.Server(uvicorn_config)
        _start_shutdown_watcher(server, shutdown_event)
        _start_parent_sentinel_watcher(server, parent_pid)
        _start_ready_watcher(server, ready_queue, server_port)
        server.run(sockets=[listen_socket])
    except BaseException as exc:
        LOGGER.exception("HTTP child failed")
        _put_nowait_drop_oldest(
            ready_queue,
            {"status": "error", "error": f"{type(exc).__name__}: {exc}", "pid": os.getpid()},
        )
    finally:
        if listen_socket is not None:
            try:
                listen_socket.close()
            except OSError:
                pass


def _start_shutdown_watcher(server: uvicorn.Server, shutdown_event: Any) -> None:
    """Start a daemon thread that maps the shutdown event to Uvicorn exit."""
    def watch() -> None:
        """Wait for parent shutdown and ask Uvicorn to exit."""
        try:
            shutdown_event.wait()
        except Exception:
            return
        server.should_exit = True

    threading.Thread(target=watch, name="http-child-shutdown", daemon=True).start()


def _start_parent_sentinel_watcher(
    server: uvicorn.Server,
    parent_pid: int,
    force_exit: Callable[[int], None] = os._exit,
) -> None:
    """Start a daemon thread that force-exits the child when the parent disappears."""
    parent = multiprocessing.parent_process()
    if parent is None:
        LOGGER.debug("HTTP child parent sentinel unavailable for pid=%s", parent_pid)
        return

    def watch() -> None:
        """Block on Python's parent sentinel and terminate immediately on orphaning."""
        try:
            parent.join()
        except Exception:
            LOGGER.debug("HTTP child parent sentinel failed", exc_info=True)
            return
        if server.should_exit:
            return
        LOGGER.warning("HTTP child parent process pid=%s is gone; force exiting", parent_pid)
        force_exit(0)

    threading.Thread(target=watch, name="http-child-parent-sentinel", daemon=True).start()


def _start_ready_watcher(server: uvicorn.Server, ready_queue: Any, server_port: int) -> None:
    """Notify the parent once Uvicorn has started."""
    def watch() -> None:
        """Poll Uvicorn's started flag and publish one ready message."""
        deadline = time.monotonic() + _SERVER_START_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if server.started:
                _put_nowait_drop_oldest(
                    ready_queue,
                    {"status": "started", "port": server_port, "pid": os.getpid()},
                )
                return
            if server.should_exit:
                return
            time.sleep(0.02)

    threading.Thread(target=watch, name="http-child-ready", daemon=True).start()


def _settings_from_constants() -> _ServerSettings:
    """Capture monkeypatchable module constants for the child process."""
    return _ServerSettings(
        _MAX_HEADER_COUNT,
        _MAX_HEADER_BYTES,
        _MAX_REQUEST_PATH_CHARS,
        _MAX_REQUEST_BYTES,
        _REQUEST_READ_TIMEOUT_SECONDS,
        _HTTP_MAX_CONNECTIONS,
        _HTTP_REQUEST_QUEUE_SIZE,
        _IMAGE_RATE_LIMIT_PER_MINUTE,
        _IMAGE_RATE_LIMIT_BURST,
    )


def _build_control_snapshot(
    lifecycle: dict[str, object],
    health_failure_count: int,
    health_error: str,
    health_at: str,
) -> dict[str, object]:
    """Convert lifecycle and health data into GUI display fields."""
    state = str(lifecycle.get("state") or "stopped")
    process_alive = bool(lifecycle.get("process_alive"))
    detail = str(lifecycle.get("last_failure_reason") or "")
    detail_at = str(lifecycle.get("last_failed_at") or "")
    display_state = state
    display_text = "未运行"
    severity = "idle"
    primary_action = "start"
    button_text = "开始 HTTP server"
    button_enabled = True
    if state == "starting":
        display_text = "启动中"
        severity = "busy"
        primary_action = "none"
        button_enabled = False
    elif state == "running":
        display_text = "运行中"
        severity = "ok"
        primary_action = "stop"
        button_text = "停止 HTTP server"
        if not process_alive:
            display_state = "degraded"
            display_text = "子进程未运行"
            severity = "error"
        elif health_failure_count > 0 and health_error:
            display_state = "degraded"
            display_text = "响应异常"
            severity = "warning"
            detail = f"连续失败 {health_failure_count} 次: {health_error}"
            detail_at = health_at
    elif state == "restarting":
        display_text = "重启中"
        severity = "busy"
        primary_action = "none"
        button_enabled = False
    elif state == "stopping":
        display_text = "停止中"
        severity = "busy"
        primary_action = "none"
        button_enabled = False
    elif state == "failed":
        display_text = "故障"
        severity = "error"
    return {
        "display_state": display_state,
        "display_text": display_text,
        "severity": severity,
        "detail": detail,
        "detail_at": detail_at,
        "primary_action": primary_action,
        "button_text": button_text,
        "button_enabled": button_enabled,
    }


def _bind_socket(host: str, port: int, backlog: int) -> socket.socket:
    """Bind a TCP socket for Uvicorn to use."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, int(port)))
        sock.listen(max(1, int(backlog)))
        return sock
    except OSError:
        sock.close()
        raise


def _probe_livez(port: int) -> tuple[bool, str]:
    """Probe /livez on the child process."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(f"http://{_PROBE_HOST}:{int(port)}/livez", timeout=_HEALTH_CHECK_TIMEOUT_SECONDS) as response:
            data = response.read(4096)
            return _looks_like_livez(data), "" if response.status == 200 else f"HTTP {response.status}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _looks_like_livez(data: bytes) -> bool:
    """Return whether data looks like this server's /livez payload."""
    try:
        payload = json.loads(data.decode("utf-8"))
    except Exception:
        return False
    return isinstance(payload, dict) and payload.get("http_ok") is True and payload.get("status") == "ok"


def _resolve_image_path(image_root: Path, image_name: str) -> Path | None:
    """Resolve one public image file by safe file name."""
    candidates = [image_root / image_name]
    if image_root.exists():
        candidates.extend(child / image_name for child in image_root.iterdir() if child.is_dir())
    try:
        root = image_root.resolve(strict=False)
    except OSError:
        return None
    for candidate in candidates:
        try:
            resolved = candidate.resolve(strict=False)
            resolved.relative_to(root)
        except (OSError, ValueError):
            continue
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _content_length(request: Request) -> int | None:
    """Read the Content-Length header as an integer."""
    value = request.headers.get("content-length")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return -1


def _text_response(status_code: int, body: str) -> PlainTextResponse:
    """Return a UTF-8 plain text response."""
    return PlainTextResponse(body, status_code=status_code)


def _json_response(status_code: int, payload: dict[str, object]) -> JSONResponse:
    """Return a JSON response."""
    return JSONResponse(payload, status_code=status_code)


def _client_ip(request: Request) -> str:
    """Return the remote client IP for a Starlette request."""
    return request.client.host if request.client is not None else "unknown"


def _next_request_id() -> int:
    """Return a process-local increasing request id."""
    global _REQUEST_COUNTER
    with _REQUEST_COUNTER_LOCK:
        _REQUEST_COUNTER += 1
        return _REQUEST_COUNTER


def _process_alive(process: Any) -> bool:
    """Return a safe liveness value for Process-like objects."""
    if process is None:
        return False
    try:
        return bool(process.is_alive())
    except Exception:
        return False


def _join_process(process: Any, timeout: float | None) -> None:
    """Join a Process-like object without surfacing cleanup errors."""
    try:
        process.join(timeout=timeout)
    except Exception:
        pass


def _close_process(process: Any) -> None:
    """Close a Process-like object when supported."""
    close = getattr(process, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _close_queue(queue_obj: Any) -> None:
    """Close a multiprocessing Queue-like object when supported."""
    if queue_obj is None:
        return
    close = getattr(queue_obj, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass
    join_thread = getattr(queue_obj, "join_thread", None)
    if callable(join_thread):
        try:
            join_thread()
        except Exception:
            pass


def _close_connection(connection: Any) -> None:
    """Close a multiprocessing Connection-like object."""
    close = getattr(connection, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _send_ingress_ack(ack_sender: Any, accepted: bool) -> None:
    """Send the child process a parent-side ingress acceptance result."""
    try:
        ack_sender.send(bool(accepted))
    except Exception:
        LOGGER.warning("failed to send HTTP ingress ack", exc_info=True)
    finally:
        _close_connection(ack_sender)


def _wait_ingress_ack(ack_receiver: Any) -> bool:
    """Wait briefly for the parent process to accept an ingress request."""
    try:
        if not ack_receiver.poll(_INGRESS_ACK_TIMEOUT_SECONDS):
            return False
        return ack_receiver.recv() is True
    except Exception:
        LOGGER.warning("failed to receive HTTP ingress ack", exc_info=True)
        return False


def _queue_empty(queue_obj: Any) -> bool:
    """Return a best-effort empty check for Queue-like objects."""
    if queue_obj is None:
        return True
    try:
        return bool(queue_obj.empty())
    except Exception:
        return True


def _safe_queue_size(queue_obj: Any) -> int | None:
    """Return a best-effort Queue size."""
    if queue_obj is None:
        return None
    try:
        return int(queue_obj.qsize())
    except Exception:
        return None


def _drain_queue(queue_obj: Any) -> None:
    """Remove all currently available items from a Queue-like object."""
    while queue_obj is not None:
        try:
            queue_obj.get_nowait()
        except queue.Empty:
            return
        except Exception:
            return


def _put_nowait_drop_oldest(queue_obj: Any, item: object) -> None:
    """Put into a bounded queue, dropping one old item if necessary."""
    if queue_obj is None:
        return
    try:
        queue_obj.put_nowait(item)
        return
    except queue.Full:
        try:
            queue_obj.get_nowait()
        except Exception:
            pass
    try:
        queue_obj.put_nowait(item)
    except Exception:
        pass


def _coerce_int(value: object) -> int | None:
    """Convert a value to int when possible."""
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _multiprocessing_parent_alive() -> bool:
    """Return whether multiprocessing's parent sentinel still reports alive."""
    parent = multiprocessing.parent_process()
    if parent is None:
        return False
    try:
        return bool(parent.is_alive())
    except Exception:
        return False
