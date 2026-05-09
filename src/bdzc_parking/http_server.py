"""Hikvision inbound HTTP server supervised by the GUI process."""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
import multiprocessing
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from ipaddress import ip_address
from pathlib import Path
from typing import Any
from urllib.parse import unquote
from uuid import uuid4

import uvicorn
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from bdzc_parking.common import iso_now
from bdzc_parking.config import AppConfig
from bdzc_parking.logging_setup import setup_logging
from bdzc_parking.service import ParkingBridgeService, PartnerClient
from bdzc_parking.storage import EventStore


LOGGER = logging.getLogger(__name__)
_LISTEN_HOST = "0.0.0.0"
_PROBE_HOST = "127.0.0.1"
_MAX_HEADER_COUNT = 100
_MAX_HEADER_BYTES = 16 * 1024
_MAX_REQUEST_PATH_CHARS = 2048
_MAX_REQUEST_BYTES = 1_048_576
_ADMIN_MAX_REQUEST_BYTES = 64 * 1024
_REQUEST_READ_TIMEOUT_SECONDS = 15.0
_HTTP_MAX_CONNECTIONS = 64
_HTTP_REQUEST_QUEUE_SIZE = 128
_IMAGE_RATE_LIMIT_PER_MINUTE = 60
_IMAGE_RATE_LIMIT_BURST = 20
_IMAGE_RATE_LIMIT_STALE_SECONDS = 600.0
_SERVER_START_TIMEOUT_SECONDS = 8.0
_SERVER_STOP_GRACE_SECONDS = 1.0
_SERVER_KILL_TIMEOUT_SECONDS = 1.0
_PROCESS_HEALTH_CHECK_INTERVAL_SECONDS = 1.0
_STATUS_HEALTH_CHECK_INTERVAL_SECONDS = 10.0
_HEALTH_CHECK_TIMEOUT_SECONDS = 3.0
_HEALTH_FAILURE_THRESHOLD = 2
_ADMIN_TASK_LIMIT = 100
_ADMIN_TASK_TTL_SECONDS = 30 * 60.0
_ADMIN_TASK_TIMEOUT_SECONDS = 180.0
_ADMIN_TASK_POLL_SECONDS = 0.2
_REQUEST_COUNTER = 0
_REQUEST_COUNTER_LOCK = threading.Lock()


@dataclass(frozen=True)
class _ServerSettings:
    """Picklable HTTP limits passed to the child process."""

    max_header_count: int
    max_header_bytes: int
    max_request_path_chars: int
    max_request_bytes: int
    admin_max_request_bytes: int
    request_read_timeout_seconds: float
    max_connections: int
    request_queue_size: int
    image_rate_limit_per_minute: int
    image_rate_limit_burst: int


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
        """Record that the parent is starting the HTTP child process."""
        self.state = "starting"
        self.desired_running = True
        self.last_start_requested_at = iso_now()
        self.state_changed_monotonic = time.monotonic()

    def mark_running(self, port: int, pid: int | None) -> None:
        """Record that the HTTP child is accepting traffic."""
        self.state = "running"
        self.server_port = int(port)
        self.process_pid = pid
        self.last_started_at = iso_now()
        self.last_failed_at = ""
        self.last_failure_reason = ""
        self.state_changed_monotonic = time.monotonic()

    def mark_restarting(self, reason: str) -> None:
        """Record that the health guard is replacing the child."""
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
        """Record that no HTTP child process is active."""
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


@dataclass
class _AdminTask:
    """One short-lived admin task tracked inside the HTTP child process."""

    task_id: str
    kind: str
    status: str
    created_at: str
    params: dict[str, object]
    started_at: str = ""
    finished_at: str = ""
    message: str = ""
    error: str = ""
    updated_monotonic: float = field(default_factory=time.monotonic)

    def snapshot(self) -> dict[str, object]:
        """Return the public task state."""
        payload = {
            "task_id": self.task_id,
            "kind": self.kind,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "message": self.message,
            "error": self.error,
        }
        payload.update(self.params)
        return payload


class BridgeHTTPServer:
    """Parent-process supervisor for the inbound HTTP child process."""

    def __init__(self, config: AppConfig):
        """Store configuration and initialize lifecycle state."""
        self.config = config
        self._lock = threading.RLock()
        self._lifecycle = _LifecycleState()
        self._mp_context = multiprocessing.get_context("spawn")
        self._process: multiprocessing.Process | None = None
        self._parent_stop_event = threading.Event()
        self._health_thread: threading.Thread | None = None
        self._health_failure_count = 0
        self._last_probe_at = ""
        self._last_probe_error = ""
        self._last_status_payload: dict[str, object] = {}

    @property
    def is_running(self) -> bool:
        """Return whether the tracked child process is currently running."""
        with self._lock:
            return self._lifecycle.state == "running" and _process_alive(self._process)

    def start(self) -> None:
        """Start the HTTP child process and the parent health guard."""
        if int(self.config.listen_port) <= 0:
            raise ValueError("listen_port must be a fixed port greater than 0")

        with self._lock:
            if self._lifecycle.state in {"starting", "running"} and _process_alive(self._process):
                self._ensure_health_thread_locked()
                return
            self._lifecycle.mark_starting()
            self._parent_stop_event.clear()

        process: multiprocessing.Process | None = None
        try:
            process = self._start_child()
        except Exception as exc:
            if process is not None:
                self._stop_child_process(process)
            with self._lock:
                self._process = None
                self._lifecycle.mark_failed(f"start failed: {type(exc).__name__}: {exc}")
            raise

        with self._lock:
            self._process = process
            self._lifecycle.mark_running(int(self.config.listen_port), process.pid)
            self._health_failure_count = 0
            self._last_probe_error = ""
            self._ensure_health_thread_locked()
        LOGGER.info("HTTP server started on %s:%s child_pid=%s", _LISTEN_HOST, self.config.listen_port, process.pid)

    def stop(self) -> None:
        """Stop the HTTP child process and the parent health guard."""
        with self._lock:
            self._lifecycle.desired_running = False
            if self._lifecycle.state in {"starting", "running", "restarting", "failed"}:
                self._lifecycle.mark_stopping()
            process = self._process
            port = self._lifecycle.server_port

        self._request_child_stop(port)
        self._stop_child_process(process)
        self._parent_stop_event.set()
        self._join_health_thread()

        with self._lock:
            self._process = None
            self._lifecycle.mark_stopped()
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
        """Return HTTP server lifecycle status for GUI."""
        with self._lock:
            return self._lifecycle.snapshot(_process_alive(self._process))

    def get_runtime_snapshot(self) -> dict[str, object]:
        """Return parent-side supervisor health metrics."""
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
                    "last_status": dict(self._last_status_payload),
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

    def submit_resend(self, event_id: int) -> dict[str, object]:
        """Submit one resend admin task to the HTTP child process."""
        return self._post_admin_json("/admin/resend", {"event_id": int(event_id)})

    def submit_cleanup(self, reason: str = "manual") -> dict[str, object]:
        """Submit one cleanup admin task to the HTTP child process."""
        return self._post_admin_json("/admin/cleanup", {"reason": str(reason or "manual")})

    def get_admin_task(self, task_id: str) -> dict[str, object]:
        """Read one admin task state from the HTTP child process."""
        port = self._running_port()
        return _request_json("GET", f"http://{_PROBE_HOST}:{port}/admin/tasks/{task_id}")[1]

    def _post_admin_json(self, path: str, payload: dict[str, object]) -> dict[str, object]:
        """POST a JSON payload to one loopback admin endpoint."""
        port = self._running_port()
        return _request_json("POST", f"http://{_PROBE_HOST}:{port}{path}", payload)[1]

    def _running_port(self) -> int:
        """Return the active fixed port or raise when the child is not running."""
        lifecycle = self.get_lifecycle_snapshot()
        if lifecycle.get("state") != "running" or not lifecycle.get("process_alive"):
            raise RuntimeError("HTTP server is not running")
        port = lifecycle.get("server_port")
        if not isinstance(port, int):
            raise RuntimeError("HTTP server port is unknown")
        return port

    def _start_child(self) -> multiprocessing.Process:
        """Spawn the child process and wait until /status is reachable."""
        process = self._mp_context.Process(
            target=_run_http_child,
            name="bdzc-parking-http",
            args=(self.config, _settings_from_constants(), os.getpid()),
            daemon=True,
        )
        process.start()
        try:
            self._wait_child_ready(process, int(self.config.listen_port))
        except Exception:
            self._request_child_stop(int(self.config.listen_port))
            self._stop_child_process(process)
            raise
        return process

    def _wait_child_ready(self, process: multiprocessing.Process, port: int) -> None:
        """Wait until the child process answers the /status route."""
        deadline = time.monotonic() + _SERVER_START_TIMEOUT_SECONDS
        last_error = "not probed"
        while time.monotonic() < deadline:
            if not _process_alive(process):
                raise OSError(f"child process exited before ready: {last_error}")
            reached, error = _probe_status_reachable(port)
            if reached:
                return
            last_error = error
            time.sleep(0.05)
        raise TimeoutError(f"HTTP child did not become ready in time: {last_error}")

    def _ensure_health_thread_locked(self) -> None:
        """Start the parent-side health guard if needed."""
        if self._health_thread is not None and self._health_thread.is_alive():
            return
        self._health_thread = threading.Thread(
            target=self._health_loop,
            name="http-health-guard",
            daemon=True,
        )
        self._health_thread.start()

    def _join_health_thread(self) -> None:
        """Join the health guard after shutdown."""
        thread = self._health_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2)
        self._health_thread = None

    def _health_loop(self) -> None:
        """Probe child process health and restart it when needed."""
        next_status_probe_at = time.monotonic() + _STATUS_HEALTH_CHECK_INTERVAL_SECONDS
        while not self._parent_stop_event.wait(_PROCESS_HEALTH_CHECK_INTERVAL_SECONDS):
            try:
                now = time.monotonic()
                probe_status = now >= next_status_probe_at
                restarted = self._health_check_once(probe_status=probe_status)
                if restarted or probe_status:
                    next_status_probe_at = now + _STATUS_HEALTH_CHECK_INTERVAL_SECONDS
            except Exception:
                LOGGER.exception("HTTP health guard failed")

    def _health_check_once(self, probe_status: bool = True) -> bool:
        """Run one health check and restart on process or /status failure."""
        restart_reason = ""
        with self._lock:
            if not self._lifecycle.desired_running:
                return False
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
            self._restart_child(restart_reason, request_old_shutdown=False)
            return True
        if not probe_status:
            return False

        assert port is not None
        ok, error, payload = _probe_status(port)
        with self._lock:
            self._last_probe_at = iso_now()
            self._last_status_payload = dict(payload)
            if ok:
                self._health_failure_count = 0
                self._last_probe_error = ""
                return False
            self._health_failure_count += 1
            self._last_probe_error = error
            if self._health_failure_count < _HEALTH_FAILURE_THRESHOLD:
                return False
        self._restart_child(f"/status failed: {error}")
        return True

    def _restart_child(self, reason: str, request_old_shutdown: bool = True) -> None:
        """Replace the child process after a health failure."""
        with self._lock:
            if not self._lifecycle.desired_running:
                return
            old_process = self._process
            old_port = self._lifecycle.server_port
            self._lifecycle.mark_restarting(reason)

        LOGGER.warning("restarting HTTP server, reason: %s", reason)
        if request_old_shutdown:
            self._request_child_stop(old_port)
        self._stop_child_process(old_process)
        with self._lock:
            self._process = None

        try:
            process = self._start_child()
        except Exception as exc:
            with self._lock:
                if self._lifecycle.desired_running:
                    self._lifecycle.mark_failed(f"restart failed: {type(exc).__name__}: {exc}")
                self._process = None
            LOGGER.exception("failed to restart HTTP child")
            return

        should_discard = False
        with self._lock:
            if not self._lifecycle.desired_running:
                should_discard = True
            else:
                self._process = process
                self._lifecycle.restart_count += 1
                self._lifecycle.mark_running(int(self.config.listen_port), process.pid)
                self._health_failure_count = 0
                self._last_probe_error = ""
        if should_discard:
            LOGGER.info(
                "discarding restarted HTTP child pid=%s because stop was requested",
                getattr(process, "pid", None),
            )
            self._request_child_stop(int(self.config.listen_port))
            self._stop_child_process(process)
            return
        with self._lock:
            restart_count = self._lifecycle.restart_count
        LOGGER.info(
            "started new HTTP server pid=%s port=%s restart_count=%s",
            process.pid,
            self.config.listen_port,
            restart_count,
        )

    def _request_child_stop(self, port: int | None) -> None:
        """Ask the child process to stop through the loopback admin API."""
        if port is None:
            return
        try:
            _request_json(
                "POST",
                f"http://{_PROBE_HOST}:{int(port)}/admin/shutdown",
                {},
                timeout=0.5,
            )
        except Exception:
            LOGGER.debug("HTTP child admin shutdown request failed", exc_info=True)

    def _stop_child_process(self, process: Any) -> None:
        """Wait briefly for graceful exit, then kill the child."""
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


class _AdminTaskRegistry:
    """Short-lived in-memory admin task registry inside the HTTP child."""

    def __init__(self):
        """Create the task map and lock."""
        self._lock = threading.Lock()
        self._tasks: OrderedDict[str, _AdminTask] = OrderedDict()

    def submit(
        self,
        kind: str,
        params: dict[str, object],
        runner: Callable[[], str],
    ) -> dict[str, object]:
        """Create a task, start its worker thread, and return its queued state."""
        task = _AdminTask(uuid4().hex, kind, "queued", iso_now(), dict(params))
        with self._lock:
            self._tasks[task.task_id] = task
            self._prune_locked()
        LOGGER.info("admin task accepted task_id=%s kind=%s params=%s", task.task_id, kind, params)
        threading.Thread(
            target=self._run_task,
            args=(task.task_id, runner),
            name=f"admin-{kind}",
            daemon=True,
        ).start()
        return task.snapshot()

    def get(self, task_id: str) -> dict[str, object]:
        """Return one task snapshot or a not_found payload."""
        with self._lock:
            self._prune_locked()
            task = self._tasks.get(str(task_id))
            if task is None:
                return {"task_id": str(task_id), "status": "not_found"}
            return task.snapshot()

    def snapshot(self) -> dict[str, object]:
        """Return compact registry metrics for /status."""
        with self._lock:
            self._prune_locked()
            counts: dict[str, int] = {}
            for task in self._tasks.values():
                counts[task.status] = counts.get(task.status, 0) + 1
            return {"count": len(self._tasks), "status_counts": counts}

    def _run_task(self, task_id: str, runner: Callable[[], str]) -> None:
        """Run one task and record its final state."""
        self._mark_running(task_id)
        try:
            message = runner()
        except Exception as exc:
            self._mark_finished(task_id, "failed", "", f"{type(exc).__name__}: {exc}")
            LOGGER.exception("admin task failed task_id=%s", task_id)
            return
        self._mark_finished(task_id, "succeeded", message, "")
        LOGGER.info("admin task succeeded task_id=%s message=%s", task_id, message)

    def _mark_running(self, task_id: str) -> None:
        """Mark a task running."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return
            task.status = "running"
            task.started_at = iso_now()
            task.updated_monotonic = time.monotonic()
        LOGGER.info("admin task started task_id=%s", task_id)

    def _mark_finished(self, task_id: str, status: str, message: str, error: str) -> None:
        """Mark a task finished with success or failure details."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None:
                return
            task.status = status
            task.finished_at = iso_now()
            task.message = str(message or "")[:1000]
            task.error = str(error or "")[:1000]
            task.updated_monotonic = time.monotonic()

    def _prune_locked(self) -> None:
        """Drop stale or excess task records."""
        now = time.monotonic()
        for task_id, task in list(self._tasks.items()):
            if now - task.updated_monotonic >= _ADMIN_TASK_TTL_SECONDS:
                self._tasks.pop(task_id, None)
        while len(self._tasks) > _ADMIN_TASK_LIMIT:
            self._tasks.popitem(last=False)


class _ChildHTTPApp:
    """HTTP routes and request state that live inside the child process."""

    def __init__(
        self,
        config: AppConfig,
        settings: _ServerSettings,
        store: EventStore,
        service: ParkingBridgeService,
        server_port: int,
        parent_pid: int,
    ):
        """Store child-process dependencies and runtime counters."""
        self.config = config
        self.settings = settings
        self.store = store
        self.service = service
        self.server_port = server_port
        self.parent_pid = parent_pid
        self.process_pid = os.getpid()
        self.stats = _RuntimeStats()
        self.admin_tasks = _AdminTaskRegistry()
        self.image_limiter = _ImageRateLimiter(
            settings.image_rate_limit_per_minute,
            settings.image_rate_limit_burst,
        )
        self.server: uvicorn.Server | None = None

    def set_server(self, server: uvicorn.Server) -> None:
        """Attach the Uvicorn server so admin shutdown can stop it."""
        self.server = server

    def build_app(self) -> Starlette:
        """Build the Starlette application served by Uvicorn."""
        app = Starlette(
            routes=[
                Route("/{path:path}", self._handle_get, methods=["GET"]),
                Route("/{path:path}", self._handle_post_or_put, methods=["POST", "PUT"]),
            ]
        )
        app.add_middleware(_RequestLimitMiddleware, owner=self)
        return app

    async def _handle_get(self, request: Request) -> Response:
        """Route GET requests to /status, admin task state, images, or 404."""
        path = request.url.path
        if path == "/status":
            return self._handle_status()
        if path.startswith("/admin/tasks/"):
            if not self._is_loopback_request(request):
                return _json_response(403, {"status": "error", "message": "admin endpoint requires loopback"})
            task_id = path.removeprefix("/admin/tasks/").strip("/")
            return _json_response(200, self.admin_tasks.get(task_id))
        if self._is_image_path(path):
            return await self._handle_image(request, path)
        return _text_response(404, "Not Found")

    async def _handle_post_or_put(self, request: Request) -> Response:
        """Route POST/PUT requests to admin actions or Hikvision ingress."""
        path = request.url.path
        if path == "/admin/shutdown":
            return await self._handle_admin_shutdown(request)
        if path == "/admin/resend":
            return await self._handle_admin_resend(request)
        if path == "/admin/cleanup":
            return await self._handle_admin_cleanup(request)
        return await self._handle_ingress(request)

    def _handle_status(self) -> Response:
        """Build and return child-local business health."""
        try:
            runtime = self.service.get_runtime_snapshot()
            try:
                db_health = self.service.get_database_health()
            except Exception:
                LOGGER.exception("database health probe failed")
                db_health = {
                    "ok": False,
                    "kind": "error",
                    "message": "database health probe raised an exception",
                }
            db_ok = bool(db_health.get("ok"))
            db_error_kind = str(db_health.get("kind") or "")
            db_error_message = str(db_health.get("message") or "")
            database = self.service.get_status_snapshot() if db_ok else {}
            payload = {
                "status": "ok" if db_ok else "error",
                "time": iso_now(),
                "db_ok": db_ok,
                "db_error_kind": db_error_kind,
                "db_error_message": db_error_message,
                "queues": runtime.get("queues", {}),
                "workers": runtime.get("workers", {}),
                "cleanup": runtime.get("cleanup", {}),
                "errors": runtime.get("errors", {}),
                "events": {
                    "last_success_sent_at": str(database.get("last_success_sent_at") or ""),
                    "dead_letter": database.get("dead_letter_count"),
                    "failure_backlog": database.get("failure_backlog_count"),
                },
                "database": {
                    "main_size_bytes": database.get("db_main_size_bytes"),
                    "total_size_bytes": database.get("db_total_size_bytes"),
                },
                "admin_tasks": self.admin_tasks.snapshot(),
                "http_server": self._runtime_payload(),
            }
            return _json_response(200 if db_ok else 503, payload)
        except Exception as exc:
            LOGGER.exception("failed to build HTTP status payload")
            self.stats.record_exception(f"status failed: {type(exc).__name__}: {exc}")
            return _json_response(
                503,
                {
                    "status": "error",
                    "time": iso_now(),
                    "db_ok": False,
                    "db_error_kind": "error",
                    "db_error_message": f"status failed: {type(exc).__name__}: {exc}",
                    "message": f"status failed: {type(exc).__name__}: {exc}",
                    "http_server": self._runtime_payload(),
                },
            )

    def _runtime_payload(self) -> dict[str, object]:
        """Return child HTTP request counters and lifecycle data."""
        payload = self.stats.snapshot()
        payload["lifecycle"] = {
            "state": "running",
            "desired_running": True,
            "thread_alive": True,
            "process_alive": True,
            "server_port": self.server_port,
            "process_pid": self.process_pid,
            "parent_pid": self.parent_pid,
            "parent_alive": _multiprocessing_parent_alive(),
            "restart_count": 0,
        }
        return payload

    async def _handle_ingress(self, request: Request) -> Response:
        """Validate and enqueue one Hikvision report in the child service."""
        if request.url.path != self.config.listen_path:
            return _text_response(404, "Not Found")
        body_result = await self._read_limited_body(request, self.settings.max_request_bytes, require_length=True)
        if isinstance(body_result, Response):
            return body_result
        try:
            accepted = self.service.enqueue_http_request(
                request.headers.get("content-type", ""),
                body_result,
                _client_ip(request),
                getattr(request.state, "request_id", "-"),
                block=False,
            )
        except Exception as exc:
            self._record_exception(request, exc)
            LOGGER.exception("failed to enqueue inbound HTTP request")
            return _text_response(503, "Busy")
        if not accepted:
            self.stats.record_busy()
            return _text_response(503, "Busy")
        return _text_response(200, "OK")

    async def _handle_admin_shutdown(self, request: Request) -> Response:
        """Accept a loopback shutdown request and ask Uvicorn to exit."""
        if not self._is_loopback_request(request):
            return _json_response(403, {"status": "error", "message": "admin endpoint requires loopback"})
        LOGGER.info("HTTP child admin shutdown accepted")
        server = self.server
        if server is not None:
            threading.Thread(target=self._stop_server_soon, name="http-admin-shutdown", daemon=True).start()
        return _json_response(200, {"status": "shutting_down"})

    async def _handle_admin_resend(self, request: Request) -> Response:
        """Accept one resend task and return a task id immediately."""
        if not self._is_loopback_request(request):
            return _json_response(403, {"status": "error", "message": "admin endpoint requires loopback"})
        data = await self._read_json_body(request)
        if isinstance(data, Response):
            return data
        try:
            event_id = int(data.get("event_id"))
        except (TypeError, ValueError):
            return _json_response(400, {"status": "error", "message": "event_id must be an integer"})
        task = self.admin_tasks.submit(
            "resend",
            {"event_id": event_id},
            lambda: self._run_resend_task(event_id),
        )
        return _json_response(202, {"status": "accepted", "task_id": task["task_id"], "task_status": task["status"]})

    async def _handle_admin_cleanup(self, request: Request) -> Response:
        """Accept one cleanup task and return a task id immediately."""
        if not self._is_loopback_request(request):
            return _json_response(403, {"status": "error", "message": "admin endpoint requires loopback"})
        data = await self._read_json_body(request)
        if isinstance(data, Response):
            return data
        reason = str(data.get("reason") or "manual")
        task = self.admin_tasks.submit(
            "cleanup",
            {"reason": reason},
            lambda: self._run_cleanup_task(reason),
        )
        return _json_response(202, {"status": "accepted", "task_id": task["task_id"], "task_status": task["status"]})

    def _run_resend_task(self, event_id: int) -> str:
        """Run one manual resend through the child service and wait for a final state."""
        if self.store.get_event(event_id) is None:
            raise RuntimeError(f"event {event_id} does not exist")
        if not self.service.manual_resend(event_id):
            raise RuntimeError(f"event {event_id} is not resendable or service queue is full")
        deadline = time.monotonic() + _ADMIN_TASK_TIMEOUT_SECONDS
        last_status = ""
        while time.monotonic() < deadline:
            row = self.store.get_event(event_id)
            if row is None:
                raise RuntimeError(f"event {event_id} disappeared")
            last_status = str(row.get("status") or "")
            if last_status == "sent":
                return f"event {event_id} sent"
            if last_status == "dead_letter":
                error = str(row.get("last_error") or row.get("response_text") or "send failed")
                raise RuntimeError(f"event {event_id} dead_letter: {error}")
            time.sleep(_ADMIN_TASK_POLL_SECONDS)
        raise TimeoutError(f"event {event_id} resend did not finish; last_status={last_status or '-'}")

    def _run_cleanup_task(self, reason: str) -> str:
        """Run one cleanup task through the child service and wait until it finishes."""
        before = self.service.get_runtime_snapshot().get("cleanup", {})
        before_finished_at = str(before.get("finished_at") or "") if isinstance(before, dict) else ""
        if not self.service.request_cleanup(reason):
            raise RuntimeError("cleanup task is already active, pending, or service queue is full")
        deadline = time.monotonic() + _ADMIN_TASK_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            cleanup = self.service.get_runtime_snapshot().get("cleanup", {})
            if not isinstance(cleanup, dict):
                time.sleep(_ADMIN_TASK_POLL_SECONDS)
                continue
            pending = bool(cleanup.get("pending"))
            active = bool(cleanup.get("active"))
            finished_at = str(cleanup.get("finished_at") or "")
            if not pending and not active and finished_at and finished_at != before_finished_at:
                return f"cleanup finished: {cleanup.get('summary') or {}}"
            time.sleep(_ADMIN_TASK_POLL_SECONDS)
        raise TimeoutError("cleanup task did not finish in time")

    async def _handle_image(self, request: Request, path: str) -> Response:
        """Serve a saved event image by safe file name."""
        image_name = self._image_name(path)
        if image_name is None:
            return _text_response(404, "Not Found")
        client_ip = _client_ip(request)
        if not self.image_limiter.allow(client_ip):
            return _text_response(429, "Too Many Requests")
        image_path = await asyncio.to_thread(self.store.resolve_public_image_path, image_name)
        if image_path is None:
            return _text_response(404, "Not Found")
        try:
            data = await asyncio.to_thread(image_path.read_bytes)
        except OSError:
            return _text_response(404, "Not Found")
        return Response(data, media_type=mimetypes.guess_type(image_path.name)[0] or "application/octet-stream")

    async def _read_json_body(self, request: Request) -> dict[str, object] | Response:
        """Read and parse a small admin JSON request body."""
        body_result = await self._read_limited_body(request, self.settings.admin_max_request_bytes, require_length=False)
        if isinstance(body_result, Response):
            return body_result
        if not body_result:
            return {}
        try:
            value = json.loads(body_result.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return _json_response(400, {"status": "error", "message": f"invalid JSON: {exc}"})
        if not isinstance(value, dict):
            return _json_response(400, {"status": "error", "message": "JSON root must be an object"})
        return value

    async def _read_limited_body(
        self,
        request: Request,
        max_bytes: int,
        require_length: bool,
    ) -> bytes | Response:
        """Read a request body with Content-Length and timeout protections."""
        content_length = _content_length(request)
        if content_length is None:
            if require_length:
                return _text_response(400, "Missing Content-Length")
        elif content_length < 0:
            return _text_response(400, "Invalid Content-Length")
        elif content_length > max_bytes:
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
        if content_length is not None and len(body) != content_length:
            return _text_response(400, "Bad Request")
        if len(body) > max_bytes:
            return _text_response(413, "Payload Too Large")
        return body

    def _stop_server_soon(self) -> None:
        """Give the shutdown response a moment to flush before stopping Uvicorn."""
        time.sleep(0.05)
        if self.server is not None:
            self.server.should_exit = True

    def _is_loopback_request(self, request: Request) -> bool:
        """Return whether this request comes from a loopback address."""
        host = _client_ip(request)
        try:
            return ip_address(host).is_loopback
        except ValueError:
            return host in {"localhost"}

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
        if request.method == "GET" and request.url.path == "/status" and response.status_code < 400:
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


def _run_http_child(config: AppConfig, settings: _ServerSettings, parent_pid: int) -> None:
    """Child-process target that owns the HTTP server and business runtime."""
    setup_logging(config.log_path)
    listen_socket: socket.socket | None = None
    service: ParkingBridgeService | None = None
    try:
        listen_socket = _bind_socket(_LISTEN_HOST, int(config.listen_port), settings.request_queue_size)
        server_port = int(listen_socket.getsockname()[1])
        store = EventStore(config.db_path)
        service = ParkingBridgeService(config, store, PartnerClient(config))
        child_app = _ChildHTTPApp(config, settings, store, service, server_port, parent_pid)
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
        child_app.set_server(server)
        _start_parent_sentinel_watcher(server, parent_pid)
        server.run(sockets=[listen_socket])
    except BaseException:
        LOGGER.exception("HTTP child failed")
    finally:
        if service is not None:
            service.close()
        if listen_socket is not None:
            try:
                listen_socket.close()
            except OSError:
                pass


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


def _settings_from_constants() -> _ServerSettings:
    """Capture monkeypatchable module constants for the child process."""
    return _ServerSettings(
        _MAX_HEADER_COUNT,
        _MAX_HEADER_BYTES,
        _MAX_REQUEST_PATH_CHARS,
        _MAX_REQUEST_BYTES,
        _ADMIN_MAX_REQUEST_BYTES,
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
            display_text = "HTTP 进程未运行"
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


def _probe_status(port: int) -> tuple[bool, str, dict[str, object]]:
    """Probe /status and require a healthy JSON payload."""
    try:
        status_code, payload = _request_json(
            "GET",
            f"http://{_PROBE_HOST}:{int(port)}/status",
            timeout=_HEALTH_CHECK_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}", {}
    if status_code != 200:
        if status_code == 503 and payload.get("db_error_kind") == "timeout":
            return True, "database health timeout ignored", payload
        return False, f"HTTP {status_code}", payload
    if payload.get("status") != "ok" or payload.get("db_ok") is not True:
        return False, f"unhealthy status: {payload.get('status')}", payload
    return True, "", payload


def _probe_status_reachable(port: int) -> tuple[bool, str]:
    """Return whether /status is reachable, regardless of business health."""
    try:
        _request_json("GET", f"http://{_PROBE_HOST}:{int(port)}/status", timeout=0.5)
        return True, ""
    except urllib.error.HTTPError as exc:
        return exc.code in {200, 503}, f"HTTP {exc.code}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _request_json(
    method: str,
    url: str,
    payload: dict[str, object] | None = None,
    timeout: float = _HEALTH_CHECK_TIMEOUT_SECONDS,
) -> tuple[int, dict[str, object]]:
    """Send a small loopback JSON request without system proxy settings."""
    data: bytes | None = None
    headers: dict[str, str] = {}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
        headers["Content-Length"] = str(len(data))
    request = urllib.request.Request(url, data=data, method=method.upper(), headers=headers)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=timeout) as response:
            body = response.read(1_000_000)
            return int(response.status), _decode_json_object(body)
    except urllib.error.HTTPError as exc:
        body = exc.read(1_000_000)
        return int(exc.code), _decode_json_object(body)


def _decode_json_object(body: bytes) -> dict[str, object]:
    """Decode a JSON object from bytes, returning an empty object on blank body."""
    if not body:
        return {}
    value = json.loads(body.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("response JSON root is not an object")
    return value


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


def _multiprocessing_parent_alive() -> bool:
    """Return whether multiprocessing's parent sentinel still reports alive."""
    parent = multiprocessing.parent_process()
    if parent is None:
        return False
    try:
        return bool(parent.is_alive())
    except Exception:
        return False
