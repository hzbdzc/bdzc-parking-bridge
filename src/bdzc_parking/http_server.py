"""Hikvision inbound HTTP server managed as a child Uvicorn process."""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import mimetypes
import multiprocessing
import os
import queue
import signal
import socket
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass, field
from logging.handlers import QueueHandler
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import uvicorn
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from bdzc_parking.common import iso_now, iso_seconds_from_now
from bdzc_parking.config import AppConfig
from bdzc_parking.service import ParkingBridgeService


LOGGER = logging.getLogger(__name__)
_LISTEN_HOST = "0.0.0.0"
_PROBE_HOST = "127.0.0.1"
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
_IPC_INGRESS_QUEUE_SIZE = 256
_IPC_STATUS_QUEUE_SIZE = 8
_IPC_STATUS_REQUEST_QUEUE_SIZE = 16
_IPC_READY_QUEUE_SIZE = 4
_IPC_LOG_QUEUE_SIZE = 1024
_REQUEST_COUNTER = 0
_REQUEST_COUNTER_LOCK = threading.Lock()
_SERVER_START_TIMEOUT_SECONDS = 5.0
_SERVER_STOP_GRACE_SECONDS = 2.0
_SERVER_TERMINATE_TIMEOUT_SECONDS = 1.0
_SERVER_KILL_TIMEOUT_SECONDS = 1.0
_SERVER_TRANSITION_STALE_SECONDS = 15.0
_ORPHAN_EXIT_GRACE_SECONDS = 2.0
_ORPHAN_STATUS_PROBE_TIMEOUT_SECONDS = 0.5
_ORPHAN_TERMINATE_TIMEOUT_SECONDS = 3.0
_HEALTH_CHECK_INTERVAL_SECONDS = 10.0
_HEALTH_CHECK_TIMEOUT_SECONDS = 3.0
_HEALTH_FAILURE_THRESHOLD = 2
_HEALTH_RESTART_COOLDOWN_SECONDS = 30.0
_STATUS_PUBLISH_INTERVAL_SECONDS = 1.0
_STATUS_REQUEST_TIMEOUT_SECONDS = 2.0
_IPC_DRAIN_ENQUEUE_TIMEOUT_SECONDS = 1.0
_ROOT_LIVENESS_BODY = "BDZC Parking Bridge is running"


@dataclass(frozen=True)
class _ChildSettings:
    """Picklable HTTP limits passed from the main process into the child."""

    max_header_count: int
    max_header_bytes: int
    max_request_path_chars: int
    max_request_bytes: int
    request_read_timeout_seconds: float
    http_max_connections: int
    http_request_queue_size: int
    image_rate_limit_per_minute: int
    image_rate_limit_burst: int


@dataclass(frozen=True)
class _IngressIPCRequest:
    """Raw HTTP request item sent from the child process to the main process."""

    content_type: str
    body: bytes
    client_ip: str
    request_id: int | str


@dataclass(frozen=True)
class _BridgePortOwner:
    """Identity of a BDZC HTTP child process currently bound to a port."""

    pid: int | None
    orphaned: bool
    detail: str


@dataclass(frozen=True)
class _ChildStartResult:
    """Resources created for one child-process start attempt."""

    process: multiprocessing.Process
    server_port: int
    process_pid: int | None
    shutdown_event: Any
    ready_queue: Any
    parent_sentinel_writer: Any


@dataclass
class _LifecycleState:
    """Store process lifecycle fields and render their public snapshot."""

    state: str = "stopped"
    desired_running: bool = False
    stop_requested: bool = False
    last_start_requested_at: str = ""
    last_started_at: str = ""
    last_stopped_at: str = ""
    last_failed_at: str = ""
    last_failure_reason: str = ""
    process_pid: int | None = None
    server_port: int | None = None
    restart_count: int = 0
    state_changed_monotonic: float = field(default_factory=time.monotonic, repr=False)

    def mark_starting(self) -> None:
        """Record that startup was requested by the main program."""
        self.state = "starting"
        self.state_changed_monotonic = time.monotonic()
        self.desired_running = True
        self.stop_requested = False
        self.last_start_requested_at = iso_now()

    def mark_running(self, server_port: int, process_pid: int | None) -> None:
        """Record that the child process confirmed Uvicorn is listening."""
        self.state = "running"
        self.state_changed_monotonic = time.monotonic()
        self.stop_requested = False
        self.server_port = int(server_port)
        self.process_pid = process_pid
        self.last_started_at = iso_now()
        self.last_failed_at = ""
        self.last_failure_reason = ""

    def mark_stopping(self) -> None:
        """Record that graceful child-process shutdown was requested."""
        self.state = "stopping"
        self.state_changed_monotonic = time.monotonic()
        self.stop_requested = True

    def mark_restarting(self, reason: str) -> None:
        """Record that the manager is replacing an unhealthy child process."""
        self.state = "restarting"
        self.state_changed_monotonic = time.monotonic()
        self.desired_running = True
        self.stop_requested = False
        self.last_failed_at = iso_now()
        self.last_failure_reason = str(reason or "health restart")[:1000]

    def mark_stopped(self) -> None:
        """Record a normal stop and clear process identity fields."""
        self.state = "stopped"
        self.state_changed_monotonic = time.monotonic()
        self.stop_requested = False
        self.last_stopped_at = iso_now()
        self.process_pid = None
        self.server_port = None

    def record_failure(self, reason: str) -> None:
        """Record a non-user-requested failure reason."""
        self.state = "failed"
        self.state_changed_monotonic = time.monotonic()
        self.last_failed_at = iso_now()
        self.last_failure_reason = str(reason or "unknown failure")[:1000]

    def record_restart(self) -> None:
        """Increment the child-process restart counter."""
        self.restart_count += 1

    def snapshot(self, process_alive: bool) -> dict[str, object]:
        """Return the lifecycle JSON shape used by GUI and /status."""
        return {
            "state": self.state,
            "desired_running": self.desired_running,
            "thread_alive": process_alive,
            "process_alive": process_alive,
            "process_pid": self.process_pid,
            "server_port": self.server_port,
            "restart_count": self.restart_count,
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
        """Record a 503 response caused by ingress backpressure."""
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
    """Main-process manager for a child Uvicorn HTTP server process."""

    def __init__(self, config: AppConfig, service: ParkingBridgeService):
        """Save config, business service, and process-management state."""
        self.config = config
        self.service = service
        self._lock = threading.RLock()
        self._lifecycle = _LifecycleState()
        self._mp_context = multiprocessing.get_context("spawn")
        self._process: multiprocessing.Process | None = None
        self._shutdown_event: Any = None
        self._ingress_queue: Any = None
        self._status_queue: Any = None
        self._status_request_queue: Any = None
        self._ready_queue: Any = None
        self._log_queue: Any = None
        self._parent_sentinel_writer: Any = None
        self._parent_stop_event = threading.Event()
        self._ingress_thread: threading.Thread | None = None
        self._status_thread: threading.Thread | None = None
        self._health_thread: threading.Thread | None = None
        self._log_thread: threading.Thread | None = None
        self._restart_thread: threading.Thread | None = None
        self._restart_generation = 0
        self._health_failure_count = 0
        self._last_probe_at = ""
        self._last_probe_error = ""
        self._last_restart_at = ""
        self._next_restart_allowed_at = ""
        self._next_restart_allowed_monotonic = 0.0
        self._ipc_dropped_count = 0

    @property
    def is_running(self) -> bool:
        """Return whether the manager believes the child process is running."""
        with self._lock:
            return self._lifecycle.state == "running" and self._process_alive_locked()

    def start(self) -> None:
        """Start the child Uvicorn process and parent management threads."""
        with self._lock:
            self._recover_stale_transition_locked()
            if self._lifecycle.state in {"starting", "running"} and self._process_alive_locked():
                self._ensure_parent_threads_locked()
                return
            if self._lifecycle.state == "stopping":
                raise RuntimeError("HTTP server is stopping")
            self._restart_generation += 1
            self._lifecycle.mark_starting()
            self._parent_stop_event.clear()
            self._ensure_ipc_locked()
            self._ensure_parent_threads_locked()

        try:
            process, server_port, process_pid = self._start_child_process()
        except Exception as exc:
            self._record_start_failure(exc)
            raise

        with self._lock:
            self._process = process
            self._lifecycle.mark_running(server_port, process_pid)
            self._health_failure_count = 0
            self._last_probe_error = ""
            self._publish_status_snapshot_locked()

        LOGGER.info(
            "HTTP server child process listening on %s:%s parent_pid=%s child_pid=%s",
            _LISTEN_HOST,
            server_port,
            os.getpid(),
            process_pid,
        )

    def stop(self) -> None:
        """Gracefully stop the child, then terminate or kill it if needed."""
        with self._lock:
            self._lifecycle.desired_running = False
            self._restart_generation += 1
            self._restart_thread = None
            if self._lifecycle.state in {"starting", "running", "restarting", "failed"}:
                self._lifecycle.mark_stopping()
            process = self._process
            shutdown_event = self._shutdown_event
            parent_sentinel_writer = self._parent_sentinel_writer
            self._parent_sentinel_writer = None

        self._request_child_stop(shutdown_event)
        _close_connection(parent_sentinel_writer)
        self._stop_child_process(process)
        self._parent_stop_event.set()
        self._join_parent_threads()

        with self._lock:
            self._process = None
            if self._lifecycle.state in {"starting", "running", "restarting", "stopping", "failed"}:
                self._lifecycle.mark_stopped()
            self._close_ipc_locked()

        LOGGER.info("HTTP server child process stopped")

    def get_lifecycle_snapshot(self) -> dict[str, object]:
        """Return HTTP server lifecycle status for GUI and /status."""
        with self._lock:
            self._recover_stale_transition_locked()
            return self._lifecycle.snapshot(self._process_alive_locked())

    def get_runtime_snapshot(self) -> dict[str, object]:
        """Return main-process HTTP manager metrics."""
        lifecycle = self.get_lifecycle_snapshot()
        control = self.get_control_snapshot(lifecycle)
        with self._lock:
            next_restart_allowed_at = self._next_restart_allowed_at
            if self._next_restart_allowed_monotonic <= time.monotonic():
                next_restart_allowed_at = ""
            return {
                "active_requests": 0,
                "busy_response_count": 0,
                "request_exception_count": 0,
                "last_request_exception_at": "",
                "last_request_exception": "",
                "lifecycle": lifecycle,
                "control": control,
                "health": {
                    "enabled": self._health_thread is not None and self._health_thread.is_alive(),
                    "failure_count": self._health_failure_count,
                    "last_probe_at": self._last_probe_at,
                    "last_probe_error": self._last_probe_error,
                    "last_restart_at": self._last_restart_at,
                    "next_restart_allowed_at": next_restart_allowed_at,
                },
                "ipc": {
                    "ingress_queue_length": _safe_queue_size(self._ingress_queue),
                    "status_queue_length": _safe_queue_size(self._status_queue),
                    "dropped_ingress_count": self._ipc_dropped_count,
                },
            }

    def get_control_snapshot(
        self,
        lifecycle: dict[str, object] | None = None,
    ) -> dict[str, object]:
        """Return display and button state so GUI does not inspect process details."""
        if lifecycle is None:
            lifecycle = self.get_lifecycle_snapshot()
        with self._lock:
            self._recover_stale_transition_locked()
            lifecycle = self._lifecycle.snapshot(self._process_alive_locked())
            health_failure_count = self._health_failure_count
            health_probe_error = self._last_probe_error
            health_probe_at = self._last_probe_at
        return _build_control_snapshot(lifecycle, health_failure_count, health_probe_error, health_probe_at)

    def _recover_stale_transition_locked(self) -> None:
        """Move stale startup/restart states to failed so the GUI never stays busy forever."""
        if self._lifecycle.state not in {"starting", "restarting"}:
            return
        elapsed = time.monotonic() - self._lifecycle.state_changed_monotonic
        if elapsed < _SERVER_TRANSITION_STALE_SECONDS:
            return
        reason = f"HTTP server {self._lifecycle.state} timed out after {elapsed:.1f}s"
        LOGGER.error(reason)
        self._lifecycle.record_failure(reason)
        self._process = None
        self._restart_generation += 1
        self._restart_thread = None
        _close_connection(self._parent_sentinel_writer)
        self._parent_sentinel_writer = None
        self._last_probe_error = reason
        self._next_restart_allowed_monotonic = 0.0
        self._next_restart_allowed_at = ""

    def _ensure_ipc_locked(self) -> None:
        """Create IPC queues/events when they are missing."""
        if self._shutdown_event is None:
            self._shutdown_event = self._mp_context.Event()
        else:
            self._shutdown_event.clear()
        if self._ingress_queue is None:
            self._ingress_queue = self._mp_context.Queue(maxsize=_IPC_INGRESS_QUEUE_SIZE)
        if self._status_queue is None:
            self._status_queue = self._mp_context.Queue(maxsize=_IPC_STATUS_QUEUE_SIZE)
        if self._status_request_queue is None:
            self._status_request_queue = self._mp_context.Queue(maxsize=_IPC_STATUS_REQUEST_QUEUE_SIZE)
        if self._ready_queue is None:
            self._ready_queue = self._mp_context.Queue(maxsize=_IPC_READY_QUEUE_SIZE)
        if self._log_queue is None:
            self._log_queue = self._mp_context.Queue(maxsize=_IPC_LOG_QUEUE_SIZE)

    def _close_ipc_locked(self) -> None:
        """Close IPC objects after all parent threads have stopped."""
        _close_connection(self._parent_sentinel_writer)
        for queue_obj in (
            self._ingress_queue,
            self._status_queue,
            self._status_request_queue,
            self._ready_queue,
            self._log_queue,
        ):
            _close_queue(queue_obj)
        self._shutdown_event = None
        self._ingress_queue = None
        self._status_queue = None
        self._status_request_queue = None
        self._ready_queue = None
        self._log_queue = None
        self._parent_sentinel_writer = None

    def _ensure_parent_threads_locked(self) -> None:
        """Start the parent-side IPC, status, health, and log workers."""
        if self._ingress_thread is None or not self._ingress_thread.is_alive():
            self._ingress_thread = threading.Thread(
                target=self._ingress_drain_loop,
                name="hikvision-http-ipc-ingress",
                daemon=True,
            )
            self._ingress_thread.start()
        if self._status_thread is None or not self._status_thread.is_alive():
            self._status_thread = threading.Thread(
                target=self._status_publisher_loop,
                name="hikvision-http-status-publisher",
                daemon=True,
            )
            self._status_thread.start()
        if self._health_thread is None or not self._health_thread.is_alive():
            self._health_thread = threading.Thread(
                target=self._health_loop,
                name="hikvision-http-health",
                daemon=True,
            )
            self._health_thread.start()
        if self._log_thread is None or not self._log_thread.is_alive():
            self._log_thread = threading.Thread(
                target=self._log_drain_loop,
                name="hikvision-http-log-drain",
                daemon=True,
            )
            self._log_thread.start()

    def _start_child_process(self) -> tuple[multiprocessing.Process, int, int | None]:
        """Spawn the child process and wait for its ready notification."""
        result = self._start_child_process_attempt()
        with self._lock:
            self._publish_child_start_result_locked(result)
        return result.process, result.server_port, result.process_pid

    def _start_child_process_attempt(self) -> _ChildStartResult:
        """Start one child process with per-attempt shutdown and ready objects."""
        self._cleanup_orphan_http_process_on_port()
        parent_sentinel_reader: Any = None
        parent_sentinel_writer: Any = None
        shutdown_event: Any = None
        ready_queue: Any = None
        with self._lock:
            self._ensure_ipc_locked()
            assert self._ingress_queue is not None
            assert self._status_queue is not None
            assert self._status_request_queue is not None
            assert self._log_queue is not None
            shutdown_event = self._mp_context.Event()
            ready_queue = self._mp_context.Queue(maxsize=_IPC_READY_QUEUE_SIZE)
            parent_sentinel_reader, parent_sentinel_writer = self._mp_context.Pipe(duplex=False)
            settings = _settings_from_constants()
            process = self._mp_context.Process(
                target=_run_uvicorn_child,
                name="hikvision-uvicorn-child",
                args=(
                    self.config,
                    settings,
                    self._ingress_queue,
                    self._status_queue,
                    self._status_request_queue,
                    ready_queue,
                    self._log_queue,
                    shutdown_event,
                    parent_sentinel_reader,
                ),
                daemon=False,
            )

        try:
            process.start()
        except Exception:
            _close_connection(parent_sentinel_reader)
            _close_connection(parent_sentinel_writer)
            _close_queue(ready_queue)
            raise
        _close_connection(parent_sentinel_reader)

        try:
            deadline = time.monotonic() + _SERVER_START_TIMEOUT_SECONDS
            last_message: dict[str, object] | None = None
            while time.monotonic() < deadline:
                try:
                    message = ready_queue.get(timeout=0.05)
                except queue.Empty:
                    if not _process_is_alive(process):
                        process.join(timeout=0.1)
                        break
                    continue
                if isinstance(message, dict):
                    last_message = message
                    if message.get("status") == "started":
                        port = int(message["port"])
                        pid = message.get("pid")
                        return _ChildStartResult(
                            process,
                            port,
                            int(pid) if pid is not None else process.pid,
                            shutdown_event,
                            ready_queue,
                            parent_sentinel_writer,
                        )
                    if message.get("status") == "error":
                        self._request_child_stop(shutdown_event)
                        _close_connection(parent_sentinel_writer)
                        self._stop_child_process(process)
                        _close_queue(ready_queue)
                        error = str(message.get("error") or "HTTP server child failed to start")
                        if message.get("type") == "OSError":
                            raise OSError(error)
                        raise RuntimeError(error)

            self._request_child_stop(shutdown_event)
            _close_connection(parent_sentinel_writer)
            self._stop_child_process(process)
            _close_queue(ready_queue)
            if last_message is not None:
                raise RuntimeError(str(last_message.get("error") or "HTTP server startup failed"))
            raise TimeoutError("HTTP server startup timed out")
        except Exception:
            _close_connection(parent_sentinel_writer)
            _close_queue(ready_queue)
            raise

    def _publish_child_start_result_locked(self, result: _ChildStartResult) -> None:
        """Make one successful child start attempt the active managed process."""
        _close_connection(self._parent_sentinel_writer)
        _close_queue(self._ready_queue)
        self._shutdown_event = result.shutdown_event
        self._ready_queue = result.ready_queue
        self._parent_sentinel_writer = result.parent_sentinel_writer

    def _cleanup_orphan_http_process_on_port(self) -> None:
        """Stop a confirmed orphan BDZC HTTP child before binding its port."""
        port = int(self.config.listen_port)
        if port <= 0:
            return
        owner = _inspect_bridge_port_owner(port)
        if owner is None:
            return
        if not owner.orphaned:
            raise OSError(
                f"HTTP server port {port} is already used by another running bridge instance"
                f"{_format_pid_suffix(owner.pid)}"
            )
        if owner.pid is None:
            raise OSError(f"HTTP server port {port} is used by an orphan bridge HTTP process with unknown pid")
        LOGGER.warning("stopping orphan HTTP child on port %s pid=%s detail=%s", port, owner.pid, owner.detail)
        if not _terminate_process_id(owner.pid):
            raise OSError(f"failed to terminate orphan HTTP child pid={owner.pid} on port {port}")
        if not _wait_for_port_release(port, _ORPHAN_TERMINATE_TIMEOUT_SECONDS):
            raise OSError(f"orphan HTTP child pid={owner.pid} did not release port {port}")

    def _request_child_stop(self, shutdown_event: Any) -> None:
        """Ask the child process to exit through the shared shutdown event."""
        if shutdown_event is not None:
            try:
                shutdown_event.set()
            except Exception:
                LOGGER.debug("failed to set child shutdown event", exc_info=True)

    def _stop_child_process(self, process: Any) -> None:
        """Wait for graceful exit, then terminate and kill as a final fallback."""
        if process is None:
            return
        if not _process_is_alive(process):
            _join_and_close_process(process, 0.1)
            return

        _join_process(process, _SERVER_STOP_GRACE_SECONDS)
        if not _process_is_alive(process):
            _join_and_close_process(process, 0.1)
            return

        LOGGER.warning(
            "HTTP server child pid=%s did not exit after %.1fs; terminating",
            getattr(process, "pid", None),
            _SERVER_STOP_GRACE_SECONDS,
        )
        try:
            process.terminate()
        except Exception:
            LOGGER.debug("failed to terminate HTTP server child", exc_info=True)
        _join_process(process, _SERVER_TERMINATE_TIMEOUT_SECONDS)
        if not _process_is_alive(process):
            _join_and_close_process(process, 0.1)
            return

        LOGGER.warning("HTTP server child pid=%s still alive; killing", getattr(process, "pid", None))
        kill = getattr(process, "kill", None)
        if callable(kill):
            try:
                kill()
            except Exception:
                LOGGER.debug("failed to kill HTTP server child", exc_info=True)
        _join_process(process, _SERVER_KILL_TIMEOUT_SECONDS)
        if _process_is_alive(process):
            LOGGER.error("HTTP server child pid=%s is still alive after kill", getattr(process, "pid", None))
        else:
            _join_and_close_process(process, 0.1)

    def _join_parent_threads(self) -> None:
        """Wait briefly for parent management threads to exit."""
        current = threading.current_thread()
        for thread in (
            self._ingress_thread,
            self._status_thread,
            self._health_thread,
            self._log_thread,
        ):
            if thread is None or thread is current:
                continue
            thread.join(timeout=3)
            if thread.is_alive():
                LOGGER.warning("HTTP parent management thread did not exit: %s", thread.name)
        self._ingress_thread = None
        self._status_thread = None
        self._health_thread = None
        self._log_thread = None

    def _ingress_drain_loop(self) -> None:
        """Forward child-process ingress items into the business service queue."""
        while True:
            if self._parent_stop_event.is_set() and _queue_empty(self._ingress_queue):
                break
            try:
                item = self._ingress_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            except (OSError, EOFError):
                break
            if not isinstance(item, _IngressIPCRequest):
                continue
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
                accepted = False
                LOGGER.exception(
                    "failed to forward HTTP ingress request_id=%s client=%s",
                    item.request_id,
                    item.client_ip,
                )
            if not accepted:
                with self._lock:
                    self._ipc_dropped_count += 1
                LOGGER.warning(
                    "HTTP ingress item dropped request_id=%s client=%s",
                    item.request_id,
                    item.client_ip,
                )

    def _status_publisher_loop(self) -> None:
        """Publish parent status snapshots and answer child /status requests."""
        last_published_at = 0.0
        while not self._parent_stop_event.is_set():
            request_id = ""
            try:
                value = self._status_request_queue.get(timeout=0.1)
                request_id = str(value or "")
            except queue.Empty:
                pass
            except (OSError, EOFError):
                break

            now = time.monotonic()
            if request_id:
                self._publish_status_snapshot(request_id)
                last_published_at = now
                continue
            if now - last_published_at >= _STATUS_PUBLISH_INTERVAL_SECONDS:
                self._publish_status_snapshot("")
                last_published_at = now

    def _health_loop(self) -> None:
        """Periodically probe the child process and restart it on repeated failure."""
        while not self._parent_stop_event.wait(_HEALTH_CHECK_INTERVAL_SECONDS):
            try:
                self._health_check_once()
            except Exception:
                LOGGER.exception("HTTP server health loop failed")

    def _log_drain_loop(self) -> None:
        """Forward child-process log records into this process logging tree."""
        while True:
            if self._parent_stop_event.is_set() and _queue_empty(self._log_queue):
                break
            try:
                record = self._log_queue.get(timeout=0.2)
            except queue.Empty:
                continue
            except (OSError, EOFError):
                break
            if isinstance(record, logging.LogRecord):
                logging.getLogger(record.name).handle(record)

    def _health_check_once(self) -> None:
        """Perform one child-process liveness and HTTP reachability probe."""
        with self._lock:
            self._recover_stale_transition_locked()
            if not self._lifecycle.desired_running or self._lifecycle.state in {"starting", "restarting", "stopping"}:
                return
            process = self._process
            port = self._lifecycle.server_port
            if process is None or not self._process_alive_locked():
                error = "HTTP server child process is not alive"
            elif port is None:
                error = "HTTP server port is unknown"
            else:
                error = ""

        if not error:
            assert port is not None
            root_ok, root_error = _probe_http_root(port)
            status_ok, status_error = _probe_http_status(port)
            if not root_ok:
                error = f"GET / failed: {root_error}"
            elif not status_ok:
                error = f"GET /status failed: {status_error}"

        if error:
            failure_count = self._record_health_failure(error)
            if failure_count >= _HEALTH_FAILURE_THRESHOLD:
                self._restart_from_health(error)
            return
        self._record_health_success()

    def _record_health_success(self) -> None:
        """Clear consecutive health-probe failures."""
        with self._lock:
            self._last_probe_at = iso_now()
            self._last_probe_error = ""
            self._health_failure_count = 0

    def _record_health_failure(self, error: str) -> int:
        """Record one health-probe failure and return the current count."""
        with self._lock:
            self._last_probe_at = iso_now()
            self._last_probe_error = error[:1000]
            self._health_failure_count += 1
            return self._health_failure_count

    def _restart_from_health(self, reason: str) -> None:
        """Schedule a child-process restart after repeated failed health checks."""
        now = time.monotonic()
        with self._lock:
            if not self._lifecycle.desired_running:
                return
            if now < self._next_restart_allowed_monotonic:
                return
            self._restart_generation += 1
            generation = self._restart_generation
            old_process = self._process
            old_shutdown_event = self._shutdown_event
            old_parent_sentinel_writer = self._parent_sentinel_writer
            old_process_alive = self._process_alive_locked()
            self._process = None
            self._parent_sentinel_writer = None
            self._next_restart_allowed_monotonic = now + _HEALTH_RESTART_COOLDOWN_SECONDS
            self._next_restart_allowed_at = iso_seconds_from_now(_HEALTH_RESTART_COOLDOWN_SECONDS)
            self._lifecycle.mark_restarting(f"health restart: {reason}")
            thread = threading.Thread(
                target=self._restart_worker,
                args=(generation, reason, old_process, old_shutdown_event, old_parent_sentinel_writer, old_process_alive),
                name=f"hikvision-http-restart-{generation}",
                daemon=True,
            )
            self._restart_thread = thread

        LOGGER.warning("HTTP server health restarting child: %s", reason)
        thread.start()

    def _restart_worker(
        self,
        generation: int,
        reason: str,
        old_process: Any,
        old_shutdown_event: Any,
        old_parent_sentinel_writer: Any,
        old_process_alive: bool,
    ) -> None:
        """Run one restart attempt without blocking the health loop."""
        try:
            self._request_child_stop(old_shutdown_event)
            _close_connection(old_parent_sentinel_writer)
            if old_process_alive:
                self._stop_child_process(old_process)
            else:
                self._cleanup_old_process_in_background(old_process)
            result = self._start_child_process_attempt()
        except Exception as exc:
            self._record_restart_worker_failure(generation, exc)
            return

        stale_result = False
        with self._lock:
            if generation != self._restart_generation or not self._lifecycle.desired_running:
                stale_result = True
            else:
                self._publish_child_start_result_locked(result)
                self._process = result.process
                self._lifecycle.record_restart()
                self._lifecycle.mark_running(result.server_port, result.process_pid)
                self._health_failure_count = 0
                self._last_probe_error = ""
                self._last_restart_at = iso_now()
                self._restart_thread = None
                self._publish_status_snapshot_locked()

        if stale_result:
            self._request_child_stop(result.shutdown_event)
            _close_connection(result.parent_sentinel_writer)
            self._stop_child_process(result.process)
            _close_queue(result.ready_queue)
            return

        LOGGER.info(
            "HTTP server health restarted child on %s:%s child_pid=%s reason=%s",
            _LISTEN_HOST,
            result.server_port,
            result.process_pid,
            reason,
        )

    def _record_restart_worker_failure(self, generation: int, exc: BaseException) -> None:
        """Record a restart worker failure only if it is still current."""
        with self._lock:
            if generation != self._restart_generation:
                return
            self._lifecycle.record_failure(f"restart failed: {type(exc).__name__}: {exc}")
            self._process = None
            self._restart_thread = None
            self._next_restart_allowed_monotonic = 0.0
            self._next_restart_allowed_at = ""
            self._last_probe_error = str(exc)[:1000]
        LOGGER.error(
            "failed to restart HTTP server child process",
            exc_info=(type(exc), exc, exc.__traceback__),
        )

    def _cleanup_old_process_in_background(self, process: Any) -> None:
        """Clean a dead or stale Process handle without blocking service recovery."""
        if process is None:
            return

        def cleanup() -> None:
            """Best-effort cleanup for an abandoned Process object."""
            try:
                self._stop_child_process(process)
            except Exception:
                LOGGER.debug("background HTTP process cleanup failed", exc_info=True)

        thread = threading.Thread(target=cleanup, name="hikvision-http-old-process-cleanup", daemon=True)
        thread.start()

    def _publish_status_snapshot(self, request_id: str = "") -> None:
        """Build and publish a parent status snapshot to the child process."""
        try:
            payload = self._build_status_payload(request_id)
        except Exception:
            LOGGER.exception("failed to build HTTP status snapshot")
            payload = {
                "status": "error",
                "db_ok": False,
                "message": "failed to build status snapshot",
                "time": iso_now(),
                "_status_request_id": request_id,
            }
        with self._lock:
            self._publish_status_snapshot_locked(payload)

    def _publish_status_snapshot_locked(self, payload: dict[str, object] | None = None) -> None:
        """Put the latest status snapshot into the child status queue."""
        if self._status_queue is None:
            return
        if payload is None:
            payload = self._build_status_payload("")
        _put_latest(self._status_queue, payload)

    def _build_status_payload(self, request_id: str = "") -> dict[str, object]:
        """Combine service, storage, and HTTP manager snapshots for /status."""
        service_runtime = self.service.get_runtime_snapshot()
        try:
            db_ok = self.service.is_database_healthy()
        except Exception:
            LOGGER.exception("database health probe failed")
            db_ok = False
        database_snapshot = self.service.get_status_snapshot() if db_ok else None
        database_snapshot = database_snapshot or {}
        payload = {
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
            "_status_request_id": request_id,
        }
        return payload

    def _record_start_failure(self, exc: BaseException) -> None:
        """Record a child-process startup failure."""
        reason = f"start failed: {type(exc).__name__}: {exc}"
        with self._lock:
            self._lifecycle.record_failure(reason)
            self._process = None
            _close_connection(self._parent_sentinel_writer)
            self._parent_sentinel_writer = None
        LOGGER.error(
            "failed to start HTTP server child process",
            exc_info=(type(exc), exc, exc.__traceback__),
        )

    def _process_alive_locked(self) -> bool:
        """Return whether the tracked child process is alive."""
        return _process_is_alive(self._process)


class _ChildHTTPContext:
    """HTTP routing context that exists only inside the child process."""

    def __init__(
        self,
        config: AppConfig,
        settings: _ChildSettings,
        ingress_queue: Any,
        status_queue: Any,
        status_request_queue: Any,
        server_port: int,
        parent_pid: int,
        parent_lost_event: threading.Event,
    ):
        """Save child-only HTTP routing dependencies."""
        self.config = config
        self.settings = settings
        self.ingress_queue = ingress_queue
        self.status_queue = status_queue
        self.status_request_queue = status_request_queue
        self.server_port = server_port
        self.process_pid = os.getpid()
        self.parent_pid = parent_pid
        self.parent_lost_event = parent_lost_event
        self.stats = _RuntimeStats()
        self.image_rate_limiter = ImageRateLimiter(
            settings.image_rate_limit_per_minute,
            settings.image_rate_limit_burst,
        )
        self.image_root = Path(config.db_path).parent / "images"

    def build_asgi_app(self) -> Starlette:
        """Create the Starlette app served by the child Uvicorn process."""
        app = Starlette(
            routes=[
                Route("/", self._handle_get_request, methods=["GET"]),
                Route("/{path:path}", self._handle_get_request, methods=["GET"]),
                Route("/{path:path}", self._handle_hikvision_event, methods=["POST", "PUT"]),
            ]
        )
        app.add_middleware(_RequestLifecycleMiddleware, owner=self)
        return app

    def get_runtime_snapshot(self, parent_http: dict[str, object] | None = None) -> dict[str, object]:
        """Merge parent manager state with child request metrics."""
        parent_http = copy.deepcopy(parent_http or {})
        parent_lifecycle = parent_http.get("lifecycle")
        lifecycle = parent_lifecycle if isinstance(parent_lifecycle, dict) else {}
        lifecycle.update(
            {
                "thread_alive": True,
                "process_alive": True,
                "process_pid": self.process_pid,
                "parent_pid": self.parent_pid,
                "parent_alive": not self.parent_lost_event.is_set(),
                "orphaned": self.parent_lost_event.is_set(),
                "server_port": self.server_port,
            }
        )
        snapshot = self.stats.snapshot(lifecycle)
        parent_http.update(snapshot)
        return parent_http

    def _begin_request(self) -> int:
        """Record a child HTTP request start."""
        return self.stats.begin_request()

    def _finish_request(self) -> None:
        """Record a child HTTP request finish."""
        self.stats.finish_request()

    def _record_busy_response(self) -> None:
        """Record one child-side 503 Busy response."""
        self.stats.record_busy_response()

    def _record_request_exception(self, context: str, request: Request, exc: BaseException) -> None:
        """Record a child request exception summary."""
        summary = (
            f"{context} client={_client_ip(request)} method={request.method} "
            f"path={request.url.path} {type(exc).__name__}: {exc}"
        )
        self.stats.record_request_exception(summary)

    async def _handle_get_request(self, request: Request) -> Response:
        """Handle GET routes for root, status, and public images."""
        path = request.url.path
        if path == "/":
            return _text_response(200, _ROOT_LIVENESS_BODY)
        if path == "/status":
            return await self._handle_status_request()
        if self.is_image_request(path):
            return await self._handle_image_request(request, path)
        return _text_response(404, "Not Found")

    async def _handle_hikvision_event(self, request: Request) -> Response:
        """Validate and enqueue a Hikvision HTTP report into IPC."""
        if request.url.path != self.config.listen_path:
            return _text_response(404, "Not Found")

        length = _content_length(request)
        if length is None:
            return _text_response(400, "Missing Content-Length")
        if length < 0:
            return _text_response(400, "Invalid Content-Length")
        if length > self.settings.max_request_bytes:
            request.state.request_body_length = length
            return _text_response(413, "Payload Too Large")

        try:
            body = await asyncio.wait_for(
                request.body(),
                timeout=self.settings.request_read_timeout_seconds,
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
        request_id = getattr(request.state, "request_id", "-")
        LOGGER.debug(
            "HTTP body accepted method=%s path=%s client=%s bytes=%s content_type=%s",
            request.method,
            request.url.path,
            client_ip,
            length,
            content_type or "-",
        )
        try:
            self.ingress_queue.put_nowait(
                _IngressIPCRequest(content_type, body, client_ip, request_id)
            )
        except queue.Full:
            self._record_busy_response()
            return _text_response(503, "Busy")
        except Exception:
            LOGGER.exception("failed to enqueue HTTP request into IPC")
            self._record_busy_response()
            return _text_response(503, "Busy")
        return _text_response(200, "OK")

    async def _handle_status_request(self) -> Response:
        """Return the latest parent status snapshot plus child HTTP metrics."""
        request_id = str(_next_request_id())
        try:
            self.status_request_queue.put_nowait(request_id)
        except queue.Full:
            LOGGER.warning("status request queue is full")
            return _json_response(
                503,
                {
                    "status": "error",
                    "db_ok": False,
                    "message": "status request queue is full",
                    "time": iso_now(),
                    "http_server": self.get_runtime_snapshot(),
                },
            )
        except Exception:
            LOGGER.exception("failed to request parent status snapshot")
            return _json_response(
                503,
                {
                    "status": "error",
                    "db_ok": False,
                    "message": "parent status snapshot unavailable",
                    "time": iso_now(),
                    "http_server": self.get_runtime_snapshot(),
                },
            )

        payload = await asyncio.to_thread(self._wait_for_parent_status, request_id)
        if payload is None:
            return _json_response(
                503,
                {
                    "status": "error",
                    "db_ok": False,
                    "message": "parent status snapshot unavailable",
                    "time": iso_now(),
                    "http_server": self.get_runtime_snapshot(),
                },
            )

        payload.pop("_status_request_id", None)
        parent_http = payload.get("http_server")
        payload["http_server"] = self.get_runtime_snapshot(parent_http if isinstance(parent_http, dict) else None)
        status_code = 200 if payload.get("db_ok") is True else 503
        LOGGER.debug("status snapshot returned status=%s db_ok=%s", payload.get("status"), payload.get("db_ok"))
        return _json_response(status_code, payload)

    def _wait_for_parent_status(self, request_id: str) -> dict[str, object] | None:
        """Wait for a status snapshot that matches this request id."""
        deadline = time.monotonic() + _STATUS_REQUEST_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                payload = self.status_queue.get(timeout=0.05)
            except queue.Empty:
                continue
            except (OSError, EOFError):
                return None
            if not isinstance(payload, dict):
                continue
            if str(payload.get("_status_request_id") or "") == request_id:
                return payload
        return None

    async def _handle_image_request(self, request: Request, path: str) -> Response:
        """Return a locally saved event image without opening SQLite."""
        image_name = self.image_name_from_path(path)
        if image_name is None:
            return _text_response(404, "Not Found")

        client_ip = _client_ip(request)
        if not self.image_rate_limiter.allow(client_ip):
            LOGGER.warning("image rate limit exceeded for %s: %s", client_ip, path)
            return _text_response(429, "Too Many Requests")

        image_path = await asyncio.to_thread(_resolve_public_image_path, self.image_root, image_name)
        if image_path is None:
            LOGGER.debug("image not found client=%s name=%s", client_ip, image_name)
            return _text_response(404, "Not Found")

        try:
            data = await asyncio.to_thread(image_path.read_bytes)
        except OSError:
            LOGGER.exception("failed to read image file: %s", image_path)
            return _text_response(404, "Not Found")

        content_type = mimetypes.guess_type(image_path.name)[0] or "application/octet-stream"
        LOGGER.debug("image served client=%s name=%s bytes=%s", client_ip, image_name, len(data))
        return _bytes_response(200, data, content_type)

    def is_image_request(self, path: str) -> bool:
        """Return whether the path targets the configured public image prefix."""
        prefix = self.config.external_image_path
        if not prefix:
            return False
        return path.startswith(f"{prefix}/")

    def image_name_from_path(self, path: str) -> str | None:
        """Extract a safe image file name from a public image path."""
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
    """Provide request boundaries, protocol limits, metrics, and access logs."""

    def __init__(self, app: Any, owner: _ChildHTTPContext):
        """Save the child context used for metrics and limits."""
        super().__init__(app)
        self.owner = owner

    async def dispatch(self, request: Request, call_next: Callable[[Request], Any]) -> Response:
        """Handle one HTTP request lifecycle."""
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
        """Reject abnormal paths or oversized headers before route handling."""
        settings = self.owner.settings
        if len(str(request.url.path)) > settings.max_request_path_chars:
            return _text_response(414, "URI Too Long")

        header_items = list(request.headers.raw)
        if len(header_items) > settings.max_header_count:
            return _text_response(431, "Too Many Request Headers")

        header_bytes = sum(len(name) + len(value) + 4 for name, value in header_items)
        if header_bytes > settings.max_header_bytes:
            return _text_response(431, "Request Header Fields Too Large")
        return None

    def _log_request_summary(self, request: Request, response: Response, started_at: float) -> None:
        """Log the compact request summary used for diagnostics."""
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
        """Return whether a successful lightweight probe should skip summary logs."""
        return (
            request.method == "GET"
            and request.url.path in {"/", "/status"}
            and response.status_code < 400
        )


@dataclass
class _RateBucket:
    """Token bucket state for one client IP."""

    tokens: float
    updated_at: float
    last_seen: float


class ImageRateLimiter:
    """In-memory per-IP token bucket for public image access."""

    def __init__(self, per_minute: int, burst: int):
        """Initialize token capacity and refill rate."""
        self.per_minute = max(1, int(per_minute))
        self.burst = max(1, int(burst))
        self.refill_per_second = self.per_minute / 60.0
        self._buckets: dict[str, _RateBucket] = {}
        self._lock = threading.Lock()
        self._cleanup_counter = 0

    def allow(self, key: str) -> bool:
        """Return whether the current request may pass."""
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
        """Remove stale client buckets."""
        stale_keys = [
            key
            for key, bucket in self._buckets.items()
            if now - bucket.last_seen >= _RATE_LIMIT_STALE_SECONDS
        ]
        for key in stale_keys:
            self._buckets.pop(key, None)


def _run_uvicorn_child(
    config: AppConfig,
    settings: _ChildSettings,
    ingress_queue: Any,
    status_queue: Any,
    status_request_queue: Any,
    ready_queue: Any,
    log_queue: Any,
    shutdown_event: Any,
    parent_sentinel: Any,
) -> None:
    """Child-process target that builds and runs the Starlette/Uvicorn app."""
    _configure_child_logging(log_queue)
    ready_sent = False
    listen_socket: socket.socket | None = None
    server: uvicorn.Server | None = None
    parent_pid = os.getppid()
    parent_lost_event = threading.Event()
    server_finished_event = threading.Event()
    try:
        listen_socket = _bind_listen_socket(
            _LISTEN_HOST,
            int(config.listen_port),
            settings.http_request_queue_size,
        )
        server_port = int(listen_socket.getsockname()[1])
        context = _ChildHTTPContext(
            config,
            settings,
            ingress_queue,
            status_queue,
            status_request_queue,
            server_port,
            parent_pid,
            parent_lost_event,
        )
        uvicorn_config = uvicorn.Config(
            context.build_asgi_app(),
            host=_LISTEN_HOST,
            port=server_port,
            log_config=None,
            access_log=False,
            server_header=False,
            log_level="warning",
            lifespan="off",
            limit_concurrency=settings.http_max_connections + 1,
            backlog=settings.http_request_queue_size,
            timeout_keep_alive=settings.request_read_timeout_seconds,
            timeout_graceful_shutdown=_SERVER_STOP_GRACE_SECONDS,
        )
        server = uvicorn.Server(uvicorn_config)
        _start_shutdown_watcher(server, shutdown_event)
        _start_parent_sentinel_watcher(server, parent_sentinel, parent_lost_event, server_finished_event)
        _start_ready_watcher(server, ready_queue, server_port)
        server.run(sockets=[listen_socket])
        ready_sent = True
    except BaseException as exc:
        LOGGER.error(
            "HTTP server child process failed",
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        if not ready_sent:
            _put_ready_error(ready_queue, exc)
    finally:
        server_finished_event.set()
        if listen_socket is not None:
            try:
                listen_socket.close()
            except OSError:
                pass
        _close_connection(parent_sentinel)


def _configure_child_logging(log_queue: Any) -> None:
    """Send child-process logs back to the parent logging tree."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.addHandler(QueueHandler(log_queue))
    root.setLevel(logging.DEBUG)
    for name in {"uvicorn", "uvicorn.error", "uvicorn.access"}:
        logging.getLogger(name).setLevel(logging.WARNING)


def _start_shutdown_watcher(server: uvicorn.Server, shutdown_event: Any) -> None:
    """Start a child thread that maps the IPC shutdown event to Uvicorn exit."""
    def watch() -> None:
        """Wait for parent shutdown and ask Uvicorn to exit."""
        try:
            shutdown_event.wait()
        except Exception:
            return
        server.should_exit = True

    thread = threading.Thread(target=watch, name="uvicorn-child-shutdown", daemon=True)
    thread.start()


def _start_parent_sentinel_watcher(
    server: uvicorn.Server,
    parent_sentinel: Any,
    parent_lost_event: threading.Event,
    server_finished_event: threading.Event,
    grace_seconds: float = _ORPHAN_EXIT_GRACE_SECONDS,
    force_exit: Callable[[int], None] | None = None,
) -> threading.Thread:
    """Exit the child if its parent process disappears unexpectedly."""
    force_exit = force_exit or _force_exit_process

    def watch() -> None:
        """Block on the parent pipe and stop Uvicorn when it closes."""
        if parent_sentinel is None:
            return
        try:
            parent_sentinel.recv_bytes()
        except (EOFError, OSError):
            pass
        except Exception:
            LOGGER.debug("parent sentinel watcher failed", exc_info=True)
            return
        parent_lost_event.set()
        server.should_exit = True
        if not server_finished_event.wait(timeout=max(0.0, grace_seconds)):
            LOGGER.warning("HTTP child parent disappeared; forcing child process exit")
            force_exit(0)

    thread = threading.Thread(target=watch, name="uvicorn-child-parent-sentinel", daemon=True)
    thread.start()
    return thread


def _start_ready_watcher(server: uvicorn.Server, ready_queue: Any, server_port: int) -> None:
    """Notify the parent once Uvicorn reports that it has started."""
    def watch() -> None:
        """Poll Uvicorn's started flag and publish one ready message."""
        deadline = time.monotonic() + _SERVER_START_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if server.started:
                _put_latest(
                    ready_queue,
                    {"status": "started", "port": server_port, "pid": os.getpid()},
                )
                return
            if server.should_exit:
                return
            time.sleep(0.02)

    thread = threading.Thread(target=watch, name="uvicorn-child-ready", daemon=True)
    thread.start()


def _put_ready_error(ready_queue: Any, exc: BaseException) -> None:
    """Send a child startup error to the parent if possible."""
    _put_latest(
        ready_queue,
        {
            "status": "error",
            "type": type(exc).__name__,
            "error": str(exc),
            "pid": os.getpid(),
        },
    )


def _settings_from_constants() -> _ChildSettings:
    """Capture module constants so tests can monkeypatch them before spawn."""
    return _ChildSettings(
        max_header_count=int(_MAX_HEADER_COUNT),
        max_header_bytes=int(_MAX_HEADER_BYTES),
        max_request_path_chars=int(_MAX_REQUEST_PATH_CHARS),
        max_request_bytes=int(_MAX_REQUEST_BYTES),
        request_read_timeout_seconds=float(_REQUEST_READ_TIMEOUT_SECONDS),
        http_max_connections=int(_HTTP_MAX_CONNECTIONS),
        http_request_queue_size=int(_HTTP_REQUEST_QUEUE_SIZE),
        image_rate_limit_per_minute=int(_IMAGE_RATE_LIMIT_PER_MINUTE),
        image_rate_limit_burst=int(_IMAGE_RATE_LIMIT_BURST),
    )


def _build_control_snapshot(
    lifecycle: dict[str, object],
    health_failure_count: int,
    health_probe_error: str,
    health_probe_at: str,
) -> dict[str, object]:
    """Convert process lifecycle and health data into GUI-ready controls."""
    state = str(lifecycle.get("state") or "stopped")
    process_alive = bool(lifecycle.get("process_alive", lifecycle.get("thread_alive", False)))
    failure_reason = str(lifecycle.get("last_failure_reason") or "")
    failure_at = str(lifecycle.get("last_failed_at") or "")
    health_probe_error = str(health_probe_error or "")
    health_probe_at = str(health_probe_at or "")

    display_state = state
    display_text = "未运行"
    severity = "idle"
    detail = failure_reason
    detail_at = failure_at
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
            display_text = "子进程未运行/未响应"
            severity = "error"
            detail = health_probe_error or "HTTP server 子进程未运行"
            detail_at = health_probe_at
        elif health_failure_count > 0 and health_probe_error:
            display_state = "degraded"
            display_text = "响应异常"
            severity = "warning"
            detail = f"连续失败 {health_failure_count} 次：{health_probe_error}"
            detail_at = health_probe_at
    elif state == "restarting":
        display_state = "restarting"
        display_text = "重启中"
        severity = "busy"
        detail = failure_reason or health_probe_error
        detail_at = failure_at or health_probe_at
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
    elif state == "stopped":
        display_text = "未运行"
        severity = "idle"
    else:
        display_state = "stopped"

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


def _bind_listen_socket(host: str, port: int, backlog: int) -> socket.socket:
    """Synchronously bind a listening socket inside the child process."""
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


def _probe_http_root(port: int) -> tuple[bool, str]:
    """Return whether the root liveness endpoint is reachable."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(f"http://{_PROBE_HOST}:{port}/", method="GET")
    try:
        with opener.open(request, timeout=_HEALTH_CHECK_TIMEOUT_SECONDS) as response:
            response.read(1)
            if response.status == 200:
                return True, ""
            return False, f"HTTP {response.status}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _probe_http_status(port: int) -> tuple[bool, str]:
    """Return whether /status is reachable without treating db errors as child failure."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(f"http://{_PROBE_HOST}:{port}/status", method="GET")
    try:
        with opener.open(request, timeout=_HEALTH_CHECK_TIMEOUT_SECONDS) as response:
            data = response.read(4096)
            return _looks_like_status_payload(data), "" if data else "empty status response"
    except urllib.error.HTTPError as exc:
        data = exc.read(4096)
        if _looks_like_status_payload(data):
            return True, ""
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _looks_like_status_payload(data: bytes) -> bool:
    """Return whether bytes look like this server's /status JSON payload."""
    try:
        payload = json.loads(data.decode("utf-8"))
    except Exception:
        return False
    return isinstance(payload, dict) and ("db_ok" in payload or "http_server" in payload)


def _inspect_bridge_port_owner(port: int) -> _BridgePortOwner | None:
    """Return confirmed BDZC HTTP process identity for a bound local port."""
    root_body = _fetch_http_root_body(port)
    if root_body != _ROOT_LIVENESS_BODY:
        return None

    payload = _fetch_http_status_payload(port)
    if payload is None:
        return _BridgePortOwner(None, False, "bridge root responded but /status was unavailable")

    http_server = payload.get("http_server")
    if not isinstance(http_server, dict):
        return _BridgePortOwner(None, False, "bridge status payload did not include http_server")
    lifecycle = http_server.get("lifecycle")
    lifecycle = lifecycle if isinstance(lifecycle, dict) else {}
    pid = _coerce_int(lifecycle.get("process_pid") or http_server.get("process_pid"))
    parent_pid = _coerce_int(lifecycle.get("parent_pid"))
    parent_alive = lifecycle.get("parent_alive")
    orphaned = lifecycle.get("orphaned") is True or parent_alive is False
    message = str(payload.get("message") or "")
    if message == "parent status snapshot unavailable" and parent_alive is not True:
        orphaned = True
    if parent_pid is not None and parent_pid != os.getpid() and not _pid_exists(parent_pid):
        orphaned = True

    detail_parts = [f"message={message or '-'}"]
    if parent_pid is not None:
        detail_parts.append(f"parent_pid={parent_pid}")
    if parent_alive is not None:
        detail_parts.append(f"parent_alive={parent_alive}")
    return _BridgePortOwner(pid, orphaned, " ".join(detail_parts))


def _fetch_http_root_body(port: int) -> str | None:
    """Fetch the root liveness body from a candidate port."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(f"http://{_PROBE_HOST}:{port}/", method="GET")
    try:
        with opener.open(request, timeout=_ORPHAN_STATUS_PROBE_TIMEOUT_SECONDS) as response:
            data = response.read(256)
            if response.status != 200:
                return None
            return data.decode("utf-8", errors="replace")
    except Exception:
        return None


def _fetch_http_status_payload(port: int) -> dict[str, object] | None:
    """Fetch and parse /status from a candidate bridge process."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(f"http://{_PROBE_HOST}:{port}/status", method="GET")
    try:
        with opener.open(request, timeout=_ORPHAN_STATUS_PROBE_TIMEOUT_SECONDS) as response:
            data = response.read(8192)
    except urllib.error.HTTPError as exc:
        data = exc.read(8192)
    except Exception:
        return None
    try:
        payload = json.loads(data.decode("utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _terminate_process_id(pid: int) -> bool:
    """Terminate one process by pid and report whether it appears to exit."""
    if pid <= 0 or pid == os.getpid():
        return False
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return not _pid_exists(pid)
    deadline = time.monotonic() + _ORPHAN_TERMINATE_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(0.05)
    sigkill = getattr(signal, "SIGKILL", None)
    if sigkill is not None:
        try:
            os.kill(pid, sigkill)
        except OSError:
            return not _pid_exists(pid)
    return not _pid_exists(pid)


def _wait_for_port_release(port: int, timeout_seconds: float) -> bool:
    """Wait until binding the configured HTTP port succeeds."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _port_can_bind(port):
            return True
        time.sleep(0.05)
    return _port_can_bind(port)


def _port_can_bind(port: int) -> bool:
    """Return whether the HTTP listen port is available on all interfaces."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((_LISTEN_HOST, int(port)))
        return True
    except OSError:
        return False


def _pid_exists(pid: int) -> bool:
    """Return whether a process id appears to exist."""
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    if os.name == "nt":
        return _windows_pid_exists(pid)
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _windows_pid_exists(pid: int) -> bool:
    """Return whether a Windows process exists without sending it a signal."""
    try:
        import ctypes
    except Exception:
        return False
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    process_query_limited_information = 0x1000
    handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
    if handle:
        kernel32.CloseHandle(handle)
        return True
    error_access_denied = 5
    return ctypes.get_last_error() == error_access_denied


def _coerce_int(value: object) -> int | None:
    """Convert a JSON value to int when possible."""
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _format_pid_suffix(pid: int | None) -> str:
    """Return a readable pid suffix for errors."""
    return f" pid={pid}" if pid is not None else ""


def _resolve_public_image_path(image_root: Path, image_name: str) -> Path | None:
    """Resolve a public image by file name without opening SQLite."""
    text = str(image_name or "").strip()
    if not text:
        return None
    if Path(text).name != text or "/" in text or "\\" in text:
        return None
    candidates = [image_root / text]
    if image_root.exists():
        for child in image_root.iterdir():
            if child.is_dir():
                candidates.append(child / text)
    for candidate in candidates:
        try:
            resolved = candidate.resolve(strict=False)
            root = image_root.resolve(strict=False)
        except OSError:
            continue
        try:
            resolved.relative_to(root)
        except ValueError:
            continue
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def _content_length(request: Request) -> int | None:
    """Read and validate Content-Length."""
    header_value = request.headers.get("content-length")
    if header_value is None:
        return None
    try:
        return int(header_value)
    except ValueError:
        return -1


def _text_response(status_code: int, body: str) -> PlainTextResponse:
    """Create a UTF-8 plain text response."""
    return PlainTextResponse(body, status_code=status_code)


def _json_response(status_code: int, payload: dict[str, object]) -> JSONResponse:
    """Create a UTF-8 JSON response."""
    return JSONResponse(payload, status_code=status_code)


def _bytes_response(status_code: int, body: bytes, content_type: str) -> Response:
    """Create a binary response."""
    return Response(body, status_code=status_code, media_type=content_type)


def _client_ip(request: Request) -> str:
    """Return the current request client IP."""
    return request.client.host if request.client is not None else "unknown"


def _next_request_id() -> int:
    """Generate a process-local monotonically increasing request id."""
    global _REQUEST_COUNTER
    with _REQUEST_COUNTER_LOCK:
        _REQUEST_COUNTER += 1
        return _REQUEST_COUNTER


def _safe_queue_size(queue_obj: Any) -> int | None:
    """Return a best-effort queue size without failing on platform limits."""
    if queue_obj is None:
        return None
    try:
        return int(queue_obj.qsize())
    except Exception:
        return None


def _queue_empty(queue_obj: Any) -> bool:
    """Return a best-effort queue empty flag."""
    if queue_obj is None:
        return True
    try:
        return bool(queue_obj.empty())
    except Exception:
        return True


def _drain_queue(queue_obj: Any) -> None:
    """Remove all currently available queue items."""
    if queue_obj is None:
        return
    while True:
        try:
            queue_obj.get_nowait()
        except queue.Empty:
            return
        except (OSError, EOFError):
            return


def _put_latest(queue_obj: Any, item: object) -> None:
    """Put an item into a bounded queue, dropping stale items if necessary."""
    if queue_obj is None:
        return
    for _ in range(3):
        try:
            queue_obj.put_nowait(item)
            return
        except queue.Full:
            try:
                queue_obj.get_nowait()
            except queue.Empty:
                pass
        except (OSError, EOFError):
            return


def _process_is_alive(process: Any) -> bool:
    """Return a safe liveness check for multiprocessing Process-like objects."""
    if process is None:
        return False
    try:
        return bool(process.is_alive())
    except Exception:
        return False


def _join_process(process: Any, timeout: float | None) -> None:
    """Join a process-like object without leaking exceptions into lifecycle state."""
    try:
        process.join(timeout=timeout)
    except Exception:
        pass


def _close_queue(queue_obj: Any) -> None:
    """Close a multiprocessing queue if it supports close operations."""
    if queue_obj is None:
        return
    try:
        queue_obj.close()
    except Exception:
        pass
    try:
        queue_obj.join_thread()
    except Exception:
        pass


def _close_connection(connection: Any) -> None:
    """Close a multiprocessing connection-like object if present."""
    if connection is None:
        return
    close = getattr(connection, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _join_and_close_process(process: Any, timeout: float) -> None:
    """Join and close a multiprocessing Process-like object."""
    _join_process(process, timeout)
    close = getattr(process, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _force_exit_process(code: int) -> None:
    """Exit the current process immediately when graceful Uvicorn shutdown stalls."""
    os._exit(code)
