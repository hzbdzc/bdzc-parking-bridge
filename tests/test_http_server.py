"""HTTP server process, probe, image, and limit tests."""

from __future__ import annotations

import http.client
import json
import logging
import socket
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

import bdzc_parking.http_server as http_server_module
from bdzc_parking.config import AppConfig
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.models import SendResult
from bdzc_parking.service import ParkingBridgeService
from bdzc_parking.storage import EventStore
from helpers import free_tcp_port, wait_until


class FakeClient:
    """Minimal partner client used by HTTP server integration tests."""

    def __init__(self, config: AppConfig):
        self.config = config

    def send_once(self, payload: dict[str, object], attempt: int = 1) -> SendResult:
        """Return a successful partner response."""
        return SendResult(True, attempt, 200, '{"status":200,"msg":"ok"}')


def test_livez_returns_http_only_health_and_root_is_404(tmp_path: Path) -> None:
    """The explicit /livez probe replaces the old root liveness response."""
    with _bridge_server(tmp_path) as server:
        with _open_url(_url(server, "/livez")) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert response.status == 200
            assert payload["status"] == "ok"
            assert payload["http_ok"] is True
            assert isinstance(payload["process_pid"], int)

        with pytest_raises_http_error(404):
            _open_url(_url(server, "/"))


def test_status_returns_business_health_and_worker_snapshot(tmp_path: Path) -> None:
    """The /status endpoint should merge HTTP, service, and database state."""
    with _bridge_server(tmp_path) as server:
        with _open_url(_url(server, "/status")) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert response.status == 200
            assert payload["status"] == "ok"
            assert payload["db_ok"] is True
            assert payload["queues"]["service"] == 0
            assert payload["workers"]["service_total"] == 3
            assert payload["workers"]["service_idle"] == 3
            assert payload["http_server"]["lifecycle"]["state"] == "running"
            assert payload["http_server"]["lifecycle"]["process_alive"] is True


def test_status_returns_503_when_database_probe_fails(tmp_path: Path) -> None:
    """Business health failures should make /status return 503 without breaking /livez."""
    with _bridge_server(tmp_path) as server:
        server.service.store.probe_database_health = lambda: False

        with pytest.raises(urllib.error.HTTPError) as exc_info:
            _open_url(_url(server, "/status"))
        payload = json.loads(exc_info.value.read().decode("utf-8"))
        assert exc_info.value.code == 503
        assert payload["status"] == "error"
        assert payload["db_ok"] is False

        with _open_url(_url(server, "/livez")) as response:
            assert response.status == 200


def test_image_route_serves_file_and_rejects_traversal(tmp_path: Path) -> None:
    """Configured public image paths should serve safe file names only."""
    with _bridge_server(tmp_path) as server:
        image_path = server.service.store.image_dir / "20260412" / "sample.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(b"sample-image")

        with _open_url(_url(server, "/parking-images/sample.jpg")) as response:
            assert response.status == 200
            assert response.read() == b"sample-image"
            assert response.headers["Content-Type"] == "image/jpeg"

        with pytest_raises_http_error(404):
            _open_url(_url(server, "/parking-images/%2e%2e%2fsample.jpg"))


def test_image_route_rate_limits_same_ip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Image requests from one IP should be limited by the child token bucket."""
    monkeypatch.setattr(http_server_module, "_IMAGE_RATE_LIMIT_BURST", 2)
    with _bridge_server(tmp_path) as server:
        image_path = server.service.store.image_dir / "20260412" / "sample.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(b"sample-image")
        image_url = _url(server, "/parking-images/sample.jpg")

        with _open_url(image_url) as response:
            assert response.status == 200
        with _open_url(image_url) as response:
            assert response.status == 200
        with pytest_raises_http_error(429):
            _open_url(image_url)


def test_post_request_is_enqueued_and_returns_before_business_finishes(tmp_path: Path) -> None:
    """The child should return after IPC enqueue, while service workers process later."""
    with _bridge_server(tmp_path) as server:
        called = False

        def slow_enqueue(content_type, body, client_ip="unknown", request_id="-", block=False, timeout=None):
            nonlocal called
            called = True
            return True

        server.service.enqueue_http_request = slow_enqueue
        assert _post_park(_server_port(server), b"{}") == 200
        assert wait_until(lambda: called)


def test_post_returns_503_when_parent_rejects_ingress(tmp_path: Path) -> None:
    """A child POST should not return 200 unless the parent accepts the task."""
    with _bridge_server(tmp_path) as server:
        server.service.enqueue_http_request = lambda *args, **kwargs: False

        assert _post_park(_server_port(server), b"{}") == 503


def test_post_returns_503_and_parent_ingress_survives_exception(
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """Parent ingress exceptions should be logged and should not kill the thread."""
    with _bridge_server(tmp_path) as server:
        calls = 0

        def flaky_enqueue(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("forced enqueue failure")
            return True

        server.service.enqueue_http_request = flaky_enqueue
        caplog.set_level(logging.WARNING)

        assert _post_park(_server_port(server), b"{}") == 503
        assert _post_park(_server_port(server), b"{}") == 200
        assert "failed to forward inbound HTTP request" in caplog.text


def test_post_returns_503_when_parent_ack_times_out(tmp_path: Path) -> None:
    """If the parent does not answer the ack pipe, the child must fail the POST."""
    with _bridge_server(tmp_path) as server:
        server._parent_stop_event.set()
        assert wait_until(lambda: server._ingress_thread is not None and not server._ingress_thread.is_alive())

        assert _post_park(_server_port(server), b"{}") == 503


def test_status_parent_payload_exception_returns_error_and_loop_survives(
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
) -> None:
    """A parent-side status exception should become a 503 payload, not a dead thread."""
    with _bridge_server(tmp_path) as server:
        original_snapshot = server.service.get_runtime_snapshot
        calls = 0

        def flaky_snapshot():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("forced status failure")
            return original_snapshot()

        server.service.get_runtime_snapshot = flaky_snapshot
        caplog.set_level(logging.ERROR)

        with pytest.raises(urllib.error.HTTPError) as exc_info:
            _open_url(_url(server, "/status"))
        payload = json.loads(exc_info.value.read().decode("utf-8"))
        assert exc_info.value.code == 503
        assert payload["status"] == "error"
        assert "parent status failed" in payload["message"]
        assert "failed to build HTTP status payload" in caplog.text

        with _open_url(_url(server, "/status")) as response:
            assert response.status == 200


def test_request_limits_reject_bad_payloads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The child should reject missing length, oversized body, long path, and large headers."""
    monkeypatch.setattr(http_server_module, "_MAX_REQUEST_BYTES", 4)
    with _bridge_server(tmp_path) as server:
        port = _server_port(server)
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            connection.putrequest("POST", "/park")
            connection.putheader("Content-Type", "application/json")
            connection.endheaders()
            response = connection.getresponse()
            assert response.status == 400
        finally:
            connection.close()

        assert _post_park(port, b"12345") == 413
        long_path = "/" + ("x" * (http_server_module._MAX_REQUEST_PATH_CHARS + 1))
        assert _raw_http_status(port, f"GET {long_path} HTTP/1.1\r\nHost: x\r\n\r\n") == 414
        large_header = "x" * (http_server_module._MAX_HEADER_BYTES + 1)
        assert _raw_http_status(port, f"GET /livez HTTP/1.1\r\nHost: x\r\nX-Large: {large_header}\r\n\r\n") == 431


def test_server_can_stop_and_start_same_port(tmp_path: Path) -> None:
    """A normal stop should release the configured port for the next start."""
    port = free_tcp_port()
    manager = _bridge_server(tmp_path, listen_port=port)
    try:
        manager.server.start()
        assert _livez_ok(port)
        manager.server.stop()
        assert manager.server.is_running is False
        manager.server.start()
        assert _server_port(manager.server) == port
        assert _livez_ok(port)
    finally:
        manager.server.stop()
        manager.service.close()


def test_health_guard_restarts_dead_child(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The health guard should immediately replace a dead child process."""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class DeadProcess:
        """Process-like object that is already dead."""

        pid = 111

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None:
            return None

        def close(self) -> None:
            return None

    class AliveProcess:
        """Process-like replacement object."""

        pid = 222

        def is_alive(self) -> bool:
            return True

        def join(self, timeout: float | None = None) -> None:
            return None

        def close(self) -> None:
            return None

    replacement_event = _FakeShutdownEvent()
    old_event = _FakeShutdownEvent()
    monkeypatch.setattr(manager.server, "_start_child", lambda: (AliveProcess(), 1888, 222, replacement_event))
    try:
        with manager.server._lock:
            manager.server._process = DeadProcess()
            manager.server._shutdown_event = old_event
            manager.server._lifecycle.desired_running = True
            manager.server._lifecycle.mark_running(1888, 111)

        manager.server._health_check_once()
        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["restart_count"] == 1
        assert snapshot["process_pid"] == 222
        assert snapshot["state"] == "running"
        assert manager.server._shutdown_event is replacement_event
        assert old_event.set_count == 0
    finally:
        manager.server.stop()
        manager.service.close()


def test_livez_failures_restart_child(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Two consecutive /livez probe failures should trigger a child restart."""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class AliveProcess:
        """Process-like object used for health restart tests."""

        def __init__(self, pid: int):
            self.pid = pid
            self.alive = True
            self.kill_count = 0

        def is_alive(self) -> bool:
            return self.alive

        def join(self, timeout: float | None = None) -> None:
            return None

        def kill(self) -> None:
            self.kill_count += 1
            self.alive = False

        def close(self) -> None:
            return None

    replacement = AliveProcess(333)
    replacement_event = _FakeShutdownEvent()
    old_event = _FakeShutdownEvent()
    monkeypatch.setattr(manager.server, "_start_child", lambda: (replacement, 1888, 333, replacement_event))
    try:
        with manager.server._lock:
            manager.server._process = AliveProcess(111)
            manager.server._shutdown_event = old_event
            manager.server._lifecycle.desired_running = True
            manager.server._lifecycle.mark_running(1888, 111)
        monkeypatch.setattr(http_server_module, "_probe_livez", lambda port: (False, "forced"))

        manager.server._health_check_once()
        assert manager.server.get_lifecycle_snapshot()["restart_count"] == 0
        manager.server._health_check_once()

        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["restart_count"] == 1
        assert snapshot["process_pid"] == 333
        assert snapshot["state"] == "running"
        assert manager.server._shutdown_event is replacement_event
        assert old_event.set_count == 1
    finally:
        manager.server.stop()
        manager.service.close()


def test_request_child_stop_skips_dead_process_event(tmp_path: Path) -> None:
    """A dead child should not touch its stale shutdown event."""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class DeadProcess:
        """Process-like object that is already stopped."""

        def is_alive(self) -> bool:
            return False

    event = _FakeShutdownEvent()
    try:
        manager.server._request_child_stop(DeadProcess(), event)
        assert event.set_count == 0
    finally:
        manager.service.close()


def test_request_child_stop_does_not_block_on_stuck_event(tmp_path: Path) -> None:
    """A broken multiprocessing Event.set call must not block GUI shutdown."""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class AliveProcess:
        """Process-like object that is still running."""

        def is_alive(self) -> bool:
            return True

    class BlockingEvent:
        """Event-like object whose set call blocks until the test releases it."""

        def __init__(self) -> None:
            self.entered = threading.Event()
            self.release = threading.Event()

        def set(self) -> None:
            self.entered.set()
            self.release.wait(timeout=5)

    event = BlockingEvent()
    started_at = time.monotonic()
    try:
        manager.server._request_child_stop(AliveProcess(), event)
        elapsed = time.monotonic() - started_at
        assert event.entered.wait(timeout=1)
        assert elapsed < 1.0
    finally:
        event.release.set()
        manager.service.close()


def test_parent_sentinel_force_exits_when_parent_ends(monkeypatch: pytest.MonkeyPatch) -> None:
    """The child should force-exit quickly after the parent sentinel fires."""

    class FakeServer:
        """Server-like object exposing Uvicorn's should_exit flag."""

        should_exit = False

    class FakeParent:
        """Parent-process test double whose sentinel has already fired."""

        def join(self) -> None:
            """Return immediately like a closed multiprocessing parent sentinel."""

    server = FakeServer()
    parent = FakeParent()
    exit_codes: list[int] = []
    monkeypatch.setattr(http_server_module.multiprocessing, "parent_process", lambda: parent)

    started_at = time.monotonic()
    http_server_module._start_parent_sentinel_watcher(server, 999999, force_exit=exit_codes.append)

    assert wait_until(lambda: exit_codes == [0], timeout_seconds=0.2)
    assert time.monotonic() - started_at < 0.2
    assert server.should_exit is False


def test_parent_sentinel_ignores_already_exiting_server(monkeypatch: pytest.MonkeyPatch) -> None:
    """The sentinel should not force-exit after Uvicorn is already exiting."""

    class FakeServer:
        """Server-like object exposing Uvicorn's should_exit flag."""

        should_exit = True

    class FakeParent:
        """Parent-process test double whose sentinel has already fired."""

        def __init__(self) -> None:
            self.joined = threading.Event()

        def join(self) -> None:
            """Record that the sentinel watcher waited on this parent."""
            self.joined.set()

    parent = FakeParent()
    exit_codes: list[int] = []
    monkeypatch.setattr(http_server_module.multiprocessing, "parent_process", lambda: parent)

    http_server_module._start_parent_sentinel_watcher(FakeServer(), 12345, force_exit=exit_codes.append)

    assert parent.joined.wait(timeout=0.2)
    assert exit_codes == []


def test_parent_sentinel_does_nothing_without_multiprocessing_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sentinel should quietly skip setup when multiprocessing exposes no parent."""

    class FakeServer:
        """Server-like object exposing Uvicorn's should_exit flag."""

        should_exit = False

    monkeypatch.setattr(http_server_module.multiprocessing, "parent_process", lambda: None)
    exit_codes: list[int] = []

    http_server_module._start_parent_sentinel_watcher(FakeServer(), 12345, force_exit=exit_codes.append)
    time.sleep(0.05)

    assert exit_codes == []


def test_stop_child_process_kills_unresponsive_process(tmp_path: Path) -> None:
    """Unresponsive child cleanup should call kill after the one-second grace."""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class FakeProcess:
        """Process-like object that ignores graceful shutdown."""

        pid = 12345

        def __init__(self) -> None:
            self.alive = True
            self.joins: list[float | None] = []
            self.kill_count = 0
            self.close_count = 0

        def is_alive(self) -> bool:
            return self.alive

        def join(self, timeout: float | None = None) -> None:
            self.joins.append(timeout)

        def kill(self) -> None:
            self.kill_count += 1
            self.alive = False

        def close(self) -> None:
            self.close_count += 1

    process = FakeProcess()
    try:
        manager.server._stop_child_process(process)
        assert process.joins[0] == http_server_module._SERVER_STOP_GRACE_SECONDS
        assert process.kill_count == 1
        assert process.close_count == 1
    finally:
        manager.service.close()


class _bridge_server:
    """Test helper owning a BridgeHTTPServer and its service."""

    def __init__(self, tmp_path: Path, **config_overrides: object):
        config_values = {
            "listen_port": 0,
            "external_url_base": "https://public.example.com/parking-images",
            "db_path": tmp_path / "events.sqlite3",
        }
        config_values.update(config_overrides)
        config = AppConfig(**config_values)
        self.store = EventStore(config.db_path)
        self.service = ParkingBridgeService(config, self.store, FakeClient(config))
        self.server = BridgeHTTPServer(config, self.service)

    def __enter__(self) -> BridgeHTTPServer:
        self.server.start()
        return self.server

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.stop()
        self.service.close()


class _FakeShutdownEvent:
    """Event-like test double that records graceful shutdown signals."""

    def __init__(self) -> None:
        self.set_count = 0

    def set(self) -> None:
        self.set_count += 1


def _open_url(url: str):
    """Open one URL without using system proxy settings."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(url, timeout=5)


def _url(server: BridgeHTTPServer, path: str) -> str:
    """Build a local URL for a running bridge server."""
    return f"http://127.0.0.1:{_server_port(server)}{path}"


def _server_port(server: BridgeHTTPServer) -> int:
    """Return the active server port from lifecycle snapshot."""
    port = server.get_lifecycle_snapshot()["server_port"]
    assert isinstance(port, int)
    return port


def _post_park(port: int, body: bytes) -> int:
    """POST JSON bytes to /park and return HTTP status."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request(
            "POST",
            "/park",
            body=body,
            headers={"Content-Type": "application/json", "Content-Length": str(len(body))},
        )
        response = connection.getresponse()
        response.read()
        return int(response.status)
    finally:
        connection.close()


def _raw_http_status(port: int, request_text: str) -> int:
    """Send a raw HTTP request and parse the status code."""
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(request_text.encode("ascii"))
        data = sock.recv(256)
    first_line = data.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
    return int(first_line.split()[1])


def _livez_ok(port: int) -> bool:
    """Return whether /livez is reachable."""
    try:
        with _open_url(f"http://127.0.0.1:{port}/livez") as response:
            return response.status == 200
    except Exception:
        return False


class pytest_raises_http_error:
    """Small context manager asserting urllib HTTPError code."""

    def __init__(self, status_code: int):
        self.status_code = status_code

    def __enter__(self):
        """Return this assertion helper."""
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        """Validate the raised urllib error."""
        if exc_type is None:
            raise AssertionError(f"expected HTTPError {self.status_code}")
        if not issubclass(exc_type, urllib.error.HTTPError):
            return False
        if exc.code != self.status_code:
            raise AssertionError(f"expected HTTP {self.status_code}, got {exc.code}")
        return True
