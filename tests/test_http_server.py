"""HTTP server process, status, admin task, image, and limit tests."""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

import bdzc_parking.http_server as http_server_module
from bdzc_parking.config import AppConfig
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.service import ParkingBridgeService, PartnerClient
from bdzc_parking.storage import EventStore
from helpers import HIKVISION_CONTENT_TYPE, free_tcp_port, sample_body, sample_event, wait_until


class FakePartnerHandler(BaseHTTPRequestHandler):
    """Local partner API used by HTTP child integration tests."""

    calls = 0
    request_bodies: list[bytes] = []

    def do_POST(self) -> None:
        """Record one request and return a partner success response."""
        type(self).calls += 1
        length = int(self.headers.get("Content-Length", "0") or "0")
        type(self).request_bodies.append(self.rfile.read(length))
        body = b'{"status":200,"msg":"ok"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        """Suppress default test server access logs."""
        return


def test_status_returns_business_health_and_livez_is_removed(tmp_path: Path) -> None:
    """The child-local /status endpoint should be the only health probe."""
    with _bridge_server(tmp_path) as manager:
        with _open_url(_url(manager.server, "/status")) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert response.status == 200
            assert payload["status"] == "ok"
            assert payload["db_ok"] is True
            assert payload["queues"]["service"] == 0
            assert payload["workers"]["service_total"] == 3
            assert payload["http_server"]["lifecycle"]["process_alive"] is True
            assert payload["database"]["total_size_bytes"] >= 0
            assert "events" in payload

        with pytest_raises_http_error(404):
            _open_url(_url(manager.server, "/livez"))


def test_status_returns_503_when_database_probe_fails(tmp_path: Path) -> None:
    """Status payload construction should mark failed database probes unhealthy."""
    config = AppConfig(listen_port=free_tcp_port(), db_path=tmp_path / "events.sqlite3")
    store = EventStore(config.db_path)
    service = ParkingBridgeService(config, store, PartnerClient(config))
    app = http_server_module._ChildHTTPApp(
        config,
        http_server_module._settings_from_constants(),
        store,
        service,
        config.listen_port,
        12345,
    )
    try:
        service.get_database_health = lambda: {
            "ok": False,
            "kind": "error",
            "message": "database failed",
        }
        response = app._handle_status()
        assert response.status_code == 503
        payload = json.loads(response.body.decode("utf-8"))
        assert payload["status"] == "error"
        assert payload["db_ok"] is False
        assert payload["db_error_kind"] == "error"
    finally:
        service.close()


def test_probe_status_ignores_database_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """父进程健康探针遇到数据库短暂超时时，不应要求重启 HTTP 子进程。"""
    monkeypatch.setattr(
        http_server_module,
        "_request_json",
        lambda *_args, **_kwargs: (
            503,
            {"status": "error", "db_ok": False, "db_error_kind": "timeout"},
        ),
    )

    ok, error, payload = http_server_module._probe_status(1888)

    assert ok is True
    assert "timeout" in error
    assert payload["db_error_kind"] == "timeout"


def test_probe_status_fails_on_non_timeout_503(monkeypatch: pytest.MonkeyPatch) -> None:
    """非 timeout 的 /status 503 仍应触发健康守护失败计数。"""
    monkeypatch.setattr(
        http_server_module,
        "_request_json",
        lambda *_args, **_kwargs: (
            503,
            {"status": "error", "db_ok": False, "db_error_kind": "error"},
        ),
    )

    ok, error, payload = http_server_module._probe_status(1888)

    assert ok is False
    assert error == "HTTP 503"
    assert payload["db_error_kind"] == "error"


def test_image_route_serves_file_and_rejects_traversal(tmp_path: Path) -> None:
    """Configured public image paths should serve safe file names only."""
    with _bridge_server(tmp_path) as manager:
        image_path = manager.store.image_dir / "20260412" / "sample.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(b"sample-image")

        with _open_url(_url(manager.server, "/parking-images/sample.jpg")) as response:
            assert response.status == 200
            assert response.read() == b"sample-image"
            assert response.headers["Content-Type"] == "image/jpeg"

        with pytest_raises_http_error(404):
            _open_url(_url(manager.server, "/parking-images/%2e%2e%2fsample.jpg"))


def test_image_route_rate_limits_same_ip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Image requests from one IP should be limited by the child token bucket."""
    monkeypatch.setattr(http_server_module, "_IMAGE_RATE_LIMIT_BURST", 2)
    with _bridge_server(tmp_path) as manager:
        image_path = manager.store.image_dir / "20260412" / "sample.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(b"sample-image")
        image_url = _url(manager.server, "/parking-images/sample.jpg")

        with _open_url(image_url) as response:
            assert response.status == 200
        with _open_url(image_url) as response:
            assert response.status == 200
        with pytest_raises_http_error(429):
            _open_url(image_url)


def test_post_request_is_processed_inside_http_child(tmp_path: Path) -> None:
    """POST ingress should be queued directly in the child service and persisted."""
    with _bridge_server(tmp_path) as manager:
        assert _post_park(_server_port(manager.server), b"{not-json}") == 200
        assert wait_until(lambda: bool(manager.store.list_events()))

        row = manager.store.list_events()[0]
        assert row["status"] == "parse_error"
        assert "payload is not valid JSON" in row["last_error"]


def test_request_limits_reject_bad_payloads(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The child should reject missing length, oversized body, long path, and large headers."""
    monkeypatch.setattr(http_server_module, "_MAX_REQUEST_BYTES", 4)
    with _bridge_server(tmp_path) as manager:
        port = _server_port(manager.server)
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
        assert _raw_http_status(port, f"GET /status HTTP/1.1\r\nHost: x\r\nX-Large: {large_header}\r\n\r\n") == 431


def test_server_can_stop_and_start_same_port(tmp_path: Path) -> None:
    """A normal stop should release the configured port for the next start."""
    port = free_tcp_port()
    manager = _bridge_server(tmp_path, listen_port=port)
    try:
        manager.server.start()
        assert _status_ok(port)
        manager.server.stop()
        assert manager.server.is_running is False
        manager.server.start()
        assert _server_port(manager.server) == port
        assert _status_ok(port)
    finally:
        manager.server.stop()


def test_admin_resend_returns_task_and_final_result(tmp_path: Path) -> None:
    """Manual resend should be submitted through admin task API and reach a final state."""
    with _fake_partner_server() as partner_url:
        with _bridge_server(
            tmp_path,
            partner_api_url=partner_url,
            max_event_age_seconds=0.0,
        ) as manager:
            body = sample_body("20260412_063354_226439_body.bin")
            assert _post_park(_server_port(manager.server), body, HIKVISION_CONTENT_TYPE) == 200
            assert wait_until(lambda: bool(manager.store.list_events()))
            event_id = int(manager.store.list_events()[0]["id"])

            accepted = manager.server.submit_resend(event_id)
            assert accepted["status"] == "accepted"
            assert accepted["task_id"]

            task = _wait_admin_task(manager.server, str(accepted["task_id"]))
            assert task["status"] == "succeeded"
            assert wait_until(lambda: FakePartnerHandler.calls == 1)


def test_admin_resend_rebuilds_missing_partner_payload(tmp_path: Path) -> None:
    """HTTP 子进程运行时，旧记录只要能生成 payload 就应允许手动重发。"""
    with _fake_partner_server() as partner_url:
        with _bridge_server(tmp_path, partner_api_url=partner_url) as manager:
            event = sample_event("20260412_063354_226439_body.bin")
            event_id, _ = manager.store.add_event(event, "skipped", False)
            row = manager.store.get_event(event_id)
            assert row is not None
            assert row["partner_payload_json"] == "{}"

            accepted = manager.server.submit_resend(event_id)
            assert accepted["status"] == "accepted"

            task = _wait_admin_task(manager.server, str(accepted["task_id"]))
            assert task["status"] == "succeeded"
            assert wait_until(lambda: FakePartnerHandler.calls == 1)
            sent = manager.store.get_event(event_id)
            assert sent is not None
            assert sent["status"] == "sent"


def test_admin_cleanup_returns_task_and_final_result(tmp_path: Path) -> None:
    """Manual cleanup should be submitted through admin task API and finish in the child."""
    with _bridge_server(tmp_path) as manager:
        accepted = manager.server.submit_cleanup("test")
        assert accepted["status"] == "accepted"

        task = _wait_admin_task(manager.server, str(accepted["task_id"]))
        assert task["status"] == "succeeded"
        assert "cleanup finished" in str(task["message"])


def test_health_guard_restarts_dead_child(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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

    monkeypatch.setattr(manager.server, "_start_child", lambda: AliveProcess())
    stop_ports: list[int | None] = []
    monkeypatch.setattr(manager.server, "_request_child_stop", stop_ports.append)
    try:
        with manager.server._lock:
            manager.server._process = DeadProcess()
            manager.server._lifecycle.desired_running = True
            manager.server._lifecycle.mark_running(1888, 111)

        assert manager.server._health_check_once() is True
        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["restart_count"] == 1
        assert snapshot["process_pid"] == 222
        assert snapshot["state"] == "running"
        assert stop_ports == []
    finally:
        manager.server.stop()


def test_status_failures_restart_child(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Two consecutive /status probe failures should trigger a child restart."""
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
    monkeypatch.setattr(manager.server, "_start_child", lambda: replacement)
    stop_ports: list[int | None] = []
    monkeypatch.setattr(manager.server, "_request_child_stop", stop_ports.append)
    monkeypatch.setattr(http_server_module, "_probe_status", lambda port: (False, "forced", {}))
    try:
        with manager.server._lock:
            manager.server._process = AliveProcess(111)
            manager.server._lifecycle.desired_running = True
            manager.server._lifecycle.mark_running(1888, 111)

        assert manager.server._health_check_once() is False
        assert manager.server.get_lifecycle_snapshot()["restart_count"] == 0
        assert manager.server._health_check_once() is True

        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["restart_count"] == 1
        assert snapshot["process_pid"] == 333
        assert snapshot["state"] == "running"
        assert stop_ports == [1888]
    finally:
        manager.server.stop()


def test_process_liveness_check_skips_status_probe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The one-second process liveness check should not call /status."""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class AliveProcess:
        """Process-like object that stays alive."""

        pid = 111

        def is_alive(self) -> bool:
            return True

    status_calls: list[int] = []
    monkeypatch.setattr(
        http_server_module,
        "_probe_status",
        lambda port: status_calls.append(port) or (True, "", {"status": "ok", "db_ok": True}),
    )
    try:
        with manager.server._lock:
            manager.server._process = AliveProcess()
            manager.server._lifecycle.desired_running = True
            manager.server._lifecycle.mark_running(1888, 111)

        assert manager.server._health_check_once(probe_status=False) is False

        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["restart_count"] == 0
        assert status_calls == []
    finally:
        manager.server.stop()


def test_start_child_cleans_started_process_when_ready_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A child that starts but never becomes ready should be stopped before re-raising."""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class FakeProcess:
        """Process-like object that stays alive until killed."""

        pid = 444

        def __init__(self) -> None:
            self.started = False
            self.alive = True
            self.joins: list[float | None] = []
            self.kill_count = 0
            self.close_count = 0

        def start(self) -> None:
            self.started = True

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
    stop_ports: list[int | None] = []
    monkeypatch.setattr(
        manager.server,
        "_mp_context",
        SimpleNamespace(Process=lambda **_kwargs: process),
    )
    monkeypatch.setattr(
        manager.server,
        "_wait_child_ready",
        lambda _process, _port: (_ for _ in ()).throw(TimeoutError("ready failed")),
    )
    monkeypatch.setattr(manager.server, "_request_child_stop", stop_ports.append)

    with pytest.raises(TimeoutError):
        manager.server._start_child()

    assert process.started is True
    assert stop_ports == [manager.config.listen_port]
    assert process.kill_count == 1
    assert process.close_count == 1


def test_restart_child_discards_replacement_if_stop_was_requested(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A health restart must not publish a replacement after the user requested stop."""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class FakeProcess:
        """Process-like object used for restart race tests."""

        def __init__(self, pid: int):
            self.pid = pid
            self.alive = True
            self.kill_count = 0
            self.close_count = 0

        def is_alive(self) -> bool:
            return self.alive

        def join(self, timeout: float | None = None) -> None:
            return None

        def kill(self) -> None:
            self.kill_count += 1
            self.alive = False

        def close(self) -> None:
            self.close_count += 1

    old_process = FakeProcess(111)
    replacement = FakeProcess(222)
    stop_ports: list[int | None] = []

    def fake_start_child() -> FakeProcess:
        with manager.server._lock:
            manager.server._lifecycle.desired_running = False
        return replacement

    monkeypatch.setattr(manager.server, "_start_child", fake_start_child)
    monkeypatch.setattr(manager.server, "_request_child_stop", stop_ports.append)
    with manager.server._lock:
        manager.server._process = old_process
        manager.server._lifecycle.desired_running = True
        manager.server._lifecycle.mark_running(manager.config.listen_port, old_process.pid)

    manager.server._restart_child("test stop race")

    snapshot = manager.server.get_lifecycle_snapshot()
    assert snapshot["state"] != "running"
    assert snapshot["restart_count"] == 0
    assert snapshot["process_pid"] != replacement.pid
    assert replacement.kill_count == 1
    assert replacement.close_count == 1
    assert manager.server._process is None
    assert stop_ports == [manager.config.listen_port, manager.config.listen_port]


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
    manager.server._stop_child_process(process)
    assert process.joins[0] == http_server_module._SERVER_STOP_GRACE_SECONDS
    assert process.kill_count == 1
    assert process.close_count == 1


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


class _bridge_server:
    """Test helper owning a BridgeHTTPServer and parent-side store reader."""

    def __init__(self, tmp_path: Path, **config_overrides: object):
        config_values = {
            "listen_port": free_tcp_port(),
            "external_url_base": "https://public.example.com/parking-images",
            "db_path": tmp_path / "events.sqlite3",
            "log_path": tmp_path / "bdzc_parking.log",
            "request_timeout_seconds": 0.5,
        }
        config_values.update(config_overrides)
        self.config = AppConfig(**config_values)
        self.store = EventStore(self.config.db_path)
        self.server = BridgeHTTPServer(self.config)

    def __enter__(self) -> "_bridge_server":
        self.server.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.stop()


class _fake_partner_server:
    """Context manager for a local successful partner endpoint."""

    def __init__(self):
        self.server: ThreadingHTTPServer | None = None
        self.thread: threading.Thread | None = None

    def __enter__(self) -> str:
        FakePartnerHandler.calls = 0
        FakePartnerHandler.request_bodies = []
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FakePartnerHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return f"http://127.0.0.1:{self.server.server_port}/api"

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        if self.thread is not None:
            self.thread.join(timeout=5)


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


def _post_park(port: int, body: bytes, content_type: str = "application/json") -> int:
    """POST bytes to /park and return HTTP status."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        connection.request(
            "POST",
            "/park",
            body=body,
            headers={"Content-Type": content_type, "Content-Length": str(len(body))},
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


def _status_ok(port: int) -> bool:
    """Return whether /status reports a healthy child."""
    try:
        with _open_url(f"http://127.0.0.1:{port}/status") as response:
            payload = json.loads(response.read().decode("utf-8"))
            return response.status == 200 and payload.get("status") == "ok"
    except Exception:
        return False


def _wait_admin_task(server: BridgeHTTPServer, task_id: str) -> dict[str, object]:
    """Wait for one admin task to reach a terminal state."""
    result: dict[str, object] = {}

    def finished() -> bool:
        nonlocal result
        result = server.get_admin_task(task_id)
        return str(result.get("status") or "") not in {"queued", "running"}

    assert wait_until(finished, timeout_seconds=5.0, interval_seconds=0.05)
    return result


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
