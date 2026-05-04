"""HTTP server 图片访问与限流测试。"""

from __future__ import annotations

import http.client
import json
import logging
import socket
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

from bdzc_parking.config import AppConfig
from bdzc_parking.app import setup_logging
import bdzc_parking.http_server as http_server_module
import bdzc_parking.service as service_module
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.models import SendResult
from bdzc_parking.service import ParkingBridgeService
from bdzc_parking.storage import EventStore
from helpers import free_tcp_port, wait_until


class FakeClient:
    """测试用大园区客户端，占位即可。"""

    def __init__(self, config: AppConfig):
        self.config = config

    def send_once(self, payload: dict[str, object], attempt: int = 1) -> SendResult:
        return SendResult(True, attempt, 200, '{"status":200,"msg":"ok"}')


def test_image_route_serves_saved_file(tmp_path: Path) -> None:
    """启用 external_url_base 后，应能从对应路径访问本地图片。"""
    with _bridge_server(tmp_path) as server:
        image_path = server.service.store.image_dir / "20260412" / "sample.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(b"sample-image")

        with _open_url(_url(server, "/parking-images/sample.jpg")) as response:
            assert response.status == 200
            assert response.read() == b"sample-image"
            assert response.headers["Content-Type"] == "image/jpeg"


def test_image_route_rejects_path_traversal(tmp_path: Path) -> None:
    """图片访问路径不应允许目录穿越。"""
    with _bridge_server(tmp_path) as server:
        image_path = server.service.store.image_dir / "20260412" / "sample.jpg"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(b"sample-image")

        with pytest_raises_http_error(404):
            _open_url(_url(server, "/parking-images/%2e%2e%2fsample.jpg"))


def test_image_route_rate_limits_same_ip(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """同一 IP 在短时间内连续请求图片，应命中令牌桶限流。"""
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


def test_http_server_rejects_oversized_payload(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """海康上报请求体超过上限时，应直接返回 413。"""
    monkeypatch.setattr(http_server_module, "_MAX_REQUEST_BYTES", 4)

    with _bridge_server(tmp_path) as server:
        connection = http.client.HTTPConnection("127.0.0.1", _server_port(server), timeout=5)
        try:
            connection.request(
                "POST",
                "/park",
                body=b"12345",
                headers={"Content-Type": "application/json", "Content-Length": "5"},
            )
            response = connection.getresponse()
            assert response.status == 413
        finally:
            connection.close()


def test_root_route_still_returns_plain_text(tmp_path: Path, caplog) -> None:
    """GET / 仍应保持原有纯文本存活响应。"""
    with _bridge_server(tmp_path) as server:
        caplog.set_level(logging.DEBUG, logger="bdzc_parking.http_server")
        caplog.clear()
        with _open_url(_url(server, "/")) as response:
            assert response.status == 200
            assert response.read().decode("utf-8") == "BDZC Parking Bridge is running"
            assert response.headers["Content-Type"] == "text/plain; charset=utf-8"
            assert response.headers["Connection"].lower() == "close"
        assert "HTTP request request_id=" not in caplog.text

        caplog.clear()
        with pytest_raises_http_error(404):
            _open_url(_url(server, "/missing"))
        assert "HTTP request request_id=" in caplog.text
        assert "path=/missing status=404" in caplog.text


def test_healthz_route_is_removed(tmp_path: Path) -> None:
    """GET /healthz 不再是独立健康检查路由。"""
    with _bridge_server(tmp_path) as server:
        with pytest_raises_http_error(404):
            _open_url(_url(server, "/healthz"))


def test_status_returns_503_when_database_probe_fails(tmp_path: Path) -> None:
    """GET /status 应在数据库探针失败时返回 503 JSON。"""
    with _bridge_server(tmp_path) as server:
        server.service.store.probe_database_health = lambda: False

        with pytest.raises(urllib.error.HTTPError) as exc_info:
            _open_url(_url(server, "/healthz"))
        assert exc_info.value.code == 404

        with pytest.raises(urllib.error.HTTPError) as exc_info:
            _open_url(_url(server, "/status"))
        error = exc_info.value
        payload = json.loads(error.read().decode("utf-8"))
        assert error.code == 503
        assert payload["status"] == "error"
        assert payload["db_ok"] is False
        assert payload["queues"]["send"] == 0
        assert payload["workers"]["http_ingress_alive"] == service_module._HTTP_INGRESS_WORKER_COUNT
        assert payload["events"]["failure_backlog"] is None
        assert payload["database"]["main_size_bytes"] is None
        assert payload["http_server"]["lifecycle"]["state"] == "running"


def test_status_times_out_when_store_lock_is_busy(tmp_path: Path) -> None:
    """数据库锁被业务占用时，/status 应快速返回 503 而不是一直等待。"""
    with _bridge_server(tmp_path) as server:
        started_at = time.monotonic()
        with server.service.store._lock:
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                _open_url(_url(server, "/status"))
            payload = json.loads(exc_info.value.read().decode("utf-8"))
            assert exc_info.value.code == 503
            assert payload["status"] == "error"
            assert payload["db_ok"] is False
        assert time.monotonic() - started_at < 2.5


def test_status_returns_failure_backlog_and_database_size(tmp_path: Path) -> None:
    """GET /status 应返回精简健康、队列、失败堆积和数据库大小。"""
    with _bridge_server(tmp_path) as server:
        now = datetime.now().isoformat(timespec="seconds")
        future = "2999-01-01T00:00:00"
        with server.service.store._connect() as conn:
            conn.execute(
                """
                INSERT INTO events (event_key, received_at, updated_at, status)
                VALUES (?, ?, ?, ?)
                """,
                ("sent-event", now, now, "sent"),
            )
            conn.execute(
                """
                INSERT INTO events (event_key, received_at, updated_at, status, next_retry_at, partner_payload_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("retry-event", now, now, "failed_retryable", future, '{"car":"浙A00001"}'),
            )
            conn.execute(
                """
                INSERT INTO events (event_key, received_at, updated_at, status, dead_lettered_at, partner_payload_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("dead-event", now, now, "dead_letter", now, '{"car":"浙A00002"}'),
            )

        with _open_url(_url(server, "/status")) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert response.status == 200
            assert payload["status"] == "ok"
            assert payload["db_ok"] is True
            assert payload["queues"]["send"] == 0
            assert payload["queues"]["http_ingress"] == 0
            assert payload["queues"]["http_ingress_active"] == 0
            assert payload["queues"]["http_ingress_rejected"] == 0
            assert payload["workers"]["http_ingress_alive"] == service_module._HTTP_INGRESS_WORKER_COUNT
            assert payload["workers"]["http_ingress_total"] == service_module._HTTP_INGRESS_WORKER_COUNT
            assert payload["events"]["last_success_sent_at"] == now
            assert payload["events"]["failed_retryable"] == 1
            assert payload["events"]["dead_letter"] == 1
            assert payload["events"]["failure_backlog"] == 2
            assert payload["database"]["main_size_bytes"] > 0
            assert payload["database"]["total_size_bytes"] >= payload["database"]["main_size_bytes"]
            assert payload["http_server"]["active_requests"] >= 1
            assert "max_connections" not in payload["http_server"]
            assert "total_requests" not in payload["http_server"]
            assert "request_queue_size" not in payload["http_server"]
            assert payload["http_server"]["lifecycle"]["state"] == "running"
            assert payload["http_server"]["lifecycle"]["desired_running"] is True
            assert payload["http_server"]["lifecycle"]["thread_alive"] is True
            assert payload["http_server"]["lifecycle"]["last_failure_reason"] == ""


def test_status_success_does_not_write_request_summary(tmp_path: Path, caplog) -> None:
    """成功 GET /status 不应刷单次请求摘要日志。"""
    caplog.set_level(logging.DEBUG, logger="bdzc_parking.http_server")
    with _bridge_server(tmp_path) as server:
        caplog.clear()
        with _open_url(_url(server, "/status")) as response:
            assert response.status == 200
            response.read()
        assert "HTTP request request_id=" not in caplog.text
        assert "status snapshot returned status=ok db_ok=True" in caplog.text


def test_setup_logging_adds_file_handler_when_root_already_has_handler(tmp_path: Path) -> None:
    """root 已有 handler 时仍必须补上项目 RotatingFileHandler。"""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_root_level = root.level
    log_path = tmp_path / "app.log"
    try:
        for handler in original_handlers:
            root.removeHandler(handler)
        existing_handler = logging.NullHandler()
        root.addHandler(existing_handler)

        setup_logging(log_path)

        assert any(
            isinstance(handler, RotatingFileHandler)
            and Path(handler.baseFilename).resolve() == log_path.resolve()
            for handler in root.handlers
        )
        assert root.level == logging.DEBUG
        file_handlers = [
            handler
            for handler in root.handlers
            if isinstance(handler, RotatingFileHandler)
            and Path(handler.baseFilename).resolve() == log_path.resolve()
        ]
        console_handlers = [
            handler for handler in root.handlers if type(handler) is logging.StreamHandler
        ]
        assert file_handlers
        assert all(handler.level == logging.DEBUG for handler in file_handlers)
        assert console_handlers
        assert all(handler.level == logging.INFO for handler in console_handlers)
        logging.getLogger("bdzc_parking.tests").info("probe after setup")
        logging.getLogger("bdzc_parking.tests").debug("debug probe after setup")
        for handler in root.handlers:
            if hasattr(handler, "flush"):
                handler.flush()
        text = log_path.read_text(encoding="utf-8")
        assert "logging initialized" in text
        assert "probe after setup" in text
        assert "debug probe after setup" in text
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            if handler not in original_handlers:
                handler.close()
        for handler in original_handlers:
            root.addHandler(handler)
        root.setLevel(original_root_level)


def test_http_server_can_restart_on_same_configured_port(tmp_path: Path) -> None:
    """停止 HTTP server 后，同一个配置端口应能再次启动并响应状态检查。"""
    port = free_tcp_port()
    manager = _bridge_server(tmp_path, listen_port=port)
    try:
        manager.server.start()
        with _open_url(_url(manager.server, "/status")) as response:
            assert response.status == 200

        manager.server.stop()
        assert manager.server.is_running is False

        manager.server.start()
        assert manager.server.get_lifecycle_snapshot()["state"] == "running"
        assert _server_port(manager.server) == port
        with _open_url(_url(manager.server, "/status")) as response:
            assert response.status == 200
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_lifecycle_starts_and_stops_without_failure(tmp_path: Path) -> None:
    """手动停止 HTTP server 应进入 stopped，不应留下故障状态。"""
    manager = _bridge_server(tmp_path)
    try:
        manager.server.start()
        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["state"] == "running"
        assert snapshot["desired_running"] is True
        assert snapshot["thread_alive"] is True

        manager.server.stop()
        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["state"] == "stopped"
        assert snapshot["desired_running"] is False
        assert snapshot["thread_alive"] is False
        assert snapshot["last_failure_reason"] == ""
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_start_fails_when_port_is_already_bound(tmp_path: Path) -> None:
    """监听端口被占用时，start 应同步抛出并记录 failed 状态。"""
    port = free_tcp_port()
    occupied = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    occupied.bind(("127.0.0.1", port))
    occupied.listen(1)
    manager = _bridge_server(tmp_path, listen_port=port)
    try:
        with pytest.raises(OSError):
            manager.server.start()
        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["state"] == "failed"
        assert "start failed" in snapshot["last_failure_reason"]
    finally:
        occupied.close()
        manager.server.stop()
        manager.service.close()


def test_http_thread_exception_marks_failed_and_logs(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    """Uvicorn 主循环抛错时，应记录 failed 和完整异常日志，不自动重启。"""
    caplog.set_level(logging.ERROR, logger="bdzc_parking.http_server")

    def crash(_self, runtime):
        """模拟 Uvicorn 主循环启动后崩溃。"""
        runtime.server.started = True
        raise RuntimeError("forced uvicorn crash")

    monkeypatch.setattr(http_server_module.BridgeHTTPServer, "_run_uvicorn", crash)
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())
    try:
        try:
            manager.server.start()
        except RuntimeError:
            pass
        assert wait_until(
            lambda: manager.server.get_lifecycle_snapshot()["state"] == "failed",
            timeout_seconds=3.0,
        )
        snapshot = manager.server.get_lifecycle_snapshot()
        assert "forced uvicorn crash" in snapshot["last_failure_reason"]
        assert manager.server.is_running is False
    finally:
        manager.server.stop()
        manager.service.close()

    assert "HTTP server thread crashed" in caplog.text
    assert "forced uvicorn crash" in caplog.text


def test_http_thread_unexpected_return_marks_failed_and_closes_socket(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """主循环非用户停止却正常返回时，应标记 failed 并释放监听端口。"""
    def return_unexpectedly(_self, runtime):
        """模拟 Uvicorn 主循环无异常提前返回。"""
        runtime.server.started = True
        return None

    monkeypatch.setattr(http_server_module.BridgeHTTPServer, "_run_uvicorn", return_unexpectedly)
    port = free_tcp_port()
    manager = _bridge_server(tmp_path, listen_port=port)
    try:
        try:
            manager.server.start()
        except RuntimeError:
            pass
        assert wait_until(
            lambda: manager.server.get_lifecycle_snapshot()["state"] == "failed"
            and not _tcp_port_accepts(port),
            timeout_seconds=3.0,
        )
        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["last_failure_reason"] == "server returned unexpectedly"
    finally:
        manager.server.stop()
        manager.service.close()


def test_request_exception_returns_500_records_status_and_logs(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    """ASGI 处理函数抛错时，应返回 500、记录 /status 指标并保持 server 可用。"""
    caplog.set_level(logging.ERROR, logger="bdzc_parking.http_server")
    manager = _bridge_server(tmp_path)
    failed_once = False
    original_handler = manager.server._handle_get_request

    async def fail_once(request):
        """第一次 GET 抛出异常，之后恢复正常。"""
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise RuntimeError("forced ASGI handler failure")
        return await original_handler(request)

    monkeypatch.setattr(manager.server, "_handle_get_request", fail_once)
    try:
        manager.server.start()
        port = _server_port(manager.server)
        status, body = _raw_http_response(port, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        assert status == 500
        assert body == b"Internal Server Error"
        assert wait_until(lambda: _root_is_healthy(port), timeout_seconds=3.0)

        with _open_url(_url(manager.server, "/status")) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert payload["http_server"]["request_exception_count"] >= 1
            assert "forced ASGI handler failure" in payload["http_server"]["last_request_exception"]
    finally:
        manager.server.stop()
        manager.service.close()

    assert "HTTP request handling failed" in caplog.text
    assert "forced ASGI handler failure" in caplog.text


def test_http_server_rejects_missing_content_length(tmp_path: Path) -> None:
    """POST /park 缺少 Content-Length 时应快速返回 400。"""
    with _bridge_server(tmp_path) as server:
        connection = http.client.HTTPConnection("127.0.0.1", _server_port(server), timeout=5)
        try:
            connection.putrequest("POST", "/park")
            connection.putheader("Content-Type", "application/json")
            connection.endheaders()
            response = connection.getresponse()
            assert response.status == 400
        finally:
            connection.close()


def test_http_server_records_parse_error_for_invalid_json(tmp_path: Path) -> None:
    """非法 JSON 不应影响 HTTP server，应由业务层记录 parse_error 并返回 200。"""
    with _bridge_server(tmp_path) as server:
        connection = http.client.HTTPConnection("127.0.0.1", _server_port(server), timeout=5)
        try:
            connection.request(
                "POST",
                "/park",
                body=b"not-json",
                headers={"Content-Type": "application/json", "Content-Length": "8"},
            )
            response = connection.getresponse()
            assert response.status == 200
        finally:
            connection.close()

        assert wait_until(lambda: bool(server.service.store.list_events()))
        rows = server.service.store.list_events()
        assert rows
        assert rows[0]["status"] == "parse_error"


def test_http_server_rejects_too_long_path(tmp_path: Path) -> None:
    """请求路径超过限制时应返回 414，避免异常路径消耗资源。"""
    with _bridge_server(tmp_path) as server:
        path = "/" + ("x" * (http_server_module._MAX_REQUEST_PATH_CHARS + 1))
        status = _raw_http_status(_server_port(server), f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n")
        assert status == 414


def test_http_server_rejects_large_headers(tmp_path: Path) -> None:
    """header 总长度超过限制时应返回 431。"""
    with _bridge_server(tmp_path) as server:
        value = "x" * (http_server_module._MAX_HEADER_BYTES + 1)
        request = f"GET / HTTP/1.1\r\nHost: x\r\nX-Large: {value}\r\n\r\n"
        status = _raw_http_status(_server_port(server), request)
        assert status == 431


def test_http_server_returns_busy_when_concurrency_limit_exceeded(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """慢连接占满并发槽位时，新连接应收到 503 Busy。"""
    monkeypatch.setattr(http_server_module, "_HTTP_MAX_CONNECTIONS", 1)
    monkeypatch.setattr(http_server_module, "_REQUEST_READ_TIMEOUT_SECONDS", 2.0)
    manager = _bridge_server(tmp_path)
    try:
        manager.server.start()
        port = _server_port(manager.server)
        first = socket.create_connection(("127.0.0.1", port), timeout=5)
        try:
            first.sendall(
                b"POST /park HTTP/1.1\r\n"
                b"Host: x\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: 1\r\n"
                b"\r\n"
            )
            time.sleep(0.2)
            status = _raw_http_status(port, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
            assert status == 503
        finally:
            first.close()

        assert wait_until(lambda: _root_is_healthy(port), timeout_seconds=3.0)
    finally:
        manager.server.stop()
        manager.service.close()


def test_park_request_returns_after_ingress_enqueue(tmp_path: Path) -> None:
    """慢业务处理应由 ingress worker 执行，不占住 HTTP 请求线程。"""
    with _bridge_server(tmp_path) as server:
        called = threading.Event()
        release = threading.Event()

        def slow_handle_request(content_type: str, body: bytes, client_ip: str = "unknown") -> None:
            called.set()
            release.wait(timeout=3)

        server.service.handle_request = slow_handle_request
        started_at = time.monotonic()
        try:
            status = _post_park(_server_port(server), b"{}")
            elapsed = time.monotonic() - started_at
            assert status == 200
            assert elapsed < 0.5
            assert wait_until(called.is_set)
        finally:
            release.set()


def test_park_ingress_queue_full_returns_busy_without_blocking_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """业务接收队列满时 /park 返回 503，但 /status 仍绕开业务队列。"""
    monkeypatch.setattr(service_module, "_HTTP_INGRESS_WORKER_COUNT", 1)
    monkeypatch.setattr(service_module, "_HTTP_INGRESS_QUEUE_SIZE", 1)
    manager = _bridge_server(tmp_path)
    block_worker = threading.Event()
    worker_started = threading.Event()
    try:
        manager.server.start()

        def slow_handle_request(content_type: str, body: bytes, client_ip: str = "unknown") -> None:
            worker_started.set()
            block_worker.wait(timeout=3)

        manager.service.handle_request = slow_handle_request
        port = _server_port(manager.server)
        assert _post_park(port, b"{}") == 200
        assert wait_until(worker_started.is_set)
        assert _post_park(port, b"{}") == 200
        assert _post_park(port, b"{}") == 503

        with _open_url(_url(manager.server, "/status")) as response:
            assert response.status == 200
    finally:
        block_worker.set()
        manager.server.stop()
        manager.service.close()


def test_partial_body_disconnect_does_not_break_server(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """客户端上传半个 body 后断开时，server 后续请求仍应正常响应。"""
    monkeypatch.setattr(http_server_module, "_REQUEST_READ_TIMEOUT_SECONDS", 0.5)

    with _bridge_server(tmp_path) as server:
        port = _server_port(server)
        sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        try:
            sock.sendall(
                b"POST /park HTTP/1.1\r\n"
                b"Host: x\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: 5\r\n"
                b"\r\n"
                b"12"
            )
        finally:
            sock.close()

        assert wait_until(lambda: _root_is_healthy(port), timeout_seconds=3.0)


class _bridge_server:
    """测试用桥接 HTTP server 上下文管理器。"""

    def __init__(self, tmp_path: Path, **config_overrides: object):
        config_values = {
            "listen_port": 0,
            "external_url_base": "https://public.example.com/parking-images",
        }
        config_values.update(config_overrides)
        config = AppConfig(**config_values)
        self.store = EventStore(tmp_path / "events.sqlite3")
        self.service = ParkingBridgeService(config, self.store, FakeClient(config))
        self.server = BridgeHTTPServer(config, self.service)

    def __enter__(self) -> BridgeHTTPServer:
        self.server.start()
        return self.server

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.stop()
        self.service.close()


def _open_url(url: str):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(url, timeout=5)


def _url(server: BridgeHTTPServer, path: str) -> str:
    return f"http://127.0.0.1:{_server_port(server)}{path}"


def _server_port(server: BridgeHTTPServer) -> int:
    """Return the active bound HTTP port from the public lifecycle snapshot."""
    port = server.get_lifecycle_snapshot()["server_port"]
    assert isinstance(port, int)
    return port


def _tcp_port_accepts(port: int) -> bool:
    """判断本机 TCP 端口是否仍可建立连接。"""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.2):
            return True
    except OSError:
        return False


def _raw_http_status(port: int, request_text: str) -> int:
    """发送原始 HTTP 请求并解析状态码。"""
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(request_text.encode("ascii"))
        data = sock.recv(256)
    first_line = data.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
    parts = first_line.split()
    if len(parts) < 2:
        raise AssertionError(f"invalid HTTP response: {first_line!r}")
    return int(parts[1])


def _raw_http_response(port: int, request_text: str) -> tuple[int, bytes]:
    """发送原始 HTTP 请求并返回状态码和响应体。"""
    chunks: list[bytes] = []
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(request_text.encode("ascii"))
        while True:
            try:
                data = sock.recv(4096)
            except socket.timeout:
                break
            if not data:
                break
            chunks.append(data)
    response = b"".join(chunks)
    first_line = response.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
    parts = first_line.split()
    if len(parts) < 2:
        raise AssertionError(f"invalid HTTP response: {first_line!r}")
    body = response.split(b"\r\n\r\n", 1)[1] if b"\r\n\r\n" in response else b""
    return int(parts[1]), body


def _post_park(port: int, body: bytes) -> int:
    """向 /park 发送 JSON 测试请求并返回 HTTP 状态码。"""
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


def _root_is_healthy(port: int) -> bool:
    """判断根路由是否仍能正常响应。"""
    try:
        with _open_url(f"http://127.0.0.1:{port}/") as response:
            return response.status == 200
    except Exception:
        return False


class pytest_raises_http_error:
    """轻量封装 urllib 的 HTTPError 断言。"""

    def __init__(self, status_code: int):
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type is None:
            raise AssertionError(f"expected HTTPError {self.status_code}")
        if not issubclass(exc_type, urllib.error.HTTPError):
            return False
        if exc.code != self.status_code:
            raise AssertionError(f"expected HTTP {self.status_code}, got {exc.code}")
        return True
