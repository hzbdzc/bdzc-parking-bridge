"""HTTP server 图片访问与限流测试。"""

from __future__ import annotations

import http.client
import json
import logging
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bdzc_parking.config import AppConfig
from bdzc_parking.app import setup_logging
import bdzc_parking.http_server as http_server_module
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.models import SendResult
from bdzc_parking.safe_logging import configure_emergency_logging, emergency_log_path, log_exception
from bdzc_parking.service import ParkingBridgeService
from bdzc_parking.storage import EventStore


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


def test_image_route_rate_limits_same_ip(tmp_path: Path) -> None:
    """同一 IP 在短时间内连续请求图片，应命中令牌桶限流。"""
    with _bridge_server(tmp_path, image_rate_limit_burst=2) as server:
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


def test_http_server_rejects_oversized_payload(tmp_path: Path) -> None:
    """海康上报请求体超过上限时，应直接返回 413。"""
    with _bridge_server(tmp_path, max_request_bytes=4) as server:
        connection = http.client.HTTPConnection("127.0.0.1", server._server.server_port, timeout=5)
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


def test_root_route_still_returns_plain_text(tmp_path: Path) -> None:
    """GET / 仍应保持原有纯文本存活响应。"""
    with _bridge_server(tmp_path) as server:
        with _open_url(_url(server, "/")) as response:
            assert response.status == 200
            assert response.read().decode("utf-8") == "BDZC Parking Bridge is running"
            assert response.headers["Content-Type"] == "text/plain; charset=utf-8"
            assert response.headers["Connection"].lower() == "close"


def test_healthz_returns_json_when_database_is_healthy(tmp_path: Path) -> None:
    """GET /healthz 应在数据库探针成功时返回 200 JSON。"""
    with _bridge_server(tmp_path) as server:
        with _open_url(_url(server, "/healthz")) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert response.status == 200
            assert payload["status"] == "ok"
            assert payload["server_running"] is True
            assert payload["db_ok"] is True


def test_healthz_returns_503_when_database_probe_fails(tmp_path: Path) -> None:
    """GET /healthz 应在数据库探针失败时返回 503 JSON。"""
    with _bridge_server(tmp_path) as server:
        server.service.store.probe_database_health = lambda: False

        with pytest_raises_http_error(503):
            _open_url(_url(server, "/healthz"))


def test_healthz_times_out_when_store_lock_is_busy(tmp_path: Path) -> None:
    """数据库锁被业务占用时，/healthz 应快速返回 503 而不是一直等待。"""
    with _bridge_server(tmp_path) as server:
        started_at = time.monotonic()
        with server.service.store._lock:
            with pytest_raises_http_error(503):
                _open_url(_url(server, "/healthz"))
        assert time.monotonic() - started_at < 2.5


def test_status_returns_failure_backlog_and_database_size(tmp_path: Path) -> None:
    """GET /status 应返回失败堆积、最近成功时间和数据库大小。"""
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
            assert payload["server_running"] is True
            assert payload["queue_length"] == 0
            assert payload["http_ingress_queue_length"] == 0
            assert payload["http_ingress_queue_size"] == server.config.http_ingress_queue_size
            assert payload["http_ingress_workers_alive"] == server.config.http_ingress_workers
            assert payload["http_ingress_active_requests"] == 0
            assert payload["http_ingress_rejected_count"] == 0
            assert payload["http_server"]["max_connections"] == server.config.http_max_connections
            assert payload["http_server"]["request_queue_size"] == server.config.http_request_queue_size
            assert payload["last_success_sent_at"] == now
            assert payload["failed_retryable_count"] == 1
            assert payload["dead_letter_count"] == 1
            assert payload["failure_backlog_count"] == 2
            assert payload["db_main_size_bytes"] > 0
            assert payload["db_total_size_bytes"] >= payload["db_main_size_bytes"]
            assert payload["http_watchdog"]["enabled"] is False
            assert payload["http_watchdog"]["failure_count"] == 0
            assert payload["http_watchdog"]["restart_count"] == 0


def test_setup_logging_adds_file_handler_when_root_already_has_handler(tmp_path: Path) -> None:
    """root 已有 handler 时仍必须补上项目 RotatingFileHandler。"""
    root = logging.getLogger()
    original_handlers = list(root.handlers)
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
        logging.getLogger("bdzc_parking.tests").info("probe after setup")
        for handler in root.handlers:
            if hasattr(handler, "flush"):
                handler.flush()
        text = log_path.read_text(encoding="utf-8")
        assert "logging initialized" in text
        assert "probe after setup" in text
    finally:
        for handler in list(root.handlers):
            root.removeHandler(handler)
            if handler not in original_handlers:
                handler.close()
        for handler in original_handlers:
            root.addHandler(handler)


def test_log_exception_writes_emergency_when_logging_handler_fails(tmp_path: Path) -> None:
    """普通 logging handler 抛错时，应急日志仍要写入原始异常。"""
    configure_emergency_logging(tmp_path / "broken.log")
    logger = logging.getLogger("bdzc_parking.tests.failing")
    original_handlers = list(logger.handlers)
    original_propagate = logger.propagate

    class FailingHandler(logging.Handler):
        """用于模拟普通 logging 输出失败的 handler。"""

        def emit(self, record: logging.LogRecord) -> None:
            """每次 emit 都抛错，模拟文件或控制台 handler 故障。"""
            raise OSError("forced logging handler failure")

    try:
        logger.handlers = [FailingHandler()]
        logger.propagate = False
        try:
            raise RuntimeError("original exception for emergency log")
        except RuntimeError:
            log_exception(logger, "failed handler path")

        emergency_text = emergency_log_path().read_text(encoding="utf-8")
        assert "forced logging handler failure" in emergency_text
        assert "original exception for emergency log" in emergency_text
    finally:
        for handler in logger.handlers:
            handler.close()
        logger.handlers = original_handlers
        logger.propagate = original_propagate


def test_http_server_can_restart_on_same_configured_port(tmp_path: Path) -> None:
    """停止 HTTP server 后，同一个配置端口应能再次启动并响应健康检查。"""
    port = _free_tcp_port()
    manager = _bridge_server(tmp_path, listen_port=port)
    try:
        manager.server.start()
        with _open_url(_url(manager.server, "/healthz")) as response:
            assert response.status == 200

        manager.server.stop()
        assert manager.server.is_running is False

        manager.server.start()
        assert manager.server._server is not None
        assert manager.server._server.server_port == port
        with _open_url(_url(manager.server, "/healthz")) as response:
            assert response.status == 200
    finally:
        manager.server.stop()
        manager.service.close()


def test_process_request_failure_returns_503_and_logs(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    """请求线程创建失败时应返回 503、落应急日志，并保持 server 可用。"""
    configure_emergency_logging(tmp_path / "http.log")
    caplog.set_level(logging.ERROR, logger="bdzc_parking.http_server")
    original_process_request = http_server_module.ThreadingHTTPServer.process_request
    failed_once = False

    def fail_once(self, request, client_address):
        """第一次模拟线程启动入口失败，之后恢复标准库行为。"""
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise RuntimeError("forced process_request failure")
        return original_process_request(self, request, client_address)

    monkeypatch.setattr(http_server_module.ThreadingHTTPServer, "process_request", fail_once)
    with _bridge_server(tmp_path) as server:
        port = server._server.server_port
        status, body = _raw_http_response(port, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        assert status == 503
        assert body == b"Service Unavailable"
        assert _wait_until(lambda: _root_is_healthy(port), timeout=3.0)

        with _open_url(_url(server, "/status")) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert payload["http_server"]["request_entry_exception_count"] >= 1
            assert "forced process_request failure" in payload["http_server"]["last_request_entry_exception"]

    emergency_text = emergency_log_path().read_text(encoding="utf-8")
    assert "forced process_request failure" in emergency_text
    assert "HTTP process_request failed before request thread" in caplog.text


def test_handler_setup_failure_returns_500_and_logs(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    """handler 初始化失败时应返回 500、落应急日志，并保持 server 可用。"""
    configure_emergency_logging(tmp_path / "http.log")
    caplog.set_level(logging.ERROR, logger="bdzc_parking.http_server")
    original_setup = http_server_module.HikRequestHandler.setup
    failed_once = False

    def fail_once(self):
        """第一次模拟 BaseHTTPRequestHandler setup 失败，之后恢复正常。"""
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise RuntimeError("forced handler setup failure")
        return original_setup(self)

    monkeypatch.setattr(http_server_module.HikRequestHandler, "setup", fail_once)
    with _bridge_server(tmp_path) as server:
        port = server._server.server_port
        status, body = _raw_http_response(port, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
        assert status == 500
        assert body == b"Internal Server Error"
        assert _wait_until(lambda: _root_is_healthy(port), timeout=3.0)

    emergency_text = emergency_log_path().read_text(encoding="utf-8")
    assert "forced handler setup failure" in emergency_text
    assert "HTTP request thread failed before handler completed" in caplog.text


def test_http_server_closes_socket_when_thread_exits_without_bridge_stop(tmp_path: Path) -> None:
    """serve_forever 提前返回时，应关闭监听 socket，避免端口假监听。"""
    manager = _bridge_server(
        tmp_path,
        listen_port=_free_tcp_port(),
        http_watchdog_interval_seconds=999.0,
    )
    try:
        manager.server.start()
        stale_server = manager.server._server
        stale_thread = manager.server._thread
        assert stale_server is not None
        assert stale_thread is not None
        port = stale_server.server_port
        assert _tcp_port_accepts(port)

        stale_server.shutdown()
        stale_thread.join(timeout=3)
        assert not stale_thread.is_alive()

        assert _wait_until(
            lambda: manager.server._server is None and not _tcp_port_accepts(port),
            timeout=3.0,
        )
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_server_rejects_missing_content_length(tmp_path: Path) -> None:
    """POST /park 缺少 Content-Length 时应快速返回 400。"""
    with _bridge_server(tmp_path) as server:
        connection = http.client.HTTPConnection("127.0.0.1", server._server.server_port, timeout=5)
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
        connection = http.client.HTTPConnection("127.0.0.1", server._server.server_port, timeout=5)
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

        assert _wait_until(lambda: bool(server.service.store.list_events()))
        rows = server.service.store.list_events()
        assert rows
        assert rows[0]["status"] == "parse_error"


def test_http_server_rejects_too_long_path(tmp_path: Path) -> None:
    """请求路径超过限制时应返回 414，避免异常路径消耗资源。"""
    with _bridge_server(tmp_path) as server:
        path = "/" + ("x" * (http_server_module._MAX_REQUEST_PATH_CHARS + 1))
        status = _raw_http_status(server._server.server_port, f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n")
        assert status == 414


def test_http_server_rejects_large_headers(tmp_path: Path) -> None:
    """header 总长度超过限制时应返回 431。"""
    with _bridge_server(tmp_path) as server:
        value = "x" * (http_server_module._MAX_HEADER_BYTES + 1)
        request = f"GET / HTTP/1.1\r\nHost: x\r\nX-Large: {value}\r\n\r\n"
        status = _raw_http_status(server._server.server_port, request)
        assert status == 431


def test_http_server_returns_busy_when_concurrency_limit_exceeded(tmp_path: Path) -> None:
    """慢连接占满并发槽位时，新连接应收到 503 Busy。"""
    manager = _bridge_server(
        tmp_path,
        http_max_connections=1,
        request_read_timeout_seconds=2.0,
    )
    try:
        manager.server.start()
        port = manager.server._server.server_port
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

        assert _wait_until(lambda: _root_is_healthy(port), timeout=3.0)
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
            status = _post_park(server._server.server_port, b"{}")
            elapsed = time.monotonic() - started_at
            assert status == 200
            assert elapsed < 0.5
            assert _wait_until(called.is_set)
        finally:
            release.set()


def test_park_ingress_queue_full_returns_busy_without_blocking_healthz(tmp_path: Path) -> None:
    """业务接收队列满时 /park 返回 503，但 /healthz 仍绕开业务队列。"""
    manager = _bridge_server(
        tmp_path,
        http_ingress_workers=1,
        http_ingress_queue_size=1,
    )
    block_worker = threading.Event()
    worker_started = threading.Event()
    try:
        manager.server.start()

        def slow_handle_request(content_type: str, body: bytes, client_ip: str = "unknown") -> None:
            worker_started.set()
            block_worker.wait(timeout=3)

        manager.service.handle_request = slow_handle_request
        port = manager.server._server.server_port
        assert _post_park(port, b"{}") == 200
        assert _wait_until(worker_started.is_set)
        assert _post_park(port, b"{}") == 200
        assert _post_park(port, b"{}") == 503

        with _open_url(_url(manager.server, "/healthz")) as response:
            assert response.status == 200
    finally:
        block_worker.set()
        manager.server.stop()
        manager.service.close()


def test_partial_body_disconnect_does_not_break_server(tmp_path: Path) -> None:
    """客户端上传半个 body 后断开时，server 后续请求仍应正常响应。"""
    with _bridge_server(tmp_path, request_read_timeout_seconds=0.5) as server:
        port = server._server.server_port
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

        assert _wait_until(lambda: _root_is_healthy(port), timeout=3.0)


class _bridge_server:
    """测试用桥接 HTTP server 上下文管理器。"""

    def __init__(self, tmp_path: Path, **config_overrides: object):
        config_values = {
            "listen_host": "127.0.0.1",
            "listen_port": 0,
            "external_url_base": "https://public.example.com/parking-images",
            "sender_worker_count": 1,
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
    return f"http://127.0.0.1:{server._server.server_port}{path}"


def _free_tcp_port() -> int:
    """向操作系统申请一个当前空闲的本地 TCP 端口。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


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


def _wait_until(predicate, timeout: float = 3.0) -> bool:
    """等待异步 HTTP/worker 测试条件满足。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


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
