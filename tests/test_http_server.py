"""HTTP server 图片访问与限流测试。"""

from __future__ import annotations

import http.client
import json
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bdzc_parking.config import AppConfig
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.models import SendResult
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
            assert payload["last_success_sent_at"] == now
            assert payload["failed_retryable_count"] == 1
            assert payload["dead_letter_count"] == 1
            assert payload["failure_backlog_count"] == 2
            assert payload["db_main_size_bytes"] > 0
            assert payload["db_total_size_bytes"] >= payload["db_main_size_bytes"]
            assert payload["http_watchdog"]["enabled"] is True
            assert payload["http_watchdog"]["failure_count"] == 0
            assert payload["http_watchdog"]["restart_count"] == 0


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


def test_http_watchdog_starts_and_stops_with_server(tmp_path: Path) -> None:
    """HTTP server 启停时，watchdog 应同步启停且手动停止后不自动拉起。"""
    manager = _bridge_server(
        tmp_path,
        http_watchdog_interval_seconds=0.05,
        http_watchdog_timeout_seconds=0.05,
    )
    try:
        manager.server.start()
        assert manager.server.get_watchdog_snapshot()["enabled"] is True

        manager.server.stop()
        assert manager.server.get_watchdog_snapshot()["enabled"] is False
        assert manager.server.is_running is False
        time.sleep(0.15)
        assert manager.server.is_running is False
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_watchdog_restarts_after_consecutive_probe_failures(tmp_path: Path) -> None:
    """连续探测失败达到阈值后，watchdog 应自动重启 HTTP server。"""
    manager = _bridge_server(
        tmp_path,
        listen_port=_free_tcp_port(),
        http_watchdog_interval_seconds=0.05,
        http_watchdog_timeout_seconds=0.05,
        http_watchdog_failure_threshold=2,
        http_watchdog_restart_cooldown_seconds=0.5,
    )
    try:
        manager.server.start()
        manager.server._probe_http_root = lambda: (False, "simulated probe failure")

        assert _wait_until(lambda: manager.server.get_watchdog_snapshot()["restart_count"] >= 1)
        with _open_url(_url(manager.server, "/")) as response:
            assert response.status == 200
        with _open_url(_url(manager.server, "/status")) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert payload["http_watchdog"]["restart_count"] >= 1
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_watchdog_restarts_when_server_thread_is_dead(tmp_path: Path) -> None:
    """接收线程异常退出但对象仍残留时，watchdog 应清理旧实例并重启。"""
    manager = _bridge_server(
        tmp_path,
        listen_port=_free_tcp_port(),
        http_watchdog_interval_seconds=0.05,
        http_watchdog_timeout_seconds=0.05,
        http_watchdog_restart_cooldown_seconds=0.5,
    )
    try:
        manager.server.start()
        stale_server = manager.server._server
        stale_thread = manager.server._thread
        assert stale_server is not None
        assert stale_thread is not None

        stale_server.shutdown()
        stale_thread.join(timeout=3)
        assert not stale_thread.is_alive()

        assert _wait_until(lambda: manager.server.get_watchdog_snapshot()["restart_count"] >= 1)
        assert manager.server.is_running is True
        assert manager.server._server is not stale_server
        with _open_url(_url(manager.server, "/healthz")) as response:
            assert response.status == 200
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_watchdog_clears_single_failure_without_restart(tmp_path: Path) -> None:
    """一次失败后恢复成功时，watchdog 应清零失败计数且不重启。"""
    manager = _bridge_server(
        tmp_path,
        http_watchdog_interval_seconds=0.05,
        http_watchdog_timeout_seconds=0.05,
        http_watchdog_failure_threshold=2,
    )
    calls = {"count": 0}

    def fake_probe() -> tuple[bool, str]:
        calls["count"] += 1
        if calls["count"] == 1:
            return False, "first probe failed"
        return True, ""

    try:
        manager.server.start()
        manager.server._probe_http_root = fake_probe

        assert _wait_until(lambda: calls["count"] >= 2)
        snapshot = manager.server.get_watchdog_snapshot()
        assert snapshot["failure_count"] == 0
        assert snapshot["restart_count"] == 0
        assert snapshot["last_probe_error"] == ""
    finally:
        manager.server.stop()
        manager.service.close()


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


def _wait_until(predicate, timeout: float = 3.0) -> bool:
    """等待异步 watchdog 测试条件满足。"""
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
