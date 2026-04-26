"""HTTP server 图片访问与限流测试。"""

from __future__ import annotations

import http.client
import json
import sys
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


class _bridge_server:
    """测试用桥接 HTTP server 上下文管理器。"""

    def __init__(self, tmp_path: Path, **config_overrides: object):
        config = AppConfig(
            listen_host="127.0.0.1",
            listen_port=0,
            external_url_base="https://public.example.com/parking-images",
            sender_worker_count=1,
            **config_overrides,
        )
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
