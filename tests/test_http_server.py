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
        assert wait_until(lambda: "HTTP request request_id=" in caplog.text)
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
            assert payload["http_server"]["lifecycle"]["process_alive"] is True
            assert isinstance(payload["http_server"]["lifecycle"]["process_pid"], int)
            assert isinstance(payload["http_server"]["lifecycle"]["parent_pid"], int)
            assert payload["http_server"]["lifecycle"]["parent_alive"] is True
            assert payload["http_server"]["lifecycle"]["orphaned"] is False
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
        assert wait_until(lambda: "status snapshot returned status=ok db_ok=True" in caplog.text)


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
        assert snapshot["process_alive"] is True
        assert isinstance(snapshot["process_pid"], int)

        manager.server.stop()
        snapshot = manager.server.get_lifecycle_snapshot()
        assert snapshot["state"] == "stopped"
        assert snapshot["desired_running"] is False
        assert snapshot["thread_alive"] is False
        assert snapshot["process_alive"] is False
        assert snapshot["process_pid"] is None
        assert snapshot["last_failure_reason"] == ""
    finally:
        with manager.server._lock:
            manager.server._process = None
            manager.server._lifecycle.desired_running = False
        manager.service.close()


def test_http_control_snapshot_covers_display_and_button_states(tmp_path: Path) -> None:
    """HTTP server 应统一提供 GUI 可直接展示的状态和按钮控制信息。"""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class AliveProcess:
        """测试用存活进程替身。"""

        def is_alive(self) -> bool:
            return True

    try:
        assert manager.server.get_control_snapshot()["primary_action"] == "start"

        with manager.server._lock:
            manager.server._lifecycle.mark_starting()
        starting = manager.server.get_control_snapshot()
        assert starting["display_state"] == "starting"
        assert starting["button_enabled"] is False
        assert starting["primary_action"] == "none"

        with manager.server._lock:
            manager.server._process = AliveProcess()
            manager.server._lifecycle.mark_running(1888, 12345)
        running = manager.server.get_control_snapshot()
        assert running["display_state"] == "running"
        assert running["severity"] == "ok"
        assert running["primary_action"] == "stop"

        with manager.server._lock:
            manager.server._health_failure_count = 1
            manager.server._last_probe_error = "GET / failed"
        degraded = manager.server.get_control_snapshot()
        assert degraded["display_state"] == "degraded"
        assert degraded["display_text"] == "响应异常"
        assert degraded["severity"] == "warning"
        assert degraded["primary_action"] == "stop"

        with manager.server._lock:
            manager.server._process = None
            manager.server._health_failure_count = 0
            manager.server._last_probe_error = ""
        dead_child = manager.server.get_control_snapshot()
        assert dead_child["display_state"] == "degraded"
        assert dead_child["severity"] == "error"
        assert dead_child["primary_action"] == "stop"

        with manager.server._lock:
            manager.server._lifecycle.mark_restarting("health restart")
        restarting = manager.server.get_control_snapshot()
        assert restarting["display_state"] == "restarting"
        assert restarting["button_enabled"] is False
        assert restarting["primary_action"] == "none"

        with manager.server._lock:
            manager.server._lifecycle.record_failure("forced failure")
        failed = manager.server.get_control_snapshot()
        assert failed["display_state"] == "failed"
        assert failed["severity"] == "error"
        assert failed["primary_action"] == "start"

        with manager.server._lock:
            manager.server._lifecycle.mark_stopping()
        stopping = manager.server.get_control_snapshot()
        assert stopping["display_state"] == "stopping"
        assert stopping["button_enabled"] is False
        assert stopping["primary_action"] == "none"
    finally:
        with manager.server._lock:
            manager.server._process = None
            manager.server._lifecycle.desired_running = False
        manager.service.close()


def test_http_control_snapshot_treats_unreadable_process_as_degraded(tmp_path: Path) -> None:
    """进程句柄已关闭或不可读时，状态快照不应抛错。"""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class BrokenProcess:
        """模拟 multiprocessing.Process.close() 后再读取 is_alive 的行为。"""

        def is_alive(self) -> bool:
            raise ValueError("process object is closed")

    try:
        with manager.server._lock:
            manager.server._process = BrokenProcess()
            manager.server._lifecycle.mark_running(1888, 12345)
        lifecycle = manager.server.get_lifecycle_snapshot()
        control = manager.server.get_control_snapshot(lifecycle)

        assert lifecycle["process_alive"] is False
        assert control["display_state"] == "degraded"
        assert control["severity"] == "error"
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_control_snapshot_recovers_stale_restarting_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """restarting 超时后应自动落到 failed，避免 GUI 永远卡在重启中。"""
    monkeypatch.setattr(http_server_module, "_SERVER_TRANSITION_STALE_SECONDS", 0.01)
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())
    try:
        with manager.server._lock:
            manager.server._lifecycle.mark_restarting("manual kill")
            manager.server._lifecycle.state_changed_monotonic = time.monotonic() - 1.0

        control = manager.server.get_control_snapshot()
        snapshot = manager.server.get_lifecycle_snapshot()

        assert snapshot["state"] == "failed"
        assert control["display_state"] == "failed"
        assert control["primary_action"] == "start"
        assert "timed out" in snapshot["last_failure_reason"]
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_child_process_restart_recovers_same_port(tmp_path: Path) -> None:
    """主进程触发重启后，应拉起新的 Uvicorn 子进程并恢复同一端口。"""
    port = free_tcp_port()
    manager = _bridge_server(tmp_path, listen_port=port)
    try:
        manager.server.start()
        first_pid = manager.server.get_lifecycle_snapshot()["process_pid"]
        assert isinstance(first_pid, int)
        assert _root_is_healthy(port)

        manager.server._restart_from_health("forced test restart")

        assert wait_until(
            lambda: manager.server.get_lifecycle_snapshot()["restart_count"] >= 1
            and manager.server.get_lifecycle_snapshot()["process_pid"] != first_pid
            and _root_is_healthy(port),
            timeout_seconds=5.0,
        )
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_health_restart_clears_dead_process_before_starting_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """旧进程已死时，健康重启应先拉起新进程恢复服务。"""
    monkeypatch.setattr(http_server_module, "_HEALTH_FAILURE_THRESHOLD", 1)
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class DeadProcess:
        """模拟已被任务管理器杀死的子进程。"""

        pid = 111

        def __init__(self) -> None:
            self.close_count = 0

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None:
            return None

        def close(self) -> None:
            self.close_count += 1

    class AliveProcess:
        """模拟新拉起的子进程。"""

        pid = 222

        def __init__(self) -> None:
            self.alive = True

        def is_alive(self) -> bool:
            return self.alive

        def join(self, timeout: float | None = None) -> None:
            return None

        def terminate(self) -> None:
            self.alive = False

        def close(self) -> None:
            return None

    old_process = DeadProcess()

    def start_replacement() -> http_server_module._ChildStartResult:
        assert manager.server._process is None
        return http_server_module._ChildStartResult(AliveProcess(), 1888, 222, None, None, None)

    monkeypatch.setattr(manager.server, "_start_child_process_attempt", start_replacement)
    try:
        with manager.server._lock:
            manager.server._process = old_process
            manager.server._lifecycle.desired_running = True
            manager.server._lifecycle.mark_running(1888, old_process.pid)

        manager.server._health_check_once()
        assert wait_until(lambda: manager.server.get_lifecycle_snapshot()["restart_count"] == 1)

        snapshot = manager.server.get_lifecycle_snapshot()
        control = manager.server.get_control_snapshot(snapshot)
        assert snapshot["state"] == "running"
        assert snapshot["process_pid"] == 222
        assert snapshot["restart_count"] == 1
        assert control["display_state"] == "running"
        assert wait_until(lambda: old_process.close_count == 1, timeout_seconds=1.0)
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_health_restart_does_not_wait_for_dead_process_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """旧进程句柄清理很慢时，也不应阻断新 HTTP 子进程启动。"""
    monkeypatch.setattr(http_server_module, "_HEALTH_FAILURE_THRESHOLD", 1)
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())
    cleanup_can_continue = threading.Event()

    class DeadSlowProcess:
        """模拟已被任务管理器杀死但 join/close 很慢的旧进程句柄。"""

        pid = 111

        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None:
            cleanup_can_continue.wait(timeout=2)

        def close(self) -> None:
            cleanup_can_continue.wait(timeout=2)

    class AliveProcess:
        """模拟新拉起的子进程。"""

        pid = 222

        def is_alive(self) -> bool:
            return True

        def join(self, timeout: float | None = None) -> None:
            return None

        def terminate(self) -> None:
            return None

        def close(self) -> None:
            return None

    def start_replacement() -> http_server_module._ChildStartResult:
        return http_server_module._ChildStartResult(AliveProcess(), 1888, 222, None, None, None)

    monkeypatch.setattr(manager.server, "_start_child_process_attempt", start_replacement)
    try:
        with manager.server._lock:
            manager.server._process = DeadSlowProcess()
            manager.server._lifecycle.desired_running = True
            manager.server._lifecycle.mark_running(1888, 111)

        started_at = time.monotonic()
        manager.server._health_check_once()
        assert wait_until(lambda: manager.server.get_lifecycle_snapshot()["restart_count"] == 1)

        snapshot = manager.server.get_lifecycle_snapshot()
        assert time.monotonic() - started_at < 1.0
        assert snapshot["state"] == "running"
        assert snapshot["process_pid"] == 222
        assert snapshot["restart_count"] == 1
    finally:
        cleanup_can_continue.set()
        manager.server.stop()
        manager.service.close()


def test_http_health_restart_failure_does_not_leave_restarting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """旧进程仍存活且清理失败时，状态应落到 failed 而不是卡在 restarting。"""
    monkeypatch.setattr(http_server_module, "_HEALTH_FAILURE_THRESHOLD", 1)
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class AliveProcess:
        """模拟仍占用旧端口且清理时触发异常的旧进程。"""

        pid = 333

        def is_alive(self) -> bool:
            return True

    def fail_stop_child(process: object) -> None:
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(manager.server, "_stop_child_process", fail_stop_child)
    try:
        port = int(manager.server.config.listen_port)
        with manager.server._lock:
            manager.server._process = AliveProcess()
            manager.server._lifecycle.desired_running = True
            manager.server._lifecycle.mark_running(port, 333)

        manager.server._health_check_once()
        assert wait_until(lambda: manager.server.get_lifecycle_snapshot()["state"] == "failed")

        snapshot = manager.server.get_lifecycle_snapshot()
        control = manager.server.get_control_snapshot(snapshot)
        assert snapshot["state"] == "failed"
        assert "cleanup failed" in snapshot["last_failure_reason"]
        assert control["display_state"] == "failed"
        assert control["primary_action"] == "start"
    finally:
        with manager.server._lock:
            manager.server._process = None
            manager.server._lifecycle.desired_running = False
        manager.service.close()


def test_stop_child_process_treats_unreadable_liveness_as_exited(tmp_path: Path) -> None:
    """旧进程句柄在 join 后不可读时，清理函数不应向外抛异常。"""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class FlakyProcess:
        """模拟 Windows 上被手动杀死后 is_alive 变得不可读的进程句柄。"""

        pid = 444

        def __init__(self) -> None:
            self.check_count = 0
            self.close_count = 0

        def is_alive(self) -> bool:
            self.check_count += 1
            if self.check_count == 1:
                return True
            raise ValueError("process object is closed")

        def join(self, timeout: float | None = None) -> None:
            return None

        def close(self) -> None:
            self.close_count += 1

    process = FlakyProcess()
    try:
        manager.server._stop_child_process(process)
        assert process.close_count == 1
    finally:
        manager.server.stop()
        manager.service.close()


def test_parent_sentinel_requests_child_exit_on_eof() -> None:
    """父进程 sentinel 断开时，子进程 watcher 应请求 Uvicorn 退出。"""

    class FakeServer:
        """只暴露 watcher 需要的 should_exit 字段。"""

        should_exit = False

    class EOFConnection:
        """模拟父进程写端已关闭的 Pipe 读端。"""

        def recv_bytes(self) -> bytes:
            raise EOFError

    server = FakeServer()
    parent_lost = threading.Event()
    server_finished = threading.Event()
    server_finished.set()

    http_server_module._start_parent_sentinel_watcher(
        server, EOFConnection(), parent_lost, server_finished
    )

    assert wait_until(lambda: server.should_exit and parent_lost.is_set(), timeout_seconds=1.0)


def test_parent_sentinel_forces_child_exit_after_grace() -> None:
    """父进程消失且 Uvicorn 不退出时，watcher 应走强制退出兜底。"""

    class FakeServer:
        """只暴露 watcher 需要的 should_exit 字段。"""

        should_exit = False

    class EOFConnection:
        """模拟父进程写端已关闭的 Pipe 读端。"""

        def recv_bytes(self) -> bytes:
            raise EOFError

    exit_codes: list[int] = []
    server = FakeServer()
    parent_lost = threading.Event()
    server_finished = threading.Event()

    http_server_module._start_parent_sentinel_watcher(
        server,
        EOFConnection(),
        parent_lost,
        server_finished,
        grace_seconds=0.01,
        force_exit=exit_codes.append,
    )

    assert wait_until(lambda: server.should_exit and parent_lost.is_set(), timeout_seconds=1.0)
    assert wait_until(lambda: exit_codes == [0], timeout_seconds=1.0)


def test_http_start_cleans_orphan_bridge_process_before_binding(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """启动前确认端口属于本程序孤儿子进程时，应先清理再启动。"""
    port = free_tcp_port()
    terminated_pids: list[int] = []
    waited_ports: list[int] = []
    monkeypatch.setattr(
        http_server_module,
        "_inspect_bridge_port_owner",
        lambda inspected_port: http_server_module._BridgePortOwner(98765, True, "orphaned")
        if inspected_port == port
        else None,
    )
    monkeypatch.setattr(
        http_server_module,
        "_terminate_process_id",
        lambda pid: terminated_pids.append(pid) or True,
    )
    monkeypatch.setattr(
        http_server_module,
        "_wait_for_port_release",
        lambda waited_port, timeout: waited_ports.append(waited_port) or True,
    )
    manager = _bridge_server(tmp_path, listen_port=port)
    try:
        manager.server.start()
        assert terminated_pids == [98765]
        assert waited_ports == [port]
        assert _root_is_healthy(port)
    finally:
        manager.server.stop()
        manager.service.close()


def test_http_start_refuses_running_bridge_instance_on_port(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """端口属于另一个仍有父进程的本程序实例时，不应自动杀掉。"""
    port = free_tcp_port()
    terminated_pids: list[int] = []
    monkeypatch.setattr(
        http_server_module,
        "_inspect_bridge_port_owner",
        lambda inspected_port: http_server_module._BridgePortOwner(24680, False, "parent_alive=True")
        if inspected_port == port
        else None,
    )
    monkeypatch.setattr(
        http_server_module,
        "_terminate_process_id",
        lambda pid: terminated_pids.append(pid) or True,
    )
    manager = _bridge_server(tmp_path, listen_port=port)
    try:
        with pytest.raises(OSError, match="another running bridge instance"):
            manager.server.start()
        assert terminated_pids == []
        assert manager.server.get_lifecycle_snapshot()["state"] == "failed"
    finally:
        manager.server.stop()
        manager.service.close()


def test_bridge_port_owner_detects_legacy_orphan_status(monkeypatch: pytest.MonkeyPatch) -> None:
    """旧版残留子进程只有 parent status unavailable 时，也应识别为孤儿。"""
    monkeypatch.setattr(
        http_server_module,
        "_fetch_http_root_body",
        lambda port: http_server_module._ROOT_LIVENESS_BODY,
    )
    monkeypatch.setattr(
        http_server_module,
        "_fetch_http_status_payload",
        lambda port: {
            "status": "error",
            "message": "parent status snapshot unavailable",
            "http_server": {"lifecycle": {"process_pid": 13579}},
        },
    )

    owner = http_server_module._inspect_bridge_port_owner(1888)

    assert owner is not None
    assert owner.pid == 13579
    assert owner.orphaned is True


def test_http_stop_terminates_and_kills_unresponsive_process(tmp_path: Path) -> None:
    """stop() 先通知退出并等 2 秒，仍不退出则 terminate，再 kill 兜底。"""
    manager = _bridge_server(tmp_path, listen_port=free_tcp_port())

    class FakeShutdownEvent:
        """记录主进程是否发出正常退出通知。"""

        def __init__(self) -> None:
            self.set_count = 0

        def set(self) -> None:
            self.set_count += 1

    class FakeProcess:
        """模拟忽略正常退出和 terminate 的子进程。"""

        pid = 12345

        def __init__(self) -> None:
            self.alive = True
            self.joins: list[float | None] = []
            self.terminate_count = 0
            self.kill_count = 0
            self.close_count = 0

        def is_alive(self) -> bool:
            return self.alive

        def join(self, timeout: float | None = None) -> None:
            self.joins.append(timeout)

        def terminate(self) -> None:
            self.terminate_count += 1

        def kill(self) -> None:
            self.kill_count += 1
            self.alive = False

        def close(self) -> None:
            self.close_count += 1

    shutdown_event = FakeShutdownEvent()
    process = FakeProcess()
    try:
        with manager.server._lock:
            manager.server._shutdown_event = shutdown_event
            manager.server._process = process
            manager.server._lifecycle.mark_starting()
            manager.server._lifecycle.mark_running(1888, process.pid)

        manager.server.stop()

        assert shutdown_event.set_count == 1
        assert process.joins[0] == http_server_module._SERVER_STOP_GRACE_SECONDS
        assert process.terminate_count == 1
        assert process.kill_count == 1
        assert process.close_count == 1
    finally:
        manager.service.close()


def test_http_start_fails_when_port_is_already_bound(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """监听端口被占用时，start 应同步抛出并记录 failed 状态。"""
    monkeypatch.setattr(http_server_module, "_ORPHAN_STATUS_PROBE_TIMEOUT_SECONDS", 0.05)
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
    monkeypatch.setattr(http_server_module, "_IPC_INGRESS_QUEUE_SIZE", 1)
    monkeypatch.setattr(http_server_module, "_IPC_DRAIN_ENQUEUE_TIMEOUT_SECONDS", 5.0)
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
        statuses = [_post_park(port, b"{}") for _ in range(5)]
        assert 503 in statuses

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
