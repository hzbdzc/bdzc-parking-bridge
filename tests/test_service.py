"""桥接服务跳过发送与手动发送前置条件测试。"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import replace
from pathlib import Path

import pytest

import bdzc_parking.service as service_module
from bdzc_parking.config import AppConfig
from bdzc_parking.models import SendResult
from bdzc_parking.service import ParkingBridgeService
from bdzc_parking.storage import EventStore
from helpers import HIKVISION_CONTENT_TYPE, sample_body, wait_until


class FakeClient:
    """用于验证 skipped 记录不会触发自动发送的假客户端。"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.calls = 0

    def send_once(self, payload: dict[str, object], attempt: int = 1):
        self.calls += 1
        raise AssertionError(f"unexpected send: {payload}")


class CapturingClient:
    """记录发送 payload，供断言动态图片 URL 使用。"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.calls = 0
        self.payloads: list[dict[str, object]] = []

    def send_once(self, payload: dict[str, object], attempt: int = 1) -> SendResult:
        self.calls += 1
        self.payloads.append(dict(payload))
        return SendResult(True, attempt, 200, '{"status":200,"msg":"ok"}')


class FlakyClient:
    """按失败次数控制返回结果，覆盖 worker 内 inline 重试流转。"""

    def __init__(self, config: AppConfig, fail_until: int):
        self.config = config
        self.fail_until = fail_until
        self.attempts: list[int] = []

    def send_once(self, payload: dict[str, object], attempt: int = 1) -> SendResult:
        self.attempts.append(attempt)
        if attempt <= self.fail_until:
            return SendResult(False, attempt, 200, '{"status":500,"msg":"temporary"}', "temporary")
        return SendResult(True, attempt, 200, '{"status":200,"msg":"ok"}')


class BlockingClient:
    """发送时阻塞的客户端，用于观察 sender worker busy 状态。"""

    def __init__(self, config: AppConfig):
        self.config = config
        self.started = threading.Event()
        self.release = threading.Event()

    def send_once(self, payload: dict[str, object], attempt: int = 1) -> SendResult:
        self.started.set()
        self.release.wait(timeout=3)
        return SendResult(True, attempt, 200, '{"status":200,"msg":"ok"}')


def test_skipped_stale_record_with_valid_plate_still_generates_partner_payload(tmp_path: Path) -> None:
    """过旧跳过的有效车牌记录也应预生成 payload，供详情页手动发送。"""
    config = AppConfig(
        local_exit_cid="EXIT-001",
        local_exit_cname="北门出口",
        local_entry_cid="ENTRY-001",
        local_entry_cname="南门入口",
        max_event_age_seconds=0.0,
    )
    store = EventStore(tmp_path / "events.sqlite3")
    client = FakeClient(config)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_063354_226439_body.bin")

    try:
        service.handle_request(HIKVISION_CONTENT_TYPE, body)

        rows = store.list_events()
        assert len(rows) == 1
        row = rows[0]
        payload = json.loads(row["partner_payload_json"])
        expected_cid = config.local_entry_cid if row["direction"] == "enter" else config.local_exit_cid
        expected_cname = config.local_entry_cname if row["direction"] == "enter" else config.local_exit_cname
        expected_hobby = config.local_entry_hobby if row["direction"] == "enter" else config.local_exit_hobby

        assert row["status"] == "skipped"
        assert row["auto_send"] == 0
        assert row["skip_reason"].startswith("过车时间过旧:")
        assert payload["car"] == row["plate_no"]
        assert payload["cid"] == expected_cid
        assert payload["cname"] == expected_cname
        assert payload["hobby"] == expected_hobby
        assert row["last_request_payload_json"] == ""
        assert client.calls == 0
    finally:
        service.close()


def test_stop_record_is_auto_sent(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """停车触发 stop 记录也应进入自动发送流程。"""
    caplog.set_level(logging.INFO, logger="bdzc_parking.service")
    config = AppConfig(max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = CapturingClient(config)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_071503_319787_body.bin")

    try:
        service.handle_request(HIKVISION_CONTENT_TYPE, body)
        assert wait_until(lambda: client.calls == 1)

        row = store.list_events()[0]
        assert row["status"] == "sent"
        assert row["auto_send"] == 1
        assert row["skip_reason"] == ""
        assert row["passing_type"] == "stop"
        assert client.payloads[0]["car"] == row["plate_no"]
        assert f"event_id={row['id']}" in caplog.text
        assert f"plate={row['plate_no']}" in caplog.text
        assert "Hik event stored" in caplog.text
        assert "partner send result" in caplog.text
        assert "success=yes" in caplog.text
        assert "final_status=sent" in caplog.text
    finally:
        service.close()


def test_manual_record_is_auto_sent(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """manual 手动放行记录也应进入自动发送流程。"""
    original_extract_event = service_module.extract_event

    def extract_manual_event(raw):
        return replace(original_extract_event(raw), passing_type="manual")

    monkeypatch.setattr(service_module, "extract_event", extract_manual_event)
    config = AppConfig(max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = CapturingClient(config)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_063354_226439_body.bin")

    try:
        service.handle_request(HIKVISION_CONTENT_TYPE, body)
        assert wait_until(lambda: client.calls == 1)

        row = store.list_events()[0]
        assert row["status"] == "sent"
        assert row["auto_send"] == 1
        assert row["skip_reason"] == ""
        assert row["passing_type"] == "manual"
        assert client.payloads[0]["car"] == row["plate_no"]
    finally:
        service.close()


def test_send_record_uses_latest_external_url_base(tmp_path: Path) -> None:
    """手动重发前修改 external_url_base 时，应按新值生成图片 URL。"""
    config = AppConfig(max_event_age_seconds=0.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = CapturingClient(config)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_063354_226439_body.bin")

    try:
        service.handle_request(HIKVISION_CONTENT_TYPE, body)
        row = store.list_events()[0]
        event_id = int(row["id"])

        config.external_url_base = "https://public.example.com/parking-images"
        service.manual_resend(event_id)
        assert wait_until(lambda: client.calls == 1)

        assert client.calls == 1
        assert client.payloads[0]["img"].startswith("https://public.example.com/parking-images/")
        sent_row = store.get_event(event_id)
        assert sent_row is not None
        sent_payload = json.loads(sent_row["last_request_payload_json"])
        assert sent_payload["img"].startswith("https://public.example.com/parking-images/")
    finally:
        service.close()


def test_manual_resend_rebuilds_missing_partner_payload(tmp_path: Path) -> None:
    """旧记录未预存 payload 时，手动重发应从字段重建后发送。"""
    config = AppConfig()
    store = EventStore(tmp_path / "events.sqlite3")
    client = CapturingClient(config)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_063354_226439_body.bin")
    event = service_module.extract_event(
        service_module.parse_hikvision_payload(HIKVISION_CONTENT_TYPE, body)
    )
    event_id, _ = store.add_event(event, "skipped", False)

    try:
        row = store.get_event(event_id)
        assert row is not None
        assert row["partner_payload_json"] == "{}"

        assert service.manual_resend(event_id)
        assert wait_until(lambda: client.calls == 1)

        assert client.payloads[0]["car"] == event.plate_no
        sent = store.get_event(event_id)
        assert sent is not None
        assert sent["status"] == "sent"
    finally:
        service.close()


def test_manual_resend_rejects_record_without_payload_inputs(tmp_path: Path) -> None:
    """无法生成 payload 的记录应在入队前拒绝手动重发。"""
    config = AppConfig()
    store = EventStore(tmp_path / "events.sqlite3")
    client = CapturingClient(config)
    service = ParkingBridgeService(config, store, client)
    event_id = store.add_parse_error("bad-record", "bad payload", "application/json", b"{}")

    try:
        assert not service.manual_resend(event_id)
        assert client.calls == 0
        row = store.get_event(event_id)
        assert row is not None
        assert row["status"] == "parse_error"
    finally:
        service.close()


def test_failed_event_retries_inline_then_succeeds(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """首次发送失败后，应在同一 worker 内等待并重试到最终成功。"""
    monkeypatch.setattr(service_module, "_RETRY_DELAYS_SECONDS", (0.01, 0.02, 0.03))

    config = AppConfig(max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = FlakyClient(config, fail_until=1)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_063354_226439_body.bin")

    try:
        service.handle_request(HIKVISION_CONTENT_TYPE, body)
        row = store.list_events()[0]
        assert row["status"] == "sent"
        assert row["attempts"] == 2
        assert client.attempts == [1, 2]
    finally:
        service.close()


def test_failed_event_retries_then_becomes_dead_letter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """四次总发送都失败后，记录应停止自动补发并转为 dead_letter。"""
    monkeypatch.setattr(service_module, "_RETRY_DELAYS_SECONDS", (0.01, 0.02, 0.03))

    config = AppConfig(max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = FlakyClient(config, fail_until=99)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_063354_226439_body.bin")

    try:
        service.handle_request(HIKVISION_CONTENT_TYPE, body)
        assert wait_until(
            lambda: bool(store.list_events()) and store.list_events()[0]["status"] == "dead_letter",
            timeout_seconds=2.0,
        )

        row = store.list_events()[0]
        assert row["status"] == "dead_letter"
        assert row["attempts"] == 4
        assert row["dead_lettered_at"] != ""
        assert client.attempts == [1, 2, 3, 4]
    finally:
        service.close()


def test_close_during_retry_restores_pending_without_dead_letter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """重试等待中关闭 service 时，不应把未满四次实际发送的记录写成死信。"""
    monkeypatch.setattr(service_module, "_RETRY_DELAYS_SECONDS", (5.0, 5.0, 5.0))

    config = AppConfig(max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = FlakyClient(config, fail_until=99)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_063354_226439_body.bin")

    try:
        assert service.enqueue_http_request(HIKVISION_CONTENT_TYPE, body)
        assert wait_until(lambda: client.attempts == [1])

        service.close()
        row = store.list_events()[0]
        assert row["status"] == "pending"
        assert row["attempts"] == 0
        assert row["dead_lettered_at"] == ""
        assert "service stopped before retries completed" in row["last_error"]
    finally:
        service.close()


def test_service_startup_leaves_legacy_sending_record_unchanged(tmp_path: Path) -> None:
    """Startup should not recover or resend sending records left by an old process."""
    config = AppConfig()
    store = EventStore(tmp_path / "events.sqlite3")
    body = sample_body("20260412_063354_226439_body.bin")
    event = service_module.extract_event(
        service_module.parse_hikvision_payload(HIKVISION_CONTENT_TYPE, body)
    )
    event_id, _ = store.add_event(
        event,
        "pending",
        True,
        partner_payload={"car": event.plate_no},
    )
    assert store.mark_send_started(event_id, "2026-04-12T06:33:55")
    client = CapturingClient(config)
    service = ParkingBridgeService(config, store, client)
    try:
        row = store.get_event(event_id)
        assert row is not None
        assert row["status"] == "sending"
        assert row["attempts"] == 0
        assert client.calls == 0
    finally:
        service.close()


def test_runtime_snapshot_reports_idle_service_workers(tmp_path: Path) -> None:
    """运行快照应显示统一 service worker 固定 3 个且空闲。"""
    config = AppConfig()
    store = EventStore(tmp_path / "events.sqlite3")
    service = ParkingBridgeService(config, store, CapturingClient(config))
    try:
        snapshot = service.get_runtime_snapshot()
        workers = snapshot["workers"]
        assert workers["service_total"] == 3
        assert workers["service_alive"] == 3
        assert workers["service_active"] == 0
        assert workers["service_idle"] == 3
    finally:
        service.close()


def test_runtime_snapshot_reports_busy_service_worker(tmp_path: Path) -> None:
    """发送阻塞期间，运行快照应显示 service worker 忙碌。"""
    config = AppConfig(max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = BlockingClient(config)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_063354_226439_body.bin")
    try:
        assert service.enqueue_http_request(HIKVISION_CONTENT_TYPE, body)
        assert wait_until(client.started.is_set)

        workers = service.get_runtime_snapshot()["workers"]
        assert workers["service_total"] == 3
        assert workers["service_alive"] == 3
        assert workers["service_active"] == 1
        assert workers["service_idle"] == 2
    finally:
        client.release.set()
        service.close()


def test_manual_cleanup_request_runs_and_resets_timer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """手动清理应投递到统一 worker，并重置下一次定时清理时间。"""
    caplog.set_level(logging.INFO, logger="bdzc_parking.service")
    config = AppConfig()
    store = EventStore(tmp_path / "events.sqlite3")
    service = ParkingBridgeService(config, store, CapturingClient(config))
    calls: list[tuple[int, int]] = []

    def fake_prune(event_days: int, artifact_days: int) -> dict[str, int]:
        calls.append((event_days, artifact_days))
        return {"events_deleted": 0, "artifacts_cleared": 0, "files_deleted": 0}

    monkeypatch.setattr(store, "prune_old_data", fake_prune)
    before_cleanup_at = service._next_cleanup_at
    try:
        assert service.request_cleanup("test")
        assert wait_until(lambda: bool(calls))

        snapshot = service.get_runtime_snapshot()
        assert calls == [(180, 180)]
        assert snapshot["cleanup"]["finished_at"] != ""
        assert service._next_cleanup_at > before_cleanup_at
        assert "cleanup finished reason=test" in caplog.text
        assert "events_deleted" in caplog.text
    finally:
        service.close()


def test_worker_pool_handles_new_ingress_while_one_worker_waits_to_retry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """一个 worker 等待重试时，其他 service worker 仍应能处理新入站消息。"""
    monkeypatch.setattr(service_module, "_RETRY_DELAYS_SECONDS", (0.3, 0.01, 0.01))
    config = AppConfig(max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = FlakyClient(config, fail_until=1)
    service = ParkingBridgeService(config, store, client)
    body = sample_body("20260412_063354_226439_body.bin")
    try:
        assert service.enqueue_http_request(HIKVISION_CONTENT_TYPE, body)
        assert wait_until(lambda: client.attempts == [1])

        assert service.enqueue_http_request("application/json", b"{}")
        assert wait_until(lambda: any(row["status"] == "parse_error" for row in store.list_events()))
    finally:
        service.close()


def test_service_worker_recovers_after_cleanup_exception(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """清理任务抛异常后，worker 应继续存活并能处理后续入站消息。"""
    config = AppConfig()
    store = EventStore(tmp_path / "events.sqlite3")
    service = ParkingBridgeService(config, store, CapturingClient(config))
    entered = threading.Event()

    def raising_cleanup(_task) -> None:
        entered.set()
        raise RuntimeError("cleanup boom")

    monkeypatch.setattr(service, "_handle_cleanup_task", raising_cleanup)
    try:
        assert service._enqueue_task(service_module._CleanupTask("test"))
        assert wait_until(entered.is_set)
        assert wait_until(lambda: "cleanup boom" in str(service.get_runtime_snapshot()["last_error"]))

        assert service.enqueue_http_request("application/json", b"{}")
        assert wait_until(lambda: any(row["status"] == "parse_error" for row in store.list_events()))
        assert service.get_runtime_snapshot()["workers"]["service_alive"] == 3
    finally:
        service.close()
