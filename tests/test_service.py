"""桥接服务跳过发送与手动发送前置条件测试。"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "references" / "hik_events"
sys.path.insert(0, str(ROOT / "src"))

import bdzc_parking.service as service_module
from bdzc_parking.config import AppConfig
from bdzc_parking.models import SendResult
from bdzc_parking.service import ParkingBridgeService
from bdzc_parking.storage import EventStore


CONTENT_TYPE = "multipart/form-data; boundary=---------------------------7e13971310878"


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
    """按失败次数控制返回结果，覆盖 failed_retryable 和 dead_letter 流转。"""

    def __init__(self, config: AppConfig, fail_until: int):
        self.config = config
        self.fail_until = fail_until
        self.attempts: list[int] = []

    def send_once(self, payload: dict[str, object], attempt: int = 1) -> SendResult:
        self.attempts.append(attempt)
        if attempt <= self.fail_until:
            return SendResult(False, attempt, 200, '{"status":500,"msg":"temporary"}', "temporary")
        return SendResult(True, attempt, 200, '{"status":200,"msg":"ok"}')


def test_skipped_stale_record_with_valid_plate_still_generates_partner_payload(tmp_path: Path) -> None:
    """过旧跳过的有效车牌记录也应预生成 payload，供详情页手动发送。"""
    config = AppConfig(
        local_exit_cid="EXIT-001",
        local_exit_cname="北门出口",
        local_entry_cid="ENTRY-001",
        local_entry_cname="南门入口",
        sender_worker_count=1,
        max_event_age_seconds=0.0,
    )
    store = EventStore(tmp_path / "events.sqlite3")
    client = FakeClient(config)
    service = ParkingBridgeService(config, store, client)
    body = (SAMPLES / "20260412_063354_226439_body.bin").read_bytes()

    try:
        service.handle_request(CONTENT_TYPE, body)

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


def test_stop_record_is_auto_sent(tmp_path: Path) -> None:
    """停车触发 stop 记录也应进入自动发送流程。"""
    config = AppConfig(sender_worker_count=1, max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = CapturingClient(config)
    service = ParkingBridgeService(config, store, client)
    body = (SAMPLES / "20260412_071503_319787_body.bin").read_bytes()

    try:
        service.handle_request(CONTENT_TYPE, body)
        assert _wait_until(lambda: client.calls == 1)

        row = store.list_events()[0]
        assert row["status"] == "sent"
        assert row["auto_send"] == 1
        assert row["skip_reason"] == ""
        assert row["passing_type"] == "stop"
        assert client.payloads[0]["car"] == row["plate_no"]
    finally:
        service.close()


def test_send_record_uses_latest_external_url_base(tmp_path: Path) -> None:
    """手动重发前修改 external_url_base 时，应按新值生成图片 URL。"""
    config = AppConfig(sender_worker_count=1, max_event_age_seconds=0.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = CapturingClient(config)
    service = ParkingBridgeService(config, store, client)
    body = (SAMPLES / "20260412_063354_226439_body.bin").read_bytes()

    try:
        service.handle_request(CONTENT_TYPE, body)
        row = store.list_events()[0]
        event_id = int(row["id"])

        config.external_url_base = "https://public.example.com/parking-images"
        service.manual_resend(event_id)
        assert _wait_until(lambda: client.calls == 1)

        assert client.calls == 1
        assert client.payloads[0]["img"].startswith("https://public.example.com/parking-images/")
        sent_row = store.get_event(event_id)
        assert sent_row is not None
        sent_payload = json.loads(sent_row["last_request_payload_json"])
        assert sent_payload["img"].startswith("https://public.example.com/parking-images/")
    finally:
        service.close()


def test_failed_event_sets_next_retry_after_first_failure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """首次发送失败后，记录应转为 failed_retryable 并写入下次重试时间。"""
    monkeypatch.setattr(service_module, "_RETRY_DELAYS_SECONDS", (10.0, 20.0, 30.0))
    monkeypatch.setattr(service_module, "_MAINTENANCE_INTERVAL_SECONDS", 0.01)

    config = AppConfig(sender_worker_count=1, max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = FlakyClient(config, fail_until=1)
    service = ParkingBridgeService(config, store, client)
    body = (SAMPLES / "20260412_063354_226439_body.bin").read_bytes()

    try:
        service.handle_request(CONTENT_TYPE, body)
        assert _wait_until(
            lambda: bool(store.list_events())
            and store.list_events()[0]["status"] == "failed_retryable"
            and store.list_events()[0]["attempts"] == 1
        )

        row = store.list_events()[0]
        assert row["status"] == "failed_retryable"
        assert row["attempts"] == 1
        assert row["next_retry_at"] != ""
        next_retry_at = datetime.fromisoformat(str(row["next_retry_at"]))
        last_attempt_at = datetime.fromisoformat(str(row["last_attempt_at"]))
        assert 9 <= (next_retry_at - last_attempt_at).total_seconds() <= 11
        assert client.attempts == [1]
    finally:
        service.close()


def test_failed_event_retries_then_becomes_dead_letter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """四次总发送都失败后，记录应停止自动补发并转为 dead_letter。"""
    monkeypatch.setattr(service_module, "_RETRY_DELAYS_SECONDS", (0.01, 0.02, 0.03))
    monkeypatch.setattr(service_module, "_MAINTENANCE_INTERVAL_SECONDS", 0.01)

    config = AppConfig(sender_worker_count=1, max_event_age_seconds=10_000_000_000.0)
    store = EventStore(tmp_path / "events.sqlite3")
    client = FlakyClient(config, fail_until=99)
    service = ParkingBridgeService(config, store, client)
    body = (SAMPLES / "20260412_063354_226439_body.bin").read_bytes()

    try:
        service.handle_request(CONTENT_TYPE, body)
        assert _wait_until(
            lambda: bool(store.list_events()) and store.list_events()[0]["status"] == "dead_letter",
            timeout_seconds=2.0,
        )

        row = store.list_events()[0]
        assert row["status"] == "dead_letter"
        assert row["attempts"] == 4
        assert row["next_retry_at"] == ""
        assert row["dead_lettered_at"] != ""
        assert client.attempts == [1, 2, 3, 4]
    finally:
        service.close()


def test_stale_sending_record_is_recovered_and_sent_again(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """启动时卡在 sending 的旧记录应恢复为可重试并再次发送。"""
    monkeypatch.setattr(service_module, "_RETRY_DELAYS_SECONDS", (0.01, 0.02, 0.03))
    monkeypatch.setattr(service_module, "_MAINTENANCE_INTERVAL_SECONDS", 0.01)

    config = AppConfig(sender_worker_count=1, max_event_age_seconds=10_000_000_000.0, stale_sending_seconds=0.0)
    store = EventStore(tmp_path / "events.sqlite3")
    body = (SAMPLES / "20260412_063354_226439_body.bin").read_bytes()

    bootstrap_client = CapturingClient(config)
    bootstrap_service = ParkingBridgeService(config, store, bootstrap_client)
    try:
        bootstrap_service.handle_request(CONTENT_TYPE, body)
        assert _wait_until(lambda: bool(store.list_events()) and store.list_events()[0]["status"] == "sent")
    finally:
        bootstrap_service.close()

    row = store.list_events()[0]
    with store._connect() as conn:
        conn.execute(
            """
            UPDATE events
            SET status = 'sending', updated_at = ?, attempts = 1, next_retry_at = ''
            WHERE id = ?
            """,
            ("2000-01-01T00:00:00", int(row["id"])),
        )

    recovery_client = CapturingClient(config)
    recovery_service = ParkingBridgeService(config, store, recovery_client)
    try:
        assert _wait_until(lambda: recovery_client.calls == 1)
        recovered = store.get_event(int(row["id"]))
        assert recovered is not None
        assert recovered["status"] == "sent"
        assert recovered["attempts"] == 2
    finally:
        recovery_service.close()


def _wait_until(predicate, timeout_seconds: float = 2.0) -> bool:
    """在有限时间内轮询条件，供后台发送测试等待状态收敛。"""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()
