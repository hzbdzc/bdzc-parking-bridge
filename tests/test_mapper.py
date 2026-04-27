"""海康事件到大园区 API payload 的映射测试。"""

from __future__ import annotations

import sys
from dataclasses import replace
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "references" / "hik_events"
# 测试直接运行源码目录，避免未安装包时找不到 bdzc_parking。
sys.path.insert(0, str(ROOT / "src"))

from bdzc_parking.config import AppConfig
from bdzc_parking.models import map_to_partner_payload, should_forward
from bdzc_parking.parser import extract_event, parse_hikvision_payload


CONTENT_TYPE = "multipart/form-data; boundary=---------------------------7e13971310878"


def _event(sample_name: str):
    """读取真实样本并转换为标准 HikEvent。"""
    raw = parse_hikvision_payload(CONTENT_TYPE, (SAMPLES / sample_name).read_bytes())
    return extract_event(raw)


def test_enter_maps_to_partner_out_using_local_entry_channel() -> None:
    """我方入口进场事件应映射为大园区 out，并使用入口侧配置。"""
    payload = map_to_partner_payload(
        _event("20260412_063354_226439_body.bin"),
        AppConfig(local_entry_cid="LOCAL-ENTRY-001", local_entry_cname="博达入口"),
    )

    assert payload["hobby"] == "out"
    assert payload["cid"] == "LOCAL-ENTRY-001"
    assert payload["cname"] == "博达入口"
    assert payload["phone"] == "13800000000"
    assert payload["parkid"] == "50810"
    assert payload["timestamp"] == "1775946564"


def test_exit_maps_to_partner_in_using_local_exit_channel() -> None:
    """我方出口出场事件应映射为大园区 in，并使用出口侧配置。"""
    payload = map_to_partner_payload(
        _event("20260412_092815_853600_body.bin"),
        AppConfig(local_exit_cid="LOCAL-EXIT-001", local_exit_cname="博达出口"),
    )

    assert payload["hobby"] == "in"
    assert payload["cid"] == "LOCAL-EXIT-001"
    assert payload["cname"] == "博达出口"


def test_mapping_uses_configured_hobby_values() -> None:
    """hobby 应读取配置值，允许现场显式调整出口和入口的 API 类型。"""
    config = AppConfig(local_entry_hobby="in", local_exit_hobby="out")

    enter_payload = map_to_partner_payload(_event("20260412_063354_226439_body.bin"), config)
    exit_payload = map_to_partner_payload(_event("20260412_092815_853600_body.bin"), config)

    assert enter_payload["hobby"] == "in"
    assert exit_payload["hobby"] == "out"


def test_filter_accepts_stop_but_rejects_no_plate() -> None:
    """stop 触发应自动发送，但无车牌记录仍应跳过。"""
    stop_event = _event("20260412_071503_319787_body.bin")
    assert should_forward(stop_event) == (True, "")

    no_plate_event = replace(_event("20260412_113252_857892_body.bin"), passing_type="plateRecognition")
    assert should_forward(no_plate_event) == (False, "invalid plateNo")


def test_filter_accepts_manual_passing_type() -> None:
    """manual 手动放行记录也应进入自动发送流程。"""
    manual_event = replace(_event("20260412_063354_226439_body.bin"), passing_type="manual")

    assert should_forward(manual_event) == (True, "")


def test_filter_rejects_manual_without_plate() -> None:
    """manual 手动放行记录仍必须有有效车牌。"""
    manual_no_plate_event = replace(_event("20260412_113252_857892_body.bin"), passing_type="manual")

    assert should_forward(manual_no_plate_event) == (False, "invalid plateNo")


def test_filter_rejects_stale_event_by_config() -> None:
    """过车时间相对接收时间超过配置秒数时不应自动发送。"""
    event = _event("20260412_063354_226439_body.bin")
    received_at = datetime.fromtimestamp(event.timestamp + 61)

    assert should_forward(event, AppConfig(max_event_age_seconds=60), received_at) == (
        False,
        "过车时间过旧: 61秒 > 60秒",
    )
