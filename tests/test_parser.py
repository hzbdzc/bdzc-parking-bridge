"""海康消息解析器的真实样本回归测试。"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "references" / "hik_events"
# 测试直接运行源码目录，避免未安装包时找不到 bdzc_parking。
sys.path.insert(0, str(ROOT / "src"))

from bdzc_parking.parser import extract_event, parse_hikvision_payload


CONTENT_TYPE = "multipart/form-data; boundary=---------------------------7e13971310878"


def _sample(name: str) -> bytes:
    """读取 hik_events 目录里的原始海康 body 样本。"""
    return (SAMPLES / name).read_bytes()


def test_parse_real_enter_plate_recognition_sample() -> None:
    """真实进场牌识样本应能解析出关键字段。"""
    raw = parse_hikvision_payload(CONTENT_TYPE, _sample("20260412_063354_226439_body.bin"))
    event = extract_event(raw)

    assert event.event_type == "vehiclePassingInParkingLot"
    assert event.event_state == "active"
    assert event.direction == "enter"
    assert event.passing_type == "plateRecognition"
    assert event.plate_no == "川Q21C65"
    assert event.lane_name == "入口1"
    assert event.timestamp > 0
    assert event.image is not None
    assert event.image.name == "detectionPicture.jpg"
    assert event.image.data.startswith(bytes.fromhex("ffd8"))
    assert "__bdzc_parking_images__" not in event.raw


def test_parse_real_exit_plate_recognition_sample() -> None:
    """真实出场牌识样本应能解析出关键字段。"""
    raw = parse_hikvision_payload(CONTENT_TYPE, _sample("20260412_092815_853600_body.bin"))
    event = extract_event(raw)

    assert event.direction == "exit"
    assert event.passing_type == "plateRecognition"
    assert event.plate_no == "浙A619W4"
    assert event.lane_name == "出口1"
