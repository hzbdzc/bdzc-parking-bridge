"""海康消息解析器的真实样本回归测试。"""

from __future__ import annotations

from bdzc_parking.parser import extract_event, parse_hikvision_payload
from helpers import HIKVISION_CONTENT_TYPE, sample_body


def test_parse_real_enter_plate_recognition_sample() -> None:
    """真实进场牌识样本应能解析出关键字段。"""
    raw = parse_hikvision_payload(HIKVISION_CONTENT_TYPE, sample_body("20260412_063354_226439_body.bin"))
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
    raw = parse_hikvision_payload(HIKVISION_CONTENT_TYPE, sample_body("20260412_092815_853600_body.bin"))
    event = extract_event(raw)

    assert event.direction == "exit"
    assert event.passing_type == "plateRecognition"
    assert event.plate_no == "浙A619W4"
    assert event.lane_name == "出口1"
