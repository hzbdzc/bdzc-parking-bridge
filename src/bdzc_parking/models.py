"""跨模块共享的数据模型。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from bdzc_parking.config import AppConfig


FORWARD_PASSING_TYPES = {"plateRecognition", "stop", "manual"}


@dataclass(frozen=True)
class HikEventImage:
    """从海康 multipart 消息中提取出的过车图片。"""

    name: str
    content_type: str
    data: bytes


@dataclass(frozen=True)
class HikEvent:
    """从海康过车消息中提取出的标准化事件。"""

    event_key: str
    event_type: str
    event_state: str
    direction: str
    passing_type: str
    plate_no: str
    event_time: str
    timestamp: int
    gate_name: str
    lane_name: str
    lane_id: str
    raw: dict[str, Any]
    image: HikEventImage | None = None


@dataclass(frozen=True)
class SendResult:
    """向大园区 API 发送一次记录后的结果。"""

    success: bool
    attempts: int
    status_code: int | None = None
    response_text: str = ""
    error: str = ""


class MappingError(ValueError):
    """海康事件无法映射到大园区请求时抛出的错误。"""

    pass


def should_forward(
    event: HikEvent,
    config: AppConfig | None = None,
    received_at: datetime | None = None,
) -> tuple[bool, str]:
    """判断海康事件是否应该发送给大园区，并返回跳过原因。"""
    # 只处理停车场出入口的有效牌识过车事件，其他事件只入库展示。
    if event.event_type != "vehiclePassingInParkingLot":
        return False, f"unsupported eventType: {event.event_type}"
    if event.event_state != "active":
        return False, f"unsupported eventState: {event.event_state}"
    if event.direction not in {"enter", "exit"}:
        return False, f"unsupported directionType: {event.direction}"
    if event.passing_type not in FORWARD_PASSING_TYPES:
        return False, f"unsupported passingType: {event.passing_type}"
    if not has_partner_payload_inputs(event):
        return False, "invalid plateNo"
    if config is not None and received_at is not None:
        age_seconds = received_at.timestamp() - event.timestamp
        if age_seconds > config.max_event_age_seconds:
            age_text = _format_seconds(age_seconds)
            limit_text = _format_seconds(config.max_event_age_seconds)
            return False, f"过车时间过旧: {age_text}秒 > {limit_text}秒"
    return True, ""


def has_partner_payload_inputs(event: HikEvent) -> bool:
    """判断事件是否具备生成大园区 payload 的最小字段。"""
    return event.direction in {"enter", "exit"} and bool(event.plate_no) and event.plate_no != "无车牌"


def _format_seconds(value: float) -> str:
    """把秒数格式化为适合跳过原因展示的简洁文本。"""
    return f"{value:.1f}".rstrip("0").rstrip(".")


def map_to_partner_payload(event: HikEvent, config: AppConfig) -> dict[str, object]:
    """把有效海康事件转换为大园区 API 所需字段。"""
    # hobby 由配置显式定义，默认保持业务方向反转：入口 out，出口 in。
    if event.direction == "enter":
        hobby = _config_hobby(config.local_entry_hobby)
        cid = config.local_entry_cid
        cname = config.local_entry_cname
    elif event.direction == "exit":
        hobby = _config_hobby(config.local_exit_hobby)
        cid = config.local_exit_cid
        cname = config.local_exit_cname
    else:
        raise MappingError(f"unsupported directionType: {event.direction}")

    return {
        "hobby": hobby,
        "cid": cid,
        "cname": cname,
        "car": event.plate_no,
        "phone": config.default_phone,
        "timestamp": str(event.timestamp),
        "parkid": config.park_id,
    }


def _config_hobby(value: str) -> str:
    """标准化配置里的大园区进出类型。"""
    hobby = str(value).strip().lower()
    if hobby not in {"in", "out"}:
        raise MappingError(f"unsupported hobby: {value}")
    return hobby
