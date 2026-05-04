"""海康停车终端 HTTP 消息解析模块。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from email import policy
from email.parser import BytesParser
from typing import Any

from bdzc_parking.common import first_non_empty_text, is_supported_image_part
from bdzc_parking.models import HikEvent, HikEventImage


INTERNAL_IMAGES_KEY = "__bdzc_parking_images__"


class HikParseError(ValueError):
    """海康消息结构或内容不符合预期时抛出的解析错误。"""

    pass


def parse_hikvision_payload(content_type: str, body: bytes) -> dict[str, Any]:
    """解析海康 HTTP body，支持 multipart 和裸 JSON 两种格式。"""
    if "multipart/form-data" in content_type.lower():
        return _parse_multipart(content_type, body)
    return _parse_json_bytes(body)


def extract_event(raw: dict[str, Any]) -> HikEvent:
    """从海康原始 JSON 中提取业务需要的过车字段。"""
    raw_json = _json_only_raw(raw)
    images = raw.get(INTERNAL_IMAGES_KEY)
    passing_root = raw.get("VehiclePassingInParkingLot")
    if not isinstance(passing_root, dict):
        raise HikParseError("missing VehiclePassingInParkingLot")

    passing_info = passing_root.get("PassingInfo")
    vehicle_info = passing_root.get("VehicleInfo")
    if not isinstance(passing_info, dict) or not isinstance(vehicle_info, dict):
        raise HikParseError("missing PassingInfo or VehicleInfo")

    direction = str(passing_info.get("directionType", "")).strip()
    # 进场优先使用 enterTime，出场优先使用 exitTime，再按字段可用性兜底。
    event_time = first_non_empty_text(
        passing_info.get("enterTime") if direction == "enter" else None,
        passing_info.get("exitTime") if direction == "exit" else None,
        passing_info.get("enterTime"),
        passing_info.get("exitTime"),
        raw.get("dateTime"),
    )
    if not event_time:
        raise HikParseError("missing event time")

    # enterID/exitID 是最稳定的事件主键；缺失时再使用字段组合 hash。
    event_key = first_non_empty_text(
        passing_info.get("enterID") if direction == "enter" else None,
        passing_info.get("exitID") if direction == "exit" else None,
        passing_info.get("enterID"),
        passing_info.get("exitID"),
    )
    if not event_key:
        event_key = _fallback_event_key(raw, passing_info, vehicle_info)

    return HikEvent(
        event_key=event_key,
        event_type=str(raw.get("eventType", "")).strip(),
        event_state=str(raw.get("eventState", "")).strip(),
        direction=direction,
        passing_type=str(passing_info.get("passingType", "")).strip(),
        plate_no=str(vehicle_info.get("plateNo", "")).strip(),
        event_time=event_time,
        timestamp=_parse_timestamp(event_time),
        gate_name=str(passing_info.get("gateName", "")).strip(),
        lane_name=str(passing_info.get("laneName", "")).strip(),
        lane_id=str(passing_info.get("laneID", "")).strip(),
        raw=raw_json,
        image=_choose_event_image(images),
    )


def raw_body_key(body: bytes) -> str:
    """为无法解析的原始 body 生成稳定去重 key。"""
    return hashlib.sha256(body).hexdigest()


def _parse_multipart(content_type: str, body: bytes) -> dict[str, Any]:
    """从 multipart/form-data 中找出 JSON 事件 part 并解析。"""
    # email.parser 需要完整 MIME 头，这里把 HTTP Content-Type 补成 MIME 消息。
    message_bytes = (
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("ascii")
        + body
    )
    message = BytesParser(policy=policy.default).parsebytes(message_bytes)
    images: list[HikEventImage] = []
    raw_event: dict[str, Any] | None = None

    # 海康样本中 JSON part 的 Content-Disposition 名称为 .xml，但内容是 JSON。
    for part in message.iter_parts():
        payload = part.get_payload(decode=True) or b""
        image = _image_from_part(part.get_filename(), part.get_content_type(), payload)
        if image is not None:
            images.append(image)

        stripped_payload = payload.strip()
        if not stripped_payload.startswith(b"{"):
            continue

        disposition = part.get("Content-Disposition", "")
        part_content_type = part.get_content_type()
        if raw_event is None and (
            ".xml" in disposition or "xml" in part_content_type or "json" in part_content_type
        ):
            raw_event = _parse_json_bytes(stripped_payload)

    # 兜底：如果 part 头不规范，只要 payload 以 JSON 对象开头也尝试解析。
    if raw_event is None:
        for part in message.iter_parts():
            payload = (part.get_payload(decode=True) or b"").strip()
            if payload.startswith(b"{"):
                raw_event = _parse_json_bytes(payload)
                break

    if raw_event is None:
        raise HikParseError("multipart payload does not contain a JSON event part")

    if images:
        raw_event[INTERNAL_IMAGES_KEY] = images
    return raw_event


def _parse_json_bytes(data: bytes) -> dict[str, Any]:
    """把 UTF-8 JSON 字节解析为对象，并校验根节点类型。"""
    try:
        value = json.loads(data.decode("utf-8-sig"))
    except UnicodeDecodeError as exc:
        raise HikParseError(f"payload is not UTF-8: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise HikParseError(f"payload is not valid JSON: {exc}") from exc

    if not isinstance(value, dict):
        raise HikParseError("event JSON root is not an object")
    return value


def _parse_timestamp(value: str) -> int:
    """把 ISO8601 时间转换为秒级 Unix 时间戳。"""
    try:
        return int(datetime.fromisoformat(value).timestamp())
    except ValueError as exc:
        raise HikParseError(f"invalid event time: {value}") from exc


def _image_from_part(name: str | None, content_type: str, payload: bytes) -> HikEventImage | None:
    """把 multipart 中的图片 part 转换为过车图片对象。"""
    normalized_name = (name or "").strip()
    if not is_supported_image_part(normalized_name, content_type, payload):
        return None

    return HikEventImage(
        name=normalized_name or "vehicle-image",
        content_type=content_type,
        data=payload,
    )


def _choose_event_image(images: object) -> HikEventImage | None:
    """从海康图片列表中优先选择过车全景图，缺失时使用第一张图片。"""
    if not isinstance(images, list):
        return None

    event_images = [image for image in images if isinstance(image, HikEventImage)]
    if not event_images:
        return None

    for image in event_images:
        if image.name.lower() == "detectionpicture.jpg":
            return image
    return event_images[0]


def _json_only_raw(raw: dict[str, Any]) -> dict[str, Any]:
    """复制原始 JSON，并去掉 GUI 内部使用的二进制图片字段。"""
    return {key: value for key, value in raw.items() if key != INTERNAL_IMAGES_KEY}


def _fallback_event_key(
    raw: dict[str, Any], passing_info: dict[str, Any], vehicle_info: dict[str, Any]
) -> str:
    """在海康事件 ID 缺失时，用关键字段组合生成去重 key。"""
    parts = [
        str(passing_info.get("directionType", "")),
        str(vehicle_info.get("plateNo", "")),
        str(passing_info.get("enterTime", "")),
        str(passing_info.get("exitTime", "")),
        str(raw.get("dateTime", "")),
        str(passing_info.get("laneID", "")),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
