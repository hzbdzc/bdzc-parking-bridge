"""跨模块共享的纯工具函数。"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from pathlib import Path


_FILENAME_TOKEN_MAP = {
    "京": "JING",
    "津": "JIN",
    "沪": "HU",
    "渝": "YU",
    "冀": "JI",
    "晋": "JIN",
    "蒙": "MENG",
    "辽": "LIAO",
    "吉": "JI",
    "黑": "HEI",
    "苏": "SU",
    "浙": "ZHE",
    "皖": "WAN",
    "闽": "MIN",
    "赣": "GAN",
    "鲁": "LU",
    "豫": "YU",
    "鄂": "E",
    "湘": "XIANG",
    "粤": "YUE",
    "桂": "GUI",
    "琼": "QIONG",
    "川": "CHUAN",
    "蜀": "SHU",
    "贵": "GUI",
    "云": "YUN",
    "滇": "DIAN",
    "藏": "ZANG",
    "陕": "SHAAN",
    "秦": "QIN",
    "甘": "GAN",
    "青": "QING",
    "宁": "NING",
    "新": "XIN",
    "港": "HK",
    "澳": "MO",
    "学": "XUE",
    "警": "JING",
    "领": "LING",
    "使": "SHI",
    "挂": "GUA",
    "临": "LIN",
    "试": "SHI",
    "超": "CHAO",
}


def iso_now() -> str:
    """返回秒级 ISO8601 本地时间字符串。"""
    return datetime.now().isoformat(timespec="seconds")


def iso_seconds_ago(seconds: float) -> str:
    """返回若干秒前的秒级 ISO8601 本地时间字符串。"""
    return (datetime.now() - timedelta(seconds=seconds)).isoformat(timespec="seconds")


def iso_seconds_from_now(seconds: float) -> str:
    """返回若干秒后的秒级 ISO8601 本地时间字符串。"""
    return (datetime.now() + timedelta(seconds=seconds)).isoformat(timespec="seconds")


def iso_days_ago(days: int) -> str:
    """返回若干天前的秒级 ISO8601 本地时间字符串。"""
    return (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")


def timestamp_for_filename() -> str:
    """返回适合用于文件名的本地时间戳。"""
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")


def format_seconds(value: float) -> str:
    """把秒数格式化为简洁文本。"""
    return f"{value:.1f}".rstrip("0").rstrip(".")


def first_non_empty_text(*values: object) -> str:
    """返回第一个非空字符串值。"""
    for value in values:
        text = str(value).strip() if value is not None else ""
        if text:
            return text
    return ""


def text_or(value: object, fallback: str = "-") -> str:
    """把空值转换成指定占位文本。"""
    text = str(value or "").strip()
    return text or fallback


def json_loads_or_text(text: str) -> object:
    """尽量把 JSON 文本解析为对象，失败时保留原文。"""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def pretty_json_text(text: str) -> str:
    """如果文本是 JSON 就格式化显示，否则原样显示。"""
    value = json_loads_or_text(text)
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


def file_size_or_zero(path: Path) -> int:
    """返回文件大小；文件不存在或无法读取时返回 0。"""
    try:
        return path.stat().st_size
    except OSError:
        return 0


def unique_path(path: Path) -> Path:
    """如果目标文件已存在，自动追加序号避免覆盖。"""
    if not path.exists():
        return path

    for index in range(2, 10_000):
        candidate = path.with_name(f"{path.stem}_{index}{path.suffix}")
        if not candidate.exists():
            return candidate
    return path.with_name(f"{path.stem}_{timestamp_for_filename()}{path.suffix}")


def _safe_filename_part(value: object) -> str:
    """清理 Windows 文件名非法字符，并保留中文等可读字符。"""
    text = str(value or "").strip()
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", text)
    text = re.sub(r"\s+", "_", text)
    return text.strip(" ._")


def ascii_filename_part(value: object) -> str:
    """把任意文本转换成稳定的 ASCII 文件名片段。"""
    text = _safe_filename_part(value)
    if not text:
        return ""

    pieces: list[str] = []
    for char in text:
        if re.fullmatch(r"[A-Za-z0-9._-]", char):
            pieces.append(char)
        elif char in _FILENAME_TOKEN_MAP:
            pieces.append(_FILENAME_TOKEN_MAP[char])
        else:
            pieces.append(f"u{ord(char):x}")
    normalized = "".join(pieces)
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("._-")


def is_supported_image_part(
    name: str | None,
    content_type: str,
    payload: bytes | None = None,
) -> bool:
    """判断 multipart part 是否是当前支持的图片类型。"""
    if payload is not None and not payload:
        return False
    normalized_type = (content_type or "").lower()
    normalized_name = (name or "").strip().lower()
    if normalized_type.startswith("image/"):
        return True
    return normalized_name.endswith((".jpg", ".jpeg", ".png"))


def image_suffix_from_parts(content_type: str, image_name: str) -> str:
    """根据图片 MIME 类型和文件名推断图片扩展名。"""
    content_type = content_type.lower()
    if "png" in content_type:
        return ".png"
    if "jpeg" in content_type or "jpg" in content_type:
        return ".jpg"

    suffix = Path(image_name).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png"}:
        return suffix
    return ".jpg"
