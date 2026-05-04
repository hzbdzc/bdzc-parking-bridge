"""测试用共享 helper。"""

from __future__ import annotations

import socket
import time
from collections.abc import Callable
from pathlib import Path

from bdzc_parking.parser import extract_event, parse_hikvision_payload
from bdzc_parking.storage import EventStore


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "references" / "hik_events"
HIKVISION_CONTENT_TYPE = "multipart/form-data; boundary=---------------------------7e13971310878"


def sample_body(name: str) -> bytes:
    """读取 hik_events 目录里的原始海康 body 样本。"""
    return (SAMPLES / name).read_bytes()


def sample_event(name: str):
    """读取真实样本并转换为标准 HikEvent。"""
    raw = parse_hikvision_payload(HIKVISION_CONTENT_TYPE, sample_body(name))
    return extract_event(raw)


def wait_until(
    predicate: Callable[[], bool],
    timeout_seconds: float = 2.0,
    interval_seconds: float = 0.01,
) -> bool:
    """在有限时间内轮询条件，供后台线程测试等待状态收敛。"""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_seconds)
    return predicate()


def free_tcp_port() -> int:
    """向操作系统申请一个当前空闲的本地 TCP 端口。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def insert_request_record(store: EventStore, event_key: str, received_body_path: str) -> int:
    """插入一条只用于测试 raw request 路径迁移的最小事件记录。"""
    with store._connect() as conn:
        cursor = conn.execute(
            """
            INSERT INTO events (
                event_key, received_at, updated_at, status, raw_json,
                received_content_type, received_body_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_key,
                "2026-04-12T16:25:51",
                "2026-04-12T16:25:51",
                "parse_error",
                "{}",
                "application/octet-stream",
                received_body_path,
            ),
        )
        return int(cursor.lastrowid)
