"""维护命令入口的回归测试。"""

from __future__ import annotations

import contextlib
import io
import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bdzc_parking.config import AppConfig
from bdzc_parking.maintenance import main
from bdzc_parking.storage import EventStore


def test_maintenance_command_migrates_raw_requests_and_prints_summary(tmp_path: Path) -> None:
    """维护命令应迁移历史 raw_requests 文件，并打印迁移汇总。"""
    config_path = tmp_path / "config.json"
    config = AppConfig(
        db_path=tmp_path / "events.sqlite3",
        log_path=tmp_path / "logs" / "bdzc_parking.log",
        config_path=config_path,
    )
    config.save()
    store = EventStore(config.db_path)
    root_file = store.request_dir / "20260412_162551_132336_cli.bin"
    root_file.parent.mkdir(parents=True, exist_ok=True)
    root_file.write_bytes(b"legacy-request")
    event_id = _insert_request_record(store, "legacy-cli-file", root_file.as_posix())

    previous_config = os.environ.get("HKPARKING_CONFIG")
    os.environ["HKPARKING_CONFIG"] = str(config_path)
    stdout = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout):
            exit_code = main(["migrate-raw-requests"])
    finally:
        if previous_config is None:
            os.environ.pop("HKPARKING_CONFIG", None)
        else:
            os.environ["HKPARKING_CONFIG"] = previous_config

    migrated_path = store.request_dir / "20260412" / root_file.name
    row = store.get_event(event_id)
    output = stdout.getvalue()
    assert exit_code == 0
    assert "scanned_files: 1" in output
    assert "moved_files: 1" in output
    assert "updated_rows: 1" in output
    assert migrated_path.exists()
    assert row is not None
    assert row["received_body_path"] == migrated_path.as_posix()


def _insert_request_record(store: EventStore, event_key: str, received_body_path: str) -> int:
    """插入一条只用于测试维护命令的最小事件记录。"""
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
