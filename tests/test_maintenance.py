"""维护命令入口的回归测试。"""

from __future__ import annotations

import contextlib
import io
import os
from pathlib import Path

from bdzc_parking.config import AppConfig
from bdzc_parking.maintenance import main
from bdzc_parking.storage import EventStore
from helpers import insert_request_record


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
    event_id = insert_request_record(store, "legacy-cli-file", root_file.as_posix())

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
