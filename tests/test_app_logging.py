"""应用日志轮转策略测试。"""

from __future__ import annotations

import os
import time

from bdzc_parking.app import RetentionRotatingFileHandler


def test_log_rollover_uses_timestamp_and_prunes_older_than_retention(tmp_path) -> None:
    """日志轮转应生成时间戳历史文件，并删除超过保留期的历史日志。"""
    log_path = tmp_path / "bdzc_parking.log"
    log_path.write_text("current log\n", encoding="utf-8")
    old_archive = tmp_path / "bdzc_parking.20000101_000000.log"
    recent_archive = tmp_path / "bdzc_parking.20990101_000000.log"
    old_archive.write_text("old\n", encoding="utf-8")
    recent_archive.write_text("recent\n", encoding="utf-8")

    now = time.time()
    os.utime(old_archive, (now - 181 * 24 * 60 * 60, now - 181 * 24 * 60 * 60))
    os.utime(recent_archive, (now, now))
    handler = RetentionRotatingFileHandler(
        log_path,
        max_bytes=10 * 1024 * 1024,
        retention_days=180,
        encoding="utf-8",
    )
    try:
        handler.doRollover()
    finally:
        handler.close()

    assert handler.last_rollover_path is not None
    assert handler.last_rollover_path.exists()
    assert handler.last_rollover_path.name.startswith("bdzc_parking.")
    assert handler.last_rollover_path.suffix == ".log"
    assert log_path.exists()
    assert not old_archive.exists()
    assert recent_archive.exists()
