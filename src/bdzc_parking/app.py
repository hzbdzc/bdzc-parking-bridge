"""应用启动组装模块，负责串起配置、日志、存储、HTTP 进程和 GUI。"""

from __future__ import annotations

import logging
import sys

from bdzc_parking.config import AppConfig
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.logging_setup import (
    RetentionRotatingFileHandler,
    rotate_current_log_file,
    setup_logging,
)
from bdzc_parking.storage import EventStore

def main() -> int:
    """初始化桥接程序运行所需组件，并启动 Qt 图形界面。"""
    config = AppConfig.load()
    setup_logging(config.log_path)

    store = EventStore(config.db_path)
    http_server = BridgeHTTPServer(config)

    if config.auto_start_server:
        try:
            http_server.start()
        except Exception:
            logging.getLogger(__name__).exception("failed to auto-start HTTP server")

    try:
        from bdzc_parking.gui import run_gui
    except ImportError as exc:
        print(
            "PySide6 is not installed. Install dependencies with `uv sync`, then run `uv run bdzc_parking`.",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        return 1

    try:
        return run_gui(http_server, store)
    finally:
        http_server.stop()
