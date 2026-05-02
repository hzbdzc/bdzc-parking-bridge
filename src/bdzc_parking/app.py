"""应用启动组装模块，负责串起配置、日志、存储、服务和 GUI。"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from bdzc_parking.config import AppConfig
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.safe_logging import (
    configure_emergency_logging,
    emergency_log_path,
    ensure_emergency_handler,
    install_global_exception_hooks,
    log_exception,
)
from bdzc_parking.sender import PartnerClient
from bdzc_parking.service import ParkingBridgeService
from bdzc_parking.storage import EventStore


def setup_logging(log_path: Path) -> None:
    """配置根 logger，并把日志同时写入文件和控制台。"""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    configure_emergency_logging(log_path)
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ensure_emergency_handler(root)

    resolved_log_path = log_path.resolve()
    has_file_handler = any(
        isinstance(handler, RotatingFileHandler)
        and Path(handler.baseFilename).resolve() == resolved_log_path
        for handler in root.handlers
    )
    if not has_file_handler:
        file_handler = RotatingFileHandler(
            log_path,
            maxBytes=10 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    has_console_handler = any(
        type(handler) is logging.StreamHandler for handler in root.handlers
    )
    if not has_console_handler:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)

    install_global_exception_hooks()
    logging.getLogger(__name__).info(
        "logging initialized path=%s emergency_log_path=%s",
        log_path,
        emergency_log_path(),
    )


def main() -> int:
    """初始化桥接程序运行所需组件，并启动 Qt 图形界面。"""
    config = AppConfig.load()
    setup_logging(config.log_path)

    store = EventStore(config.db_path)
    client = PartnerClient(config)
    service = ParkingBridgeService(config, store, client)
    http_server = BridgeHTTPServer(config, service)

    if config.auto_start_server:
        try:
            http_server.start()
        except OSError:
            log_exception(logging.getLogger(__name__), "failed to auto-start HTTP server")

    try:
        from bdzc_parking.gui import run_gui
    except ImportError as exc:
        print(
            "PySide6 is not installed. Install dependencies with `uv sync`, then run `uv run bdzc_parking`.",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        service.close()
        return 1

    try:
        return run_gui(http_server, service, store)
    finally:
        http_server.stop()
        service.close()
