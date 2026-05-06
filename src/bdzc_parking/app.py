"""应用启动组装模块，负责串起配置、日志、存储、服务和 GUI。"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

from bdzc_parking.config import AppConfig
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.service import ParkingBridgeService, PartnerClient
from bdzc_parking.storage import EventStore


LOG_RETENTION_DAYS = 180
LOG_MAX_BYTES = 10 * 1024 * 1024


class RetentionRotatingFileHandler(RotatingFileHandler):
    """按大小轮转日志，并按文件修改时间保留指定天数的历史日志。"""

    def __init__(self, filename: Path, max_bytes: int, retention_days: int, encoding: str):
        """保存保留天数，并复用 RotatingFileHandler 的大小判断。"""
        super().__init__(filename, maxBytes=max_bytes, backupCount=0, encoding=encoding)
        self.retention_days = int(retention_days)
        self.last_rollover_path: Path | None = None

    def doRollover(self) -> None:  # noqa: N802
        """把当前日志重命名成带时间戳的历史文件，并清理过期历史日志。"""
        if self.stream:
            self.stream.close()
            self.stream = None

        base_path = Path(self.baseFilename)
        self.last_rollover_path = None
        if base_path.exists() and base_path.stat().st_size > 0:
            target_path = _next_log_archive_path(base_path)
            base_path.replace(target_path)
            self.last_rollover_path = target_path

        if not self.delay:
            self.stream = self._open()
        _delete_expired_log_archives(base_path, self.retention_days)


def setup_logging(log_path: Path) -> None:
    """配置根 logger，并把日志同时写入文件和控制台。"""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    resolved_log_path = log_path.resolve()
    has_file_handler = any(
        isinstance(handler, RotatingFileHandler)
        and Path(handler.baseFilename).resolve() == resolved_log_path
        for handler in root.handlers
    )
    if not has_file_handler:
        file_handler = RetentionRotatingFileHandler(
            log_path,
            max_bytes=LOG_MAX_BYTES,
            retention_days=LOG_RETENTION_DAYS,
            encoding="utf-8",
        )
        root.addHandler(file_handler)
    for handler in root.handlers:
        if (
            isinstance(handler, RotatingFileHandler)
            and Path(handler.baseFilename).resolve() == resolved_log_path
        ):
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(formatter)

    has_console_handler = any(
        type(handler) is logging.StreamHandler for handler in root.handlers
    )
    if not has_console_handler:
        console_handler = logging.StreamHandler()
        root.addHandler(console_handler)
    for handler in root.handlers:
        if type(handler) is logging.StreamHandler:
            handler.setLevel(logging.INFO)
            handler.setFormatter(formatter)

    _delete_expired_log_archives(resolved_log_path, LOG_RETENTION_DAYS)

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)

    logging.getLogger(__name__).info(
        "logging initialized path=%s retention_days=%s",
        log_path,
        LOG_RETENTION_DAYS,
    )


def rotate_current_log_file(log_path: Path) -> Path | None:
    """手动轮转当前日志文件，并返回生成的历史日志路径。"""
    handler = _find_log_handler(log_path)
    if handler is None:
        raise RuntimeError(f"未找到当前日志 handler: {log_path}")
    handler.doRollover()
    logging.getLogger(__name__).info(
        "log file rotated manually path=%s archived_to=%s",
        log_path,
        handler.last_rollover_path or "",
    )
    return handler.last_rollover_path


def _find_log_handler(log_path: Path) -> RetentionRotatingFileHandler | None:
    """从根 logger 中查找指定日志文件对应的轮转 handler。"""
    resolved_log_path = Path(log_path).resolve()
    for handler in logging.getLogger().handlers:
        if not isinstance(handler, RetentionRotatingFileHandler):
            continue
        if Path(handler.baseFilename).resolve() == resolved_log_path:
            return handler
    return None


def _next_log_archive_path(log_path: Path) -> Path:
    """生成不会覆盖已有历史日志的时间戳文件名。"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = log_path.with_name(f"{log_path.stem}.{timestamp}{log_path.suffix}")
    if not base.exists():
        return base
    for index in range(2, 10_000):
        candidate = log_path.with_name(f"{log_path.stem}.{timestamp}_{index}{log_path.suffix}")
        if not candidate.exists():
            return candidate
    return log_path.with_name(f"{log_path.stem}.{timestamp}_{datetime.now().microsecond}{log_path.suffix}")


def _delete_expired_log_archives(log_path: Path, retention_days: int) -> None:
    """删除超过保留天数的历史日志文件，当前日志文件始终保留。"""
    cutoff = datetime.now() - timedelta(days=max(1, int(retention_days)))
    for archive_path in _iter_log_archives(log_path):
        try:
            modified_at = datetime.fromtimestamp(archive_path.stat().st_mtime)
        except OSError:
            continue
        if modified_at >= cutoff:
            continue
        try:
            archive_path.unlink()
        except OSError:
            logging.getLogger(__name__).warning("failed to delete expired log archive: %s", archive_path)


def _iter_log_archives(log_path: Path) -> list[Path]:
    """列出当前主日志旁边的历史轮转日志文件。"""
    if not log_path.parent.exists():
        return []
    archives: list[Path] = []
    for path in log_path.parent.iterdir():
        if path == log_path or not path.is_file():
            continue
        if path.name.startswith(f"{log_path.name}."):
            archives.append(path)
            continue
        if path.suffix == log_path.suffix and path.stem.startswith(f"{log_path.stem}."):
            archives.append(path)
    return archives


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
        service.close()
        return 1

    try:
        return run_gui(http_server, service, store)
    finally:
        http_server.stop()
        service.close()
