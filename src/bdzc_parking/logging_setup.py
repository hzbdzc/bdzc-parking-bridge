"""日志初始化和轮转工具，供主进程与 HTTP 子进程复用。"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import BinaryIO, Iterator


LOG_RETENTION_DAYS = 180
LOG_MAX_BYTES = 2 * 1024 * 1024


class RetentionRotatingFileHandler(RotatingFileHandler):
    """按大小轮转日志，并按文件修改时间保留指定天数的历史日志。"""

    def __init__(self, filename: Path, max_bytes: int, retention_days: int, encoding: str):
        """保存保留天数，并为多进程写入准备旁路锁文件。"""
        super().__init__(
            filename,
            maxBytes=max_bytes,
            backupCount=0,
            encoding=encoding,
            delay=True,
        )
        self.retention_days = int(retention_days)
        self.last_rollover_path: Path | None = None
        self._lock_path = Path(self.baseFilename).with_name(f"{Path(self.baseFilename).name}.lock")
        self._file_lock_depth = 0

    def emit(self, record: logging.LogRecord) -> None:
        """加跨进程文件锁后写入一条日志，并在写完后释放当前文件句柄。"""
        try:
            with self._locked_file():
                super().emit(record)
        except Exception:
            self.handleError(record)
        finally:
            self._close_stream()

    def doRollover(self) -> None:  # noqa: N802
        """把当前日志重命名成带时间戳的历史文件，并清理过期历史日志。"""
        with self._locked_file():
            self._do_rollover_locked()

    @contextmanager
    def _locked_file(self) -> Iterator[None]:
        """用同目录 lock 文件串行化跨进程日志写入和轮转。"""
        if self._file_lock_depth > 0:
            self._file_lock_depth += 1
            try:
                yield
            finally:
                self._file_lock_depth -= 1
            return

        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock_path.open("a+b") as lock_file:
            _lock_file(lock_file)
            self._file_lock_depth = 1
            try:
                yield
            finally:
                self._file_lock_depth = 0
                _unlock_file(lock_file)

    def _do_rollover_locked(self) -> None:
        """在已持有跨进程锁时完成实际日志轮转。"""
        self._close_stream()

        base_path = Path(self.baseFilename)
        self.last_rollover_path = None
        if base_path.exists() and base_path.stat().st_size > 0:
            target_path = _next_log_archive_path(base_path)
            base_path.replace(target_path)
            self.last_rollover_path = target_path
            base_path.touch()

        _delete_expired_log_archives(base_path, self.retention_days)

    def _close_stream(self) -> None:
        """关闭当前日志文件句柄，避免 Windows 多进程轮转时文件被占用。"""
        if self.stream is None:
            return
        try:
            self.stream.flush()
        finally:
            self.stream.close()
            self.stream = None


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

    has_console_handler = any(type(handler) is logging.StreamHandler for handler in root.handlers)
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


def _lock_file(file_obj: BinaryIO) -> None:
    """在 Windows 或 POSIX 上锁住 lock 文件的第一个字节。"""
    file_obj.seek(0, os.SEEK_END)
    if file_obj.tell() == 0:
        file_obj.write(b"\0")
        file_obj.flush()
    file_obj.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(file_obj.fileno(), msvcrt.LK_LOCK, 1)
        return

    import fcntl

    fcntl.flock(file_obj.fileno(), fcntl.LOCK_EX)


def _unlock_file(file_obj: BinaryIO) -> None:
    """释放 lock 文件上的跨进程文件锁。"""
    file_obj.seek(0)
    if os.name == "nt":
        import msvcrt

        msvcrt.locking(file_obj.fileno(), msvcrt.LK_UNLCK, 1)
        return

    import fcntl

    fcntl.flock(file_obj.fileno(), fcntl.LOCK_UN)


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
        if path.name == f"{log_path.name}.lock":
            continue
        if path.name.startswith(f"{log_path.name}."):
            archives.append(path)
            continue
        if path.suffix == log_path.suffix and path.stem.startswith(f"{log_path.stem}."):
            archives.append(path)
    return archives
