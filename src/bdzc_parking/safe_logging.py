"""可靠日志辅助工具，为普通日志失败时提供应急落盘。"""

from __future__ import annotations

import logging
import os
import sys
import tempfile
import threading
import traceback
from datetime import datetime
from pathlib import Path
from types import TracebackType


_MAX_LOG_BYTES = 10 * 1024 * 1024
_BACKUP_COUNT = 5
_EMERGENCY_LOCK = threading.RLock()
_EMERGENCY_LOG_PATH: Path | None = None
_HOOKS_INSTALLED = False
_ORIGINAL_SYS_EXCEPTHOOK = sys.excepthook
_ORIGINAL_THREADING_EXCEPTHOOK = threading.excepthook
_ORIGINAL_UNRAISABLEHOOK = sys.unraisablehook


class EmergencyLogHandler(logging.Handler):
    """把 error 级别日志和异常镜像到不依赖 logging emit 链路的应急日志。"""

    def __init__(self) -> None:
        """创建应急日志 handler，并只处理错误级别以上的记录。"""
        super().__init__(level=logging.ERROR)

    def emit(self, record: logging.LogRecord) -> None:
        """安全写入一条日志记录，失败时不能再向外抛出异常。"""
        try:
            if record.levelno < logging.ERROR and not record.exc_info:
                return
            _write_emergency_text(_format_log_record(record))
        except Exception:
            return


def configure_emergency_logging(log_path: Path) -> Path:
    """根据主日志路径配置应急日志路径，并返回最终候选路径。"""
    global _EMERGENCY_LOG_PATH
    path = Path(log_path)
    emergency_name = f"{path.stem}.emergency{path.suffix or '.log'}"
    _EMERGENCY_LOG_PATH = path.with_name(emergency_name)
    return _EMERGENCY_LOG_PATH


def emergency_log_path() -> Path:
    """返回当前配置的应急日志路径；未配置时返回系统临时目录路径。"""
    if _EMERGENCY_LOG_PATH is not None:
        return _EMERGENCY_LOG_PATH
    return Path(tempfile.gettempdir()) / "bdzc_parking.emergency.log"


def ensure_emergency_handler(root: logging.Logger | None = None) -> None:
    """确保 root logger 的最前面有一个不会抛异常的应急 handler。"""
    target = root if root is not None else logging.getLogger()
    for handler in target.handlers:
        if isinstance(handler, EmergencyLogHandler):
            return
    target.handlers.insert(0, EmergencyLogHandler())


def install_global_exception_hooks() -> None:
    """安装进程级异常钩子，覆盖主线程、后台线程和 unraisable 异常。"""
    global _HOOKS_INSTALLED
    if _HOOKS_INSTALLED:
        return
    sys.excepthook = _sys_excepthook
    threading.excepthook = _threading_excepthook
    sys.unraisablehook = _unraisablehook
    _HOOKS_INSTALLED = True


def log_exception(
    logger: logging.Logger,
    message: str,
    *args: object,
    exc_info: bool | tuple[type[BaseException], BaseException, TracebackType | None] | None = True,
    **kwargs: object,
) -> None:
    """记录异常；若 logging handler 本身失败，则直接写入应急日志。"""
    exception_info = _normalize_exc_info(exc_info)
    formatted_message = _format_percent_message(message, args)
    safe_log_exception(formatted_message, exception_info)
    try:
        logger.error(message, *args, exc_info=exception_info, **kwargs)
    except Exception:
        safe_log_exception(
            f"logging failed while recording: {formatted_message}",
            sys.exc_info(),
        )


def safe_log_exception(
    message: str,
    exc_info: tuple[type[BaseException], BaseException, TracebackType | None] | None = None,
) -> None:
    """不经过 logging handler，直接把异常信息写入应急日志。"""
    exception_info = exc_info if exc_info is not None else sys.exc_info()
    _write_emergency_text(_format_exception_text(message, exception_info))


def _sys_excepthook(
    exc_type: type[BaseException],
    exc_value: BaseException,
    exc_traceback: TracebackType | None,
) -> None:
    """记录主线程未捕获异常，并继续调用原始 excepthook。"""
    safe_log_exception("uncaught exception", (exc_type, exc_value, exc_traceback))
    _call_previous_hook(_ORIGINAL_SYS_EXCEPTHOOK, exc_type, exc_value, exc_traceback)


def _threading_excepthook(args: threading.ExceptHookArgs) -> None:
    """记录后台线程未捕获异常，并继续调用原始 threading excepthook。"""
    thread_name = args.thread.name if args.thread is not None else "unknown"
    safe_log_exception(
        f"uncaught thread exception thread={thread_name}",
        (args.exc_type, args.exc_value, args.exc_traceback),
    )
    _call_previous_hook(_ORIGINAL_THREADING_EXCEPTHOOK, args)


def _unraisablehook(args: sys.UnraisableHookArgs) -> None:
    """记录析构或回调中的 unraisable 异常，并继续调用原始 hook。"""
    safe_log_exception(
        f"unraisable exception object={args.object!r} err_msg={args.err_msg!r}",
        (args.exc_type, args.exc_value, args.exc_traceback),
    )
    _call_previous_hook(_ORIGINAL_UNRAISABLEHOOK, args)


def _call_previous_hook(hook, *args: object) -> None:
    """调用 Python 原始异常钩子，并隔离钩子自身异常。"""
    try:
        hook(*args)
    except Exception:
        safe_log_exception("previous exception hook failed", sys.exc_info())


def _normalize_exc_info(
    exc_info: bool | tuple[type[BaseException], BaseException, TracebackType | None] | None,
) -> tuple[type[BaseException], BaseException, TracebackType | None] | None:
    """把 logging 风格的 exc_info 参数转换为明确的异常三元组。"""
    if exc_info is None:
        return None
    if exc_info is True:
        current = sys.exc_info()
        if current[0] is None or current[1] is None:
            return None
        return current
    if exc_info is False:
        return None
    return exc_info


def _format_log_record(record: logging.LogRecord) -> str:
    """把 logging 记录格式化成应急日志中的纯文本块。"""
    header = _emergency_header(record.levelname, record.name, record.getMessage())
    parts = [header]
    if record.exc_info:
        parts.append("".join(traceback.format_exception(*record.exc_info)).rstrip())
    if record.stack_info:
        parts.append(str(record.stack_info).rstrip())
    return "\n".join(part for part in parts if part)


def _format_exception_text(
    message: str,
    exc_info: tuple[type[BaseException] | None, BaseException | None, TracebackType | None] | None,
) -> str:
    """把手工记录的异常格式化成应急日志中的纯文本块。"""
    parts = [_emergency_header("ERROR", "bdzc_parking.safe_logging", message)]
    if exc_info and exc_info[0] is not None and exc_info[1] is not None:
        parts.append("".join(traceback.format_exception(*exc_info)).rstrip())
    return "\n".join(parts)


def _emergency_header(level: str, logger_name: str, message: str) -> str:
    """生成应急日志每条记录的首行，包含进程与线程信息。"""
    now = datetime.now().isoformat(timespec="seconds")
    thread = threading.current_thread()
    return (
        f"{now} {level} [{logger_name}] pid={os.getpid()} "
        f"thread={thread.name}/{thread.ident} {message}"
    )


def _write_emergency_text(text: str) -> None:
    """向配置路径写入应急日志，失败时再尝试系统临时目录。"""
    data = text.rstrip() + "\n"
    for path in _emergency_path_candidates():
        try:
            with _EMERGENCY_LOCK:
                path.parent.mkdir(parents=True, exist_ok=True)
                _rotate_if_needed(path)
                with path.open("a", encoding="utf-8") as file:
                    file.write(data)
            return
        except Exception:
            continue


def _emergency_path_candidates() -> list[Path]:
    """返回应急日志首选路径和临时目录兜底路径。"""
    primary = emergency_log_path()
    fallback = Path(tempfile.gettempdir()) / "bdzc_parking.emergency.log"
    if primary == fallback:
        return [primary]
    return [primary, fallback]


def _rotate_if_needed(path: Path) -> None:
    """在应急日志超过大小限制时做简单数字后缀轮转。"""
    if not path.exists() or path.stat().st_size < _MAX_LOG_BYTES:
        return
    oldest = path.with_name(f"{path.name}.{_BACKUP_COUNT}")
    if oldest.exists():
        oldest.unlink()
    for index in range(_BACKUP_COUNT - 1, 0, -1):
        source = path.with_name(f"{path.name}.{index}")
        target = path.with_name(f"{path.name}.{index + 1}")
        if source.exists():
            os.replace(source, target)
    os.replace(path, path.with_name(f"{path.name}.1"))


def _format_percent_message(message: str, args: tuple[object, ...]) -> str:
    """用 logging 的百分号风格尽力格式化消息。"""
    if not args:
        return message
    try:
        return message % args
    except Exception:
        return f"{message} args={args!r}"
