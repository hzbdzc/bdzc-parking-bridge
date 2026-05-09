"""应用启动组装模块，负责串起配置、日志、存储、HTTP 进程和 GUI。"""

from __future__ import annotations

import ctypes
import logging
import sys
from ctypes import wintypes

from bdzc_parking.config import AppConfig
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.logging_setup import (
    RetentionRotatingFileHandler,
    rotate_current_log_file,
    setup_logging,
)
from bdzc_parking.storage import EventStore

LOGGER = logging.getLogger(__name__)

_POWER_REQUEST_CONTEXT_VERSION = 0
_POWER_REQUEST_CONTEXT_SIMPLE_STRING = 0x00000001
_POWER_REQUEST_DISPLAY_REQUIRED = 0
_POWER_REQUEST_SYSTEM_REQUIRED = 1
_ES_SYSTEM_REQUIRED = 0x00000001
_ES_DISPLAY_REQUIRED = 0x00000002
_ES_CONTINUOUS = 0x80000000
_INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
_POWER_REQUEST_REASON = "博达智创停车桥接程序运行中，阻止系统休眠和屏幕关闭"


class _ReasonContext(ctypes.Structure):
    """Windows PowerCreateRequest 使用的简单原因字符串结构。"""

    _fields_ = [
        ("Version", wintypes.ULONG),
        ("Flags", wintypes.DWORD),
        ("SimpleReasonString", wintypes.LPWSTR),
    ]


class RuntimePowerGuard:
    """在程序运行期间请求 Windows 保持系统和屏幕唤醒。"""

    def __init__(self) -> None:
        """初始化运行期电源请求状态。"""
        self._kernel32 = None
        self._handle: object | None = None
        self._request_types: list[int] = []
        self._using_thread_state = False
        self._active = False
        self._reason_context: _ReasonContext | None = None

    def acquire(self) -> None:
        """申请阻止系统自动休眠和屏幕关闭；失败时只记录日志。"""
        if self._active:
            return
        if sys.platform != "win32":
            LOGGER.info("runtime power guard skipped on non-Windows platform")
            return

        try:
            kernel32 = _load_kernel32()
        except Exception as exc:
            LOGGER.warning("failed to load Windows power API: %s", exc)
            return

        try:
            self._acquire_power_request(kernel32)
        except Exception as exc:
            LOGGER.warning("failed to acquire Windows power request; trying fallback: %s", exc)
            self._reset()
            self._acquire_thread_execution_state(kernel32)

    def release(self) -> None:
        """释放运行期电源请求，让系统恢复默认电源策略。"""
        if not self._active:
            return

        try:
            if self._handle is not None:
                self._release_power_request()
            elif self._using_thread_state:
                self._release_thread_execution_state()
        except Exception as exc:
            LOGGER.warning("failed to release runtime power guard: %s", exc)
        finally:
            self._reset()

    def _acquire_power_request(self, kernel32: object) -> None:
        """优先使用进程级 Power Request API 保持运行和屏幕常亮。"""
        reason_context = _ReasonContext(
            _POWER_REQUEST_CONTEXT_VERSION,
            _POWER_REQUEST_CONTEXT_SIMPLE_STRING,
            _POWER_REQUEST_REASON,
        )
        handle = kernel32.PowerCreateRequest(ctypes.byref(reason_context))
        if _is_invalid_handle(handle):
            raise OSError(f"PowerCreateRequest failed: {_last_windows_error_message()}")

        self._kernel32 = kernel32
        self._handle = handle
        self._reason_context = reason_context
        try:
            for request_type in (_POWER_REQUEST_SYSTEM_REQUIRED, _POWER_REQUEST_DISPLAY_REQUIRED):
                if not kernel32.PowerSetRequest(handle, request_type):
                    raise OSError(f"PowerSetRequest failed: {_last_windows_error_message()}")
                self._request_types.append(request_type)
        except Exception:
            self._release_power_request()
            raise

        self._active = True
        LOGGER.info("runtime power guard acquired via PowerSetRequest")

    def _release_power_request(self) -> None:
        """清理 Power Request API 句柄和已设置的请求类型。"""
        if self._kernel32 is None or self._handle is None:
            return

        for request_type in reversed(self._request_types):
            if not self._kernel32.PowerClearRequest(self._handle, request_type):
                LOGGER.warning(
                    "PowerClearRequest failed request_type=%s: %s",
                    request_type,
                    _last_windows_error_message(),
                )
        if not self._kernel32.CloseHandle(self._handle):
            LOGGER.warning("CloseHandle for power request failed: %s", _last_windows_error_message())
        LOGGER.info("runtime power guard released from PowerSetRequest")

    def _acquire_thread_execution_state(self, kernel32: object) -> None:
        """回退到线程执行状态 API 保持系统和屏幕唤醒。"""
        flags = _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED | _ES_DISPLAY_REQUIRED
        if not kernel32.SetThreadExecutionState(flags):
            LOGGER.warning("SetThreadExecutionState acquire failed: %s", _last_windows_error_message())
            return

        self._kernel32 = kernel32
        self._using_thread_state = True
        self._active = True
        LOGGER.info("runtime power guard acquired via SetThreadExecutionState")

    def _release_thread_execution_state(self) -> None:
        """释放线程执行状态 API 设置的运行期唤醒请求。"""
        if self._kernel32 is None:
            return
        if not self._kernel32.SetThreadExecutionState(_ES_CONTINUOUS):
            LOGGER.warning("SetThreadExecutionState release failed: %s", _last_windows_error_message())
            return
        LOGGER.info("runtime power guard released from SetThreadExecutionState")

    def _reset(self) -> None:
        """重置本对象记录的电源请求状态。"""
        self._handle = None
        self._request_types = []
        self._using_thread_state = False
        self._active = False
        self._reason_context = None


def _load_kernel32() -> object:
    """加载并声明本模块用到的 Windows kernel32 电源 API。"""
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.PowerCreateRequest.argtypes = [ctypes.POINTER(_ReasonContext)]
    kernel32.PowerCreateRequest.restype = wintypes.HANDLE
    kernel32.PowerSetRequest.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.PowerSetRequest.restype = wintypes.BOOL
    kernel32.PowerClearRequest.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    kernel32.PowerClearRequest.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.SetThreadExecutionState.argtypes = [wintypes.DWORD]
    kernel32.SetThreadExecutionState.restype = wintypes.DWORD
    return kernel32


def _is_invalid_handle(handle: object) -> bool:
    """判断 Windows HANDLE 是否表示创建失败。"""
    return handle in {None, 0, -1, _INVALID_HANDLE_VALUE}


def _last_windows_error_message() -> str:
    """返回最近一次 Windows API 错误码，供日志定位现场问题。"""
    get_last_error = getattr(ctypes, "get_last_error", None)
    if not callable(get_last_error):
        return "GetLastError unavailable"
    code = get_last_error()
    if code:
        return f"GetLastError={code}"
    return "GetLastError unavailable"


def main() -> int:
    """初始化桥接程序运行所需组件，并启动 Qt 图形界面。"""
    config = AppConfig.load()
    setup_logging(config.log_path)
    power_guard = RuntimePowerGuard()
    power_guard.acquire()

    http_server: BridgeHTTPServer | None = None
    try:
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

        return run_gui(http_server, store)
    finally:
        try:
            if http_server is not None:
                http_server.stop()
        finally:
            power_guard.release()
