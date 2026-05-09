"""应用日志轮转策略测试。"""

from __future__ import annotations

import os
import logging
import sys
import time
import types

import pytest

import bdzc_parking.app as app_module
import bdzc_parking.logging_setup as logging_setup
from bdzc_parking.logging_setup import RetentionRotatingFileHandler


def test_log_default_rollover_limit_is_two_mb() -> None:
    """默认日志轮转阈值应符合 AGENTS 中 2M 的要求。"""
    assert logging_setup.LOG_MAX_BYTES == 2 * 1024 * 1024


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


def test_two_log_handlers_can_write_and_roll_over_same_file(tmp_path) -> None:
    """父进程和 HTTP 子进程各自的 handler 指向同一文件时不应因占用失败。"""
    log_path = tmp_path / "shared.log"
    first = RetentionRotatingFileHandler(log_path, max_bytes=120, retention_days=180, encoding="utf-8")
    second = RetentionRotatingFileHandler(log_path, max_bytes=120, retention_days=180, encoding="utf-8")
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "message %s", ("x" * 80,), None)
    try:
        first.emit(record)
        second.emit(record)
        first.emit(record)
    finally:
        first.close()
        second.close()

    assert log_path.exists()
    assert any(path.name.startswith("shared.") and path.suffix == ".log" for path in tmp_path.iterdir())
    assert (tmp_path / "shared.log.lock").exists()


def test_runtime_power_guard_uses_power_request_api(monkeypatch) -> None:
    """Windows 下应优先使用 Power Request API 同时保持系统和屏幕唤醒。"""
    kernel32 = _FakeKernel32()
    monkeypatch.setattr(app_module.sys, "platform", "win32")
    monkeypatch.setattr(app_module, "_load_kernel32", lambda: kernel32)

    guard = app_module.RuntimePowerGuard()
    guard.acquire()
    guard.release()

    assert kernel32.calls == [
        ("PowerCreateRequest",),
        ("PowerSetRequest", 100, app_module._POWER_REQUEST_SYSTEM_REQUIRED),
        ("PowerSetRequest", 100, app_module._POWER_REQUEST_DISPLAY_REQUIRED),
        ("PowerClearRequest", 100, app_module._POWER_REQUEST_DISPLAY_REQUIRED),
        ("PowerClearRequest", 100, app_module._POWER_REQUEST_SYSTEM_REQUIRED),
        ("CloseHandle", 100),
    ]


def test_runtime_power_guard_falls_back_to_thread_execution_state(monkeypatch) -> None:
    """Power Request API 不可用时，应回退到 SetThreadExecutionState。"""
    kernel32 = _FakeKernel32(create_handle=0)
    monkeypatch.setattr(app_module.sys, "platform", "win32")
    monkeypatch.setattr(app_module, "_load_kernel32", lambda: kernel32)

    guard = app_module.RuntimePowerGuard()
    guard.acquire()
    guard.release()

    assert ("SetThreadExecutionState", app_module._ES_CONTINUOUS | app_module._ES_SYSTEM_REQUIRED | app_module._ES_DISPLAY_REQUIRED) in kernel32.calls
    assert kernel32.calls[-1] == ("SetThreadExecutionState", app_module._ES_CONTINUOUS)


def test_runtime_power_guard_logs_failure_without_raising(monkeypatch, caplog) -> None:
    """所有底层电源 API 均失败时，只应记录告警而不影响程序运行。"""
    kernel32 = _FakeKernel32(create_handle=0, thread_results=[0])
    monkeypatch.setattr(app_module.sys, "platform", "win32")
    monkeypatch.setattr(app_module, "_load_kernel32", lambda: kernel32)
    caplog.set_level(logging.WARNING, logger="bdzc_parking.app")

    guard = app_module.RuntimePowerGuard()
    guard.acquire()
    guard.release()

    assert "failed to acquire Windows power request" in caplog.text
    assert "SetThreadExecutionState acquire failed" in caplog.text


def test_main_releases_power_guard_when_http_stop_fails(monkeypatch, tmp_path) -> None:
    """即使 HTTP server 停止失败，主流程也必须释放运行期电源请求。"""
    calls: list[str] = []
    config = types.SimpleNamespace(
        log_path=tmp_path / "app.log",
        db_path=tmp_path / "events.sqlite3",
        auto_start_server=False,
    )
    fake_gui = types.ModuleType("bdzc_parking.gui")
    fake_gui.run_gui = lambda _http_server, _store: calls.append("run_gui") or 0

    class FakePowerGuard:
        """记录主流程中的电源守护 acquire/release 调用。"""

        def acquire(self) -> None:
            """记录电源守护申请。"""
            calls.append("power_acquire")

        def release(self) -> None:
            """记录电源守护释放。"""
            calls.append("power_release")

    class FakeHTTPServer:
        """停止时抛错的 HTTP server 替身。"""

        def __init__(self, _config) -> None:
            """记录 HTTP server 创建。"""
            calls.append("http_init")

        def stop(self) -> None:
            """模拟 HTTP server 停止失败。"""
            calls.append("http_stop")
            raise RuntimeError("stop failed")

    monkeypatch.setattr(app_module.AppConfig, "load", lambda: config)
    monkeypatch.setattr(app_module, "setup_logging", lambda _log_path: calls.append("setup_logging"))
    monkeypatch.setattr(app_module, "RuntimePowerGuard", FakePowerGuard)
    monkeypatch.setattr(app_module, "EventStore", lambda _db_path: calls.append("store_init") or object())
    monkeypatch.setattr(app_module, "BridgeHTTPServer", FakeHTTPServer)
    monkeypatch.setitem(sys.modules, "bdzc_parking.gui", fake_gui)

    with pytest.raises(RuntimeError, match="stop failed"):
        app_module.main()

    assert calls == [
        "setup_logging",
        "power_acquire",
        "store_init",
        "http_init",
        "run_gui",
        "http_stop",
        "power_release",
    ]


class _FakeKernel32:
    """测试用 kernel32 电源 API 替身。"""

    def __init__(
        self,
        create_handle: int = 100,
        thread_results: list[int] | None = None,
    ) -> None:
        """保存 fake API 的返回值和调用记录。"""
        self.create_handle = create_handle
        self.thread_results = list(thread_results or [1, 1])
        self.calls: list[tuple[object, ...]] = []

    def PowerCreateRequest(self, _reason_context) -> int:  # noqa: N802
        """模拟创建 Power Request 句柄。"""
        self.calls.append(("PowerCreateRequest",))
        return self.create_handle

    def PowerSetRequest(self, handle: int, request_type: int) -> int:  # noqa: N802
        """模拟启用一种 Power Request。"""
        self.calls.append(("PowerSetRequest", handle, request_type))
        return 1

    def PowerClearRequest(self, handle: int, request_type: int) -> int:  # noqa: N802
        """模拟清理一种 Power Request。"""
        self.calls.append(("PowerClearRequest", handle, request_type))
        return 1

    def CloseHandle(self, handle: int) -> int:  # noqa: N802
        """模拟关闭 Windows HANDLE。"""
        self.calls.append(("CloseHandle", handle))
        return 1

    def SetThreadExecutionState(self, flags: int) -> int:  # noqa: N802
        """模拟线程执行状态 API。"""
        self.calls.append(("SetThreadExecutionState", flags))
        if self.thread_results:
            return self.thread_results.pop(0)
        return 1
