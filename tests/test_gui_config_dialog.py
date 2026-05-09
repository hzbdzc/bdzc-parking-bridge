"""配置窗口操作测试。"""

from __future__ import annotations

import os
import logging
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QMessageBox

from bdzc_parking.config import AppConfig
from bdzc_parking.gui import ConfigDialog, MainWindow, MockSendDialog
from bdzc_parking.models import SendResult
from bdzc_parking.storage import EventStore
from helpers import sample_event


def test_config_dialog_cleanup_button_confirms_and_schedules(
    monkeypatch, tmp_path
) -> None:
    """清理旧数据按钮确认后应调用 HTTP 子进程清理入口。"""
    app = QApplication.instance() or QApplication([])
    calls: list[str] = []
    infos: list[str] = []
    http_server = SimpleNamespace(config=AppConfig(config_path=tmp_path / "config.json"))

    monkeypatch.setattr(
        QMessageBox,
        "warning",
        lambda *args, **kwargs: QMessageBox.StandardButton.Yes,
    )
    monkeypatch.setattr(
        QMessageBox,
        "information",
        lambda _parent, _title, message, *args, **kwargs: infos.append(str(message)),
    )

    dialog = ConfigDialog(
        http_server,
        cleanup_handler=lambda: calls.append("cleanup") is None or True,
    )
    try:
        assert dialog.cleanup_button.isEnabled()

        dialog.cleanup_old_data()

        assert calls == ["cleanup"]
        assert any("HTTP 子进程" in message for message in infos)
    finally:
        dialog.close()
        app.processEvents()


def test_config_dialog_refreshes_http_server_for_http_config(monkeypatch, tmp_path) -> None:
    """保存 HTTP 相关配置后，应要求运行中的 HTTP server 刷新子进程。"""

    class FakeHTTPServer:
        """记录 refresh 调用的最小 HTTP server 替身。"""

        def __init__(self) -> None:
            self.config = AppConfig(config_path=tmp_path / "config.json")
            self.refresh_calls: list[str] = []

        def refresh(self, reason: str) -> None:
            """记录刷新原因。"""
            self.refresh_calls.append(reason)

    app = QApplication.instance() or QApplication([])
    http_server = FakeHTTPServer()
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: None)

    dialog = ConfigDialog(http_server)
    try:
        dialog.config_fields["listen_path"].setText("/new-park")
        dialog.save_config()

        assert http_server.refresh_calls == ["config saved"]
    finally:
        dialog.close()
        app.processEvents()


def test_config_dialog_does_not_refresh_http_server_for_non_http_config(monkeypatch, tmp_path) -> None:
    """保存非 HTTP 配置时，不应重启 HTTP server。"""

    class FakeHTTPServer:
        """记录 refresh 调用的最小 HTTP server 替身。"""

        def __init__(self) -> None:
            self.config = AppConfig(config_path=tmp_path / "config.json")
            self.refresh_calls: list[str] = []

        def refresh(self, reason: str) -> None:
            """记录刷新原因。"""
            self.refresh_calls.append(reason)

    app = QApplication.instance() or QApplication([])
    http_server = FakeHTTPServer()
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: None)

    dialog = ConfigDialog(http_server)
    try:
        dialog.config_fields["auto_start_server"].setChecked(
            not http_server.config.auto_start_server
        )
        dialog.save_config()

        assert http_server.refresh_calls == []
    finally:
        dialog.close()
        app.processEvents()


def test_config_dialog_refreshes_http_server_for_runtime_partner_config(
    monkeypatch, tmp_path
) -> None:
    """Saving runtime partner fields should refresh the HTTP child process."""

    class FakeHTTPServer:
        """Minimal HTTP server double that records refresh calls."""

        def __init__(self) -> None:
            self.config = AppConfig(config_path=tmp_path / "config.json")
            self.refresh_calls: list[str] = []

        def refresh(self, reason: str) -> None:
            """Record the refresh reason."""
            self.refresh_calls.append(reason)

    app = QApplication.instance() or QApplication([])
    http_server = FakeHTTPServer()
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)
    monkeypatch.setattr(QMessageBox, "warning", lambda *args, **kwargs: None)

    dialog = ConfigDialog(http_server)
    try:
        dialog.config_fields["default_phone"].setText("13900000000")
        dialog.config_fields["local_entry_cid"].setText("ENTRY-NEW")
        dialog.save_config()

        assert http_server.refresh_calls == ["config saved"]
    finally:
        dialog.close()
        app.processEvents()


def test_config_dialog_rotate_log_button_confirms_and_rotates(monkeypatch, tmp_path) -> None:
    """轮转日志按钮确认后应调用当前日志轮转入口。"""
    app = QApplication.instance() or QApplication([])
    calls: list[str] = []
    infos: list[str] = []
    http_server = SimpleNamespace(config=AppConfig(config_path=tmp_path / "config.json"))

    monkeypatch.setattr(
        QMessageBox,
        "warning",
        lambda *args, **kwargs: QMessageBox.StandardButton.Yes,
    )
    monkeypatch.setattr(
        QMessageBox,
        "information",
        lambda _parent, _title, message, *args, **kwargs: infos.append(str(message)),
    )

    dialog = ConfigDialog(
        http_server,
        log_rotate_handler=lambda: calls.append("rotate") or (tmp_path / "logs" / "old.log"),
    )
    try:
        assert dialog.rotate_log_button.isEnabled()

        dialog.rotate_log_file()

        assert calls == ["rotate"]
        assert any("历史日志已保存" in message for message in infos)
    finally:
        dialog.close()
        app.processEvents()


def test_main_window_refresh_table_skips_unchanged_table_rebuild(tmp_path) -> None:
    """列表签名不变时，周期刷新不应重建表格或刷新筛选项。"""

    class FakeHTTPServer:
        """MainWindow 需要的最小 HTTP server 替身。"""

        def __init__(self) -> None:
            self.config = AppConfig(config_path=tmp_path / "config.json")
            self.is_running = False

        def get_control_snapshot(self) -> dict[str, object]:
            """返回静止状态的按钮数据。"""
            return {"button_text": "开始 HTTP server", "button_enabled": True, "display_text": "未运行"}

        def get_lifecycle_snapshot(self) -> dict[str, object]:
            """返回静止状态的 lifecycle 数据。"""
            return {"state": "stopped"}

        def get_runtime_snapshot(self) -> dict[str, object]:
            """返回静止状态的 runtime 数据。"""
            return {"health": {}}

        def stop(self) -> None:
            """关闭窗口时兼容 MainWindow.closeEvent。"""
            return None

    app = QApplication.instance() or QApplication([])
    store = EventStore(tmp_path / "events.sqlite3")
    event = sample_event("20260412_063354_226439_body.bin")
    event_id, _ = store.add_event(event, "pending", True, partner_payload={"car": event.plate_no})
    window = MainWindow(FakeHTTPServer(), store)
    window.refresh_timer.stop()
    calls = {"list_events": 0, "filter_values": 0}
    original_list_events = store.list_events
    original_filter_values = store.list_event_filter_values

    def list_events(*args, **kwargs):
        calls["list_events"] += 1
        return original_list_events(*args, **kwargs)

    def list_event_filter_values(*args, **kwargs):
        calls["filter_values"] += 1
        return original_filter_values(*args, **kwargs)

    store.list_events = list_events
    store.list_event_filter_values = list_event_filter_values
    try:
        window.refresh_table()
        assert calls == {"list_events": 0, "filter_values": 0}

        with store._connect() as conn:
            conn.execute(
                """
                UPDATE events
                SET status = 'sent', attempts = 1, updated_at = '2026-04-12T06:33:56'
                WHERE id = ?
                """,
                (event_id,),
            )
        window.refresh_table()

        assert calls["list_events"] == 1
        assert calls["filter_values"] == 0
    finally:
        window._force_close = True
        window.close()
        app.processEvents()


def test_manual_resend_requires_running_http_server(monkeypatch) -> None:
    """HTTP server 未运行时，详情页手动重发应直接拒绝。"""
    infos: list[str] = []
    questions: list[str] = []
    submissions: list[object] = []
    row = {
        "id": 12,
        "partner_payload_json": '{"car":"浙A12345"}',
        "plate_no": "浙A12345",
        "event_time": "2026-04-12T06:33:55",
        "received_at": "2026-04-12T06:33:56",
    }
    window = SimpleNamespace(
        _selected_event_id=lambda: 12,
        detail_panel=SimpleNamespace(current_event_id=None),
        last_selected_event_id=None,
        store=SimpleNamespace(get_event=lambda event_id: row),
        http_server=SimpleNamespace(
            is_running=False,
            submit_resend=lambda event_id: submissions.append(event_id),
        ),
        _submit_admin_task=lambda *args, **kwargs: submissions.append(args),
    )
    monkeypatch.setattr(
        QMessageBox,
        "information",
        lambda _parent, _title, message, *args, **kwargs: infos.append(str(message)),
    )
    monkeypatch.setattr(
        QMessageBox,
        "question",
        lambda *args, **kwargs: questions.append("asked") or QMessageBox.StandardButton.Yes,
    )

    MainWindow.manual_resend_selected(window)

    assert any("HTTP server 未运行" in message for message in infos)
    assert questions == []
    assert submissions == []


def test_manual_resend_uses_http_admin_when_running(monkeypatch) -> None:
    """HTTP server 运行时，详情页手动重发仍应提交 admin resend 任务。"""
    submitted: list[tuple[str, dict[str, object]]] = []
    row = {
        "id": 12,
        "partner_payload_json": '{"car":"浙A12345"}',
        "plate_no": "浙A12345",
        "event_time": "2026-04-12T06:33:55",
        "received_at": "2026-04-12T06:33:56",
    }
    http_server = SimpleNamespace(
        is_running=True,
        config=AppConfig(),
        submit_resend=lambda event_id: {"task_id": f"task-{event_id}"},
    )
    window = SimpleNamespace(
        _selected_event_id=lambda: 12,
        detail_panel=SimpleNamespace(current_event_id=None),
        last_selected_event_id=None,
        store=SimpleNamespace(get_event=lambda event_id: row),
        http_server=http_server,
    )

    def submit_admin_task(kind: str, submitter) -> bool:
        submitted.append((kind, submitter()))
        return True

    window._submit_admin_task = submit_admin_task
    monkeypatch.setattr(
        QMessageBox,
        "question",
        lambda *args, **kwargs: QMessageBox.StandardButton.Yes,
    )
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)

    MainWindow.manual_resend_selected(window)

    assert submitted == [("resend", {"task_id": "task-12"})]


def test_manual_resend_allows_rebuildable_payload_without_saved_json(monkeypatch) -> None:
    """记录未预存 payload 但字段足够时，GUI 应允许提交手动重发。"""
    submitted: list[tuple[str, dict[str, object]]] = []
    row = {
        "id": 12,
        "event_key": "event-12",
        "partner_payload_json": "{}",
        "direction": "enter",
        "passing_type": "plateRecognition",
        "plate_no": "浙A12345",
        "event_time": "2026-04-12T06:33:55+08:00",
        "received_at": "2026-04-12T06:33:56",
        "gate_name": "",
        "lane_name": "",
        "lane_id": "",
        "image_path": "",
    }
    http_server = SimpleNamespace(
        is_running=True,
        config=AppConfig(),
        submit_resend=lambda event_id: {"task_id": f"task-{event_id}"},
    )
    window = SimpleNamespace(
        _selected_event_id=lambda: 12,
        detail_panel=SimpleNamespace(current_event_id=None),
        last_selected_event_id=None,
        store=SimpleNamespace(get_event=lambda event_id: row),
        http_server=http_server,
    )

    def submit_admin_task(kind: str, submitter) -> bool:
        submitted.append((kind, submitter()))
        return True

    window._submit_admin_task = submit_admin_task
    monkeypatch.setattr(
        QMessageBox,
        "question",
        lambda *args, **kwargs: QMessageBox.StandardButton.Yes,
    )
    monkeypatch.setattr(QMessageBox, "information", lambda *args, **kwargs: None)

    MainWindow.manual_resend_selected(window)

    assert submitted == [("resend", {"task_id": "task-12"})]


def test_manual_resend_rejects_unbuildable_payload(monkeypatch) -> None:
    """HTTP server 运行时，字段不足的记录仍不应提交手动重发。"""
    infos: list[str] = []
    questions: list[str] = []
    submissions: list[object] = []
    row = {
        "id": 12,
        "event_key": "event-12",
        "partner_payload_json": "{}",
        "direction": "enter",
        "plate_no": "无车牌",
        "event_time": "2026-04-12T06:33:55+08:00",
        "image_path": "",
    }
    window = SimpleNamespace(
        _selected_event_id=lambda: 12,
        detail_panel=SimpleNamespace(current_event_id=None),
        last_selected_event_id=None,
        store=SimpleNamespace(get_event=lambda event_id: row),
        http_server=SimpleNamespace(
            is_running=True,
            config=AppConfig(),
            submit_resend=lambda event_id: submissions.append(event_id),
        ),
        _submit_admin_task=lambda *args, **kwargs: submissions.append(args),
    )
    monkeypatch.setattr(
        QMessageBox,
        "information",
        lambda _parent, _title, message, *args, **kwargs: infos.append(str(message)),
    )
    monkeypatch.setattr(
        QMessageBox,
        "question",
        lambda *args, **kwargs: questions.append("asked") or QMessageBox.StandardButton.Yes,
    )

    MainWindow.manual_resend_selected(window)

    assert any("没有可发送" in message for message in infos)
    assert questions == []
    assert submissions == []


def test_mock_send_logs_partner_result(monkeypatch, tmp_path, caplog) -> None:
    """模拟发送结束时应按正式发送结果日志的字段记录一行 info 日志。"""

    class FakePartnerClient:
        """替代真实 HTTP 客户端，避免测试访问网络。"""

        def __init__(self, config: AppConfig):
            self.config = config

        def send_once(self, payload: dict[str, object], attempt: int = 1) -> SendResult:
            """返回一个成功的大园区响应。"""
            return SendResult(True, attempt, 200, '{"status":200,"msg":"ok"}')

    app = QApplication.instance() or QApplication([])
    config = AppConfig(config_path=tmp_path / "config.json")
    monkeypatch.setattr("bdzc_parking.gui.PartnerClient", FakePartnerClient)
    caplog.set_level(logging.INFO, logger="bdzc_parking.gui")

    dialog = MockSendDialog(config)
    try:
        dialog._send_payload({"car": "浙A12345"}, "http://example.test/api")

        assert "partner mock send result event_id=mock" in caplog.text
        assert "attempt=1" in caplog.text
        assert "success=yes" in caplog.text
        assert "status_code=200" in caplog.text
        assert "final_status=sent" in caplog.text
    finally:
        dialog.close()
        app.processEvents()
