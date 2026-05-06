"""配置窗口操作测试。"""

from __future__ import annotations

import os
import logging
from types import SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PySide6.QtWidgets import QApplication, QMessageBox

from bdzc_parking.config import AppConfig
from bdzc_parking.gui import ConfigDialog, MockSendDialog
from bdzc_parking.models import SendResult


def test_config_dialog_cleanup_button_confirms_and_schedules(
    monkeypatch, tmp_path
) -> None:
    """清理旧数据按钮确认后应调用 service 清理入口。"""
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
        assert any("service worker" in message for message in infos)
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
        dialog.config_fields["default_phone"].setText("13900000000")
        dialog.save_config()

        assert http_server.refresh_calls == []
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
    service = SimpleNamespace(config=AppConfig(config_path=tmp_path / "config.json"))
    monkeypatch.setattr("bdzc_parking.gui.PartnerClient", FakePartnerClient)
    caplog.set_level(logging.INFO, logger="bdzc_parking.gui")

    dialog = MockSendDialog(service)
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
