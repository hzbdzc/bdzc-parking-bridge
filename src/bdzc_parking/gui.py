"""Qt 图形界面，用于控制 HTTP server、查看记录和手动发送。"""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from PySide6.QtCore import QDateTime, QEvent, Qt, QObject, QTimer, QUrl, Signal
from PySide6.QtGui import QAction, QColor, QDesktopServices, QIcon, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QDialog,
    QDialogButtonBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTextBrowser,
    QTextEdit,
    QSystemTrayIcon,
    QVBoxLayout,
    QWidget,
)

from bdzc_parking.config import AppConfig
from bdzc_parking.http_server import BridgeHTTPServer
from bdzc_parking.models import HikEvent, SendResult, map_to_partner_payload
from bdzc_parking.sender import PartnerClient
from bdzc_parking.service import ParkingBridgeService
from bdzc_parking.storage import EventStore, backup_database_and_reset, load_partner_payload


CONFIG_FIELD_GROUPS = [
    (
        "大园区接口",
        (
            "partner_api_url",
            "external_url_base",
            "park_id",
            "default_phone",
            "local_exit_hobby",
            "local_exit_cid",
            "local_exit_cname",
            "local_entry_hobby",
            "local_entry_cid",
            "local_entry_cname",
        ),
    ),
    (
        "桥接与发送",
        (
            "retry_count",
            "retry_delay_seconds",
            "request_timeout_seconds",
            "max_event_age_seconds",
            "http_watchdog_interval_seconds",
            "http_watchdog_timeout_seconds",
            "http_watchdog_failure_threshold",
            "http_watchdog_restart_cooldown_seconds",
            "event_page_size",
        ),
    ),
]


CONFIG_FIELD_LABELS = {
    "listen_host": "监听地址",
    "listen_port": "监听端口",
    "listen_path": "接收路径",
    "auto_start_server": "自动开启 HTTP server",
    "partner_api_url": "大园区 API 地址",
    "external_url_base": "外部图片 URL base",
    "park_id": "大园区停车场 ID",
    "local_exit_hobby": "博达出口 hobby",
    "local_exit_cid": "博达出口 CID",
    "local_exit_cname": "博达出口名称",
    "local_entry_hobby": "博达入口 hobby",
    "local_entry_cid": "博达入口 CID",
    "local_entry_cname": "博达入口名称",
    "default_phone": "默认手机号",
    "retry_count": "重试次数",
    "retry_delay_seconds": "重试间隔秒",
    "request_timeout_seconds": "请求超时秒",
    "max_event_age_seconds": "过旧跳过秒数",
    "http_watchdog_interval_seconds": "HTTP 守护探测间隔秒",
    "http_watchdog_timeout_seconds": "HTTP 守护探测超时秒",
    "http_watchdog_failure_threshold": "HTTP 守护失败次数",
    "http_watchdog_restart_cooldown_seconds": "HTTP 守护重启冷却秒",
    "event_page_size": "每页记录数",
    "db_path": "数据库路径",
    "log_path": "日志路径",
}

CONFIG_FIELD_TOOLTIPS = {
    "listen_host": "HTTP server 固定监听所有网卡地址 0.0.0.0，海康终端只需要访问本机实际 IP。",
    "listen_port": "海康终端向本程序回调时使用的端口；若程序正在监听，修改后需要停止并重新开始 HTTP server。",
    "listen_path": "海康终端上报消息时访问的 URL 路径，例如 /park；修改后会立即影响后续请求。",
    "auto_start_server": "勾选后，程序下次启动时会自动开始监听 HTTP server。",
    "partner_api_url": "大园区停车系统接收过车数据的 HTTP API 地址。",
    "external_url_base": "图片对外访问的 URL 前缀，例如 https://host/parking-images；保存后会立即影响后续生成的图片外链。",
    "park_id": "发送给大园区 API 的停车场 ID。",
    "local_exit_hobby": "博达出口车辆出博达园区、进入上园路时发送的大园区类型，通常为 in。",
    "local_exit_cid": "博达出口通道 CID；车辆出博达园区、进入上园路时，发送 hobby=in。",
    "local_exit_cname": "博达出口通道名称；车辆出博达园区、进入上园路时，发送 hobby=in。",
    "local_entry_hobby": "博达入口车辆出上园路、进入博达园区时发送的大园区类型，通常为 out。",
    "local_entry_cid": "博达入口通道 CID；车辆出上园路、进入博达园区时，发送 hobby=out。",
    "local_entry_cname": "博达入口通道名称；车辆出上园路、进入博达园区时，发送 hobby=out。",
    "default_phone": "大园区 payload 中默认填充的手机号，模拟发送也会默认使用这个值。",
    "retry_count": "首次发送失败后额外重试的次数，0 表示只发 1 次。",
    "retry_delay_seconds": "每次重试之间等待的秒数。",
    "request_timeout_seconds": "向大园区 API 发起 HTTP 请求时的超时秒数，必须大于 0。",
    "max_event_age_seconds": "过车时间相对收到时间超过这个秒数时，自动跳过发送，但仍会保留记录。",
    "http_watchdog_interval_seconds": "HTTP server 运行时，每隔多少秒从本机探测一次 GET /。",
    "http_watchdog_timeout_seconds": "HTTP server 健康探测的单次超时秒数，必须大于 0。",
    "http_watchdog_failure_threshold": "连续多少次健康探测失败后，自动重启 HTTP server。",
    "http_watchdog_restart_cooldown_seconds": "自动重启失败或刚重启后，至少等待多少秒才允许再次重启。",
    "event_page_size": "主列表每页显示的记录数；默认 1000，翻页可查看更旧记录。",
    "db_path": "SQLite 数据库文件路径；可通过配置文件修改，变更后需要重启程序生效。",
    "log_path": "程序日志文件路径；可通过配置文件修改，变更后需要重启程序生效。",
}

READONLY_CONFIG_FIELDS = ("db_path", "log_path")
OPTIONAL_CONFIG_FIELDS = {"external_url_base"}

DIRECTION_LABELS = {
    "enter": "我方入口进场",
    "exit": "我方出口出场",
}

PASSING_TYPE_LABELS = {
    "plateRecognition": "车牌识别",
    "stop": "停车触发",
    "manual": "手动放行",
}

STATUS_LABELS = {
    "pending": "待发送",
    "sending": "发送中",
    "failed_retryable": "待重试",
    "dead_letter": "死信",
    "sent": "已发送",
    "failed": "发送失败",
    "skipped": "已跳过",
    "parse_error": "解析失败",
}


class BridgeSignals(QObject):
    """把后台线程事件转换成 Qt 主线程可处理的信号。"""

    changed = Signal(int)


class MockSignals(QObject):
    """把 mock 发送线程结果转换成 Qt 主线程信号。"""

    finished = Signal(object, object, object)


class SortableTableWidgetItem(QTableWidgetItem):
    """支持按隐藏排序键排序的表格单元格。"""

    def __init__(self, text: str, sort_value: object | None = None):
        """保存显示文本，并在需要时挂载排序键。"""
        super().__init__(text)
        if sort_value is not None:
            self.setData(Qt.ItemDataRole.UserRole, sort_value)

    def __lt__(self, other: QTableWidgetItem) -> bool:
        """优先按 UserRole 中的排序键比较，缺失时退回默认逻辑。"""
        left = self.data(Qt.ItemDataRole.UserRole)
        right = other.data(Qt.ItemDataRole.UserRole)
        if left is not None and right is not None:
            try:
                return left < right
            except TypeError:
                return str(left) < str(right)
        return super().__lt__(other)



class ConfigDialog(QDialog):
    """配置编辑弹窗。"""

    def __init__(
        self,
        http_server: BridgeHTTPServer,
        reset_database_handler: Callable[[], Path] | None = None,
        parent: QWidget | None = None,
    ):
        """绑定 HTTP server 配置对象并创建配置输入框。"""
        super().__init__(parent)
        self.http_server = http_server
        self.reset_database_handler = reset_database_handler
        self.config_fields: dict[str, QLineEdit | QCheckBox] = {}
        self.readonly_fields: dict[str, QLabel] = {}
        self._original_config = replace(self.http_server.config)
        self._saved = False

        self.setWindowTitle("配置")
        self.setWindowIcon(_app_icon())
        self.resize(1020, 680)
        self._build_ui()
        self._load_config_fields()

    def _build_ui(self) -> None:
        """创建结构化配置表单、读写按钮和关闭按钮。"""
        listen_group = self._build_listen_group()
        partner_group = self._build_config_group("大园区接口", CONFIG_FIELD_GROUPS[0][1])
        bridge_group = self._build_config_group("桥接与发送", CONFIG_FIELD_GROUPS[1][1])

        groups_layout = QGridLayout()
        groups_layout.setHorizontalSpacing(18)
        groups_layout.setVerticalSpacing(16)
        groups_layout.addWidget(partner_group, 0, 0, 1, 2)
        groups_layout.addWidget(listen_group, 1, 0)
        groups_layout.addWidget(bridge_group, 1, 1)
        groups_layout.setColumnStretch(0, 1)
        groups_layout.setColumnStretch(1, 1)

        self.load_button = QPushButton("载入配置")
        self.load_button.clicked.connect(self.load_config)
        self.export_button = QPushButton("存到文件")
        self.export_button.clicked.connect(self.save_to_file)
        self.reset_database_button = QPushButton("备份并启用新数据库")
        self.reset_database_button.clicked.connect(self.reset_database)
        self.reset_database_button.setEnabled(self.reset_database_handler is not None)
        self.save_button = QPushButton("保存配置")
        self.save_button.clicked.connect(self.save_config)
        self.close_button = QPushButton("取消")
        self.close_button.clicked.connect(self.reject)

        button_bar = QHBoxLayout()
        button_bar.addWidget(self.load_button)
        button_bar.addWidget(self.export_button)
        button_bar.addWidget(self.reset_database_button)
        button_bar.addStretch(1)
        button_bar.addWidget(self.save_button)
        button_bar.addWidget(self.close_button)

        layout = QVBoxLayout()
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(16)
        layout.addLayout(groups_layout)
        layout.addStretch(1)
        layout.addLayout(button_bar)
        self.setLayout(layout)

    def _build_listen_group(self) -> QGroupBox:
        """创建监听相关配置分组，并合并运行文件路径展示。"""
        group = QGroupBox("监听设置")
        form = self._new_form_layout()

        host_label = QLabel(CONFIG_FIELD_LABELS["listen_host"])
        host_label.setToolTip(CONFIG_FIELD_TOOLTIPS["listen_host"])
        host_value = self._readonly_value_label()
        host_value.setText("0.0.0.0 (所有 IP)")
        host_value.setToolTip(CONFIG_FIELD_TOOLTIPS["listen_host"])
        form.addRow(host_label, host_value)

        for key in ("listen_port", "listen_path", "auto_start_server"):
            self._add_config_row(form, key)
        for key in READONLY_CONFIG_FIELDS:
            self._add_readonly_row(form, key)

        group.setLayout(form)
        return group

    def _build_config_group(self, title: str, keys: tuple[str, ...]) -> QGroupBox:
        """按字段列表创建一个可编辑配置分组。"""
        group = QGroupBox(title)
        form = self._new_form_layout()
        for key in keys:
            self._add_config_row(form, key)
        group.setLayout(form)
        return group

    def _add_readonly_row(self, form: QFormLayout, key: str) -> None:
        """向表单中添加一个只读路径配置项。"""
        label = QLabel(CONFIG_FIELD_LABELS[key])
        label.setToolTip(CONFIG_FIELD_TOOLTIPS[key])
        value_label = self._readonly_value_label()
        value_label.setToolTip(CONFIG_FIELD_TOOLTIPS[key])
        self.readonly_fields[key] = value_label
        form.addRow(label, value_label)

    def _new_form_layout(self) -> QFormLayout:
        """创建统一样式的配置表单布局。"""
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        form.setFormAlignment(Qt.AlignmentFlag.AlignTop)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)
        form.setHorizontalSpacing(16)
        form.setVerticalSpacing(10)
        return form

    def _add_config_row(self, form: QFormLayout, key: str) -> None:
        """向表单中添加一个带 tooltip 的可编辑配置项。"""
        tooltip = CONFIG_FIELD_TOOLTIPS[key]
        label = QLabel(CONFIG_FIELD_LABELS[key])
        label.setToolTip(tooltip)
        field: QLineEdit | QCheckBox
        if key == "auto_start_server":
            field = QCheckBox("程序启动时自动开始监听")
        else:
            field = QLineEdit()
            field.setMinimumWidth(520 if key in {"partner_api_url", "external_url_base"} else 280)
        field.setToolTip(tooltip)
        self.config_fields[key] = field
        form.addRow(label, field)

    def _readonly_value_label(self) -> QLabel:
        """创建用于展示长路径等只读文本的标签。"""
        label = QLabel()
        label.setWordWrap(True)
        label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        return label

    def _load_config_fields(self) -> None:
        """把当前配置对象的值加载到输入框和只读标签。"""
        for key, field in self.config_fields.items():
            value = getattr(self.http_server.config, key)
            if isinstance(field, QCheckBox):
                field.setChecked(bool(value))
            else:
                field.setText(str(value))
        for key, label in self.readonly_fields.items():
            label.setText(str(getattr(self.http_server.config, key)))

    def save_config(self) -> None:
        """校验并保存弹窗中的配置到当前活动配置文件，然后关闭窗口。"""
        previous = replace(self.http_server.config)
        try:
            candidate = self._build_candidate_config(self.http_server.config.config_path)
        except ValueError as exc:
            QMessageBox.warning(self, "配置错误", str(exc))
            return

        self.http_server.config.update_from_dict(candidate.to_dict())
        self.http_server.config.save()
        self._load_config_fields()
        notices = self._config_change_notices(previous, f"配置已保存到 {self.http_server.config.config_path}")
        self._saved = True
        QMessageBox.information(self, "配置已保存", "\n".join(notices))
        self.accept()

    def load_config(self) -> None:
        """读取用户选择的配置文件并立即应用到当前运行配置。"""
        selected_path, _ = QFileDialog.getOpenFileName(
            self,
            "载入配置",
            str(self.http_server.config.config_path),
            "JSON Files (*.json);;All Files (*)",
        )
        if not selected_path:
            return

        try:
            loaded = AppConfig.read_from_path(selected_path)
        except ValueError as exc:
            QMessageBox.warning(self, "载入失败", str(exc))
            return

        previous = replace(self.http_server.config)
        self.http_server.config.update_from_dict(loaded.to_dict())
        self._load_config_fields()
        notices = self._config_change_notices(
            previous,
            f"已从 {selected_path} 载入配置",
            imported_path=Path(selected_path),
        )
        QMessageBox.information(self, "配置已载入", "\n".join(notices))

    def save_to_file(self) -> None:
        """把当前表单中的配置导出到用户选择的文件。"""
        suggested = self.http_server.config.config_path.with_name(
            f"{self.http_server.config.config_path.stem}-导出.json"
        )
        selected_path, _ = QFileDialog.getSaveFileName(
            self,
            "存到文件",
            str(suggested),
            "JSON Files (*.json);;All Files (*)",
        )
        if not selected_path:
            return

        target_path = Path(selected_path)
        if not target_path.suffix:
            target_path = target_path.with_suffix(".json")

        try:
            candidate = self._build_candidate_config(target_path)
        except ValueError as exc:
            QMessageBox.warning(self, "导出失败", str(exc))
            return

        candidate.save_to_path(target_path)
        QMessageBox.information(self, "配置已导出", f"当前配置已保存到 {target_path}")

    def reset_database(self) -> None:
        """确认后备份当前数据库，并让程序立即切换到同一路径的新空库。"""
        if self.reset_database_handler is None:
            return
        db_path = self.http_server.config.db_path
        result = QMessageBox.warning(
            self,
            "确认启用新数据库",
            (
                "此操作会先备份当前数据库，然后清空当前运行数据库并立即启用新库。\n\n"
                f"当前数据库：{db_path}\n\n"
                "HTTP server 和后台发送会短暂停止后恢复。是否继续？"
            ),
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if result != QMessageBox.StandardButton.Yes:
            return

        try:
            backup_path = self.reset_database_handler()
        except Exception as exc:
            QMessageBox.critical(self, "数据库切换失败", str(exc))
            return

        self._load_config_fields()
        QMessageBox.information(
            self,
            "已启用新数据库",
            f"当前数据库已备份到：\n{backup_path}\n\n程序已启用新的空数据库。",
        )

    def _build_candidate_config(self, config_path: Path | str) -> AppConfig:
        """把表单编辑值与当前只读配置合并成一个完整候选配置。"""
        data = self.http_server.config.to_dict()
        data.update(self._collect_config_form())
        return AppConfig.from_dict(data, config_path=config_path)

    def _collect_config_form(self) -> dict[str, object]:
        """从输入框收集配置，并转换为正确的字段类型。"""
        values: dict[str, object] = {"listen_host": "0.0.0.0"}
        for key, field in self.config_fields.items():
            if isinstance(field, QCheckBox):
                values[key] = field.isChecked()
                continue

            text = field.text().strip()
            if not text:
                if key in OPTIONAL_CONFIG_FIELDS:
                    values[key] = ""
                    continue
                raise ValueError(f"{key} 不能为空")
            try:
                if key in {"listen_port", "retry_count", "http_watchdog_failure_threshold", "event_page_size"}:
                    values[key] = int(text)
                elif key in {
                    "retry_delay_seconds",
                    "request_timeout_seconds",
                    "max_event_age_seconds",
                    "http_watchdog_interval_seconds",
                    "http_watchdog_timeout_seconds",
                    "http_watchdog_restart_cooldown_seconds",
                }:
                    values[key] = float(text)
                else:
                    values[key] = text
            except ValueError as exc:
                raise ValueError(f"{key} 的值不合法: {text}") from exc
        return values

    def _config_change_notices(
        self,
        previous: AppConfig,
        headline: str,
        imported_path: Path | None = None,
    ) -> list[str]:
        """按配置变化生成给用户看的提示信息。"""
        notices = [headline]
        if imported_path is not None:
            notices.append("已导入外部配置；普通“保存配置”仍会写回当前活动配置文件。")
        if self.http_server.is_running and (
            previous.listen_host != self.http_server.config.listen_host
            or previous.listen_port != self.http_server.config.listen_port
        ):
            notices.append("监听地址或端口变更需要停止并重新开始 HTTP server 后生效。")
        if previous.listen_path != self.http_server.config.listen_path:
            notices.append("接收路径变更会立即影响后续进入程序的 HTTP 请求。")
        if previous.external_url_base != self.http_server.config.external_url_base:
            notices.append("外部图片 URL base 已更新，后续生成的图片链接会立即按新地址拼接。")
        if previous.db_path != self.http_server.config.db_path:
            notices.append("数据库路径变更需要重启程序后生效。")
        if previous.log_path != self.http_server.config.log_path:
            notices.append("日志路径变更需要重启程序后生效。")
        if previous.auto_start_server != self.http_server.config.auto_start_server:
            notices.append("自动开启 HTTP server 会在下次程序启动时生效。")
        return notices


    def reject(self) -> None:
        """取消配置编辑，回滚本次弹窗期间对共享配置的所有未保存变动。"""
        if not self._saved:
            self.http_server.config.update_from_dict(self._original_config.to_dict())
            self.http_server.config.config_path = self._original_config.config_path
        super().reject()


class EventDetailDialog(QDialog):
    """过车详情弹窗，展示图片和完整 JSON 内容。"""

    def __init__(self, row: dict[str, object], parent: QWidget | None = None):
        """绑定数据库记录并创建详情视图。"""
        super().__init__(parent)
        self.row = row
        self.image_pixmap: QPixmap | None = None

        self.setWindowTitle(f"过车详情 - ID {row.get('id', '')}")
        self.resize(900, 720)
        self._build_ui()
        self._load_detail()

    def resizeEvent(self, event) -> None:  # noqa: N802
        """窗口尺寸变化时重新缩放过车图片。"""
        super().resizeEvent(event)
        self._refresh_image_label()

    def _build_ui(self) -> None:
        """创建图片区域、JSON 详情文本框和关闭按钮。"""
        self.image_label = QLabel("无过车图片")
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_label.setMinimumHeight(280)
        self.image_label.setStyleSheet("border: 1px solid #c8c8c8; background: #f7f7f7;")

        self.detail = QTextEdit()
        self.detail.setReadOnly(True)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        button_box.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addWidget(self.image_label)
        layout.addWidget(self.detail, 1)
        layout.addWidget(button_box)
        self.setLayout(layout)

    def _load_detail(self) -> None:
        """从数据库记录中加载图片和 JSON 详情内容。"""
        record = dict(self.row)
        image_data = record.pop("image_data", None)
        image_bytes = bytes(image_data) if image_data is not None else b""
        record["image_size"] = len(image_bytes)

        if image_bytes:
            pixmap = QPixmap()
            if pixmap.loadFromData(image_bytes):
                self.image_pixmap = pixmap
                self._refresh_image_label()
            else:
                self.image_label.setText("图片数据无法显示")

        detail = {
            "record": record,
            "partner_payload": _load_json(str(self.row.get("partner_payload_json") or "{}")),
            "hikvision_event": _load_json(str(self.row.get("raw_json") or "{}")),
        }
        self.detail.setPlainText(json.dumps(detail, ensure_ascii=False, indent=2))

    def _refresh_image_label(self) -> None:
        """按当前标签尺寸保持比例显示图片。"""
        if self.image_pixmap is None:
            return

        target_size = self.image_label.size()
        scaled = self.image_pixmap.scaled(
            target_size,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        self.image_label.setPixmap(scaled)


class EventDetailPanel(QWidget):
    """主窗口右侧常驻详情面板，显示选中记录的摘要、图片和原始消息。"""

    def __init__(self, config_provider, resend_handler, parent: QWidget | None = None):
        """绑定当前配置和重发处理函数，并创建右侧详情区域控件。"""
        super().__init__(parent)
        self.config_provider = config_provider
        self.resend_handler = resend_handler
        self.image_pixmap: QPixmap | None = None
        self._last_image_size: tuple[int, int] | None = None
        self.current_event_id: int | None = None
        self._build_ui()
        self.clear()
        self.setFixedWidth(520)

    def resizeEvent(self, event) -> None:  # noqa: N802
        """面板尺寸变化时按比例刷新过车图片。"""
        super().resizeEvent(event)
        current_size = (self.image_label.width(), self.image_label.height())
        if current_size != self._last_image_size:
            self._refresh_image_label()

    def _build_ui(self) -> None:
        """创建详情表格、过车图片和消息原文查看区域。"""
        self.title_label = QLabel("过车详情")
        self.title_label.setStyleSheet("font-weight: 600;")
        self.resend_button = QPushButton("手动发送")
        self.resend_button.clicked.connect(self.resend_handler)

        title_bar = QHBoxLayout()
        title_bar.addWidget(self.title_label)
        title_bar.addStretch(1)
        title_bar.addWidget(self.resend_button)

        self.info_table = QTableWidget(0, 2)
        self.info_table.setHorizontalHeaderLabels(["项目", "内容"])
        self.info_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        self.info_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.info_table.verticalHeader().setVisible(False)
        self.info_table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.info_table.setSelectionMode(QTableWidget.SelectionMode.NoSelection)

        self.image_container = QWidget()
        self.image_container.setFixedHeight(260)
        self.image_label = QLabel("无过车图片")
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_label.setStyleSheet("border: 1px solid #c8c8c8; background: #f7f7f7;")
        self.zoom_button = QPushButton("放大")
        self.zoom_button.setFixedSize(58, 28)
        self.zoom_button.setToolTip("放大查看过车图片")
        self.zoom_button.clicked.connect(self.open_image_preview)
        self.zoom_button.setVisible(False)

        image_layout = QGridLayout(self.image_container)
        image_layout.setContentsMargins(0, 0, 0, 0)
        image_layout.setSpacing(0)
        image_layout.addWidget(self.image_label, 0, 0)
        image_layout.addWidget(
            self.zoom_button,
            0,
            0,
            alignment=Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignRight,
        )

        layout = QVBoxLayout()
        layout.addLayout(title_bar)
        layout.addWidget(self.image_container)
        layout.addWidget(self.info_table, 1)
        self.setLayout(layout)

    def clear(self) -> None:
        """清空详情面板，等待用户在左侧列表选择记录。"""
        self.image_pixmap = None
        self._last_image_size = None
        self.current_event_id = None
        self.title_label.setText("过车详情")
        self.resend_button.setEnabled(False)
        self.info_table.setRowCount(0)
        self.image_label.clear()
        self.image_label.setText("请在左侧选择一条过车记录")
        self.zoom_button.setVisible(False)

    def set_event(self, row: dict[str, object]) -> None:
        """加载并显示当前选中的过车记录。"""
        event_id = int(row.get("id") or 0)
        received_http_scroll = (
            self._received_http_scroll_value() if self.current_event_id == event_id else 0
        )
        self.current_event_id = event_id
        self.title_label.setText(f"过车详情 - ID {row.get('id', '')}")
        self.resend_button.setEnabled(_has_partner_payload(row))
        self._load_image(row)
        self._fill_info_table(row, received_http_scroll)

    def _fill_info_table(self, row: dict[str, object], received_http_scroll: int = 0) -> None:
        """把人最常查看的字段写入详情表格。"""
        fields = [
            ("记录ID", "id"),
            ("接收时间", "received_at"),
            ("过车时间", "event_time"),
            ("记录更新时间", "updated_at"),
            ("车牌号", "plate_no"),
            ("我方方向", "direction"),
            ("通道", "lane_name"),
            ("门岗", "gate_name"),
            ("过车类型", "passing_type"),
            ("发送状态", "status"),
            ("发送次数", "attempts"),
            ("首次发送时间", "first_attempt_at"),
            ("最后尝试时间", "last_attempt_at"),
            ("下次重试时间", "next_retry_at"),
            ("死信时间", "dead_lettered_at"),
            ("返回信息", "api_return_info"),
            ("自动发送", "auto_send"),
            ("跳过原因", "skip_reason"),
            ("图片文件", "image_path"),
            ("接收原文文件", "received_body_path"),
            ("接收HTTP", "received_http"),
            ("发送API", "sent_api"),
        ]
        self.info_table.setRowCount(len(fields))
        for row_index, (label, key) in enumerate(fields):
            label_item = QTableWidgetItem(label)
            label_item.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
            self.info_table.setItem(row_index, 0, label_item)
            if key == "api_return_info":
                value = _api_return_info(row)
            elif key == "direction":
                value = _direction_label(row.get("direction"))
            elif key == "passing_type":
                value = _passing_type_label(row.get("passing_type"))
            elif key == "status":
                value = _status_label(row.get("status"))
            elif key in {
                "event_time",
                "updated_at",
                "first_attempt_at",
                "last_attempt_at",
                "next_retry_at",
                "dead_lettered_at",
            }:
                value = _short_datetime(row.get(key))
            elif key == "received_http":
                value = _format_received_message(row)
            elif key == "sent_api":
                value = _format_partner_request_message(row, self.config_provider())
            else:
                value = row.get(key)
            if key == "received_http":
                received_http_text = _readonly_text_edit("接收 HTTP JSON")
                received_http_text.setPlainText(_display_value(value))
                self.info_table.setCellWidget(row_index, 1, received_http_text)
                self.info_table.setRowHeight(row_index, 220)
                self._restore_text_scroll(received_http_text, received_http_scroll)
                continue
            if key == "sent_api":
                sent_api_text = _readonly_text_edit("Sent API request")
                sent_api_text.setPlainText(_display_value(value))
                self.info_table.setCellWidget(row_index, 1, sent_api_text)
                self.info_table.setRowHeight(row_index, 220)
                continue
            value_item = QTableWidgetItem(_display_value(value))
            value_item.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)
            self.info_table.setItem(row_index, 1, value_item)
        self.info_table.resizeRowsToContents()
        for row_index, (_label, key) in enumerate(fields):
            if key in {"received_http", "sent_api"}:
                self.info_table.setRowHeight(row_index, 220)

    def _received_http_scroll_value(self) -> int:
        """读取接收 HTTP 滚动框当前位置，用于刷新详情时恢复。"""
        for row_index in range(self.info_table.rowCount()):
            item = self.info_table.item(row_index, 0)
            if item is None or item.text() != "接收HTTP":
                continue
            widget = self.info_table.cellWidget(row_index, 1)
            if isinstance(widget, QTextEdit):
                return widget.verticalScrollBar().value()
        return 0

    def _restore_text_scroll(self, text_edit: QTextEdit, value: int) -> None:
        """在 QTextEdit 完成布局后恢复滚动位置。"""
        if value <= 0:
            return

        def restore() -> None:
            scrollbar = text_edit.verticalScrollBar()
            scrollbar.setValue(min(value, scrollbar.maximum()))

        QTimer.singleShot(0, restore)

    def _load_image(self, row: dict[str, object]) -> None:
        """优先从独立图片文件加载过车图片，兼容旧数据库中的 BLOB 图片。"""
        self.image_pixmap = None
        self._last_image_size = None
        self.image_label.clear()
        self.zoom_button.setVisible(False)

        image_path = str(row.get("image_path") or "")
        if image_path:
            pixmap = QPixmap(image_path)
            if not pixmap.isNull():
                self.image_pixmap = pixmap
                self.zoom_button.setVisible(True)
                self._refresh_image_label()
                return
            self.image_label.setText(f"图片文件无法显示：{image_path}")
            return

        image_data = row.get("image_data")
        image_bytes = bytes(image_data) if image_data is not None else b""
        if image_bytes:
            pixmap = QPixmap()
            if pixmap.loadFromData(image_bytes):
                self.image_pixmap = pixmap
                self.zoom_button.setVisible(True)
                self._refresh_image_label()
                return
            self.image_label.setText("图片数据无法显示")
            return

        self.image_label.setText("无过车图片")

    def _refresh_image_label(self) -> None:
        """按标签可用空间等比例显示图片。"""
        if self.image_pixmap is None:
            return

        scaled = self.image_pixmap.scaled(
            self.image_label.size(),
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.FastTransformation,
        )
        self.image_label.setPixmap(scaled)
        self._last_image_size = (self.image_label.width(), self.image_label.height())

    def open_image_preview(self) -> None:
        """在独立弹窗中大幅显示当前过车图片。"""
        if self.image_pixmap is None:
            return

        dialog = ImagePreviewDialog(
            self.image_pixmap,
            f"过车图片 - ID {self.current_event_id or ''}",
            self,
        )
        dialog.exec()


class ImagePreviewDialog(QDialog):
    """用于大幅查看过车图片的弹窗。"""

    def __init__(self, pixmap: QPixmap, title: str, parent: QWidget | None = None):
        """保存原始图片，并创建可滚动的大图预览区域。"""
        super().__init__(parent)
        self.image_pixmap = pixmap
        self.preview_label = QLabel()
        self.preview_label.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.setWindowTitle(title)
        self.setWindowIcon(_app_icon())
        self._build_ui()
        self.resize(1100, 760)
        QTimer.singleShot(0, self._refresh_preview)

    def resizeEvent(self, event) -> None:  # noqa: N802
        """窗口尺寸变化时按可视区域刷新图片大小。"""
        super().resizeEvent(event)
        self._refresh_preview()

    def _build_ui(self) -> None:
        """创建滚动区域和关闭按钮。"""
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.scroll_area.setWidget(self.preview_label)

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        button_box.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addWidget(self.scroll_area, 1)
        layout.addWidget(button_box)
        self.setLayout(layout)

    def _refresh_preview(self) -> None:
        """按预览窗口尺寸缩放图片，保留原始比例。"""
        target_size = self.scroll_area.viewport().size()
        if target_size.width() <= 0 or target_size.height() <= 0:
            return

        scaled = self.image_pixmap.scaled(
            target_size,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        self.preview_label.setPixmap(scaled)


class MockSendDialog(QDialog):
    """手动构造过车信息并发送到大园区 API 的测试弹窗。"""

    def __init__(self, service: ParkingBridgeService, parent: QWidget | None = None):
        """绑定业务服务并创建 mock 测试表单。"""
        super().__init__(parent)
        self.service = service
        self.signals = MockSignals()
        self.signals.finished.connect(self._show_send_result)

        self.setWindowTitle("模拟发送")
        self.resize(760, 640)
        self._build_ui()

    def _build_ui(self) -> None:
        """创建 mock 过车输入框、发送按钮和结果显示区域。"""
        form = QFormLayout()

        self.api_url_field = QLineEdit(self.service.config.partner_api_url)
        form.addRow("大园区 API", self.api_url_field)

        self.plate_field = QLineEdit("浙A12345")
        form.addRow("车牌号", self.plate_field)

        self.direction_field = QComboBox()
        self.direction_field.addItem("我方入口进场 -> 大园区 out", "enter")
        self.direction_field.addItem("我方出口出场 -> 大园区 in", "exit")
        form.addRow("过车方向", self.direction_field)

        self.event_time_field = QDateTimeEdit(QDateTime.currentDateTime())
        self.event_time_field.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.event_time_field.setCalendarPopup(True)
        self.now_button = QPushButton("设为当前时刻")
        self.now_button.clicked.connect(self._set_event_time_now)

        event_time_row = QHBoxLayout()
        event_time_row.addWidget(self.event_time_field, 1)
        event_time_row.addWidget(self.now_button)
        form.addRow("过车时间", event_time_row)

        self.phone_field = QLineEdit(self.service.config.default_phone)
        form.addRow("手机号", self.phone_field)

        self.send_button = QPushButton("发送到大园区 API")
        self.send_button.clicked.connect(self.send_mock_event)

        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        self.result_text.setPlaceholderText("发送后会显示请求 payload 和大园区返回结果")

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        button_box.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addLayout(form)
        layout.addWidget(self.send_button)
        layout.addWidget(self.result_text, 1)
        layout.addWidget(button_box)
        self.setLayout(layout)

    def send_mock_event(self) -> None:
        """校验表单并在后台线程发送一条 mock 过车记录。"""
        try:
            payload = self._build_mock_payload()
            api_url = self._mock_api_url()
        except ValueError as exc:
            QMessageBox.warning(self, "模拟发送参数错误", str(exc))
            return

        self.send_button.setEnabled(False)
        self.result_text.setPlainText(
            json.dumps(
                {"request_url": api_url, "request_payload": payload, "sending": True},
                ensure_ascii=False,
                indent=2,
            )
        )

        thread = threading.Thread(target=self._send_payload, args=(payload, api_url), daemon=True)
        thread.start()

    def _build_mock_payload(self) -> dict[str, object]:
        """把表单中的过车信息转换成大园区 API payload。"""
        plate_no = self.plate_field.text().strip()
        if not plate_no:
            raise ValueError("车牌号不能为空")

        event_time = self._selected_event_time()
        timestamp = _parse_mock_timestamp(event_time)
        event = HikEvent(
            event_key=f"mock-{uuid4().hex}",
            event_type="vehiclePassingInParkingLot",
            event_state="active",
            direction=str(self.direction_field.currentData()),
            passing_type="plateRecognition",
            plate_no=plate_no,
            event_time=event_time,
            timestamp=timestamp,
            gate_name="",
            lane_name="",
            lane_id="mock",
            raw={"mock": True},
        )
        payload = map_to_partner_payload(event, self.service.config)
        payload["phone"] = self.phone_field.text().strip() or self.service.config.default_phone
        return payload

    def _mock_api_url(self) -> str:
        """读取 Mock 测试临时使用的大园区 API 地址。"""
        api_url = self.api_url_field.text().strip()
        if not api_url:
            raise ValueError("大园区 API 地址不能为空")
        return api_url

    def _selected_event_time(self) -> str:
        """把日期时间控件中的本地时间转换为带时区的 ISO8601 文本。"""
        selected = self.event_time_field.dateTime().toPython()
        return selected.astimezone().isoformat(timespec="seconds")

    def _set_event_time_now(self) -> None:
        """把过车时间快捷设置为当前本地时刻。"""
        self.event_time_field.setDateTime(QDateTime.currentDateTime())

    def _send_payload(self, payload: dict[str, object], api_url: str) -> None:
        """在线程中用临时 API 地址发送 payload，避免阻塞 Qt 主界面。"""
        mock_config = replace(self.service.config, partner_api_url=api_url)
        result = PartnerClient(mock_config).send_once(payload)
        self.signals.finished.emit(payload, result, api_url)

    def _show_send_result(
        self, payload: dict[str, object], result: SendResult, api_url: str
    ) -> None:
        """在弹窗中显示大园区 API 的返回结果和错误信息。"""
        self.send_button.setEnabled(True)
        detail = {
            "request_url": api_url,
            "request_payload": payload,
            "success": result.success,
            "attempts": result.attempts,
            "status_code": result.status_code,
            "response_text": _load_json(result.response_text) if result.response_text else "",
            "error": result.error,
        }
        self.result_text.setPlainText(json.dumps(detail, ensure_ascii=False, indent=2))


class HelpDialog(QDialog):
    """显示 Markdown 格式帮助文档的弹窗。"""

    def __init__(self, parent: QWidget | None = None):
        """创建帮助文档浏览区域和关闭按钮。"""
        super().__init__(parent)
        self.setWindowTitle("帮助")
        self.setWindowIcon(_app_icon())
        self.resize(760, 680)
        self._build_ui()

    def _build_ui(self) -> None:
        """读取 Markdown 帮助内容并显示到可滚动文本浏览器中。"""
        self.help_text = QTextBrowser()
        self.help_text.setOpenExternalLinks(True)
        self.help_text.setMarkdown(_help_markdown())

        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Close)
        button_box.rejected.connect(self.reject)

        layout = QVBoxLayout()
        layout.addWidget(self.help_text, 1)
        layout.addWidget(button_box)
        self.setLayout(layout)


class MainWindow(QMainWindow):
    """桥接程序主窗口。"""

    def __init__(
        self,
        http_server: BridgeHTTPServer,
        service: ParkingBridgeService,
        store: EventStore,
    ):
        """绑定服务对象并初始化窗口控件和刷新定时器。"""
        super().__init__()
        self.http_server = http_server
        self.service = service
        self.store = store
        self.last_selected_event_id: int | None = None
        self.current_page_index = 0
        self.total_event_count = 0
        self.sort_column = 0
        self.sort_order = Qt.SortOrder.DescendingOrder
        self.signals = BridgeSignals()
        self.column_width_save_timer = QTimer(self)
        self.column_width_save_timer.setSingleShot(True)
        self.column_width_save_timer.setInterval(500)
        self.column_width_save_timer.timeout.connect(self._save_table_column_widths)
        # 后台线程不直接操作 Qt 控件，而是通过 signal 切回主线程刷新。
        self.signals.changed.connect(lambda _event_id: self.refresh_table())
        self._attach_service_listener()
        self._force_close = False
        self._tray_message_shown = False
        self.tray_icon: QSystemTrayIcon | None = None
        self.tray_menu: QMenu | None = None

        self.setWindowTitle("博达智创停车桥接程序")
        self.setWindowIcon(_app_icon())
        self.resize(1180, 720)
        self._build_ui()
        self._setup_tray_icon()

        self.refresh_timer = QTimer(self)
        self.refresh_timer.setInterval(1000)
        self.refresh_timer.timeout.connect(self.refresh_table)
        self.refresh_timer.start()
        self.refresh_table()

    def changeEvent(self, event) -> None:  # noqa: N802
        """窗口最小化时隐藏到系统托盘。"""
        super().changeEvent(event)
        if event.type() != QEvent.Type.WindowStateChange:
            return
        if self._force_close or self.tray_icon is None:
            return
        if self.isMinimized():
            QTimer.singleShot(0, self._hide_to_tray)

    def closeEvent(self, event) -> None:  # noqa: N802
        """窗口关闭时优先隐藏到托盘，仅在显式退出时真正关闭。"""
        if not self._force_close:
            if self.tray_icon is not None:
                event.ignore()
                self._hide_to_tray()
                return
            if not self._confirm_exit():
                event.ignore()
                return
        if self.tray_icon is not None:
            self.tray_icon.hide()
        self.http_server.stop()
        self.service.close()
        super().closeEvent(event)

    def _setup_tray_icon(self) -> None:
        """初始化 Windows 系统托盘图标和右键菜单。"""
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return

        self.tray_icon = QSystemTrayIcon(self.windowIcon(), self)
        self.tray_icon.setToolTip(self.windowTitle())
        self.tray_menu = QMenu(self)
        show_action = QAction("显示界面", self)
        exit_action = QAction("退出", self)
        show_action.triggered.connect(self._show_from_tray)
        exit_action.triggered.connect(self._exit_from_tray)
        self.tray_menu.addAction(show_action)
        self.tray_menu.addSeparator()
        self.tray_menu.addAction(exit_action)
        self.tray_icon.setContextMenu(self.tray_menu)
        self.tray_icon.activated.connect(self._handle_tray_activated)
        self.tray_icon.show()

    def _hide_to_tray(self) -> None:
        """把窗口隐藏到系统托盘，并给出一次提示。"""
        if self.tray_icon is None or self._force_close:
            return
        self.hide()
        if not self._tray_message_shown:
            self.tray_icon.showMessage(
                "程序仍在运行",
                "已最小化到系统托盘。右键托盘图标可显示界面或退出。",
                QSystemTrayIcon.MessageIcon.Information,
                3000,
            )
            self._tray_message_shown = True

    def _show_from_tray(self) -> None:
        """从系统托盘恢复主界面。"""
        if self.isMinimized():
            self.setWindowState(self.windowState() & ~Qt.WindowState.WindowMinimized)
        self.showNormal()
        self.raise_()
        self.activateWindow()

    def request_exit(self) -> None:
        """从主界面显式退出程序。"""
        if not self._confirm_exit():
            return
        self._force_close = True
        self.close()

    def _confirm_exit(self) -> bool:
        """弹出退出确认警告。"""
        message = "确认退出程序？"
        if self.http_server.is_running:
            message = "退出程序将停止 HTTP server，并中断外部图片访问和海康上报接收。是否继续？"
        result = QMessageBox.warning(
            self,
            "确认退出",
            message,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        return result == QMessageBox.StandardButton.Yes

    def _exit_from_tray(self) -> None:
        """通过托盘菜单直接退出程序。"""
        self._force_close = True
        self.close()

    def _handle_tray_activated(self, reason: QSystemTrayIcon.ActivationReason) -> None:
        """单击或双击托盘图标时恢复窗口。"""
        if reason in {
            QSystemTrayIcon.ActivationReason.Trigger,
            QSystemTrayIcon.ActivationReason.DoubleClick,
        }:
            self._show_from_tray()

    def _build_ui(self) -> None:
        """创建顶部控制栏和过车列表。"""
        self.server_button = QPushButton()
        self.config_button = QPushButton("配置")
        self.mock_button = QPushButton("模拟发送")
        self.log_button = QPushButton("查看日志")
        self.help_button = QPushButton("帮助")
        self.exit_button = QPushButton("退出")
        self.status_dot = QLabel()
        self.status_dot.setFixedSize(14, 14)
        self.status_label = QLabel()

        self.server_button.clicked.connect(self.toggle_server)
        self.config_button.clicked.connect(self.open_config_dialog)
        self.mock_button.clicked.connect(self.open_mock_dialog)
        self.log_button.clicked.connect(self.open_log_file)
        self.help_button.clicked.connect(self.show_help_dialog)
        self.exit_button.clicked.connect(self.request_exit)

        top_bar = QHBoxLayout()
        top_bar.addWidget(self.server_button)
        top_bar.addWidget(self.status_dot)
        top_bar.addWidget(self.status_label)
        top_bar.addSpacing(12)
        top_bar.addWidget(self.config_button)
        top_bar.addWidget(self.mock_button)
        top_bar.addWidget(self.log_button)
        top_bar.addWidget(self.help_button)
        top_bar.addWidget(self.exit_button)
        top_bar.addStretch(1)

        self.first_page_button = QPushButton("首页")
        self.prev_page_button = QPushButton("上一页")
        self.next_page_button = QPushButton("下一页")
        self.last_page_button = QPushButton("末页")
        self.page_info_label = QLabel()
        self.first_page_button.clicked.connect(self.go_first_page)
        self.prev_page_button.clicked.connect(self.go_previous_page)
        self.next_page_button.clicked.connect(self.go_next_page)
        self.last_page_button.clicked.connect(self.go_last_page)

        page_bar = QHBoxLayout()
        page_bar.addWidget(self.first_page_button)
        page_bar.addWidget(self.prev_page_button)
        page_bar.addWidget(self.next_page_button)
        page_bar.addWidget(self.last_page_button)
        page_bar.addSpacing(12)
        page_bar.addWidget(self.page_info_label)
        page_bar.addStretch(1)

        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(
            ["ID", "过车时间", "车牌", "方向", "通道", "类型", "状态/次数/原因", "返回信息"]
        )
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setStretchLastSection(False)
        header.setMinimumSectionSize(48)
        header.setSortIndicatorShown(True)
        header.setSortIndicator(self.sort_column, self.sort_order)
        header.sortIndicatorChanged.connect(self._remember_sort)
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QTableWidget.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.table.setSortingEnabled(True)
        self.table.itemSelectionChanged.connect(self.show_selected_detail)
        self._restore_table_column_widths()
        header.sectionResized.connect(
            lambda _logical_index, _old_size, _new_size: self._schedule_save_table_column_widths()
        )
        self.detail_panel = EventDetailPanel(
            lambda: self.service.config,
            self.manual_resend_selected,
        )

        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(self.table)
        splitter.addWidget(self.detail_panel)
        splitter.setStretchFactor(0, 1)
        splitter.setStretchFactor(1, 0)
        splitter.setSizes([760, 520])

        layout = QVBoxLayout()
        layout.addLayout(top_bar)
        layout.addLayout(page_bar)
        layout.addWidget(splitter, 1)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)
        self._update_buttons()

    def open_config_dialog(self) -> None:
        """点击配置按钮后打开配置编辑弹窗。"""
        dialog = ConfigDialog(self.http_server, self.backup_and_start_new_database, self)
        dialog.exec()
        self._restore_table_column_widths()
        self.refresh_table()
        self._update_buttons()

    def backup_and_start_new_database(self) -> Path:
        """备份当前数据库，并重建运行服务以启用同一路径的新空库。"""
        db_path = Path(self.http_server.config.db_path)
        was_running = self.http_server.is_running
        old_service = self.service
        old_store = self.store

        self.refresh_timer.stop()
        self.http_server.stop()
        old_service.close()
        try:
            backup_path = backup_database_and_reset(db_path)
            self._replace_runtime_service(EventStore(db_path))
        except Exception:
            self._replace_runtime_service(old_store)
            if was_running:
                try:
                    self.http_server.start()
                finally:
                    self._update_buttons()
            self.refresh_timer.start()
            raise

        self.last_selected_event_id = None
        self.current_page_index = 0
        self.detail_panel.clear()
        self.refresh_table()
        self._update_buttons()
        if was_running:
            try:
                self.http_server.start()
            except OSError as exc:
                QMessageBox.warning(self, "HTTP server 未恢复", str(exc))
            self._update_buttons()
        self.refresh_timer.start()
        return backup_path

    def _replace_runtime_service(self, store: EventStore) -> None:
        """用指定存储重建业务服务，并把 GUI 与 HTTP server 指向新服务。"""
        self.store = store
        self.service = ParkingBridgeService(
            self.http_server.config,
            self.store,
            PartnerClient(self.http_server.config),
        )
        self.http_server.service = self.service
        self._attach_service_listener()

    def _attach_service_listener(self) -> None:
        """把当前业务服务的事件变化通知转发到 Qt 主线程。"""
        self.service.add_listener(lambda event_id: self.signals.changed.emit(event_id))

    def open_mock_dialog(self) -> None:
        """点击模拟发送按钮后打开手动发送测试弹窗。"""
        dialog = MockSendDialog(self.service, self)
        dialog.exec()

    def open_log_file(self) -> None:
        """用系统默认程序打开当前日志文件。"""
        log_path = Path(self.http_server.config.log_path)
        if not log_path.is_absolute():
            log_path = Path.cwd() / log_path
        if not log_path.exists():
            QMessageBox.information(self, "查看日志", f"日志文件尚未生成：{log_path}")
            return
        if not QDesktopServices.openUrl(QUrl.fromLocalFile(str(log_path.resolve()))):
            QMessageBox.warning(self, "查看日志失败", f"无法打开日志文件：{log_path}")

    def show_help_dialog(self) -> None:
        """显示程序简介和简要使用帮助。"""
        dialog = HelpDialog(self)
        dialog.exec()

    def toggle_server(self) -> None:
        """按当前运行状态切换 HTTP server 的开始或停止。"""
        self.server_button.setEnabled(False)
        try:
            if self.http_server.is_running:
                self.http_server.stop()
                return

            try:
                self.http_server.start()
            except OSError as exc:
                address = f"{self.http_server.config.listen_host}:{self.http_server.config.listen_port}"
                QMessageBox.critical(self, "启动失败", f"无法启动 HTTP server：{address}\n\n{exc}")
        finally:
            self.server_button.setEnabled(True)
            self._update_buttons()

    def go_first_page(self) -> None:
        """跳转到最新记录所在的第一页。"""
        self._set_page(0)

    def go_previous_page(self) -> None:
        """跳转到上一页，也就是更新的一页记录。"""
        self._set_page(self.current_page_index - 1)

    def go_next_page(self) -> None:
        """跳转到下一页，也就是更旧的一页记录。"""
        self._set_page(self.current_page_index + 1)

    def go_last_page(self) -> None:
        """跳转到最旧记录所在的最后一页。"""
        self._set_page(_page_count(self.total_event_count, self._event_page_size()) - 1)

    def _set_page(self, page_index: int) -> None:
        """设置当前页并刷新表格，页码使用从 0 开始的内部索引。"""
        max_page_index = max(0, _page_count(self.total_event_count, self._event_page_size()) - 1)
        self.current_page_index = min(max(0, page_index), max_page_index)
        self.last_selected_event_id = None
        self.refresh_table()

    def _event_page_size(self) -> int:
        """读取配置中的分页大小，并兜底为 1000。"""
        return max(1, int(getattr(self.http_server.config, "event_page_size", 1000) or 1000))

    def _update_pagination_controls(self) -> None:
        """根据当前页和总数更新翻页按钮状态与页码说明。"""
        page_size = self._event_page_size()
        page_count = _page_count(self.total_event_count, page_size)
        current_page = min(self.current_page_index + 1, page_count)
        has_previous = self.current_page_index > 0
        has_next = self.current_page_index < page_count - 1

        self.first_page_button.setEnabled(has_previous)
        self.prev_page_button.setEnabled(has_previous)
        self.next_page_button.setEnabled(has_next)
        self.last_page_button.setEnabled(has_next)
        self.page_info_label.setText(
            f"第 {current_page} / {page_count} 页，共 {self.total_event_count} 条，每页 {page_size} 条"
        )

    def refresh_table(self) -> None:
        """从数据库读取最新记录并刷新表格。"""
        selected_id = (
            self._selected_event_id()
            or self.last_selected_event_id
            or self.detail_panel.current_event_id
        )
        previous_detail_id = self.detail_panel.current_event_id
        page_size = self._event_page_size()
        self.total_event_count = self.store.count_events()
        max_page_index = max(0, _page_count(self.total_event_count, page_size) - 1)
        if self.current_page_index > max_page_index:
            self.current_page_index = max_page_index
        offset = self.current_page_index * page_size
        rows = self.store.list_events(limit=page_size, offset=offset)
        selected_after_refresh: int | None = None
        scroll_state = self._capture_table_scroll_state()

        signals_were_blocked = self.table.blockSignals(True)
        sorting_enabled = self.table.isSortingEnabled()
        try:
            self.table.setSortingEnabled(False)
            self.table.clearContents()
            self.table.setRowCount(len(rows))

            # 主窗口只展示操作时最常用的字段，完整内容在点击行后弹窗查看。
            for row_index, row in enumerate(rows):
                background = _table_row_background(row)
                values = [
                    (row["id"], int(row["id"])),
                    (_short_datetime(row["event_time"] or row["received_at"]), str(row["event_time"] or row["received_at"] or "")),
                    (row["plate_no"], str(row.get("plate_no") or "")),
                    (_direction_label(row["direction"]), _direction_label(row["direction"])),
                    (row["lane_name"], str(row.get("lane_name") or "")),
                    (_passing_type_label(row["passing_type"]), _passing_type_label(row["passing_type"])),
                    (_status_attempts(row), _status_attempts(row)),
                    (_api_return_info(row), _api_return_info(row)),
                ]
                for col_index, (display, sort_value) in enumerate(values):
                    self.table.setItem(row_index, col_index, _table_item(display, sort_value, background))

            self.table.setSortingEnabled(sorting_enabled)
            if sorting_enabled:
                self.table.sortItems(self.sort_column, self.sort_order)

            # 刷新后尽量保留用户当前选中的记录，避免用户操作焦点跳动。
            if selected_id is not None and self._reselect_event(selected_id):
                selected_after_refresh = selected_id
            elif rows:
                self.table.selectRow(0)
                selected_after_refresh = self._selected_event_id()
        finally:
            self._restore_table_scroll_state(scroll_state)
            self.table.blockSignals(signals_were_blocked)

        if selected_after_refresh is None:
            self.last_selected_event_id = None
            self.detail_panel.clear()
        elif previous_detail_id != selected_after_refresh:
            self.last_selected_event_id = selected_after_refresh
            self.show_selected_detail()
        self._update_pagination_controls()

    def show_selected_detail(self, _item: QTableWidgetItem | None = None) -> None:
        """左侧列表选择变化后，在右侧常驻面板显示完整详情。"""
        event_id = self._selected_event_id()
        if event_id is None:
            if self.table.rowCount() > 0 and self.last_selected_event_id is not None:
                signals_were_blocked = self.table.blockSignals(True)
                try:
                    restored = self._reselect_event(self.last_selected_event_id)
                finally:
                    self.table.blockSignals(signals_were_blocked)
                if restored:
                    return
            self.last_selected_event_id = None
            self.detail_panel.clear()
            return

        self.last_selected_event_id = event_id
        row = self.store.get_event(event_id)
        if row is None:
            self.last_selected_event_id = None
            self.detail_panel.clear()
            return

        self.detail_panel.set_event(row)

    def manual_resend_selected(self) -> None:
        """对当前选中的可发送记录执行手动发送。"""
        event_id = (
            self._selected_event_id()
            or self.detail_panel.current_event_id
            or self.last_selected_event_id
        )
        if event_id is None:
            QMessageBox.information(self, "手动发送", "请先选择一条记录")
            return
        row = self.store.get_event(event_id)
        if row is None or not _has_partner_payload(row):
            QMessageBox.information(self, "手动发送", "这条记录没有可发送的大园区请求")
            return

        # 手动发送前弹出确认框，避免误点击后立即向大园区重复发送。
        confirm_message = (
            "确认手动发送这条记录吗？\n\n"
            f"车牌：{row.get('plate_no') or '-'}\n"
            f"时间：{row.get('event_time') or row.get('received_at') or '-'}"
        )
        confirm_result = QMessageBox.question(
            self,
            "确认手动发送",
            confirm_message,
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if confirm_result != QMessageBox.StandardButton.Yes:
            return

        self.service.manual_resend(event_id)

    def _selected_event_id(self) -> int | None:
        """读取当前表格选中行对应的数据库记录 ID。"""
        items = self.table.selectedItems()
        if not items:
            return None
        row = items[0].row()
        item = self.table.item(row, 0)
        if item is None:
            return None
        try:
            return int(item.text())
        except ValueError:
            return None

    def _reselect_event(self, event_id: int) -> bool:
        """刷新表格后按记录 ID 恢复选中行。"""
        for row_index in range(self.table.rowCount()):
            item = self.table.item(row_index, 0)
            if item is not None and item.text() == str(event_id):
                self.table.selectRow(row_index)
                return True
        return False

    def _capture_table_scroll_state(self) -> tuple[int, int]:
        """记录过车列表刷新前的横向、纵向滚动条位置。"""
        return (
            self.table.horizontalScrollBar().value(),
            self.table.verticalScrollBar().value(),
        )

    def _restore_table_scroll_state(self, scroll_state: tuple[int, int]) -> None:
        """在列表刷新后恢复滚动条位置，避免定时刷新打断当前浏览位置。"""
        horizontal_value, vertical_value = scroll_state
        self.table.horizontalScrollBar().setValue(horizontal_value)
        self.table.verticalScrollBar().setValue(vertical_value)

    def _restore_table_column_widths(self) -> None:
        """按配置恢复过车列表列宽，未配置时使用一组易读默认宽度。"""
        widths = self.http_server.config.event_table_column_widths
        if len(widths) != self.table.columnCount():
            widths = [70, 170, 110, 90, 120, 90, 110, 260]

        for column_index, width in enumerate(widths):
            self.table.setColumnWidth(column_index, max(48, int(width)))

    def _remember_sort(self, column: int, order: Qt.SortOrder) -> None:
        """记录用户最近一次通过列表头选择的排序方式。"""
        self.sort_column = column
        self.sort_order = order

    def _schedule_save_table_column_widths(self) -> None:
        """用户拖动列宽后延迟保存，避免拖动过程中频繁写配置文件。"""
        self.column_width_save_timer.start()

    def _save_table_column_widths(self) -> None:
        """把当前过车列表列宽保存到配置文件，供下次启动恢复。"""
        widths = [self.table.columnWidth(index) for index in range(self.table.columnCount())]
        self.http_server.config.event_table_column_widths = widths
        self.http_server.config.save()

    def _update_buttons(self) -> None:
        """根据 HTTP server 状态更新按钮文案和状态灯。"""
        running = self.http_server.is_running
        self.server_button.setText("停止 HTTP server" if running else "开始 HTTP server")
        self.status_dot.setStyleSheet(
            f"border: 1px solid #7a7a7a; border-radius: 7px; background: {'#2da44e' if running else '#cf222e'};"
        )
        self.status_label.setText("运行中" if running else "未运行")


def _has_partner_payload(row: dict[str, object]) -> bool:
    """判断记录是否已经生成可手动发送的大园区 payload。"""
    payload_text = str(row.get("partner_payload_json") or "").strip()
    return payload_text not in {"", "{}"}


def _table_item(
    display_value: object,
    sort_value: object | None = None,
    background: QColor | None = None,
) -> QTableWidgetItem:
    """创建带排序键和背景色的过车列表单元格。"""
    item = SortableTableWidgetItem(str(display_value), sort_value if sort_value is not None else str(display_value))
    if background is not None:
        item.setBackground(background)
    item.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
    return item


def _page_count(total: int, page_size: int) -> int:
    """按总记录数和每页大小计算页数，空表也显示 1 页。"""
    safe_total = max(0, int(total))
    safe_page_size = max(1, int(page_size))
    return max(1, (safe_total + safe_page_size - 1) // safe_page_size)


def run_gui(
    http_server: BridgeHTTPServer,
    service: ParkingBridgeService,
    store: EventStore,
) -> int:
    """创建并运行 Qt 应用主循环。"""
    app = QApplication([])
    app.setWindowIcon(_app_icon())
    window = MainWindow(http_server, service, store)
    window.show()
    return app.exec()


def _app_icon() -> QIcon:
    """读取应用占位图标，后续只需要替换同名文件。"""
    icon_path = Path(__file__).with_name("assets") / "app_icon.ico"
    if icon_path.exists():
        return QIcon(str(icon_path))
    return QIcon()


def _help_markdown() -> str:
    """读取 Markdown 帮助文档，文件缺失时使用内置兜底内容。"""
    help_path = Path(__file__).with_name("assets") / "help.md"
    try:
        return help_path.read_text(encoding="utf-8")
    except OSError:
        return _default_help_markdown()


def _default_help_markdown() -> str:
    """返回帮助文件缺失时使用的内置 Markdown 内容。"""
    return """# 博达智创-管委会停车桥接程序

本程序接收海康威视停车终端上报的过车消息，解析车牌、方向、时间和图片后，按规则转换并发送给大园区停车系统。

## 简要使用

1. 在 **配置** 中确认监听端口、接收路径和大园区 API 地址。
2. 点击顶部 **HTTP server** 按钮开始或停止监听；也可在配置中启用自动开启。
3. 左侧查看过车列表，右侧查看详情、图片、接收 HTTP JSON 和发送 API 内容。
4. 需要测试时点击 **模拟发送**；需要排查运行记录时点击 **查看日志**。
5. 对默认跳过但已生成 payload 的记录，也可以在右侧详情点击 **手动发送**。

## 作者

陈哲达
"""


def _load_json(text: str) -> object:
    """尽量把数据库中的 JSON 文本还原成对象，失败时保留原文。"""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _readonly_text_edit(placeholder: str) -> QTextEdit:
    """创建只读原文显示文本框。"""
    text_edit = QTextEdit()
    text_edit.setReadOnly(True)
    text_edit.setPlaceholderText(placeholder)
    return text_edit


def _display_value(value: object) -> str:
    """把数据库字段值转换成适合给人查看的表格文本。"""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "是" if value else "否"
    if value in {0, 1}:
        return "是" if value == 1 else "否"
    return str(value)


def _short_datetime(value: object) -> str:
    """把 ISO8601 时间转成界面短显示，去掉末尾时区偏移。"""
    text = str(value or "")
    text = re.sub(r"(?:[+-]\d{1,2}:\d{2}|Z)$", "", text)
    return text.replace("T", " ")


def _direction_label(value: object) -> str:
    """把海康方向枚举转换成保安容易阅读的中文。"""
    text = str(value or "")
    return DIRECTION_LABELS.get(text, text)


def _passing_type_label(value: object) -> str:
    """把海康过车类型枚举转换成中文。"""
    text = str(value or "")
    return PASSING_TYPE_LABELS.get(text, text)


def _status_label(value: object) -> str:
    """把内部发送状态转换成中文。"""
    text = str(value or "")
    return STATUS_LABELS.get(text, text)


def _status_attempts(row: dict[str, object]) -> str:
    """把发送状态、次数或跳过原因合并成列表中的单列文本。"""
    if row.get("status") == "skipped":
        return str(row.get("skip_reason") or "已跳过")

    attempts = int(row.get("attempts") or 0)
    return f"{_status_label(row.get('status'))} / {attempts}次"


def _table_row_background(row: dict[str, object]) -> QColor | None:
    """按发送结果返回过车列表的浅色行底纹。"""
    status = str(row.get("status") or "")
    if status == "sent":
        return QColor("#EAF7EA")
    if status == "skipped":
        return QColor("#FFF8D8")
    if status == "failed_retryable":
        return QColor("#FFF2D8")
    if status == "dead_letter":
        return QColor("#FDECEC")
    if status == "failed":
        return QColor("#FDECEC")
    return None


def _api_return_info(row: dict[str, object]) -> str:
    """把大园区 API 返回状态码和信息整理成一行给人查看。"""
    parts: list[str] = []
    http_status = row.get("status_code")
    if http_status not in {None, ""}:
        parts.append(f"HTTP {http_status}")

    response_text = str(row.get("response_text") or "")
    response = _load_json(response_text) if response_text else {}
    if isinstance(response, dict):
        partner_status = response.get("status")
        partner_msg = response.get("msg")
        if partner_status not in {None, ""}:
            parts.append(f"API {partner_status}")
        if partner_msg not in {None, ""}:
            parts.append(str(partner_msg))
    elif response_text:
        parts.append(response_text)

    last_error = str(row.get("last_error") or "")
    if last_error and last_error not in parts:
        parts.append(last_error)
    return " | ".join(parts)


def _format_received_message(row: dict[str, object]) -> str:
    """格式化右侧详情中的接收 HTTP JSON 部分，不显示 multipart 二进制内容。"""
    return _pretty_json_text(str(row.get("raw_json") or "{}"))


def _format_partner_request_message(row: dict[str, object], config: AppConfig) -> str:
    """格式化发送给大园区 API 的 HTTP 请求原文。"""
    last_request_payload_text = str(row.get("last_request_payload_json") or "").strip()
    if last_request_payload_text not in {"", "{}"}:
        request_url = str(row.get("last_request_url") or config.partner_api_url)
        title = "实际发送 API"
        body = _pretty_json_text(last_request_payload_text)
    else:
        try:
            payload = load_partner_payload(row, config)
        except Exception:
            return "这条记录的大园区 API 请求无法解析。"
        if not payload:
            return "这条记录没有生成大园区 API 请求。"
        request_url = config.partner_api_url
        title = "待发送 API"
        body = json.dumps(payload, ensure_ascii=False, indent=2)
    return "\n".join(
        [
            title,
            f"POST {request_url} HTTP/1.1",
            "Content-Type: text/json; charset=utf-8",
            "",
            body,
        ]
    )


def _pretty_json_text(text: str) -> str:
    """如果文本是 JSON 就格式化显示，否则原样显示。"""
    value = _load_json(text)
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2)


def _default_event_time() -> str:
    """返回 mock 表单默认使用的当前本地 ISO8601 时间。"""
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _parse_mock_timestamp(value: str) -> int:
    """把 mock 表单中的 ISO8601 时间转换为秒级 Unix 时间戳。"""
    try:
        return int(datetime.fromisoformat(value).timestamp())
    except ValueError as exc:
        raise ValueError("过车时间必须是 ISO8601 格式，例如 2026-04-12T10:30:00+08:00") from exc
