"""程序配置定义，支持从单一 JSON 配置文件读写。"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse


DEFAULT_CONFIG_PATH = Path("config.json")
CONFIG_PATH_ENV = "HKPARKING_CONFIG"
CONFIG_VALUE_KEYS = {
    "listen_host",
    "listen_port",
    "listen_path",
    "auto_start_server",
    "partner_api_url",
    "park_id",
    "local_exit_hobby",
    "local_exit_cid",
    "local_exit_cname",
    "local_entry_hobby",
    "local_entry_cid",
    "local_entry_cname",
    "default_phone",
    "external_url_base",
    "retry_count",
    "retry_delay_seconds",
    "request_timeout_seconds",
    "max_event_age_seconds",
    "max_request_bytes",
    "request_read_timeout_seconds",
    "http_max_connections",
    "http_request_queue_size",
    "http_ingress_queue_size",
    "http_ingress_workers",
    "http_watchdog_interval_seconds",
    "http_watchdog_timeout_seconds",
    "http_watchdog_failure_threshold",
    "http_watchdog_restart_cooldown_seconds",
    "sender_worker_count",
    "sender_queue_size",
    "stale_sending_seconds",
    "event_retention_days",
    "artifact_retention_days",
    "image_rate_limit_per_minute",
    "image_rate_limit_burst",
    "event_page_size",
    "db_path",
    "log_path",
    "event_table_column_widths",
}


@dataclass
class AppConfig:
    """桥接程序的运行配置。"""

    listen_host: str = "0.0.0.0"
    listen_port: int = 1888
    listen_path: str = "/park"
    auto_start_server: bool = False
    partner_api_url: str = "http://example.com/api"
    park_id: str = "50810"
    local_exit_hobby: str = "in"
    local_exit_cid: str = "67592112"
    local_exit_cname: str = "博达出口"
    local_entry_hobby: str = "out"
    local_entry_cid: str = "cd3b6049"
    local_entry_cname: str = "博达入口"
    default_phone: str = "13800000000"
    external_url_base: str = ""
    retry_count: int = 3
    retry_delay_seconds: float = 1.0
    request_timeout_seconds: float = 5.0
    max_event_age_seconds: float = 60.0
    max_request_bytes: int = 1_048_576
    request_read_timeout_seconds: float = 15.0
    http_max_connections: int = 64
    http_request_queue_size: int = 128
    http_ingress_queue_size: int = 256
    http_ingress_workers: int = 1
    http_watchdog_interval_seconds: float = 10.0
    http_watchdog_timeout_seconds: float = 3.0
    http_watchdog_failure_threshold: int = 2
    http_watchdog_restart_cooldown_seconds: float = 30.0
    sender_worker_count: int = 4
    sender_queue_size: int = 1000
    stale_sending_seconds: float = 300.0
    event_retention_days: int = 180
    artifact_retention_days: int = 30
    image_rate_limit_per_minute: int = 60
    image_rate_limit_burst: int = 20
    event_page_size: int = 1000
    db_path: Path = Path("data/bdzc_parking.sqlite3")
    log_path: Path = Path("logs/bdzc_parking.log")
    event_table_column_widths: list[int] = field(default_factory=list)
    config_path: Path = DEFAULT_CONFIG_PATH

    @classmethod
    def load(cls, config_path: Path | str | None = None) -> "AppConfig":
        """从配置文件加载配置；文件不存在时写入默认配置。"""
        path = _resolve_config_path(config_path)
        if not path.exists():
            config = cls(config_path=path)
            config.save()
            return config

        config = cls.read_from_path(path)
        config.save()
        return config

    @classmethod
    def read_from_path(cls, config_path: Path | str) -> "AppConfig":
        """只读加载并校验指定配置文件，不创建文件也不回写。"""
        path = Path(config_path)
        data = _read_json_object(path)
        return cls.from_dict(data, config_path=path)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        config_path: Path | str | None = None,
    ) -> "AppConfig":
        """从字典构造配置对象，未知字段会被忽略。"""
        config = cls(config_path=_resolve_config_path(config_path))
        config.update_from_dict(data)
        config.validate()
        return config

    def update_from_dict(self, data: dict[str, Any]) -> None:
        """用字典中的配置项更新当前配置对象。"""
        for key, value in data.items():
            if key not in CONFIG_VALUE_KEYS:
                continue
            setattr(self, key, _coerce_value(key, value))

    def to_dict(self) -> dict[str, object]:
        """转换为可写入 JSON 配置文件的字典。"""
        return {
            "listen_host": self.listen_host,
            "listen_port": self.listen_port,
            "listen_path": self.listen_path,
            "auto_start_server": self.auto_start_server,
            "partner_api_url": self.partner_api_url,
            "park_id": self.park_id,
            "local_exit_hobby": self.local_exit_hobby,
            "local_exit_cid": self.local_exit_cid,
            "local_exit_cname": self.local_exit_cname,
            "local_entry_hobby": self.local_entry_hobby,
            "local_entry_cid": self.local_entry_cid,
            "local_entry_cname": self.local_entry_cname,
            "default_phone": self.default_phone,
            "external_url_base": self.external_url_base_normalized,
            "retry_count": self.retry_count,
            "retry_delay_seconds": self.retry_delay_seconds,
            "request_timeout_seconds": self.request_timeout_seconds,
            "max_event_age_seconds": self.max_event_age_seconds,
            "max_request_bytes": self.max_request_bytes,
            "request_read_timeout_seconds": self.request_read_timeout_seconds,
            "http_max_connections": self.http_max_connections,
            "http_request_queue_size": self.http_request_queue_size,
            "http_ingress_queue_size": self.http_ingress_queue_size,
            "http_ingress_workers": self.http_ingress_workers,
            "http_watchdog_interval_seconds": self.http_watchdog_interval_seconds,
            "http_watchdog_timeout_seconds": self.http_watchdog_timeout_seconds,
            "http_watchdog_failure_threshold": self.http_watchdog_failure_threshold,
            "http_watchdog_restart_cooldown_seconds": self.http_watchdog_restart_cooldown_seconds,
            "sender_worker_count": self.sender_worker_count,
            "sender_queue_size": self.sender_queue_size,
            "stale_sending_seconds": self.stale_sending_seconds,
            "event_retention_days": self.event_retention_days,
            "artifact_retention_days": self.artifact_retention_days,
            "image_rate_limit_per_minute": self.image_rate_limit_per_minute,
            "image_rate_limit_burst": self.image_rate_limit_burst,
            "event_page_size": self.event_page_size,
            "db_path": _path_to_config_text(self.db_path),
            "log_path": _path_to_config_text(self.log_path),
            "event_table_column_widths": self.event_table_column_widths,
        }

    @property
    def external_url_base_normalized(self) -> str:
        """返回已去掉末尾斜杠的外部访问 URL 前缀。"""
        return _normalize_external_url_base(self.external_url_base)

    @property
    def external_image_path(self) -> str:
        """返回外部图片访问 URL 的 path 前缀。"""
        if not self.external_url_base_normalized:
            return ""
        return urlparse(self.external_url_base_normalized).path.rstrip("/")

    def build_external_image_url(self, image_name: str) -> str:
        """根据当前配置把图片文件名转换为外部可访问的 URL。"""
        base = self.external_url_base_normalized
        clean_name = Path(str(image_name or "")).name
        if not base or not clean_name:
            return clean_name
        return f"{base}/{quote(clean_name)}"

    def validate(self) -> None:
        """校验配置值是否满足运行约束。"""
        for key in {
            "listen_host",
            "listen_path",
            "partner_api_url",
            "park_id",
            "local_exit_hobby",
            "local_exit_cid",
            "local_exit_cname",
            "local_entry_hobby",
            "local_entry_cid",
            "local_entry_cname",
            "default_phone",
        }:
            value = str(getattr(self, key)).strip()
            if not value:
                raise ValueError(f"{key} 不能为空")
        for key in {"local_exit_hobby", "local_entry_hobby"}:
            value = str(getattr(self, key)).strip()
            if value not in {"in", "out"}:
                raise ValueError(f"{key} 必须是 in 或 out")
        if self.listen_port <= 0 or self.listen_port > 65535:
            raise ValueError("listen_port 必须在 1 到 65535 之间")
        if not self.listen_path.startswith("/"):
            raise ValueError("listen_path 必须以 / 开头")
        if self.retry_count < 0:
            raise ValueError("retry_count 不能小于 0")
        if self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds 不能小于 0")
        if self.request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds 必须大于 0")
        if self.max_event_age_seconds < 0:
            raise ValueError("max_event_age_seconds 不能小于 0")
        if self.max_request_bytes <= 0:
            raise ValueError("max_request_bytes 必须大于 0")
        if self.request_read_timeout_seconds <= 0:
            raise ValueError("request_read_timeout_seconds 必须大于 0")
        if self.http_max_connections <= 0:
            raise ValueError("http_max_connections 必须大于 0")
        if self.http_request_queue_size <= 0:
            raise ValueError("http_request_queue_size 必须大于 0")
        if self.http_ingress_queue_size <= 0:
            raise ValueError("http_ingress_queue_size 必须大于 0")
        if self.http_ingress_workers <= 0:
            raise ValueError("http_ingress_workers 必须大于 0")
        if self.http_watchdog_interval_seconds <= 0:
            raise ValueError("http_watchdog_interval_seconds 必须大于 0")
        if self.http_watchdog_timeout_seconds <= 0:
            raise ValueError("http_watchdog_timeout_seconds 必须大于 0")
        if self.http_watchdog_failure_threshold <= 0:
            raise ValueError("http_watchdog_failure_threshold 必须大于 0")
        if self.http_watchdog_restart_cooldown_seconds <= 0:
            raise ValueError("http_watchdog_restart_cooldown_seconds 必须大于 0")
        if self.sender_worker_count <= 0:
            raise ValueError("sender_worker_count 必须大于 0")
        if self.sender_queue_size <= 0:
            raise ValueError("sender_queue_size 必须大于 0")
        if self.stale_sending_seconds <= 0:
            raise ValueError("stale_sending_seconds 必须大于 0")
        if self.event_retention_days <= 0:
            raise ValueError("event_retention_days 必须大于 0")
        if self.artifact_retention_days <= 0:
            raise ValueError("artifact_retention_days 必须大于 0")
        if self.image_rate_limit_per_minute <= 0:
            raise ValueError("image_rate_limit_per_minute 必须大于 0")
        if self.image_rate_limit_burst <= 0:
            raise ValueError("image_rate_limit_burst 必须大于 0")
        if self.event_page_size <= 0:
            raise ValueError("event_page_size 必须大于 0")
        if self.external_url_base.strip():
            _validate_external_url_base(self.external_url_base)

    def save(self) -> None:
        """把当前配置写回配置文件。"""
        self.save_to_path(self.config_path)

    def save_to_path(self, config_path: Path | str) -> None:
        """把当前配置写入指定路径，但不修改活动配置文件路径。"""
        path = Path(config_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
        path.write_text(f"{text}\n", encoding="utf-8")


def _resolve_config_path(config_path: Path | str | None) -> Path:
    """解析配置文件路径，默认使用当前运行目录下的 config.json。"""
    if config_path is not None:
        return Path(config_path)
    return Path(os.getenv(CONFIG_PATH_ENV, str(DEFAULT_CONFIG_PATH)))


def _read_json_object(path: Path) -> dict[str, Any]:
    """从配置文件读取 JSON object。"""
    if not path.exists():
        raise ValueError(f"配置文件不存在: {path}")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"配置文件不是合法 JSON: {path}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"配置文件根节点必须是 JSON object: {path}")
    return data


def _coerce_value(key: str, value: Any) -> object:
    """把 JSON 值转换为 AppConfig 字段需要的类型。"""
    if key in {
        "listen_port",
        "retry_count",
        "http_watchdog_failure_threshold",
        "max_request_bytes",
        "http_max_connections",
        "http_request_queue_size",
        "http_ingress_queue_size",
        "http_ingress_workers",
        "sender_worker_count",
        "sender_queue_size",
        "event_retention_days",
        "artifact_retention_days",
        "image_rate_limit_per_minute",
        "image_rate_limit_burst",
        "event_page_size",
    }:
        return int(value)
    if key in {
        "retry_delay_seconds",
        "request_timeout_seconds",
        "max_event_age_seconds",
        "request_read_timeout_seconds",
        "http_watchdog_interval_seconds",
        "http_watchdog_timeout_seconds",
        "http_watchdog_restart_cooldown_seconds",
        "stale_sending_seconds",
    }:
        return float(value)
    if key == "auto_start_server":
        return _coerce_bool(value)
    if key in {"local_exit_hobby", "local_entry_hobby"}:
        return str(value).strip().lower()
    if key in {"db_path", "log_path"}:
        return Path(str(value))
    if key == "event_table_column_widths":
        return _coerce_int_list(value)
    return str(value)


def _path_to_config_text(path: Path) -> str:
    """把路径写成 JSON 中更易读的斜杠形式。"""
    return path.as_posix()


def _normalize_external_url_base(value: str) -> str:
    """标准化外部访问 URL 前缀。"""
    return str(value or "").strip().rstrip("/")


def _validate_external_url_base(value: str) -> None:
    """校验外部访问 URL 前缀是否合法。"""
    parsed = urlparse(_normalize_external_url_base(value))
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("external_url_base 必须是 http:// 或 https:// 开头的绝对 URL")
    if not parsed.netloc:
        raise ValueError("external_url_base 必须包含主机名")
    if parsed.query or parsed.fragment:
        raise ValueError("external_url_base 不能包含 query 或 fragment")
    path = parsed.path.rstrip("/")
    if path in {"", "/"}:
        raise ValueError("external_url_base 必须包含明确的路径前缀，例如 https://host/parking-images")


def _coerce_bool(value: Any) -> bool:
    """把 JSON 或表单里的布尔配置值转换为 bool。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value != 0

    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "on", "是", "开启"}:
        return True
    if text in {"false", "0", "no", "n", "off", "否", "关闭"}:
        return False
    raise ValueError(f"无法转换为布尔值: {value}")


def _coerce_int_list(value: Any) -> list[int]:
    """把 JSON 中的列表配置转换为整数列表。"""
    if not isinstance(value, list):
        return []

    numbers: list[int] = []
    for item in value:
        try:
            number = int(item)
        except (TypeError, ValueError):
            continue
        if number > 0:
            numbers.append(number)
    return numbers
