"""配置文件读写逻辑测试。"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bdzc_parking.config import AppConfig


def test_load_creates_default_config_file(tmp_path: Path) -> None:
    """配置文件不存在时应自动创建默认 JSON 文件。"""
    config_path = tmp_path / "config.json"

    config = AppConfig.load(config_path)

    assert config.config_path == config_path
    assert config_path.exists()
    data = json.loads(config_path.read_text(encoding="utf-8"))
    assert data["listen_port"] == 1888
    assert data["auto_start_server"] is False
    assert data["max_event_age_seconds"] == 60.0
    assert data["external_url_base"] == ""
    assert data["image_rate_limit_per_minute"] == 60
    assert data["event_page_size"] == 1000
    assert data["http_max_connections"] == 64
    assert data["http_request_queue_size"] == 128
    assert data["http_ingress_queue_size"] == 256
    assert data["http_ingress_workers"] == 1
    assert data["http_watchdog_interval_seconds"] == 10.0
    assert data["http_watchdog_timeout_seconds"] == 3.0
    assert data["http_watchdog_failure_threshold"] == 2
    assert data["http_watchdog_restart_cooldown_seconds"] == 30.0
    assert data["event_table_column_widths"] == []
    assert data["partner_api_url"] == config.partner_api_url
    assert data["local_exit_hobby"] == "in"
    assert data["local_exit_cid"] == config.local_exit_cid
    assert data["local_entry_hobby"] == "out"
    assert data["local_entry_cname"] == config.local_entry_cname


def test_load_updates_from_existing_config_file(tmp_path: Path) -> None:
    """配置文件已有内容时应读取并转换字段类型。"""
    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps(
            {
                "listen_port": "1999",
                "auto_start_server": "true",
                "partner_api_url": "http://example.com/api",
                "external_url_base": "https://public.example.com/parking-images/",
                "local_exit_hobby": " IN ",
                "local_exit_cname": "北门出口",
                "local_entry_hobby": " Out ",
                "local_entry_cid": "ENTRY-001",
                "max_event_age_seconds": "30.5",
                "http_max_connections": "32",
                "http_request_queue_size": "96",
                "http_ingress_queue_size": "128",
                "http_ingress_workers": "2",
                "http_watchdog_interval_seconds": "5.5",
                "http_watchdog_timeout_seconds": "1.25",
                "http_watchdog_failure_threshold": "4",
                "http_watchdog_restart_cooldown_seconds": "15.0",
                "image_rate_limit_per_minute": "90",
                "event_page_size": "1000",
                "db_path": "custom.sqlite3",
                "event_table_column_widths": ["80", 160, "bad", -10],
            }
        ),
        encoding="utf-8",
    )

    config = AppConfig.load(config_path)

    assert config.listen_port == 1999
    assert config.auto_start_server is True
    assert config.partner_api_url == "http://example.com/api"
    assert config.external_url_base_normalized == "https://public.example.com/parking-images"
    assert config.local_exit_hobby == "in"
    assert config.local_exit_cname == "北门出口"
    assert config.local_entry_hobby == "out"
    assert config.local_entry_cid == "ENTRY-001"
    assert config.max_event_age_seconds == 30.5
    assert config.http_max_connections == 32
    assert config.http_request_queue_size == 96
    assert config.http_ingress_queue_size == 128
    assert config.http_ingress_workers == 2
    assert config.http_watchdog_interval_seconds == 5.5
    assert config.http_watchdog_timeout_seconds == 1.25
    assert config.http_watchdog_failure_threshold == 4
    assert config.http_watchdog_restart_cooldown_seconds == 15.0
    assert config.image_rate_limit_per_minute == 90
    assert config.event_page_size == 1000
    assert config.db_path == Path("custom.sqlite3")
    assert config.event_table_column_widths == [80, 160]


def test_read_from_path_does_not_rewrite_source_file(tmp_path: Path) -> None:
    """只读载入外部配置时不应修改原始文件内容。"""
    config_path = tmp_path / "import.json"
    original_text = '{"listen_port": 2888}\n'
    config_path.write_text(original_text, encoding="utf-8")

    config = AppConfig.read_from_path(config_path)

    assert config.listen_port == 2888
    assert config_path.read_text(encoding="utf-8") == original_text


def test_save_to_path_writes_copy_without_switching_active_config(tmp_path: Path) -> None:
    """导出配置文件时不应改动当前活动配置路径。"""
    active_path = tmp_path / "active.json"
    export_path = tmp_path / "exports" / "copy.json"
    config = AppConfig(config_path=active_path, local_entry_cname="南门入口")

    config.save_to_path(export_path)

    assert config.config_path == active_path
    data = json.loads(export_path.read_text(encoding="utf-8"))
    assert data["local_entry_cname"] == "南门入口"


def test_read_from_path_rejects_invalid_values(tmp_path: Path) -> None:
    """外部配置语义非法时应直接报错。"""
    config_path = tmp_path / "bad.json"
    config_path.write_text(json.dumps({"listen_port": 70000}), encoding="utf-8")

    with pytest.raises(ValueError, match="listen_port"):
        AppConfig.read_from_path(config_path)


def test_read_from_path_rejects_invalid_hobby(tmp_path: Path) -> None:
    """入口和出口 hobby 只能配置为大园区 API 支持的 in/out。"""
    config_path = tmp_path / "bad-hobby.json"
    config_path.write_text(json.dumps({"local_exit_hobby": "leave"}), encoding="utf-8")

    with pytest.raises(ValueError, match="local_exit_hobby"):
        AppConfig.read_from_path(config_path)


def test_read_from_path_rejects_invalid_event_page_size(tmp_path: Path) -> None:
    """主列表分页大小必须大于 0。"""
    config_path = tmp_path / "bad-page-size.json"
    config_path.write_text(json.dumps({"event_page_size": 0}), encoding="utf-8")

    with pytest.raises(ValueError, match="event_page_size"):
        AppConfig.read_from_path(config_path)


@pytest.mark.parametrize(
    "key",
    [
        "http_max_connections",
        "http_request_queue_size",
        "http_ingress_queue_size",
        "http_ingress_workers",
        "http_watchdog_interval_seconds",
        "http_watchdog_timeout_seconds",
        "http_watchdog_failure_threshold",
        "http_watchdog_restart_cooldown_seconds",
    ],
)
def test_read_from_path_rejects_invalid_http_runtime_values(tmp_path: Path, key: str) -> None:
    """HTTP server 运行参数必须全部大于 0。"""
    config_path = tmp_path / "bad-watchdog.json"
    config_path.write_text(json.dumps({key: 0}), encoding="utf-8")

    with pytest.raises(ValueError, match=key):
        AppConfig.read_from_path(config_path)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("ftp://example.com/images", "http:// 或 https://"),
        ("https://example.com", "明确的路径前缀"),
        ("https://example.com/images?a=1", "query 或 fragment"),
    ],
)
def test_external_url_base_validation(value: str, message: str) -> None:
    """external_url_base 应拦截非法 URL。"""
    with pytest.raises(ValueError, match=message):
        AppConfig.from_dict({"external_url_base": value})


def test_build_external_image_url_returns_filename_when_disabled() -> None:
    """未配置外链前缀时，图片引用仍保持文件名。"""
    config = AppConfig()

    assert config.build_external_image_url("示例图片.jpg") == "示例图片.jpg"


def test_build_external_image_url_uses_normalized_base() -> None:
    """外链前缀启用后，应去掉末尾斜杠并拼出完整图片 URL。"""
    config = AppConfig(external_url_base="https://public.example.com/parking-images/")
    config.validate()

    assert config.external_url_base_normalized == "https://public.example.com/parking-images"
    assert (
        config.build_external_image_url("YUEA0C547_enter_20260412_062924.jpg")
        == "https://public.example.com/parking-images/YUEA0C547_enter_20260412_062924.jpg"
    )
