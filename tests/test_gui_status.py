"""GUI runtime status text helper tests."""

from __future__ import annotations

from bdzc_parking.gui import _runtime_status_bar_segments, _runtime_status_bar_text


def test_runtime_status_bar_text_shows_http_process() -> None:
    """状态栏文本应显示 HTTP 子进程状态。"""
    text = _runtime_status_bar_text(
        {"display_text": "运行中"},
        {"server_port": 1888, "process_pid": 1234},
        {"health": {"failure_count": 0}},
    )

    assert text == "HTTP: 运行中 port=1888 pid=1234"


def test_runtime_status_bar_text_merges_http_failure_detail() -> None:
    """HTTP 故障信息应合并到状态栏文本。"""
    text = _runtime_status_bar_text(
        {"display_text": "未运行"},
        {},
        {},
    )

    failed_text = _runtime_status_bar_text(
        {"display_text": "故障", "severity": "error", "detail": "port already in use"},
        {},
        {},
    )
    segments = _runtime_status_bar_segments(
        {"display_text": "故障", "severity": "error", "detail": "port already in use"},
        {},
        {},
    )

    assert text == "HTTP: 未运行"
    assert failed_text == "HTTP: 故障: port already in use"
    assert segments["http"]["severity"] == "error"
