"""GUI runtime status text helper tests."""

from __future__ import annotations

from bdzc_parking.gui import _runtime_status_bar_segments, _runtime_status_bar_text


def test_runtime_status_bar_text_shows_idle_service_workers() -> None:
    """状态栏文本应显示 HTTP 和统一 service worker 空闲状态。"""
    text = _runtime_status_bar_text(
        {"display_text": "运行中"},
        {"server_port": 1888, "process_pid": 1234},
        {
            "queues": {"service": 0, "service_rejected": 0},
            "workers": {
                "service_alive": 3,
                "service_total": 3,
                "service_active": 0,
            },
            "cleanup": {"active": False},
        },
    )

    assert "HTTP: 运行中 port=1888 pid=1234" in text
    assert "Service: 3/3 空闲 q=0 rejected=0" in text


def test_runtime_status_bar_text_shows_stopped_and_busy_service_workers() -> None:
    """状态栏文本应区分停止和忙碌 service worker。"""
    control = {"display_text": "未运行"}
    lifecycle: dict[str, object] = {}
    runtime = {
        "queues": {"service": 2, "service_rejected": 1},
        "workers": {
            "service_alive": 2,
            "service_total": 3,
            "service_active": 1,
        },
        "cleanup": {"active": False},
    }
    text = _runtime_status_bar_text(control, lifecycle, runtime)
    segments = _runtime_status_bar_segments(control, lifecycle, runtime)

    assert "Service: 2/3 忙碌 q=2 rejected=1" in text
    assert segments["service"]["severity"] == "busy"

    stopped = _runtime_status_bar_segments(
        control,
        lifecycle,
        {"queues": {}, "workers": {"service_alive": 0, "service_total": 3}},
    )
    assert stopped["service"]["severity"] == "error"


def test_runtime_status_bar_text_merges_http_failure_detail() -> None:
    """HTTP 故障信息应合并到状态栏文本。"""
    text = _runtime_status_bar_text(
        {"display_text": "未运行"},
        {},
        {"queues": {}, "workers": {"service_alive": 0, "service_total": 3}},
    )

    failed_text = _runtime_status_bar_text(
        {"display_text": "故障", "severity": "error", "detail": "port already in use"},
        {},
        {"queues": {}, "workers": {}},
    )

    assert "HTTP: 未运行" in text
    assert "HTTP: 故障: port already in use" in failed_text
