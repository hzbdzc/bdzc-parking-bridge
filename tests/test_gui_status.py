"""GUI runtime status text helper tests."""

from __future__ import annotations

from bdzc_parking.gui import _runtime_status_bar_segments, _runtime_status_bar_text


def test_runtime_status_bar_text_shows_idle_workers() -> None:
    """状态栏文本应显示 HTTP 和 service worker 空闲状态。"""
    text = _runtime_status_bar_text(
        {"display_text": "运行中"},
        {"server_port": 1888, "process_pid": 1234},
        {
            "queues": {"send": 0, "http_ingress": 0},
            "workers": {
                "send_alive": 1,
                "send_total": 1,
                "send_active": 0,
                "http_ingress_alive": 1,
                "http_ingress_total": 1,
                "http_ingress_active": 0,
                "maintenance_alive": 1,
                "maintenance_total": 1,
                "maintenance_active": 0,
            },
        },
    )

    assert "HTTP: 运行中 port=1888 pid=1234" in text
    assert "发送: 1/1 空闲 q=0" in text
    assert "接收: 1/1 空闲 q=0" in text
    assert "维护: 1/1 空闲" in text


def test_runtime_status_bar_text_shows_stopped_and_busy_workers() -> None:
    """状态栏文本应区分停止和忙碌 worker。"""
    control = {"display_text": "未运行"}
    lifecycle: dict[str, object] = {}
    runtime = {
        "queues": {"send": 2, "http_ingress": 3},
        "workers": {
            "send_alive": 1,
            "send_total": 1,
            "send_active": 1,
            "http_ingress_alive": 0,
            "http_ingress_total": 1,
            "http_ingress_active": 0,
            "maintenance_alive": 1,
            "maintenance_total": 1,
            "maintenance_active": 1,
        },
    }
    text = _runtime_status_bar_text(control, lifecycle, runtime)
    segments = _runtime_status_bar_segments(control, lifecycle, runtime)

    assert "发送: 1/1 忙碌 q=2" in text
    assert "接收: 0/1 停止 q=3" in text
    assert "维护: 1/1 忙碌" in text
    assert segments["send"]["severity"] == "busy"
    assert segments["http_ingress"]["severity"] == "error"
    assert segments["maintenance"]["severity"] == "busy"


def test_runtime_status_bar_text_merges_http_failure_detail() -> None:
    """HTTP 故障信息应合并到底部状态栏文本。"""
    text = _runtime_status_bar_text(
        {"display_text": "未运行"},
        {},
        {
            "queues": {},
            "workers": {
                "send_alive": 0,
                "send_total": 1,
                "http_ingress_total": 0,
                "maintenance_total": 0,
            },
        },
    )

    failed_text = _runtime_status_bar_text(
        {"display_text": "故障", "severity": "error", "detail": "port already in use"},
        {},
        {"queues": {}, "workers": {}},
    )

    assert "HTTP: 未运行" in text
    assert "HTTP: 故障: port already in use" in failed_text
