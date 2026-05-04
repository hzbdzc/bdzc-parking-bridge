"""大园区 API 客户端发送逻辑测试。"""

from __future__ import annotations

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from bdzc_parking.config import AppConfig
from bdzc_parking.sender import PartnerClient
from helpers import free_tcp_port


class FakePartnerHandler(BaseHTTPRequestHandler):
    """用于测试 PartnerClient 的本地假大园区 HTTP handler。"""

    calls = 0
    request_bodies: list[bytes] = []
    fail_until = 0

    def do_POST(self) -> None:
        """记录请求体，并按 fail_until 控制返回成功或临时失败。"""
        type(self).calls += 1
        length = int(self.headers.get("Content-Length", "0") or "0")
        type(self).request_bodies.append(self.rfile.read(length))

        if type(self).calls <= type(self).fail_until:
            body = b'{"status":500,"msg":"temporary"}'
        else:
            body = b'{"status":200,"msg":"ok"}'

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        """测试中屏蔽 BaseHTTPRequestHandler 默认访问日志。"""
        return


def test_sender_posts_text_json_and_interprets_success() -> None:
    """客户端应以 text/json POST，并识别大园区成功响应。"""
    with _fake_server(fail_until=0) as url:
        client = PartnerClient(AppConfig(partner_api_url=url))
        result = client.send_once({"car": "浙A0C547"})

    assert result.success is True
    assert result.attempts == 1
    assert FakePartnerHandler.calls == 1
    assert json.loads(FakePartnerHandler.request_bodies[0].decode("utf-8"))["car"] == "浙A0C547"


def test_sender_interprets_partner_failure_without_retrying() -> None:
    """客户端只发送一次，业务重试由 service 持久化状态机调度。"""
    with _fake_server(fail_until=99) as url:
        client = PartnerClient(AppConfig(partner_api_url=url))
        result = client.send_once({"car": "浙A0C547"})

    assert result.success is False
    assert result.attempts == 1
    assert FakePartnerHandler.calls == 1
    assert result.error == "temporary"


def test_sender_url_error_logs_debug_not_warning(caplog) -> None:
    """底层 urllib 连接错误只作为 DEBUG 细节记录，业务结果由 service 汇总。"""
    port = free_tcp_port()
    caplog.set_level(logging.DEBUG, logger="bdzc_parking.sender")
    client = PartnerClient(
        AppConfig(
            partner_api_url=f"http://127.0.0.1:{port}/api",
            request_timeout_seconds=0.2,
        )
    )

    result = client.send_once({"car": "浙A0C547"})

    assert result.success is False
    sender_records = [record for record in caplog.records if record.name == "bdzc_parking.sender"]
    assert any(
        record.levelno == logging.DEBUG and "partner API URL error" in record.getMessage()
        for record in sender_records
    )
    assert not any(record.levelno >= logging.WARNING for record in sender_records)


class _fake_server:
    """测试用临时 HTTP server 上下文管理器。"""

    def __init__(self, fail_until: int):
        """保存失败次数，并延迟到进入上下文时启动 server。"""
        self.fail_until = fail_until
        self.server: ThreadingHTTPServer | None = None
        self.thread: threading.Thread | None = None

    def __enter__(self) -> str:
        """启动本地 HTTP server，并返回可请求的 API URL。"""
        FakePartnerHandler.calls = 0
        FakePartnerHandler.request_bodies = []
        FakePartnerHandler.fail_until = self.fail_until
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), FakePartnerHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return f"http://127.0.0.1:{self.server.server_port}/api"

    def __exit__(self, exc_type, exc, tb) -> None:
        """关闭测试 HTTP server 并等待线程退出。"""
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
        if self.thread is not None:
            self.thread.join(timeout=5)
