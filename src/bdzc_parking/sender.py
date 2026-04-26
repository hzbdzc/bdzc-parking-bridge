"""大园区停车 API 客户端。"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request

from bdzc_parking.config import AppConfig
from bdzc_parking.models import SendResult

LOGGER = logging.getLogger(__name__)


class PartnerClient:
    """负责向大园区 API 发送转换后的过车记录。"""

    def __init__(self, config: AppConfig):
        """保存 API 地址、超时和重试等发送配置。"""
        self.config = config

    def send_with_retry(self, payload: dict[str, object]) -> SendResult:
        """按配置重试发送请求，返回最后一次发送结果。"""
        max_attempts = self.config.retry_count + 1
        last_result = SendResult(success=False, attempts=0, error="not attempted")

        # 总尝试次数 = 首次发送 + retry_count 次重试。
        for attempt in range(1, max_attempts + 1):
            last_result = self.send_once(payload, attempt)
            if last_result.success:
                return last_result
            if attempt < max_attempts:
                time.sleep(self.config.retry_delay_seconds)

        return last_result

    def send_once(self, payload: dict[str, object], attempt: int = 1) -> SendResult:
        """向大园区 API 发起一次 HTTP POST。"""
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            self.config.partner_api_url,
            data=data,
            method="POST",
            headers={"Content-Type": "text/json; charset=utf-8"},
        )

        try:
            # 运行环境常带全局代理变量，桥接到园区/本机接口时要显式直连。
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open(request, timeout=self.config.request_timeout_seconds) as response:
                response_body = response.read().decode("utf-8", errors="replace")
                return _interpret_response(attempt, response.status, response_body)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            LOGGER.warning("partner API HTTP error: %s %s", exc.code, body)
            return SendResult(False, attempt, exc.code, body, f"HTTP {exc.code}")
        except urllib.error.URLError as exc:
            LOGGER.warning("partner API URL error: %s", exc)
            return SendResult(False, attempt, error=str(exc.reason))
        except OSError as exc:
            LOGGER.warning("partner API send failed: %s", exc)
            return SendResult(False, attempt, error=str(exc))


def _interpret_response(attempt: int, status_code: int, response_text: str) -> SendResult:
    """按大园区 API 约定解释 HTTP 响应是否成功。"""
    try:
        data = json.loads(response_text)
    except json.JSONDecodeError:
        return SendResult(False, attempt, status_code, response_text, "partner response is not JSON")

    partner_status = data.get("status")
    if status_code == 200 and str(partner_status) == "200":
        return SendResult(True, attempt, status_code, response_text)

    msg = data.get("msg") or f"partner status={partner_status}"
    return SendResult(False, attempt, status_code, response_text, str(msg))
