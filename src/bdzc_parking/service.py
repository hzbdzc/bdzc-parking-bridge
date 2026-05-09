"""桥接业务服务，串联解析、筛选、入库、发送和定时清理流程。"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime

from bdzc_parking.common import iso_now, text_or
from bdzc_parking.config import AppConfig
from bdzc_parking.models import SendResult, has_partner_payload_inputs, map_to_partner_payload, should_forward
from bdzc_parking.parser import extract_event, parse_hikvision_payload, raw_body_key
from bdzc_parking.storage import EventStore, load_partner_payload


LOGGER = logging.getLogger(__name__)
_TASK_SENTINEL = object()
_CLEANUP_INTERVAL_SECONDS = 3 * 3600.0
_RETRY_DELAYS_SECONDS = (1.0, 5.0, 10.0)
_SERVICE_WORKER_COUNT = 3
_SERVICE_QUEUE_SIZE = 512
_EVENT_RETENTION_DAYS = 180
_ARTIFACT_RETENTION_DAYS = 180


class PartnerClient:
    """负责向大园区 API 发送转换后的过车记录。"""

    def __init__(self, config: AppConfig):
        """保存 API 地址和请求超时配置。"""
        self.config = config

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
            LOGGER.debug("partner API HTTP error: %s %s", exc.code, body)
            return SendResult(False, attempt, exc.code, body, f"HTTP {exc.code}")
        except urllib.error.URLError as exc:
            LOGGER.debug("partner API URL error: %s", exc)
            return SendResult(False, attempt, error=str(exc.reason))
        except OSError as exc:
            LOGGER.debug("partner API send failed: %s", exc)
            return SendResult(False, attempt, error=str(exc))


@dataclass(frozen=True)
class _HttpIngressTask:
    """HTTP 接收进程交给 service worker 的原始请求任务。"""

    content_type: str
    body: bytes
    client_ip: str
    request_id: int | str


@dataclass(frozen=True)
class _ManualResendTask:
    """GUI 交给 service worker 的手动重发任务。"""

    event_id: int


@dataclass(frozen=True)
class _CleanupTask:
    """service worker 定时执行的数据清理任务。"""

    reason: str


class ParkingBridgeService:
    """处理海康 HTTP 请求并驱动大园区同步。"""

    def __init__(
        self,
        config: AppConfig,
        store: EventStore,
        client: PartnerClient,
        *,
        start_workers: bool = True,
    ):
        """保存配置、事件存储和大园区 API 客户端，并启动统一 worker 池。"""
        self.config = config
        self.store = store
        self.client = client
        self._stop_event = threading.Event()
        self._task_queue: queue.Queue[_HttpIngressTask | _ManualResendTask | _CleanupTask | object] = queue.Queue(
            maxsize=_SERVICE_QUEUE_SIZE
        )
        self._workers: list[threading.Thread] = []
        self._runtime_lock = threading.Lock()
        self._active_count = 0
        self._rejected_count = 0
        self._task_failure_count = 0
        self._last_error = ""
        self._last_error_at = ""
        self._cleanup_pending = False
        self._cleanup_active = False
        self._cleanup_started_at = ""
        self._cleanup_finished_at = ""
        self._cleanup_summary: dict[str, int] = {}
        self._next_cleanup_at = time.monotonic() + _CLEANUP_INTERVAL_SECONDS
        self._send_ids_lock = threading.Lock()
        self._active_send_ids: set[int] = set()

        if start_workers:
            self._start_workers()

    def close(self) -> None:
        """停止 service worker；发送等待会通过 stop_event 尽快中断。"""
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        for _ in self._workers:
            try:
                self._task_queue.put_nowait(_TASK_SENTINEL)
            except queue.Full:
                break
        for worker in self._workers:
            worker.join(timeout=2)

    def handle_request(self, content_type: str, body: bytes, client_ip: str = "unknown") -> None:
        """同步处理一次海康 HTTP 消息，主要供测试和本地工具复用。"""
        try:
            self._handle_ingress_task(
                _HttpIngressTask(content_type, bytes(body), client_ip, request_id="sync")
            )
        except Exception as exc:
            self._record_error("synchronous ingress handling failed", exc)

    def enqueue_http_request(
        self,
        content_type: str,
        body: bytes,
        client_ip: str = "unknown",
        request_id: int | str = "-",
        block: bool = False,
        timeout: float | None = None,
    ) -> bool:
        """把 HTTP 收到的原始请求放入统一 service 队列，队列满时返回 False。"""
        task = _HttpIngressTask(content_type, bytes(body), client_ip, request_id)
        return self._enqueue_task(task, block=block, timeout=timeout, count_rejection=True)

    def manual_resend(self, event_id: int) -> bool:
        """把可重发记录重置为待发送，并交给统一 service worker 处理。"""
        row = self.store.get_event(event_id)
        if row is None:
            LOGGER.info("event %s does not exist", event_id)
            return False
        try:
            payload = load_partner_payload(row, self.config)
        except Exception:
            LOGGER.exception("event %s cannot build partner payload for manual resend", event_id)
            return False
        if not payload:
            LOGGER.info("event %s has no partner payload for manual resend", event_id)
            return False
        if not self.store.set_manual_retry(event_id):
            LOGGER.info("event %s is not resendable", event_id)
            return False
        if not self._enqueue_task(_ManualResendTask(event_id), count_rejection=True):
            self.store.mark_dead_letter(event_id, "service queue is full; manual resend was not scheduled")
            return False
        return True

    def request_cleanup(self, reason: str = "manual") -> bool:
        """投递一次手动旧数据清理任务，并重置下一次定时清理时间。"""
        if self._stop_event.is_set():
            return False
        with self._runtime_lock:
            if self._cleanup_active or self._cleanup_pending:
                return False
            self._cleanup_pending = True

        if not self._enqueue_task(_CleanupTask(reason), count_rejection=True):
            with self._runtime_lock:
                self._cleanup_pending = False
            return False

        with self._runtime_lock:
            self._next_cleanup_at = time.monotonic() + _CLEANUP_INTERVAL_SECONDS
        return True

    def get_status_snapshot(self) -> dict[str, object]:
        """返回 /status 所需的数据库运维指标快照。"""
        return self.store.get_status_snapshot()

    def get_runtime_snapshot(self) -> dict[str, object]:
        """返回不访问 SQLite 的 service 队列、worker 和清理状态快照。"""
        snapshot = self.get_service_snapshot()
        return {
            "queues": {
                "service": snapshot["service_queue_length"],
                "service_rejected": snapshot["service_rejected_count"],
            },
            "workers": {
                "service_alive": snapshot["service_workers_alive"],
                "service_total": snapshot["service_worker_count"],
                "service_active": snapshot["service_active_tasks"],
                "service_idle": snapshot["service_idle_workers"],
            },
            "cleanup": {
                "active": snapshot["cleanup_active"],
                "pending": snapshot["cleanup_pending"],
                "started_at": snapshot["cleanup_started_at"],
                "finished_at": snapshot["cleanup_finished_at"],
                "summary": snapshot["cleanup_summary"],
            },
            "errors": {
                "last_error": snapshot["last_error"],
                "last_error_at": snapshot["last_error_at"],
                "task_failures": snapshot["task_failure_count"],
            },
            "service_queue_length": snapshot["service_queue_length"],
            "service_rejected_count": snapshot["service_rejected_count"],
            "cleanup_active": snapshot["cleanup_active"],
            "last_error": snapshot["last_error"],
        }

    def get_service_snapshot(self) -> dict[str, object]:
        """返回统一 service worker 的运行状态。"""
        with self._runtime_lock:
            active_count = self._active_count
            rejected_count = self._rejected_count
            failure_count = self._task_failure_count
            last_error = self._last_error
            last_error_at = self._last_error_at
            cleanup_pending = self._cleanup_pending
            cleanup_active = self._cleanup_active
            cleanup_started_at = self._cleanup_started_at
            cleanup_finished_at = self._cleanup_finished_at
            cleanup_summary = dict(self._cleanup_summary)
        alive_count = sum(1 for worker in self._workers if worker.is_alive())
        return {
            "service_queue_length": self._task_queue.qsize(),
            "service_queue_size": _SERVICE_QUEUE_SIZE,
            "service_workers_alive": alive_count,
            "service_worker_count": len(self._workers),
            "service_active_tasks": active_count,
            "service_idle_workers": max(0, alive_count - active_count),
            "service_rejected_count": rejected_count,
            "task_failure_count": failure_count,
            "last_error": last_error,
            "last_error_at": last_error_at,
            "cleanup_pending": cleanup_pending,
            "cleanup_active": cleanup_active,
            "cleanup_started_at": cleanup_started_at,
            "cleanup_finished_at": cleanup_finished_at,
            "cleanup_summary": cleanup_summary,
        }

    def is_database_healthy(self) -> bool:
        """检查 SQLite 是否可正常响应，供 /status 健康状态使用。"""
        return self.store.probe_database_health()

    def get_database_health(self) -> dict[str, object]:
        """返回 SQLite 健康详情，供 /status 区分短暂超时和真实错误。"""
        return self.store.probe_database_health_detail()

    def _start_workers(self) -> None:
        """启动固定数量的统一 service worker。"""
        for index in range(_SERVICE_WORKER_COUNT):
            worker = threading.Thread(
                target=self._service_worker_loop,
                name=f"service-worker-{index + 1}",
                daemon=True,
            )
            worker.start()
            self._workers.append(worker)

    def _service_worker_loop(self) -> None:
        """循环消费统一任务队列；顶层兜底保证 worker 不因任务异常退出。"""
        while not self._stop_event.is_set():
            try:
                self._maybe_enqueue_cleanup()
                try:
                    task = self._task_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                if task is _TASK_SENTINEL:
                    self._task_queue.task_done()
                    break

                self._mark_worker_active(1)
                try:
                    self._handle_task(task)
                except Exception as exc:
                    self._record_error("service task failed", exc)
                finally:
                    self._mark_worker_active(-1)
                    self._task_queue.task_done()
            except Exception as exc:
                self._record_error("service worker loop recovered from unexpected error", exc)
                self._stop_event.wait(0.1)

    def _handle_task(self, task: _HttpIngressTask | _ManualResendTask | _CleanupTask | object) -> None:
        """按任务类型分发到具体业务处理函数。"""
        if isinstance(task, _HttpIngressTask):
            self._handle_ingress_task(task)
            return
        if isinstance(task, _ManualResendTask):
            self._send_stored_record(task.event_id)
            return
        if isinstance(task, _CleanupTask):
            self._handle_cleanup_task(task)
            return
        LOGGER.warning("unknown service task ignored: %r", task)

    def _handle_ingress_task(self, task: _HttpIngressTask) -> None:
        """解析并保存一条海康消息，需要发送时在当前 worker 内完成重试。"""
        received_at = datetime.now()
        try:
            raw = parse_hikvision_payload(task.content_type, task.body)
            event = extract_event(raw)
        except Exception as exc:
            LOGGER.exception("failed to parse Hikvision event from ip=%s", task.client_ip)
            event_id = self.store.add_parse_error(
                raw_body_key(task.body),
                str(exc),
                task.content_type,
                task.body,
            )
            return

        can_send, skip_reason = should_forward(event, self.config, received_at)
        partner_payload = self._build_partner_payload(event)
        event_status = "pending" if can_send else "skipped"
        event_id, created = self.store.add_event(
            event,
            status=event_status,
            auto_send=can_send,
            skip_reason=skip_reason,
            partner_payload=partner_payload,
            received_content_type=task.content_type,
            received_body=task.body,
            received_at=received_at.isoformat(timespec="seconds"),
        )
        LOGGER.info(
            "Hik event stored event_id=%s event_key=%s created=%s request_id=%s ip=%s plate=%s direction=%s lane=%s gate=%s time=%s status=%s auto_send=%s%s",
            event_id,
            event.event_key,
            "yes" if created else "no",
            task.request_id,
            task.client_ip,
            text_or(event.plate_no, "-"),
            _direction_text(event.direction),
            text_or(event.lane_name, "-"),
            text_or(event.gate_name, "-"),
            text_or(event.event_time, "-"),
            event_status,
            "yes" if can_send else "no",
            f" skip_reason={skip_reason}" if skip_reason else "",
        )

        if created and can_send:
            self._send_stored_record(event_id)
        elif not created:
            LOGGER.info("duplicate event ignored: %s", event.event_key)

    def _send_stored_record(self, event_id: int) -> None:
        """读取已入库 payload，并在当前 worker 内完成最多四次发送。"""
        if not self._try_mark_event_sending(event_id):
            LOGGER.info("event %s is already being sent", event_id)
            return
        try:
            row = self.store.get_event(event_id)
            if row is None:
                LOGGER.info("event %s does not exist", event_id)
                return

            try:
                payload = load_partner_payload(row, self.config)
            except Exception as exc:
                LOGGER.exception("failed to load partner payload for event %s", event_id)
                self.store.mark_dead_letter(event_id, f"failed to load partner payload: {exc}")
                return

            if not payload:
                LOGGER.info("event %s has no partner payload; mark dead letter", event_id)
                self.store.mark_dead_letter(event_id, "event has no partner payload")
                return

            first_attempt_at = iso_now()
            if not self.store.mark_send_started(event_id, first_attempt_at):
                LOGGER.info("event %s cannot be marked sending", event_id)
                return

            result, last_attempt_at = self._send_payload_with_retries(event_id, payload)
            if result is None:
                reason = "service stopped before retries completed"
                if self.store.reset_interrupted_send(event_id, reason):
                    LOGGER.info("partner send aborted event_id=%s status=pending reason=%s", event_id, reason)
                return
            self.store.finish_send_result(
                event_id,
                result,
                self.client.config.partner_api_url,
                payload,
                first_attempt_at,
                last_attempt_at,
            )
            final_status = "sent" if result.success else "dead_letter"
            LOGGER.info(
                "partner send result event_id=%s attempt=%s success=%s status_code=%s final_status=%s error=%s",
                event_id,
                result.attempts,
                "yes" if result.success else "no",
                result.status_code if result.status_code is not None else "-",
                final_status,
                text_or(result.error, "-"),
            )
        finally:
            self._clear_event_sending(event_id)

    def _send_payload_with_retries(
        self,
        event_id: int,
        payload: dict[str, object],
    ) -> tuple[SendResult | None, str]:
        """按 1/5/10 秒等待策略在当前 worker 内重试发送。"""
        result = SendResult(False, 0, error="send was not attempted")
        last_attempt_at = iso_now()
        max_attempts = len(_RETRY_DELAYS_SECONDS) + 1
        for attempt in range(1, max_attempts + 1):
            last_attempt_at = iso_now()
            try:
                result = self.client.send_once(payload, attempt=attempt)
            except Exception as exc:
                LOGGER.exception("partner client raised for event %s attempt %s", event_id, attempt)
                self._record_error(f"partner send exception event_id={event_id} attempt={attempt}", exc)
                result = SendResult(False, attempt, error=str(exc))

            if result.success:
                return result, last_attempt_at
            if attempt >= max_attempts:
                return result, last_attempt_at

            delay_seconds = _RETRY_DELAYS_SECONDS[attempt - 1]
            LOGGER.info(
                "partner send failed event_id=%s attempt=%s retry_after=%ss error=%s",
                event_id,
                attempt,
                delay_seconds,
                text_or(result.error, "-"),
            )
            if self._stop_event.wait(delay_seconds):
                return None, last_attempt_at
        return result, last_attempt_at

    def _handle_cleanup_task(self, task: _CleanupTask) -> None:
        """执行一次旧数据清理，任何异常由 worker 顶层兜底记录。"""
        with self._runtime_lock:
            self._cleanup_pending = False
            self._cleanup_active = True
            self._cleanup_started_at = iso_now()
        try:
            LOGGER.info("cleanup started reason=%s", task.reason)
            summary = self.store.prune_old_data(
                _EVENT_RETENTION_DAYS,
                _ARTIFACT_RETENTION_DAYS,
            )
            with self._runtime_lock:
                self._cleanup_summary = dict(summary)
                self._cleanup_finished_at = iso_now()
                self._next_cleanup_at = time.monotonic() + _CLEANUP_INTERVAL_SECONDS
            LOGGER.info("cleanup finished reason=%s summary=%s", task.reason, summary)
        except Exception as exc:
            with self._runtime_lock:
                self._cleanup_finished_at = iso_now()
                self._next_cleanup_at = time.monotonic() + _CLEANUP_INTERVAL_SECONDS
            LOGGER.warning("cleanup finished reason=%s success=no error=%s", task.reason, exc)
            raise
        finally:
            with self._runtime_lock:
                self._cleanup_active = False

    def _maybe_enqueue_cleanup(self) -> None:
        """到达三小时间隔后投递清理任务，避免重复投递。"""
        now = time.monotonic()
        with self._runtime_lock:
            if self._cleanup_active or self._cleanup_pending or now < self._next_cleanup_at:
                return
            self._cleanup_pending = True
        if not self._enqueue_task(_CleanupTask("scheduled"), count_rejection=False):
            with self._runtime_lock:
                self._cleanup_pending = False

    def _build_partner_payload(self, event) -> dict[str, object] | None:
        """为可映射记录预生成 payload，供自动发送和手动发送复用。"""
        if not has_partner_payload_inputs(event):
            return None
        return map_to_partner_payload(event, self.config)

    def _enqueue_task(
        self,
        task: _HttpIngressTask | _ManualResendTask | _CleanupTask | object,
        block: bool = False,
        timeout: float | None = None,
        count_rejection: bool = False,
    ) -> bool:
        """把任务放入统一队列，失败时按需累计拒绝计数。"""
        try:
            if block:
                self._task_queue.put(task, block=True, timeout=timeout)
            else:
                self._task_queue.put_nowait(task)
        except queue.Full:
            if count_rejection:
                with self._runtime_lock:
                    self._rejected_count += 1
            LOGGER.warning("service task queue is full; task rejected: %s", type(task).__name__)
            return False
        return True

    def _mark_worker_active(self, delta: int) -> None:
        """增减当前正在执行任务的 worker 数量。"""
        with self._runtime_lock:
            self._active_count = max(0, self._active_count + delta)

    def _record_error(self, context: str, exc: BaseException) -> None:
        """记录最近一次 service 异常，并保证调用方可以继续恢复。"""
        message = f"{context}: {exc}"
        with self._runtime_lock:
            self._task_failure_count += 1
            self._last_error = message
            self._last_error_at = iso_now()
        LOGGER.exception(context)

    def _try_mark_event_sending(self, event_id: int) -> bool:
        """在进程内防止同一事件被多个 worker 同时发送。"""
        with self._send_ids_lock:
            if event_id in self._active_send_ids:
                return False
            self._active_send_ids.add(event_id)
            return True

    def _clear_event_sending(self, event_id: int) -> None:
        """清除进程内发送占用标记。"""
        with self._send_ids_lock:
            self._active_send_ids.discard(event_id)


def _direction_text(value: str) -> str:
    """把海康方向字段转换成适合日志阅读的中文文本。"""
    if value == "enter":
        return "进场"
    if value == "exit":
        return "出场"
    return value or "-"


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
