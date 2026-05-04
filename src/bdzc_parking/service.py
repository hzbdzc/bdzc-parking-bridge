"""桥接业务服务，串联解析、筛选、入库和发送流程。"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from bdzc_parking.common import iso_seconds_from_now, text_or
from bdzc_parking.config import AppConfig
from bdzc_parking.models import has_partner_payload_inputs, map_to_partner_payload, should_forward
from bdzc_parking.parser import extract_event, parse_hikvision_payload, raw_body_key
from bdzc_parking.sender import PartnerClient
from bdzc_parking.storage import EventStore, load_partner_payload


LOGGER = logging.getLogger(__name__)
Listener = Callable[[int], None]
_QUEUE_SENTINEL = -1
_HTTP_INGRESS_SENTINEL = object()
_MAINTENANCE_INTERVAL_SECONDS = 0.5
_CLEANUP_INTERVAL_SECONDS = 3600.0
_RETRY_DELAYS_SECONDS = (1.0, 5.0, 10.0)
_SENDER_WORKER_COUNT = 4
_SENDER_QUEUE_SIZE = 1000
_HTTP_INGRESS_QUEUE_SIZE = 256
_HTTP_INGRESS_WORKER_COUNT = 1
_STALE_SENDING_SECONDS = 300.0
_EVENT_RETENTION_DAYS = 180
_ARTIFACT_RETENTION_DAYS = 30


@dataclass(frozen=True)
class _HttpIngressRequest:
    """HTTP 接收线程交给后台 worker 的原始请求任务。"""

    content_type: str
    body: bytes
    client_ip: str
    request_id: int | str


class ParkingBridgeService:
    """处理海康 HTTP 请求并驱动大园区同步。"""

    def __init__(self, config: AppConfig, store: EventStore, client: PartnerClient):
        """保存配置、事件存储和大园区 API 客户端。"""
        self.config = config
        self.store = store
        self.client = client
        self._listeners: list[Listener] = []
        self._listeners_lock = threading.Lock()
        self._scheduled_event_ids: set[int] = set()
        self._scheduled_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._maintenance_wakeup = threading.Event()
        self._send_queue: queue.Queue[int] = queue.Queue(maxsize=_SENDER_QUEUE_SIZE)
        self._workers: list[threading.Thread] = []
        self._http_ingress_queue: queue.Queue[_HttpIngressRequest | object] = queue.Queue(
            maxsize=_HTTP_INGRESS_QUEUE_SIZE
        )
        self._http_ingress_workers: list[threading.Thread] = []
        self._http_ingress_lock = threading.Lock()
        self._http_ingress_active_count = 0
        self._http_ingress_rejected_count = 0

        recovered = self.store.recover_stale_sending(_STALE_SENDING_SECONDS)
        if recovered:
            LOGGER.warning("recovered %s stale sending records", recovered)

        self._start_http_ingress_workers()
        self._start_workers()
        self._run_cleanup()
        self._schedule_pending_events()
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_loop,
            name="parking-bridge-maintenance",
            daemon=True,
        )
        self._maintenance_thread.start()

    def close(self) -> None:
        """停止后台 worker 和维护线程。"""
        if self._stop_event.is_set():
            return
        self._stop_event.set()
        self._maintenance_wakeup.set()
        for _ in self._http_ingress_workers:
            try:
                self._http_ingress_queue.put_nowait(_HTTP_INGRESS_SENTINEL)
            except queue.Full:
                break
        for _ in self._workers:
            try:
                self._send_queue.put_nowait(_QUEUE_SENTINEL)
            except queue.Full:
                break
        for worker in self._http_ingress_workers:
            worker.join(timeout=2)
        for worker in self._workers:
            worker.join(timeout=2)
        self._maintenance_thread.join(timeout=2)

    def add_listener(self, listener: Listener) -> None:
        """注册事件变化监听器，供 GUI 刷新表格使用。"""
        with self._listeners_lock:
            self._listeners.append(listener)

    def handle_request(self, content_type: str, body: bytes, client_ip: str = "unknown") -> None:
        """处理一次海康 HTTP 消息：解析、入库，并在需要时自动发送。"""
        received_at = datetime.now()

        try:
            raw = parse_hikvision_payload(content_type, body)
            event = extract_event(raw)
        except Exception as exc:
            LOGGER.exception("failed to parse Hikvision event from ip=%s", client_ip)
            event_id = self.store.add_parse_error(raw_body_key(body), str(exc), content_type, body)
            self._notify(event_id)
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
            received_content_type=content_type,
            received_body=body,
            received_at=received_at.isoformat(timespec="seconds"),
        )
        LOGGER.info(
            "Hik event stored event_id=%s event_key=%s created=%s ip=%s plate=%s direction=%s lane=%s gate=%s time=%s status=%s auto_send=%s%s",
            event_id,
            event.event_key,
            "yes" if created else "no",
            client_ip,
            text_or(event.plate_no, "-"),
            _direction_text(event.direction),
            text_or(event.lane_name, "-"),
            text_or(event.gate_name, "-"),
            text_or(event.event_time, "-"),
            event_status,
            "yes" if can_send else "no",
            f" skip_reason={skip_reason}" if skip_reason else "",
        )
        self._notify(event_id)

        if created and can_send:
            self.send_record_async(event_id)
        elif not created:
            LOGGER.info("duplicate event ignored: %s", event.event_key)

    def enqueue_http_request(
        self,
        content_type: str,
        body: bytes,
        client_ip: str = "unknown",
        request_id: int | str = "-",
    ) -> bool:
        """把 HTTP 收到的原始请求放入有界接收队列，队列满时返回 False。"""
        item = _HttpIngressRequest(content_type, bytes(body), client_ip, request_id)
        try:
            self._http_ingress_queue.put_nowait(item)
        except queue.Full:
            with self._http_ingress_lock:
                self._http_ingress_rejected_count += 1
            LOGGER.warning(
                "HTTP ingress queue is full request_id=%s client=%s size=%s",
                request_id,
                client_ip,
                _HTTP_INGRESS_QUEUE_SIZE,
            )
            return False
        return True

    def send_record_async(self, event_id: int) -> None:
        """把指定事件加入后台发送队列。"""
        self._schedule_send(event_id)

    def send_record(self, event_id: int) -> None:
        """同步发送一条已入库事件，并回写 sending/重试/死信状态。"""
        row = self.store.claim_ready_event(event_id)
        if row is None:
            return
        self._notify(event_id)

        try:
            payload = load_partner_payload(row, self.config)
        except Exception as exc:
            LOGGER.exception("failed to load partner payload for event %s", event_id)
            self.store.mark_dead_letter(event_id, f"failed to load partner payload: {exc}")
            self._notify(event_id)
            return

        if not payload:
            LOGGER.info("event %s has no partner payload; mark dead letter", event_id)
            self.store.mark_dead_letter(event_id, "event has no partner payload")
            self._notify(event_id)
            return

        attempt = int(row.get("attempts") or 0) + 1
        self.store.record_send_request(event_id, self.client.config.partner_api_url, payload)
        result = self.client.send_once(payload, attempt=attempt)
        next_retry_at = "" if result.success else _next_retry_at(result.attempts)
        self.store.update_send_result(event_id, result, next_retry_at or None)
        final_status = "sent" if result.success else ("failed_retryable" if next_retry_at else "dead_letter")
        LOGGER.info(
            "partner send result event_id=%s attempt=%s success=%s status_code=%s final_status=%s next_retry_at=%s error=%s",
            event_id,
            result.attempts,
            "yes" if result.success else "no",
            result.status_code if result.status_code is not None else "-",
            final_status,
            next_retry_at or "-",
            text_or(result.error, "-"),
        )
        self._notify(event_id)

    def get_status_snapshot(self) -> dict[str, object]:
        """返回 /status 所需的数据库运维指标快照。"""
        return self.store.get_status_snapshot()

    def get_runtime_snapshot(self) -> dict[str, object]:
        """返回不访问 SQLite 的运行队列和 worker 快照。"""
        ingress = self.get_http_ingress_snapshot()
        return {
            "queues": {
                "send": self._send_queue.qsize(),
                "http_ingress": ingress["http_ingress_queue_length"],
                "http_ingress_active": ingress["http_ingress_active_requests"],
                "http_ingress_rejected": ingress["http_ingress_rejected_count"],
            },
            "workers": {
                "http_ingress_alive": ingress["http_ingress_workers_alive"],
                "http_ingress_total": ingress["http_ingress_worker_count"],
            },
        }

    def is_database_healthy(self) -> bool:
        """检查 SQLite 是否可正常响应，供 /status 健康状态使用。"""
        return self.store.probe_database_health()

    def get_http_ingress_snapshot(self) -> dict[str, object]:
        """返回 HTTP 接收队列和后台 worker 的运行状态。"""
        with self._http_ingress_lock:
            active_count = self._http_ingress_active_count
            rejected_count = self._http_ingress_rejected_count
        return {
            "http_ingress_queue_length": self._http_ingress_queue.qsize(),
            "http_ingress_queue_size": _HTTP_INGRESS_QUEUE_SIZE,
            "http_ingress_workers_alive": sum(
                1 for worker in self._http_ingress_workers if worker.is_alive()
            ),
            "http_ingress_worker_count": len(self._http_ingress_workers),
            "http_ingress_active_requests": active_count,
            "http_ingress_rejected_count": rejected_count,
        }

    def manual_resend(self, event_id: int) -> None:
        """把可重发记录改回待发送状态，并异步重新发送。"""
        if not self.store.set_manual_retry(event_id):
            LOGGER.info("event %s is not resendable", event_id)
            return
        self._notify(event_id)
        self.send_record_async(event_id)

    def _build_partner_payload(self, event) -> dict[str, object] | None:
        """为可映射记录预生成 payload，供手动发送复用。"""
        if not has_partner_payload_inputs(event):
            return None
        return map_to_partner_payload(event, self.config)

    def _notify(self, event_id: int) -> None:
        """通知所有监听器某条事件记录发生变化。"""
        with self._listeners_lock:
            listeners = list(self._listeners)
        for listener in listeners:
            try:
                listener(event_id)
            except Exception:
                LOGGER.exception("event listener failed")

    def _start_workers(self) -> None:
        """启动固定数量的发送 worker。"""
        for index in range(_SENDER_WORKER_COUNT):
            worker = threading.Thread(
                target=self._send_worker_loop,
                name=f"partner-sender-{index + 1}",
                daemon=True,
            )
            worker.start()
            self._workers.append(worker)

    def _start_http_ingress_workers(self) -> None:
        """启动固定数量的 HTTP 接收 worker。"""
        for index in range(_HTTP_INGRESS_WORKER_COUNT):
            worker = threading.Thread(
                target=self._http_ingress_worker_loop,
                name=f"http-ingress-{index + 1}",
                daemon=True,
            )
            worker.start()
            self._http_ingress_workers.append(worker)

    def _http_ingress_worker_loop(self) -> None:
        """循环消费 HTTP 原始请求队列，隔离慢业务和 HTTP 请求线程。"""
        while True:
            try:
                item = self._http_ingress_queue.get(timeout=0.5)
            except queue.Empty:
                if self._stop_event.is_set():
                    break
                continue

            if item is _HTTP_INGRESS_SENTINEL:
                self._http_ingress_queue.task_done()
                break
            if not isinstance(item, _HttpIngressRequest):
                self._http_ingress_queue.task_done()
                continue

            with self._http_ingress_lock:
                self._http_ingress_active_count += 1
            try:
                LOGGER.debug(
                    "HTTP ingress worker handling request_id=%s client=%s bytes=%s",
                    item.request_id,
                    item.client_ip,
                    len(item.body),
                )
                self.handle_request(item.content_type, item.body, client_ip=item.client_ip)
            except Exception:
                LOGGER.exception(
                    "HTTP ingress worker failed request_id=%s client=%s",
                    item.request_id,
                    item.client_ip,
                )
            finally:
                with self._http_ingress_lock:
                    self._http_ingress_active_count -= 1
                self._http_ingress_queue.task_done()

    def _send_worker_loop(self) -> None:
        """循环消费发送队列中的待发送记录。"""
        while not self._stop_event.is_set():
            try:
                event_id = self._send_queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if event_id == _QUEUE_SENTINEL:
                self._send_queue.task_done()
                break
            try:
                self.send_record(event_id)
            except Exception:
                LOGGER.exception("background send failed for event %s", event_id)
            finally:
                with self._scheduled_lock:
                    self._scheduled_event_ids.discard(event_id)
                self._send_queue.task_done()
                self._maintenance_wakeup.set()

    def _schedule_send(self, event_id: int) -> None:
        """调度单条记录进入发送队列；队列满时等待补捞线程稍后重试。"""
        if self._enqueue_send(event_id):
            return
        self._maintenance_wakeup.set()

    def _enqueue_send(self, event_id: int) -> bool:
        """尝试把记录放入发送队列，避免重复入队。"""
        with self._scheduled_lock:
            if event_id in self._scheduled_event_ids:
                return True
            self._scheduled_event_ids.add(event_id)
        try:
            self._send_queue.put_nowait(event_id)
            return True
        except queue.Full:
            with self._scheduled_lock:
                self._scheduled_event_ids.discard(event_id)
            LOGGER.warning("send queue is full; event %s remains pending", event_id)
            return False

    def _schedule_pending_events(self) -> None:
        """扫描数据库中的可发送记录，并补入发送队列。"""
        batch_limit = max(_SENDER_QUEUE_SIZE, _SENDER_WORKER_COUNT * 2)
        for event_id in self.store.list_ready_event_ids(limit=batch_limit):
            if not self._enqueue_send(event_id):
                break

    def _maintenance_loop(self) -> None:
        """周期性补捞 ready 记录，并执行保留期清理。"""
        last_cleanup_at = time.monotonic()
        while not self._stop_event.is_set():
            self._maintenance_wakeup.wait(_MAINTENANCE_INTERVAL_SECONDS)
            self._maintenance_wakeup.clear()
            if self._stop_event.is_set():
                break
            self._schedule_pending_events()
            now = time.monotonic()
            if now - last_cleanup_at >= _CLEANUP_INTERVAL_SECONDS:
                self._run_cleanup()
                last_cleanup_at = now

    def _run_cleanup(self) -> None:
        """执行事件和附件保留期清理。"""
        summary = self.store.prune_old_data(
            _EVENT_RETENTION_DAYS,
            _ARTIFACT_RETENTION_DAYS,
        )
        if any(summary.values()):
            LOGGER.info("cleanup summary: %s", summary)


def _direction_text(value: str) -> str:
    """把海康方向字段转换成适合日志阅读的中文文本。"""
    if value == "enter":
        return "进场"
    if value == "exit":
        return "出场"
    return value or "-"


def _next_retry_at(attempts: int) -> str:
    """按固定 1/5/10 秒策略计算下一次重试时间，超出次数则放弃自动重试。"""
    if attempts <= 0 or attempts > len(_RETRY_DELAYS_SECONDS):
        return ""
    delay_seconds = _RETRY_DELAYS_SECONDS[attempts - 1]
    return iso_seconds_from_now(delay_seconds)
