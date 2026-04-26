"""桥接业务服务，串联解析、筛选、入库和发送流程。"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta

from bdzc_parking.config import AppConfig
from bdzc_parking.models import has_partner_payload_inputs, map_to_partner_payload, should_forward
from bdzc_parking.parser import extract_event, parse_hikvision_payload, raw_body_key
from bdzc_parking.sender import PartnerClient
from bdzc_parking.storage import EventStore, load_partner_payload


LOGGER = logging.getLogger(__name__)
Listener = Callable[[int], None]
_QUEUE_SENTINEL = -1
_MAINTENANCE_INTERVAL_SECONDS = 0.5
_CLEANUP_INTERVAL_SECONDS = 3600.0
_RETRY_DELAYS_SECONDS = (1.0, 5.0, 10.0)


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
        self._send_queue: queue.Queue[int] = queue.Queue(maxsize=self.config.sender_queue_size)
        self._workers: list[threading.Thread] = []

        recovered = self.store.recover_stale_sending(self.config.stale_sending_seconds)
        if recovered:
            LOGGER.warning("recovered %s stale sending records", recovered)

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
        for _ in self._workers:
            try:
                self._send_queue.put_nowait(_QUEUE_SENTINEL)
            except queue.Full:
                break
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
        LOGGER.info(
            "Hik event ip=%s plate=%s direction=%s lane=%s gate=%s time=%s auto_send=%s%s",
            client_ip,
            _log_text(event.plate_no, "-"),
            _direction_text(event.direction),
            _log_text(event.lane_name, "-"),
            _log_text(event.gate_name, "-"),
            _log_text(event.event_time, "-"),
            "yes" if can_send else "no",
            f" skip_reason={skip_reason}" if skip_reason else "",
        )
        event_id, created = self.store.add_event(
            event,
            status="pending" if can_send else "skipped",
            auto_send=can_send,
            skip_reason=skip_reason,
            partner_payload=partner_payload,
            received_content_type=content_type,
            received_body=body,
            received_at=received_at.isoformat(timespec="seconds"),
        )
        self._notify(event_id)

        if created and can_send:
            self.send_record_async(event_id)
        elif not created:
            LOGGER.info("duplicate event ignored: %s", event.event_key)

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
        self._notify(event_id)

    def get_status_snapshot(self) -> dict[str, object]:
        """返回 /status 所需的最小运维指标快照。"""
        snapshot = self.store.get_status_snapshot()
        snapshot.update(
            {
                "time": datetime.now().isoformat(timespec="seconds"),
                "server_running": True,
                "queue_length": self._send_queue.qsize(),
            }
        )
        return snapshot

    def is_database_healthy(self) -> bool:
        """检查 SQLite 是否可正常响应，供 /healthz 使用。"""
        return self.store.probe_database_health()

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
        for index in range(self.config.sender_worker_count):
            worker = threading.Thread(
                target=self._send_worker_loop,
                name=f"partner-sender-{index + 1}",
                daemon=True,
            )
            worker.start()
            self._workers.append(worker)

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
        batch_limit = max(self.config.sender_queue_size, self.config.sender_worker_count * 2)
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
            self.config.event_retention_days,
            self.config.artifact_retention_days,
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


def _log_text(value: object, fallback: str = "-") -> str:
    """把日志字段中的空值统一转换成更容易识别的占位文本。"""
    text = str(value or "").strip()
    return text or fallback


def _next_retry_at(attempts: int) -> str:
    """按固定 1/5/10 秒策略计算下一次重试时间，超出次数则放弃自动重试。"""
    if attempts <= 0 or attempts > len(_RETRY_DELAYS_SECONDS):
        return ""
    delay_seconds = _RETRY_DELAYS_SECONDS[attempts - 1]
    return (datetime.now() + timedelta(seconds=delay_seconds)).isoformat(timespec="seconds")
