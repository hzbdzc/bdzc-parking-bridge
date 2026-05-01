"""SQLite 事件存储模块，保存解析、过滤和发送状态。"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import threading
from datetime import datetime, timedelta
from email import policy
from email.parser import BytesParser
from pathlib import Path
from typing import Any

from bdzc_parking.config import AppConfig
from bdzc_parking.models import HikEvent, SendResult


LOGGER = logging.getLogger(__name__)
SQLITE_BUSY_TIMEOUT_MS = 5000
SQLITE_HEALTH_TIMEOUT_SECONDS = 1.0
EVENT_FILTER_VALUE_LIMIT = 500
EVENT_FILTER_COLUMNS = {
    "direction": "direction",
    "lane_name": "lane_name",
    "passing_type": "passing_type",
    "status": "status",
}
EVENT_DATE_EXPRESSION = "substr(COALESCE(NULLIF(event_time, ''), received_at), 1, 10)"
_FILENAME_TOKEN_MAP = {
    "京": "JING",
    "津": "JIN",
    "沪": "HU",
    "渝": "YU",
    "冀": "JI",
    "晋": "JIN",
    "蒙": "MENG",
    "辽": "LIAO",
    "吉": "JI",
    "黑": "HEI",
    "苏": "SU",
    "浙": "ZHE",
    "皖": "WAN",
    "闽": "MIN",
    "赣": "GAN",
    "鲁": "LU",
    "豫": "YU",
    "鄂": "E",
    "湘": "XIANG",
    "粤": "YUE",
    "桂": "GUI",
    "琼": "QIONG",
    "川": "CHUAN",
    "蜀": "SHU",
    "贵": "GUI",
    "云": "YUN",
    "滇": "DIAN",
    "藏": "ZANG",
    "陕": "SHAAN",
    "秦": "QIN",
    "甘": "GAN",
    "青": "QING",
    "宁": "NING",
    "新": "XIN",
    "港": "HK",
    "澳": "MO",
    "学": "XUE",
    "警": "JING",
    "领": "LING",
    "使": "SHI",
    "挂": "GUA",
    "临": "LIN",
    "试": "SHI",
    "超": "CHAO",
}


class _ClosingConnection(sqlite3.Connection):
    """离开 with 代码块时自动关闭文件句柄的 SQLite 连接。"""

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        """保持 sqlite3 原有提交/回滚语义，并在最后关闭连接。"""
        try:
            return bool(super().__exit__(exc_type, exc_value, traceback))
        finally:
            self.close()


class EventStore:
    """封装事件表的读写操作。"""

    def __init__(self, db_path: Path):
        """初始化数据库路径、线程锁，并确保表结构存在。"""
        self.db_path = db_path
        self.image_dir = self.db_path.parent / "images"
        self.request_dir = self.db_path.parent / "raw_requests"
        self._lock = threading.RLock()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def init_db(self) -> None:
        """创建事件记录表。"""
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_key TEXT NOT NULL UNIQUE,
                    received_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    auto_send INTEGER NOT NULL DEFAULT 0,
                    skip_reason TEXT NOT NULL DEFAULT '',
                    event_time TEXT NOT NULL DEFAULT '',
                    direction TEXT NOT NULL DEFAULT '',
                    passing_type TEXT NOT NULL DEFAULT '',
                    plate_no TEXT NOT NULL DEFAULT '',
                    gate_name TEXT NOT NULL DEFAULT '',
                    lane_name TEXT NOT NULL DEFAULT '',
                    raw_json TEXT NOT NULL DEFAULT '',
                    received_content_type TEXT NOT NULL DEFAULT '',
                    received_body_path TEXT NOT NULL DEFAULT '',
                    partner_payload_json TEXT NOT NULL DEFAULT '',
                    last_request_url TEXT NOT NULL DEFAULT '',
                    last_request_payload_json TEXT NOT NULL DEFAULT '',
                    image_name TEXT NOT NULL DEFAULT '',
                    image_content_type TEXT NOT NULL DEFAULT '',
                    image_path TEXT NOT NULL DEFAULT '',
                    image_data BLOB,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    first_attempt_at TEXT NOT NULL DEFAULT '',
                    last_attempt_at TEXT NOT NULL DEFAULT '',
                    next_retry_at TEXT NOT NULL DEFAULT '',
                    dead_lettered_at TEXT NOT NULL DEFAULT '',
                    status_code INTEGER,
                    response_text TEXT NOT NULL DEFAULT '',
                    last_error TEXT NOT NULL DEFAULT ''
                )
                """
            )
            self._ensure_image_columns(conn)
            self._ensure_indexes(conn)
            self._migrate_legacy_failed_statuses(conn)
            self._migrate_existing_images(conn)
            self._rename_existing_image_files(conn)

    def add_event(
        self,
        event: HikEvent,
        status: str,
        auto_send: bool,
        skip_reason: str = "",
        partner_payload: dict[str, object] | None = None,
        received_content_type: str = "",
        received_body: bytes | None = None,
        received_at: str | None = None,
    ) -> tuple[int, bool]:
        """新增一条已解析事件，返回记录 ID 和是否首次创建。"""
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM events WHERE event_key = ?", (event.event_key,)
            ).fetchone()
            if existing is not None:
                return int(existing["id"]), False

            now = received_at or _now()
            image_path = self._save_event_image(event)
            received_body_path = self._save_request_body(
                event.event_key,
                received_content_type,
                received_body,
                event.event_time,
            )
            partner_payload = _payload_with_image_reference(partner_payload or {}, image_path)
            cursor = conn.execute(
                """
                INSERT INTO events (
                    event_key, received_at, updated_at, status, auto_send,
                    skip_reason, event_time, direction, passing_type, plate_no,
                    gate_name, lane_name, raw_json, received_content_type,
                    received_body_path, partner_payload_json,
                    image_name, image_content_type, image_path, image_data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_key,
                    now,
                    now,
                    status,
                    int(auto_send),
                    skip_reason,
                    event.event_time,
                    event.direction,
                    event.passing_type,
                    event.plate_no,
                    event.gate_name,
                    event.lane_name,
                    json.dumps(event.raw, ensure_ascii=False),
                    received_content_type,
                    received_body_path,
                    json.dumps(partner_payload, ensure_ascii=False),
                    event.image.name if event.image is not None else "",
                    event.image.content_type if event.image is not None else "",
                    image_path,
                    None,
                ),
            )
            return int(cursor.lastrowid), True

    def add_parse_error(
        self, event_key: str, error: str, content_type: str, body: bytes | None = None
    ) -> int:
        """保存解析失败的原始请求摘要，方便后续排查。"""
        with self._lock, self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM events WHERE event_key = ?", (event_key,)
            ).fetchone()
            if existing is not None:
                return int(existing["id"])

            now = _now()
            received_body_path = self._save_request_body(event_key, content_type, body, now)
            cursor = conn.execute(
                """
                INSERT INTO events (
                    event_key, received_at, updated_at, status, skip_reason,
                    raw_json, received_content_type, received_body_path, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_key,
                    now,
                    now,
                    "parse_error",
                    error,
                    json.dumps({"content_type": content_type}, ensure_ascii=False),
                    content_type,
                    received_body_path,
                    error,
                ),
            )
            return int(cursor.lastrowid)

    def claim_ready_event(self, event_id: int) -> dict[str, Any] | None:
        """原子地把待发送或到期待重试记录抢占为 sending，并返回最新记录。"""
        now = _now()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE events
                SET status = 'sending',
                    updated_at = ?,
                    first_attempt_at = CASE
                        WHEN first_attempt_at = '' THEN ?
                        ELSE first_attempt_at
                    END,
                    last_attempt_at = ?,
                    next_retry_at = '',
                    dead_lettered_at = '',
                    last_error = ''
                WHERE id = ?
                  AND partner_payload_json NOT IN ('', '{}')
                  AND (
                    status = 'pending'
                    OR (status = 'failed_retryable' AND next_retry_at != '' AND next_retry_at <= ?)
                  )
                """,
                (now, now, now, event_id, now),
            )
            if cursor.rowcount <= 0:
                return None
            row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
            return dict(row) if row is not None else None

    def record_send_request(
        self, event_id: int, request_url: str, payload: dict[str, object]
    ) -> None:
        """记录系统实际准备发出的 API 请求。"""
        payload_json = json.dumps(payload, ensure_ascii=False)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE events
                SET updated_at = ?, partner_payload_json = ?, last_request_url = ?,
                    last_request_payload_json = ?
                WHERE id = ?
                """,
                (_now(), payload_json, request_url, payload_json, event_id),
            )

    def update_send_result(
        self,
        event_id: int,
        result: SendResult,
        next_retry_at: str | None = None,
    ) -> None:
        """根据本次发送结果把记录写成 sent、failed_retryable 或 dead_letter。"""
        now = _now()
        status = "sent" if result.success else ("failed_retryable" if next_retry_at else "dead_letter")
        dead_lettered_at = now if status == "dead_letter" else ""
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE events
                SET status = ?, updated_at = ?, attempts = ?, next_retry_at = ?,
                    dead_lettered_at = ?, status_code = ?, response_text = ?, last_error = ?
                WHERE id = ?
                """,
                (
                    status,
                    now,
                    result.attempts,
                    next_retry_at or "",
                    dead_lettered_at,
                    result.status_code,
                    result.response_text,
                    "" if result.success else result.error,
                    event_id,
                ),
            )

    def list_events(
        self,
        limit: int = 300,
        offset: int = 0,
        filters: dict[str, object] | None = None,
    ) -> list[dict[str, Any]]:
        """按记录 ID 倒序分页读取事件列表，并按 GUI 筛选条件限制结果。"""
        safe_limit = max(1, int(limit))
        safe_offset = max(0, int(offset))
        where_sql, params = _event_filter_where_clause(filters)
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    id, event_key, received_at, updated_at, status, auto_send,
                    skip_reason, event_time, direction, passing_type, plate_no,
                    gate_name, lane_name, raw_json, received_content_type,
                    received_body_path, partner_payload_json,
                    last_request_url, last_request_payload_json,
                    image_name, image_content_type, image_path,
                    CASE
                        WHEN image_path != '' THEN -1
                        WHEN image_data IS NULL THEN 0
                        ELSE length(image_data)
                    END AS image_size,
                    attempts, first_attempt_at, last_attempt_at, next_retry_at,
                    dead_lettered_at, status_code, response_text, last_error
                FROM events
                {where_sql}
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, safe_limit, safe_offset),
            ).fetchall()
            return [self._with_image_file_size(dict(row)) for row in rows]

    def count_events(self, filters: dict[str, object] | None = None) -> int:
        """返回事件总数，供 GUI 按筛选条件计算分页信息。"""
        where_sql, params = _event_filter_where_clause(filters)
        with self._lock, self._connect() as conn:
            row = conn.execute(
                f"SELECT count(*) AS total FROM events {where_sql}",
                params,
            ).fetchone()
            return int(row["total"] if row is not None else 0)

    def list_event_filter_values(
        self,
        filter_key: str,
        filters: dict[str, object] | None = None,
        limit: int = EVENT_FILTER_VALUE_LIMIT,
    ) -> list[str]:
        """读取指定列表列的下拉筛选候选值，忽略该列自身当前筛选。"""
        safe_limit = max(1, int(limit))
        where_sql, params = _event_filter_where_clause(filters, exclude_key=filter_key)
        with self._lock, self._connect() as conn:
            if filter_key == "event_date":
                value_where_sql = _append_where_condition(where_sql, f"{EVENT_DATE_EXPRESSION} != ''")
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT {EVENT_DATE_EXPRESSION} AS value
                    FROM events
                    {value_where_sql}
                    ORDER BY value DESC
                    LIMIT ?
                    """,
                    (*params, safe_limit),
                ).fetchall()
                return [str(row["value"]) for row in rows]

            if filter_key == "return_info":
                return _list_return_info_filter_values(conn, where_sql, params, safe_limit)

            column = EVENT_FILTER_COLUMNS.get(filter_key)
            if column is None:
                return []

            value_where_sql = _append_where_condition(where_sql, f"{column} != ''")
            rows = conn.execute(
                f"""
                SELECT DISTINCT {column} AS value
                FROM events
                {value_where_sql}
                ORDER BY {column} COLLATE NOCASE ASC
                LIMIT ?
                """,
                (*params, safe_limit),
            ).fetchall()
            return [str(row["value"]) for row in rows]

    def get_event(self, event_id: int) -> dict[str, Any] | None:
        """按数据库 ID 读取单条事件记录。"""
        with self._lock, self._connect() as conn:
            row = conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
            return dict(row) if row is not None else None

    def list_ready_event_ids(self, limit: int = 300) -> list[int]:
        """返回当前可发送记录 ID，包括 pending 和到期待重试记录。"""
        now = _now()
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id
                FROM events
                WHERE partner_payload_json NOT IN ('', '{}')
                  AND (
                    status = 'pending'
                    OR (status = 'failed_retryable' AND next_retry_at != '' AND next_retry_at <= ?)
                  )
                ORDER BY
                    CASE
                        WHEN status = 'pending' THEN received_at
                        ELSE next_retry_at
                    END ASC,
                    id ASC
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()
            return [int(row["id"]) for row in rows]

    def recover_stale_sending(self, stale_seconds: float) -> int:
        """把长时间停留在 sending 的记录改为 failed_retryable 并立即允许重试。"""
        cutoff = _time_seconds_ago(stale_seconds)
        now = _now()
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE events
                SET status = 'failed_retryable', updated_at = ?, next_retry_at = ?, dead_lettered_at = ''
                WHERE status = 'sending' AND updated_at < ?
                """,
                (now, now, cutoff),
            )
            return int(cursor.rowcount)

    def set_manual_retry(self, event_id: int) -> bool:
        """把有 payload 的记录重置为 pending，并清空旧的重试和返回元数据。"""
        with self._lock, self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE events
                SET status = 'pending',
                    updated_at = ?,
                    attempts = 0,
                    first_attempt_at = '',
                    last_attempt_at = '',
                    next_retry_at = '',
                    dead_lettered_at = '',
                    status_code = NULL,
                    response_text = '',
                    last_error = ''
                WHERE id = ? AND partner_payload_json NOT IN ('', '{}')
                """,
                (_now(), event_id),
            )
            return cursor.rowcount > 0

    def mark_dead_letter(self, event_id: int, error: str) -> None:
        """把内部无法继续处理的记录写成 dead_letter，避免卡在 sending。"""
        now = _now()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE events
                SET status = 'dead_letter',
                    updated_at = ?,
                    next_retry_at = '',
                    dead_lettered_at = ?,
                    last_error = ?
                WHERE id = ?
                """,
                (now, now, error, event_id),
            )

    def probe_database_health(self) -> bool:
        """执行轻量 SQLite 读写探针，供 /healthz 判断数据库是否可用。"""
        if not self._lock.acquire(timeout=SQLITE_HEALTH_TIMEOUT_SECONDS):
            LOGGER.warning("database health probe timed out waiting for store lock")
            return False
        try:
            conn = sqlite3.connect(
                self.db_path,
                timeout=SQLITE_HEALTH_TIMEOUT_SECONDS,
                factory=_ClosingConnection,
            )
            try:
                conn.execute(
                    f"PRAGMA busy_timeout = {int(SQLITE_HEALTH_TIMEOUT_SECONDS * 1000)}"
                )
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute("SELECT 1")
                finally:
                    conn.execute("ROLLBACK")
            finally:
                conn.close()
        except sqlite3.Error:
            LOGGER.exception("database health probe failed")
            return False
        finally:
            self._lock.release()
        return True

    def get_status_snapshot(self) -> dict[str, object]:
        """汇总 /status 所需的失败堆积、最近成功时间和数据库大小。"""
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM events
                GROUP BY status
                """
            ).fetchall()
            counts = {str(row["status"]): int(row["count"]) for row in rows}
            last_success_row = conn.execute(
                """
                SELECT updated_at
                FROM events
                WHERE status = 'sent'
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
                """
            ).fetchone()

        db_sizes = self._database_size_bytes()
        failed_retryable_count = int(counts.get("failed_retryable", 0))
        dead_letter_count = int(counts.get("dead_letter", 0))
        return {
            "last_success_sent_at": (
                str(last_success_row["updated_at"]) if last_success_row is not None else ""
            ),
            "failed_retryable_count": failed_retryable_count,
            "dead_letter_count": dead_letter_count,
            "failure_backlog_count": failed_retryable_count + dead_letter_count,
            **db_sizes,
        }

    def prune_old_data(
        self,
        event_retention_days: int,
        artifact_retention_days: int,
    ) -> dict[str, int]:
        """按保留期清理过期事件、附件和孤儿文件。"""
        event_cutoff = _time_days_ago(event_retention_days)
        artifact_cutoff = _time_days_ago(artifact_retention_days)

        with self._lock, self._connect() as conn:
            expired_rows = conn.execute(
                """
                SELECT id, image_path, received_body_path
                FROM events
                WHERE received_at < ?
                """,
                (event_cutoff,),
            ).fetchall()
            expired_event_ids = [int(row["id"]) for row in expired_rows]
            event_files = {
                str(row["image_path"] or "") for row in expired_rows if str(row["image_path"] or "")
            }
            request_files = {
                str(row["received_body_path"] or "")
                for row in expired_rows
                if str(row["received_body_path"] or "")
            }
            if expired_event_ids:
                conn.executemany(
                    "DELETE FROM events WHERE id = ?",
                    [(event_id,) for event_id in expired_event_ids],
                )

            artifact_rows = conn.execute(
                """
                SELECT id, image_path, received_body_path, partner_payload_json
                FROM events
                WHERE received_at < ?
                  AND (image_path != '' OR received_body_path != '')
                """,
                (artifact_cutoff,),
            ).fetchall()
            artifact_event_ids: list[int] = []
            artifact_files: set[str] = set()
            artifact_requests: set[str] = set()
            for row in artifact_rows:
                event_id = int(row["id"])
                artifact_event_ids.append(event_id)
                image_path = str(row["image_path"] or "")
                request_path = str(row["received_body_path"] or "")
                if image_path:
                    artifact_files.add(image_path)
                if request_path:
                    artifact_requests.add(request_path)
                conn.execute(
                    """
                    UPDATE events
                    SET updated_at = ?, image_name = '', image_content_type = '', image_path = '',
                        image_data = NULL, received_body_path = '', partner_payload_json = ?
                    WHERE id = ?
                    """,
                    (_now(), _remove_payload_image_reference(row["partner_payload_json"]), event_id),
                )

            referenced_rows = conn.execute(
                "SELECT image_path, received_body_path FROM events"
            ).fetchall()
            referenced_images = {
                str(row["image_path"] or "") for row in referenced_rows if str(row["image_path"] or "")
            }
            referenced_requests = {
                str(row["received_body_path"] or "")
                for row in referenced_rows
                if str(row["received_body_path"] or "")
            }

        deleted_images = _delete_files(event_files | artifact_files)
        deleted_requests = _delete_files(request_files | artifact_requests)
        deleted_orphan_images = self._delete_orphan_files(
            self.image_dir,
            referenced_images,
            artifact_cutoff,
        )
        deleted_orphan_requests = self._delete_orphan_files(
            self.request_dir,
            referenced_requests,
            artifact_cutoff,
        )
        return {
            "expired_events": len(expired_event_ids),
            "expired_artifact_rows": len(artifact_event_ids),
            "deleted_images": deleted_images + deleted_orphan_images,
            "deleted_requests": deleted_requests + deleted_orphan_requests,
        }

    def resolve_public_image_path(self, image_name: str) -> Path | None:
        """按文件名解析对外可访问的图片文件，拒绝目录穿越。"""
        text = str(image_name or "").strip()
        if not text:
            return None
        if Path(text).name != text or "/" in text or "\\" in text:
            return None
        candidates = [self.image_dir / text]
        if self.image_dir.exists():
            for child in self.image_dir.iterdir():
                if child.is_dir():
                    candidates.append(child / text)
        for candidate in candidates:
            try:
                resolved = candidate.resolve(strict=False)
                image_root = self.image_dir.resolve(strict=False)
            except OSError:
                continue
            try:
                resolved.relative_to(image_root)
            except ValueError:
                continue
            if candidate.exists() and candidate.is_file():
                return candidate
        return None

    def _update_status(self, event_id: int, status: str) -> None:
        """更新事件状态和更新时间。"""
        with self._lock, self._connect() as conn:
            conn.execute(
                "UPDATE events SET status = ?, updated_at = ? WHERE id = ?",
                (status, _now(), event_id),
            )

    def _connect(self) -> sqlite3.Connection:
        """创建 SQLite 连接，并启用适合长期运行的并发配置。"""
        conn = sqlite3.connect(
            self.db_path,
            timeout=SQLITE_BUSY_TIMEOUT_MS / 1000,
            factory=_ClosingConnection,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    def _ensure_image_columns(self, conn: sqlite3.Connection) -> None:
        """为旧数据库补齐过车图片字段。"""
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(events)").fetchall()
        }
        if "image_name" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN image_name TEXT NOT NULL DEFAULT ''")
        if "received_content_type" not in columns:
            conn.execute(
                "ALTER TABLE events ADD COLUMN received_content_type TEXT NOT NULL DEFAULT ''"
            )
        if "received_body_path" not in columns:
            conn.execute(
                "ALTER TABLE events ADD COLUMN received_body_path TEXT NOT NULL DEFAULT ''"
            )
        if "image_content_type" not in columns:
            conn.execute(
                "ALTER TABLE events ADD COLUMN image_content_type TEXT NOT NULL DEFAULT ''"
            )
        if "image_path" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN image_path TEXT NOT NULL DEFAULT ''")
        if "image_data" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN image_data BLOB")
        if "first_attempt_at" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN first_attempt_at TEXT NOT NULL DEFAULT ''")
        if "last_attempt_at" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN last_attempt_at TEXT NOT NULL DEFAULT ''")
        if "next_retry_at" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN next_retry_at TEXT NOT NULL DEFAULT ''")
        if "dead_lettered_at" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN dead_lettered_at TEXT NOT NULL DEFAULT ''")
        if "status_code" not in columns:
            conn.execute("ALTER TABLE events ADD COLUMN status_code INTEGER")
        if "last_request_url" not in columns:
            conn.execute(
                "ALTER TABLE events ADD COLUMN last_request_url TEXT NOT NULL DEFAULT ''"
            )
        if "last_request_payload_json" not in columns:
            conn.execute(
                """
                ALTER TABLE events
                ADD COLUMN last_request_payload_json TEXT NOT NULL DEFAULT ''
                """
            )

    def _ensure_indexes(self, conn: sqlite3.Connection) -> None:
        """在字段补齐后创建查询性能依赖的索引。"""
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_status_id ON events(status, id)")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_status_next_retry_id
            ON events(status, next_retry_at, id)
            """
        )

    def _migrate_legacy_failed_statuses(self, conn: sqlite3.Connection) -> None:
        """把旧版本遗留的 failed 状态统一升级为 dead_letter。"""
        now = _now()
        conn.execute(
            """
            UPDATE events
            SET status = 'dead_letter',
                updated_at = ?,
                next_retry_at = '',
                dead_lettered_at = CASE
                    WHEN dead_lettered_at = '' THEN ?
                    ELSE dead_lettered_at
                END
            WHERE status = 'failed'
            """,
            (now, now),
        )

    def _save_event_image(self, event: HikEvent) -> str:
        """把过车图片写入独立文件，并返回数据库中保存的路径文本。"""
        if event.image is None or not event.image.data:
            return ""
        target_dir = self._event_image_dir(event.event_time)
        target_dir.mkdir(parents=True, exist_ok=True)
        image_path = _unique_path(
            target_dir / f"{_event_image_filename(event)}{_image_suffix(event)}"
        )
        image_path.write_bytes(event.image.data)
        return image_path.as_posix()

    def _save_request_body(
        self, event_key: str, content_type: str, body: bytes | None, time_text: str
    ) -> str:
        """把接收到的原始 HTTP body 写入独立文件，并返回文件路径。"""
        if not body:
            return ""
        target_dir = self._request_artifact_dir(time_text)
        target_dir.mkdir(parents=True, exist_ok=True)
        safe_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", event_key).strip("._") or "event"
        request_path = target_dir / f"{_timestamp_for_filename()}_{safe_key[:80]}.bin"
        request_path.write_bytes(_prepare_request_body_for_storage(content_type, body))
        return request_path.as_posix()

    def _with_image_file_size(self, row: dict[str, Any]) -> dict[str, Any]:
        """列表查询时补充独立图片文件大小。"""
        image_path = str(row.get("image_path") or "")
        if not image_path:
            return row

        try:
            row["image_size"] = Path(image_path).stat().st_size
        except OSError:
            row["image_size"] = 0
        return row

    def _migrate_existing_images(self, conn: sqlite3.Connection) -> None:
        """把旧数据库中仍存于 BLOB 的图片补写为独立文件。"""
        rows = conn.execute(
            """
            SELECT id, event_key, image_name, image_content_type, image_data, event_time
            FROM events
            WHERE image_path = '' AND image_data IS NOT NULL
            """
        ).fetchall()
        if not rows:
            return
        for row in rows:
            target_dir = self._event_image_dir(str(row["event_time"] or ""))
            target_dir.mkdir(parents=True, exist_ok=True)
            image_path = target_dir / (
                f"legacy_{row['id']}_{_ascii_filename_part(row['event_key'])[:80]}"
                f"{_image_suffix_from_parts(row['image_content_type'], row['image_name'])}"
            )
            image_path.write_bytes(row["image_data"])
            conn.execute(
                "UPDATE events SET image_path = ? WHERE id = ?",
                (image_path.as_posix(), row["id"]),
            )

    def _rename_existing_image_files(self, conn: sqlite3.Connection) -> None:
        """把旧图片文件名重命名为车牌、方向和时间戳更明确的格式。"""
        rows = conn.execute(
            """
            SELECT id, image_path, image_content_type, image_name, plate_no, direction,
                   event_time, event_key
            FROM events
            WHERE image_path != ''
            """
        ).fetchall()
        for row in rows:
            current_path = Path(row["image_path"])
            if not current_path.exists():
                continue
            target_dir = self._event_image_dir(str(row["event_time"] or ""))
            target_dir.mkdir(parents=True, exist_ok=True)
            target_name = (
                f"{_event_image_filename_from_parts(row['plate_no'], row['direction'], row['event_time'])}"
                f"{_image_suffix_from_parts(row['image_content_type'], row['image_name'])}"
            )
            target_path = current_path
            desired_path = target_dir / target_name
            if current_path != desired_path:
                target_path = _unique_path(desired_path)
                try:
                    current_path.rename(target_path)
                except OSError:
                    target_path = current_path
            conn.execute(
                "UPDATE events SET image_path = ? WHERE id = ?",
                (target_path.as_posix(), row["id"]),
            )
            self._update_payload_image_reference(conn, row["id"], target_path.as_posix())

    def _update_payload_image_reference(
        self, conn: sqlite3.Connection, event_id: int, image_path: str
    ) -> None:
        """把本地图片引用写入已有 payload 的 img 字段。"""
        row = conn.execute(
            "SELECT partner_payload_json FROM events WHERE id = ?",
            (event_id,),
        ).fetchone()
        if row is None:
            return

        try:
            payload = json.loads(row["partner_payload_json"] or "{}")
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict) or not payload:
            return

        updated_payload = _payload_with_image_reference(payload, image_path)
        if updated_payload == payload:
            return
        conn.execute(
            "UPDATE events SET partner_payload_json = ? WHERE id = ?",
            (json.dumps(updated_payload, ensure_ascii=False), event_id),
        )

    def _delete_orphan_files(
        self,
        directory: Path,
        referenced_paths: set[str],
        cutoff_text: str,
    ) -> int:
        """删除目录中早于保留期且未被数据库引用的文件。"""
        if not directory.exists():
            return 0

        cutoff = datetime.fromisoformat(cutoff_text)
        deleted = 0
        for file_path in directory.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.as_posix() in referenced_paths:
                continue
            try:
                modified_at = datetime.fromtimestamp(file_path.stat().st_mtime)
            except OSError:
                continue
            if modified_at >= cutoff:
                continue
            try:
                file_path.unlink()
                deleted += 1
            except OSError:
                LOGGER.warning("failed to delete orphan file: %s", file_path)
        for child in sorted(directory.rglob("*"), key=lambda path: len(path.parts), reverse=True):
            if child == directory or not child.is_dir():
                continue
            try:
                child.rmdir()
            except OSError:
                continue
        return deleted

    def _event_image_dir(self, event_time: str) -> Path:
        """根据过车时间返回图片应落入的日期子目录。"""
        return self.image_dir / _event_date_for_directory(event_time)

    def _request_artifact_dir(self, time_text: str) -> Path:
        """根据接收时间返回原始请求文件应落入的日期子目录。"""
        return self.request_dir / _event_date_for_directory(time_text)

    def migrate_raw_request_files_by_date(self) -> dict[str, Any]:
        """把 raw_requests 根目录下的历史文件迁移到按日期划分的子目录。"""
        summary: dict[str, Any] = {
            "scanned_files": 0,
            "moved_files": 0,
            "skipped_files": 0,
            "updated_rows": 0,
            "failed_files": 0,
            "failures": [],
            "skips": [],
        }
        if not self.request_dir.exists():
            return summary

        with self._lock, self._connect() as conn:
            root_files = sorted(
                file_path for file_path in self.request_dir.iterdir() if file_path.is_file()
            )
            for file_path in root_files:
                summary["scanned_files"] += 1
                matched = re.match(r"^(?P<date>\d{8})_", file_path.name)
                if matched is None:
                    summary["skipped_files"] += 1
                    summary["skips"].append(
                        f"{file_path.as_posix()}: filename does not start with YYYYMMDD_"
                    )
                    continue

                target_dir = self.request_dir / matched.group("date")
                target_dir.mkdir(parents=True, exist_ok=True)
                target_path = _unique_path(target_dir / file_path.name)
                try:
                    file_path.rename(target_path)
                except OSError as exc:
                    summary["failed_files"] += 1
                    summary["failures"].append(f"{file_path.as_posix()}: {exc}")
                    continue

                summary["moved_files"] += 1
                summary["updated_rows"] += self._update_received_body_path_references(
                    conn,
                    file_path,
                    target_path,
                )

        return summary

    def _update_received_body_path_references(
        self, conn: sqlite3.Connection, old_path: Path, new_path: Path
    ) -> int:
        """把数据库中指向旧 raw request 文件的路径更新到新路径。"""
        updated_rows = 0
        for old_text, new_text in _path_rewrite_pairs(old_path, new_path):
            cursor = conn.execute(
                """
                UPDATE events
                SET received_body_path = ?, updated_at = ?
                WHERE received_body_path = ?
                """,
                (new_text, _now(), old_text),
            )
            updated_rows += max(0, int(cursor.rowcount))
        return updated_rows

    def _database_size_bytes(self) -> dict[str, int]:
        """统计 SQLite 主库及 WAL/SHM 文件大小。"""
        db_main = _file_size(self.db_path)
        db_wal = _file_size(self.db_path.with_name(f"{self.db_path.name}-wal"))
        db_shm = _file_size(self.db_path.with_name(f"{self.db_path.name}-shm"))
        return {
            "db_main_size_bytes": db_main,
            "db_wal_size_bytes": db_wal,
            "db_shm_size_bytes": db_shm,
            "db_total_size_bytes": db_main + db_wal + db_shm,
        }


def backup_database_and_reset(db_path: Path) -> Path:
    """备份当前 SQLite 数据库文件，并移除原文件以便后续启用空库。"""
    db_path = Path(db_path)
    backup_path = _next_database_backup_path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path.parent.mkdir(parents=True, exist_ok=True)

    # SQLite backup API 会读取 WAL 中尚未 checkpoint 的内容，得到一致快照。
    source = sqlite3.connect(db_path)
    target = sqlite3.connect(backup_path)
    try:
        source.backup(target)
    finally:
        target.close()
        source.close()

    # 备份完成后删除原库及 SQLite 运行辅助文件，EventStore 会重新建表。
    for path in _database_runtime_files(db_path):
        try:
            path.unlink()
        except FileNotFoundError:
            continue
    LOGGER.info("database backed up to %s and reset at %s", backup_path, db_path)
    return backup_path


def load_partner_payload(
    row: dict[str, Any],
    config: AppConfig | None = None,
) -> dict[str, object]:
    """从数据库记录中读取大园区请求 payload，并按当前配置补齐图片引用。"""
    text = row.get("partner_payload_json") or "{}"
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("partner_payload_json is not an object")
    return _payload_with_image_reference(value, str(row.get("image_path") or ""), config)


def _now() -> str:
    """返回秒级 ISO8601 本地时间字符串。"""
    return datetime.now().isoformat(timespec="seconds")


def _time_seconds_ago(seconds: float) -> str:
    """返回若干秒前的本地时间字符串。"""
    return (datetime.now() - timedelta(seconds=seconds)).isoformat(timespec="seconds")


def _time_days_ago(days: int) -> str:
    """返回若干天前的本地时间字符串。"""
    return (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")


def _event_filter_where_clause(
    filters: dict[str, object] | None,
    exclude_key: str | None = None,
) -> tuple[str, tuple[object, ...]]:
    """把 GUI 列表筛选状态转换成 SQLite WHERE 子句和参数。"""
    clauses: list[str] = []
    params: list[object] = []
    for key, raw_value in (filters or {}).items():
        if key == exclude_key:
            continue
        value = str(raw_value or "").strip()
        if not value:
            continue

        if key == "event_date":
            clauses.append(f"{EVENT_DATE_EXPRESSION} = ?")
            params.append(value)
            continue

        if key == "plate_no":
            clauses.append("plate_no LIKE ? ESCAPE '\\' COLLATE NOCASE")
            params.append(f"%{_escape_like(value)}%")
            continue

        if key == "return_info":
            return_clause, return_params = _return_info_filter_clause(value)
            if return_clause:
                clauses.append(return_clause)
                params.extend(return_params)
            continue

        column = EVENT_FILTER_COLUMNS.get(key)
        if column is not None:
            clauses.append(f"{column} = ?")
            params.append(value)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return where_sql, tuple(params)


def _append_where_condition(where_sql: str, condition: str) -> str:
    """给可能为空的 WHERE 子句追加一个额外条件。"""
    if where_sql:
        return f"{where_sql} AND {condition}"
    return f"WHERE {condition}"


def _escape_like(value: str) -> str:
    """转义用户输入中的 LIKE 通配符，保证车牌筛选按字面量匹配。"""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _return_info_filter_clause(value: str) -> tuple[str, tuple[object, ...]]:
    """把返回信息下拉值转换成可执行的 SQL 条件。"""
    if value == "empty:":
        return "(status_code IS NULL AND response_text = '' AND last_error = '')", ()
    if value.startswith("status_code:"):
        return "status_code = ?", (value.removeprefix("status_code:"),)
    if value.startswith("response_text:"):
        return "response_text = ?", (value.removeprefix("response_text:"),)
    if value.startswith("last_error:"):
        return "last_error = ?", (value.removeprefix("last_error:"),)

    like_value = f"%{_escape_like(value)}%"
    return (
        "(CAST(status_code AS TEXT) LIKE ? ESCAPE '\\' "
        "OR response_text LIKE ? ESCAPE '\\' COLLATE NOCASE "
        "OR last_error LIKE ? ESCAPE '\\' COLLATE NOCASE)",
        (like_value, like_value, like_value),
    )


def _list_return_info_filter_values(
    conn: sqlite3.Connection,
    where_sql: str,
    params: tuple[object, ...],
    limit: int,
) -> list[str]:
    """读取返回信息列的候选筛选值，包括 HTTP 状态、返回原文和错误原文。"""
    values: list[str] = []
    empty_where_sql = _append_where_condition(
        where_sql,
        "status_code IS NULL AND response_text = '' AND last_error = ''",
    )
    empty_row = conn.execute(
        f"SELECT 1 FROM events {empty_where_sql} LIMIT 1",
        params,
    ).fetchone()
    if empty_row is not None:
        values.append("empty:")

    status_where_sql = _append_where_condition(where_sql, "status_code IS NOT NULL")
    status_rows = conn.execute(
        f"""
        SELECT DISTINCT status_code AS value
        FROM events
        {status_where_sql}
        ORDER BY status_code ASC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()
    values.extend(f"status_code:{row['value']}" for row in status_rows)

    remaining = max(0, limit - len(values))
    if remaining > 0:
        values.extend(
            _distinct_text_filter_values(conn, where_sql, params, "response_text", "response_text:", remaining)
        )

    remaining = max(0, limit - len(values))
    if remaining > 0:
        values.extend(
            _distinct_text_filter_values(conn, where_sql, params, "last_error", "last_error:", remaining)
        )
    return values[:limit]


def _distinct_text_filter_values(
    conn: sqlite3.Connection,
    where_sql: str,
    params: tuple[object, ...],
    column: str,
    prefix: str,
    limit: int,
) -> list[str]:
    """读取指定文本列的非空去重值，并带上筛选类型前缀。"""
    text_where_sql = _append_where_condition(where_sql, f"{column} != ''")
    rows = conn.execute(
        f"""
        SELECT DISTINCT {column} AS value
        FROM events
        {text_where_sql}
        ORDER BY {column} COLLATE NOCASE ASC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()
    return [f"{prefix}{row['value']}" for row in rows]


def _payload_with_image_reference(
    payload: dict[str, object],
    image_path: str,
    config: AppConfig | None = None,
) -> dict[str, object]:
    """把本地图片文件名或外链 URL 写入 payload 的 img 字段。"""
    if not payload:
        return payload

    updated_payload = dict(payload)
    image_name = Path(image_path).name if image_path else ""
    if not image_name:
        updated_payload.pop("img", None)
        return updated_payload

    updated_payload["img"] = config.build_external_image_url(image_name) if config else image_name
    return updated_payload


def _remove_payload_image_reference(payload_text: str) -> str:
    """从已保存 payload 中移除 img 字段。"""
    try:
        payload = json.loads(payload_text or "{}")
    except json.JSONDecodeError:
        return payload_text
    if not isinstance(payload, dict):
        return payload_text
    payload.pop("img", None)
    return json.dumps(payload, ensure_ascii=False)


def _delete_files(paths: set[str]) -> int:
    """删除一组文件并返回成功删除的数量。"""
    deleted = 0
    for path_text in paths:
        path = Path(path_text)
        if not path.exists() or not path.is_file():
            continue
        try:
            path.unlink()
            deleted += 1
        except OSError:
            LOGGER.warning("failed to delete file: %s", path)
    return deleted


def _file_size(path: Path) -> int:
    """返回文件大小；文件不存在或无法读取时返回 0。"""
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _database_runtime_files(db_path: Path) -> tuple[Path, Path, Path]:
    """返回 SQLite 主库和 WAL/SHM 辅助文件路径。"""
    return (
        db_path,
        db_path.with_name(f"{db_path.name}-wal"),
        db_path.with_name(f"{db_path.name}-shm"),
    )


def _next_database_backup_path(db_path: Path) -> Path:
    """生成不会覆盖已有文件的数据库备份路径。"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = db_path.suffix or ".sqlite3"
    backup_dir = db_path.parent / "backups"
    return _unique_path(backup_dir / f"{db_path.stem}_backup_{timestamp}{suffix}")


def _path_rewrite_pairs(old_path: Path, new_path: Path) -> list[tuple[str, str]]:
    """生成一组旧路径到新路径的等价文本，用于数据库精确回写。"""
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    old_candidates = _path_text_variants(old_path)
    new_candidates = _path_text_variants(new_path)
    for old_text, new_text in zip(old_candidates, new_candidates, strict=False):
        pair = (old_text, new_text)
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return pairs


def _path_text_variants(path: Path) -> list[str]:
    """返回同一路径可能出现在数据库中的多种文本形式。"""
    variants: list[str] = []
    for candidate in (path.as_posix(), str(path)):
        if candidate not in variants:
            variants.append(candidate)
    try:
        resolved = path.resolve(strict=False)
    except OSError:
        resolved = None
    if resolved is not None:
        for candidate in (resolved.as_posix(), str(resolved)):
            if candidate not in variants:
                variants.append(candidate)
    return variants


def _timestamp_for_filename() -> str:
    """返回适合用于文件名的本地时间戳。"""
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")


def _image_suffix(event: HikEvent) -> str:
    """根据图片 MIME 类型或原始文件名选择落地扩展名。"""
    if event.image is None:
        return ".jpg"

    return _image_suffix_from_parts(event.image.content_type, event.image.name)


def _event_date_for_directory(value: str) -> str:
    """把过车时间转换成图片目录名 YYYYMMDD。"""
    try:
        return datetime.fromisoformat(value).strftime("%Y%m%d")
    except ValueError:
        fallback = _ascii_filename_part(value)[:32]
        return fallback or "unknown-date"


def _prepare_request_body_for_storage(content_type: str, body: bytes) -> bytes:
    """生成适合落盘保存的请求原文，避免把图片二进制直接写入 raw_requests。"""
    if "multipart/form-data" not in content_type.lower():
        return body
    try:
        return _redact_multipart_body(content_type, body)
    except Exception as exc:
        return (
            "multipart body was not stored verbatim because binary image data is omitted.\n"
            f"content_type: {content_type}\n"
            f"error: {exc}\n"
        ).encode("utf-8")


def _redact_multipart_body(content_type: str, body: bytes) -> bytes:
    """把 multipart 请求中的图片 part 替换为文本提示，其余 part 原样保留。"""
    message_bytes = (
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("ascii")
        + body
    )
    message = BytesParser(policy=policy.default).parsebytes(message_bytes)
    boundary = message.get_boundary()
    if not boundary:
        raise ValueError("multipart boundary is missing")

    boundary_bytes = boundary.encode("utf-8")
    chunks: list[bytes] = []
    for part in message.iter_parts():
        chunks.append(b"--" + boundary_bytes)
        for key, value in part.items():
            chunks.append(f"{key}: {value}".encode("utf-8"))
        chunks.append(b"")
        payload = part.get_payload(decode=True) or b""
        if _is_image_part_for_storage(part.get_filename(), part.get_content_type(), payload):
            placeholder = (
                "[image binary omitted]"
                f" filename={part.get_filename() or '-'}"
                f" content_type={part.get_content_type()}"
                f" size={len(payload)} bytes"
            )
            chunks.append(placeholder.encode("utf-8"))
        else:
            chunks.append(payload)
    chunks.append(b"--" + boundary_bytes + b"--")
    chunks.append(b"")
    return b"\r\n".join(chunks)


def _is_image_part_for_storage(name: str | None, content_type: str, payload: bytes) -> bool:
    """判断 multipart part 是否为图片，从而在 raw_requests 中做二进制脱敏。"""
    if not payload:
        return False
    normalized_type = (content_type or "").lower()
    normalized_name = (name or "").strip().lower()
    if normalized_type.startswith("image/"):
        return True
    return normalized_name.endswith((".jpg", ".jpeg", ".png"))


def _event_image_filename(event: HikEvent) -> str:
    """生成包含车牌、方向和时间戳的过车图片文件名主体。"""
    return _event_image_filename_from_parts(event.plate_no, event.direction, event.event_time)


def _event_image_filename_from_parts(plate_no: str, direction: str, event_time: str) -> str:
    """用数据库字段生成图片文件名主体。"""
    plate = _ascii_filename_part(plate_no) or "unknown_plate"
    direction_text = _direction_for_filename(direction)
    timestamp = _event_time_for_filename(event_time)
    return f"{plate}_{direction_text}_{timestamp}"


def _direction_for_filename(direction: str) -> str:
    """把海康方向值转换成适合文件名的 ASCII 文本。"""
    if direction == "enter":
        return "enter"
    if direction == "exit":
        return "exit"
    return _ascii_filename_part(direction) or "unknown_direction"


def _event_time_for_filename(value: str) -> str:
    """把过车时间转换为文件名里的时间戳。"""
    try:
        return datetime.fromisoformat(value).strftime("%Y%m%d_%H%M%S")
    except ValueError:
        return _ascii_filename_part(value)[:40] or _timestamp_for_filename()


def _safe_filename_part(value: object) -> str:
    """清理 Windows 文件名非法字符，并保留车牌中的中文。"""
    text = str(value or "").strip()
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", text)
    text = re.sub(r"\s+", "_", text)
    return text.strip(" ._")


def _ascii_filename_part(value: object) -> str:
    """把任意文本转换成稳定的 ASCII 文件名片段，避免 URL 中出现中文转义。"""
    text = _safe_filename_part(value)
    if not text:
        return ""

    pieces: list[str] = []
    for char in text:
        if re.fullmatch(r"[A-Za-z0-9._-]", char):
            pieces.append(char)
        elif char in _FILENAME_TOKEN_MAP:
            pieces.append(_FILENAME_TOKEN_MAP[char])
        else:
            pieces.append(f"u{ord(char):x}")
    normalized = "".join(pieces)
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("._-")


def _unique_path(path: Path) -> Path:
    """如果目标文件已存在，自动追加序号避免覆盖。"""
    if not path.exists():
        return path

    for index in range(2, 10_000):
        candidate = path.with_name(f"{path.stem}_{index}{path.suffix}")
        if not candidate.exists():
            return candidate
    return path.with_name(f"{path.stem}_{_timestamp_for_filename()}{path.suffix}")


def _image_suffix_from_parts(content_type: str, image_name: str) -> str:
    """根据图片 MIME 类型和文件名推断图片扩展名。"""
    content_type = content_type.lower()
    if "png" in content_type:
        return ".png"
    if "jpeg" in content_type or "jpg" in content_type:
        return ".jpg"

    suffix = Path(image_name).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png"}:
        return suffix
    return ".jpg"
