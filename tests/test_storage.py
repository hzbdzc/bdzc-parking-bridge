"""事件存储模块的回归测试。"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from bdzc_parking.config import AppConfig
from bdzc_parking.models import SendResult
from bdzc_parking.parser import extract_event, parse_hikvision_payload
from bdzc_parking.storage import EventStore, backup_database_and_reset, load_partner_payload
from helpers import HIKVISION_CONTENT_TYPE, insert_request_record, sample_body


def test_store_saves_event_image(tmp_path: Path) -> None:
    """新增事件时应保存过车图片，并在列表中只返回轻量图片信息。"""
    body = sample_body("20260412_063354_226439_body.bin")
    raw = parse_hikvision_payload(HIKVISION_CONTENT_TYPE, body)
    event = extract_event(raw)
    store = EventStore(tmp_path / "events.sqlite3")

    event_id, created = store.add_event(
        event,
        "pending",
        True,
        partner_payload={"car": event.plate_no},
        received_content_type=HIKVISION_CONTENT_TYPE,
        received_body=body,
    )

    listed = store.list_events()
    stored = store.get_event(event_id)

    assert created is True
    assert listed[0]["image_name"] == "detectionPicture.jpg"
    assert listed[0]["image_size"] > 0
    assert "image_data" not in listed[0]
    assert stored is not None
    assert stored["image_data"] is None
    image_path = Path(stored["image_path"])
    assert image_path.read_bytes().startswith(bytes.fromhex("ffd8"))
    assert image_path.parent.name == "20260412"
    assert image_path.name.isascii()
    assert "_enter_" in image_path.name
    assert "20260412_062924" in image_path.name
    payload = json.loads(stored["partner_payload_json"])
    assert payload["img"] == image_path.name
    assert load_partner_payload(stored)["img"] == image_path.name
    external_payload = load_partner_payload(
        stored,
        AppConfig(external_url_base="https://public.example.com/parking-images"),
    )
    assert external_payload["img"].startswith("https://public.example.com/parking-images/")
    assert "%" not in external_payload["img"]
    assert stored["received_content_type"] == HIKVISION_CONTENT_TYPE
    received_body_path = Path(stored["received_body_path"])
    saved_body = received_body_path.read_bytes()
    assert received_body_path.parent.name == "20260412"
    assert b"VehiclePassingInParkingLot" in saved_body
    assert b"[image binary omitted]" in saved_body
    assert bytes.fromhex("ffd8") not in saved_body

    request_payload = {"car": event.plate_no, "img": image_path.name}
    store.record_send_request(event_id, "https://example.test/api", request_payload)
    requested = store.get_event(event_id)
    assert requested is not None
    assert requested["last_request_url"] == "https://example.test/api"
    assert json.loads(requested["last_request_payload_json"]) == request_payload

    store.update_send_result(event_id, SendResult(True, 1, 200, '{"status":200,"msg":"ok"}'))
    sent = store.get_event(event_id)
    assert sent is not None
    assert sent["status"] == "sent"
    assert sent["status_code"] == 200


def test_list_events_supports_pagination_and_total_count(tmp_path: Path) -> None:
    """事件列表应能按 ID 倒序分页读取，并提供总数。"""
    store = EventStore(tmp_path / "events.sqlite3")
    with store._connect() as conn:
        for index in range(1005):
            conn.execute(
                """
                INSERT INTO events (event_key, received_at, updated_at, status, event_time, plate_no)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    f"page-event-{index}",
                    "2026-04-27T14:00:00",
                    "2026-04-27T14:00:00",
                    "sent",
                    "2026-04-27T14:00:00+08:00",
                    f"浙A{index:05d}",
                ),
            )

    first_page = store.list_events(limit=1000, offset=0)
    second_page = store.list_events(limit=1000, offset=1000)

    assert store.count_events() == 1005
    assert len(first_page) == 1000
    assert len(second_page) == 5
    assert first_page[0]["id"] == 1005
    assert first_page[-1]["id"] == 6
    assert second_page[0]["id"] == 5
    assert second_page[-1]["id"] == 1


def test_list_events_supports_gui_filters(tmp_path: Path) -> None:
    """GUI 列表筛选应支持车牌包含匹配、下拉精确匹配和返回信息匹配。"""
    store = EventStore(tmp_path / "events.sqlite3")
    with store._connect() as conn:
        conn.execute(
            """
            INSERT INTO events (
                event_key, received_at, updated_at, status, event_time, direction,
                passing_type, plate_no, lane_name, status_code, response_text
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "filter-1",
                "2026-04-27T14:00:00",
                "2026-04-27T14:00:00",
                "sent",
                "2026-04-27T14:00:00+08:00",
                "enter",
                "plateRecognition",
                "浙A12345",
                "入口一",
                200,
                '{"status":200,"msg":"ok"}',
            ),
        )
        conn.execute(
            """
            INSERT INTO events (
                event_key, received_at, updated_at, status, event_time, direction,
                passing_type, plate_no, lane_name, last_error
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "filter-2",
                "2026-04-28T15:00:00",
                "2026-04-28T15:00:00",
                "dead_letter",
                "2026-04-28T15:00:00+08:00",
                "exit",
                "manual",
                "川Q21C65",
                "出口一",
                "timeout",
            ),
        )

    plate_rows = store.list_events(filters={"plate_no": "A123"})
    date_rows = store.list_events(filters={"event_date": "2026-04-27"})
    direction_rows = store.list_events(filters={"direction": "exit"})
    return_rows = store.list_events(filters={"return_info": "last_error:timeout"})

    assert store.count_events({"plate_no": "A123"}) == 1
    assert plate_rows[0]["plate_no"] == "浙A12345"
    assert [row["plate_no"] for row in date_rows] == ["浙A12345"]
    assert direction_rows[0]["plate_no"] == "川Q21C65"
    assert return_rows[0]["status"] == "dead_letter"
    assert store.list_event_filter_values("event_date") == ["2026-04-28", "2026-04-27"]
    assert "exit" in store.list_event_filter_values("direction", {"plate_no": "Q21"})
    assert "status_code:200" in store.list_event_filter_values("return_info")


def test_reopen_store_migrates_legacy_failed_to_dead_letter(tmp_path: Path) -> None:
    """旧版本遗留的 failed 状态应在重新打开数据库时升级为 dead_letter。"""
    body = sample_body("20260412_063354_226439_body.bin")
    raw = parse_hikvision_payload(HIKVISION_CONTENT_TYPE, body)
    event = extract_event(raw)
    db_path = tmp_path / "events.sqlite3"

    store = EventStore(db_path)
    event_id, _created = store.add_event(event, "pending", True, partner_payload={"car": event.plate_no})
    with store._connect() as conn:
        conn.execute(
            """
            UPDATE events
            SET status = 'failed', updated_at = '2000-01-01T00:00:00'
            WHERE id = ?
            """,
            (event_id,),
        )

    reopened = EventStore(db_path)
    row = reopened.get_event(event_id)
    assert row is not None
    assert row["status"] == "dead_letter"
    assert row["dead_lettered_at"] != ""


def test_backup_database_and_reset_keeps_backup_and_allows_empty_reopen(tmp_path: Path) -> None:
    """备份并重置数据库后，备份保留旧记录，原路径可重新打开为空库。"""
    body = sample_body("20260412_063354_226439_body.bin")
    raw = parse_hikvision_payload(HIKVISION_CONTENT_TYPE, body)
    event = extract_event(raw)
    db_path = tmp_path / "events.sqlite3"

    store = EventStore(db_path)
    store.add_event(event, "pending", True, partner_payload={"car": event.plate_no})

    backup_path = backup_database_and_reset(db_path)

    backup_store = EventStore(backup_path)
    fresh_store = EventStore(db_path)
    assert backup_path.exists()
    assert backup_store.list_events()
    assert fresh_store.list_events() == []


def test_open_legacy_database_without_retry_columns_succeeds(tmp_path: Path) -> None:
    """旧数据库缺少 next_retry_at 等新字段时，初始化应先补列再建索引。"""
    db_path = tmp_path / "legacy.sqlite3"

    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE events (
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
                status_code INTEGER,
                response_text TEXT NOT NULL DEFAULT '',
                last_error TEXT NOT NULL DEFAULT ''
            )
            """
        )

    reopened = EventStore(db_path)
    with reopened._connect() as conn:
        columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(events)").fetchall()
        }
        indexes = {
            row["name"] for row in conn.execute("PRAGMA index_list(events)").fetchall()
        }

    assert {"first_attempt_at", "last_attempt_at", "next_retry_at", "dead_lettered_at"} <= columns
    assert "idx_events_status_next_retry_id" in indexes


def test_migrate_raw_request_files_by_date_moves_root_files_and_updates_database(tmp_path: Path) -> None:
    """历史 raw_requests 根目录文件应迁移到日期子目录，并同步回写数据库路径。"""
    store = EventStore(tmp_path / "events.sqlite3")
    root_file = store.request_dir / "20260412_162551_132336_sample.bin"
    root_file.parent.mkdir(parents=True, exist_ok=True)
    root_file.write_bytes(b"legacy-request")
    event_id = insert_request_record(store, "legacy-root-file", root_file.as_posix())

    summary = store.migrate_raw_request_files_by_date()

    migrated_path = store.request_dir / "20260412" / root_file.name
    row = store.get_event(event_id)
    assert summary["scanned_files"] == 1
    assert summary["moved_files"] == 1
    assert summary["skipped_files"] == 0
    assert summary["updated_rows"] == 1
    assert summary["failed_files"] == 0
    assert not root_file.exists()
    assert migrated_path.exists()
    assert row is not None
    assert row["received_body_path"] == migrated_path.as_posix()

    second = store.migrate_raw_request_files_by_date()
    assert second["scanned_files"] == 0
    assert second["moved_files"] == 0
    assert second["updated_rows"] == 0


def test_migrate_raw_request_files_by_date_skips_invalid_root_filenames(tmp_path: Path) -> None:
    """不符合 YYYYMMDD_ 前缀规则的根目录文件应被跳过并保留原位。"""
    store = EventStore(tmp_path / "events.sqlite3")
    invalid_root_file = store.request_dir / "legacy_request.bin"
    nested_file = store.request_dir / "20260412" / "20260412_162551_nested.bin"
    invalid_root_file.parent.mkdir(parents=True, exist_ok=True)
    nested_file.parent.mkdir(parents=True, exist_ok=True)
    invalid_root_file.write_bytes(b"legacy-request")
    nested_file.write_bytes(b"nested-request")

    summary = store.migrate_raw_request_files_by_date()

    assert summary["scanned_files"] == 1
    assert summary["moved_files"] == 0
    assert summary["skipped_files"] == 1
    assert summary["failed_files"] == 0
    assert invalid_root_file.exists()
    assert nested_file.exists()
    assert summary["skips"]


def test_migrate_raw_request_files_by_date_uses_unique_name_on_conflict(tmp_path: Path) -> None:
    """目标日期目录已存在同名文件时，应生成唯一文件名并回写数据库。"""
    store = EventStore(tmp_path / "events.sqlite3")
    root_file = store.request_dir / "20260412_162551_132336_conflict.bin"
    target_dir = store.request_dir / "20260412"
    target_file = target_dir / root_file.name
    root_file.parent.mkdir(parents=True, exist_ok=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    root_file.write_bytes(b"legacy-request")
    target_file.write_bytes(b"existing-request")
    event_id = insert_request_record(store, "legacy-conflict-file", root_file.as_posix())

    summary = store.migrate_raw_request_files_by_date()

    row = store.get_event(event_id)
    assert summary["moved_files"] == 1
    assert summary["updated_rows"] == 1
    assert row is not None
    assert row["received_body_path"].endswith("_2.bin")
    assert Path(row["received_body_path"]).exists()
    assert target_file.read_bytes() == b"existing-request"
