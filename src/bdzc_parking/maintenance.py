"""维护命令入口，提供一次性数据整理工具。"""

from __future__ import annotations

import sys
from pathlib import Path

from bdzc_parking.app import setup_logging
from bdzc_parking.config import AppConfig
from bdzc_parking.storage import EventStore


def main(argv: list[str] | None = None) -> int:
    """解析维护命令行参数，并执行对应的一次性维护动作。"""
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        _print_usage(sys.stderr)
        return 2
    if args[0] != "migrate-raw-requests":
        print(f"Unknown command: {args[0]}", file=sys.stderr)
        _print_usage(sys.stderr)
        return 2
    return migrate_raw_requests_by_date()


def migrate_raw_requests_by_date() -> int:
    """执行 raw_requests 历史文件按日期迁移，并打印汇总结果。"""
    config = AppConfig.load()
    setup_logging(config.log_path)
    store = EventStore(config.db_path)
    summary = store.migrate_raw_request_files_by_date()

    print(f"scanned_files: {summary['scanned_files']}")
    print(f"moved_files: {summary['moved_files']}")
    print(f"skipped_files: {summary['skipped_files']}")
    print(f"updated_rows: {summary['updated_rows']}")
    print(f"failed_files: {summary['failed_files']}")

    for item in summary["skips"]:
        print(f"SKIP {item}")
    for item in summary["failures"]:
        print(f"FAIL {item}")
    return 0 if int(summary["failed_files"]) == 0 else 1


def _print_usage(stream) -> None:
    """输出维护命令的简要用法说明。"""
    print("Usage: python -m bdzc_parking.maintenance migrate-raw-requests", file=stream)


if __name__ == "__main__":
    raise SystemExit(main())
