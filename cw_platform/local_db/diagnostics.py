# cw_platform/local_db/diagnostics.py
# CrossWatch - Local database diagnostics
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .db import crosswatch_db_path, get_conn
from .schema import SCHEMA_VERSION

_ORPHAN_QUERIES = {
    "baseline_items_missing_feature": (
        "SELECT COUNT(*) FROM baseline_items b "
        "LEFT JOIN provider_feature_state p ON p.id=b.provider_state_id WHERE p.id IS NULL"
    ),
    "manual_policy_blocks_missing_feature": (
        "SELECT COUNT(*) FROM manual_policy_blocks b "
        "LEFT JOIN manual_policy_features f ON f.id=b.feature_id WHERE f.id IS NULL"
    ),
    "manual_policy_add_items_missing_feature": (
        "SELECT COUNT(*) FROM manual_policy_add_items i "
        "LEFT JOIN manual_policy_features f ON f.id=i.feature_id WHERE f.id IS NULL"
    ),
    "currently_watching_ids_missing_stream": (
        "SELECT COUNT(*) FROM currently_watching_stream_ids i "
        "LEFT JOIN currently_watching_streams s ON s.stream_key=i.stream_key WHERE s.stream_key IS NULL"
    ),
    "activity_ids_missing_event": (
        "SELECT COUNT(*) FROM activity_event_ids i "
        "LEFT JOIN activity_events e ON e.event_id=i.event_id WHERE e.event_id IS NULL"
    ),
    "sync_timeline_missing_report": (
        "SELECT COUNT(*) FROM sync_run_timeline t "
        "LEFT JOIN sync_run_reports r ON r.run_id=t.run_id WHERE r.run_id IS NULL"
    ),
    "sync_provider_counts_missing_report": (
        "SELECT COUNT(*) FROM sync_run_provider_counts c "
        "LEFT JOIN sync_run_reports r ON r.run_id=c.run_id WHERE r.run_id IS NULL"
    ),
    "sync_feature_lanes_missing_report": (
        "SELECT COUNT(*) FROM sync_run_feature_lanes l "
        "LEFT JOIN sync_run_reports r ON r.run_id=l.run_id WHERE r.run_id IS NULL"
    ),
    "sync_spotlight_items_missing_report": (
        "SELECT COUNT(*) FROM sync_run_spotlight_items i "
        "LEFT JOIN sync_run_reports r ON r.run_id=i.run_id WHERE r.run_id IS NULL"
    ),
    "statistics_current_providers_missing_item": (
        "SELECT COUNT(*) FROM statistics_current_providers p "
        "LEFT JOIN statistics_current_items i ON i.feature=p.feature AND i.item_key=p.item_key "
        "WHERE i.item_key IS NULL"
    ),
}


def _file_size(path: Path) -> int:
    try:
        return int(path.stat().st_size) if path.exists() else 0
    except Exception:
        return 0


def _one(conn: Any, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = conn.execute(sql, params).fetchone()
    return int(row[0] or 0) if row is not None else 0


def _table_counts(conn: Any) -> dict[str, int]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    out: dict[str, int] = {}
    for row in rows:
        name = str(row["name"] or "")
        if name:
            out[name] = _one(conn, f"SELECT COUNT(*) FROM {name}")
    return out


def _schema_version(conn: Any) -> int | None:
    row = conn.execute(
        "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1"
    ).fetchone()
    return int(row["version"]) if row is not None and row["version"] is not None else None


def _last_sync(conn: Any) -> dict[str, Any]:
    state_row = conn.execute(
        "SELECT value_text,value_int,value_real,value_type,updated_at FROM state_meta WHERE key='last_sync_epoch'"
    ).fetchone()
    summary_row = conn.execute(
        "SELECT started_at,finished_at,updated_at FROM last_sync_summary WHERE id=1"
    ).fetchone()
    return {
        "state_epoch": (
            state_row["value_int"]
            if state_row is not None and state_row["value_type"] in {"int", "bool"}
            else state_row["value_real"]
            if state_row is not None and state_row["value_type"] == "real"
            else state_row["value_text"]
            if state_row is not None
            else None
        ),
        "state_updated_at": state_row["updated_at"] if state_row is not None else None,
        "started_at": summary_row["started_at"] if summary_row is not None else None,
        "finished_at": summary_row["finished_at"] if summary_row is not None else None,
        "summary_updated_at": summary_row["updated_at"] if summary_row is not None else None,
    }


def diagnostics(base_path: str | Path | None = None) -> dict[str, Any]:
    path = crosswatch_db_path(base_path)
    existed = path.exists()
    conn = get_conn(base_path)
    if conn is None:
        return {
            "ok": False,
            "error": "database_unavailable",
            "path": str(path),
            "exists": existed,
            "healthy": False,
        }

    integrity_row = conn.execute("PRAGMA integrity_check").fetchone()
    integrity = str(integrity_row[0] if integrity_row is not None else "unknown")
    foreign_key_errors = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    orphan_counts = {
        key: _one(conn, sql)
        for key, sql in _ORPHAN_QUERIES.items()
    }
    stale_counts = {
        "ttl_expired": _one(conn, "SELECT COUNT(*) FROM ttl_dedupe_entries WHERE expires_at<?", (time.time(),)),
        "currently_watching_inactive": _one(
            conn,
            "SELECT COUNT(*) FROM currently_watching_streams WHERE COALESCE(state,'') NOT IN ('playing','buffering')",
        ),
    }
    table_counts = _table_counts(conn)
    schema_version = _schema_version(conn)
    issues = sum(orphan_counts.values()) + foreign_key_errors
    schema_ok = schema_version == SCHEMA_VERSION
    healthy = integrity == "ok" and issues == 0 and schema_ok
    wal_path = Path(str(path) + "-wal")
    shm_path = Path(str(path) + "-shm")
    return {
        "ok": True,
        "path": str(path),
        "exists": existed or path.exists(),
        "healthy": healthy,
        "integrity": integrity,
        "schema_version": schema_version,
        "expected_schema_version": SCHEMA_VERSION,
        "schema_ok": schema_ok,
        "foreign_key_errors": foreign_key_errors,
        "size_bytes": _file_size(path),
        "wal_size_bytes": _file_size(wal_path),
        "shm_size_bytes": _file_size(shm_path),
        "table_counts": table_counts,
        "orphan_counts": orphan_counts,
        "stale_counts": stale_counts,
        "last_sync": _last_sync(conn),
    }
