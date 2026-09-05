# cw_platform/event_archive/run_scope.py
# CrossWatch - Per-run pair scope and change counters
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections.abc import Iterable, Mapping
from typing import Any

from .db import get_conn

_LOG = logging.getLogger("crosswatch.event_archive")

_COLUMNS = (
    "run_id", "pair_id", "scope_key", "feature", "pair_key",
    "src_provider", "src_instance", "dst_provider", "dst_instance",
    "added", "removed", "updated", "errors",
)

_BACKFILL_DAYS = 400
_BACKFILL_CHUNK = 200


def normalize_endpoint(provider: Any, instance: Any) -> str:
    prov = str(provider or "").strip().upper()
    inst = str(instance or "").strip().lower() or "default"
    return f"{prov}:{inst}" if prov else ""


def scope_key(src_provider: Any, src_instance: Any, dst_provider: Any, dst_instance: Any) -> str:
    ends = [e for e in (normalize_endpoint(src_provider, src_instance), normalize_endpoint(dst_provider, dst_instance)) if e]
    return "|".join(sorted(ends))


def pair_id_from_scope(raw: Any) -> str:
    parts = str(raw or "").split(":")
    return parts[2].strip() if len(parts) >= 3 else ""


def record_run_pairs(
    run_id: str,
    rows: Iterable[Mapping[str, Any]],
    *,
    conn: sqlite3.Connection | None = None,
) -> int:
    prepared: list[tuple[Any, ...]] = []
    for row in rows or ():
        if not isinstance(row, Mapping):
            continue
        prepared.append((
            str(run_id),
            str(row.get("pair_id") or ""),
            str(row.get("scope_key") or ""),
            str(row.get("feature") or ""),
            str(row.get("pair_key") or ""),
            str(row.get("src_provider") or ""),
            str(row.get("src_instance") or ""),
            str(row.get("dst_provider") or ""),
            str(row.get("dst_instance") or ""),
            int(row.get("added") or 0),
            int(row.get("removed") or 0),
            int(row.get("updated") or 0),
            int(row.get("errors") or 0),
        ))
    if not prepared:
        return 0
    c = conn or get_conn()
    if c is None:
        return 0
    sql = f"INSERT OR REPLACE INTO run_pairs ({','.join(_COLUMNS)}) VALUES ({','.join('?' for _ in _COLUMNS)})"
    try:
        with c:
            c.executemany(sql, prepared)
        return len(prepared)
    except Exception as exc:
        _LOG.warning("run scope write failed: %s", exc)
        return 0


def _detail_counts(raw: Any) -> tuple[int, int, int, int]:
    if isinstance(raw, Mapping):
        data: Mapping[str, Any] = raw
    else:
        try:
            parsed = json.loads(raw) if raw else {}
        except Exception:
            return (0, 0, 0, 0)
        data = parsed if isinstance(parsed, Mapping) else {}

    def _n(key: str) -> int:
        try:
            return max(0, int(data.get(key) or 0))
        except Exception:
            return 0

    return (_n("added"), _n("removed"), _n("updated"), _n("errors"))


def _backfill_batch(conn: sqlite3.Connection, run_ids: list[str]) -> int:
    if not run_ids:
        return 0
    placeholders = ",".join("?" for _ in run_ids)
    rows = conn.execute(
        f"SELECT run_id, pair_key, feature, source_provider, source_instance, "
        f"destination_provider, destination_instance, detail FROM events "
        f"WHERE run_id IN ({placeholders}) AND event_type='write_attempted'",
        run_ids,
    ).fetchall()

    agg: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in rows:
        added, removed, updated, errors = _detail_counts(row["detail"])
        if not (added or removed or updated or errors):
            continue
        src = str(row["source_provider"] or "").strip().upper()
        dst = str(row["destination_provider"] or "").strip().upper()
        si = str(row["source_instance"] or "").strip().lower() or "default"
        di = str(row["destination_instance"] or "").strip().lower() or "default"
        feature = str(row["feature"] or "")
        key = (str(row["run_id"]), "", scope_key(src, si, dst, di), feature)
        entry = agg.get(key)
        if entry is None:
            entry = agg[key] = {
                "pair_key": str(row["pair_key"] or ""),
                "src_provider": src, "src_instance": si,
                "dst_provider": dst, "dst_instance": di,
                "added": 0, "removed": 0, "updated": 0, "errors": 0,
            }
        entry["added"] += added
        entry["removed"] += removed
        entry["updated"] += updated
        entry["errors"] += errors

    if not agg:
        return 0
    prepared = [
        (key[0], key[1], key[2], key[3], e["pair_key"],
         e["src_provider"], e["src_instance"], e["dst_provider"], e["dst_instance"],
         e["added"], e["removed"], e["updated"], e["errors"])
        for key, e in agg.items()
    ]
    sql = f"INSERT OR REPLACE INTO run_pairs ({','.join(_COLUMNS)}) VALUES ({','.join('?' for _ in _COLUMNS)})"
    with conn:
        conn.executemany(sql, prepared)
    return len(prepared)


def backfill_run_pairs(conn: sqlite3.Connection | None = None, *, days: int = _BACKFILL_DAYS) -> int:
    c = conn or get_conn()
    if c is None:
        return 0
    since = int(time.time()) - int(days) * 86400
    try:
        pending = [
            str(row[0]) for row in c.execute(
                "SELECT run_id FROM sync_runs WHERE started_at>=? AND run_id IS NOT NULL AND run_id<>'' "
                "AND NOT EXISTS (SELECT 1 FROM run_pairs p WHERE p.run_id=sync_runs.run_id) "
                "ORDER BY started_at DESC",
                (since,),
            ).fetchall()
        ]
    except Exception as exc:
        _LOG.warning("run scope backfill scan failed: %s", exc)
        return 0

    written = 0
    for i in range(0, len(pending), _BACKFILL_CHUNK):
        try:
            written += _backfill_batch(c, pending[i:i + _BACKFILL_CHUNK])
        except Exception as exc:
            _LOG.warning("run scope backfill batch failed: %s", exc)
            break
    if written:
        _LOG.info("run scope backfill wrote %d rows for %d runs", written, len(pending))
    return written
