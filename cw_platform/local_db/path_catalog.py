# cw_platform/local_db/path_catalog.py
# CrossWatch - Persistent scoped media path catalogues
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from typing import Any

from .db import crosswatch_db_path, get_conn


_generation = 0


def generation() -> int:
    return _generation


def usage(base_path: Any = None) -> dict[str, int]:
    if not crosswatch_db_path(base_path).exists():
        return {"count": 0, "bytes": 0}
    conn = get_conn(base_path)
    if conn is None:
        return {"count": 0, "bytes": 0}
    row = conn.execute("SELECT COUNT(*), COALESCE(SUM(length(CAST(items_json AS BLOB))), 0) FROM media_path_catalogues").fetchone()
    return {"count": int(row[0]), "bytes": int(row[1])}


def clear(base_path: Any = None) -> int:
    global _generation
    if not crosswatch_db_path(base_path).exists():
        _generation += 1
        return 0
    conn = get_conn(base_path)
    if conn is None:
        raise RuntimeError("path_catalog_database_unavailable")
    with conn:
        removed = conn.execute("DELETE FROM media_path_catalogues").rowcount
    _generation += 1
    return removed


def load(key: str) -> dict[str, Any] | None:
    conn = get_conn()
    if conn is None:
        return None
    row = conn.execute("SELECT revision, refreshed_at, items_json FROM media_path_catalogues WHERE cache_key=?", (key,)).fetchone()
    if row is None:
        return None
    try:
        items = json.loads(row["items_json"])
        if not isinstance(items, dict) or any(not isinstance(item, dict) for item in items.values()):
            return None
        return {"revision": row["revision"], "refreshed_at": float(row["refreshed_at"]), "items": items}
    except (ValueError, TypeError):
        return None


def save(key: str, revision: str | None, refreshed_at: float, items: dict[str, Any]) -> None:
    conn = get_conn()
    if conn is None:
        return
    payload = json.dumps(items, separators=(",", ":"), ensure_ascii=False)
    with conn:
        conn.execute("DELETE FROM media_path_catalogues WHERE refreshed_at < ?", (time.time() - 7 * 86400,))
        conn.execute("INSERT INTO media_path_catalogues(cache_key, revision, refreshed_at, items_json) VALUES(?,?,?,?) "
                     "ON CONFLICT(cache_key) DO UPDATE SET revision=excluded.revision, refreshed_at=excluded.refreshed_at, items_json=excluded.items_json",
                     (key, revision, refreshed_at, payload))
