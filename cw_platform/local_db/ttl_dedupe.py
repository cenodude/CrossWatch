# cw_platform/local_db/ttl_dedupe.py
# CrossWatch - SQLite-backed TTL dedupe store
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from pathlib import Path

from .db import get_conn


def base_path_from_state_dir(state_dir: str | Path | None) -> Path | None:
    if state_dir is None:
        return None
    path = Path(state_dir)
    return path.parent if path.name == ".cw_state" else path


def once_per_ttl(
    base_path: str | Path | None,
    namespace: str,
    key: str,
    *,
    ttl_seconds: float,
    max_entries: int = 2000,
) -> bool:
    name = str(namespace or "").strip() or "default"
    dedupe_key = str(key or "").strip()
    if not dedupe_key:
        return True
    conn = get_conn(base_path)
    if conn is None:
        return True
    now = time.time()
    ttl = max(0.0, float(ttl_seconds or 0.0))
    cutoff = now - ttl
    try:
        with conn:
            conn.execute("DELETE FROM ttl_dedupe_entries WHERE expires_at<?", (now,))
            row = conn.execute(
                "SELECT seen_at FROM ttl_dedupe_entries WHERE namespace=? AND dedupe_key=?",
                (name, dedupe_key),
            ).fetchone()
            if row is not None and now - float(row["seen_at"] or 0.0) < ttl:
                return False
            conn.execute(
                "INSERT INTO ttl_dedupe_entries(namespace,dedupe_key,seen_at,expires_at,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(namespace,dedupe_key) DO UPDATE SET seen_at=excluded.seen_at,"
                "expires_at=excluded.expires_at,updated_at=excluded.updated_at",
                (name, dedupe_key, now, now + ttl, int(time.time_ns())),
            )
            conn.execute(
                "DELETE FROM ttl_dedupe_entries WHERE namespace=? AND seen_at<?",
                (name, cutoff),
            )
            cap = max(1, int(max_entries or 2000))
            conn.execute(
                "DELETE FROM ttl_dedupe_entries WHERE namespace=? AND dedupe_key NOT IN ("
                "SELECT dedupe_key FROM ttl_dedupe_entries WHERE namespace=? ORDER BY seen_at DESC LIMIT ?"
                ")",
                (name, name, cap),
            )
        return True
    except Exception:
        return True


def clear_namespace(base_path: str | Path | None, namespace: str | None = None) -> int:
    conn = get_conn(base_path)
    if conn is None:
        return 0
    name = str(namespace or "").strip()
    if name:
        row = conn.execute("SELECT COUNT(*) AS c FROM ttl_dedupe_entries WHERE namespace=?", (name,)).fetchone()
        count = int(row["c"] or 0) if row is not None else 0
        with conn:
            conn.execute("DELETE FROM ttl_dedupe_entries WHERE namespace=?", (name,))
        return count
    row = conn.execute("SELECT COUNT(*) AS c FROM ttl_dedupe_entries").fetchone()
    count = int(row["c"] or 0) if row is not None else 0
    with conn:
        conn.execute("DELETE FROM ttl_dedupe_entries")
    return count
