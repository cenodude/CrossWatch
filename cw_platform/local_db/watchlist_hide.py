# cw_platform/local_db/watchlist_hide.py
# CrossWatch - SQLite-backed watchlist hide storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Iterable
from pathlib import Path

from .db import get_conn


def _now() -> int:
    return int(time.time_ns())


def _keys(keys: Iterable[object]) -> list[str]:
    return sorted({str(key or "").strip() for key in keys if str(key or "").strip()})


def load_hidden(base_path: str | Path) -> set[str]:
    conn = get_conn(base_path)
    if conn is None:
        return set()
    rows = conn.execute("SELECT item_key FROM watchlist_hidden_items ORDER BY item_key").fetchall()
    return {str(row["item_key"]) for row in rows if row["item_key"]}


def save_hidden(base_path: str | Path, keys: Iterable[object]) -> None:
    conn = get_conn(base_path)
    if conn is None:
        return
    ts = _now()
    rows = [(key, ts, ts) for key in _keys(keys)]
    with conn:
        conn.execute("DELETE FROM watchlist_hidden_items")
        if rows:
            conn.executemany(
                "INSERT INTO watchlist_hidden_items(item_key,created_at,updated_at) VALUES(?,?,?)",
                rows,
            )


def fingerprint(base_path: str | Path) -> tuple[int, int] | None:
    conn = get_conn(base_path)
    if conn is None:
        return None
    row = conn.execute("SELECT COUNT(*), COALESCE(MAX(updated_at),0) FROM watchlist_hidden_items").fetchone()
    return (int(row[0] or 0), int(row[1] or 0))


def clear_hidden(base_path: str | Path) -> None:
    conn = get_conn(base_path)
    if conn is None:
        return
    with conn:
        conn.execute("DELETE FROM watchlist_hidden_items")
