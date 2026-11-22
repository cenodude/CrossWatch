# services/plex_recovery_store.py
# CrossWatch - Persistent Plex recovery reviews
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from contextlib import closing
import json
import sqlite3
from typing import Any

from cw_platform.config_base import CONFIG_BASE


def path():
    return CONFIG_BASE() / ".cw_state" / "plex_recovery.sqlite3"


def save(data: dict[str, Any], raw: list[dict[str, Any]] | None = None) -> None:
    target = path()
    target.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(target, timeout=15)) as connection, connection:
        connection.execute("CREATE TABLE IF NOT EXISTS reviews (id TEXT PRIMARY KEY, touched REAL NOT NULL, payload TEXT NOT NULL, raw TEXT)")
        connection.execute(
            "INSERT INTO reviews VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
            "touched=excluded.touched, payload=excluded.payload, raw=COALESCE(excluded.raw, reviews.raw)",
            (data["id"], data["touched"], json.dumps(data, ensure_ascii=False),
             json.dumps(raw, ensure_ascii=False) if raw is not None else None),
        )


def load() -> list[dict[str, Any]]:
    target = path()
    if not target.exists():
        return []
    with closing(sqlite3.connect(target, timeout=15)) as connection:
        return [{**json.loads(payload), "touched": touched, "raw": json.loads(raw or "[]")}
                for touched, payload, raw in connection.execute("SELECT touched, payload, raw FROM reviews")]


def touch(key: str, touched: float) -> None:
    target = path()
    if target.exists():
        with closing(sqlite3.connect(target, timeout=15)) as connection, connection:
            connection.execute("UPDATE reviews SET touched=? WHERE id=?", (touched, key))


def delete(key: str) -> None:
    target = path()
    if target.exists():
        with closing(sqlite3.connect(target, timeout=15)) as connection, connection:
            connection.execute("DELETE FROM reviews WHERE id=?", (key,))
