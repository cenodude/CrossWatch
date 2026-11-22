# cw_platform/notification_store.py
# CrossWatch - Notification state shared by every device of a user
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from pathlib import Path
from collections.abc import Generator
from typing import Any

from .config_base import CONFIG_BASE

MAX_KEYS = 2000
MAX_KEY_LENGTH = 512
MAX_SCOPE_LENGTH = 128
MAX_SCOPES = 8
MAX_WAITS = 8
MAX_SUBSCRIPTIONS = 10
WAIT_SECONDS = 30 * 60
ABANDON_SECONDS = 24 * 3600
READY_SECONDS = 14 * 24 * 3600
KINDS = ("dismissed", "seen", "pushed")
REVIEW_KINDS = ("month", "wrapped")

_LOCK = threading.RLock()
_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS marks (id INTEGER PRIMARY KEY, user_id TEXT NOT NULL, scope TEXT NOT NULL, kind TEXT NOT NULL, "
    "key TEXT NOT NULL, marked_at REAL NOT NULL, UNIQUE (user_id, scope, kind, key))",
    "CREATE TABLE IF NOT EXISTS review_waits (user_id TEXT NOT NULL, scope TEXT NOT NULL, kind TEXT NOT NULL, instance TEXT NOT NULL, "
    "period TEXT NOT NULL, label TEXT NOT NULL, created_at REAL NOT NULL, ready_revision TEXT, ready_at REAL, "
    "PRIMARY KEY (user_id, scope, kind, instance, period))",
    "CREATE TABLE IF NOT EXISTS push_subscriptions (endpoint TEXT PRIMARY KEY, user_id TEXT NOT NULL, scope TEXT NOT NULL, "
    "user_profile TEXT NOT NULL, view_as TEXT NOT NULL DEFAULT '', p256dh TEXT NOT NULL, auth TEXT NOT NULL, label TEXT NOT NULL, "
    "created_at REAL NOT NULL, failures INTEGER NOT NULL DEFAULT 0)",
    "CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value TEXT NOT NULL)",
)


def push_scope(endpoint: str) -> str:
    return "push:" + hashlib.sha256(endpoint.encode("utf-8")).hexdigest()[:40]


def store_path() -> Path:
    return CONFIG_BASE() / ".cw_state" / "notifications.sqlite3"


def clean_scope(value: Any) -> str:
    scope = value.strip() if isinstance(value, str) else ""
    return scope if len(scope) <= MAX_SCOPE_LENGTH else ""


def clean_keys(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    keys = dict.fromkeys(v for v in values if isinstance(v, str) and 0 < len(v) <= MAX_KEY_LENGTH)
    return list(keys)[-MAX_KEYS:]


@contextmanager
def _db(path: Path | None = None) -> Generator[sqlite3.Connection, None, None]:
    target = path or store_path()
    with _LOCK:
        target.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(target, timeout=5.0)) as conn:
            for statement in _SCHEMA:
                conn.execute(statement)
            with conn:
                yield conn


@contextmanager
def _existing(path: Path | None = None) -> Generator[sqlite3.Connection | None, None, None]:
    target = path or store_path()
    if not target.exists():
        yield None
        return
    with _db(target) as conn:
        yield conn


def _keys(conn: sqlite3.Connection, user_id: str, scope: str, kind: str) -> list[str]:
    rows = conn.execute("SELECT key FROM marks WHERE user_id=? AND scope=? AND kind=? ORDER BY id", (user_id, scope, kind))
    return [row[0] for row in rows]


def marks(user_id: str, scopes: list[str], kinds: tuple[str, ...] = ("dismissed", "seen"), path: Path | None = None) -> dict[str, dict[str, list[str]]]:
    wanted = list(dict.fromkeys(s for s in map(clean_scope, scopes) if s))[:MAX_SCOPES]
    out = {scope: {kind: [] for kind in kinds} for scope in wanted}
    with _existing(path) as conn:
        if conn is not None:
            for scope in wanted:
                for kind in kinds:
                    out[scope][kind] = _keys(conn, user_id, scope, kind)
    return out


def add_marks(user_id: str, scope: str, kind: str, keys: Any, path: Path | None = None) -> list[str]:
    scope = clean_scope(scope)
    if not scope or kind not in KINDS:
        raise ValueError("invalid scope")
    fresh = clean_keys(keys)
    now = time.time()
    with _db(path) as conn:
        conn.executemany(
            "INSERT OR IGNORE INTO marks (user_id, scope, kind, key, marked_at) VALUES (?, ?, ?, ?, ?)",
            [(user_id, scope, kind, key, now) for key in fresh],
        )
        conn.execute(
            "DELETE FROM marks WHERE id IN (SELECT id FROM marks WHERE user_id=? AND scope=? AND kind=? ORDER BY id DESC LIMIT -1 OFFSET ?)",
            (user_id, scope, kind, MAX_KEYS),
        )
        return _keys(conn, user_id, scope, kind)


def dismiss(user_id: str, scope: str, keys: Any, path: Path | None = None) -> list[str]:
    return add_marks(user_id, scope, "dismissed", keys, path)


def wait_for_review(user_id: str, scope: str, kind: str, instance: str, period: str, label: str, path: Path | None = None) -> None:
    now = time.time()
    with _db(path) as conn:
        conn.execute(
            "INSERT INTO review_waits (user_id, scope, kind, instance, period, label, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (user_id, scope, kind, instance, period) DO UPDATE SET label=excluded.label, created_at=excluded.created_at, "
            "ready_revision=NULL, ready_at=NULL",
            (user_id, scope, kind, instance, period, label[:120], now),
        )
        conn.execute(
            "DELETE FROM review_waits WHERE rowid IN (SELECT rowid FROM review_waits WHERE user_id=? AND scope=? AND kind=? "
            "ORDER BY created_at DESC LIMIT -1 OFFSET ?)",
            (user_id, scope, kind, MAX_WAITS),
        )


def review_viewed(user_id: str, kind: str, instance: str, period: str, path: Path | None = None) -> None:
    with _existing(path) as conn:
        if conn is not None:
            conn.execute("DELETE FROM review_waits WHERE user_id=? AND kind=? AND instance=? AND period=?", (user_id, kind, instance, period))


def review_waits(user_id: str, scope: str, path: Path | None = None, *, now: float | None = None) -> list[dict[str, Any]]:
    current = time.time() if now is None else now
    with _existing(path) as conn:
        if conn is None:
            return []
        conn.execute(
            "DELETE FROM review_waits WHERE (ready_at IS NULL AND created_at < ?) OR (ready_at IS NOT NULL AND ready_at < ?)",
            (current - ABANDON_SECONDS, current - READY_SECONDS),
        )
        rows = conn.execute(
            "SELECT kind, instance, period, label, created_at, ready_revision, ready_at FROM review_waits "
            "WHERE user_id=? AND scope=? ORDER BY created_at",
            (user_id, scope),
        ).fetchall()
    names = ("kind", "instance", "period", "label", "created_at", "ready_revision", "ready_at")
    return [dict(zip(names, row)) for row in rows]


def review_ready(user_id: str, scope: str, kind: str, instance: str, period: str, revision: str, path: Path | None = None) -> None:
    with _db(path) as conn:
        conn.execute(
            "UPDATE review_waits SET ready_revision=?, ready_at=? WHERE user_id=? AND scope=? AND kind=? AND instance=? AND period=?",
            (revision, time.time(), user_id, scope, kind, instance, period),
        )


def review_dropped(user_id: str, scope: str, kind: str, instance: str, period: str, path: Path | None = None) -> None:
    with _db(path) as conn:
        conn.execute(
            "DELETE FROM review_waits WHERE user_id=? AND scope=? AND kind=? AND instance=? AND period=?",
            (user_id, scope, kind, instance, period),
        )


def save_subscription(user_id: str, scope: str, user_profile: str, endpoint: str, p256dh: str, auth: str, label: str,
                      path: Path | None = None, *, view_as: str = "") -> None:
    with _db(path) as conn:
        conn.execute(
            "INSERT INTO push_subscriptions (endpoint, user_id, scope, user_profile, view_as, p256dh, auth, label, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (endpoint) DO UPDATE SET user_id=excluded.user_id, scope=excluded.scope, "
            "user_profile=excluded.user_profile, view_as=excluded.view_as, p256dh=excluded.p256dh, auth=excluded.auth, "
            "label=excluded.label, failures=0",
            (endpoint, user_id, scope, user_profile, view_as, p256dh, auth, label[:80], time.time()),
        )
        conn.execute(
            "DELETE FROM push_subscriptions WHERE endpoint IN (SELECT endpoint FROM push_subscriptions WHERE user_id=? "
            "ORDER BY created_at DESC LIMIT -1 OFFSET ?)",
            (user_id, MAX_SUBSCRIPTIONS),
        )


def subscriptions(user_id: str | None = None, path: Path | None = None) -> list[dict[str, Any]]:
    with _existing(path) as conn:
        if conn is None:
            return []
        sql = "SELECT endpoint, user_id, scope, user_profile, view_as, p256dh, auth, label, created_at, failures FROM push_subscriptions"
        rows = conn.execute(sql + (" WHERE user_id=?" if user_id is not None else "") + " ORDER BY created_at",
                            (user_id,) if user_id is not None else ()).fetchall()
    names = ("endpoint", "user_id", "scope", "user_profile", "view_as", "p256dh", "auth", "label", "created_at", "failures")
    return [dict(zip(names, row)) for row in rows]


def remove_subscription(endpoint: str, user_id: str | None = None, path: Path | None = None) -> bool:
    with _existing(path) as conn:
        if conn is None:
            return False
        if user_id is None:
            removed = conn.execute("DELETE FROM push_subscriptions WHERE endpoint=?", (endpoint,)).rowcount > 0
        else:
            removed = conn.execute("DELETE FROM push_subscriptions WHERE endpoint=? AND user_id=?", (endpoint, user_id)).rowcount > 0
        if removed:
            conn.execute("DELETE FROM marks WHERE scope=? AND kind='pushed'", (push_scope(endpoint),))
        return removed


def subscription_failed(endpoint: str, limit: int = 5, path: Path | None = None) -> None:
    with _existing(path) as conn:
        if conn is not None:
            conn.execute("UPDATE push_subscriptions SET failures=failures+1 WHERE endpoint=?", (endpoint,))
            if conn.execute("DELETE FROM push_subscriptions WHERE endpoint=? AND failures>=?", (endpoint, limit)).rowcount:
                conn.execute("DELETE FROM marks WHERE scope=? AND kind='pushed'", (push_scope(endpoint),))


def subscription_ok(endpoint: str, path: Path | None = None) -> None:
    with _existing(path) as conn:
        if conn is not None:
            conn.execute("UPDATE push_subscriptions SET failures=0 WHERE endpoint=?", (endpoint,))


def meta(name: str, path: Path | None = None) -> str | None:
    with _existing(path) as conn:
        row = conn.execute("SELECT value FROM meta WHERE name=?", (name,)).fetchone() if conn is not None else None
    return row[0] if row else None


def set_meta_once(name: str, value: str, path: Path | None = None) -> str:
    with _db(path) as conn:
        conn.execute("INSERT OR IGNORE INTO meta (name, value) VALUES (?, ?)", (name, value))
        return conn.execute("SELECT value FROM meta WHERE name=?", (name,)).fetchone()[0]


def forget_user(user_id: str, path: Path | None = None) -> None:
    with _existing(path) as conn:
        if conn is not None:
            for table in ("marks", "review_waits", "push_subscriptions"):
                conn.execute(f"DELETE FROM {table} WHERE user_id=?", (user_id,))
