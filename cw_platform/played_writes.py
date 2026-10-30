# cw_platform/played_writes.py
# CrossWatch - Record of played marks CrossWatch wrote to media servers, shared across processes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import re
import time
from typing import Any

from cw_platform.local_db.db import get_conn

NAMESPACE = "played_writes"
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
WRITE_TTL_SECONDS = 3600.0
MATCH_SLACK_SECONDS = 120.0


def _part(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text.replace("-", "") if _UUID.fullmatch(text) else text


def _key(provider: str, scope: Any, item_id: Any) -> str:
    return json.dumps([_part(provider), _part(scope), _part(item_id)])


def note_write(provider: str, scope: Any, item_id: Any) -> None:
    if not _part(provider) or not _part(item_id):
        return
    conn = get_conn(None)
    if conn is None:
        return
    now = time.time()
    try:
        with conn:
            conn.execute("DELETE FROM ttl_dedupe_entries WHERE namespace=? AND expires_at<?", (NAMESPACE, now))
            conn.execute(
                "INSERT INTO ttl_dedupe_entries(namespace,dedupe_key,seen_at,expires_at,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(namespace,dedupe_key) DO UPDATE SET seen_at=excluded.seen_at,"
                "expires_at=excluded.expires_at,updated_at=excluded.updated_at",
                (NAMESPACE, _key(provider, scope, item_id), now, now + WRITE_TTL_SECONDS, int(time.time_ns())),
            )
    except Exception:
        pass


def written_by_crosswatch(provider: str, scope: Any, item_id: Any, observed_at: float | None, now: float | None = None) -> bool:
    if not _part(provider) or not _part(item_id):
        return False
    conn = get_conn(None)
    if conn is None:
        return False
    moment = now if now is not None else time.time()
    try:
        row = conn.execute(
            "SELECT seen_at FROM ttl_dedupe_entries WHERE namespace=? AND dedupe_key=? AND expires_at>=?",
            (NAMESPACE, _key(provider, scope, item_id), moment),
        ).fetchone()
    except Exception:
        return False
    if row is None:
        return False
    if observed_at is None:
        return True
    return float(observed_at) <= float(row["seen_at"]) + MATCH_SLACK_SECONDS
