# cw_platform/local_db/continue_rows.py
# CrossWatch - Last seen Continue Watching row per pair and provider, for removal detection
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .db import get_conn


def save(base: str | Path, scope: str, provider: str, instance: str, state: Mapping[str, Any]) -> None:
    conn = get_conn(base)
    if conn is None:
        raise RuntimeError("Continue Watching database is unavailable")
    with conn:
        conn.execute("""INSERT INTO pair_continue_rows(pair_scope,provider,instance,state_json,updated_at) VALUES(?,?,?,?,?)
            ON CONFLICT(pair_scope,provider,instance) DO UPDATE SET state_json=excluded.state_json,updated_at=excluded.updated_at""",
                     (scope, provider.upper(), instance or "default", json.dumps(dict(state), sort_keys=True), time.time_ns()))


def load(base: str | Path, scope: str, provider: str, instance: str) -> dict[str, Any] | None:
    conn = get_conn(base)
    if conn is None:
        return None
    row = conn.execute("SELECT state_json FROM pair_continue_rows WHERE pair_scope=? AND provider=? AND instance=?",
                       (scope, provider.upper(), instance or "default")).fetchone()
    if row is None:
        return None
    try:
        state = json.loads(row[0])
    except Exception:
        return None
    return state if isinstance(state, dict) else None
