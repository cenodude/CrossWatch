# cw_platform/local_db/pair_skips.py
# CrossWatch - Items a pair leaves out on purpose, kept for the analyzer
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from .db import get_conn


def save(base: str | Path, scope: str, feature: str, state: Mapping[str, Any] | None) -> None:
    conn = get_conn(base)
    if conn is None:
        raise RuntimeError("Pair skip database is unavailable")
    with conn:
        if not state:
            conn.execute("DELETE FROM pair_analyzer_skips WHERE pair_scope=? AND feature=?", (scope, feature))
            return
        conn.execute("""INSERT INTO pair_analyzer_skips(pair_scope,feature,state_json,updated_at) VALUES(?,?,?,?)
            ON CONFLICT(pair_scope,feature) DO UPDATE SET state_json=excluded.state_json,updated_at=excluded.updated_at""",
                     (scope, feature, json.dumps(dict(state), sort_keys=True), time.time_ns()))


def load(base: str | Path, scopes: Iterable[str]) -> dict[tuple[str, str], dict[str, Any]]:
    wanted = sorted({str(scope) for scope in scopes if scope})
    if not wanted:
        return {}
    conn = get_conn(base)
    if conn is None:
        return {}
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for start in range(0, len(wanted), 500):
        part = wanted[start:start + 500]
        rows = conn.execute(
            "SELECT pair_scope,feature,state_json FROM pair_analyzer_skips WHERE pair_scope IN ("
            + ",".join("?" for _ in part) + ")", part,
        ).fetchall()
        for row in rows:
            try:
                state = json.loads(row[2])
            except Exception:
                continue
            if isinstance(state, dict):
                out[(str(row[0]), str(row[1]))] = state
    return out
