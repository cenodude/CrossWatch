# cw_platform/local_db/episode_groups.py
# CrossWatch - Persistent pair episode group observations
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

from .db import get_conn


def load(base, scope):
    conn = get_conn(base)
    if conn is None:
        raise RuntimeError("Episode group state database is unavailable")
    return {str(row[0]): json.loads(row[1]) for row in conn.execute(
        "SELECT group_id,state_json FROM episode_group_state WHERE pair_scope=?", (scope,)
    )}


def save(base, scope, states):
    conn = get_conn(base)
    if conn is None:
        raise RuntimeError("Episode group state database is unavailable")
    with conn:
        conn.execute("DELETE FROM episode_group_state WHERE pair_scope=?", (scope,))
        conn.executemany("INSERT INTO episode_group_state(pair_scope,group_id,state_json) VALUES(?,?,?)",
                         [(scope, group_id, json.dumps(state, sort_keys=True)) for group_id, state in states.items()])


def load_scrobble(base, pair_scope, delivery_scope):
    conn = get_conn(base)
    if conn is None:
        raise RuntimeError("Episode group state database is unavailable")
    row = conn.execute("SELECT state_json FROM episode_group_scrobbles WHERE pair_scope=? AND delivery_scope=?",
                       (pair_scope, delivery_scope)).fetchone()
    return json.loads(row[0]) if row else {"completed": [], "sent": []}


def save_scrobble(base, pair_scope, delivery_scope, state):
    conn = get_conn(base)
    if conn is None:
        raise RuntimeError("Episode group state database is unavailable")
    with conn:
        conn.execute("""INSERT INTO episode_group_scrobbles(pair_scope,delivery_scope,state_json) VALUES(?,?,?)
            ON CONFLICT(pair_scope,delivery_scope) DO UPDATE SET state_json=excluded.state_json""",
                     (pair_scope, delivery_scope, json.dumps(state, sort_keys=True)))
