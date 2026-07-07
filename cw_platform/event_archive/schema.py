# cw_platform/event_archive/schema.py
# CrossWatch - Versioned schema and migrations
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 3

_CREATE_SYNC_RUNS = """
CREATE TABLE IF NOT EXISTS sync_runs (
    run_id        TEXT PRIMARY KEY,
    started_at    INTEGER,
    finished_at   INTEGER,
    mode          TEXT,
    dry_run       INTEGER DEFAULT 0,
    status        TEXT,
    pairs         INTEGER DEFAULT 0,
    added         INTEGER DEFAULT 0,
    removed       INTEGER DEFAULT 0,
    updated       INTEGER DEFAULT 0,
    unresolved    INTEGER DEFAULT 0,
    blocked       INTEGER DEFAULT 0,
    errors        INTEGER DEFAULT 0,
    summary       TEXT
)
"""

_CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    event_hash             TEXT UNIQUE,
    created_at             INTEGER,
    run_id                 TEXT,
    event_type             TEXT,
    severity               TEXT,
    feature                TEXT,
    operation              TEXT,
    pair_key               TEXT,
    direction              TEXT,
    source_provider        TEXT,
    source_instance        TEXT,
    destination_provider   TEXT,
    destination_instance   TEXT,
    origin_provider        TEXT,
    origin_instance        TEXT,
    origin_confidence      TEXT,
    item_key               TEXT,
    title                  TEXT,
    year                   INTEGER,
    media_type             TEXT,
    season                 INTEGER,
    episode                INTEGER,
    old_value              TEXT,
    new_value              TEXT,
    value_type             TEXT,
    reason_code            TEXT,
    reason                 TEXT,
    match_basis            TEXT,
    source_kind            TEXT,
    source_file            TEXT,
    source_mtime           INTEGER,
    detail                 TEXT,
    acknowledged_at        INTEGER,
    acknowledged_by        TEXT,
    group_id               INTEGER
)
"""

_CREATE_EVENT_GROUPS = """
CREATE TABLE IF NOT EXISTS event_groups (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    group_hash             TEXT UNIQUE,
    created_at             INTEGER,
    updated_at             INTEGER,
    first_event_at         INTEGER,
    last_event_at          INTEGER,
    event_count            INTEGER DEFAULT 0,
    status                 TEXT,
    severity               TEXT,
    feature                TEXT,
    operation              TEXT,
    source_provider        TEXT,
    source_instance        TEXT,
    destination_provider   TEXT,
    destination_instance   TEXT,
    origin_provider        TEXT,
    origin_instance        TEXT,
    pair_key               TEXT,
    direction              TEXT,
    item_key               TEXT,
    title                  TEXT,
    year                   INTEGER,
    media_type             TEXT,
    season                 INTEGER,
    episode                INTEGER,
    reason_code            TEXT,
    reason                 TEXT,
    summary                TEXT,
    acknowledged_at        INTEGER,
    acknowledged_by        TEXT
)
"""

_CREATE_EVENT_IMPORTS = """
CREATE TABLE IF NOT EXISTS event_imports (
    source_file        TEXT PRIMARY KEY,
    source_mtime       INTEGER,
    source_size        INTEGER,
    last_imported_at   INTEGER,
    imported_rows      INTEGER DEFAULT 0
)
"""

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_run_id ON events(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_events_source_provider ON events(source_provider)",
    "CREATE INDEX IF NOT EXISTS idx_events_destination_provider ON events(destination_provider)",
    "CREATE INDEX IF NOT EXISTS idx_events_origin_provider ON events(origin_provider)",
    "CREATE INDEX IF NOT EXISTS idx_events_pair_feature ON events(pair_key, feature)",
    "CREATE INDEX IF NOT EXISTS idx_events_item_key ON events(item_key)",
    "CREATE INDEX IF NOT EXISTS idx_events_reason_code ON events(reason_code)",
)

_GROUP_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_groups_last_event_at ON event_groups(last_event_at)",
    "CREATE INDEX IF NOT EXISTS idx_groups_item_key ON event_groups(item_key)",
    "CREATE INDEX IF NOT EXISTS idx_groups_feature ON event_groups(feature)",
    "CREATE INDEX IF NOT EXISTS idx_groups_source_provider ON event_groups(source_provider)",
    "CREATE INDEX IF NOT EXISTS idx_groups_destination_provider ON event_groups(destination_provider)",
    "CREATE INDEX IF NOT EXISTS idx_groups_origin_provider ON event_groups(origin_provider)",
    "CREATE INDEX IF NOT EXISTS idx_groups_status ON event_groups(status)",
    "CREATE INDEX IF NOT EXISTS idx_groups_acknowledged ON event_groups(acknowledged_at)",
)


def _create_all(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.execute(_CREATE_SYNC_RUNS)
    cur.execute(_CREATE_EVENTS)
    cur.execute(_CREATE_EVENT_GROUPS)
    cur.execute(_CREATE_EVENT_IMPORTS)
    for stmt in _INDEXES:
        cur.execute(stmt)
    for stmt in _GROUP_INDEXES:
        cur.execute(stmt)
    cur.close()


def _migrate(conn: sqlite3.Connection, from_ver: int) -> None:
    if from_ver < 2:
        for col, typ in (("acknowledged_at", "INTEGER"), ("acknowledged_by", "TEXT")):
            try:
                conn.execute(f"ALTER TABLE events ADD COLUMN {col} {typ}")
            except Exception:
                pass
    if from_ver < 3:
        try:
            conn.execute("ALTER TABLE events ADD COLUMN group_id INTEGER")
        except Exception:
            pass
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_acknowledged ON events(acknowledged_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_events_group_id ON events(group_id)")


def apply_schema(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    ver = int(cur.execute("PRAGMA user_version").fetchone()[0] or 0)
    cur.close()

    if ver >= SCHEMA_VERSION:
        _create_all(conn)
        with conn:
            _migrate(conn, SCHEMA_VERSION)
        return ver

    with conn:
        _create_all(conn)
        _migrate(conn, ver)
        conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
    return SCHEMA_VERSION
