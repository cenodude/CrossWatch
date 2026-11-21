# cw_platform/event_archive/schema.py
# CrossWatch - Versioned schema and migrations
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import sqlite3

SCHEMA_VERSION = 8

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
    domain                 TEXT DEFAULT 'sync',
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
    session_key            TEXT,
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
    domain                 TEXT DEFAULT 'sync',
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

_CREATE_RUN_PAIRS = """
CREATE TABLE IF NOT EXISTS run_pairs (
    run_id        TEXT NOT NULL,
    pair_id       TEXT NOT NULL DEFAULT '',
    scope_key     TEXT NOT NULL DEFAULT '',
    feature       TEXT NOT NULL DEFAULT '',
    pair_key      TEXT NOT NULL DEFAULT '',
    src_provider  TEXT NOT NULL DEFAULT '',
    src_instance  TEXT NOT NULL DEFAULT '',
    dst_provider  TEXT NOT NULL DEFAULT '',
    dst_instance  TEXT NOT NULL DEFAULT '',
    added         INTEGER NOT NULL DEFAULT 0,
    removed       INTEGER NOT NULL DEFAULT 0,
    updated       INTEGER NOT NULL DEFAULT 0,
    errors        INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, pair_id, scope_key, feature)
) WITHOUT ROWID
"""

_CREATE_RUN_ISSUE_COUNTS = """
CREATE TABLE IF NOT EXISTS run_issue_counts (
    run_id   TEXT NOT NULL,
    feature  TEXT NOT NULL DEFAULT '',
    items    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (run_id, feature)
) WITHOUT ROWID
"""

_CREATE_EVENT_TITLES = """
CREATE TABLE IF NOT EXISTS event_titles (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    title_hash       TEXT UNIQUE,
    domain           TEXT DEFAULT 'sync',
    created_at       INTEGER,
    updated_at       INTEGER,
    dirty            INTEGER DEFAULT 1,
    run_id           TEXT,
    first_event_at   INTEGER,
    last_event_at    INTEGER,
    event_count      INTEGER DEFAULT 0,
    status           TEXT,
    attention        INTEGER DEFAULT 0,
    item_key         TEXT,
    ident            TEXT,
    title            TEXT,
    year             INTEGER,
    media_type       TEXT,
    season           INTEGER,
    episode          INTEGER,
    source_provider  TEXT,
    source_instance  TEXT,
    kinds            TEXT,
    states           TEXT,
    dests            TEXT,
    routes           TEXT,
    destinations     TEXT,
    ok_dests         TEXT,
    ids              TEXT,
    rating           TEXT,
    progress         TEXT,
    acknowledged_at  INTEGER
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
    "CREATE INDEX IF NOT EXISTS idx_events_run_type_group ON events(run_id, event_type, group_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_group_created_id ON events(group_id, created_at, id)",
    "CREATE INDEX IF NOT EXISTS idx_events_domain_created ON events(domain, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_source_kind_created ON events(source_kind, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_severity_created ON events(severity, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_type_created ON events(event_type, created_at)",
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
    "CREATE INDEX IF NOT EXISTS idx_groups_ack_last_id ON event_groups(acknowledged_at, last_event_at, id)",
    "CREATE INDEX IF NOT EXISTS idx_groups_domain_last ON event_groups(domain, last_event_at, id)",
    "CREATE INDEX IF NOT EXISTS idx_groups_operation_window ON event_groups(operation, last_event_at, first_event_at)",
)


_ADD_COLUMNS = (
    ("events", "acknowledged_at", "INTEGER"),
    ("events", "acknowledged_by", "TEXT"),
    ("events", "group_id", "INTEGER"),
    ("events", "domain", "TEXT DEFAULT 'sync'"),
    ("events", "session_key", "TEXT"),
    ("event_groups", "domain", "TEXT DEFAULT 'sync'"),
    ("event_groups", "run_owned", "INTEGER DEFAULT 0"),
    ("event_groups", "health_only", "INTEGER DEFAULT 0"),
    ("event_groups", "activity_hash", "TEXT"),
    ("events", "title_id", "INTEGER"),
    ("event_titles", "routes", "TEXT"),
    ("event_titles", "ok_dests", "TEXT"),
)

_EXTRA_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_events_acknowledged ON events(acknowledged_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_group_id ON events(group_id)",
    "CREATE INDEX IF NOT EXISTS idx_groups_activity ON event_groups(domain, activity_hash, last_event_at, id)",
    "CREATE INDEX IF NOT EXISTS idx_sync_runs_started ON sync_runs(started_at)",
    "CREATE INDEX IF NOT EXISTS idx_sync_runs_finished ON sync_runs(finished_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_title_id ON events(title_id)",
    "CREATE INDEX IF NOT EXISTS idx_titles_domain_last ON event_titles(domain, last_event_at, id)",
    "CREATE INDEX IF NOT EXISTS idx_titles_attention ON event_titles(domain, last_event_at) WHERE attention=1",
    "CREATE INDEX IF NOT EXISTS idx_titles_run ON event_titles(run_id)",
    "CREATE INDEX IF NOT EXISTS idx_titles_ident ON event_titles(ident)",
    "CREATE INDEX IF NOT EXISTS idx_titles_item_key ON event_titles(item_key)",
    "CREATE INDEX IF NOT EXISTS idx_titles_dirty ON event_titles(dirty) WHERE dirty=1",
)


def _create_tables(conn: sqlite3.Connection) -> None:
    for stmt in (_CREATE_SYNC_RUNS, _CREATE_EVENTS, _CREATE_EVENT_GROUPS, _CREATE_EVENT_IMPORTS, _CREATE_RUN_PAIRS,
                 _CREATE_RUN_ISSUE_COUNTS, _CREATE_EVENT_TITLES):
        conn.execute(stmt)


def _ensure_columns(conn: sqlite3.Connection) -> None:
    for table, col, typ in _ADD_COLUMNS:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {typ}")
        except Exception:
            pass


def _create_indexes(conn: sqlite3.Connection) -> None:
    for stmt in (*_INDEXES, *_GROUP_INDEXES, *_EXTRA_INDEXES):
        conn.execute(stmt)


def apply_schema(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    ver = int(cur.execute("PRAGMA user_version").fetchone()[0] or 0)
    cur.close()

    with conn:
        _create_tables(conn)
        _ensure_columns(conn)
        _create_indexes(conn)
        if ver < SCHEMA_VERSION:
            conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    if ver < 7:
        try:
            from .run_scope import backfill_run_pairs

            backfill_run_pairs(conn)
        except Exception:
            pass
    return max(ver, SCHEMA_VERSION)
