# cw_platform/local_db/schema.py
# CrossWatch - Local database schema
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import sqlite3
import time

SCHEMA_VERSION = 1

ID_KEYS = (
    "tmdb",
    "imdb",
    "tvdb",
    "trakt",
    "simkl",
    "mal",
    "anilist",
    "kitsu",
    "anidb",
    "plex",
    "jellyfin",
    "mdblist",
    "emby",
    "guid",
    "slug",
)


def _id_columns(prefix: str) -> str:
    return ",\n".join(f"    {prefix}_{key} TEXT" for key in ID_KEYS)


_CREATE_SCHEMA_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INTEGER PRIMARY KEY,
    applied_at  INTEGER NOT NULL,
    name        TEXT NOT NULL
)
"""

_CREATE_STATE_META = """
CREATE TABLE IF NOT EXISTS state_meta (
    key          TEXT PRIMARY KEY,
    value_text   TEXT,
    value_int    INTEGER,
    value_real   REAL,
    value_type   TEXT NOT NULL DEFAULT 'text',
    updated_at   INTEGER NOT NULL
)
"""

_CREATE_LOCAL_META = """
CREATE TABLE IF NOT EXISTS local_meta (
    key          TEXT PRIMARY KEY,
    value_text   TEXT,
    value_int    INTEGER,
    value_real   REAL,
    value_type   TEXT NOT NULL DEFAULT 'text',
    updated_at   INTEGER NOT NULL
)
"""

_CREATE_PROVIDER_FEATURE_STATE = """
CREATE TABLE IF NOT EXISTS provider_feature_state (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    provider          TEXT NOT NULL,
    instance          TEXT NOT NULL DEFAULT 'default',
    feature           TEXT NOT NULL,
    mode              TEXT NOT NULL DEFAULT 'collapsed',
    checkpoint_text   TEXT,
    checkpoint_int    INTEGER,
    checkpoint_real   REAL,
    checkpoint_type   TEXT,
    updated_at        INTEGER NOT NULL,
    UNIQUE(provider, instance, feature)
)
"""

_CREATE_BASELINE_ITEMS = f"""
CREATE TABLE IF NOT EXISTS baseline_items (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    provider_state_id        INTEGER NOT NULL,
    item_key                 TEXT NOT NULL,
    base_key                 TEXT,
    event_key                TEXT,
    media_type               TEXT,
    title                    TEXT,
    year                     INTEGER,
    season                   INTEGER,
    episode                  INTEGER,
    series_title             TEXT,
{_id_columns("ids")},
{_id_columns("show_ids")},
    watched                  INTEGER,
    watched_at               TEXT,
    rating                   REAL,
    rated_at                 TEXT,
    progress_ms              INTEGER,
    progress_percent         REAL,
    duration_ms              INTEGER,
    progress_at              TEXT,
    progress_at_source       TEXT,
    history_id               TEXT,
    trakt_history_id         TEXT,
    simkl_bucket             TEXT,
    anime_type               TEXT,
    simkl_episode_number     INTEGER,
    floppy_consumption_id    TEXT,
    floppy_list_item_id      TEXT,
    floppy_season            INTEGER,
    floppy_episode           INTEGER,
    trakt_number_abs         INTEGER,
    cw_marked                INTEGER,
    cw_instance              TEXT,
    provider_item_id         TEXT,
    provider_event_id        TEXT,
    updated_at               INTEGER NOT NULL,
    UNIQUE(provider_state_id, item_key),
    FOREIGN KEY(provider_state_id) REFERENCES provider_feature_state(id) ON DELETE CASCADE
)
"""

_CREATE_LAST_SYNC_SUMMARY = """
CREATE TABLE IF NOT EXISTS last_sync_summary (
    id             INTEGER PRIMARY KEY CHECK(id=1),
    started_at     INTEGER,
    finished_at    INTEGER,
    updated_at     INTEGER NOT NULL
)
"""

_CREATE_LAST_SYNC_FIELDS = """
CREATE TABLE IF NOT EXISTS last_sync_fields (
    key          TEXT PRIMARY KEY,
    value_text   TEXT,
    value_int    INTEGER,
    value_real   REAL,
    value_type   TEXT NOT NULL DEFAULT 'text',
    updated_at   INTEGER NOT NULL
)
"""

_CREATE_LAST_SYNC_RESULT_METRICS = """
CREATE TABLE IF NOT EXISTS last_sync_result_metrics (
    key          TEXT PRIMARY KEY,
    value_text   TEXT,
    value_int    INTEGER,
    value_real   REAL,
    value_type   TEXT NOT NULL DEFAULT 'text',
    updated_at   INTEGER NOT NULL
)
"""

_CREATE_LAST_SYNC_TIMELINE = """
CREATE TABLE IF NOT EXISTS last_sync_timeline (
    flag        TEXT PRIMARY KEY,
    value       INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_META = """
CREATE TABLE IF NOT EXISTS statistics_meta (
    key          TEXT PRIMARY KEY,
    value_text   TEXT,
    value_int    INTEGER,
    value_real   REAL,
    value_type   TEXT NOT NULL DEFAULT 'text',
    updated_at   INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_EVENTS = """
CREATE TABLE IF NOT EXISTS statistics_events (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    ts                INTEGER NOT NULL,
    action            TEXT,
    feature           TEXT,
    item_key          TEXT,
    source            TEXT,
    title             TEXT,
    name              TEXT,
    media_type        TEXT,
    year              INTEGER,
    season            INTEGER,
    episode           INTEGER,
    series_title      TEXT,
    show_title        TEXT,
    ids_tmdb          TEXT,
    ids_imdb          TEXT,
    ids_tvdb          TEXT,
    ids_simkl         TEXT,
    ids_slug          TEXT,
    show_ids_tmdb     TEXT,
    show_ids_imdb     TEXT,
    show_ids_tvdb     TEXT,
    show_ids_simkl    TEXT,
    show_ids_slug     TEXT,
    added_at          TEXT,
    listed_at         TEXT,
    watched_at        TEXT,
    rated_at          TEXT,
    last_watched_at   TEXT,
    user_rated_at     TEXT,
    sync_ts           INTEGER,
    ingested_ts       INTEGER,
    seen_ts           INTEGER,
    updated_at        INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_SAMPLES = """
CREATE TABLE IF NOT EXISTS statistics_samples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    feature     TEXT NOT NULL DEFAULT 'watchlist',
    ts          INTEGER NOT NULL,
    count       INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_CURRENT_ITEMS = """
CREATE TABLE IF NOT EXISTS statistics_current_items (
    feature     TEXT NOT NULL,
    item_key    TEXT NOT NULL,
    src         TEXT,
    title       TEXT,
    media_type  TEXT,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY(feature, item_key)
)
"""

_CREATE_STATISTICS_CURRENT_PROVIDERS = """
CREATE TABLE IF NOT EXISTS statistics_current_providers (
    feature   TEXT NOT NULL,
    item_key  TEXT NOT NULL,
    provider  TEXT NOT NULL,
    PRIMARY KEY(feature, item_key, provider),
    FOREIGN KEY(feature, item_key) REFERENCES statistics_current_items(feature, item_key) ON DELETE CASCADE
)
"""

_CREATE_STATISTICS_COUNTERS = """
CREATE TABLE IF NOT EXISTS statistics_counters (
    name        TEXT PRIMARY KEY,
    value       INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_LAST_RUN = """
CREATE TABLE IF NOT EXISTS statistics_last_run (
    id          INTEGER PRIMARY KEY CHECK(id=1),
    added       INTEGER NOT NULL DEFAULT 0,
    removed     INTEGER NOT NULL DEFAULT 0,
    updated     INTEGER NOT NULL DEFAULT 0,
    ts          INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_HTTP_EVENTS = """
CREATE TABLE IF NOT EXISTS statistics_http_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ts              INTEGER NOT NULL,
    provider        TEXT NOT NULL,
    endpoint        TEXT,
    method          TEXT,
    status          INTEGER,
    ok              INTEGER,
    ms              INTEGER,
    bytes_in        INTEGER,
    bytes_out       INTEGER,
    rate_remaining  INTEGER,
    rate_reset      TEXT,
    updated_at      INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_HTTP_COUNTERS = """
CREATE TABLE IF NOT EXISTS statistics_http_counters (
    provider             TEXT PRIMARY KEY,
    calls                INTEGER NOT NULL DEFAULT 0,
    ok                   INTEGER NOT NULL DEFAULT 0,
    err                  INTEGER NOT NULL DEFAULT 0,
    bytes_in             INTEGER NOT NULL DEFAULT 0,
    bytes_out            INTEGER NOT NULL DEFAULT 0,
    ms_sum               INTEGER NOT NULL DEFAULT 0,
    last_status          INTEGER NOT NULL DEFAULT 0,
    last_ok              INTEGER NOT NULL DEFAULT 0,
    last_at              INTEGER NOT NULL DEFAULT 0,
    last_rate_remaining  INTEGER,
    updated_at           INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_HTTP_LAST = """
CREATE TABLE IF NOT EXISTS statistics_http_last (
    request_key      TEXT PRIMARY KEY,
    ts               INTEGER NOT NULL,
    provider         TEXT NOT NULL,
    endpoint         TEXT,
    method           TEXT,
    status           INTEGER,
    ok               INTEGER,
    ms               INTEGER,
    bytes_in         INTEGER,
    bytes_out        INTEGER,
    rate_remaining   INTEGER,
    rate_reset       TEXT,
    updated_at       INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_FEATURE_TOTALS = """
CREATE TABLE IF NOT EXISTS statistics_feature_totals (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          INTEGER NOT NULL,
    feature     TEXT NOT NULL,
    added       INTEGER NOT NULL DEFAULT 0,
    removed     INTEGER NOT NULL DEFAULT 0,
    updated     INTEGER NOT NULL DEFAULT 0,
    src         TEXT,
    run_id      TEXT,
    kind        TEXT,
    updated_at  INTEGER NOT NULL
)
"""

_CREATE_STATISTICS_INGESTED_RUNS = """
CREATE TABLE IF NOT EXISTS statistics_ingested_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT NOT NULL UNIQUE,
    updated_at  INTEGER NOT NULL
)
"""

_CREATE_MANUAL_POLICY_FEATURES = """
CREATE TABLE IF NOT EXISTS manual_policy_features (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider    TEXT NOT NULL,
    instance    TEXT NOT NULL DEFAULT 'default',
    feature     TEXT NOT NULL,
    ordinal     INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL,
    UNIQUE(provider, instance, feature)
)
"""

_CREATE_MANUAL_POLICY_BLOCKS = """
CREATE TABLE IF NOT EXISTS manual_policy_blocks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    feature_id  INTEGER NOT NULL,
    item_key    TEXT NOT NULL,
    ordinal     INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL,
    UNIQUE(feature_id, item_key),
    FOREIGN KEY(feature_id) REFERENCES manual_policy_features(id) ON DELETE CASCADE
)
"""

_CREATE_MANUAL_POLICY_ADD_ITEMS = f"""
CREATE TABLE IF NOT EXISTS manual_policy_add_items (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    feature_id               INTEGER NOT NULL,
    item_key                 TEXT NOT NULL,
    ordinal                  INTEGER NOT NULL DEFAULT 0,
    media_type               TEXT,
    title                    TEXT,
    name                     TEXT,
    year                     INTEGER,
    season                   INTEGER,
    episode                  INTEGER,
    series_title             TEXT,
    show_title               TEXT,
{_id_columns("ids")},
{_id_columns("show_ids")},
    watched                  INTEGER,
    watched_at               TEXT,
    last_watched_at          TEXT,
    rating                   REAL,
    user_rating              REAL,
    rated_at                 TEXT,
    user_rated_at            TEXT,
    progress_ms              INTEGER,
    progress_percent         REAL,
    duration_ms              INTEGER,
    progress_at              TEXT,
    progress_at_source       TEXT,
    provider_item_id         TEXT,
    provider_event_id        TEXT,
    updated_at               INTEGER NOT NULL,
    UNIQUE(feature_id, item_key),
    FOREIGN KEY(feature_id) REFERENCES manual_policy_features(id) ON DELETE CASCADE
)
"""

_CREATE_WATCHLIST_HIDDEN_ITEMS = """
CREATE TABLE IF NOT EXISTS watchlist_hidden_items (
    item_key    TEXT PRIMARY KEY,
    created_at  INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
)
"""

_CREATE_CURRENTLY_WATCHING_STREAMS = """
CREATE TABLE IF NOT EXISTS currently_watching_streams (
    stream_key         TEXT PRIMARY KEY,
    source             TEXT NOT NULL,
    provider_instance  TEXT,
    media_type         TEXT,
    title              TEXT,
    year               INTEGER,
    season             INTEGER,
    episode            INTEGER,
    progress           INTEGER,
    duration_ms        INTEGER,
    cover              TEXT,
    state              TEXT,
    updated            INTEGER,
    started            INTEGER,
    account            TEXT,
    server_uuid        TEXT,
    session_key        TEXT,
    stored_at          INTEGER NOT NULL
)
"""

_CREATE_CURRENTLY_WATCHING_IDS = """
CREATE TABLE IF NOT EXISTS currently_watching_stream_ids (
    stream_key  TEXT NOT NULL,
    id_type     TEXT NOT NULL,
    id_value    TEXT NOT NULL,
    PRIMARY KEY(stream_key, id_type),
    FOREIGN KEY(stream_key) REFERENCES currently_watching_streams(stream_key) ON DELETE CASCADE
)
"""

_CREATE_ACTIVITY_EVENTS = """
CREATE TABLE IF NOT EXISTS activity_events (
    event_id         TEXT PRIMARY KEY,
    kind             TEXT NOT NULL,
    method           TEXT,
    event            TEXT,
    status           TEXT NOT NULL DEFAULT 'ok',
    source           TEXT,
    source_instance  TEXT,
    target           TEXT,
    target_instance  TEXT,
    media_type       TEXT,
    title            TEXT,
    year             INTEGER,
    season           INTEGER,
    episode          INTEGER,
    progress         INTEGER,
    account          TEXT,
    watched_at       INTEGER,
    captured_at      INTEGER,
    updated_at       INTEGER NOT NULL
)
"""

_CREATE_ACTIVITY_EVENT_IDS = """
CREATE TABLE IF NOT EXISTS activity_event_ids (
    event_id  TEXT NOT NULL,
    id_type   TEXT NOT NULL,
    id_value  TEXT NOT NULL,
    PRIMARY KEY(event_id, id_type),
    FOREIGN KEY(event_id) REFERENCES activity_events(event_id) ON DELETE CASCADE
)
"""

_CREATE_TTL_DEDUPE_ENTRIES = """
CREATE TABLE IF NOT EXISTS ttl_dedupe_entries (
    namespace   TEXT NOT NULL,
    dedupe_key  TEXT NOT NULL,
    seen_at     REAL NOT NULL,
    expires_at  REAL NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY(namespace, dedupe_key)
)
"""

_CREATE_SYNC_RUN_REPORTS = """
CREATE TABLE IF NOT EXISTS sync_run_reports (
    run_id          TEXT PRIMARY KEY,
    started_at      TEXT,
    finished_at     TEXT,
    raw_started_ts  REAL,
    duration_sec    REAL,
    result          TEXT,
    exit_code       INTEGER,
    cmd             TEXT,
    running         INTEGER,
    added_last      INTEGER NOT NULL DEFAULT 0,
    removed_last    INTEGER NOT NULL DEFAULT 0,
    updated_last    INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
)
"""

_CREATE_SYNC_RUN_TIMELINE = """
CREATE TABLE IF NOT EXISTS sync_run_timeline (
    run_id      TEXT NOT NULL,
    flag        TEXT NOT NULL,
    value       INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY(run_id, flag),
    FOREIGN KEY(run_id) REFERENCES sync_run_reports(run_id) ON DELETE CASCADE
)
"""

_CREATE_SYNC_RUN_PROVIDER_COUNTS = """
CREATE TABLE IF NOT EXISTS sync_run_provider_counts (
    run_id      TEXT NOT NULL,
    phase       TEXT NOT NULL,
    provider    TEXT NOT NULL,
    value       INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY(run_id, phase, provider),
    FOREIGN KEY(run_id) REFERENCES sync_run_reports(run_id) ON DELETE CASCADE
)
"""

_CREATE_SYNC_RUN_FEATURE_LANES = """
CREATE TABLE IF NOT EXISTS sync_run_feature_lanes (
    run_id      TEXT NOT NULL,
    feature     TEXT NOT NULL,
    enabled     INTEGER NOT NULL DEFAULT 1,
    added       INTEGER NOT NULL DEFAULT 0,
    removed     INTEGER NOT NULL DEFAULT 0,
    updated     INTEGER NOT NULL DEFAULT 0,
    updated_at  INTEGER NOT NULL,
    PRIMARY KEY(run_id, feature),
    FOREIGN KEY(run_id) REFERENCES sync_run_reports(run_id) ON DELETE CASCADE
)
"""

_CREATE_SYNC_RUN_SPOTLIGHT_ITEMS = f"""
CREATE TABLE IF NOT EXISTS sync_run_spotlight_items (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         TEXT NOT NULL,
    feature        TEXT NOT NULL,
    bucket         TEXT NOT NULL,
    ordinal        INTEGER NOT NULL DEFAULT 0,
    item_key       TEXT,
    title          TEXT,
    name           TEXT,
    display_title  TEXT,
    media_type     TEXT,
    year           INTEGER,
    season         INTEGER,
    episode        INTEGER,
    series_title   TEXT,
    show_title     TEXT,
    source         TEXT,
    ts             INTEGER,
{_id_columns("ids")},
{_id_columns("show_ids")},
    updated_at     INTEGER NOT NULL,
    FOREIGN KEY(run_id) REFERENCES sync_run_reports(run_id) ON DELETE CASCADE
)
"""

_INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_pfs_provider_feature ON provider_feature_state(provider, instance, feature)",
    "CREATE INDEX IF NOT EXISTS idx_bi_state_key ON baseline_items(provider_state_id, item_key)",
    "CREATE INDEX IF NOT EXISTS idx_bi_state_base ON baseline_items(provider_state_id, base_key)",
    "CREATE INDEX IF NOT EXISTS idx_bi_state_event ON baseline_items(provider_state_id, event_key)",
    "CREATE INDEX IF NOT EXISTS idx_bi_state_watched ON baseline_items(provider_state_id, watched_at)",
    "CREATE INDEX IF NOT EXISTS idx_stats_events_ts ON statistics_events(ts)",
    "CREATE INDEX IF NOT EXISTS idx_stats_events_feature_ts ON statistics_events(feature, ts)",
    "CREATE INDEX IF NOT EXISTS idx_stats_samples_feature_ts ON statistics_samples(feature, ts)",
    "CREATE INDEX IF NOT EXISTS idx_stats_http_events_provider_ts ON statistics_http_events(provider, ts)",
    "CREATE INDEX IF NOT EXISTS idx_stats_feature_totals_ts ON statistics_feature_totals(ts)",
    "CREATE INDEX IF NOT EXISTS idx_manual_policy_feature ON manual_policy_features(provider, instance, feature)",
    "CREATE INDEX IF NOT EXISTS idx_manual_policy_blocks_key ON manual_policy_blocks(item_key)",
    "CREATE INDEX IF NOT EXISTS idx_manual_policy_add_items_key ON manual_policy_add_items(item_key)",
    "CREATE INDEX IF NOT EXISTS idx_watchlist_hidden_updated ON watchlist_hidden_items(updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_cw_streams_updated ON currently_watching_streams(updated)",
    "CREATE INDEX IF NOT EXISTS idx_cw_streams_state_updated ON currently_watching_streams(state, updated)",
    "CREATE INDEX IF NOT EXISTS idx_cw_streams_source_instance ON currently_watching_streams(source, provider_instance)",
    "CREATE INDEX IF NOT EXISTS idx_cw_stream_ids_type_value ON currently_watching_stream_ids(id_type, id_value)",
    "CREATE INDEX IF NOT EXISTS idx_activity_captured ON activity_events(captured_at DESC, watched_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_activity_kind_captured ON activity_events(kind, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_activity_status_captured ON activity_events(status, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_activity_media_captured ON activity_events(media_type, captured_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_activity_ids_type_value ON activity_event_ids(id_type, id_value)",
    "CREATE INDEX IF NOT EXISTS idx_ttl_dedupe_expires ON ttl_dedupe_entries(expires_at)",
    "CREATE INDEX IF NOT EXISTS idx_ttl_dedupe_namespace_seen ON ttl_dedupe_entries(namespace, seen_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_sync_reports_created ON sync_run_reports(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_sync_feature_run ON sync_run_feature_lanes(feature, run_id)",
    "CREATE INDEX IF NOT EXISTS idx_sync_spotlight_run_feature ON sync_run_spotlight_items(run_id, feature, bucket, ordinal)",
)


def apply_schema(conn: sqlite3.Connection) -> int:
    with conn:
        conn.execute(_CREATE_SCHEMA_MIGRATIONS)
        conn.execute(_CREATE_STATE_META)
        conn.execute(_CREATE_LOCAL_META)
        conn.execute(_CREATE_PROVIDER_FEATURE_STATE)
        conn.execute(_CREATE_BASELINE_ITEMS)
        conn.execute(_CREATE_LAST_SYNC_SUMMARY)
        conn.execute(_CREATE_LAST_SYNC_FIELDS)
        conn.execute(_CREATE_LAST_SYNC_RESULT_METRICS)
        conn.execute(_CREATE_LAST_SYNC_TIMELINE)
        conn.execute(_CREATE_STATISTICS_META)
        conn.execute(_CREATE_STATISTICS_EVENTS)
        conn.execute(_CREATE_STATISTICS_SAMPLES)
        conn.execute(_CREATE_STATISTICS_CURRENT_ITEMS)
        conn.execute(_CREATE_STATISTICS_CURRENT_PROVIDERS)
        conn.execute(_CREATE_STATISTICS_COUNTERS)
        conn.execute(_CREATE_STATISTICS_LAST_RUN)
        conn.execute(_CREATE_STATISTICS_HTTP_EVENTS)
        conn.execute(_CREATE_STATISTICS_HTTP_COUNTERS)
        conn.execute(_CREATE_STATISTICS_HTTP_LAST)
        conn.execute(_CREATE_STATISTICS_FEATURE_TOTALS)
        conn.execute(_CREATE_STATISTICS_INGESTED_RUNS)
        conn.execute(_CREATE_MANUAL_POLICY_FEATURES)
        conn.execute(_CREATE_MANUAL_POLICY_BLOCKS)
        conn.execute(_CREATE_MANUAL_POLICY_ADD_ITEMS)
        conn.execute(_CREATE_WATCHLIST_HIDDEN_ITEMS)
        conn.execute(_CREATE_CURRENTLY_WATCHING_STREAMS)
        conn.execute(_CREATE_CURRENTLY_WATCHING_IDS)
        conn.execute(_CREATE_ACTIVITY_EVENTS)
        conn.execute(_CREATE_ACTIVITY_EVENT_IDS)
        conn.execute(_CREATE_TTL_DEDUPE_ENTRIES)
        conn.execute(_CREATE_SYNC_RUN_REPORTS)
        conn.execute(_CREATE_SYNC_RUN_TIMELINE)
        conn.execute(_CREATE_SYNC_RUN_PROVIDER_COUNTS)
        conn.execute(_CREATE_SYNC_RUN_FEATURE_LANES)
        conn.execute(_CREATE_SYNC_RUN_SPOTLIGHT_ITEMS)
        for stmt in _INDEXES:
            conn.execute(stmt)
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations(version, applied_at, name) VALUES(?,?,?)",
            (SCHEMA_VERSION, int(time.time()), "local_state"),
        )
    return SCHEMA_VERSION
