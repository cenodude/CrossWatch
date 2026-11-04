# tests/test_tracker_speed.py
# CrossWatch - Tracker database reads stay on indexes and aggregates stay current
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from cw_platform import tracker_store
from cw_platform.tracker_store import TrackerStore


def _episode(show: int, ep: int, when: str) -> dict[str, Any]:
    return {
        "type": "episode", "title": f"S01E{ep:02d}", "series_title": f"Show {show}", "series_year": 2000 + show,
        "season": 1, "episode": ep, "show_ids": {"tmdb": str(show)}, "ids": {"tmdb": str(9000 + show * 100 + ep)},
        "watched_at": when,
    }


def _history() -> dict[str, Any]:
    items: dict[str, Any] = {}
    for i in range(1, 61):
        items[f"tmdb:{i}"] = {"type": "movie", "title": f"Movie {i}", "ids": {"tmdb": str(i), "imdb": f"tt{i:07d}"},
                              "watched_at": f"2026-{1 + i % 10:02d}-{1 + i % 27:02d}T10:00:00.000Z"}
    for show in range(1, 6):
        for ep in range(1, 13):
            items[f"tmdb:{show}#s01e{ep:02d}"] = _episode(show, ep, f"2025-{ep:02d}-{show:02d}T20:00:00.000Z")
    items["tmdb:7@1700000000"] = {"type": "movie", "title": "Movie 7", "ids": {"tmdb": "7"}, "watched_at": "2023-11-14T22:13:20.000Z"}
    return items


@pytest.fixture()
def store(tmp_path):
    tracker_store._CACHE.clear()
    store = TrackerStore(tmp_path)
    store.write_state("history", _history())
    store.write_state("watchlist", {"tmdb:3": {"type": "show", "title": "Show 3", "ids": {"tmdb": "3"}, "listed_at": "2026-01-01T00:00:00Z"}})
    return store


@pytest.fixture()
def traced(store, monkeypatch):
    statements: list[str] = []
    connect = TrackerStore._connect

    def tracing(self):
        conn = connect(self)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(TrackerStore, "_connect", tracing)
    tracker_store._CACHE.clear()
    return statements


def _plans(store: TrackerStore, statements: list[str]) -> dict[str, list[str]]:
    conn = sqlite3.connect(str(store.db_path))
    try:
        return {
            sql: [str(row[3]) for row in conn.execute("EXPLAIN QUERY PLAN " + sql).fetchall()]
            for sql in dict.fromkeys(statements)
            if sql.lstrip().upper().startswith("SELECT") and " items" in sql
        }
    finally:
        conn.close()


def _full_scans(plans: dict[str, list[str]]) -> dict[str, list[str]]:
    return {sql: plan for sql, plan in plans.items() if any(step.startswith(("SCAN items", "SCAN i ")) or step == "SCAN i" for step in plan)}


def test_every_tracker_read_uses_an_index(store, traced):
    for sort in ("recent", "oldest", "title"):
        for media in ("", "movie", "show"):
            store.query("history", sort=sort, media=media, limit=20)
    store.query("history", text="show 2", limit=20)
    store.query("history", until="2026-03-01T00:00:00.000Z", limit=20)
    store.query("history", attention=True)
    store.query("history", health="unchecked")
    store.months("history")
    store.months("history", media="movie", text="movie")
    store.show_summaries()
    store.show_summaries(text="show 4")
    store.history_stats()
    store.counts()
    store.health_counts()
    store.attention_counts()
    store.history_event_mode()
    store.related(show=True, tmdb="3")
    store.related(show=True, title="Show 3")
    store.related(show=False, tmdb="7", imdb="tt0000007", base="tmdb:7")
    store.item("history", "tmdb:1")
    store.checks_for("history", ["tmdb:1", "tmdb:2"])

    plans = _plans(store, traced)
    assert len(plans) > 20
    assert _full_scans(plans) == {}
    assert [sql for sql in plans if "json_extract" in sql] == []


def test_note_reads_use_their_indexes(store, traced):
    for i in range(40):
        store.save_note(f"movie:tmdb:{i}", {"type": "movie", "title": f"Movie {i}", "status": "watching" if i % 2 else "dropped", "tags": [f"t{i % 3}"]})
    traced.clear()
    store.notes(status="watching", limit=10)
    store.notes(tag="T1", limit=10)
    store.notes_for(["movie:tmdb:1", "movie:tmdb:2"])
    store.note_summary()

    conn = sqlite3.connect(str(store.db_path))
    try:
        plans = {
            sql: [str(row[3]) for row in conn.execute("EXPLAIN QUERY PLAN " + sql).fetchall()]
            for sql in dict.fromkeys(traced) if sql.lstrip().upper().startswith("SELECT")
        }
    finally:
        conn.close()
    scans = {sql: plan for sql, plan in plans.items() if any(step.startswith(("SCAN notes", "SCAN note_tags")) and "INDEX" not in step for step in plan)}
    assert scans == {}
    assert any("notes_status" in step for plan in plans.values() for step in plan)
    assert any("note_tags_tag" in step for plan in plans.values() for step in plan)


def test_list_pages_are_read_in_index_order_without_sorting(store, traced):
    for sort in ("recent", "oldest", "title"):
        for media in ("", "movie", "show"):
            store.query("history", sort=sort, media=media, limit=20)
    store.query("history", text="movie", limit=20)
    store.query("history", until="2026-03-01T00:00:00.000Z", limit=20)

    pages = {sql: plan for sql, plan in _plans(store, traced).items() if "LIMIT" in sql and "COUNT(" not in sql}
    assert len(pages) == 11
    assert {sql: plan for sql, plan in pages.items() if any("TEMP B-TREE" in step for step in plan)} == {}


def test_list_order_filters_and_month_jump(store):
    _, recent = store.query("history", limit=3)
    assert [k for k, _ in recent] == ["tmdb:49", "tmdb:19", "tmdb:39"]
    total, movies = store.query("history", media="movie", sort="oldest", limit=2)
    assert total == 61 and movies[0][0] == "tmdb:7@1700000000"
    total, shows = store.query("history", media="show", sort="title", limit=3)
    assert total == 60 and [k for k, _ in shows] == ["tmdb:1#s01e01", "tmdb:1#s01e02", "tmdb:1#s01e03"]
    assert store.query("history", text="SHOW 4", limit=1)[0] == 12
    assert all(d["watched_at"] < "2025-03" for _, d in store.query("history", until="2025-03", limit=200)[1])
    assert store.months("history", media="show")[0] == {"month": "2025-12", "count": 5}


def test_show_summaries_and_stats(store):
    total, shows = store.show_summaries(limit=2)
    assert total == 5
    assert shows[0] == {"key": "tmdb:5", "title": "Show 5", "year": 2005, "episodes": 12, "plays": 12,
                        "last_watched": "2025-12-05T20:00:00.000Z", "last_season": 1, "last_episode": 12}
    assert store.show_summaries(text="show 2")[0] == 1
    stats = store.history_stats()
    assert (stats["total"], stats["movies"], stats["episodes"]) == (121, 61, 60)
    assert stats["days"][0] == max(stats["days"])


def test_related_finds_movies_by_id_and_event_keys_and_shows_by_title(store):
    movie = store.related(show=False, tmdb="", imdb="", base="tmdb:7")
    assert [(f, k) for f, k, _ in movie] == [("history", "tmdb:7"), ("history", "tmdb:7@1700000000")]
    assert [k for _, k, _ in store.related(show=False, imdb="tt0000012")] == ["tmdb:12"]
    by_title = store.related(show=True, title="SHOW 3")
    assert [(f, k) for f, k, _ in by_title] == [("watchlist", "tmdb:3")]
    assert len(store.related(show=True, tmdb="3")) == 13


def test_sync_write_keeps_unchanged_rows_and_rewrites_only_changes(store):
    def rowids() -> dict[str, int]:
        conn = sqlite3.connect(str(store.db_path))
        try:
            return {k: r for k, r in conn.execute("SELECT item_key, rowid FROM items WHERE feature='history'")}
        finally:
            conn.close()

    before = rowids()
    items = _history()
    items["tmdb:1"] = {**items["tmdb:1"], "watched_at": "2026-11-01T00:00:00.000Z"}
    del items["tmdb:2"]
    items["tmdb:99"] = {"type": "movie", "title": "New", "ids": {"tmdb": "99"}, "watched_at": "2026-11-02T00:00:00.000Z"}
    assert store.write_state("history", items)

    after = rowids()
    assert "tmdb:2" not in after and "tmdb:99" in after
    assert {k: v for k, v in after.items() if k in before and k != "tmdb:1"} == {k: v for k, v in before.items() if k not in ("tmdb:1", "tmdb:2")}
    assert [k for k, _ in store.query("history", limit=2)[1]] == ["tmdb:99", "tmdb:1"]
    assert store.read_current("history")["items"] == items


def test_cached_aggregates_follow_every_kind_of_write(store):
    assert store.counts()["history"] == 121
    assert store.show_summaries()[0] == 5
    batch = store.change_items("history", [("tmdb:5#s01e01", None, None)], action="delete")
    assert store.counts()["history"] == 120
    assert store.show_summaries(limit=5)[1][0]["episodes"] == 11
    store.undo(batch)
    assert store.counts()["history"] == 121
    store.write_state("history", {})
    assert store.counts()["history"] == 0 and store.show_summaries() == (0, [])
    assert store.months("history") == [] and store.history_stats()["total"] == 0


def test_unchanged_sync_write_keeps_cached_aggregates(store, monkeypatch):
    def rev() -> str:
        conn = sqlite3.connect(str(store.db_path))
        try:
            return str(conn.execute("SELECT value FROM meta WHERE key='rev'").fetchone()[0])
        finally:
            conn.close()

    store.show_summaries()
    before = rev()
    assert store.write_state("history", _history())
    assert rev() == before
    monkeypatch.setattr(store, "_bump", lambda conn: pytest.fail("unchanged data must not drop the cache"))
    assert store.show_summaries()[0] == 5
    monkeypatch.undo()
    store.prune_checks()
    assert rev() == before


def test_check_runtime_feeds_watch_time_and_health(store):
    item = _history()["tmdb:1"]
    store.save_checks([("history", "tmdb:1", "ok", {"runtime": 120}, tracker_store._dumps(item))])
    stats = store.history_stats()
    assert (stats["minutes"], stats["timed"]) == (120, 1)
    assert store.health_counts()["history"]["ok"] == 1


def test_v3_database_upgrades_in_place(tmp_path):
    db = tmp_path / tracker_store.DB_NAME
    conn = sqlite3.connect(str(db))
    conn.executescript(
        "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);"
        "CREATE TABLE states(scope TEXT NOT NULL, feature TEXT NOT NULL, ts INTEGER NOT NULL, updated_ns INTEGER NOT NULL, PRIMARY KEY(scope, feature));"
        "CREATE TABLE items(scope TEXT NOT NULL, feature TEXT NOT NULL, item_key TEXT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(scope, feature, item_key));"
        "CREATE TABLE checks(scope TEXT NOT NULL, feature TEXT NOT NULL, item_key TEXT NOT NULL, code TEXT NOT NULL, detail TEXT NOT NULL,"
        " checked_ns INTEGER NOT NULL, PRIMARY KEY(scope, feature, item_key));"
        "PRAGMA user_version=3;"
    )
    for key, item in _history().items():
        conn.execute("INSERT INTO items VALUES('', 'history', ?, ?)", (key, tracker_store._dumps(item)))
    conn.execute("INSERT INTO states VALUES('', 'history', 1, 1)")
    conn.execute("INSERT INTO checks VALUES('', 'history', 'tmdb:1', 'ok', '{\"runtime\": 95}', 1)")
    conn.commit()
    conn.close()

    tracker_store._CACHE.clear()
    store = TrackerStore(tmp_path)
    assert store.query("history", media="show", limit=1)[0] == 60
    assert store.show_summaries()[0] == 5
    assert store.history_stats()["minutes"] == 95
    conn = sqlite3.connect(str(db))
    try:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == tracker_store.SCHEMA_VERSION
        assert conn.execute("SELECT COUNT(*) FROM items WHERE kind IS NULL").fetchone()[0] == 0
        assert {r[1] for r in conn.execute("PRAGMA index_list(items)")} >= {"items_when", "items_show", "items_title", "items_tmdb", "items_imdb"}
    finally:
        conn.close()
