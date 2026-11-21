# tests/test_event_titles.py
# CrossWatch - Events Titles view rows, capped success records and review state
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time

import pytest

from cw_platform.event_archive import titles
from cw_platform.event_archive.db import connect
from cw_platform.event_archive.groups import correlate
from cw_platform.event_archive.recorder import RunRecorder, make_event, record_events
from cw_platform.event_archive.retention import apply_retention
from cw_platform.orchestrator import _applier


def _movie(n: int) -> dict:
    return {"type": "movie", "title": f"Movie {n}", "year": 2000 + n % 20, "ids": {"tmdb": str(1000 + n), "imdb": f"tt{n:07d}"}}


def _episode() -> dict:
    return {"type": "episode", "series_title": "The Bear", "title": "Violet", "season": 3, "episode": 4,
            "ids": {"tvdb": "777"}, "show_ids": {"tmdb": "136315", "imdb": "tt14452776"}}


@pytest.fixture
def conn(monkeypatch):
    from cw_platform.event_archive import groups, recorder, scrobble_recorder

    c = connect(":memory:")
    for module in (titles, groups, recorder, scrobble_recorder):
        monkeypatch.setattr(module, "get_conn", lambda: c)
    monkeypatch.setattr(titles, "start_backfill", lambda: pytest.fail("backfill must not start"))
    yield c
    c.close()


def _write(recorder, items, *, dst="TRAKT", feature="history", op="add", dry_run=False, keys=None):
    from cw_platform.id_map import canonical_key

    confirmed = keys if keys is not None else [canonical_key(i) for i in items]
    _applier._emit_archive_successes(
        recorder.emit, dst_name=dst, cfg={}, feature=feature, op=op, items=items,
        res={"confirmed_keys": confirmed, "confirmed": len(confirmed)}, dry_run=dry_run,
    )


def test_successes_become_one_title_row_per_run(conn):
    rec = RunRecorder(lambda *a, **kw: None, run_id="run-1", conn=conn)
    rec.emit("feature:start", src="PLEX", dst="TRAKT", feature="history")
    _write(rec, [_movie(1), _episode()], dst="TRAKT")
    _write(rec, [_movie(1), _episode()], dst="SIMKL")
    _write(rec, [_movie(2)], dst="MDBLIST", dry_run=True)
    rec.close()

    page = titles.list_titles(domain="sync", conn=conn)
    assert page["total"] == 2
    assert page["preparing"] is False
    rows = {r["title"]: r for r in page["items"]}
    assert set(rows) == {"Movie 1", "The Bear"}
    assert [(d["provider"], d["state"]) for d in rows["Movie 1"]["destinations"]] == [("SIMKL", "ok"), ("TRAKT", "ok")]
    assert rows["Movie 1"]["status"] == "completed"
    assert rows["Movie 1"]["kinds"] == ["history:add"]
    assert rows["Movie 1"]["source_provider"] == "PLEX"
    assert rows["The Bear"]["ids"] == {"tvdb": "777", "show_tmdb": "136315", "show_imdb": "tt14452776"}
    assert (rows["The Bear"]["season"], rows["The Bear"]["episode"]) == (3, 4)
    assert conn.execute("SELECT COUNT(*) FROM events WHERE event_type='write_succeeded'").fetchone()[0] == 0


def test_history_event_keys_match_their_items(conn):
    from cw_platform.id_map import canonical_key

    rec = RunRecorder(lambda *a, **kw: None, run_id="run-1", conn=conn)
    rec.emit("feature:start", src="PLEX", dst="TRAKT", feature="history")
    item = _movie(3)
    _write(rec, [item], keys=[f"{canonical_key(item)}@1700000000"])
    rec.close()
    assert titles.list_titles(domain="sync", conn=conn)["total"] == 1


def test_success_records_are_capped_per_destination(conn):
    items = [_movie(n) for n in range(titles.SUCCESS_CAP + 40)]
    rec = RunRecorder(lambda *a, **kw: None, run_id="run-big", conn=conn)
    rec.emit("feature:start", src="PLEX", dst="TRAKT", feature="history")
    _write(rec, items)
    rec.close()
    assert titles.list_titles(domain="sync", conn=conn)["total"] == titles.SUCCESS_CAP


def test_failure_merges_into_row_and_review_clears_attention(conn):
    rec = RunRecorder(lambda *a, **kw: None, run_id="run-1", conn=conn)
    rec.emit("feature:start", src="PLEX", dst="TRAKT", feature="history")
    item = _movie(5)
    _write(rec, [item], dst="TRAKT")
    rec.emit("archive:item_failures", provider="MDBLIST", feature="history", op="add",
             items=[{"key": "tmdb:1005", "item": item, "reason": "http_429"}])
    rec.close()

    attention = titles.list_titles(domain="sync", attention=True, conn=conn)
    assert attention["total"] == 1
    row = attention["items"][0]
    assert row["status"] == "failed"
    assert {(d["provider"], d["state"]) for d in row["destinations"]} == {("MDBLIST", "fail"), ("TRAKT", "ok")}
    assert titles.list_titles(domain="sync", destination="mdblist", conn=conn)["total"] == 1
    assert titles.list_titles(domain="sync", outcome="failed", conn=conn)["total"] == 1
    assert titles.list_titles(domain="sync", what="ratings", conn=conn)["total"] == 0

    assert titles.acknowledge(row["id"], by="tester", conn=conn)["attention"] == 0
    assert titles.list_titles(domain="sync", attention=True, conn=conn)["total"] == 0
    assert titles.list_titles(domain="sync", review="reviewed", conn=conn)["total"] == 1
    assert titles.acknowledge(row["id"], undo=True, conn=conn)["attention"] == 1


def test_scrobble_activity_is_one_row_with_destination_outcomes(conn):
    from cw_platform.event_archive import scrobble_recorder

    now = int(time.time())
    for dst, event_type in (("TRAKT", "scrobble_completed"), ("SIMKL", "scrobble_failed")):
        scrobble_recorder.record(event_type=event_type, source_provider="PLEX", destination_provider=dst,
                                 ids={"imdb": "tt9", "tmdb": "5"}, media_type="movie", title="Heat", year=1995,
                                 progress=98, session_key="s1", created_at=now)
    page = titles.list_titles(domain="scrobble", conn=conn)
    assert page["total"] == 1
    row = page["items"][0]
    assert row["kinds"] == ["scrobble:watch"]
    assert row["ids"] == {"tmdb": "5", "imdb": "tt9"}
    assert {(d["provider"], d["state"]) for d in row["destinations"]} == {("SIMKL", "fail"), ("TRAKT", "ok")}


def test_profile_scope_hides_other_destinations(conn):
    rec = RunRecorder(lambda *a, **kw: None, run_id="run-1", conn=conn)
    rec.emit("feature:start", src="PLEX", dst="TRAKT", feature="history")
    _write(rec, [_movie(7)], dst="TRAKT")
    _write(rec, [_movie(7)], dst="SIMKL")
    _write(rec, [_movie(8)], dst="SIMKL")
    rec.close()
    only_trakt = lambda row: row.get("destination_provider") in (None, "TRAKT")
    page = titles.list_titles(domain="sync", row_filter=only_trakt, conn=conn)
    assert page["total"] == 1
    assert [d["provider"] for d in page["items"][0]["destinations"]] == ["TRAKT"]


def test_old_events_are_backfilled_and_retention_drops_old_rows(conn):
    old = int(time.time()) - 400 * 86400
    record_events([make_event(hash_extra="x", event_type="write_failed", operation="add", feature="history",
                              run_id="old", source_provider="PLEX", destination_provider="TRAKT",
                              item_key="imdb:tt1", title="Old", created_at=old)], conn=conn)
    correlate(conn=conn)
    with conn:
        conn.execute("UPDATE events SET title_id=NULL")
        conn.execute("DELETE FROM event_titles")
        conn.execute("DELETE FROM local_meta WHERE key=?", (titles.TITLES_META,))
    assert titles.backfill_pending(conn)
    assert titles.backfill(conn) == 1
    assert not titles.backfill_pending(conn)
    assert conn.execute("SELECT COUNT(*) FROM event_titles").fetchone()[0] == 1

    titles.record_successes(conn, run_id="old-ok", feature="history", op="add", source=("PLEX", "default"),
                            destination=("SIMKL", "default"), items=[{"key": "imdb:tt2", "title": "Gone"}], at=old)
    with conn:
        conn.execute("UPDATE event_groups SET acknowledged_at=1")
    apply_retention(conn=conn)
    assert conn.execute("SELECT COUNT(*) FROM event_titles").fetchone()[0] == 0


def test_cover_lookup_rejects_unsafe_ids(monkeypatch):
    from fastapi.testclient import TestClient
    from fastapi import FastAPI
    from api import metaAPI

    calls = []
    monkeypatch.setattr(metaAPI, "load_config", lambda: {"tmdb": {"api_key": "k"}})
    monkeypatch.setattr(metaAPI, "_lookup_tmdb_id", lambda *a: calls.append(a) or 42)
    app = FastAPI()
    app.include_router(metaAPI.router)
    client = TestClient(app)
    assert client.get("/art/lookup/movie?imdb=../../etc").status_code == 404
    assert calls == []
    res = client.get("/art/lookup/show?imdb=tt0000001&size=w185", follow_redirects=False)
    assert res.status_code == 307
    assert res.headers["location"] == "/art/tmdb/tv/42?fallback=2&size=w185"


def test_scoped_review_only_touches_visible_destinations(conn):
    rec = RunRecorder(lambda *a, **kw: None, run_id="run-1", conn=conn)
    rec.emit("feature:start", src="PLEX", dst="TRAKT", feature="history")
    item = _movie(9)
    for provider in ("MDBLIST", "SIMKL"):
        rec.emit("archive:item_failures", provider=provider, feature="history", op="add",
                 items=[{"key": "tmdb:1009", "item": item, "reason": "http_500"}])
    rec.close()
    row = titles.list_titles(domain="sync", conn=conn)["items"][0]
    visible = [d for d in row["destinations"] if d["provider"] == "MDBLIST"]
    titles.acknowledge(row["id"], by="user", destinations=visible, conn=conn)
    acked = dict(conn.execute("SELECT destination_provider, acknowledged_at IS NOT NULL FROM event_groups WHERE destination_provider IS NOT NULL").fetchall())
    assert acked == {"MDBLIST": 1, "SIMKL": 0}
    assert titles.list_titles(domain="sync", attention=True, conn=conn)["total"] == 1


def test_success_records_stay_out_of_the_sync_log():
    from api.syncAPI import _slim_sync_log_obj

    assert _slim_sync_log_obj({"event": "archive:item_successes", "items": [{"key": "tmdb:1"}] * 3}) == {}


def test_repeated_failures_need_attention_once(conn):
    item = _movie(11)
    for run in ("run-1", "run-2"):
        rec = RunRecorder(lambda *a, **kw: None, run_id=run, conn=conn)
        rec.emit("feature:start", src="PLEX", dst="TRAKT", feature="history")
        rec.emit("archive:item_failures", provider="TRAKT", feature="history", op="add",
                 items=[{"key": "tmdb:1011", "item": item, "reason": "not_found"}])
        rec.close()
        time.sleep(1.1)
    assert titles.list_titles(domain="sync", outcome="failed", conn=conn)["total"] == 2
    attention = titles.list_titles(domain="sync", attention=True, conn=conn)
    assert attention["total"] == 1
    assert attention["items"][0]["run_id"] == "run-2"


def test_mark_all_reviewed_follows_filters_and_scope(conn):
    rec = RunRecorder(lambda *a, **kw: None, run_id="run-1", conn=conn)
    rec.emit("feature:start", src="PLEX", dst="TRAKT", feature="history")
    for n, provider in ((21, "MDBLIST"), (22, "SIMKL"), (23, "MDBLIST")):
        rec.emit("archive:item_failures", provider=provider, feature="history", op="add",
                 items=[{"key": f"tmdb:{1000 + n}", "item": _movie(n), "reason": "http_500"}])
    rec.close()
    assert titles.list_titles(domain="sync", attention=True, conn=conn)["total"] == 3
    only_simkl = lambda row: row.get("destination_provider") in (None, "SIMKL")
    res = titles.acknowledge_matching(domain="sync", destination="mdblist", row_filter=only_simkl, by="user", conn=conn)
    assert res == {"ok": True, "titles": 0, "groups": 0}
    res = titles.acknowledge_matching(domain="sync", destination="mdblist", by="admin", conn=conn)
    assert res["titles"] == 2
    left = titles.list_titles(domain="sync", attention=True, conn=conn)
    assert [d["provider"] for d in left["items"][0]["destinations"]] == ["SIMKL"]
    assert left["total"] == 1
