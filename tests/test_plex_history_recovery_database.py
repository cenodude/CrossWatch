# tests/test_plex_history_recovery_database.py
# CrossWatch - Plex database history recovery tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace as NS
import sqlite3
import threading
import time
import tracemalloc
import zipfile

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from providers.sync.plex import _recovery as plex_recovery, _recovery_db as recovery
from services import plex_history_recovery as service

DAY = 1700000000


def library(path, views=(), items=(), tags=(), sections=((1, "Movies"), (2, "Shows"))):
    con = sqlite3.connect(path)
    con.executescript("""
        CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE library_sections (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE metadata_items (id INTEGER PRIMARY KEY, guid TEXT, metadata_type INTEGER, parent_id INTEGER, "index" INTEGER);
        CREATE TABLE tags (id INTEGER PRIMARY KEY, tag TEXT, tag_type INTEGER);
        CREATE TABLE taggings (id INTEGER PRIMARY KEY, metadata_item_id INTEGER, tag_id INTEGER);
        CREATE TABLE metadata_item_views (id INTEGER PRIMARY KEY, account_id INTEGER, guid TEXT, metadata_type INTEGER,
            library_section_id INTEGER, grandparent_title TEXT, parent_index INTEGER, parent_title TEXT, "index" INTEGER,
            title TEXT, thumb_url TEXT, viewed_at INTEGER, grandparent_guid TEXT, originally_available_at INTEGER,
            device_id INTEGER, view_type INTEGER);
    """)
    con.executemany("INSERT INTO accounts VALUES (?, ?)", [(1, "owner"), (7, "friend")])
    con.executemany("INSERT INTO library_sections VALUES (?, ?)", sections)
    con.executemany('INSERT INTO metadata_items (id, guid, metadata_type, parent_id, "index") VALUES (?, ?, ?, ?, ?)', items)
    for item_id, tag in tags:
        tag_id = con.execute("INSERT INTO tags (tag, tag_type) VALUES (?, 314)", (tag,)).lastrowid
        con.execute("INSERT INTO taggings (metadata_item_id, tag_id) VALUES (?, ?)", (item_id, tag_id))
    for view in views:
        row = {**dict(account_id=1, metadata_type=1, library_section_id=1, viewed_at=DAY, view_type=0), **view}
        con.execute(f"INSERT INTO metadata_item_views ({', '.join(f'[{key}]' for key in row)}) VALUES ({', '.join('?' * len(row))})", list(row.values()))
    con.commit()
    con.close()
    return Path(path)


def movie(guid, title="Movie", **kw):
    return dict(guid=guid, title=title, originally_available_at=946681200, **kw)


def episode(guid, number, show="plex://show/s", title="Series", season=1, **kw):
    return {**dict(guid=guid, metadata_type=4, library_section_id=2, grandparent_title=title, grandparent_guid=show,
                   parent_index=season, index=number, title=f"Episode {number}", viewed_at=DAY + (number or 0)), **kw}


def scan(path, known=None, **kwargs):
    asked = []
    def resolve(kind, guid):
        asked.append((kind, guid))
        return (known or {}).get(guid, {})
    options = dict(account=1, libraries=set(), progress=lambda *a: None, check_cancel=lambda: None)
    options.update(kwargs)
    return recovery.scan(path, resolve=resolve, **options), asked


def test_removed_movies_use_stored_ids_and_resolve_plex_guids_once(tmp_path):
    path = library(tmp_path / "lib.db", views=[
        movie("com.plexapp.agents.imdb://tt0000001?lang=en", "Legacy"),
        movie("plex://movie/aa", "Catalog"), movie("plex://movie/aa", "Catalog", viewed_at=DAY + 50),
        movie("plex://movie/aa", "Catalog"), movie("plex://movie/gone", "Gone"), movie("local://9", "Home video"),
        movie("plex://movie/aa/edition/Trailer", "Catalog"), movie("plex://movie/kept", "Kept"),
        movie("plex://movie/other", "Friend", account_id=7)],
        items=[(1, "plex://movie/kept", 1, None, None)])
    result, asked = scan(path, {"plex://movie/aa": {"tmdb": "22", "plex": "aa", "guid": "plex://movie/aa"}})
    assert [r["item"]["title"] for r in result] == ["Legacy", "Catalog", "Gone", "Home video", "Catalog"]
    assert [r["item"]["ids"] for r in result] == [{"imdb": "tt0000001"}, {"tmdb": "22"}, {}, {}, {"tmdb": "22"}]
    assert [r["recovery_match"] for r in result] == ["Known identity", "Matched by Plex GUID", "Missing identity — enter a match",
                                                    "Missing identity — enter a match", "Matched by Plex GUID"]
    assert [r["recovery_valid"] for r in result] == [True, True, False, False, True]
    assert asked == [("movie", "plex://movie/aa"), ("movie", "plex://movie/gone")]
    assert {r["item"]["year"] for r in result} == {2000}
    assert result[1]["item"]["watched_at"] == "2023-11-14T22:13:20Z" and result[4]["item"]["watched_at"] == "2023-11-14T22:14:10Z"
    assert all(r["source_path"] == "Plex database" and r["item"]["watched"] for r in result)


def test_removed_shows_resolve_once_and_legacy_and_hama_guids_need_no_lookup(tmp_path, monkeypatch):
    monkeypatch.setattr(recovery, "query_edges", lambda *a, **kw: [])
    path = library(tmp_path / "lib.db", views=[
        episode("plex://episode/1", 1), episode("plex://episode/2", 2), episode("plex://episode/3", 3, season=0),
        episode("com.plexapp.agents.thetvdb://500/2/4?lang=en", None, show="com.plexapp.agents.thetvdb://500?lang=en", season=None,
                viewed_at=DAY + 4),
        episode("com.plexapp.agents.hama://tvdb-81797/1/5?lang=en", 5, show="com.plexapp.agents.hama://tvdb-81797?lang=en", title="Anime"),
        episode("com.plexapp.agents.hama://anidb-69/1/1?lang=en", 1, show="com.plexapp.agents.hama://anidb-69?lang=en", title="AniDB",
                viewed_at=DAY + 6),
        episode("plex://episode/9", 9, show=None, title="No show guid")])
    result, asked = scan(path, {"plex://show/s": {"tvdb": "300", "tmdb": "301"}, "plex://episode/9": {"tvdb": "900"}})
    assert asked == [("show", "plex://show/s"), ("episode", "plex://episode/9")]
    assert [(r["item"]["season"], r["item"]["episode"]) for r in result] == [(1, 1), (1, 2), (0, 3), (2, 4), (1, 5), (1, 1), (1, 9)]
    assert [r["item"]["show_ids"] for r in result] == [{"tmdb": "301", "tvdb": "300"}] * 3 + [{"tvdb": "500"}, {"tvdb": "81797"}, {}, {"tvdb": "900"}]
    assert [r["recovery_valid"] for r in result] == [True, True, True, True, True, False, True]
    assert [r["item"]["title"] for r in result][:3] == ["S01E01", "S01E02", "S00E03"]
    assert all(r["item"]["ids"] == {} and r["item"]["year"] is None for r in result)
    assert len({r["original_group"] for r in result[:3]}) == 1


def test_titles_readded_under_a_new_guid_still_count_as_in_plex(tmp_path):
    path = library(tmp_path / "lib.db", views=[
        movie("com.plexapp.agents.imdb://tt0000001", "Rematched"), movie("plex://movie/old", "Retagged"),
        episode("plex://episode/1", 1, show="plex://show/old"), episode("plex://episode/2", 2, show="plex://show/old")],
        items=[(1, "plex://movie/new", 1, None, None), (2, "plex://show/new", 2, None, None), (3, "plex://season/new", 3, 2, 1),
               (4, "plex://episode/new", 4, 3, 1)],
        tags=[(1, "imdb://tt0000001"), (1, "tmdb://77"), (2, "tvdb://300")])
    updates = []
    result, _ = scan(path, {"plex://movie/old": {"tmdb": "77"}, "plex://show/old": {"tvdb": "300"}},
                     progress=lambda *a: updates.append(a))
    assert [(r["item"]["type"], r["item"].get("episode")) for r in result] == [("episode", 2)]
    final = updates[-1][4]
    assert (final["checked"], final["found"], final["skipped"]) == (4, 1, 3)
    events = [u[4]["event"]["result"] for u in updates if len(u) == 5 and "event" in u[4]]
    assert events.count("Skipped: still in Plex") == 3


@pytest.mark.parametrize("prefix,season,scope", [("anidb-", 2, "R"), ("", 1, "R"), ("anidb-", 0, "S")])
def test_hama_uses_episode_identity_and_explicit_numbering(tmp_path, monkeypatch, prefix, season, scope):
    calls = []
    def edges(tag, provider, ident, **kwargs):
        calls.append((tag, provider, ident, kwargs["scope"]))
        return [dict(target_provider="tvdb", target_id="900", target_kind="show", target_scope="s3",
                     source_range="1-12", target_range="13-24")]
    monkeypatch.setattr(recovery, "query_edges", edges)
    path = library(tmp_path / "lib.db", views=[
        episode(f"com.plexapp.agents.hama://{prefix}123/{season}/2?lang=en", 24, season=2,
                show="com.plexapp.agents.hama://anidb-456?lang=en")])
    result, asked = scan(path, release_tag="custom")
    item = result[0]["item"]
    assert calls == [("custom", "anidb", "123", scope)]
    assert (item["show_ids"], item["season"], item["episode"]) == ({"tvdb": "900"}, 3, 14)
    assert item["watched_at"] == "2023-11-14T22:13:44Z"
    assert result[0]["recovery_valid"] and not asked


@pytest.mark.parametrize("case", ["missing", "ambiguous", "extras", "unknown"])
def test_unmapped_hama_never_inherits_parent_or_title_matches(tmp_path, monkeypatch, case):
    rows = [] if case == "missing" else [
        dict(target_provider="tvdb", target_id=ident, target_kind="show", target_scope="s1",
             source_range="1-200", target_range="1-200") for ident in ("900", "901")]
    monkeypatch.setattr(recovery, "query_edges", lambda *a, **kw: rows)
    guid = "com.plexapp.agents.hama://" + ("unknown-123/1/2" if case == "unknown" else
                                             "anidb-123/0/101" if case == "extras" else "anidb-123/1/2")
    path = library(tmp_path / "lib.db", views=[episode(guid, 2, show="tvdb://555")])
    searches = []
    result, asked = scan(path, search=lambda item: searches.append(item) or {"tvdb": "777"})
    assert not result[0]["recovery_valid"]
    assert result[0]["item"]["show_ids"] == {}
    assert not searches and not asked


def test_hama_tvdb_uses_guid_coordinates(tmp_path):
    path = library(tmp_path / "lib.db", views=[
        episode("com.plexapp.agents.hama://tvdb-900/3/14", 2, show="tvdb://555")])
    result, _ = scan(path)
    item = result[0]["item"]
    assert (item["show_ids"], item["season"], item["episode"]) == ({"tvdb": "900"}, 3, 14)


def test_hama_matches_do_not_spread_to_other_entries_under_the_same_parent(tmp_path, monkeypatch):
    def edges(tag, provider, ident, **kwargs):
        return [dict(target_provider="tvdb", target_id="900", target_kind="show", target_scope="s2",
                     source_range="1-12", target_range="1-12")] if ident == "123" else []
    monkeypatch.setattr(recovery, "query_edges", edges)
    path = library(tmp_path / "lib.db", views=[
        episode("com.plexapp.agents.hama://anidb-123/1/1", 1, show="tvdb://555"),
        episode("com.plexapp.agents.hama://anidb-456/1/2", 2, show="tvdb://555")])
    result, _ = scan(path)
    assert [r["recovery_valid"] for r in result] == [True, False]
    assert [r["item"]["show_ids"] for r in result] == [{"tvdb": "900"}, {}]


def test_database_title_fallback_requires_review_and_keeps_watch_date(tmp_path):
    path = library(tmp_path / "lib.db", views=[movie("plex://movie/gone"), movie("imdb://tt1234567")])
    searches = []
    result, _ = scan(path, search=lambda item: searches.append(dict(item)) or {"tmdb": "123"})
    assert len(searches) == 1
    assert result[0]["item"]["ids"] == {"tmdb": "123"}
    assert result[0]["recovery_valid"] and result[0]["recovery_requires_review"]
    assert result[0]["item"]["watched_at"] == "2023-11-14T22:13:20Z"
    assert not result[1]["recovery_requires_review"]


def test_title_matching_reports_remaining_work_until_searches_finish(tmp_path):
    path = library(tmp_path / "lib.db", views=[movie("plex://movie/one", "One"),
                                               movie("plex://movie/two", "Two"), movie("imdb://tt1234567")])
    updates = []
    searches = []

    def search(item):
        stage, message, done, total, activity = updates[-1]
        assert stage == "title_matching"
        assert (done, total) == (len(searches), 2)
        assert done < total
        assert activity["checked"] == 3
        assert activity["current_item"] == item["title"]
        searches.append(item["title"])
        return {"tmdb": "123"} if item["title"] == "One" else {}

    rows, _ = scan(path, search=search, progress=lambda *args: updates.append(args))
    assert searches == ["One", "Two"]
    assert len(rows) == 3
    assert [(u[2], u[3]) for u in updates if u[0] == "title_matching"] == [(0, 2), (1, 2), (1, 2), (2, 2)]
    assert all(u[4]["current_item"] for u in updates if u[0] == "title_matching")
    assert updates[-1][4]["current_item"] == "Two"
    assert updates[-1][4]["current_action"] == "Last checked"


def test_library_scope_applies_to_existing_sections_and_keeps_deleted_libraries(tmp_path):
    path = library(tmp_path / "lib.db", views=[
        movie("imdb://tt0000001", "In scope", library_section_id=1), movie("imdb://tt0000002", "Out of scope", library_section_id=2),
        movie("imdb://tt0000003", "Deleted library", library_section_id=99), movie("imdb://tt0000004", "No library", library_section_id=None)])
    result, _ = scan(path, libraries={"1"})
    assert [r["item"]["title"] for r in result] == ["In scope", "Deleted library", "No library"]
    assert len(scan(path)[0]) == 4
    assert scan(path, account=7)[0] == []


def test_row_limit_and_cancel_never_return_partial_results(tmp_path):
    path = library(tmp_path / "lib.db", views=[movie(f"imdb://tt000000{i}", viewed_at=DAY + i) for i in range(1, 4)])
    with pytest.raises(RuntimeError, match="More than 2"):
        scan(path, max_rows=2)
    calls = []
    def cancel():
        calls.append(1)
        if len(calls) > 2:
            raise InterruptedError()
    with pytest.raises(InterruptedError):
        scan(path, check_cancel=cancel)


def test_prepare_accepts_a_database_a_plex_download_zip_and_rejects_other_files(tmp_path):
    db = library(tmp_path / "com.plexapp.plugins.library.db", views=[movie("imdb://tt0000001")])
    assert recovery.prepare(db, tmp_path) == db
    archive = tmp_path / "Plex Media Server Databases.zip"
    with zipfile.ZipFile(archive, "w") as out:
        out.writestr("notes.txt", "hello")
        out.write(db, "databaseBackup.db1234")
    extracted = recovery.prepare(archive, tmp_path / "")
    assert extracted.parent == tmp_path and extracted != db
    assert [a["name"] for a in recovery.accounts(extracted)] == ["owner"]
    other = tmp_path / "other.db"
    sqlite3.connect(other).execute("CREATE TABLE x (id INTEGER)").connection.commit()
    for path in (other, tmp_path / "notes.zip", tmp_path / "plain.txt"):
        if not path.exists():
            path.write_bytes(b"not a database")
        with pytest.raises(recovery.RecoveryError, match="No Plex library database"):
            recovery.prepare(path, tmp_path)


def test_episodes_repaired_by_a_later_lookup_still_skip_titles_back_in_plex(tmp_path):
    path = library(tmp_path / "lib.db", views=[episode(f"plex://episode/{n}", n, show="plex://show/old") for n in (1, 2, 3)],
                   items=[(2, "plex://show/new", 2, None, None), (3, "plex://season/new", 3, 2, 1), (4, "plex://episode/new", 4, 3, 1)],
                   tags=[(2, "tvdb://300")])
    result, asked = scan(path, {"plex://episode/2": {"tvdb": "300"}})
    assert asked == [("show", "plex://show/old"), ("episode", "plex://episode/1"), ("episode", "plex://episode/2")]
    assert [r["item"]["episode"] for r in result] == [2, 3]
    assert all(r["recovery_valid"] and r["item"]["show_ids"] == {"tvdb": "300"} for r in result)


def test_account_listing_ignores_duplicate_account_rows_and_slow_queries_stop(tmp_path):
    path = library(tmp_path / "lib.db", views=[movie(f"imdb://tt{i:07d}", viewed_at=DAY + i) for i in range(1, 301)])
    con = sqlite3.connect(path)
    con.executescript("DROP TABLE accounts; CREATE TABLE accounts (id INTEGER, name TEXT);")
    con.executemany("INSERT INTO accounts VALUES (1, ?)", [(f"owner{i:03d}",) for i in range(300)])
    con.commit(); con.close()
    assert recovery.accounts(path) == [{"id": 1, "name": "owner000", "plays": 300}]
    endless = "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n LIMIT 100000000) SELECT COUNT(*) FROM n"
    with pytest.raises(recovery.RecoveryError, match="too long"):
        with recovery._reading(path, -1) as (con, clock), clock:
            con.execute(endless).fetchone()
    def cancelled():
        raise InterruptedError()
    with pytest.raises(InterruptedError):
        with recovery._reading(path, 60, cancelled) as (con, clock), clock:
            con.execute(endless).fetchone()


def test_history_is_read_in_capped_batches_and_stops_at_the_event_limit(tmp_path, monkeypatch):
    huge = "x" * 65536
    path = library(tmp_path / "lib.db", views=[movie(f"imdb://tt{i:07d}", huge, viewed_at=DAY + i, parent_index=huge) for i in range(1, 501)])
    monkeypatch.setattr(recovery, "BATCH_ROWS", 20)
    tracemalloc.start()
    try:
        with pytest.raises(RuntimeError, match="More than 1"):
            scan(path, max_rows=1)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < 4 * 1024 * 1024
    result, _ = scan(path)
    assert len(result) == 500 and {len(r["item"]["title"]) for r in result} == {recovery.MAX_TEXT}


def test_oversized_view_ids_stay_out_of_python_and_keep_the_first_duplicate(tmp_path, monkeypatch):
    path = library(tmp_path / "lib.db")
    con = sqlite3.connect(path)
    con.executescript("DROP TABLE metadata_item_views; CREATE TABLE metadata_item_views (id TEXT, account_id INTEGER, guid TEXT, "
                      "metadata_type INTEGER, title TEXT, viewed_at INTEGER);")
    con.executemany("INSERT INTO metadata_item_views VALUES (?, 1, ?, 1, 'Movie', ?)",
                    [(f"{i:05d}" + "x" * 65536, f"imdb://tt{i:07d}", DAY + i) for i in range(1, 501)])
    con.executemany("INSERT INTO metadata_item_views VALUES (?, 1, 'imdb://tt9999999', 1, ?, ?)",
                    [("2", "Later copy", DAY), ("1", "First copy", DAY)])
    con.commit(); con.close()
    monkeypatch.setattr(recovery, "BATCH_ROWS", 20)
    tracemalloc.start()
    try:
        with pytest.raises(RuntimeError, match="More than 1"):
            scan(path, max_rows=1)
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < 4 * 1024 * 1024
    result, _ = scan(path)
    assert len(result) == 501 and result[0]["item"]["title"] == "First copy"


def test_library_token_cap_refuses_oversized_libraries(tmp_path, monkeypatch):
    path = library(tmp_path / "lib.db", views=[movie("imdb://tt0000001")],
                   items=[(1, "plex://movie/a", 1, None, None), (2, "plex://movie/b", 1, None, None)],
                   tags=[(1, "imdb://tt0000002"), (2, "imdb://tt0000003")])
    monkeypatch.setattr(recovery, "MAX_LIBRARY_TOKENS", 1)
    with pytest.raises(recovery.RecoveryError, match="too large to check"):
        scan(path)


def test_sql_clock_counts_only_time_spent_reading():
    clock = recovery._Clock(0.2)
    with clock:
        time.sleep(0.05)
    time.sleep(0.3)
    assert not clock.expired()
    with clock:
        time.sleep(0.2)
        assert clock.expired()
    assert clock.expired()


def test_database_errors_show_only_messages_written_for_users():
    job = service.Job("owner", "default", "default", "hash", database="server")
    assert "owner access is required" in service._scan_error(job, recovery.RecoveryError("Plex server owner access is required to download the database."))
    assert "rejected the account token" in service._scan_error(job, PermissionError("Plex rejected the account token."))
    for error in (PermissionError(13, "Permission denied", r"C:\secret\db-1"), ValueError(r"bad C:\secret"), OSError(r"C:\secret")):
        message = service._scan_error(job, error)
        assert "secret" not in message and "Permission denied" not in message and "Nothing was imported" in message


def test_download_streams_the_server_database_and_reports_owner_access(tmp_path, monkeypatch):
    db = library(tmp_path / "source.db", views=[movie("imdb://tt0000001")])
    archive = tmp_path / "download.zip"
    with zipfile.ZipFile(archive, "w") as out:
        out.write(db, "databaseBackup.db1")
    payload = archive.read_bytes()
    seen = {}

    class Response:
        def __init__(self, status):
            self.status_code, self.headers = status, {"Content-Length": str(len(payload))}
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def raise_for_status(self): pass
        def iter_content(self, size): yield from (payload[i:i + size] for i in range(0, len(payload), size))

    status = NS(code=200)
    def get(url, **kw):
        seen.update(url=url, **kw)
        return Response(status.code)
    server = NS(_session=NS(get=get))
    monkeypatch.setattr(recovery.common, "_as_base_url", lambda srv: "http://plex:32400")
    monkeypatch.setattr(recovery.common, "active_pms_token", lambda srv: "pms")
    folder = tmp_path / "work"; folder.mkdir()
    path = recovery.download(NS(client=NS(server=server)), folder, progress=lambda *a: None, check_cancel=lambda: None)
    assert seen["url"] == "http://plex:32400/diagnostics/databases" and seen["stream"] is True
    assert seen["headers"]["X-Plex-Token"] == "pms"
    assert [p.name for p in folder.iterdir()] == [path.name] and recovery.accounts(path)[0]["plays"] == 1
    status.code = 401
    with pytest.raises(recovery.RecoveryError, match="owner access"):
        recovery.download(NS(client=NS(server=server)), folder, progress=lambda *a: None, check_cancel=lambda: None)
    with pytest.raises(RuntimeError, match="Connect a Plex server"):
        recovery.download(NS(client=NS(server=None)), folder, progress=lambda *a: None, check_cancel=lambda: None)


@pytest.fixture
def api(monkeypatch, config_base, tmp_path):
    from api import syncAPI
    from services import importer
    cfg = {"plex": {"account_token": "token", "server_url": "http://plex"},
           "crosswatch": {"connected": True, "root_dir": str(config_base / "tracker")}}
    state = NS(cfg=cfg)
    monkeypatch.setattr(service, "load_config", lambda: deepcopy(state.cfg))
    monkeypatch.setattr(importer, "load_config", lambda: deepcopy(state.cfg))
    monkeypatch.setattr(service, "request_user", lambda _: None)
    monkeypatch.setattr(importer, "request_user", lambda _: None)
    monkeypatch.setattr(service, "user_can_access_instance", lambda *a: True)
    monkeypatch.setattr(importer, "user_can_access_instance", lambda *a: True)
    monkeypatch.setattr(service, "DATABASE_FOLDER", tmp_path / "recovery")
    monkeypatch.setattr(syncAPI, "_is_sync_running", lambda: False)
    monkeypatch.setattr(plex_recovery, "guid_resolver", lambda token: lambda kind, guid: {"tmdb": "55"} if guid == "plex://movie/aa" else {})
    rt = ({}, {}, threading.Lock())
    monkeypatch.setattr(syncAPI, "_rt", lambda: rt)
    service.JOBS.clear(); service.UPLOADS.clear()
    app = FastAPI(); app.include_router(service.router)
    state.client, state.rt, state.tmp = TestClient(app), rt, tmp_path
    yield state
    service.JOBS.clear(); service.UPLOADS.clear()


def run(api, payload):
    response = api.client.post("/api/import/plex-recovery", json=payload)
    if response.status_code == 200:
        thread = api.rt[1].get("SYNC")
        if thread is not None:
            thread.join(5)
    return response


def upload(api, path, name="com.plexapp.plugins.library.db"):
    with open(path, "rb") as handle:
        return api.client.post("/api/import/plex-recovery/database", files={"file": (name, handle)})


def test_uploaded_database_scans_the_chosen_account_and_is_deleted_afterwards(api):
    db = library(api.tmp / "lib.db", views=[movie("plex://movie/aa", "Old"), movie("imdb://tt0000002", "Friend", account_id=7)])
    response = upload(api, db)
    assert response.status_code == 200, response.text
    body = response.json()
    assert [(a["id"], a["name"], a["plays"]) for a in body["accounts"]] == [(1, "owner", 1), (7, "friend", 1)]
    folder = Path(service.UPLOADS[body["upload_id"]]["folder"])
    assert folder.is_dir()
    wrong = run(api, dict(source="plex_db", upload_id=body["upload_id"], db_account=3))
    assert wrong.status_code == 400 and "Choose the Plex account" in wrong.json()["detail"]
    response = run(api, dict(source="plex_db", upload_id=body["upload_id"], db_account=7))
    assert response.status_code == 200, response.text
    url = "/api/import/plex-recovery/" + response.json()["id"]
    job = api.client.get(url).json()
    assert (job["status"], job["source"], job["database"]) == ("review", "plex_db", "upload")
    rows = api.client.get(url + "/rows").json()
    assert [r["title"] for r in rows["rows"]] == ["Friend"] and rows["summary"]["ready"] == 1
    assert not folder.exists() and not service.UPLOADS
    again = run(api, dict(source="plex_db", upload_id=body["upload_id"], db_account=7))
    assert again.status_code == 404 and "expired" in again.json()["detail"]


def test_bad_uploads_are_rejected_and_a_new_upload_replaces_the_previous_one(api):
    junk = api.tmp / "junk.db"; junk.write_bytes(b"x" * 100)
    response = upload(api, junk)
    assert response.status_code == 400 and "No Plex library database" in response.json()["detail"]
    assert not any((api.tmp / "recovery").iterdir())
    db = library(api.tmp / "lib.db", views=[movie("imdb://tt0000001")])
    first, second = upload(api, db).json(), upload(api, db).json()
    assert list(service.UPLOADS) == [second["upload_id"]]
    assert len(list((api.tmp / "recovery").iterdir())) == 1
    assert api.client.delete(f"/api/import/plex-recovery/database/{second['upload_id']}").json() == {"ok": True}
    assert not service.UPLOADS and not any((api.tmp / "recovery").iterdir())
    assert first["upload_id"] != second["upload_id"]


def test_server_database_uses_the_connected_account_and_history_scope(api, monkeypatch):
    from providers.sync import _mod_PLEX
    db = library(api.tmp / "server.db", views=[movie("plex://movie/aa", "Mine", library_section_id=1),
                                               movie("imdb://tt0000002", "Out of scope", library_section_id=2),
                                               movie("imdb://tt0000003", "Friend", account_id=7)])
    seen = {}
    class Module:
        def __init__(self, cfg):
            self.client = NS(server=NS(), user_account_id=1, session=None)
            self.config = {"plex": {"history": {"libraries": ["1"]}}}
            self.cfg = cfg
    def download(adapter, folder, *, progress, check_cancel):
        seen["folder"] = folder
        target = Path(folder) / "db.db"
        target.write_bytes(db.read_bytes())
        return target
    monkeypatch.setattr(_mod_PLEX, "PLEXModule", Module)
    monkeypatch.setattr(recovery, "download", download)
    monkeypatch.setattr(recovery.common, "plex_feature_library_ids", lambda adapter, feature: {"1"})
    monkeypatch.setattr(recovery.common, "plex_cfg_get", lambda adapter, key, default=None: default)
    response = run(api, dict(source="plex_db"))
    assert response.status_code == 200, response.text
    url = "/api/import/plex-recovery/" + response.json()["id"]
    job = api.client.get(url).json()
    assert (job["status"], job["database"]) == ("review", "server"), job["message"]
    rows = api.client.get(url + "/rows").json()["rows"]
    assert [(r["title"], r["ids"]) for r in rows] == [("Mine", {"tmdb": "55"})]
    assert not Path(seen["folder"]).exists()


def test_database_source_needs_a_plex_account_and_reports_owner_access(api, monkeypatch):
    from providers.sync import _mod_PLEX
    api.cfg = {**api.cfg, "plex": {"server_url": "http://plex"}}
    response = run(api, dict(source="plex_db"))
    assert response.status_code == 409 and "Connect a Plex account" in response.json()["detail"]
    api.cfg = {**api.cfg, "plex": {"account_token": "token", "server_url": "http://plex"}}
    monkeypatch.setattr(_mod_PLEX, "PLEXModule", lambda cfg: NS(client=NS(server=NS(), session=None)))
    def denied(adapter, folder, **kw):
        raise recovery.RecoveryError("Plex server owner access is required to download the database.")
    monkeypatch.setattr(recovery, "download", denied)
    job = api.client.get("/api/import/plex-recovery/" + run(api, dict(source="plex_db")).json()["id"]).json()
    assert job["status"] == "error" and "owner access is required" in job["message"]
    assert not any((api.tmp / "recovery").iterdir()) and "SYNC" not in api.rt[1]
