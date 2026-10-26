# tests/test_tracearr_history_recovery.py
# CrossWatch - Tracearr history recovery tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from copy import deepcopy
from types import SimpleNamespace as NS
import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from providers.sync.tracearr import _recovery as recovery
from services import plex_history_recovery as service

OWNER = {"id": "u1", "username": "Owner"}
FRIEND = {"id": "u2", "username": "Friend"}


def movie(rid, user=OWNER, **kwargs):
    return {**dict(id=rid, media_type="movie", server_type="plex", media_title=f"Movie {rid}", year=2000,
                   imdb_id=f"tt000000{rid}", tmdb_id=int(rid), tvdb_id=None, library_id="1", watched=True,
                   started_at="2026-09-20T16:00:00.000Z", stopped_at="2026-09-20T18:00:00.000Z", user=user), **kwargs}


def episode(rid, number, show="s1", user=OWNER, **kwargs):
    return {**dict(id=rid, media_type="episode", server_type="plex", media_title=f"Episode {number}", show_title="Series",
                   year=2026, season_number=1, episode_number=number, show_media_id=show, imdb_id="tt9", tvdb_id=99,
                   library_id="3", watched=True, started_at="2026-09-21T16:00:00.000Z",
                   stopped_at=f"2026-09-21T16:{number:02d}:00.000Z", user=user), **kwargs}


class Tracearr:
    def __init__(self, rows, shows=None, settings=None, page=2):
        self.rows, self.shows, self.page, self.calls = rows, shows or {}, page, []
        self.cfg = {"tracearr": {"history": {"user_id": "u1", **(settings or {})}}}
        self.client = NS(get=self.get, show_ids=self.show_ids)

    def get(self, path, **params):
        self.calls.append((path, params))
        start = int(params.get("cursor") or 0)
        rows = [r for r in self.rows if not params.get("user_id") or r["user"]["id"] == params["user_id"]]
        page = rows[start:start + self.page]
        return {"data": page, "meta": {"nextCursor": str(start + self.page) if len(page) == self.page else None}}

    def show_ids(self, show_media_id):
        return dict(self.shows.get(show_media_id, {}))


def scan(source, **kwargs):
    return recovery.scan(source, progress=lambda *a: None, check_cancel=lambda: None, **kwargs)


def test_rows_keep_tracearr_ids_and_show_ids_come_from_the_show_lookup():
    source = Tracearr([movie("1"), episode("2", 1), episode("3", 2), episode("4", 3, show="gone")],
                      shows={"s1": {"tvdb": "300", "tmdb": "301", "imdb": "tt300"}})
    result = scan(source)
    assert [r["item"]["ids"] for r in result] == [{"imdb": "tt0000001", "tmdb": "1"}, {}, {}, {}]
    assert [r["item"].get("show_ids") for r in result][1:] == [{"tmdb": "301", "imdb": "tt300", "tvdb": "300"}] * 2 + [{}]
    assert [r["recovery_valid"] for r in result] == [True, True, True, False]
    assert [r["item"]["title"] for r in result] == ["Movie 1", "S01E01", "S01E02", "S01E03"]
    assert result[0]["item"]["watched_at"] == "2026-09-20T18:00:00Z" and result[0]["item"]["year"] == 2000
    assert all(r["item"]["year"] is None for r in result[1:]) and len({r["original_group"] for r in result[1:3]}) == 1
    assert all(r["source_path"] == "Tracearr history" and r["item"]["watched"] for r in result)
    assert [params.get("cursor") for path, params in source.calls] == [None, "2", "4"]


def test_only_the_chosen_users_watched_plex_plays_are_recovered():
    rows = [movie("1"), movie("2", user=FRIEND), movie("3", server_type="jellyfin"),
            movie("4", watched=False, percent_complete=40), dict(movie("5"), media_type="track")]
    source = Tracearr(rows)
    assert [r["item"]["title"] for r in scan(source)] == ["Movie 1"]
    assert all(params["user_id"] == "u1" for _, params in source.calls)
    friend = Tracearr(rows, settings={"user_id": "u2"})
    assert [r["item"]["title"] for r in scan(friend)] == ["Movie 2"]
    unfiltered = Tracearr(rows)
    unfiltered.get = unfiltered.client.get = lambda path, **params: {"data": rows, "meta": {}}
    assert [r["item"]["title"] for r in scan(unfiltered)] == ["Movie 1"]
    with pytest.raises(ValueError):
        scan(Tracearr([movie("1")], settings={"user_id": ""}))


def test_plays_of_items_no_longer_in_a_library_are_kept():
    source = Tracearr([movie("1", library_id=None), episode("2", 1, library_id="")], shows={"s1": {"tvdb": "300"}})
    assert [r["recovery_valid"] for r in scan(source)] == [True, True]


@pytest.mark.parametrize("count", [2, 4])
def test_a_full_last_page_ends_with_an_empty_page(count):
    source = Tracearr([movie(str(i)) for i in range(1, count + 1)])
    assert len(scan(source)) == count
    assert source.calls[-1][1]["cursor"] == str(count)


@pytest.mark.parametrize("payload", [None, {"data": "bad"}])
def test_unreadable_history_fails_instead_of_returning_a_partial_scan(payload):
    source = Tracearr([movie("1")])
    source.client.get = lambda path, **params: payload
    with pytest.raises(RuntimeError):
        scan(source)


def test_repeated_cursor_and_row_limit_fail():
    source = Tracearr([movie(str(i)) for i in range(1, 6)])
    source.client.get = lambda path, **params: {"data": [movie("1")], "meta": {"nextCursor": "same"}}
    with pytest.raises(RuntimeError, match="repeated_cursor"):
        scan(source)
    with pytest.raises(RuntimeError, match="More than 2"):
        scan(Tracearr([movie(str(i)) for i in range(1, 6)]), max_rows=2)


@pytest.fixture
def api(monkeypatch, config_base):
    from api import syncAPI
    from services import importer
    cfg = {"plex": {"account_token": "token", "username": "Owner"},
           "tracearr": {"server_url": "http://tracearr", "api_key": "key", "history": {"user_id": "u1"}},
           "crosswatch": {"connected": True, "root_dir": str(config_base / "tracker")}}
    state = NS(cfg=cfg)
    monkeypatch.setattr(service, "load_config", lambda: deepcopy(state.cfg))
    monkeypatch.setattr(importer, "load_config", lambda: deepcopy(state.cfg))
    monkeypatch.setattr(service, "request_user", lambda _: None)
    monkeypatch.setattr(importer, "request_user", lambda _: None)
    monkeypatch.setattr(service, "user_can_access_instance", lambda *a: True)
    monkeypatch.setattr(importer, "user_can_access_instance", lambda *a: True)
    monkeypatch.setattr(syncAPI, "_is_sync_running", lambda: False)
    rt = ({}, {}, threading.Lock())
    monkeypatch.setattr(syncAPI, "_rt", lambda: rt)
    service.JOBS.clear()
    app = FastAPI(); app.include_router(service.router)
    state.client, state.rt = TestClient(app), rt
    yield state
    service.JOBS.clear()


def run(api, payload):
    response = api.client.post("/api/import/plex-recovery", json=payload)
    if response.status_code == 200:
        thread = api.rt[1].get("SYNC")
        if thread is not None:
            thread.join(5)
    return response


def test_tracearr_source_scans_into_the_shared_review_flow(api, monkeypatch):
    seen = NS(adapter=None)
    def read(adapter, *, progress, check_cancel, max_rows):
        seen.adapter = adapter
        return [dict(feature="history", item=dict(type="movie", title="Old", year=2000, ids={"tmdb": "9"},
                                                  watched_at="2026-09-20T12:00:00Z", watched=True),
                     source_path="Tracearr history", recovery_match="Known identity", recovery_valid=True,
                     recovery_requires_review=False, original_group="g", original_year=2000, original_title="Old")]
    monkeypatch.setattr(recovery, "scan", read)
    options = api.client.get("/api/import/plex-recovery/options").json()
    assert [t["id"] for t in options["tracearr"]] == ["default"]
    response = run(api, dict(source="tracearr"))
    assert response.status_code == 200, response.text
    url = "/api/import/plex-recovery/" + response.json()["id"]
    job = api.client.get(url).json()
    assert (job["status"], job["source"], job["tracearr_instance"]) == ("review", "tracearr", "default")
    assert seen.adapter.cfg["tracearr"]["api_key"] == "key" and seen.adapter.cfg["tracearr"]["history"]["user_id"] == "u1"
    rows = api.client.get(url + "/rows").json()
    assert rows["summary"]["ready"] == 1 and rows["rows"][0]["recovery_match"] == "Known identity"
    api.cfg = {**api.cfg, "tracearr": {"server_url": "http://other", "api_key": "key", "history": {"user_id": "u1"}}}
    changed = api.client.get(url + "/match-groups", params={"revision": job["revision"]})
    assert changed.status_code == 409 and "settings changed" in changed.json()["detail"]


def test_tracearr_source_needs_a_connection_and_a_known_user(api):
    api.cfg = {**api.cfg, "tracearr": {}}
    assert "Connect Tracearr" in run(api, dict(source="tracearr")).json()["detail"]
    api.cfg = {**api.cfg, "tracearr": {"server_url": "http://tracearr", "api_key": "key"}}
    assert "Choose your Tracearr user" in run(api, dict(source="tracearr")).json()["detail"]
    assert api.cfg["plex"]["username"] == "Owner"
    api.cfg = {**api.cfg, "tracearr": {"server_url": "http://tracearr", "api_key": "key", "history": {"user_id": "u1"}}}
    assert run(api, dict(source="tracearr", tracearr_instance="default")).status_code == 200
    assert not api.client.get("/api/import/plex-recovery/options").json()["tautulli"]
