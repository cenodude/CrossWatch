# tests/test_tautulli_history_recovery.py
# CrossWatch - Tautulli history recovery tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from copy import deepcopy
from types import SimpleNamespace as NS
import threading

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from providers.sync.plex import _common as common, _recovery as plex_recovery
from providers.sync.tautulli import _recovery as recovery
from services import plex_history_recovery as service


def movie(rk, guid, **kwargs):
    return dict(media_type="movie", rating_key=rk, guid=guid, title=f"Movie {rk}", year=2000,
                stopped=1700000000, watched_status=1, **kwargs)


def episode(rk, guid, number, show="50", **kwargs):
    return dict(media_type="episode", rating_key=rk, guid=guid, title=f"Episode {number}", year=2005,
                grandparent_rating_key=show, grandparent_title="Series", parent_media_index=1, media_index=number,
                stopped=1700000000 + number, watched_status=1, **kwargs)


class Tautulli:
    def __init__(self, rows, metadata=None, settings=None):
        self.rows, self.metadata, self.calls = rows, metadata or {}, []
        self.cfg = {"tautulli": {"history": settings or {}}}
        self.client = NS(call=self.call)

    def call(self, cmd, **params):
        self.calls.append((cmd, params))
        if cmd == "get_history":
            page = self.rows[params["start"]:params["start"] + params["length"]]
            return {"recordsFiltered": len(self.rows), "data": page}
        if params["rating_key"] not in self.metadata:
            raise RuntimeError("Unable to retrieve metadata")
        return self.metadata[params["rating_key"]]

    def lookups(self):
        return [params["rating_key"] for cmd, params in self.calls if cmd == "get_metadata"]


def scan(source, known=None, **kwargs):
    asked = []
    def resolve(kind, guid):
        asked.append((kind, guid))
        return (known or {}).get(guid, {})
    result = recovery.scan(source, resolve=resolve, progress=lambda *a: None, check_cancel=lambda: None, **kwargs)
    return result, asked


def test_movies_use_the_cheapest_identity_source_and_never_store_plex_ids():
    source = Tautulli([movie("1", "com.plexapp.agents.imdb://tt0000001?lang=en"), movie("2", "plex://movie/aa"),
                       movie("3", "plex://movie/bb"), movie("4", "local://4"), movie("5", "plex://movie/cc")],
                      metadata={"2": {"guid": "plex://movie/aa", "guids": ["tmdb://22", "imdb://tt0000022"]}})
    result, asked = scan(source, {"plex://movie/bb": {"tmdb": "33", "plex": "bb", "guid": "plex://movie/bb"}})
    assert [r["item"]["ids"] for r in result] == [{"imdb": "tt0000001"}, {"tmdb": "22", "imdb": "tt0000022"}, {"tmdb": "33"}, {}, {}]
    assert [r["recovery_match"] for r in result] == ["Known identity", "Known identity", "Matched by Plex GUID",
                                                    "Missing identity — enter a match", "Missing identity — enter a match"]
    assert [r["recovery_valid"] for r in result] == [True, True, True, False, False]
    assert asked == [("movie", "plex://movie/bb"), ("movie", "plex://movie/cc")]
    assert source.lookups() == ["2", "3", "4", "5"]
    assert all(not r["recovery_requires_review"] and r["item"]["watched_at"] and r["item"]["watched"] for r in result)


def test_show_still_in_plex_costs_one_lookup_for_all_its_episodes():
    source = Tautulli([episode(str(i), f"plex://episode/{i}", i) for i in range(1, 6)],
                      metadata={"50": {"guid": "plex://show/s", "guids": ["tvdb://500", "tmdb://501"]}})
    result, asked = scan(source)
    assert source.lookups() == ["50"]
    assert asked == []
    assert all(r["item"]["show_ids"] == {"tmdb": "501", "tvdb": "500"} and r["item"]["ids"] == {} for r in result)
    assert [r["item"]["title"] for r in result][:2] == ["S01E01", "S01E02"]
    assert {r["original_title"] for r in result} == {"Series"} and len({r["original_group"] for r in result}) == 1


def test_removed_show_resolves_once_and_repairs_episodes_with_retired_guids():
    source = Tautulli([episode("1", "plex://episode/retired", 1), episode("2", "plex://episode/ok", 2),
                       episode("3", "plex://episode/later", 3), episode("9", "plex://episode/other", 1, show="60")])
    result, asked = scan(source, {"plex://episode/ok": {"tvdb": "500"}})
    assert asked == [("episode", "plex://episode/retired"), ("episode", "plex://episode/ok"), ("episode", "plex://episode/other")]
    assert source.lookups() == ["50", "60"]
    assert [r["recovery_valid"] for r in result] == [True, True, True, False]
    assert [r["item"]["show_ids"] for r in result] == [{"tvdb": "500"}] * 3 + [{}]
    assert result[0]["recovery_match"] == "Matched by Plex GUID"
    assert result[3]["recovery_match"] == "Missing identity — enter a match"


def test_legacy_episode_guid_is_the_show_identity_and_missing_numbers_stay_invalid():
    broken = episode("2", "com.plexapp.agents.thetvdb://500/1/2?lang=en", 2)
    broken["media_index"] = ""
    source = Tautulli([episode("1", "com.plexapp.agents.thetvdb://500/1/1?lang=en", 1), broken])
    result, asked = scan(source)
    assert not asked and not source.lookups()
    assert [r["item"]["show_ids"] for r in result] == [{"tvdb": "500"}, {"tvdb": "500"}]
    assert [r["recovery_valid"] for r in result] == [True, False]


def test_history_filters_paging_and_user_scope_follow_the_instance_settings():
    rows = [movie(str(i), f"com.plexapp.agents.imdb://tt{i:07d}") for i in range(1, 6)]
    rows[1]["watched_status"] = 0
    rows[2]["media_type"] = "track"
    source = Tautulli(rows, settings={"user_id": "7", "per_page": 2})
    updates = []
    result = recovery.scan(source, resolve=lambda *a: {}, progress=lambda *a: updates.append(a), check_cancel=lambda: None)
    history_calls = [params for cmd, params in source.calls if cmd == "get_history"]
    assert [call["start"] for call in history_calls] == [0, 2, 4]
    assert all(call["user_id"] == "7" and call["length"] == 2 for call in history_calls)
    assert [r["item"]["title"] for r in result] == ["Movie 1", "Movie 4", "Movie 5"]
    final = updates[-1][4]
    assert (final["checked"], final["found"], final["skipped"]) == (5, 3, 2)
    partial = Tautulli(deepcopy(rows), settings={"watched_only": False})
    assert len(scan(partial)[0]) == 4


def test_failed_history_read_cancel_and_row_limit_never_publish_results():
    def fail(*a, **kw): raise RuntimeError("HTTP 500")
    with pytest.raises(RuntimeError, match="HTTP 500"):
        recovery.scan(NS(cfg={}, client=NS(call=fail)), resolve=lambda *a: {}, progress=lambda *a: None, check_cancel=lambda: None)
    source = Tautulli([movie("1", "tmdb://1"), movie("2", "tmdb://2")])
    def cancelled(): raise InterruptedError()
    with pytest.raises(InterruptedError):
        recovery.scan(source, resolve=lambda *a: {}, progress=lambda *a: None, check_cancel=cancelled)
    assert source.calls == []
    with pytest.raises(RuntimeError, match="Tautulli user"):
        scan(source, max_rows=1)


def test_guid_resolver_checks_the_token_and_returns_only_external_ids(monkeypatch):
    status = NS(code=401)
    monkeypatch.setattr(common, "_metadata_request", lambda *a, **kw: NS(status_code=status.code))
    with pytest.raises(PermissionError):
        plex_recovery.guid_resolver("bad")
    status.code = 404
    monkeypatch.setattr(common, "hydrate_external_ids", lambda token, ref: {"tmdb": "1", "plex": ref})
    monkeypatch.setattr(common, "_hydrate_show_ids_from_episode_rk",
                        lambda token, ref: {"tvdb": 2, "guid": "plex://show/x", "plex": "x", "imdb": ""})
    resolve = plex_recovery.guid_resolver("token")
    assert resolve("movie", "plex://movie/abc") == {"tmdb": "1"}
    assert resolve("episode", "plex://episode/abc") == {"tvdb": "2"}
    assert resolve("movie", "local://5") == {}


@pytest.fixture
def api(monkeypatch, config_base):
    from api import syncAPI
    from services import importer
    cfg = {"plex": {"account_token": "token"}, "tautulli": {"server_url": "http://tautulli", "api_key": "key"},
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


def test_tautulli_source_scans_into_the_shared_review_flow(api, monkeypatch):
    seen = NS(token=None, resolve=None, adapter=None)
    def resolver(token):
        seen.token = token
        return lambda kind, guid: {}
    def read(adapter, *, resolve, progress, check_cancel, max_rows):
        seen.adapter, seen.resolve = adapter, resolve
        return [dict(feature="history", item=dict(type="movie", title="Old", year=2000, ids={"tmdb": "9"},
                                                  watched_at="2020-01-01T12:00:00Z", watched=True),
                     source_path="Tautulli history", recovery_match="Matched by Plex GUID", recovery_valid=True,
                     recovery_requires_review=False, original_group="g", original_year=2000, original_title="Old")]
    monkeypatch.setattr(plex_recovery, "guid_resolver", resolver)
    monkeypatch.setattr(recovery, "scan", read)
    options = api.client.get("/api/import/plex-recovery/options").json()
    assert [t["id"] for t in options["tautulli"]] == ["default"] and [s["id"] for s in options["lookups"]] == ["default"]
    assert options["sources"] == []
    response = run(api, dict(source="tautulli"))
    assert response.status_code == 200, response.text
    url = "/api/import/plex-recovery/" + response.json()["id"]
    job = api.client.get(url).json()
    assert (job["status"], job["source"], job["tautulli_instance"]) == ("review", "tautulli", "default")
    assert seen.token == "token" and seen.adapter.cfg["tautulli"]["api_key"] == "key"
    rows = api.client.get(url + "/rows").json()
    assert rows["summary"]["ready"] == 1 and rows["rows"][0]["recovery_match"] == "Matched by Plex GUID"
    api.cfg = {**api.cfg, "tautulli": {"server_url": "http://other", "api_key": "key"}}
    changed = api.client.get(url + "/match-groups", params={"revision": job["revision"]})
    assert changed.status_code == 409 and "settings changed" in changed.json()["detail"]


def test_tautulli_source_needs_tautulli_and_a_plex_account_and_reports_a_rejected_token(api, monkeypatch):
    api.cfg = {**api.cfg, "tautulli": {}}
    assert "Connect Tautulli" in run(api, dict(source="tautulli")).json()["detail"]
    api.cfg = {**api.cfg, "tautulli": {"server_url": "http://tautulli", "api_key": "key"}, "plex": {}}
    assert "Connect a Plex account" in run(api, dict(source="tautulli")).json()["detail"]
    assert not service.JOBS
    api.cfg = {**api.cfg, "plex": {"account_token": "token"}}
    def rejected(token): raise PermissionError("Plex rejected the account token.")
    monkeypatch.setattr(plex_recovery, "guid_resolver", rejected)
    response = run(api, dict(source="tautulli"))
    job = api.client.get("/api/import/plex-recovery/" + response.json()["id"]).json()
    assert job["status"] == "error" and "Plex rejected the account token" in job["message"]
    assert "SYNC" not in api.rt[1]
