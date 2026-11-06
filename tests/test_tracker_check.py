# tests/test_tracker_check.py
# CrossWatch - Tracker TMDB metadata check
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from functools import partial
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore
from services import tracker_check
from services.tracker_check import Abort, Lookups, classify

REPO = Path(__file__).resolve().parents[1]
HEAT = {"id": 949, "title": "Heat", "original_title": "Heat", "release_date": "1995-12-15"}
AMELIE = {"id": 194, "title": "Amelie", "original_title": "Le Fabuleux Destin d'Amélie Poulain", "release_date": "2001-04-25"}
SEVERANCE = {"id": 95396, "name": "Severance", "original_name": "Severance", "first_air_date": "2022-02-17"}
SEASON_ONE = {"id": 1, "season_number": 1, "episodes": [{"episode_number": n, "season_number": 1} for n in range(1, 10)]}


def _http(status):
    return requests.HTTPError(response=SimpleNamespace(status_code=status))


class FakeTmdb:
    def __init__(self, table=None, on_call=None):
        self.table = {
            ("movie", "949", None): HEAT,
            ("movie", "194", None): AMELIE,
            ("show", "95396", None): SEVERANCE,
            ("show", "95396", 1): SEASON_ONE,
            **(table or {}),
        }
        self.calls = []
        self.on_call = on_call

    def lookup(self, kind, tmdb, season=None, **_):
        self.calls.append((kind, str(tmdb), season))
        if self.on_call:
            self.on_call()
        value = self.table.get((kind, str(tmdb), season), _http(404))
        if isinstance(value, Exception):
            raise value
        return SimpleNamespace(data=value, fetched_at=1_760_000_000.0, source="cache")

    def get_response(self, url, params, **options):
        assert options == {"quiet_404": True, "max_age": tracker_check.MAX_AGE, "timeout": 10, "max_retries": 2}
        return self.lookup(params["external_source"].removesuffix("_id"), url.rsplit("/", 1)[-1])


def _movie(**extra):
    return {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949"}, **extra}


def _episode(**extra):
    return {"type": "episode", "series_title": "Severance", "season": 1, "episode": 2, "ids": {}, "show_ids": {"tmdb": "95396"}, **extra}


@pytest.mark.parametrize(("item", "code"), [
    (_movie(), "ok"),
    (_movie(title="Le Fabuleux Destin", year=2001, ids={"tmdb": "194"}), "ok"),
    (_movie(title="Heat (Director's Cut)", year=1995), "ok"),
    (_movie(title="Arrival", year=2016), "mismatch"),
    (_movie(title="Arrival", year=1995), "ok"),
    (_movie(year=None), "incomplete"),
    (_movie(title=""), "incomplete"),
    (_movie(ids={"imdb": "tt0113277"}), "no_tmdb"),
    (_movie(ids={"tmdb": "1"}), "not_found"),
    (_movie(ids={"tmdb": "abc"}), "not_found"),
    (_episode(), "ok"),
    (_episode(episode=40), "episode_missing"),
    (_episode(season=7), "episode_missing"),
    (_episode(episode=40, _anime_absolute=40), "ok"),
    (_episode(show_ids={}), "no_tmdb"),
    ({"type": "person", "ids": {"tmdb": "1"}}, "skipped"),
])
def test_classify_reports_what_tmdb_disagrees_with(item, code):
    assert classify(item, Lookups(FakeTmdb(), pause=0))[0] == code


def test_classify_keeps_tmdb_details_for_the_fix():
    code, detail = classify(_movie(title="Arrival", year=2016), Lookups(FakeTmdb(), pause=0))
    assert code == "mismatch"
    assert (detail["title"], detail["year"], detail["tmdb"], detail["source"]) == ("Heat", 1995, "949", "cache")


@pytest.mark.parametrize(("item", "source", "identifier", "results", "expected"), [
    (_movie(ids={"imdb": "tt0113277"}), "imdb", "tt0113277", {"movie_results": [{"id": 949}]}, "949"),
    ({"type": "show", "title": "Severance", "year": 2022, "ids": {"imdb": "tt11280740"}}, "imdb", "tt11280740", {"tv_results": [{"id": 95396}]}, "95396"),
    ({"type": "show", "title": "Severance", "year": 2022, "ids": {"tvdb": "371980"}}, "tvdb", "371980", {"tv_results": [{"id": 95396}]}, "95396"),
    (_episode(show_ids={"tvdb": "371980"}), "tvdb", "371980", {"tv_results": [{"id": 95396}]}, "95396"),
    (_episode(show_ids={"imdb": "tt11280740"}), "imdb", "tt11280740", {"tv_results": [{"id": 95396}]}, "95396"),
    (_episode(show_ids={}, ids={"imdb": "tt12345678"}), "imdb", "tt12345678", {"tv_episode_results": [{"id": 123, "show_id": 95396, "season_number": 1, "episode_number": 2}]}, "95396"),
    (_episode(show_ids={}, ids={"tvdb": "12345"}), "tvdb", "12345", {"tv_episode_results": [{"id": 123, "show_id": 95396, "season_number": 1, "episode_number": 2}]}, "95396"),
    (_episode(type="season", show_ids={}, ids={"tvdb": "12345"}), "tvdb", "12345", {"tv_season_results": [{"id": 123, "show_id": 95396, "season_number": 1}]}, "95396"),
])
def test_external_ids_resolve_the_right_title_before_checking(item, source, identifier, results, expected):
    provider = FakeTmdb({(source, identifier, None): results})
    code, detail = classify(item, Lookups(provider, pause=0))
    assert code == "ok"
    assert detail["tmdb"] == expected
    assert provider.calls[0] == (source, identifier, None)
    assert ("show" if item["type"] != "movie" else "movie", expected, None) in provider.calls
    assert "tmdb" not in item["ids"]


@pytest.mark.parametrize("results", [
    {"movie_results": []},
    {"tv_results": [{"id": 949}]},
    {"movie_results": [{"id": 949}, {"id": 194}]},
    {"movie_results": [{"id": True}]},
])
def test_external_lookup_does_not_guess_a_title(results):
    provider = FakeTmdb({("imdb", "tt0113277", None): results})
    assert classify(_movie(ids={"imdb": "tt0113277"}), Lookups(provider, pause=0))[0] == "no_tmdb"
    assert len(provider.calls) == 1


def test_episode_ids_are_not_used_as_show_ids_or_for_different_episode_numbers():
    provider = FakeTmdb({("tvdb", "123", None): {
        "tv_results": [{"id": 95396}],
        "tv_episode_results": [{"id": 456, "show_id": 95396, "season_number": 2, "episode_number": 2}],
    }})
    assert classify(_episode(show_ids={}, ids={"tvdb": "123"}), Lookups(provider, pause=0))[0] == "no_tmdb"
    assert provider.calls == [("tvdb", "123", None)]


def test_external_lookup_tries_tvdb_when_imdb_has_no_show_match_and_reuses_results():
    provider = FakeTmdb({
        ("imdb", "tt11280740", None): {"tv_results": []},
        ("tvdb", "371980", None): {"tv_results": [{"id": 95396, "overview": "large"}]},
    })
    lookups = Lookups(provider, pause=0)
    for episode in (2, 3):
        assert classify(_episode(episode=episode, show_ids={"imdb": "tt11280740", "tvdb": "371980"}), lookups)[0] == "ok"
    assert provider.calls == [("imdb", "tt11280740", None), ("tvdb", "371980", None), ("show", "95396", None), ("show", "95396", 1)]
    assert lookups.cache[("tvdb", "371980", None)].data["tv_results"] == [{"id": 95396}]


def test_resolved_external_ids_still_report_title_and_episode_mismatches():
    provider = FakeTmdb({
        ("imdb", "tt0113277", None): {"movie_results": [{"id": 949}]},
        ("tvdb", "371980", None): {"tv_results": [{"id": 95396}]},
    })
    lookups = Lookups(provider, pause=0)
    assert classify(_movie(title="Arrival", year=2016, ids={"imdb": "tt0113277"}), lookups)[0] == "mismatch"
    assert classify(_episode(episode=40, show_ids={"tvdb": "371980"}), lookups)[0] == "episode_missing"


@pytest.mark.parametrize("failure", [requests.ConnectionError("down"), _http(429), _http(503), {}, {"movie_results": None}])
def test_external_lookup_failures_are_not_saved_as_missing_matches(failure):
    provider = FakeTmdb({("imdb", "tt0113277", None): failure})
    lookups = Lookups(provider, pause=0)
    assert classify(_movie(ids={"imdb": "tt0113277"}), lookups) is None
    assert classify(_movie(ids={"imdb": "tt0113277"}), lookups) is None
    assert len(provider.calls) == 1


@pytest.mark.parametrize(("failure", "code"), [(_http(401), "tmdb_key_invalid"), (RuntimeError("missing key"), "tmdb_key_missing")])
def test_external_lookup_auth_errors_stop_the_scan(failure, code):
    with pytest.raises(Abort) as stopped:
        classify(_movie(ids={"imdb": "tt0113277"}), Lookups(FakeTmdb({("imdb", "tt0113277", None): failure}), pause=0))
    assert stopped.value.code == code


def test_existing_tmdb_ids_still_take_precedence_and_movie_tvdb_ids_are_not_queried():
    provider = FakeTmdb()
    lookups = Lookups(provider, pause=0)
    assert classify(_movie(ids={"tmdb": "949", "imdb": "tt0113277"}), lookups)[0] == "ok"
    assert classify(_movie(ids={"tvdb": "123"}), lookups)[0] == "no_tmdb"
    assert classify(_episode(show_ids={"imdb": "../123", "tvdb": "-1"}), lookups)[0] == "no_tmdb"
    assert provider.calls == [("movie", "949", None)]


def test_failed_lookup_is_not_a_finding_and_is_not_retried_per_episode():
    tmdb = FakeTmdb({("show", "95396", None): requests.ConnectionError("down")})
    lookups = Lookups(tmdb, pause=0)
    assert classify(_episode(), lookups) is None
    assert classify(_episode(episode=3), lookups) is None
    assert tmdb.calls == [("show", "95396", None)]


def test_rejected_key_and_a_dead_tmdb_stop_the_check(monkeypatch):
    with pytest.raises(Abort) as rejected:
        classify(_movie(), Lookups(FakeTmdb({("movie", "949", None): _http(401)}), pause=0))
    assert rejected.value.code == "tmdb_key_invalid"
    monkeypatch.setattr(tracker_check, "MAX_FAILURES", 3)
    lookups = Lookups(FakeTmdb({("movie", str(n), None): _http(503) for n in range(1, 5)}), pause=0)
    with pytest.raises(Abort) as dead:
        for n in range(1, 5):
            classify(_movie(ids={"tmdb": str(n)}), lookups)
    assert dead.value.code == "tmdb_unreachable"


@pytest.fixture()
def base(tmp_path):
    root = tmp_path / ".cw_provider"
    store = TrackerStore(root)
    store.write_state("history", {
        "tmdb:949": {**_movie(), "watched_at": "2026-01-02T00:00:00.000Z"},
        "tmdb:95396#s01e40": {**_episode(episode=40), "watched_at": "2026-01-03T00:00:00.000Z"},
        "imdb:tt2543164": {"type": "movie", "title": "Arrival", "year": 2016, "ids": {"imdb": "tt2543164"}, "watched_at": "2026-01-04T00:00:00.000Z"},
    })
    store.write_state("ratings", {"tmdb:194": {**_movie(title="Amelie", year=2001, ids={"tmdb": "194"}), "rating": 8}})
    return root


@pytest.fixture()
def client(base, monkeypatch):
    cfg = {"crosswatch": {"root_dir": str(base)}, "tmdb": {"api_key": "k"}}
    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(trackerAPI, "_tmdb_provider", lambda: FakeTmdb())
    monkeypatch.setattr(trackerAPI.tracker_check, "start", partial(tracker_check.start, background=False))
    app = FastAPI()
    app.include_router(trackerAPI.router)
    test_client = TestClient(app)
    test_client.cfg = cfg
    return test_client


def test_check_runs_flags_records_and_reports_in_summary(client):
    res = client.post("/api/tracker/check", json={})
    assert res.status_code == 200, res.text
    job = client.get("/api/tracker/check").json()["job"]
    assert (job["state"], job["done"], job["total"], job["attention"], job["failed"]) == ("completed", 4, 4, 2, 0)
    summary = client.get("/api/tracker/summary").json()
    assert summary["attention"]["history"] == 2 and summary["attention"]["ratings"] == 0
    assert summary["check"]["state"] == "completed"
    flagged = client.get("/api/tracker/items", params={"feature": "history", "attention": "true"}).json()
    assert {row["key"]: row["check"]["code"] for row in flagged["items"]} == {"tmdb:95396#s01e40": "episode_missing", "imdb:tt2543164": "no_tmdb"}
    everything = client.get("/api/tracker/items", params={"feature": "history"}).json()["items"]
    heat = next(row for row in everything if row["key"] == "tmdb:949")
    assert heat["check"]["code"] == "ok" and heat["check"]["fetched_at"].startswith("2025-") and heat["check"]["checked_at"]


def test_fixing_a_record_clears_its_flag(client):
    client.post("/api/tracker/check", json={})
    res = client.post("/api/tracker/edit", json={"feature": "history", "key": "imdb:tt2543164", "changes": {"ids": {"tmdb": "329865"}}})
    assert res.status_code == 200, res.text
    assert client.get("/api/tracker/summary").json()["attention"]["history"] == 1


def test_bulk_scan_checks_external_ids_without_rewriting_records(client, base, monkeypatch):
    provider = FakeTmdb({
        ("imdb", "tt2543164", None): {"movie_results": [{"id": 329865}]},
        ("movie", "329865", None): {"title": "Arrival", "release_date": "2016-11-10", "runtime": 116},
    })
    monkeypatch.setattr(trackerAPI, "_tmdb_provider", lambda: provider)
    store = TrackerStore(base)
    before = store.read_current("history")["items"]
    assert client.post("/api/tracker/check", json={}).status_code == 200
    job = client.get("/api/tracker/check").json()["job"]
    assert (job["state"], job["done"], job["failed"], job["attention"]) == ("completed", 4, 0, 1)
    items = client.get("/api/tracker/items", params={"feature": "history", "health": "ok"}).json()["items"]
    arrival = next(item for item in items if item["key"] == "imdb:tt2543164")
    assert arrival["check"]["tmdb"] == "329865"
    assert store.read_current("history")["items"] == before


def test_check_needs_a_tmdb_key(client):
    client.cfg.pop("tmdb")
    res = client.post("/api/tracker/check", json={})
    assert res.status_code == 400 and res.json()["error"] == "tmdb_key_missing"
    assert client.get("/api/tracker/summary").json()["tmdb_key"] is False


def test_only_one_check_runs_at_a_time(client, monkeypatch):
    monkeypatch.setitem(tracker_check._ACTIVE, "other.db", {"state": "running"})
    res = client.post("/api/tracker/check", json={})
    assert res.status_code == 409 and res.json()["error"] == "check_running"


def test_stopping_keeps_the_results_so_far(base):
    store = TrackerStore(base)
    tmdb = FakeTmdb(on_call=lambda: tracker_check.cancel(store))
    tracker_check.start(store, tmdb, instance="default", background=False)
    job = tracker_check.status(store)
    assert job["state"] == "cancelled" and 0 < job["done"] < job["total"]


def test_a_check_cut_off_by_a_restart_reads_as_interrupted(base):
    store = TrackerStore(base)
    store.save_job(tracker_check.JOB, {"id": "x", "state": "running", "started_at": 1.0})
    assert tracker_check.status(store)["state"] == "interrupted"


def test_check_results_leave_with_the_records(base):
    store = TrackerStore(base)
    tracker_check.start(store, FakeTmdb(), instance="default", background=False)
    store.write_state("history", {})
    store.prune_checks()
    assert store.attention_counts()["history"] == 0
    store.clear(state=True, snapshots=False)
    assert store.checks_for("ratings", ["tmdb:194"]) == {}


def test_current_check_names_the_instance_for_the_bell(client, base):
    assert client.get("/api/tracker/check/current").json()["job"] is None
    client.post("/api/tracker/check", json={})
    job = client.get("/api/tracker/check/current").json()["job"]
    assert (job["instance"], job["label"], job["state"], job["attention"]) == ("default", "Default", "completed", 2)


def test_check_endpoints_need_write_access_for_managed_users():
    from api.appAuthAPI import non_admin_api_allowed
    from crosswatch import _non_admin_permission_allowed

    for method, path in (("POST", "/api/tracker/check"), ("GET", "/api/tracker/check/current")):
        assert non_admin_api_allowed(path, method)
        assert not _non_admin_permission_allowed({"permissions": {"monitor": True}}, path, method)


def test_bell_feeder_is_loaded_for_admins_and_managed_tracker_users():
    import ui_frontend

    assert "tracker-notifications.js" in ui_frontend._APP_SCRIPTS
    assert "tracker-notifications.js" not in ui_frontend._USER_APP_SCRIPTS
    assert "tracker-notifications.js" not in ui_frontend._FULL_USER_APP_SCRIPTS
    assert 'app_scripts = (*app_scripts, "tracker-notifications.js")' in (REPO / "ui_frontend.py").read_text("utf-8")
    feeder = (REPO / "assets" / "js" / "tracker-notifications.js").read_text("utf-8")
    assert "/api/tracker/check/current" in feeder and '"/#tracker?attention=1"' in feeder
    core = (REPO / "assets" / "helpers" / "core.js").read_text("utf-8")
    assert 'if (tab === "tracker") return "#tracker"' in core


def test_run_cache_keeps_only_what_the_check_reads():
    big = {**SEASON_ONE, "episodes": [{"episode_number": 1, "season_number": 1, "overview": "x" * 5000, "crew": [{}] * 50}]}
    lookups = Lookups(FakeTmdb({("show", "95396", 1): big}), pause=0)
    classify(_episode(episode=1), lookups)
    assert lookups.cache[("show", "95396", 1)].data == {"episodes": [{"episode_number": 1}]}
    assert set(lookups.cache[("show", "95396", None)].data) == {"name", "original_name", "first_air_date"}


def test_a_result_for_a_record_edited_during_the_check_is_dropped(base):
    store = TrackerStore(base)
    feature, key, data, text = next(row for row in store.iter_records() if row[1] == "tmdb:949")
    store.change_items("history", [(key, key, {**data, "title": "Fixed"})], action="edit")
    store.save_checks([(feature, key, "mismatch", {"title": "x"}, text)])
    assert store.checks_for("history", [key]) == {}
    fresh = next(row for row in store.iter_records() if row[1] == "tmdb:949")
    store.save_checks([(fresh[0], fresh[1], "ok", {}, fresh[3])])
    assert store.checks_for("history", [key])[key][0] == "ok"


def test_run_cache_stays_bounded(monkeypatch):
    monkeypatch.setattr(tracker_check, "CACHE_LIMIT", 3)
    lookups = Lookups(FakeTmdb({("movie", str(n), None): HEAT for n in range(1, 10)}), pause=0)
    for n in range(1, 10):
        lookups.get("movie", str(n))
    assert list(lookups.cache) == [("movie", "7", None), ("movie", "8", None), ("movie", "9", None)]


def test_health_counts_and_filter_cover_unchecked_records(client):
    before = client.get("/api/tracker/summary").json()["health"]["history"]
    assert before == {"ok": 0, "attention": 0, "unchecked": 3}
    client.post("/api/tracker/check", json={})
    after = client.get("/api/tracker/summary").json()["health"]
    assert after["history"] == {"ok": 1, "attention": 2, "unchecked": 0}
    assert after["ratings"] == {"ok": 1, "attention": 0, "unchecked": 0}
    ok = client.get("/api/tracker/items", params={"feature": "history", "health": "ok"}).json()["items"]
    assert [row["key"] for row in ok] == ["tmdb:949"]
    assert client.get("/api/tracker/items", params={"feature": "history", "health": "unchecked"}).json()["total"] == 0
