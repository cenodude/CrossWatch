# tests/test_tracker_type_conversion.py
# CrossWatch - Tracker history type conversion and repeated imports
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy
from types import SimpleNamespace

import pytest

from api import trackerAPI
from cw_platform.tracker_history_identity import TYPE_ORIGINS
from cw_platform.tracker_store import TrackerStore
from services import importer, plex_history_recovery as recovery
from test_plex_history_recovery import api


@pytest.fixture
def converted_case(api, monkeypatch):
    api.client.app.include_router(trackerAPI.router)
    monkeypatch.setattr(trackerAPI, "load_config", lambda: api.cfg)
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    monkeypatch.setattr(trackerAPI, "_tmdb_provider", lambda: SimpleNamespace(
        get_response=lambda *a, **kw: SimpleNamespace(data={"id": 267970, "imdb_id": "tt1234567"})))
    api.cfg["tmdb"] = {"api_key": "test"}
    api.job.raw = [{**api.job.raw[0], "item": {
        "type": "episode", "title": "S01E01", "series_title": "Miss 267970", "season": 1, "episode": 1,
        "ids": {}, "show_ids": {"tmdb": "267970"}, "watched_at": "2017-02-08T23:33:27Z",
    }}]
    recovery._shape(api.job, api.cfg)
    url = f"/api/import/plex-recovery/{api.job.id}"
    result = api.client.post(url + "/commit", json={"import_id": api.job.id, "revision": 0, "mode": "ready"})
    assert result.status_code == 200, result.text
    assert result.json()["applied"] == 1
    api.store = TrackerStore(api.cfg["crosswatch"]["root_dir"])
    api.before = api.store.read_current("history")["items"]
    api.key = next(iter(api.before))
    api.movie = {"type": "movie", "title": "Miss 267970", "year": 2016, "ids": {"tmdb": "267970"}}
    return api


def convert(case):
    return case.client.post("/api/tracker/replace", json={"feature": "history", "key": case.key, "item": case.movie})


def test_import_convert_reimport_restart_and_undo(converted_case):
    case = converted_case
    result = convert(case)
    assert result.status_code == 200, result.text
    saved = result.json()
    assert saved["key"] == case.key.replace("#s01e01", "")
    current = TrackerStore(case.store.root).read_current("history")["items"]
    movie = current[saved["key"]]
    assert movie["type"] == "movie"
    assert movie["watched_at"] == case.before[case.key]["watched_at"]
    assert movie["ids"] == {"tmdb": "267970", "imdb": "tt1234567"}
    assert movie[TYPE_ORIGINS]
    assert not {"season", "episode", "show_ids", "series_title"}.intersection(movie)
    recovery._shape(case.job, case.cfg)
    assert case.job.rows[0]["status"] == "exists"
    preview = importer._shape_rows(case.job.raw, case.cfg, "default", "file")
    assert preview[0]["status"] == "exists"
    result = case.client.post(f"/api/import/plex-recovery/{case.job.id}/commit", json={
        "import_id": case.job.id, "revision": 1, "mode": "selected", "row_ids": ["0"],
    })
    assert result.status_code == 200, result.text
    assert result.json()["applied"] == 0
    assert case.store.read_current("history")["items"] == current
    result = case.client.post("/api/tracker/undo", json={"batch": saved["batch"]})
    assert result.status_code == 200, result.text
    assert case.store.read_current("history")["items"] == case.before
    case.store.write_state("history", {})
    recovery._shape(case.job, case.cfg)
    assert case.job.rows[0]["status"] == "ready"


def test_conversion_is_scoped_to_the_original_watch_and_tracker(converted_case, monkeypatch):
    case = converted_case
    assert convert(case).status_code == 200
    original = deepcopy(case.job.raw[0])
    other_episode = deepcopy(original)
    other_episode["item"]["episode"] = 2
    rewatch = deepcopy(original)
    rewatch["item"]["watched_at"] = "2020-01-01T00:00:00Z"
    other_show = deepcopy(original)
    other_show["item"]["show_ids"] = {"tmdb": "123"}
    rows = importer._shape_rows([original, other_episode, rewatch, other_show], case.cfg, "default", "file")
    assert [row["status"] for row in rows] == ["exists", "ready", "ready", "ready"]
    monkeypatch.setattr(importer, "_existing_keys", lambda *a: {"history": {}})
    assert importer._shape_rows([original], case.cfg, "other", "file")[0]["status"] == "ready"


def test_provider_add_cannot_restore_the_converted_episode(converted_case):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    case = converted_case
    saved = convert(case).json()
    adapter = CROSSWATCHModule({**case.cfg, "_cw_history_rewatches": True})
    result = adapter.add("history", [case.job.raw[0]["item"]])
    assert result["ok"] and result["count"] == 0
    assert set(case.store.read_current("history")["items"]) == {saved["key"]}
    assert case.client.post("/api/tracker/undo", json={"batch": saved["batch"]}).status_code == 200
    assert case.store.read_current("history")["items"] == case.before


def test_conversion_refuses_to_overwrite_an_existing_movie(converted_case):
    case = converted_case
    movie_key = case.key.replace("#s01e01", "")
    before = {**case.before, movie_key: {**case.movie, "watched_at": case.before[case.key]["watched_at"]}}
    case.store.write_state("history", before)
    response = convert(case)
    assert response.status_code == 409
    assert response.json()["error"] == "key_exists"
    assert case.store.read_current("history")["items"] == before


def test_bulk_type_conversion_is_rejected(converted_case):
    case = converted_case
    response = case.client.post("/api/tracker/edit-title", json={"show": "tmdb:267970", "item": case.movie})
    assert response.status_code == 400
    assert response.json()["error"] == "type_change_requires_record"
    assert case.store.read_current("history")["items"] == case.before


def test_movie_match_search_is_opt_in_and_tracker_only(converted_case, monkeypatch):
    from api import editorAPI
    from services import interactive_sync_mapping

    case = converted_case
    case.client.app.include_router(editorAPI.router)
    monkeypatch.setattr(editorAPI, "load_config", lambda: case.cfg)
    monkeypatch.setattr(editorAPI, "_require_instance_scope", lambda *a: None)
    searched = []
    monkeypatch.setattr(interactive_sync_mapping, "search_candidates", lambda cfg, row, q, **kw:
                        searched.append(row["item"]["type"]) or {"results": []})
    body = {"provider": "CROSSWATCH", "feature": "history", "key": case.key,
            "original": case.before[case.key], "action": "search", "q": "Miss 267970"}
    assert case.client.post("/api/editor/mapping", json=body).status_code == 200
    assert case.client.post("/api/editor/mapping", json={**body, "search_type": "movie"}).status_code == 200
    assert searched == ["episode", "movie"]
    for extra in ({"provider": "PLEX"}, {"feature": "ratings"}):
        response = case.client.post("/api/editor/mapping", json={**body, **extra, "search_type": "movie"})
        assert response.status_code == 400
    assert searched == ["episode", "movie"]


def test_conversion_removes_old_episode_metadata_and_preserves_other_records(converted_case):
    case = converted_case
    current = deepcopy(case.before)
    current[case.key].update(_cw_anime_map={"x": 1}, _cw_episode_order="absolute", tvdb="100", show_title="Wrong show")
    neighbour = {**case.before[case.key], "episode": 2}
    neighbour_key = case.key.replace("e01", "e02")
    current[neighbour_key] = neighbour
    case.store.write_state("history", current)
    result = convert(case)
    assert result.status_code == 200, result.text
    items = case.store.read_current("history")["items"]
    assert items[neighbour_key] == neighbour
    movie = items[result.json()["key"]]
    assert not {"_cw_anime_map", "_cw_episode_order", "tvdb", "show_title"}.intersection(movie)
    assert case.client.post("/api/tracker/undo", json={"batch": result.json()["batch"]}).status_code == 200
    assert case.store.read_current("history")["items"] == current


def test_episode_to_movie_rekeys_even_when_ids_and_title_are_unchanged(converted_case):
    case = converted_case
    before = {**case.before[case.key], "ids": case.movie["ids"], "title": case.movie["title"], "year": 2016}
    case.store.write_state("history", {case.key: before})
    case.cfg.pop("tmdb")
    response = convert(case)
    assert response.status_code == 200, response.text
    assert response.json()["key"] == case.key.replace("#s01e01", "")


def test_legacy_episode_ids_are_remembered_without_show_ids(converted_case):
    case = converted_case
    original = {**case.before[case.key], "ids": {"tmdb": "267970"}}
    original.pop("show_ids", None)
    case.store.write_state("history", {case.key: original})
    assert convert(case).status_code == 200
    rows = importer._shape_rows([{"feature": "history", "item": original}], case.cfg, "default", "file")
    assert rows[0]["status"] == "exists"


@pytest.mark.parametrize("tmdb", ["", "invalid", "0", True])
def test_conversion_rejects_invalid_movie_ids(converted_case, tmdb):
    case = converted_case
    case.cfg.pop("tmdb")
    case.movie["ids"] = {"tmdb": tmdb}
    result = convert(case)
    assert result.status_code == 400
    assert case.store.read_current("history")["items"] == case.before


def test_whole_show_watch_can_be_converted_without_changing_its_date(converted_case):
    case = converted_case
    original = {"type": "show", "title": "Miss 267970", "ids": {"tmdb": "267970"},
                "watched_at": case.before[case.key]["watched_at"]}
    case.key = case.key.replace("#s01e01", "#show")
    case.store.write_state("history", {case.key: original})
    result = convert(case)
    assert result.status_code == 200, result.text
    assert result.json()["key"] == case.key.replace("#show", "")
    assert importer._shape_rows([{"feature": "history", "item": original}], case.cfg, "default", "file")[0]["status"] == "exists"
    assert case.client.post("/api/tracker/undo", json={"batch": result.json()["batch"]}).status_code == 200
    assert case.store.read_current("history")["items"] == {case.key: original}


def test_local_correction_survives_movie_refresh_but_is_not_inherited_from_an_import(converted_case):
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    case = converted_case
    result = convert(case).json()
    adapter = CROSSWATCHModule({**case.cfg, "_cw_history_rewatches": True})
    movie = case.store.read_current("history")["items"][result["key"]]
    adapter.add("history", [{**case.movie, "watched_at": movie["watched_at"]}])
    assert case.store.read_current("history")["items"][result["key"]][TYPE_ORIGINS] == movie[TYPE_ORIGINS]
    other = CROSSWATCHModule({"crosswatch": {"connected": True, "root_dir": str(case.base / "other")}, "_cw_history_rewatches": True})
    assert other.add("history", [movie])["count"] == 1
    incoming = next(iter(TrackerStore(case.base / "other").read_current("history")["items"].values()))
    assert TYPE_ORIGINS not in incoming
