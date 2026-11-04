# tests/test_tracker_notes.py
# CrossWatch - Personal tracker notes, tags and custom fields that never sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import io

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore

MOVIE = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949", "imdb": "tt0113277"}, "watched_at": "2026-01-02T00:00:00.000Z"}
EPISODE = {
    "type": "episode", "title": "S01E02", "series_title": "Severance", "series_year": 2022, "season": 1, "episode": 2,
    "ids": {}, "show_ids": {"tmdb": "95396"}, "watched_at": "2026-03-01T00:00:00.000Z",
}
WRONG = {"type": "movie", "title": "Arrival", "year": 2016, "ids": {"tmdb": "1"}, "watched_at": "2025-12-01T00:00:00.000Z"}


@pytest.fixture()
def base(tmp_path):
    root = tmp_path / ".cw_provider"
    TrackerStore(root).write_state("history", {"tmdb:949": MOVIE, "tmdb:95396#s01e02": EPISODE, "tmdb:1": WRONG})
    return root


@pytest.fixture()
def api(base, monkeypatch):
    monkeypatch.setattr(trackerAPI, "load_config", lambda: {"crosswatch": {"root_dir": str(base)}})
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: True)
    app = FastAPI()
    app.include_router(trackerAPI.router)
    return TestClient(app)


def _save(api, key, **fields):
    return api.post("/api/tracker/note", json={"key": key, **fields})


def test_title_carries_its_note_and_saving_works_during_a_sync(api, base):
    title = api.get("/api/tracker/title?feature=history&key=tmdb:949").json()
    assert (title["note_key"], title["note"]) == ("movie:tmdb:949", None)

    saved = _save(api, "movie:tmdb:949", title="Heat", year=1995, status="completed", started="2026-01-01",
                  finished="2026-01-02", notes="Still the best bank job.", tags=["Crime", "crime", " Noir  film "])
    assert saved.status_code == 200
    note = saved.json()["note"]
    assert note["tags"] == ["Crime", "Noir film"]
    assert (note["status"], note["finished"], note["art"]) == ("completed", "2026-01-02", {"kind": "movie", "tmdb": "949"})

    again = api.get("/api/tracker/title?feature=history&key=tmdb:949").json()["note"]
    assert again["notes"] == "Still the best bank job."
    show = api.get("/api/tracker/title?show=tmdb:95396").json()
    assert show["note_key"] == "show:tmdb:95396"
    assert TrackerStore(base).read_current("history")["items"]["tmdb:949"] == MOVIE


def test_notes_never_reach_the_synced_state(api, base):
    _save(api, "show:tmdb:95396", title="Severance", status="watching", tags=["Apple"])
    store = TrackerStore(base)
    for feature in ("history", "watchlist", "ratings"):
        try:
            items = store.read_current(feature)["items"]
        except FileNotFoundError:
            continue
        assert all("status" not in item and "tags" not in item for item in items.values())
    assert store.write_state("history", {"tmdb:949": MOVIE})
    assert store.note("show:tmdb:95396")["status"] == "watching"


def test_my_titles_list_filters_by_status_tag_and_text(api):
    _save(api, "movie:tmdb:949", title="Heat", status="completed", tags=["Crime"])
    _save(api, "show:tmdb:95396", title="Severance", status="watching", tags=["Apple", "crime"])
    _save(api, "movie:tmdb:5", title="Gone Movie", status="planned")

    every = api.get("/api/tracker/notes").json()
    assert every["total"] == 3 and every["summary"]["statuses"] == {"completed": 1, "watching": 1, "planned": 1}
    assert {"tag": "Crime", "count": 2} in every["summary"]["tags"]
    assert [n["key"] for n in api.get("/api/tracker/notes?status=watching").json()["notes"]] == ["show:tmdb:95396"]
    assert {n["key"] for n in api.get("/api/tracker/notes?tag=CRIME").json()["notes"]} == {"movie:tmdb:949", "show:tmdb:95396"}
    assert [n["key"] for n in api.get("/api/tracker/notes?q=gone").json()["notes"]] == ["movie:tmdb:5"]

    orphan = api.get("/api/tracker/title?note=movie:tmdb:5").json()
    assert (orphan["title"]["title"], orphan["plays"], orphan["note"]["status"]) == ("Gone Movie", [], "planned")
    assert api.get("/api/tracker/title?note=movie:tmdb:404").status_code == 404

    shows = api.get("/api/tracker/shows").json()["shows"]
    assert (shows[0]["status"], shows[0]["tags"]) == ("watching", ["Apple", "crime"])


def test_custom_fields_validate_and_only_keep_known_fields(api):
    bad = api.post("/api/tracker/note-fields", json={"fields": [{"name": "Mood", "type": "choice", "choices": []}]})
    assert (bad.status_code, bad.json()["error"]) == (400, "invalid_field")
    fields = api.post("/api/tracker/note-fields", json={"fields": [
        {"name": "Watched with", "type": "text"}, {"name": "Rewatch value", "type": "number"},
        {"name": "Mood", "type": "choice", "choices": ["Fun", "Sad", "Fun"]}, {"name": "Cinema", "type": "bool"},
    ]}).json()["fields"]
    ids = {f["name"]: f["id"] for f in fields}
    assert fields[2]["choices"] == ["Fun", "Sad"]

    note = _save(api, "movie:tmdb:949", fields={
        ids["Watched with"]: "  Sam ", ids["Rewatch value"]: "8", ids["Mood"]: "Fun", ids["Cinema"]: "true", "ghost": "x",
    }).json()["note"]
    assert note["fields"] == {ids["Watched with"]: "Sam", ids["Rewatch value"]: 8, ids["Mood"]: "Fun", ids["Cinema"]: True}
    assert _save(api, "movie:tmdb:949", fields={ids["Mood"]: "Angry"}).json()["error"] == "invalid_field_value"

    renamed = api.post("/api/tracker/note-fields", json={"fields": [{**fields[0], "name": "With"}]}).json()["fields"]
    assert renamed == [{"id": ids["Watched with"], "name": "With", "type": "text"}]
    assert api.get("/api/tracker/summary").json()["note_fields"] == renamed


@pytest.mark.parametrize(("body", "error"), [
    ({"status": "binge"}, "invalid_status"),
    ({"started": "2026-02-30"}, "invalid_note_date"),
    ({"started": "2026-02-02", "finished": "2026-02-01"}, "finished_before_started"),
    ({"tags": [f"t{i}" for i in range(31)]}, "too_many_tags"),
    ({"tags": ["a,b"]}, "invalid_tags"),
    ({"notes": "x" * 10_001}, "notes_too_long"),
])
def test_bad_note_values_are_refused(api, body, error):
    res = _save(api, "movie:tmdb:949", **body)
    assert (res.status_code, res.json()["error"]) == (400, error)
    assert api.post("/api/tracker/note", json={"key": "nonsense", "status": "planned"}).json()["error"] == "invalid_note_key"


def test_clearing_every_field_removes_the_note_and_changes_can_be_undone(api, base):
    first = _save(api, "movie:tmdb:949", title="Heat", status="planned").json()
    _save(api, "movie:tmdb:949", title="Heat", status="completed")
    cleared = _save(api, "movie:tmdb:949", status="", tags=[], notes="  ").json()
    assert cleared["note"] is None and TrackerStore(base).note("movie:tmdb:949") is None

    log = api.get("/api/tracker/changes").json()["changes"]
    assert [(c["feature"], c["action"]) for c in log[:3]] == [("notes", "note")] * 3
    assert log[1]["items"][0]["fields"] == ["status"]
    assert log[0]["items"][0]["row"]["title"] == "Heat"

    api.post("/api/tracker/undo", json={"batch": cleared["batch"]})
    assert TrackerStore(base).note("movie:tmdb:949")["status"] == "completed"
    stale = _save(api, "movie:tmdb:949", status="dropped", expected=first["note"])
    assert (stale.status_code, stale.json()["error"]) == (409, "changed_since")
    assert _save(api, "movie:tmdb:949", status="completed").json()["batch"] is None


def test_fix_match_takes_the_note_along_and_undo_brings_it_back(api, base, monkeypatch):
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    _save(api, "movie:tmdb:1", title="Arrival", status="completed", tags=["Sci-fi"])
    fixed = api.post("/api/tracker/replace", json={
        "feature": "history", "key": "tmdb:1",
        "item": {"type": "movie", "title": "Arrival", "year": 2016, "ids": {"tmdb": "329865"}},
    }).json()
    store = TrackerStore(base)
    assert store.note("movie:tmdb:1") is None
    moved = store.note("movie:tmdb:329865")
    assert (moved["status"], moved["tags"], moved["tmdb"]) == ("completed", ["Sci-fi"], "329865")
    head = api.get("/api/tracker/changes").json()["changes"][0]
    assert (head["feature"], head["count"]) == ("history", 1)

    api.post("/api/tracker/undo", json={"batch": fixed["batch"]})
    assert store.note("movie:tmdb:329865") is None and store.note("movie:tmdb:1")["status"] == "completed"


def test_fix_match_of_one_episode_leaves_the_show_note(api, base, monkeypatch):
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    TrackerStore(base).write_state("history", {
        "tmdb:949": MOVIE, "tmdb:95396#s01e02": EPISODE,
        "tmdb:95396#s01e03": {**EPISODE, "title": "S01E03", "episode": 3},
    })
    _save(api, "show:tmdb:95396", title="Severance", status="watching")
    api.post("/api/tracker/replace", json={
        "feature": "history", "key": "tmdb:95396#s01e03",
        "item": {**EPISODE, "series_title": "Other", "show_ids": {"tmdb": "1"}, "episode": 3},
    })
    store = TrackerStore(base)
    assert store.note("show:tmdb:95396")["status"] == "watching" and store.note("show:tmdb:1") is None


def test_notes_survive_tracker_export_and_import(api, base, tmp_path):
    api.post("/api/tracker/note-fields", json={"fields": [{"name": "Mood", "type": "text"}]})
    _save(api, "movie:tmdb:949", title="Heat", status="completed", tags=["Crime"])
    other = TrackerStore(tmp_path / "copy")
    stats = other.import_zip(io.BytesIO(TrackerStore(base).export_zip()))
    assert stats["notes"] == 1
    assert other.note("movie:tmdb:949")["tags"] == ["Crime"]
    assert other.note_fields()[0]["name"] == "Mood"
