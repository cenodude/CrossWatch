# tests/test_tracker_filters.py
# CrossWatch - Tracker title filters across lists, shows and the timeline
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import pytest

from api import trackerAPI
from cw_platform.tracker_store import FEATURES, TrackerStore
from test_tracker_notes import api, base, _save, MOVIE, EPISODE, WRONG


@pytest.fixture
def filtered(api, base):
    store = TrackerStore(base)
    items = {
        "movie": MOVIE,
        "episode": {**EPISODE, "ids": {"tmdb": "949"}},
        "rewatch": {**EPISODE, "watched_at": "2026-04-01T00:00:00.000Z"},
        "other": WRONG,
        "same-number": {**MOVIE, "title": "Different movie", "ids": {"tmdb": "95396"}},
    }
    for feature in FEATURES:
        store.write_state(feature, items)
    _save(api, "movie:tmdb:949", title="Heat", status="completed", tags=["Crime"])
    _save(api, "show:tmdb:95396", title="Severance", status="dropped", tags=["Sci-fi"])
    _save(api, "movie:tmdb:95396", title="Different movie", status="watching")
    return store


@pytest.mark.parametrize("feature", FEATURES)
def test_status_and_tags_filter_every_list_before_pagination(api, filtered, feature):
    params = {"feature": feature, "status": "dropped", "tag": "sCI-Fi", "limit": 1, "sort": "oldest"}
    first = api.get("/api/tracker/items", params=params).json()
    assert first["total"] == 2
    assert [r["key"] for r in first["items"]] == ["episode"]
    second = api.get("/api/tracker/items", params={**params, "offset": 1}).json()
    assert second["total"] == 2
    assert [r["key"] for r in second["items"]] == ["rewatch"]
    assert api.get("/api/tracker/items", params={**params, "media": "movie"}).json()["total"] == 0
    assert api.get("/api/tracker/items", params={**params, "q": "missing"}).json()["total"] == 0
    assert api.get("/api/tracker/items", params={"feature": feature, "status": "watching"}).json()["total"] == 1


def test_no_status_includes_absent_notes_and_notes_without_status(api, filtered):
    params = {"status": "none"}
    assert [r["key"] for r in api.get("/api/tracker/items", params=params).json()["items"]] == ["other"]
    _save(api, "movie:tmdb:1", title="Arrival", notes="A note", tags=["Later"])
    assert api.get("/api/tracker/items", params={**params, "tag": "Later"}).json()["total"] == 1
    assert api.get("/api/tracker/notes", params=params).json()["total"] == 1
    assert api.get("/api/tracker/items", params={**params, "tag": "Crime"}).json()["total"] == 0


def test_month_counts_follow_status_and_tag_filters_and_note_edits_invalidate_cache(api, filtered):
    params = {"status": "dropped", "tag": "Sci-fi"}
    expected = [{"month": "2026-04", "count": 1}, {"month": "2026-03", "count": 1}]
    assert api.get("/api/tracker/months", params=params).json()["months"] == expected
    items = api.get("/api/tracker/items", params={**params, "month": "2026-03"}).json()
    assert items["total"] == 1 and items["items"][0]["key"] == "episode"
    _save(api, "show:tmdb:95396", title="Severance", status="completed", tags=["Sci-fi"])
    assert api.get("/api/tracker/months", params=params).json()["months"] == []
    assert api.get("/api/tracker/items", params=params).json()["total"] == 0


def test_shows_filter_before_pagination_and_explicit_status_overrides_hide_dropped(api, filtered):
    params = {"status": "dropped", "tag": "Sci-fi", "hide_dropped": True, "q": "Severance S01E02"}
    data = api.get("/api/tracker/shows", params=params).json()
    assert data["total"] == 1
    assert data["shows"][0]["plays"] == 2
    assert api.get("/api/tracker/shows", params={**params, "status": "completed"}).json()["total"] == 0
    assert api.get("/api/tracker/shows", params={"hide_dropped": True}).json()["total"] == 0
    assert api.get("/api/tracker/shows", params={"status": "none"}).json()["total"] == 0
    filtered.save_note("show:tmdb:95396", None)
    assert api.get("/api/tracker/shows", params={"status": "none"}).json()["total"] == 1


@pytest.mark.parametrize("path", ["items", "months", "shows", "notes"])
def test_invalid_status_is_rejected(api, path):
    response = api.get(f"/api/tracker/{path}", params={"status": "invalid"})
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_status"


def test_no_status_filter_value_cannot_be_saved_as_a_title_status(api):
    assert _save(api, "movie:tmdb:949", title="Heat", status="none").status_code == 400


@pytest.mark.parametrize("key,item", [
    ("movie", MOVIE),
    ("episode", EPISODE),
    ("season", {**EPISODE, "type": "season"}),
    ("show", {"type": "show", "title": "Show", "ids": {"tmdb": "20"}}),
    ("anime", {"type": "anime", "title": "Anime", "ids": {"tmdb": "20"}}),
    ("show-title", {"type": "show", "title": "A Show", "series_title": "Other", "ids": {}}),
    ("unicode-title", {"type": "show", "title": "ÉCOLE", "ids": {}}),
    ("episode-title", {"type": "episode", "series_title": "A Show", "title": "Pilot", "ids": {"tmdb": "949"}}),
    ("imdb", {"type": "movie", "title": "Movie", "ids": {"imdb": "tt12345"}}),
    ("fallback@2026-01-01", {"type": "movie", "title": "Movie", "ids": {}}),
])
def test_filter_identity_matches_the_title_details_identity(tmp_path, key, item):
    store = TrackerStore(tmp_path)
    store.write_state("history", {key: item, "unrelated": {"type": "movie", "title": "Unrelated", "ids": {"tmdb": "999"}}})
    note_key = trackerAPI._title_key(trackerAPI._identity_of(item, key))
    store.save_note(note_key, {"type": "show" if note_key.startswith("show:") else "movie", "title": "A title", "status": "dropped", "tags": ["Keep"]})
    total, rows = store.query("history", status="dropped", tag="keep")
    assert total == 1 and [k for k, _ in rows] == [key]
    total, rows = store.query("history", status="none")
    assert total == 1 and [k for k, _ in rows] == ["unrelated"]


def test_duplicate_filter_keeps_rewatches_types_and_episode_coordinates_separate(api, base):
    store = TrackerStore(base)
    entries = {
        "movie-a": MOVIE,
        "movie-b": {**MOVIE, "ids": {"tmdb": "949"}},
        "rewatch": {**MOVIE, "watched_at": "2026-05-01T00:00:00.000Z"},
        "episode-a": EPISODE,
        "episode-b": {**EPISODE, "ids": {"tmdb": "949"}},
        "next-episode": {**EPISODE, "episode": 3},
        "same-number": {**MOVIE, "ids": {"tmdb": "95396"}, "watched_at": EPISODE["watched_at"]},
        "undated-a": {**MOVIE, "watched_at": None},
        "undated-b": {**MOVIE, "watched_at": None},
    }
    store.write_state("history", entries)
    before = store.read_current("history")
    params = {"duplicates": True, "limit": 2, "sort": "title"}
    first = api.get("/api/tracker/items", params=params).json()
    second = api.get("/api/tracker/items", params={**params, "offset": 2}).json()
    assert first["total"] == second["total"] == 4
    assert {r["key"] for r in first["items"] + second["items"]} == {"movie-a", "movie-b", "episode-a", "episode-b"}
    assert api.get("/api/tracker/months", params={"duplicates": True}).json()["months"] == [
        {"month": "2026-03", "count": 2}, {"month": "2026-01", "count": 2}]
    _save(api, "movie:tmdb:949", title="Heat", status="completed", tags=["Crime"])
    assert api.get("/api/tracker/items", params={**params, "status": "completed", "tag": "Crime"}).json()["total"] == 2
    assert store.read_current("history") == before
