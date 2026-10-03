from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import publishedListsAPI as api
from services import published_lists as svc

ITEMS = [
    {"type": "movie", "title": "Heat", "ids": {"imdb": "tt0113277", "tmdb": "949"}},
    {"type": "movie", "title": "No IMDb", "ids": {"tmdb": "12"}},
    {"type": "movie", "title": "Local only", "ids": {"plex": "5"}},
    {"type": "show", "title": "MobLand", "ids": {"tvdb": "443311", "tmdb": "247718"}},
    {"type": "anime", "title": "Bebop", "ids": {"imdb": "tt0213338"}},
    {"type": "episode", "title": "Pilot", "series_title": "MobLand", "season": 1, "episode": 1,
     "ids": {"tvdb": "999"}, "show_ids": {"tvdb": "443311"}},
]


def test_formats_pick_safe_ids_and_split_movies_from_shows() -> None:
    assert svc.format_items(ITEMS, "kometa-movies.json") == ([{"imdb_id": "tt0113277"}, {"tmdb_id": 12}], 1)
    assert svc.format_items(ITEMS, "kometa-shows.json") == ([{"tvdb_id": 443311}, {"imdb_id": "tt0213338"}], 0)
    assert svc.format_items(ITEMS, "radarr.json") == ([{"title": "Heat", "imdb_id": "tt0113277"}], 2)
    assert svc.format_items(ITEMS, "sonarr.json") == ([{"title": "MobLand", "tvdbId": 443311}], 1)


def test_publish_is_stable_per_list_and_a_new_key_replaces_the_old_one(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(svc, "_path", lambda: tmp_path / "published_lists.json")
    feed = svc.publish("default", "cwpl_1")
    assert svc.publish("default", "cwpl_1")["id"] == feed["id"]
    assert svc.publish("p2", "cwpl_1")["id"] != feed["id"]
    old_key = feed["key"]
    assert old_key not in (tmp_path / "published_lists.json").read_text("utf-8")
    assert svc.authorized(feed["id"], old_key)
    assert not svc.authorized(feed["id"], "wrong")
    new_key = svc.rotate(feed["id"])["key"]
    assert new_key != old_key
    assert not svc.authorized(feed["id"], old_key)
    assert svc.unpublish(feed["id"])
    assert not svc.authorized(feed["id"], new_key)


def test_public_address_needs_the_key_and_records_the_read(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(svc, "_path", lambda: tmp_path / "published_lists.json")
    monkeypatch.setattr(api, "load_config", lambda: {})
    monkeypatch.setattr(svc, "list_items", lambda cfg, feed: ITEMS)
    app = FastAPI()
    app.include_router(api.public_router)
    client = TestClient(app)
    feed = svc.publish("default", "cwpl_1")
    base = f"/published/{feed['id']}"
    assert client.get(f"{base}/sonarr.json").status_code == 404
    assert client.get(f"{base}/sonarr.json?key=nope").status_code == 404
    assert client.get(f"{base}/other.json?key={feed['key']}").status_code == 404
    res = client.get(f"{base}/sonarr.json?key={feed['key']}")
    assert res.status_code == 200
    assert res.json() == [{"title": "MobLand", "tvdbId": 443311}]
    assert res.headers["x-crosswatch-skipped"] == "1"
    stored = svc.feeds()[feed["id"]]
    assert stored["fetch_count"] == 1
    assert stored["last_fetch_format"] == "sonarr.json"
