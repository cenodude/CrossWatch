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


MIXED = [
    {"type": "movie", "title": "Heat", "year": 1995, "ids": {"imdb": "tt0113277", "tmdb": "949"}},
    {"type": "show", "title": "MobLand", "ids": {"tvdb": "443311", "tmdb": "247718"}},
    {"type": "movie", "title": "Local only", "ids": {"imdb": "tt0000001"}},
    {"type": "anime", "title": "Bebop", "ids": {"tmdb": "30991"}},
    {"type": "season", "title": "Specials", "series_title": "MobLand", "season": 0, "show_ids": {"tmdb": "247718"}},
    {"type": "episode", "title": "Pilot", "series_title": "MobLand", "season": 1, "episode": 1,
     "ids": {"tmdb": "999"}, "show_ids": {"tmdb": "247718"}},
    {"type": "movie", "title": "Heat again", "ids": {"tmdb": "949"}},
]


def test_mdblist_json_mixes_movies_and_shows_with_tmdb_ids_in_list_order() -> None:
    rows, skipped = svc.format_items(MIXED, "mdblist.json")
    assert skipped == 1
    assert rows == [
        {"id": 949, "rank": 1, "title": "Heat", "imdb_id": "tt0113277", "mediatype": "movie", "release_year": 1995},
        {"id": 247718, "rank": 2, "title": "MobLand", "tvdbid": 443311, "mediatype": "show"},
        {"id": 30991, "rank": 3, "title": "Bebop", "mediatype": "show"},
        {"id": 247718, "rank": 4, "title": "MobLand", "mediatype": "season", "season": 0},
        {"id": 247718, "rank": 5, "title": "MobLand", "mediatype": "episode", "season": 1, "episode": 1},
    ]


def test_random_sort_shuffles_and_other_values_keep_list_order(monkeypatch) -> None:
    monkeypatch.setattr(svc.random, "shuffle", lambda rows: rows.reverse())
    assert svc.sort_rows([{"id": 1}, {"id": 2}], "random") == [{"id": 2}, {"id": 1}]
    assert svc.sort_rows([{"id": 1}, {"id": 2}], "") == [{"id": 1}, {"id": 2}]
    assert svc.sort_rows([{"id": 1}, {"id": 2}], "title") == [{"id": 1}, {"id": 2}]


SORTABLE = [
    {"id": 3, "rank": 1, "title": "beta", "imdb_id": "tt10000000", "mediatype": "show", "release_year": 2001},
    {"id": 1, "rank": 2, "title": "Alpha", "tvdbid": 7, "mediatype": "episode", "season": 1, "episode": 2},
    {"id": 2, "rank": 3, "title": "Gamma", "imdb_id": "tt9000000", "tvdbid": 5, "mediatype": "movie", "release_year": 1999},
    {"id": 4, "rank": 4, "title": "alpha", "mediatype": "season", "season": 0},
]


def _sorted_ids(sort: str, order: str = "") -> list[int]:
    return [row["id"] for row in svc.sort_rows(list(SORTABLE), sort, order)]


def test_sort_uses_row_fields_and_order_flips_the_direction() -> None:
    assert _sorted_ids("title") == [1, 4, 3, 2]
    assert _sorted_ids("title", "desc") == [2, 3, 1, 4]
    assert _sorted_ids("TITLE", "DESC") == [2, 3, 1, 4]
    assert _sorted_ids("id") == [1, 2, 3, 4]
    assert _sorted_ids("id", "desc") == [4, 3, 2, 1]
    assert _sorted_ids("year") == _sorted_ids("release_year") == [2, 3, 1, 4]
    assert _sorted_ids("season") == [4, 1, 3, 2]


def test_sort_keeps_rows_without_the_field_last_in_list_order() -> None:
    assert _sorted_ids("year", "desc") == [3, 2, 1, 4]
    assert _sorted_ids("tvdbid", "desc") == [1, 2, 3, 4]


def test_sort_by_mediatype_runs_from_movies_to_episodes() -> None:
    assert _sorted_ids("mediatype") == _sorted_ids("media_type") == [2, 3, 4, 1]
    assert _sorted_ids("media_type", "desc") == [1, 4, 3, 2]


def test_sort_by_imdb_id_is_numeric() -> None:
    assert _sorted_ids("imdb_id") == [2, 3, 1, 4]
    rows = [{"imdb_id": "tt0000002"}, {"imdb_id": "tt00000001"}, {"imdb_id": "tt0000001"}, {"imdb_id": "tt10000000"}]
    assert svc.sort_rows(list(rows), "imdb_id") == [rows[1], rows[2], rows[0], rows[3]]
    assert svc.sort_rows(list(rows), "imdb_id", "desc") == [rows[3], rows[0], rows[1], rows[2]]
    odd = [{"imdb_id": "ttx"}, {"imdb_id": "tt"}, {"imdb_id": "tt1"}]
    assert svc.sort_rows(list(odd), "imdb_id") == [odd[1], odd[2], odd[0]]


def test_rank_means_list_position_on_formats_without_a_rank_key() -> None:
    rows = [{"tmdb_id": 1}, {"tmdb_id": 2}]
    assert svc.sort_rows(list(rows), "rank", "desc") == svc.sort_rows(list(rows), "", "desc") == [rows[1], rows[0]]


def test_order_without_a_sort_reverses_the_list_and_random_ignores_it(monkeypatch) -> None:
    assert _sorted_ids("", "desc") == _sorted_ids("rank", "desc") == [4, 2, 1, 3]
    assert _sorted_ids("", "asc") == _sorted_ids("rank") == [3, 1, 2, 4]
    assert _sorted_ids("nope", "desc") == [3, 1, 2, 4]
    monkeypatch.setattr(svc.random, "shuffle", lambda rows: rows.reverse())
    assert _sorted_ids("random", "desc") == [4, 2, 1, 3]


def test_sort_finds_the_same_id_in_every_format() -> None:
    kometa = [{"imdb_id": "tt0000001"}, {"tmdb_id": 9}, {"tmdb_id": 2}]
    assert svc.sort_rows(kometa, "id", "desc") == [{"tmdb_id": 9}, {"tmdb_id": 2}, {"imdb_id": "tt0000001"}]
    sonarr = [{"title": "A", "tvdbId": 9}, {"title": "B", "tvdbId": 2}]
    assert svc.sort_rows(sonarr, "tvdbid") == [{"title": "B", "tvdbId": 2}, {"title": "A", "tvdbId": 9}]
    assert svc.sort_rows(sonarr, "tvdbId") == [{"title": "B", "tvdbId": 2}, {"title": "A", "tvdbId": 9}]
    shows = [{"tvdb_id": 9}, {"imdb_id": "tt0000001"}, {"tvdb_id": 2}]
    assert svc.sort_rows(shows, "tvdb_id") == [{"tvdb_id": 2}, {"tvdb_id": 9}, {"imdb_id": "tt0000001"}]


def _filtered_titles(mediatype: str) -> list[str]:
    return [item["title"] for item in svc.filter_items(MIXED, mediatype)]


def test_mediatype_filter_keeps_named_types_and_ignores_unknown_values() -> None:
    assert _filtered_titles("movie") == ["Heat", "Local only", "Heat again"]
    assert _filtered_titles(" SHOW ") == ["MobLand", "Bebop"]
    assert _filtered_titles("season,episode") == ["Specials", "Pilot"]
    assert _filtered_titles("nope,episode") == ["Pilot"]
    assert _filtered_titles("nope") == _filtered_titles("") == [item["title"] for item in MIXED]


def test_page_rows_slices_by_offset_and_limit_and_ignores_bad_values() -> None:
    rows = [{"id": n} for n in range(1, 6)]
    assert svc.page_rows(rows, "1", "2") == [{"id": 2}, {"id": 3}]
    assert svc.page_rows(rows, "3", "") == [{"id": 4}, {"id": 5}]
    assert svc.page_rows(rows, "", "2") == [{"id": 1}, {"id": 2}]
    assert svc.page_rows(rows, "9", "2") == []
    assert svc.page_rows(rows, "-1", "abc") == rows
    assert svc.page_rows(rows, "x", "0") == rows


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
    monkeypatch.setattr(svc.random, "shuffle", lambda rows: rows.reverse())
    mixed = client.get(f"{base}/mdblist.json?key={feed['key']}&sort=random")
    assert mixed.status_code == 200
    assert [(row["id"], row["mediatype"]) for row in mixed.json()] == [(247718, "show"), (12, "movie"), (949, "movie")]
    assert svc.feeds()[feed["id"]]["last_fetch_format"] == "mdblist.json"
    monkeypatch.setattr(svc, "list_items", lambda cfg, feed: MIXED)
    paged = client.get(f"{base}/mdblist.json?key={feed['key']}&offset=1&limit=2")
    assert paged.status_code == 200
    assert [(row["rank"], row["mediatype"]) for row in paged.json()] == [(2, "show"), (3, "show")]
    assert paged.headers["x-total-items"] == "5"
    assert client.get(f"{base}/mdblist.json?key={feed['key']}&offset=abc&limit=-3").status_code == 200
    shuffled = client.get(f"{base}/mdblist.json?key={feed['key']}&sort=random&limit=2")
    assert [row["rank"] for row in shuffled.json()] == [5, 4]
    assert shuffled.headers["x-total-items"] == "5"
    titled = client.get(f"{base}/mdblist.json?key={feed['key']}&sort=title&order=desc&limit=1")
    assert [row["title"] for row in titled.json()] == ["MobLand"]
    assert titled.headers["x-total-items"] == "5"
    reversed_list = client.get(f"{base}/mdblist.json?key={feed['key']}&order=desc&limit=1")
    assert [row["rank"] for row in reversed_list.json()] == [5]
    movies = client.get(f"{base}/mdblist.json?key={feed['key']}&mediatype=movie")
    assert [(row["id"], row["rank"]) for row in movies.json()] == [(949, 1)]
    assert movies.headers["x-total-items"] == "1"
    assert movies.headers["x-crosswatch-skipped"] == "1"
    both = client.get(f"{base}/mdblist.json?key={feed['key']}&mediatype=season,episode&sort=mediatype&order=desc")
    assert [row["mediatype"] for row in both.json()] == ["episode", "season"]
    assert client.get(f"{base}/kometa-movies.json?key={feed['key']}&mediatype=show").json() == []
    monkeypatch.setattr(svc, "list_items", lambda cfg, feed: ITEMS)
    kometa = client.get(f"{base}/kometa-movies.json?key={feed['key']}&offset=1&limit=1")
    assert kometa.json() == [{"tmdb_id": 12}]
    assert kometa.headers["x-total-items"] == "2"


class _Lists:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    def list_playlist_resources(self, cfg, instance="default"):
        from cw_platform.playlists import PlaylistResource

        return [PlaylistResource(provider="CROSSWATCH", id=lid, name=lid, instance=instance)
                for lid in ("cwpl_1", "cwpl_2") if lid not in self.deleted]

    def delete_playlist(self, cfg, playlist_id, instance="default"):
        self.deleted.append(playlist_id)
        return {"ok": True}


def test_local_lists_report_delete_rights_and_endpoint_use(monkeypatch) -> None:
    monkeypatch.setattr(svc, "_ops", lambda: _Lists())
    monkeypatch.setattr(svc, "list_instance_ids", lambda cfg, provider: ["default"])
    monkeypatch.setattr(svc, "build_provider_config_view", lambda cfg, provider, instance: {})
    cfg = {"playlists": {"endpoints": [
        {"id": "ep1", "provider": "CROSSWATCH", "instance": "default", "playlist_id": "cwpl_1"},
        {"id": "ep2", "provider": "TRAKT", "instance": "default", "playlist_id": "cwpl_2"},
    ]}}
    rows = {row["list_id"]: row for row in svc.local_lists(cfg)}
    assert rows["cwpl_1"]["can_delete"] and rows["cwpl_1"]["endpoint_count"] == 1
    assert rows["cwpl_2"]["can_delete"] and rows["cwpl_2"]["endpoint_count"] == 0


def test_deleting_a_crosswatch_list_drops_its_published_feed(tmp_path, monkeypatch) -> None:
    from services import playlists

    ops = _Lists()
    monkeypatch.setattr(svc, "_path", lambda: tmp_path / "published_lists.json")
    monkeypatch.setattr(playlists, "_providers", lambda: {"CROSSWATCH": ops})
    monkeypatch.setattr(playlists, "supports_playlists", lambda ops: True)
    monkeypatch.setattr(playlists, "build_provider_config_view", lambda cfg, provider, instance: {})
    gone = svc.publish("default", "cwpl_2")
    kept = svc.publish("default", "cwpl_1")
    assert playlists.delete_provider_playlist({}, "CROSSWATCH", "default", "cwpl_2")["ok"]
    assert ops.deleted == ["cwpl_2"]
    assert set(svc.feeds()) == {kept["id"]}
    assert not svc.authorized(gone["id"], gone["key"])
    assert not svc.unpublish_list("default", "cwpl_2")
