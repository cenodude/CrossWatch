from __future__ import annotations

from typing import Any

import pytest

from api import metaAPI, profileAPI
from providers.metadata._meta_TMDB import TmdbProvider
from services import profile_history

PLEX = {"provider": "PLEX", "instance": "default"}
TRAKT = {"provider": "TRAKT", "instance": "default"}

DETAILS: dict[str, Any] = {
    "name": "Game of Thrones",
    "first_air_date": "2011-04-17",
    "episode_run_time": [60],
    "vote_average": 8.4,
    "vote_count": 100,
    "seasons": [
        {"season_number": 0, "name": "Specials", "episode_count": 3, "air_date": "2010-12-05", "poster_path": "/s0.jpg"},
        {"season_number": 1, "name": "Season 1", "episode_count": 10, "air_date": "2011-04-17", "poster_path": "/s1.jpg"},
    ],
    "created_by": [{"id": 9813, "name": "David Benioff", "profile_path": "/db.jpg"}],
    "credits": {
        "cast": [{"id": 22970, "name": "Peter Dinklage", "character": "Tyrion Lannister", "profile_path": "/pd.jpg"}],
        "crew": [
            {"id": 1, "name": "Ramin Djawadi", "job": "Original Music Composer"},
            {"id": 2, "name": "Someone", "job": "Gaffer"},
        ],
    },
    "recommendations": {
        "results": [
            {"id": 1402, "media_type": "tv", "name": "The Walking Dead", "first_air_date": "2010-10-31", "poster_path": "/wd.jpg", "vote_average": 8.1},
        ]
    },
}


def test_tmdb_fetch_adds_modal_sections_with_one_details_call(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []
    provider = TmdbProvider(lambda: {}, lambda _cfg: None)

    def fake_get(url, params=None, **_kwargs):
        calls.append((url, dict(params or {})))
        return DETAILS

    monkeypatch.setattr(provider, "_get", fake_get)
    out = provider.fetch(
        entity="tv",
        ids={"tmdb": "1399"},
        locale="nl-NL",
        need={"credits": True, "recommendations": True},
    )

    assert calls == [
        (
            "https://api.themoviedb.org/3/tv/1399",
            {"language": "nl-NL", "append_to_response": "credits,recommendations"},
        )
    ]
    assert out["detail"]["seasons"][1] == {"season": 1, "name": "Season 1", "episode_count": 10, "air_date": "2011-04-17", "poster_path": "/s1.jpg"}
    assert out["credits"]["cast"][0]["character"] == "Tyrion Lannister"
    assert [row["job"] for row in out["credits"]["crew"]] == ["Creator", "Original Music Composer"]
    assert out["recommendations"][0] == {
        "id": 1402, "type": "tv", "title": "The Walking Dead", "year": 2010,
        "poster_path": "/wd.jpg", "backdrop_path": "", "vote_average": 8.1,
    }
    assert "watch_providers" not in out


def test_tmdb_fetch_skips_appends_when_not_requested(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    provider = TmdbProvider(lambda: {}, lambda _cfg: None)
    monkeypatch.setattr(provider, "_get", lambda url, params=None, **_k: calls.append(dict(params or {})) or DETAILS)

    out = provider.fetch(entity="tv", ids={"tmdb": "1399"}, locale="en-US", need={"genres": True})

    assert calls == [{"language": "en-US"}]
    assert "credits" not in out and "recommendations" not in out


def test_cached_metadata_needs_the_modal_sections() -> None:
    need = {"credits": True, "recommendations": True, "seasons": True}

    assert not metaAPI._need_satisfied({"type": "tv", "detail": {}}, need)
    assert metaAPI._need_satisfied({"type": "tv", "credits": {}, "recommendations": [], "detail": {"seasons": []}}, need)
    assert metaAPI._need_satisfied({"type": "movie", "credits": {}, "recommendations": [], "detail": {}}, need)


def test_tmdb_image_proxy_only_accepts_tmdb_paths(tmp_path, monkeypatch) -> None:
    downloads: list[str] = []
    monkeypatch.setattr(metaAPI, "_cache_download", lambda url, dest, timeout=15.0: (downloads.append(url), (dest, "image/jpeg"))[1])

    local, mime = metaAPI.get_tmdb_image_file("/pdAbC12.jpg", "w185", tmp_path)

    assert downloads == ["https://image.tmdb.org/t/p/w185/pdAbC12.jpg"]
    assert mime == "image/jpeg"
    assert local.endswith(".jpg")
    png, _ = metaAPI.get_tmdb_image_file("/logoXyZ9.PNG", "w92", tmp_path)
    assert png.endswith(".png")
    assert downloads[-1] == "https://image.tmdb.org/t/p/w92/logoXyZ9.PNG"
    for bad in ("../etc/passwd", "/a/../bcdef.jpg", "https://x.y/zzzz.jpg", "/abcdef.gif", "abcdef.jpg"):
        with pytest.raises(ValueError):
            metaAPI.get_tmdb_image_file(bad, "w185", tmp_path)
    with pytest.raises(ValueError):
        metaAPI.get_tmdb_image_file("/pdAbC12.jpg", "h9999", tmp_path)


def _watchlist_index() -> dict[str, Any]:
    items = [
        {"key": "tmdb:11", "type": "movie", "title": "Star Wars", "ids": {"tmdb": 11}, "sources_by_provider": {"plex": ["default"], "trakt": ["default"]}, "added_epoch": 1788220800},
        {"key": "tmdb:1399", "type": "tv", "title": "Game of Thrones", "ids": {"tmdb": 1399}, "sources_by_provider": {"trakt": ["default"]}, "added_epoch": 1788220000},
    ]
    return profile_history.build_history_index("watchlist", state=items)


def test_title_presence_finds_movies_and_shows_by_tmdb_id() -> None:
    index = _watchlist_index()

    movie = profile_history.title_presence(index, "movie", 11)
    show = profile_history.title_presence(index, "show", "1399")

    assert movie is not None and movie["present"] == [PLEX, TRAKT] and movie["missing"] == []
    assert show is not None and show["present"] == [TRAKT] and show["missing"] == [PLEX]
    assert profile_history.title_presence(index, "movie", 1399) is None


def test_title_presence_collects_episode_plays_for_a_show() -> None:
    index: dict[str, Any] = {
        "source": "synced",
        "endpoints": [("PLEX", "default"), ("TRAKT", "default")],
        "rows": [
            {"key": "a", "_type": "episode", "ids": {"tmdb": 55, "show_ids": {"tmdb": 1399}}, "season": 1, "episode": 2, "sort_epoch": 200, "_endpoints": [("TRAKT", "default")]},
            {"key": "b", "_type": "episode", "ids": {"tmdb": 54, "show_ids": {"tmdb": 1399}}, "season": 1, "episode": 1, "sort_epoch": 100, "_endpoints": [("PLEX", "default")]},
        ],
    }

    result = profile_history.title_presence(index, "show", 1399)

    assert result is not None
    assert result["count"] == 2 and result["last_epoch"] == 200
    assert result["present"] == [TRAKT, PLEX] and result["missing"] == []
    assert result["episodes"] == [{"season": 1, "episode": 1, "epoch": 100}, {"season": 1, "episode": 2, "epoch": 200}]
    assert profile_history.title_presence(index, "show", 55) is None


def test_title_presence_reports_each_provider_rating() -> None:
    index: dict[str, Any] = {
        "source": "ratings",
        "endpoints": [("PLEX", "default"), ("TRAKT", "default")],
        "rows": [
            {"key": "tmdb:11", "_type": "movie", "ids": {"tmdb": 11}, "rating": 8, "sort_epoch": 50, "_endpoints": [("PLEX", "default"), ("TRAKT", "default")], "_ratings": {("PLEX", "default"): 8, ("TRAKT", "default"): 7}, "_mismatch": True},
        ],
    }

    result = profile_history.title_presence(index, "movie", 11)

    assert result is not None
    assert result["rating"] == 8 and result["agree"] is False
    assert result["present"] == [{**PLEX, "rating": 8}, {**TRAKT, "rating": 7}]


def test_profile_title_payload_hides_watchlist_without_permission(monkeypatch) -> None:
    index = _watchlist_index()
    monkeypatch.setattr(profileAPI, "_profile_history_index", lambda _cfg, _profile, _source: (index, ""))
    monkeypatch.setattr(profileAPI, "_collection_index", lambda *_a, **_k: ([], {}, {}))

    hidden = profileAPI.build_profile_title_payload({}, media="movie", tmdb=11, watchlist=False)
    shown = profileAPI.build_profile_title_payload({}, media="movie", tmdb=11, watchlist=True)

    assert "watchlist" not in hidden
    assert shown["watchlist"]["count"] == 1
    assert shown["collection"] is None
