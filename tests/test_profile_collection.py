from pathlib import Path

from api import profileAPI


def _baseline(items: dict) -> dict:
    return {"collection": {"baseline": {"items": items}}}


def test_profile_collection_merges_owned_sources_for_user_profile() -> None:
    state = {
        "providers": {
            "PLEX": {
                "instances": {
                    "PLEX-P01": _baseline({
                        "tmdb:10": {
                            "type": "movie",
                            "title": "Owned Movie",
                            "year": 2026,
                            "ids": {"tmdb": "10"},
                            "collected_at": "2026-01-01T00:00:00Z",
                            "library_title": "Movies",
                        }
                    })
                }
            },
            "MDBLIST": _baseline({
                "tmdb:10": {
                    "type": "movie",
                    "title": "Owned Movie",
                    "year": 2026,
                    "ids": {"tmdb": "10"},
                    "collected_at": "2026-02-01T00:00:00Z",
                }
            }),
            "TRAKT": _baseline({
                "tmdb:99": {
                    "type": "movie",
                    "title": "Other Profile",
                    "ids": {"tmdb": "99"},
                }
            }),
        }
    }
    cfg = {
        "plex": {"instances": {"PLEX-P01": {"label": "Plex"}}},
        "mdblist": {},
        "user_profiles": {
            "alice": {"label": "Alice", "instances": {"PLEX": ["PLEX-P01"], "MDBLIST": ["default"]}},
        }
    }

    payload = profileAPI.build_profile_collection_payload(state, cfg, profile_id="alice")

    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["title"] == "Owned Movie"
    assert item["owned_instance_count"] == 2
    assert item["providers"] == ["mdblist", "plex"]
    assert item["sources_by_provider"] == {"plex": ["PLEX-P01"], "mdblist": ["default"]}
    assert item["first_collected_at"] == "2026-01-01T00:00:00Z"
    assert item["last_collected_at"] == "2026-02-01T00:00:00Z"
    assert item["libraries"] == ["Movies"]
    assert payload["counts"]["movie"] == 1


def test_profile_collection_ui_assets_are_registered() -> None:
    root = Path(__file__).resolve().parents[1]
    html = (root / "ui_frontend.py").read_text(encoding="utf-8")
    js = (root / "assets/js/profile-page.js").read_text(encoding="utf-8")
    css = (root / "assets/css/profile-page.css").read_text(encoding="utf-8")

    assert 'data-profile-tab="collection"' in html
    assert 'id="profile-panel-collection"' in html
    assert "Your unified library across all providers" in html
    assert 'data-collection-view="grid"' in html
    assert 'data-collection-view="list"' in html
    assert 'id="profile-collection-page-size"' in html
    assert "/assets/helpers/icon-select.js" in html
    assert "/api/profile/collection" in js
    assert "IconSelect.enhance" in js
    assert "last_collected_at" in js
    assert "cw-profile-collection-own" not in js
    assert ".cw-collection-grid" in css


def test_profile_collection_index_is_cached_per_fingerprint() -> None:
    state = {
        "providers": {
            "PLEX": _baseline({
                "tmdb:1": {"type": "movie", "title": "One", "ids": {"tmdb": "1"}},
                "tmdb:2": {"type": "show", "title": "Two", "ids": {"tmdb": "2"}},
            })
        }
    }
    loads = []

    def loader():
        loads.append(1)
        return state

    first = profileAPI.build_profile_collection_payload(loader, {}, cache_key=("unit", 1))
    second = profileAPI.build_profile_collection_payload(loader, {}, cache_key=("unit", 1), sort="title")

    assert first["total"] == 2
    assert second["total"] == 2
    assert len(loads) == 1

    profileAPI.build_profile_collection_payload(loader, {}, cache_key=("unit", 2))
    assert len(loads) == 2

    uncached = profileAPI.build_profile_collection_payload(state, {})
    assert uncached["total"] == 2


def test_profile_collection_cache_does_not_leak_sorts_between_requests() -> None:
    state = {
        "providers": {
            "PLEX": _baseline({
                "tmdb:1": {"type": "movie", "title": "Bravo", "ids": {"tmdb": "1"}, "collected_at": "2026-01-01T00:00:00Z"},
                "tmdb:2": {"type": "movie", "title": "Alpha", "ids": {"tmdb": "2"}, "collected_at": "2026-02-01T00:00:00Z"},
            })
        }
    }
    key = ("unit-sort", 1)

    by_title = profileAPI.build_profile_collection_payload(state, {}, cache_key=key, sort="title")
    by_date = profileAPI.build_profile_collection_payload(state, {}, cache_key=key, sort="collected_at")

    assert [item["title"] for item in by_title["items"]] == ["Alpha", "Bravo"]
    assert [item["title"] for item in by_date["items"]] == ["Alpha", "Bravo"]
    assert by_date["counts"]["all"] == 2

    filtered = profileAPI.build_profile_collection_payload(state, {}, cache_key=key, search="alpha")
    assert filtered["total"] == 1
    assert profileAPI.build_profile_collection_payload(state, {}, cache_key=key)["total"] == 2


def test_profile_collection_keeps_show_ids_for_episodes() -> None:
    state = {
        "providers": {
            "TRAKT": _baseline({
                "tmdb:7461052": {
                    "type": "episode",
                    "title": "Pilot",
                    "series_title": "Lioness",
                    "season": 3,
                    "episode": 4,
                    "show_ids": {"tmdb": "125988", "tvdb": "404126", "imdb": "tt13111078"},
                    "ids": {"tmdb": "7461052", "trakt": "999"},
                    "collected_at": "2026-08-23T00:00:00Z",
                }
            })
        }
    }

    item = profileAPI.build_profile_collection_payload(state, {})["items"][0]

    assert item["show_ids"]["tmdb"] == "125988"
    assert item["ids"]["tmdb_show"] == "125988"
    assert item["ids"]["imdb_show"] == "tt13111078"
    assert item["ids"]["tvdb_show"] == "404126"
    assert item["ids"]["tmdb"] == "7461052"


def test_profile_collection_movies_keep_plain_ids() -> None:
    state = {"providers": {"TRAKT": _baseline({
        "tmdb:550": {"type": "movie", "title": "Fight Club", "year": 1999, "ids": {"tmdb": "550"}},
    })}}

    item = profileAPI.build_profile_collection_payload(state, {})["items"][0]

    assert item["ids"] == {"tmdb": "550"}
    assert "tmdb_show" not in item["ids"]


def test_profile_page_uses_show_ids_for_episode_art() -> None:
    js = Path(__file__).resolve().parents[1].joinpath("assets/js/profile-page.js").read_text(encoding="utf-8")

    assert "const showTmdbId = (item)" in js
    assert "const id = episode ? showTmdbId(item) : tmdbId(item);" in js
