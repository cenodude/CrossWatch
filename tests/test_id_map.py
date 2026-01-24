# CrossWatch test scripts
from __future__ import annotations

from cw_platform.id_map import (
    canonical_key,
    ids_from_guid,
    ids_from_jellyfin_providerids,
    keys_for_item,
    merge_ids,
)


def test_ids_from_guid_common_patterns() -> None:
    assert ids_from_guid("com.plexapp.agents.imdb://tt1234567") == {"imdb": "tt1234567"}
    assert ids_from_guid("com.plexapp.agents.themoviedb://12345") == {"tmdb": "12345"}
    assert ids_from_guid("com.plexapp.agents.thetvdb://987") == {"tvdb": "987"}
    assert ids_from_guid("imdb://title/tt7654321") == {"imdb": "tt7654321"}
    assert ids_from_guid("tmdb://movie/550") == {"tmdb": "550"}
    assert ids_from_guid("tvdb://series/121361") == {"tvdb": "121361"}

    out = ids_from_guid("plex://movie/5d7769e8f")
    assert out.get("guid", "").startswith("plex://")


def test_ids_from_jellyfin_providerids_normalizes() -> None:
    ids = ids_from_jellyfin_providerids({"Imdb": "tt0012345", "Tmdb": " 550 ", "Tvdb": "tvdb-42"})
    assert ids == {"imdb": "tt0012345", "tmdb": "550", "tvdb": "42"}


def test_canonical_key_prefers_best_external_id() -> None:
    item = {"type": "movie", "title": "Fight Club", "year": 1999, "ids": {"tmdb": 550, "imdb": "tt0137523"}}
    assert canonical_key(item) == "imdb:tt0137523"


def test_canonical_key_episode_uses_show_id_and_se() -> None:
    item = {
        "type": "episode",
        "season": 1,
        "episode": 2,
        "show_ids": {"imdb": "tt0903747"},
        "title": "Cat's in the Bag...",
        "year": 2008,
    }
    assert canonical_key(item) == "imdb:tt0903747#s01e02"


def test_fallback_title_year_key_when_no_ids() -> None:
    item = {"type": "movie", "title": "Some Indie", "year": 2024}
    assert canonical_key(item) == "movie|title:some indie|year:2024"


def test_keys_for_item_contains_multiple_matching_tokens() -> None:
    item = {"type": "show", "title": "Arcane", "year": 2021, "ids": {"tmdb": "94605"}}
    keys = keys_for_item(item)
    assert "tmdb:94605" in keys
    assert "show|title:arcane|year:2021" in keys


def test_merge_ids_keeps_priority_and_fills_gaps() -> None:
    old = {"imdb": "tt0000001", "tmdb": None}
    new = {"tmdb": 123}
    merged = merge_ids(old, new)
    assert merged["imdb"] == "tt0000001"
    assert merged["tmdb"] == "123"
