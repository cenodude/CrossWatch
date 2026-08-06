# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.episodes import Resolution, resolve_absolute, resolution_matches_ids

MAPPINGS: dict[str, Any] = {
    "tvdb_show:81472:s1": {
        "anidb:1530:R": {"1-39": "1-39"},
        "anilist:813": {"1-39": "1-39"},
        "mal:813": {"1-39": "1-39"},
        "tmdb_show:12971:s1": {"1-39": "1-39"},
    },
    "tvdb_show:81472:s2": {
        "anilist:813": {"1-35": "40-74"},
        "mal:813": {"1-35": "40-74"},
    },
    "tvdb_show:267440:s4": {
        "anilist:131681": {"17-28": "1-12"},
        "tmdb_show:1429:s4": {"1-28": "1-28"},
    },
    "tvdb_show:267440:s0": {"anilist:18397": {"7": "1"}},
    "tvdb_show:555:s1": {"anilist:999": {"1-12": "1-12|2"}},
    "tvdb_show:700:s1": {
        "anidb:700:R": {"1-12": "2-13"},
        "anilist:700": {"1-12": "1-12"},
    },
    "tvdb_show:701:s1": {
        "anidb:701:R": {"1-12": "1-12"},
        "mal:701": {"1-12": "1-12"},
        "anilist:701": {"1-12": "1-12"},
    },
    "tvdb_show:702:s1": {"anidb:702:S": {"1-12": "1-12"}},
}


@pytest.fixture()
def index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return paths["db"]


def _episode(show_ids: dict[str, str], season: int, episode: int) -> dict[str, Any]:
    return {"type": "episode", "show_ids": show_ids, "season": season, "episode": episode}


def test_resolves_absolute_from_tvdb(index: Path) -> None:
    got = resolve_absolute(_episode({"tvdb": "81472"}, 2, 13))
    assert isinstance(got, Resolution)
    assert got.absolute == 52
    assert got.namespace == "mal"
    assert got.target_id == "813"
    assert got.entry == "tvdb_direct"


def test_prefers_anidb_when_available(index: Path) -> None:
    got = resolve_absolute(_episode({"tvdb": "81472"}, 1, 5))
    assert got is not None
    assert (got.absolute, got.namespace, got.target_id) == (5, "anidb", "1530")


def test_disagreeing_namespaces_are_refused(index: Path) -> None:
    assert resolve_absolute(_episode({"tvdb": "700"}, 1, 3)) is None


def test_agreeing_namespaces_are_accepted(index: Path) -> None:
    got = resolve_absolute(_episode({"tvdb": "701"}, 1, 3))
    assert got is not None
    assert got.absolute == 3


def test_anidb_specials_scope_not_used_for_regular_seasons(index: Path) -> None:
    assert resolve_absolute(_episode({"tvdb": "702"}, 1, 2)) is None


def test_resolves_split_cour_to_native_entry(index: Path) -> None:
    got = resolve_absolute(_episode({"tvdb": "267440"}, 4, 17))
    assert got is not None
    assert (got.absolute, got.target_id) == (1, "131681")


def test_tmdb_only_source_uses_show_pair_hop(index: Path) -> None:
    got = resolve_absolute(_episode({"tmdb": "12971"}, 2, 13))
    assert got is not None
    assert got.absolute == 52
    assert got.entry == "show_pair_hop"


def test_show_pairs_are_bidirectional(index: Path) -> None:
    assert storage.query_show_pair("v3", "tvdb", "81472") == "12971"
    assert storage.query_show_pair("v3", "tmdb", "12971") == "81472"
    assert storage.query_show_pair("v3", "tvdb", "404404") is None
    assert storage.query_show_pair("v3", "imdb", "tt1") is None


def test_specials_refused_unless_opted_in(index: Path) -> None:
    special = _episode({"tvdb": "267440"}, 0, 7)
    assert resolve_absolute(special) is None
    assert resolve_absolute(special, allow_specials=True) is not None


def test_ratio_edges_are_refused(index: Path) -> None:
    assert resolve_absolute(_episode({"tvdb": "555"}, 1, 3)) is None


@pytest.mark.parametrize(
    "item",
    [
        {"type": "episode", "show_ids": {}, "season": 1, "episode": 1},
        {"type": "episode", "show_ids": {"tvdb": "81472"}, "season": 2, "episode": 0},
        {"type": "episode", "show_ids": {"tvdb": "81472"}, "season": 2, "episode": 99},
        {"type": "episode", "show_ids": {"tvdb": "404404"}, "season": 1, "episode": 1},
    ],
)
def test_unresolvable_inputs_return_none(index: Path, item: dict[str, Any]) -> None:
    assert resolve_absolute(item) is None


def test_external_lookup_is_last_resort(index: Path) -> None:
    calls: list[tuple[str, str]] = []

    def lookup(source: str, ident: str) -> dict[str, str]:
        calls.append((source, ident))
        return {"tvdb": "81472"}

    got = resolve_absolute(_episode({"imdb": "tt0121220"}, 2, 13), resolve_external=lookup)
    assert got is not None
    assert (got.absolute, got.entry) == (52, "external_lookup")
    assert calls == [("imdb", "tt0121220")]

    resolve_absolute(_episode({"tvdb": "81472"}, 2, 13), resolve_external=lookup)
    assert len(calls) == 1


def test_resolution_matches_ids_guards_wrong_entry(index: Path) -> None:
    got = resolve_absolute(_episode({"tvdb": "81472"}, 2, 13))
    assert resolution_matches_ids(got, {"mal": "813"}) is True
    assert resolution_matches_ids(got, {"mal": "99999"}) is False
    assert resolution_matches_ids(got, {}) is False
    assert resolution_matches_ids(None, {"anilist": "813"}) is False


def test_existing_config_is_migrated_to_include_simkl_and_history() -> None:
    from cw_platform.config_base import _normalize_anime_mapping

    cfg = {"anime_mapping": {"enabled": True, "use_for_pairs": ["anilist"], "features": ["watchlist", "ratings"]}}
    _normalize_anime_mapping(cfg)
    assert "simkl" in cfg["anime_mapping"]["use_for_pairs"]
    assert "history" in cfg["anime_mapping"]["features"]


def test_migration_keeps_custom_entries() -> None:
    from cw_platform.config_base import _normalize_anime_mapping

    cfg = {"anime_mapping": {"enabled": True, "use_for_pairs": ["*"], "features": ["history"]}}
    _normalize_anime_mapping(cfg)
    assert cfg["anime_mapping"]["use_for_pairs"][0] == "*"
    assert set(cfg["anime_mapping"]["features"]) >= {"watchlist", "ratings", "history"}
