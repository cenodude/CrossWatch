# tests/test_anime_resolver.py
# CrossWatch - Central anime episode resolver on real AniBridge rows
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import cw_mappings, overrides, storage
from cw_platform.anime_mapping.resolver import Coordinate, resolve_episode

pytestmark = pytest.mark.real_cw_mappings

REAL: dict[str, Any] = {
    "anidb:10452:R": {"anilist:20635": {"1-61": "1-61"}, "mal:22777": {"1-61": "1-61"}, "tmdb_show:61709:s2": {"1-26": "1-26"}, "tvdb_show:88031:s2": {"1-61": "1-61"}},
    "anidb:6399:R": {"anilist:6033": {"1-97": "1-97"}, "mal:6033": {"1-97": "1-97"}, "tmdb_show:61709:s1": {"1-26": "1-26"}, "tvdb_show:88031:s1": {"1-97": "1-97"}},
    "anilist:20635": {"anidb:10452:R": {"1-61": "1-61"}, "tmdb_show:61709:s5": {"9-43": "1-35"}, "tmdb_show:61709:s6": {"44-69": "1-26"}, "tvdb_show:88031:s2": {"9-69": "1-61"}},
    "mal:22777": {"anidb:10452:R": {"1-61": "1-61"}, "tmdb_show:61709:s5": {"1-35": "1-35"}, "tmdb_show:61709:s6": {"36-61": "1-26"}, "tvdb_show:88031:s2": {"1-61": "1-61"}},
    "anilist:6033": {"anidb:6399:R": {"1-97": "1-97"}, "mal:6033": {"1-97": "1-97"}, "tmdb_show:61709:s1": {"1-26": "1-26"}, "tmdb_show:61709:s2": {"27-52": "1-26"}, "tmdb_show:61709:s3": {"53-77": "1-25"}, "tmdb_show:61709:s4": {"78-97": "1-20"}, "tvdb_show:88031:s1": {"1-97": "1-97"}},
    "mal:6033": {"anidb:6399:R": {"1-97": "1-97"}, "anilist:6033": {"1-97": "1-97"}, "tvdb_show:88031:s1": {"1-97": "1-97"}},
    "tmdb_show:61709:s1": {"anidb:6399:R": {"1-26": "1-26"}, "anilist:6033": {"1-26": "1-26"}},
    "tmdb_show:61709:s2": {"anidb:10452:R": {"1-26": "1-26"}, "anilist:6033": {"1-26": "27-52"}},
    "tmdb_show:61709:s5": {"anilist:20635": {"1-35": "9-43"}, "mal:22777": {"1-35": "1-35"}},
    "tvdb_show:88031:s1": {"anidb:6399:R": {"1-97": "1-97"}, "anilist:6033": {"1-97": "1-97"}, "mal:6033": {"1-97": "1-97"}},
    "tvdb_show:88031:s2": {"anidb:10452:R": {"1-61": "1-61"}, "anilist:20635": {"1-61": "9-69"}, "mal:22777": {"1-61": "1-61"}},
    "tmdb_show:120089:s1": {"anidb:16947:R": {"1-12": "1-12"}, "anidb:17061:R": {"13-25": "1-13"}, "anilist:140960": {"1-12": "1-12"}, "anilist:142838": {"13-25": "1-13"}, "mal:50265": {"1-12": "1-12"}, "mal:50602": {"13-25": "1-13"}, "tvdb_show:405920:s1": {"1-25": "1-25"}},
    "mal:50602": {"anidb:17061:R": {"1-13": "1-13"}, "anilist:142838": {"1-13": "1-13"}, "tmdb_show:120089:s1": {"1-13": "13-25"}, "tvdb_show:405920:s1": {"1-13": "13-25"}},
    "tmdb_show:117465:s1": {"anidb:16012:R": {"1-13": "1-13"}, "anilist:128893": {"1-13": "1-13"}, "mal:46569": {"1-13": "1-13"}, "tvdb_show:402474:s1": {"1-13": "1-13"}},
    "tmdb_show:117465:s2": {"anidb:18090:R": {"1-12": "1-12"}, "anilist:166613": {"1-12": "1-12"}, "mal:55825": {"1-12": "1-12"}, "tvdb_show:402474:s2": {"1-12": "1-12"}},
}

IDENTITY = (
    "title\tanidb\tanilist\tkitsu\tmyanimelist\tsimkl\n"
    "Dragon Ball Kai\t6399\t6033\t4200\t6033\t38994\n"
    "Dragon Ball Kai (2014)\t10452\t20635\t8351\t22777\t47872\n"
    "Spy x Family\t16947\t140960\t45398\t50265\t1766469\n"
    "Spy x Family Part 2\t17061\t142838\t46474\t50602\t1809622\n"
)


def _build(config_base: Path, mappings: dict[str, Any] = REAL) -> None:
    pp = storage.paths("v3")
    pp["root"].mkdir(parents=True, exist_ok=True)
    pp["mappings"].write_bytes(json.dumps(mappings).encode("utf-8"))
    pp["identity"].write_bytes(IDENTITY.encode("utf-8"))
    storage.rebuild_sqlite_from_mappings(release_tag="v3")


def _ep(season: int, episode: int, **show_ids: str) -> dict[str, Any]:
    return {"type": "episode", "season": season, "episode": episode, "show_ids": dict(show_ids)}


@pytest.fixture()
def index(config_base: Path) -> Path:
    _build(config_base)
    return config_base


@pytest.mark.parametrize(
    "item, source, target, kwargs, expected",
    [
        (_ep(1, 32, tvdb="88031", tmdb="61709"), "tvdb", "tmdb", {}, Coordinate("tmdb", "61709", 6, 2)),
        (_ep(2, 6, tvdb="88031", tmdb="61709"), "tvdb", "tmdb", {}, Coordinate("tmdb", "61709", 6, 5)),
        (_ep(2, 6, tmdb="61709", tvdb="88031"), "tmdb", "tvdb", {}, Coordinate("tvdb", "88031", 32, 1)),
        (_ep(2, 6, tmdb="61709"), "tmdb", "simkl", {}, Coordinate("simkl", "38994", 32)),
        (_ep(5, 6, tmdb="61709"), "tmdb", "simkl", {}, Coordinate("simkl", "47872", 6)),
        (_ep(1, 13, tmdb="120089"), "tmdb", "simkl", {}, Coordinate("simkl", "1809622", 1)),
        (_ep(1, 13, tmdb="120089"), "tmdb", "anime_entry", {"target_namespace": "mal"}, Coordinate("anime_entry", "50602", 1, None, "mal")),
        (_ep(1, 13, tmdb="120089"), "tmdb", "anime_entry", {"target_namespace": "kitsu"}, Coordinate("anime_entry", "46474", 1, None, "kitsu")),
        (_ep(1, 1, mal="55825"), "anime_entry", "tmdb", {"source_namespace": "mal"}, Coordinate("tmdb", "117465", 14, 1)),
    ],
    ids=["kai_tvdb_s1e32", "kai_final_chapters_tvdb", "kai_tmdb_to_tvdb", "kai_tmdb_to_simkl", "final_chapters_tmdb_to_simkl",
         "spy_part2_tmdb_to_simkl", "spy_part2_tmdb_to_mal", "spy_part2_tmdb_to_kitsu", "hells_paradise_mal_to_tmdb"],
)
def test_resolves_between_numberings(index: Path, item: dict[str, Any], source: str, target: str, kwargs: dict[str, str], expected: Coordinate) -> None:
    out = resolve_episode(item, source=source, target=target, **kwargs)
    assert out.ok, out.reason
    assert out.coordinate == expected


@pytest.mark.parametrize(
    "simkl_entry, mal, episode, expected",
    [
        ("47872", "22777", 6, Coordinate("tmdb", "61709", 6, 5)),
        ("1809622", "50602", 1, Coordinate("tmdb", "120089", 13, 1)),
    ],
    ids=["final_chapters", "spy_part2_is_not_series_absolute"],
)
def test_simkl_entry_numbers_stay_per_entry(index: Path, simkl_entry: str, mal: str, episode: int, expected: Coordinate) -> None:
    item = {"type": "episode", "season": 1, "episode": episode, "_simkl_episode_number": episode,
            "simkl_bucket": "anime", "show_ids": {"simkl": simkl_entry, "tmdb": "999999"}}
    out = resolve_episode(item, source="simkl", target="tmdb")
    assert out.coordinate == expected


def test_a_show_without_anime_mapping_is_not_anime(index: Path) -> None:
    out = resolve_episode(_ep(1, 1, tmdb="1396"), source="tmdb", target="simkl")
    assert (out.ok, out.reason) == (False, "not_anime")


def test_disagreeing_anime_ids_are_refused(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    empty = config_base / "empty_cw.json"
    empty.write_bytes(json.dumps({"schema_version": 1, "revision": "2000-01-01.1", "entries": [
        {"id": "x", "remove": [{"source": "mal:1", "target": "anilist:1"}]}]}).encode("utf-8"))
    monkeypatch.setattr(cw_mappings, "BUNDLED_PATH", empty)
    _build(config_base)
    out = resolve_episode(_ep(2, 6, tvdb="88031"), source="tvdb", target="tmdb")
    assert (out.ok, out.reason) == (False, "ambiguous_mapping")
    assert {(c.season, c.episode) for c in out.candidates} == {(2, 6), (5, 6)}


@pytest.mark.parametrize(
    "layout, reason",
    [({1: 13}, "target_episode_missing"), ({2: 12}, "target_season_missing")],
    ids=["episode_beyond_season", "season_absent"],
)
def test_never_returns_a_coordinate_the_destination_lacks(index: Path, layout: dict[int, int], reason: str) -> None:
    out = resolve_episode(_ep(1, 1, mal="55825"), source="anime_entry", target="tmdb", source_namespace="mal", layout=layout)
    assert (out.ok, out.reason) == (False, reason)
    assert out.candidates == (Coordinate("tmdb", "117465", 14, 1),)


def test_layout_can_be_looked_up_per_show(index: Path) -> None:
    seen: list[str] = []

    def layout(show: str) -> dict[int, int]:
        seen.append(show)
        return {1: 25}

    out = resolve_episode(_ep(1, 1, mal="55825"), source="anime_entry", target="tmdb", source_namespace="mal", layout=layout)
    assert out.coordinate == Coordinate("tmdb", "117465", 14, 1)
    assert seen == ["117465"]


def test_user_custom_mapping_wins(index: Path) -> None:
    overrides.upsert_override({"media_type": "show", "match_provider": "tmdb", "match_id": "61709", "match_season": 2,
                               "episode_from": 1, "episode_to": 26, "episode_start_at": 1,
                               "target_namespace": "mal", "target_id": "22777"})
    out = resolve_episode(_ep(2, 6, tmdb="61709"), source="tmdb", target="simkl")
    assert out.basis == "user_override"
    assert out.coordinate == Coordinate("simkl", "47872", 6)


def test_missing_index_is_reported(config_base: Path) -> None:
    out = resolve_episode(_ep(1, 1, tmdb="61709"), source="tmdb", target="tvdb")
    assert (out.ok, out.reason) == (False, "mapping_unavailable")


def test_unknown_numbering_is_a_programming_error(index: Path) -> None:
    with pytest.raises(ValueError):
        resolve_episode(_ep(1, 1, tmdb="61709"), source="tmdb", target="mdblist")


@pytest.mark.parametrize(
    "season_layout, ok",
    [(range(1, 26), True), (set(range(14, 26)), True), (set(range(892, 1089)), False), (13, False)],
    ids=["range_from_one", "listed_numbers", "absolute_numbers_elsewhere", "count_too_small"],
)
def test_layout_checks_real_episode_numbers(index: Path, season_layout: Any, ok: bool) -> None:
    out = resolve_episode(_ep(1, 1, mal="55825"), source="anime_entry", target="tmdb", source_namespace="mal", layout={1: season_layout})
    assert out.ok is ok
    if not ok:
        assert out.reason == "target_episode_missing"
