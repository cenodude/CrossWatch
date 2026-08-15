# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.episodes import resolve_axis_coordinates, resolve_source_coordinate
from cw_platform.anime_mapping.history_coords import (
    HistoryCoordinateAliases,
    absolute_numbered_shows,
    build_history_coordinate_aliases,
    native_anime_absolute,
)

MAPPINGS: dict[str, Any] = {
    "tvdb_show:76666:s1": {"mal:223": {"1-13": "1-13"}},
    "tvdb_show:76666:s2": {"mal:223": {"1-15": "14-28"}},
    "tvdb_show:76666:s3": {"mal:223": {"1-40": "29-68"}},
    "tmdb_show:12609:s1": {"mal:223": {"1-153": "1-153"}},
    "tvdb_show:81472:s1": {"mal:813": {"1-39": "1-39"}},
    "tvdb_show:81472:s2": {"mal:813": {"1-35": "40-74"}},
    "tmdb_show:12971:s1": {"mal:813": {"1-39": "1-39"}},
}

DRAGON_BALL_IDS = {"tmdb": "12609", "imdb": "tt0088509", "tvdb": "76666", "mal": "223", "anilist": "223"}
DBZ_IDS = {"tmdb": "12971", "imdb": "tt0121220", "tvdb": "81472", "mal": "813", "anilist": "813"}
ONE_PIECE_IDS = {"tmdb": "37854", "imdb": "tt0388629", "tvdb": "81797", "mal": "21", "anilist": "21"}

MDBLIST_DRAGON_BALL_IDS = {"tmdb": "12609", "imdb": "tt0088509", "trakt": "12553"}
MDBLIST_DBZ_IDS = {"tmdb": "12971", "imdb": "tt0121220", "trakt": "12915"}
MDBLIST_ONE_PIECE_IDS = {"tmdb": "37854", "imdb": "tt0388629", "trakt": "37696"}

CFG = {"anime_mapping": {"enabled": True, "release_tag": "v3"}}


@pytest.fixture()
def anime_index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return paths["db"]


def _episode(show_ids: dict[str, str], season: int, episode: int, absolute: int | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "type": "episode",
        "title": f"S{season:02d}E{episode:02d}",
        "season": season,
        "episode": episode,
        "watched": True,
        "watched_at": "2024-01-01T00:00:00Z",
        "ids": {},
        "show_ids": dict(show_ids),
    }
    if absolute is not None:
        out["_simkl_episode_number"] = absolute
        out["simkl_bucket"] = "anime"
    return out


def _mdblist_run(show_ids: dict[str, str], seasons: list[tuple[int, list[int]]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for season, numbers in seasons:
        for number in numbers:
            out[f"tmdb:{show_ids['tmdb']}#s{season:02d}e{number:02d}"] = _episode(show_ids, season, number)
    return out


def _base_tokens(item: dict[str, Any]) -> set[str]:
    if str(item.get("type") or "") != "episode":
        return {f"{k}:{str(v).lower()}" for k, v in (item.get("ids") or {}).items() if v}
    season = item.get("season")
    episode = item.get("episode")
    if season is None or episode is None:
        return set()
    frag = f"#s{int(season):02d}e{int(episode):02d}"
    out: set[str] = set()
    for source in (item.get("show_ids") or {}, item.get("ids") or {}):
        out.update(f"{k}:{str(v).lower()}{frag}" for k, v in source.items() if v)
    return out


def _typed_tokens(aliases: HistoryCoordinateAliases, item: dict[str, Any]) -> set[str]:
    return _base_tokens(item) | aliases.tokens(item)


def _matched(aliases: HistoryCoordinateAliases, source: dict[str, Any], destination: dict[str, Any]) -> bool:
    return bool(_typed_tokens(aliases, source) & _typed_tokens(aliases, destination))


def test_axis_coordinates_return_both_axes_where_the_unanimous_resolver_refuses(anime_index: Path) -> None:
    assert resolve_source_coordinate(DRAGON_BALL_IDS, 14) is None

    coords = resolve_axis_coordinates(DRAGON_BALL_IDS, 14)

    assert coords == {(2, 1), (1, 14)}


def test_dragon_ball_simkl_shape_translates_to_the_mdblist_flat_coordinate(anime_index: Path) -> None:
    aliases = build_history_coordinate_aliases(CFG, "history", ())

    tokens = aliases.tokens(_episode(DRAGON_BALL_IDS, 2, 1, absolute=14))

    assert "tmdb:12609#s01e14" in tokens
    assert "tvdb:76666#s02e01" in tokens


def test_dragon_ball_z_flat_shape_translates_to_the_mdblist_aired_coordinate(anime_index: Path) -> None:
    aliases = build_history_coordinate_aliases(CFG, "history", ())

    tokens = aliases.tokens(_episode(DBZ_IDS, 1, 40, absolute=40))

    assert "tvdb:81472#s02e01" in tokens


def test_dragon_ball_pair_compares_as_already_synced(anime_index: Path) -> None:
    source = _episode(DRAGON_BALL_IDS, 2, 1, absolute=14)
    destination = _episode(MDBLIST_DRAGON_BALL_IDS, 1, 14)
    aliases = build_history_coordinate_aliases(CFG, "history", ({"a": source}, {"b": destination}))

    assert _matched(aliases, source, destination)


def test_dragon_ball_z_pair_compares_as_already_synced(anime_index: Path) -> None:
    source = _episode(DBZ_IDS, 1, 40, absolute=40)
    destination = _episode(MDBLIST_DBZ_IDS, 2, 1)
    dst_index = _mdblist_run(MDBLIST_DBZ_IDS, [(1, list(range(1, 40))), (2, list(range(1, 36)))])
    aliases = build_history_coordinate_aliases(CFG, "history", ({"a": source}, dst_index))

    assert _matched(aliases, source, destination)


def test_one_piece_matches_a_destination_that_numbers_episodes_absolutely(config_base: Path) -> None:
    dst_index = _mdblist_run(
        MDBLIST_ONE_PIECE_IDS,
        [(1, list(range(1, 62))), (22, list(range(62, 100)))],
    )
    source = _episode(ONE_PIECE_IDS, 3, 7, absolute=75)
    destination = dst_index["tmdb:37854#s22e75"]
    aliases = build_history_coordinate_aliases(CFG, "history", ({"a": source}, dst_index))

    assert _matched(aliases, source, destination)
    assert "tmdb:37854#abs:75" in aliases.tokens(source)


def test_a_row_with_its_own_absolute_never_claims_its_episode_number_is_one(config_base: Path) -> None:
    dst_index = _mdblist_run(MDBLIST_ONE_PIECE_IDS, [(1, list(range(1, 21)))])
    source = _episode(ONE_PIECE_IDS, 3, 11, absolute=21)
    aliases = build_history_coordinate_aliases(CFG, "history", (dst_index,))

    tokens = aliases.tokens(source)

    assert "tmdb:37854#abs:21" in tokens
    assert "tmdb:37854#abs:11" not in tokens
    assert not _matched(aliases, source, dst_index["tmdb:37854#s01e11"])


def test_per_season_restart_numbering_is_not_treated_as_absolute(config_base: Path) -> None:
    dst_index = _mdblist_run(MDBLIST_DBZ_IDS, [(1, list(range(1, 40))), (2, list(range(1, 36)))])

    assert absolute_numbered_shows(dst_index) == {}


def test_specials_only_shows_never_produce_an_absolute_token(config_base: Path) -> None:
    oad = {"tmdb": "256378", "imdb": "tt2560140"}
    dst_index = {
        f"tmdb:256378#s00e{n:02d}": _episode(oad, 0, n) for n in (1, 2, 4, 6)
    }

    assert absolute_numbered_shows(dst_index) == {}

    aliases = build_history_coordinate_aliases(CFG, "history", (dst_index,))
    assert aliases.tokens(dst_index["tmdb:256378#s00e01"]) == set()


def test_a_few_misfiled_rows_do_not_disqualify_the_whole_show(config_base: Path) -> None:
    dst_index = _mdblist_run(MDBLIST_ONE_PIECE_IDS, [(1, list(range(1, 100)))])
    dst_index["tmdb:37854#s08e04"] = _episode(MDBLIST_ONE_PIECE_IDS, 8, 4)
    dst_index["tmdb:37854#s08e05"] = _episode(MDBLIST_ONE_PIECE_IDS, 8, 5)

    numbers = absolute_numbered_shows(dst_index)["tmdb:37854"]

    assert 75 in numbers
    assert 4 not in numbers
    assert 5 not in numbers


def test_a_source_index_cannot_mask_an_absolutely_numbered_destination(config_base: Path) -> None:
    dst_index = _mdblist_run(MDBLIST_ONE_PIECE_IDS, [(1, list(range(1, 62))), (22, list(range(62, 100)))])
    src_index = {"tmdb:37854#s03e07": _episode(ONE_PIECE_IDS, 3, 7, absolute=75)}

    assert "tmdb:37854" in absolute_numbered_shows(src_index, dst_index)


def test_a_single_completed_season_is_not_treated_as_absolute(config_base: Path) -> None:
    xmen = {"tmdb": "138502", "imdb": "tt16026746", "tvdb": "412432"}
    dst_index = _mdblist_run(xmen, [(1, list(range(1, 11)))])

    assert absolute_numbered_shows(dst_index) == {}


def test_a_new_season_does_not_alias_onto_the_first_season(config_base: Path) -> None:
    xmen = {"tmdb": "138502", "imdb": "tt16026746", "tvdb": "412432"}
    dst_index = _mdblist_run(xmen, [(1, list(range(1, 11)))])
    source = _episode(xmen, 2, 1)
    aliases = build_history_coordinate_aliases(CFG, "history", ({"a": source}, dst_index))

    assert not _matched(aliases, source, dst_index["tmdb:138502#s01e01"])


def test_an_absolute_run_never_claims_a_season_it_was_not_seen_in(config_base: Path) -> None:
    dst_index = _mdblist_run(MDBLIST_ONE_PIECE_IDS, [(1, list(range(1, 62))), (22, list(range(62, 100)))])
    aliases = build_history_coordinate_aliases(CFG, "history", (dst_index,))

    assert "tmdb:37854#abs:7" in aliases.tokens(_episode(MDBLIST_ONE_PIECE_IDS, 1, 7))
    assert aliases.tokens(_episode(MDBLIST_ONE_PIECE_IDS, 3, 7)) == set()


def test_translated_episode_is_not_planned_again_on_a_second_sync(anime_index: Path) -> None:
    source = _episode(DRAGON_BALL_IDS, 2, 1, absolute=14)
    destination = _episode(MDBLIST_DRAGON_BALL_IDS, 1, 14)
    src_index = {"tmdb:12609#s02e01": source}
    dst_index = {"tmdb:12609#s01e14": destination}

    for _ in range(2):
        aliases = build_history_coordinate_aliases(CFG, "history", (src_index, dst_index))
        alias_map = {tok: key for key, item in dst_index.items() for tok in aliases.tokens(item)}
        alias_map.update({f"tmdb:12609#s01e14": "tmdb:12609#s01e14"})
        assert any(tok in alias_map for tok in aliases.tokens(source))


def test_a_translated_destination_row_is_not_planned_for_removal(anime_index: Path) -> None:
    source = _episode(DRAGON_BALL_IDS, 2, 1, absolute=14)
    destination = _episode(MDBLIST_DRAGON_BALL_IDS, 1, 14)
    src_index = {"tmdb:12609#s02e01": source}
    dst_index = {"tmdb:12609#s01e14": destination}
    aliases = build_history_coordinate_aliases(CFG, "history", (src_index, dst_index))

    src_base = {tok for item in src_index.values() for tok in _base_tokens(item)}
    src_tokens = {tok for item in src_index.values() for tok in _typed_tokens(aliases, item)}

    assert _base_tokens(destination) & src_base == set()
    assert _typed_tokens(aliases, destination) & src_tokens


def test_a_genuinely_absent_episode_is_still_planned_for_removal(anime_index: Path) -> None:
    source = _episode(DRAGON_BALL_IDS, 2, 1, absolute=14)
    orphan = _episode(MDBLIST_DRAGON_BALL_IDS, 1, 99)
    src_index = {"tmdb:12609#s02e01": source}
    dst_index = {"tmdb:12609#s01e14": _episode(MDBLIST_DRAGON_BALL_IDS, 1, 14), "tmdb:12609#s01e99": orphan}
    aliases = build_history_coordinate_aliases(CFG, "history", (src_index, dst_index))

    src_tokens = {tok for item in src_index.values() for tok in _typed_tokens(aliases, item)}

    assert _typed_tokens(aliases, orphan) & src_tokens == set()


def test_regular_television_produces_no_alias_tokens(anime_index: Path) -> None:
    aliases = build_history_coordinate_aliases(CFG, "history", ())
    regular = _episode({"tmdb": "100088", "imdb": "tt3581920", "tvdb": "392256"}, 1, 2)

    assert native_anime_absolute(regular) is None
    assert aliases.tokens(regular) == set()


def test_movies_produce_no_alias_tokens(anime_index: Path) -> None:
    aliases = build_history_coordinate_aliases(CFG, "history", ())
    movie = {"type": "movie", "title": "Akira", "year": 1988, "ids": {"tmdb": "149"}}

    assert aliases.tokens(movie) == set()


def test_absolute_without_a_native_anime_id_is_ignored(anime_index: Path) -> None:
    aliases = build_history_coordinate_aliases(CFG, "history", ())
    row = _episode({"tmdb": "100088", "tvdb": "392256"}, 1, 2)
    row["_simkl_episode_number"] = 2

    assert native_anime_absolute(row) is None
    assert aliases.tokens(row) == set()


def test_native_anime_id_without_an_absolute_is_ignored(anime_index: Path) -> None:
    aliases = build_history_coordinate_aliases(CFG, "history", ())
    row = _episode(DRAGON_BALL_IDS, 2, 1)

    assert native_anime_absolute(row) is None
    assert aliases.tokens(row) == set()


def test_non_history_features_are_disabled(anime_index: Path) -> None:
    for feature in ("watchlist", "ratings", "progress"):
        aliases = build_history_coordinate_aliases(CFG, feature, ())
        assert not aliases.enabled
        assert aliases.tokens(_episode(DRAGON_BALL_IDS, 2, 1, absolute=14)) == set()


def test_disabled_anime_mapping_produces_no_tokens(anime_index: Path) -> None:
    aliases = build_history_coordinate_aliases({"anime_mapping": {"enabled": False}}, "history", ())

    assert not aliases.enabled
    assert aliases.tokens(_episode(DRAGON_BALL_IDS, 2, 1, absolute=14)) == set()


def test_missing_mapping_index_still_allows_absolute_matching(config_base: Path) -> None:
    dst_index = _mdblist_run(MDBLIST_ONE_PIECE_IDS, [(1, list(range(1, 62)))])
    source = _episode(ONE_PIECE_IDS, 1, 3, absolute=3)
    aliases = build_history_coordinate_aliases(CFG, "history", (dst_index,))

    assert _matched(aliases, source, dst_index["tmdb:37854#s01e03"])


def test_episode_rows_without_episode_ids_keep_empty_ids(anime_index: Path) -> None:
    aliases = build_history_coordinate_aliases(CFG, "history", ())
    source = _episode(DRAGON_BALL_IDS, 2, 1, absolute=14)
    before = json.dumps(source, sort_keys=True)

    aliases.tokens(source)

    assert source["ids"] == {}
    assert json.dumps(source, sort_keys=True) == before
