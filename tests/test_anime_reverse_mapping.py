# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import overrides as ov
from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.episodes import resolve_absolute, resolve_source_coordinate
from cw_platform.id_map import canonical_key
from providers.sync.simkl import _history as simkl_history

DBZ_MAPPINGS: dict[str, Any] = {
    "tvdb_show:81472:s1": {"mal:813": {"1-39": "1-39"}},
    "tvdb_show:81472:s2": {"mal:813": {"1-35": "40-74"}},
    "tvdb_show:81472:s3": {"mal:813": {"1-33": "75-107"}},
}


@pytest.fixture()
def dbz_index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(DBZ_MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return paths["db"]


def _dandadan_rules() -> list[dict[str, Any]]:
    return [
        {
            "id": "ovr_dandadan_s1",
            "media_type": "show",
            "match_provider": "tmdb",
            "match_id": "240411",
            "match_season": 1,
            "target_namespace": "simkl",
            "target_id": "2308514",
            "episode_from": 1,
            "episode_to": 12,
            "episode_start_at": 1,
        },
        {
            "id": "ovr_dandadan_s2",
            "media_type": "show",
            "match_provider": "tmdb",
            "match_id": "240411",
            "match_season": 1,
            "target_namespace": "simkl",
            "target_id": "2665178",
            "episode_from": 13,
            "episode_to": 24,
            "episode_start_at": 1,
        },
    ]


def test_override_reverse_recovers_the_source_coordinate(config_base: Path) -> None:
    for rule in _dandadan_rules():
        ov.upsert_override(rule)

    coord = resolve_source_coordinate({"simkl": "2665178", "tmdb": "299464"}, 1)

    assert coord is not None
    assert coord.basis == "user_override"
    assert (coord.provider, coord.ident) == ("tmdb", "240411")
    assert (coord.season, coord.episode) == (1, 13)


def test_override_reverse_round_trips_the_forward_map(config_base: Path) -> None:
    for rule in _dandadan_rules():
        ov.upsert_override(rule)

    for episode in range(13, 25):
        forward = resolve_absolute(
            {"type": "episode", "show_ids": {"tmdb": "240411"}, "season": 1, "episode": episode}
        )
        assert forward is not None
        assert (forward.namespace, forward.target_id) == ("simkl", "2665178")

        back = resolve_source_coordinate({"simkl": forward.target_id}, forward.absolute)
        assert back is not None
        assert (back.provider, back.ident, back.season, back.episode) == ("tmdb", "240411", 1, episode)


def test_override_reverse_picks_the_range_that_contains_the_episode(config_base: Path) -> None:
    seasons = [(1, 1, 39, 1), (2, 1, 35, 40), (3, 1, 33, 75)]
    for season, ep_from, ep_to, start in seasons:
        ov.upsert_override(
            {
                "id": f"ovr_dbz_s{season}",
                "media_type": "show",
                "match_provider": "tmdb",
                "match_id": "12971",
                "match_season": season,
                "target_namespace": "simkl",
                "target_id": "41487",
                "episode_from": ep_from,
                "episode_to": ep_to,
                "episode_start_at": start,
            }
        )

    assert resolve_source_coordinate({"simkl": "41487"}, 39).season == 1
    assert resolve_source_coordinate({"simkl": "41487"}, 40).season == 2
    assert resolve_source_coordinate({"simkl": "41487"}, 40).episode == 1
    assert resolve_source_coordinate({"simkl": "41487"}, 74).episode == 35
    assert resolve_source_coordinate({"simkl": "41487"}, 75).season == 3
    assert resolve_source_coordinate({"simkl": "41487"}, 999) is None


def test_anibridge_reverse_maps_absolute_back_to_aired(dbz_index: Path) -> None:
    coord = resolve_source_coordinate({"mal": "813", "tvdb": "81472"}, 40)

    assert coord is not None
    assert coord.basis == "anibridge_reverse"
    assert (coord.provider, coord.ident) == ("tvdb", "81472")
    assert (coord.season, coord.episode) == (2, 1)


def test_anibridge_reverse_round_trips_every_dbz_season(dbz_index: Path) -> None:
    for season, count, offset in ((1, 39, 0), (2, 35, 39), (3, 33, 74)):
        for episode in range(1, count + 1):
            forward = resolve_absolute(
                {"type": "episode", "show_ids": {"tvdb": "81472"}, "season": season, "episode": episode}
            )
            assert forward is not None
            assert forward.absolute == offset + episode

            back = resolve_source_coordinate({"mal": forward.target_id}, forward.absolute)
            assert back is not None
            assert (back.season, back.episode) == (season, episode)


def test_user_override_outranks_the_downloaded_dataset(dbz_index: Path) -> None:
    ov.upsert_override(
        {
            "id": "ovr_dbz_custom",
            "media_type": "show",
            "match_provider": "tmdb",
            "match_id": "12971",
            "match_season": 5,
            "target_namespace": "mal",
            "target_id": "813",
            "episode_from": 1,
            "episode_to": 35,
            "episode_start_at": 40,
        }
    )

    coord = resolve_source_coordinate({"mal": "813", "tvdb": "81472"}, 40)

    assert coord is not None
    assert coord.basis == "user_override"
    assert (coord.provider, coord.ident, coord.season, coord.episode) == ("tmdb", "12971", 5, 1)


def test_reverse_returns_nothing_without_ids_or_a_usable_number(dbz_index: Path) -> None:
    assert resolve_source_coordinate({}, 40) is None
    assert resolve_source_coordinate({"mal": "813"}, 0) is None
    assert resolve_source_coordinate({"mal": "813"}, None) is None
    assert resolve_source_coordinate({"mal": "99999"}, 40) is None


def _floppy_episode(tmdb: str, season: int, episode: int) -> dict[str, Any]:
    return {
        "type": "episode",
        "show_ids": {"tmdb": tmdb},
        "season": season,
        "episode": episode,
        "watched_at": "2026-08-03T13:35:19Z",
    }


def _simkl_anime_row(show_ids: dict[str, Any], numbers: list[int]) -> dict[str, Any]:
    return {
        "show": {"title": "Anime", "ids": dict(show_ids)},
        "seasons": [
            {
                "number": 1,
                "episodes": [{"number": n, "watched_at": "2026-08-03T13:35:19Z"} for n in numbers],
            }
        ],
    }


def _episodes_from(out: dict[str, Any]) -> list[dict[str, Any]]:
    return [v for v in out.values() if str(v.get("type")) == "episode"]


def test_readback_uses_an_override_to_match_the_source_key(config_base: Path, monkeypatch) -> None:
    for rule in _dandadan_rules():
        ov.upsert_override(rule)
    monkeypatch.setattr(simkl_history, "_load_source_aliases", lambda: {})

    row = _simkl_anime_row({"simkl": 2665178, "tmdb": "299464"}, [1, 2, 3])
    out, *_ = simkl_history._parse_rows([], [], [row], limit=None, bridge_tag="v3")

    eps = sorted(_episodes_from(out), key=lambda e: e["episode"])
    assert [(e["season"], e["episode"]) for e in eps] == [(1, 13), (1, 14), (1, 15)]
    assert [e["show_ids"]["tmdb"] for e in eps] == ["240411"] * 3
    assert canonical_key(eps[0]) == canonical_key(_floppy_episode("240411", 1, 13))


def test_readback_without_the_bridge_keeps_the_unmatched_key(config_base: Path, monkeypatch) -> None:
    for rule in _dandadan_rules():
        ov.upsert_override(rule)
    monkeypatch.setattr(simkl_history, "_load_source_aliases", lambda: {})

    row = _simkl_anime_row({"simkl": 2665178, "tmdb": "299464"}, [1])
    out, *_ = simkl_history._parse_rows([], [], [row], limit=None)

    ep = _episodes_from(out)[0]
    assert (ep["season"], ep["episode"]) == (1, 1)
    assert canonical_key(ep) != canonical_key(_floppy_episode("240411", 1, 13))


def test_readback_reverses_dbz_absolute_numbers_through_anibridge(dbz_index: Path, monkeypatch) -> None:
    monkeypatch.setattr(simkl_history, "_load_source_aliases", lambda: {})

    row = _simkl_anime_row({"simkl": 41487, "tmdb": "12971", "tvdb": "81472", "mal": "813"}, [39, 40, 74, 75])
    out, *_ = simkl_history._parse_rows([], [], [row], limit=None, bridge_tag="v3")

    eps = sorted(_episodes_from(out), key=lambda e: (e["season"], e["episode"]))
    assert [(e["season"], e["episode"]) for e in eps] == [(1, 39), (2, 1), (2, 35), (3, 1)]
    assert canonical_key(eps[1]) == canonical_key(_floppy_episode("12971", 2, 1))
    assert all(e["show_ids"]["tmdb"] == "12971" for e in eps)


def test_readback_leaves_non_anime_rows_alone(dbz_index: Path, monkeypatch) -> None:
    monkeypatch.setattr(simkl_history, "_load_source_aliases", lambda: {})

    show_row = {
        "show": {"title": "Loki", "ids": {"simkl": 1074318, "tmdb": "331223"}},
        "seasons": [{"number": 2, "episodes": [{"number": 4, "watched_at": "2023-12-29T17:47:00Z"}]}],
    }
    out, *_ = simkl_history._parse_rows([], [show_row], [], limit=None, bridge_tag="v3")

    ep = _episodes_from(out)[0]
    assert (ep["season"], ep["episode"]) == (2, 4)
    assert ep["show_ids"]["tmdb"] == "331223"


def test_disabled_rules_do_not_reverse_map(config_base: Path) -> None:
    rule = dict(_dandadan_rules()[1])
    rule["enabled"] = False
    ov.upsert_override(rule)

    assert resolve_source_coordinate({"simkl": "2665178"}, 1) is None
