# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import services.analyzer as A


WATCHED = "2024-01-01T00:00:00Z"
WATCHED_EPOCH = 1704067200


def _episode(show_tmdb, season, episode, *, series, watched=WATCHED, ep_trakt=None):
    item: dict[str, Any] = {
        "type": "episode",
        "series_title": series,
        "season": season,
        "episode": episode,
        "watched_at": watched,
        "show_ids": {"tmdb": str(show_tmdb)},
        "ids": {"trakt": str(ep_trakt)} if ep_trakt else {},
    }
    return item


def _state(simkl_items, trakt_items):
    return {
        "providers": {
            "SIMKL": {"instances": {"SIMKL-P01": {
                "history": {"baseline": {"items": dict(simkl_items)}}}}},
            "TRAKT": {"instances": {"TRAKT-P01": {
                "history": {"baseline": {"items": dict(trakt_items)}}}}},
        }
    }


def _cfg(src="SIMKL", dst="TRAKT", src_inst="SIMKL-P01", dst_inst="TRAKT-P01"):
    pair: dict[str, Any] = {
        "id": "p1",
        "enabled": True,
        "source": src,
        "target": dst,
        "mode": "one-way",
        "features": {"history": {"enable": True}},
    }
    if src_inst:
        pair["source_instance"] = src_inst
    if dst_inst:
        pair["target_instance"] = dst_inst
    return {"pairs": [pair]}


def _write_alias(tmp_path: Path, items, *, scope="one-way:SIMKL#SIMKL-P01-TRAKT#TRAKT-P01:p1|SIMKL>TRAKT",
                 name="trakt_history.pair_alias.one-way_SIMKL_SIMKL-P01-TRAKT_TRAKT-P01_p1.json"):
    (tmp_path / name).write_text(json.dumps({"scope": scope, "items": items}), encoding="utf-8")


@pytest.fixture()
def cws(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    sandbox = tmp_path / ".cw_state"
    sandbox.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(A, "CWS_DIR", sandbox)
    return sandbox


def _synced(state, cfg):
    ctx = A._analysis_context(state, cfg)
    stats = A._pair_stats(state, cfg, ctx)
    assert len(stats) == 1
    return stats[0]


def test_dbz_alias_matches_translated_trakt_episode(cws) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(12971, 1, 40, series="Dragon Ball Z")}
    trakt = {"tmdb:12971#s02e01": _episode(12971, 2, 1, series="Dragon Ball Z", ep_trakt=498798)}
    _write_alias(cws, {
        f"tmdb:12971#s01e40@{WATCHED_EPOCH}": {
            "destination_event_key": f"tmdb:12971#s02e01@{WATCHED_EPOCH}",
            "destination_key": "tmdb:12971#s02e01",
            "watched_at": WATCHED,
        }
    })

    stats = _synced(_state(simkl, trakt), _cfg())

    assert stats["total"] == 1
    assert stats["synced"] == 1


def test_monster_alias_matches_different_show_and_season(cws) -> None:
    simkl = {"tmdb:225634#s02e01": _episode(225634, 2, 1, series="Monster")}
    trakt = {"tmdb:900001#s01e01": _episode(900001, 1, 1, series="Monster 2")}
    _write_alias(cws, {
        f"tmdb:225634#s02e01@{WATCHED_EPOCH}": {
            "destination_event_key": f"tmdb:900001#s01e01@{WATCHED_EPOCH}",
            "destination_key": "tmdb:900001#s01e01",
            "watched_at": WATCHED,
        }
    })

    stats = _synced(_state(simkl, trakt), _cfg())

    assert stats["synced"] == 1


def test_black_mirror_special_alias_matches(cws) -> None:
    simkl = {"tmdb:42009#s00e01": _episode(42009, 0, 1, series="Black Mirror")}
    trakt = {"tmdb:42009#s02e04": _episode(42009, 2, 4, series="Black Mirror")}
    _write_alias(cws, {
        f"tmdb:42009#s00e01@{WATCHED_EPOCH}": {
            "destination_event_key": f"tmdb:42009#s02e04@{WATCHED_EPOCH}",
            "destination_key": "tmdb:42009#s02e04",
            "watched_at": WATCHED,
        }
    })

    stats = _synced(_state(simkl, trakt), _cfg())

    assert stats["synced"] == 1


def test_alias_ignored_when_destination_record_absent(cws) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(12971, 1, 40, series="Dragon Ball Z")}
    _write_alias(cws, {
        f"tmdb:12971#s01e40@{WATCHED_EPOCH}": {
            "destination_event_key": f"tmdb:12971#s02e01@{WATCHED_EPOCH}",
            "destination_key": "tmdb:12971#s02e01",
            "watched_at": WATCHED,
        }
    })

    stats = _synced(_state(simkl, {}), _cfg())

    assert stats["total"] == 1
    assert stats["synced"] == 0


def test_alias_ignored_when_watched_at_differs_by_more_than_a_minute(cws) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(12971, 1, 40, series="Dragon Ball Z")}
    trakt = {"tmdb:12971#s02e01": _episode(
        12971, 2, 1, series="Dragon Ball Z", watched="2024-06-06T00:00:00Z")}
    _write_alias(cws, {
        f"tmdb:12971#s01e40@{WATCHED_EPOCH}": {
            "destination_event_key": f"tmdb:12971#s02e01@{WATCHED_EPOCH}",
            "destination_key": "tmdb:12971#s02e01",
            "watched_at": WATCHED,
        }
    })

    stats = _synced(_state(simkl, trakt), _cfg())

    assert stats["synced"] == 0


def test_alias_from_another_pair_is_ignored(cws) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(12971, 1, 40, series="Dragon Ball Z")}
    trakt = {"tmdb:12971#s02e01": _episode(12971, 2, 1, series="Dragon Ball Z")}
    _write_alias(cws, {
        f"tmdb:12971#s01e40@{WATCHED_EPOCH}": {
            "destination_event_key": f"tmdb:12971#s02e01@{WATCHED_EPOCH}",
            "destination_key": "tmdb:12971#s02e01",
            "watched_at": WATCHED,
        }
    }, scope="one-way:PLEX#PLEX-P01-TRAKT#TRAKT-P01:p9|PLEX>TRAKT",
       name="trakt_history.pair_alias.one-way_PLEX_PLEX-P01-TRAKT_TRAKT-P01_p9.json")

    stats = _synced(_state(simkl, trakt), _cfg())

    assert stats["synced"] == 0


def test_alias_from_another_instance_is_ignored(cws) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(12971, 1, 40, series="Dragon Ball Z")}
    trakt = {"tmdb:12971#s02e01": _episode(12971, 2, 1, series="Dragon Ball Z")}
    _write_alias(cws, {
        f"tmdb:12971#s01e40@{WATCHED_EPOCH}": {
            "destination_event_key": f"tmdb:12971#s02e01@{WATCHED_EPOCH}",
            "destination_key": "tmdb:12971#s02e01",
            "watched_at": WATCHED,
        }
    }, scope="one-way:SIMKL#SIMKL-P01-TRAKT#TRAKT-P02:p1|SIMKL>TRAKT",
       name="trakt_history.pair_alias.one-way_SIMKL_SIMKL-P01-TRAKT_TRAKT-P02_p1.json")

    stats = _synced(_state(simkl, trakt), _cfg())

    assert stats["synced"] == 0


def test_ordinary_same_coordinates_still_match_without_alias(cws) -> None:
    simkl = {"tmdb:71912#s01e01": _episode(71912, 1, 1, series="Ahsoka")}
    trakt = {"tmdb:71912#s01e01": _episode(71912, 1, 1, series="Ahsoka")}

    stats = _synced(_state(simkl, trakt), _cfg())

    assert stats["total"] == 1
    assert stats["synced"] == 1


def test_unaliased_translation_still_counts_as_unsynced(cws) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(12971, 1, 40, series="Dragon Ball Z")}
    trakt = {"tmdb:12971#s02e01": _episode(12971, 2, 1, series="Dragon Ball Z")}

    stats = _synced(_state(simkl, trakt), _cfg())

    assert stats["synced"] == 0


def test_state_baselines_are_not_rewritten(cws) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(12971, 1, 40, series="Dragon Ball Z")}
    trakt = {"tmdb:12971#s02e01": _episode(12971, 2, 1, series="Dragon Ball Z", ep_trakt=498798)}
    state = _state(simkl, trakt)
    before = json.dumps(state, sort_keys=True)
    _write_alias(cws, {
        f"tmdb:12971#s01e40@{WATCHED_EPOCH}": {
            "destination_event_key": f"tmdb:12971#s02e01@{WATCHED_EPOCH}",
            "destination_key": "tmdb:12971#s02e01",
            "watched_at": WATCHED,
        }
    })

    _synced(state, _cfg())

    assert json.dumps(state, sort_keys=True) == before
    simkl_block = state["providers"]["SIMKL"]["instances"]["SIMKL-P01"]
    trakt_block = state["providers"]["TRAKT"]["instances"]["TRAKT-P01"]
    assert simkl_block["history"]["baseline"]["items"]["tmdb:12971#s01e40"]["season"] == 1
    assert trakt_block["history"]["baseline"]["items"]["tmdb:12971#s02e01"]["season"] == 2
