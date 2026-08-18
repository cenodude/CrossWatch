# CrossWatch test scripts
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import services.analyzer as A
from cw_platform.anime_mapping import storage

MAPPINGS: dict[str, Any] = {
    "tvdb_show:81472:s1": {"mal:813": {"1-39": "1-39"}},
    "tvdb_show:81472:s2": {"mal:813": {"1-35": "40-74"}},
    "tmdb_show:12971:s1": {"mal:813": {"1-39": "1-39"}},
}

DBZ_SHOW_IDS = {"tmdb": "12971", "imdb": "tt0121220", "tvdb": "81472", "mal": "813", "anilist": "813"}
WATCHED = "2024-01-01T00:00:00Z"


@pytest.fixture()
def anime_index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return paths["db"]


@pytest.fixture()
def cws(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    sandbox = tmp_path / ".cw_state_analyzer"
    sandbox.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(A, "CWS_DIR", sandbox)
    return sandbox


def _episode(season: int, episode: int, absolute: int | None = None, *, show_ids=None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "type": "episode",
        "title": f"S{season:02d}E{episode:02d}",
        "series_title": "Dragon Ball Z",
        "season": season,
        "episode": episode,
        "watched": True,
        "watched_at": WATCHED,
        "ids": {},
        "show_ids": dict(show_ids if show_ids is not None else DBZ_SHOW_IDS),
    }
    if absolute is not None:
        out["_simkl_episode_number"] = absolute
        out["simkl_bucket"] = "anime"
    return out


def _state(simkl_items: dict[str, Any], trakt_items: dict[str, Any]) -> dict[str, Any]:
    return {
        "providers": {
            "SIMKL": {"instances": {"SIMKL-P01": {"history": {"baseline": {"items": dict(simkl_items)}}}}},
            "TRAKT": {"instances": {"TRAKT-P01": {"history": {"baseline": {"items": dict(trakt_items)}}}}},
        }
    }


def _cfg(*, anime: bool = True, pair_anime: bool = True) -> dict[str, Any]:
    history: dict[str, Any] = {"enable": True, "use_anime_mapping": bool(pair_anime)}
    cfg: dict[str, Any] = {
        "pairs": [
            {
                "id": "p1",
                "enabled": True,
                "source": "SIMKL",
                "target": "TRAKT",
                "source_instance": "SIMKL-P01",
                "target_instance": "TRAKT-P01",
                "mode": "one-way",
                "features": {"history": history},
            }
        ]
    }
    if anime:
        cfg["anime_mapping"] = {"enabled": True, "release_tag": "v3", "use_for_pairs": ["simkl", "anilist"]}
    return cfg


def _stats(state: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    ctx = A._analysis_context(state, cfg)
    stats = A._pair_stats(state, cfg, ctx)
    assert len(stats) == 1
    return stats[0]


def _missing(state: dict[str, Any], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    ctx = A._analysis_context(state, cfg)
    probs = A._problems(state, None, cfg=cfg, ctx=ctx, include_system=False, include_hints=False)
    return [p for p in probs if p.get("type") == "missing_peer"]


def test_absolute_episode_counts_as_synced_via_anibridge(anime_index: Path, cws: Path) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(1, 40, absolute=40)}
    trakt = {"tmdb:12971#s02e01": _episode(2, 1)}

    stats = _stats(_state(simkl, trakt), _cfg())

    assert stats["total"] == 1
    assert stats["synced"] == 1
    assert stats["anime_synced"] == 1


def test_absolute_episode_is_not_reported_as_missing_peer(anime_index: Path, cws: Path) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(1, 40, absolute=40)}
    trakt = {"tmdb:12971#s02e01": _episode(2, 1)}

    assert _missing(_state(simkl, trakt), _cfg()) == []


def test_absolute_episode_stays_missing_when_anime_mapping_is_off(anime_index: Path, cws: Path) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(1, 40, absolute=40)}
    trakt = {"tmdb:12971#s02e01": _episode(2, 1)}
    cfg = _cfg(anime=False, pair_anime=False)

    stats = _stats(_state(simkl, trakt), cfg)

    assert stats["synced"] == 0
    assert "anime_synced" not in stats
    assert len(_missing(_state(simkl, trakt), cfg)) == 1


def test_destination_absolute_run_matches_without_mapping_data(cws: Path, config_base: Path) -> None:
    simkl = {"tmdb:12971#s02e01": _episode(2, 1, absolute=40)}
    flat = {f"tmdb:12971#s01e{n:02d}": _episode(1, n) for n in range(1, 75)}

    stats = _stats(_state(simkl, flat), _cfg())

    assert stats["synced"] == 1
    assert stats["anime_synced"] == 1


def test_genuinely_absent_anime_episode_is_still_reported(anime_index: Path, cws: Path) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(1, 40, absolute=40)}
    trakt = {"tmdb:12971#s01e01": _episode(1, 1)}

    stats = _stats(_state(simkl, trakt), _cfg())

    assert stats["synced"] == 0
    problems = _missing(_state(simkl, trakt), _cfg())
    assert len(problems) == 1
    assert problems[0]["targets"] == ["TRAKT@TRAKT-P01"]


def test_non_anime_episode_is_untouched_by_the_anime_path(anime_index: Path, cws: Path) -> None:
    ids = {"tmdb": "71912", "tvdb": "348473"}
    simkl = {"tmdb:71912#s01e01": _episode(1, 1, show_ids=ids)}
    trakt = {"tmdb:71912#s02e05": _episode(2, 5, show_ids=ids)}

    stats = _stats(_state(simkl, trakt), _cfg())

    assert stats["synced"] == 0
    assert "anime_synced" not in stats


def test_unresolved_backlog_drops_entries_the_anime_mapping_now_covers(anime_index: Path, cws: Path) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(1, 40, absolute=40)}
    trakt = {"tmdb:12971#s02e01": _episode(2, 1)}
    state = _state(simkl, trakt)
    cfg = _cfg()
    (cws / "trakt_history.unresolved.json").write_text(
        json.dumps({"tmdb:12971#s01e40": {"item": _episode(1, 40, absolute=40), "reason": "not_found"}}),
        encoding="utf-8",
    )

    ctx = A._analysis_context(state, cfg)
    attention = A._attention_from_analysis([], None, ctx)

    assert attention["counts"]["pending_retry"] == 0
    assert attention["counts"]["anime_resolved"] == 1


def test_unresolved_backlog_keeps_entries_that_are_still_absent(anime_index: Path, cws: Path) -> None:
    simkl = {"tmdb:12971#s01e40": _episode(1, 40, absolute=40)}
    state = _state(simkl, {"tmdb:12971#s01e01": _episode(1, 1)})
    cfg = _cfg()
    (cws / "trakt_history.unresolved.json").write_text(
        json.dumps({"tmdb:12971#s01e40": {"item": _episode(1, 40, absolute=40), "reason": "not_found"}}),
        encoding="utf-8",
    )

    ctx = A._analysis_context(state, cfg)
    attention = A._attention_from_analysis([], None, ctx)

    assert attention["counts"]["pending_retry"] == 1
    assert "anime_resolved" not in attention["counts"]


def test_pair_opt_in_without_the_global_switch_is_flagged(cws: Path, config_base: Path) -> None:
    state = _state({"tmdb:12971#s01e40": _episode(1, 40, absolute=40)}, {})
    cfg = _cfg(anime=False, pair_anime=True)
    ctx = A._analysis_context(state, cfg)

    probs = A._anime_mapping_diagnostics(ctx)

    assert [p["type"] for p in probs] == ["anime_mapping_disabled_globally"]
    assert probs[0]["pairs"] == ["SIMKL>TRAKT"]


def test_missing_anibridge_index_is_flagged(cws: Path, config_base: Path) -> None:
    state = _state({"tmdb:12971#s01e40": _episode(1, 40, absolute=40)}, {})
    ctx = A._analysis_context(state, _cfg())

    probs = A._anime_mapping_diagnostics(ctx)

    assert [p["type"] for p in probs] == ["anime_mapping_index_not_ready"]
