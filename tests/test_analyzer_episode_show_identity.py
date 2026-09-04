# CrossWatch test scripts
from __future__ import annotations

from typing import Any

import services.analyzer as A


SHOW_TMDB = "83631"
SHOW_IMDB = "tt7908628"
SHOW_TVDB = "364093"
EP_IMDB = "tt33029958"


def _emby_episode(*, with_episode_imdb: bool = True) -> dict[str, Any]:
    item: dict[str, Any] = {
        "type": "episode",
        "season": 6,
        "episode": 3,
        "series_title": "What We Do in the Shadows",
        "series_year": 2019,
        "title": "S06E03",
        "watched": True,
        "watched_at": "2026-01-02T00:00:00Z",
        "show_ids": {"tmdb": SHOW_TMDB, "imdb": SHOW_IMDB},
    }
    if with_episode_imdb:
        item["ids"] = {"imdb": EP_IMDB}
    return item


def _simkl_episode(show_ids: dict[str, str]) -> dict[str, Any]:
    return {
        "type": "episode",
        "season": 6,
        "episode": 3,
        "ids": {},
        "title": "S06E03",
        "series_title": "What We Do in the Shadows",
        "series_year": 2019,
        "show_ids": dict(show_ids),
        "watched": True,
        "watched_at": "2026-01-02T00:05:00Z",
    }


def _state(emby: dict[str, Any], emby_key: str, simkl: dict[str, Any], simkl_key: str) -> dict[str, Any]:
    return {
        "providers": {
            "EMBY": {"history": {"baseline": {"items": {emby_key: emby}}}},
            "SIMKL": {"history": {"baseline": {"items": {simkl_key: simkl}}}},
        }
    }


CFG = {
    "pairs": [
        {
            "id": "p1",
            "enabled": True,
            "source": "EMBY",
            "target": "SIMKL",
            "mode": "one-way",
            "features": {"history": {"enable": True}},
        }
    ]
}


def _match(emby: dict[str, Any], emby_key: str, simkl: dict[str, Any], simkl_key: str) -> str:
    state = _state(emby, emby_key, simkl, simkl_key)
    ctx = A._analysis_context(state, CFG)
    return A._target_peer_match(ctx, "EMBY", "history", emby_key, emby, "SIMKL")


def test_episode_matches_when_providers_share_only_imdb_show_id() -> None:
    # SIMKL has no TMDb for this show; Emby has no TVDb. The one shared namespace
    # (IMDb) must still be enough to pair the two episodes.
    emby = _emby_episode()
    simkl = _simkl_episode({"simkl": "111", "imdb": SHOW_IMDB, "tvdb": SHOW_TVDB})
    assert _match(emby, f"tmdb:{SHOW_TMDB}#s06e03@1767312000", simkl, f"imdb:{SHOW_IMDB}#s06e03@1767312300")


def test_episode_level_imdb_id_does_not_change_peer_matching() -> None:
    simkl = _simkl_episode({"simkl": "111", "tmdb": SHOW_TMDB, "imdb": SHOW_IMDB})
    key = f"tmdb:{SHOW_TMDB}#s06e03@1767312000"
    dst_key = f"tmdb:{SHOW_TMDB}#s06e03@1767312300"
    with_id = _match(_emby_episode(with_episode_imdb=True), key, simkl, dst_key)
    without_id = _match(_emby_episode(with_episode_imdb=False), key, simkl, dst_key)
    assert with_id == without_id == "history_exact"


def test_alias_keys_never_expose_an_episode_id_as_show_identity() -> None:
    item = dict(_emby_episode())
    item["_key"] = f"tmdb:{SHOW_TMDB}#s06e03@1767312000"
    aliases = A._alias_keys(item)
    assert f"imdb:{EP_IMDB}" not in aliases
    assert f"episode:imdb:{EP_IMDB}" not in aliases
    assert f"ep:imdb:{EP_IMDB}" in aliases
    assert f"tmdb:{SHOW_TMDB}#s06e03" in aliases
    assert f"imdb:{SHOW_IMDB}#s06e03" in aliases


def test_alias_keys_scope_show_ids_to_the_episode_coordinate() -> None:
    # Two episodes of the same show must not alias onto each other.
    s06e03 = dict(_emby_episode())
    s01e01 = dict(_emby_episode())
    s01e01["season"] = 1
    s01e01["episode"] = 1
    s01e01["ids"] = {"imdb": "tt9022222"}
    assert not (set(A._alias_keys(s06e03)) & set(A._alias_keys(s01e01)))
