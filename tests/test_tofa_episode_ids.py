# tests/test_tofa_episode_ids.py
# CrossWatch - Tofa resolves episodes by their TMDB episode id before coordinates
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

from providers.sync.tofa._common import build_library, resolve_item


SHOW = {"tmdb": "1429", "tvdb": "267440", "imdb": "tt2560140"}


def row(fid: str, s: int, e: int, tmdb_episode_id: int | None) -> dict[str, Any]:
    return {"media_file_id": fid, "media_id": "aot", "media_type": "tv", "title": "Attack on Titan", "edition": None,
            "available": True, "tmdb_id": 1429, "imdb_id": "tt2560140", "tvdb_id": "267440", "episode_id": f"ep-{s}-{e}",
            "season_number": s, "episode_number": e, "tmdb_episode_id": tmdb_episode_id}


def lib(*rows: dict[str, Any]):
    return build_library({"lib": list(rows)})


def item(ids: dict[str, str], s: int = 1, e: int = 2, show_ids: dict[str, str] | None = None) -> dict[str, Any]:
    return {"type": "episode", "ids": ids, "show_ids": dict(SHOW if show_ids is None else show_ids), "season": s, "episode": e}


def test_tmdb_episode_id_beats_coordinates():
    library = lib(row("f-102", 1, 2, 65490), row("f-201", 2, 1, 1265623))

    assert resolve_item(library, item({"tmdb": "1265623"})) == (("aot", 2, 1), "episode")


def test_conflicting_coordinate_hit_is_not_used():
    library = lib(row("f-102", 1, 2, 65490))

    assert resolve_item(library, item({"tmdb": "1265623"})) == ("", "episode")


def test_coordinates_still_work_without_episode_ids():
    library = lib(row("f-102", 1, 2, 65490), row("f-201", 2, 1, 1265623))

    assert resolve_item(library, item({})) == (("aot", 1, 2), "episode")


def test_coordinate_hit_without_tmdb_episode_id_still_matches():
    library = lib(row("f-102", 1, 2, None))

    assert resolve_item(library, item({"tmdb": "1265623"})) == (("aot", 1, 2), "episode")


def test_show_tmdb_in_episode_slot_is_not_an_episode_id():
    library = lib(row("f-102", 1, 2, 65490))

    assert resolve_item(library, item({"tmdb": "1429"})) == (("aot", 1, 2), "episode")


def test_tvdb_only_item_keeps_coordinates():
    library = lib(row("f-102", 1, 2, 65490), row("f-201", 2, 1, 1265623))

    assert resolve_item(library, item({"tvdb": "5978808"})) == (("aot", 1, 2), "episode")


def test_whitelisted_view_keeps_the_episode_id_index():
    full = build_library({"a": [row("f-102", 1, 2, 65490)], "b": [row("f-201", 2, 1, 1265623)]})
    view = build_library(full.rows_by_library, {"b"})

    assert resolve_item(view, item({"tmdb": "1265623"})) == (("aot", 2, 1), "episode")
    assert resolve_item(view, item({"tmdb": "65490"})) == ("", "episode")
