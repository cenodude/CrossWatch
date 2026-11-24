# tests/test_silo_episode_ids.py
# CrossWatch - Silo resolves episodes by their own episode ids before coordinates
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from providers.sync.silo import _common as silo


SHOW = {"tmdb": "1429", "tvdb": "267440", "imdb": "tt2560140"}
EPISODE_IDS = {
    "ep-102": {"tmdb_id": "65490", "tvdb_id": "4520243"},
    "ep-201": {"tmdb_id": "1265623", "tvdb_id": "5978808", "imdb_id": "tt1234567"},
}


def library(*, listed_ids: bool = False) -> silo.Library:
    lib = silo.Library()
    lib.add_series({"content_id": "series-aot", "title": "Attack on Titan", "tmdb_id": "1429", "tvdb_id": "267440", "imdb_id": "tt2560140"})
    for cid, s, e in (("ep-102", 1, 2), ("ep-201", 2, 1)):
        row: dict[str, Any] = {"content_id": cid, "series_id": "series-aot", "season_number": s, "episode_number": e}
        if listed_ids:
            row.update(EPISODE_IDS[cid])
        lib.add_episode(row)
    return lib


@pytest.fixture(autouse=True)
def no_episode_requests(monkeypatch):
    def unexpected_request(*args, **kwargs):
        pytest.fail("Episode matching must use the loaded catalog without extra requests")

    monkeypatch.setattr(silo, "silo_request", unexpected_request)


def item(ids: dict[str, str], s: int = 1, e: int = 2) -> dict[str, Any]:
    return {"type": "episode", "ids": ids, "show_ids": dict(SHOW), "season": s, "episode": e}


ADAPTER = SimpleNamespace()


@pytest.mark.parametrize("provider,value", [("tvdb", "5978808"), ("tmdb", "1265623"), ("imdb", "tt1234567")])
def test_episode_id_beats_coordinates(provider, value):
    lib = library(listed_ids=True)

    assert silo.resolve_item(ADAPTER, lib, item({provider: value}), feature="history") == ("ep-201", "episode")


def test_partial_catalog_ids_match_on_first_and_repeated_lookups():
    lib = library(listed_ids=True)
    lib.episodes["ep-102"]["ids"] = {}

    for _ in range(2):
        assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "5978808"}), feature="history") == ("ep-201", "episode")
        assert silo.resolve_item(ADAPTER, lib, item({"tmdb": "65490"}), feature="history") == ("ep-102", "episode")


def test_ambiguous_episode_ids_do_not_fall_back_to_coordinates():
    lib = library(listed_ids=True)
    lib.episodes["ep-102"]["ids"]["tvdb"] = "5978808"

    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "5978808"}), feature="history") == ("", "episode")


@pytest.mark.parametrize("partial", [False, True])
def test_conflicting_coordinate_hit_is_not_used(partial):
    lib = library(listed_ids=True)
    if partial:
        lib.episodes["ep-201"]["ids"] = {}

    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "9999999"}), feature="history") == ("", "episode")


def test_item_without_episode_ids_keeps_coordinates_and_makes_no_call():
    lib = library()

    assert silo.resolve_item(ADAPTER, lib, item({}), feature="history") == ("ep-102", "episode")


def test_show_id_in_episode_slot_is_not_an_episode_id():
    lib = library()

    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "267440"}), feature="history") == ("ep-102", "episode")


def test_missing_catalog_ids_fall_back_to_coordinates_without_requests():
    lib = library()

    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "5978808"}), feature="history") == ("ep-102", "episode")
    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "5978808"}), feature="history") == ("ep-102", "episode")


def test_episode_reads_stay_without_episode_ids():
    lib = library(listed_ids=True)

    assert silo.minimal_for(lib, "ep-201")["ids"] == {}
