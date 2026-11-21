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
    "ep-201": {"tmdb_id": "1265623", "tvdb_id": "5978808"},
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


@pytest.fixture
def episodes_call(monkeypatch):
    calls: list[str] = []

    def pages(adapter, path, *, feature, params=None, allow_disabled=False):
        calls.append(path)
        return [{"content_id": cid, **ids} for cid, ids in EPISODE_IDS.items()]

    monkeypatch.setattr(silo, "pages", pages)
    return calls


def item(ids: dict[str, str], s: int = 1, e: int = 2) -> dict[str, Any]:
    return {"type": "episode", "ids": ids, "show_ids": dict(SHOW), "season": s, "episode": e}


ADAPTER = SimpleNamespace()


def test_episode_id_beats_coordinates(episodes_call):
    lib = library()

    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "5978808"}), feature="history") == ("ep-201", "episode")
    assert episodes_call == ["/catalog/items/series-aot/episodes"]


def test_episode_ids_are_fetched_once_per_series(episodes_call):
    lib = library()
    silo.resolve_item(ADAPTER, lib, item({"tvdb": "5978808"}), feature="history")
    silo.resolve_item(ADAPTER, lib, item({"tmdb": "65490"}), feature="history")

    assert len(episodes_call) == 1


def test_listed_episode_ids_need_no_extra_call(episodes_call):
    lib = library(listed_ids=True)

    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "5978808"}), feature="history") == ("ep-201", "episode")
    assert episodes_call == []


def test_conflicting_coordinate_hit_is_not_used(episodes_call):
    lib = library()

    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "9999999"}), feature="history") == ("", "episode")


def test_item_without_episode_ids_keeps_coordinates_and_makes_no_call(episodes_call):
    lib = library()

    assert silo.resolve_item(ADAPTER, lib, item({}), feature="history") == ("ep-102", "episode")
    assert episodes_call == []


def test_show_id_in_episode_slot_is_not_an_episode_id(episodes_call):
    lib = library()

    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "267440"}), feature="history") == ("ep-102", "episode")
    assert episodes_call == []


def test_failed_episode_list_falls_back_to_coordinates_and_is_not_retried(monkeypatch):
    calls: list[str] = []

    def broken(_adapter, path, **_k):
        calls.append(path)
        raise silo.SiloSyncError("silo_history_http_500")

    monkeypatch.setattr(silo, "pages", broken)
    lib = library()

    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "5978808"}), feature="history") == ("ep-102", "episode")
    assert silo.resolve_item(ADAPTER, lib, item({"tvdb": "5978808"}), feature="history") == ("ep-102", "episode")
    assert len(calls) == 1


def test_episode_reads_stay_without_episode_ids():
    lib = library(listed_ids=True)

    assert silo.minimal_for(lib, "ep-201")["ids"] == {}
