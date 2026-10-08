# /tests/test_anilist_history_orchestrator.py
# CrossWatch - History sync to and from AniList through the orchestrator
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.overrides import upsert_override
from cw_platform.anime_mapping.service import anime_only_adds
from cw_platform.id_map import canonical_key
from cw_platform.orchestrator.facade import Orchestrator
from providers.sync import _mod_ANILIST as anilist_mod
from providers.sync.anilist import _history as history

MAPPINGS: dict[str, Any] = {
    "tvdb_show:267440:s0": {"anilist:18397": {"7": "1"}},
    "tvdb_show:267440:s1": {"anilist:16498": {"1-25": "1-25"}},
    "tvdb_show:267440:s3": {"anilist:99147": {"1-12": "1-12"}, "anilist:104578": {"13-22": "1-10"}},
}

MEDIA: dict[int, dict[str, Any]] = {
    16498: {"id": 16498, "idMal": 16498, "format": "TV", "episodes": 25, "status": "FINISHED", "title": {"english": "Attack on Titan"}},
    104578: {"id": 104578, "idMal": 38524, "format": "TV", "episodes": 10, "status": "FINISHED", "title": {"english": "Attack on Titan Season 3 Part 2"}},
    21519: {"id": 21519, "idMal": 32281, "format": "MOVIE", "episodes": 1, "status": "FINISHED", "title": {"english": "Your Name."}},
}

AOT = {"tvdb": "267440"}
DARK = {"tvdb": "334824"}
YOUR_NAME = {"anilist": "21519", "mal": "32281"}


class FakeAniList:
    entries: dict[int, dict[str, Any]] = {}
    saves: list[dict[str, Any]] = []
    calls: list[str] = []

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def viewer(self) -> dict[str, Any]:
        return {"id": 1, "name": "tester"}

    def gql(self, query: str, variables: Mapping[str, Any] | None = None, **_kwargs: Any) -> dict[str, Any]:
        variables = dict(variables or {})
        self.calls.append(query)
        if "MediaListCollection" in query:
            rows = [{**entry, "mediaId": media_id, "media": MEDIA[media_id]} for media_id, entry in self.entries.items()]
            return {"MediaListCollection": {"lists": [{"entries": rows}]}}
        if "SaveMediaListEntry" in query:
            self.saves.append(variables)
            if "mediaId" in variables:
                entry = self.entries.setdefault(int(variables["mediaId"]), {"id": len(self.entries) + 1, "updatedAt": 1790000000})
            else:
                entry = next(row for row in self.entries.values() if row["id"] == variables["id"])
            entry.update({key: variables[key] for key in ("status", "progress", "startedAt", "completedAt") if key in variables})
            return {"SaveMediaListEntry": {"id": entry["id"], "status": entry.get("status"), "progress": entry.get("progress"),
                                          "completedAt": entry.get("completedAt")}}
        if "id_in" in query:
            return {"Page": {"media": [MEDIA[i] for i in variables.get("ids") or [] if i in MEDIA]}}
        raise AssertionError(f"unexpected query: {query[:60]}")

    @staticmethod
    def normalize(obj: Any) -> dict[str, Any]:
        return anilist_mod.id_minimal(obj)

    @staticmethod
    def key_of(obj: Any) -> str:
        return canonical_key(anilist_mod.id_minimal(obj)) or ""


@dataclass
class FakeSource:
    index: dict[str, dict[str, Any]]
    add_calls: list[list[dict[str, Any]]] = field(default_factory=list)

    def name(self) -> str:
        return "JELLYFIN"

    def label(self) -> str:
        return "JELLYFIN"

    def features(self) -> Mapping[str, bool]:
        return {"history": True}

    def capabilities(self) -> Mapping[str, Any]:
        return {"features": {"history": True}, "observed_deletes": False, "index_semantics": "present"}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return True

    def health(self, cfg: Mapping[str, Any], **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"history": True}, "api": {}}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return dict(self.index)

    def add(self, cfg, items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        self.add_calls.append([dict(x) for x in items])
        return {"ok": True, "count": 0}

    def remove(self, cfg, items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return {"ok": True, "count": 0}


@pytest.fixture()
def anilist(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> type[FakeAniList]:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    FakeAniList.entries = {}
    FakeAniList.saves = []
    FakeAniList.calls = []
    monkeypatch.setattr(anilist_mod, "ANILISTClient", FakeAniList)
    monkeypatch.setattr(type(anilist_mod.OPS), "health", lambda self, cfg: {
        "ok": True, "status": "ok", "features": {"watchlist": True, "ratings": True, "history": True}, "api": {}})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda _cfg, _name: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_unresolved_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_blackbox_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_blackbox_keys", lambda *_a, **_k: set())
    return FakeAniList


def _entry(status: str, progress: int) -> dict[str, Any]:
    return {"id": 1000 + progress, "status": status, "progress": progress, "updatedAt": 1785000000, "createdAt": 1780000000}


def _episode(show_ids: dict[str, str], season: int, episode: int, watched_at: str = "2026-09-01T12:00:00Z") -> dict[str, Any]:
    return {"type": "episode", "title": f"S{season:02d}E{episode:02d}", "season": season, "episode": episode,
            "watched": True, "watched_at": watched_at, "ids": {}, "show_ids": dict(show_ids)}


def _movie(ids: dict[str, str], watched_at: str = "2026-09-02T20:00:00Z") -> dict[str, Any]:
    return {"type": "movie", "title": "Your Name.", "year": 2016, "ids": dict(ids), "watched": True, "watched_at": watched_at}


def _cfg(mapping_enabled: bool = True, source_status: bool = False) -> dict[str, Any]:
    return {
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "anilist": {"access_token": "token"},
        "anime_mapping": {"enabled": mapping_enabled, "release_tag": "v3", "use_for_pairs": ["anilist", "simkl"]},
        "sync": {"dry_run": False, "enable_add": True, "enable_remove": False,
                 "include_observed_deletes": False, "allow_mass_delete": False},
        "pairs": [{"id": "p1", "enabled": True, "source": "JELLYFIN", "target": "ANILIST", "mode": "one-way",
                   "feature": "history", "features": {"history": {"enable": True, "add": True, "remove": False,
                                                                 "use_source_status": source_status}}}],
    }


def _sync(monkeypatch: pytest.MonkeyPatch, source: list[dict[str, Any]], *, mapping_enabled: bool = True,
          source_status: bool = False) -> list[dict[str, Any]]:
    src = FakeSource({canonical_key(item): item for item in source})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"JELLYFIN": src, "ANILIST": anilist_mod.OPS})
    before = len(FakeAniList.saves)
    Orchestrator(_cfg(mapping_enabled, source_status)).run()
    assert src.add_calls == []
    return FakeAniList.saves[before:]


def test_index_reports_list_progress_as_aired_episodes(anilist: type[FakeAniList]) -> None:
    anilist.entries = {16498: _entry("CURRENT", 2), 104578: _entry("CURRENT", 3), 21519: _entry("COMPLETED", 1)}
    index = anilist_mod.OPS.build_index(_cfg(), feature="history")

    assert sorted(index) == ["mal:32281", "tvdb:267440#s01e01", "tvdb:267440#s01e02",
                             "tvdb:267440#s03e13", "tvdb:267440#s03e14", "tvdb:267440#s03e15"]
    assert index["mal:32281"]["type"] == "movie"
    assert all(item["watched_at"] for item in index.values())


@pytest.mark.parametrize("target", ["SIMKL", "FLOPPY"])
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("custom", [False, True])
def test_history_syncs_from_anilist_and_stays_stable(anilist, monkeypatch, target, mode, custom):
    paths = storage.paths("v3")
    paths["mappings"].write_text(json.dumps({
        "tmdb_show:1429:s1": {"anilist:16498": {"1-25": "1-25"}},
        "tmdb_show:1429:s3": {"anilist:104578": {"13-22": "1-10"}},
        "tmdb_movie:372058": {"anilist:21519": {"1": "1"}},
    }), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    if custom:
        upsert_override({"media_type": "show", "match_provider": "tmdb", "match_id": "4242", "match_season": 2,
                         "episode_from": 5, "episode_to": 29, "episode_start_at": 1,
                         "target_namespace": "anilist", "target_id": "16498"})

    class Peer(FakeSource):
        def name(self):
            return target

        def label(self):
            return target

        def add(self, cfg, items, *, feature, dry_run=False):
            rows = [dict(item) for item in items]
            self.add_calls.append(rows)
            confirmed = [canonical_key(item) for item in rows]
            self.index.update(zip(confirmed, rows))
            return {"ok": True, "count": len(confirmed), "confirmed_keys": confirmed}

    seed = [_episode({"tmdb": "1429"}, 3, n) for n in (13, 14)] if mode == "two-way" else []
    peer = Peer({canonical_key(item): item for item in seed})
    anilist.entries = {16498: _entry("CURRENT", 2), 21519: _entry("COMPLETED", 1)}
    cfg = _cfg()
    cfg["pairs"][0].update(source="ANILIST", target=target, mode=mode)
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {target: peer, "ANILIST": anilist_mod.OPS})
    Orchestrator(cfg).run()
    added = [item for batch in peer.add_calls for item in batch]
    episodes = [item for item in added if item["type"] == "episode"]
    assert {(item["show_ids"]["tmdb"], item["season"], item["episode"]) for item in episodes} == (
        {("4242", 2, 5), ("4242", 2, 6)} if custom else {("1429", 1, 1), ("1429", 1, 2)})
    assert [item["ids"]["tmdb"] for item in added if item["type"] == "movie"] == ["372058"]
    assert all(item.get("watched_at") for item in added)
    if mode == "two-way":
        assert anilist.entries[104578]["progress"] == 2
    else:
        assert not anilist.saves
    writes = (len(peer.add_calls), len(anilist.saves))
    Orchestrator(cfg).run()
    assert (len(peer.add_calls), len(anilist.saves)) == writes


def test_only_episodes_beyond_anilist_progress_are_written_once_per_entry(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    anilist.entries = {16498: _entry("CURRENT", 5), 104578: _entry("CURRENT", 3)}
    source = [_episode(AOT, 1, 5), _episode(AOT, 1, 6), _episode(AOT, 1, 7), _episode(AOT, 3, 15), _episode(AOT, 3, 16)]

    saves = _sync(monkeypatch, source)

    assert sorted((save["mediaId"], save["progress"], save["status"]) for save in saves) == [
        (16498, 7, "CURRENT"), (104578, 4, "CURRENT")]


def test_second_run_writes_nothing(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    source = [_episode(AOT, 1, 1), _episode(AOT, 1, 2), _episode(AOT, 3, 13)]

    first = _sync(monkeypatch, source)
    second = _sync(monkeypatch, source)

    assert sorted((save["mediaId"], save["progress"]) for save in first) == [(16498, 2), (104578, 1)]
    assert second == []


def test_non_anime_and_specials_never_reach_anilist(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    saves = _sync(monkeypatch, [_episode(DARK, 1, 1), _episode(AOT, 0, 7), _episode(AOT, 1, 1)])

    assert [(save["mediaId"], save["progress"]) for save in saves] == [(16498, 1)]


@pytest.mark.parametrize("provider,season", [("tvdb", 1), ("tmdb", 2), ("imdb", 3)])
def test_custom_episode_mapping_syncs_without_anibridge_entry(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch,
                                                             provider: str, season: int) -> None:
    target_id = 999901
    source_id = "tt9999999" if provider == "imdb" else "999999"
    monkeypatch.setitem(MEDIA, target_id, {**MEDIA[16498], "id": target_id})
    upsert_override({"media_type": "show", "match_provider": provider, "match_id": source_id, "match_season": season,
                     "target_namespace": "anilist", "target_id": str(target_id), "episode_from": 5,
                     "episode_to": 10, "episode_start_at": 1})
    source = [_episode({provider: source_id}, season, 7)]

    [save] = _sync(monkeypatch, source)

    assert (save["mediaId"], save["progress"]) == (target_id, 3)
    assert _sync(monkeypatch, source) == []


@pytest.mark.parametrize("enabled,season,episode", [(False, 2, 5), (True, 1, 5), (True, 2, 4), (True, 2, 11), (True, 0, 5)])
def test_custom_episode_mapping_does_not_sync_disabled_or_unmatched_rules(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch,
                                                                        enabled: bool, season: int, episode: int) -> None:
    upsert_override({"enabled": enabled, "media_type": "show", "match_provider": "tvdb", "match_id": "999999",
                     "match_season": 0 if season == 0 else 2, "target_namespace": "anilist", "target_id": "16498",
                     "episode_from": 5, "episode_to": 10, "episode_start_at": 1})

    assert _sync(monkeypatch, [_episode({"tvdb": "999999"}, season, episode)]) == []


def _rule(**fields: Any) -> None:
    upsert_override({"media_type": "show", "match_provider": "tvdb", "match_id": "999999", "match_season": 2,
                     "target_namespace": "anilist", "target_id": "999901", "episode_from": 5, "episode_to": 10,
                     "episode_start_at": 1, **fields})


def test_custom_episode_mapping_takes_priority_over_anibridge(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(MEDIA, 999901, {**MEDIA[16498], "id": 999901})
    _rule(match_id="267440", match_season=1)
    source = [_episode(AOT, 1, 7)]

    [save] = _sync(monkeypatch, source)

    assert (save["mediaId"], save["progress"]) == (999901, 3)
    assert _sync(monkeypatch, source) == []


def test_custom_episode_mapping_applies_when_the_anibridge_entry_is_already_ahead(anilist: type[FakeAniList],
                                                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    anilist.entries = {16498: _entry("CURRENT", 25)}
    monkeypatch.setitem(MEDIA, 999901, {**MEDIA[16498], "id": 999901})
    _rule(match_id="267440", match_season=1)
    source = [_episode(AOT, 1, 4), _episode(AOT, 1, 7)]

    [save] = _sync(monkeypatch, source)

    assert (save["mediaId"], save["progress"]) == (999901, 3)
    assert anilist.entries[16498]["progress"] == 25
    assert _sync(monkeypatch, source) == []
    index = anilist_mod.OPS.build_index(_cfg(), feature="history")
    assert {"tvdb:267440#s01e04", "tvdb:267440#s01e07", "tvdb:267440#s01e11"} <= set(index)
    assert "tvdb:267440#s01e08" not in index


def test_disabled_custom_episode_mapping_falls_back_to_anibridge(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    _rule(match_id="267440", match_season=1, enabled=False)

    [save] = _sync(monkeypatch, [_episode(AOT, 1, 7)])

    assert (save["mediaId"], save["progress"]) == (16498, 7)


@pytest.mark.parametrize("enabled,season,episode", [(False, 2, 5), (True, 1, 5), (True, 2, 4), (True, 2, 11)])
def test_history_filter_drops_disabled_or_unmatched_custom_mappings(anilist: type[FakeAniList], enabled: bool,
                                                                   season: int, episode: int) -> None:
    _rule(enabled=enabled)
    item = _episode({"tvdb": "999999"}, season, episode)

    assert anime_only_adds([item], _cfg(), {}, "history") == ([], 1)
    _rule(match_season=season, episode_from=episode, episode_to=episode)
    assert anime_only_adds([item], _cfg(), {}, "history") == ([item], 0)


def test_history_filter_drops_specials_even_with_a_custom_mapping(anilist: type[FakeAniList]) -> None:
    _rule(match_season=0)

    assert anime_only_adds([_episode({"tvdb": "999999"}, 0, 7)], _cfg(), {}, "history") == ([], 1)


@pytest.mark.parametrize("feature", ["watchlist", "ratings"])
@pytest.mark.parametrize("options", [{}, {"use_anime_mapping": False, "anime_only_sync": False}])
def test_anilist_title_filter_is_always_anime_only(anilist: type[FakeAniList], feature: str, options: dict) -> None:
    anime = _movie(YOUR_NAME)
    other = {"type": "movie", "ids": {"tmdb": "101"}, "title": "Leon"}

    assert anime_only_adds([anime, other], _cfg(), options, feature) == ([anime], 1)


@pytest.mark.parametrize("feature", ["watchlist", "ratings"])
def test_anilist_title_filter_keeps_native_ids_without_mapping(config_base: Path, feature: str) -> None:
    anime = _movie(YOUR_NAME)
    other = {"type": "movie", "ids": {"tmdb": "101"}, "title": "Leon"}

    assert anime_only_adds([anime, other], {}, {}, feature) == ([anime], 1)


@pytest.mark.parametrize("namespace", ["simkl", "kitsu", "mal", "anidb"])
def test_history_filter_drops_custom_mappings_that_cannot_reach_anilist(anilist: type[FakeAniList], namespace: str) -> None:
    _rule(target_namespace=namespace, target_id="777")

    assert anime_only_adds([_episode({"tvdb": "999999"}, 2, 7)], _cfg(), {}, "history") == ([], 1)
    assert anime_only_adds([_episode(AOT, 1, 7)], _cfg(), {}, "history")[1] == 0
    _rule(match_id="267440", match_season=1, target_namespace=namespace, target_id="777")
    assert anime_only_adds([_episode(AOT, 1, 7)], _cfg(), {}, "history") == ([], 1)


def test_custom_mapping_to_a_mal_id_syncs_once(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    storage.paths("v3")["mappings"].write_text(json.dumps({**MAPPINGS, "mal:777": {"anilist:999901": {"1-25": "1-25"}}}), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    monkeypatch.setitem(MEDIA, 999901, {**MEDIA[16498], "id": 999901, "idMal": 777})
    _rule(target_namespace="mal", target_id="777")
    source = [_episode({"tvdb": "999999"}, 2, 7)]

    [save] = _sync(monkeypatch, source)

    assert (save["mediaId"], save["progress"]) == (999901, 3)
    assert _sync(monkeypatch, source) == []
    assert "tvdb:999999#s02e07" in anilist_mod.OPS.build_index(_cfg(), feature="history")


def _remap(extra: dict[str, Any]) -> None:
    storage.paths("v3")["mappings"].write_text(json.dumps({**MAPPINGS, **extra}), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")


def test_a_lower_priority_custom_mapping_does_not_hide_the_winning_one(anilist: type[FakeAniList],
                                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    anilist.entries = {16498: _entry("CURRENT", 25)}
    monkeypatch.setitem(MEDIA, 999901, {**MEDIA[16498], "id": 999901})
    _rule()
    _rule(target_id="16498")
    source = [_episode({"tvdb": "999999"}, 2, 7)]

    [save] = _sync(monkeypatch, source)

    assert (save["mediaId"], save["progress"]) == (999901, 3)
    assert _sync(monkeypatch, source) == []


def test_custom_mapping_to_a_mal_id_respects_the_mal_episode_offset(anilist: type[FakeAniList],
                                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    _remap({"mal:777": {"anilist:999901": {"1-20": "6-25"}}})
    monkeypatch.setitem(MEDIA, 999901, {**MEDIA[16498], "id": 999901, "idMal": 777})
    anilist.entries = {999901: _entry("CURRENT", 8)}
    _rule(target_namespace="mal", target_id="777", episode_from=1, episode_to=20)
    source = [_episode({"tvdb": "999999"}, 2, 6)]

    index = anilist_mod.OPS.build_index(_cfg(), feature="history")
    [save] = _sync(monkeypatch, source)

    assert sorted(index) == ["tvdb:999999#s02e01", "tvdb:999999#s02e02", "tvdb:999999#s02e03"]
    assert (save["mediaId"], save["progress"]) == (999901, 11)
    assert _sync(monkeypatch, source) == []


def test_custom_mapping_to_an_anidb_id_syncs_once(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    _remap({"anidb:555:R": {"anilist:999901": {"1-25": "1-25"}}})
    monkeypatch.setitem(MEDIA, 999901, {**MEDIA[16498], "id": 999901})
    _rule(target_namespace="anidb", target_id="555")
    source = [_episode({"tvdb": "999999"}, 2, 7)]

    [save] = _sync(monkeypatch, source)

    assert (save["mediaId"], save["progress"]) == (999901, 3)
    assert _sync(monkeypatch, source) == []
    assert "tvdb:999999#s02e07" in anilist_mod.OPS.build_index(_cfg(), feature="history")


def test_two_custom_mappings_for_one_entry_are_both_indexed(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(MEDIA, 999901, {**MEDIA[16498], "id": 999901})
    _rule()
    _rule(match_provider="tmdb", match_id="555")

    [save] = _sync(monkeypatch, [_episode({"tvdb": "999999", "tmdb": "555"}, 2, 7)])
    index = anilist_mod.OPS.build_index(_cfg(), feature="history")

    assert (save["mediaId"], save["progress"]) == (999901, 3)
    assert {"tvdb:999999#s02e07", "tmdb:555#s02e07"} <= set(index)


def test_finishing_an_entry_completes_it_with_the_real_watch_dates(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    source = [_episode(AOT, 3, 12 + number, f"2026-08-{number:02d}T19:00:00Z") for number in range(1, 11)]

    [save] = _sync(monkeypatch, source)

    assert (save["mediaId"], save["progress"], save["status"]) == (104578, 10, "COMPLETED")
    assert save["startedAt"] == {"year": 2026, "month": 8, "day": 1}
    assert save["completedAt"] == {"year": 2026, "month": 8, "day": 10}


def test_higher_or_completed_anilist_progress_is_never_touched(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    anilist.entries = {16498: _entry("PAUSED", 9), 104578: _entry("COMPLETED", 10)}

    assert _sync(monkeypatch, [_episode(AOT, 1, 5), _episode(AOT, 3, 14)]) == []


def test_anime_movie_is_marked_completed_and_then_stays_quiet(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    first = _sync(monkeypatch, [_movie(YOUR_NAME)])
    second = _sync(monkeypatch, [_movie(YOUR_NAME)])

    assert [(save["mediaId"], save["progress"], save["status"]) for save in first] == [(21519, 1, "COMPLETED")]
    assert second == []


def test_nothing_is_planned_or_written_without_anime_mapping(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    anilist.entries = {16498: _entry("CURRENT", 2)}

    assert _sync(monkeypatch, [_episode(AOT, 1, 5), _episode(DARK, 1, 1)], mapping_enabled=False) == []
    assert anilist_mod.OPS.build_index(_cfg(False), feature="history") == {}
    result = anilist_mod.OPS.add(_cfg(False), [_episode(AOT, 1, 5)], feature="history")
    assert (result["count"], result["unresolved"][0]["reason"]) == (0, "anime_mapping_unavailable")
    assert anilist.saves == []


def _with_status(items: list[dict[str, Any]], status: str) -> list[dict[str, Any]]:
    return [{**item, "watch_status": status} for item in items]


@pytest.mark.parametrize("status,expected", [("dropped", "DROPPED"), ("on_hold", "PAUSED"), ("watching", "CURRENT"), ("planning", "CURRENT")])
def test_source_watch_status_is_used_when_the_option_is_on(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch,
                                                           status: str, expected: str) -> None:
    source = _with_status([_episode(AOT, 1, 1), _episode(AOT, 1, 2), _episode(AOT, 1, 3), _episode(AOT, 1, 4)], status)

    [save] = _sync(monkeypatch, source, source_status=True)

    assert (save["mediaId"], save["progress"], save["status"]) == (16498, 4, expected)


def test_source_watch_status_is_ignored_when_the_option_is_off(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    [save] = _sync(monkeypatch, _with_status([_episode(AOT, 1, 1), _episode(AOT, 1, 2)], "dropped"))

    assert (save["progress"], save["status"]) == (2, "CURRENT")


def test_a_finished_title_is_completed_even_when_the_source_says_dropped(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch) -> None:
    source = _with_status([_episode(AOT, 3, 12 + number) for number in range(1, 11)], "dropped")

    [save] = _sync(monkeypatch, source, source_status=True)

    assert (save["progress"], save["status"]) == (10, "COMPLETED")


@pytest.mark.parametrize("option,expected", [(True, "DROPPED"), (False, "CURRENT")])
def test_an_entry_dropped_on_anilist_stays_dropped_only_with_the_option(anilist: type[FakeAniList], monkeypatch: pytest.MonkeyPatch,
                                                                        option: bool, expected: str) -> None:
    anilist.entries = {16498: _entry("DROPPED", 2)}

    [save] = _sync(monkeypatch, [_episode(AOT, 1, 3), _episode(AOT, 1, 4)], source_status=option)

    assert (save["progress"], save["status"]) == (4, expected)


def test_simkl_history_attaches_the_watch_status_only_when_asked() -> None:
    from types import SimpleNamespace

    from cw_platform.anime_mapping.service import config_with_pair_feature_options
    from providers.sync.simkl import _history as simkl_history

    body = {"anime": [{"status": "dropped", "show": {"ids": {"simkl": 9001}}}, {"status": "hold", "show": {"ids": {"simkl": 9002}}}],
            "shows": [{"status": "watching", "show": {"ids": {"simkl": 9003}}}], "movies": [{"status": "completed", "movie": {"ids": {"simkl": 9004}}}]}
    calls: list[str] = []

    def get(url: str, **_kwargs: Any) -> Any:
        calls.append(url)
        return SimpleNamespace(ok=True, json=lambda: body)

    def adapter(option: bool) -> Any:
        raw = config_with_pair_feature_options({}, {"use_source_status": option, "feature": "history"})
        return SimpleNamespace(client=SimpleNamespace(session=SimpleNamespace(get=get)), cfg=SimpleNamespace(timeout=5), raw_cfg=raw)

    def index() -> dict[str, dict[str, Any]]:
        return {"a": {"type": "episode", "show_ids": {"simkl": "9001"}, "season": 1, "episode": 1},
                "b": {"type": "episode", "show_ids": {"simkl": "9002"}, "season": 1, "episode": 1},
                "c": {"type": "episode", "show_ids": {"simkl": "9003"}, "season": 1, "episode": 1},
                "d": {"type": "movie", "ids": {"simkl": "9004"}},
                "e": {"type": "movie", "ids": {"simkl": "1"}}}

    original = simkl_history._headers
    simkl_history._headers = lambda *_a, **_k: {}
    try:
        off = simkl_history._with_watch_status(adapter(False), index())
        on = simkl_history._with_watch_status(adapter(True), index())
    finally:
        simkl_history._headers = original

    assert all("watch_status" not in item for item in off.values())
    assert {key: item.get("watch_status") for key, item in on.items()} == {
        "a": "dropped", "b": "on_hold", "c": "watching", "d": "completed", "e": None}
    assert len(calls) == 1


@pytest.mark.parametrize("status", ["CURRENT", "COMPLETED", "PAUSED", "DROPPED", "REPEATING"])
def test_history_remove_batches_tail_and_preserves_fields(anilist, status):
    anilist.entries = {16498: {**_entry(status, 25), "score": 8, "notes": "Keep", "repeat": 2,
                             "startedAt": {"year": 2025}, "completedAt": {"year": 2026, "month": 1, "day": 1}}}
    items = [_episode(AOT, 1, n) for n in range(2, 26)]
    result = anilist_mod.OPS.remove(_cfg(), items, feature="history")
    assert result["count"] == 24 and not result["unresolved"]
    assert len(anilist.calls) == 2 and len(anilist.saves) == 1
    entry = anilist.entries[16498]
    assert (entry["progress"], entry["status"]) == (1, "CURRENT")
    assert (entry["score"], entry["notes"], entry["repeat"], entry["startedAt"]) == (8, "Keep", 2, {"year": 2025})
    assert entry["completedAt"] == {"year": None, "month": None, "day": None}
    assert set(anilist.saves[0]) == {"id", "progress", "status", "completedAt"}
    assert anilist_mod.OPS.capabilities()["history"]["remove"] is True


@pytest.mark.parametrize("movie", [False, True])
def test_history_remove_reset_and_repeat_are_idempotent(anilist, movie):
    ident = 21519 if movie else 104578
    anilist.entries = {ident: {**_entry("COMPLETED", 0), "notes": "Keep"}}
    items = [_movie(YOUR_NAME)] if movie else [_episode(AOT, 3, n) for n in range(13, 23)]
    result = anilist_mod.OPS.remove(_cfg(), items, feature="history")
    assert result["count"] == len(items) and not result["unresolved"]
    assert len(anilist.calls) == 2
    assert (anilist.entries[ident]["progress"], anilist.entries[ident]["status"]) == (0, "PAUSED")
    assert anilist.entries[ident]["notes"] == "Keep"
    assert anilist_mod.OPS.remove(_cfg(), items, feature="history")["count"] == len(items)
    assert len(anilist.saves) == 1


def test_history_remove_keeps_gaps_and_deduplicates_aliases(anilist):
    anilist.entries = {16498: _entry("CURRENT", 5)}
    items = [_episode(AOT, 1, n) for n in (5, 5, 4, 2, 7)]
    result = anilist_mod.OPS.remove(_cfg(), items, feature="history")
    assert result["count"] == 3
    assert result["unresolved"][0]["reason"] == "anilist_history_remove_would_erase_later_episodes"
    assert anilist.entries[16498]["progress"] == 3
    assert len(anilist.saves) == 1


@pytest.mark.parametrize("bad", ["raise", "null", "wrong_progress", "wrong_id", "wrong_status", "kept_date", "missing_date"])
def test_history_remove_never_confirms_failed_or_unverified_writes(anilist, monkeypatch, bad):
    anilist.entries = {16498: _entry("CURRENT", 3)}
    original = anilist.gql

    def gql(self, query, variables=None, **kwargs):
        if query != history.GQL_REMOVE_HISTORY:
            return original(self, query, variables, **kwargs)
        if bad == "raise":
            raise RuntimeError("write failed")
        saved = {"id": variables["id"], "status": variables["status"], "progress": variables["progress"], "completedAt": None}
        if bad == "null":
            return {"SaveMediaListEntry": None}
        if bad == "wrong_progress":
            saved["progress"] = 3
        if bad == "wrong_id":
            saved["id"] = 999
        if bad == "wrong_status":
            saved["status"] = "COMPLETED"
        if bad == "kept_date":
            saved["completedAt"] = {"year": 2026}
        if bad == "missing_date":
            saved.pop("completedAt")
        return {"SaveMediaListEntry": saved}

    monkeypatch.setattr(anilist, "gql", gql)
    result = anilist_mod.OPS.remove(_cfg(), [_episode(AOT, 1, n) for n in (1, 2, 3)], feature="history")
    assert not result["confirmed_keys"] and len(result["unresolved"]) == 3


@pytest.mark.parametrize("bad", ["raise", "null", "bad_bucket", "bad_entry", "missing_id"])
def test_history_remove_does_not_treat_failed_reads_as_absent(anilist, monkeypatch, bad):
    def gql(self, query, variables=None, **kwargs):
        assert query == history.GQL_HISTORY_LIST
        if bad == "raise":
            raise RuntimeError("read failed")
        return {"MediaListCollection": None if bad == "null" else {"lists": [
            None if bad == "bad_bucket" else {"entries": [None if bad == "bad_entry" else {"media": {"id": 16498}, "progress": 3}]}]}}

    monkeypatch.setattr(anilist, "gql", gql)
    result = anilist_mod.OPS.remove(_cfg(), [_episode(AOT, 1, n) for n in (1, 2)], feature="history")
    assert not result["confirmed_keys"] and len(result["unresolved"]) == 2
    assert not anilist.saves


def test_history_remove_absent_mapping_disabled_specials_and_unknown_total(anilist, monkeypatch):
    item = _episode(AOT, 1, 1)
    assert anilist_mod.OPS.remove(_cfg(), [item], feature="history")["count"] == 1
    result = anilist_mod.OPS.remove(_cfg(False), [item], feature="history")
    assert result["unresolved"][0]["reason"] == "anime_mapping_unavailable"
    anilist.entries = {16498: _entry("COMPLETED", 3), 21519: _entry("COMPLETED", 1)}
    monkeypatch.setitem(MEDIA[16498], "episodes", None)
    items = [item, _episode(AOT, 0, 7), _movie({"anilist": "16498"}), _episode(DARK, 1, 1)]
    result = anilist_mod.OPS.remove(_cfg(), items, feature="history")
    assert not result["confirmed_keys"] and len(result["unresolved"]) == 4
    assert {row["reason"] for row in result["unresolved"]} == {
        "anilist_history_unknown_completed_total", history.SPECIALS_UNSUPPORTED, "anime_media_type_mismatch", history.NOT_MAPPED}
    assert not anilist.saves


@pytest.mark.parametrize("dry_run", [False, True])
def test_history_remove_custom_mapping_is_ordered_before_chunking(anilist, dry_run):
    from cw_platform.orchestrator._applier import apply_remove

    anilist.entries = {16498: _entry("CURRENT", 3)}
    upsert_override({"media_type": "show", "match_provider": "tmdb", "match_id": "999", "match_season": 2,
                     "episode_from": 1, "episode_to": 1, "episode_start_at": 3,
                     "target_namespace": "anilist", "target_id": "16498"})
    items = [_episode(AOT, 1, 1), _episode({"tmdb": "999"}, 2, 1), _episode(AOT, 1, 2)]
    result = apply_remove(dst_ops=anilist_mod.OPS, cfg=_cfg(), dst_name="ANILIST", feature="history",
                          items=items, dry_run=dry_run, emit=lambda *a, **k: None, dbg=lambda *a, **k: None,
                          chunk_size=1, chunk_pause_ms=0)
    assert not result["unresolved"] and result["count"] == (0 if dry_run else 3)
    assert [row["progress"] for row in anilist.saves] == ([] if dry_run else [2, 1, 0])


def test_history_remove_failure_is_isolated_to_one_title(anilist, monkeypatch):
    anilist.entries = {16498: {**_entry("CURRENT", 3), "id": 16498}, 21519: {**_entry("COMPLETED", 1), "id": 21519}}
    original = anilist.gql

    def gql(self, query, variables=None, **kwargs):
        if query == history.GQL_REMOVE_HISTORY and variables["id"] == 16498:
            raise RuntimeError("write failed")
        return original(self, query, variables, **kwargs)

    monkeypatch.setattr(anilist, "gql", gql)
    items = [_episode(AOT, 1, n) for n in (1, 2, 3, 5)] + [_movie(YOUR_NAME)]
    result = anilist_mod.OPS.remove(_cfg(), items, feature="history")
    assert result["count"] == 2 and len(result["unresolved"]) == 3
    assert set(result["confirmed_keys"]) == {canonical_key(items[-1]), canonical_key(items[-2])}
    assert anilist.entries[16498]["progress"] == 3
    assert anilist.entries[21519]["progress"] == 0


def test_history_remove_runs_through_orchestrator_and_stays_stable(anilist, monkeypatch):
    source = FakeSource({canonical_key(item): item for item in [_episode(AOT, 1, n) for n in (1, 2, 3)]})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"JELLYFIN": source, "ANILIST": anilist_mod.OPS})
    cfg = _cfg()
    cfg["sync"].update(enable_remove=True, allow_mass_delete=True)
    cfg["pairs"][0]["features"]["history"].update(remove=True, remove_mode="mirror")
    Orchestrator(cfg).run()
    assert anilist.entries[16498]["progress"] == 3
    remaining = _episode(AOT, 1, 1)
    source.index = {canonical_key(remaining): remaining}
    Orchestrator(cfg).run()
    assert anilist.entries[16498]["progress"] == 1
    assert len(anilist.saves) == 2
    Orchestrator(cfg).run()
    assert len(anilist.saves) == 2
