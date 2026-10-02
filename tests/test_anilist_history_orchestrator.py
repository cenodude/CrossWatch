# /tests/test_anilist_history_orchestrator.py
# CrossWatch - One-way history sync to AniList through the orchestrator
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest

from cw_platform.anime_mapping import storage
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

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        pass

    def viewer(self) -> dict[str, Any]:
        return {"id": 1, "name": "tester"}

    def gql(self, query: str, variables: Mapping[str, Any] | None = None, **_kwargs: Any) -> dict[str, Any]:
        variables = dict(variables or {})
        if "MediaListCollection" in query:
            rows = [{**entry, "mediaId": media_id, "media": MEDIA[media_id]} for media_id, entry in self.entries.items()]
            return {"MediaListCollection": {"lists": [{"entries": rows}]}}
        if "SaveMediaListEntry" in query:
            self.saves.append(variables)
            entry = self.entries.setdefault(int(variables["mediaId"]), {"id": len(self.entries) + 1, "updatedAt": 1790000000})
            entry.update({key: variables[key] for key in ("status", "progress", "startedAt", "completedAt") if key in variables})
            return {"SaveMediaListEntry": {"id": entry["id"], "status": entry.get("status"), "progress": entry.get("progress")}}
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


def _cfg(mapping_enabled: bool = True) -> dict[str, Any]:
    return {
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "anilist": {"access_token": "token"},
        "anime_mapping": {"enabled": mapping_enabled, "release_tag": "v3", "use_for_pairs": ["anilist", "simkl"]},
        "sync": {"dry_run": False, "enable_add": True, "enable_remove": False,
                 "include_observed_deletes": False, "allow_mass_delete": False},
        "pairs": [{"id": "p1", "enabled": True, "source": "JELLYFIN", "target": "ANILIST", "mode": "one-way",
                   "feature": "history", "features": {"history": {"enable": True, "add": True, "remove": False}}}],
    }


def _sync(monkeypatch: pytest.MonkeyPatch, source: list[dict[str, Any]], *, mapping_enabled: bool = True) -> list[dict[str, Any]]:
    src = FakeSource({canonical_key(item): item for item in source})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"JELLYFIN": src, "ANILIST": anilist_mod.OPS})
    before = len(FakeAniList.saves)
    Orchestrator(_cfg(mapping_enabled)).run()
    assert src.add_calls == []
    return FakeAniList.saves[before:]


def test_index_reports_list_progress_as_aired_episodes(anilist: type[FakeAniList]) -> None:
    anilist.entries = {16498: _entry("CURRENT", 2), 104578: _entry("CURRENT", 3), 21519: _entry("COMPLETED", 1)}
    index = anilist_mod.OPS.build_index(_cfg(), feature="history")

    assert sorted(index) == ["mal:32281", "tvdb:267440#s01e01", "tvdb:267440#s01e02",
                             "tvdb:267440#s03e13", "tvdb:267440#s03e14", "tvdb:267440#s03e15"]
    assert index["mal:32281"]["type"] == "movie"
    assert all(item["watched_at"] for item in index.values())


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


def test_remove_is_refused(anilist: type[FakeAniList]) -> None:
    result = anilist_mod.OPS.remove(_cfg(), [_episode(AOT, 1, 1)], feature="history")

    assert result["count"] == 0
    assert result["unresolved"][0]["reason"] == history.REMOVE_UNSUPPORTED
    assert anilist.saves == []
