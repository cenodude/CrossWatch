# CrossWatch test scripts
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.orchestrator.facade import Orchestrator

MAPPINGS: dict[str, Any] = {
    "tvdb_show:76666:s1": {"mal:223": {"1-13": "1-13"}},
    "tvdb_show:76666:s2": {"mal:223": {"1-15": "14-28"}},
    "tmdb_show:12609:s1": {"mal:223": {"1-153": "1-153"}},
}

SRC_SHOW_IDS = {"tmdb": "12609", "imdb": "tt0088509", "tvdb": "76666", "mal": "223", "anilist": "223"}
DST_SHOW_IDS = {"tmdb": "12609", "imdb": "tt0088509", "trakt": "12553"}


@dataclass
class FakeOps:
    provider: str
    index: dict[str, dict[str, Any]]
    add_calls: list[list[dict[str, Any]]] = field(default_factory=list)
    remove_calls: list[list[dict[str, Any]]] = field(default_factory=list)

    def name(self) -> str:
        return self.provider

    def label(self) -> str:
        return self.provider

    def features(self) -> Mapping[str, bool]:
        return {"history": True}

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "features": {"history": True},
            "observed_deletes": False,
            "index_semantics": "present",
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return True

    def health(self, cfg: Mapping[str, Any], **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"history": True}, "api": {}}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return dict(self.index)

    def add(self, cfg, items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.add_calls.append(batch)
        return {"ok": True, "added": len(batch), "count": len(batch)}

    def remove(self, cfg, items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.remove_calls.append(batch)
        return {"ok": True, "removed": len(batch), "count": len(batch)}


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
        "series_title": "Dragon Ball",
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


def _cfg(anime_enabled: bool) -> dict[str, Any]:
    return {
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "anime_mapping": {"enabled": anime_enabled, "release_tag": "v3", "use_for_pairs": ["anilist", "simkl"]},
        "sync": {
            "dry_run": False,
            "enable_add": True,
            "enable_remove": False,
            "include_observed_deletes": False,
            "allow_mass_delete": False,
        },
        "pairs": [
            {
                "id": "p1",
                "enabled": True,
                "source": "CROSSWATCH",
                "target": "MDBLIST",
                "mode": "one-way",
                "feature": "history",
                "features": {"history": {"enable": True, "add": True, "remove": False}},
            }
        ],
    }


def _run(monkeypatch: pytest.MonkeyPatch, anime_enabled: bool) -> FakeOps:
    src = FakeOps("CROSSWATCH", {"tmdb:12609#s02e01": _episode(SRC_SHOW_IDS, 2, 1, absolute=14)})
    dst = FakeOps("MDBLIST", {"tmdb:12609#s01e14": _episode(DST_SHOW_IDS, 1, 14)})
    monkeypatch.setattr(
        "cw_platform.orchestrator.facade.load_sync_providers",
        lambda: {"CROSSWATCH": src, "MDBLIST": dst},
    )
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda _cfg, _name: True)
    Orchestrator(_cfg(anime_enabled)).run()
    return dst


def test_orchestrator_plans_a_false_add_without_the_anime_toggle(
    anime_index: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dst = _run(monkeypatch, anime_enabled=False)

    planned = [it for batch in dst.add_calls for it in batch]
    assert len(planned) == 1
    assert (planned[0].get("season"), planned[0].get("episode")) == (2, 1)


def test_orchestrator_plans_no_add_with_the_anime_toggle_on(
    anime_index: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dst = _run(monkeypatch, anime_enabled=True)

    planned = [it for batch in dst.add_calls for it in batch]
    assert planned == []
