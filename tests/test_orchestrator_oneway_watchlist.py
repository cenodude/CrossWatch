# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator.facade import Orchestrator


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
        return {"watchlist": True}

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "features": {"watchlist": True},
            "observed_deletes": True,
            "index_semantics": "present",
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:  # used by some registry paths
        return True

    def health(self, cfg: Mapping[str, Any], **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"watchlist": True}, "api": {}}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        assert feature == "watchlist"
        return dict(self.index)

    def add(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.add_calls.append(batch)
        if not dry_run:
            for it in batch:
                k = canonical_key(it)
                if k:
                    self.index[k] = dict(it)
        return {"ok": True, "added": len(batch), "count": len(batch)}

    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        batch = [dict(x) for x in items]
        self.remove_calls.append(batch)
        if not dry_run:
            for it in batch:
                k = canonical_key(it)
                if k:
                    self.index.pop(k, None)
        return {"ok": True, "removed": len(batch), "count": len(batch)}


def test_orchestrator_oneway_watchlist_add_then_observed_remove(
    config_base: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    src_items = {
        "imdb:tt01": {"type": "movie", "title": "A", "year": 2000, "ids": {"imdb": "tt01"}},
        "imdb:tt03": {"type": "movie", "title": "C", "year": 2002, "ids": {"imdb": "tt03"}},
    }
    dst_items = {
        "imdb:tt01": {"type": "movie", "title": "A", "year": 2000, "ids": {"imdb": "tt01"}},
        "imdb:tt02": {"type": "movie", "title": "B", "year": 2001, "ids": {"imdb": "tt02"}},
    }

    src = FakeOps("SRC", src_items)
    dst = FakeOps("DST", dst_items)

    # Orchestrator discovers providers via loader; patch it to our fakes.
    monkeypatch.setattr(
        "cw_platform.orchestrator.facade.load_sync_providers",
        lambda: {"SRC": src, "DST": dst},
    )

    # Snapshot builder normally checks module registry configuration.
    monkeypatch.setattr(
        "cw_platform.orchestrator._snapshots.provider_configured",
        lambda _cfg, _name: True,
    )

    cfg = {
        "runtime": {
            "debug": False,
            "snapshot_ttl_sec": 0,
            "apply_chunk_size": 0,
            "apply_chunk_pause_ms": 0,
        },
        "sync": {
            "dry_run": False,
            "enable_add": True,
            "enable_remove": True,
            "include_observed_deletes": True,
            "allow_mass_delete": True,
        },
        "pairs": [
            {
                "id": "p1",
                "enabled": True,
                "source": "SRC",
                "target": "DST",
                "mode": "one-way",
                "feature": "watchlist",
                "features": {"watchlist": {"enable": True, "add": True, "remove": True}},
            }
        ],
    }

    orch = Orchestrator(cfg)

    # Run 1: DST should receive missing C
    orch.run()
    assert len(dst.add_calls) == 1
    assert [it.get("ids", {}).get("imdb") for it in dst.add_calls[0]] == ["tt03"]
    assert dst.remove_calls == []

    # Run 2: now DST has a baseline that includes B,  removal is allowed.
    orch.run()
    assert len(dst.remove_calls) == 1
    assert [it.get("ids", {}).get("imdb") for it in dst.remove_calls[0]] == ["tt02"]
