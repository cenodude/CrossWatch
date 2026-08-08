# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator.facade import Orchestrator


@dataclass
class FakeOps:
    provider: str
    index: dict[str, dict[str, Any]]
    add_calls: list[list[dict[str, Any]]] = field(default_factory=list)
    add_dry_run: list[bool] = field(default_factory=list)

    def name(self) -> str:
        return self.provider

    def label(self) -> str:
        return self.provider

    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True}

    def capabilities(self) -> Mapping[str, Any]:
        return {"features": {"watchlist": True}, "index_semantics": "present"}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return True

    def health(self, cfg: Mapping[str, Any], **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"watchlist": True}, "api": {}}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
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
        self.add_dry_run.append(bool(dry_run))
        if not dry_run:
            for it in batch:
                k = canonical_key(it)
                if k:
                    self.index[k] = dict(it)
        # Providers report every item as written on a dry run.
        return {"ok": True, "count": len(batch), "dry_run": True} if dry_run else {"ok": True, "count": len(batch)}

    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        return {"ok": True, "count": 0}


def _cfg(dry_run: bool) -> dict[str, Any]:
    return {
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "sync": {"dry_run": dry_run, "enable_add": True, "enable_remove": False},
        "pairs": [
            {
                "id": "p1",
                "enabled": True,
                "source": "SRC",
                "target": "DST",
                "mode": "one-way",
                "feature": "watchlist",
                "features": {"watchlist": {"enable": True, "add": True, "remove": False}},
            }
        ],
    }


def _install(monkeypatch: pytest.MonkeyPatch, src: FakeOps, dst: FakeOps, state_dir: Path) -> None:
    monkeypatch.setattr(
        "cw_platform.orchestrator.facade.load_sync_providers",
        lambda: {"SRC": src, "DST": dst},
    )
    monkeypatch.setattr(
        "cw_platform.orchestrator._snapshots.provider_configured",
        lambda _cfg, _name: True,
    )
    # STATE_DIR is a module-level absolute path, so redirect it at the modules that own it.
    state_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr("cw_platform.orchestrator._unresolved.STATE_DIR", state_dir)
    monkeypatch.setattr("cw_platform.orchestrator._blackbox.STATE_DIR", state_dir)


def _state_files(base: Path) -> list[str]:
    return sorted(p.name for p in base.rglob("*.json"))


def _fixture_ops() -> tuple[FakeOps, FakeOps]:
    src_items = {
        "imdb:tt01": {"type": "movie", "title": "A", "year": 2000, "ids": {"imdb": "tt01"}},
        "imdb:tt03": {"type": "movie", "title": "C", "year": 2002, "ids": {"imdb": "tt03"}},
    }
    dst_items = {"imdb:tt01": {"type": "movie", "title": "A", "year": 2000, "ids": {"imdb": "tt01"}}}
    return FakeOps("SRC", src_items), FakeOps("DST", dst_items)


def test_dry_run_does_not_write_success_or_unresolved_state(
    config_base: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_dir = config_base / ".cw_state"
    src, dst = _fixture_ops()
    _install(monkeypatch, src, dst, state_dir)

    Orchestrator(_cfg(dry_run=True)).run()

    # The provider was consulted, and told it was a dry run.
    assert dst.add_dry_run == [True]
    # Nothing was actually written, so the destination index is untouched.
    assert "imdb:tt03" not in dst.index
    # And no success / failure bookkeeping was persisted.
    assert _state_files(state_dir) == []


def test_dry_run_does_not_clear_existing_unresolved(
    config_base: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from cw_platform.orchestrator._unresolved import load_unresolved_keys, record_unresolved

    state_dir = config_base / ".cw_state"
    src, dst = _fixture_ops()
    _install(monkeypatch, src, dst, state_dir)

    item = {"type": "movie", "title": "C", "year": 2002, "ids": {"imdb": "tt03"}}
    record_unresolved("DST", "watchlist", [item], hint="test:seed")
    before = set(load_unresolved_keys("DST", "watchlist") or [])
    assert before

    Orchestrator(_cfg(dry_run=True)).run()

    assert set(load_unresolved_keys("DST", "watchlist") or []) == before


def test_real_run_still_records_state(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    src, dst = _fixture_ops()
    _install(monkeypatch, src, dst, config_base / ".cw_state")

    Orchestrator(_cfg(dry_run=False)).run()

    assert dst.add_dry_run == [False]
    assert "imdb:tt03" in dst.index
