# tests/test_nuvio_watchlist_removal.py
# CrossWatch Nuvio watchlist removal orchestration regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import pytest

from cw_platform.orchestrator import Orchestrator
from providers.metadata import _meta_TMDB
from providers.sync import _mod_NUVIO
from providers.sync.nuvio import _watchlist
from providers.tests.test_nuvio_watchlist import FakeAdapter
from test_orchestrator_oneway_watchlist import FakeOps


@pytest.mark.parametrize("metadata", [False, True])
def test_one_way_removal_uses_nuvio_stored_identity(config_base: Any, monkeypatch: Any, metadata: bool) -> None:
    class NuvioAdapter(FakeAdapter):
        def health(self) -> dict[str, Any]:
            return {"ok": True, "status": "ok", "features": {"watchlist": True}}

        def build_index(self, feature: str) -> dict[str, Any]:
            assert feature == "watchlist"
            return _watchlist.build_index(self)

        def remove(self, feature: str, items: Any, *, dry_run: bool = False) -> dict[str, Any]:
            assert feature == "watchlist"
            return _watchlist.remove(self, items, dry_run=dry_run)

    class FakeTmdb:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def fetch(self, **_: Any) -> dict[str, Any]:
            return {"ids": {"tmdb": "6435", "imdb": "tt0120791"}}

    adapter = NuvioAdapter([{"content_id": "tt0120791", "content_type": "movie", "name": "Practical Magic"}])
    if metadata:
        adapter.config["tmdb"] = {"api_key": "tmdb-key"}
    source = FakeOps("MDBLIST", {
        "tmdb:6435": {"type": "movie", "title": "Practical Magic", "ids": {"tmdb": "6435", "imdb": "tt0120791"}},
    })
    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", FakeTmdb)
    monkeypatch.setattr(_mod_NUVIO.OPS, "_adapter", lambda _cfg: adapter)
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"MDBLIST": source, "NUVIO": _mod_NUVIO.OPS})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *_: True)
    cfg = {
        "runtime": {"snapshot_ttl_sec": 0, "apply_chunk_pause_ms": 0},
        "sync": {"enable_add": True, "enable_remove": True, "include_observed_deletes": True, "allow_mass_delete": True},
        "pairs": [{
            "id": "nuvio-removal", "enabled": True, "source": "MDBLIST", "target": "NUVIO", "mode": "one-way",
            "features": {"watchlist": {"enable": True, "add": True, "remove": True}},
        }],
    }

    assert not Orchestrator(cfg).run()["errors"]
    assert adapter.client.rows
    assert not any(name == "sync_push_library" for name, _ in adapter.client.calls)
    source.index.clear()

    assert not Orchestrator(cfg).run()["errors"]

    assert adapter.client.rows == []
    assert sum(name == "sync_push_library" for name, _ in adapter.client.calls) == 1
    assert not Orchestrator(cfg).run()["errors"]
    assert sum(name == "sync_push_library" for name, _ in adapter.client.calls) == 1
