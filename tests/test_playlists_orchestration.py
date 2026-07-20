from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from cw_platform import playlists_runner
from cw_platform.orchestrator import _pairs as pairs
from cw_platform.orchestrator import _pairs_playlists as glue


class FakeOps:
    def __init__(self, name: str):
        self._name = name

    def name(self) -> str:
        return self._name

    def label(self) -> str:
        return self._name.title()

    def features(self) -> dict[str, bool]:
        return {"playlists": True}

    def capabilities(self) -> dict[str, Any]:
        return {}

    def health(self, cfg, **kw) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"playlists": True}, "api": {}}

    def build_index(self, cfg, *, feature):
        return {}

    def add(self, cfg, items, *, feature, dry_run=False):
        return {"ok": True, "count": 0}

    def remove(self, cfg, items, *, feature, dry_run=False):
        return {"ok": True, "count": 0}

    def list_playlist_resources(self, cfg, *, instance=None):
        return []

    def get_playlist_snapshot(self, cfg, playlist_id, *, instance=None):
        return None

    def create_playlist(self, *a, **k):
        return None

    def add_playlist_items(self, *a, **k):
        return {"ok": True}

    def remove_playlist_items(self, *a, **k):
        return {"ok": True}

    def reorder_playlist_items(self, *a, **k):
        return {"ok": True}


class FakeStats:
    def record_summary(self, **kw):
        pass

    def http_overview(self, *a, **k):
        return {}

    def overview(self, *a, **k):
        return {}


class FakeStateStore:
    def save_last(self, d):
        pass

    def load_state(self):
        return {}

    def save_state(self, d):
        pass


def _ctx(cfg):
    return SimpleNamespace(
        config=cfg,
        providers={"FAKESRC": FakeOps("FAKESRC"), "FAKEDST": FakeOps("FAKEDST")},
        emit=lambda *a, **k: None,
        emit_info=lambda *a, **k: None,
        dbg=lambda *a, **k: None,
        dry_run=False,
        tomb_prune=lambda *a, **k: None,
        state_store=FakeStateStore(),
        stats=FakeStats(),
        emit_rate_warnings=lambda: None,
    )


def _pair_cfg(mode="one-way"):
    return {
        "trakt": {},
        "plex": {},
        "runtime": {},
        "sync": {},
        "playlists": {
            "endpoints": [
                {"id": "EP-01", "name": "s", "provider": "FAKESRC", "instance": "default", "playlist_id": "L1"},
                {"id": "EP-02", "name": "t", "provider": "FAKEDST", "instance": "default", "playlist_id": "T1"},
            ],
            "mappings": [
                {"id": "MAP-01", "source_endpoint": "EP-01", "target_endpoints": ["EP-02"],
                 "membership": "managed_only", "order": "ignore", "enabled": True},
            ],
        },
        "pairs": [
            {
                "id": "p1",
                "source": "FAKESRC",
                "target": "FAKEDST",
                "source_instance": "default",
                "target_instance": "default",
                "enabled": True,
                "mode": mode,
                "features": {"playlists": {"enable": True, "mappings": ["MAP-01"]}},
            }
        ],
    }


def test_pairs_routes_playlists_to_dedicated_runner(config_base, monkeypatch):
    calls = {"playlist": 0, "oneway": 0, "twoway": 0}

    def fake_playlist(ctx, src, dst, *, fcfg, health_map, full_cfg, pair):
        calls["playlist"] += 1
        assert src == "FAKESRC" and dst == "FAKEDST"
        assert (fcfg.get("mappings") or []) == ["MAP-01"]
        return {"added": 3, "removed": 1, "updated": 2, "unresolved": 0, "skipped": 0, "errors": 0}

    def fail_oneway(*a, **k):
        calls["oneway"] += 1
        raise AssertionError("playlists must not use run_one_way_feature")

    def fail_twoway(*a, **k):
        calls["twoway"] += 1
        raise AssertionError("playlists must not use run_two_way_feature")

    monkeypatch.setattr(pairs, "run_playlist_mappings", fake_playlist)
    monkeypatch.setattr(pairs, "run_one_way_feature", fail_oneway)
    monkeypatch.setattr(pairs, "run_two_way_feature", fail_twoway)

    result = pairs.run_pairs(_ctx(_pair_cfg("one-way")))
    assert calls["playlist"] == 1
    assert calls["oneway"] == 0
    assert result["added"] == 3
    assert result["removed"] == 1
    assert result["updated"] == 2


def test_pairs_runs_playlists_directionally_on_two_way(config_base, monkeypatch):
    calls = {"playlist": 0}

    def fake_playlist(ctx, src, dst, *, fcfg, health_map, full_cfg, pair):
        calls["playlist"] += 1
        assert src == "FAKESRC" and dst == "FAKEDST"
        return {"added": 1, "removed": 0, "updated": 0, "unresolved": 0, "skipped": 0, "errors": 0}

    monkeypatch.setattr(pairs, "run_playlist_mappings", fake_playlist)
    monkeypatch.setattr(pairs, "run_one_way_feature", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no")))
    monkeypatch.setattr(pairs, "run_two_way_feature", lambda *a, **k: (_ for _ in ()).throw(AssertionError("playlists must not use run_two_way_feature")))

    result = pairs.run_pairs(_ctx(_pair_cfg("two-way")))
    assert calls["playlist"] == 1
    assert result["added"] == 1
    assert result["errors"] == 0


def test_playlist_pair_records_feature_totals(config_base, monkeypatch):
    calls: list[dict[str, Any]] = []

    class Stats:
        def record_feature_totals(self, feature, **kwargs):
            calls.append({"feature": feature, **kwargs})

    cfg = _pair_cfg("one-way")
    ctx = _ctx(cfg)
    ctx.stats = Stats()
    monkeypatch.setattr(glue, "resolve_pair_mappings", lambda _cfg, _pair: [{"id": "MAP-01"}])
    monkeypatch.setattr(glue, "run_mapping", lambda *a, **k: {"ok": True, "added": 3, "removed": 0, "reordered": 0, "unresolved_count": 0})

    result = glue.run_playlist_mappings(
        ctx,
        "FAKESRC",
        "FAKEDST",
        fcfg={"mappings": ["MAP-01"]},
        health_map={},
        full_cfg=cfg,
        pair=cfg["pairs"][0],
    )

    assert result["added"] == 3
    assert calls == [
        {
            "feature": "playlists",
            "added": 3,
            "removed": 0,
            "updated": 0,
            "src": "FAKEDST",
            "run_id": "FAKESRC->FAKEDST:playlists",
        }
    ]


def test_manual_and_scheduled_share_core_runner():
    from services import playlists as svc

    assert glue.run_mapping is playlists_runner.run_mapping
    assert svc.runner.run_mapping is playlists_runner.run_mapping
