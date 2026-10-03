# tests/test_pair_state_reset.py
# CrossWatch - Per-pair sync state reset tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy

import pytest

from api import maintenanceAPI, syncAPI
from cw_platform.id_map import canonical_key
from cw_platform.local_db.state import pair_state_counts
from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope
from services.pair_state import clear_pair_state, pair_scopes
from test_interactive_sync_features import feature_item, feature_setup


def block(number, feature="watchlist"):
    return {"baseline": {"items": {f"imdb:tt{number:07}": feature_item(feature, number)}}}


def config():
    pair = {"id": "p1", "source": "PLEX", "target": "SIMKL", "mode": "two-way",
            "features": {"watchlist": {"enable": True}, "history": {"enable": True}}}
    return {"pairs": [pair, {**deepcopy(pair), "id": "p2"}]}


def seed(config_base, cfg):
    state_dir = config_base / ".cw_state"
    state_dir.mkdir(exist_ok=True)
    store = StateStore(config_base)
    scopes = {}
    tomb = {}
    for pair in cfg["pairs"]:
        for feature in ("watchlist", "history"):
            scope = pair_feature_scope(cfg, pair, feature)
            scopes[(pair["id"], feature)] = scope
            store.for_pair(scope).save_feature_blocks({("PLEX", "default", feature): block(1, feature)})
            tomb[f"{feature}:{scope.upper()}|imdb:tt0000009"] = 1
            (state_dir / f"simkl.watermarks.{scope}.json").write_text("{}", encoding="utf-8")
            (state_dir / f"simkl_{feature}.shadow.{scope}.json").write_text("{}", encoding="utf-8")
            (state_dir / f"plex_fallback_memo.{scope}.json").write_text("{}", encoding="utf-8")
    store.save_tomb({"keys": tomb})
    (state_dir / "anime_mapping_overrides.json").write_text("{}", encoding="utf-8")
    return state_dir, scopes


def names(state_dir, scope):
    return {p.name.replace(f".{scope}", "") for p in state_dir.iterdir() if scope in p.name}


def test_pair_scopes_match_orchestrator_scopes():
    cfg = config()
    assert pair_scopes(cfg, "p2", ["history", "bogus"]) == {"history": pair_feature_scope(cfg, cfg["pairs"][1], "history", 2)}
    assert set(pair_scopes(cfg, "p1")) == {"watchlist", "history", "ratings", "progress", "collection", "playlists"}
    assert pair_scopes(cfg, "missing") is None


def test_clear_pair_state_removes_only_selected_scope(config_base):
    cfg = config()
    state_dir, scopes = seed(config_base, cfg)
    target = scopes[("p1", "history")]

    result = clear_pair_state(config_base, state_dir, [target])

    assert not result["errors"]
    assert (result["baselines"], result["items"], result["tombstones"]) == (1, 1, 1)
    assert len(result["files"]) == 3
    assert pair_state_counts(config_base, target) == (0, 0)
    assert not names(state_dir, target)
    for key, scope in scopes.items():
        if key == ("p1", "history"):
            continue
        assert pair_state_counts(config_base, scope) == (1, 1)
        assert len(names(state_dir, scope)) == 3
    keys = StateStore(config_base).load_tomb()["keys"]
    assert len(keys) == 3 and not any(target.upper() in key for key in keys)
    assert (state_dir / "anime_mapping_overrides.json").exists()


@pytest.mark.parametrize("clear_retry", [True, False])
def test_maintenance_route_resets_selected_features(config_base, monkeypatch, clear_retry):
    cfg = config()
    state_dir, scopes = seed(config_base, cfg)
    monkeypatch.setattr(maintenanceAPI, "_cw", lambda: (config_base / "cache", config_base, state_dir, None, None, None))
    monkeypatch.setattr(maintenanceAPI, "_load_config_for_state_prune", lambda _dir: cfg)

    result = maintenanceAPI.clear_pair_state_route(pair_id="p1", features=["history"], clear_retry=clear_retry)

    target = scopes[("p1", "history")]
    assert result["ok"] and result["features"] == ["history"]
    assert result["summary"]["removed_items"] == 2
    kept = {"plex_fallback_memo.json"} if clear_retry else {"plex_fallback_memo.json", "simkl_history.shadow.json"}
    assert names(state_dir, target) == kept
    assert pair_state_counts(config_base, target) == (0, 0)
    assert pair_state_counts(config_base, scopes[("p1", "watchlist")]) == (1, 1)
    assert pair_state_counts(config_base, scopes[("p2", "history")]) == (1, 1)
    assert maintenanceAPI.clear_pair_state_route(pair_id="nope", features=None, clear_retry=True)["error"] == "pair_not_found"
    assert maintenanceAPI.clear_pair_state_route(pair_id="p1", features=["bogus"], clear_retry=True)["error"] == "no_features_selected"


def test_deleting_pair_purges_its_state(config_base, monkeypatch):
    cfg = config()
    state_dir, scopes = seed(config_base, cfg)
    saved = []
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: cfg, saved.append))
    monkeypatch.setattr(syncAPI, "_cw_state_dir", lambda: state_dir)
    monkeypatch.setattr(syncAPI, "_managed_pair_blocked", lambda *_a, **_k: False)

    result = syncAPI.api_pairs_delete("p1", True, None)

    assert result["ok"] and result["deleted"] == 1 and result["state_errors"] == 0
    assert [pair["id"] for pair in saved[-1]["pairs"]] == ["p2"]
    for feature in ("watchlist", "history"):
        assert pair_state_counts(config_base, scopes[("p1", feature)]) == (0, 0)
        assert not names(state_dir, scopes[("p1", feature)])
        assert pair_state_counts(config_base, scopes[("p2", feature)]) == (1, 1)
        assert len(names(state_dir, scopes[("p2", feature)])) == 3
    keys = StateStore(config_base).load_tomb()["keys"]
    assert len(keys) == 2 and all(scopes[("p2", key.split(":", 1)[0])].upper() in key for key in keys)


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_reset_pair_does_not_propagate_old_deletes(config_base, monkeypatch, mode):
    one, two = feature_item("watchlist", 1), feature_item("watchlist", 2)
    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [one, two], [one, two], mode)
    cfg["sync"]["allow_mass_delete"] = True
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id": "p2"})
    removed = []
    dst.remove = lambda _cfg, items, *, feature, dry_run=False: removed.extend(canonical_key(x) for x in items) or {"ok": True, "count": 0}
    for pid in ("p1", "p2"):
        assert not Orchestrator(cfg).run(pair_scope_ids=[pid])["errors"]
    other = pair_feature_scope(cfg, cfg["pairs"][1], "watchlist", 2)
    before = pair_state_counts(config_base, other)

    scopes = pair_scopes(cfg, "p1", ["watchlist"])
    assert not clear_pair_state(config_base, config_base / ".cw_state", scopes.values())["errors"]
    src.index.pop(canonical_key(two))
    assert not Orchestrator(cfg).run(pair_scope_ids=["p1"])["errors"]

    assert not removed
    assert pair_state_counts(config_base, scopes["watchlist"])[0] == 2
    assert pair_state_counts(config_base, other) == before
