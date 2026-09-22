# /tests/test_applier_provider_isolation.py
# CrossWatch - Provider isolation and shared apply accounting regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cw_platform.history_events import history_sync_key, minimal_history_item
from cw_platform.id_map import canonical_key
from cw_platform.orchestrator import _applier
from cw_platform.orchestrator._providers import load_sync_providers
from cw_platform.orchestrator._scope import scope_safe


def test_only_simkl_history_opts_into_deferred_verification(config_base):
    providers = load_sync_providers()
    assert {"SIMKL", "TRAKT", "PLEX", "EMBY", "JELLYFIN", "CROSSWATCH"} <= providers.keys()
    cfg = {"_cw_provider_instance": "other", "_cw_pair_scope": "pair-1"}
    for name, ops in providers.items():
        for feature in ("history", "watchlist", "ratings", "progress", "collection", "playlists"):
            for dry_run in (False, True):
                result_cfg, finalize = _applier._add_batch_finalizer(ops, cfg, feature=feature, dry_run=dry_run)
                if name == "SIMKL" and feature == "history" and not dry_run:
                    assert callable(finalize)
                    assert result_cfg == {**cfg, "_cw_defer_add_verification": True}
                else:
                    assert finalize is None, (name, feature, dry_run)
                    assert result_cfg is cfg
    assert "_cw_defer_add_verification" not in cfg


@pytest.mark.parametrize("operation", ["add", "update", "remove"])
@pytest.mark.parametrize("feature", ["history", "watchlist", "ratings", "progress", "collection"])
def test_providers_without_hook_keep_streaming_results_and_metadata(monkeypatch, operation, feature):
    items = [{"type": "movie", "ids": {"tmdb": str(n)}} for n in range(6)]
    cfg = {"_cw_provider_instance": "second-account"}
    trace = []
    persisted = []
    monkeypatch.setattr(_applier, "record_unresolved", lambda *a, **kw: persisted.append((a, kw)))

    def write(received_cfg, chunk, *, feature, dry_run):
        assert received_cfg is cfg
        assert not dry_run
        assert len(persisted) == len(trace)
        trace.append([canonical_key(item) for item in chunk])
        key = canonical_key(chunk[0])
        return {"ok": True, "confirmed_keys": [key], "unresolved": [{"item": chunk[1], "hint": "missing"}],
                "presence_confirmed_keys": [key], "confirmed_destinations": {key: {"status": "added"}},
                "reason_counts": {"missing": 1}}

    result = getattr(_applier, f"apply_{operation}")(
        dst_ops=SimpleNamespace(add=write, remove=write), cfg=cfg, dst_name="TRAKT", feature=feature,
        items=items, dry_run=False, emit=lambda *a, **kw: None, dbg=lambda *a, **kw: None,
        chunk_size=2, chunk_pause_ms=0,
    )
    keys = [canonical_key(item) for item in items[::2]]
    assert len(trace) == len(persisted) == 3
    assert (result["attempted"], result["confirmed"], result["unresolved"], result["skipped"], result["errors"]) == (6, 3, 3, 0, 0)
    assert result["confirmed_keys"] == result["presence_confirmed_keys"] == keys
    assert result["confirmed_destinations"] == {key: {"status": "added"} for key in keys}
    assert result["reason_counts"] == {"missing": 3}
    assert all(kwargs["instance"] == "second-account" for _, kwargs in persisted)


def test_finalizers_preserve_pair_and_account_scope_without_leaking_config(monkeypatch):
    monkeypatch.setenv("CW_PAIR_SCOPE", "outer-pair")
    observed = []

    def hook(cfg, results, *, feature):
        observed.append((cfg["_cw_provider_instance"], os.environ["CW_PAIR_SCOPE"], results, feature))

    ops = SimpleNamespace(finalize_add=hook)
    first = {"_cw_provider_instance": "first", "_cw_pair_scope": "pair-first"}
    second = {"_cw_provider_instance": "second", "_cw_pair_scope": "pair-second"}
    _, finalize_first = _applier._add_batch_finalizer(ops, first, feature="history", dry_run=False)
    _, finalize_second = _applier._add_batch_finalizer(ops, second, feature="history", dry_run=False)
    assert finalize_first is not None and finalize_second is not None
    finalize_second([{"count": 2}])
    finalize_first([{"count": 1}])
    assert observed == [
        ("second", scope_safe("second", pair="pair-second"), [{"count": 2}], "history"),
        ("first", scope_safe("first", pair="pair-first"), [{"count": 1}], "history"),
    ]
    assert os.environ["CW_PAIR_SCOPE"] == "outer-pair"
    assert "_cw_defer_add_verification" not in first
    assert "_cw_defer_add_verification" not in second


@pytest.mark.parametrize("rewatches", [False, True])
def test_history_failures_keep_event_or_media_identity_as_requested(monkeypatch, rewatches):
    monkeypatch.setattr(_applier, "record_unresolved", Mock())
    items = [minimal_history_item({"type": "movie", "ids": {"tmdb": "100"},
                                   "watched_at": f"2026-09-{day:02d}T12:00:00Z"}, event_mode=rewatches)
             for day in (1, 5)]
    rows = [{"item": item, "hint": "failed"} for item in items]
    result = _applier._normalize({"ok": True, "unresolved": rows + rows}, items, "apply:add",
                                dst="TRAKT", feature="history", emit=lambda *a, **kw: None)
    expected = {history_sync_key(item, event_mode=rewatches) for item in items}
    assert set(result["unresolved_keys"]) == expected
    assert result["unresolved"] == len(expected)
    assert result["confirmed"] == 0
    if rewatches:
        assert result["skipped"] == 0
