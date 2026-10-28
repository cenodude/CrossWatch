# tests/test_pair_provider_down.py
# CrossWatch - Pairs with an unreachable provider count as errors
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from cw_platform.orchestrator import Orchestrator
from test_interactive_sync import item, setup_ops
from test_orchestrator_dry_run_no_side_effects import _cfg


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("status,errors", [("ok", 0), ("down", 1), ("auth_failed", 1)])
def test_unreachable_provider_counts_as_error(config_base, monkeypatch, mode, status, errors):
    src, dst = setup_ops(config_base, monkeypatch, [item(1)])
    dst.health = lambda *_a, **_kw: {"ok": status == "ok", "status": status, "features": {"watchlist": status == "ok"}}
    cfg = _cfg(False)
    cfg["pairs"][0]["mode"] = mode

    result = Orchestrator(cfg).run(pair_scope_ids=["p1"])

    assert result["errors"] == errors
    assert len(dst.add_calls) == 1 - errors
