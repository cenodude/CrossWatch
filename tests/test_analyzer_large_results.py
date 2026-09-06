# tests/test_analyzer_large_results.py
# CrossWatch - Analyzer Large Result Regression Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time

import pytest

import services.analyzer as A


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_five_thousand_missing_items_and_retries(mode, tmp_path, monkeypatch):
    features = A._ANALYZER_FEATURES
    providers = {provider: {feature: {"baseline": {"items": {}}} for feature in features} for provider in ("TRAKT", "SIMKL")}
    unresolved = []
    for n in range(5000):
        feature = features[n % len(features)]
        key = f"tmdb:{100000 + n}"
        item = {"type": "movie", "title": f"Movie {n:05d}", "ids": {"tmdb": str(100000 + n)},
                "watched_at": "2026-01-01T00:00:00Z", "rating": 7, "progress": 50}
        providers["TRAKT"][feature]["baseline"]["items"][key] = item
        unresolved.append({"provider": "SIMKL", "feature": feature, "key": key, "alias_keys": [key],
                           "item": item, "ids": item["ids"], "reason": "not_found"})
    state = {"providers": providers}
    cfg = {"pairs": [{"id": "large", "source": "TRAKT", "target": "SIMKL", "enabled": True,
                      "mode": mode, "features": {feature: True for feature in features}}]}
    monkeypatch.setattr(A, "CWS_DIR", tmp_path)
    monkeypatch.setattr(A, "_load_manual_state", lambda: {})
    monkeypatch.setattr(A, "_unresolved_records", lambda scopes: unresolved)
    monkeypatch.setattr(A, "_ANALYSIS_CACHE", {})
    monkeypatch.setattr(A, "_analysis_signature", lambda *args: ("large-results", mode))
    context = A._analysis_context(state, cfg)
    monkeypatch.setattr(A, "_load_analysis_state", lambda pairs: (state, context, {"large"}, cfg, {}))

    started = time.perf_counter()
    result = A._cached_analysis("large")
    elapsed = time.perf_counter() - started
    missing = [p for p in result["problems"] if p["type"] == "missing_peer"]
    assert len(missing) == 5000
    assert len({(p["provider"], p["feature"], p["key"]) for p in missing}) == 5000
    assert all(sum(p["feature"] == feature for p in missing) == 1000 for feature in features)
    assert result["attention"]["counts"] == {"current_mismatch": 5000, "pending_retry": 5000, "blocked": 0, "total": 5000}
    assert len(result["attention"]["rows"]) == 5000
    assert all(row["current_mismatch"] and row["unresolved"] for row in result["attention"]["rows"])
    assert A._cached_analysis("large")["timings_ms"]["cache_hit"] is True
    print(f"\n{mode}: 5,000 missing items + retries in {elapsed:.3f}s; response {len(json.dumps(result).encode()) / 1024 / 1024:.2f} MiB")
