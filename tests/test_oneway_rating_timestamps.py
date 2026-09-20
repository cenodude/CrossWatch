# tests/test_oneway_rating_timestamps.py
# CrossWatch - One-way rating timestamp protection tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator import Orchestrator
from test_interactive_sync import item
from test_interactive_sync_features import feature_setup


@pytest.mark.parametrize("alias", [False, True])
@pytest.mark.parametrize("source_time,target_time,expected", [
    ("2026-09-01T12:00:00Z", "2026-09-02T12:00:00Z", 9),
    ("2026-09-02T12:00:00Z", "2026-09-01T12:00:00Z", 6),
    ("2026-09-01T12:00:00Z", "2026-09-01T12:00:00Z", 6),
    (None, "2026-09-02T12:00:00Z", 6),
    ("2026-09-01T12:00:00Z", None, 6),
    ("invalid", "2026-09-02T12:00:00Z", 6),
    ("2026-09-02T13:00:00+02:00", "2026-09-02T12:00:00Z", 9),
])
def test_oneway_preserves_only_demonstrably_newer_destination_ratings(
    config_base, monkeypatch, alias, source_time, target_time, expected,
):
    source = item(1, rating=6, rated_at=source_time)
    target = item(1, rating=9, rated_at=target_time)
    if alias:
        target["ids"]["tmdb"] = "123"
        assert canonical_key(source) != canonical_key(target)
    addition = item(2, rating=7, rated_at=source_time)
    cfg, src, dst = feature_setup(config_base, monkeypatch, "ratings", [source, addition], [target], "one-way")
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *args: None)

    result = Orchestrator(cfg).run()

    assert result["ok"] is True
    writes = [row for batch in dst.add_calls for row in batch]
    assert any(row["ids"].get("imdb") == addition["ids"]["imdb"] for row in writes)
    updates = [row for row in writes if row["ids"].get("imdb") == source["ids"]["imdb"]]
    if expected == 9:
        assert not updates
        assert dst.index[canonical_key(target)]["rating"] == 9
    else:
        assert len(updates) == 1
        assert updates[0]["rating"] == 6
    assert not src.add_calls
