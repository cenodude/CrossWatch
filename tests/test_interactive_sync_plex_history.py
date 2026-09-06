# tests/test_interactive_sync_plex_history.py
# CrossWatch - Interactive Sync Plex History Read Stability Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest

from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.id_map import minimal
from providers.sync.plex import _history as history


@pytest.mark.parametrize("kind", ["movie", "episode"])
@pytest.mark.parametrize("interactive", [False, True])
@pytest.mark.parametrize("include_marked", [False, True])
def test_history_selection_survives_full_to_incremental_read(config_base, monkeypatch, kind, interactive, include_marked):
    watched_at = 1787093918
    raw = SimpleNamespace(type=kind, ratingKey="42", title="History title", guid="tmdb://123",
                          viewedAt=watched_at, grandparentTitle="Example series", grandparentGuid="tvdb://99", parentIndex=1, index=2)
    server = SimpleNamespace(history=lambda **kwargs: [] if kwargs.get("mindate") else [raw])
    adapter = SimpleNamespace(client=SimpleNamespace(server=server), cfg=SimpleNamespace(), config={"_cw_interactive_planned_at": 1788650000} if interactive else {})
    catalog = history.HistoryCatalog()
    catalog.add(dict(rk="42", type=kind, title="Catalog title", year=2026, ids={"tmdb": "123", "plex": "42"},
                     series_title="Example series", season=1, episode=2, show_ids={"tvdb": "99"},
                     watched=True, view_count=1, last_viewed_at=watched_at))
    watermark = {}
    monkeypatch.setattr(history, "home_scope_enter", lambda _: (False, False, None, None))
    monkeypatch.setattr(history, "home_scope_exit", lambda *_: None)
    monkeypatch.setattr(history, "plex_cfg_get", lambda _, key, default=None: default)
    monkeypatch.setattr(history, "_history_cfg_get", lambda _, key, default=None: include_marked if key == "include_marked_watched" else default)
    monkeypatch.setattr(history, "plex_feature_library_ids", lambda *_: set())
    monkeypatch.setattr(history, "_history_force_full", lambda _: False)
    monkeypatch.setattr(history, "_build_history_catalog", lambda *a, **k: catalog)
    monkeypatch.setattr(history, "_store_history_catalog", lambda *_: None)
    monkeypatch.setattr(history, "_load_watermark", lambda key: watermark.get(key))
    monkeypatch.setattr(history, "_save_watermark", lambda key, value: watermark.update({key: value}))
    monkeypatch.setattr(history, "_keep_in_snapshot", lambda *_: True)
    monkeypatch.setattr(history, "_pms_fetch_metadata_row", lambda *_: dict(viewCount=1, lastViewedAt=watched_at))
    monkeypatch.setattr(history, "_load_marked_state", lambda: {})
    monkeypatch.setattr(history, "_iter_marked_watched_from_library", lambda *a, **k: [])
    monkeypatch.setattr(history, "_fb_cache_flush", lambda: None)
    monkeypatch.setattr(history, "plex_worker_count", lambda *a: 1)
    first = history.build_index(adapter)
    second = history.build_index(adapter)
    assert len(first) == len(second) == 1
    if not interactive:
        expected = history.minimal_from_history_row(raw, token=None, allow_discover=False)
        row = next(iter(first.values()))
        assert row["ids"] == expected["ids"]
        assert row["year"] == expected["year"]
        if kind == "movie":
            assert row["title"] == "History title"
        return
    initial = InteractivePlan()
    initial.filter("history", "SIMKL", "SIMKL-P01", "add", [minimal(row) for row in first.values()], source="PLEX")
    recheck = InteractivePlan(preview=False, selected=set(initial.rows))
    kept = recheck.filter("history", "SIMKL", "SIMKL-P01", "add", [minimal(row) for row in second.values()], source="PLEX")
    assert len(kept) == 1
    assert kept[0]["watched_at"] == history._iso(watched_at)
    third = history.build_index(adapter)
    assert len(recheck.filter("history", "SIMKL", "SIMKL-P01", "add", [minimal(row) for row in third.values()], source="PLEX")) == 1
    catalog.by_rk["42"]["last_viewed_at"] = watched_at + 3600
    changed = history.build_index(adapter)
    assert not recheck.filter("history", "SIMKL", "SIMKL-P01", "add", [minimal(row) for row in changed.values()], source="PLEX")

    from api import syncAPI
    from services import interactive_sync as svc
    from test_interactive_sync_features import feature_setup

    cfg, src, dst = feature_setup(config_base, monkeypatch, "history", [], [], "one-way")
    snapshots = iter((first, second, third))
    monkeypatch.setattr(src, "build_index", lambda *args, **kwargs: deepcopy(next(snapshots)))
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        assert session.store.counts["selected"] == 1
        svc.apply(session, cfg, session.store.selected_ids())
        assert session.status == "complete", session.message
        assert session.apply_review is None
        assert session.summary["not_applied"] == 0
        assert len(dst.add_calls) == 1
        assert dst.add_calls[0][0]["watched_at"] == history._iso(watched_at)
    finally:
        session.close()
