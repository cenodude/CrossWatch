# tests/test_event_archive_endpoints.py
# CrossWatch - Event destination instance regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from cw_platform.event_archive.db import connect
from cw_platform.event_archive.recorder import RunRecorder
from cw_platform.orchestrator import _applier, _blackbox, _unresolved
from cw_platform.orchestrator._pairs_blocklist import apply_blocklist
from cw_platform.orchestrator._pairs_oneway import _emit_item_failures, _emit_item_resolutions
from cw_platform.orchestrator._state_store import StateStore


@pytest.mark.parametrize("providers", [("PLEX", "PLEX"), ("PLEX", "EMBY"), ("PLEX", "WETRAKR")])
@pytest.mark.parametrize("mode,reverse", [("one-way", False), ("two-way", False), ("two-way", True)])
@pytest.mark.parametrize("operation", ["add", "update", "remove"])
@pytest.mark.parametrize("failed", [False, True])
def test_apply_events_follow_destination_instance(monkeypatch, tmp_path, providers, mode, reverse, operation, failed):
    monkeypatch.setenv("CW_PAIR_SRC_INSTANCE", "default")
    monkeypatch.setenv("CW_PAIR_DST_INSTANCE", "P01")
    monkeypatch.setenv("CW_PAIR_SCOPE", "test-events")
    monkeypatch.setattr(_unresolved, "STATE_DIR", tmp_path)
    conn = connect(":memory:")
    recorder = RunRecorder(lambda *a, **kw: None, run_id="test", conn=conn)
    a, b = providers
    if mode == "two-way":
        recorder.emit("two:start", a=a, b=b, feature="watchlist")
    else:
        recorder.emit("feature:start", src=a, dst=b, feature="watchlist")
    destination = a if reverse else b
    instance = "default" if reverse else "P01"
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}

    class Ops:
        def write(self, cfg, items, **kwargs):
            if failed:
                return {"ok": False, "errors": 1, "unresolved": [{"item": item, "reason": "not_found"}]}
            return {"ok": True, "count": len(items)}

        add = update = remove = write

    cfg = {"_cw_provider_instance": instance} if a == b else {}
    getattr(_applier, f"apply_{operation}")(
        dst_ops=Ops(), cfg=cfg, dst_name=destination, feature="watchlist", items=[item],
        dry_run=False, emit=recorder.emit, dbg=lambda *a, **kw: None, chunk_size=0, chunk_pause_ms=0,
    )
    recorder._flush(force=True)
    rows = conn.execute("SELECT event_type, source_provider, source_instance, destination_provider, destination_instance FROM events").fetchall()
    assert {row[0] for row in rows} == ({"write_attempted", "write_failed", "unresolved_recorded"} if failed else {"write_attempted"})
    expected = (b, "P01", a, "default") if reverse else (a, "default", b, "P01")
    assert {tuple(row)[1:] for row in rows} == {expected}
    stats = next(iter(recorder._pair_stats.values()))
    assert (stats["src_provider"], stats["src_instance"], stats["dst_provider"], stats["dst_instance"]) == (a, "default", b, "p01")
    conn.close()


def test_retry_and_blackbox_events_keep_both_directions(monkeypatch, config_base):
    monkeypatch.setenv("CW_PAIR_SRC_INSTANCE", "default")
    monkeypatch.setenv("CW_PAIR_DST_INSTANCE", "P01")
    monkeypatch.setenv("CW_PAIR_SCOPE", "test-events")
    store = StateStore(config_base)
    monkeypatch.setattr(_unresolved, "STATE_DIR", store.cw_state_dir)
    monkeypatch.setattr(_blackbox, "STATE_DIR", store.cw_state_dir)
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}
    conn = connect(":memory:")
    recorder = RunRecorder(lambda *a, **kw: None, run_id="test", conn=conn)
    recorder.emit("two:start", a="PLEX", b="PLEX", feature="watchlist")
    for instance in ("default", "P01"):
        _emit_item_failures(recorder.emit, "PLEX", "watchlist", "PLEX-PLEX", ["tmdb:1"], {"tmdb:1": item}, {"promoted_keys": ["tmdb:1"]}, instance=instance)
        _emit_item_resolutions(recorder.emit, "PLEX", "watchlist", "PLEX-PLEX", ["tmdb:1"], {"tmdb:1": item}, instance=instance)
        _blackbox._promote("PLEX", "watchlist", "tmdb:1", reason="flapper", ts=1, pair="test-events", instance=instance)
        for _ in range(2):
            assert not apply_blocklist(store, [item], dst="PLEX", feature="watchlist", pair_key="test-events", emit=recorder.emit, instance=instance)
            recorder._flush(force=True)
    for event_type in ("write_failed", "unresolved_recorded", "unresolved_cleared", "blackbox_promoted", "blackbox_blocked"):
        rows = conn.execute("SELECT source_instance, destination_instance FROM events WHERE event_type=? AND item_key='tmdb:1'", (event_type,)).fetchall()
        assert sorted(tuple(row) for row in rows) == [("P01", "default"), ("default", "P01")]
    conn.close()
