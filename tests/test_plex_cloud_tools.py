# tests/test_plex_cloud_tools.py
# CrossWatch - Plex cloud visibility and Editor opt-in tests.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/Cenodude/CrossWatch)
from copy import deepcopy
from hashlib import sha256
import json
import time

import pytest

from api import editorAPI as E
from services import analyzer as A
from providers.sync.plex import _discover_history as D
from cw_platform.event_archive.recorder import RunRecorder
from cw_platform.event_archive.db import connect


@pytest.mark.parametrize("option,expected", [(None, False), (False, False), ("true", False), (True, True)])
def test_editor_fallback_requires_explicit_boolean(monkeypatch, option, expected):
    cfg = {"plex": {"history": {"discover_fallback": True}}}
    before = deepcopy(cfg)
    target = {"provider": "PLEX", "instance": "default"}
    monkeypatch.setattr(E, "load_config", lambda: cfg)
    monkeypatch.setattr(E, "_editor_send_targets", lambda *_: [target])
    monkeypatch.setattr(E, "_filter_targets_for_request", lambda c, r, targets: targets)
    monkeypatch.setattr(E, "load_sync_ops", lambda *_: object())
    seen = []
    def apply(**kwargs):
        seen.append(kwargs["cfg"]["plex"]["history"]["discover_fallback"])
        return {"ok": True, "confirmed": 0}
    monkeypatch.setattr(E, "apply_add", apply)
    E.api_editor_send({"kind": "history", "providers": [target], "plex_discover_fallback": option,
                       "items": [{"type": "movie", "title": "Example", "watched_at": "2025-01-01T00:00:00Z", "ids": {"tmdb": "1"}}]})
    assert seen == [expected]
    assert cfg == before


def test_editor_cloud_success_does_not_enter_server_snapshot():
    item = {"type": "movie", "ids": {"tmdb": "1"}}
    key = E._editor_item_key("history", "", item)
    assert E._items_confirmed_by_send("history", [item], {"confirmed": 1, "confirmed_keys": [key], "cloud_confirmed_keys": [key]}) == []
    assert E._items_confirmed_by_send("history", [item], {"confirmed": 1, "confirmed_keys": [key]}) == [item]


def test_analyzer_cloud_confirmation_is_account_scoped_and_expires(monkeypatch, tmp_path):
    monkeypatch.setattr(D.common, "STATE_DIR", tmp_path)
    item = {"type": "movie", "ids": {"tmdb": "1"}}
    state = {"account_fingerprint": sha256(b"test-token").hexdigest(), "username": "owner",
             "completed": {D._signature(item): {"at": time.time(), "key": "cloud:1"}}}
    path = tmp_path / "plex_discover_history.test.json"
    path.write_text(json.dumps(state))
    def context(token="test-token"):
        return A._AnalysisContext(state={}, cfg={"plex": {"account_token": token, "username": "owner"}},
                                  pairs={}, aliases={}, history_exact={}, pair_libs={}, pair_types={})
    assert context().cloud_confirmed("PLEX", item)
    assert not context("other-token").cloud_confirmed("PLEX", item)
    state["completed"][D._signature(item)]["at"] = time.time() - 90000
    path.write_text(json.dumps(state))
    assert not context().cloud_confirmed("PLEX", item)


def test_event_archive_preserves_cloud_receipts():
    conn = connect(":memory:")
    recorder = RunRecorder(lambda *a, **kw: None, run_id="cloud-test", conn=conn)
    rows = [{"key": "movie:1", "title": "Example", "outcome": "dated"}]
    recorder.emit("feature:start", src="TRAKT", dst="PLEX", feature="history")
    recorder.emit("apply:add:done", dst="PLEX", feature="history", added=1, cloud_results=rows)
    recorder._flush(force=True)
    row = conn.execute("SELECT detail FROM events WHERE event_type='write_attempted'").fetchone()
    assert json.loads(row[0])["cloud_results"] == rows


def test_large_cloud_event_preserves_counts_and_valid_json():
    conn = connect(":memory:")
    recorder = RunRecorder(lambda *a, **kw: None, run_id="large-cloud", conn=conn)
    rows = [{"key": f"movie:{n}", "title": "Long title " * 40, "outcome": "dated"} for n in range(200)]
    recorder.emit("feature:start", src="TRAKT", dst="PLEX", feature="history")
    recorder.emit("apply:add:done", dst="PLEX", feature="history", attempted=200, added=200, errors=0, cloud_results=rows)
    recorder._flush(force=True)
    raw = conn.execute("SELECT detail FROM events WHERE event_type='write_attempted'").fetchone()[0]
    detail = json.loads(raw)
    assert detail["attempted"] == detail["added"] == 200
    assert detail["errors"] == 0
    assert detail["cloud_outcome_counts"] == {"dated": 200}
    assert len(detail["cloud_results"]) + detail["cloud_results_omitted"] == 200
    assert len(raw) <= 4000


def test_make_event_never_slices_serialized_json():
    from cw_platform.event_archive.recorder import make_event

    detail = {"message": "x" * 6000}
    assert json.loads(make_event(detail=detail)["detail"]) == detail


def test_analyzer_account_id_only_home_user_excluded(monkeypatch, tmp_path):
    monkeypatch.setattr(D.common, "STATE_DIR", tmp_path)
    item = {"type": "movie", "ids": {"tmdb": "1"}}
    state = {"account_fingerprint": sha256(b"test-token").hexdigest(), "username": "owner",
             "completed": {D._signature(item): {"at": time.time(), "key": "cloud:1"}}}
    (tmp_path / "plex_discover_history.test.json").write_text(json.dumps(state))
    context = A._AnalysisContext(state={}, cfg={"plex": {"account_token": "test-token", "account_id": 42, "username": ""}},
                                pairs={}, aliases={}, history_exact={}, pair_libs={}, pair_types={})
    assert not context.cloud_confirmed("PLEX", item)


def test_applier_cloud_failure_has_readable_reason():
    from types import SimpleNamespace
    from cw_platform.orchestrator._applier import apply_add, _accepted_unverified

    events = []
    item = {"title": "Example", "type": "movie", "ids": {"tmdb": "1"}}
    result = apply_add(dst_ops=SimpleNamespace(add=lambda *a, **kw: {"count": 0, "cloud_outcomes": {"tmdb:1": "plex_discover_not_found"}}),
                       cfg={}, dst_name="PLEX", feature="history", items=[item], dry_run=False,
                       emit=lambda event, **data: events.append((event, data)), dbg=lambda *a, **kw: None, chunk_size=0, chunk_pause_ms=0)
    assert "could not match" in result["cloud_results"][0]["reason"]
    assert _accepted_unverified({"accepted_not_seen_live_keys": ["same", "same"]}, False) == 2
