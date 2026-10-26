# /tests/test_plex_discover_review.py
# Plex Discover review regression coverage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace as NS

import pytest

from cw_platform.orchestrator import _blackbox
from cw_platform.orchestrator._pairs_oneway import compute_effective_add, select_baseline_keys
from providers.sync.plex import _common as common, _discover_history as cloud, _history as history
from tests.test_plex_discover_history import MOVIE, NOW, WATCHED, remote
from tests.test_plex_history_recovery_database import library, movie, scan


def _stub_history(monkeypatch, klass=history.CLASS_NOT_IN_PLEX_CATALOG, rk=None):
    catalog = NS(resolve=lambda *a, **kw: (rk, klass), by_rk={})
    monkeypatch.setattr(history, "home_scope_enter", lambda a: (False, False, None, None))
    monkeypatch.setattr(history, "home_scope_exit", lambda *a: None)
    monkeypatch.setattr(history, "_get_history_catalog", lambda *a, **kw: catalog)
    monkeypatch.setattr(history, "resolve_path", lambda *a: (None, None))
    monkeypatch.setattr(history, "_resolve_rating_key", lambda *a, **kw: None)
    monkeypatch.setattr(history, "_maybe_trace", lambda *a, **kw: None)
    monkeypatch.setattr(history, "_shadow_add_batch", lambda *a: None)
    monkeypatch.setattr(history, "plex_worker_count", lambda *a: 1)
    monkeypatch.setattr(history, "_ensure_session_pool", lambda *a: None)
    monkeypatch.setattr(history, "_scrobble_with_date", lambda *a: True)


def _fake_writer(monkeypatch, outcome):
    monkeypatch.setattr(cloud, "Writer", lambda a: NS(add=lambda *args: outcome, close=lambda: None,
                                                    repair_pending=lambda: {}))


def test_cloud_present_is_not_committed_as_server_baseline(monkeypatch):
    _stub_history(monkeypatch)
    _fake_writer(monkeypatch, "present")
    adapter = NS(config={"plex": {"history": {"discover_fallback": True}}}, client=NS(server=NS()))
    count, _ = history.add(adapter, [MOVIE])
    assert count == 1
    meta = adapter._plex_history_write_meta
    key = history.canonical_key(MOVIE)
    assert select_baseline_keys([key], meta) == []


@pytest.mark.parametrize("outcome", ["plex_discover_rate_limited", "plex_discover_date_pending",
                                     "plex_discover_manual_review", "plex_discover_account_token_required",
                                     "plex_discover_endpoint_unavailable", "plex_discover_history_order_unavailable"])
def test_retryable_cloud_outcomes_are_not_promoted_to_blackbox(monkeypatch, tmp_path, outcome):
    monkeypatch.setattr(_blackbox, "STATE_DIR", tmp_path)
    _stub_history(monkeypatch)
    _fake_writer(monkeypatch, outcome)
    adapter = NS(config={"plex": {"history": {"discover_fallback": True}}}, client=NS(server=NS()))
    key = history.canonical_key(MOVIE)
    promoted = []
    for _ in range(3):
        _, unresolved = history.add(adapter, [MOVIE])
        meta = adapter._plex_history_write_meta
        decision = compute_effective_add(
            attempted_keys=[key], prov_confirmed=0, confirmed_keys=[],
            still_unresolved={u["key"] for u in unresolved}, skipped_keys=set(meta.get("skipped_keys") or []),
            have_exact_keys=True, verify_after_write=False, provider_skipped=False)
        deferred = compute_effective_add(
            attempted_keys=[key], prov_confirmed=0, confirmed_keys=[],
            still_unresolved={key}, skipped_keys=meta["skipped_keys"],
            deferred_keys=meta["deferred_keys"], have_exact_keys=True,
            verify_after_write=False, provider_skipped=False)
        assert deferred["success_keys"] == deferred["failed_keys"] == []
        if decision["failed_keys"]:
            result = _blackbox.record_attempts("PLEX", "history", decision["failed_keys"], pair="trakt-plex",
                                               cfg={"blackbox": {"enabled": True, "promote_after": 3}})
            promoted += result.get("promoted_keys") or []
    assert promoted == []


def test_pending_cloud_repair_is_reachable_after_item_joins_server_library(remote, monkeypatch):
    remote.fail_date = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_request_failed"
    assert json.loads(remote.path().read_text())["pending"]
    remote.fail_date = False
    _stub_history(monkeypatch, klass=history.CLASS_IN_CATALOG_UNWATCHED, rk="server-rk")
    history.add(remote.adapter, [MOVIE])
    assert json.loads(remote.path().read_text())["pending"] == {}


def test_cloud_episode_rejects_child_with_conflicting_episode_ids(remote, monkeypatch):
    def metadata(path, **kwargs):
        if path == "matches":
            return {"Metadata": [dict(type="show", ratingKey="show", Guid=[{"id": "tvdb://100"}])]}
        if path == "show/children":
            return {"Metadata": [dict(type="season", ratingKey="season", index=2)], "totalSize": 1}
        return {"Metadata": [dict(type="episode", ratingKey="other-episode", index=5, Guid=[{"id": "tvdb://999"}])],
                "totalSize": 1}
    monkeypatch.setattr(remote.writer, "_metadata", metadata)
    item = dict(type="episode", show_ids={"tvdb": "100"}, ids={"tvdb": "555"}, season=2, episode=5)
    assert remote.writer.add(item, WATCHED) not in {"dated", "present"}
    assert remote.marks == 0


def test_lagging_first_scrobble_is_not_repeated(remote, monkeypatch):
    remote.delay = True
    monkeypatch.setattr(remote.writer, "_watched", lambda key: False)
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    monkeypatch.setattr(cloud.time, "time", lambda: NOW + 61)
    remote.writer.add(MOVIE, WATCHED)
    assert remote.marks == 1


def test_database_title_match_still_in_library_is_skipped(tmp_path):
    path = library(tmp_path / "lib.db", views=[movie("plex://movie/gone", "Gone")],
                   items=[(1, "plex://movie/new", 1, None, None)], tags=[(1, "tmdb://123")])
    result, _ = scan(path, search=lambda item: {"tmdb": "123"})
    assert result == []


@pytest.mark.parametrize("presence,expected", [("absent", []), ("present", [MOVIE]), ("unknown", [MOVIE])])
def test_completed_cloud_items_filtered_only_while_absent(remote, presence, expected):
    from providers.sync._mod_PLEX import _PlexOPS
    from cw_platform.orchestrator._pairs_oneway import filter_provider_adds

    assert remote.writer.add(MOVIE, WATCHED) == "dated"
    remote.adapter.library_presence = lambda *args: [{"status": presence}]
    ops = _PlexOPS()
    ops._adapter = lambda cfg: remote.adapter
    before = len(remote.calls)
    assert filter_provider_adds(ops, {}, "history", [MOVIE]) == expected
    assert len(remote.calls) == before


def test_completed_cache_is_account_scoped_expires_and_excludes_pending(remote, monkeypatch):
    assert remote.writer.add(MOVIE, WATCHED) == "dated"
    assert cloud.completed_items(remote.adapter, [MOVIE])
    remote.adapter.client.selected_username = "another-user"
    assert not cloud.completed_items(remote.adapter, [MOVIE])
    remote.adapter.client.selected_username = "owner"
    remote.adapter.config["plex"]["account_token"] = "other-token"
    assert not cloud.completed_items(remote.adapter, [MOVIE])
    remote.adapter.config["plex"]["account_token"] = "account-secret"
    state = json.loads(remote.path().read_text())
    state["pending"]["catalog"] = {"epoch": WATCHED}
    remote.path().write_text(json.dumps(state))
    assert not cloud.completed_items(remote.adapter, [MOVIE])
    state["pending"] = {}
    remote.path().write_text(json.dumps(state))
    monkeypatch.setattr(cloud.time, "time", lambda: NOW + 86401)
    assert not cloud.completed_items(remote.adapter, [MOVIE])


def test_history_paging_rejects_cross_page_date_reversal(remote, monkeypatch):
    calls = []
    def gql(query, variables):
        calls.append(variables["after"])
        if variables["after"] is None:
            return {"user": {"watchHistory": {"nodes": [{"id": "old", "date": common.iso_from_epoch(NOW - 5)}],
                            "pageInfo": {"hasNextPage": True, "endCursor": "page2"}}}}
        return {"user": {"watchHistory": {"nodes": [{"id": "new", "date": common.iso_from_epoch(NOW),
                         "metadataItem": {"id": "catalog"}}], "pageInfo": {"hasNextPage": False}}}}
    monkeypatch.setattr(remote.writer, "_gql", gql)
    with pytest.raises(cloud.CloudError, match="history_order_unavailable"):
        remote.writer._entries("catalog", NOW - 120)
    assert calls == [None, "page2"]


def test_server_clock_used_to_identify_new_play(remote, monkeypatch):
    from email.utils import formatdate

    request = remote.writer.session.request
    def dated_response(*args, **kwargs):
        response = request(*args, **kwargs)
        response.headers["Date"] = formatdate(NOW, usegmt=True)
        return response
    monkeypatch.setattr(remote.writer.session, "request", dated_response)
    monkeypatch.setattr(cloud.time, "time", lambda: NOW + 120)
    assert remote.writer.add(MOVIE, WATCHED) == "dated"
    assert remote.marks == remote.edits == 1


@pytest.mark.parametrize("outcome", ["dated", "present", "plex_discover_date_pending", "cached"])
def test_cloud_metadata_survives_adapter_and_chunked_applier(monkeypatch, outcome):
    from providers.sync._mod_PLEX import PLEXModule
    from cw_platform.orchestrator import _applier

    _stub_history(monkeypatch)
    _fake_writer(monkeypatch, outcome)
    adapter = NS(config={"plex": {"history": {"discover_fallback": True}}}, client=NS(server=NS()))
    monkeypatch.setattr(_applier, "record_unresolved", lambda *a, **kw: None)
    adapter._is_enabled = lambda feature: True
    rows = [MOVIE, dict(MOVIE, ids={"tmdb": "456"})]
    def add(cfg, items, *, feature, dry_run):
        return PLEXModule.add(adapter, feature, items, dry_run=dry_run)
    events = []
    result = _applier.apply_add(
        dst_ops=NS(add=add), cfg={}, dst_name="PLEX", feature="history", items=rows,
        dry_run=False, emit=lambda name, **kw: events.append(dict(event=name, **kw)), dbg=lambda *a, **kw: None,
        chunk_size=1, chunk_pause_ms=0)
    keys = [history.canonical_key(item) for item in rows]
    assert result["cloud_outcomes"] == {key: outcome for key in keys}
    receipt = next(row for row in events if row["event"] == "apply:add:done")
    assert {row["key"]: row["outcome"] for row in receipt["cloud_results"]} == result["cloud_outcomes"]
    assert receipt["accepted_unverified"] == 0
    assert result["presence_confirmed_keys"] == result["live_confirmed_keys"] == []
    if outcome == "plex_discover_date_pending":
        assert result["deferred_keys"] == result["skipped_keys"] == keys
    else:
        assert result["cloud_confirmed_keys"] == keys
    if outcome == "cached":
        assert result["confirmed"] == 0
