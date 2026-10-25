# /tests/test_plex_discover_followup.py
# Plex Discover bounded reads and terminal failure regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

import requests
import pytest

from providers.sync.plex import _discover_history as cloud, _common as common
from tests.test_plex_discover_history import MOVIE, NOW, WATCHED, remote


def test_history_scan_cost_is_bounded_per_batch(remote, monkeypatch):
    calls = []
    def gql(query, variables):
        calls.append(variables.get("after"))
        page = int(variables.get("after") or 0)
        return {"user": {"watchHistory": {"nodes": [{"id": f"n{page}", "date": common.iso_from_epoch(NOW),
                                                      "metadataItem": {"id": "elsewhere"}}],
                                           "pageInfo": {"hasNextPage": True, "endCursor": str(page + 1)}}}}
    monkeypatch.setattr(remote.writer, "_gql", gql)
    items = [dict(MOVIE, ids={"tmdb": "123"}, title=f"Example {n}", year=2000 + n) for n in range(3)]
    for item in items:
        remote.writer.add(item, WATCHED)
    assert len(calls) <= 100
    assert remote.marks == 0
    assert json.loads(remote.path().read_text())["retry_at"] == NOW + 3600
    writer = cloud.Writer(remote.adapter)
    try:
        assert writer.add(MOVIE, WATCHED) == "plex_discover_rate_limited"
    finally:
        writer.close()
    assert len(calls) == 100


def test_large_history_writes_with_two_recent_page_reads(remote, monkeypatch):
    original = remote.writer._gql
    reads = []
    def gql(query, variables):
        if query.startswith("mutation"):
            return original(query, variables)
        reads.append(variables["after"])
        assert variables["after"] is None
        nodes = list(remote.nodes) + [{"id": "old", "date": common.iso_from_epoch(NOW - 3600),
                                      "metadataItem": {"id": "other"}}]
        return {"user": {"watchHistory": {"nodes": nodes, "pageInfo": {
            "hasNextPage": True, "endCursor": "79-more-pages"}}}}
    monkeypatch.setattr(remote.writer, "_gql", gql)
    assert remote.writer.add(MOVIE, WATCHED) == "dated"
    assert reads == [None, None]
    assert remote.marks == remote.edits == 1


@pytest.mark.parametrize("dates", [[NOW - 5, NOW], [None], ["invalid"]])
def test_invalid_recent_page_defers_without_scrobbling(remote, monkeypatch, dates):
    nodes = [{"id": str(i), "date": common.iso_from_epoch(d) if isinstance(d, int) else d}
             for i, d in enumerate(dates)]
    monkeypatch.setattr(remote.writer, "_gql", lambda *a: {"user": {"watchHistory": {
        "nodes": nodes, "pageInfo": {"hasNextPage": False}}}})
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_history_order_unavailable"
    assert remote.marks == 0


def test_recent_cache_does_not_hide_older_pending_play(remote, monkeypatch):
    calls = []
    def gql(query, variables):
        calls.append(variables["after"])
        page = variables["after"]
        date = NOW - 300 if page is None else NOW - 900
        return {"user": {"watchHistory": {"nodes": [{"id": str(date),
            "date": common.iso_from_epoch(date), "metadataItem": {"id": "catalog"}}],
            "pageInfo": {"hasNextPage": page is None, "endCursor": "older"}}}}
    monkeypatch.setattr(remote.writer, "_gql", gql)
    assert len(remote.writer._entries("catalog", NOW - 120)) == 1
    assert len(remote.writer._entries("catalog", NOW - 1000)) == 2
    assert calls == [None, None, "older"]


@pytest.mark.parametrize("pages,used", [(60, 0), (50, 0), (30, 45)])
def test_scrobble_requires_budget_for_another_complete_scan(remote, monkeypatch, pages, used):
    calls = []
    def gql(query, variables):
        page = int(variables.get("after") or 0)
        calls.append(page)
        return {"user": {"watchHistory": {"nodes": [], "pageInfo": {
            "hasNextPage": page + 1 < pages, "endCursor": str(page + 1)}}}}
    remote.writer.history_pages = used
    monkeypatch.setattr(remote.writer, "_gql", gql)
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_history_incomplete"
    assert len(calls) == pages
    assert remote.marks == 0
    assert json.loads(remote.path().read_text())["retry_at"] == NOW + 3600


def test_cached_scan_still_reserves_remaining_confirmation_budget(remote, monkeypatch):
    remote.writer.history_rows = []
    remote.writer.history_since = float("-inf")
    remote.writer.history_scan_pages = 30
    remote.writer.history_pages = 75
    monkeypatch.setattr(remote.writer, "_gql", lambda *args: pytest.fail("Cached scan should be reused"))
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_history_incomplete"
    assert remote.marks == 0


def test_account_limit_does_not_block_identifiable_pending_date_repairs(remote):
    remote.fail_date = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_request_failed"
    state = json.loads(remote.path().read_text())
    state["history_limit"] = {"page_budget": 100, "scan_pages": 60}
    remote.path().write_text(json.dumps(state))
    remote.fail_date = False
    writer = cloud.Writer(remote.adapter)
    try:
        assert writer.repair_pending() == {"catalog": "dated"}
        assert writer.add(MOVIE, WATCHED) == "cached"
    finally:
        writer.close()
    assert remote.marks == 1
    assert json.loads(remote.path().read_text())["pending"] == {}


def test_mark_that_never_landed_does_not_stay_pending_forever(remote, monkeypatch):
    remote.fail_mark = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_request_failed"
    remote.fail_mark = False
    monkeypatch.setattr(cloud.time, "time", lambda: NOW + 86400)
    writer = cloud.Writer(remote.adapter)
    try:
        outcome = writer.add(MOVIE, WATCHED)
    finally:
        writer.close()
    assert outcome == "plex_discover_date_pending"
    assert json.loads(remote.path().read_text())["pending"]["catalog"]["review_started_at"] == NOW + 86400
    monkeypatch.setattr(cloud.time, "time", lambda: NOW + 90001)
    writer = cloud.Writer(remote.adapter)
    try:
        outcome = writer.add(MOVIE, WATCHED)
    finally:
        writer.close()
    assert outcome == "plex_discover_manual_review"
    assert outcome not in cloud.DEFERRED_REASONS
    assert remote.marks == 1
    writer = cloud.Writer(remote.adapter)
    try:
        before = len(remote.calls)
        monkeypatch.setattr(writer, "_resolve", lambda *args: pytest.fail("Terminal entries must skip catalog lookup"))
        assert writer.add(MOVIE, WATCHED) == outcome
        assert not any(url == cloud.COMMUNITY for _, url, _ in remote.calls[before:])
    finally:
        writer.close()


@pytest.mark.parametrize("status", [401, 403])
def test_rejected_account_token_is_not_deferred_and_checked_once(remote, monkeypatch, status):
    original = requests.Session.request
    seen = []
    def request(self, method, url, **kw):
        if url.endswith("/api/v2/user"):
            seen.append(url)
            response = requests.Response()
            response.status_code = status
            response._content = b"{}"
            return response
        return original(self, method, url, **kw)
    monkeypatch.setattr(requests.Session, "request", request)
    outcomes = [remote.writer.add(dict(MOVIE, ids={"tmdb": str(n)}), WATCHED) for n in range(3)]
    assert not set(outcomes) & cloud.DEFERRED_REASONS
    assert len(seen) == 1


def test_complete_history_scan_reused_across_items(remote, monkeypatch):
    calls = []
    def gql(query, variables):
        calls.append(variables)
        return {"user": {"watchHistory": {"nodes": [
            {"id": "one", "date": common.iso_from_epoch(NOW), "metadataItem": {"id": "first"}},
            {"id": "two", "date": common.iso_from_epoch(NOW), "metadataItem": {"id": "second"}}],
            "pageInfo": {"hasNextPage": False}}}}
    monkeypatch.setattr(remote.writer, "_gql", gql)
    assert [row["id"] for row in remote.writer._entries("first", NOW)] == ["one"]
    assert [row["id"] for row in remote.writer._entries("second", NOW)] == ["two"]
    assert len(calls) == 1


def test_expired_completed_entries_pruned_without_dropping_pending(remote, tmp_path):
    path = tmp_path / "pruning.json"
    cloud._save(path, {"completed": {"expired": {"at": NOW - 86401}, "fresh": {"at": NOW}},
                       "pending": {"uncertain": {"started": NOW - 86401}}})
    state = json.loads(path.read_text())
    assert set(state["completed"]) == {"fresh"}
    assert "uncertain" in state["pending"]


def test_missing_endpoint_is_not_a_temporary_failure(remote, monkeypatch):
    def request(*args, **kwargs):
        response = requests.Response()
        response.status_code = 404
        return response
    monkeypatch.setattr(remote.writer.session, "request", request)
    outcome = remote.writer.add(MOVIE, WATCHED)
    assert outcome == "plex_discover_endpoint_unavailable"
    assert outcome not in cloud.DEFERRED_REASONS
