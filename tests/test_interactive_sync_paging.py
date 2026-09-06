# tests/test_interactive_sync_paging.py
# CrossWatch - Interactive Sync Paging and Large Review Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time

import pytest

from cw_platform.orchestrator._interactive import InteractivePlan
from services.interactive_sync_store import ReviewStore
from test_interactive_sync import api_client, item


def test_fifty_thousand_rows_have_bounded_responses_and_server_selection(api_client, monkeypatch):
    client, session, api = api_client
    session.close()
    session.store = ReviewStore()
    session.plan = InteractivePlan(rows=session.store.rows, conflicts=session.store.conflicts, copy_rows=False)
    started = time.perf_counter()
    session.plan.filter("history", "DST", "default", "add", (item(i, watched_at="2026-09-01T12:00:00Z") for i in range(50000)), source="SRC")
    session.store.finish()
    build_seconds = time.perf_counter() - started
    status = client.get(f"/api/interactive-sync/{session.id}")
    assert status.json()["counts"]["changes"] == 50000
    assert "rows" not in status.json() and "conflicts" not in status.json()
    assert len(status.content) < 2000
    started = time.perf_counter()
    first = client.get(f"/api/interactive-sync/{session.id}/rows", params=dict(revision=1)).json()
    last_response = client.get(f"/api/interactive-sync/{session.id}/rows", params=dict(revision=1, offset=49950))
    last = last_response.json()
    page_seconds = time.perf_counter() - started
    assert len(first["items"]) == 75 and len(last["items"]) == 50
    assert first["total"] == last["total"] == 50000
    assert not {row["id"] for row in first["items"]} & {row["id"] for row in last["items"]}
    assert len(last_response.content) < 100000
    filtered = client.get(f"/api/interactive-sync/{session.id}/rows", params=dict(revision=1, feature="history", result="add", q="Movie 4999")).json()
    assert filtered["total"] == 11
    response = client.post(f"/api/interactive-sync/{session.id}/selection", json=dict(revision=1, selection_version=0, selected=False, q="Movie 4999"))
    assert response.json()["counts"]["selected"] == 49989
    assert len(response.content) < 2000
    captured = []

    def launch(current, task, cfg, chosen, **kwargs):
        captured.append(chosen)

    monkeypatch.setattr(api, "launch", launch)
    applied = client.post(f"/api/interactive-sync/{session.id}/apply", json=dict(revision=1, selection_version=1))
    assert applied.status_code == 200
    assert len(captured[0]) == 49989
    assert not captured[0] & {row["id"] for row in filtered["items"]}
    print(json.dumps(dict(rows=50000, build_seconds=round(build_seconds, 3), two_pages_seconds=round(page_seconds, 3),
                         status_bytes=len(status.content), last_page_bytes=len(last_response.content), sqlite_bytes=session.store.path.stat().st_size)))


def test_conflicts_share_bounded_paging(api_client):
    client, session, api = api_client
    for i in range(1200):
        session.plan.conflict("ratings", str(i), "SRC", "DST", item(i, rating=4), item(i, rating=9), "SRC")
    session.store.finish()
    page = client.get(f"/api/interactive-sync/{session.id}/rows", params=dict(revision=1, result="conflict", offset=75)).json()
    assert page["total"] == 1200 and len(page["items"]) == 75
    assert all(row["result"] == "conflict" and not row["selectable"] for row in page["items"])
    assert len(client.get(f"/api/interactive-sync/{session.id}").content) < 2000


@pytest.mark.parametrize("params", [dict(limit=201), dict(limit=0), dict(offset=-1), dict(q="a" * 257)])
def test_page_limits_are_validated(api_client, params):
    client, session, api = api_client
    assert client.get(f"/api/interactive-sync/{session.id}/rows", params=dict(revision=1, **params)).status_code == 422


def test_stale_pages_and_selections_are_rejected(api_client):
    client, session, api = api_client
    assert client.get(f"/api/interactive-sync/{session.id}/rows", params=dict(revision=0)).status_code == 409
    payload = dict(revision=1, selection_version=0, selected=False)
    assert client.post(f"/api/interactive-sync/{session.id}/selection", json=payload).status_code == 200
    assert client.post(f"/api/interactive-sync/{session.id}/selection", json=payload).status_code == 409
    assert client.post(f"/api/interactive-sync/{session.id}/apply", json=dict(revision=1, selection_version=1)).status_code == 400


def test_invalid_selection_is_atomic_and_scoped(api_client):
    client, session, api = api_client
    payload = dict(revision=1, selection_version=0, selected=False, ids=[next(iter(session.plan.rows)), "forged"])
    assert client.post(f"/api/interactive-sync/{session.id}/selection", json=payload).status_code == 400
    assert session.store.counts["selected"] == 1
    assert client.post(f"/api/interactive-sync/{session.id}/selection", json=payload, headers={"x-test-user": "bob"}).status_code == 404
    assert client.get(f"/api/interactive-sync/{session.id}/rows", params=dict(revision=1), headers={"x-test-user": "bob"}).status_code == 404


def test_refreshed_store_preserves_only_unchanged_selections():
    old, new = ReviewStore(), ReviewStore()
    try:
        before = InteractivePlan(rows=old.rows, copy_rows=False)
        before.filter("history", "DST", "default", "add", [item(1), item(2)])
        old.finish()
        old.select(False, q="Movie 2")
        after = InteractivePlan(rows=new.rows, copy_rows=False)
        after.filter("history", "DST", "default", "add", [item(1), item(2), item(3)])
        new.finish(old)
        assert new.counts["selected"] == 1
        assert new.selected_ids() == old.selected_ids()
        assert len(new.rows) == 3
    finally:
        old.close()
        new.close()


def test_recheck_diagnostics_identify_changed_fields_without_item_payloads():
    old, new = ReviewStore(), ReviewStore()
    try:
        before = InteractivePlan(rows=old.rows, copy_rows=False)
        before.filter("history", "SIMKL", "SIMKL-P01", "add", [item(1, watched_at="2026-09-01T12:00:00Z", year=2026)])
        old.finish()
        after = InteractivePlan(rows=new.rows, copy_rows=False)
        after.filter("history", "SIMKL", "default", "add", [item(1)])
        after.filter("history", "SIMKL", "SIMKL-P01", "add", [item(1, watched_at="2026-09-01T12:00:00Z", year="2026")])
        details = new.recheck_details(old, old.selected_ids())
        assert len(details) == 1
        assert details[0]["reason"] == "changed"
        assert details[0]["fields"] == ["item.year"]
        assert details[0]["instance"] == "SIMKL-P01"
        assert "Movie 1" not in json.dumps(details)
        assert "watched_at" not in json.dumps(details)
        assert old.selected_ids()
    finally:
        old.close()
        new.close()


def test_recheck_diagnostics_bound_missing_and_ambiguous_results():
    old, new = ReviewStore(), ReviewStore()
    try:
        before = InteractivePlan(rows=old.rows, copy_rows=False)
        before.filter("history", "SIMKL", "default", "add", [item(i) for i in range(100)])
        old.finish()
        after = InteractivePlan(rows=new.rows, copy_rows=False)
        after.filter("history", "SIMKL", "default", "add", [item(i, watched_at=date) for i in range(100) for date in ("2026-09-01T12:00:00Z", "2026-09-02T12:00:00Z")])
        details = new.recheck_details(old, old.selected_ids())
        assert len(details) == 5
        assert all(row["reason"] == "ambiguous" and row["fields"] == [] for row in details)
        new.db.execute("DELETE FROM review")
        details = new.recheck_details(old, old.selected_ids())
        assert len(details) == 5
        assert all(row["reason"] == "unavailable" for row in details)
    finally:
        old.close()
        new.close()


def test_preflight_and_execution_do_not_accumulate_row_payloads():
    original = InteractivePlan()
    original.filter("history", "DST", "default", "add", [item(5)])
    selected = set(original.rows)
    for preview in (True, False):
        plan = InteractivePlan(preview=preview, selected=selected, record_rows=False)
        kept = plan.filter("history", "DST", "default", "add", (item(i) for i in range(5000)))
        assert not plan.rows and not plan.conflicts
        assert plan.seen == selected
        assert len(kept) == (0 if preview else 1)


@pytest.mark.parametrize("expire", [False, True])
def test_closed_and_expired_reviews_remove_sqlite_files(api_client, expire):
    client, session, api = api_client
    path = session.store.path
    assert path.exists()
    if expire:
        session.touched = -100000
        assert client.get(f"/api/interactive-sync/{session.id}").status_code == 404
    else:
        assert client.delete(f"/api/interactive-sync/{session.id}").status_code == 200
    assert not path.exists()


def test_playlist_order_arrays_are_not_sent_in_page_responses(api_client):
    client, session, api = api_client
    reorder = dict(type="playlist", ids={"slug": "list"}, target_order=[str(i) for i in range(50000)])
    session.plan.filter("playlists", "DST", "default", "update", [reorder])
    session.store.finish()
    response = client.get(f"/api/interactive-sync/{session.id}/rows", params=dict(revision=1, feature="playlists"))
    row = response.json()["items"][0]
    assert row["item"]["target_order_count"] == 50000
    assert "target_order" not in row["item"]
    assert len(response.content) < 2000
    assert len(session.plan.rows[row["id"]]["item"]["target_order"]) == 50000
