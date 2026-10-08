# tests/test_interactive_sync_export.py
# CrossWatch - Interactive Sync Review Export Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import csv
import io
import json
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest

from services.interactive_sync_export import build_export, capture_export, export_chunks
from test_interactive_sync import api_client, item


def download(client, session, **params):
    return client.get(f"/api/interactive-sync/{session.id}/export",
                      params=dict(revision=session.revision, selection_version=session.selection_version, **params))


def test_export_all_rows_and_current_selection(api_client):
    client, session, api = api_client
    session.plan.filter("history", "DST", "second", "add", [item(i) for i in range(300)], source="SRC")
    session.store.finish()
    session.store.select(False, feature="history", q="Movie 29")
    response = download(client, session, format="json")
    assert response.status_code == 200
    data = response.json()
    assert data["exported_rows"] == len(data["rows"]) == 301
    assert len({row["id"] for row in data["rows"]}) == 301
    assert sum(not row["selected"] for row in data["rows"]) == 11
    assert data["revision"] == session.revision
    assert data["selection_version"] == session.selection_version
    assert data["planned_at"] == session.planned_at
    assert response.headers["cache-control"] == "no-store"
    assert "attachment;" in response.headers["content-disposition"]


def test_filtered_export_matches_review_including_hidden_results(api_client):
    client, session, api = api_client
    row = dict(next(iter(session.store.rows.values())))
    row.update(id="hidden", result="not_in_library", selectable=False)
    session.store.rows["hidden"] = row
    session.store.finish()
    assert download(client, session, format="json").json()["exported_rows"] == 2
    for filters in ({}, dict(result="not_in_library"), dict(feature="history"), dict(q="Movie 1")):
        expected = client.get(f"/api/interactive-sync/{session.id}/rows", params=dict(revision=1, **filters)).json()
        data = download(client, session, format="json", scope="filtered", **filters).json()
        assert data["rows"] == expected["items"]
        assert data["exported_rows"] == expected["total"]


def test_export_retains_playlist_payload_and_conflicts(api_client):
    client, session, api = api_client
    order = [str(i) for i in range(50000)]
    session.plan.filter("playlists", "DST", "default", "update", [dict(type="playlist", ids={"slug":"list"}, target_order=order)])
    session.plan.conflict("ratings", "movie:1", "SRC", "DST", item(1, rating=4), item(1, rating=8), "DST")
    session.store.finish()
    data = download(client, session, format="json").json()
    playlist = next(row for row in data["rows"] if row["feature"] == "playlists")
    conflict = next(row for row in data["rows"] if row["result"] == "conflict")
    assert playlist["item"]["target_order"] == order
    assert conflict["left"]["rating"] == 4 and conflict["right"]["rating"] == 8
    assert conflict["winner"] == "DST" and not conflict["selected"]


def test_csv_roundtrip_and_formula_protection(api_client):
    client, session, api = api_client
    row = dict(next(iter(session.store.rows.values())))
    row["item"]["title"] = '=SUM(1,2) "Café"\nNext line'
    row["reason"] = "Ordinary, quoted \"reason\"\nNext line"
    session.store.rows[row["id"]] = row
    response = download(client, session)
    assert response.content.startswith(b"\xef\xbb\xbf")
    rows = list(csv.DictReader(io.StringIO(response.content.decode("utf-8-sig"))))
    assert len(rows) == 1
    assert rows[0]["title"] == "'" + row["item"]["title"]
    assert rows[0]["reason"] == row["reason"]
    assert json.loads(rows[0]["provider_ids"]) == row["item"]["ids"]
    assert rows[0]["source"] == "SRC" and rows[0]["destination"] == "DST"
    assert rows[0]["selected"] == "true"


def test_download_is_a_snapshot_and_closes_output(api_client):
    client, session, api = api_client
    snapshot = capture_export(session)
    try:
        session.store.select(False)
        session.pair["source"] = "CHANGED"
        session.plan.notices.append(dict(reason="Changed after snapshot"))
        session.close()
        output = build_export(snapshot, format="json")
    finally:
        snapshot.store.close()
    data = json.loads(b"".join(export_chunks(output)))
    assert data["rows"][0]["selected"] is True
    assert data["counts"]["selected"] == 1
    assert "source" not in data["pair"]
    assert data["notices"] == []
    assert output.closed
    assert not snapshot.store.path.exists()


@pytest.mark.parametrize("format", ["csv", "json"])
def test_review_actions_continue_during_export(api_client, monkeypatch, format):
    from services import interactive_sync_export as exporter

    client, session, api = api_client
    started, resume = threading.Event(), threading.Event()
    snapshots = []

    def slow_export(snapshot, **kwargs):
        snapshots.append(snapshot)
        started.set()
        assert resume.wait(10)
        return build_export(snapshot, **kwargs)

    monkeypatch.setattr(exporter, "build_export", slow_export)
    with ThreadPoolExecutor(max_workers=2) as pool:
        export = pool.submit(download, client, session, format=format)
        try:
            assert started.wait(5)
            selection = pool.submit(client.post, f"/api/interactive-sync/{session.id}/selection",
                                    json=dict(revision=1, selection_version=0, selected=False))
            assert selection.result(timeout=5).status_code == 200
            assert session.store.counts["selected"] == 0
            close = pool.submit(client.delete, f"/api/interactive-sync/{session.id}")
            assert close.result(timeout=5).status_code == 200
        finally:
            resume.set()
        response = export.result(timeout=5)
    assert response.status_code == 200
    if format == "json":
        data = response.json()
        assert data["selection_version"] == 0
        assert data["counts"]["selected"] == 1
        assert data["rows"][0]["selected"] is True
    else:
        rows = list(csv.DictReader(io.StringIO(response.content.decode("utf-8-sig"))))
        assert rows[0]["selected"] == "true"
    assert not snapshots[0].store.path.exists()


def test_failed_export_cleans_snapshot(api_client, monkeypatch):
    from services import interactive_sync_export as exporter

    client, session, api = api_client
    snapshots = []

    def fail(snapshot, **kwargs):
        snapshots.append(snapshot)
        raise RuntimeError("Export failed")

    monkeypatch.setattr(exporter, "build_export", fail)
    with pytest.raises(RuntimeError, match="Export failed"):
        download(client, session)
    assert not snapshots[0].store.path.exists()
    assert download(client, session, format="xml").status_code == 422
    assert session.store.page()["total"] == 1


@pytest.mark.parametrize("field", ["revision", "selection_version"])
def test_export_rejects_stale_version(api_client, field):
    client, session, api = api_client
    params = dict(revision=session.revision, selection_version=session.selection_version)
    params[field] += 1
    assert client.get(f"/api/interactive-sync/{session.id}/export", params=params).status_code == 409


@pytest.mark.parametrize("status", ["reading", "applying", "complete", "error"])
def test_export_requires_ready_review(api_client, status):
    client, session, api = api_client
    session.status = status
    assert download(client, session).status_code == 409


def test_export_access_and_parameter_validation(api_client):
    client, session, api = api_client
    url = f"/api/interactive-sync/{session.id}/export"
    params = dict(revision=1, selection_version=0)
    assert client.get(url, params=params, headers={"x-test-user":"bob"}).status_code == 404
    for invalid in (dict(format="xml"), dict(scope="page"), dict(q="x" * 257)):
        assert client.get(url, params={**params, **invalid}).status_code == 422
    session.touched = -100000
    assert client.get(url, params=params).status_code == 404
