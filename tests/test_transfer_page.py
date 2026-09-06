# tests/test_transfer_page.py
# CrossWatch - Import and Export Paging and Selection Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.export as exporter
import services.importer as importer
from ui_frontend import get_index_html


@pytest.mark.parametrize("admin,write,visible", [(True, False, True), (False, True, True), (False, False, False)])
def test_transfer_route_access(admin, write, visible):
    html = get_index_html(include_admin=admin, user={"permissions": {"write": write, "dashboard": True}})
    assert ('id="page-import_export"' in html) is visible
    assert 'id="tab-import_export"' not in html
    if visible:
        assert 'import_export: "Import / Export"' in html


@pytest.fixture
def transfer_client(monkeypatch, tmp_path):
    items = {f"tmdb:{index}": {"type": "movie", "title": f"{'Focus' if index < 100 else 'Movie'} {index:05}", "year": 2025, "ids": {"tmdb": str(index), "imdb": f"tt{index:07}"}} for index in range(5000)}
    state = {"providers": {"PLEX": {"watchlist": {"baseline": {"items": items}}}}}
    monkeypatch.setattr(exporter, "_load_state", lambda _features=None: state)
    monkeypatch.setattr(exporter, "_load_config_safe", lambda: {})
    monkeypatch.setattr(exporter, "_provider_rewatch_read_supported", lambda _provider: False)
    monkeypatch.setattr(importer, "load_config", lambda: {"crosswatch": {"connected": True, "root_dir": str(tmp_path)}})
    calls = []

    def add(cfg, rows, *, feature):
        calls.append((feature, rows))
        return {"ok": True, "count": len(rows)}

    monkeypatch.setattr(importer, "load_sync_ops", lambda _name: SimpleNamespace(add=add))
    preview_rows = [{"id": key, "feature": "watchlist", "media_type": "movie", "title": item["title"], "key": key, "status": "ready", "item": item} for key, item in items.items()]
    monkeypatch.setattr(importer, "_PREVIEWS", {"preview5000": {"created_at": importer.time.time(), "target_instance": "default", "rows": preview_rows}})
    app = FastAPI()
    app.include_router(importer.router)
    app.include_router(exporter.router)
    return TestClient(app), calls


def test_export_paging_covers_large_library(transfer_client):
    client, _ = transfer_client
    first = client.get("/api/export/sample", params={"provider": "PLEX", "limit": 50}).json()
    last = client.get("/api/export/sample", params={"provider": "PLEX", "limit": 50, "offset": 4950}).json()
    assert first["total"] == last["total"] == 5000
    assert len(first["items"]) == len(last["items"]) == 50
    assert not {row["key"] for row in first["items"]} & {row["key"] for row in last["items"]}
    assert client.get("/api/export/sample", params={"provider": "PLEX", "offset": 5000}).json()["items"] == []


def test_export_selection_uses_body_and_never_turns_empty_into_all(transfer_client, monkeypatch):
    client, _ = transfer_client
    received = []

    def build(provider, feature, state, keys, instance, **kwargs):
        received.append(keys)
        return exporter.Response("test", media_type="text/csv")

    monkeypatch.setattr(exporter, "_build_letterboxd", build)
    for ids in [["tmdb:3", "tmdb:4999"], []]:
        response = client.post("/api/export/file", json={"provider": "PLEX", "mode": "selected", "row_ids": ids})
        assert response.status_code == 200
        assert received[-1] == ids
    response = client.post("/api/export/file", json={"provider": "PLEX", "q": "Focus", "excluded_row_ids": ["tmdb:3"]})
    assert response.status_code == 200
    assert len(received[-1]) == 99
    assert "tmdb:3" not in received[-1]


@pytest.mark.parametrize("format", ["letterboxd", "imdb", "justwatch", "yamtrack", "tmdb"])
def test_selected_download_reuses_existing_export_formats(transfer_client, format):
    client, _ = transfer_client
    params = {"provider": "PLEX", "format": format}
    original = client.get("/api/export/file", params={**params, "ids": "tmdb:1,tmdb:3"})
    selected = client.post("/api/export/file", json={**params, "mode": "selected", "row_ids": ["tmdb:1", "tmdb:3"]})
    assert original.status_code == selected.status_code == 200
    assert original.headers["content-type"] == selected.headers["content-type"]
    assert original.content == selected.content


def test_import_commit_matches_filtered_preview_and_exclusions(transfer_client):
    client, calls = transfer_client
    params = {"features": "watchlist", "media_types": "movie", "q": "Focus", "status": "ready", "limit": 50, "offset": 50}
    page = client.get("/api/import/preview/preview5000", params=params).json()
    assert page["filtered_total"] == 100
    assert len(page["rows"]) == 50
    response = client.post("/api/import/commit", json={"import_id": "preview5000", "features": ["watchlist"], "media_types": ["movie"], "q": "Focus", "status": "ready", "excluded_row_ids": ["tmdb:3", "tmdb:99"]})
    assert response.json()["applied"] == 98
    assert {row["ids"]["tmdb"] for _, rows in calls for row in rows} == {str(index) for index in range(100)} - {"3", "99"}


@pytest.mark.parametrize("feature", ["history", "watchlist", "ratings"])
def test_import_selection_respects_result_type_and_feature(transfer_client, feature):
    client, calls = transfer_client
    for index, row in enumerate(importer._PREVIEWS["preview5000"]["rows"][:4]):
        row.update(feature=feature, status=["ready", "exists", "duplicate", "invalid"][index])
    response = client.post("/api/import/commit", json={"import_id": "preview5000", "features": [feature], "mode": "selected", "row_ids": [f"tmdb:{index}" for index in range(4)], "include_existing": True, "status": "exists"})
    assert response.json()["applied"] == 1
    assert calls[0][0] == feature
    assert calls[0][1][0]["ids"]["tmdb"] == "1"


def test_transfer_endpoints_keep_instance_access_checks(transfer_client, monkeypatch):
    client, calls = transfer_client
    monkeypatch.setattr(importer, "user_can_access_instance", lambda *args: False)
    assert client.get("/api/import/preview/preview5000").status_code == 403
    assert client.post("/api/import/commit", json={"import_id": "preview5000"}).status_code == 403
    assert calls == []
    monkeypatch.setattr(exporter, "request_user", lambda *args: {"is_admin": False})
    monkeypatch.setattr(exporter, "managed_profile_instances", lambda *args: {"PLEX": ["default"]})
    monkeypatch.setattr(exporter, "user_can_access_instance", lambda *args: False)
    response = client.post("/api/export/file", json={"provider": "PLEX", "provider_instance": "other"})
    assert response.status_code == 403


def test_import_reports_provider_failure(transfer_client, monkeypatch):
    client, _ = transfer_client
    monkeypatch.setattr(importer, "load_sync_ops", lambda _name: SimpleNamespace(add=lambda *args, **kwargs: {"ok": False, "count": 0}))
    result = client.post("/api/import/commit", json={"import_id": "preview5000", "mode": "selected", "row_ids": ["tmdb:1"]}).json()
    assert result["ok"] is False
    assert result["applied"] == 0


def test_fifty_thousand_import_rows_are_paged_and_selection_is_not_page_limited(transfer_client):
    client, calls = transfer_client
    original = importer._PREVIEWS["preview5000"]["rows"]
    importer._PREVIEWS["preview5000"]["rows"] = [dict(original[index % 5000], id=f"row:{index}") for index in range(50_000)]
    page = client.get("/api/import/preview/preview5000", params={"offset": 49_950, "limit": 50}).json()
    assert page["filtered_total"] == 50_000
    assert len(page["rows"]) == 50
    response = client.post("/api/import/commit", json={"import_id": "preview5000", "excluded_row_ids": ["row:0", "row:49999"]})
    assert response.json()["applied"] == 49_998
    assert sum(len(rows) for _, rows in calls) == 49_998


def test_empty_import_feature_selection_writes_nothing(transfer_client):
    client, calls = transfer_client
    response = client.post("/api/import/commit", json={"import_id": "preview5000", "features": []})
    assert response.json()["applied"] == 0
    assert calls == []
