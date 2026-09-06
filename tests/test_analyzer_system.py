# tests/test_analyzer_system.py
# CrossWatch - Analyzer System Health Regression Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.analyzer as A


def test_system_diagnostics_work_without_pair_snapshots(monkeypatch):
    monkeypatch.setattr(A, "request_user", lambda request: {"is_admin": True})
    def no_state(*args, **kwargs):
        raise AssertionError("System diagnostics must not depend on pair state")
    monkeypatch.setattr(A, "_load_analysis_state", no_state)
    monkeypatch.setattr(A, "_SYSTEM_CACHE", {})
    monkeypatch.setattr(A, "_state_signature", lambda pairs: ("system-test",))
    problem = {"severity": "error", "type": "provider_import_failed", "message": "Provider failed to load."}
    monkeypatch.setattr(A, "_system_diagnostics", lambda: [problem])
    assert A.api_system()["problems"] == [problem]
    assert A.api_system()["available"] is True


def test_refresh_rechecks_diagnostics_with_unchanged_snapshots(monkeypatch):
    monkeypatch.setattr(A, "_SYSTEM_CACHE", {})
    monkeypatch.setattr(A, "_state_signature", lambda pairs: ("refresh-test",))
    calls = []
    def diagnostics():
        calls.append(True)
        return [{"severity": "error", "type": "provider_import_failed"}] if len(calls) == 1 else []
    monkeypatch.setattr(A, "_system_diagnostics", diagnostics)
    assert A._cached_system()["problems"]
    assert A._cached_system()["problems"]
    assert len(calls) == 1
    assert A._cached_system(refresh=True)["problems"] == []
    assert len(calls) == 2
    assert A._cached_system()["problems"] == []


def test_diagnostic_exception_is_reported_as_failure(monkeypatch):
    monkeypatch.setattr(A, "_SYSTEM_CACHE", {})
    monkeypatch.setattr(A, "_state_signature", lambda pairs: ("failure-test",))
    def failed():
        raise RuntimeError("Cannot inspect state")
    monkeypatch.setattr(A, "_system_diagnostics", failed)
    result = A._cached_system(refresh=True)
    assert result["problems"][0]["severity"] == "error"
    assert "Cannot inspect state" in result["problems"][0]["error"]


def test_system_access_is_explicit_and_does_not_leak_admin_diagnostics(monkeypatch):
    monkeypatch.setattr(A, "request_user", lambda request: {"is_admin": False})
    def forbidden(*args, **kwargs):
        raise AssertionError("Must not read global diagnostics for managed users")
    monkeypatch.setattr(A, "_cached_system", forbidden)
    assert A.api_system(refresh=True)["available"] is False
    assert A.api_system()["problems"] == []


def test_manual_id_patch_endpoint_is_removed():
    app = FastAPI()
    app.include_router(A.router)
    with TestClient(app) as client:
        response = client.post("/api/analyzer/patch", json={"provider": "PLEX", "feature": "history", "key": "tmdb:1", "ids": {"tmdb": "2"}})
    assert response.status_code == 404
    assert not hasattr(A, "api_patch")
