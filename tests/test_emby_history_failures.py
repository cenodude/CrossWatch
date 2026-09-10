from types import SimpleNamespace

import pytest

from cw_platform.orchestrator import _snapshots
from providers.sync.emby import _history as history


@pytest.fixture
def history_adapter(monkeypatch):
    monkeypatch.setattr(history, "_emby_library_roots", lambda *a: {})
    monkeypatch.setattr(history, "_shadow_load", lambda: {})
    monkeypatch.setattr(history, "_bb_load", lambda: {})
    monkeypatch.setattr(history, "_history_page_size", lambda *a: 2)
    for name in ("_dbg", "_info", "_warn"):
        monkeypatch.setattr(history, name, lambda *a, **k: None)
    ad = SimpleNamespace(cfg=SimpleNamespace(user_id="user"), client=None)
    return "emby", history, ad


def response(status, body):
    return SimpleNamespace(status_code=status, json=lambda: body)


def watched_rows():
    return [{"Id": str(n), "Type": "Movie", "Name": f"Movie {n}", "ProviderIds": {"Tmdb": str(n)},
             "UserData": {"Played": True, "LastPlayedDate": "2026-09-01T12:00:00Z"}} for n in (1, 2)]


@pytest.mark.parametrize("status", [401, 500])
@pytest.mark.parametrize("fail_start", [0, 2])
def test_http_failure_cannot_publish_empty_or_partial_history(history_adapter, status, fail_start):
    _, history, ad = history_adapter
    calls = []

    def get(path, *, params):
        start = params.get("StartIndex", 0)
        calls.append(start)
        return response(status, {"error": "failed"}) if start == fail_start else response(200, {"Items": watched_rows()})

    ad.client = SimpleNamespace(get=get)
    with pytest.raises(RuntimeError, match=f"history_http_{status}"):
        history.build_index(ad)
    assert calls == ([0] if fail_start == 0 else [0, 2])


@pytest.mark.parametrize("body", [{}, {"Items": None}, {"Items": [None]}, {"Items": [{"Name": "Missing ID"}]}])
@pytest.mark.parametrize("fail_start", [0, 2])
def test_malformed_history_is_not_an_empty_library(history_adapter, body, fail_start):
    _, history, ad = history_adapter
    ad.client = SimpleNamespace(get=lambda path, *, params: response(
        200, body if params.get("StartIndex", 0) == fail_start else {"Items": watched_rows()}))
    with pytest.raises(ValueError, match="history_invalid"):
        history.build_index(ad)


@pytest.mark.parametrize("fail_start", [0, 2])
def test_invalid_json_cannot_publish_empty_or_partial_history(history_adapter, fail_start):
    _, history, ad = history_adapter

    def invalid_json():
        raise ValueError("Invalid JSON")

    def get(path, *, params):
        if params.get("StartIndex", 0) == fail_start:
            return SimpleNamespace(status_code=200, json=invalid_json)
        return response(200, {"Items": watched_rows()})

    ad.client = SimpleNamespace(get=get)
    with pytest.raises(ValueError, match="history_invalid_response"):
        history.build_index(ad)


def test_repeated_history_page_is_a_failure(history_adapter):
    _, history, ad = history_adapter
    ad.client = SimpleNamespace(get=lambda *a, **k: response(200, {"Items": watched_rows()}))
    with pytest.raises(RuntimeError, match="history_repeated_page"):
        history.build_index(ad)


def test_valid_empty_history_remains_supported(history_adapter):
    _, history, ad = history_adapter
    ad.client = SimpleNamespace(get=lambda *a, **k: response(200, {"Items": []}))
    assert history.build_index(ad) == {}


def test_failed_history_does_not_reach_snapshot_callback_or_cache(history_adapter, monkeypatch):
    provider, history, ad = history_adapter
    ad.client = SimpleNamespace(get=lambda *a, **k: response(500, {"error": "failed"}))
    ops = SimpleNamespace(features=lambda: {"history": True}, build_index=lambda *a, **k: history.build_index(ad))
    monkeypatch.setattr(_snapshots, "provider_configured", lambda *a: True)
    monkeypatch.setattr(_snapshots, "allowed_providers_for_feature", lambda *a: set())
    callbacks, cache = [], {}
    with pytest.raises(RuntimeError, match="history_http_500"):
        _snapshots.build_snapshots_for_feature(feature="history", config={}, providers={provider.upper(): ops},
            snap_cache=cache, snap_ttl_sec=300, dbg=lambda *a, **k: None, emit_info=lambda *a: None,
            on_snapshot=lambda *args: callbacks.append(args))
    assert callbacks == []
    assert cache == {}
