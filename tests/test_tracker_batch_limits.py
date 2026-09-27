# /tests/test_tracker_batch_limits.py
# PublicMetaDB pagination and FlickList bulk sizing regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import importlib
import json
from types import SimpleNamespace

import pytest

from cw_platform.config_base import DEFAULT_CFG, apply_migration_overrides
from cw_platform.orchestrator._chunking import effective_chunk_size


def test_tracker_batch_defaults_migrate_without_changing_credentials():
    old = {
        "publicmetadb": {"api_key": "test-key", "watchlist_page_size": 100, "history_per_page": 100, "progress_per_page": 100},
        "flicklist": {"access_token": "test-token", "credential": "session", "write_batch_size": 100, "history_per_page": 100},
        "runtime": {"apply_chunk_size": 100, "apply_chunk_size_by_provider": {"PUBLICMETADB": 500}},
    }

    migrated, _ = apply_migration_overrides(old)

    for key in ("watchlist_page_size", "history_per_page", "progress_per_page"):
        assert migrated["publicmetadb"][key] == 500
    assert migrated["publicmetadb"]["api_key"] == "test-key"
    assert migrated["flicklist"]["access_token"] == "test-token"
    assert migrated["flicklist"]["credential"] == "session"
    assert migrated["flicklist"]["history_per_page"] == 100
    assert migrated["flicklist"]["write_batch_size"] == 1000
    assert effective_chunk_size(SimpleNamespace(**migrated["runtime"]), "FLICKLIST") == 1000
    assert old["flicklist"]["write_batch_size"] == 100


@pytest.mark.parametrize("feature", ["history", "progress", "watchlist", "playlists"])
@pytest.mark.parametrize("configured,expected", [(None, 500), (1000, 500), (50, 50)])
def test_publicmetadb_page_limits_and_complete_reads(feature, configured, expected):
    module = importlib.import_module(f"providers.sync.publicmetadb._{feature}")
    field = {"history": "history_per_page", "progress": "progress_per_page"}.get(feature, "watchlist_page_size")
    cfg = SimpleNamespace(**({field: configured} if configured is not None else {}))
    calls = []
    rows = [
        {"id": str(i), "tmdb_id": i, "media_type": "movie", "title": f"Movie {i}",
         "watched_at": "2026-01-01T20:00:00Z", "position_ms": 1000, "runtime_ms": 10000}
        for i in range(1, expected + 2)
    ]

    def get_json(path, *, params):
        calls.append(params)
        return {"items": rows[:expected] if params["page"] == 1 else rows[expected:], "totalPages": 2}

    adapter = SimpleNamespace(cfg=cfg, client=SimpleNamespace(get_json=get_json))
    if feature == "playlists":
        items, ids = module._fetch_items(adapter, "test-list")
    elif feature == "watchlist":
        items, ids = module._fetch_all_items(adapter, "test-list")
    else:
        items, ids = module._fetch_all_items(adapter)

    assert len(items) == expected + 1
    assert len(ids) == expected + 1
    assert calls == [{"page": 1, "perPage": expected}, {"page": 2, "perPage": expected}]


def test_publicmetadb_config_and_normalization_use_500():
    from cw_platform.config_base import _normalize_publicmetadb
    from providers.sync._mod_PUBLICMETADB import PUBLICMETADBConfig, PUBLICMETADBModule

    cfg = {"publicmetadb": {"api_key": "test-key"}}
    module = PUBLICMETADBModule(cfg)
    normalized = copy.deepcopy(cfg)
    _normalize_publicmetadb(normalized)
    for field in ("watchlist_page_size", "history_per_page", "progress_per_page"):
        assert DEFAULT_CFG["publicmetadb"][field] == 500
        assert getattr(PUBLICMETADBConfig(api_key="test-key"), field) == 500
        assert getattr(module.cfg, field) == 500
        assert normalized["publicmetadb"][field] == 500


@pytest.mark.parametrize("feature", ["history", "watchlist", "ratings", "playlists"])
@pytest.mark.parametrize("operation", ["add", "remove"])
def test_flicklist_bulk_defaults_split_1001_items(monkeypatch, feature, operation):
    module = importlib.import_module(f"providers.sync.flicklist._{feature}")
    adapter = SimpleNamespace(config={"flicklist": copy.deepcopy(DEFAULT_CFG["flicklist"])})
    calls = []

    def request(adapter, method, url, **kwargs):
        payload = kwargs["json"]["items"]
        calls.append((method, len(payload)))
        body = {"added" if method == "POST" else "deleted": len(payload), "not_found": []}
        return SimpleNamespace(status_code=200, text=json.dumps(body), json=lambda: body)

    monkeypatch.setattr(module, "flicklist_request", request)
    items = [
        {"type": "movie", "ids": {"tmdb": str(i)}, "rating": 7.5, "watched_at": "2026-01-01T20:00:00Z"}
        for i in range(1, 1002)
    ]
    args = (adapter, "test-list", items) if feature == "playlists" else (adapter, items)
    result = getattr(module, operation)(*args)

    assert result["ok"] is True
    assert len(result["confirmed_keys"]) == 1001
    method = "POST" if operation == "add" else "DELETE"
    assert calls == [(method, 1000), (method, 1)]


def test_flicklist_session_history_reads_remain_100(monkeypatch):
    from providers.sync.flicklist import _history

    calls = []

    def request(adapter, method, url, **kwargs):
        if "params" in kwargs:
            calls.append(kwargs["params"])
        return SimpleNamespace(status_code=200, headers={}, text="[]", json=lambda: [])

    monkeypatch.setattr(_history, "flicklist_request", request)
    _history.build_index(SimpleNamespace(config={"flicklist": dict(DEFAULT_CFG["flicklist"], credential="session")}))

    assert calls == [{"page": 1, "limit": 100}]
