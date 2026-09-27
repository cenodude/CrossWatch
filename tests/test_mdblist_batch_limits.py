# tests/test_mdblist_batch_limits.py
# CrossWatch - MDBList read sizes and show batch limits
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

import pytest


WHEN = "2026-09-01T12:00:00Z"


def _response(payload):
    return SimpleNamespace(status_code=200, text=json.dumps(payload), json=lambda: payload)


def _adapter(**settings):
    return SimpleNamespace(
        config={"mdblist": {"api_key": "test-key", **settings}},
        cfg=SimpleNamespace(timeout=1, max_retries=0),
        client=SimpleNamespace(session=None),
    )


@pytest.mark.parametrize("feature", ["ratings", "history", "collection"])
@pytest.mark.parametrize("op", ["add", "remove"])
@pytest.mark.parametrize("kind", ["movie", "show", "season", "episode"])
def test_writes_split_show_payloads_without_reducing_movie_batches(monkeypatch, feature, op, kind):
    module = importlib.import_module(f"providers.sync.mdblist._{feature}")
    adapter = _adapter(**{
        f"{feature}_chunk_size": 500,
        "collection_batch_size": 500,
        f"{feature}_write_delay_ms": 0,
        "collection_verify_after_write": False,
    })
    for name in ("_load_cache", "_shadow_load"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, lambda: {})
    for name in ("_save_cache", "_shadow_bust", "update_watermark_if_new"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, lambda *a, **kw: None)
    if feature == "history":
        monkeypatch.setattr(module, "_rewatches_enabled", lambda _adapter: False)
    calls = []

    def request(_adapter, method, url, **kwargs):
        payload = kwargs["json"]
        calls.append(payload)
        assert len(payload.get("shows", [])) <= 200
        action = "removed" if op == "remove" else "added"
        return _response({action: {key: len(rows) for key, rows in payload.items()}})

    monkeypatch.setattr(module, "mdblist_request", request)
    items = []
    for number in range(1, 502):
        item = {"type": kind, "ids": {"tmdb": number}, "rating": 7.5,
                "watched_at": WHEN, "rated_at": WHEN, "collected_at": WHEN}
        if kind in {"season", "episode"}:
            item.update({"show_ids": {"tmdb": number}, "season": 1})
            if kind == "episode":
                item["episode"] = 1
                item["ids"] = {}
        items.append(item)

    result = getattr(module, op)(adapter, items)

    assert isinstance(result, tuple)
    assert result[1] == []
    bucket = "movies" if kind == "movie" else "shows"
    assert [len(payload[bucket]) for payload in calls] == ([500, 1] if kind == "movie" else [200, 200, 101])
    submitted = [row["ids"]["tmdb"] for payload in calls for row in payload[bucket]]
    assert [int(value) for value in submitted] == list(range(1, 502))


@pytest.mark.parametrize("feature", ["watchlist", "ratings"])
@pytest.mark.parametrize("configured,expected", [(None, 1000), (200, 200)])
def test_reads_keep_all_items_with_new_default_and_existing_override(monkeypatch, tmp_path, feature, configured, expected):
    module = importlib.import_module(f"providers.sync.mdblist._{feature}")
    key = "watchlist_page_size" if feature == "watchlist" else "ratings_per_page"
    adapter = _adapter(**({key: configured} if configured is not None else {}))
    path_name = "_shadow_path" if feature == "watchlist" else "_cache_path"
    monkeypatch.setattr(module, path_name, lambda: tmp_path / "cache.json")
    monkeypatch.setattr(module, "get_watermark", lambda *_a: None)
    monkeypatch.setattr(module, "save_watermark", lambda *a, **kw: None)
    monkeypatch.setattr(module, "_fetch_last_activities", lambda *a, **kw: {})
    if feature == "watchlist":
        monkeypatch.setattr(module, "_unfreeze_keys_if_present", lambda *_a: None)
    else:
        monkeypatch.setattr(module, "update_watermark_if_new", lambda *a, **kw: None)
    calls = []
    rows = [{"type": "movie", "movie": {"ids": {"tmdb": number}},
             "ids": {"tmdb": number}, "rating": 8, "rating_precise": 7.5, "rated_at": WHEN}
            for number in range(1, 1002)]

    def request(_adapter, method, url, **kwargs):
        params = kwargs["params"]
        calls.append(params)
        assert "offset" not in params
        start, limit = int(params.get("cursor", "0")), params["limit"]
        data = rows[start:start + limit]
        next_cursor = str(start + len(data)) if start + len(data) < len(rows) else None
        return _response({"movies": data, "pagination": {"next_cursor": next_cursor}})

    monkeypatch.setattr(module, "mdblist_request", request)

    result = module.build_index(adapter)

    assert len(result) == 1001
    assert [int(call.get("cursor", "0")) for call in calls] == list(range(0, 1001, expected))
    assert all(call["limit"] == expected for call in calls)
    if feature == "ratings":
        assert all(item["rating"] == 7.5 for item in result.values())
