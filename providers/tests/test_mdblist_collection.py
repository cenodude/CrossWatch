from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from providers.sync.mdblist import _collection as collection


class Resp:
    def __init__(self, status: int, payload: Any) -> None:
        self.status_code = status
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self) -> Any:
        return self._payload


def _adapter() -> SimpleNamespace:
    return SimpleNamespace(
        cfg=SimpleNamespace(timeout=1.0, max_retries=0),
        config={"mdblist": {"auth_method": "api_key", "api_key": "k"}},
        client=SimpleNamespace(session=object()),
    )


def test_collection_index_keeps_flat_episode_bucket_scope(monkeypatch) -> None:
    payload = {
        "episodes": [
            {
                "ids": {"tmdb": 73679, "tvdb": 337501},
                "title": "Japanology Plus",
                "year": 2014,
                "season": 1,
                "number": 1,
                "collected_at": "2026-01-01T00:00:00Z",
            },
            {
                "ids": {"tmdb": 73679, "tvdb": 337501},
                "title": "Japanology Plus",
                "year": 2014,
                "season": 1,
                "number": 2,
                "collected_at": "2026-01-01T00:00:00Z",
            },
        ],
        "pagination": {"has_more": False},
    }

    def fake_request(_adapter, _method, url, **_kwargs):
        if "last_activities" in url:
            return Resp(200, {})
        return Resp(200, payload)

    monkeypatch.setattr(collection, "mdblist_request", fake_request)
    monkeypatch.setattr(collection, "has_auth", lambda _cfg: True)
    monkeypatch.setattr(collection, "_shadow_load", lambda: {"ts": 0, "items": {}})
    monkeypatch.setattr(collection, "_shadow_save", lambda _items: None)
    monkeypatch.setattr(collection, "save_watermark", lambda *_args, **_kwargs: None)

    idx = collection.build_index(_adapter(), per_page=100, max_pages=1)

    assert sorted(idx) == ["tmdb:73679#s01e01", "tmdb:73679#s01e02"]
    assert idx["tmdb:73679#s01e01"]["type"] == "episode"
    assert idx["tmdb:73679#s01e01"]["show_ids"] == {"tmdb": "73679", "tvdb": "337501"}


def test_collection_index_reads_nested_episode_rows(monkeypatch) -> None:
    payload = {
        "episodes": [
            {
                "episode": {"season": 6, "number": 2, "title": "Exile", "ids": {"tmdbid": 5978363}},
                "show": {"title": "The Handmaid's Tale", "year": 2017, "ids": {"tmdbid": 69478}},
                "collected_at": "2026-01-01T00:00:00Z",
            }
        ],
        "pagination": {"has_more": False},
    }

    def fake_request(_adapter, _method, url, **_kwargs):
        if "last_activities" in url:
            return Resp(200, {})
        return Resp(200, payload)

    monkeypatch.setattr(collection, "mdblist_request", fake_request)
    monkeypatch.setattr(collection, "has_auth", lambda _cfg: True)
    monkeypatch.setattr(collection, "_shadow_load", lambda: {"ts": 0, "items": {}})
    monkeypatch.setattr(collection, "_shadow_save", lambda _items: None)
    monkeypatch.setattr(collection, "save_watermark", lambda *_args, **_kwargs: None)

    idx = collection.build_index(_adapter(), per_page=100, max_pages=1)

    item = idx["tmdb:69478#s06e02"]
    assert item["type"] == "episode"
    assert item["title"] == "Exile"
    assert item["ids"] == {"tmdb": "5978363"}
    assert item["show_ids"] == {"tmdb": "69478"}


def test_collection_add_confirms_only_items_seen_live(monkeypatch) -> None:
    def fake_request(_adapter, method, url, **kwargs):
        if method == "POST":
            return Resp(200, {"added": {"shows": 1}})
        if "last_activities" in url:
            return Resp(200, {})
        return Resp(
            200,
            {
                "shows": [
                    {
                        "title": "Show",
                        "year": 2026,
                        "ids": {"tmdb": 100},
                        "seasons": [{"number": 1, "episodes": [{"number": 1, "title": "One"}]}],
                    }
                ],
                "pagination": {"has_more": False},
            },
        )

    monkeypatch.setattr(collection, "mdblist_request", fake_request)
    monkeypatch.setattr(collection, "has_auth", lambda _cfg: True)
    monkeypatch.setattr(collection, "_shadow_load", lambda: {"ts": 0, "items": {}})
    monkeypatch.setattr(collection, "_shadow_save", lambda _items: None)
    monkeypatch.setattr(collection, "_shadow_bust", lambda: None)
    monkeypatch.setattr(collection, "get_watermark", lambda _feature: None)
    monkeypatch.setattr(collection, "save_watermark", lambda *_args, **_kwargs: None)

    result = collection.add(
        _adapter(),
        [
            {"type": "episode", "title": "One", "show_ids": {"tmdb": "100"}, "season": 1, "episode": 1},
            {"type": "episode", "title": "Two", "show_ids": {"tmdb": "100"}, "season": 1, "episode": 2},
        ],
    )

    assert isinstance(result, dict)
    assert result["confirmed_keys"] == ["tmdb:100#s01e01"]
    assert result["presence_confirmed_keys"] == ["tmdb:100#s01e01"]
    assert result["accepted_not_seen_live_keys"] == ["tmdb:100#s01e02"]
    assert result["unresolved"][-1]["hint"] == "not_seen_live"


def test_collection_remove_uses_history_style_bucket_chunks(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_request(_adapter, method, url, **kwargs):
        payload = kwargs.get("json") or {}
        calls.append(payload)
        show_count = len(payload.get("shows") or [])
        if show_count > 200:
            return Resp(400, {"error": "Too many shows in one request (max 200)"})
        return Resp(200, {"deleted": {"shows": show_count}})

    monkeypatch.setattr(collection, "mdblist_request", fake_request)
    monkeypatch.setattr(collection, "has_auth", lambda _cfg: True)
    monkeypatch.setattr(collection, "_shadow_bust", lambda: None)

    items = [{"type": "show", "ids": {"tmdb": i}} for i in range(1, 202)]

    removed, unresolved = collection.remove(_adapter(), items)

    assert removed == 201
    assert unresolved == []
    assert len(calls) == 9
    assert [len(call.get("shows") or []) for call in calls] == [25, 25, 25, 25, 25, 25, 25, 25, 1]
