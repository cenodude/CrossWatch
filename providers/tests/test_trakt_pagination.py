# /providers/tests/test_trakt_pagination.py
# Regression coverage for complete Trakt pagination and failed fetches
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
import json
from types import SimpleNamespace
from typing import Any

import pytest

from providers.sync.trakt import _history as history
from providers.sync.trakt._pagination import TraktPager, TraktPaginationError


class Response:
    def __init__(self, rows: Any, *, status: int = 200, headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self.headers = headers or {}
        self.payload = rows
        self.text = json.dumps(rows)

    def json(self) -> Any:
        return self.payload


def _row(number: int) -> dict[str, Any]:
    return {
        "id": number,
        "type": "movie",
        "watched_at": "2004-01-18T05:24:00.000Z",
        "collected_at": "2004-01-18T05:24:00.000Z",
        "rated_at": "2004-01-18T05:24:00.000Z",
        "paused_at": "2004-01-18T05:24:00.000Z",
        "rating": 8,
        "progress": 25,
        "movie": {"title": f"Movie {number}", "runtime": 90, "ids": {"trakt": number, "tmdb": number}},
    }


READERS = ("history", "watchlist", "collection", "ratings", "playlists", "progress")


def _reader(monkeypatch: Any, name: str, responses: list[Response]) -> tuple[Any, list[int], list[Any]]:
    module = importlib.import_module(f"providers.sync.trakt._{name}")
    calls: list[int] = []
    saved: list[Any] = []

    def get(url: str, **kwargs: Any) -> Response:
        if name == "progress" and url.endswith("/episodes"):
            return Response([])
        page = int(kwargs["params"]["page"])
        calls.append(page)
        return responses[page - 1] if page <= len(responses) else Response([])

    session = SimpleNamespace(get=get)
    adapter = SimpleNamespace(
        cfg=SimpleNamespace(timeout=1, max_retries=0, client_id="test", access_token="test"),
        config={},
        client=SimpleNamespace(session=session, get=get, BASE="https://api.trakt.tv"),
    )
    if hasattr(module, "request_with_retries"):
        monkeypatch.setattr(module, "request_with_retries", lambda _session, _method, url, **kw: get(url, **kw))
    for attr in ("fetch_last_activities", "update_watermarks_from_last_activities"):
        if hasattr(module, attr):
            monkeypatch.setattr(module, attr, lambda *a, **kw: {})
    if hasattr(module, "_shadow_load"):
        monkeypatch.setattr(module, "_shadow_load", lambda: {})
        monkeypatch.setattr(module, "_shadow_save", lambda *a, **kw: saved.append(a))
    if name == "history":
        fetch = lambda: module._fetch_history(session, {}, module.URL_HIST_MOV, per_page=100, max_pages=10, timeout=1, max_retries=0)
    elif name == "ratings":
        fetch = lambda: module._fetch_bucket(session, {}, module.URL_RAT_MOV, "movie", 100, 10, 1, 0)
    elif name == "playlists":
        monkeypatch.setattr(module, "list_resources", lambda _adapter: [])
        fetch = lambda: module.get_snapshot(adapter, "123").items
    else:
        fetch = lambda: module.build_index(adapter)
    return fetch, calls, saved


@pytest.mark.parametrize("name", READERS)
@pytest.mark.parametrize("headers", [{}, {"X-Pagination-Page-Count": "1", "X-Pagination-Limit": "1"}])
def test_readers_fetch_older_items_beyond_initial_count_and_short_pages(monkeypatch: Any, name: str, headers: dict[str, str]) -> None:
    fetch, calls, _ = _reader(monkeypatch, name, [Response([_row(1)], headers=headers), Response([_row(2)]), Response([])])

    assert len(fetch()) == 2
    assert calls == [1, 2, 3]


@pytest.mark.parametrize("name", READERS)
def test_readers_reject_repeated_pages_without_saving_partial_results(monkeypatch: Any, name: str) -> None:
    fetch, calls, saved = _reader(monkeypatch, name, [Response([_row(1), _row(2)]), Response([_row(2), _row(1)])])

    with pytest.raises(TraktPaginationError, match="repeated_page"):
        fetch()
    assert calls == [1, 2]
    assert saved == []


@pytest.mark.parametrize("name", ("history", "collection", "ratings", "playlists", "progress"))
def test_readers_reject_failed_later_pages(monkeypatch: Any, name: str) -> None:
    fetch, calls, saved = _reader(monkeypatch, name, [Response([_row(1)]), Response({}, status=503)])

    with pytest.raises(TraktPaginationError, match="http_failed"):
        fetch()
    assert calls == [1, 2]
    assert saved == []


def test_watchlist_failed_later_page_returns_previous_shadow(monkeypatch: Any) -> None:
    from providers.sync.trakt import _watchlist

    fetch, calls, saved = _reader(monkeypatch, "watchlist", [Response([_row(1)]), Response({}, status=503)])
    previous = {"tmdb:99": {"type": "movie", "ids": {"tmdb": "99"}}}
    monkeypatch.setattr(_watchlist, "_shadow_load", lambda: {"items": previous})

    assert fetch() == previous
    assert calls == [1, 2]
    assert saved == []


def test_history_overlapping_pages_preserve_rewatches_without_duplicate_events(monkeypatch: Any) -> None:
    first = _row(1)
    rewatch = {**first, "id": 2, "watched_at": "2005-01-18T05:24:00.000Z"}
    fetch, _, _ = _reader(monkeypatch, "history", [Response([first]), Response([first, rewatch]), Response([])])

    rows = fetch()

    assert [row["_trakt_history_id"] for row in rows] == ["1", "2"]


@pytest.mark.parametrize("payload", [None, {}, "invalid"])
def test_pager_rejects_non_array_responses(payload: Any) -> None:
    with pytest.raises(TraktPaginationError, match="invalid_response"):
        TraktPager("history").read(Response(payload), 1)


def test_pager_rejects_invalid_json() -> None:
    response = SimpleNamespace(status_code=200, json=lambda: json.loads("{"))
    with pytest.raises(TraktPaginationError, match="invalid_json"):
        TraktPager("history").read(response, 1)


def test_page_cap_allows_empty_completion_probe_but_rejects_more_data() -> None:
    pager = TraktPager("history", 1)
    assert pager.read(Response([_row(1)]), 1)
    assert pager.read(Response([]), 2) == []
    with pytest.raises(TraktPaginationError, match="safety_cap_hit"):
        pager.read(Response([_row(2)]), 2)


def test_history_discards_cache_from_before_complete_pagination(monkeypatch: Any, tmp_path: Any) -> None:
    path = tmp_path / "trakt_history.index.json"
    path.write_text(json.dumps({"schema": 2, "items": {"tmdb:1": {}}}), encoding="utf-8")
    monkeypatch.setattr(history, "_cache_path", lambda: path)
    monkeypatch.setattr(history, "_pair_scope", lambda: "test")
    monkeypatch.setattr(history, "_is_capture_mode", lambda: False)

    assert history._load_cache_doc() == {}


def test_history_failed_fetch_does_not_save_cache(monkeypatch: Any) -> None:
    _reader(monkeypatch, "history", [Response([_row(1)]), Response({}, status=503)])
    saved: list[Any] = []
    monkeypatch.setattr(history, "_load_cache_doc", lambda: {})
    monkeypatch.setattr(history, "_save_cache_doc", lambda *a, **kw: saved.append(a))
    monkeypatch.setattr(history, "_preflight_total", lambda *a, **kw: 1)
    adapter = SimpleNamespace(cfg={"timeout": 1, "max_retries": 0}, client=SimpleNamespace(session=None))
    monkeypatch.setattr(history, "headers_for_adapter", lambda _adapter: {})

    with pytest.raises(TraktPaginationError):
        history.build_index(adapter)
    assert saved == []


@pytest.mark.parametrize("name,old", [("watchlist", {}), ("ratings", {}), ("collection", {"schema": 3})])
def test_other_readers_discard_caches_from_before_complete_pagination(monkeypatch: Any, tmp_path: Any, name: str, old: dict[str, Any]) -> None:
    module = importlib.import_module(f"providers.sync.trakt._{name}")
    path = tmp_path / f"{name}.json"
    path.write_text(json.dumps({**old, "items": {"tmdb:1": {}}}), encoding="utf-8")
    monkeypatch.setattr(module, "_cache_path" if name == "ratings" else "_shadow_path", lambda: path)
    monkeypatch.setattr(module, "_pair_scope", lambda: "test")
    monkeypatch.setattr(module, "_is_capture_mode", lambda: False)
    load = module._load_cache_doc if name == "ratings" else module._shadow_load

    assert not load().get("items")


@pytest.mark.parametrize("failure", [False, True])
def test_dropped_shows_fetches_all_pages_and_does_not_cache_failures(monkeypatch: Any, tmp_path: Any, failure: bool) -> None:
    from providers.sync.trakt import _common

    calls: list[int] = []
    saved: list[Any] = []

    def get(_url: str, **kw: Any) -> Response:
        page = int(kw["params"]["page"])
        calls.append(page)
        if page == 2 and failure:
            return Response({}, status=503)
        return Response([{"show": {"title": f"Show {page}", "ids": {"tmdb": page}}}] if page < 3 else [])

    monkeypatch.setattr(_common, "headers_for_adapter", lambda _adapter: {})
    monkeypatch.setattr(_common, "fetch_last_activities", lambda *a, **kw: {})
    monkeypatch.setattr(_common, "_dropped_path", lambda: tmp_path / "dropped.json")
    monkeypatch.setattr(_common, "_read_json", lambda _path: {"version": 1, "tokens": ["tmdb:99"]})
    monkeypatch.setattr(_common, "_write_json", lambda *a: saved.append(a))
    adapter = SimpleNamespace(client=SimpleNamespace(session=None, get=get), cfg=SimpleNamespace())

    if failure:
        with pytest.raises(TraktPaginationError):
            _common.load_dropped_show_tokens(adapter)
        assert saved == []
    else:
        assert _common.load_dropped_show_tokens(adapter) == {"tmdb:1", "tmdb:2"}
        assert calls == [1, 2, 3]
        assert saved[0][1]["version"] == 2


@pytest.mark.parametrize("failure", [False, True])
def test_playback_service_fetches_all_pages_and_rejects_partial_results(monkeypatch: Any, failure: bool) -> None:
    from services.playback_progress.adapters import trakt

    calls: list[dict[str, Any]] = []

    def get(url: str, **kw: Any) -> Response:
        if url.endswith("/episodes"):
            return Response([])
        params = kw["params"]
        calls.append(params)
        page = params["page"]
        if page == 2 and failure:
            return Response({}, status=503)
        return Response([_row(page)] if page < 3 else [])

    module = SimpleNamespace(client=SimpleNamespace(BASE="https://api.trakt.tv", get=get))
    monkeypatch.setattr(trakt, "_module", lambda _config: module)
    adapter = trakt.TraktPlaybackAdapter()
    monkeypatch.setattr(adapter, "capabilities", lambda *a, **kw: None)
    monkeypatch.setattr(adapter, "_normalize", lambda row, *a: row)

    result = adapter.list_progress({}, instance_id="default", instance_label="Default")

    assert result.ok is not failure
    if failure:
        assert result.items == []
        assert result.remote_status == 503
    else:
        assert [row["id"] for row in result.items] == [1, 2]
        assert [params["page"] for params in calls] == [1, 2, 3]
        assert all(params["limit"] == 100 for params in calls)


def test_discovery_keeps_page_size_constant_when_trakt_caps_responses(monkeypatch: Any) -> None:
    from providers.sync.trakt import _playlists

    calls: list[dict[str, Any]] = []

    def get(_session: Any, _method: str, _url: str, **kw: Any) -> Response:
        params = kw["params"]
        calls.append(params)
        start = (params["page"] - 1) * 60
        return Response([_row(number) for number in range(start + 1, start + 61)])

    monkeypatch.setattr(_playlists, "request_with_retries", get)
    monkeypatch.setattr(_playlists, "headers_for_adapter", lambda _adapter: {})
    adapter = SimpleNamespace(client=SimpleNamespace(session=None), cfg=SimpleNamespace(timeout=1, max_retries=0))

    result = _playlists.get_snapshot(adapter, "discovery:trakt:movies:trending")

    assert len(result.items) == 100
    assert [params["limit"] for params in calls] == [100, 100]
