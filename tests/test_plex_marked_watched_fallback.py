# tests/test_plex_marked_watched_fallback.py
# CrossWatch - Plex Marked-Watched Library Scan Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable

import pytest

from providers.sync.plex import _history as history


LAST_VIEWED = 1787093918


class _Resp:
    def __init__(self, rows: list[dict[str, Any]]):
        self.ok = True
        self.status_code = 200
        self.headers = {"content-type": "application/json"}
        self._payload = {"MediaContainer": {"Metadata": rows, "totalSize": len(rows)}}

    def json(self) -> dict[str, Any]:
        return self._payload


class _Session:
    def __init__(self, handler: Callable[[dict[str, Any]], _Resp]):
        self.headers: dict[str, str] = {}
        self._handler = handler
        self.requests: list[dict[str, Any]] = []

    def get(self, url: str, params: Any = None, headers: Any = None, timeout: Any = None) -> _Resp:
        p = dict(params or {})
        self.requests.append(p)
        if p.get("includeMeta"):
            return _Resp([])
        return self._handler(p)


def _episode(rating_key: str, view_count: int, last_viewed: int | None = LAST_VIEWED) -> dict[str, Any]:
    row: dict[str, Any] = {
        "type": "episode",
        "ratingKey": rating_key,
        "title": f"Episode {rating_key}",
        "grandparentTitle": "Example series",
        "parentIndex": 1,
        "index": int(rating_key),
        "viewCount": view_count,
    }
    if last_viewed is not None:
        row["lastViewedAt"] = last_viewed
    return row


def _movie(rating_key: str, view_count: int, last_viewed: int | None = LAST_VIEWED) -> dict[str, Any]:
    row: dict[str, Any] = {
        "type": "movie",
        "ratingKey": rating_key,
        "title": f"Movie {rating_key}",
        "viewCount": view_count,
    }
    if last_viewed is not None:
        row["lastViewedAt"] = last_viewed
    return row


@pytest.fixture
def scan(monkeypatch):
    monkeypatch.setattr(history, "plex_headers", lambda _t: {})
    monkeypatch.setattr(
        history,
        "normalize_discover_row",
        lambda row, token=None: {
            "type": row.get("type"),
            "title": row.get("title"),
            "series_title": row.get("grandparentTitle"),
            "season": row.get("parentIndex"),
            "episode": row.get("index"),
            "ids": {"plex": str(row.get("ratingKey"))},
        },
    )

    def _run(handler: Callable[[dict[str, Any]], _Resp], section_type: str = "show") -> tuple[list, _Session]:
        ses = _Session(handler)
        server = SimpleNamespace(baseurl="http://plex.local:32400", _session=ses, token="tok")
        section = SimpleNamespace(type=section_type, key="1", title="Example section")
        adapter = SimpleNamespace(
            client=SimpleNamespace(server=server),
            libraries=lambda types=(): [section],
        )
        return history._iter_marked_watched_from_library(adapter, set()), ses

    return _run


def test_in_progress_episode_is_not_reported_as_marked_watched(scan):
    def handler(params: dict[str, Any]) -> _Resp:
        if "unwatched" in params:
            return _Resp([])
        return _Resp([_episode("2", view_count=0)])

    results, ses = scan(handler)

    assert results == []
    assert any("unwatched" not in r for r in ses.requests), "the unfiltered fallback scan should still run"


def test_fallback_still_returns_genuinely_watched_episodes(scan):
    def handler(params: dict[str, Any]) -> _Resp:
        if "unwatched" in params:
            return _Resp([])
        return _Resp([_episode("2", view_count=1)])

    results, _ = scan(handler)

    assert len(results) == 1
    meta, ts = results[0]
    assert meta["ids"]["plex"] == "2"
    assert ts == LAST_VIEWED


def test_fallback_keeps_watched_and_drops_in_progress(scan):
    def handler(params: dict[str, Any]) -> _Resp:
        if "unwatched" in params:
            return _Resp([])
        return _Resp([
            _episode("1", view_count=1),
            _episode("2", view_count=0),
            _episode("3", view_count=2),
            _episode("4", view_count=0),
        ])

    results, _ = scan(handler)

    assert sorted(m["ids"]["plex"] for m, _ in results) == ["1", "3"]


def test_filtered_scan_is_unaffected_and_skips_the_fallback(scan):
    def handler(params: dict[str, Any]) -> _Resp:
        if "unwatched" in params:
            return _Resp([_movie("7", view_count=1)])
        raise AssertionError("fallback must not run when the filtered scan returned rows")

    results, ses = scan(handler, section_type="movie")

    assert len(results) == 1
    assert results[0][0]["ids"]["plex"] == "7"
    assert all("unwatched" in r for r in ses.requests)


def test_movie_section_never_falls_back(scan):
    def handler(params: dict[str, Any]) -> _Resp:
        if "unwatched" in params:
            return _Resp([])
        raise AssertionError("movie sections must not run the unfiltered fallback")

    results, _ = scan(handler, section_type="movie")

    assert results == []


def test_watched_row_without_timestamp_is_still_returned(scan):
    def handler(params: dict[str, Any]) -> _Resp:
        if "unwatched" in params:
            return _Resp([_episode("5", view_count=1, last_viewed=None)])
        return _Resp([])

    results, _ = scan(handler)

    assert len(results) == 1
    meta, ts = results[0]
    assert ts == 0
    assert meta["watched_at"] is None
    assert meta["_cw_watched_at_missing"] is True
