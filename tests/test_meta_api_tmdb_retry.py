from __future__ import annotations

from typing import Any

import requests

from api import metaAPI


class FakeResponse:
    def __init__(self, status: int, payload: Any = None, headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> Any:
        return self._payload


def _patch(monkeypatch, responses: list[Any]) -> list[float]:
    slept: list[float] = []
    queue = list(responses)

    def fake_get(url, params=None, timeout=None, **kwargs):
        item = queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(metaAPI.requests, "get", fake_get)
    monkeypatch.setattr(metaAPI.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(metaAPI, "_tmdb_retry_policy", lambda: (3, 0.5, 4.0))
    return slept


def test_429_honours_retry_after_then_succeeds(monkeypatch) -> None:
    slept = _patch(
        monkeypatch,
        [
            FakeResponse(429, headers={"Retry-After": "2"}),
            FakeResponse(200, {"posters": [{"file_path": "/a.jpg"}]}),
        ],
    )
    data = metaAPI._tmdb_get_json("https://api.themoviedb.org/3/movie/8392/images")
    assert data == {"posters": [{"file_path": "/a.jpg"}]}
    assert slept == [2.0]


def test_500_backs_off_then_gives_up(monkeypatch) -> None:
    slept = _patch(monkeypatch, [FakeResponse(500) for _ in range(4)])
    assert metaAPI._tmdb_get_json("https://api.themoviedb.org/3/movie/1/images") is None
    assert len(slept) == 3


def test_404_returns_none_without_retrying(monkeypatch) -> None:
    slept = _patch(monkeypatch, [FakeResponse(404)])
    assert metaAPI._tmdb_get_json("https://api.themoviedb.org/3/movie/1/images") is None
    assert slept == []


def test_network_error_retries_then_gives_up(monkeypatch) -> None:
    slept = _patch(monkeypatch, [requests.exceptions.ConnectionError() for _ in range(4)])
    assert metaAPI._tmdb_get_json("https://api.themoviedb.org/3/movie/1/images") is None
    assert len(slept) == 3


def test_posters_survive_a_rate_limited_first_attempt(monkeypatch) -> None:
    _patch(
        monkeypatch,
        [
            FakeResponse(429, headers={"Retry-After": "1"}),
            FakeResponse(200, {"posters": [{"file_path": "/totoro.jpg", "iso_639_1": "en"}]}),
        ],
    )
    posters = metaAPI._tmdb_fetch_posters("key", "movie", "8392", "en-US")
    assert len(posters) == 1
    assert posters[0]["path"] == "/totoro.jpg"


def test_posters_return_empty_when_retries_exhausted(monkeypatch) -> None:
    _patch(monkeypatch, [FakeResponse(429, headers={"Retry-After": "1"}) for _ in range(4)])
    assert metaAPI._tmdb_fetch_posters("key", "movie", "8392", "en-US") == []
