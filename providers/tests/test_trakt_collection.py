from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from providers.sync.trakt import _collection as collection
from providers.sync.trakt import _history as history


class FakeResp:
    def __init__(self, status: int, payload: Any = None, headers: dict[str, str] | None = None) -> None:
        self.status_code = status
        self._payload = payload
        self.headers = headers or {}
        self.text = "x" if payload is not None else ""

    def json(self) -> Any:
        return self._payload


class FakeSession:
    pass


def _adapter() -> SimpleNamespace:
    return SimpleNamespace(
        cfg=SimpleNamespace(timeout=5, max_retries=1, client_id="cid", access_token="tok"),
        client=SimpleNamespace(session=FakeSession()),
        config={"trakt": {"collection_use_etag": False, "collection_batch_size": 100}},
    )


def test_collection_index_reads_trakt_media_endpoint(monkeypatch: Any) -> None:
    calls: list[str] = []

    def fake_request(_sess: Any, method: str, url: str, **kwargs: Any) -> FakeResp:
        calls.append(f"{method} {url}")
        page = int((kwargs.get("params") or {}).get("page") or 1)
        if url.endswith("/sync/collection/media") and page == 1:
            return FakeResp(
                200,
                [
                    {
                        "type": "movie",
                        "collected_at": "2026-08-24T20:00:00.000Z",
                        "movie": {"title": "Dune", "year": 2021, "ids": {"tmdb": 438631, "trakt": 1}},
                    }
                ],
                {"ETag": "m1", "X-Pagination-Page-Count": "2", "X-Pagination-Item-Count": "2"},
            )
        if url.endswith("/sync/collection/media") and page == 2:
            return FakeResp(
                200,
                [
                    {
                        "type": "show",
                        "show": {"title": "Severance", "year": 2022, "ids": {"tmdb": 95396, "trakt": 2}},
                        "seasons": [
                            {
                                "number": 1,
                                "episodes": [
                                    {"number": 1, "ids": {"tmdb": 1001, "trakt": 11}},
                                    {"number": 2},
                                ],
                            }
                        ],
                    }
                ],
                {"ETag": "m1", "X-Pagination-Page-Count": "2", "X-Pagination-Item-Count": "2"},
            )
        return FakeResp(404, {})

    monkeypatch.setattr(collection, "request_with_retries", fake_request)
    monkeypatch.setattr(collection, "fetch_last_activities", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(collection, "update_watermarks_from_last_activities", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(collection, "_shadow_load", lambda: {"ts": 0, "items": {}})
    monkeypatch.setattr(collection, "_shadow_save", lambda *_args, **_kwargs: None)

    idx = collection.build_index(_adapter())

    assert calls == [
        "GET https://api.trakt.tv/sync/collection/media",
        "GET https://api.trakt.tv/sync/collection/media",
    ]
    assert idx["tmdb:438631"]["type"] == "movie"
    assert idx["tmdb:438631"]["collected_at"] == "2026-08-24T20:00:00.000Z"
    assert idx["tmdb:95396#s01e01"]["type"] == "episode"
    assert idx["tmdb:95396#s01e02"]["type"] == "episode"


def test_collection_write_uses_collection_endpoint_and_shared_nested_body(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def fake_request(_sess: Any, method: str, url: str, **kwargs: Any) -> FakeResp:
        calls.append({"method": method, "url": url, "json": kwargs.get("json")})
        return FakeResp(201, {"added": {"movies": 0, "shows": 0, "seasons": 0, "episodes": 1}})

    monkeypatch.setattr(collection, "request_with_retries", fake_request)
    monkeypatch.setattr(collection, "_shadow_bust", lambda: None)

    res = collection.add(
        _adapter(),
        [
            {
                "type": "episode",
                "series_title": "Severance",
                "show_ids": {"tmdb": "95396"},
                "season": 1,
                "episode": 2,
                "ids": {},
                "collected_at": "2026-01-02T03:04:05Z",
            }
        ],
    )

    assert res["count"] == 1
    assert res["confirmed"] == 1
    assert res["unresolved"] == []
    assert calls[0]["url"] == "https://api.trakt.tv/sync/collection"
    assert calls[0]["json"] == {
        "shows": [
            {
                "ids": {"tmdb": "95396"},
                "seasons": [
                    {
                        "number": 1,
                        "episodes": [{"number": 2, "watched_at": "2026-01-02T03:04:05Z"}],
                    }
                ],
            }
        ]
    }


def test_collection_write_reports_existing_as_skipped_not_added(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def fake_request(_sess: Any, method: str, url: str, **kwargs: Any) -> FakeResp:
        calls.append({"method": method, "url": url, "json": kwargs.get("json")})
        return FakeResp(201, {"added": {}, "existing": {"movies": 1}, "not_found": {}})

    monkeypatch.setattr(collection, "request_with_retries", fake_request)
    monkeypatch.setattr(collection, "_shadow_bust", lambda: None)

    res = collection.add(
        _adapter(),
        [
            {
                "type": "movie",
                "title": "Dune",
                "ids": {"tmdb": "438631"},
                "collected_at": "2026-01-02T03:04:05Z",
            }
        ],
    )

    assert res["count"] == 0
    assert res["confirmed"] == 0
    assert res["skipped"] == 1
    assert res["skipped_keys"] == ["tmdb:438631"]
    assert res["unresolved"] == []


def test_collection_write_small_delta_maps_existing_and_not_found_exactly(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def fake_request(_sess: Any, method: str, url: str, **kwargs: Any) -> FakeResp:
        payload = kwargs.get("json") or {}
        calls.append({"method": method, "url": url, "json": payload})
        movies = payload.get("movies") or []
        tmdb = str((((movies[0] if movies else {}).get("ids") or {}).get("tmdb") or ""))
        if tmdb == "438631":
            return FakeResp(201, {"added": {}, "existing": {"movies": 1}, "not_found": {}})
        return FakeResp(201, {"added": {}, "existing": {}, "not_found": {"movies": [{"tmdb": int(tmdb)}]}})

    monkeypatch.setattr(collection, "request_with_retries", fake_request)
    monkeypatch.setattr(collection, "_shadow_bust", lambda: None)

    res = collection.add(
        _adapter(),
        [
            {"type": "movie", "title": "Dune", "ids": {"tmdb": "438631"}},
            {"type": "movie", "title": "Missing", "ids": {"tmdb": "999999"}},
        ],
    )

    assert len(calls) == 2
    assert res["count"] == 0
    assert res["confirmed"] == 0
    assert res["skipped"] == 1
    assert res["skipped_keys"] == ["tmdb:438631"]
    assert res["unresolved"][0]["key"] == "tmdb:999999"


def test_history_add_to_library_uses_trakt_collection_write_date_field() -> None:
    body = {
        "movies": [
            {"ids": {"tmdb": "438631"}, "watched_at": "2026-01-02T03:04:05Z"},
        ],
        "shows": [
            {
                "ids": {"tmdb": "95396"},
                "seasons": [
                    {
                        "number": 1,
                        "episodes": [{"number": 2, "watched_at": "2026-01-03T03:04:05Z"}],
                    }
                ],
            }
        ],
    }

    out = history._history_body_to_collection(body, {"movies", "shows"})

    assert out["movies"][0]["watched_at"] == "2026-01-02T03:04:05Z"
    assert "collected_at" not in out["movies"][0]
    assert out["shows"][0]["seasons"][0]["episodes"][0]["watched_at"] == "2026-01-03T03:04:05Z"
    assert "collected_at" not in out["shows"][0]["seasons"][0]["episodes"][0]
