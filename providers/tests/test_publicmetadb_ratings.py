from __future__ import annotations

from typing import Any

from providers.sync._mod_PUBLICMETADB import PUBLICMETADBModule
from providers.sync.publicmetadb import _ratings


class FakeResp:
    def __init__(self, status: int, payload: Any = None):
        self.status_code = status
        self._payload = payload
        self.text = "x" if payload is not None else ""

    def json(self) -> Any:
        return self._payload


class FakeCfg:
    ratings_label = "Overall"
    ratings_submit_per_hour = 200
    ratings_update_per_hour = 100


class FakeClient:
    def __init__(self, responses: dict[tuple[str, str], Any] | None = None):
        self.responses = responses or {}
        self.calls: list[dict[str, Any]] = []

    def get_json(self, path: str, **kw: Any) -> dict[str, Any]:
        self.calls.append({"method": "GET", "path": path, "params": kw.get("params")})
        return dict(self.responses.get(("GET", path), {"items": []}))

    def post_once(self, path: str, **kw: Any) -> FakeResp:
        self.calls.append({"method": "POST", "path": path, "json": kw.get("json")})
        payload = self.responses.get(("POST", path), {"item": {"id": "new-rating", "score": 50}})
        status = int(payload.get("status", 200)) if isinstance(payload, dict) else 200
        return FakeResp(status, payload)

    def delete(self, path: str, **kw: Any) -> FakeResp:
        self.calls.append({"method": "DELETE", "path": path, "json": kw.get("json")})
        payload = self.responses.get(("DELETE", path), {"success": True})
        status = int(payload.get("status", 200)) if isinstance(payload, dict) else 200
        return FakeResp(status, payload)

    @staticmethod
    def safe_json(resp: Any) -> Any:
        return resp.json()


class FakeAdapter:
    def __init__(self, client: FakeClient):
        self.cfg = FakeCfg()
        self.client = client
        self.instance_id = "default"
        self.config = {"publicmetadb": {"api_key": "k", "ratings_label": "Overall"}}


def test_build_index_refreshes_stale_shadow_rating_from_remote_record(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(_ratings, "state_file", lambda name: tmp_path / name)
    _ratings._shadow_save(
        {
            "tmdb:550#rating:overall": {
                "id": "rating-old",
                "label": "Overall",
                "item": {"type": "movie", "ids": {"tmdb": "550"}, "rating": 5, "label": "Overall"},
            }
        }
    )

    client = FakeClient(
        {
            ("GET", "/api/external/ratings"): {
                "items": [
                    {
                        "id": "rating-old",
                        "tmdb_id": 550,
                        "media_type": "movie",
                        "score": 40,
                        "label": "Overall",
                    }
                ]
            }
        }
    )

    index = _ratings.build_index(FakeAdapter(client))

    assert index["tmdb:550#rating:overall"]["rating"] == 4
    assert client.calls[0]["params"] == {"tmdb_id": 550, "media_type": "movie", "label": "Overall"}


def test_add_replaces_existing_shadow_rating_by_delete_then_post(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(_ratings, "state_file", lambda name: tmp_path / name)
    _ratings._shadow_save(
        {
            "tmdb:550#rating:overall": {
                "id": "rating-old",
                "label": "Overall",
                "item": {"type": "movie", "ids": {"tmdb": "550"}, "rating": 4, "label": "Overall"},
            }
        }
    )
    client = FakeClient({("POST", "/api/external/ratings"): {"item": {"id": "rating-new", "score": 50}}})

    count, unresolved = _ratings.add(
        FakeAdapter(client),
        [{"type": "movie", "ids": {"tmdb": "550"}, "rating": 5, "label": "Overall"}],
    )

    assert count == 1
    assert unresolved == []
    assert [(c["method"], c["path"]) for c in client.calls] == [
        ("DELETE", "/api/external/ratings/rating-old"),
        ("POST", "/api/external/ratings"),
    ]
    index = _ratings.build_index(FakeAdapter(FakeClient()))
    assert index["tmdb:550#rating:overall"]["rating"] == 5
    assert index["tmdb:550#rating:overall"]["rating_id"] == "rating-new"


def test_add_recreates_when_existing_shadow_rating_id_is_already_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(_ratings, "state_file", lambda name: tmp_path / name)
    _ratings._shadow_save(
        {
            "tmdb:550#rating:overall": {
                "id": "rating-gone",
                "label": "Overall",
                "item": {"type": "movie", "ids": {"tmdb": "550"}, "rating": 4, "label": "Overall"},
            }
        }
    )
    client = FakeClient(
        {
            ("DELETE", "/api/external/ratings/rating-gone"): {"status": 404},
            ("POST", "/api/external/ratings"): {"item": {"id": "rating-new", "score": 50}},
        }
    )

    count, unresolved = _ratings.add(
        FakeAdapter(client),
        [{"type": "movie", "ids": {"tmdb": "550"}, "rating": 5, "label": "Overall"}],
    )

    assert count == 1
    assert unresolved == []
    assert [(c["method"], c["path"]) for c in client.calls] == [
        ("DELETE", "/api/external/ratings/rating-gone"),
        ("POST", "/api/external/ratings"),
    ]


def test_add_treats_matching_409_conflict_as_existing_rating(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(_ratings, "state_file", lambda name: tmp_path / name)
    client = FakeClient(
        {
            ("POST", "/api/external/ratings"): {"status": 409, "error": "already_exists"},
            ("GET", "/api/external/ratings"): {
                "items": [
                    {
                        "id": "rating-existing",
                        "tmdb_id": 550,
                        "media_type": "movie",
                        "score": 50,
                        "label": "Overall",
                    }
                ]
            },
        }
    )

    adapter = FakeAdapter(client)
    count, unresolved = _ratings.add(
        adapter,
        [{"type": "movie", "ids": {"tmdb": "550"}, "rating": 5, "label": "Overall"}],
    )

    assert count == 0
    assert unresolved == []
    assert adapter._publicmetadb_rating_skipped_keys == ["tmdb:550"]
    assert [(c["method"], c["path"]) for c in client.calls] == [
        ("POST", "/api/external/ratings"),
        ("GET", "/api/external/ratings"),
    ]
    index = _ratings.build_index(FakeAdapter(FakeClient()))
    assert index["tmdb:550#rating:overall"]["rating_id"] == "rating-existing"


def test_module_reports_matching_409_conflict_as_skipped(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(_ratings, "state_file", lambda name: tmp_path / name)
    mod = PUBLICMETADBModule.__new__(PUBLICMETADBModule)
    mod.cfg = FakeCfg()
    mod.client = FakeClient(
        {
            ("POST", "/api/external/ratings"): {"status": 409, "error": "already_exists"},
            ("GET", "/api/external/ratings"): {
                "items": [
                    {
                        "id": "rating-existing",
                        "tmdb_id": 550,
                        "media_type": "movie",
                        "score": 50,
                        "label": "Overall",
                    }
                ]
            },
        }
    )
    mod.config = {"publicmetadb": {"api_key": "k", "ratings_label": "Overall"}}

    res = mod.add("ratings", [{"type": "movie", "ids": {"tmdb": "550"}, "rating": 5, "label": "Overall"}])

    assert res["count"] == 0
    assert res["confirmed_keys"] == []
    assert res["skipped_keys"] == ["tmdb:550"]
    assert res["unresolved"] == []
