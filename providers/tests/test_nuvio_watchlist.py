# CrossWatch test scripts
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class FakeClient:
    def __init__(self, rows: list[dict[str, Any]] | None = None):
        self.rows = list(rows or [])
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def request_json(self, method: str, path: str, *, payload: Mapping[str, Any] | None = None, **_: Any) -> Any:
        body = dict(payload or {})
        name = path.rsplit("/", 1)[-1]
        self.calls.append((name, body))
        if name == "sync_pull_library":
            offset = int(body.get("p_offset") or 0)
            limit = int(body.get("p_limit") or 500)
            return self.rows[offset : offset + limit]
        if name == "sync_push_library":
            self.rows = [dict(row) for row in body.get("p_items") or []]
            return {}
        return {}


class FakeAdapter:
    def __init__(self, rows: list[dict[str, Any]] | None = None):
        self.config = {"nuvio": {"base_url": "https://api.nuvio.tv", "refresh_token": "refresh", "profile_id": 1}}
        self.instance_id = "default"
        self.client = FakeClient(rows)


class NonPersistingClient(FakeClient):
    def request_json(self, method: str, path: str, *, payload: Mapping[str, Any] | None = None, **kwargs: Any) -> Any:
        name = path.rsplit("/", 1)[-1]
        if name == "sync_push_library":
            self.calls.append((name, dict(payload or {})))
            return {}
        return super().request_json(method, path, payload=payload, **kwargs)


def test_watchlist_reads_library_movies_and_series() -> None:
    from providers.sync.nuvio import _watchlist

    adapter = FakeAdapter(
        [
            {"content_id": "tt0137523", "content_type": "movie", "name": "Fight Club", "poster": "poster.jpg"},
            {"content_id": "tmdb:1396", "content_type": "series", "name": "Breaking Bad"},
        ]
    )

    index = _watchlist.build_index(adapter)

    assert sorted(index) == ["imdb:tt0137523", "tmdb:1396"]
    assert index["tmdb:1396"]["type"] == "show"
    assert index["imdb:tt0137523"]["_nuvio_poster"] == "poster.jpg"


def test_watchlist_add_preserves_existing_library_rows_and_verifies() -> None:
    from providers.sync.nuvio import _watchlist

    adapter = FakeAdapter([{"content_id": "tt0137523", "content_type": "movie", "name": "Fight Club", "poster": "poster.jpg"}])

    result = _watchlist.add(adapter, [{"type": "show", "ids": {"tmdb": "1396"}, "title": "Breaking Bad", "year": 2008}])

    assert result["ok"] is True
    assert set(result["confirmed_keys"]) == {"tmdb:1396"}
    push = [body for name, body in adapter.client.calls if name == "sync_push_library"][0]
    assert [row["content_id"] for row in push["p_items"]] == ["tt0137523", "tmdb:1396"]
    assert push["p_items"][0]["poster"] == "poster.jpg"
    assert push["p_items"][1]["content_type"] == "series"


def test_watchlist_add_uses_canonical_id_for_verification() -> None:
    from providers.sync.nuvio import _watchlist

    adapter = FakeAdapter([])

    result = _watchlist.add(adapter, [{"type": "movie", "ids": {"tmdb": "550", "imdb": "tt0137523"}, "title": "Fight Club"}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tmdb:550"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_library"][0]
    assert push["p_items"][0]["content_id"] == "tmdb:550"


def test_watchlist_add_resolves_imdb_to_tmdb_content_id_when_tmdb_configured(monkeypatch: Any) -> None:
    from providers.metadata import _meta_TMDB
    from providers.sync.nuvio import _watchlist

    class FakeTmdb:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def fetch(self, *, entity: str, ids: dict[str, str], locale: str | None = None, need: dict[str, bool] | None = None) -> dict[str, Any]:
            assert entity == "movie"
            assert ids in ({"imdb": "tt0137523"}, {"tmdb": "550"})
            return {"ids": {"tmdb": "550", "imdb": "tt0137523"}}

    adapter = FakeAdapter([])
    adapter.config["tmdb"] = {"api_key": "tmdb-key"}
    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", FakeTmdb)
    monkeypatch.setattr(_watchlist, "_tmdb_enrichment", lambda *_args, **_kwargs: {})

    result = _watchlist.add(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}, "title": "Fight Club"}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["imdb:tt0137523"]
    assert result["confirmed_destinations"]["imdb:tt0137523"]["key"] == "tmdb:550"
    push = [body for name, body in adapter.client.calls if name == "sync_push_library"][0]
    assert push["p_items"][0]["content_id"] == "tmdb:550"


def test_watchlist_add_skips_tmdb_enrichment_without_metadata_config() -> None:
    from providers.sync.nuvio import _watchlist

    adapter = FakeAdapter([])

    result = _watchlist.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "title": "Fight Club"}])

    assert result["ok"] is True
    push = [body for name, body in adapter.client.calls if name == "sync_push_library"][0]
    assert "poster" not in push["p_items"][0]


def test_watchlist_unresolved_rows_carry_attempted_key() -> None:
    from providers.sync.nuvio import _watchlist

    adapter = FakeAdapter([])
    result = _watchlist.add(adapter, [{"type": "movie", "ids": {"trakt": "12"}, "title": "Unknown"}])

    assert result["ok"] is False
    assert result["attempted"] == 0
    assert result["unresolved_keys"] == ["trakt:12"]
    assert result["unresolved"][0]["key"] == "trakt:12"
    assert result["unresolved"][0]["canonical_key"] == "trakt:12"


def test_watchlist_add_enriches_nuvio_payload_from_configured_tmdb(monkeypatch: Any) -> None:
    from api import metaAPI
    from providers.sync.nuvio import _watchlist

    adapter = FakeAdapter([])
    adapter.config["tmdb"] = {"api_key": "tmdb-key"}

    def fake_get_meta(*_: Any, **__: Any) -> dict[str, Any]:
        return {
            "title": "Fight Club",
            "year": 1999,
            "overview": "A restless office worker finds a new outlet.",
            "genres": ["Drama"],
            "images": {
                "poster": [{"url": "https://image.tmdb.org/t/p/w780/poster.jpg"}],
                "backdrop": [{"url": "https://image.tmdb.org/t/p/w1280/backdrop.jpg"}],
            },
        }

    monkeypatch.setattr(metaAPI, "get_meta", fake_get_meta)

    result = _watchlist.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}, "title": "Fight Club"}])

    assert result["ok"] is True
    push = [body for name, body in adapter.client.calls if name == "sync_push_library"][0]
    row = push["p_items"][0]
    assert row["poster"] == "https://image.tmdb.org/t/p/w780/poster.jpg"
    assert row["background"] == "https://image.tmdb.org/t/p/w1280/backdrop.jpg"
    assert row["description"] == "A restless office worker finds a new outlet."
    assert row["genres"] == ["Drama"]


def test_watchlist_add_resolves_tvdb_to_tmdb_content_id_when_tmdb_configured(monkeypatch: Any) -> None:
    from providers.metadata import _meta_TMDB
    from providers.sync.nuvio import _watchlist

    class FakeTmdb:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def fetch(self, *, entity: str, ids: dict[str, str], locale: str | None = None, need: dict[str, bool] | None = None) -> dict[str, Any]:
            assert entity == "tv"
            assert ids in ({"tvdb": "355567"}, {"tmdb": "69478"})
            return {"ids": {"tmdb": "69478", "tvdb": "355567"}}

    adapter = FakeAdapter([])
    adapter.config["tmdb"] = {"api_key": "tmdb-key"}
    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", FakeTmdb)
    monkeypatch.setattr(_watchlist, "_tmdb_enrichment", lambda *_args, **_kwargs: {})

    result = _watchlist.add(adapter, [{"type": "show", "ids": {"tvdb": "355567"}, "title": "The Boys"}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["tvdb:355567"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_library"][0]
    assert push["p_items"][0]["content_id"] == "tmdb:69478"
    assert push["p_items"][0]["content_type"] == "series"


def test_watchlist_read_enriches_tmdb_rows_with_external_ids(monkeypatch: Any) -> None:
    from providers.metadata import _meta_TMDB
    from providers.sync.nuvio import _watchlist

    class FakeTmdb:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def fetch(self, *, entity: str, ids: dict[str, str], locale: str | None = None, need: dict[str, bool] | None = None) -> dict[str, Any]:
            assert entity == "movie"
            assert ids == {"tmdb": "550"}
            return {"ids": {"tmdb": "550", "imdb": "tt0137523"}}

    adapter = FakeAdapter([{"content_id": "tmdb:550", "content_type": "movie", "name": "Fight Club"}])
    adapter.config["tmdb"] = {"api_key": "tmdb-key"}
    monkeypatch.setattr(_meta_TMDB, "TmdbProvider", FakeTmdb)

    item = _watchlist.build_index(adapter)["tmdb:550"]

    assert item["ids"] == {"tmdb": "550", "imdb": "tt0137523"}


def test_watchlist_remove_full_replaces_library_without_removed_item() -> None:
    from providers.sync.nuvio import _watchlist

    adapter = FakeAdapter(
        [
            {"content_id": "tt0137523", "content_type": "movie", "name": "Fight Club"},
            {"content_id": "tmdb:1396", "content_type": "series", "name": "Breaking Bad"},
        ]
    )

    result = _watchlist.remove(adapter, [{"type": "movie", "ids": {"imdb": "tt0137523"}}])

    assert result["ok"] is True
    assert result["confirmed_keys"] == ["imdb:tt0137523"]
    push = [body for name, body in adapter.client.calls if name == "sync_push_library"][0]
    assert [row["content_id"] for row in push["p_items"]] == ["tmdb:1396"]


def test_watchlist_failed_verification_reports_numeric_errors() -> None:
    from providers.sync.nuvio import _watchlist

    adapter = FakeAdapter([])
    adapter.client = NonPersistingClient([])

    result = _watchlist.add(adapter, [{"type": "movie", "ids": {"tmdb": "550"}}])

    assert result["ok"] is False
    assert result["errors"] == 1
