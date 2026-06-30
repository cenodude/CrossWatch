from __future__ import annotations

import json

from api import metaAPI
from cw_platform.config_base import DEFAULT_CFG
from cw_platform.metadata_cache import (
    merge_metadata_cache_payload,
    metadata_cache_path,
    read_metadata_cache,
    write_metadata_cache,
)


def test_metadata_default_ttl_is_30_days() -> None:
    assert DEFAULT_CFG["metadata"]["ttl_hours"] == 720


def test_shared_metadata_cache_round_trip_and_expiry(tmp_path, monkeypatch) -> None:
    path = metadata_cache_path(tmp_path, "movie", "123", "nl-NL")
    assert path == tmp_path / "movie" / "123.nl-NL.json"
    assert write_metadata_cache(path, {"title": "Example", "year": 2026}) is True
    assert read_metadata_cache(path, ttl_seconds=720 * 3600)["title"] == "Example"

    payload = json.loads(path.read_text("utf-8"))
    payload["fetched_at"] = 1
    path.write_text(json.dumps(payload), encoding="utf-8")
    assert read_metadata_cache(path, ttl_seconds=720 * 3600) is None
    assert read_metadata_cache(path, ttl_seconds=None)["title"] == "Example"

    monkeypatch.setattr(metaAPI, "_meta_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(metaAPI, "_cfg_meta_ttl_secs", lambda: 720 * 3600)
    assert metaAPI._meta_cache_path("movie", "123", "nl-NL") == path


def test_shared_metadata_cache_merge_preserves_existing_artwork() -> None:
    merged = merge_metadata_cache_payload(
        {
            "title": "Old title",
            "ids": {"tmdb": "1", "imdb": "tt1"},
            "images": {"poster": [{"url": "poster.jpg"}]},
        },
        {"title": "New title", "year": 2026, "ids": {"tmdb": "1"}},
    )

    assert merged["title"] == "New title"
    assert merged["year"] == 2026
    assert merged["ids"] == {"tmdb": "1", "imdb": "tt1"}
    assert merged["images"] == {"poster": [{"url": "poster.jpg"}]}


def test_meta_api_reuses_shared_disk_cache_after_memory_cache_reset(tmp_path, monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    class Metadata:
        def resolve(self, *, entity, ids, locale=None, need=None):
            calls.append((entity, ids["tmdb"]))
            return {"type": entity, "title": "Cached title", "year": 2026, "ids": dict(ids)}

    monkeypatch.setattr(metaAPI, "_env", lambda: (Metadata(), tmp_path.parent, lambda: {}))
    monkeypatch.setattr(metaAPI, "_meta_cache_dir", lambda: tmp_path)
    monkeypatch.setattr(metaAPI, "_meta_cache_enabled", lambda: True)
    monkeypatch.setattr(metaAPI, "_cfg_meta_ttl_secs", lambda: 720 * 3600)
    metaAPI._resolve_tmdb_cached.cache_clear()

    first = metaAPI.get_meta("unused", "movie", "321", tmp_path, need={"title": True, "year": True}, locale="en-US")
    metaAPI._resolve_tmdb_cached.cache_clear()
    second = metaAPI.get_meta("unused", "movie", "321", tmp_path, need={"title": True, "year": True}, locale="en-US")

    assert first["title"] == "Cached title"
    assert second["title"] == "Cached title"
    assert calls == [("movie", "321")]
