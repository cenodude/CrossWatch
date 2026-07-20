from __future__ import annotations

import importlib

import pytest

from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PLAYLIST_KIND_SMART,
    PlaylistItem,
    PlaylistResource,
    PlaylistSnapshot,
    normalize_ruleset,
    playlist_capabilities,
    supports_playlists,
    validate_ruleset,
)


def test_resource_roundtrip_and_normalization():
    res = PlaylistResource(
        provider="trakt",
        id=" 123 ",
        name="Weekend",
        kind="SMART",
        can_add=True,
        media_types="movies, shows",
    )
    assert res.provider == "TRAKT"
    assert res.id == "123"
    assert res.kind == PLAYLIST_KIND_SMART
    assert res.is_smart is True
    assert res.writable is False
    assert res.media_types == ("movies", "shows")

    d = res.to_dict()
    again = PlaylistResource.from_dict(d)
    assert again.to_dict() == d


def test_regular_resource_writable():
    res = PlaylistResource(provider="plex", id="9", kind="regular", can_add=True, can_remove=True)
    assert res.kind == PLAYLIST_KIND_REGULAR
    assert res.writable is True


def test_item_from_media_uses_canonical_key():
    it = PlaylistItem.from_media(
        {"type": "movie", "title": "Dune", "year": 2021, "ids": {"tmdb": "438631"}},
        playlist_item_id="pi1",
        position=3,
    )
    assert it.key == "tmdb:438631"
    assert it.playlist_item_id == "pi1"
    assert it.position == 3
    assert it.item.get("ids", {}).get("tmdb") == "438631"

    again = PlaylistItem.from_dict(it.to_dict())
    assert again.to_dict() == it.to_dict()


def test_snapshot_helpers():
    res = PlaylistResource(provider="plex", id="9")
    a = PlaylistItem(key="tmdb:1", item={"type": "movie"})
    b = PlaylistItem(key="tmdb:2", item={"type": "movie"})
    snap = PlaylistSnapshot(resource=res, items=[a, b], checkpoint="cp")
    assert snap.ordered_keys() == ["tmdb:1", "tmdb:2"]
    assert set(snap.by_key().keys()) == {"tmdb:1", "tmdb:2"}
    assert PlaylistSnapshot.from_dict(snap.to_dict()).ordered_keys() == ["tmdb:1", "tmdb:2"]


def test_ruleset_description_is_preserved():
    rs = normalize_ruleset({
        "id": "RULE-01",
        "name": "Split",
        "description": "Human readable note",
        "direction": "one_way",
        "initial_sync": "source_authoritative",
        "read_mode": "direct",
        "write_mode": "partition",
        "membership": "managed_only",
        "order": "ignore",
        "deduplicate": "canonical_id",
        "allocation": "stable_first_fit",
        "rebalance": "never",
        "overflow": "block",
        "per_endpoint_capacity": 100,
        "aggregate_capacity": 1000,
        "maximum_targets": 5,
        "track_assignments": True,
    })
    assert rs["description"] == "Human readable note"


def test_ruleset_name_is_required_and_short():
    base = {
        "id": "RULE-01",
        "name": "Split",
        "direction": "one_way",
        "initial_sync": "source_authoritative",
        "read_mode": "direct",
        "write_mode": "partition",
        "membership": "managed_only",
        "order": "ignore",
        "deduplicate": "canonical_id",
        "allocation": "stable_first_fit",
        "rebalance": "never",
        "overflow": "block",
        "per_endpoint_capacity": 100,
        "aggregate_capacity": 1000,
        "maximum_targets": 5,
        "track_assignments": True,
    }
    ok, why, _ = validate_ruleset({**base, "name": ""})
    assert not ok and "required" in why
    ok2, why2, _ = validate_ruleset({**base, "name": "VeryLongName"})
    assert not ok2 and "10 characters" in why2
    ok3, why3, _ = validate_ruleset({**base, "name": "$Bad"})
    assert not ok3 and "letter or number" in why3
    ok4, why4, _ = validate_ruleset({**base, "name": "Bad/Name"})
    assert not ok4 and "unsupported" in why4


def test_supports_playlists_discovery():
    class Bare:
        pass

    class Full:
        def list_playlist_resources(self, *a, **k): ...
        def get_playlist_snapshot(self, *a, **k): ...
        def create_playlist(self, *a, **k): ...
        def add_playlist_items(self, *a, **k): ...
        def remove_playlist_items(self, *a, **k): ...
        def reorder_playlist_items(self, *a, **k): ...

    assert supports_playlists(Bare()) is False
    assert supports_playlists(Full()) is True
    assert supports_playlists(None) is False


def test_trakt_manifest_declares_playlists():
    mod = importlib.import_module("providers.sync._mod_TRAKT")
    manifest = mod.get_manifest()
    assert manifest["features"]["playlists"] is True
    assert manifest["features"]["watchlist"] is True
    caps = playlist_capabilities(manifest)
    assert caps.get("read") is True
    assert caps.get("smart") is False
    assert "movies" in caps.get("media_types", [])
    assert supports_playlists(mod.OPS) is True


def test_plex_manifest_declares_playlists():
    pytest.importorskip("plexapi")
    mod = importlib.import_module("providers.sync._mod_PLEX")
    manifest = mod.get_manifest()
    assert manifest["features"]["playlists"] is True
    assert manifest["features"]["watchlist"] is True
    caps = playlist_capabilities(manifest)
    assert caps.get("read") is True
    assert caps.get("smart") is True
    assert caps.get("smart_writable") is False
    assert supports_playlists(mod.OPS) is True
