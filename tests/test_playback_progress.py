from __future__ import annotations

from services.playback_progress.service import (
    _combine_records,
    _overlay_live_streams,
    _profile_has_explicit_identity,
    _record_group_keys,
    PlaybackProgressService,
)
from services.playback_progress.adapters.trakt import _trakt_image_url


def _record(**overrides):
    record = {
        "provider": "plex",
        "provider_label": "Plex",
        "instance_id": "default",
        "instance_label": "Plex Default",
        "remote_id": "101",
        "canonical_key": "tmdb:123",
        "media_type": "movie",
        "title": "Pressure",
        "year": 2026,
        "ids": {"tmdb": "123"},
        "progress_percent": 2.0,
        "updated_at": "2026-06-27T10:00:00Z",
        "poster_url": "",
        "backdrop_url": "",
        "provider_metadata": {},
    }
    record.update(overrides)
    return record


def test_combined_record_keeps_available_artwork_regardless_of_input_order():
    missing = _record(provider="simkl", remote_id="simkl-1", ids={})
    artwork = _record(
        provider="trakt",
        remote_id="trakt-1",
        poster_url="https://images.example/pressure.jpg",
        backdrop_url="https://images.example/pressure-bg.jpg",
    )

    forward = _combine_records([missing, artwork])
    reverse = _combine_records([artwork, missing])

    assert forward["poster_url"] == artwork["poster_url"]
    assert forward["backdrop_url"] == artwork["backdrop_url"]
    assert forward["ids"] == {"tmdb": "123"}
    assert reverse["poster_url"] == forward["poster_url"]
    assert reverse["backdrop_url"] == forward["backdrop_url"]


def test_combined_record_uses_stable_artwork_source_instead_of_newest_record():
    newer = _record(
        provider="simkl",
        remote_id="simkl-1",
        updated_at="2026-06-27T11:00:00Z",
        poster_url="https://simkl.example/pressure.jpg",
    )
    older = _record(
        provider="mdblist",
        remote_id="mdblist-1",
        updated_at="2026-06-27T09:00:00Z",
        poster_url="https://mdblist.example/pressure.jpg",
    )

    forward = _combine_records([newer, older])
    reverse = _combine_records([older, newer])

    assert forward["poster_url"] == older["poster_url"]
    assert reverse["poster_url"] == older["poster_url"]


def test_same_title_with_different_profile_progress_does_not_group():
    default = _record(instance_id="default", progress_percent=5.1)
    profile = _record(instance_id="P01", progress_percent=22.4)

    assert set(_record_group_keys(default)).isdisjoint(_record_group_keys(profile))


def test_same_title_with_matching_progress_can_still_combine_across_profiles():
    default = _record(instance_id="default", progress_percent=5.1)
    profile = _record(instance_id="P01", progress_percent=5.4)

    assert set(_record_group_keys(default)) & set(_record_group_keys(profile))


def test_unscoped_live_stream_only_updates_default_profile():
    items = [
        _record(instance_id="default"),
        _record(instance_id="P01", remote_id="102"),
    ]
    stream = {
        "source": "plex",
        "state": "playing",
        "media_type": "movie",
        "title": "Pressure",
        "year": 2026,
        "ids": {"tmdb": "123"},
        "progress": 12,
        "updated": 1_750_000_000,
    }

    _overlay_live_streams(items, [stream])

    assert items[0]["live_state"] == "playing"
    assert items[0]["live_instance_id"] == "default"
    assert "live_state" not in items[1]


def test_scoped_live_stream_only_updates_matching_profile():
    items = [
        _record(instance_id="default"),
        _record(instance_id="P01", remote_id="102"),
    ]
    stream = {
        "source": "plex",
        "provider_instance": "P01",
        "state": "paused",
        "media_type": "movie",
        "title": "Pressure",
        "year": 2026,
        "ids": {"tmdb": "123"},
        "progress": 18,
        "updated": 1_750_000_000,
    }

    _overlay_live_streams(items, [stream])

    assert "live_state" not in items[0]
    assert items[1]["live_state"] == "paused"
    assert items[1]["live_instance_id"] == "P01"


def test_empty_provider_profile_does_not_inherit_default_identity():
    cfg = {
        "plex": {
            "account_token": "default-owner-token",
            "instances": {"PLEX-P01": {}},
        }
    }

    assert _profile_has_explicit_identity(cfg, "plex", "default") is True
    assert _profile_has_explicit_identity(cfg, "plex", "PLEX-P01") is False
    plex_specs = [spec for spec in PlaybackProgressService().provider_instances(cfg) if spec["provider"] == "plex"]
    assert [spec["instance_id"] for spec in plex_specs] == ["default"]


def test_plex_home_profile_with_explicit_user_scope_can_inherit_connection():
    cfg = {
        "plex": {
            "account_token": "default-owner-token",
            "server_url": "https://plex.example",
            "instances": {
                "PLEX-P01": {"username": "Home User", "account_id": 42},
            },
        }
    }

    assert _profile_has_explicit_identity(cfg, "plex", "PLEX-P01") is True
    plex_specs = [spec for spec in PlaybackProgressService().provider_instances(cfg) if spec["provider"] == "plex"]
    assert [spec["instance_id"] for spec in plex_specs] == ["default", "PLEX-P01"]


def test_empty_oauth_profile_is_not_treated_as_default_account():
    cfg = {
        "trakt": {
            "access_token": "default-token",
            "client_id": "client-id",
            "instances": {
                "TRAKT-P01": {},
                "TRAKT-P02": {"access_token": "second-account-token"},
            },
        }
    }

    assert _profile_has_explicit_identity(cfg, "trakt", "TRAKT-P01") is False
    assert _profile_has_explicit_identity(cfg, "trakt", "TRAKT-P02") is True
    trakt_specs = [spec for spec in PlaybackProgressService().provider_instances(cfg) if spec["provider"] == "trakt"]
    assert [spec["instance_id"] for spec in trakt_specs] == ["default", "TRAKT-P02"]


def test_trakt_image_urls_are_absolute():
    path = "media.trakt.tv/images/movies/001/077/714/posters/thumb/9646f8cb88.jpg.webp"

    assert _trakt_image_url(path) == f"https://{path}"
    assert _trakt_image_url(f"//{path}") == f"https://{path}"
    assert _trakt_image_url(f"https://{path}") == f"https://{path}"
