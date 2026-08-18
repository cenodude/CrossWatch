from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import cast

from providers.metadata._meta_TMDB import TmdbProvider

from services.playback_progress.service import (
    _combine_records,
    _overlay_live_streams,
    _progress_edit_max_exclusive,
    _profile_has_explicit_identity,
    _record_group_keys,
    _share_artwork_metadata,
    _user_filter_allows,
    _validated_progress_percent,
    PlaybackProgressService,
)
from services.playback_progress.models import PlaybackCapabilities
from services.playback_progress.adapters.base import PlaybackProgressAdapter, enrich_parallel
import services.playback_progress.adapters.base as base_adapter
import services.playback_progress.adapters.media_servers as media_servers_adapter
import services.playback_progress.service as playback_service
import services.playback_progress.adapters.floppy as floppy_playback_adapter
import services.playback_progress.adapters.mdblist as mdblist_playback_adapter
import services.playback_progress.adapters.nuvio as nuvio_playback_adapter
import services.playback_progress.adapters.stremio as stremio_playback_adapter
from services.playback_progress.adapters.crosswatch import CrossWatchPlaybackAdapter
from services.playback_progress.adapters.media_servers import JellyfinPlaybackAdapter, KodiPlaybackAdapter
from services.playback_progress.adapters.trakt import _trakt_image_url
from providers.sync.mdblist import _progress as mdblist_progress


ROOT = Path(__file__).resolve().parents[1]


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


def test_playback_bulk_footer_uses_wide_management_layout():
    js = (ROOT / "assets" / "js" / "playback_progress.js").read_text(encoding="utf-8")
    css = (ROOT / "assets" / "css" / "pages.css").read_text(encoding="utf-8")
    shell = js[js.index("function shell()") : js.index('      <div class="pp-modal hidden" id="pp-progress-dialog"')]

    assert 'class="pp-bulk-summary"' in shell
    assert 'id="pp-selected-count" class="pp-selected-number">0</span>' in shell
    assert 'class="pp-selected-copy"><strong>selected</strong><span>Select items to manage</span>' in shell
    assert 'class="pp-btn pp-bulk-choice" id="pp-select-visible">${icon("visibility")}<span>Select Visible</span>' in shell
    assert 'class="pp-btn pp-bulk-choice" id="pp-select-all">${icon("format_list_bulleted")}<span>Select All Filtered Results</span>' in shell
    assert 'class="pp-btn pp-bulk-choice pp-bulk-clear" id="pp-clear-selection">${icon("cancel")}<span>Clear Selection</span>' in shell
    assert 'class="pp-selected-pill"' not in shell
    assert 'class="pp-bulk-divider"' not in shell
    assert "#playback-progress-root .pp-bulk{position:sticky;bottom:12px;z-index:20;display:grid;grid-template-columns:minmax(220px,1fr)auto minmax(170px,1fr)" in css
    assert "#playback-progress-root .pp-bulk-actions .pp-bulk-icon{width:48px;min-width:48px;height:48px;min-height:48px" in css
    assert "#playback-progress-root .pp-bulk-actions #pp-bulk-edit{background:linear-gradient(180deg,rgba(63,126,255,0.56)" in css
    assert "#playback-progress-root .pp-bulk-actions #pp-bulk-watch{background:linear-gradient(180deg,rgba(64,166,105,0.48)" in css
    assert "#playback-progress-root .pp-bulk-actions #pp-bulk-remove{background:linear-gradient(180deg,rgba(181,48,68,0.58)" in css
    assert "#playback-progress-root .pp-card.selected:before,#playback-progress-root .pp-card.selected:after{content:none !important;display:none !important}" in css


def test_playback_progress_frontend_includes_kodi_provider_key():
    js = (ROOT / "assets" / "js" / "playback_progress.js").read_text(encoding="utf-8")

    keys = js.split("PLAYBACK_PROVIDER_KEYS")[1].split("]")[0]
    for provider in ("crosswatch", "trakt", "simkl", "mdblist", "publicmetadb", "plex", "emby", "jellyfin", "nuvio", "kodi", "stremio", "floppy"):
        assert f'"{provider}"' in keys, provider


def test_playback_progress_frontend_is_readonly_for_managed_users():
    js = (ROOT / "assets" / "js" / "playback_progress.js").read_text(encoding="utf-8")

    assert 'document.documentElement?.dataset?.cwRole === "user"' in js
    assert "if (isReadOnly()) return;" in js
    assert "user_profile" in js
    assert "window.CW?.OverviewProfile?.id" in js
    assert 'window.addEventListener("cw:overview-profile-changed"' in js
    assert "CARD.cacheScope" not in js


def test_playback_progress_provider_instances_respect_user_filter():
    service = PlaybackProgressService()
    cfg = {
        "plex": {
            "instances": {
                "PLEX-P01": {"server_url": "http://plex-a"},
                "PLEX-P02": {"server_url": "http://plex-b"},
            }
        },
        "trakt": {"access_token": "token"},
    }

    specs = service.provider_instances(cfg, user_filter={"PLEX": ["PLEX-P02"]})

    assert _user_filter_allows({"PLEX": ["PLEX-P02"]}, "plex", "PLEX-P02") is True
    assert _user_filter_allows({"PLEX": ["PLEX-P02"]}, "plex", "PLEX-P01") is False
    assert [(row["provider"], row["instance_id"]) for row in specs] == [("plex", "PLEX-P02")]


class _PolicyOps:
    def __init__(self, progress_caps):
        self._progress_caps = progress_caps

    def capabilities(self):
        return {"progress": self._progress_caps}


class _PolicyAdapter(PlaybackProgressAdapter):
    ops: _PolicyOps

    def __init__(self, progress_caps):
        self.ops = _PolicyOps(progress_caps)


def test_playback_progress_edit_validation_uses_progress_write_cutoff():
    adapter = _PolicyAdapter({"completion_policy": {"progress_write": {"mode": "auto_complete", "percent": 80}}})
    record = _record(duration_seconds=3600)
    max_exclusive = _progress_edit_max_exclusive(adapter, record)

    assert max_exclusive == 80
    assert _validated_progress_percent(79.5, max_exclusive=max_exclusive) == (79.5, "")
    progress, reason = _validated_progress_percent(80, max_exclusive=max_exclusive)
    assert progress is None
    assert "below 80 percent" in reason


def test_playback_progress_edit_validation_ignores_stop_only_cutoff():
    adapter = _PolicyAdapter({
        "completion_policy": {
            "progress_write": {"mode": "none"},
            "stop_scrobble": {"marks_watched_percent": 80, "comparison": "gte"},
        }
    })

    max_exclusive = _progress_edit_max_exclusive(adapter, _record(duration_seconds=3600))

    assert max_exclusive == 100
    assert _validated_progress_percent(95, max_exclusive=max_exclusive) == (95, "")


def test_playback_progress_edit_validation_honors_duration_floor():
    adapter = _PolicyAdapter({
        "completion_policy": {
            "progress_write": {
                "mode": "auto_complete",
                "percent": 90,
                "min_duration_seconds": 60,
            }
        }
    })

    assert _progress_edit_max_exclusive(adapter, _record(duration_seconds=3600)) == 90
    assert _progress_edit_max_exclusive(adapter, _record(duration_seconds=30)) == 100


class _NoMetadata:
    def fetch(self, **_kwargs):
        return {}


def _mdblist_caps() -> PlaybackCapabilities:
    return PlaybackCapabilities(
        provider="mdblist",
        provider_label="MDBList",
        instance_id="default",
        instance_label="MDBList Default",
        configured=True,
        read=True,
        remove_progress=True,
        mark_watched=True,
        update_progress=True,
    )


def test_mdblist_playback_adapter_parses_official_id_keys_and_uses_documented_movie_payload():
    row = {
        "id": 10467596,
        "progress": 21,
        "paused_at": "2026-08-17T19:51:05Z",
        "type": "movie",
        "movie": {"title": "Evil Dead Burn", "year": 2026, "ids": {"imdbid": "tt1234567", "tmdbid": 550, "traktid": 1}},
    }

    record = mdblist_playback_adapter.MDBListPlaybackAdapter()._normalize(row, "default", "MDBList Default", _mdblist_caps(), _NoMetadata())
    assert record is not None

    body, reason = mdblist_playback_adapter._progress_body_from_record(record.to_dict(), 21)

    assert record.ids == {"imdb": "tt1234567", "tmdb": 550, "trakt": 1}
    assert record.can_update_progress is True
    assert reason is None
    assert body == {"movie": {"ids": {"imdb": "tt1234567"}}, "progress": 21, "app_version": mdblist_progress._app_version()}


def test_mdblist_playback_adapter_uses_documented_nested_episode_payload():
    row = {
        "id": 10467597,
        "progress": 26,
        "paused_at": "2026-08-17T19:51:05Z",
        "type": "episode",
        "episode": {"season": 6, "number": 2, "title": "Exile", "ids": {"imdbid": "tt7654321", "tmdbid": 5978363}},
        "show": {"title": "The Handmaid's Tale", "year": 2017, "ids": {"imdbid": "tt5834204", "tmdbid": 69478, "tvdbid": 321239}},
    }

    record = mdblist_playback_adapter.MDBListPlaybackAdapter()._normalize(row, "default", "MDBList Default", _mdblist_caps(), _NoMetadata())
    assert record is not None

    body, reason = mdblist_playback_adapter._progress_body_from_record(record.to_dict(), 26)

    assert record.provider_metadata["show_ids"] == {"imdb": "tt5834204", "tmdb": "69478", "tvdb": "321239"}
    assert record.can_update_progress is True
    assert reason is None
    assert body == {
        "show": {"ids": {"imdb": "tt5834204"}, "season": {"number": 6, "episode": {"number": 2}}},
        "progress": 26,
        "app_version": mdblist_progress._app_version(),
    }


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


def test_same_episode_profiles_share_only_artwork_metadata():
    donor = _record(
        instance_id="default",
        remote_id="default-1",
        canonical_key="tmdb:69478#s06e02",
        media_type="episode",
        title="The Handmaid's Tale",
        series_title="The Handmaid's Tale",
        episode_title="Exile",
        season=6,
        episode=2,
        ids={"tmdb": "5978363"},
        provider_metadata={"show_ids": {"tmdb": "69478"}},
        progress_percent=21.0,
        poster_url="https://images.example/handmaid-poster.jpg",
        backdrop_url="https://images.example/handmaid-bg.jpg",
    )
    profile = _record(
        provider="simkl",
        instance_id="SIMKL-P01",
        remote_id="profile-1",
        canonical_key="tvdb:10958852#s06e02",
        media_type="episode",
        title="The Handmaid's Tale",
        series_title="The Handmaid's Tale",
        episode_title="Exile",
        season=6,
        episode=2,
        ids={"tvdb": "10958852"},
        provider_metadata={},
        progress_percent=33.0,
    )

    _share_artwork_metadata([donor, profile])

    assert set(_record_group_keys(donor)).isdisjoint(_record_group_keys(profile))
    assert profile["progress_percent"] == 33.0
    assert profile["remote_id"] == "profile-1"
    assert profile["provider_metadata"]["show_ids"] == {"tmdb": "69478"}
    assert profile["ids"]["tmdb_show"] == "69478"
    assert profile["poster_url"] == donor["poster_url"]
    assert profile["backdrop_url"] == donor["backdrop_url"]


def test_media_server_episode_metadata_resolution_passes_show_year():
    from services.playback_progress.adapters.media_servers import PlexPlaybackAdapter

    class FakeMetadata:
        def __init__(self):
            self.calls = []

        def fetch(self, *, entity, ids, locale=None, need=None):
            self.calls.append({"entity": entity, "ids": dict(ids)})
            return {"ids": {"tmdb": "210787", "tvdb": "421123"}}

    adapter = PlexPlaybackAdapter()
    metadata = FakeMetadata()
    caps = PlaybackCapabilities("plex", "Plex", "P01", "P01", True, True, True, True, True, True, True, True, True, True, False)
    record = adapter._normalize(
        "plex:25515#s01e02",
        {
            "type": "episode",
            "title": "Pak me als je kan",
            "series_title": "Shelter",
            "year": 2023,
            "season": 1,
            "episode": 2,
            "ids": {"tmdb": "4524138"},
            "show_ids": {"plex": "25515"},
            "progress_ms": 1119000,
            "duration_ms": 2675904,
        },
        cast(TmdbProvider, metadata),
        "P01",
        "Plex P01",
        caps,
    )

    assert metadata.calls[0] == {"entity": "tv", "ids": {"plex": "25515", "title": "Shelter", "year": "2023"}}
    assert record is not None
    assert record.provider_metadata["show_ids"]["tmdb"] == "210787"


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


def test_playback_progress_save_settings_stores_selection_timeout_and_ignores_unknown(monkeypatch):
    cfg = {
        "trakt": {
            "access_token": "default-token",
            "client_id": "client-id",
            "instances": {"TRAKT-P01": {"access_token": "second-token"}},
        }
    }
    saved = {}

    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)
    monkeypatch.setattr(playback_service, "save_config", lambda next_cfg: saved.setdefault("cfg", next_cfg))

    result = PlaybackProgressService().save_settings(
        {
            "provider_timeout_seconds": 19,
            "profiles": [
                {"provider": "trakt", "instance_id": "default", "included": True},
                {"provider": "trakt", "instance_id": "TRAKT-P01", "included": False},
                {"provider": "notaprovider", "instance_id": "default", "included": False},
            ],
        }
    )

    block = saved["cfg"]["playback_progress"]
    assert result["ok"] is True
    assert block["disabled_profiles"] == ["trakt:TRAKT-P01"]
    assert block["provider_timeout_seconds"] == 19.0


def test_playback_progress_settings_reports_disabled_profiles_and_clamped_timeout(monkeypatch):
    cfg = {
        "trakt": {
            "access_token": "default-token",
            "client_id": "client-id",
            "instances": {"TRAKT-P01": {"access_token": "second-token"}},
        },
        "playback_progress": {
            "disabled_profiles": ["trakt:TRAKT-P01"],
            "provider_timeout_seconds": 999,
        },
    }

    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)

    data = PlaybackProgressService().settings()
    by_key = {row["key"]: row for row in data["profiles"]}

    assert data["provider_timeout_seconds"] == 60.0
    assert by_key["trakt:default"]["included"] is True
    assert by_key["trakt:TRAKT-P01"]["included"] is False


class _FakeCrossWatchOps:
    def __init__(self) -> None:
        self.index_roots = []
        self.added = []
        self.removed = []

    def _root(self, cfg):
        return str((cfg.get("crosswatch") or {}).get("root_dir") or "")

    def capabilities(self):
        return {
            "progress": {
                "types": {"movies": True, "episodes": True},
                "upsert": True,
                "remove": True,
            },
            "history": {"upsert": True},
        }

    def is_configured(self, cfg):
        block = cfg.get("crosswatch") or {}
        value = block.get("enabled")
        if isinstance(value, bool):
            return value
        return str(value or "").strip().lower() not in {"0", "false", "no", "off", "disabled"}

    def build_index(self, cfg, *, feature):
        assert feature == "progress"
        self.index_roots.append(self._root(cfg))
        return {
            "tmdb:123": {
                "type": "movie",
                "title": "Pressure",
                "year": 2026,
                "ids": {"tmdb": "123"},
                "progress_ms": 120000,
                "duration_ms": 600000,
                "progress_at": "2026-07-30T20:00:00Z",
            }
        }

    def remove(self, cfg, items, *, feature, dry_run=False):
        self.removed.append({"feature": feature, "root": self._root(cfg), "items": list(items)})
        return {"ok": True, "count": len(items), "feature": feature}

    def add(self, cfg, items, *, feature, dry_run=False):
        self.added.append({"feature": feature, "root": self._root(cfg), "items": list(items)})
        return {"ok": True, "count": len(items), "feature": feature}


def test_playback_progress_uses_crosswatch_profiles(monkeypatch, tmp_path):
    cw_ops = _FakeCrossWatchOps()
    root = tmp_path / "cw_provider"
    cfg = {
        "crosswatch": {
            "root_dir": str(root),
            "instances": {
                "CW-P01": {"label": "Desk"},
                "CW-P02": {"enabled": False, "label": "Old"},
            },
        }
    }

    monkeypatch.setattr(CrossWatchPlaybackAdapter, "ops", cw_ops)
    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)

    service = PlaybackProgressService()
    providers = [spec for spec in service.provider_instances(cfg) if spec["provider"] == "crosswatch"]
    caps_by_instance = {
        cap.instance_id: cap
        for cap in service.capabilities(cfg)
        if cap.provider == "crosswatch"
    }

    assert providers == [
        {"provider": "crosswatch", "instance_id": "default", "instance_label": "CrossWatch Default"},
        {"provider": "crosswatch", "instance_id": "CW-P01", "instance_label": "CrossWatch Desk"},
        {"provider": "crosswatch", "instance_id": "CW-P02", "instance_label": "CrossWatch Old"},
    ]
    assert caps_by_instance["default"].read is True
    assert caps_by_instance["CW-P01"].read is True
    assert caps_by_instance["CW-P02"].configured is False
    assert caps_by_instance["CW-P02"].read is False

    result = service.items(provider="crosswatch", instance_id="CW-P01", force_refresh=True)
    record = result["items"][0]

    assert result["errors"] == []
    expected_profile_root = str(root / "profiles" / "CW-P01").replace("\\", "/")

    assert [value.replace("\\", "/") for value in cw_ops.index_roots] == [expected_profile_root]
    assert record["provider"] == "crosswatch"
    assert record["provider_label"] == "CrossWatch"
    assert record["instance_id"] == "CW-P01"
    assert record["instance_label"] == "CrossWatch Desk"
    assert record["progress_percent"] == 20.0
    assert record["can_remove_progress"] is True
    assert record["can_mark_watched"] is True
    assert record["can_update_progress"] is True

    service.remove({"provider": "crosswatch", "instance_id": "CW-P01", "record": record})
    service.mark_watched({"provider": "crosswatch", "instance_id": "CW-P01", "record": record})
    service.update_progress({"provider": "crosswatch", "instance_id": "CW-P01", "record": record, "progress_percent": 25})

    assert [call["feature"] for call in cw_ops.removed] == ["progress", "progress", "progress"]
    assert [call["feature"] for call in cw_ops.added] == ["history", "progress"]
    assert all(call["root"].replace("\\", "/") == expected_profile_root for call in cw_ops.removed + cw_ops.added)
    assert cw_ops.added[0]["items"][0]["watched_at"]
    assert cw_ops.added[1]["items"][0]["progress_ms"] == 150000


def test_playback_progress_settings_use_short_crosswatch_profile_ids() -> None:
    js = (ROOT / "assets" / "js" / "playback_progress.js").read_text(encoding="utf-8")

    assert 'String(p.provider || "").toLowerCase() === "crosswatch"' in js
    assert "/^CW-P\\d+$/i.test(id)" in js
    assert "return id.toUpperCase()" in js


class _UnreadPlaybackAdapter(PlaybackProgressAdapter):
    provider = "trakt"
    provider_label = "Trakt"

    def __init__(self, *, configured=True):
        self.configured = configured

    def capabilities(self, config_view, *, instance_id, instance_label):
        return PlaybackCapabilities(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            configured=self.configured,
            read=False,
            reason="Trakt playback support is unavailable in this installation." if self.configured else "Trakt is not connected for this instance.",
        )


def test_playback_progress_items_reports_included_unreadable_provider(monkeypatch):
    cfg = {"trakt": {"access_token": "default-token", "client_id": "client-id"}}

    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)

    service = PlaybackProgressService()
    service.adapters["trakt"] = _UnreadPlaybackAdapter()
    result = service.items(provider="trakt", force_refresh=True)

    assert result["items"] == []
    assert result["errors"] == [
        {
            "ok": False,
            "provider": "trakt",
            "provider_label": "Trakt",
            "instance_id": "default",
            "instance_label": "Trakt Default",
            "operation": "list_progress",
            "error_code": "provider_unavailable",
            "message": "Trakt playback support is unavailable in this installation.",
            "retryable": False,
            "remote_status": None,
        }
    ]


def test_playback_progress_items_ignores_included_unconfigured_provider(monkeypatch):
    cfg = {"trakt": {"access_token": "default-token", "client_id": "client-id"}}

    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)

    service = PlaybackProgressService()
    service.adapters["trakt"] = _UnreadPlaybackAdapter(configured=False)
    result = service.items(provider="trakt", force_refresh=True)

    assert result["items"] == []
    assert result["errors"] == []


class _DownJellyfinOps:
    def is_configured(self, cfg):
        return True

    def progress_write_capability(self, cfg):
        return True, "", "10.10.0"

    def health(self, cfg):
        return {
            "ok": False,
            "status": "down",
            "details": {"reason": "server_unreachable"},
            "api": {"user": {"status": None}},
        }

    def build_index(self, cfg, *, feature):
        raise AssertionError("server-down health should stop before build_index")


def test_media_server_playback_reports_configured_server_down():
    adapter = JellyfinPlaybackAdapter()
    adapter.ops = _DownJellyfinOps()

    result = adapter.list_progress(
        {"jellyfin": {"server": "http://jellyfin.local", "access_token": "token", "user_id": "u1"}},
        instance_id="default",
        instance_label="Jellyfin Default",
        force_refresh=True,
    )

    assert result.ok is False
    assert result.provider == "jellyfin"
    assert result.error_code == "provider_unavailable"
    assert result.message == "Jellyfin server is not reachable."
    assert result.retryable is True


class _FakeKodiOps:
    def __init__(self) -> None:
        self.added = []
        self.removed = []

    def is_configured(self, cfg):
        return bool(cfg.get("kodi", {}).get("server"))

    def health(self, cfg):
        return {"ok": True, "status": "ok"}

    def build_index(self, cfg, *, feature):
        assert feature == "progress"
        return {
            "tmdb:1468683": {
                "type": "movie",
                "title": "Heartstopper Forever",
                "year": 2026,
                "ids": {"tmdb": "1468683"},
                "_kodi_id": 134,
                "_kodi_type": "movie",
                "progress_ms": 120000,
                "duration_ms": 6000000,
            }
        }

    def remove(self, cfg, items, *, feature, dry_run=False):
        self.removed.extend(items)
        return {"ok": True, "count": len(items), "feature": feature, "unresolved": []}

    def add(self, cfg, items, *, feature, dry_run=False):
        self.added.extend(items)
        return {"ok": True, "count": len(items), "feature": feature, "unresolved": []}


def test_playback_progress_includes_kodi_provider(monkeypatch):
    kodi_ops = _FakeKodiOps()
    cfg = {"kodi": {"server": "http://kodi.local:8080", "connection_verified": True}}

    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)

    service = PlaybackProgressService()
    cast(KodiPlaybackAdapter, service.adapters["kodi"]).ops = kodi_ops
    providers = service.provider_instances(cfg)
    result = service.items(provider="kodi", force_refresh=True)
    record = result["items"][0]

    assert {"provider": "kodi", "instance_id": "default", "instance_label": "Kodi Default"} in providers
    assert result["errors"] == []
    assert record["provider"] == "kodi"
    assert record["provider_label"] == "Kodi"
    assert record["remote_id"] == "134"
    assert record["progress_percent"] == 2.0

    service.remove({"provider": "kodi", "instance_id": "default", "record": record})
    service.update_progress({"provider": "kodi", "instance_id": "default", "record": record, "progress_percent": 25})

    assert kodi_ops.removed[0]["_kodi_id"] == 134
    assert kodi_ops.removed[0]["_kodi_type"] == "movie"
    assert kodi_ops.added[0]["_kodi_id"] == 134
    assert kodi_ops.added[0]["progress_ms"] == 1500000


def test_kodi_playback_adapter_reports_connected_capabilities():
    adapter = KodiPlaybackAdapter()
    adapter.ops = _FakeKodiOps()

    caps = adapter.capabilities(
        {"kodi": {"server": "http://kodi.local:8080", "connection_verified": True}},
        instance_id="default",
        instance_label="Kodi Default",
    )

    assert caps.provider == "kodi"
    assert caps.provider_label == "Kodi"
    assert caps.read is True
    assert caps.update_progress is True


class _FakeNuvioOps:
    def __init__(self) -> None:
        self.removed = []
        self.index_profile_ids = []
        self.remove_profile_ids = []
        self.add_profile_ids = []

    def capabilities(self):
        return {
            "progress": {
                "types": {"movies": True, "episodes": True},
                "upsert": True,
                "remove": True,
            },
            "history": {"upsert": True},
        }

    def is_configured(self, cfg):
        return bool(cfg.get("nuvio", {}).get("profile_id"))

    def build_index(self, cfg, *, feature):
        assert feature == "progress"
        self.index_profile_ids.append(cfg.get("nuvio", {}).get("profile_id"))
        return {
            "tmdb:123": {
                "type": "movie",
                "title": "Pressure",
                "year": 2026,
                "ids": {"tmdb": "123"},
                "progress_ms": 120000,
                "duration_ms": 600000,
                "progress_at": 1785501708,
            }
        }

    def remove(self, cfg, items, *, feature, dry_run=False):
        self.removed.extend(items)
        self.remove_profile_ids.append(cfg.get("nuvio", {}).get("profile_id"))
        return {"ok": True, "count": len(items), "feature": feature}

    def add(self, cfg, items, *, feature, dry_run=False):
        self.add_profile_ids.append(cfg.get("nuvio", {}).get("profile_id"))
        return {"ok": True, "count": len(items), "feature": feature}


def test_playback_progress_uses_explicit_nuvio_adapter(monkeypatch):
    nuvio_ops = _FakeNuvioOps()

    monkeypatch.setattr(nuvio_playback_adapter, "NUVIO_OPS", nuvio_ops)
    monkeypatch.setattr(playback_service, "load_config", lambda: {"nuvio": {"access_token": "token", "profile_id": 2}})

    service = PlaybackProgressService()
    providers = service.provider_instances({"nuvio": {"access_token": "token", "profile_id": 2}})

    assert any(spec["provider"] == "nuvio" for spec in providers)

    result = service.items(provider="nuvio", force_refresh=True)

    assert result["errors"] == []
    assert len(result["items"]) == 1
    assert result["items"][0]["provider"] == "nuvio"
    assert result["items"][0]["progress_percent"] == 20.0
    assert nuvio_ops.index_profile_ids == [2]


def test_playback_progress_uses_selected_nuvio_profile(monkeypatch):
    nuvio_ops = _FakeNuvioOps()
    cfg = {
        "nuvio": {
            "base_url": "https://api.nuvio.tv",
            "access_token": "default-token",
            "refresh_token": "default-refresh",
            "profile_id": 1,
            "profile_name": "Main",
            "instances": {
                "kid": {
                    "profile_id": 2,
                    "profile_name": "Kids",
                }
            },
        }
    }

    monkeypatch.setattr(nuvio_playback_adapter, "NUVIO_OPS", nuvio_ops)
    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)

    service = PlaybackProgressService()
    providers = [spec for spec in service.provider_instances(cfg) if spec["provider"] == "nuvio"]
    result = service.items(provider="nuvio", instance_id="kid", force_refresh=True)
    record = result["items"][0]

    assert providers == [
        {"provider": "nuvio", "instance_id": "default", "instance_label": "Nuvio Main"},
        {"provider": "nuvio", "instance_id": "kid", "instance_label": "Nuvio Kids"},
    ]
    assert result["errors"] == []
    assert record["instance_id"] == "kid"
    assert record["instance_label"] == "Nuvio Kids"
    assert nuvio_ops.index_profile_ids == [2]

    service.remove({"provider": "nuvio", "instance_id": "kid", "record": record})
    service.mark_watched({"provider": "nuvio", "instance_id": "kid", "record": record})
    service.update_progress({"provider": "nuvio", "instance_id": "kid", "record": record, "progress_percent": 25})

    assert nuvio_ops.remove_profile_ids == [2, 2]
    assert nuvio_ops.add_profile_ids == [2, 2]


def test_nuvio_playback_progress_enriches_missing_episode_metadata_from_content_id(monkeypatch):
    class FakeMetadata:
        def fetch(self, *, entity, ids, locale=None, need=None):
            assert entity == "tv"
            assert ids == {"tmdb": "69478"}
            return {
                "title": "The Boys",
                "year": 2019,
                "images": {
                    "poster": [{"url": "https://image.tmdb.org/t/p/w342/poster.jpg"}],
                    "backdrop": [{"url": "https://image.tmdb.org/t/p/w780/backdrop.jpg"}],
                },
            }

    caps = nuvio_playback_adapter.NuvioPlaybackAdapter().capabilities(
        {"nuvio": {"profile_id": 1}},
        instance_id="default",
        instance_label="Default",
    )
    item = {
        "type": "episode",
        "ids": {"tmdb": "5978363"},
        "show_ids": {"tmdb": "69478"},
        "series_title": "tmdb:69478",
        "season": 6,
        "episode": 2,
        "progress_ms": 703000,
        "duration_ms": 3307930,
        "progress_at": 1784848068000,
    }

    record = nuvio_playback_adapter.NuvioPlaybackAdapter()._record(
        "tmdb:69478#s06e02",
        item,
        "default",
        "Default",
        caps,
        FakeMetadata(),
    )

    assert record is not None
    assert record.title == "The Boys"
    assert record.series_title == "The Boys"
    assert record.year == 2019
    assert record.poster_url == "https://image.tmdb.org/t/p/w342/poster.jpg"
    assert record.backdrop_url == "https://image.tmdb.org/t/p/w780/backdrop.jpg"


class _FakeStremioOps:
    def __init__(self) -> None:
        self.added = []
        self.removed = []

    def capabilities(self):
        return {
            "progress": {
                "types": {"movies": True, "episodes": True},
                "upsert": True,
                "remove": True,
            },
            "history": {"upsert": True},
        }

    def is_configured(self, cfg):
        return bool(cfg.get("stremio", {}).get("auth_key"))

    def build_index(self, cfg, *, feature):
        assert feature == "progress"
        return {
            "imdb:tt0903747#s01e02": {
                "type": "episode",
                "ids": {"imdb": "tt0903747"},
                "show_ids": {"imdb": "tt0903747"},
                "series_title": "Breaking Bad",
                "season": 1,
                "episode": 2,
                "_stremio_id": "tt0903747",
                "_stremio_video_id": "tt0903747:1:2",
                "progress_ms": 120000,
                "duration_ms": 600000,
                "progress_at": "2026-07-30T20:00:00Z",
                "poster": "https://image.example/breaking-bad.jpg",
            }
        }

    def remove(self, cfg, items, *, feature, dry_run=False):
        self.removed.extend(items)
        return {"ok": True, "count": len(items), "feature": feature}

    def add(self, cfg, items, *, feature, dry_run=False):
        self.added.append((feature, list(items)))
        return {"ok": True, "count": len(items), "feature": feature}


def test_playback_progress_uses_stremio_adapter(monkeypatch):
    stremio_ops = _FakeStremioOps()
    cfg = {"stremio": {"auth_key": "secret"}}

    monkeypatch.setattr(stremio_playback_adapter, "STREMIO_OPS", stremio_ops)
    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)

    service = PlaybackProgressService()
    providers = service.provider_instances(cfg)
    result = service.items(provider="stremio", force_refresh=True)
    record = result["items"][0]

    assert {"provider": "stremio", "instance_id": "default", "instance_label": "Stremio Default"} in providers
    assert result["errors"] == []
    assert record["provider"] == "stremio"
    assert record["provider_label"] == "Stremio"
    assert record["remote_id"] == "tt0903747:1:2"
    assert record["progress_percent"] == 20.0
    assert record["poster_url"] == "https://image.example/breaking-bad.jpg"
    assert record["provider_metadata"]["stremio_profile_id"] == "default"
    assert ("stremio", "default", "default") in service._cache

    service.remove({"provider": "stremio", "instance_id": "default", "record": record})
    service.mark_watched({"provider": "stremio", "instance_id": "default", "record": record})
    service.update_progress({"provider": "stremio", "instance_id": "default", "record": record, "progress_percent": 25})

    assert stremio_ops.removed[0]["_stremio_video_id"] == "tt0903747:1:2"
    assert stremio_ops.added[0][0] == "history"
    assert stremio_ops.added[1][0] == "progress"
    assert stremio_ops.added[1][1][0]["progress_ms"] == 150000


def test_stremio_playback_progress_update_allows_percent_only(monkeypatch):
    stremio_ops = _FakeStremioOps()
    monkeypatch.setattr(stremio_playback_adapter, "STREMIO_OPS", stremio_ops)
    record = {
        "media_type": "episode",
        "title": "Love & Death",
        "series_title": "Love & Death",
        "season": 1,
        "episode": 7,
        "ids": {"tmdb": "124800"},
        "canonical_key": "tmdb:124800#s01e07",
        "provider_metadata": {"show_ids": {"tmdb": "124800"}},
    }

    result = stremio_playback_adapter.StremioPlaybackAdapter().update_progress({"stremio": {"auth_key": "secret"}}, record, 10.63, instance_id="default", instance_label="Stremio Default")

    assert result.ok is True
    assert stremio_ops.added[0][0] == "progress"
    assert stremio_ops.added[0][1][0]["progress_percent"] == 10.63
    assert "progress_ms" not in stremio_ops.added[0][1][0]


class _FakeFloppyOps:
    def __init__(self):
        self.removed = []
        self.added = []

    def capabilities(self):
        return {
            "progress": {"read": True, "upsert": True, "remove": True, "types": {"movies": True, "episodes": True}},
            "history": {"upsert": True},
        }

    def is_configured(self, cfg):
        return bool(cfg.get("floppy", {}).get("server_url") and cfg.get("floppy", {}).get("api_token"))

    def build_index(self, cfg, *, feature):
        assert feature == "progress"
        return {
            "tmdb:22#s01e02": {
                "type": "episode",
                "show_ids": {"tmdb": "22"},
                "series_title": "Show",
                "title": "Episode",
                "season": 1,
                "episode": 2,
                "progress_ms": 120000,
                "duration_ms": 600000,
                "progress_at": "2026-08-01T12:00:00Z",
            }
        }

    def remove(self, cfg, items, *, feature, dry_run=False):
        self.removed.extend(items)
        return {"ok": True, "count": len(items), "feature": feature}

    def add(self, cfg, items, *, feature, dry_run=False):
        self.added.append((feature, list(items)))
        return {"ok": True, "count": len(items), "feature": feature}


def test_playback_progress_uses_floppy_adapter(monkeypatch):
    floppy_ops = _FakeFloppyOps()
    cfg = {"floppy": {"server_url": "http://floppy.local", "api_token": "secret"}}

    monkeypatch.setattr(floppy_playback_adapter, "FLOPPY_OPS", floppy_ops)
    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)

    service = PlaybackProgressService()
    providers = service.provider_instances(cfg)
    result = service.items(provider="floppy", force_refresh=True)
    record = result["items"][0]

    assert {"provider": "floppy", "instance_id": "default", "instance_label": "Floppy Default"} in providers
    assert result["errors"] == []
    assert record["provider"] == "floppy"
    assert record["progress_percent"] == 20.0

    service.remove({"provider": "floppy", "instance_id": "default", "record": record})
    service.mark_watched({"provider": "floppy", "instance_id": "default", "record": record})
    service.update_progress({"provider": "floppy", "instance_id": "default", "record": record, "progress_percent": 25})

    assert floppy_ops.removed[0]["show_ids"] == {"tmdb": "22"}
    assert floppy_ops.added[0][0] == "history"
    assert floppy_ops.added[1][0] == "progress"
    assert floppy_ops.added[1][1][0]["progress_ms"] == 150000


def test_playback_progress_lists_floppy_position_without_duration(monkeypatch):
    class PositionOnlyFloppyOps(_FakeFloppyOps):
        def build_index(self, cfg, *, feature):
            assert feature == "progress"
            return {
                "tmdb:223300#s01e03": {
                    "type": "episode",
                    "show_ids": {"tmdb": "223300"},
                    "series_title": "The Abandons",
                    "title": "Triage",
                    "season": 1,
                    "episode": 3,
                    "progress_ms": 749000,
                    "progress_at": "2026-08-01T18:48:14Z",
                }
            }

    cfg = {"floppy": {"server_url": "http://floppy.local", "api_token": "secret"}}

    monkeypatch.setattr(floppy_playback_adapter, "FLOPPY_OPS", PositionOnlyFloppyOps())
    monkeypatch.setattr(playback_service, "load_config", lambda: cfg)

    result = PlaybackProgressService().items(provider="floppy", force_refresh=True)
    record = result["items"][0]

    assert result["errors"] == []
    assert record["provider"] == "floppy"
    assert record["title"] == "The Abandons"
    assert record["progress_percent"] is None
    assert record["duration_seconds"] is None
    assert record["can_update_progress"] is False


def test_nuvio_playback_progress_enriches_episode_metadata_from_tvdb_id(monkeypatch):
    class FakeMetadata:
        def fetch(self, *, entity, ids, locale=None, need=None):
            assert entity == "tv"
            assert ids == {"tvdb": "355567"}
            return {"title": "The Boys", "year": 2019}

    caps = nuvio_playback_adapter.NuvioPlaybackAdapter().capabilities(
        {"nuvio": {"profile_id": 1}},
        instance_id="default",
        instance_label="Default",
    )
    item = {
        "type": "episode",
        "ids": {},
        "show_ids": {"tvdb": "355567"},
        "series_title": "Untitled",
        "season": 6,
        "episode": 2,
        "progress_ms": 703000,
        "duration_ms": 3307930,
        "progress_at": 1784848068000,
    }

    record = nuvio_playback_adapter.NuvioPlaybackAdapter()._record(
        "tvdb:355567#s06e02",
        item,
        "default",
        "Default",
        caps,
        FakeMetadata(),
    )

    assert record is not None
    assert record.title == "The Boys"
    assert record.series_title == "The Boys"


class _CountingTmdb:
    def __init__(self, *, resolves=True):
        self.resolves = resolves
        self.calls = []

    def fetch(self, *, entity, ids, need=None):
        self.calls.append(dict(ids))
        if not self.resolves:
            return {}
        return {"vote_average": 8.0, "ids": {"tmdb": "999"}}


def _progress_rows(count, *, with_ids):
    rows = {}
    for i in range(count):
        ids = {"plex": str(9000 + i)}
        if with_ids:
            ids["imdb"] = f"tt{i:07d}"
        rows[f"movie:{i}"] = {
            "type": "movie",
            "title": f"Movie {i}",
            "year": 2026,
            "ids": ids,
            "progress_ms": 60000,
            "duration_ms": 600000,
        }
    return rows


def _plex_adapter(rows):
    class _Ops:
        def is_configured(self, cfg):
            return True

        def health(self, cfg):
            return {"ok": True, "status": "ok"}

        def build_index(self, cfg, feature=None):
            return rows

    adapter = media_servers_adapter.PlexPlaybackAdapter()
    adapter.ops = _Ops()
    return adapter


def test_playback_progress_does_not_search_twice_for_unresolvable_rows(monkeypatch):
    tmdb = _CountingTmdb(resolves=False)
    monkeypatch.setattr(media_servers_adapter, "tmdb_metadata_provider", lambda cfg: tmdb)

    result = _plex_adapter(_progress_rows(5, with_ids=False)).list_progress(
        {"tmdb": {"api_key": "key"}}, instance_id="default", instance_label="Default"
    )

    assert result.ok is True
    assert len(result.items) == 5
    assert all(record.rating is None for record in result.items)
    assert len(tmdb.calls) == 5


def test_playback_progress_keeps_ratings_for_rows_resolved_by_search(monkeypatch):
    tmdb = _CountingTmdb()
    monkeypatch.setattr(media_servers_adapter, "tmdb_metadata_provider", lambda cfg: tmdb)

    result = _plex_adapter(_progress_rows(5, with_ids=False)).list_progress(
        {"tmdb": {"api_key": "key"}}, instance_id="default", instance_label="Default"
    )

    assert result.ok is True
    assert all(record.rating == 8.0 for record in result.items)


def test_playback_progress_skips_resolve_when_external_ids_exist(monkeypatch):
    tmdb = _CountingTmdb()
    monkeypatch.setattr(media_servers_adapter, "tmdb_metadata_provider", lambda cfg: tmdb)

    result = _plex_adapter(_progress_rows(5, with_ids=True)).list_progress(
        {"tmdb": {"api_key": "key"}}, instance_id="default", instance_label="Default"
    )

    assert result.ok is True
    assert all(record.rating == 8.0 for record in result.items)
    assert len(tmdb.calls) == 5
    assert all(call.get("imdb") for call in tmdb.calls)


def test_enrich_parallel_preserves_order_and_runs_concurrently():
    barrier = threading.Barrier(4, timeout=10)

    def worker(value):
        barrier.wait()
        return value * 2

    assert enrich_parallel(list(range(4)), worker) == [0, 2, 4, 6]


def test_enrich_parallel_handles_small_inputs():
    assert enrich_parallel([], lambda value: value) == []
    assert enrich_parallel([7], lambda value: value * 3) == [21]


def test_tmdb_metadata_provider_is_reused_across_refreshes():
    cfg = {"tmdb": {"api_key": "shared-key"}, "metadata": {"ttl_hours": 720}}

    first = base_adapter.tmdb_metadata_provider(cfg)
    assert first is not None
    first._cache["probe"] = (time.time(), {"cached": True})
    second = base_adapter.tmdb_metadata_provider(cfg)
    assert second is not None

    assert first is second
    assert "probe" in second._cache
    assert base_adapter.tmdb_metadata_provider({}) is None


def test_tmdb_cache_is_bounded(monkeypatch):
    provider = TmdbProvider(lambda: {"metadata": {"ttl_hours": 1}}, lambda cfg: None)
    monkeypatch.setattr(TmdbProvider, "CACHE_MAX_ENTRIES", 10)
    now = time.time()
    provider._cache["expired"] = (now - 7200, "old")
    for i in range(12):
        provider._cache[f"fresh{i}"] = (now + i, i)

    provider._prune_cache()

    assert "expired" not in provider._cache
    assert len(provider._cache) <= 10
    assert "fresh11" in provider._cache
