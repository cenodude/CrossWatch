from __future__ import annotations

from pathlib import Path

from services.playback_progress.service import (
    _combine_records,
    _overlay_live_streams,
    _profile_has_explicit_identity,
    _record_group_keys,
    _share_artwork_metadata,
    PlaybackProgressService,
)
from services.playback_progress.models import PlaybackCapabilities
import services.playback_progress.service as playback_service
import services.playback_progress.adapters.nuvio as nuvio_playback_adapter
from services.playback_progress.adapters.media_servers import JellyfinPlaybackAdapter, KodiPlaybackAdapter
from services.playback_progress.adapters.trakt import _trakt_image_url


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
    shell = js[js.index("function shell()") : js.index('      <div class="pp-modal hidden" id="pp-progress-dialog"')]

    assert 'class="pp-bulk-summary"' in shell
    assert 'id="pp-selected-count" class="pp-selected-number">0</span>' in shell
    assert 'class="pp-selected-copy"><strong>selected</strong><span>Select items to manage</span>' in shell
    assert 'class="pp-btn pp-bulk-choice" id="pp-select-visible">${icon("visibility")}<span>Select Visible</span>' in shell
    assert 'class="pp-btn pp-bulk-choice" id="pp-select-all">${icon("format_list_bulleted")}<span>Select All Filtered Results</span>' in shell
    assert 'class="pp-btn pp-bulk-choice pp-bulk-clear" id="pp-clear-selection">${icon("cancel")}<span>Clear Selection</span>' in shell
    assert 'class="pp-selected-pill"' not in shell
    assert 'class="pp-bulk-divider"' not in shell
    assert "#${ROOT_ID} .pp-bulk{position:sticky;bottom:12px;z-index:20;display:grid;grid-template-columns:minmax(220px,1fr) auto minmax(170px,1fr)" in js
    assert "#${ROOT_ID} .pp-bulk-actions .pp-bulk-icon{width:48px;min-width:48px;height:48px;min-height:48px" in js
    assert "#${ROOT_ID} .pp-bulk-actions #pp-bulk-edit{background:linear-gradient(180deg,rgba(63,126,255,.56)" in js
    assert "#${ROOT_ID} .pp-bulk-actions #pp-bulk-watch{background:linear-gradient(180deg,rgba(64,166,105,.48)" in js
    assert "#${ROOT_ID} .pp-bulk-actions #pp-bulk-remove{background:linear-gradient(180deg,rgba(181,48,68,.58)" in js
    assert "#${ROOT_ID} .pp-card.selected:before,#${ROOT_ID} .pp-card.selected:after{content:none!important;display:none!important}" in js


def test_playback_progress_frontend_includes_kodi_provider_key():
    js = (ROOT / "assets" / "js" / "playback_progress.js").read_text(encoding="utf-8")

    assert 'const PLAYBACK_PROVIDER_KEYS = ["trakt", "simkl", "mdblist", "publicmetadb", "plex", "emby", "jellyfin", "nuvio", "kodi"];' in js


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


class _UnreadPlaybackAdapter:
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
    service.adapters["kodi"].ops = kodi_ops
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
