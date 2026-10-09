# tests/test_media_scope.py
# CrossWatch - Media view sync scope regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from copy import deepcopy
import json
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from services.media_scope import MediaScope
from services import dashboard_widgets as dw


def pair(target="MYANIMELIST", instance="default", **options):
    return {"enabled": True, "source": "SIMKL", "target": target, "source_instance": instance,
            "features": {feature: {"enable": True, **options} for feature in ("history", "ratings", "watchlist")}}


def config(*pairs):
    return {"pairs": list(pairs), "anime_mapping": {"enabled": True}}


def rows():
    return {
        "mal:1": {"type": "movie", "title": "Anime", "ids": {"mal": "1"}, "rating": 8,
                  "watched_at": "2025-01-01T00:00:00Z", "rated_at": "2025-01-01T00:00:00Z"},
        "tmdb:2": {"type": "movie", "title": "Live action", "ids": {"tmdb": "2"}, "rating": 9,
                   "watched_at": "2026-01-01T00:00:00Z", "rated_at": "2026-01-01T00:00:00Z"},
    }


def state():
    return {"providers": {"SIMKL": {
        feature: {"baseline": {"items": rows()}} for feature in ("history", "ratings", "watchlist")
    }}}


@pytest.fixture(autouse=True)
def local_mapping(monkeypatch):
    from cw_platform.anime_mapping import service

    monkeypatch.setattr(service, "index_ready", lambda *_: False)
    monkeypatch.setattr(service, "find_identity_overrides", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(service, "query_edges", lambda *_args: [])
    monkeypatch.setattr(dw, "_resolve_missing_art_rows", lambda rows, **_kwargs: rows)
    monkeypatch.setattr(dw, "_history_alias_representatives", dict)
    monkeypatch.setattr(dw, "_tracker_feature_items", lambda *_: {})


@pytest.mark.parametrize("target", ["MYANIMELIST", "KITSU", "ANILIST"])
def test_anime_pair_filters_all_enabled_features_without_mutating_state(target):
    original = state()
    before = deepcopy(original)
    scope = MediaScope(config(pair(target)))
    filtered = scope.state(original)
    for feature in ("history", "ratings", "watchlist"):
        assert list(filtered["providers"]["SIMKL"][feature]["baseline"]["items"]) == ["mal:1"]
    assert original == before


def test_unrestricted_pair_wins_only_for_its_feature_and_instance():
    unrestricted = pair("TRAKT")
    unrestricted["features"] = {"ratings": {"enable": True}}
    scope = MediaScope(config(pair(), unrestricted, pair("TRAKT", instance="second")))
    assert scope.keep(rows()["tmdb:2"], "SIMKL", "default", "ratings")
    assert not scope.keep(rows()["tmdb:2"], "SIMKL", "default", "history")
    assert scope.keep(rows()["tmdb:2"], "SIMKL", "second", "history")
    assert scope.keep(rows()["tmdb:2"], "PLEX", "default", "history")


def test_disabled_pairs_do_not_expand_scope():
    disabled = {**pair("TRAKT"), "enabled": False}
    scope = MediaScope(config(pair(), disabled))
    assert not scope.keep(rows()["tmdb:2"], "SIMKL", "default", "history")
    assert MediaScope(config(disabled)).state(state()) == state()
    assert scope.state({}) == {}


def test_explicit_anime_scope_and_native_source_reverse_pair():
    scope = MediaScope(config(pair("TRAKT", use_anime_mapping=True, anime_only_sync=True)))
    assert not scope.keep(rows()["tmdb:2"], "SIMKL", "default", "history")
    reverse = pair()
    reverse.update(source="MYANIMELIST", target="SIMKL")
    scope = MediaScope(config(reverse))
    assert not scope.keep(rows()["tmdb:2"], "SIMKL", "default", "history")
    assert scope.keep({"type": "episode", "watched": True}, "MYANIMELIST", "default", "history")


def test_episode_classification_uses_cached_local_show_mapping(monkeypatch):
    from cw_platform.anime_mapping import service

    calls = []
    def edges(*args):
        calls.append(args)
        return [{"source_kind": "show", "target_provider": "mal", "target_id": "1"}]
    monkeypatch.setattr(service, "query_edges", edges)
    scope = MediaScope(config(pair()))
    for number in range(1, 11):
        assert scope.keep({"type": "episode", "season": 1, "episode": number, "show_ids": {"tvdb": "10"}},
                          "SIMKL", "default", "history")
    assert len(calls) == 1


def test_filtered_counts_respect_instances():
    data = state()
    data["providers"]["SIMKL"]["instances"] = {"second": deepcopy(data["providers"]["SIMKL"])}
    scope = MediaScope(config(pair()))
    filtered = scope.state(data)
    assert scope.totals(filtered, {"history"}, {"SIMKL": ["default"]}) == {"history": 1}
    assert scope.totals(filtered, {"history"}, {"SIMKL": ["second"]}) == {"history": 2}


def test_dashboard_filters_before_recent_limit_and_invalidates_pair_cache(monkeypatch, tmp_path):
    from api import dashboardAPI
    from cw_platform import config_base
    from cw_platform.orchestrator._state_store import StateStore

    cfg = config(pair())
    monkeypatch.setattr(config_base, "CONFIG", tmp_path)
    monkeypatch.setattr(config_base, "load_config", lambda: cfg)
    dashboardAPI._PAYLOAD_CACHE.clear()
    items = rows()
    for number in range(500):
        items[f"tmdb:{number + 10}"] = {**rows()["tmdb:2"], "ids": {"tmdb": str(number + 10)}}
    StateStore(tmp_path).save_feature_baseline(provider="SIMKL", feature="history", items=items, last_sync_epoch=1)
    first = json.loads(dashboardAPI.dashboard_widgets(include="history", history_limit=8).body)
    assert [row["title"] for row in first["recent_history"]["items"]] == ["Anime"]
    assert first["recent_history"]["library_total"] == 1
    cfg["pairs"].append(pair("TRAKT"))
    second = json.loads(dashboardAPI.dashboard_widgets(include="history", history_limit=8).body)
    assert second["version"] != first["version"]
    assert second["recent_history"]["library_total"] == 502


@pytest.mark.parametrize("feature", ["history", "ratings"])
def test_profile_pages_share_widget_scope(monkeypatch, feature):
    from api import profileAPI

    monkeypatch.setattr(profileAPI, "_history_user_filter", lambda *_: {"SIMKL": ["default"]})
    monkeypatch.setattr(profileAPI, "history_cache_version", lambda *_: "")
    monkeypatch.setattr(profileAPI, "_history_state", state)
    monkeypatch.setattr(profileAPI, "_ratings_state", state)
    monkeypatch.setattr(profileAPI, "_tracker_feature_items", lambda *_: {})
    cfg = config(pair())
    index, _ = profileAPI._profile_history_index(cfg, "user", feature)
    assert [row["title"] for row in index["rows"]] == ["Anime"]
    assert index["counts"]["all"] == 1
    assert index["provider_counts"] == {"simkl": 1}



def test_insights_counts_follow_same_scope(monkeypatch):
    from api import insightAPI

    cw = SimpleNamespace(STATS=SimpleNamespace(data={"samples": [], "events": []}), REPORT_DIR=None,
                         CACHE_DIR=None, _load_wall_snapshot=lambda: [], _append_log=lambda *_a, **_k: None)
    monkeypatch.setattr(insightAPI, "_env", lambda: (cw, lambda: config(pair()), lambda _cfg: None, lambda *_a, **_k: None))
    monkeypatch.setattr(insightAPI, "_load_state_features", lambda _: state())
    app = FastAPI()
    insightAPI.register_insights(app)
    data = TestClient(app).get("/api/insights?limit_samples=0&history=0&include_events=0").json()
    assert data["features"]["history"]["providers"]["simkl"] == 1


def test_watchlist_wall_and_profile_filter_before_counts(monkeypatch):
    from api import wallAPI, profileAPI
    from fastapi.routing import APIRoute
    from services import watchlist

    cfg = config(pair())
    monkeypatch.setattr(watchlist, "_registry_sync_providers", lambda: ["SIMKL"])
    monkeypatch.setattr(watchlist, "_load_hide_set", set)
    monkeypatch.setattr(wallAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(wallAPI, "_load_state", state)
    monkeypatch.setattr(wallAPI, "_tmdb_api_key", lambda _: "")
    monkeypatch.setattr(wallAPI, "_configured_provider_ids", lambda _: ["SIMKL"])
    monkeypatch.setattr(profileAPI, "_load_watchlist_state", state)
    monkeypatch.setattr(profileAPI, "_tmdb_api_key", lambda _: "")
    wallAPI.clear_wall_cache()
    app = FastAPI()
    wallAPI.register_wall(app)
    endpoint = next(route.endpoint for route in app.routes if isinstance(route, APIRoute) and route.path == "/api/state/wall")
    payload = endpoint(both_only=False, active_only=False, limit=1, user_profile="", known_version="")
    assert payload["total"] == 1
    assert [row["title"] for row in payload["items"]] == ["Anime"]
    assert [row["title"] for row in profileAPI._watchlist_items(cfg, {})] == ["Anime"]
    cfg["pairs"].append(pair("TRAKT"))
    updated = endpoint(both_only=False, active_only=False, limit=1, user_profile="", known_version=payload["version"])
    assert updated["total"] == 2
    assert updated["version"] != payload["version"]


def test_scope_fingerprint_tracks_mapping_updates(monkeypatch, tmp_path):
    from cw_platform.anime_mapping import storage, overrides

    index = tmp_path / "mapping.sqlite"
    rules = tmp_path / "overrides.json"
    monkeypatch.setattr(storage, "paths", lambda _: {"db": index})
    monkeypatch.setattr(overrides, "overrides_path", lambda: rules)
    scope = MediaScope(config(pair()))
    first = scope.fingerprint()
    index.write_bytes(b"updated")
    second = scope.fingerprint()
    rules.write_text("{}", encoding="utf-8")
    assert first != second != scope.fingerprint()


def test_progress_filter_precedes_summary_and_pagination(monkeypatch):
    from services.playback_progress import service
    from services.playback_progress.models import PlaybackListResult, PlaybackRecord

    anime_pair = pair("TRAKT", use_anime_mapping=True, anime_only_sync=True)
    anime_pair["features"] = {"progress": {"enable": True, "use_anime_mapping": True, "anime_only_sync": True}}
    cfg = config(anime_pair)
    monkeypatch.setattr(service, "load_config", lambda: cfg)
    monkeypatch.setattr(service, "_share_artwork_metadata", lambda _: None)
    monkeypatch.setattr(service, "_overlay_live_streams", lambda _: None)
    progress = service.PlaybackProgressService()
    monkeypatch.setattr(progress, "provider_instances", lambda *_a, **_k: [{"provider": "simkl", "instance_id": "default"}])
    caps = SimpleNamespace(provider="simkl", instance_id="default", read=True, included=True,
                           configured=True, to_dict=lambda: {})
    monkeypatch.setattr(progress, "capabilities", lambda *_a, **_k: [caps])
    records = [PlaybackRecord(provider="simkl", provider_label="SIMKL", instance_id="default", instance_label="Default",
                              remote_id=key, canonical_key=key, media_type="movie", title=row["title"], ids=row["ids"])
               for key, row in rows().items()]
    monkeypatch.setattr(progress, "_list_one", lambda *_a: PlaybackListResult(ok=True, provider="simkl", instance_id="default", items=records))
    payload = progress.items(page_size=1)
    assert payload["total"] == 1
    assert [row["title"] for row in payload["items"]] == ["Anime"]


def test_known_simkl_anime_and_local_tracker_scope():
    anime_pair = pair()
    anime_pair["source"] = "CROSSWATCH"
    scope = MediaScope(config(anime_pair))
    assert list(scope.tracker(rows(), "history")) == ["mal:1"]
    assert scope.keep({"type": "episode", "simkl_bucket": "anime"}, "CROSSWATCH", "default", "history")
    assert not scope.keep({"type": "movie", "title": "Unknown"}, "CROSSWATCH", "default", "history")


def test_filtered_watchlist_does_not_restore_legacy_items():
    data = state()
    data["providers"]["SIMKL"]["items"] = {"tmdb:2": rows()["tmdb:2"]}
    data["providers"]["SIMKL"]["watchlist"]["baseline"]["items"] = {"tmdb:2": rows()["tmdb:2"]}
    filtered = MediaScope(config(pair())).state(data)
    assert filtered["providers"]["SIMKL"]["items"] == {}
    assert filtered["providers"]["SIMKL"]["watchlist"]["baseline"]["items"] == {}
