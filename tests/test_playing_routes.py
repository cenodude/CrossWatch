# tests/test_playing_routes.py
# CrossWatch - Observed playing card routes and session isolation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import replace
import time

import pytest

from cw_platform.event_archive import playback_recorder, recorder
from cw_platform.event_archive.db import connect
from providers.scrobble import playing_routes
from providers.scrobble.scrobble import ScrobbleEvent


@pytest.fixture
def archive(tmp_path, monkeypatch):
    conn = connect(tmp_path / "playing.sqlite3")
    for module in (playback_recorder, recorder, playing_routes):
        monkeypatch.setattr(module, "get_conn", lambda: conn)
    yield conn
    conn.close()


def event(**changes):
    return replace(ScrobbleEvent("start", "episode", {"tmdb_show": "253372"}, "The Institute", 2026,
                                2, 1, 24, "alice", "server", "session-1", {}), **changes)


def stream(**changes):
    return {"source": "plex", "provider_instance": "default", "session_key": "session-1", "account": "alice",
            "media_type": "episode", "ids": {"tmdb_show": "253372"}, "title": "The Institute", "season": 2,
            "episode": 1, "started": int(time.time()), **changes}


def route(sink="trakt"):
    return {"scrobble": {"watch": {"route_id": "route-" + sink, "route_provider": "plex", "route_sink": sink}}}


def test_observed_routes_include_named_endpoints_and_multiple_destinations(archive):
    for sink in ("trakt", "simkl"):
        playback_recorder.record_playback(event(), route(sink))
    item = stream()
    playing_routes.attach_playing_routes([item], {"plex": {"label": "Home"}, "trakt": {"label": "Main"}}, {})
    assert {r["destination"] for r in item["routes"]} == {"trakt", "simkl"}
    trakt = next(r for r in item["routes"] if r["destination"] == "trakt")
    assert trakt == {"id": "route-trakt", "kind": "watcher", "source": "plex", "source_instance": "default",
                     "source_label": "Home", "destination": "trakt", "destination_instance": "default", "destination_label": "Main"}


@pytest.mark.parametrize("changes", [{"session_key": "another"}, {"account": "bob"}, {"episode": 2},
                                    {"ids": {"tmdb_show": "other"}}, {"provider_instance": "another"}, {"session_key": None}])
def test_unrelated_playback_never_inherits_routes(archive, changes):
    playback_recorder.record_playback(event(), route())
    item = stream(**changes)
    playing_routes.attach_playing_routes([item], {}, {})
    assert item["routes"] == []


def test_webhook_method_and_profile_destinations_are_scoped(archive):
    for sink in ("trakt", "simkl"):
        playback_recorder.record_playback(event(raw={"_cw_activity_method": "webhook"}), route(sink))
    items = [stream(), stream(source="plextrakt")]
    playing_routes.attach_playing_routes(items, {}, {"instances": {"PLEX": ["default"], "TRAKT": ["default"]}})
    assert items[0]["routes"] == []
    assert [r["destination"] for r in items[1]["routes"]] == ["trakt"]
    assert items[1]["routes"][0]["kind"] == "webhook"


def test_stopped_routes_and_previous_session_runs_are_not_shown(archive):
    playback_recorder.record_playback(event(), route())
    playback_recorder.record_playback(event(action="stop"), route())
    item = stream()
    playing_routes.attach_playing_routes([item], {}, {})
    assert item["routes"] == []
    playback_recorder.record_playback(event(), route("simkl"))
    item["started"] += 60
    playing_routes.attach_playing_routes([item], {}, {})
    assert item["routes"] == []


@pytest.mark.parametrize("ids", [{"imdb_show": "tt31422370", "tmdb_show": "253372"}, {"tvdb_show": "123"}])
def test_route_identity_survives_different_available_id_namespaces(archive, ids):
    playback_recorder.record_playback(event(), route())
    item = stream(ids=ids)
    playing_routes.attach_playing_routes([item], {}, {})
    assert [r["destination"] for r in item["routes"]] == ["trakt"]


def test_title_fallback_does_not_match_a_different_year(archive):
    playback_recorder.record_playback(event(), route())
    item = stream(ids={"tvdb_show": "123"}, year=2000)
    playing_routes.attach_playing_routes([item], {}, {})
    assert item["routes"] == []


def test_webhook_sessions_remain_separate_in_currently_watching(tmp_path, monkeypatch):
    from cw_platform.local_db import close_conn
    from providers.scrobble import currently_watching

    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "sessions.sqlite3"))
    close_conn()
    try:
        for session, account in (("one", "alice"), ("two", "bob")):
            currently_watching.update_from_payload("plextrakt", "movie", "Arrival", 2016, None, None, 20, False,
                                                  state="playing", session_key=session, account=account, ids={"tmdb": 329865})
        items = currently_watching.load_state()["streams"]
        assert len(items) == 2
        assert {p["account"] for p in items.values()} == {"alice", "bob"}
        currently_watching.update_from_payload("plextrakt", "movie", "Arrival", 2016, None, None, 20, True,
                                              state="stopped", session_key="one", account="alice", ids={"tmdb": 329865})
        assert list(currently_watching.load_state()["streams"]) == ["plextrakt:two"]
    finally:
        close_conn()


@pytest.mark.parametrize("source_allowed", [True, False])
def test_playback_progress_uses_observed_routes_with_profile_scope(archive, source_allowed):
    from services.playback_progress.service import _group_records, _overlay_live_streams

    for sink in ("crosswatch", "trakt"):
        playback_recorder.record_playback(event(), route(sink))
    items = [
        {"provider":provider, "instance_id":"default", "remote_id":provider,
         "canonical_key":"tmdb:253372#s02e01", "media_type":"episode", "title":"The Institute",
         "season":2, "episode":1, "ids":{"tmdb_show":"253372"}, "progress_percent":progress}
        for provider, progress in (("plex",75),("crosswatch",53),("trakt",53))
    ]
    user_filter = {"CROSSWATCH":["default"]}
    if source_allowed:
        user_filter["PLEX"] = ["default"]
    _overlay_live_streams(items, [stream(state="playing", progress=76, updated=int(time.time()))], cfg={}, user_filter=user_filter)
    assert "live_state" not in items[2]
    if source_allowed:
        combined = next(row for row in _group_records(items) if row["is_combined"])
        assert {row["provider"] for row in combined["records"]} == {"plex", "crosswatch"}
        assert combined["live_progress_percent"] == 76
        assert {route["destination"] for route in combined["live_routes"]} == {"crosswatch"}
    else:
        assert all("live_state" not in item for item in items)


def test_progress_does_not_infer_live_destinations_from_configuration(archive):
    from services.playback_progress.service import _group_records, _overlay_live_streams

    items = [
        {"provider":provider, "instance_id":"default", "remote_id":provider,
         "canonical_key":"tmdb:253372#s02e01", "media_type":"episode", "title":"The Institute",
         "season":2, "episode":1, "progress_percent":progress}
        for provider, progress in (("plex",75),("crosswatch",53))
    ]
    _overlay_live_streams(items, [stream(state="playing", progress=76)], cfg=route("crosswatch"))
    assert len(_group_records(items)) == 2
    assert "live_state" not in items[1]


def test_progress_exposes_observed_destinations_without_saved_resume_records(archive):
    from services.playback_progress.service import _group_records, _overlay_live_streams

    for sink in ("crosswatch", "trakt", "simkl"):
        playback_recorder.record_playback(event(), route(sink))
    items = [{"provider":"plex", "instance_id":"default", "remote_id":"plex-1",
              "canonical_key":"tmdb:253372#s02e01", "media_type":"episode", "title":"The Institute",
              "season":2, "episode":1, "ids":{"tmdb_show":"253372"}, "progress_percent":75,
              "can_update_progress":True}]
    _overlay_live_streams(items, [stream(state="playing", progress=76)],
                          cfg={"trakt":{"label":"My Trakt"}, "simkl":{"label":"My SIMKL"}})
    row = _group_records(items)[0]
    assert {r["destination"] for r in row["live_routes"]} == {"crosswatch", "trakt", "simkl"}
    assert {r["destination_label"] for r in row["live_routes"]} == {"Default", "My Trakt", "My SIMKL"}
    assert row["provider_count"] == 1
    assert [r["provider"] for r in row["records"]] == ["plex"]
    assert row["can_update_progress"] is True
    assert row["progress_percent"] == 75
