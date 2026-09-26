# tests/test_wetrakr_scrobble.py
# CrossWatch - WeTrakr scrobble delivery, profiles and Plex ratings
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import replace

import pytest

from test_wetrakr_sync import EPISODE, MOVIE, SHOW, WHEN, Response, env
from test_wetrakr_ratings_progress import features
from providers.scrobble.scrobble import ScrobbleEvent
from providers.scrobble.wetrakr import sink


def event(**changes):
    return ScrobbleEvent(**{**dict(action="start", media_type="movie", ids={"tmdb": "155"}, title="Test movie", year=2008,
                                  season=None, number=None, progress=10, account="tester", server_uuid="server",
                                  session_key="session", raw={}), **changes})


@pytest.fixture
def live(features, monkeypatch):
    previous = features.server.hook
    def hook(method, path, kwargs):
        if path == "/media/external/tmdb/155":
            return Response({"id": MOVIE["id"], "type": "movie"})
        if method == "POST" and path.startswith("/scrobble/"):
            body = kwargs["json"]
            media = MOVIE if "movie" in body else EPISODE
            if path.endswith("/stop"):
                features.server.play(media, WHEN)
                features.playing.pop(media["id"], None)
                return Response({"action": "scrobble", "progress": body["progress"]})
            response = previous(method, "/scrobble/pause", kwargs)
            if path.endswith("/start"):
                features.playing[media["id"]]["playback"]["status"] = "playing"
                return Response({"action": "start", "progress": body["progress"]})
            return response
        return previous(method, path, kwargs)
    features.server.hook = hook
    features.archived, features.completed, features.removed = [], [], []
    monkeypatch.setattr(sink, "record_watch", lambda *a, **kw: features.archived.append(kw))
    monkeypatch.setattr(sink, "record_scrobble_event", lambda *a, **kw: features.completed.append(kw))
    monkeypatch.setattr(sink, "remove_across_providers_by_ids", lambda *a, **kw: features.removed.append((a, kw)))
    features.sink = sink.WeTrakrSink(lambda: features.view, "P01")
    return features


def posts(live):
    return [(path, args["json"]) for method, path, args in live.server.calls if method == "POST"]


@pytest.mark.parametrize("episode", [False, True])
def test_start_pause_resume_incomplete_stop_completion_and_rewatch(live, episode):
    ev = event(**({"media_type": "episode", "ids": {"tmdb": "62085", "tmdb_show": "1396"}, "season": 1, "number": 1} if episode else {}))
    for action, progress in (("start", 10), ("pause", 45), ("start", 46), ("stop", 85)):
        assert live.sink.send(replace(ev, action=action, progress=progress))["ok"]
    assert not live.server.history
    assert posts(live)[-1][0] == "/scrobble/pause"
    complete = replace(ev, action="stop", progress=95)
    assert live.sink.send(complete)["ok"]
    assert len(live.server.history) == len(live.completed) == 1
    assert not live.playing
    assert live.sink.send(complete)["skipped"]
    assert live.sink.send(replace(complete, session_key="rewatch"))["ok"]
    assert len(live.server.history) == len(live.completed) == 2
    assert all(row["destination_instance"] == "P01" for row in live.archived)
    assert len([p for m, p, _ in live.server.calls if m == "GET" and p.startswith("/media/external")]) == 1


def test_profile_only_factories_routes_and_credentials(live):
    from providers.scrobble.watch_manager import _make_sink
    from providers.webhooks import config, dispatch
    from providers.scrobble.routes import ROUTE_RATING_SINKS, ROUTE_SINKS
    assert "wetrakr" in ROUTE_SINKS & ROUTE_RATING_SINKS
    cfg = {"wetrakr": {"access_token": "", "instances": {"P01": {"access_token": "token"}}}}
    assert config.sink_configured(cfg, "wetrakr", "P01")
    assert not config.sink_configured(cfg, "wetrakr", "default")
    for created in (_make_sink("wetrakr", lambda: cfg, "P01"), dispatch._make_sink("wetrakr", "P01", lambda: cfg)):
        assert isinstance(created, sink.WeTrakrSink) and created.instance_id == "P01"
    assert sink.WeTrakrSink(lambda: cfg, "missing").send(event())["error"] == "not_configured"


@pytest.mark.parametrize("action,progress,expected", [("start", 0, "/scrobble/start"), ("pause", 0, "/scrobble/pause"),
                                                    ("stop", 0, "/scrobble/pause"), ("pause", 95, "/scrobble/pause")])
def test_zero_never_triggers_progress_deletion_or_watched(live, action, progress, expected):
    assert live.sink.send(event(action=action, progress=progress))["ok"]
    assert posts(live)[-1][0] == expected and posts(live)[-1][1]["progress"] > 0
    assert not live.server.history


def test_progress_step_seek_debounce_and_suspicious_completion(live):
    assert live.sink.send(event())["ok"]
    assert live.sink.send(event(progress=11))["reason"] == "progress_step"
    assert live.sink.send(event(progress=12, raw={"_cw_seek": True}))["ok"]
    assert live.sink.send(event(action="pause", progress=12))["ok"]
    assert live.sink.send(event(action="pause", progress=12))["reason"] == "debounced"
    assert live.sink.send(event(action="stop", progress=100))["ok"]
    assert posts(live)[-1][0] == "/scrobble/pause"
    assert not live.server.history
    assert live.sink.send(event(action="stop", progress=100, raw={"_cw_preserve_stop": True}))["action"] == "scrobble"


@pytest.mark.parametrize("action,status,retryable", [("start", 503, True), ("stop", 503, False), ("stop", 429, True), ("start", 401, False)])
def test_failure_does_not_record_completion_and_controls_retry(live, monkeypatch, action, status, retryable):
    original = sink.request
    def fail(adapter, method, path, **kwargs):
        if method == "POST":
            raise sink.WeTrakrSyncError("request_rejected", status=status, retry_after=30 if status == 429 else 0)
        return original(adapter, method, path, **kwargs)
    monkeypatch.setattr(sink, "request", fail)
    ev = event(action=action, progress=95)
    result = live.sink.send(ev)
    assert not result["ok"] and result["retryable"] is retryable
    assert not live.completed
    monkeypatch.setattr(sink, "request", original)
    retried = live.sink.send(ev)
    if action == "stop" and status == 503:
        assert retried["error"] == "completion_unconfirmed" and not posts(live)
    else:
        assert retried["ok"]


def test_unresolved_media_is_not_silently_confirmed(live):
    old = live.server.hook
    live.server.hook = lambda m, p, kw: Response({}) if p.startswith("/media/external") else old(m, p, kw)
    assert live.sink.send(event())["error"] == "movie_not_resolved"
    assert not posts(live)


@pytest.mark.parametrize("action,progress", [("start", 40), ("pause", 45), ("stop", 95)])
@pytest.mark.parametrize("reason", ["Track already watched (stale play tick)", None])
def test_ignored_scrobble_is_skipped_without_completion_or_side_effects(live, action, progress, reason):
    previous = live.server.hook
    live.server.hook = lambda method, path, kwargs: Response({"action": action, "ignored": True, "reason": reason}) if method == "POST" else previous(method, path, kwargs)
    result = live.sink.send(event(action=action, progress=progress))
    assert result == {"ok": True, "skipped": True, "ignored": True, "reason": reason or "ignored_by_provider"}
    assert not live.archived and not live.completed and not live.removed
    assert all(not state.get("completed") and not state.get("uncertain") for state in live.sink._sessions.values())
    live.server.hook = previous
    assert live.sink.send(event(action=action, progress=progress))["ok"]
    assert len(posts(live)) == 2


def test_dispatcher_allows_real_start_after_ignored_autoplay_without_retry(live):
    from providers.scrobble.scrobble import Dispatcher

    previous = live.server.hook
    live.server.hook = lambda method, path, kwargs: Response({"action": "start", "ignored": True, "reason": "Below scrobble threshold (5%)"}) if method == "POST" and kwargs["json"]["progress"] < 5 else previous(method, path, kwargs)
    dispatcher = Dispatcher([live.sink], cfg_provider=lambda: live.view)
    assert dispatcher.dispatch(event(progress=1))
    assert not dispatcher._pending and not dispatcher._last_action
    assert not live.archived
    assert dispatcher.dispatch(event(progress=6))
    assert len(posts(live)) == 2 and len(live.archived) == 1
    assert live.playing[MOVIE["id"]]["playback"]["status"] == "playing"


def test_scrobble_post_not_found_is_terminal_without_completion(live):
    previous = live.server.hook
    live.server.hook = lambda method, path, kwargs: Response({"error": "NOT_FOUND"}, status=404) if method == "POST" else previous(method, path, kwargs)
    result = live.sink.send(event(action="stop", progress=95))
    assert not result["ok"] and not result["retryable"]
    assert not live.completed and not live.removed
    assert all(not state.get("uncertain") for state in live.sink._sessions.values())


def test_episode_never_uses_episode_ids_as_parent(live):
    result = live.sink.send(event(media_type="episode", ids={"tmdb": "62085"}, season=1, number=1))
    assert not result["ok"] and not live.server.calls


def test_completed_session_dedup_survives_id_enrichment(live):
    ev = event(action="stop", progress=95, ids={"tmdb": "155", "imdb": "tt0468569"})
    assert live.sink.send(ev)["ok"]
    assert live.sink.send(replace(ev, ids={"imdb": "tt0468569"}))["skipped"]
    assert len(live.server.history) == 1


@pytest.mark.parametrize("field,value", [("session_key", "new"), ("account", "other"), ("server_uuid", "other")])
def test_completion_dedup_keeps_sessions_accounts_and_servers_separate(live, field, value):
    ev = event(action="stop", progress=95)
    assert live.sink.send(ev)["ok"]
    assert live.sink.send(replace(ev, **{field: value}))["ok"]
    assert len(live.server.history) == 2


def test_route_threshold_and_restart_follow_shared_policy(live):
    cfg = {**live.view, "scrobble": {"trakt": {"watched_at": 96}}}
    assert live.sink.send(event(action="stop", progress=95), cfg)["action"] == "pause"
    assert live.sink.send(event(action="stop", progress=97), cfg)["action"] == "scrobble"
    assert live.sink.send(event(progress=0), cfg)["action"] == "start"
    assert live.sink.send(event(action="stop", progress=97), cfg)["action"] == "scrobble"
    assert len(live.server.history) == 2


def test_dispatcher_retries_transient_failure_without_losing_event(live, monkeypatch):
    from providers.scrobble.scrobble import Dispatcher
    original = sink.request
    def fail(adapter, method, path, **kwargs):
        if method == "POST":
            raise sink.WeTrakrSyncError("rate_limited", status=429, retry_after=30)
        return original(adapter, method, path, **kwargs)
    monkeypatch.setattr(sink, "request", fail)
    dispatcher = Dispatcher([live.sink], cfg_provider=lambda: live.view)
    dispatcher.dispatch(event())
    assert dispatcher._pending and not posts(live)
    monkeypatch.setattr(sink, "request", original)
    for key in dispatcher._retry_after:
        dispatcher._retry_after[key] = (0, 1)
    dispatcher.retry_pending()
    assert not dispatcher._pending and len(posts(live)) == 1


def test_rating_episode_specials_keep_zero_season_and_missing_parent(live):
    from providers.scrobble.plex.ratings_sync import item_from_plex_rating, send_rating
    item = item_from_plex_rating("episode", {"parentIndex": 0, "index": 1}, {"tmdb": 62085}, 8,
                                 show_ids={}, episode_ids={"tmdb": 62085})
    assert item and item["season"] == 0 and not item.get("show_ids")
    assert not send_rating("wetrakr", live.view, "P01", item, 8)["ok"]
    assert not posts(live)


def test_scrobble_auto_remove_only_after_confirmed_completion(live):
    cfg = {**live.view, "scrobble": {"delete_plex": True, "delete_plex_types": ["movies"]}}
    assert live.sink.send(event(action="stop", progress=40), cfg)["ok"]
    assert not live.removed
    assert live.sink.send(event(action="stop", progress=95), cfg)["ok"]
    assert len(live.removed) == 1 and live.removed[0][1]["scope"] == "wetrakr:P01"


@pytest.mark.parametrize("kind", ["movie", "show", "episode"])
def test_plex_rating_dispatch_add_update_remove_uses_selected_profile(live, kind):
    from providers.scrobble.plex.ratings_sync import dispatch_ops_ratings
    md = {"title": "Title", "parentIndex": 1, "index": 1}
    ids = {"tmdb": 155 if kind == "movie" else 1396}
    for rating in (6, 9, 0):
        result = dispatch_ops_ratings(kind, md, ids, rating, live.view, enabled=["wetrakr"], instance_for=lambda _: "P01",
                                      show_ids={"tmdb": 1396}, episode_ids={"tmdb": 62085})
        assert result["wetrakr"]["ok"]
        assert len(live.ratings) == (1 if rating else 0)
        if rating:
            assert next(iter(live.ratings.values()))["interactions"]["user"]["rating"]["rating"] == rating


@pytest.mark.parametrize("watcher", [False, True])
@pytest.mark.parametrize("kind", ["movie", "show", "episode"])
def test_plex_rating_entrypoints_keep_movie_show_episode_ids_separate(live, monkeypatch, watcher, kind):
    from providers.webhooks import plex
    from providers.scrobble.plex import watch
    monkeypatch.setattr(plex, "_save_config", lambda cfg: None)
    plex._LAST_RATING_BY_ACC.clear()
    watch._LAST_RATING_BY_ACC.clear()
    cfg = {**live.cfg, "scrobble": {"enabled": True, "sources": {"watcher": True, "webhook": True},
                                   "watch": {"plex_wetrakr_ratings": True, "route_sink_instance": "P01"},
                                   "webhook": {"sinks": ["wetrakr"], "sink_instances": {"wetrakr": "P01"}, "plex_wetrakr_ratings": True}}}
    md = {"type": kind, "title": "Test", "ratingKey": "test", "Guid": [{"id": "tmdb://" + str({"movie": 155, "show": 1396, "episode": 62085}[kind])}]}
    if kind == "episode":
        md.update(parentIndex=1, index=1, grandparentGuid="tmdb://1396", grandparentTitle="Test show")
    for rating in (6, 9, 0):
        payload = {"event": "media.rate", "Account": {"title": "tester"}, "Metadata": {**md, "userRating": rating}}
        result = watch.process_rating_webhook(payload, {}, cfg_override=cfg) if watcher else plex.process_webhook(payload, {}, cfg=cfg)
        assert result.get("wetrakr", {}).get("ok"), result
        assert len(live.ratings) == (1 if rating else 0)


def test_playback_service_bulk_and_active_protection(live, monkeypatch):
    from services.playback_progress import service as playback
    from services.playback_progress.adapters.wetrakr import WeTrakrPlaybackAdapter
    monkeypatch.setattr(playback, "load_config", lambda: live.cfg)
    svc = playback.PlaybackProgressService()
    adapter = WeTrakrPlaybackAdapter()
    args = {"instance_id": "P01", "instance_label": "Test"}
    evs = [event(), event(media_type="episode", ids={"tmdb_show": "1396"}, season=1, number=1, session_key="episode")]
    for ev in evs:
        assert live.sink.send(ev)["ok"]
    active = adapter.list_progress(live.view, **args)
    assert all(not r.can_update_progress and not r.can_remove_progress for r in active.items)
    for ev in evs:
        assert live.sink.send(replace(ev, action="pause", progress=40))["ok"]
    records = [r.to_dict() for r in adapter.list_progress(live.view, **args).items]
    for record in records:
        assert svc.update_progress({"provider": "wetrakr", "instance_id": "P01", "record": record, "progress_percent": 60})["ok"]
    records = [r.to_dict() for r in adapter.list_progress(live.view, **args).items]
    result = svc.bulk({"action": "remove_progress", "items": records})
    assert result["successful"] == 2 and result["failed"] == 0 and not live.playing
