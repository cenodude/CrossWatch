# tests/test_plex_scrobble_sink.py
# CrossWatch - Plex Scrobble Sink Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit
from xml.etree.ElementTree import Element, SubElement

import pytest

from providers.scrobble.plex import sink as plex
from providers.scrobble import _media_server as common
from providers.scrobble.routes import build_route_cfg, same_scrobble_endpoint, scrobble_sink_config
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent


def config():
    return {
        "plex": {"server_url": "http://default", "pms_token": "default-token",
                 "instances": {"P01": {"server_url": "http://p01", "pms_token": "p01-token"}}},
        "scrobble": {"trakt": {"watched_at": 90, "progress_step": 25},
                     "watch": {"route_provider": "jellyfin", "route_provider_instance": "default", "route_sink": "plex", "route_sink_instance": "default"},
                     "webhook": {"sinks": ["plex"], "sink_instances": {"plex": "default"}}},
    }


def event(**kwargs):
    base = ScrobbleEvent("stop", "movie", {"tmdb": "42"}, "Movie", 2020, None, None, 95,
                         "alice", "source-server", "session-1", {})
    return replace(base, **kwargs)


def media(rk="10", kind="movie", ids=None, **kwargs):
    return SimpleNamespace(ratingKey=rk, type=kind, title="Movie", year=2020,
                           guid=None, guids=[SimpleNamespace(id=f"{k}://{v}") for k, v in (ids or {"tmdb": "42"}).items()],
                           librarySectionID="1", duration=100_000, viewOffset=0, viewCount=0, **kwargs)


class Server:
    def __init__(self, *items):
        self.items = {str(x.ratingKey): x for x in items}
        self.calls = []
        self.playing = []
        self.fail_write = False
        self.fail_sessions = False
        self._session = SimpleNamespace(put=lambda: None, post=lambda: None, close=lambda: None)

    def query(self, path, method=None, params=None):
        verb = "PUT" if method is self._session.put else "POST" if method is self._session.post else method
        self.calls.append((path, verb, params))
        if method:
            if self.fail_write:
                raise RuntimeError("failed")
            if path == "/:/scrobble":
                self.items[str(params["key"])].viewCount = 1
            if path == "/:/timeline":
                self.items[str(params["ratingKey"])].viewOffset = params["time"]
            return Element("MediaContainer")
        query = parse_qs(urlsplit(path).query)
        assert "guid" in query, "Sink must not scan the entire library"
        root = Element("MediaContainer")
        for item in self.items.values():
            if query["guid"][0] in [x.id for x in item.guids]:
                SubElement(root, "Video", {"ratingKey": str(item.ratingKey)})
        return root

    def fetchItem(self, key):
        return self.items[str(key)]

    def search(self, title, mediatype):
        return [x for x in self.items.values() if x.type == mediatype]

    def sessions(self):
        if self.fail_sessions:
            raise RuntimeError("session lookup failed")
        return self.playing


@pytest.fixture
def harness(monkeypatch):
    from providers.sync import _mod_PLEX

    server = Server(media())
    configs = []
    records = []

    def adapter(cfg):
        configs.append(copy.deepcopy(cfg))
        return SimpleNamespace(config=cfg, cfg=SimpleNamespace(client_id="sink-client"),
                               client=SimpleNamespace(server=server, session=server._session))

    monkeypatch.setattr(_mod_PLEX, "PLEXModule", adapter)
    monkeypatch.setattr(common, "record_watch", lambda *a, **k: records.append(k))
    monkeypatch.setattr(common, "record_scrobble_event", lambda *a, **k: None)
    monkeypatch.setattr(common, "remove_across_providers_by_ids", lambda *a, **k: None)
    return server, configs, records


def test_completion_writes_put_once_and_never_uses_source_rating_key(harness):
    server, configs, records = harness
    dest = plex.PlexSink()
    ev = event(ids={"tmdb": "42", "plex": "999"}, raw={"ratingKey": "999"})
    assert dest.send(ev, config()) == {"ok": True}
    assert dest.send(ev, config())["reason"] == "duplicate"
    writes = [x for x in server.calls if x[1]]
    assert writes == [("/:/scrobble", "PUT", {"key": "10", "identifier": "com.plexapp.plugins.library"})]
    assert records[0]["destination_provider"] == "plex"


def test_resume_uses_destination_duration_stopped_timeline_and_throttles(harness):
    server, _, _ = harness
    dest = plex.PlexSink()
    assert dest.send(event(action="start", progress=30, duration_ms=200_000), config())["ok"]
    assert dest.send(event(action="start", progress=31), config())["reason"] == "duplicate"
    assert dest.send(event(action="pause", progress=32), config())["ok"]
    writes = [x for x in server.calls if x[1]]
    assert [x[2]["time"] for x in writes] == [30_000, 32_000]
    assert all(x[0] == "/:/timeline" and x[1] == "POST" and x[2]["state"] == "stopped" for x in writes)


def test_below_threshold_stop_does_not_mark_watched(harness):
    server, _, _ = harness
    assert plex.PlexSink().send(event(progress=40), config())["ok"]
    assert not any(x[0] == "/:/scrobble" for x in server.calls)


def test_completion_clears_resume_without_unwatching(harness):
    server, _, _ = harness
    server.items["10"].viewOffset = 20_000
    assert plex.PlexSink().send(event(), config())["ok"]
    assert server.items["10"].viewOffset == 0
    assert server.items["10"].viewCount == 1


@pytest.mark.parametrize("source,target,allowed", [("default", "default", False), ("P01", "P01", False), ("default", "P01", True), ("P01", "default", True)])
def test_watcher_projection_and_self_route_protection(harness, source, target, allowed):
    server, configs, _ = harness
    cfg = config()
    route = {"provider": "plex", "provider_instance": source, "sink": "plex", "sink_instance": target}
    view = build_route_cfg(cfg, route)
    expected = "default-token" if target == "default" else "p01-token"
    assert scrobble_sink_config(view, "plex", target)["plex"]["pms_token"] == expected
    assert view["plex"]["pms_token"] == ("default-token" if source == "default" else "p01-token")
    assert plex.PlexSink(instance_id=target).send(event(), view)["ok"] is allowed
    if allowed:
        assert configs[0]["plex"]["pms_token"] == expected
    else:
        assert not configs and not server.calls


def test_webhook_projection_keeps_default_destination_credentials(harness):
    from cw_platform.provider_instances import build_provider_config_view
    from providers.webhooks.dispatch import dispatch_scrobble, _SINKS

    _SINKS.clear()
    original = config()
    cfg = build_provider_config_view(original, "plex", "P01")
    cfg["_cw_scrobble_provider_configs"] = {"plex": original["plex"]}
    result = dispatch_scrobble("plex", "/scrobble/stop", media_type="movie", ids={"tmdb": "42"}, progress=95, cfg=cfg, provider_instance="P01")
    assert result.json()["targets"][0]["ok"]
    assert harness[1][0]["plex"]["pms_token"] == "default-token"
    _SINKS.clear()


def test_direct_webhook_self_target_never_dispatches(harness):
    from providers.webhooks.dispatch import dispatch_scrobble
    result = dispatch_scrobble("plex", "/scrobble/stop", media_type="movie", ids={"tmdb": "42"}, progress=95, cfg=config())
    assert result.status_code == 502
    assert result.json()["targets"][0]["error"] == "same_source_destination"
    assert not harness[1]


@pytest.mark.parametrize("reason", ["active", "sessions_failed", "scope_failed", "missing_ids", "conflict", "ambiguous", "outside_library"])
def test_rejects_unsafe_or_unresolved_writes(harness, monkeypatch, reason):
    server, _, _ = harness
    cfg, ev = config(), event()
    if reason == "active":
        server.playing = [SimpleNamespace(ratingKey="10")]
    elif reason == "sessions_failed":
        server.fail_sessions = True
    elif reason == "scope_failed":
        monkeypatch.setattr(plex, "home_scope_enter", lambda _: (True, False, 42, "alice"))
    elif reason == "missing_ids":
        ev = event(ids={"plex": "10"})
    elif reason == "conflict":
        ev = event(ids={"tmdb": "42", "imdb": "tt123"})
        server.items["10"].guids.append(SimpleNamespace(id="imdb://tt999"))
    elif reason == "ambiguous":
        server.items["11"] = media("11")
    elif reason == "outside_library":
        cfg["plex"]["history"] = {"libraries": [2]}
    result = plex.PlexSink().send(ev, cfg)
    assert result.get("skipped") or not result["ok"]
    assert not any(x[1] for x in server.calls)


def test_failed_completion_is_retryable(harness):
    server, _, _ = harness
    dest = plex.PlexSink()
    server.fail_write = True
    assert not dest.send(event(), config())["ok"]
    server.fail_write = False
    assert dest.send(event(), config())["ok"]


def test_stale_cached_rating_key_is_revalidated(harness):
    server, _, _ = harness
    dest = plex.PlexSink()
    assert dest.send(event(action="pause", progress=30), config())["ok"]
    server.items["10"].guids = [SimpleNamespace(id="tmdb://999")]
    server.items["20"] = media("20")
    assert dest.send(event(), config())["ok"]
    assert server.items["10"].viewCount == 0
    assert server.items["20"].viewCount == 1


def test_episode_matches_show_ids_and_coordinates(harness):
    server, _, _ = harness
    episode = media("20", "episode", {"tmdb": "100"}, parentIndex=0, index=1, grandparentRatingKey="30")
    show = media("30", "show", {"tmdb": "42"}, episodes=lambda **kw: [episode])
    server.items = {"20": episode, "30": show}
    ev = event(media_type="episode", ids={"tmdb_show": "42", "tmdb": "42"}, season=0, number=1)
    assert plex.PlexSink().send(ev, config())["ok"]
    assert episode.viewCount == 1 and show.viewCount == 0


@pytest.mark.parametrize("season", [0, 1])
@pytest.mark.parametrize("action,progress", [("start", 20), ("stop", 95)])
def test_episode_conflicting_id_accepts_verified_show_and_coordinates(harness, season, action, progress):
    server, _, _ = harness
    episode = media("20", "episode", {"imdb": "tt0000999", "tvdb": "456"}, parentIndex=season, index=2, grandparentRatingKey="30")
    show = media("30", "show", {"tmdb": "42"}, episodes=lambda **kw: [episode])
    server.items = {"20": episode, "30": show}
    ev = event(action=action, progress=progress, media_type="episode", ids={"imdb": "tt0000111", "tvdb": "456", "tmdb_show": "42"}, season=season, number=2)
    assert plex.PlexSink().send(ev, config())["ok"]
    assert episode.viewCount == (1 if action == "stop" else 0)
    assert show.viewCount == 0
    assert any(call[1] for call in server.calls)


@pytest.mark.parametrize("case", ["show", "season", "episode", "missing_show", "missing_coordinates", "parent_type", "library", "ambiguous"])
def test_episode_conflicting_id_fallback_rejects_invalid_matches(harness, case):
    server, _, _ = harness
    episode = media("20", "episode", {"imdb": "tt0000999", "tvdb": "456"}, parentIndex=1, index=2, grandparentRatingKey="30")
    show = media("30", "show", {"tmdb": "42", "tvdb": "43"}, episodes=lambda **kw: [episode])
    server.items = {"20": episode, "30": show}
    cfg = config()
    ids = {"imdb": "tt0000111", "tvdb": "456", "tmdb_show": "42", "tvdb_show": "43"}
    if case == "show":
        show.guids = [SimpleNamespace(id="tmdb://42"), SimpleNamespace(id="tvdb://999")]
    elif case == "season":
        episode.parentIndex = 2
    elif case == "episode":
        episode.index = 3
    elif case == "missing_show":
        ids = {"imdb": "tt0000111", "tvdb": "456"}
    elif case == "parent_type":
        show.type = "movie"
    elif case == "library":
        cfg["plex"]["history"] = {"libraries": [2]}
    elif case == "ambiguous":
        duplicate = copy.deepcopy(episode)
        duplicate.ratingKey = "21"
        server.items["21"] = duplicate
        show.episodes = lambda **kw: [episode, duplicate]
    ev = event(media_type="episode", ids=ids, season=None if case == "missing_coordinates" else 1, number=2)
    assert not plex.PlexSink().send(ev, cfg)["ok"]
    assert not any(call[1] for call in server.calls)


def test_direct_episode_id_keeps_different_numbering_support(harness):
    server, _, _ = harness
    episode = media("20", "episode", {"tmdb": "100"}, parentIndex=2, index=3)
    server.items = {"20": episode}
    assert plex.PlexSink().send(event(media_type="episode", ids={"tmdb": "100"}, season=1, number=2), config())["ok"]
    assert episode.viewCount == 1


def test_duplicate_episode_coordinates_are_rejected(harness):
    server, _, _ = harness
    episodes = [media(key, "episode", {"tmdb": "100"}, parentIndex=1, index=2, grandparentRatingKey="30") for key in ("20", "21")]
    show = media("30", "show", {"tmdb": "42"}, episodes=lambda **kw: episodes)
    server.items = {str(x.ratingKey): x for x in [show, *episodes]}
    result = plex.PlexSink().send(event(media_type="episode", ids={"tmdb_show": "42"}, season=1, number=2), config())
    assert result == {"ok": False, "error": "ambiguous_ids", "retryable": False}
    assert not any(x[1] for x in server.calls)


def test_truncated_id_results_cannot_choose_an_arbitrary_item(harness, monkeypatch):
    server, _, _ = harness
    original = server.query
    def query(path, **kwargs):
        result = original(path, **kwargs)
        result.set("totalSize", "101")
        return result
    monkeypatch.setattr(server, "query", query)
    assert plex.PlexSink().send(event(), config()) == {"ok": False, "error": "ambiguous_ids", "retryable": False}
    assert not any(x[1] for x in server.calls)


@pytest.mark.parametrize("fail", [False, True])
def test_selected_account_scope_covers_writes_and_is_restored(harness, monkeypatch, fail):
    server, _, _ = harness
    active = []
    def enter(adapter):
        active.append("selected-user")
        return True, True, 123, "selected-user"
    def leave(adapter, switched):
        assert switched and active.pop() == "selected-user"
    original = server.query
    def query(path, **kwargs):
        assert active == ["selected-user"]
        return original(path, **kwargs)
    monkeypatch.setattr(plex, "home_scope_enter", enter)
    monkeypatch.setattr(plex, "home_scope_exit", leave)
    monkeypatch.setattr(server, "query", query)
    server.fail_write = fail
    assert plex.PlexSink().send(event(), config())["ok"] is not fail
    assert active == []


@pytest.mark.parametrize("mode,enabled,removed", [("inherit", False, False), ("inherit", True, True), ("off", True, False), ("on", False, True)])
def test_completion_activity_and_watchlist_policy(harness, monkeypatch, mode, enabled, removed):
    records, removals = [], []
    monkeypatch.setattr(common, "record_scrobble_event", lambda *a, **kw: records.append(kw))
    monkeypatch.setattr(common, "remove_across_providers_by_ids", lambda *a, **kw: removals.append((a, kw)))
    cfg = config()
    cfg["scrobble"].update(delete_plex=enabled, delete_plex_types=["movies"])
    cfg["scrobble"]["watch"]["route_options"] = {"auto_remove_watchlist": mode}
    dest = plex.PlexSink()
    assert dest.send(event(action="pause", progress=40), cfg)["ok"]
    assert not records and not removals
    assert dest.send(event(), cfg)["ok"]
    assert dest.send(event(), cfg)["reason"] == "duplicate"
    assert len(records) == 1 and records[0]["target"] == "plex"
    assert bool(removals) is removed
    if removed:
        assert removals == [(({"tmdb": "42"}, "movie"), {"scope": "plex:default"})]


@pytest.mark.parametrize("source,target", [("default", "P01"), ("P01", "default")])
def test_existing_scrob_sink_uses_captured_destination_config(source, target):
    from providers.scrobble.scrob.sink import ScrobSink
    cfg = {"scrob": {"server_url": "http://default", "api_key": "default-key", "instances": {
        "P01": {"server_url": "http://p01", "api_key": "p01-key"}}}}
    view = build_route_cfg(cfg, {"provider": "scrob", "provider_instance": source, "sink": "scrob", "sink_instance": target})
    dest = ScrobSink(cfg_provider=lambda: view, instance_id=target)
    assert view["scrob"]["server_url"] == f"http://{source.lower()}"
    assert dest._block(dest.config)["server_url"] == f"http://{target.lower()}"


def test_dispatcher_does_not_acknowledge_failed_sink_and_allows_retry(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("providers.scrobble.scrobble.time.monotonic", lambda: now[0])
    class Sink:
        attempts = 0
        def send(self, ev, cfg=None):
            self.attempts += 1
            return {"ok": self.attempts > 1}
    dest = Sink()
    dispatcher = Dispatcher([dest], cfg_provider=config)
    assert not dispatcher.dispatch(event())
    assert not dispatcher.dispatch(event())
    assert dest.attempts == 1
    now[0] += 30
    assert dispatcher.dispatch(event())


@pytest.mark.parametrize("result,status", [
    ({"ok": False, "error": "unmatched_in_jellyfin", "retryable": False}, "failed"),
    ({"ok": True, "skipped": True, "reason": "destination_already_watched"}, "skipped"),
    ({"ok": True}, "accepted"),
])
def test_route_logs_destination_outcome_without_scheduler_success(monkeypatch, result, status):
    from providers.scrobble.watch_manager import _SchedulerEventSink

    messages = []
    monkeypatch.setattr("providers.scrobble.scrobble._log", lambda message, *a: messages.append(message))
    monkeypatch.setattr(_SchedulerEventSink, "send", lambda *a, **kw: None)
    class Destination:
        def send(self, event, cfg=None):
            return result
    cfg = config()
    cfg["scrobble"]["watch"].update(route_id="R4", route_provider="plex", route_sink="jellyfin")
    dispatcher = Dispatcher([Destination(), _SchedulerEventSink("R4", "plex", "default")], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event(action="start", progress=19))
    matching = [message for message in messages if f"route R4 plex->jellyfin: {status} start" in message]
    assert len(matching) == 1
    assert not any(": sent " in message for message in messages)
    if status != "accepted":
        assert not any(": accepted " in message for message in messages)
        assert (result.get("reason") or result.get("error")) in matching[0]


@pytest.mark.parametrize("retryable", [False, True])
def test_dispatcher_failure_never_replays_successful_scheduler_delivery(monkeypatch, retryable):
    now = [100.0]
    monkeypatch.setattr("providers.scrobble.scrobble.time.monotonic", lambda: now[0])
    calls = []
    class Destination:
        def send(self, ev, cfg=None):
            calls.append("destination")
            return {"ok": False, "error": "unmatched_in_plex", "retryable": retryable}
    class Scheduler:
        def send(self, ev):
            calls.append("scheduler")
    dispatcher = Dispatcher([Destination(), Scheduler()], cfg_provider=config)
    for _ in range(5):
        dispatcher.dispatch(event())
    assert calls == ["destination", "scheduler"]
    now[0] += 30
    dispatcher.retry_pending()
    assert calls.count("scheduler") == 1
    assert calls.count("destination") == (2 if retryable else 1)


def test_active_destination_completion_retries_without_another_source_event(harness, monkeypatch):
    server, _, _ = harness
    now = [100.0]
    monkeypatch.setattr("providers.scrobble.scrobble.time.monotonic", lambda: now[0])
    server.playing = [SimpleNamespace(ratingKey="10")]
    dispatcher = Dispatcher([plex.PlexSink()], cfg_provider=config)
    assert not dispatcher.dispatch(event())
    assert not any(c[1] for c in server.calls)
    server.playing = []
    now[0] += 30
    dispatcher.retry_pending()
    assert server.items["10"].viewCount == 1
    assert not dispatcher._pending


def test_retry_uses_latest_progress_and_cancels_on_configuration_change(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("providers.scrobble.scrobble.time.monotonic", lambda: now[0])
    calls = []
    class Destination:
        def send(self, ev, cfg=None):
            calls.append(ev.progress)
            return {"ok": False, "retryable": True}
    cfg = config()
    dispatcher = Dispatcher([Destination()], cfg_provider=lambda: cfg)
    dispatcher.dispatch(event(action="start", progress=10))
    dispatcher.dispatch(event(action="stop", progress=95))
    now[0] += 30
    dispatcher.retry_pending()
    assert calls == [10, 95]
    cfg["plex"]["pms_token"] = "replacement-token"
    now[0] += 60
    dispatcher.retry_pending()
    assert calls == [10, 95] and not dispatcher._pending


def test_pending_retries_expire_and_respect_watcher_shutdown(monkeypatch):
    from threading import Event

    now = [100.0]
    monkeypatch.setattr("providers.scrobble.scrobble.time.monotonic", lambda: now[0])
    calls = []
    class Destination:
        def send(self, ev, cfg=None):
            calls.append(ev)
            return {"ok": False, "retryable": True}
    dispatcher = Dispatcher([Destination()], cfg_provider=config)
    dispatcher.dispatch(event())
    now[0] += 30
    stopped = Event()
    stopped.set()
    dispatcher.retry_pending(stopped)
    assert len(calls) == 1
    now[0] += 3600
    dispatcher.retry_pending()
    assert len(calls) == 1 and not dispatcher._pending and not dispatcher._retry_after


def test_successful_sink_dedup_survives_configuration_changes():
    calls = []
    class Destination:
        def send(self, ev, cfg=None):
            calls.append(ev)
    cfg = config()
    dispatcher = Dispatcher([Destination()], cfg_provider=lambda: cfg)
    assert dispatcher.dispatch(event())
    cfg["unrelated_setting"] = True
    assert not dispatcher.dispatch(event())
    assert len(calls) == 1


def test_watcher_shutdown_stops_its_retry_worker():
    from threading import Thread
    from providers.scrobble.watch_manager import WatchGroup, WatchManager, _retry_deliveries

    watcher = SimpleNamespace(stop=lambda: None, is_alive=lambda: False)
    group = WatchGroup("plex", "default", watcher, [], 0)
    app = SimpleNamespace(state=SimpleNamespace(watch_groups={"plex:default": group}))
    thread = Thread(target=_retry_deliveries, args=(group,), daemon=True)
    thread.start()
    WatchManager(app).stop_all()
    thread.join(timeout=2)
    assert group.retry_stop.is_set() and not thread.is_alive()


def test_plex_sink_context_does_not_escape_or_cross_threads(harness, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from providers.sync import _mod_PLEX
    from providers.sync.plex import _common

    monkeypatch.setattr(_common, "_PLEX_CTX", {"baseurl": "http://source", "token": "source", "account_token": "source-cloud"})
    barrier = Barrier(2)
    original = _mod_PLEX.PLEXModule
    def adapter(cfg):
        token = cfg["plex"]["pms_token"]
        _common.configure_plex_context(baseurl=cfg["plex"]["server_url"], token=token, account_token=token)
        barrier.wait(timeout=5)
        assert _common.plex_context()["token"] == token
        assert _common._PLEX_CTX["token"] == "source"
        return original(cfg)
    monkeypatch.setattr(_mod_PLEX, "PLEXModule", adapter)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(plex.PlexSink()._new_adapter, scrobble_sink_config(config(), "plex", inst)) for inst in ("default", "P01")]
        for future in futures:
            future.result(timeout=10)
    assert _common.plex_context() == {"baseurl": "http://source", "token": "source", "account_token": "source-cloud"}


def test_plex_context_is_restored_after_scoped_delivery_failure(harness, monkeypatch):
    from providers.sync.plex import _common

    monkeypatch.setattr(_common, "_PLEX_CTX", {"baseurl": "http://source", "token": "source", "account_token": "cloud"})
    def enter(adapter):
        _common.configure_plex_context(baseurl="http://destination", token="destination-user")
        return True, True, 2, "destination-user"
    def leave(adapter, switched):
        assert switched
        assert _common.plex_context()["token"] == "destination-user"
        _common.configure_plex_context(baseurl="http://destination", token="destination-owner")
    monkeypatch.setattr(plex, "home_scope_enter", enter)
    monkeypatch.setattr(plex, "home_scope_exit", leave)
    harness[0].fail_write = True
    assert not plex.PlexSink().send(event(), config())["ok"]
    assert _common.plex_context()["token"] == "source"


def test_plex_active_session_guard_uses_authenticated_destination_user(harness, monkeypatch):
    from providers.sync import _mod_PLEX

    server, _, _ = harness
    original = _mod_PLEX.PLEXModule
    def adapter(cfg):
        result = original(cfg)
        result.client.user_account_id = 2
        result.client.user_username = "backup"
        return result
    monkeypatch.setattr(_mod_PLEX, "PLEXModule", adapter)
    server.playing = [SimpleNamespace(ratingKey="10", accountID=1, usernames=["source-user"])]
    assert plex.PlexSink().send(event(), config())["ok"]


def test_missing_target_is_cached_and_rechecked_after_expiry(harness, monkeypatch):
    now = [100.0]
    monkeypatch.setattr(common.time, "monotonic", lambda: now[0])
    server, _, _ = harness
    server.items.clear()
    sink = plex.PlexSink()
    assert sink.send(event(), config())["error"] == "unmatched_in_plex"
    calls = len(server.calls)
    server.items["10"] = media()
    assert sink.send(event(), config())["error"] == "unmatched_in_plex"
    assert len(server.calls) == calls
    now[0] += 300
    assert sink.send(event(), config())["ok"]


def test_session_close_failure_does_not_prevent_reconnection(harness, monkeypatch):
    now = [100.0]
    monkeypatch.setattr(common.time, "monotonic", lambda: now[0])
    server, configs, _ = harness
    def failed_close():
        raise RuntimeError("closed transport")
    server._session.close = failed_close
    sink = plex.PlexSink()
    assert sink.send(event(action="pause", progress=10), config())["ok"]
    now[0] += common.CACHE_TTL
    assert sink.send(event(), config())["ok"]
    assert len(configs) == 2


def test_management_rejects_self_routes_before_credentials():
    from api.scrobblerManagementAPI import ValidationFailure, _validate_route
    for provider in ("plex", "jellyfin", "emby", "kodi", "scrob"):
        assert same_scrobble_endpoint(provider.upper(), " Default ", provider, "default")
    with pytest.raises(ValidationFailure) as exc:
        _validate_route(config(), {"provider": "plex", "sink": "plex"})
    assert "same_source_destination" in str(exc.value.errors)


def test_management_webhook_rejects_implicit_self_instance(monkeypatch):
    from api import scrobblerManagementAPI as api
    cfg = config()
    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api, "_save_and_runtime", lambda *a: pytest.fail("must not save invalid route"))
    response = api.api_profile_webhook_save(None, {"provider": "plex", "sinks": ["plex"]})
    assert response.status_code == 400
    assert b"same_source_destination" in response.body


def test_missing_destination_instance_does_not_fall_back_to_default():
    from providers.webhooks.config import sink_configured
    assert not sink_configured(config(), "plex", "missing")


@pytest.mark.parametrize("source,target", [("default", "P01"), ("P01", "default")])
def test_management_accepts_cross_instance_routes(monkeypatch, source, target):
    from api import scrobblerManagementAPI as api
    cfg = config()
    route = api._validate_route(cfg, {"provider": "plex", "provider_instance": source, "sink": "plex", "sink_instance": target})
    assert route["sink_instance"] == target
    cfg["scrobble"]["webhook"] = {}
    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api, "_ensure_media_profile_webhook_ids", lambda *a, **k: [])
    monkeypatch.setattr(api, "_save_and_runtime", lambda req, before, after: {"ok": True, "config": after})
    result = api.api_profile_webhook_save(None, {"provider": "plex", "provider_instance": source, "sinks": ["plex"], "sink_instances": {"plex": target}})
    assert result.status_code == 200
    node = json.loads(result.body)["config"]["scrobble"]["webhook"]["profiles"]["plex"][source]
    assert node["sink_instances"] == {"plex": target}


def test_watcher_self_route_is_blocked_even_with_a_custom_sink():
    class Sink:
        def send(self, event):
            pytest.fail("self route must not dispatch")
    cfg = build_route_cfg(config(), {"provider": "plex", "sink": "plex"})
    assert not Dispatcher([Sink()], cfg_provider=lambda: cfg).dispatch(event())


def test_destination_change_does_not_reuse_completion_acknowledgement(harness):
    server, configs, _ = harness
    dest = plex.PlexSink()
    assert dest.send(event(), config())["ok"]
    cfg = config()
    cfg["plex"]["pms_token"] = "replacement-token"
    server.items["10"].viewCount = 0
    assert dest.send(event(), cfg) == {"ok": True}
    assert len(configs) == 2


def test_existing_watched_item_is_not_unwatched_by_partial_scrobble(harness):
    server, _, _ = harness
    server.items["10"].viewCount = 1
    result = plex.PlexSink().send(event(action="pause", progress=30), config())
    assert result["reason"] == "destination_already_watched"
    assert not any(x[1] for x in server.calls)


def test_scrobbler_instance_dropdowns():
    import shutil
    import subprocess
    from pathlib import Path
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is required for modal behavior checks")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([node, "--test", "tests/scrobbler-sink-instances.test.mjs"], cwd=root,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
