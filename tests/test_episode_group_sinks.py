# tests/test_episode_group_sinks.py
# CrossWatch - Grouped episode delivery through provider sinks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy
from importlib import import_module
from types import SimpleNamespace

import pytest

from cw_platform.local_db import manual_policy
from providers.scrobble.episode_groups import send_grouped
from services import activity
from test_episode_group_scrobbles import event, setup
from test_media_server_scrobble_sinks import config as media_config, kodi, kodi_row, media_server, row
from test_plex_scrobble_sink import config as plex_config, harness, media
from test_wetrakr_sync import env
from test_wetrakr_ratings_progress import features
from test_wetrakr_scrobble import live


def target(config_base, setup, provider, instance="default"):
    cfg, _ = setup
    policy = manual_policy.load_policy(config_base)
    group = policy["pairs"]["p1"]["episode_groups"][0]
    group["target"].update(provider=provider.upper(), instance=instance)
    cfg["pairs"][0].update(target=provider.upper(), target_instance=instance)
    cfg["scrobble"]["watch"].update(route_sink=provider, route_sink_instance=instance)
    if provider == "plex":
        group["source"]["provider"] = "TRAKT"
        cfg["pairs"][0]["source"] = "TRAKT"
        cfg["scrobble"]["watch"]["route_provider"] = "trakt"
    manual_policy.save_policy(config_base, policy)
    return cfg


@pytest.fixture(autouse=True)
def isolate_activity(monkeypatch):
    monkeypatch.setattr(activity, "add_event", lambda *a, **k: None)


def exercise_retry(cfg, send, fail):
    assert send_grouped(event(), cfg, send)["reason"] == "episode_group_waiting_for_parts"
    fail(True)
    assert not send_grouped(event(3), cfg, send)["ok"]
    fail(False)
    assert send_grouped(event(3), cfg, send)["ok"]
    assert send_grouped(event(3), cfg, send)["reason"] == "episode_group_already_completed"


@pytest.mark.parametrize("provider", ["floppy", "punchplay", "scrob", "bingebase", "bingebase_kodi", "flicklist"])
def test_webhook_sink_confirms_group_only_after_success(config_base, setup, monkeypatch, provider):
    kind = provider.split("_")[0]
    cfg = target(config_base, setup, kind)
    cfg[kind] = {
        "floppy": {"server_url": "http://floppy.local", "api_token": "token"},
        "punchplay": {"access_token": "token", "device_id": "device"},
        "scrob": {"server_url": "http://scrob.local", "api_key": "key", "username": "user", "password": "pass",
                  "access_token": "token", "expires_at": 4102444800},
        "bingebase": {"webhook_url": "https://bingebase.com/api/webhooks/jellyfin?token=test", "api_key": "key"},
        "flicklist": {"api_key": "key"},
    }[kind]
    if provider == "bingebase_kodi":
        cfg[kind]["webhook_url"] = "https://bingebase.com/webhooks/kodi/test"
    module = import_module(f"providers.scrobble.{kind}.sink")
    for name in ("record_watch", "_auto_remove_across"):
        if hasattr(module, name):
            monkeypatch.setattr(module, name, lambda *a, **k: None)
    if hasattr(module, "_SESSIONS"):
        monkeypatch.setattr(module, "_SESSIONS", {})
    cls = {"floppy": "FloppySink", "punchplay": "PunchPlaySink", "scrob": "ScrobSink",
           "bingebase": "BingeBaseSink", "flicklist": "FlickListSink"}[kind]
    sink = getattr(module, cls)(cfg_provider=lambda: cfg)
    calls, failing = [], [False]
    def post(*args, **kwargs):
        body = kwargs.get("json", args[-1] if kind == "scrob" else None)
        calls.append(deepcopy(body))
        if kind == "floppy":
            if failing[0]:
                raise RuntimeError("transport failed")
            return {"detail": "ok"}
        return SimpleNamespace(status_code=503 if failing[0] else 200, headers={}, text="{}", json=lambda: {"status": "ok"})
    if kind == "bingebase":
        monkeypatch.setattr(sink.session, "post", post)
    else:
        name = {"floppy": "api_post", "punchplay": "punchplay_request", "scrob": "webhook_post", "flicklist": "flicklist_request"}[kind]
        monkeypatch.setattr(module, name, post)
    exercise_retry(cfg, sink.send, lambda value: failing.__setitem__(0, value))
    assert len(calls) == 2
    transient = {"played_at", "playback_session_id", "event_id", "event_created_at"}
    assert {k: v for k, v in calls[0].items() if k not in transient} == {k: v for k, v in calls[1].items() if k not in transient}
    body = calls[-1]
    if kind == "floppy":
        assert body["action"] == "stop" and body["completed"] is True
        assert body["season_number"] == 1 and body["episode_number"] == 2
        assert "duration_seconds" not in body
    elif kind == "punchplay":
        assert body["watched"] is True and body["progress"] == 1
    elif kind == "scrob":
        assert body["method"] == "Player.OnStop" and body["params"]["data"]["end"] is True
    elif provider == "bingebase":
        assert body["Percentage"] == 100 and body["Item"]["IndexNumber"] == 2
    elif provider == "bingebase_kodi":
        assert body["progress"]["percent"] == 100
    else:
        assert body["progress"] == 100


def test_local_tracker_group_completion_retries_failed_history(config_base, setup, monkeypatch):
    from providers.scrobble.crosswatch import sink as module

    cfg = target(config_base, setup, "crosswatch")
    calls, failing = [], [False]
    def add(view, items, **kwargs):
        calls.append((deepcopy(items), kwargs["feature"]))
        return {"ok": not failing[0], "count": 0 if failing[0] else 1}
    monkeypatch.setattr(module, "CROSSWATCH_OPS", SimpleNamespace(is_configured=lambda cfg: True, add=add,
                        remove=lambda *a, **k: {"ok": True}))
    monkeypatch.setattr(module, "_tmdb_enrich", lambda *a, **k: {})
    monkeypatch.setattr(module, "record_watch", lambda *a, **k: None)
    sink = module.CrossWatchSink(cfg_provider=lambda: cfg)
    exercise_retry(cfg, sink.send, lambda value: failing.__setitem__(0, value))
    assert len(calls) == 2 and all(feature == "history" for _, feature in calls)
    assert calls[-1][0][0]["show_ids"] == {"tmdb": "100"}
    assert calls[-1][0][0]["episode"] == 2


def test_media_server_group_completion_resolves_destination_episode(config_base, setup, media_server):
    provider, cls, http, _ = media_server
    cfg = target(config_base, setup, provider)
    cfg[provider] = media_config(provider)[provider]
    http.rows = {"30": row("30", "Series", {"Tmdb": "100"}),
                 "20": row("20", "Episode", {"Tmdb": "200"}, SeriesId="30", ParentIndexNumber=1, IndexNumber=2)}
    sink = cls()
    exercise_retry(cfg, lambda ev: sink.send(ev, cfg), lambda value: setattr(http, "fail", value))
    assert http.rows["20"]["UserData"]["Played"]
    assert not http.rows["30"]["UserData"]["Played"]


def test_kodi_group_completion_resolves_destination_episode(config_base, setup, kodi):
    cls, client, _ = kodi
    cfg = target(config_base, setup, "kodi")
    cfg["kodi"] = media_config("kodi")["kodi"]
    client.rows = {"show:30": kodi_row(30, "show", uniqueid={"tmdb": "100"}),
                   "episode:20": kodi_row(20, "episode", uniqueid={"tmdb": "200"}, tvshowid=30, season=1, episode=2)}
    sink = cls()
    exercise_retry(cfg, lambda ev: sink.send(ev, cfg), lambda value: setattr(client, "fail", value))
    assert client.rows["episode:20"]["playcount"] == 1
    assert client.rows["show:30"]["playcount"] == 0


def test_plex_group_completion_resolves_destination_episode(config_base, setup, harness):
    from providers.scrobble.plex.sink import PlexSink

    server, _, _ = harness
    cfg = target(config_base, setup, "plex")
    cfg["plex"] = plex_config()["plex"]
    part = media("20", "episode", {"tmdb": "200"}, parentIndex=1, index=2, grandparentRatingKey="30")
    show = media("30", "show", {"tmdb": "100"}, episodes=lambda **kw: [part])
    server.items = {"20": part, "30": show}
    sink = PlexSink()
    exercise_retry(cfg, lambda ev: sink.send(ev, cfg), lambda value: setattr(server, "fail_write", value))
    assert part.viewCount == 1 and show.viewCount == 0


@pytest.mark.parametrize("provider,cls_name", [("myanimelist", "MyAnimeListSink"), ("kitsu", "KitsuSink")])
def test_anime_history_sink_receives_mapped_coordinates(config_base, setup, monkeypatch, provider, cls_name):
    cfg = target(config_base, setup, provider)
    module = import_module(f"providers.scrobble.{provider}.sink")
    calls, failing = [], [False]
    def add(feature, items):
        calls.append((feature, deepcopy(items)))
        return {"unresolved": [{"reason": "request_failed"}]} if failing[0] else {"count": 1}
    monkeypatch.setattr(module.OPS, "is_configured", lambda cfg: True)
    monkeypatch.setattr(module, f"{provider.upper()}Module", lambda cfg: SimpleNamespace(add=add))
    monkeypatch.setattr(module, "record_watch", lambda *a, **k: None)
    sink = getattr(module, cls_name)(cfg_provider=lambda: cfg)
    exercise_retry(cfg, sink.send, lambda value: failing.__setitem__(0, value))
    assert len(calls) == 2 and calls[0] == calls[1]
    assert calls[-1][0] == "history"
    assert calls[-1][1][0]["show_ids"] == {"tmdb": "100"}
    assert calls[-1][1][0]["episode"] == 2


@pytest.mark.parametrize("already_watched", [False, True])
def test_anilist_group_accepts_confirmed_progress(config_base, setup, monkeypatch, already_watched):
    from collections import OrderedDict
    from providers.scrobble.anilist import sink as module

    cfg = target(config_base, setup, "anilist")
    cfg.update(anilist={"access_token": "token"}, anime_mapping={"enabled": True})
    monkeypatch.setattr(module, "_DONE", OrderedDict())
    monkeypatch.setattr(module, "AnimeMappingService", lambda cfg: SimpleNamespace(ready=lambda: True))
    monkeypatch.setattr(module, "ANILISTModule", lambda cfg: SimpleNamespace(client=object()))
    monkeypatch.setattr(module, "record_watch", lambda *a, **k: None)
    items, calls, failing = [], [], [False]
    monkeypatch.setattr(module, "resolve_target", lambda cfg, item: items.append(item) or (123, item["episode"]))
    def apply(client, iid, episode):
        calls.append((iid, episode))
        if failing[0]:
            raise RuntimeError("transport failed")
        return {"ok": True, "skipped": already_watched, "reason": "not_newer" if already_watched else "updated"}
    monkeypatch.setattr(module, "apply_progress", apply)
    sink = module.AniListSink(cfg_provider=lambda: cfg)
    exercise_retry(cfg, sink.send, lambda value: failing.__setitem__(0, value))
    assert calls == [(123, 2), (123, 2)]
    assert all(item["show_ids"] == {"tmdb": "100"} for item in items)


def test_wetrakr_group_waits_for_confirmed_stop(config_base, setup, live, monkeypatch):
    from providers.scrobble.wetrakr import sink as module

    cfg = target(config_base, setup, "wetrakr", "P01")
    cfg["wetrakr"] = live.view["wetrakr"]
    policy = manual_policy.load_policy(config_base)
    member = policy["pairs"]["p1"]["episode_groups"][0]["target"]["episodes"][0]
    member.update(show_ids={"tmdb": "1396"}, episode=1)
    manual_policy.save_policy(config_base, policy)
    original, failing = module.request, [False]
    def request(adapter, method, path, **kwargs):
        if failing[0] and method == "POST":
            raise module.WeTrakrSyncError("request_rejected", status=429, retry_after=30)
        return original(adapter, method, path, **kwargs)
    monkeypatch.setattr(module, "request", request)
    monkeypatch.setattr(module, "record_scrobble_event", activity.record_scrobble_event)
    exercise_retry(cfg, lambda ev: live.sink.send(ev, cfg), lambda value: failing.__setitem__(0, value))
    assert len(live.server.history) == 1
    writes = [(path, args["json"]) for method, path, args in live.server.calls if method == "POST"]
    assert len(writes) == 1 and writes[0][0] == "/scrobble/stop" and writes[0][1]["progress"] == 100


@pytest.mark.parametrize("provider", ["wetrakr", "simkl"])
def test_group_respects_provider_minimum_completion(config_base, setup, provider):
    cfg = target(config_base, setup, provider)
    cfg["scrobble"]["trakt"]["watched_at"] = 50
    cfg["scrobble"]["watch"]["route_options"] = {"watch": {"simkl_rewatches": True}}
    calls = []
    send = lambda ev: calls.append(ev) or {"ok": True}
    assert send_grouped(event(progress=79), cfg, send)["reason"] == "episode_group_completion_only"
    send_grouped(event(3), cfg, send)
    assert not calls
    assert send_grouped(event(progress=80), cfg, send)["ok"]
    assert len(calls) == 1


def test_other_sinks_do_not_accept_anilist_progress_reason(setup):
    cfg, _ = setup
    send = lambda ev: {"ok": True, "skipped": True, "reason": "not_newer"}
    send_grouped(event(), cfg, send)
    assert not send_grouped(event(3), cfg, send)["ok"]


@pytest.mark.parametrize("provider", ["myanimelist", "kitsu", "wetrakr", "plex", "jellyfin", "emby", "kodi"])
def test_group_uses_same_shared_threshold_as_destination(config_base, setup, provider):
    cfg = target(config_base, setup, provider)
    cfg["scrobble"][provider] = {"watched_at": 50}
    calls = []
    send = lambda ev: calls.append(ev) or {"ok": True}
    assert send_grouped(event(progress=85), cfg, send)["reason"] == "episode_group_completion_only"
    send_grouped(event(3), cfg, send)
    assert not calls
    send_grouped(event(progress=90), cfg, send)
    assert len(calls) == 1


@pytest.mark.parametrize("provider", ["wetrakr", "simkl"])
@pytest.mark.parametrize("threshold", [float("nan"), float("inf")])
def test_provider_minimum_does_not_hide_invalid_threshold(config_base, setup, provider, threshold):
    cfg = target(config_base, setup, provider)
    cfg["scrobble"]["trakt"]["watched_at"] = threshold
    cfg["scrobble"]["watch"]["route_options"] = {"watch": {"simkl_rewatches": True}}
    result = send_grouped(event(progress=100), cfg, lambda ev: pytest.fail("Invalid threshold completed a group"))
    assert result["reason"] == "episode_group_completion_only"
