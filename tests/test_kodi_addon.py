# tests/test_kodi_addon.py
# CrossWatch - Kodi add-on event source tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from providers.scrobble.kodi import addon
from providers.scrobble.kodi import watch as kodi_watch
from providers.scrobble.kodi.watch import KodiWatchService
from providers.sync.kodi import _common as kodi_common

TOKEN = "tok-living-room-0123456789"
BEDROOM_TOKEN = "tok-bedroom-0123456789abcd"


class FakeDispatcher:
    def __init__(self, accept: bool = True) -> None:
        self.accept = accept
        self.checked: list[Any] = []
        self.events: list[Any] = []

    def accepts(self, event: Any) -> bool:
        self.checked.append(event)
        return self.accept

    def dispatch(self, event: Any) -> bool:
        self.events.append(event)
        return True


def route(route_id: str, sink_instance: str, whitelist: list[str] | None = None, instance: str = "default") -> dict[str, Any]:
    return {
        "id": route_id,
        "enabled": True,
        "provider": "kodi",
        "provider_instance": instance,
        "sink": "trakt",
        "sink_instance": sink_instance,
        "filters": {"username_whitelist": list(whitelist or [])},
    }


def make_cfg(routes: list[dict[str, Any]] | None = None, *, tokens: bool = True, kodi: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "security": {"webhook_ids": {"kodiwatcher:default": TOKEN, "kodiwatcher:bedroom": BEDROOM_TOKEN} if tokens else {}},
        "kodi": dict(kodi or {}),
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True},
            "watch": {"routes": list(routes or [])},
            "trakt": {"force_stop_at": 95},
        },
    }


def make_app(cfg: dict[str, Any], dispatchers: dict[str, FakeDispatcher], instance: str = "default") -> SimpleNamespace:
    runners = [SimpleNamespace(route_id=rid, dispatcher=disp) for rid, disp in dispatchers.items()]
    group = SimpleNamespace(provider="kodi", provider_instance=instance, routes=runners)
    return SimpleNamespace(state=SimpleNamespace(watch_groups={f"kodi:{instance}": group}))


def playback(event: str = "progress", **media: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "type": "episode",
        "title": "The Expanse",
        "episode_title": "Home",
        "year": 2015,
        "season": 2,
        "episode": 5,
        "ids": {"tmdb_show": "63639", "tvdb_show": "280619", "imdb_show": "tt3230854"},
        "percent": 42.7,
        "position_ms": 1180000,
        "duration_ms": 2760000,
        "file": "smb://nas/tv/The Expanse/Season 02/S02E05.mkv",
        "source": "library",
    }
    base.update(media)
    return {
        "version": 1,
        "event": event,
        "event_id": "7c1e4f0a",
        "session_id": "kodi-livingroom-1",
        "sent_at": "2026-09-19T20:15:03Z",
        "addon_version": "1.0.0",
        "device": {"id": "b8f1c2d4-livingroom", "name": "Living room"},
        "viewers": ["anna", "tom"],
        "media": {k: v for k, v in base.items() if v is not None},
    }


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    monkeypatch.setattr(addon, "_state_file", lambda: tmp_path / "kodi_addon.json")
    monkeypatch.setattr(addon, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(addon, "_cw_update_payload", lambda *a, **k: None)
    addon.reset_state()
    yield
    addon.reset_state()


def test_token_decides_enablement_and_source_ready() -> None:
    cfg = make_cfg()
    assert addon.instance_enabled(cfg, "default") is True
    assert addon.source_ready(cfg, "default") is True
    assert addon.instance_for_token(cfg, TOKEN) == "default"
    assert addon.instance_for_token(cfg, BEDROOM_TOKEN) == "bedroom"
    assert addon.instance_for_token(cfg, "nope") is None

    off = make_cfg(tokens=False)
    assert addon.instance_enabled(off, "default") is False
    assert addon.source_ready(off, "default") is False
    assert addon.source_ready(make_cfg(tokens=False, kodi={"server": "http://kodi", "connection_verified": True}), "default") is True


def test_masked_token_does_not_enable_instance() -> None:
    cfg = make_cfg()
    cfg["security"]["webhook_ids"]["kodiwatcher:default"] = "•" * 8
    assert addon.instance_enabled(cfg, "default") is False
    assert addon.instance_for_token(cfg, "•" * 8) is None


def test_set_instance_enabled_mints_keeps_and_removes_token() -> None:
    cfg: dict[str, Any] = {}
    first = addon.set_instance_enabled(cfg, "den", True)
    assert len(first) >= addon.MIN_TOKEN_LENGTH
    assert addon.set_instance_enabled(cfg, "den", True) == first
    assert addon.set_instance_enabled(cfg, "den", True, regenerate=True) != first
    addon.set_instance_enabled(cfg, "den", False)
    assert addon.instance_enabled(cfg, "den") is False


def test_clean_ids_drops_placeholders_and_marks_episode_ids() -> None:
    ids = addon.clean_ids(
        {"tmdb_show": "63639", "imdb_show": "None", "tvdb": "5534478", "imdb": "0", "unknown": "123", "tmdb": -1, "trakt": "9"},
        "episode",
    )
    assert ids == {"tmdb_show": "63639", "tvdb_episode": "5534478"}
    assert addon.clean_ids({"imdb": "tt2543164", "tmdb": "nan", "tmdb_show": "1"}, "movie") == {"imdb": "tt2543164"}


def test_build_event_maps_progress_to_start() -> None:
    item, reason = addon.build_event(playback("progress"), "default")
    assert reason == ""
    assert item is not None and item.deliverable is True
    ev = item.event
    assert (ev.action, ev.media_type, ev.season, ev.number) == ("start", "episode", 2, 5)
    assert ev.progress == pytest.approx(42.7)
    assert ev.server_uuid == "b8f1c2d4-livingroom"
    assert ev.session_key == "kodi:default:addon:kodi-livingroom-1"
    assert ev.raw["item"]["file"] == "smb://nas/tv/The Expanse/Season 02/S02E05.mkv"
    assert item.viewers == ["anna", "tom"]


def test_build_event_unknown_percent_is_never_zero_progress_write() -> None:
    start, _ = addon.build_event(playback("start", percent=None, position_ms=None, duration_ms=None), "default")
    assert start is not None and start.deliverable is False

    stop, _ = addon.build_event(playback("stop", percent=None, position_ms=None, duration_ms=None), "default")
    assert stop is not None and stop.deliverable is False

    done, _ = addon.build_event(playback("stop", percent=None, position_ms=60000, duration_ms=None, completed=True), "default")
    assert done is not None and done.deliverable is True
    assert done.event.progress == 100.0
    assert done.event.raw["_cw_preserve_stop"] is True


def test_build_event_rejects_missing_ids_and_other_media() -> None:
    assert addon.build_event(playback(ids={"unknown": "1", "imdb_show": "null"}), "default") == (None, "no_ids")
    assert addon.build_event(playback(type="channel"), "default") == (None, "unsupported_media")
    assert addon.build_event({"event": "seek"}, "default") == (None, "unsupported_event")


def test_build_event_replayed_stop_carries_sent_at() -> None:
    body = playback("stop", percent=97.0)
    body["replayed"] = True
    sent = int(datetime(2026, 9, 19, 20, 15, 3, tzinfo=timezone.utc).timestamp())
    item, _ = addon.build_event(body, "default", now=sent + 86400)
    assert item is not None and item.replayed is True
    assert item.event.raw["_cw_watched_at"] == sent

    early, _ = addon.build_event(body, "default", now=sent - 60)
    assert early is not None and "_cw_watched_at" not in early.event.raw

    live, _ = addon.build_event(playback("stop", percent=97.0), "default", now=sent + 86400)
    assert live is not None and "_cw_watched_at" not in live.event.raw


def test_handle_gives_each_route_its_own_viewer_once() -> None:
    cfg = make_cfg([route("R1", "anna", ["anna"]), route("R2", "tom", ["Tom"]), route("R3", "kim", ["kim"]), route("R4", "all")])
    disps = {rid: FakeDispatcher() for rid in ("R1", "R2", "R3", "R4")}
    out = addon.handle(make_app(cfg, disps), cfg, "default", playback("progress"))

    assert out["ok"] is True and out["ignored"] is False
    assert [e.account for e in disps["R1"].events] == ["anna"]
    assert [e.account for e in disps["R2"].events] == ["tom"]
    assert disps["R3"].events == [] and disps["R3"].checked == []
    assert [e.account for e in disps["R4"].events] == ["anna"]


def test_handle_empty_viewers_leaves_whitelists_to_the_dispatcher() -> None:
    cfg = make_cfg([route("R1", "anna", ["anna"])])
    disp = FakeDispatcher(accept=False)
    body = playback("progress")
    body["viewers"] = []
    out = addon.handle(make_app(cfg, {"R1": disp}), cfg, "default", body)

    assert [e.account for e in disp.checked] == [None]
    assert disp.events == []
    assert out["ignored"] is True and out["error"] == "no_matching_route"


def test_handle_unknown_progress_updates_card_without_dispatch(monkeypatch) -> None:
    cards: list[dict[str, Any]] = []
    monkeypatch.setattr(addon, "_cw_update_payload", lambda *a, **k: cards.append({"args": a, "kwargs": k}))
    cfg = make_cfg([route("R1", "anna", ["anna"])])
    disp = FakeDispatcher()
    out = addon.handle(make_app(cfg, {"R1": disp}), cfg, "default", playback("start", percent=None, position_ms=None, duration_ms=None))

    assert out["ignored"] is False
    assert disp.events == []
    assert len(cards) == 1 and cards[0]["kwargs"]["state"] == "playing"


def test_handle_replayed_stop_does_not_touch_card(monkeypatch) -> None:
    touched: list[Any] = []
    monkeypatch.setattr(addon, "_cw_update", lambda *a, **k: touched.append(a))
    monkeypatch.setattr(addon, "_cw_update_payload", lambda *a, **k: touched.append(a))
    cfg = make_cfg([route("R1", "anna", ["anna"])])
    disp = FakeDispatcher()
    body = playback("stop", percent=96.0)
    body["replayed"] = True
    addon.handle(make_app(cfg, {"R1": disp}), cfg, "default", body)

    assert len(disp.events) == 1 and disp.events[0].action == "stop"
    assert touched == []


def test_handle_respects_scrobble_library_scope() -> None:
    cfg = make_cfg([route("R1", "all")], kodi={"scrobble": {"libraries": ["smb://nas/movies"]}})
    disp = FakeDispatcher()
    out = addon.handle(make_app(cfg, {"R1": disp}), cfg, "default", playback("progress"))

    assert out["ignored"] is True and out["error"] == "outside_library_scope"
    assert disp.checked == []


def test_handle_without_routes_is_ignored() -> None:
    cfg = make_cfg([])
    out = addon.handle(SimpleNamespace(state=SimpleNamespace(watch_groups={})), cfg, "default", playback("progress"))
    assert out["ignored"] is True and out["error"] == "no_routes"
    assert addon.is_active("default") is True


def test_ping_lists_routes_and_stores_viewers() -> None:
    cfg = make_cfg([route("R1", "anna", ["anna"]), route("R2", "tom", ["tom"]), route("R3", "all")])
    disps = {rid: FakeDispatcher() for rid in ("R1", "R2", "R3")}
    body = {"version": 1, "event": "ping", "addon_version": "1.2.0", "device": {"id": "dev-1", "name": "Living room"}, "viewers": ["anna", "tom"], "pkc_skipped": 3}
    out = addon.handle(make_app(cfg, disps), cfg, "default", body)

    assert out["ignored"] is False
    assert out["instance"] == "Living room"
    assert [(r["id"], r["sink"], r["viewers"]) for r in out["routes"]] == [("R1", "trakt", ["anna"]), ("R2", "trakt", ["tom"]), ("R3", "trakt", ["anna", "tom"])]
    assert all(disp.checked == [] for disp in disps.values())
    assert addon.known_viewers("default") == ["anna", "tom"]
    assert addon.device_uuid("default") == "dev-1"

    info = addon.status(cfg, "default", "https://cw.example")
    assert info["mode"] == "addon" and info["pkc_skipped"] == 3 and info["addon_version"] == "1.2.0"
    assert info["paired"] is True
    assert addon.status(cfg, "bedroom", "https://cw.example")["paired"] is False
    assert info["url"] == f"https://cw.example/webhook/kodiwatcher?token={TOKEN}"


def test_ping_without_routes_is_a_plain_ok() -> None:
    cfg = make_cfg([])
    out = addon.handle(SimpleNamespace(state=SimpleNamespace(watch_groups={})), cfg, "default", {"event": "ping", "viewers": ["anna"]})
    assert out["ok"] is True and out["ignored"] is False
    assert out["routes"] == [] and "error" not in out

    cfg["scrobble"]["sources"]["watcher"] = False
    off = addon.handle(SimpleNamespace(state=SimpleNamespace(watch_groups={})), cfg, "default", {"event": "ping", "viewers": ["anna"]})
    assert off["ignored"] is True and off["error"] == "watcher_disabled"


def test_is_active_expires_after_fresh_window() -> None:
    addon.mark_seen("default", {"event": "progress"}, now=1000.0)
    assert addon.is_active("default", now=1000.0 + addon.ADDON_FRESH_SECONDS - 1) is True
    assert addon.is_active("default", now=1000.0 + addon.ADDON_FRESH_SECONDS + 1) is False
    assert addon.is_active("bedroom", now=1000.0) is False


def test_state_survives_reload(tmp_path) -> None:
    addon.mark_seen("default", {"event": "ping", "viewers": ["anna"], "device": {"id": "dev-9"}}, now=time.time())
    with addon._STATE_LOCK:
        addon._STATE.clear()
        addon._STATE_LOADED = False
    assert addon.known_viewers("default") == ["anna"]
    assert addon.device_uuid("default") == "dev-9"
    assert addon.is_active("default") is True


def _watch_cfg(connected: bool) -> dict[str, Any]:
    kodi = {"server": "http://kodi.local:8080", "connection_verified": True} if connected else {}
    cfg = make_cfg([], kodi=kodi)
    cfg["scrobble"]["watch"] = {"filters": {}}
    return cfg


def test_watcher_skips_polling_while_addon_is_fresh() -> None:
    calls: list[str] = []
    service = KodiWatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: _watch_cfg(True), instance_id="default", quiet_startup=True)
    service._rpc = lambda method, params=None: calls.append(method) or []  # type: ignore[method-assign]
    service._sessions["s"] = {"session_key": "s"}

    addon.mark_seen("default", {"event": "ping", "viewers": []})
    assert service._tick() is False
    assert calls == [] and service._sessions == {}

    addon.mark_seen("default", {"event": "progress"}, now=time.time() - addon.ADDON_FRESH_SECONDS - 5)
    service._tick()
    assert calls == ["Player.GetActivePlayers"]


def test_watcher_never_polls_an_addon_only_instance() -> None:
    calls: list[str] = []
    service = KodiWatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: _watch_cfg(False), instance_id="default", quiet_startup=True)
    service._rpc = lambda method, params=None: calls.append(method) or []  # type: ignore[method-assign]
    assert service._tick() is False
    assert calls == []
    assert service._addon_enabled(_watch_cfg(False)) is True


def test_watcher_reuses_addon_device_id_as_server_uuid() -> None:
    service = KodiWatchService(dispatcher=FakeDispatcher(), cfg_provider=lambda: _watch_cfg(True), instance_id="default", quiet_startup=True)
    hashed = service._server_uuid(_watch_cfg(True))
    assert hashed.startswith("kodi:")
    addon.mark_seen("default", {"event": "ping", "viewers": [], "device": {"id": "dev-1"}})
    assert service._server_uuid(_watch_cfg(True)) == "dev-1"
    off = _watch_cfg(True)
    off["security"]["webhook_ids"].pop("kodiwatcher:default")
    assert service._server_uuid(off) == hashed


def test_uniqueid_normalizers_drop_placeholder_ids() -> None:
    raw = {"imdb": "None", "tmdb": "0", "tvdb": "-1", "tvshow.tmdb": "63639", "anidb": "NaN"}
    assert kodi_common.normalize_uniqueids(raw) == {"tmdb": "63639"}
    assert kodi_watch._normalize_uniqueids(raw, "episode") == {"tmdb_show": "63639"}
    assert kodi_watch._normalize_uniqueids({"imdb": "tt2543164", "tmdb": "null"}, "movie") == {"imdb": "tt2543164"}


def test_strip_path_userinfo() -> None:
    assert kodi_common.strip_path_userinfo("smb://user:p@ss@nas/tv/a.mkv") == "smb://nas/tv/a.mkv"
    assert kodi_common.strip_path_userinfo("smb://nas/tv/a.mkv") == "smb://nas/tv/a.mkv"
    assert kodi_common.strip_path_userinfo("plugin://plugin.video.x/?a=b@c") == "plugin://plugin.video.x/?a=b@c"
    assert kodi_common.strip_path_userinfo("/movies/a@b.mkv") == "/movies/a@b.mkv"


def _webhook_client(monkeypatch, cfg: dict[str, Any], app_state: SimpleNamespace | None = None) -> TestClient:
    from api import scrobbleAPI

    monkeypatch.setattr(scrobbleAPI, "load_config", lambda: cfg)
    app = FastAPI()
    app.include_router(scrobbleAPI.router)
    if app_state is not None:
        app.state.watch_groups = app_state.state.watch_groups
    return TestClient(app)


def test_webhook_rejects_everything_when_no_instance_has_the_addon(monkeypatch) -> None:
    res = _webhook_client(monkeypatch, make_cfg(tokens=False)).post("/webhook/kodiwatcher", json=playback(), headers={"X-CrossWatch-Token": TOKEN})
    assert res.status_code == 401 and res.json()["error"] == "invalid_token"
    assert addon.is_active("default") is False


def test_webhook_rejects_bad_token(monkeypatch) -> None:
    client = _webhook_client(monkeypatch, make_cfg())
    assert client.post("/webhook/kodiwatcher", json=playback()).status_code == 401
    assert client.post("/webhook/kodiwatcher?token=wrong", json=playback()).status_code == 401


def test_webhook_accepts_header_and_query_token(monkeypatch) -> None:
    cfg = make_cfg([route("R1", "anna", ["anna"])])
    disp = FakeDispatcher()
    client = _webhook_client(monkeypatch, cfg, make_app(cfg, {"R1": disp}))

    by_header = client.post("/webhook/kodiwatcher", json=playback("start"), headers={"X-CrossWatch-Token": TOKEN})
    by_query = client.post(f"/webhook/kodiwatcher?token={TOKEN}", json=playback("pause"))

    assert by_header.status_code == 200 and by_header.json()["ignored"] is False
    assert by_query.status_code == 200 and by_query.json()["ignored"] is False
    assert "crosswatch_version" in by_header.json()
    assert [e.action for e in disp.events] == ["start", "pause"]


def test_webhook_bad_json_is_ignored_not_an_error(monkeypatch) -> None:
    res = _webhook_client(monkeypatch, make_cfg()).post("/webhook/kodiwatcher", content=b"{nope", headers={"X-CrossWatch-Token": TOKEN})
    assert res.status_code == 200
    assert res.json()["error"] == "invalid_payload"


def _auth_client(monkeypatch, cfg: dict[str, Any]) -> TestClient:
    from api.authenticationAPI import register_auth

    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda *_a, **_k: None)
    app = FastAPI()
    register_auth(app)
    return TestClient(app)


def test_addon_api_is_available_without_any_switch(monkeypatch) -> None:
    client = _auth_client(monkeypatch, {"kodi": {}})
    res = client.get("/api/kodi/addon")
    assert res.status_code == 200 and res.json()["enabled"] is False and res.json()["paired"] is False


def test_addon_api_enables_and_disables_an_instance(monkeypatch) -> None:
    cfg: dict[str, Any] = {"kodi": {}}
    client = _auth_client(monkeypatch, cfg)

    assert client.get("/api/kodi/addon").json()["enabled"] is False
    on = client.post("/api/kodi/addon", json={"enabled": True}).json()
    assert on["enabled"] is True and "/webhook/kodiwatcher?token=" in on["url"]
    assert on["mode"] == "waiting"
    again = client.post("/api/kodi/addon", json={"enabled": True, "regenerate": True}).json()
    assert again["url"] != on["url"]
    off = client.post("/api/kodi/addon", json={"enabled": False}).json()
    assert off["enabled"] is False and off["url"] == ""


def test_kodi_users_offers_addon_viewers_without_jsonrpc(monkeypatch) -> None:
    cfg = make_cfg()
    addon.mark_seen("default", {"event": "ping", "viewers": ["anna", "tom"]})
    res = _auth_client(monkeypatch, cfg).get("/api/kodi/users")
    assert res.status_code == 200
    assert [u["name"] for u in res.json()["users"]] == ["anna", "tom"]

    assert _auth_client(monkeypatch, make_cfg(tokens=False)).get("/api/kodi/users").status_code == 401


def test_pair_code_shape_and_single_use() -> None:
    code, ttl = addon.create_pair_code("default", now=1000.0)
    assert len(code) == addon.PAIR_CODE_LENGTH and set(code) <= set(addon.PAIR_ALPHABET)
    assert not set("0O1I") & set(addon.PAIR_ALPHABET)
    assert ttl == 600
    assert addon.active_pair_code("default", now=1100.0) == (code, 500)

    assert addon.redeem_pair_code(f" {code[:3].lower()} {code[3:].lower()} ", "kodi-1", now=1100.0) == ("default", "")
    assert addon.redeem_pair_code(code, "kodi-1", now=1101.0) == (None, "invalid_code")
    assert addon.active_pair_code("default", now=1101.0) == ("", 0)


def test_pair_code_expires_and_is_replaced_per_instance() -> None:
    first, _ = addon.create_pair_code("default", now=1000.0)
    other, _ = addon.create_pair_code("bedroom", now=1000.0)
    second, _ = addon.create_pair_code("default", now=1001.0)

    assert addon.redeem_pair_code(first, "a", now=1002.0) == (None, "invalid_code")
    assert addon.redeem_pair_code(other, "a", now=1002.0) == ("bedroom", "")
    assert addon.redeem_pair_code(second, "a", now=1001.0 + addon.PAIR_TTL_SECONDS + 1) == (None, "invalid_code")


def test_pair_rate_limit_is_per_client() -> None:
    code, _ = addon.create_pair_code("default", now=1000.0)
    for _ in range(addon.PAIR_MAX_FAILURES):
        assert addon.redeem_pair_code("ZZZZZZ", "attacker", now=1001.0) == (None, "invalid_code")
    assert addon.redeem_pair_code(code, "attacker", now=1002.0) == (None, "rate_limited")
    assert addon.redeem_pair_code(code, "kodi-1", now=1002.0) == ("default", "")
    again, _ = addon.create_pair_code("default", now=1003.0)
    assert addon.redeem_pair_code(again, "attacker", now=1001.0 + addon.PAIR_TTL_SECONDS + 1) == ("default", "")


def test_clean_base_url_accepts_only_plain_addresses() -> None:
    assert addon.clean_base_url("http://192.168.1.10:8787/") == "http://192.168.1.10:8787"
    assert addon.clean_base_url("https://cw.example/webhook/kodiwatcher") == "https://cw.example"
    assert addon.clean_base_url("https://cw.example/webhook/kodiwatcher/pair") == "https://cw.example"
    assert addon.clean_base_url("192.168.1.10:8787") == ""
    assert addon.clean_base_url("http://cw.example/?token=abc") == ""
    assert addon.clean_base_url("http://") == ""
    assert addon.link_params("http://cw:8787", "abc 234") == ["action=link", "url=http://cw:8787/webhook/kodiwatcher", "code=ABC234"]


def test_pair_webhook_swaps_code_for_token(monkeypatch) -> None:
    client = _webhook_client(monkeypatch, make_cfg())
    code, _ = addon.create_pair_code("bedroom")

    wrong = client.post("/webhook/kodiwatcher/pair", json={"code": "ZZZZZZ"})
    assert wrong.status_code == 401 and wrong.json() == {"ok": False, "error": "invalid_code", "crosswatch_version": wrong.json()["crosswatch_version"]}

    ok = client.post("/webhook/kodiwatcher/pair", json={"code": code.lower()})
    assert ok.status_code == 200
    body = ok.json()
    assert body["ok"] is True and body["token"] == BEDROOM_TOKEN and body["instance"] == "bedroom"
    assert body["url"].endswith("/webhook/kodiwatcher") and "token" not in body["url"]

    assert client.post("/webhook/kodiwatcher/pair", json={"code": code}).status_code == 401
    assert client.post("/webhook/kodiwatcher/pair", content=b"{nope").status_code == 401


def test_pair_webhook_rate_limits_wrong_codes(monkeypatch) -> None:
    client = _webhook_client(monkeypatch, make_cfg())
    for _ in range(addon.PAIR_MAX_FAILURES):
        assert client.post("/webhook/kodiwatcher/pair", json={"code": "ZZZZZZ"}).status_code == 401
    limited = client.post("/webhook/kodiwatcher/pair", json={"code": "ZZZZZZ"})
    assert limited.status_code == 429 and limited.json()["error"] == "rate_limited"


def test_pair_api_turns_the_addon_on_and_returns_a_code(monkeypatch) -> None:
    cfg: dict[str, Any] = {"kodi": {}}
    client = _auth_client(monkeypatch, cfg)

    assert client.get("/api/kodi/addon").json()["enabled"] is False
    first = client.post("/api/kodi/addon/pair").json()
    assert first["enabled"] is True and first["paired"] is False
    assert len(first["pair_code"]) == 6 and 0 < first["pair_expires_in"] <= 600
    assert "/webhook/kodiwatcher?token=" in first["url"]
    assert first["address"] == "http://testserver"
    assert client.get("/api/kodi/addon").json()["pair_code"] == first["pair_code"]
    assert client.post("/api/kodi/addon/pair").json()["pair_code"] != first["pair_code"]

    client.post("/api/kodi/addon", json={"enabled": False})
    assert addon.active_pair_code("default") == ("", 0)


def test_link_api_pushes_endpoint_and_token_over_jsonrpc(monkeypatch) -> None:
    calls: list[tuple[str, Any]] = []

    def rpc(_server: str, method: str, **kwargs: Any) -> Any:
        calls.append((method, kwargs.get("params")))
        if method == "Addons.GetAddonDetails":
            return {"addon": {"addonid": "service.crosswatch", "enabled": True}}
        return "OK"

    def pushes() -> list[Any]:
        return [params for method, params in calls if method == "Addons.ExecuteAddon"]

    monkeypatch.setattr("providers.sync.kodi._common.jsonrpc_call", rpc)
    cfg = make_cfg(kodi={"server": "http://kodi.local:8080", "connection_verified": True})
    client = _auth_client(monkeypatch, cfg)

    res = client.post("/api/kodi/addon/link", json={"address": "http://192.168.1.10:8787/"})
    assert res.status_code == 200 and res.json()["address"] == "http://192.168.1.10:8787"
    assert calls[0][0] == "Addons.GetAddonDetails"
    sent = pushes()[0]
    assert sent["addonid"] == "service.crosswatch" and sent["wait"] is False
    assert sent["params"][:2] == ["action=link", "url=http://192.168.1.10:8787/webhook/kodiwatcher"]
    link_code = sent["params"][2].removeprefix("code=")
    assert len(link_code) == addon.PAIR_CODE_LENGTH and TOKEN not in str(sent)
    assert client.get("/api/kodi/addon").json()["pair_code"] == ""
    assert addon.redeem_pair_code(link_code, "kodi-1") == ("default", "")

    assert client.post("/api/kodi/addon/link", json={"address": "nas:8787"}).status_code == 400
    assert client.post("/api/kodi/addon/link", json={}).json()["address"] == "http://testserver"
    assert len(pushes()) == 2

    fresh: dict[str, Any] = {"kodi": {"server": "http://kodi.local:8080", "connection_verified": True}}
    first = _auth_client(monkeypatch, fresh).post("/api/kodi/addon/link", json={"address": "http://cw:8787"})
    assert first.status_code == 200 and addon.instance_enabled(fresh, "default") is True
    fresh_code = pushes()[-1]["params"][2].removeprefix("code=")
    assert addon.instance_token(fresh, "default") not in str(pushes()[-1])
    assert addon.pair(fresh, fresh_code, "http://cw:8787", "kodi-1")[1]["token"] == addon.instance_token(fresh, "default")


def test_link_api_needs_jsonrpc_and_an_installed_addon(monkeypatch) -> None:
    from providers.auth._auth_KODI import KodiAuthError

    assert _auth_client(monkeypatch, make_cfg()).post("/api/kodi/addon/link", json={}).status_code == 400

    calls: list[str] = []

    def missing(_server: str, method: str, **_kwargs: Any) -> Any:
        calls.append(method)
        raise KodiAuthError("Kodi JSON-RPC error", reason="jsonrpc_error")

    monkeypatch.setattr("providers.sync.kodi._common.jsonrpc_call", missing)
    fresh: dict[str, Any] = {"kodi": {"server": "http://kodi.local:8080", "connection_verified": True}}
    res = _auth_client(monkeypatch, fresh).post("/api/kodi/addon/link", json={})
    assert res.status_code == 400 and res.json()["reason"] == "addon_missing" and "not installed" in res.json()["error"]
    assert calls == ["Addons.GetAddonDetails"]
    assert addon.instance_enabled(fresh, "default") is False

    monkeypatch.setattr("providers.sync.kodi._common.jsonrpc_call", lambda _s, _m, **_k: {"addon": {"enabled": False}})
    off = _auth_client(monkeypatch, fresh).post("/api/kodi/addon/link", json={})
    assert off.status_code == 400 and off.json()["reason"] == "addon_disabled"
    assert addon.instance_enabled(fresh, "default") is False

    def down(*_a: Any, **_k: Any) -> Any:
        raise KodiAuthError("Kodi server is unreachable", reason="unreachable")

    monkeypatch.setattr("providers.sync.kodi._common.jsonrpc_call", down)
    assert _auth_client(monkeypatch, fresh).post("/api/kodi/addon/link", json={}).status_code == 502


def test_route_source_accepts_addon_only_instance() -> None:
    from api.scrobblerManagementAPI import _scrobble_source_connected

    assert _scrobble_source_connected(make_cfg(), "kodi", "default") is True
    assert _scrobble_source_connected(make_cfg(tokens=False), "kodi", "default") is False
    assert _scrobble_source_connected({"kodi": {"server": "http://kodi", "connection_verified": True}}, "kodi", "default") is True


def test_media_server_played_at_uses_replayed_watch_time() -> None:
    from providers.scrobble import _media_server
    from providers.scrobble.scrobble import ScrobbleEvent

    def event(raw: dict[str, Any]) -> ScrobbleEvent:
        return ScrobbleEvent(action="stop", media_type="movie", ids={"imdb": "tt1"}, title="A", year=2020, season=None, number=None,
                             progress=100.0, account=None, server_uuid=None, session_key="s", raw=raw)

    now = int(time.time())
    assert _media_server._played_at(event({"_cw_watched_at": now - 3600})) == now - 3600
    assert abs(_media_server._played_at(event({})) - now) <= 2
    assert abs(_media_server._played_at(event({"_cw_watched_at": now + 9999})) - now) <= 2


def test_kodi_disconnect_also_turns_the_addon_off(monkeypatch) -> None:
    cfg = make_cfg(kodi={"server": "http://kodi.local:8080", "connection_verified": True})
    addon.create_pair_code("default")
    client = _auth_client(monkeypatch, cfg)

    assert client.post("/api/kodi/disconnect").json()["ok"] is True
    assert addon.instance_enabled(cfg, "default") is False
    assert addon.instance_enabled(cfg, "bedroom") is True
    assert addon.active_pair_code("default") == ("", 0)


def test_settings_save_keeps_the_addon_token_when_the_ui_posts_its_mask(monkeypatch) -> None:
    import copy

    from api import configAPI as cfg_api
    from cw_platform import config_base

    current = make_cfg()
    saved: dict[str, Any] = {}
    monkeypatch.setattr(
        cfg_api,
        "_env",
        lambda: {
            "CW": None,
            "cfg_base": config_base,
            "load": lambda: copy.deepcopy(current),
            "save": lambda cfg: saved.update(cfg),
            "prune": lambda *_: None,
            "ensure": lambda *_: None,
            "norm_pair": lambda *_: None,
            "probes_cache": None,
            "probes_status_cache": None,
            "scheduler": None,
        },
    )
    mask = "•" * 8
    posted = {"security": {"webhook_ids": {"kodiwatcher:default": mask, "kodiwatcher:bedroom": mask}}}

    res = cfg_api.api_config_save(SimpleNamespace(app=SimpleNamespace()), posted)

    assert res["ok"] is True
    assert addon.instance_token(saved, "default") == TOKEN
    assert addon.instance_token(saved, "bedroom") == BEDROOM_TOKEN


def test_paired_survives_silence_and_resets_on_regenerate(monkeypatch) -> None:
    cfg: dict[str, Any] = {"kodi": {}}
    client = _auth_client(monkeypatch, cfg)
    client.post("/api/kodi/addon/pair")
    addon.mark_seen("default", {"event": "ping", "viewers": []}, now=time.time() - addon.ADDON_FRESH_SECONDS - 60)

    quiet = client.get("/api/kodi/addon").json()
    assert quiet["paired"] is True and quiet["active"] is False

    again = client.post("/api/kodi/addon", json={"enabled": True, "regenerate": True}).json()
    assert again["enabled"] is True and again["paired"] is False


def test_link_code_and_typed_code_do_not_replace_each_other() -> None:
    typed, _ = addon.create_pair_code("default", now=1000.0)
    linked, _ = addon.create_pair_code("default", link=True, now=1000.0)
    relinked, _ = addon.create_pair_code("default", link=True, now=1001.0)

    assert addon.active_pair_code("default", now=1002.0)[0] == typed
    assert addon.redeem_pair_code(linked, "a", now=1002.0) == (None, "invalid_code")
    assert addon.redeem_pair_code(relinked, "a", now=1002.0) == ("default", "")
    assert addon.redeem_pair_code(typed, "a", now=1002.0) == ("default", "")


def test_old_addon_switch_is_removed_from_saved_config() -> None:
    from cw_platform import config_base

    assert "kodi_addon" not in config_base.DEFAULT_CFG["runtime"]
    cfg: dict[str, Any] = {"runtime": {"kodi_addon": True, "debug": False}, "mobile_auth": {}}
    assert config_base.cleanup_obsolete_config_keys(cfg) == ["mobile_auth", "runtime.kodi_addon"]
    assert cfg == {"runtime": {"debug": False}}
    assert config_base.cleanup_obsolete_config_keys({"ui": {}}) == []


def rate(rating: Any = 8, viewers: list[str] | None = None, **media: Any) -> dict[str, Any]:
    body = playback("rate", percent=None, position_ms=None, duration_ms=None, file=None, **media)
    body["rating"] = rating
    body["viewers"] = ["anna"] if viewers is None else viewers
    return body


def sink_route(route_id: str, sink: str, whitelist: list[str] | None = None) -> dict[str, Any]:
    out = route(route_id, "default", whitelist)
    out["sink"] = sink
    return out


@pytest.fixture()
def sent_ratings(monkeypatch) -> list[dict[str, Any]]:
    from providers.scrobble import route_ratings

    calls: list[dict[str, Any]] = []

    def fake_send(cfg: Any, sink: Any, instance: Any, item: Any, rating: Any) -> dict[str, Any]:
        calls.append({"sink": sink, "instance": instance, "item": dict(item), "rating": rating})
        return {"ok": True}

    monkeypatch.setattr(route_ratings, "send", fake_send)
    return calls


def test_parse_rating_accepts_only_whole_numbers_from_0_to_10() -> None:
    assert [addon.parse_rating(v) for v in (0, 1, 10, "7", 8.0)] == [0, 1, 10, 7, 8]
    assert [addon.parse_rating(v) for v in (None, True, -1, 11, 7.5, "x", "")] == [None] * 7


def test_rating_support_per_destination() -> None:
    from providers.scrobble import route_ratings

    from providers.scrobble.routes import ROUTE_RATING_SINKS

    assert set(route_ratings.RATING_SINKS) == ROUTE_RATING_SINKS | {"plex"}
    for sink in route_ratings.RATING_SINKS:
        assert route_ratings.supports(sink, "movie") is True
    for sink in ("emby", "jellyfin", "kodi", "bingebase", ""):
        assert route_ratings.supports(sink, "movie") is False
    movie_only = {s for s in route_ratings.RATING_SINKS if not route_ratings.supports(s, "episode")}
    assert movie_only == {"simkl", "anilist", "kitsu", "myanimelist"}


def test_rate_goes_to_the_viewers_route_with_show_scoped_episode(sent_ratings) -> None:
    cfg = make_cfg([sink_route("R1", "trakt", ["anna"]), sink_route("R2", "trakt", ["tom"]), sink_route("R3", "plex")])
    disps = {rid: FakeDispatcher() for rid in ("R1", "R2", "R3")}
    out = addon.handle(make_app(cfg, disps), cfg, "default", rate(8))

    assert out["ignored"] is False
    assert [(r["id"], r["sink"], r["ok"]) for r in out["rated"]] == [("R1", "trakt", True), ("R3", "plex", True)]
    assert [c["sink"] for c in sent_ratings] == ["trakt", "plex"]
    item = sent_ratings[0]["item"]
    assert item["type"] == "episode" and item["season"] == 2 and item["episode"] == 5 and item["rating"] == 8.0
    assert item["show_ids"] == {"tmdb": "63639", "tvdb": "280619", "imdb": "tt3230854"}
    assert "ids" not in item and item["series_title"] == "The Expanse" and item["title"] == "Home"
    assert all(disp.events == [] for disp in disps.values())


def test_rate_zero_removes_and_movie_ids_are_passed(sent_ratings) -> None:
    cfg = make_cfg([sink_route("R1", "simkl", ["anna"])])
    body = rate(0, type="movie", title="Arrival", year=2016, season=None, episode=None, episode_title=None, ids={"imdb": "tt2543164", "tmdb": "329865"})
    out = addon.handle(make_app(cfg, {"R1": FakeDispatcher()}), cfg, "default", body)

    assert out["ignored"] is False and out["rated"] == [{"id": "R1", "sink": "simkl", "ok": True}]
    assert sent_ratings[0]["rating"] is None
    assert sent_ratings[0]["item"] == {"type": "movie", "ids": {"imdb": "tt2543164", "tmdb": "329865"}, "title": "Arrival", "year": 2016}


def test_rate_without_a_destination_that_takes_it(sent_ratings) -> None:
    cfg = make_cfg([sink_route("R1", "simkl", ["anna"]), sink_route("R2", "emby", ["anna"])])
    disps = {"R1": FakeDispatcher(), "R2": FakeDispatcher()}
    out = addon.handle(make_app(cfg, disps), cfg, "default", rate(9))

    assert out["ignored"] is True and out["error"] == "no_rating_target"
    assert sent_ratings == []

    other = addon.handle(make_app(cfg, disps), cfg, "default", rate(9, viewers=["kim"]))
    assert other["error"] == "no_matching_route"


def test_rate_rejects_bad_values_and_missing_ids(sent_ratings) -> None:
    cfg = make_cfg([sink_route("R1", "trakt")])
    app = make_app(cfg, {"R1": FakeDispatcher()})

    assert addon.handle(app, cfg, "default", rate(11))["error"] == "invalid_rating"
    assert addon.handle(app, cfg, "default", rate("great"))["error"] == "invalid_rating"
    assert addon.handle(app, cfg, "default", rate(8, ids={"unknown": "1"}))["error"] == "no_ids"
    assert sent_ratings == []
    assert addon.is_active("default") is True


def test_same_rating_twice_in_a_row_is_sent_once(sent_ratings) -> None:
    cfg = make_cfg([sink_route("R1", "trakt", ["anna"])])
    app = make_app(cfg, {"R1": FakeDispatcher()})

    first = addon.handle(app, cfg, "default", rate(8))
    again = addon.handle(app, cfg, "default", rate(8))
    changed = addon.handle(app, cfg, "default", rate(6))

    assert first["rated"][0].get("repeated") is None
    assert again["rated"][0]["repeated"] is True and again["ignored"] is False
    assert [c["rating"] for c in sent_ratings] == [8.0, 6.0]
    assert changed["rated"][0]["ok"] is True


def test_rate_reports_a_failed_write(monkeypatch) -> None:
    from providers.scrobble import route_ratings

    monkeypatch.setattr(route_ratings, "send", lambda *a, **k: {"ok": False, "error": "boom"})
    cfg = make_cfg([sink_route("R1", "trakt", ["anna"])])
    out = addon.handle(make_app(cfg, {"R1": FakeDispatcher()}), cfg, "default", rate(8))
    assert out["ignored"] is False and out["rated"] == [{"id": "R1", "sink": "trakt", "ok": False}]


def test_route_ratings_send_uses_the_sync_writer_and_anilist_for_movies(monkeypatch) -> None:
    from providers.scrobble import route_ratings
    from providers.scrobble.anilist import ratings as anilist_ratings
    from providers.scrobble.plex import ratings_sync

    seen: list[tuple[Any, ...]] = []
    monkeypatch.setattr(ratings_sync, "send_rating", lambda provider, cfg, inst, item, rating, sinks=None: seen.append((provider, inst, rating, tuple(sinks))) or {"ok": True})
    monkeypatch.setattr(anilist_ratings, "send_plex_rating", lambda cfg, inst, kind, md, ids, show_ids, rating: seen.append(("anilist", inst, kind, md, ids, rating)) or {"ok": True})

    movie = route_ratings.build_item("movie", ids={"imdb": "tt2543164"}, title="Arrival", year=2016, rating=7.0)
    episode = route_ratings.build_item("episode", show_ids={"tmdb": "63639"}, title="Home", series_title="The Expanse", season=2, episode=5, rating=7.0)

    assert route_ratings.send({}, "plex", "P1", movie, 7.0) == {"ok": True}
    assert route_ratings.send({}, "anilist", "default", movie, 7.0) == {"ok": True}
    assert route_ratings.send({}, "simkl", "default", episode, 7.0)["skipped"] is True
    assert route_ratings.send({}, "kitsu", "default", episode, 7.0)["skipped"] is True
    assert route_ratings.send({}, "emby", "default", movie, 7.0)["skipped"] is True
    assert seen == [
        ("plex", "P1", 7.0, route_ratings.OPS_SINKS),
        ("anilist", "default", "movie", {"title": "Arrival"}, {"imdb": "tt2543164"}, 7.0),
    ]
