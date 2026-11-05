# CrossWatch test scripts
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, cast

import pytest

SERVER = "http://tofa.local:33333"
PROFILE = "p-1"
USER = "u-1"


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return "" if self.payload is None else "x"

    def json(self) -> Any:
        return self.payload


def _movie(fid: str, mid: str, tmdb: int, title: str) -> dict[str, Any]:
    return {"media_file_id": fid, "media_id": mid, "media_type": "movie", "title": title, "edition": None, "available": True,
            "tmdb_id": tmdb, "imdb_id": f"tt{tmdb}", "tvdb_id": None}


def _episode(fid: str, s: int, e: int) -> dict[str, Any]:
    return {"media_file_id": fid, "media_id": "show-1", "media_type": "tv", "title": "Gamma", "edition": None, "available": True,
            "tmdb_id": 300, "imdb_id": "tt300", "tvdb_id": "3000", "episode_id": f"ep-{s}-{e}", "season_number": s, "episode_number": e,
            "tmdb_episode_id": 9000 + e}


FILES = {"lib-movies": [_movie("f-alpha", "m-alpha", 100, "Alpha")], "lib-tv": [_episode("f-e1", 1, 1), _episode("f-e2", 1, 2)]}


class FakeServer:
    def __init__(self) -> None:
        self.routes: dict[tuple[str, str], Callable[[dict[str, Any]], ResponseStub]] = {}
        self.calls: list[dict[str, Any]] = []
        self.progress: dict[str, dict[str, Any]] = {}
        self.on("GET", "/libraries", lambda kw: ResponseStub(200, [{"id": lid, "name": lid, "media_type": "movie"} for lid in FILES]))
        self.on("GET", "/media/files", lambda kw: ResponseStub(200, {"items": FILES[kw["params"]["library_id"]], "next_after": None}))
        self.on("GET", "/media/m-alpha", lambda kw: ResponseStub(200, {"files": [{"id": "f-alpha", "duration_ms": 6_000_000}]}))
        self.on("GET", "/media/show-1", lambda kw: ResponseStub(200, {"files": [], "seasons": [{"episodes": [
            {"files": [{"id": "f-e1", "duration_ms": 2_700_000}]}, {"files": [{"id": "f-e2", "duration_ms": 2_700_000}]}]}]}))
        for fid in ("f-alpha", "f-e1", "f-e2"):
            self.on("GET", f"/media/{fid}/progress", lambda kw, fid=fid: ResponseStub(200, {"media_file_id": fid, "position_ms": 0, "completed": False,
                                                                                           **self.progress.get(fid, {})}))
        self.on("GET", "/system/playback/sessions", lambda kw: ResponseStub(200, []))

    def on(self, method: str, path: str, handler: Callable[[dict[str, Any]], ResponseStub]) -> None:
        self.routes[(method, path)] = handler

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v1", 1)[1]
        kwargs.setdefault("params", {})
        self.calls.append({"method": method, "path": path, **kwargs})
        handler = self.routes.get((method, path))
        if handler is None:
            raise AssertionError(f"unexpected {method} {path}")
        return handler(kwargs)

    def count(self, method: str, path: str) -> int:
        return sum(1 for c in self.calls if c["method"] == method and c["path"] == path)


def _cfg() -> dict[str, Any]:
    return {
        "tofa": {"server_url": SERVER, "server_id": "srv-1", "access_token": "a", "refresh_token": "r", "expires_at": 4_000_000_000,
                 "profile_id": PROFILE, "profile_name": "Alice", "user_id": USER, "profile_primary": True},
        "scrobble": {"enabled": True, "sources": {"watcher": True}},
    }


@pytest.fixture()
def server(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> Any:
    from providers.scrobble import _media_server
    from providers.sync.tofa import _common

    fake = FakeServer()
    monkeypatch.setattr(_common, "request_with_auth", fake)
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(_media_server, "record_watch", lambda *a, **k: None)
    monkeypatch.setattr(_media_server, "record_scrobble_event", lambda *a, **k: None)
    monkeypatch.setattr(_media_server, "auto_remove", lambda *a, **k: None)
    monkeypatch.setattr("providers.scrobble.tofa.sink.note_write", lambda *a, **k: None)
    _common._LIBRARIES.clear()
    yield fake
    _common._LIBRARIES.clear()


class Collector:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def dispatch(self, ev: Any) -> bool:
        self.events.append(ev)
        return True


def _watcher(monkeypatch: pytest.MonkeyPatch, cfg: dict[str, Any] | None = None) -> tuple[Any, Collector]:
    from providers.scrobble.tofa import watch

    monkeypatch.setattr(watch, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(watch, "_cw_update_payload", lambda *a, **k: None)
    monkeypatch.setattr(watch, "COMPLETION_RECHECK_SECONDS", 0.0)
    collector = Collector()
    active = cfg or _cfg()
    return watch.TofaWatchService(dispatcher=collector, cfg_provider=lambda: active), collector


def _started(sid: str = "s-1", fid: str = "f-alpha", profile: str | None = PROFILE, title: str = "Alpha") -> str:
    return json.dumps({"type": "StreamStarted", "session_id": sid, "user_id": USER, "username": "alice", "profile_id": profile,
                       "profile_name": "Alice", "media_title": title, "media_id": "m-alpha", "media_file_id": fid, "play_method": "DirectPlay"})


def _progress(pct: float, *, sid: str = "s-1", paused: bool = False, duration_ms: int = 6_000_000) -> str:
    ticks = int(duration_ms * pct / 100) * 10000
    return json.dumps({"type": "StreamProgress", "session_id": sid, "user_id": USER, "progress_pct": pct, "position_ticks": ticks, "is_paused": paused})


def _stopped(sid: str = "s-1") -> str:
    return json.dumps({"type": "StreamStopped", "session_id": sid, "user_id": USER, "username": "alice", "media_title": "Alpha", "media_id": "m-alpha", "reason": None})


def _iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def test_watcher_emits_start_pause_resume_stop_for_connected_profile_only(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    svc, got = _watcher(monkeypatch)
    for msg in (
        _started(sid="s-kid", profile="p-kid"), _progress(10.0, sid="s-kid"),
        _started(), _progress(10.0), _progress(10.2, paused=True), _progress(10.2, paused=True), _progress(10.3), _progress(30.0),
        json.dumps({"type": "AppLog", "message": "noise"}), _stopped(),
    ):
        svc.handle_message(msg)

    assert [e.action for e in got.events] == ["start", "pause", "start", "stop"]
    first = got.events[0]
    assert first.media_type == "movie" and first.ids == {"tmdb": "100", "imdb": "tt100"} and first.title == "Alpha"
    assert first.account == "Alice" and first.progress == pytest.approx(10.0)
    assert first.duration_ms == 6_000_000 and first.position_ms == 600_000
    assert first.raw["provider"] == "tofa" and first.raw["content_id"] == "f-alpha" and first.server_uuid.startswith("tofa:")
    assert got.events[-1].progress == pytest.approx(30.0)
    assert server.count("GET", "/media/m-alpha") == 1


def test_watcher_episode_ids_and_auto_next_session_reuse(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    svc, got = _watcher(monkeypatch)
    for msg in (_started(fid="f-e1", title="Gamma - S01E01"), _progress(40.0, duration_ms=2_700_000),
                _started(fid="f-e2", title="Gamma - S01E02"), _progress(5.0, duration_ms=2_700_000)):
        svc.handle_message(msg)

    actions = [(e.action, e.number) for e in got.events]
    assert actions == [("start", 1), ("stop", 1), ("start", 2)]
    ev = got.events[0]
    assert ev.media_type == "episode" and ev.season == 1 and ev.title == "Gamma"
    assert ev.ids == {"tmdb_show": "300", "imdb_show": "tt300", "tvdb_show": "3000", "tmdb_episode": "9001"}


@pytest.mark.parametrize(("completed", "offset", "expected"), [
    (True, 30.0, 100.0),
    (True, -3600.0, 88.0),
    (False, 30.0, 88.0),
])
def test_stop_uses_tofa_completion_in_session(monkeypatch: pytest.MonkeyPatch, server: FakeServer, completed: bool, offset: float, expected: float) -> None:
    from providers.scrobble.floppy.sink import _payload

    svc, got = _watcher(monkeypatch)
    svc.handle_message(_started())
    svc.handle_message(_progress(88.0))
    started = svc._sessions["s-1"]["started_at"]
    server.progress["f-alpha"] = {"completed": completed, "completed_at": _iso(started + offset) if completed else None}
    svc.handle_message(_stopped())

    stop = got.events[-1]
    assert stop.action == "stop" and stop.progress == pytest.approx(expected)
    assert stop.raw["_cw_preserve_stop"] is (expected == 100.0)
    assert stop.position_ms == (6_000_000 if expected == 100.0 else 5_280_000)
    assert (_payload(stop) or {})["completed"] is (expected == 100.0)


def test_credits_skip_below_threshold_is_completed_when_tofa_marked_it(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    svc, got = _watcher(monkeypatch)
    svc.handle_message(_started())
    for pct in (20.0, 45.0, 70.0, 84.0):
        svc.handle_message(_progress(pct))
    server.progress["f-alpha"] = {"completed": True, "completed_at": _iso(time.time())}
    svc.handle_message(_stopped())
    assert got.events[-1].progress == 100.0


@pytest.mark.parametrize(("pct", "expected", "checks"), [(88.0, 100.0, 2), (20.0, 20.0, 1)])
def test_stop_rechecks_late_completion_only_past_half(monkeypatch: pytest.MonkeyPatch, server: FakeServer, pct: float, expected: float, checks: int) -> None:
    svc, got = _watcher(monkeypatch)
    svc.handle_message(_started())
    svc.handle_message(_progress(pct))
    seen: list[int] = []

    def progress(kw: dict[str, Any]) -> ResponseStub:
        seen.append(1)
        done = len(seen) > 1
        return ResponseStub(200, {"completed": done, "completed_at": _iso(time.time()) if done else None})

    server.on("GET", "/media/f-alpha/progress", progress)
    svc.handle_message(_stopped())
    assert got.events[-1].progress == pytest.approx(expected) and len(seen) == checks


def test_rewatch_reset_by_tofa_is_not_upgraded(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    svc, got = _watcher(monkeypatch)
    svc.handle_message(_started())
    svc.handle_message(_progress(20.0))
    server.progress["f-alpha"] = {"completed": False, "completed_at": None, "position_ms": 1_200_000}
    svc.handle_message(_stopped())
    assert got.events[-1].progress == pytest.approx(20.0)


def test_skips_recheck_when_watcher_stops(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    svc, _got = _watcher(monkeypatch)
    svc._stop.set()
    assert svc._completed_after_stop({"file_id": "f-alpha", "started_at": time.time()}, 88.0) is False
    assert server.count("GET", "/media/f-alpha/progress") == 1


def test_primary_profile_accepts_events_without_profile_id(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    svc, got = _watcher(monkeypatch)
    svc.handle_message(_started(profile=None))
    svc.handle_message(_progress(10.0))
    assert [e.action for e in got.events] == ["start"]

    cfg = _cfg()
    cfg["tofa"]["profile_primary"] = False
    other, got2 = _watcher(monkeypatch, cfg)
    other.handle_message(_started(profile=None))
    other.handle_message(_progress(10.0))
    assert got2.events == []


def test_scrobble_whitelist_skips_other_libraries(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    cfg = _cfg()
    cfg["tofa"]["scrobble"] = {"libraries": ["lib-tv"]}
    svc, got = _watcher(monkeypatch, cfg)
    svc.handle_message(_started())
    svc.handle_message(_progress(10.0))
    svc.handle_message(_stopped())
    assert got.events == []


def test_quiet_and_vanished_sessions_are_closed(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    from providers.scrobble.tofa import watch

    svc, got = _watcher(monkeypatch)
    svc.handle_message(_started())
    svc.handle_message(_progress(10.0))
    svc._sessions["s-1"]["last_event_at"] -= watch.STALE_PLAYING_SECONDS + 1
    svc._expire_stale(time.time())
    assert got.events[-1].action == "stop" and not svc._sessions

    svc.handle_message(_started(sid="s-2"))
    svc.handle_message(_progress(10.0, sid="s-2"))
    svc._reconcile()
    assert got.events[-1].action == "stop" and not svc._sessions


class FakeSocket:
    def __init__(self, reply: dict[str, Any]) -> None:
        self.reply = reply
        self.sent: list[dict[str, Any]] = []
        self.closed = False

    def send(self, data: str) -> None:
        self.sent.append(json.loads(data))

    def recv(self) -> str:
        return json.dumps(self.reply)

    def close(self) -> None:
        self.closed = True


def test_connect_sends_profile_unlock_and_flags_locked_profile(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    from providers.auth import _auth_TOFA as auth
    from providers.scrobble.tofa import watch

    cfg = _cfg()
    cfg["tofa"].update(profile_locked=True, profile_token="ptok", profile_token_expires_at=4_000_000_000)
    seen: dict[str, Any] = {}
    sock = FakeSocket({"type": "auth_ok"})
    monkeypatch.setattr(watch.websocket, "create_connection", lambda url, **kw: seen.update(url=url, **kw) or sock)
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    svc, _got = _watcher(monkeypatch, cfg)

    assert svc._connect() is sock
    assert seen["url"] == "ws://tofa.local:33333/api/v1/ws" and seen["sslopt"]["cert_reqs"] == 0
    assert sock.sent == [{"type": "auth", "token": "a", "profile_id": PROFILE, "profile_token": "ptok"}]

    flagged: list[bool] = []
    locked = FakeSocket({"error": "profile_locked"})
    monkeypatch.setattr(watch.websocket, "create_connection", lambda url, **kw: locked)
    monkeypatch.setattr(auth.TofaClient, "_flag_profile_pin", lambda self: flagged.append(True))
    with pytest.raises(auth.TofaError, match="profile_locked"):
        svc._connect()
    assert flagged == [True] and locked.closed


def test_manual_marks_come_from_the_change_feed(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    from providers.scrobble.tofa import watch

    monkeypatch.setattr(watch, "wants_marks", lambda dispatch: True)
    monkeypatch.setattr(watch, "written_by_crosswatch", lambda provider, scope, item, ts, now: item == "f-e2")
    now = _iso(time.time())
    feed = [
        {"revision": 1, "kind": "watched", "media_file_id": "f-alpha", "completed": True, "completed_at": now},
        {"revision": 2, "kind": "watched", "media_file_id": "f-alpha", "completed": True, "completed_at": now},
        {"revision": 3, "kind": "watched", "media_file_id": "f-e2", "completed": True, "completed_at": now},
        {"revision": 4, "kind": "watched", "media_file_id": "f-e1", "completed": False, "completed_at": None},
        {"revision": 5, "kind": "watchlist", "tmdb_id": 100, "media_type": "movie", "on_watchlist": True},
    ]
    server.on("GET", "/users/me/watch-state", lambda kw: ResponseStub(200, {"items": [], "changes_cursor": "c-0"}))
    server.on("GET", "/users/me/state-changes", lambda kw: ResponseStub(200, {"changes": feed if kw["params"]["cursor"] == "c-0" else [],
                                                                             "next_cursor": "c-5", "has_more": False, "requires_full_sync": False}))
    svc, got = _watcher(monkeypatch)
    svc._poll_manual_marks(_cfg())
    assert got.events == [] and svc._mark_cursors.get(PROFILE) == "c-0"

    svc._marks._next_poll = 0.0
    svc._poll_manual_marks(_cfg())
    assert len(got.events) == 1
    mark = got.events[0]
    assert mark.action == "stop" and mark.progress == 100.0 and mark.ids["tmdb"] == "100"
    assert mark.raw["_cw_manual_mark"] is True and isinstance(mark.raw["_cw_watched_at"], int)
    assert svc._mark_cursors.get(PROFILE) == "c-5"


def _sink_cfg() -> dict[str, Any]:
    cfg = _cfg()
    cfg["scrobble"]["watch"] = {"route_provider": "plex", "route_provider_instance": "default"}
    return cfg


def _event(action: str, progress: float, media: str = "movie") -> Any:
    from providers.scrobble.scrobble import ScrobbleEvent

    ids = {"tmdb": "100"} if media == "movie" else {"tvdb_show": "3000"}
    return ScrobbleEvent(action=cast(Any, action), media_type=cast(Any, media), ids=ids, title="Alpha", year=2024, season=1 if media == "episode" else None,
                         number=1 if media == "episode" else None, progress=progress, account="plexuser", server_uuid="plex-1",
                         session_key="k1", raw={"_cw_watched_at": 1_700_000_000})


def test_sink_complete_writes_dated_watch_and_verifies(server: FakeServer) -> None:
    from providers.scrobble.tofa.sink import TofaSink

    sent: list[Any] = []

    def write(kw: dict[str, Any]) -> ResponseStub:
        sent.append(kw["json"])
        server.progress["f-alpha"] = {"completed": True}
        return ResponseStub(200, {"updated": 1})

    server.on("POST", "/media/watched/batch", write)
    sink = TofaSink(cfg_provider=_sink_cfg, instance_id="default")
    res = sink.send(_event("stop", 96.0), _sink_cfg())
    again = sink.send(_event("stop", 96.0), _sink_cfg())

    assert res == {"ok": True}
    assert sent == [{"items": [{"media_file_id": "f-alpha", "watched": True, "watched_at": "2023-11-14T22:13:20Z"}]}]
    assert again.get("skipped") is True


def test_sink_progress_never_unwatches_and_respects_minimum(server: FakeServer) -> None:
    from providers.scrobble.tofa.sink import TofaSink

    puts: list[Any] = []
    server.on("PUT", "/media/f-e1/progress", lambda kw: puts.append(kw["json"]) or ResponseStub(200, {"position_ms": kw["json"]["position_ms"]}))
    sink = TofaSink(cfg_provider=_sink_cfg, instance_id="default")

    assert sink.send(_event("pause", 40.0, "episode"), _sink_cfg()) == {"ok": True}
    assert puts == [{"position_ms": 1_080_000}]

    sink._sent.clear()
    assert sink.send(_event("pause", 1.0, "episode"), _sink_cfg())["reason"] == "below_server_minimum"

    server.progress["f-e1"] = {"completed": True}
    sink._sent.clear()
    assert sink.send(_event("pause", 50.0, "episode"), _sink_cfg())["reason"] == "destination_already_watched"
    assert len(puts) == 1


def test_sink_unmatched_is_not_retryable(server: FakeServer) -> None:
    from providers.scrobble.scrobble import ScrobbleEvent
    from providers.scrobble.tofa.sink import TofaSink

    ev = ScrobbleEvent(action="stop", media_type="movie", ids={"tmdb": "999"}, title="Nope", year=None, season=None, number=None,
                       progress=99.0, account=None, server_uuid=None, session_key="k2", raw={})
    res = TofaSink(cfg_provider=_sink_cfg, instance_id="default").send(ev, _sink_cfg())
    assert res["ok"] is False and res["error"] == "unmatched_in_tofa" and res["retryable"] is False


def test_factories_routes_and_sink_configuration() -> None:
    from providers.scrobble import routes, watch_manager
    from providers.scrobble.tofa.sink import TofaSink
    from providers.scrobble.tofa.watch import TofaWatchService
    from providers.webhooks import dispatch
    from providers.webhooks.config import sink_configured

    assert "tofa" in routes.ROUTE_PROVIDERS and "tofa" in routes.ROUTE_SINKS
    assert "tofa" not in routes.ROUTE_MEDIA_SINKS and "tofa" not in routes.ROUTE_POLL_PROVIDERS and "tofa" not in routes.ROUTE_RATING_SINKS
    assert isinstance(watch_manager._make_sink("tofa", _cfg, "default"), TofaSink)
    assert isinstance(dispatch._make_sink("tofa", "default", _cfg), TofaSink)
    assert isinstance(watch_manager._make_watcher("tofa", watch_manager.MultiDispatcher([]), _cfg, "default"), TofaWatchService)
    assert sink_configured(_cfg(), "tofa", "default") is True
    assert sink_configured({"tofa": {"server_url": SERVER, "access_token": "a"}}, "tofa", "default") is False


def test_playback_adapter_lists_and_marks_watched(server: FakeServer) -> None:
    from services.playback_progress.adapters.tofa import TofaPlaybackAdapter
    from services.playback_progress.service import PHASE1_PROVIDERS

    assert "tofa" in PHASE1_PROVIDERS
    state = {"f-alpha": {"media_file_id": "f-alpha", "completed": False, "position_ms": 600_000, "last_watched_at": "2026-11-01T21:00:00Z"}}
    server.on("GET", "/users/me/watch-state", lambda kw: ResponseStub(200, {"items": [r for r in state.values() if r.get("completed") or kw["params"].get("completed_only") != "true"],
                                                                            "next_after": None, "changes_cursor": "c"}))

    def batch(kw: dict[str, Any]) -> ResponseStub:
        for item in kw["json"]["items"]:
            state[item["media_file_id"]] = {"media_file_id": item["media_file_id"], "completed": item["watched"], "completed_at": item.get("watched_at"), "position_ms": 0}
        return ResponseStub(200, {"updated": len(kw["json"]["items"])})

    server.on("POST", "/media/watched/batch", batch)
    adapter = TofaPlaybackAdapter()
    listed = adapter.list_progress(_cfg(), instance_id="default", instance_label="Alice")
    assert listed.ok and len(listed.items) == 1
    record = listed.items[0]
    assert record.progress_percent == 10.0 and record.duration_seconds == 6000 and record.can_mark_watched

    result = adapter.mark_watched(_cfg(), record.__dict__, instance_id="default", instance_label="Alice", watched_at="2026-11-02T10:00:00Z")
    assert result.ok and state["f-alpha"]["completed"] is True and state["f-alpha"]["completed_at"] == "2026-11-02T10:00:00Z"


def test_sink_marks_only_unwatched_parts_and_never_writes_progress_over_a_watched_part(server: FakeServer) -> None:
    from providers.scrobble.tofa.sink import TofaSink
    from providers.sync.tofa import _common

    FILES["lib-movies"].append(_movie("f-alpha-pt2", "m-alpha", 100, "Alpha"))
    try:
        _common._LIBRARIES.clear()
        server.progress["f-alpha"] = {"completed": True}
        server.on("GET", "/media/f-alpha-pt2/progress", lambda kw: ResponseStub(200, {"completed": False, **server.progress.get("f-alpha-pt2", {})}))
        sent: list[Any] = []

        def write(kw: dict[str, Any]) -> ResponseStub:
            sent.append(kw["json"])
            server.progress["f-alpha-pt2"] = {"completed": True}
            return ResponseStub(200, {"updated": 1})

        server.on("POST", "/media/watched/batch", write)
        sink = TofaSink(cfg_provider=_sink_cfg, instance_id="default")
        assert sink.send(_event("pause", 50.0), _sink_cfg())["reason"] == "destination_already_watched"
        assert sink.send(_event("stop", 96.0), _sink_cfg()) == {"ok": True}
        assert sent == [{"items": [{"media_file_id": "f-alpha-pt2", "watched": True, "watched_at": "2023-11-14T22:13:20Z"}]}]
        sink._sent.clear()
        assert sink.send(_event("stop", 96.0), _sink_cfg())["reason"] == "destination_already_watched"
    finally:
        FILES["lib-movies"].pop()


def test_housekeeping_still_runs_while_messages_keep_arriving(monkeypatch: pytest.MonkeyPatch, server: FakeServer) -> None:
    from providers.scrobble.tofa import watch

    monkeypatch.setattr(watch, "RECV_TIMEOUT_SECONDS", 0.0)
    svc, _got = _watcher(monkeypatch)
    runs: list[int] = []
    monkeypatch.setattr(svc, "_housekeeping", lambda: runs.append(1) or setattr(svc, "_last_housekeeping", time.monotonic()))

    class Flood:
        def __init__(self) -> None:
            self.left = 5

        def recv(self) -> str:
            self.left -= 1
            if self.left < 0:
                svc._stop.set()
                raise ConnectionError("closed")
            return json.dumps({"type": "AppLog", "message": "scan"})

        def close(self) -> None:
            pass

    monkeypatch.setattr(svc, "_connect", lambda: Flood())
    svc.start()
    assert len(runs) >= 5


@pytest.mark.parametrize("payload", [{"media_file_id": "f-alpha", "position_ms": 0}, {"completed": "yes"}, ["junk"]])
def test_sink_never_writes_after_an_invalid_progress_read(server: FakeServer, payload: Any) -> None:
    from providers.scrobble.tofa.sink import TofaSink

    writes: list[Any] = []
    server.on("GET", "/media/f-alpha/progress", lambda kw: ResponseStub(200, payload))
    server.on("POST", "/media/watched/batch", lambda kw: writes.append(kw) or ResponseStub(200, {"updated": 1}))
    res = TofaSink(cfg_provider=_sink_cfg, instance_id="default").send(_event("stop", 96.0), _sink_cfg())
    assert res["ok"] is False and res["error"] == "tofa_invalid_progress" and writes == []


def test_sink_treats_a_never_played_file_as_unwatched(server: FakeServer) -> None:
    from providers.scrobble.tofa.sink import TofaSink

    writes: list[Any] = []
    server.on("GET", "/media/f-alpha/progress", lambda kw: ResponseStub(200, None))
    server.on("POST", "/media/watched/batch", lambda kw: writes.append(kw["json"]) or ResponseStub(200, {"updated": 1}))
    TofaSink(cfg_provider=_sink_cfg, instance_id="default").send(_event("stop", 96.0), _sink_cfg())
    assert len(writes) == 1


@pytest.mark.parametrize("second_done,emitted", [(False, 0), (True, 1)])
def test_manual_mark_on_one_part_needs_the_whole_edition(monkeypatch: pytest.MonkeyPatch, server: FakeServer,
                                                          second_done: bool, emitted: int) -> None:
    from providers.scrobble.tofa import watch

    FILES["lib-movies"].extend([_movie("f-p1", "m-parts", 555, "Parts"), _movie("f-p2", "m-parts", 555, "Parts")])
    try:
        monkeypatch.setattr(watch, "wants_marks", lambda dispatch: True)
        monkeypatch.setattr(watch, "written_by_crosswatch", lambda *a: False)
        now = _iso(time.time())
        feed = [{"revision": 1, "kind": "watched", "media_file_id": "f-p1", "completed": True, "completed_at": now}]
        if second_done:
            feed.append({"revision": 2, "kind": "watched", "media_file_id": "f-p2", "completed": True, "completed_at": now})
        server.on("GET", "/media/f-p2/progress", lambda kw: ResponseStub(200, None))
        server.on("GET", "/users/me/watch-state", lambda kw: ResponseStub(200, {"items": [], "changes_cursor": "c-0"}))
        server.on("GET", "/users/me/state-changes", lambda kw: ResponseStub(200, {"changes": feed if kw["params"]["cursor"] == "c-0" else [],
                                                                                 "next_cursor": "c-9", "has_more": False, "requires_full_sync": False}))
        svc, got = _watcher(monkeypatch)
        svc._poll_manual_marks(_cfg())
        svc._marks._next_poll = 0.0
        svc._poll_manual_marks(_cfg())
        assert len(got.events) == emitted
        if emitted:
            assert got.events[0].ids["tmdb"] == "555" and got.events[0].progress == 100.0
    finally:
        del FILES["lib-movies"][-2:]


class _BadJson(ResponseStub):
    @property
    def text(self) -> str:
        return "{not json"

    def json(self) -> Any:
        raise ValueError("bad json")


@pytest.mark.parametrize("broken", ["http_500", "invalid_json", "empty_object"])
def test_manual_mark_is_retried_when_the_other_part_cannot_be_read(monkeypatch: pytest.MonkeyPatch, server: FakeServer, broken: str) -> None:
    from providers.scrobble.tofa import watch

    FILES["lib-movies"].extend([_movie("f-p1", "m-parts", 555, "Parts"), _movie("f-p2", "m-parts", 555, "Parts")])
    try:
        monkeypatch.setattr(watch, "wants_marks", lambda dispatch: True)
        monkeypatch.setattr(watch, "written_by_crosswatch", lambda *a: False)
        feed = [{"revision": 1, "kind": "watched", "media_file_id": "f-p2", "completed": True, "completed_at": _iso(time.time())}]
        part1 = {"ok": False}

        def progress(kw: dict[str, Any]) -> ResponseStub:
            if part1["ok"]:
                return ResponseStub(200, {"media_file_id": "f-p1", "completed": True})
            if broken == "http_500":
                return ResponseStub(500, {"error": "internal"})
            return _BadJson(200, None) if broken == "invalid_json" else ResponseStub(200, {})

        server.on("GET", "/media/f-p1/progress", progress)
        server.on("GET", "/users/me/watch-state", lambda kw: ResponseStub(200, {"items": [], "changes_cursor": "c-0"}))
        server.on("GET", "/users/me/state-changes", lambda kw: ResponseStub(200, {"changes": feed if kw["params"]["cursor"] == "c-0" else [],
                                                                                 "next_cursor": "c-9", "has_more": False, "requires_full_sync": False}))
        svc, got = _watcher(monkeypatch)
        svc._poll_manual_marks(_cfg())
        svc._marks._next_poll = 0.0
        svc._poll_manual_marks(_cfg())
        assert got.events == [] and svc._mark_cursors.get(PROFILE) == "c-9"

        assert PROFILE in svc._mark_retry
        part1["ok"] = True
        svc._marks._next_poll = 0.0
        svc._poll_manual_marks(_cfg())
        assert len(got.events) == 1 and got.events[0].ids["tmdb"] == "555"

        svc._marks._next_poll = 0.0
        svc._poll_manual_marks(_cfg())
        assert len(got.events) == 1
    finally:
        del FILES["lib-movies"][-2:]
