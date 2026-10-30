# tests/test_manual_marks.py
# CrossWatch - Manual played-mark detection tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, cast

import pytest

from cw_platform.played_writes import note_write, written_by_crosswatch
from providers.scrobble.jellyfin import watch as jellyfin_watch
from providers.scrobble.manual_marks import (
    MAX_MARKS_PER_POLL,
    MarkTracker,
    interval_seconds,
    log_read_failure,
    log_read_health,
    parse_epoch,
)
from providers.scrobble.routes import normalize_route_options
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent

BASE = 1_793_000_000.0


@pytest.fixture(autouse=True)
def _isolated_registry(config_base):
    return config_base


class FakeDispatcher:
    def __init__(self, result: bool = True) -> None:
        self.events: list[ScrobbleEvent] = []
        self.wants = True
        self.result = result

    def accepts_user(self, ev: ScrobbleEvent) -> bool:
        return ev.account != "blocked"

    def wants_manual_marks(self) -> bool:
        return self.wants

    def dispatch(self, ev: ScrobbleEvent) -> bool:
        self.events.append(ev)
        return self.result


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.1234567Z")


def _row(iid: str, name: str, ts: float, ticks: int = 0) -> dict[str, Any]:
    return {
        "Id": iid,
        "Name": name,
        "Type": "Movie",
        "ProductionYear": 2020,
        "ProviderIds": {"Imdb": f"tt{iid}", "Tmdb": "123"},
        "RunTimeTicks": 60_000_000_000,
        "UserData": {"Played": True, "LastPlayedDate": _iso(ts), "PlaybackPositionTicks": ticks},
    }


def _service(monkeypatch, played: dict[str, list[dict[str, Any]]], dispatcher: FakeDispatcher | None = None) -> tuple[Any, FakeDispatcher]:
    monkeypatch.setattr(jellyfin_watch, "_server_id", lambda *a, **k: "server")
    cfg = {"jellyfin": {"server": "http://jellyfin.local", "access_token": "token"}, "scrobble": {"watch": {}}}
    dispatcher = dispatcher or FakeDispatcher()
    service = jellyfin_watch.JellyfinWatchService(dispatcher=dispatcher, cfg_provider=lambda: cfg, quiet_startup=True)
    service._log = lambda msg, level="INFO": None  # type: ignore[method-assign]

    def fake_get(path: str, _cfg: dict[str, Any]) -> Any:
        if path == "/Users":
            return [
                {"Id": "u1", "Name": "Pascal", "Policy": {}},
                {"Id": "u2", "Name": "blocked", "Policy": {}},
                {"Id": "u3", "Name": "off", "Policy": {"IsDisabled": True}},
            ]
        uid = path.split("/")[2]
        start = int(path.split("StartIndex=")[1].split("&")[0])
        rows = sorted(played.get(uid, []), key=lambda r: r["UserData"]["LastPlayedDate"], reverse=True)
        return {"Items": rows[start:start + 50]}

    service._mark_get = fake_get  # type: ignore[method-assign]
    return service, dispatcher


def _rows(*pairs: tuple[str, float]) -> list[tuple[str, str, float]]:
    return [(key, key, ts) for key, ts in pairs]


def test_parse_epoch_handles_server_formats():
    assert parse_epoch("2026-10-30T00:01:29.0000000Z") == 1793318489.0
    assert parse_epoch("2026-10-29T23:54:47.9059853Z") is not None
    assert parse_epoch(1793389741) == 1793389741.0
    assert parse_epoch("") is None
    assert parse_epoch(None) is None


def test_tracker_seeds_then_reports_only_new_marks():
    tracker = MarkTracker()
    assert tracker.fresh("u", _rows(("a", BASE - 500)), BASE, 150) == []
    assert tracker.fresh("u", _rows(("b", BASE + 25), ("a", BASE - 500)), BASE + 30, 150) == ["b"]
    assert tracker.fresh("u", _rows(("b", BASE + 25)), BASE + 60, 150) == []
    assert tracker.fresh("u", _rows(("c", BASE - 100)), BASE + 90, 150) == []


def test_tracker_ignores_dated_imports_from_before_last_poll():
    tracker = MarkTracker()
    tracker.fresh("u", _rows(("old", BASE - 86400 * 5)), BASE, 150)
    assert tracker.fresh("u", _rows(("imported", BASE - 86400)), BASE + 30, 150) == []


def test_tracker_future_rows_do_not_block_later_marks():
    tracker = MarkTracker()
    tracker.fresh("u", _rows(("future", BASE + 86400)), BASE, 150)
    assert tracker.fresh("u", _rows(("future", BASE + 86400), ("now", BASE + 20)), BASE + 30, 150) == ["now"]
    assert tracker.watermark("u") == BASE + 20


def test_tracker_startup_second_is_closed():
    tracker = MarkTracker()
    tracker.fresh("u", _rows(("a", BASE)), BASE + 10, 150)
    later = _rows(*((key, BASE) for key in ("a", "b", "c", "d", "e")))
    assert tracker.fresh("u", later, BASE + 40, 150) == []
    assert tracker.fresh("u", later + _rows(("f", BASE + 45)), BASE + 70, 150) == ["f"]


def test_played_pages_continue_across_watermark_ties(monkeypatch):
    played: dict[str, list[dict[str, Any]]] = {"u1": [_row(f"m{i:03}", f"Movie {i}", BASE) for i in range(51)]}
    service, _ = _service(monkeypatch, played)
    watermark = parse_epoch(played["u1"][0]["UserData"]["LastPlayedDate"])
    assert len(service._mark_played_rows("u1", service._active_cfg(), watermark)) == 51


def test_tracker_keeps_marks_that_share_the_watermark_second():
    tracker = MarkTracker()
    tracker.fresh("u", [], BASE, 150)
    assert tracker.fresh("u", _rows(("a", BASE + 10)), BASE + 30, 150) == ["a"]
    assert tracker.fresh("u", _rows(("a", BASE + 10), ("b", BASE + 10)), BASE + 60, 150) == ["b"]
    assert tracker.fresh("u", _rows(("a", BASE + 10), ("b", BASE + 10)), BASE + 90, 150) == []


def test_registry_is_scoped_and_normalized():
    note_write("jellyfin", "7a30f8f0-2e1e-4356-a28a-efb4d4d18c74", "Item-1")
    now = time.time()
    assert written_by_crosswatch("jellyfin", "userB", "item-1", now, now) is False
    assert written_by_crosswatch("jellyfin", "7A30F8F02E1E4356A28AEFB4D4D18C74", "ITEM-1", now, now) is True
    assert written_by_crosswatch("jellyfin", "7a30f8f02e1e4356a28aefb4d4d18c74", "item1", now, now) is False
    assert written_by_crosswatch("emby", "7a30f8f02e1e4356a28aefb4d4d18c74", "item-1", now, now) is False


def test_registry_keeps_silo_episode_coordinates_distinct():
    note_write("silo", "p1", "episode-tvdb-123-1-23")
    now = time.time()
    assert written_by_crosswatch("silo", "p1", "episode-tvdb-123-12-3", now, now) is False
    assert written_by_crosswatch("silo", "p1", "episode-tvdb-123-1-23", now, now) is True


def test_registry_matches_only_the_written_play_and_is_shared_by_observers():
    note_write("plex", "server-1", "42")
    now = time.time()
    assert written_by_crosswatch("plex", "server-1", "42", now - 3600, now) is True
    assert written_by_crosswatch("plex", "server-1", "42", now + 5, now) is True
    assert written_by_crosswatch("plex", "server-1", "42", now + 5, now) is True
    assert written_by_crosswatch("plex", "server-1", "42", now + 600, now + 600) is False


def test_jellyfin_manual_mark_dispatches_watched_stop(monkeypatch):
    played: dict[str, list[dict[str, Any]]] = {"u1": [_row("a001", "Old", BASE - 86400)], "u2": []}
    service, dispatcher = _service(monkeypatch, played)
    cfg = service._active_cfg()

    service._poll_manual_marks(cfg, BASE)
    assert dispatcher.events == []

    played["u1"].append(_row("a002", "Marked", BASE + 5))
    played["u2"].append(_row("b001", "Other user", BASE + 5))
    service._poll_manual_marks(cfg, BASE + 31)

    assert [ev.title for ev in dispatcher.events] == ["Marked"]
    ev = dispatcher.events[0]
    assert ev.action == "stop"
    assert ev.progress == 100
    assert ev.account == "Pascal"
    assert ev.raw["_cw_manual_mark"] is True
    assert ev.raw["_cw_preserve_stop"] is True
    assert ev.ids["imdb"] == "tta002"
    assert ev.session_key and ev.session_key.endswith(".1234567Z")


def test_session_items_resume_positions_and_own_writes_are_not_marks(monkeypatch):
    played: dict[str, list[dict[str, Any]]] = {"u1": []}
    service, dispatcher = _service(monkeypatch, played)
    cfg = service._active_cfg()
    service._poll_manual_marks(cfg, BASE)

    service._note_mark_session({"UserId": "u1", "NowPlayingItem": {"Id": "a010"}}, BASE + 10)
    note_write("jellyfin", "u1", "a011")
    note_write("jellyfin", "u9", "a013")
    played["u1"].extend([
        _row("a010", "Session", BASE + 20),
        _row("a011", "Synced", BASE + 21),
        _row("a012", "Manual", BASE + 22),
        _row("a013", "Other user synced", BASE + 23),
        _row("a014", "Partial rewatch", BASE + 24, ticks=5_000_000),
    ])
    service._poll_manual_marks(cfg, BASE + 31)

    assert [ev.title for ev in dispatcher.events] == ["Manual", "Other user synced"]


def test_mark_cap_counts_admitted_marks_even_when_delivery_fails(monkeypatch):
    played: dict[str, list[dict[str, Any]]] = {"u1": []}
    service, dispatcher = _service(monkeypatch, played, FakeDispatcher(result=False))
    cfg = service._active_cfg()
    service._poll_manual_marks(cfg, BASE)

    played["u1"].extend(_row(f"m{i:03}", f"Movie {i}", BASE + 10) for i in range(150))
    service._poll_manual_marks(cfg, BASE + 31)

    assert len(dispatcher.events) == MAX_MARKS_PER_POLL


def test_poll_respects_interval_and_route_toggle(monkeypatch):
    played: dict[str, list[dict[str, Any]]] = {"u1": []}
    service, dispatcher = _service(monkeypatch, played)
    cfg = service._active_cfg()
    service._poll_manual_marks(cfg, BASE)
    assert service._marks.seconds_until_due(BASE + 10) == pytest.approx(20.0)

    played["u1"].append(_row("a020", "Too soon", BASE + 5))
    service._poll_manual_marks(cfg, BASE + 10)
    assert dispatcher.events == []

    dispatcher.wants = False
    service._poll_manual_marks(cfg, BASE + 31)
    assert dispatcher.events == []


def test_mark_interval_follows_idle_poll_but_never_exceeds_thirty_seconds():
    def cfg(idle: Any) -> dict[str, Any]:
        return {"scrobble": {"watch": {"idle_poll_seconds": idle}}}

    assert interval_seconds({}) == 30.0
    assert interval_seconds(cfg(10)) == 10.0
    assert interval_seconds(cfg(120)) == 30.0


def test_dispatcher_drops_marks_when_route_option_is_off():
    def route_cfg(options: dict[str, Any]) -> dict[str, Any]:
        return {"scrobble": {"watch": {"route_enabled": True, "route_options": {"watch": options}, "filters": {}}}}

    off = Dispatcher([], cfg_provider=lambda: route_cfg({"manual_marks": False}))
    on = Dispatcher([], cfg_provider=lambda: route_cfg({}))
    mark = ScrobbleEvent(
        action="stop", media_type="movie", ids={"imdb": "tt1"}, title="x", year=None, season=None, number=None,
        progress=100, account="Pascal", server_uuid=None, session_key="mark:1", raw={"_cw_manual_mark": True},
    )
    play = ScrobbleEvent(**{**mark.__dict__, "raw": {}})

    assert off.wants_manual_marks() is False
    assert on.wants_manual_marks() is True
    assert off.accepts(mark) is False
    assert off.accepts(play) is True
    assert on.accepts(mark) is True


class _Resp:
    def __init__(self, body: Any) -> None:
        self.status_code = 200
        self._body = body
        self.text = "x"

    def json(self) -> Any:
        return self._body


def test_silo_manual_mark_dispatches_watched_stop(monkeypatch):
    from providers.scrobble.silo import watch as silo_watch

    progress: list[dict[str, Any]] = [{"media_item_id": "movie-tmdb-1", "completed": True, "position_seconds": 0, "duration_seconds": 6000, "updated_at": _iso(BASE - 86400)}]
    sessions: list[dict[str, Any]] = []

    def fake_request(adapter: Any, method: str, path: str, **kwargs: Any) -> _Resp:
        if path == silo_watch.SESSIONS_PATH:
            return _Resp({"items": sessions})
        rows = sorted(progress, key=lambda r: r["updated_at"], reverse=True)
        return _Resp({"items": rows[:50], "page": {}})

    monkeypatch.setattr(silo_watch, "silo_request", fake_request)
    cfg = {
        "silo": {"server_url": "http://silo.local", "access_token": "token", "profile_id": "p1", "profile_name": "Pascal"},
        "scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {}},
    }
    dispatcher = FakeDispatcher()
    service = silo_watch.SiloWatchService(dispatcher=dispatcher, cfg_provider=lambda: cfg, quiet_startup=True)
    monkeypatch.setattr(service, "_outside_whitelist", lambda cid: False)
    monkeypatch.setattr(service, "_metadata", lambda row: {"media_type": "movie", "ids": {"tmdb": row["content_id"].rsplit("-", 1)[-1]}, "title": "Movie", "year": None, "season": None, "number": None})

    def tick() -> None:
        service._marks._next_poll = 0.0
        service._tick()

    tick()
    assert dispatcher.events == []

    sessions.append({"id": "s1", "content_id": "movie-tmdb-2", "profile_id": "p1"})
    tick()
    sessions.clear()
    note_write("silo", "p1", "movie-tmdb-3")
    now = datetime.now(timezone.utc).timestamp()
    progress.extend([
        {"media_item_id": "movie-tmdb-2", "completed": True, "position_seconds": 0, "duration_seconds": 6000, "updated_at": _iso(now)},
        {"media_item_id": "movie-tmdb-3", "completed": True, "position_seconds": 0, "duration_seconds": 6000, "updated_at": _iso(now)},
        {"media_item_id": "movie-tmdb-4", "completed": True, "position_seconds": 0, "duration_seconds": 6000, "updated_at": _iso(now)},
        {"media_item_id": "movie-tmdb-5", "completed": True, "position_seconds": 900, "duration_seconds": 6000, "updated_at": _iso(now)},
    ])
    tick()

    marks = [ev for ev in dispatcher.events if ev.raw.get("_cw_manual_mark")]
    assert [ev.ids["tmdb"] for ev in marks] == ["4"]
    assert marks[0].action == "stop"
    assert marks[0].progress == 100.0
    assert marks[0].account == "Pascal"
    assert marks[0].raw["_cw_preserve_stop"] is True


def test_silo_numeric_content_id_resolves_kind_from_catalog(monkeypatch):
    from providers.scrobble.silo import watch as silo_watch

    cfg = {
        "silo": {"server_url": "http://silo.local", "access_token": "token", "profile_id": "p1", "profile_name": "Pascal"},
        "scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {}},
    }
    dispatcher = FakeDispatcher()
    service = silo_watch.SiloWatchService(dispatcher=dispatcher, cfg_provider=lambda: cfg, quiet_startup=True)
    seen: list[dict[str, Any]] = []
    monkeypatch.setattr(service, "_outside_whitelist", lambda cid: False)
    monkeypatch.setattr(silo_watch, "item_detail", lambda adapter, cid, feature: {"type": "episode", "season_number": 2, "episode_number": 3, "title": "Show"})

    def metadata(row: dict[str, Any]) -> dict[str, Any]:
        seen.append(dict(row))
        return {"media_type": "episode", "ids": {"tvdb_show": "1"}, "title": "Show", "year": None, "season": 2, "number": 3}

    monkeypatch.setattr(service, "_metadata", metadata)

    assert service._emit_manual_mark("p1", "Pascal", {"media_item_id": "149382054478429163", "updated_at": _iso(BASE)}) is True
    assert seen == [{"content_id": "149382054478429163", "media_type": "episode", "season_number": 2, "episode_number": 3}]
    assert dispatcher.events[0].season == 2 and dispatcher.events[0].number == 3


def test_silo_numeric_episode_uses_show_title(monkeypatch):
    from providers.scrobble.silo import watch as silo_watch

    dispatcher = FakeDispatcher()
    service = silo_watch.SiloWatchService(dispatcher=dispatcher, cfg_provider=lambda: {}, quiet_startup=True)
    details = {
        "99": {"type": "episode", "series_id": "series-tvdb-123", "season_number": 1, "episode_number": 2, "title": "Episode title"},
        "series-tvdb-123": {"type": "series", "title": "Show title"},
    }
    monkeypatch.setattr(service, "_outside_whitelist", lambda cid: False)
    monkeypatch.setattr(silo_watch, "item_detail", lambda adapter, cid, feature: details[cid])
    monkeypatch.setattr(silo_watch, "item_ids", lambda adapter, cid, feature: {"tvdb": "123"})

    assert service._emit_manual_mark("p1", "User", {"media_item_id": "99", "updated_at": "2026-10-30T12:00:00Z"}) is True
    ev = dispatcher.events[0]
    assert (ev.title, ev.season, ev.number) == ("Show title", 1, 2)


def test_plex_marks_pause_when_account_config_changes_on_a_live_connection():
    from providers.scrobble.plex import watch as plex_watch

    queries: list[str] = []

    class FakePlex:
        machineIdentifier = "server-1"

        def query(self, key: str) -> list[Any]:
            queries.append(key)
            return []

    def cfg(user: str) -> dict[str, Any]:
        return {
            "plex": {"server_url": "http://plex.local", "account_token": f"tok-{user}", "username": user, "account_id": 1},
            "scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {}},
        }

    state = {"cfg": cfg("ownerA")}
    service = plex_watch.WatchService(dispatcher=cast(Any, FakeDispatcher()), cfg_provider=lambda: state["cfg"], quiet_startup=True)
    service._plex = FakePlex()  # type: ignore[assignment]
    service._mark_conn = (service._mark_fingerprint(state["cfg"]), service._manual_mark_identity(state["cfg"]))

    service._poll_manual_marks()
    assert len(queries) == 2

    state["cfg"] = cfg("ownerB")
    service._marks._next_poll = 0.0
    service._poll_manual_marks()
    assert len(queries) == 2


def test_plex_reconnects_and_resumes_marks_after_account_change(monkeypatch):
    from providers.scrobble.plex import watch as plex_watch

    connections: list[str] = []
    polled_as: list[str] = []

    class FakePlex:
        machineIdentifier = "server-1"

        def __init__(self, base: str, token: str, session: Any = None) -> None:
            connections.append(token)

        def query(self, key: str) -> list[Any]:
            return []

    class FakeListener:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def start(self) -> None:
            pass

        def stop(self) -> None:
            pass

        def is_alive(self) -> bool:
            return True

    class FakeSession:
        def close(self) -> None:
            pass

    def cfg(user: str) -> dict[str, Any]:
        return {
            "plex": {"server_url": "http://plex.local", "account_token": f"tok-{user}", "username": user, "account_id": 1},
            "scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {}},
        }

    state = {"cfg": cfg("ownerA"), "ticks": 0}
    service = plex_watch.WatchService(dispatcher=cast(Any, FakeDispatcher()), cfg_provider=lambda: state["cfg"], quiet_startup=True)
    monkeypatch.setattr(plex_watch, "PlexServer", FakePlex)
    monkeypatch.setattr(plex_watch, "_PlexAlertListener", FakeListener)
    monkeypatch.setattr(plex_watch, "_build_session", lambda token, verify: FakeSession())
    monkeypatch.setattr(plex_watch, "_plex_btok", lambda c, instance_id=None: ("http://plex.local", c["plex"]["account_token"]))
    monkeypatch.setattr(service, "_refresh_account_context", lambda: None)
    original_poll = service._poll_manual_marks

    def poll() -> None:
        conn = service._mark_conn
        if conn is not None:
            polled_as.append(str(conn[1].get("name")))
        original_poll()

    def tick(_seconds: float) -> None:
        state["ticks"] += 1
        if state["ticks"] == 2:
            state["cfg"] = cfg("ownerB")
        if state["ticks"] >= 5:
            service._stop.set()

    monkeypatch.setattr(service, "_poll_manual_marks", poll)
    monkeypatch.setattr(plex_watch.time, "sleep", tick)
    service._run()

    assert connections == ["tok-ownerA", "tok-ownerB"]
    assert polled_as[0] == "ownerA"
    assert polled_as[-1] == "ownerB"


def test_user_list_fallback_keeps_cached_household_and_counts_failure(monkeypatch):
    service, _ = _service(monkeypatch, {"u1": [], "u2": []})
    logs: list[tuple[str, str]] = []
    service._log = lambda msg, level="INFO": logs.append((level, msg))  # type: ignore[method-assign]
    cfg = service._active_cfg()
    good = service._mark_get
    state = {"fail": False}

    def flaky(path: str, c: dict[str, Any]) -> Any:
        if path == "/Users" and state["fail"]:
            raise RuntimeError("http:500")
        if path == "/Users/u1":
            return {"Id": "u1", "Name": "Pascal", "Policy": {}}
        return good(path, c)

    service._mark_get = flaky  # type: ignore[method-assign]
    service._mark_users(cfg, BASE)
    assert [uid for uid, _ in service._mark_users_cache[1]] == ["u1", "u2"]

    cfg["jellyfin"]["user_id"] = "u1"
    state["fail"] = True
    for i in range(3):
        service._mark_users(cfg, BASE + 700 + i)
    assert "u2" in [uid for uid, _ in service._mark_users_cache[1]]
    assert [level for level, msg in logs if "user list" in msg] == ["DEBUG", "DEBUG", "WARNING"]


def test_user_list_failures_warn_and_recover(monkeypatch):
    service, _ = _service(monkeypatch, {"u1": []})
    logs: list[tuple[str, str]] = []
    service._log = lambda msg, level="INFO": logs.append((level, msg))  # type: ignore[method-assign]
    good = service._mark_get
    state = {"fail": True}

    def flaky(path: str, cfg: dict[str, Any]) -> Any:
        if state["fail"] and path == "/Users":
            raise RuntimeError("http:401")
        return good(path, cfg)

    service._mark_get = flaky  # type: ignore[method-assign]
    cfg = service._active_cfg()
    for i in range(4):
        service._poll_manual_marks(cfg, BASE + i * 31)
    state["fail"] = False
    service._poll_manual_marks(cfg, BASE + 4 * 31)

    levels = [level for level, msg in logs if "user list" in msg]
    assert levels == ["DEBUG", "DEBUG", "WARNING", "DEBUG", "INFO"]


def test_tracker_warns_after_repeated_failures_and_on_recovery():
    tracker = MarkTracker()
    logs: list[tuple[str, str]] = []
    log = lambda msg, level="INFO": logs.append((level, msg))
    for _ in range(4):
        log_read_failure(tracker, "u", "user=Pa***", RuntimeError("http:401"), log)
    log_read_health(tracker, "u", "user=Pa***", BASE, log)
    log_read_health(tracker, "u", "user=Pa***", BASE + 30, log)
    assert [level for level, _ in logs] == ["DEBUG", "DEBUG", "WARNING", "DEBUG", "INFO"]
    assert "3 times in a row" in logs[2][1]


def test_tracker_warns_about_future_rows_once_per_hour():
    tracker = MarkTracker()
    logs: list[tuple[str, str]] = []
    log = lambda msg, level="INFO": logs.append((level, msg))
    tracker.fresh("u", _rows(("ahead", BASE + 3600)), BASE, 150)
    log_read_health(tracker, "u", "user=Pa***", BASE, log)
    tracker.fresh("u", _rows(("ahead", BASE + 3600)), BASE + 30, 150)
    log_read_health(tracker, "u", "user=Pa***", BASE + 30, log)
    assert [level for level, _ in logs] == ["WARNING"]
    assert "check the server clock" in logs[0][1]


def test_archive_records_manual_mark_trigger():
    from cw_platform.event_archive.db import get_conn
    from cw_platform.event_archive.playback_recorder import prepare_playback, record_playback

    cfg = {"scrobble": {"watch": {"route_provider": "jellyfin", "route_sink": "trakt"}}}
    ev = ScrobbleEvent(
        action="stop", media_type="movie", ids={"imdb": "tt1655441"}, title="The Age of Adaline", year=2015, season=None,
        number=None, progress=100, account="Pascal", server_uuid="s", session_key="mark:u1:a1:x", raw={"_cw_manual_mark": True},
    )
    record_playback(prepare_playback(ev), cfg)
    conn = get_conn()
    assert conn is not None
    row = conn.execute("SELECT source_kind, detail FROM events WHERE event_type='scrobble_playback'").fetchone()
    assert row["source_kind"] == "watcher"
    assert json.loads(row["detail"])["manual_mark"] is True


def test_route_options_keep_manual_marks_flag():
    assert normalize_route_options({"watch": {"manual_marks": False}})["watch"]["manual_marks"] is False
    assert "manual_marks" not in normalize_route_options({"watch": {}})["watch"]
