# CrossWatch test scripts
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import pytest

from providers.scrobble.scrob import sink as scrob_sink
from providers.scrobble.scrob import watch as scrob_watch
from providers.scrobble.scrobble import ScrobbleEvent


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return json.dumps(self.payload) if self.payload is not None else ""

    def json(self) -> Any:
        if self.payload is None:
            raise ValueError("no json")
        return self.payload


def scrob_cfg(**over: Any) -> dict[str, Any]:
    block = {
        "server_url": "http://host:7330",
        "api_key": "KEY",
        "username": "u",
        "password": "p",
        "api_prefix": "/api/proxy",
        "access_token": "JWT",
        "expires_at": 4102444800,
    }
    block.update(over)
    return {"scrob": block}


def event(action: str, *, progress: float = 10.0, media_type: str = "movie", session: str = "s1", **over: Any) -> ScrobbleEvent:
    ids = over.pop("ids", {"tmdb": "550"} if media_type == "movie" else {"tmdb_show": "1399"})
    return ScrobbleEvent(
        action=action,  # type: ignore[arg-type]
        media_type=media_type,  # type: ignore[arg-type]
        ids=ids,
        title=over.pop("title", "Fight Club"),
        year=over.pop("year", 1999),
        season=over.pop("season", None),
        number=over.pop("number", None),
        progress=progress,
        account=over.pop("account", "user"),
        server_uuid=over.pop("server_uuid", "srv"),
        session_key=session,
        raw=over.pop("raw", {}),
        position_ms=over.pop("position_ms", None),
        duration_ms=over.pop("duration_ms", None),
    )


@pytest.fixture(autouse=True)
def _clean_sink_state():
    scrob_sink._SESSIONS.clear()
    yield
    scrob_sink._SESSIONS.clear()


@pytest.fixture()
def posts(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    seen: list[dict[str, Any]] = []

    def fake_webhook(adapter: Any, path: str, payload: Any) -> ResponseStub:
        seen.append({"path": path, "payload": payload})
        return ResponseStub(200, {"status": "ok"})

    monkeypatch.setattr(scrob_sink, "webhook_post", fake_webhook)
    monkeypatch.setattr(scrob_sink, "record_scrobble_event", lambda *a, **k: None)
    monkeypatch.setattr(scrob_sink, "record_watch", lambda *a, **k: None)
    monkeypatch.setattr(scrob_sink, "_auto_remove_across", lambda *a, **k: None)
    return seen


def test_sink_maps_the_session_lifecycle_to_kodi_events(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    s.send(event("start", progress=1.0))
    s.send(event("start", progress=40.0))
    s.send(event("pause", progress=45.0))
    s.send(event("start", progress=46.0))
    s.send(event("stop", progress=99.0))

    events = [p["payload"].get("event") or p["payload"].get("method") for p in posts]
    assert events == ["playback_started", "playback_seeked", "playback_paused", "playback_resumed", "Player.OnStop"]
    assert all(p["path"] == "webhooks/kodi" for p in posts)


def test_sink_flags_a_completed_stop_as_watched(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    s.send(event("start", progress=5.0))
    s.send(event("stop", progress=97.0))
    assert posts[-1]["payload"]["params"] == {"data": {"end": True}}


def test_sink_leaves_an_early_stop_unwatched(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    s.send(event("start", progress=5.0))
    s.send(event("stop", progress=30.0))
    assert posts[-1]["payload"]["params"] == {"data": {"end": False}}


def test_sink_tags_sessions_so_the_watcher_can_ignore_them(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    s.send(event("start"))
    session_id = posts[0]["payload"]["session_id"]
    assert session_id.startswith("crosswatch-")
    assert scrob_sink.is_crosswatch_session(f"kodi:1:{session_id}")
    assert not scrob_sink.is_crosswatch_session("plex:1:42")


def test_sink_sends_episode_show_ids_and_numbers(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    s.send(event("start", media_type="episode", ids={"tmdb_show": "1399", "tvdb_show": "121361"}, season=1, number=1, title="Game of Thrones"))
    item = posts[0]["payload"]["item"]
    assert item["type"] == "episode"
    assert item["uniqueid"] == {"tmdb": "1399", "tvdb": "121361"}
    assert item["season"] == 1 and item["episode"] == 1
    assert item["showtitle"] == "Game of Thrones"


def test_sink_computes_position_from_duration_when_absent(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    s.send(event("start", progress=50.0, duration_ms=8340000))
    assert posts[0]["payload"]["total_seconds"] == 8340
    assert posts[0]["payload"]["position_seconds"] == 4170


def test_sink_skips_events_without_supported_ids(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    s.send(event("start", ids={}))
    assert posts == []


def test_sink_skips_when_not_connected(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=lambda: {"scrob": {"server_url": "", "api_key": ""}})
    s.send(event("start"))
    assert posts == []


def test_sink_rolls_back_session_state_when_delivery_fails(monkeypatch: pytest.MonkeyPatch):
    calls: list[Any] = []

    def failing(adapter: Any, path: str, payload: Any) -> ResponseStub:
        calls.append(payload)
        return ResponseStub(500, {"detail": "boom"})

    monkeypatch.setattr(scrob_sink, "webhook_post", failing)
    monkeypatch.setattr(scrob_sink, "record_watch", lambda *a, **k: None)
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    s.send(event("start"))
    assert scrob_sink._SESSIONS == {}
    s.send(event("start"))
    assert calls[1].get("event") == "playback_started"


def test_sink_delivers_watched_and_unwatched_state(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    assert s.send_watched_state(event("stop"), watched=False) is True
    payload = posts[0]["payload"]
    assert posts[0]["path"] == "webhooks/jellyfin"
    assert payload["NotificationType"] == "UserDataSaved"
    assert payload["SaveReason"] == "TogglePlayed"
    assert payload["Played"] is False
    assert payload["Provider_tmdb"] == "550"

    assert s.send_watched_state(event("stop"), watched=True) is True
    assert posts[1]["payload"]["Played"] is True


def test_sink_watched_state_needs_episode_numbers(posts: list[dict[str, Any]]):
    s = scrob_sink.ScrobSink(cfg_provider=scrob_cfg)
    assert s.send_watched_state(event("stop", media_type="episode", ids={"tmdb": "63056"}), watched=False) is False
    assert posts == []


class RecordingDispatcher:
    def __init__(self) -> None:
        self.events: list[ScrobbleEvent] = []

    def dispatch(self, ev: ScrobbleEvent) -> bool:
        self.events.append(ev)
        return True

    def accepts(self, ev: ScrobbleEvent) -> bool:
        return True


def session_row(**over: Any) -> dict[str, Any]:
    row = {
        "session_key": "plex:1:42",
        "source": "plex",
        "state": "playing",
        "progress_percent": 0.05,
        "progress_seconds": 300,
        "started_at": "2026-08-11T20:00:00",
        "updated_at": "2026-08-11T20:05:00",
        "media": {"id": 5, "tmdb_id": 550, "type": "movie", "title": "Fight Club", "runtime": 139},
    }
    row.update(over)
    return row


def make_watcher(monkeypatch: pytest.MonkeyPatch, pages: list[list[dict[str, Any]]]) -> tuple[Any, RecordingDispatcher]:
    dispatcher = RecordingDispatcher()
    watcher = scrob_watch.ScrobWatchService(dispatcher=dispatcher, cfg_provider=scrob_cfg, instance_id="default")
    queue = list(pages)

    def fake_fetch() -> list[dict[str, Any]]:
        return queue.pop(0) if queue else []

    monkeypatch.setattr(watcher, "_fetch_sessions", fake_fetch)
    monkeypatch.setattr(scrob_watch, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(scrob_watch, "_cw_update_payload", lambda *a, **k: None)
    return watcher, dispatcher


def test_watcher_emits_start_for_a_new_session(monkeypatch: pytest.MonkeyPatch):
    watcher, dispatcher = make_watcher(monkeypatch, [[session_row()]])
    watcher._tick()
    assert [e.action for e in dispatcher.events] == ["start"]
    ev = dispatcher.events[0]
    assert ev.ids == {"tmdb": "550"}
    assert ev.title == "Fight Club"
    assert ev.duration_ms == 139 * 60000


def test_watcher_does_not_repeat_identical_progress(monkeypatch: pytest.MonkeyPatch):
    rows = [[session_row()], [session_row()], [session_row()]]
    watcher, dispatcher = make_watcher(monkeypatch, rows)
    for _ in rows:
        watcher._tick()
    assert [e.action for e in dispatcher.events] == ["start"]


def test_watcher_emits_pause_then_resume(monkeypatch: pytest.MonkeyPatch):
    rows = [
        [session_row()],
        [session_row(state="paused", progress_percent=0.20)],
        [session_row(state="playing", progress_percent=0.21)],
    ]
    watcher, dispatcher = make_watcher(monkeypatch, rows)
    for _ in rows:
        watcher._tick()
    assert [e.action for e in dispatcher.events] == ["start", "pause", "start"]


def test_watcher_emits_progress_after_a_meaningful_jump(monkeypatch: pytest.MonkeyPatch):
    rows = [
        [session_row(progress_percent=0.05)],
        [session_row(progress_percent=0.10)],
        [session_row(progress_percent=0.40)],
    ]
    watcher, dispatcher = make_watcher(monkeypatch, rows)
    for _ in rows:
        watcher._tick()
    actions = [e.action for e in dispatcher.events]
    assert actions == ["start", "start"]
    assert dispatcher.events[-1].progress == pytest.approx(40.0)


def test_watcher_emits_stop_when_the_session_disappears(monkeypatch: pytest.MonkeyPatch):
    rows = [[session_row(progress_percent=0.95)], []]
    watcher, dispatcher = make_watcher(monkeypatch, rows)
    watcher._tick()
    watcher._tick()
    assert [e.action for e in dispatcher.events] == ["start", "stop"]
    assert dispatcher.events[-1].progress == pytest.approx(95.0)


def test_watcher_stop_carries_watched_progress(monkeypatch: pytest.MonkeyPatch):
    rows = [[session_row(progress_percent=0.99)], []]
    watcher, dispatcher = make_watcher(monkeypatch, rows)
    watcher._tick()
    watcher._tick()
    stop = dispatcher.events[-1]
    from providers.scrobble._watched_gate import resolve_stop_action

    assert resolve_stop_action(stop.progress, 90.0) == "stop"


def test_watcher_ignores_sessions_crosswatch_wrote(monkeypatch: pytest.MonkeyPatch):
    rows = [[session_row(session_key="kodi:1:crosswatch-abc123")]]
    watcher, dispatcher = make_watcher(monkeypatch, rows)
    assert watcher._tick() is False
    assert dispatcher.events == []


def test_watcher_maps_episode_sessions(monkeypatch: pytest.MonkeyPatch):
    media = {
        "id": 9,
        "tmdb_id": 63056,
        "type": "episode",
        "title": "Winter Is Coming",
        "season_number": 1,
        "episode_number": 1,
        "show_title": "Game of Thrones",
        "show_tmdb_id": 1399,
        "show_tvdb_id": 121361,
    }
    watcher, dispatcher = make_watcher(monkeypatch, [[session_row(media=media)]])
    watcher._tick()
    ev = dispatcher.events[0]
    assert ev.media_type == "episode"
    assert ev.ids == {"tmdb_episode": "63056", "tmdb_show": "1399", "tvdb_show": "121361"}
    assert ev.title == "Game of Thrones"
    assert ev.season == 1 and ev.number == 1


def test_watcher_skips_sessions_without_ids(monkeypatch: pytest.MonkeyPatch):
    media = {"id": 1, "tmdb_id": None, "type": "movie", "title": "Unknown"}
    watcher, dispatcher = make_watcher(monkeypatch, [[session_row(media=media)]])
    watcher._tick()
    assert dispatcher.events == []


def test_watcher_survives_a_provider_outage(monkeypatch: pytest.MonkeyPatch):
    watcher, dispatcher = make_watcher(monkeypatch, [])

    def boom() -> list[dict[str, Any]]:
        raise RuntimeError("http:502")

    monkeypatch.setattr(watcher, "_fetch_sessions", boom)
    assert watcher._tick() is False
    assert watcher._offline is True
    assert dispatcher.events == []
