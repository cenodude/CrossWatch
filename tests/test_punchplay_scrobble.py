# CrossWatch test scripts
from __future__ import annotations

import json
from typing import Any

import pytest


class _Resp:
    def __init__(self, status_code: int = 200, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload


class Event:
    def __init__(self, **kw: Any) -> None:
        self.action = kw.get("action", "start")
        self.media_type = kw.get("media_type", "movie")
        self.ids = kw.get("ids", {"tmdb": "550"})
        self.title = kw.get("title", "Fight Club")
        self.year = kw.get("year", 1999)
        self.season = kw.get("season")
        self.number = kw.get("number")
        self.progress = kw.get("progress", 10.0)
        self.account = kw.get("account", "u")
        self.server_uuid = kw.get("server_uuid", "srv-1")
        self.session_key = kw.get("session_key", "sess-1")
        self.raw = kw.get("raw", {})
        self.position_ms = kw.get("position_ms", 100000)
        self.duration_ms = kw.get("duration_ms", 1000000)


CFG = {
    "punchplay": {"access_token": "at", "device_id": "crosswatch-abc"},
    "scrobble": {"watch": {"watched_at": 90}},
}


@pytest.fixture(autouse=True)
def _reset_sessions():
    from providers.scrobble.punchplay import sink as s

    s._SESSIONS.clear()
    yield
    s._SESSIONS.clear()


@pytest.fixture()
def sink(monkeypatch: pytest.MonkeyPatch):
    from providers.scrobble.punchplay import sink as s

    calls: list[dict[str, Any]] = []

    def fake(adapter: Any, method: str, url: str, **kw: Any) -> _Resp:
        calls.append({"method": method, "url": url, **kw})
        return _Resp(200, {})

    monkeypatch.setattr(s, "punchplay_request", fake)
    return s.PunchPlaySink(cfg_provider=lambda: dict(CFG)), calls


def _action_of(call: dict[str, Any]) -> str:
    return call["url"].rsplit("/", 1)[-1]


def test_sink_is_registered_in_the_factory() -> None:
    from providers.scrobble.punchplay.sink import PunchPlaySink
    from providers.scrobble.watch_manager import _make_sink

    made = _make_sink("punchplay", lambda: dict(CFG), "default")
    assert isinstance(made, PunchPlaySink)


def test_punchplay_is_available_as_configured_watcher_destination() -> None:
    from api.scrobblerManagementAPI import _destination_availability
    from providers.scrobble.routes import ROUTE_RATING_SINKS, ROUTE_SINKS, build_route_cfg, normalize_route
    from providers.webhooks.config import sink_configured, webhook_sinks

    cfg = {
        "plex": {"account_token": "source-token"},
        "punchplay": {"access_token": "at", "device_id": "crosswatch-abc"},
        "scrobble": {"webhook": {"sinks": ["punchplay"]}},
    }
    route = normalize_route({"id": "R1", "provider": "plex", "sink": "punchplay"}, "R1")
    view = build_route_cfg(cfg, route)

    assert "punchplay" in ROUTE_SINKS
    assert "punchplay" in ROUTE_RATING_SINKS
    assert route["sink"] == "punchplay"
    assert view["punchplay"]["access_token"] == "at"
    assert sink_configured(cfg, "punchplay", "default") is True
    assert "punchplay" in webhook_sinks(cfg, "plex", "default")

    availability = {row["provider"]: row for row in _destination_availability(cfg)}
    assert availability["punchplay"]["profiles"][0]["configured"] is True


def test_start_pause_resume_stop_lifecycle(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", progress=5.0))
    s.send(Event(action="pause", progress=20.0))
    s.send(Event(action="start", progress=25.0))
    s.send(Event(action="stop", progress=95.0))

    assert [_action_of(c) for c in calls] == ["start", "pause", "resume", "stop"]

    session_ids = {c["json"]["playback_session_id"] for c in calls}
    assert len(session_ids) == 1, "all events in one viewing session share a session id"


def test_repeat_start_becomes_progress(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", progress=5.0))
    s.send(Event(action="start", progress=15.0))
    s.send(Event(action="start", progress=25.0))

    assert [_action_of(c) for c in calls] == ["start", "progress", "progress"]


def test_new_session_key_gets_a_new_session_id(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", session_key="sess-1"))
    s.send(Event(action="start", session_key="sess-2"))

    assert calls[0]["json"]["playback_session_id"] != calls[1]["json"]["playback_session_id"]


def test_stop_below_threshold_is_an_incomplete_stop(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", progress=5.0))
    s.send(Event(action="stop", progress=40.0))

    assert [_action_of(c) for c in calls] == ["start", "stop"]
    assert calls[1]["json"]["watched"] is False, "an incomplete stop saves resume progress, never a watch"


def test_pause_without_a_live_session_becomes_an_incomplete_stop(sink) -> None:
    s, calls = sink

    s.send(Event(action="pause", progress=40.0))

    assert [_action_of(c) for c in calls] == ["stop"], "a standalone pause is rejected with 409"
    assert calls[0]["json"]["watched"] is False


def test_failed_start_is_retried_as_a_start(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.punchplay import sink as s

    calls: list[str] = []
    codes = iter([500, 200, 200])

    def fake(adapter: Any, method: str, url: str, **kw: Any) -> _Resp:
        calls.append(url.rsplit("/", 1)[-1])
        return _Resp(next(codes, 200), {"error": "server_error"})

    monkeypatch.setattr(s, "punchplay_request", fake)
    s._SESSIONS.clear()

    a = s.PunchPlaySink(cfg_provider=lambda: dict(CFG))
    a.send(Event(action="start", progress=1.0))
    a.send(Event(action="start", progress=5.0))

    assert calls == ["start", "start"], "a rejected start must not leave a phantom local session"
    assert len(s._SESSIONS) == 1


def test_failed_stop_keeps_the_session_for_a_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.punchplay import sink as s

    calls: list[str] = []
    codes = iter([200, 500, 200])

    def fake(adapter: Any, method: str, url: str, **kw: Any) -> _Resp:
        calls.append(url.rsplit("/", 1)[-1])
        return _Resp(next(codes, 200), {"error": "server_error"})

    monkeypatch.setattr(s, "punchplay_request", fake)
    s._SESSIONS.clear()

    a = s.PunchPlaySink(cfg_provider=lambda: dict(CFG))
    a.send(Event(action="start", progress=5.0))
    a.send(Event(action="stop", progress=95.0))
    a.send(Event(action="stop", progress=95.0))

    assert calls == ["start", "stop", "stop"]


def test_zero_threshold_omits_watched_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.punchplay import sink as s

    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(
        s, "punchplay_request",
        lambda adapter, method, url, **kw: captured.append(kw["json"]) or _Resp(200, {}),
    )
    s._SESSIONS.clear()

    cfg = {**CFG, "scrobble": {"punchplay": {"watched_at": 0}}}
    s.PunchPlaySink(cfg_provider=lambda: cfg).send(Event(action="stop", progress=95.0))

    assert "watched_threshold" not in captured[0], "the spec requires watched_threshold > 0"


def test_stop_above_threshold_marks_watched(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", progress=5.0))
    s.send(Event(action="stop", progress=95.0))

    stop = calls[1]["json"]
    assert _action_of(calls[1]) == "stop"
    assert stop["watched"] is True
    assert stop["watched_threshold"] == 0.9


def test_payload_shape_matches_playback_input(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", progress=10.0, position_ms=100000, duration_ms=1000000))
    p = calls[0]["json"]

    assert p["media_type"] == "movie"
    assert p["tmdb_id"] == 550
    assert p["title"] == "Fight Club"
    assert p["year"] == 1999
    assert p["position_seconds"] == 100
    assert p["duration_seconds"] == 1000
    assert p["progress"] == 0.1
    assert p["device_id"] == "crosswatch-abc"
    assert isinstance(p["event_created_at"], int)
    assert p["event_id"] and p["playback_session_id"]


def test_episode_payload_carries_season_and_episode(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", media_type="episode", season=1, number=2,
                 ids={"tmdb": "5978363", "tmdb_show": "95396"}, title="Severance"))
    p = calls[0]["json"]

    assert p["media_type"] == "episode"
    assert p["season"] == 1 and p["episode"] == 2


def test_episode_payload_identifies_the_show(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", media_type="episode", season=6, number=2,
                 ids={"tmdb": "5978363", "imdb": "tt35707151", "tvdb": "10958852",
                      "tmdb_show": "69478", "imdb_show": "tt5834204", "tvdb_show": "328487"},
                 title="The Handmaid's Tale"))
    p = calls[0]["json"]

    assert p["tmdb_id"] == 69478, "episode playback must identify the show, not the episode"
    assert p["imdb_id"] == "tt5834204"
    assert p["tvdb_id"] == 328487


def test_episode_without_show_ids_is_skipped(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", media_type="episode", season=1, number=2,
                 ids={"tmdb": "5978363"}, title="Severance"))

    assert calls == []


def test_episode_without_numbering_is_skipped(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", media_type="episode", season=None, number=None))

    assert calls == []


def test_event_without_a_title_is_skipped(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", title=""))

    assert calls == [], "title is a required field"


def test_incomplete_stop_without_a_position_is_skipped(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", progress=5.0))
    s.send(Event(action="stop", progress=40.0, position_ms=None, duration_ms=None))

    assert [_action_of(c) for c in calls] == ["start"], "an incomplete stop needs a resume position"


def test_start_without_a_position_is_still_sent(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", progress=5.0, position_ms=None, duration_ms=None))

    assert [_action_of(c) for c in calls] == ["start"], "start has no position requirement"


def test_a_skipped_write_does_not_leave_a_phantom_session(sink) -> None:
    from providers.scrobble.punchplay import sink as s_mod

    s, calls = sink

    s.send(Event(action="stop", progress=40.0, position_ms=None, duration_ms=None))
    s.send(Event(action="start", progress=5.0))

    assert [_action_of(c) for c in calls] == ["start"]
    assert len(s_mod._SESSIONS) == 1


def test_a_recycled_session_key_on_new_media_gets_a_new_session_id(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", session_key="42", ids={"tmdb": "550"}, title="Fight Club"))
    s.send(Event(action="start", session_key="42", ids={"tmdb": "603"}, title="The Matrix"))

    assert calls[0]["json"]["playback_session_id"] != calls[1]["json"]["playback_session_id"]
    assert [_action_of(c) for c in calls] == ["start", "start"]


def test_event_ids_are_unique_per_event(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", progress=5.0))
    s.send(Event(action="start", progress=15.0))

    assert calls[0]["json"]["event_id"] != calls[1]["json"]["event_id"]


def test_skips_when_not_connected(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.punchplay import sink as s

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(s, "punchplay_request", lambda *a, **k: calls.append(k) or _Resp(200, {}))

    disconnected = {"punchplay": {"access_token": ""}}
    s.PunchPlaySink(cfg_provider=lambda: disconnected).send(Event(action="start"))

    assert calls == []


def test_skips_when_no_supported_ids(sink) -> None:
    s, calls = sink

    s.send(Event(action="start", ids={"anidb": "99"}))

    assert calls == []


# --- CrossWatch cross-cutting behaviour ---------------------------------------

def test_sink_has_parity_with_other_scrobble_sinks() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    ours = (root / "providers" / "scrobble" / "punchplay" / "sink.py").read_text(encoding="utf-8")

    for symbol in ("record_watch", "record_scrobble_event", "mask_account", "once_per_ttl", "_rm_across", "resolve_stop_action"):
        assert symbol in ours, f"punchplay sink is missing {symbol}"


def test_start_and_stop_are_written_to_the_event_archive(sink, monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.punchplay import sink as s

    archived: list[dict[str, Any]] = []
    monkeypatch.setattr(s, "record_watch", lambda ev, **kw: archived.append(kw))

    a, _calls = sink
    a.send(Event(action="start", progress=5.0))
    a.send(Event(action="start", progress=15.0))
    a.send(Event(action="stop", progress=95.0))

    actions = [x["action"] for x in archived]
    assert actions == ["start", "stop"], "progress ticks must not spam the archive"
    assert archived[0]["destination_provider"] == "punchplay"
    assert archived[-1]["progress"] == 95.0


def test_failed_scrobble_is_archived_as_a_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.punchplay import sink as s

    archived: list[dict[str, Any]] = []
    monkeypatch.setattr(s, "record_watch", lambda ev, **kw: archived.append(kw))
    monkeypatch.setattr(s, "punchplay_request", lambda *a, **k: _Resp(500, {"error": "server_error"}))
    s._SESSIONS.clear()

    s.PunchPlaySink(cfg_provider=lambda: dict(CFG)).send(Event(action="start", progress=5.0))

    assert archived and archived[0]["status"] == "fail"
    assert archived[0]["reason"]


def test_watched_stop_records_activity_and_auto_removes(sink, monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.punchplay import sink as s

    activity: list[dict[str, Any]] = []
    removed: list[tuple[Any, ...]] = []
    monkeypatch.setattr(s, "record_scrobble_event", lambda ev, **kw: activity.append(kw))
    monkeypatch.setattr(s, "_rm_across", lambda ids, mt, scope="": removed.append((ids, mt, scope)))
    monkeypatch.setattr(s, "_ar_seen", lambda key: False)

    enabled = dict(CFG)
    enabled["scrobble"] = {**CFG["scrobble"], "delete_plex": True, "delete_plex_types": ["movie", "show"]}
    a = s.PunchPlaySink(cfg_provider=lambda: enabled)
    a.send(Event(action="start", progress=5.0))
    a.send(Event(action="stop", progress=95.0))

    assert len(activity) == 1
    assert activity[0]["target"] == "punchplay"
    assert removed and removed[0][1] == "movie"


def test_auto_remove_is_off_unless_configured(sink, monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.punchplay import sink as s

    removed: list[Any] = []
    monkeypatch.setattr(s, "record_scrobble_event", lambda ev, **kw: None)
    monkeypatch.setattr(s, "_rm_across", lambda ids, mt, scope="": removed.append(ids))
    monkeypatch.setattr(s, "_ar_seen", lambda key: False)

    a, _calls = sink
    a.send(Event(action="start", progress=5.0))
    a.send(Event(action="stop", progress=95.0))

    assert removed == [], "auto-remove must never fire unless the user enabled it"


def test_unwatched_stop_does_not_record_activity_or_remove(sink, monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.punchplay import sink as s

    activity: list[dict[str, Any]] = []
    removed: list[Any] = []
    monkeypatch.setattr(s, "record_scrobble_event", lambda ev, **kw: activity.append(kw))
    monkeypatch.setattr(s, "_rm_across", lambda ids, mt, scope="": removed.append(ids))

    a, _calls = sink
    a.send(Event(action="start", progress=5.0))
    a.send(Event(action="stop", progress=40.0))

    assert activity == []
    assert removed == []


# --- ratings (webhook / watcher rating sink) ----------------------------------

def test_punchplay_is_a_registered_rating_sink() -> None:
    from providers.scrobble.plex.ratings_sync import _ops
    from providers.sync._mod_PUNCHPLAY import OPS

    assert _ops("punchplay") is OPS


def test_send_rating_writes_a_movie_rating(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex.ratings_sync import send_rating
    from providers.sync.punchplay import _common

    sent: list[dict[str, Any]] = []

    def fake(adapter: Any, method: str, url: str, **kw: Any) -> _Resp:
        sent.append({"url": url, **kw})
        return _Resp(200, {"results": [{"index": 0, "status": "updated", "resolved_tmdb_id": 550}]})

    monkeypatch.setattr(_common, "punchplay_request", fake)

    from providers.scrobble.plex.ratings_sync import item_from_plex_rating

    item = item_from_plex_rating("movie", {"type": "movie", "title": "Fight Club", "year": 1999}, {"tmdb": "550"}, 8)
    assert item is not None
    res = send_rating("punchplay", CFG, "default", item, 8)

    assert res["ok"] is True
    assert sent[0]["url"].endswith("/sync/ratings")
    assert sent[0]["json"]["items"][0]["rating"] == 8
    assert "Idempotency-Key" in sent[0]["headers"]


def test_send_rating_zero_clears_the_rating(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex.ratings_sync import send_rating
    from providers.sync.punchplay import _common

    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(
        _common, "punchplay_request",
        lambda adapter, method, url, **kw: (sent.append(kw), _Resp(200, {"results": [{"index": 0, "status": "updated", "resolved_tmdb_id": 550}]}))[1],
    )

    send_rating("punchplay", CFG, "default", {"type": "movie", "ids": {"tmdb": "550"}}, 0)

    assert sent[0]["json"]["items"][0]["rating"] is None


def test_send_rating_handles_episode_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.scrobble.plex.ratings_sync import send_rating
    from providers.sync.punchplay import _common

    sent: list[dict[str, Any]] = []
    monkeypatch.setattr(
        _common, "punchplay_request",
        lambda adapter, method, url, **kw: (sent.append(kw), _Resp(200, {"results": [{"index": 0, "status": "updated", "resolved_tmdb_id": 69478}]}))[1],
    )

    from providers.scrobble.plex.ratings_sync import item_from_plex_rating

    md = {"type": "episode", "parentIndex": 6, "index": 2,
          "grandparentTitle": "The Handmaid's Tale", "title": "Exile"}
    item = item_from_plex_rating("episode", md, {}, 9,
                                 show_ids={"tmdb": "69478"}, episode_ids={"tmdb": "5978363"})
    assert item is not None
    res = send_rating("punchplay", CFG, "default", item, 9)

    p = sent[0]["json"]["items"][0]
    assert res["ok"] is True
    assert p["tmdb_id"] == 69478, "episode ratings must identify the show, not the episode"
    assert p["scope"] == "episode"
    assert p["season"] == 6 and p["episode"] == 2
    assert p["rating"] == 9


def test_plex_watch_exposes_punchplay_rating_toggle() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    watch = (root / "providers" / "scrobble" / "plex" / "watch.py").read_text(encoding="utf-8")

    assert 'enable_punchplay = "punchplay" in custom_targets' in watch
    assert 'enable_punchplay = bool(watch_cfg.get("plex_punchplay_ratings"))' in watch
    assert "dispatch_ops_ratings" in watch

    from providers.scrobble.plex.ratings_sync import OPS_RATING_SINKS, RATING_SINKS

    assert "punchplay" in OPS_RATING_SINKS
    assert "punchplay" in RATING_SINKS


def test_punchplay_ratings_reach_the_plex_webhook() -> None:
    from pathlib import Path

    import providers.webhooks.plex as wh

    root = Path(__file__).resolve().parents[1]
    text = (root / "providers" / "webhooks" / "plex.py").read_text(encoding="utf-8")

    assert 'enable_punchplay_ratings = bool(wh.get("plex_punchplay_ratings", False))' in text
    assert "dispatch_ops_ratings" in text
    assert "plex_punchplay_ratings" in wh._DEF_WEBHOOK
    assert "punchplay" in wh.RATING_SINKS


def test_rating_sink_lists_include_punchplay() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    for rel in (
        "assets/js/modals/scrobbler-route/index.js",
        "assets/js/modals/scrobbler-webhook/index.js",
    ):
        text = (root / rel).read_text(encoding="utf-8")
        assert '"punchplay"' in text.split("const sinks")[1].split("]")[0], rel
        assert '"punchplay"' in text.split("ratingSinks")[1].split("]")[0], rel
        assert 'punchplay: "PunchPlay"' in text, rel

    webhook = (root / "assets/js/modals/scrobbler-webhook/index.js").read_text(encoding="utf-8")
    assert '" (not connected)"' not in webhook
    assert "availableSinks().includes(sink)" in webhook
    assert "availableRatingSinks()" in webhook

    api = (root / "api" / "scrobblerManagementAPI.py").read_text(encoding="utf-8")
    assert '"punchplay": bool(watch.get("plex_punchplay_ratings"))' in api
    assert '"plex_punchplay_ratings"' in api


# PlaybackInput from https://docs.punchplay.tv/openapi.yaml; the schema is additionalProperties: false
PLAYBACK_INPUT_FIELDS = {
    "event_id", "media_type", "title", "year", "imdb_id", "tmdb_id", "tvdb_id", "punchplay_id",
    "season", "episode", "episode_end", "absolute_episode", "episode_title", "multi_episode",
    "anime", "progress", "duration_seconds", "position_seconds", "device_id",
    "playback_session_id", "event_created_at", "client_version", "watched", "watched_threshold",
    "raw_filename", "jellyfin_user_id",
}
PLAYBACK_INPUT_REQUIRED = {"event_id", "event_created_at", "media_type", "playback_session_id", "title"}


@pytest.mark.parametrize("action,progress", [("start", 5.0), ("pause", 40.0), ("stop", 95.0)])
def test_payload_validates_against_the_spec(monkeypatch: pytest.MonkeyPatch, action: str, progress: float) -> None:
    from providers.scrobble.punchplay import sink as s

    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(
        s, "punchplay_request",
        lambda adapter, method, url, **kw: captured.append(kw["json"]) or _Resp(200, {}),
    )
    s._SESSIONS.clear()

    a = s.PunchPlaySink(cfg_provider=lambda: dict(CFG))
    a.send(Event(action="start", progress=1.0))
    a.send(Event(action=action, progress=progress))

    for payload in captured:
        unknown = set(payload) - PLAYBACK_INPUT_FIELDS
        assert not unknown, f"fields not declared in PlaybackInput: {sorted(unknown)}"
        assert PLAYBACK_INPUT_REQUIRED <= set(payload)
        assert 0.0 <= payload["progress"] <= 1.0
        assert payload.get("watched_threshold", 1.0) > 0.0
