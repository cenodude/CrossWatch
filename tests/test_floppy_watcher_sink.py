# tests/test_floppy_watcher_sink.py
# CrossWatch test scripts
from __future__ import annotations

from pathlib import Path
from typing import Any

import providers.scrobble.floppy.sink as floppy_sink
import providers.scrobble.watch_manager as watch_manager
from providers.scrobble.floppy.sink import FloppySink
from providers.scrobble.routes import build_route_cfg, normalize_route
from providers.scrobble.scrobble import ScrobbleEvent
from providers.webhooks.config import sink_configured, webhook_sinks

import pytest


@pytest.fixture(autouse=True)
def _isolate_floppy_dedupe(monkeypatch):
    """Floppy dedupe is persistent (SQLite); give every test a clean slate."""
    seen: set[tuple[str, str]] = set()

    def fake_once_per_ttl(base_path, namespace, key, *, ttl_seconds, max_entries=2000):
        entry = (str(namespace), str(key))
        if entry in seen:
            return False
        seen.add(entry)
        return True

    monkeypatch.setattr(floppy_sink, "once_per_ttl", fake_once_per_ttl)
    return seen



ROOT = Path(__file__).resolve().parents[1]


def _cfg() -> dict[str, Any]:
    return {
        "floppy": {
            "server_url": "http://floppy.local",
            "api_token": "token",
            "verify_ssl": False,
        },
        "scrobble": {
            "watch": {
                "route_provider": "plex",
                "route_provider_instance": "default",
                "route_sink": "floppy",
                "route_sink_instance": "default",
            },
            "trakt": {"watched_at": 90},
        },
    }


def _movie(action: str = "pause", progress: float = 40.0) -> ScrobbleEvent:
    return ScrobbleEvent(
        action=action,
        media_type="movie",
        ids={"tmdb": "1435092", "imdb": "tt1234567"},
        title="Goodbye June",
        year=2026,
        season=None,
        number=None,
        progress=progress,
        account="Living Room",
        server_uuid="server-1",
        session_key="session-1",
        raw={"duration": 6000000, "viewOffset": 2400000},
    )


def _episode(action: str = "stop", progress: float = 92.0) -> ScrobbleEvent:
    return ScrobbleEvent(
        action=action,
        media_type="episode",
        ids={"tmdb_show": "124800", "tvdb_episode": "9621656"},
        title="Love & Death",
        year=2023,
        season=1,
        number=7,
        progress=progress,
        account="Living Room",
        server_uuid="server-1",
        session_key="session-2",
        raw={"duration": 3000000},
    )


def test_watcher_routes_accept_floppy_sink_profile() -> None:
    cfg = {"plex": {"account_token": "source-token"}, "floppy": {"instances": {"FLOPPY-P01": {"server_url": "http://x", "api_token": "t"}}}}
    route = normalize_route({"id": "R1", "provider": "plex", "sink": "floppy", "sink_instance": "FLOPPY-P01"}, "R1")
    view = build_route_cfg(cfg, route)

    assert route["sink"] == "floppy"
    assert view["floppy"]["server_url"] == "http://x"


def test_watch_manager_can_create_floppy_sink() -> None:
    sink = watch_manager._make_sink("floppy", _cfg, "default")

    assert isinstance(sink, FloppySink)


def test_floppy_is_available_as_configured_watcher_destination() -> None:
    cfg = _cfg()
    cfg["scrobble"]["webhook"] = {"sinks": ["floppy", "trakt"]}

    assert sink_configured(cfg, "floppy", "default") is True
    assert "floppy" in webhook_sinks(cfg, "plex", "default")


def test_floppy_sink_sends_movie_pause_payload(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda _adapter, path, **kwargs: calls.append({"path": path, "body": kwargs.get("json")}) or {"detail": "ok"})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *args, **kwargs: events.append(kwargs))

    FloppySink(cfg_provider=_cfg).send(_movie())

    assert calls == [
        {
            "path": "scrobble",
            "body": {
                "action": "pause",
                "media_type": "movie",
                "ids": {"tmdb": "1435092", "imdb": "tt1234567"},
                "title": "Goodbye June",
                "duration_seconds": 6000,
                "position_seconds": 2400,
            },
        }
    ]
    assert events[0]["destination_provider"] == "floppy"


def test_floppy_sink_sends_completed_episode_stop(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []
    activities: list[dict[str, Any]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda _adapter, path, **kwargs: calls.append({"path": path, "body": kwargs.get("json")}) or {"detail": "ok"})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(floppy_sink, "record_scrobble_event", lambda *args, **kwargs: activities.append(kwargs))
    monkeypatch.setattr(floppy_sink, "utc_now_iso", lambda: "2026-08-01T12:00:00Z")

    FloppySink(cfg_provider=_cfg).send(_episode(progress=99.5))

    body = calls[0]["body"]
    assert body["action"] == "stop"
    assert body["media_type"] == "episode"
    assert body["ids"] == {"tmdb": "124800"}
    assert body["series_title"] == "Love & Death"
    assert body["season_number"] == 1
    assert body["episode_number"] == 7
    assert body["position_seconds"] == 2985
    assert body["duration_seconds"] == 3000
    assert body["completed"] is True
    assert body["played_at"] == "2026-08-01T12:00:00Z"
    assert activities[0]["target"] == "floppy"


def test_floppy_sink_sends_completed_false_for_resume_stop(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda _adapter, path, **kwargs: calls.append({"path": path, "body": kwargs.get("json")}) or {"detail": "ok"})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(floppy_sink, "utc_now_iso", lambda: "2026-08-01T12:00:00Z")

    FloppySink(cfg_provider=_cfg).send(_episode(progress=68.0))

    body = calls[0]["body"]
    assert body["action"] == "stop"
    assert body["position_seconds"] == 2040
    assert body["completed"] is False


def test_floppy_sink_does_not_let_floppy_fallback_complete_resume_without_duration(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda _adapter, path, **kwargs: calls.append({"path": path, "body": kwargs.get("json")}) or {"detail": "ok"})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(floppy_sink, "utc_now_iso", lambda: "2026-08-01T12:00:00Z")

    ev = ScrobbleEvent(**{**_episode(progress=53.0).__dict__, "raw": {"viewOffset": 1886000}})

    FloppySink(cfg_provider=_cfg).send(ev)

    body = calls[0]["body"]
    assert body["position_seconds"] == 1886
    assert body["completed"] is False
    assert "duration_seconds" not in body


def test_floppy_sink_prefers_normalized_event_timing(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda _adapter, path, **kwargs: calls.append({"path": path, "body": kwargs.get("json")}) or {"detail": "ok"})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(floppy_sink, "utc_now_iso", lambda: "2026-08-01T12:00:00Z")

    ev = ScrobbleEvent(**{**_episode(progress=31.0).__dict__, "raw": {"viewOffset": 749000}, "position_ms": 749000, "duration_ms": 2416000})

    FloppySink(cfg_provider=_cfg).send(ev)

    body = calls[0]["body"]
    assert body["position_seconds"] == 749
    assert body["duration_seconds"] == 2416
    assert body["completed"] is False


def test_floppy_sink_uses_nested_duration_when_raw_has_zero_first(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda _adapter, path, **kwargs: calls.append({"path": path, "body": kwargs.get("json")}) or {"detail": "ok"})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(floppy_sink, "utc_now_iso", lambda: "2026-08-01T12:00:00Z")

    ev = ScrobbleEvent(**{**_episode(progress=68.0).__dict__, "raw": {"duration": 0, "MediaContainer": {"Metadata": [{"duration": 3000000, "viewOffset": 2040000}]}}})

    FloppySink(cfg_provider=_cfg).send(ev)

    body = calls[0]["body"]
    assert body["duration_seconds"] == 3000
    assert body["position_seconds"] == 2040
    assert body["completed"] is False


def test_scrobbler_route_modal_lists_floppy_sink() -> None:
    text = (ROOT / "assets" / "js" / "modals" / "scrobbler-route" / "index.js").read_text("utf-8")
    meta = (ROOT / "assets" / "helpers" / "provider-meta.js").read_text("utf-8")

    assert '"floppy"' in text.split("const sinks")[1].split("]")[0]
    assert 'floppy: "Floppy"' in text
    assert '"floppy"' in text.split("const ratingSinks")[1].split("]")[0]
    assert "FLOPPY:" in meta
    assert "scrobblerSink: true" in meta


def test_scrobbler_webhook_modal_lists_floppy_sink() -> None:
    text = (ROOT / "assets" / "js" / "modals" / "scrobbler-webhook" / "index.js").read_text("utf-8")

    assert '"floppy"' in text.split("const sinks")[1].split("]")[0]
    assert '"floppy"' in text.split("const ratingSinks")[1].split("]")[0]
    assert 'floppy: "Floppy"' in text
    assert 'floppy: "/assets/img/FLOPPY.png"' in text
    assert "for (const sink of ratingSinks)" in text


# --- sink parity fixes --------------------------------------------------------

def test_completion_honours_the_configured_watched_threshold() -> None:
    from providers.scrobble.floppy import sink as s

    # 80% watched of a 1000s title
    assert s._completed(800_000, 1_000_000, 80.0, 80.0) is True
    assert s._completed(800_000, 1_000_000, 80.0, 95.0) is False
    # no duration -> fall back to the reported percentage
    assert s._completed(1, None, 85.0, 80.0) is True
    assert s._completed(1, None, 85.0, 90.0) is False


def test_watched_threshold_reads_provider_then_trakt_then_default() -> None:
    from providers.scrobble.floppy import sink as s

    assert s._watched_at({"scrobble": {"floppy": {"watched_at": 75}}}) == 75.0
    assert s._watched_at({"scrobble": {"trakt": {"watched_at": 85}}}) == 85.0
    assert s._watched_at({}) == s.DEFAULT_WATCHED_AT


def test_payload_uses_the_configured_threshold() -> None:
    from providers.scrobble.floppy import sink as s
    from providers.scrobble.scrobble import ScrobbleEvent

    ev = ScrobbleEvent(
        action="stop", media_type="movie", ids={"tmdb": "550"}, title="Fight Club", year=1999,
        season=None, number=None, progress=82.0, account="u", server_uuid="srv", session_key="k",
        raw={}, position_ms=820_000, duration_ms=1_000_000,
    )

    lenient = s._payload(ev, 80.0)
    strict = s._payload(ev, 95.0)
    assert lenient is not None and strict is not None
    assert lenient["completed"] is True
    assert strict["completed"] is False


def test_floppy_sink_has_parity_with_other_sinks() -> None:
    text = (ROOT / "providers" / "scrobble" / "floppy" / "sink.py").read_text("utf-8")

    for symbol in ("record_watch", "record_scrobble_event", "mask_account", "once_per_ttl", "_rm_across"):
        assert symbol in text, f"floppy sink is missing {symbol}"
    assert "self._completed" not in text, "completion dedupe must be persistent, not in-memory"


def test_completed_stop_is_deduped_across_restarts(monkeypatch, _isolate_floppy_dedupe) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda adapter, path, **kw: calls.append(kw) or {})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *a, **k: None)
    monkeypatch.setattr(floppy_sink, "record_scrobble_event", lambda *a, **k: None)
    monkeypatch.setattr(floppy_sink, "_rm_across", lambda *a, **k: None)

    event = _episode(progress=99.5)
    FloppySink(cfg_provider=_cfg).send(event)
    # a brand new sink instance stands in for a process restart
    FloppySink(cfg_provider=_cfg).send(event)

    assert len(calls) == 1, "persistent dedupe must survive a new sink instance"


def _cfg_with_auto_remove() -> dict[str, Any]:
    cfg = dict(_cfg())
    scrobble = dict(cfg.get("scrobble") or {})
    scrobble["delete_plex"] = True
    scrobble["delete_plex_types"] = ["movie", "show", "episode"]
    cfg["scrobble"] = scrobble
    return cfg


def test_completed_stop_auto_removes_when_enabled(monkeypatch, _isolate_floppy_dedupe) -> None:
    removed: list[tuple[Any, ...]] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda adapter, path, **kw: {})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *a, **k: None)
    monkeypatch.setattr(floppy_sink, "record_scrobble_event", lambda *a, **k: None)
    monkeypatch.setattr(floppy_sink, "_rm_across", lambda ids, mt, scope="": removed.append((ids, mt, scope)))

    FloppySink(cfg_provider=_cfg_with_auto_remove).send(_episode(progress=99.5))

    assert removed, "a completed stop should auto-remove when the feature is enabled"
    assert removed[0][1] == "episode"
    assert removed[0][2].startswith("floppy:")


def test_auto_remove_is_off_unless_configured(monkeypatch, _isolate_floppy_dedupe) -> None:
    removed: list[Any] = []
    monkeypatch.setattr(floppy_sink, "api_post", lambda adapter, path, **kw: {})
    monkeypatch.setattr(floppy_sink, "record_watch", lambda *a, **k: None)
    monkeypatch.setattr(floppy_sink, "record_scrobble_event", lambda *a, **k: None)
    monkeypatch.setattr(floppy_sink, "_rm_across", lambda ids, mt, scope="": removed.append(ids))

    FloppySink(cfg_provider=_cfg).send(_episode(progress=99.5))

    assert removed == [], "auto-remove must never fire unless the user enabled it"
