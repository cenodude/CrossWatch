from __future__ import annotations

from typing import Any

import pytest

from providers.auth._auth_KODI import KodiAuthError
from providers.scrobble.mdblist import sink as mdblist_sink
from providers.scrobble.simkl import sink as simkl_sink
from providers.scrobble.trakt.sink import TraktSink
from providers.scrobble.kodi import watch as kodi_watch
from providers.scrobble.kodi.watch import KodiWatchService


class FakeDispatcher:
    def __init__(self) -> None:
        self.events = []

    def dispatch(self, event):
        self.events.append(event)
        return True

    def accepts(self, event):
        return True


class RpcScript:
    def __init__(self, **queues: list[Any]) -> None:
        self.queues = {k: list(v) for k, v in queues.items()}
        self.calls = []

    def __call__(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.calls.append((method, params))
        queue = self.queues.setdefault(method, [])
        if not queue:
            raise AssertionError(f"Unexpected Kodi RPC call: {method}")
        value = queue.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def cfg(progress_step: int = 5, kodi_overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    kodi = {"server": "http://kodi.local:8080", "connection_verified": True, "verify_ssl": False}
    kodi.update(kodi_overrides or {})
    return {
        "kodi": kodi,
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True},
            "watch": {"filters": {}},
            "trakt": {"progress_step": progress_step, "force_stop_at": 95},
        },
    }


def svc(
    script: RpcScript,
    dispatcher: FakeDispatcher | None = None,
    progress_step: int = 5,
    kodi_overrides: dict[str, Any] | None = None,
) -> tuple[KodiWatchService, FakeDispatcher]:
    disp = dispatcher or FakeDispatcher()
    service = KodiWatchService(dispatcher=disp, cfg_provider=lambda: cfg(progress_step, kodi_overrides), instance_id="living-room", quiet_startup=True)
    service._rpc = script  # type: ignore[method-assign]
    return service, disp


def active(player_id: int = 1) -> list[dict[str, Any]]:
    return [{"playerid": player_id, "type": "video"}]


def profile(label: str = "Living Room") -> dict[str, Any]:
    return {"label": label}


def props(speed: int = 1, percentage: float = 10.0, live: bool = False) -> dict[str, Any]:
    return {
        "speed": speed,
        "percentage": percentage,
        "time": {"hours": 0, "minutes": 10, "seconds": 0, "milliseconds": 0},
        "totaltime": {"hours": 1, "minutes": 40, "seconds": 0, "milliseconds": 0},
        "live": live,
    }


def movie_item(**extra: Any) -> dict[str, Any]:
    item = {
        "id": 42,
        "type": "movie",
        "title": "Arrival",
        "year": 2016,
        "uniqueid": {"imdb": "tt2543164", "tmdb": "329865"},
        "file": "/movies/arrival.mkv",
        "duration": 6960,
    }
    item.update(extra)
    return item


def episode_item(**extra: Any) -> dict[str, Any]:
    item = {
        "id": 77,
        "type": "episode",
        "title": "The Target",
        "showtitle": "The Expanse",
        "season": 1,
        "episode": 1,
        "year": 2015,
        "uniqueid": {"imdb": "tt3230854", "tvdb": "5534478", "tvshow.tmdb": "63639"},
        "file": "/shows/the-expanse/s01e01.mkv",
        "duration": 2700,
    }
    item.update(extra)
    return item


def test_movie_start_mapping(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active()],
            "Player.GetItem": [{"item": movie_item()}],
            "Player.GetProperties": [props(1, 12)],
            "Profiles.GetCurrentProfile": [profile("Cinema")],
        }
    )
    service, disp = svc(script)

    assert service._tick() is True

    ev = disp.events[-1]
    assert ev.action == "start"
    assert ev.media_type == "movie"
    assert ev.title == "Arrival"
    assert ev.year == 2016
    assert ev.ids == {"imdb": "tt2543164", "tmdb": "329865"}
    assert ev.account == "Cinema"
    assert ev.session_key.startswith("kodi:living-room:1:movie:id:42:")
    get_item = next(params for method, params in script.calls if method == "Player.GetItem")
    assert get_item["properties"] == ["title", "showtitle", "season", "episode", "year", "uniqueid", "file", "duration"]
    get_props = next(params for method, params in script.calls if method == "Player.GetProperties")
    assert get_props["properties"] == ["speed", "time", "totaltime", "percentage", "live"]


def test_start_progress_is_floored_like_other_watchers(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active()],
            "Player.GetItem": [{"item": movie_item()}],
            "Player.GetProperties": [props(1, 0)],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script)

    service._tick()

    assert disp.events[-1].action == "start"
    assert disp.events[-1].progress == 1.0


def test_episode_metadata_and_unique_id_mapping(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active()],
            "Player.GetItem": [{"item": episode_item()}],
            "Player.GetProperties": [props(1, 20)],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script)

    assert service._tick() is True

    ev = disp.events[-1]
    assert ev.action == "start"
    assert ev.media_type == "episode"
    assert ev.title == "The Expanse"
    assert ev.season == 1
    assert ev.number == 1
    assert ev.ids["imdb_episode"] == "tt3230854"
    assert ev.ids["tvdb_episode"] == "5534478"
    assert ev.ids["tmdb_show"] == "63639"
    assert "tvdb" not in ev.ids


def test_kodi_episode_payloads_use_show_title_not_episode_title(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active()],
            "Player.GetItem": [{"item": episode_item(title="The Big Top", showtitle="Task", uniqueid={"tvdb": "9621656"})}],
            "Player.GetProperties": [props(1, 2)],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script)

    assert service._tick() is True

    ev = disp.events[-1]
    assert ev.title == "Task"
    mdblist_body = mdblist_sink._bodies(ev, 2.0)[0]
    simkl_body = simkl_sink._bodies(ev, 2.0)[0]
    assert ev.ids == {"tvdb_episode": "9621656"}
    assert mdblist_body["show"]["title"] == "Task"
    assert simkl_body["show"]["title"] == "Task"


def test_kodi_episode_enriches_show_ids_for_scrobble_sinks(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active()],
            "Player.GetItem": [{"item": episode_item(title="The Manhunt", showtitle="Love & Death", uniqueid={"tvdb": "9621656"})}],
            "Player.GetProperties": [props(1, 2)],
            "VideoLibrary.GetEpisodeDetails": [{"episodedetails": {"tvshowid": 9}}],
            "VideoLibrary.GetTVShowDetails": [{"tvshowdetails": {"tvshowid": 9, "title": "Love & Death", "uniqueid": {"tmdb": "124800", "tvdb": "421130"}}}],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script)

    assert service._tick() is True

    ev = disp.events[-1]
    assert ev.title == "Love & Death"
    assert ev.ids["tvdb_episode"] == "9621656"
    assert ev.ids["tmdb_show"] == "124800"
    assert ev.ids["tvdb_show"] == "421130"
    assert "tvdb" not in ev.ids

    mdblist_body = mdblist_sink._bodies(ev, 2.0)[0]
    trakt_body = TraktSink()._bodies(ev, 2.0)[0]
    simkl_body = simkl_sink._bodies(ev, 2.0)[0]

    assert mdblist_body["show"]["ids"] == {"tmdb": 124800, "tvdb": 421130}
    assert trakt_body["show"]["ids"] == {"tmdb": "124800", "tvdb": "421130"}
    assert simkl_body["show"]["ids"] == {"tmdb": "124800", "tvdb": "421130"}


def test_pause_and_resume_detection(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active(), active(), active()],
            "Player.GetItem": [{"item": movie_item()}],
            "Player.GetProperties": [props(1, 10), props(0, 11), props(1, 12)],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script)

    service._tick()
    service._tick()
    service._tick()

    assert [e.action for e in disp.events] == ["start", "pause", "start"]


def test_progress_update_detection(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active(), active(), active()],
            "Player.GetItem": [{"item": movie_item()}],
            "Player.GetProperties": [props(1, 10), props(1, 12), props(1, 16)],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script, progress_step=5)

    service._tick()
    service._tick()
    service._tick()

    assert [(e.action, int(e.progress)) for e in disp.events] == [("start", 10), ("start", 16)]


def test_stop_after_two_successful_empty_polls(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    monkeypatch.setattr(kodi_watch, "_cw_update_payload", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active(), [], []],
            "Player.GetItem": [{"item": movie_item()}],
            "Player.GetProperties": [props(1, 92)],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script)

    service._tick()
    service._tick()
    assert [e.action for e in disp.events] == ["start"]
    service._tick()

    assert [e.action for e in disp.events] == ["start", "stop"]
    assert int(disp.events[-1].progress) == 92
    assert not service._sessions


def test_connection_failure_does_not_generate_stop(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active(), KodiAuthError("timeout", reason="unreachable")],
            "Player.GetItem": [{"item": movie_item()}],
            "Player.GetProperties": [props(1, 30)],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script)

    service._tick()
    assert service._tick() is True

    assert [e.action for e in disp.events] == ["start"]
    assert service._sessions


def test_repeated_offline_failures_backoff_without_log_spam(monkeypatch):
    logs = []
    monkeypatch.setattr(kodi_watch, "_log", lambda msg, level="INFO": logs.append((level, msg)))
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [
                KodiAuthError("Kodi server is unreachable: timeout", reason="unreachable"),
                KodiAuthError("Kodi server is unreachable: timeout", reason="unreachable"),
                KodiAuthError("Kodi server is unreachable: timeout", reason="unreachable"),
            ],
        }
    )
    service, disp = svc(script)

    assert service._tick() is False
    assert service._offline is True
    assert service._offline_retry == 30.0
    assert service._tick() is False
    assert service._offline_retry == 60.0
    assert service._tick() is False
    assert service._offline_retry == 120.0

    assert disp.events == []
    assert logs == [("WARNING", "Kodi watcher offline: Kodi server is unreachable: timeout; retrying with backoff")]


def test_offline_reconnect_resets_backoff_and_logs_once(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    logs = []
    monkeypatch.setattr(kodi_watch, "_log", lambda msg, level="INFO": logs.append((level, msg)))
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [
                KodiAuthError("Kodi server is unreachable: timeout", reason="unreachable"),
                active(),
            ],
            "Player.GetItem": [{"item": movie_item()}],
            "Player.GetProperties": [props(1, 33)],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script)

    assert service._tick() is False
    assert service._offline is True
    assert service._tick() is True

    assert service._offline is False
    assert service._offline_retry == 30.0
    assert disp.events[-1].action == "start"
    assert int(disp.events[-1].progress) == 33
    lifecycle_logs = [row for row in logs if row[0] in {"WARNING", "INFO"}]
    assert lifecycle_logs == [
        ("WARNING", "Kodi watcher offline: Kodi server is unreachable: timeout; retrying with backoff"),
        ("INFO", "Kodi watcher reconnected"),
    ]


def test_active_playback_is_recovered_when_watcher_starts(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active()],
            "Player.GetItem": [{"item": movie_item(title="Dune")}],
            "Player.GetProperties": [props(1, 43)],
            "Profiles.GetCurrentProfile": [profile()],
        }
    )
    service, disp = svc(script)

    assert service._tick() is True

    ev = disp.events[-1]
    assert ev.action == "start"
    assert ev.title == "Dune"
    assert int(ev.progress) == 43


@pytest.mark.parametrize(
    ("item", "properties"),
    [
        (movie_item(type="musicvideo"), props(1, 10)),
        (movie_item(), props(1, 10, live=True)),
    ],
)
def test_unsupported_and_live_media_are_ignored(monkeypatch, item, properties):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active()],
            "Player.GetItem": [{"item": item}],
            "Player.GetProperties": [properties],
        }
    )
    service, disp = svc(script)

    assert service._tick() is False
    assert disp.events == []
    assert not service._sessions


def test_scrobble_source_whitelist_ignores_unselected_paths(monkeypatch):
    monkeypatch.setattr(kodi_watch, "_cw_update", lambda *a, **k: None)
    logs = []
    monkeypatch.setattr(kodi_watch, "_log", lambda msg, level="INFO": logs.append((level, msg)))
    script = RpcScript(
        **{
            "Player.GetActivePlayers": [active()],
            "Player.GetItem": [{"item": movie_item(file="/other/arrival.mkv")}],
            "Player.GetProperties": [props(1, 10)],
        }
    )
    service, disp = svc(script, kodi_overrides={"instances": {"living-room": {"scrobble": {"libraries": ["/movies"]}}}})

    assert service._tick() is False
    assert disp.events == []
    assert not service._sessions
    assert logs
    assert logs[-1][0] == "DEBUG"
    assert "event filtered by scrobble whitelist" in logs[-1][1]
    assert "allowed=['/movies']" in logs[-1][1]
