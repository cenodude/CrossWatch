from __future__ import annotations

from copy import deepcopy
from typing import Any


class _Resp:
    def __init__(self, status_code: int = 200, *, activity_recorded: bool = True) -> None:
        self.status_code = status_code
        self.text = ""
        self._body = {"activity_recorded": activity_recorded}

    def json(self) -> dict[str, Any]:
        return dict(self._body)


def _cfg() -> dict[str, Any]:
    return {
        "scrobble": {
            "enabled": True,
            "sources": {"webhook": True},
            "webhook": {"sinks": ["trakt"], "pause_debounce_seconds": 0},
            "trakt": {"complete_at": 95, "force_stop_at": 95, "watched_at": 90},
        }
    }


def _emby_payload(session_id: str = "session-1") -> dict[str, Any]:
    return {
        "NotificationType": "PlaybackStop",
        "SessionId": session_id,
        "UserName": "pasca",
        "Progress": 95,
        "Item": {
            "Id": "movie-1",
            "Type": "Movie",
            "Name": "Replay Movie",
            "ProductionYear": 2026,
            "ProviderIds": {"Tmdb": "550"},
        },
    }


def _jellyfin_payload(session_id: str = "session-1") -> dict[str, Any]:
    return {
        "NotificationType": "scrobble",
        "SessionId": session_id,
        "UserName": "pasca",
        "Progress": 95,
        "Item": {
            "Id": "movie-1",
            "Type": "Movie",
            "Name": "Replay Movie",
            "ProductionYear": 2026,
            "ProviderIds": {"Tmdb": "550"},
        },
    }


def _plex_payload(session_id: str = "session-1") -> dict[str, Any]:
    return {
        "event": "media.scrobble",
        "sessionKey": session_id,
        "Account": {"title": "pasca", "uuid": "account-1"},
        "Player": {"uuid": "player-1"},
        "Metadata": {
            "type": "movie",
            "title": "Replay Movie",
            "year": 2026,
            "ratingKey": "movie-1",
            "duration": 100_000,
            "viewOffset": 95_000,
            "Guid": [{"id": "tmdb://550"}],
        },
    }


def _patch_common(monkeypatch: Any, module: Any, responses: list[_Resp] | None = None) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    queued = list(responses or [_Resp()])

    def fake_dispatch(provider: str, action: str, **kwargs: Any) -> _Resp:
        calls.append({"provider": provider, "action": action, **kwargs})
        if queued:
            return queued.pop(0)
        return _Resp()

    if hasattr(module, "_SCROBBLE_STATE"):
        module._SCROBBLE_STATE.clear()
    if hasattr(module, "_LAST_FINISH_BY_ACC"):
        module._LAST_FINISH_BY_ACC.clear()
    if hasattr(module, "_TRAKT_ID_CACHE"):
        module._TRAKT_ID_CACHE.clear()

    monkeypatch.setattr(module, "_dispatch_scrobble", fake_dispatch)
    monkeypatch.setattr(module, "_cw_update", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_call_remove_across", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_archive", lambda *args, **kwargs: None)
    if hasattr(module, "_resolve_trakt_movie_id"):
        monkeypatch.setattr(module, "_resolve_trakt_movie_id", lambda *args, **kwargs: None)
    return calls


def test_emby_normal_completion_still_writes_once(monkeypatch: Any) -> None:
    from providers.webhooks import emby

    calls = _patch_common(monkeypatch, emby)

    result = emby.process_webhook(deepcopy(_emby_payload()), {}, cfg=_cfg())

    assert result["ok"] is True
    assert result["action"] == "/scrobble/stop"
    assert len(calls) == 1
    assert calls[0]["action"] == "/scrobble/stop"


def test_emby_exact_successful_completion_replay_does_not_write_again(monkeypatch: Any) -> None:
    from providers.webhooks import emby

    calls = _patch_common(monkeypatch, emby)

    first = emby.process_webhook(deepcopy(_emby_payload()), {}, cfg=_cfg())
    replay = emby.process_webhook(deepcopy(_emby_payload()), {}, cfg=_cfg())

    assert first["ok"] is True
    assert replay["ok"] is True
    assert replay["dedup"] is True
    assert len(calls) == 1


def test_emby_failed_outbound_completion_can_be_retried(monkeypatch: Any) -> None:
    from providers.webhooks import emby

    calls = _patch_common(
        monkeypatch,
        emby,
        [_Resp(502, activity_recorded=False), _Resp(200, activity_recorded=True)],
    )

    failed = emby.process_webhook(deepcopy(_emby_payload()), {}, cfg=_cfg())
    retried = emby.process_webhook(deepcopy(_emby_payload()), {}, cfg=_cfg())

    assert failed["ok"] is False
    assert retried["ok"] is True
    assert len(calls) == 2


def test_jellyfin_failed_outbound_completion_can_be_retried_immediately(monkeypatch: Any) -> None:
    from providers.webhooks import jellyfin

    calls = _patch_common(
        monkeypatch,
        jellyfin,
        [_Resp(502, activity_recorded=False), _Resp(200, activity_recorded=True)],
    )
    monkeypatch.setattr(jellyfin.time, "time", lambda: 1_000.0)

    failed = jellyfin.process_webhook(deepcopy(_jellyfin_payload("session-1")), {}, cfg=_cfg())
    retried = jellyfin.process_webhook(deepcopy(_jellyfin_payload("session-1")), {}, cfg=_cfg())

    assert failed["ok"] is False
    assert retried["ok"] is True
    assert len(calls) == 2


def test_plex_failed_outbound_completion_can_be_retried_immediately(monkeypatch: Any) -> None:
    from providers.webhooks import plex

    calls = _patch_common(
        monkeypatch,
        plex,
        [_Resp(502, activity_recorded=False), _Resp(200, activity_recorded=True)],
    )
    monkeypatch.setattr(plex.time, "time", lambda: 1_000.0)

    failed = plex.process_webhook(deepcopy(_plex_payload("session-1")), {}, cfg=_cfg())
    retried = plex.process_webhook(deepcopy(_plex_payload("session-1")), {}, cfg=_cfg())

    assert failed["ok"] is False
    assert retried["ok"] is True
    assert len(calls) == 2


def test_jellyfin_later_legitimate_rewatch_same_media_writes(monkeypatch: Any) -> None:
    from providers.webhooks import jellyfin

    calls = _patch_common(monkeypatch, jellyfin)
    now = [1_000.0]
    monkeypatch.setattr(jellyfin.time, "time", lambda: now[0])

    first = jellyfin.process_webhook(deepcopy(_jellyfin_payload("session-1")), {}, cfg=_cfg())
    now[0] += 30.0
    rewatch = jellyfin.process_webhook(deepcopy(_jellyfin_payload("session-2")), {}, cfg=_cfg())

    assert first["ok"] is True
    assert rewatch["ok"] is True
    assert len(calls) == 2
    assert [call["session_key"] for call in calls] == ["session-1|movie-1", "session-2|movie-1"]


def test_jellyfin_exact_successful_completion_replay_after_window_is_suppressed(monkeypatch: Any) -> None:
    from providers.webhooks import jellyfin

    calls = _patch_common(monkeypatch, jellyfin)
    now = [1_000.0]
    monkeypatch.setattr(jellyfin.time, "time", lambda: now[0])

    first = jellyfin.process_webhook(deepcopy(_jellyfin_payload("session-1")), {}, cfg=_cfg())
    now[0] += 181.0
    replay = jellyfin.process_webhook(deepcopy(_jellyfin_payload("session-1")), {}, cfg=_cfg())

    assert first["ok"] is True
    assert replay["ok"] is True
    assert replay["dedup"] is True
    assert len(calls) == 1


def test_plex_safe_session_completion_replay_after_window_is_suppressed(monkeypatch: Any) -> None:
    from providers.webhooks import plex

    calls = _patch_common(monkeypatch, plex)
    now = [1_000.0]
    monkeypatch.setattr(plex.time, "time", lambda: now[0])

    first = plex.process_webhook(deepcopy(_plex_payload("session-1")), {}, cfg=_cfg())
    now[0] += 181.0
    replay = plex.process_webhook(deepcopy(_plex_payload("session-1")), {}, cfg=_cfg())

    assert first["ok"] is True
    assert replay["ok"] is True
    assert replay["dedup"] is True
    assert len(calls) == 1


def test_plex_separate_playback_sessions_same_media_are_not_deduplicated(monkeypatch: Any) -> None:
    from providers.webhooks import plex

    calls = _patch_common(monkeypatch, plex)
    now = [1_000.0]
    monkeypatch.setattr(plex.time, "time", lambda: now[0])

    first = plex.process_webhook(deepcopy(_plex_payload("session-1")), {}, cfg=_cfg())
    now[0] += 30.0
    second = plex.process_webhook(deepcopy(_plex_payload("session-2")), {}, cfg=_cfg())

    assert first["ok"] is True
    assert second["ok"] is True
    assert len(calls) == 2
    assert [call["session_key"] for call in calls] == [
        "rk:movie-1|s:session-1|p:player-1|u:account-1",
        "rk:movie-1|s:session-2|p:player-1|u:account-1",
    ]
