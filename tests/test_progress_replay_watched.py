from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

import pytest

from providers.sync import _progress_policy as policy
from providers.sync.jellyfin import _progress as jf_progress


ISSUE_689_SOURCE_TS = "2026-08-06T17:50:23.000Z"
ISSUE_689_TARGET_TS = "2026-08-06T17:51:05.853Z"
ISSUE_689_PROGRESS_MS = 932_400
RUNTIME_MS = 6_660_000


def _decide(**overrides: Any) -> policy.ProgressDecision:
    kwargs: dict[str, Any] = {
        "active_session": False,
        "source_timestamp": ISSUE_689_SOURCE_TS,
        "target_timestamp": ISSUE_689_TARGET_TS,
        "source_progress_ms": ISSUE_689_PROGRESS_MS,
        "source_duration_ms": RUNTIME_MS,
        "target_progress_ms": None,
        "target_duration_ms": RUNTIME_MS,
        "target_watched": True,
        "same_origin": False,
        "replay_enabled": True,
        "timestamp_tolerance_seconds": 30,
    }
    kwargs.update(overrides)
    return policy.decide_progress_write(**kwargs)


def test_replay_overrides_target_newer_for_watched_target() -> None:
    decision = _decide()

    assert decision.apply is True
    assert decision.reason == "apply"
    assert decision.unwatch_first is True


def test_target_newer_still_blocks_unwatched_target_when_replay_enabled() -> None:
    decision = _decide(target_watched=False)

    assert decision.apply is False
    assert decision.reason == "target_newer"
    assert decision.unwatch_first is False


def test_target_newer_still_blocks_watched_target_without_replay() -> None:
    decision = _decide(replay_enabled=False)

    assert decision.apply is False
    assert decision.reason == "target_newer"


def test_replay_ignores_progress_unchanged_while_target_is_still_watched() -> None:
    decision = _decide(target_progress_ms=ISSUE_689_PROGRESS_MS)

    assert decision.apply is True
    assert decision.unwatch_first is True


def test_replay_converges_once_target_is_unwatched() -> None:
    decision = _decide(
        target_watched=False,
        target_timestamp=ISSUE_689_SOURCE_TS,
        target_progress_ms=ISSUE_689_PROGRESS_MS,
    )

    assert decision.apply is False
    assert decision.reason == "progress_unchanged"


def test_replay_does_not_override_active_session_or_same_origin() -> None:
    assert _decide(active_session=True).reason == "active_session"
    assert _decide(same_origin=True).reason == "same_origin"


def test_watched_target_without_replay_reports_already_watched() -> None:
    decision = _decide(replay_enabled=False, target_timestamp=ISSUE_689_SOURCE_TS)

    assert decision.apply is False
    assert decision.reason == "already_watched"


class _Response:
    def __init__(self, status_code: int, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload


@dataclass
class _FakeCfg:
    user_id: str = "user-1"
    progress_replay_enabled: bool = True
    progress_timestamp_tolerance_seconds: int = 30
    progress_libraries: list[str] | None = None
    strict_id_matching: bool = False


@dataclass
class _FakeHttp:
    watched: bool = True
    calls: list[tuple[str, str]] = field(default_factory=list)
    posts: list[dict[str, Any]] = field(default_factory=list)

    def get(self, path: str, params: Mapping[str, Any] | None = None) -> _Response:
        self.calls.append(("GET", path))
        if path == "/Sessions":
            return _Response(200, [])
        return _Response(
            200,
            {
                "Id": "jf-item-1",
                "RunTimeTicks": RUNTIME_MS * 10_000,
                "LibraryId": "lib-1",
                "UserData": {
                    "Played": self.watched,
                    "PlaybackPositionTicks": 0,
                    "LastPlayedDate": ISSUE_689_TARGET_TS,
                },
            },
        )

    def delete(self, path: str, params: Mapping[str, Any] | None = None) -> _Response:
        self.calls.append(("DELETE", path))
        self.watched = False
        return _Response(204)

    def post(self, path: str, params: Mapping[str, Any] | None = None, json: Any = None) -> _Response:
        self.calls.append(("POST", path))
        self.posts.append(dict(json or {}))
        return _Response(204)


class _FakeAdapter:
    def __init__(self, http: _FakeHttp, cfg: _FakeCfg) -> None:
        self.client = http
        self.cfg = cfg


def _mdblist_item() -> dict[str, Any]:
    return {
        "type": "movie",
        "title": "Example",
        "year": 2026,
        "ids": {"tmdb": "1083381", "imdb": "tt26657236"},
        "jellyfin_item_id": "jf-item-1",
        "progress_ms": ISSUE_689_PROGRESS_MS,
        "duration_ms": RUNTIME_MS,
        "progress_at": ISSUE_689_SOURCE_TS,
    }


@pytest.fixture(autouse=True)
def _clear_pair_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("CW_PAIR_SRC", "CW_PAIR_DST", "CW_PAIR_SRC_INSTANCE", "CW_PAIR_DST_INSTANCE"):
        monkeypatch.delenv(name, raising=False)


def test_jellyfin_replay_unwatches_before_writing_resume_position() -> None:
    http = _FakeHttp(watched=True)
    adapter = _FakeAdapter(http, _FakeCfg(progress_replay_enabled=True))

    applied, unresolved = jf_progress.add(adapter, [_mdblist_item()])

    assert applied == 1
    assert unresolved == []
    assert ("DELETE", "/UserPlayedItems/jf-item-1") in http.calls
    assert http.calls.index(("DELETE", "/UserPlayedItems/jf-item-1")) < http.calls.index(
        ("POST", "/UserItems/jf-item-1/UserData")
    )
    assert http.posts == [
        {"PlaybackPositionTicks": ISSUE_689_PROGRESS_MS * 10_000, "LastPlayedDate": ISSUE_689_SOURCE_TS}
    ]

    results = getattr(adapter, "_progress_write_results")
    assert [row["status"] for row in results] == ["applied"]


def test_jellyfin_without_replay_still_skips_on_target_newer() -> None:
    http = _FakeHttp(watched=True)
    adapter = _FakeAdapter(http, _FakeCfg(progress_replay_enabled=False))

    applied, unresolved = jf_progress.add(adapter, [_mdblist_item()])

    assert applied == 0
    assert unresolved == []
    assert not any(method == "DELETE" for method, _path in http.calls)
    assert not http.posts

    results = getattr(adapter, "_progress_write_results")
    assert [(row["status"], row["reason"]) for row in results] == [("skipped", "target_newer")]
