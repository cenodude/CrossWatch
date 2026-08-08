# CrossWatch test scripts
from __future__ import annotations

from typing import Any

import pytest

from providers.sync.trakt import _common, _history


@pytest.fixture(autouse=True)
def _reset_settings_memo() -> Any:
    _common._SETTINGS_MEMO = (0.0, None)
    yield
    _common._SETTINGS_MEMO = (0.0, None)


class _Resp:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self._payload = payload
        self.status_code = status
        self.text = "{}"

    def json(self) -> Any:
        return self._payload


class _Session:
    def __init__(self, payload: Any, status: int = 200) -> None:
        self.payload = payload
        self.status = status
        self.calls = 0

    def request(self, *_a: Any, **_k: Any) -> _Resp:
        self.calls += 1
        return _Resp(self.payload, self.status)

    def get(self, *_a: Any, **_k: Any) -> _Resp:
        self.calls += 1
        return _Resp(self.payload, self.status)


class _Cfg:
    timeout = 10.0
    max_retries = 1


class _Client:
    def __init__(self, session: _Session) -> None:
        self.session = session


class _Adapter:
    def __init__(self, session: _Session) -> None:
        self.client = _Client(session)
        self.cfg = _Cfg()
        self.config = {}


def _adapter(settings: Any) -> _Adapter:
    return _Adapter(_Session(settings))


def _play(key: str, watched_at: str, tmdb: str = "111") -> dict[str, Any]:
    return {
        "type": "movie",
        "title": "Example",
        "ids": {"tmdb": tmdb},
        "watched_at": watched_at,
        "_cw_rewatch_sync": True,
        "_cw_event_key": key,
    }


# --- reading the setting ------------------------------------------------------


def test_watch_only_once_true(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common, "headers_for_adapter", lambda a: {})
    assert _common.watch_only_once(_adapter({"browsing": {"watch_only_once": True}})) is True


def test_watch_only_once_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common, "headers_for_adapter", lambda a: {})
    assert _common.watch_only_once(_adapter({"browsing": {"watch_only_once": False}})) is False


def test_watch_only_once_absent_key_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common, "headers_for_adapter", lambda a: {})
    assert _common.watch_only_once(_adapter({"browsing": {}})) is False


def test_missing_browsing_object_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common, "headers_for_adapter", lambda a: {})
    assert _common.watch_only_once(_adapter({})) is None


def test_unreachable_settings_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_common, "headers_for_adapter", lambda a: {})
    adapter = _Adapter(_Session({}, status=500))
    assert _common.watch_only_once(adapter) is None


# --- collapsing ---------------------------------------------------------------


def test_collapse_keeps_newest_play_per_item() -> None:
    items = [
        _play("a@1", "2026-01-01T00:00:00Z"),
        _play("a@2", "2026-03-01T00:00:00Z"),
        _play("a@3", "2026-02-01T00:00:00Z"),
    ]
    out = _history._collapse_rewatch_plays(items)
    assert len(out) == 1
    assert out[0]["watched_at"] == "2026-03-01T00:00:00Z"


def test_collapse_strips_rewatch_markers() -> None:
    out = _history._collapse_rewatch_plays([_play("a@1", "2026-01-01T00:00:00Z")])
    assert "_cw_rewatch_sync" not in out[0]
    assert "_cw_event_key" not in out[0]


def test_collapse_keeps_distinct_items_apart() -> None:
    items = [
        _play("a@1", "2026-01-01T00:00:00Z", tmdb="111"),
        _play("b@1", "2026-01-01T00:00:00Z", tmdb="222"),
    ]
    out = _history._collapse_rewatch_plays(items)
    assert len(out) == 2


def test_collapse_passes_through_non_rewatch_items() -> None:
    plain = {"type": "movie", "ids": {"tmdb": "999"}, "watched_at": "2026-01-01T00:00:00Z"}
    out = _history._collapse_rewatch_plays([plain])
    assert out == [plain]


# --- the guard ----------------------------------------------------------------


def test_guard_collapses_when_setting_is_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_history, "watch_only_once", lambda a: True)
    items = [_play("a@1", "2026-01-01T00:00:00Z"), _play("a@2", "2026-02-01T00:00:00Z")]
    out = _history._guard_watch_only_once(_adapter({}), items)
    assert len(out) == 1
    assert "_cw_rewatch_sync" not in out[0]


def test_guard_is_a_noop_when_setting_is_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_history, "watch_only_once", lambda a: False)
    items = [_play("a@1", "2026-01-01T00:00:00Z"), _play("a@2", "2026-02-01T00:00:00Z")]
    out = _history._guard_watch_only_once(_adapter({}), items)
    assert out == items


def test_guard_skips_the_api_call_without_rewatch_items(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"n": 0}

    def _boom(_a: Any) -> bool:
        called["n"] += 1
        return True

    monkeypatch.setattr(_history, "watch_only_once", _boom)
    plain = [{"type": "movie", "ids": {"tmdb": "999"}, "watched_at": "2026-01-01T00:00:00Z"}]
    assert _history._guard_watch_only_once(_adapter({}), plain) == plain
    assert called["n"] == 0


def test_guard_fails_open_when_settings_lookup_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise(_a: Any) -> bool:
        raise RuntimeError("network down")

    monkeypatch.setattr(_history, "watch_only_once", _raise)
    items = [_play("a@1", "2026-01-01T00:00:00Z"), _play("a@2", "2026-02-01T00:00:00Z")]
    assert _history._guard_watch_only_once(_adapter({}), items) == items


def test_guard_fails_open_when_state_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_history, "watch_only_once", lambda a: None)
    items = [_play("a@1", "2026-01-01T00:00:00Z"), _play("a@2", "2026-02-01T00:00:00Z")]
    assert _history._guard_watch_only_once(_adapter({}), items) == items


@pytest.mark.parametrize(
    "state,expected_event",
    [(None, "watch_only_once_unknown"), (True, "watch_only_once_active"), (False, None)],
)
def test_guard_logs_the_right_event(
    monkeypatch: pytest.MonkeyPatch, state: Any, expected_event: str | None
) -> None:
    seen: list[str] = []
    monkeypatch.setattr(_history, "watch_only_once", lambda a: state)
    monkeypatch.setattr(_history, "_warn", lambda event, **_k: seen.append(event))
    items = [_play("a@1", "2026-01-01T00:00:00Z"), _play("a@2", "2026-02-01T00:00:00Z")]
    _history._guard_watch_only_once(_adapter({}), items)
    assert seen == ([expected_event] if expected_event else [])


def test_unknown_state_does_not_collapse_plays(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_history, "watch_only_once", lambda a: None)
    monkeypatch.setattr(_history, "_warn", lambda *_a, **_k: None)
    items = [_play("a@1", "2026-01-01T00:00:00Z"), _play("a@2", "2026-02-01T00:00:00Z")]
    out = _history._guard_watch_only_once(_adapter({}), items)
    assert len(out) == 2
    assert all(it.get("_cw_rewatch_sync") is True for it in out)
