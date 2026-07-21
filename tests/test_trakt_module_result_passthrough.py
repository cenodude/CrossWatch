# CrossWatch test scripts
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import providers.sync._mod_TRAKT as mod


DEST = {
    "key": "tmdb:12971#s03e26",
    "event_key": "tmdb:12971#s03e26@1704067200",
    "item": {"type": "episode", "season": 3, "episode": 26, "show_ids": {"tmdb": "12971"}},
    "status": "added",
}


def _module(monkeypatch: pytest.MonkeyPatch, result: dict[str, Any]) -> Any:
    stub = SimpleNamespace(add=lambda adapter, items: dict(result))
    monkeypatch.setitem(mod._FEATURES, "history", stub)
    m = mod.TRAKTModule({"trakt": {"access_token": "t", "client_id": "c"}}, connect=False)
    monkeypatch.setattr(m, "_is_enabled", lambda feature: True)
    return m


def test_add_preserves_confirmed_destinations(monkeypatch: pytest.MonkeyPatch) -> None:
    m = _module(monkeypatch, {
        "ok": True,
        "confirmed_keys": ["tmdb:12971#s01e100"],
        "presence_confirmed_keys": ["tmdb:12971#s01e100"],
        "confirmed_destinations": {"tmdb:12971#s01e100": DEST},
        "unresolved": [],
    })

    out = m.add("history", [{"type": "episode", "ids": {"tmdb": "1"}}])

    assert out["confirmed_destinations"] == {"tmdb:12971#s01e100": DEST}
    assert out["presence_confirmed_keys"] == ["tmdb:12971#s01e100"]
    assert out["confirmed_keys"] == ["tmdb:12971#s01e100"]
    assert out["count"] == 1


def test_add_preserves_unknown_forward_compatible_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    m = _module(monkeypatch, {
        "ok": True,
        "confirmed_keys": ["a"],
        "removed_destination_keys": ["x"],
        "some_future_field": {"kept": True},
        "unresolved": [],
    })

    out = m.add("history", [{"type": "episode", "ids": {"tmdb": "1"}}])

    assert out["removed_destination_keys"] == ["x"]
    assert out["some_future_field"] == {"kept": True}


def test_add_still_recomputes_count_and_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    m = _module(monkeypatch, {
        "ok": True,
        "count": 99,
        "confirmed_keys": ["a", "b"],
        "skipped_keys": ["c"],
        "unresolved": [],
    })

    out = m.add("history", [{"type": "episode", "ids": {"tmdb": "1"}}])

    assert out["count"] == 2
    assert out["skipped"] == 1
