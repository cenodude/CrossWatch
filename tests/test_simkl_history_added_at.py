# /tests/test_simkl_history_added_at.py
# CrossWatch - SIMKL history added_at stamping for items new to the library
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cw_platform.history_events import minimal_history_item
from providers.sync.simkl import _history as history


def _response(payload, status=200):
    return SimpleNamespace(status_code=status, ok=200 <= status < 300,
                           text=json.dumps(payload), json=lambda: payload)


def _movie(tmdb, watched_at):
    return minimal_history_item({"type": "movie", "ids": {"tmdb": tmdb}, "watched_at": watched_at}, event_mode=False)


def _episode(number, watched_at, tmdb="96777"):
    item = {"type": "episode", "season": 1, "episode": number,
            "show_ids": {"tmdb": tmdb}, "watched_at": watched_at}
    return minimal_history_item(item, event_mode=False)


@pytest.fixture
def env(config_base, monkeypatch):
    cache: dict[str, dict] = {}
    monkeypatch.setattr(history, "state_file", lambda name: config_base / name)
    monkeypatch.setattr(history, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(history, "_cache_load", lambda: dict(cache))
    monkeypatch.setattr(history, "_inject_adds_into_cache", Mock())
    monkeypatch.setattr(history, "_remember_source_aliases", Mock())
    session = SimpleNamespace(post=Mock(return_value=_response({
        "added": {"movies": 1, "shows": 1, "episodes": 1},
        "not_found": {"movies": [], "shows": [], "episodes": []},
    })), get=Mock(return_value=_response({})))
    adapter = SimpleNamespace(client=SimpleNamespace(session=session),
                              cfg=SimpleNamespace(timeout=5), config={}, raw_cfg={})
    return adapter, session, cache


def _body(session):
    return session.post.call_args.kwargs["json"]


def test_new_movie_and_show_get_added_at_from_earliest_watch(env):
    adapter, session, _cache = env
    history.add(adapter, [
        _movie("101", "2024-03-01T20:00:00Z"),
        _episode(2, "2024-04-01T20:00:00Z"),
        _episode(1, "2024-03-15T20:00:00Z"),
    ])
    body = _body(session)
    assert body["movies"][0]["added_at"] == "2024-03-01T20:00:00Z"
    assert body["shows"][0]["added_at"] == "2024-03-15T20:00:00Z"


def test_items_already_in_history_keep_their_added_date(env):
    adapter, session, cache = env
    cache["m"] = {"type": "movie", "ids": {"tmdb": "101", "simkl": "53192"}, "watched_at": "2023-01-01T00:00:00Z"}
    cache["e"] = {"type": "episode", "season": 1, "episode": 1, "show_ids": {"tmdb": "96777", "simkl": "12345"},
                  "watched_at": "2023-01-01T00:00:00Z"}
    history.add(adapter, [
        _movie("101", "2024-03-01T20:00:00Z"),
        _episode(2, "2024-04-01T20:00:00Z"),
    ])
    body = _body(session)
    assert "added_at" not in body["movies"][0]
    assert "added_at" not in body["shows"][0]


def test_movie_and_show_ids_do_not_share_a_namespace(env):
    adapter, session, cache = env
    cache["e"] = {"type": "episode", "season": 1, "episode": 1, "show_ids": {"tmdb": "101"},
                  "watched_at": "2023-01-01T00:00:00Z"}
    history.add(adapter, [_movie("101", "2024-03-01T20:00:00Z")])
    assert _body(session)["movies"][0]["added_at"] == "2024-03-01T20:00:00Z"


def test_unreadable_cache_sends_no_added_at(env, monkeypatch):
    adapter, session, _cache = env
    monkeypatch.setattr(history, "_cache_load", Mock(side_effect=RuntimeError("boom")))
    history.add(adapter, [_movie("101", "2024-03-01T20:00:00Z")])
    assert "added_at" not in _body(session)["movies"][0]
