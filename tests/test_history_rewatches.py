# tests/test_history_rewatches.py
# CrossWatch - rewatch history event tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Mapping

from cw_platform.history_events import history_sync_key, minimal_history_item
from cw_platform.orchestrator._history_rewatches import (
    collapse_history_latest,
    filter_history_events,
    history_event_diff,
    history_rewatch_pair_enabled,
)


def _episode(watched_at: str, *, event_id: str = "") -> dict[str, Any]:
    item: dict[str, Any] = {
        "type": "episode",
        "series_title": "The Show",
        "season": 1,
        "episode": 2,
        "watched_at": watched_at,
        "show_ids": {"tmdb": "42"},
        "ids": {},
    }
    if event_id:
        item["provider_event_id"] = event_id
    return item


def _tokens(item: Mapping[str, Any]) -> set[str]:
    ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else item.get("ids")
    values = ids if isinstance(ids, Mapping) else {}
    return {f"{k}:{v}#s{int(item['season']):02d}e{int(item['episode']):02d}" for k, v in values.items()}


class _Ops:
    def __init__(self, read: bool, write: bool) -> None:
        self._read = read
        self._write = write

    def capabilities(self) -> dict[str, Any]:
        return {"history": {"rewatches": {"read": self._read, "write": self._write}}}


def test_plex_can_source_rewatches_but_not_bidirectional() -> None:
    from providers.sync import _mod_PLEX

    plex = _mod_PLEX.OPS
    target = _Ops(True, True)

    assert history_rewatch_pair_enabled("history", {"rewatches": True}, "PLEX", plex, "TARGET", target)
    assert not history_rewatch_pair_enabled("history", {"rewatches": True}, "PLEX", plex, "TARGET", target, bidirectional=True)


def test_emby_and_jellyfin_do_not_source_rewatches() -> None:
    from providers.sync import _mod_EMBY, _mod_JELLYFIN

    target = _Ops(True, True)

    assert not history_rewatch_pair_enabled("history", {"rewatches": True}, "EMBY", _mod_EMBY.OPS, "TARGET", target)
    assert not history_rewatch_pair_enabled("history", {"rewatches": True}, "JELLYFIN", _mod_JELLYFIN.OPS, "TARGET", target)


def test_event_history_providers_can_source_rewatches() -> None:
    from providers.sync import _mod_FLICKLIST, _mod_PUNCHPLAY, _mod_TAUTULLI

    target = _Ops(True, True)

    for name, ops in (
        ("FLICKLIST", _mod_FLICKLIST.OPS),
        ("PUNCHPLAY", _mod_PUNCHPLAY.OPS),
        ("TAUTULLI", _mod_TAUTULLI.OPS),
    ):
        assert history_rewatch_pair_enabled("history", {"rewatches": True}, name, ops, "TARGET", target)

    assert not history_rewatch_pair_enabled(
        "history",
        {"rewatches": True},
        "TAUTULLI",
        _mod_TAUTULLI.OPS,
        "TARGET",
        target,
        bidirectional=True,
    )


def test_tautulli_rewatch_mode_keeps_each_play_row() -> None:
    from providers.sync.tautulli import _history

    class Client:
        def call(self, cmd: str, **_params: Any) -> Mapping[str, Any]:
            assert cmd == "get_history"
            return {
                "data": [
                    {
                        "row_id": 7,
                        "media_type": "movie",
                        "date": 1704153600,
                        "title": "Fight Club",
                        "year": 1999,
                        "guid": "com.plexapp.agents.themoviedb://550",
                    },
                    {
                        "row_id": 9,
                        "media_type": "movie",
                        "date": 1704067200,
                        "title": "Fight Club",
                        "year": 1999,
                        "guid": "com.plexapp.agents.themoviedb://550",
                    },
                ],
                "recordsTotal": 2,
            }

    idx = _history.build_index(SimpleNamespace(cfg={"_cw_history_rewatches": True}, client=Client()))

    assert sorted(idx) == ["tmdb:550@1704067200", "tmdb:550@1704153600"]
    assert all(item.get("_cw_rewatch_sync") is True for item in idx.values())


def test_history_event_keys_keep_rewatches_separate() -> None:
    a = _episode("2024-01-01T00:00:00Z")
    b = _episode("2024-01-02T00:00:00Z")

    assert history_sync_key(a, event_mode=False) == history_sync_key(b, event_mode=False)
    assert history_sync_key(a, event_mode=True) != history_sync_key(b, event_mode=True)


def test_filter_events_and_collapse_latest_preserve_existing_behavior() -> None:
    first = _episode("2024-01-01T00:00:00Z")
    second = _episode("2024-01-02T00:00:00Z")
    idx = {
        history_sync_key(first, event_mode=True): first,
        history_sync_key(second, event_mode=True): second,
    }

    assert len(filter_history_events(idx, event_mode=True)) == 2
    collapsed = collapse_history_latest(idx)
    assert len(collapsed) == 1
    assert next(iter(collapsed.values()))["watched_at"] == "2024-01-02T00:00:00Z"


def test_state_filter_requires_history_timestamp() -> None:
    item = {
        "type": "episode",
        "series_title": "The Show",
        "season": 1,
        "episode": 2,
        "watched": True,
        "show_ids": {"tmdb": "42"},
        "ids": {},
    }

    state = filter_history_events({"tmdb:42#s01e02": item}, event_mode=False)
    events = filter_history_events({"tmdb:42#s01e02": item}, event_mode=True)

    assert state == {}
    assert events == {}


def test_event_diff_compares_exact_history_identity() -> None:
    first = _episode("2024-01-01T00:00:00Z")
    second = _episode("2024-01-02T00:00:00Z")
    src = {
        history_sync_key(first, event_mode=True): minimal_history_item(first, event_mode=True),
        history_sync_key(second, event_mode=True): minimal_history_item(second, event_mode=True),
    }
    dst = {
        history_sync_key(first, event_mode=True): minimal_history_item(first, event_mode=True),
    }

    adds, removes = history_event_diff(src, dst, _tokens)

    assert len(adds) == 1
    assert adds[0]["watched_at"] == "2024-01-02T00:00:00Z"
    assert removes == []


def test_rewatch_pair_requires_read_write_capabilities() -> None:
    assert history_rewatch_pair_enabled("history", {"rewatches": True}, "A", _Ops(True, False), "B", _Ops(True, True))
    assert not history_rewatch_pair_enabled("history", {"rewatches": True}, "A", _Ops(True, False), "B", _Ops(True, False))
    assert not history_rewatch_pair_enabled("history", {"rewatches": False}, "A", _Ops(True, True), "B", _Ops(True, True))
