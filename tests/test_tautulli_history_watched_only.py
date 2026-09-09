# tests/test_tautulli_history_watched_only.py
# CrossWatch - Tautulli History Partial Playback Gate Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from providers.sync.tautulli import _history as history


WATCHED_AT = 1787093918


def _row(**over: Any) -> dict[str, Any]:
    row = {
        "media_type": "movie",
        "title": "Example movie",
        "year": 2026,
        "rating_key": "42",
        "guid": "com.plexapp.agents.imdb://tt1234567",
        "date": WATCHED_AT,
        "watched_status": 1,
        "percent_complete": 100,
    }
    row.update(over)
    return row


def _adapter(rows: list[dict[str, Any]], cfg: dict[str, Any] | None = None) -> SimpleNamespace:
    calls: list[dict[str, Any]] = []

    def call(cmd: str, **params: Any) -> Any:
        calls.append({"cmd": cmd, **params})
        if cmd != "get_history":
            return None
        if params.get("start", 0):
            return {"data": [], "recordsFiltered": len(rows)}
        return {"data": list(rows), "recordsFiltered": len(rows)}

    client = SimpleNamespace(call=call, calls=calls)
    history_cfg = {"per_page": 100, "max_pages": 5}
    history_cfg.update(cfg or {})
    return SimpleNamespace(client=client, cfg={"tautulli": {"history": history_cfg}}, calls=calls)


def test_partial_playback_is_not_imported_as_watched():
    adapter = _adapter([_row(watched_status=0, percent_complete=2)])
    assert history.build_index(adapter) == {}


def test_half_watched_status_is_not_imported():
    adapter = _adapter([_row(watched_status=0.5, percent_complete=55)])
    assert history.build_index(adapter) == {}


def test_completed_playback_is_imported():
    adapter = _adapter([_row()])
    out = history.build_index(adapter)
    assert len(out) == 1
    assert next(iter(out.values()))["watched_at"].startswith("2026-")


def test_partial_row_skips_metadata_lookups():
    adapter = _adapter([_row(watched_status=0, percent_complete=2, guid="")])
    history.build_index(adapter)
    assert not [c for c in adapter.calls if c["cmd"] == "get_metadata"]


def test_watched_only_disabled_keeps_partial_rows():
    adapter = _adapter([_row(watched_status=0, percent_complete=2)], {"watched_only": False})
    assert len(history.build_index(adapter)) == 1


@pytest.mark.parametrize(
    ("percent", "min_percent", "kept"),
    [(2, 90, False), (40, 90, False), (95, 90, True), (80, 75, True), (80, 90, False)],
)
def test_percent_complete_is_the_fallback_when_watched_status_is_absent(percent, min_percent, kept):
    row = _row(percent_complete=percent)
    row.pop("watched_status")
    adapter = _adapter([row], {"min_percent": min_percent})
    assert len(history.build_index(adapter)) == (1 if kept else 0)


def test_rows_without_completion_fields_are_kept():
    row = _row()
    row.pop("watched_status")
    row.pop("percent_complete")
    adapter = _adapter([row])
    assert len(history.build_index(adapter)) == 1


def test_unparsable_completion_fields_fall_back_rather_than_dropping():
    adapter = _adapter([_row(watched_status="", percent_complete="")])
    assert len(history.build_index(adapter)) == 1


def test_episode_partial_playback_is_not_imported():
    adapter = _adapter([
        _row(
            media_type="episode",
            watched_status=0,
            percent_complete=3,
            parent_media_index=1,
            media_index=2,
            grandparent_title="Example series",
            grandparent_guid="com.plexapp.agents.thetvdb://99",
        )
    ])
    assert history.build_index(adapter) == {}
