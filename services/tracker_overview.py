# services/tracker_overview.py
# CrossWatch - Tracker inputs for shared episode suggestions and upcoming dates
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import date
from typing import Any

from cw_platform.tracker_store import TrackerStore
from services.episode_overview import episode_payload


def _inputs(store: TrackerStore, offset: int) -> tuple[int, list[tuple[dict[str, Any], set[tuple[int, int]]]]]:
    with store.session():
        hidden = {key[5:] for key in store.status_keys("dropped") if key.startswith("show:")}
        total, shows = store.show_summaries(offset=offset, limit=32, exclude=hidden)
        notes = store.notes_for(f"show:{show['key']}" for show in shows)
        watched = store.watched_episodes(show["key"] for show in shows)
        for show in shows:
            show["status"] = (notes.get(f"show:{show['key']}") or {}).get("status")
    return total, [(show, watched.get(show["key"], set())) for show in shows]


def overview_payload(store: TrackerStore, provider: Any, today: date) -> dict[str, Any]:
    def shows():
        offset = 0
        while True:
            total, batch = _inputs(store, offset)
            yield from batch
            offset += len(batch)
            if not batch or offset >= total:
                return

    return episode_payload(shows(), provider, today)
