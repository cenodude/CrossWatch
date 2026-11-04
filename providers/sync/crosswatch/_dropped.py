# /providers/sync/crosswatch/_dropped.py
# CrossWatch tracker Module dropped show status
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ._common import cw_log, readonly, tracker_store

DROPPED = "dropped"


def dropped_show_tokens(adapter: Any) -> set[str]:
    return tracker_store(adapter).note_show_tokens(DROPPED)


def tag_dropped_shows(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows: list[tuple[str, dict[str, Any]]] = []
    skipped = 0
    for item in items or []:
        raw = item.get("ids") if isinstance(item, Mapping) else None
        ids: Mapping[str, Any] = raw if isinstance(raw, Mapping) else {}
        tmdb = str(ids.get("tmdb") or "").strip()
        if not tmdb.isdecimal():
            skipped += 1
            continue
        key = f"show:tmdb:{tmdb}"
        if any(k == key for k, _ in rows):
            continue
        rows.append((key, {"title": str(item.get("title") or "").strip(), "tmdb": tmdb}))
    if not rows or readonly(adapter):
        return {"attempted": len(rows), "confirmed": 0, "skipped": skipped + len(rows), "not_found": 0, "errors": 0}
    try:
        _, changed, same = tracker_store(adapter).set_show_status(rows, DROPPED)
    except Exception as exc:
        cw_log("CROSSWATCH", "history", "warn", "dropped_tag_failed", count=len(rows), error=str(exc))
        return {"attempted": len(rows), "confirmed": 0, "skipped": skipped, "not_found": 0, "errors": len(rows)}
    cw_log("CROSSWATCH", "history", "info", "dropped_tagged", attempted=len(rows), changed=changed, unchanged=same, skipped=skipped)
    return {"attempted": len(rows), "confirmed": changed + same, "skipped": skipped, "not_found": 0, "errors": 0}
