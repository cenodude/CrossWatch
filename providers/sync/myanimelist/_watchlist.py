# providers/sync/myanimelist/_watchlist.py
# CrossWatch - MyAnimeList planned anime watchlist
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
from cw_platform.id_map import canonical_key
from ._common import apply_items, media_item, number


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    out = {}
    for entry, media in adapter.client.entries():
        if entry.get("status") == "plan_to_watch":
            item = media_item(adapter, media)
            out[canonical_key(item)] = item
    return out


def _add(adapter: Any, item: Any, ident: str, episode: int) -> None:
    if item.get("type") not in ("movie", "show"):
        raise ValueError("unsupported_media_type")
    entry = adapter.client.lookup(ident)
    if entry and entry.get("status") != "plan_to_watch":
        raise ValueError("existing_watch_status_preserved")
    if not entry:
        adapter.client.save(ident, None, {"status": "plan_to_watch"})


def _remove(adapter: Any, item: Any, ident: str, episode: int) -> None:
    if item.get("type") not in ("movie", "show"):
        raise ValueError("unsupported_media_type")
    entry = adapter.client.lookup(ident)
    if not entry or entry.get("status") != "plan_to_watch":
        return
    attr = entry
    if number(attr.get("num_episodes_watched")) or number(attr.get("score")) or any(
        attr.get(key) for key in ("comments", "num_times_rewatched", "is_rewatching", "priority", "rewatch_value", "tags", "start_date", "finish_date")
    ):
        adapter.client.save(ident, entry, {"status": "on_hold"})
    else:
        adapter.client.delete(ident)


def add(adapter: Any, items: Any) -> dict[str, Any]:
    return apply_items(adapter, "watchlist", items, _add)


def remove(adapter: Any, items: Any) -> dict[str, Any]:
    return apply_items(adapter, "watchlist", items, _remove)
