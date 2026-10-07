# providers/sync/kitsu/_watchlist.py
# CrossWatch - Kitsu planned anime watchlist
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
from cw_platform.id_map import canonical_key
from ._common import apply_items, media_item, number


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    out = {}
    for entry, media in adapter.client.entries():
        if entry["attributes"].get("status") == "planned":
            item = media_item(adapter, media)
            out[canonical_key(item)] = item
    return out


def _add(adapter: Any, item: Any, ident: str, episode: int) -> None:
    if item.get("type") not in ("movie", "show"):
        raise ValueError("unsupported_media_type")
    entry = adapter.client.lookup(ident)
    if entry and entry["attributes"].get("status") != "planned":
        raise ValueError("existing_watch_status_preserved")
    if not entry:
        adapter.client.save(ident, None, {"status": "planned"})


def _remove(adapter: Any, item: Any, ident: str, episode: int) -> None:
    if item.get("type") not in ("movie", "show"):
        raise ValueError("unsupported_media_type")
    entry = adapter.client.lookup(ident)
    if not entry or entry["attributes"].get("status") != "planned":
        return
    attr = entry["attributes"]
    if number(attr.get("progress")) or attr.get("ratingTwenty") is not None or any(
        attr.get(key) for key in ("notes", "reconsumeCount", "reconsuming", "private", "startedAt", "finishedAt")
    ):
        adapter.client.save(ident, entry, {"status": "on_hold"})
    else:
        adapter.client.delete(entry)


def add(adapter: Any, items: Any) -> dict[str, Any]:
    return apply_items(adapter, "watchlist", items, _add)


def remove(adapter: Any, items: Any) -> dict[str, Any]:
    return apply_items(adapter, "watchlist", items, _remove)
