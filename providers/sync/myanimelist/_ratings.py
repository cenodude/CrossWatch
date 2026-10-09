# providers/sync/myanimelist/_ratings.py
# CrossWatch - MyAnimeList anime ratings
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
from cw_platform.id_map import canonical_key
from ._common import apply_items, media_item, number


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    out = {}
    for entry, media in adapter.client.entries():
        attr = entry
        rating = number(attr.get("score"))
        if 1 <= rating <= 10:
            item = media_item(adapter, media)
            item["rating"] = rating
            out[canonical_key(item)] = item
    return out


def _write(adapter: Any, item: Any, ident: str, episode: int, *, remove: bool = False) -> None:
    if item.get("type") not in ("movie", "show"):
        raise ValueError("unsupported_media_type")
    rating = number(item.get("rating"))
    if not remove and not 1 <= rating <= 10:
        raise ValueError("rating_must_be_1_to_10")
    entry = adapter.client.lookup(ident)
    if remove and not entry:
        return
    attr: dict[str, Any] = {"score": 0 if remove else rating}
    if not entry:
        attr["status"] = "on_hold"
    adapter.client.save(ident, entry, attr)


def add(adapter: Any, items: Any) -> dict[str, Any]:
    return apply_items(adapter, "ratings", items, _write)


def remove(adapter: Any, items: Any) -> dict[str, Any]:
    return apply_items(adapter, "ratings", items, lambda a, i, k, e: _write(a, i, k, e, remove=True))
