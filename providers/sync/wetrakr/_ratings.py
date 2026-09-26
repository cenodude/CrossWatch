# providers/sync/wetrakr/_ratings.py
# CrossWatch - WeTrakr movie, show, season and episode ratings sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from providers.sync._log import log
from ._common import WeTrakrSyncError, int_value, item_key, media_item, tracking_rows, write_items


def build_index(adapter: Any, *, force: bool = False) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for kind, rows in tracking_rows(adapter, "ratings", force=force).items():
        for row in rows:
            item = media_item(row, kind)
            interactions = row.get("interactions")
            user = interactions.get("user") if isinstance(interactions, Mapping) else None
            rating = user.get("rating") if isinstance(user, Mapping) else None
            value = int_value(rating.get("rating")) if isinstance(rating, Mapping) else -1
            if not 1 <= value <= 10:
                raise WeTrakrSyncError("invalid_rating_entry")
            item["rating"] = value
            if isinstance(rating, Mapping) and rating.get("rated_at"):
                item["rated_at"] = rating["rated_at"]
            key = item_key(adapter, "ratings", item)
            if key in out:
                raise WeTrakrSyncError("duplicate_media_identity")
            out[key] = item
    log("WETRAKR", "ratings", "info", "index_done", count=len(out))
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "ratings", items, remove=False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "ratings", items, remove=True, dry_run=dry_run)
