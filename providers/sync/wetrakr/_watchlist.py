# providers/sync/wetrakr/_watchlist.py
# CrossWatch - WeTrakr planning list sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from providers.sync._log import log
from ._common import WeTrakrSyncError, item_key, media_item, tracking_rows, write_items


def build_index(adapter: Any, *, force: bool = False) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    snapshot = tracking_rows(adapter, "watchlist", force=force)
    for kind in ("movie", "show"):
        for row in snapshot[kind]:
            item = media_item(row, kind)
            key = item_key(adapter, "watchlist", item)
            if key in out:
                raise WeTrakrSyncError("duplicate_media_identity")
            out[key] = item
    log("WETRAKR", "watchlist", "info", "index_done", count=len(out))
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "watchlist", items, remove=False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "watchlist", items, remove=True, dry_run=dry_run)
