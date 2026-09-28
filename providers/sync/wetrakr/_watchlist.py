# providers/sync/wetrakr/_watchlist.py
# CrossWatch - WeTrakr planning list sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.interactive_reads import retained_read

from collections.abc import Iterable, Mapping
from typing import Any

from providers.sync._log import log
from ._common import add_index_item, item_key, media_item, tracking_rows, write_items


@retained_read
def build_index(adapter: Any, *, force: bool = False) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    snapshot = tracking_rows(adapter, "watchlist", force=force)
    for kind in ("movie", "show"):
        for row in snapshot[kind]:
            item = media_item(row, kind)
            key = item_key(adapter, "watchlist", item)
            add_index_item(out, "watchlist", key, item)
    log("WETRAKR", "watchlist", "info", "index_done", count=len(out))
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "watchlist", items, remove=False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "watchlist", items, remove=True, dry_run=dry_run)
