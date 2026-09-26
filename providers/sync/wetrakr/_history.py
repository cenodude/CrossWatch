# providers/sync/wetrakr/_history.py
# CrossWatch - WeTrakr watched state and individual watch events
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.history_events import history_epoch_from_value, minimal_history_item
from providers.sync._log import log
from ._common import WeTrakrSyncError, item_key, media_item, tracking_rows, write_items


def build_index(adapter: Any, *, force: bool = False) -> dict[str, dict[str, Any]]:
    event_mode = bool(adapter.config.get("_cw_history_rewatches"))
    out: dict[str, dict[str, Any]] = {}
    snapshot = tracking_rows(adapter, "history", force=force)
    for kind in ("movie", "episode"):
        for row in snapshot[kind]:
            media = row.get(kind) if event_mode else row
            if not isinstance(media, Mapping):
                raise WeTrakrSyncError("invalid_history_item")
            item = media_item(media, kind)
            item["watched"] = True
            if row.get("watched_at_unknown"):
                item["_wetrakr_watched_at_unknown"] = True
            elif history_epoch_from_value(row.get("watched_at")) is not None:
                item["watched_at"] = row["watched_at"]
            elif event_mode:
                raise WeTrakrSyncError("missing_watch_timestamp")
            if event_mode:
                event_id = str(row.get("id") or "")
                if not event_id:
                    raise WeTrakrSyncError("missing_history_id")
                item["_wetrakr_history_id"] = event_id
            item = minimal_history_item(item, event_mode=event_mode)
            key = item_key(adapter, "history", item)
            if key in out:
                if not event_mode or out[key].get("_wetrakr_history_id") == item.get("_wetrakr_history_id"):
                    raise WeTrakrSyncError("duplicate_history_identity")
                key += "~" + item["_wetrakr_history_id"]
                if key in out:
                    raise WeTrakrSyncError("duplicate_history_identity")
                item["_cw_event_key"] = key
            out[key] = item
    log("WETRAKR", "history", "info", "index_done", count=len(out), rewatches=event_mode)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "history", items, remove=False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "history", items, remove=True, dry_run=dry_run)
