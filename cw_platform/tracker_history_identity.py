# cw_platform/tracker_history_identity.py
# CrossWatch - Local history type correction identities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cw_platform.history_events import history_epoch_from_item
from cw_platform.id_map import canonical_key, minimal

TYPE_ORIGINS = "_cw_history_type_origins"


def event_identities(item: Mapping[str, Any]) -> set[str]:
    timestamp = history_epoch_from_item(item)
    if timestamp is None:
        return set()
    data = minimal(item)
    typ = data.get("type")
    field = "show_ids" if typ in ("episode", "season") and data.get("show_ids") else "ids"
    result = set()
    for namespace, value in (data.get(field) or {}).items():
        if namespace not in ("tmdb", "tvdb", "imdb"):
            continue
        key = canonical_key({**data, "ids": {}, field: {namespace: value}})
        if key != "unknown:":
            result.add(f"{typ}:{key}@{timestamp}")
    return result


def type_origins(items: Mapping[str, Any]) -> set[str]:
    if not isinstance(items, Mapping):
        return set()
    return {origin for item in items.values() if isinstance(item, Mapping)
            if isinstance(item.get(TYPE_ORIGINS), list)
            for origin in item[TYPE_ORIGINS] if isinstance(origin, str)}
