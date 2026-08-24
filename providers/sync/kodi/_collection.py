# providers/sync/kodi/_collection.py
# CrossWatch Kodi collection source
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from providers.sync._mod_common import build_op_result

from ._common import item_key, library_index, log


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    idx = library_index(adapter, "collection")
    out: dict[str, dict[str, Any]] = {}
    for base in idx.items:
        item = dict(base)
        key = item_key(item)
        if key and key != "unknown:":
            out[key] = item
    log("collection", "info", "index_done", count=len(out))
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    rows = [dict(item) for item in items or []]
    unresolved = [{"item": item, "reason": "kodi_collection_write_unsupported"} for item in rows]
    log("collection", "info", "write_skipped", op="add", reason="unsupported", unresolved=len(unresolved), dry_run=bool(dry_run))
    return build_op_result(ok=True, count=0, unresolved=unresolved, reason="unsupported")


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    rows = [dict(item) for item in items or []]
    unresolved = [{"item": item, "reason": "kodi_collection_write_unsupported"} for item in rows]
    log("collection", "info", "write_skipped", op="remove", reason="unsupported", unresolved=len(unresolved), dry_run=bool(dry_run))
    return build_op_result(ok=True, count=0, unresolved=unresolved, reason="unsupported")
