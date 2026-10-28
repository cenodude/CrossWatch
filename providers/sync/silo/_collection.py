# /providers/sync/silo/_collection.py
# Silo collection inventory sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from ._common import (
    cfg_section,
    file_details,
    key_of,
    library,
    minimal_for,
    unresolved_entry,
    _info,
)

FEATURE = "collection"


def media_info_enabled(adapter: Any) -> bool:
    block = cfg_section(adapter).get("collection")
    return bool(block.get("media_info")) if isinstance(block, Mapping) else False


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter, feature=FEATURE)
    records: list[tuple[str, Mapping[str, Any]]] = [(cid, rec) for table in (lib.movies, lib.series, lib.episodes) for cid, rec in table.items()]
    include_media = media_info_enabled(adapter)
    matched: list[tuple[str, str, dict[str, Any]]] = []
    unmatched = 0
    for cid, rec in records:
        item = minimal_for(lib, cid)
        key = key_of(item) if item else ""
        if not item or not key:
            unmatched += 1
            continue
        if rec.get("added_at"):
            item["collected_at"] = rec["added_at"]
        matched.append((cid, key, item))
    playable = [(cid, item) for cid, _key, item in matched if item.get("type") in ("movie", "episode")]
    details = file_details(adapter, [(cid, item.get("collected_at")) for cid, item in playable], feature=FEATURE, strict=False) if include_media else {}
    out: dict[str, dict[str, Any]] = {}
    for cid, key, item in matched:
        if include_media and item.get("type") in ("movie", "episode"):
            item["format"] = "digital"
            media = (details.get(cid) or {}).get("media")
            if media:
                item["media_info"] = dict(media)
        out[key] = item
    _info(FEATURE, "index_done", count=len(out), movies=len(lib.movies), series=len(lib.series), episodes=len(lib.episodes),
          unmatched=unmatched, media_info=include_media)
    return out


def _unsupported(items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    unresolved = [unresolved_entry(key, "silo_collection_write_unsupported", item) for item in items or [] if (key := key_of(item))]
    return {"ok": True, "confirmed_keys": [], "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _unsupported(items)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _unsupported(items)
