# /providers/sync/tofa/_collection.py
# Tofa collection inventory sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from ._common import all_files, cfg_section, key_of, library, media_details, media_id_of, minimal_for, unresolved_entry, _info

FEATURE = "collection"


def media_info_enabled(adapter: Any) -> bool:
    block = cfg_section(adapter).get("collection")
    return bool(block.get("media_info")) if isinstance(block, Mapping) else False


def _best_file(files: Mapping[str, Any], fids: list[str]) -> Mapping[str, Any]:
    rows = [files[fid] for fid in fids if isinstance(files.get(fid), Mapping)]
    if not rows:
        return {}
    return max(rows, key=lambda r: (int(r.get("width") or 0), int(r.get("bitrate") or 0)))


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter, feature=FEATURE)
    include_media = media_info_enabled(adapter)
    details = media_details(adapter, lib, list(lib.movies) + list(lib.shows), feature=FEATURE)
    out: dict[str, dict[str, Any]] = {}
    unmatched = dated = with_media = 0
    targets = [("movie", t) for t in lib.movies] + [("show", t) for t in lib.shows] + [("episode", t) for t in lib.episodes]
    for kind, target in targets:
        item = minimal_for(lib, kind, target)
        key = key_of(item) if item else ""
        if not item or not key:
            unmatched += 1
            continue
        detail = details.get(str(target) if kind == "show" else media_id_of(lib, kind, target)) or {}
        raw_files = detail.get("file")
        files: Mapping[str, Any] = raw_files if isinstance(raw_files, Mapping) else {}
        fids = all_files(lib, kind, target)
        if kind == "episode":
            stamps = sorted(s for s in (str((files.get(fid) or {}).get("created_at") or "") for fid in fids) if s)
            collected = stamps[0] if stamps else ""
        else:
            collected = str(detail.get("added_at") or "")
        if collected:
            item["collected_at"] = collected
            dated += 1
        if include_media and kind in ("movie", "episode"):
            item["format"] = "digital"
            media = _best_file(files, fids).get("media")
            if media:
                item["media_info"] = dict(media)
                with_media += 1
        out[key] = item
    _info(FEATURE, "index_done", count=len(out), movies=len(lib.movies), shows=len(lib.shows), episodes=len(lib.episodes),
          unmatched=unmatched, dated=dated, media_info=include_media, with_media=with_media)
    return out


def _unsupported(items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    unresolved = [unresolved_entry(key, "tofa_collection_write_unsupported", item) for item in items or [] if (key := key_of(item))]
    return {"ok": True, "confirmed_keys": [], "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _unsupported(items)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _unsupported(items)
