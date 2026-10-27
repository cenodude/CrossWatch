# /providers/sync/silo/_collection.py
# Silo collection inventory sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from typing import Any, Iterable, Mapping

from cw_platform.collection_media import stream_media_info

from ._common import (
    DETAIL_TTL_SEC,
    cfg_section,
    item_detail,
    key_of,
    library,
    minimal_for,
    read_state,
    server_scope,
    unresolved_entry,
    write_state,
    _info,
    _warn,
)

FEATURE = "collection"
STATE_NAME = "media"


def media_info_enabled(adapter: Any) -> bool:
    block = cfg_section(adapter).get("collection")
    return bool(block.get("media_info")) if isinstance(block, Mapping) else False


def _streams(version: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for v in version.get("video_tracks") or []:
        if isinstance(v, Mapping):
            out.append({"Type": "Video", "Width": v.get("width"), "Height": v.get("height"), "IsInterlaced": bool(v.get("interlaced")),
                        "VideoRangeType": "HDR10Plus" if v.get("hdr10_plus") else v.get("video_range_type"), "ColorTransfer": v.get("color_transfer")})
    for a in version.get("audio_tracks") or []:
        if isinstance(a, Mapping):
            out.append({"Type": "Audio", "Codec": a.get("codec"), "Profile": a.get("profile"), "Title": a.get("title"), "Language": a.get("language"),
                        "Channels": a.get("channels"), "ChannelLayout": a.get("layout"), "BitRate": a.get("bitrate"), "IsDefault": a.get("default") is True})
    return out


def media_info_of(detail: Mapping[str, Any]) -> dict[str, Any]:
    versions = [v for v in detail.get("versions") or [] if isinstance(v, Mapping)]
    if not versions:
        return {}
    best = max(versions, key=lambda v: (max((int(t.get("width") or 0) for t in v.get("video_tracks") or [] if isinstance(t, Mapping)), default=0),
                                         int(v.get("bitrate") or 0)))
    return stream_media_info({"MediaStreams": _streams(best)})


def _media_infos(adapter: Any, rows: list[tuple[str, str | None]]) -> dict[str, dict[str, Any]]:
    scope = server_scope(adapter)
    cache = read_state(STATE_NAME, scope)
    now = int(time.time())
    fetched = failed = 0
    out: dict[str, dict[str, Any]] = {}
    for cid, stamp in rows:
        rec = cache.get(cid)
        if isinstance(rec, Mapping) and rec.get("stamp") == stamp and now - int(rec.get("at") or 0) < DETAIL_TTL_SEC:
            out[cid] = dict(rec.get("media") or {})
            continue
        detail = item_detail(adapter, cid, feature=FEATURE)
        if not detail:
            failed += 1
            continue
        media = media_info_of(detail)
        cache[cid] = {"stamp": stamp, "media": media, "at": now}
        out[cid] = media
        fetched += 1
    if fetched:
        write_state(STATE_NAME, scope, cache)
    if failed:
        _warn(FEATURE, "media_info_lookup_failed", count=failed)
    return out


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
    media = _media_infos(adapter, [(cid, item.get("collected_at")) for cid, item in playable]) if include_media else {}
    out: dict[str, dict[str, Any]] = {}
    for cid, key, item in matched:
        if include_media and item.get("type") in ("movie", "episode"):
            item["format"] = "digital"
            if media.get(cid):
                item["media_info"] = media[cid]
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
