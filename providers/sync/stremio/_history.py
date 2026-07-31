# providers/sync/stremio/_history.py
# CrossWatch Stremio history functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import base64
import copy
import math
import zlib
from collections.abc import Iterable, Mapping
from typing import Any

import requests

from cw_platform.id_map import minimal as id_minimal
from providers.auth._auth_STREMIO import StremioAuthError
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import (
    canonical_item_key,
    datastore_get,
    datastore_put,
    default_record,
    imdb_ids_from_item,
    imdb_id,
    iso_from_epoch_ms,
    item_from_episode,
    item_from_movie_record,
    library_records,
    now_iso,
    positive_int,
    record_id,
    state_of,
    stremio_id_for_item,
    poster_url_from_item,
    tmdb_metadata_provider,
    video_id_for_episode,
)

CINEMETA_BASE = "https://v3-cinemeta.strem.io"


def _bits_from_bytes(raw: bytes, length: int) -> list[bool]:
    return [bool(raw[i // 8] & (1 << (i % 8))) if i // 8 < len(raw) else False for i in range(max(0, length))]


def _bytes_from_bits(bits: list[bool]) -> bytes:
    raw = bytearray(max(0, math.ceil(len(bits) / 8)))
    for i, value in enumerate(bits):
        if value:
            raw[i // 8] |= 1 << (i % 8)
    return bytes(raw)


def _parse_serialized(value: str) -> tuple[str, int, bytes]:
    parts = str(value or "").split(":")
    if len(parts) < 3:
        raise ValueError("malformed_watched_bitfield")
    payload = parts[-1]
    anchor_length = int(parts[-2])
    anchor_id = ":".join(parts[:-2])
    if not anchor_id or anchor_length < 0:
        raise ValueError("malformed_watched_bitfield")
    return anchor_id, anchor_length, zlib.decompress(base64.b64decode(payload))


def watched_bits(value: Any, video_ids: list[str]) -> list[bool]:
    raw_value = str(value or "").strip()
    if not raw_value:
        return [False] * len(video_ids)
    anchor_id, anchor_length, packed = _parse_serialized(raw_value)
    try:
        anchor_index = video_ids.index(anchor_id)
    except ValueError:
        return [False] * len(video_ids)
    if anchor_index == anchor_length - 1:
        bits = _bits_from_bytes(packed, len(video_ids))
        return bits[: len(video_ids)] + [False] * max(0, len(video_ids) - len(bits))
    old_bits = _bits_from_bytes(packed, anchor_length)
    offset = (anchor_length - 1) - anchor_index
    return [old_bits[i + offset] if 0 <= i + offset < len(old_bits) else False for i in range(len(video_ids))]


def serialize_watched_bits(bits: list[bool], video_ids: list[str]) -> str:
    last = -1
    for idx, value in enumerate(bits):
        if value:
            last = idx
    if last < 0:
        return ""
    packed = zlib.compress(_bytes_from_bits(bits))
    return f"{video_ids[last]}:{last + 1}:{base64.b64encode(packed).decode('ascii')}"


def decode_watched_episodes(value: Any, video_ids: list[str]) -> set[str]:
    return {video_ids[i] for i, value in enumerate(watched_bits(value, video_ids)) if value}


def set_episode_watched_value(value: Any, video_ids: list[str], video_id: str, watched: bool) -> str:
    bits = watched_bits(value, video_ids)
    index = video_ids.index(video_id)
    bits[index] = bool(watched)
    return serialize_watched_bits(bits, video_ids)


def cinemeta_videos(adapter: Any, imdb: str) -> list[Mapping[str, Any]]:
    cache = getattr(adapter, "_stremio_cinemeta_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(adapter, "_stremio_cinemeta_cache", cache)
    if imdb in cache:
        return cache[imdb]
    resp = requests.get(f"{CINEMETA_BASE}/meta/series/{imdb}.json", timeout=15)
    resp.raise_for_status()
    data = resp.json()
    meta = data.get("meta") if isinstance(data, Mapping) else None
    videos = meta.get("videos") if isinstance(meta, Mapping) else None
    if not isinstance(videos, list):
        raise ValueError("cinemeta_invalid")
    rows = [row for row in videos if isinstance(row, Mapping) and str(row.get("id") or "").strip()]
    cache[imdb] = rows
    return rows


def parse_movie_history_record(record: Mapping[str, Any]) -> dict[str, Any] | None:
    item = item_from_movie_record(record)
    if not item:
        return None
    state = state_of(record)
    watched = (positive_int(state.get("timesWatched")) or 0) > 0 or (positive_int(state.get("flaggedWatched")) or 0) > 0
    item["watched"] = watched
    watched_at = iso_from_epoch_ms(state.get("lastWatched")) or iso_from_epoch_ms(record.get("_mtime"))
    if watched_at:
        item["watched_at"] = watched_at
    return item


def _image_url(detail: Mapping[str, Any], kind: str) -> str:
    images = detail.get("images")
    image_map = images if isinstance(images, Mapping) else {}
    rows = image_map.get(kind)
    if not isinstance(rows, list):
        return ""
    for row in rows:
        if isinstance(row, Mapping):
            url = str(row.get("url") or "").strip()
            if url:
                return url
    return ""


def _metadata_enriched(adapter: Any, item: Mapping[str, Any], typ: str) -> dict[str, Any]:
    out = dict(item)
    stremio_id = stremio_id_for_item(out)
    if stremio_id:
        if not str(out.get("poster") or out.get("poster_url") or "").strip():
            out["poster"] = poster_url_from_item(out, stremio_id)
        return out
    provider = tmdb_metadata_provider(adapter)
    if provider is None:
        return out
    show_ids_raw = out.get("show_ids")
    show_ids: Mapping[str, Any] = show_ids_raw if isinstance(show_ids_raw, Mapping) else {}
    source: Mapping[str, Any] = show_ids if typ in {"episode", "episodes"} and show_ids else imdb_ids_from_item(out)
    lookup = {k: str(source.get(k) or "").strip() for k in ("tmdb", "imdb", "tvdb") if str(source.get(k) or "").strip()}
    title = str(out.get("series_title") or out.get("show_title") or out.get("title") or "").strip()
    if title:
        lookup["title"] = title
    if out.get("year"):
        lookup["year"] = str(out.get("year"))
    if not lookup:
        return out
    try:
        detail = provider.fetch(entity="tv" if typ in {"episode", "episodes"} else "movie", ids=lookup, need={"poster": True, "backdrop": False, "ids": True})
    except Exception:
        return out
    if not isinstance(detail, Mapping):
        return out
    ids_raw = detail.get("ids")
    ids: Mapping[str, Any] = ids_raw if isinstance(ids_raw, Mapping) else {}
    imdb = imdb_id(ids.get("imdb"))
    if imdb:
        if typ in {"episode", "episodes"}:
            merged: dict[str, Any] = dict(show_ids)
            merged["imdb"] = imdb
            out["show_ids"] = merged
        else:
            out_ids_raw = out.get("ids")
            out_ids: Mapping[str, Any] = out_ids_raw if isinstance(out_ids_raw, Mapping) else {}
            merged = dict(out_ids)
            merged["imdb"] = imdb
            out["ids"] = merged
    if not str(out.get("poster") or out.get("poster_url") or "").strip():
        poster = poster_url_from_item(out, imdb) or _image_url(detail, "poster")
        if poster:
            out["poster"] = poster
    return out


def build_index(adapter: Any, **kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for record in library_records(adapter, incremental=bool(kwargs.get("incremental"))):
        typ = str(record.get("type") or "").strip().lower()
        if typ == "movie":
            item = parse_movie_history_record(record)
            if not item or not item.get("watched"):
                continue
            out[canonical_item_key(item)] = item
        elif typ in {"series", "show"}:
            show_id = imdb_id(record.get("_id"))
            watched_value = state_of(record).get("watched")
            if not show_id or not str(watched_value or "").strip():
                continue
            try:
                videos = cinemeta_videos(adapter, show_id)
                video_ids = [str(v.get("id") or "").strip() for v in videos]
                watched_ids = decode_watched_episodes(watched_value, video_ids)
            except Exception:
                continue
            by_id = {str(v.get("id") or "").strip(): v for v in videos}
            for video_id in watched_ids:
                video = by_id.get(video_id) or {}
                item = item_from_episode(show_id, video.get("season"), video.get("episode"), record, video)
                if not item:
                    continue
                item["watched"] = True
                fallback_ts = iso_from_epoch_ms(record.get("_mtime"))
                if fallback_ts:
                    item["_stremio_changed_at"] = fallback_ts
                out[canonical_item_key(item)] = item
    return out


def _history_ts(item: Mapping[str, Any]) -> str:
    return iso_from_epoch_ms(item.get("watched_at") or item.get("last_watched_at") or item.get("lastWatchedAt")) or now_iso()


def _unresolved(item: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {"status": "unresolved", "reason": reason, "item": id_minimal(item)}


def _api_failure(item: Mapping[str, Any], key: str, exc: StremioAuthError, fallback: str) -> dict[str, Any]:
    entry = {"status": "failed", "reason": str(getattr(exc, "reason", "") or fallback), "item": id_minimal(item), "canonical_key": key}
    detail = str(getattr(exc, "detail", "") or "").replace("\n", " ").replace("\r", " ").strip()
    if detail:
        for token in ("authKey", "auth_key", "password"):
            detail = detail.replace(token, f"{token[0]}***")
        entry["detail"] = detail[:180]
    return entry


def _apply_poster(record: dict[str, Any], item: Mapping[str, Any]) -> None:
    poster = poster_url_from_item(item, record.get("_id"))
    if poster and not str(record.get("poster") or "").strip():
        record["poster"] = poster


def _apply_movie(record: dict[str, Any], item: Mapping[str, Any], watched: bool) -> None:
    ts = now_iso()
    state = record.setdefault("state", {})
    _apply_poster(record, item)
    record["_mtime"] = ts
    if watched:
        state["lastWatched"] = _history_ts(item)
        state["timesWatched"] = max(1, positive_int(state.get("timesWatched")) or 0)
        state["flaggedWatched"] = 1
        state["timeOffset"] = 0
    else:
        state["lastWatched"] = ""
        state["timesWatched"] = 0
        state["flaggedWatched"] = 0
        state["timeOffset"] = 0


def _apply_episode(adapter: Any, record: dict[str, Any], item: Mapping[str, Any], watched: bool) -> str | None:
    show_id = imdb_id(record.get("_id"))
    if not show_id:
        return "stremio_id_missing"
    videos = cinemeta_videos(adapter, show_id)
    video_ids = [str(v.get("id") or "").strip() for v in videos]
    video_id = video_id_for_episode(item, show_id)
    if not video_id or video_id not in video_ids:
        return "stremio_episode_unresolved"
    state = record.setdefault("state", {})
    _apply_poster(record, item)
    try:
        state["watched"] = set_episode_watched_value(state.get("watched"), video_ids, video_id, watched)
    except Exception:
        return "stremio_watched_bitfield_malformed"
    record["_mtime"] = now_iso()
    return None


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, True, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, False, dry_run=dry_run)


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], watched: bool, *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[tuple[str, dict[str, Any], str]]] = {}
    attempted = 0
    for item in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(item)
        typ = str(item.get("type") or id_minimal(item).get("type") or "").strip().lower()
        item = _metadata_enriched(adapter, item, typ)
        stremio_id = stremio_id_for_item(item)
        if not stremio_id or typ not in {"movie", "episode", "episodes"}:
            entry = _unresolved(item, "stremio_id_missing")
            unresolved.append(entry)
            results.append(entry)
            continue
        attempted += 1
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(item), "canonical_key": key})
            continue
        grouped.setdefault((stremio_id, "movie" if typ == "movie" else "series"), []).append((key, item, typ))
    if grouped and not dry_run:
        ids = list(dict.fromkeys(stremio_id for stremio_id, _item_type in grouped))
        try:
            current = {record_id(row): copy.deepcopy(dict(row)) for row in datastore_get(adapter, ids=ids, all_records=False)}
        except StremioAuthError as exc:
            for ops in grouped.values():
                for key, item, _typ in ops:
                    entry = _api_failure(item, key, exc, "stremio_history_write_failed")
                    unresolved.append(entry)
                    results.append(entry)
            return build_op_result(ok=False, count=0, confirmed_keys=[], unresolved_keys=unresolved_keys(unresolved, canonical_item_key), unresolved=unresolved, results=results, attempted=attempted)
        prepared: list[tuple[dict[str, Any], list[tuple[str, dict[str, Any]]]]] = []
        for (stremio_id, item_type), ops in grouped.items():
            record = current.get(stremio_id) or default_record(stremio_id, item_type, ops[0][1])
            applied: list[tuple[str, dict[str, Any]]] = []
            for key, item, typ in ops:
                try:
                    reason = None
                    if typ == "movie":
                        _apply_movie(record, item, watched)
                    else:
                        reason = _apply_episode(adapter, record, item, watched)
                    if reason:
                        raise ValueError(reason)
                except ValueError as exc:
                    entry = _unresolved(item, str(exc) or "stremio_history_write_failed")
                    unresolved.append(entry)
                    results.append(entry)
                    continue
                except Exception:
                    entry = {"status": "failed", "reason": "stremio_history_write_failed", "item": id_minimal(item), "canonical_key": key}
                    unresolved.append(entry)
                    results.append(entry)
                    continue
                applied.append((key, item))
            if applied:
                prepared.append((record, applied))
        pending = prepared
        if prepared:
            try:
                datastore_put(adapter, [record for record, _ops in prepared])
            except Exception:
                pending = []
                for record, ops in prepared:
                    try:
                        datastore_put(adapter, [record])
                    except StremioAuthError as exc:
                        for key, item in ops:
                            entry = _api_failure(item, key, exc, "stremio_history_write_failed")
                            unresolved.append(entry)
                            results.append(entry)
                        continue
                    except Exception:
                        for key, item in ops:
                            entry = {"status": "failed", "reason": "stremio_history_write_failed", "item": id_minimal(item), "canonical_key": key}
                            unresolved.append(entry)
                            results.append(entry)
                        continue
                    pending.append((record, ops))
        for _record, ops in pending:
            for key, item in ops:
                confirmed.append(key)
                results.append({"status": "applied", "item": id_minimal(item), "canonical_key": key})
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved, canonical_item_key), unresolved=unresolved, results=results, attempted=attempted)
