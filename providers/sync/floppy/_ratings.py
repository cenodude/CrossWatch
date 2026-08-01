# providers/sync/floppy/_ratings.py
# CrossWatch - Floppy ratings sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import minimal as id_minimal
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import PLANNING, api_patch, canonical_item_key, failure_reason, floppy_type_for_item, item_from_row, paged, rating_number, track_media, tmdb_id_for_item, unresolved

_SHADOW_TTL = 180.0
_WRITE_SHADOW: dict[tuple[str, str], dict[str, Any]] = {}


def _scope(adapter: Any) -> str:
    pair = os.getenv("CW_PAIR_KEY") or os.getenv("CW_PAIR_SCOPE") or os.getenv("CW_SYNC_PAIR") or os.getenv("CW_PAIR")
    if not pair:
        return ""
    instance = str(getattr(adapter, "instance_id", "default") or "default").strip() or "default"
    return f"{instance}:{pair}"


def _merge_shadow(adapter: Any, out: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    scope = _scope(adapter)
    if not scope:
        return out
    now = time.time()
    for shadow_key, row in list(_WRITE_SHADOW.items()):
        row_scope, key = shadow_key
        if row_scope != scope:
            continue
        if now - float(row.get("_ts") or 0) > _SHADOW_TTL:
            _WRITE_SHADOW.pop(shadow_key, None)
            continue
        if key in out:
            _WRITE_SHADOW.pop(shadow_key, None)
            continue
        item = row.get("item")
        if isinstance(item, Mapping):
            out[key] = dict(item)
    return out


def _remember(adapter: Any, key: str, item: Mapping[str, Any] | None) -> None:
    scope = _scope(adapter)
    if not scope:
        return
    shadow_key = (scope, key)
    if item is None:
        _WRITE_SHADOW.pop(shadow_key, None)
        return
    _WRITE_SHADOW[shadow_key] = {"_ts": time.time(), "item": dict(item)}


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for media_type in ("movie", "tv"):
        for row in paged(adapter, f"media/{media_type}"):
            rating = rating_number(row.get("score"))
            if rating is None or rating <= 0:
                continue
            item = item_from_row(row, force_type=media_type)
            if not item:
                continue
            item["rating"] = rating
            item["_floppy_consumption_id"] = row.get("consumption_id")
            out[canonical_item_key(item)] = item
    return _merge_shadow(adapter, out)


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, clear=False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, clear=True, dry_run=dry_run)


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, clear: bool, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    skipped: list[str] = []
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        typ = floppy_type_for_item(raw)
        tmdb_id = tmdb_id_for_item(raw)
        rating = None if clear else rating_number(raw.get("rating"))
        if typ not in {"movie", "tv"}:
            skipped.append(key)
            results.append({"status": "skipped", "reason": "floppy_rating_type_unsupported", "item": id_minimal(raw), "canonical_key": key})
            continue
        if not tmdb_id:
            entry = unresolved(raw, "floppy_tmdb_id_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if rating is None and not clear:
            entry = unresolved(raw, "floppy_rating_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if rating is not None and rating <= 0 and not clear:
            skipped.append(key)
            results.append({"status": "skipped", "reason": "floppy_rating_zero_is_clear", "item": id_minimal(raw), "canonical_key": key})
            continue
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(raw), "canonical_key": key})
            continue
        try:
            if clear:
                api_patch(adapter, f"media/{typ}/tmdb/{tmdb_id}", json={"score": None})
            else:
                track_media(adapter, typ, tmdb_id, payload={"status": PLANNING, "score": rating}, patch_payload={"score": rating})
        except Exception as exc:
            entry = unresolved(raw, failure_reason(exc))
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        if clear:
            _remember(adapter, key, None)
        else:
            cached = id_minimal(raw)
            cached["rating"] = rating
            if raw.get("rated_at"):
                cached["rated_at"] = raw.get("rated_at")
            _remember(adapter, key, cached)
        results.append({"status": "applied", "item": id_minimal(raw), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(skipped) + len(unresolved_rows), skipped=len(skipped), skipped_keys=skipped)
