# providers/sync/floppy/_watchlist.py
# CrossWatch - Floppy watchlist sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import minimal as id_minimal
from providers.auth._auth_FLOPPY import FloppyAuthError
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import api_delete, api_get, api_post, api_put, canonical_item_key, ensure_media, floppy_type_for_item, item_from_row, paged, tmdb_id_for_item, unresolved, watchlist_name


def _list_id(adapter: Any, *, create: bool = False) -> str | None:
    wanted = watchlist_name(adapter).lower()
    for row in paged(adapter, "lists"):
        rid = str(row.get("id") or "").strip()
        name = str(row.get("name") or "").strip().lower()
        if rid and name == wanted:
            return rid
    if not create:
        return None
    data = api_post(adapter, "lists", json={"name": watchlist_name(adapter)})
    return str(data.get("id") or "").strip() if isinstance(data, Mapping) else None


def _list_items(adapter: Any, list_id: str) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in paged(adapter, f"lists/{list_id}/items"):
        item = item_from_row(row)
        if not item or item.get("type") not in {"movie", "show"}:
            continue
        list_rows = row.get("lists")
        entries = list_rows if isinstance(list_rows, list) else []
        for entry in entries:
            if isinstance(entry, Mapping) and str(entry.get("list_id")) == str(list_id):
                item["_floppy_list_item_id"] = entry.get("list_item_id")
                break
        key = canonical_item_key(item)
        out[key] = item
    return out


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    lid = _list_id(adapter)
    return _list_items(adapter, lid) if lid else {}


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    lid = None if dry_run else _list_id(adapter, create=True)
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        typ = floppy_type_for_item(raw)
        tmdb_id = tmdb_id_for_item(raw)
        if typ not in {"movie", "tv"} or not tmdb_id:
            entry = unresolved(raw, "floppy_tmdb_id_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(raw), "canonical_key": key})
            continue
        if not lid:
            entry = unresolved(raw, "floppy_watchlist_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        try:
            api_put(adapter, f"media/{typ}/tmdb/{tmdb_id}/lists/{lid}")
        except FloppyAuthError as exc:
            if getattr(exc, "status_code", None) == 404:
                try:
                    ensure_media(adapter, typ, tmdb_id)
                    api_put(adapter, f"media/{typ}/tmdb/{tmdb_id}/lists/{lid}")
                except FloppyAuthError as inner:
                    if getattr(inner, "status_code", None) != 409:
                        entry = unresolved(raw, str(getattr(inner, "reason", "") or "floppy_watchlist_write_failed"))
                        unresolved_rows.append(entry)
                        results.append(entry)
                        continue
            elif getattr(exc, "status_code", None) != 409:
                entry = unresolved(raw, str(getattr(exc, "reason", "") or "floppy_watchlist_write_failed"))
                unresolved_rows.append(entry)
                results.append(entry)
                continue
        confirmed.append(key)
        results.append({"status": "applied", "item": id_minimal(raw), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    lid = None if dry_run else _list_id(adapter)
    current = {} if dry_run or not lid else _list_items(adapter, lid)
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        typ = floppy_type_for_item(raw)
        tmdb_id = tmdb_id_for_item(raw)
        if typ not in {"movie", "tv"} or not tmdb_id:
            entry = unresolved(raw, "floppy_tmdb_id_missing")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(raw), "canonical_key": key})
            continue
        if not lid:
            confirmed.append(key)
            results.append({"status": "skipped", "reason": "already_absent", "item": id_minimal(raw), "canonical_key": key})
            continue
        try:
            list_item_id = raw.get("_floppy_list_item_id") or current.get(key, {}).get("_floppy_list_item_id")
            if list_item_id:
                api_delete(adapter, f"lists/{lid}/items/{list_item_id}")
            else:
                api_delete(adapter, f"media/{typ}/tmdb/{tmdb_id}/lists/{lid}")
        except Exception:
            entry = unresolved(raw, "floppy_watchlist_remove_failed")
            unresolved_rows.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        results.append({"status": "applied", "item": id_minimal(raw), "canonical_key": key})
    return build_op_result(ok=not unresolved_rows, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved_rows, canonical_item_key), unresolved=unresolved_rows, results=results, attempted=len(confirmed) + len(unresolved_rows))
