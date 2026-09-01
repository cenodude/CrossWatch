# /providers/sync/crosswatch/_playlists.py
# CrossWatch playlists module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
import uuid
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from cw_platform.id_map import canonical_key, merge_ids
from cw_platform.playlists import PLAYLIST_KIND_REGULAR, PlaylistItem, PlaylistResource, PlaylistSnapshot

from ._common import _atomic_write, _capture_mode, _root, make_logger, merge_tracker_identity, readonly, tracker_minimal

_dbg, _info, _warn, _error = make_logger("playlists")

_PROVIDER = "CROSSWATCH"
_MEDIA_TYPES = ("movie", "show", "season", "episode")
_VERSION = 1


class CrossWatchPlaylistError(RuntimeError):
    pass


class CrossWatchPlaylistNotFound(CrossWatchPlaylistError):
    pass


def _instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default").strip() or "default"


def _path(adapter: Any) -> Path:
    return _root(adapter) / "playlists.json"


def _empty_state() -> dict[str, Any]:
    return {"version": _VERSION, "ts": 0, "lists": {}}


def _read_state(adapter: Any) -> dict[str, Any]:
    try:
        raw = json.loads(_path(adapter).read_text("utf-8"))
    except Exception:
        return _empty_state()
    if not isinstance(raw, Mapping):
        return _empty_state()
    lists = raw.get("lists")
    if not isinstance(lists, Mapping):
        return _migrate_legacy(raw)
    out = _empty_state()
    out["version"] = int(raw.get("version") or _VERSION)
    out["ts"] = int(raw.get("ts") or 0)
    out["lists"] = {
        str(k): _clean_list_row(str(k), v)
        for k, v in lists.items()
        if isinstance(v, Mapping) and str(k).strip()
    }
    return out


def _migrate_legacy(raw: Mapping[str, Any]) -> dict[str, Any]:
    out = _empty_state()
    if isinstance(raw.get("items"), Mapping):
        out["lists"]["watchlist"] = {
            "id": "watchlist",
            "name": "Watchlist",
            "type": "playlist",
            "items": {str(k): tracker_minimal(v) for k, v in raw.get("items", {}).items() if isinstance(v, Mapping)},
            "order": [str(k) for k in raw.get("items", {}).keys()],
            "created_at": int(raw.get("ts") or 0),
            "updated_at": int(raw.get("ts") or 0),
        }
    return out


def _write_state(adapter: Any, state: Mapping[str, Any]) -> None:
    if _capture_mode() or readonly(adapter):
        return
    payload = {
        "version": _VERSION,
        "ts": int(time.time()),
        "lists": dict(state.get("lists") or {}),
    }
    _atomic_write(_path(adapter), payload)


def _clean_list_row(list_id: str, row: Mapping[str, Any]) -> dict[str, Any]:
    lid = str(row.get("id") or list_id).strip() or list_id
    name = str(row.get("name") or row.get("title") or lid).strip() or lid
    items_raw = row.get("items")
    order_raw = row.get("order")
    items: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    if isinstance(items_raw, Mapping):
        for raw_key, raw_value in items_raw.items():
            if not isinstance(raw_value, Mapping):
                continue
            item = tracker_minimal(raw_value)
            key = canonical_key(item) or str(raw_key)
            if not key:
                continue
            items[key] = item
    elif isinstance(items_raw, list):
        for raw_value in items_raw:
            if not isinstance(raw_value, Mapping):
                continue
            item = tracker_minimal(raw_value)
            key = canonical_key(item)
            if not key:
                continue
            items[key] = item
            order.append(key)
    if isinstance(order_raw, list):
        order = [str(k).strip() for k in order_raw if str(k).strip()]
    order = _normalized_order(order, items)
    return {
        "id": lid,
        "name": name,
        "type": "playlist",
        "items": items,
        "order": order,
        "created_at": int(row.get("created_at") or 0),
        "updated_at": int(row.get("updated_at") or 0),
    }


def _normalized_order(order: Sequence[str], items: Mapping[str, Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw_key in order or []:
        key = str(raw_key or "").strip()
        if key and key in items and key not in seen:
            out.append(key)
            seen.add(key)
    for key in items.keys():
        if key not in seen:
            out.append(key)
            seen.add(key)
    return out


def _items_map(row: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = row.get("items")
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, dict[str, Any]] = {}
    for key, value in raw.items():
        if isinstance(value, Mapping):
            out[str(key)] = dict(value)
    return out


def _order_list(row: Mapping[str, Any]) -> list[str]:
    raw = row.get("order")
    return [str(k) for k in raw] if isinstance(raw, list) else []


def _resource(adapter: Any, row: Mapping[str, Any]) -> PlaylistResource:
    items = _items_map(row)
    return PlaylistResource(
        provider=_PROVIDER,
        id=str(row.get("id") or ""),
        name=str(row.get("name") or row.get("id") or ""),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=True,
        media_types=_MEDIA_TYPES,
        extra={
            "endpoint_type": "playlist",
            "raw_id": str(row.get("id") or ""),
            "item_count": len(items),
            "can_rename": True,
            "can_delete": True,
            "local": True,
        },
    )


def list_resources(adapter: Any) -> list[PlaylistResource]:
    state = _read_state(adapter)
    rows = [row for row in (state.get("lists") or {}).values() if isinstance(row, Mapping)]
    out = [_resource(adapter, row) for row in sorted(rows, key=lambda r: str(r.get("name") or "").lower())]
    _info("list_resources_done", count=len(out))
    return out


def _get_row(adapter: Any, playlist_id: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise CrossWatchPlaylistNotFound("missing crosswatch playlist id")
    state = _read_state(adapter)
    row = (state.get("lists") or {}).get(lid)
    if not isinstance(row, Mapping):
        raise CrossWatchPlaylistNotFound("crosswatch playlist not found")
    return state, dict(row)


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    _state, row = _get_row(adapter, playlist_id)
    items_raw = _items_map(row)
    order = _normalized_order(_order_list(row), items_raw)
    items: list[PlaylistItem] = []
    for pos, key in enumerate(order):
        raw = items_raw.get(key)
        if not isinstance(raw, Mapping):
            continue
        item = PlaylistItem.from_media(raw, playlist_item_id=key, position=pos, provider_media_id=key)
        items.append(item)
    _info("snapshot_done", list_id=str(row.get("id") or ""), count=len(items))
    return PlaylistSnapshot(resource=_resource(adapter, row), items=items, checkpoint=str(row.get("updated_at") or ""))


def _new_id(existing: Mapping[str, Any]) -> str:
    while True:
        lid = f"cwpl_{uuid.uuid4().hex[:12]}"
        if lid not in existing:
            return lid


def create(
    adapter: Any,
    name: str,
    *,
    media_type: str | None = None,
    items: Sequence[Mapping[str, Any]] | None = None,
    dry_run: bool = False,
) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    if dry_run:
        return PlaylistResource(provider=_PROVIDER, id="", name=nm, instance=_instance_id(adapter), can_add=True, can_remove=True, can_reorder=True, media_types=_MEDIA_TYPES)
    state = _read_state(adapter)
    lists = dict(state.get("lists") or {})
    lid = _new_id(lists)
    now = int(time.time())
    row = {"id": lid, "name": nm, "type": "playlist", "items": {}, "order": [], "created_at": now, "updated_at": now}
    lists[lid] = row
    state["lists"] = lists
    _write_state(adapter, state)
    if items:
        add(adapter, lid, items)
        state, row = _get_row(adapter, lid)
    _info("create_done", list_id=lid, name=nm)
    return _resource(adapter, row)


def _accepted(items: Sequence[Mapping[str, Any]]) -> tuple[list[tuple[str, dict[str, Any]]], list[dict[str, Any]]]:
    accepted: list[tuple[str, dict[str, Any]]] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        try:
            item = tracker_minimal(raw)
        except Exception:
            unresolved.append({"item": dict(raw), "hint": "invalid_item"})
            continue
        key = canonical_key(item)
        if not key:
            unresolved.append({"item": item, "hint": "missing_supported_id"})
            continue
        if key in seen:
            continue
        seen.add(key)
        accepted.append((key, item))
    return accepted, unresolved


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    state, row = _get_row(adapter, playlist_id)
    accepted, unresolved = _accepted(list(items or []))
    cur = _items_map(row)
    order = _normalized_order(_order_list(row), cur)
    changed = 0
    confirmed: list[str] = []
    for key, item in accepted:
        existing = cur.get(key)
        if isinstance(existing, Mapping):
            old_ids = existing.get("ids") if isinstance(existing.get("ids"), Mapping) else {}
            new_ids = item.get("ids") if isinstance(item.get("ids"), Mapping) else {}
            merged = merge_ids(old_ids, new_ids)
            if merged:
                item["ids"] = merged
            item = merge_tracker_identity(existing, item)
        if existing != item:
            cur[key] = item
            changed += 1
        if key not in order:
            order.append(key)
        confirmed.append(key)
    if changed:
        row["items"] = cur
        row["order"] = _normalized_order(order, cur)
        row["updated_at"] = int(time.time())
        lists = dict(state.get("lists") or {})
        lists[str(row.get("id"))] = row
        state["lists"] = lists
        _write_state(adapter, state)
    _info("write_done", op="add", list_id=str(row.get("id") or ""), applied=changed, unresolved=len(unresolved))
    return {"ok": True, "count": changed, "unresolved": unresolved, "confirmed_keys": confirmed}


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    state, row = _get_row(adapter, playlist_id)
    accepted, unresolved = _accepted(list(items or []))
    cur = _items_map(row)
    want = {key for key, _item in accepted}
    confirmed = [key for key in want if key in cur]
    changed = 0
    for key in confirmed:
        cur.pop(key, None)
        changed += 1
    if changed:
        row["items"] = cur
        row["order"] = _normalized_order(_order_list(row), cur)
        row["updated_at"] = int(time.time())
        lists = dict(state.get("lists") or {})
        lists[str(row.get("id"))] = row
        state["lists"] = lists
        _write_state(adapter, state)
    _info("write_done", op="remove", list_id=str(row.get("id") or ""), applied=changed, unresolved=len(unresolved))
    return {"ok": True, "count": changed, "unresolved": unresolved, "confirmed_keys": confirmed}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    state, row = _get_row(adapter, playlist_id)
    cur = _items_map(row)
    before = _normalized_order(_order_list(row), cur)
    after = _normalized_order([str(k) for k in ordered_keys or []], cur)
    if after != before:
        row["order"] = after
        row["updated_at"] = int(time.time())
        lists = dict(state.get("lists") or {})
        lists[str(row.get("id"))] = row
        state["lists"] = lists
        _write_state(adapter, state)
    _info("reorder_done", list_id=str(row.get("id") or ""), changed=after != before)
    return {"ok": True, "count": len(after), "reordered": len(after) if after != before else 0}


def rename(adapter: Any, playlist_id: Any, name: str) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    state, row = _get_row(adapter, playlist_id)
    if row.get("name") != nm:
        row["name"] = nm
        row["updated_at"] = int(time.time())
        lists = dict(state.get("lists") or {})
        lists[str(row.get("id"))] = row
        state["lists"] = lists
        _write_state(adapter, state)
    _info("rename_done", list_id=str(row.get("id") or ""), name=nm)
    return _resource(adapter, row)


def delete(adapter: Any, playlist_id: Any) -> dict[str, Any]:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise CrossWatchPlaylistNotFound("missing crosswatch playlist id")
    state = _read_state(adapter)
    lists = dict(state.get("lists") or {})
    if lid not in lists:
        raise CrossWatchPlaylistNotFound("crosswatch playlist not found")
    lists.pop(lid, None)
    state["lists"] = lists
    _write_state(adapter, state)
    _info("delete_done", list_id=lid)
    return {"ok": True, "playlist_id": lid}
