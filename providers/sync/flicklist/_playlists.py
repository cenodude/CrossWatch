# CrossWatch - FlickList playlist sync
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.playlists import PLAYLIST_KIND_REGULAR, PLAYLIST_KIND_SMART, PlaylistItem, PlaylistResource, PlaylistSnapshot

from ._common import (
    URL_LIST,
    URL_LISTS,
    URL_LIST_ITEMS,
    chunk,
    classify_write,
    error_of,
    flicklist_request,
    header_int,
    key_of,
    ok_status,
    read_minimal,
    safe_json,
    write_ident,
    write_batch_size,
    _info,
    _warn,
)

FEATURE = "playlists"
MEDIA_TYPES = ("movie", "show", "episode")


class FlickListPlaylistError(RuntimeError):
    pass


class FlickListPlaylistNotFound(FlickListPlaylistError):
    pass


def _instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def _rows(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, Mapping):
        for key in ("items", "data", "results", "lists"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def _header_page_count(resp: Any, current: int) -> int:
    page_count = header_int(resp, "X-FlickList-Page-Count")
    return max(current, page_count) if page_count is not None else current


def _resource_from_row(adapter: Any, row: Mapping[str, Any]) -> PlaylistResource | None:
    raw_id = str(row.get("id") or row.get("list_id") or "").strip()
    if not raw_id:
        return None
    is_smart = bool(row.get("is_smart") or row.get("smart") or str(row.get("type") or "").lower() == "smart")
    name = str(row.get("name") or row.get("title") or raw_id).strip()
    return PlaylistResource(
        provider="FLICKLIST",
        id=raw_id,
        name=name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_SMART if is_smart else PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=not is_smart,
        can_remove=not is_smart,
        can_reorder=False,
        media_types=MEDIA_TYPES,
        extra={
            "raw_id": raw_id,
            "description": str(row.get("description") or "").strip(),
            "item_count": row.get("item_count") or row.get("items_count") or row.get("count"),
            "type": str(row.get("type") or "").strip().lower(),
        },
    )


def list_resources(adapter: Any) -> list[PlaylistResource]:
    resp = flicklist_request(adapter, "GET", URL_LISTS)
    if not ok_status(resp):
        _warn(FEATURE, "list_resources_failed", status=int(resp.status_code), error=error_of(resp))
        return []
    out = [_resource_from_row(adapter, row) for row in _rows(safe_json(resp)) if isinstance(row, Mapping)]
    resources = [res for res in out if res is not None]
    _info(FEATURE, "list_resources_done", count=len(resources))
    return resources


def _find_resource(adapter: Any, playlist_id: Any) -> PlaylistResource | None:
    lid = str(playlist_id or "").strip()
    if not lid:
        return None
    for resource in list_resources(adapter):
        if resource.id == lid:
            return resource
    return None


def _item_from_row(row: Mapping[str, Any]) -> PlaylistItem | None:
    media = read_minimal(row, default_type=row.get("media_type") or row.get("type") or "movie")
    if not media:
        return None
    key = canonical_key(media)
    if not key:
        return None
    item_id = str(row.get("id") or row.get("item_id") or row.get("list_item_id") or "").strip() or None
    return PlaylistItem.from_media(media, playlist_item_id=item_id, provider_media_id=str(row.get("fldb") or media.get("_flicklist_fldb") or ""))


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise FlickListPlaylistNotFound("missing flicklist list id")
    resource = _find_resource(adapter, lid) or PlaylistResource(
        provider="FLICKLIST",
        id=lid,
        name=lid,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=False,
        media_types=MEDIA_TYPES,
        extra={"raw_id": lid},
    )
    items: list[PlaylistItem] = []
    page = 1
    while True:
        resp = flicklist_request(adapter, "GET", URL_LIST_ITEMS.format(id=lid), params={"page": page, "limit": 100})
        if not ok_status(resp):
            raise FlickListPlaylistError(error_of(resp) or f"http:{int(resp.status_code)}")
        items.extend(item for item in (_item_from_row(row) for row in _rows(safe_json(resp)) if isinstance(row, Mapping)) if item is not None)
        if page >= _header_page_count(resp, page):
            break
        page += 1
    _info(FEATURE, "snapshot_done", list_id=lid, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


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
        return PlaylistResource(provider="FLICKLIST", id="", name=nm, instance=_instance_id(adapter), can_add=True, can_remove=True, media_types=MEDIA_TYPES)
    resp = flicklist_request(adapter, "POST", URL_LISTS, json={"name": nm})
    if not ok_status(resp):
        raise FlickListPlaylistError(error_of(resp) or f"http:{int(resp.status_code)}")
    data = safe_json(resp)
    row = data.get("list") if isinstance(data, Mapping) and isinstance(data.get("list"), Mapping) else data
    resource = _resource_from_row(adapter, row if isinstance(row, Mapping) else {})
    if resource is None:
        raise FlickListPlaylistError("flicklist create list returned no id")
    if items:
        add(adapter, resource.id, items)
    _info(FEATURE, "create_done", list_id=resource.id, name=resource.name)
    return resource


def _accepted(items: Sequence[Mapping[str, Any]]) -> tuple[list[tuple[str, dict[str, Any]]], list[dict[str, Any]]]:
    accepted: list[tuple[str, dict[str, Any]]] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        media = id_minimal(raw)
        key = key_of(media)
        payload = write_ident(media)
        if not key or payload is None:
            unresolved.append({"item": media, "hint": "missing_supported_id"})
            continue
        if key in seen:
            continue
        seen.add(key)
        accepted.append((key, payload))
    return accepted, unresolved


def _write_items(adapter: Any, method: str, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise FlickListPlaylistNotFound("missing flicklist list id")
    accepted, unresolved = _accepted(items)
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    ok = True
    if not accepted:
        return {"ok": True, "count": 0, "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}
    for batch in chunk([{"key": k, "payload": p} for k, p in accepted], write_batch_size(adapter, FEATURE)):
        sent = [(str(row["key"]), row["payload"]) for row in batch]
        resp = flicklist_request(adapter, method, URL_LIST_ITEMS.format(id=lid), json={"items": [payload for _key, payload in sent]})
        if ok_status(resp):
            yes, misses, miss_rows, counters = classify_write(sent, safe_json(resp), method=method)
            if misses:
                ok = False
                _warn(FEATURE, "write_items_partial", method=method, list_id=lid, sent=len(sent), confirmed=len(yes), unresolved=len(misses), **counters)
            confirmed.extend(yes)
            unresolved_keys.extend(misses)
            unresolved.extend({"item": {"key": row.get("key")}, "hint": row.get("status")} for row in miss_rows)
        else:
            ok = False
            for key, _payload in sent:
                unresolved_keys.append(key)
                unresolved.append({"item": {"key": key}, "hint": f"http:{int(resp.status_code)}", "error": error_of(resp)})
            _warn(FEATURE, "write_items_failed", method=method, list_id=lid, status=int(resp.status_code), error=error_of(resp))
    _info(FEATURE, "write_items_done", method=method.lower(), list_id=lid, confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write_items(adapter, "POST", playlist_id, items)


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write_items(adapter, "DELETE", playlist_id, items)


def delete(adapter: Any, playlist_id: Any) -> dict[str, Any]:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise FlickListPlaylistNotFound("missing flicklist list id")
    resp = flicklist_request(adapter, "DELETE", URL_LIST.format(id=lid))
    return {"ok": ok_status(resp) or int(resp.status_code) == 404, "status": int(resp.status_code)}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}
