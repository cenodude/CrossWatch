from __future__ import annotations

from typing import Any, Mapping, Sequence

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.playlists import PLAYLIST_KIND_REGULAR, PlaylistItem, PlaylistResource, PlaylistSnapshot

from ._common import (
    _fetch_all_collection_items,
    _fetch_all_playlist_items,
    chunked,
    collection_add_items,
    collection_remove_items,
    create_collection,
    create_playlist,
    key_of as emby_key_of,
    make_logger,
    normalize as emby_normalize,
    playlist_add_items,
    playlist_move_item,
    playlist_remove_entries,
    resolve_item_id,
    sleep_ms,
)

_PROVIDER = "EMBY"
_PLAYLIST_MEDIA_TYPES = ("movie", "episode")
_COLLECTION_MEDIA_TYPES = ("movie", "show")
_RESOURCE_PREFIXES = {"playlist": "playlist:", "collection": "collection:"}
_dbg, _info, _warn, _error = make_logger("playlists")


class EmbyPlaylistError(RuntimeError):
    pass


class EmbyPlaylistNotFound(EmbyPlaylistError):
    pass


def _instance_id(adapter: Any) -> str:
    inst = getattr(adapter, "instance_id", None)
    return str(inst).strip() if inst else "default"


def _prefixed(endpoint_type: str, raw_id: Any) -> str:
    return f"{_RESOURCE_PREFIXES[endpoint_type]}{str(raw_id or '').strip()}"


def _raw_id(resource_id: Any) -> tuple[str | None, str]:
    s = str(resource_id or "").strip()
    for endpoint_type, prefix in _RESOURCE_PREFIXES.items():
        if s.lower().startswith(prefix):
            return endpoint_type, s[len(prefix):].strip()
    return None, s


def _endpoint_type(row: Mapping[str, Any]) -> str | None:
    typ = str(row.get("Type") or "").strip().lower()
    if typ == "playlist":
        return "playlist"
    if typ == "boxset":
        return "collection"
    return None


def _media_types(endpoint_type: str) -> tuple[str, ...]:
    return _COLLECTION_MEDIA_TYPES if endpoint_type == "collection" else _PLAYLIST_MEDIA_TYPES


def _resource_from_row(adapter: Any, row: Mapping[str, Any], endpoint_type: str | None = None) -> PlaylistResource | None:
    raw = str(row.get("Id") or "").strip()
    if not raw:
        return None
    et = endpoint_type or _endpoint_type(row)
    if et not in _RESOURCE_PREFIXES:
        return None
    name = str(row.get("Name") or "").strip() or raw
    return PlaylistResource(
        provider=_PROVIDER,
        id=_prefixed(et, raw),
        name=name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=et == "playlist",
        media_types=_media_types(et),
        extra={
            "endpoint_type": et,
            "raw_id": raw,
            "item_count": row.get("ChildCount") if row.get("ChildCount") is not None else row.get("RecursiveItemCount"),
        },
    )


def _fetch_resources(adapter: Any, endpoint_type: str) -> list[PlaylistResource]:
    http = adapter.client
    uid = adapter.cfg.user_id
    include = "Playlist" if endpoint_type == "playlist" else "BoxSet"
    out: list[PlaylistResource] = []
    start = 0
    limit = 500
    total: int | None = None
    while True:
        r = http.get(
            f"/Users/{uid}/Items",
            params={
                "UserId": uid,
                "IncludeItemTypes": include,
                "Recursive": True,
                "Fields": "ChildCount,RecursiveItemCount,DateCreated,DateLastMediaAdded",
                "StartIndex": start,
                "Limit": limit,
                "EnableTotalRecordCount": True,
            },
        )
        if getattr(r, "status_code", 0) != 200:
            _warn("list_failed", endpoint_type=endpoint_type, status=getattr(r, "status_code", None))
            return out
        body = r.json() or {}
        rows = body.get("Items") or []
        if total is None:
            try:
                total = int(body.get("TotalRecordCount") or 0)
            except Exception:
                total = 0
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            res = _resource_from_row(adapter, row, endpoint_type)
            if res:
                out.append(res)
        start += len(rows)
        if not rows or len(rows) < limit or (total is not None and total > 0 and start >= total):
            break
    return out


def list_resources(adapter: Any) -> list[PlaylistResource]:
    out = _fetch_resources(adapter, "playlist")
    out.extend(_fetch_resources(adapter, "collection"))
    out.sort(key=lambda r: (str(r.extra.get("endpoint_type") or ""), r.name.lower(), r.id))
    _info("list_resources_done", count=len(out))
    return out


def _find_resource(adapter: Any, resource_id: Any) -> PlaylistResource | None:
    et, rid = _raw_id(resource_id)
    for res in list_resources(adapter):
        raw = str(res.extra.get("raw_id") or "").strip()
        if res.id == str(resource_id or "").strip() or raw == rid:
            if not et or str(res.extra.get("endpoint_type") or "") == et:
                return res
    return None


def _resolved_resource(adapter: Any, resource_id: Any) -> tuple[str, str, PlaylistResource]:
    et, rid = _raw_id(resource_id)
    res = _find_resource(adapter, resource_id)
    if res:
        raw = str(res.extra.get("raw_id") or "").strip()
        kind = str(res.extra.get("endpoint_type") or et or "playlist")
        return kind, raw or rid, res
    if not rid:
        raise EmbyPlaylistNotFound("missing emby playlist id")
    if not et:
        et = "playlist"
    return et, rid, PlaylistResource(
        provider=_PROVIDER,
        id=_prefixed(et, rid),
        name=rid,
        instance=_instance_id(adapter),
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=et == "playlist",
        media_types=_media_types(et),
        extra={"endpoint_type": et, "raw_id": rid},
    )


def _accepts(endpoint_type: str, row: Mapping[str, Any]) -> bool:
    typ = str(row.get("Type") or row.get("type") or "").strip().lower()
    if endpoint_type == "collection":
        return typ in ("movie", "show", "series")
    return typ in ("movie", "episode")


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    endpoint_type, raw_id, resource = _resolved_resource(adapter, playlist_id)
    page_size = max(200, int(getattr(adapter.cfg, "watchlist_query_limit", 1000) or 1000))
    if endpoint_type == "collection":
        rows, _total = _fetch_all_collection_items(adapter.client, adapter.cfg.user_id, raw_id, page_size=page_size)
    else:
        rows, _total = _fetch_all_playlist_items(adapter.client, raw_id, page_size=page_size)
    items: list[PlaylistItem] = []
    for pos, row in enumerate(rows):
        if not isinstance(row, Mapping) or not _accepts(endpoint_type, row):
            continue
        media = emby_normalize(row)
        items.append(
            PlaylistItem.from_media(
                media,
                playlist_item_id=row.get("PlaylistItemId") or row.get("playlistItemId") or row.get("Id"),
                position=pos if endpoint_type == "playlist" else None,
                provider_media_id=row.get("Id"),
            )
        )
    _info("snapshot_done", list_id=resource.id, endpoint_type=endpoint_type, count=len(items))
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
    endpoint_type = "collection" if str(media_type or "").strip().lower() in ("collection", "boxset") else "playlist"
    if dry_run:
        return PlaylistResource(
            provider=_PROVIDER,
            id="",
            name=nm,
            instance=_instance_id(adapter),
            can_add=True,
            can_remove=True,
            can_reorder=endpoint_type == "playlist",
            media_types=_media_types(endpoint_type),
            extra={"endpoint_type": endpoint_type},
        )
    if endpoint_type == "collection":
        rid = create_collection(adapter.client, nm)
    else:
        rid = create_playlist(adapter.client, adapter.cfg.user_id, nm, is_public=False)
    if not rid:
        raise EmbyPlaylistError(f"emby create {endpoint_type} failed")
    res = _resource_from_row(adapter, {"Id": rid, "Name": nm, "Type": "BoxSet" if endpoint_type == "collection" else "Playlist"}, endpoint_type)
    if res is None:
        raise EmbyPlaylistError(f"emby create {endpoint_type} returned no id")
    _info("create_done", list_id=res.id, endpoint_type=endpoint_type, name=res.name)
    return res


def _accepted_items(adapter: Any, endpoint_type: str, items: Sequence[Mapping[str, Any]]) -> tuple[list[tuple[str, str]], list[dict[str, Any]]]:
    accepted: list[tuple[str, str]] = []
    unresolved: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for raw in items or []:
        media = id_minimal(raw)
        typ = str(media.get("type") or "").strip().lower()
        if typ == "series":
            media["type"] = "show"
            typ = "show"
        key = canonical_key(media)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        if typ not in _media_types(endpoint_type):
            unresolved.append({"item": media, "hint": "unsupported_type"})
            continue
        iid = resolve_item_id(adapter, media, feature="playlists")
        if not iid:
            unresolved.append({"item": media, "hint": "not_in_library"})
            continue
        accepted.append((key, str(iid)))
    return accepted, unresolved


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    endpoint_type, raw_id, resource = _resolved_resource(adapter, playlist_id)
    accepted, unresolved = _accepted_items(adapter, endpoint_type, list(items or []))
    if not accepted:
        return {"ok": True, "count": 0, "unresolved": unresolved, "confirmed_keys": []}
    qlim = max(1, int(getattr(adapter.cfg, "watchlist_query_limit", 25) or 25))
    delay = int(getattr(adapter.cfg, "watchlist_write_delay_ms", 0) or 0)
    added = 0
    confirmed: list[str] = []
    for chunk in chunked(accepted, qlim):
        ids = [iid for _key, iid in chunk]
        ok = collection_add_items(adapter.client, raw_id, ids) if endpoint_type == "collection" else playlist_add_items(adapter.client, raw_id, adapter.cfg.user_id, ids)
        if ok:
            added += len(ids)
            confirmed.extend(key for key, _iid in chunk)
        else:
            for key, _iid in chunk:
                unresolved.append({"item": {"key": key}, "hint": f"{endpoint_type}_add_failed"})
        sleep_ms(delay)
    _info("write_done", op="add", list_id=resource.id, endpoint_type=endpoint_type, applied=added, unresolved=len(unresolved))
    return {"ok": True, "count": added, "unresolved": unresolved, "confirmed_keys": confirmed}


def _current_members(adapter: Any, endpoint_type: str, raw_id: str) -> tuple[list[str], dict[str, list[str]], dict[str, set[str]]]:
    page_size = max(200, int(getattr(adapter.cfg, "watchlist_query_limit", 1000) or 1000))
    if endpoint_type == "collection":
        rows, _total = _fetch_all_collection_items(adapter.client, adapter.cfg.user_id, raw_id, page_size=page_size)
    else:
        rows, _total = _fetch_all_playlist_items(adapter.client, raw_id, page_size=page_size)
    ordered_keys: list[str] = []
    by_key: dict[str, list[str]] = {}
    keys_by_id: dict[str, set[str]] = {}
    for row in rows:
        if not isinstance(row, Mapping) or not _accepts(endpoint_type, row):
            continue
        key = emby_key_of(row)
        remote_id = row.get("Id")
        member_id = remote_id if endpoint_type == "collection" else (row.get("PlaylistItemId") or row.get("playlistItemId"))
        if not key or not member_id:
            continue
        sid = str(member_id)
        ordered_keys.append(key)
        by_key.setdefault(key, []).append(sid)
        keys_by_id.setdefault(sid, set()).add(key)
    return ordered_keys, by_key, keys_by_id


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    endpoint_type, raw_id, resource = _resolved_resource(adapter, playlist_id)
    _ordered, by_key, keys_by_id = _current_members(adapter, endpoint_type, raw_id)
    wanted: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for raw in items or []:
        key = canonical_key(id_minimal(raw))
        ids = by_key.get(key) or []
        if not ids:
            unresolved.append({"item": id_minimal(raw), "hint": f"not_in_{endpoint_type}"})
            continue
        wanted.extend(ids)
    seen: set[str] = set()
    wanted = [x for x in wanted if not (x in seen or seen.add(x))]
    if not wanted:
        return {"ok": True, "count": 0, "unresolved": unresolved, "confirmed_keys": []}
    qlim = max(1, int(getattr(adapter.cfg, "watchlist_query_limit", 25) or 25))
    delay = int(getattr(adapter.cfg, "watchlist_write_delay_ms", 0) or 0)
    removed = 0
    confirmed_set: set[str] = set()
    for chunk in chunked(wanted, qlim):
        ok = collection_remove_items(adapter.client, raw_id, chunk) if endpoint_type == "collection" else playlist_remove_entries(adapter.client, raw_id, chunk)
        if ok:
            removed += len(chunk)
            for sid in chunk:
                confirmed_set.update(keys_by_id.get(sid, set()))
        else:
            for sid in chunk:
                unresolved.append({"item": {"key": ",".join(sorted(keys_by_id.get(sid, set())))}, "hint": f"{endpoint_type}_remove_failed"})
        sleep_ms(delay)
    _info("write_done", op="remove", list_id=resource.id, endpoint_type=endpoint_type, applied=removed, unresolved=len(unresolved))
    return {"ok": True, "count": removed, "unresolved": unresolved, "confirmed_keys": sorted(confirmed_set)}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    endpoint_type, raw_id, resource = _resolved_resource(adapter, playlist_id)
    if endpoint_type != "playlist":
        return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}
    current, by_key, _keys_by_id = _current_members(adapter, endpoint_type, raw_id)
    item_id_by_key: dict[str, str] = {}
    for key, ids in by_key.items():
        if ids and key not in item_id_by_key:
            item_id_by_key[key] = ids[0]
    target = [str(k) for k in (ordered_keys or []) if str(k) in item_id_by_key]
    work = [k for k in current if k in item_id_by_key]
    moves = 0
    for idx, key in enumerate(target):
        if idx < len(work) and work[idx] == key:
            continue
        item_id = item_id_by_key.get(key)
        if not item_id:
            continue
        if not playlist_move_item(adapter.client, raw_id, item_id, idx):
            _warn("reorder_failed", list_id=resource.id, item_id=item_id, index=idx)
            return {"ok": False, "count": moves, "reordered": moves, "error": "move_failed"}
        try:
            work.remove(key)
        except ValueError:
            pass
        work.insert(idx, key)
        moves += 1
    _info("reorder_done", list_id=resource.id, moves=moves)
    return {"ok": True, "count": moves, "reordered": moves}
