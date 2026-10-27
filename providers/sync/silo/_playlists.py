# /providers/sync/silo/_playlists.py
# Silo collections and favorites as playlist endpoints
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Mapping, Sequence
from urllib.parse import quote

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.playlists import PLAYLIST_KIND_REGULAR, PLAYLIST_KIND_SMART, PlaylistItem, PlaylistResource, PlaylistSnapshot

from ._common import (
    SiloSyncError,
    cfg_section,
    library,
    minimal_for,
    pages,
    problem_of,
    resolve_item,
    safe_json,
    silo_request,
    _info,
    _warn,
)

FEATURE = "playlists"
PROVIDER = "SILO"
MEDIA_TYPES = ("movie", "show")
FAVORITES_ID = "favorites"
COLLECTION_PREFIX = "collection:"


class SiloPlaylistError(RuntimeError):
    pass


def _instance(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", "") or "default")


def _favorites_resource(adapter: Any, count: int | None = None) -> PlaylistResource:
    extra: dict[str, Any] = {"endpoint_type": "favorites", "virtual": True}
    if count is not None:
        extra["item_count"] = count
    return PlaylistResource(provider=PROVIDER, id=FAVORITES_ID, name="Favorites", instance=_instance(adapter), kind=PLAYLIST_KIND_REGULAR,
                            can_read=True, can_add=True, can_remove=True, can_reorder=False, media_types=MEDIA_TYPES, extra=extra)


def _collection_resource(adapter: Any, row: Mapping[str, Any]) -> PlaylistResource | None:
    raw = str(row.get("id") or "").strip()
    if not raw:
        return None
    own = str(row.get("creator_profile_id") or "") == str(cfg_section(adapter).get("profile_id") or "")
    manual = str(row.get("collection_type") or "manual") == "manual"
    writable = own and manual
    return PlaylistResource(
        provider=PROVIDER,
        id=f"{COLLECTION_PREFIX}{raw}",
        name=str(row.get("name") or raw).strip(),
        instance=_instance(adapter),
        kind=PLAYLIST_KIND_REGULAR if manual else PLAYLIST_KIND_SMART,
        can_read=True,
        can_add=writable,
        can_remove=writable,
        can_reorder=writable,
        media_types=MEDIA_TYPES,
        extra={"endpoint_type": "collection", "raw_id": raw, "item_count": row.get("item_count"), "shared": not own,
               "collection_type": str(row.get("collection_type") or "manual")},
    )


def _raw_collection_id(playlist_id: Any) -> str:
    pid = str(playlist_id or "").strip()
    if pid.startswith(COLLECTION_PREFIX):
        return pid[len(COLLECTION_PREFIX):]
    raise SiloPlaylistError("unknown silo playlist id")


def _collections(adapter: Any) -> list[dict[str, Any]]:
    resp = silo_request(adapter, "GET", "/collections")
    if resp.status_code != 200:
        raise SiloSyncError(f"silo_playlists_http_{resp.status_code}")
    data = safe_json(resp)
    if not isinstance(data, Mapping) or not isinstance(data.get("items"), list):
        raise SiloSyncError("silo_playlists_invalid_response")
    return [dict(r) for r in data["items"] if isinstance(r, Mapping)]


def list_resources(adapter: Any) -> list[PlaylistResource]:
    out = [res for res in (_collection_resource(adapter, row) for row in _collections(adapter)) if res]
    out.sort(key=lambda r: (bool(r.extra.get("shared")), r.name.lower(), r.id))
    out.insert(0, _favorites_resource(adapter))
    _info(FEATURE, "list_resources_done", count=len(out))
    return out


def _resource(adapter: Any, playlist_id: Any) -> PlaylistResource:
    if str(playlist_id or "").strip() == FAVORITES_ID:
        return _favorites_resource(adapter)
    raw = _raw_collection_id(playlist_id)
    resp = silo_request(adapter, "GET", f"/collections/{quote(raw, safe='')}")
    if resp.status_code == 404:
        raise SiloPlaylistError("silo collection not found")
    if resp.status_code != 200:
        raise SiloSyncError(f"silo_playlists_http_{resp.status_code}")
    res = _collection_resource(adapter, safe_json(resp) or {})
    if res is None:
        raise SiloPlaylistError("silo collection not found")
    return res


def _member_ids(adapter: Any, playlist_id: Any) -> list[str]:
    if str(playlist_id or "").strip() == FAVORITES_ID:
        rows = pages(adapter, "/favorites", feature=FEATURE) or []
        return [str(r.get("content_id") or "") for r in rows if r.get("content_id")]
    raw = _raw_collection_id(playlist_id)
    rows = pages(adapter, f"/collections/{quote(raw, safe='')}/items", feature=FEATURE) or []
    rows.sort(key=lambda r: int(r.get("position") or 0))
    return [str(r.get("media_item_id") or "") for r in rows if r.get("media_item_id")]


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    resource = _resource(adapter, playlist_id)
    lib = library(adapter)
    ordered = resource.extra.get("endpoint_type") == "collection"
    items: list[PlaylistItem] = []
    for pos, cid in enumerate(_member_ids(adapter, playlist_id)):
        media = minimal_for(lib, cid)
        if not media or media.get("type") not in MEDIA_TYPES:
            continue
        items.append(PlaylistItem.from_media(media, playlist_item_id=cid, position=pos if ordered else None, provider_media_id=cid))
    _info(FEATURE, "snapshot_done", list_id=resource.id, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _writable(resource: PlaylistResource) -> None:
    if not resource.writable:
        raise SiloPlaylistError("silo collection is read-only")


def create(adapter: Any, name: str, *, media_type: str | None = None, items: Sequence[Mapping[str, Any]] | None = None,
           dry_run: bool = False) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    if dry_run:
        return PlaylistResource(provider=PROVIDER, id="", name=nm, instance=_instance(adapter), can_add=True, can_remove=True,
                                can_reorder=True, media_types=MEDIA_TYPES, extra={"endpoint_type": "collection"})
    resp = silo_request(adapter, "POST", "/collections", json_body={"name": nm, "collection_type": "manual"})
    if resp.status_code not in (200, 201):
        raise SiloPlaylistError(f"silo create collection failed: {resp.status_code} {problem_of(resp)}")
    res = _collection_resource(adapter, safe_json(resp) or {})
    if res is None:
        raise SiloPlaylistError("silo create collection returned no id")
    _info(FEATURE, "create_done", list_id=res.id, name=res.name)
    if items:
        add(adapter, res.id, items)
    return res


def _etag(adapter: Any, path: str) -> str:
    resp = silo_request(adapter, "GET", path)
    if resp.status_code != 200:
        raise SiloSyncError(f"silo_playlists_http_{resp.status_code}")
    tag = str(resp.headers.get("ETag") or "").strip()
    if not tag:
        raise SiloPlaylistError("silo did not return a validator")
    return tag


def rename(adapter: Any, playlist_id: Any, name: str, *, dry_run: bool = False) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    resource = _resource(adapter, playlist_id)
    _writable(resource)
    raw = _raw_collection_id(playlist_id)
    if dry_run:
        return PlaylistResource(provider=PROVIDER, id=resource.id, name=nm, instance=resource.instance, can_add=True, can_remove=True,
                                can_reorder=True, media_types=MEDIA_TYPES, extra=dict(resource.extra))
    path = f"/collections/{quote(raw, safe='')}"
    resp = silo_request(adapter, "PATCH", path, json_body={"name": nm}, headers={"If-Match": _etag(adapter, path)})
    if resp.status_code != 200:
        raise SiloPlaylistError(f"silo rename collection failed: {resp.status_code} {problem_of(resp)}")
    res = _collection_resource(adapter, safe_json(resp) or {})
    if res is None:
        raise SiloPlaylistError("silo rename returned no collection")
    _info(FEATURE, "rename_done", list_id=res.id, name=res.name)
    return res


def delete(adapter: Any, playlist_id: Any, *, dry_run: bool = False) -> dict[str, Any]:
    resource = _resource(adapter, playlist_id)
    _writable(resource)
    if resource.extra.get("endpoint_type") != "collection":
        raise SiloPlaylistError("silo favorites cannot be deleted")
    if dry_run:
        return {"ok": True, "dry_run": True}
    path = f"/collections/{quote(_raw_collection_id(playlist_id), safe='')}"
    resp = silo_request(adapter, "DELETE", path, headers={"If-Match": _etag(adapter, path)})
    if resp.status_code not in (200, 204):
        raise SiloPlaylistError(f"silo delete collection failed: {resp.status_code} {problem_of(resp)}")
    _info(FEATURE, "delete_done", list_id=resource.id)
    return {"ok": True, "count": 1}


def _accepted(adapter: Any, items: Sequence[Mapping[str, Any]]) -> tuple[list[tuple[str, str]], list[dict[str, Any]]]:
    lib = library(adapter)
    accepted: list[tuple[str, str]] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        media = id_minimal(raw)
        if str(media.get("type") or "").lower() == "series":
            media["type"] = "show"
        key = canonical_key(media)
        if not key or key in seen:
            continue
        seen.add(key)
        if media.get("type") not in MEDIA_TYPES:
            unresolved.append({"item": media, "hint": "unsupported_type"})
            continue
        cid, _kind = resolve_item(adapter, lib, media, feature=FEATURE)
        if not cid:
            unresolved.append({"item": media, "hint": "not_in_library"})
            continue
        accepted.append((key, cid))
    return accepted, unresolved


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    resource = _resource(adapter, playlist_id)
    _writable(resource)
    accepted, unresolved = _accepted(adapter, list(items or []))
    confirmed: list[str] = []
    favorites = resource.extra.get("endpoint_type") == "favorites"
    current = set(_member_ids(adapter, playlist_id))
    position = len(current)
    for key, cid in accepted:
        if cid in current:
            confirmed.append(key)
            continue
        if favorites:
            resp = silo_request(adapter, "PUT", f"/favorites/{quote(cid, safe='')}")
        else:
            raw = _raw_collection_id(playlist_id)
            resp = silo_request(adapter, "PUT", f"/collections/{quote(raw, safe='')}/items/{quote(cid, safe='')}", json_body={"position": position})
        if resp.status_code in (200, 204):
            confirmed.append(key)
            current.add(cid)
            position += 1
        else:
            _warn(FEATURE, "write_failed", op="add", status=resp.status_code, problem=problem_of(resp) or None)
            unresolved.append({"item": {"key": key}, "hint": f"http_{resp.status_code}"})
    _info(FEATURE, "write_done", op="add", list_id=resource.id, applied=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "count": len(confirmed), "unresolved": unresolved, "confirmed_keys": confirmed}


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    resource = _resource(adapter, playlist_id)
    _writable(resource)
    lib = library(adapter)
    by_key: dict[str, str] = {}
    for cid in _member_ids(adapter, playlist_id):
        media = minimal_for(lib, cid)
        if media:
            by_key[canonical_key(media)] = cid
    favorites = resource.extra.get("endpoint_type") == "favorites"
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for raw in items or []:
        media = id_minimal(raw)
        key = canonical_key(media)
        cid = by_key.get(key) or resolve_item(adapter, lib, media, feature=FEATURE)[0]
        if not cid:
            unresolved.append({"item": media, "hint": "not_in_playlist"})
            continue
        if favorites:
            resp = silo_request(adapter, "DELETE", f"/favorites/{quote(cid, safe='')}")
        else:
            raw_id = _raw_collection_id(playlist_id)
            resp = silo_request(adapter, "DELETE", f"/collections/{quote(raw_id, safe='')}/items/{quote(cid, safe='')}")
        if resp.status_code in (200, 204, 404):
            confirmed.append(key)
        else:
            _warn(FEATURE, "write_failed", op="remove", status=resp.status_code, problem=problem_of(resp) or None)
            unresolved.append({"item": {"key": key}, "hint": f"http_{resp.status_code}"})
    _info(FEATURE, "write_done", op="remove", list_id=resource.id, applied=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "count": len(confirmed), "unresolved": unresolved, "confirmed_keys": confirmed}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    resource = _resource(adapter, playlist_id)
    if resource.extra.get("endpoint_type") != "collection" or not resource.can_reorder:
        return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}
    lib = library(adapter)
    members = _member_ids(adapter, playlist_id)
    by_key = {canonical_key(m): cid for cid in members for m in [minimal_for(lib, cid)] if m}
    wanted = [by_key[k] for k in ordered_keys if k in by_key]
    rest = [cid for cid in members if cid not in set(wanted)]
    ordered = wanted + rest
    if ordered == members:
        return {"ok": True, "count": 0, "reordered": 0}
    path = f"/collections/{quote(_raw_collection_id(playlist_id), safe='')}/items/order"
    resp = silo_request(adapter, "PUT", path, json_body={"ordered_ids": ordered}, headers={"If-Match": _etag(adapter, path)})
    if resp.status_code != 200:
        raise SiloPlaylistError(f"silo reorder collection failed: {resp.status_code} {problem_of(resp)}")
    _info(FEATURE, "reorder_done", list_id=resource.id, count=len(ordered))
    return {"ok": True, "count": len(ordered), "reordered": len(ordered)}
