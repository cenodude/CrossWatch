# /providers/sync/publicmetadb/_playlists.py
# PublicMetaDB Module for playlist sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.interactive_reads import remember_resource, retained_read

import re
from typing import Any, Mapping, Sequence
from urllib.parse import quote, unquote, urlparse

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PlaylistItem,
    PlaylistLinkError,
    PlaylistReadOnlyError,
    PlaylistResource,
    PlaylistSnapshot,
    PlaylistUnavailableError,
)

from .._log import log as cw_log
from ._common import as_int, paged_items, tmdb_id_for_item

_PROVIDER = "PUBLICMETADB"
_FEATURE = "playlists"
_MEDIA_TYPES = ("movie", "show")
_HOSTS = {"publicmetadb.com", "www.publicmetadb.com"}
_LIST_ID = re.compile(r"^[a-z0-9]{15}$")


class PublicMetaDBPlaylistError(RuntimeError):
    pass


class PublicMetaDBPlaylistNotFound(PublicMetaDBPlaylistError, PlaylistUnavailableError):
    pass


def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)


def _instance_id(adapter: Any) -> str:
    inst = getattr(adapter, "instance_id", None)
    return str(inst).strip() if inst else "default"


def _norm_type(value: Any) -> str:
    s = str(value or "").strip().lower()
    if s in ("show", "shows", "series", "tv"):
        return "show"
    return "movie"


def _supported_type(value: Any) -> str | None:
    s = str(value or "").strip().lower()
    if s in ("show", "shows", "series", "tv"):
        return "show"
    if s in ("movie", "movies", "film"):
        return "movie"
    return None


def _resource_from_row(adapter: Any, row: Mapping[str, Any], *, public: bool = False) -> PlaylistResource | None:
    raw_id = str(row.get("id") or row.get("list_id") or "").strip()
    if not raw_id:
        return None
    name = str(row.get("name") or row.get("title") or "").strip() or raw_id
    count = as_int(row.get("item_count") or row.get("items_count") or row.get("items") or row.get("count"))
    public_raw = row.get("is_public")
    if public_raw is None:
        public_raw = row.get("public")
    list_type = str(row.get("type") or "").strip().lower()
    is_watchlist = list_type == "watchlist" and not public
    owner = str(row.get("contributor") or "").strip()
    extra: dict[str, Any] = {
        "endpoint_type": "playlist",
        "raw_id": raw_id,
        "item_count": count,
        "private": not bool(public_raw),
        "type": list_type,
        "slug": str(row.get("slug") or "").strip(),
        "description": str(row.get("description") or "").strip(),
        "builtin": is_watchlist,
        "can_rename": False,
        "can_delete": not is_watchlist and not public,
    }
    if public:
        extra.update(endpoint_type="public_list", public_list=True, owner=owner)
    return PlaylistResource(
        provider=_PROVIDER,
        id=raw_id,
        name=f"{name} by {owner}" if public and owner else name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=not public,
        can_remove=not public,
        can_reorder=False,
        media_types=_MEDIA_TYPES,
        extra=extra,
    )


@retained_read
def list_resources(adapter: Any) -> list[PlaylistResource]:
    out: list[PlaylistResource] = []
    for row in paged_items(adapter, "/api/external/lists", per_page=500, max_pages=1000, what="playlists_resources"):
        res = _resource_from_row(adapter, row)
        if res:
            out.append(res)
    _info("list_resources_done", count=len(out))
    return out


def _find_resource(adapter: Any, playlist_id: Any) -> PlaylistResource | None:
    lid = str(playlist_id or "").strip()
    if not lid:
        return None
    for res in list_resources(adapter):
        if res.id == lid:
            return res
    return None


def _list_head(adapter: Any, path: str, label: str) -> Mapping[str, Any]:
    r = adapter.client.get(path, params={"page": 1, "perPage": 1})
    if int(r.status_code) in (400, 403, 404):
        raise PublicMetaDBPlaylistNotFound(f"The PublicMetaDB list {label} is private or no longer exists")
    if not (200 <= int(r.status_code) < 300):
        raise PublicMetaDBPlaylistError(f"publicmetadb list lookup failed: HTTP {r.status_code}")
    data = adapter.client.safe_json(r)
    head = data.get("list") if isinstance(data, Mapping) else None
    if not isinstance(head, Mapping) or not str(head.get("id") or "").strip():
        raise PublicMetaDBPlaylistError("publicmetadb list lookup returned no list")
    return head


def _public_resource(adapter: Any, head: Mapping[str, Any]) -> PlaylistResource:
    known = _find_resource(adapter, head.get("id"))
    if known is not None:
        return known
    res = _resource_from_row(adapter, head, public=True)
    if res is None:
        raise PublicMetaDBPlaylistError("publicmetadb list lookup returned no list")
    return res


def _minimal_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    tmdb = tmdb_id_for_item(row)
    if tmdb is None:
        return None
    typ = _norm_type(row.get("media_type") or row.get("type"))
    out: dict[str, Any] = {"type": typ, "ids": {"tmdb": str(tmdb)}}
    title = str(row.get("title") or row.get("name") or "").strip()
    if title:
        out["title"] = title
    year = as_int(row.get("year") or row.get("release_year") or row.get("first_air_year"))
    if year is not None:
        out["year"] = year
    return id_minimal(out)


def _playlist_item_id(row: Mapping[str, Any]) -> str:
    return str(row.get("id") or row.get("item_id") or row.get("list_item_id") or "").strip()


@retained_read
def _fetch_items(adapter: Any, list_id: str) -> tuple[list[PlaylistItem], dict[str, str]]:
    out: list[PlaylistItem] = []
    remote_ids: dict[str, str] = {}
    per_page = int(getattr(adapter.cfg, "watchlist_page_size", 500) or 500)
    per_page = max(1, min(per_page, 500))
    for row in paged_items(adapter, f"/api/external/lists/{list_id}/items", per_page=per_page, max_pages=1000, what="playlists_items"):
        media = _minimal_from_row(row)
        if not media:
            continue
        item_id = _playlist_item_id(row)
        key = canonical_key(media)
        if item_id:
            remote_ids[key] = item_id
        out.append(
            PlaylistItem.from_media(
                media,
                playlist_item_id=item_id or None,
                position=None,
                provider_media_id=item_id or str(tmdb_id_for_item(media) or ""),
            )
        )
    return out, remote_ids


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise PublicMetaDBPlaylistNotFound("missing publicmetadb list id")
    resource = _find_resource(adapter, lid)
    if resource is None:
        if not _LIST_ID.match(lid):
            raise PublicMetaDBPlaylistNotFound(f"The PublicMetaDB list {lid} could not be found")
        resource = _public_resource(adapter, _list_head(adapter, f"/api/external/lists/{lid}/items", lid))
    items, _remote_ids = _fetch_items(adapter, resource.id)
    _info("snapshot_done", list_id=resource.id, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _link(ref: Any) -> tuple[str, str, str]:
    raw = str(ref or "").strip()
    if _LIST_ID.match(raw):
        return "id", raw, ""
    if "://" not in raw and raw.split("/", 1)[0].lower() in _HOSTS:
        raw = f"https://{raw}"
    if "://" in raw:
        parsed = urlparse(raw)
        if (parsed.hostname or "").lower() not in _HOSTS:
            return "", "", ""
        path = parsed.path
    else:
        path = raw.split("?", 1)[0].split("#", 1)[0]
    parts = [unquote(p) for p in path.split("/") if p]
    if len(parts) == 4 and parts[0].lower() == "lists" and parts[1].lower() == "u":
        return "user", parts[2], parts[3]
    if len(parts) == 3 and parts[0].lower() == "lists" and parts[1].lower() == "my":
        return "mine", parts[2], ""
    return "", "", ""


def resolve(adapter: Any, ref: Any) -> PlaylistResource:
    kind, first, slug = _link(ref)
    if not kind:
        raise PlaylistLinkError("unsupported PublicMetaDB list link")
    if kind == "mine":
        for res in list_resources(adapter):
            if res.extra.get("slug") == first or res.id == first or (first == "watchlist" and res.extra.get("builtin")):
                return res
        raise PublicMetaDBPlaylistNotFound(f"The PublicMetaDB list {first} no longer exists")
    if kind == "id":
        known = _find_resource(adapter, first)
        if known is not None:
            return known
        return _public_resource(adapter, _list_head(adapter, f"/api/external/lists/{first}/items", first))
    path = f"/api/external/lists/u/{quote(first, safe='')}/{quote(slug, safe='')}/items"
    return _public_resource(adapter, _list_head(adapter, path, slug))


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
        return PlaylistResource(
            provider=_PROVIDER,
            id="",
            name=nm,
            instance=_instance_id(adapter),
            kind=PLAYLIST_KIND_REGULAR,
            can_add=True,
            can_remove=True,
            can_reorder=False,
            media_types=_MEDIA_TYPES,
            extra={"endpoint_type": "playlist"},
        )
    data = adapter.client.post_json(
        "/api/external/lists",
        json={"name": nm, "description": "", "is_public": False, "type": "custom"},
    )
    item = data.get("item") if isinstance(data, Mapping) else None
    row = dict(item) if isinstance(item, Mapping) else dict(data) if isinstance(data, Mapping) else {}
    row.setdefault("name", nm)
    res = _resource_from_row(adapter, row)
    if res is None:
        raise PublicMetaDBPlaylistError("publicmetadb create list returned no id")
    remember_resource(list_resources, adapter, res)
    _info("create_done", list_id=res.id, name=res.name)
    if items:
        add(adapter, res.id, items)
    return res


def _writable(adapter: Any, playlist_id: Any) -> str:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise PublicMetaDBPlaylistNotFound("missing publicmetadb list id")
    res = _find_resource(adapter, lid)
    if res is None or not (res.can_add or res.can_remove):
        raise PlaylistReadOnlyError(f"The PublicMetaDB list {lid} is read only")
    return res.id


def _accepted_items(items: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        media = id_minimal(raw)
        typ = _supported_type(media.get("type"))
        if typ is None:
            unresolved.append({"item": media, "hint": "unsupported_media_type"})
            continue
        tmdb = tmdb_id_for_item(media)
        if tmdb is None:
            unresolved.append({"item": media, "hint": "missing_tmdb_id"})
            continue
        key = canonical_key(media)
        if not key or key in seen:
            continue
        seen.add(key)
        accepted.append({"tmdb_id": int(tmdb), "media_type": "tv" if typ == "show" else "movie"})
        keys.append(key)
    return accepted, keys, unresolved


def _write_confirmed(adapter: Any, resp: Any, payload: Mapping[str, Any]) -> bool:
    data = adapter.client.safe_json(resp)
    item = data.get("item") if isinstance(data, Mapping) else None
    return bool(
        isinstance(data, Mapping)
        and data.get("success") is True
        and isinstance(item, Mapping)
        and as_int(item.get("tmdb_id")) == payload["tmdb_id"]
        and str(item.get("media_type") or "").strip().lower() == payload["media_type"]
    )


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = _writable(adapter, playlist_id)
    accepted, keys, unresolved = _accepted_items(list(items or []))
    ok = 0
    confirmed: list[str] = []
    for idx, payload in enumerate(accepted):
        key = keys[idx] if idx < len(keys) else ""
        r = adapter.client.post(f"/api/external/lists/{lid}/items", json=payload)
        if 200 <= r.status_code < 300 and _write_confirmed(adapter, r, payload):
            ok += 1
            confirmed.append(key)
        elif 200 <= r.status_code < 300:
            unresolved.append({"item": {"key": key}, "hint": "write_not_verified", "index": idx})
        else:
            unresolved.append({"item": {"key": key}, "hint": f"http:{r.status_code}", "index": idx})
    _info("write_done", op="add", list_id=lid, applied=ok, unresolved=len(unresolved))
    return {"ok": True, "count": ok, "unresolved": unresolved, "confirmed_keys": confirmed}


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = _writable(adapter, playlist_id)
    _current, remote_ids = _fetch_items(adapter, lid)
    unresolved: list[dict[str, Any]] = []
    ok = 0
    confirmed: list[str] = []
    for raw in items or []:
        media = id_minimal(raw)
        typ = _supported_type(media.get("type"))
        if typ is None:
            unresolved.append({"item": media, "hint": "unsupported_media_type"})
            continue
        tmdb = tmdb_id_for_item(media)
        if tmdb is None:
            unresolved.append({"item": media, "hint": "missing_tmdb_id"})
            continue
        key = canonical_key(media)
        item_id = remote_ids.get(key)
        if not item_id:
            unresolved.append({"item": media, "hint": "missing_remote_item_id"})
            continue
        r = adapter.client.delete(f"/api/external/lists/{lid}/items/{item_id}")
        if 200 <= r.status_code < 300:
            ok += 1
            confirmed.append(key)
        else:
            unresolved.append({"item": media, "hint": f"http:{r.status_code}"})
    _info("write_done", op="remove", list_id=lid, applied=ok, unresolved=len(unresolved))
    return {"ok": True, "count": ok, "unresolved": unresolved, "confirmed_keys": confirmed}


def delete(adapter: Any, playlist_id: Any) -> dict[str, Any]:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise PublicMetaDBPlaylistNotFound("missing publicmetadb list id")
    resource = _find_resource(adapter, lid)
    if resource is None:
        raise PlaylistReadOnlyError("The PublicMetaDB list is not yours to delete")
    if (resource.extra or {}).get("type") == "watchlist":
        raise PublicMetaDBPlaylistError("publicmetadb watchlist cannot be deleted")
    r = adapter.client.delete(f"/api/external/lists/{lid}")
    if not (200 <= r.status_code < 300):
        raise PublicMetaDBPlaylistError(f"publicmetadb delete failed: HTTP {r.status_code}")
    _info("delete_done", list_id=lid)
    return {"ok": True, "playlist_id": lid}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}
