from __future__ import annotations

from typing import Any, Mapping, Sequence

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.playlists import PLAYLIST_KIND_REGULAR, PlaylistItem, PlaylistResource, PlaylistSnapshot

from .._log import log as cw_log
from ._common import as_int, tmdb_id_for_item

_PROVIDER = "PUBLICMETADB"
_FEATURE = "playlists"
_MEDIA_TYPES = ("movie", "show")


class PublicMetaDBPlaylistError(RuntimeError):
    pass


class PublicMetaDBPlaylistNotFound(PublicMetaDBPlaylistError):
    pass


def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)


def _instance_id(adapter: Any) -> str:
    inst = getattr(adapter, "instance_id", None)
    return str(inst).strip() if inst else "default"


def _rows(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if not isinstance(data, Mapping):
        return []
    for key in ("items", "results", "data", "lists"):
        val = data.get(key)
        if isinstance(val, list):
            return val
    return []


def _total_pages(data: Any, page: int) -> int:
    if not isinstance(data, Mapping):
        return page
    for key in ("totalPages", "total_pages", "pages", "pageCount"):
        value = as_int(data.get(key))
        if value is not None:
            return max(page, value)
    pagination = data.get("pagination")
    if isinstance(pagination, Mapping):
        for key in ("totalPages", "total_pages", "pages", "pageCount"):
            value = as_int(pagination.get(key))
            if value is not None:
                return max(page, value)
    return page


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


def _resource_from_row(adapter: Any, row: Mapping[str, Any]) -> PlaylistResource | None:
    raw_id = str(row.get("id") or row.get("list_id") or "").strip()
    if not raw_id:
        return None
    name = str(row.get("name") or row.get("title") or "").strip() or raw_id
    count = as_int(row.get("item_count") or row.get("items_count") or row.get("items") or row.get("count"))
    public_raw = row.get("is_public")
    if public_raw is None:
        public_raw = row.get("public")
    return PlaylistResource(
        provider=_PROVIDER,
        id=raw_id,
        name=name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=False,
        media_types=_MEDIA_TYPES,
        extra={
            "endpoint_type": "playlist",
            "raw_id": raw_id,
            "item_count": count,
            "private": not bool(public_raw),
            "type": str(row.get("type") or "").strip().lower(),
            "description": str(row.get("description") or "").strip(),
        },
    )


def list_resources(adapter: Any) -> list[PlaylistResource]:
    out: list[PlaylistResource] = []
    page = 1
    per_page = 500
    while page <= 1000:
        data = adapter.client.get_json("/api/external/lists", params={"page": page, "perPage": per_page})
        rows = _rows(data)
        if not rows:
            break
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            res = _resource_from_row(adapter, row)
            if res:
                out.append(res)
        if page >= _total_pages(data, page):
            break
        page += 1
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


def _fetch_items(adapter: Any, list_id: str) -> tuple[list[PlaylistItem], dict[str, str]]:
    out: list[PlaylistItem] = []
    remote_ids: dict[str, str] = {}
    page = 1
    per_page = int(getattr(adapter.cfg, "watchlist_page_size", 100) or 100)
    per_page = max(1, min(per_page, 500))
    while page <= 1000:
        data = adapter.client.get_json(
            f"/api/external/lists/{list_id}/items",
            params={"page": page, "perPage": per_page},
        )
        rows = _rows(data)
        if not rows:
            break
        for row in rows:
            if not isinstance(row, Mapping):
                continue
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
        if page >= _total_pages(data, page):
            break
        page += 1
    return out, remote_ids


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise PublicMetaDBPlaylistNotFound("missing publicmetadb list id")
    resource = _find_resource(adapter, lid)
    if resource is None:
        resource = PlaylistResource(
            provider=_PROVIDER,
            id=lid,
            name=lid,
            instance=_instance_id(adapter),
            kind=PLAYLIST_KIND_REGULAR,
            can_read=True,
            can_add=True,
            can_remove=True,
            can_reorder=False,
            media_types=_MEDIA_TYPES,
            extra={"endpoint_type": "playlist", "raw_id": lid},
        )
    items, _remote_ids = _fetch_items(adapter, lid)
    _info("snapshot_done", list_id=lid, count=len(items))
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
        json={"name": nm, "description": "", "is_public": False, "type": "list"},
    )
    item = data.get("item") if isinstance(data, Mapping) else None
    row = dict(item) if isinstance(item, Mapping) else dict(data) if isinstance(data, Mapping) else {}
    row.setdefault("name", nm)
    res = _resource_from_row(adapter, row)
    if res is None:
        raise PublicMetaDBPlaylistError("publicmetadb create list returned no id")
    _info("create_done", list_id=res.id, name=res.name)
    if items:
        add(adapter, res.id, items)
    return res


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


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise PublicMetaDBPlaylistNotFound("missing publicmetadb list id")
    accepted, keys, unresolved = _accepted_items(list(items or []))
    ok = 0
    confirmed: list[str] = []
    for idx, payload in enumerate(accepted):
        r = adapter.client.post(f"/api/external/lists/{lid}/items", json=payload)
        if 200 <= r.status_code < 300:
            ok += 1
            if idx < len(keys):
                confirmed.append(keys[idx])
        else:
            key = keys[idx] if idx < len(keys) else ""
            unresolved.append({"item": {"key": key}, "hint": f"http:{r.status_code}", "index": idx})
    _info("write_done", op="add", list_id=lid, applied=ok, unresolved=len(unresolved))
    return {"ok": True, "count": ok, "unresolved": unresolved, "confirmed_keys": confirmed}


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise PublicMetaDBPlaylistNotFound("missing publicmetadb list id")
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


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}
