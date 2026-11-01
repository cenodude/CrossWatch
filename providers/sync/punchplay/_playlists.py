# providers/sync/punchplay/_playlists.py
# CrossWatch - PunchPlay lists as playlists: own, shared, imported, dynamic and public lists
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import unquote, urlparse

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.interactive_reads import remember_resource, retained_read
from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PLAYLIST_KIND_SMART,
    PlaylistAuthError,
    PlaylistItem,
    PlaylistLinkError,
    PlaylistReadOnlyError,
    PlaylistResource,
    PlaylistSnapshot,
    PlaylistUnavailableError,
)

from ._common import (
    URL_LIST_DETAILS,
    URL_LIST_ITEMS,
    URL_LISTS,
    PLATFORM_BASE,
    PunchPlayError,
    _as_int,
    _info,
    _warn,
    cfg_section,
    error_of,
    punchplay_request,
    request_id_of,
    safe_json,
)

FEATURE = "playlists"
MEDIA_TYPES = ("movie", "show", "season", "episode")
URL_CREATE_LIST = f"{PLATFORM_BASE}/lists"
URL_LIST_ITEM = f"{PLATFORM_BASE}/lists/{{list_id}}/items/{{item_id}}"
INDEX_PAGE = 50
INDEX_MAX_PAGES = 200
DYNAMIC_PAGE = 100
DYNAMIC_MAX_PAGES = 500
_HOSTS = {"punchplay.tv", "www.punchplay.tv"}


class PunchPlayPlaylistError(PunchPlayError):
    pass


class PunchPlayListUnavailable(PunchPlayPlaylistError, PlaylistUnavailableError):
    pass


class PunchPlayListsTokenRejected(PunchPlayPlaylistError, PlaylistAuthError):
    pass


def _instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def _me(adapter: Any) -> str:
    try:
        return str(cfg_section(adapter).get("username") or "").strip().lower()
    except Exception:
        return ""


def _resource(adapter: Any, row: Mapping[str, Any], *, public: bool = False) -> PlaylistResource | None:
    lid = _as_int(row.get("id"))
    if lid is None:
        return None
    dynamic = bool(row.get("isDynamicList"))
    external = str(row.get("externalSource") or "").strip()
    watchlist = bool(row.get("isWatchlist"))
    owner = str(row.get("ownerUsername") or "").strip()
    owner_is_me = not owner or owner.lower() == _me(adapter)
    collaborator = bool(row.get("isCollaborator")) or (not public and not owner_is_me)
    shared = public or (not owner_is_me and not row.get("isOwner"))
    writable = not public and not dynamic and not external
    name = str(row.get("name") or "").strip() or str(lid)
    extra: dict[str, Any] = {
        "item_count": _as_int(row.get("itemCount")),
        "private": not bool(row.get("isPublic")),
        "dynamic": dynamic,
        "external_source": external,
        "owner": owner,
        "builtin": watchlist and not shared,
        "can_rename": not shared and not watchlist and not external,
        "can_delete": not shared and not watchlist and not external,
    }
    if public:
        extra.update(endpoint_type="public_list", public_list=True)
    elif collaborator and shared:
        extra["endpoint_type"] = "collaboration"
    if dynamic:
        extra.update(source_kind="smart")
    return PlaylistResource(
        provider="PUNCHPLAY",
        id=str(lid),
        name=f"{name} by {owner}" if shared and owner else name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_SMART if dynamic else PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=writable,
        can_remove=writable,
        can_reorder=False,
        media_types=MEDIA_TYPES,
        extra=extra,
    )


@retained_read
def list_resources(adapter: Any) -> list[PlaylistResource]:
    out: list[PlaylistResource] = []
    seen: set[str] = set()
    cursor: str | None = None
    for _page in range(INDEX_MAX_PAGES):
        params: dict[str, Any] = {"limit": INDEX_PAGE}
        if cursor:
            params["cursor"] = cursor
        resp = punchplay_request(adapter, "GET", URL_LISTS, params=params)
        if resp.status_code in (401, 403):
            raise PunchPlayListsTokenRejected("PunchPlay rejected the lists token. Reconnect PunchPlay")
        if resp.status_code != 200:
            _warn(FEATURE, "list_resources_failed", status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
            raise PunchPlayPlaylistError(f"punchplay_playlists_lists_http_{resp.status_code}")
        data = safe_json(resp)
        rows = data.get("items") if isinstance(data, Mapping) else None
        if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
            raise PunchPlayPlaylistError("punchplay_playlists_lists_invalid_response")
        for row in rows:
            res = _resource(adapter, row)
            if res is not None and res.id not in seen:
                seen.add(res.id)
                out.append(res)
        nxt = data.get("nextCursor") or None
        if not nxt:
            break
        if nxt == cursor:
            raise PunchPlayPlaylistError("punchplay_playlists_lists_repeated_cursor")
        cursor = nxt
    else:
        raise PunchPlayPlaylistError("punchplay_playlists_lists_page_guard_tripped")
    _info(FEATURE, "list_resources_done", count=len(out))
    return out


def _find_resource(adapter: Any, lid: str) -> PlaylistResource | None:
    for res in list_resources(adapter):
        if res.id == lid:
            return res
    return None


def _detail(adapter: Any, lid: str) -> Mapping[str, Any]:
    resp = punchplay_request(adapter, "GET", URL_LIST_DETAILS.format(list_id=lid))
    if resp.status_code in (400, 401, 403, 404):
        raise PunchPlayListUnavailable(f"The PunchPlay list {lid} is private or no longer exists")
    if resp.status_code != 200:
        raise PunchPlayPlaylistError(f"punchplay_playlists_detail_http_{resp.status_code}")
    data = safe_json(resp)
    if not isinstance(data, Mapping) or _as_int(data.get("id")) is None:
        raise PunchPlayPlaylistError("punchplay_playlists_detail_invalid_response")
    return data


def _detail_resource(adapter: Any, data: Mapping[str, Any]) -> PlaylistResource | None:
    if data.get("isOwner") or data.get("isCollaborator"):
        return _resource(adapter, data)
    return _resource(adapter, data, public=True)


def _year(value: Any) -> int | None:
    text = str(value or "").strip()
    return int(text[:4]) if len(text) >= 4 and text[:4].isdigit() else None


def _media(row: Mapping[str, Any]) -> dict[str, Any] | None:
    tmdb = _as_int(row.get("tmdbId"))
    kind = str(row.get("type") or "").strip().lower()
    if tmdb is None:
        return None
    title = str(row.get("title") or "").strip()
    part_raw = row.get("part")
    part: Mapping[str, Any] = part_raw if isinstance(part_raw, Mapping) else {}
    if kind in ("movie", "show"):
        out: dict[str, Any] = {"type": kind, "ids": {"tmdb": str(tmdb)}}
        if title:
            out["title"] = title
        year = _year(row.get("releaseDate"))
        if year is not None:
            out["year"] = year
        return id_minimal(out)
    season = _as_int(part.get("season")) if part.get("season") != 0 else 0
    if kind == "season" and season is not None:
        return id_minimal({"type": "season", "show_ids": {"tmdb": str(tmdb)}, "season": season, "series_title": title, "ids": {}})
    episode = _as_int(part.get("episode"))
    if kind == "episode" and season is not None and episode is not None:
        return id_minimal({"type": "episode", "show_ids": {"tmdb": str(tmdb)}, "season": season, "episode": episode, "series_title": title, "ids": {}})
    return None


def _items_from_rows(rows: Any) -> list[PlaylistItem]:
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise PunchPlayPlaylistError("punchplay_playlists_items_invalid_response")
    items: list[PlaylistItem] = []
    for pos, row in enumerate(rows):
        kind = str(row.get("type") or "").strip().lower()
        if kind not in MEDIA_TYPES:
            continue
        media = _media(row)
        if media is None:
            _warn(FEATURE, "invalid_list_item", item_type=kind, item_id=row.get("id"))
            raise PunchPlayPlaylistError("punchplay_playlists_invalid_list_item")
        rank = _as_int(row.get("position"))
        item = PlaylistItem.from_media(media, playlist_item_id=row.get("id"), position=rank if rank is not None else pos, provider_media_id=str(row.get("tmdbId") or ""))
        if not item.key:
            raise PunchPlayPlaylistError("punchplay_playlists_invalid_list_item")
        items.append(item)
    return items


def _dynamic_rows(adapter: Any, lid: str) -> list[Any]:
    rows: list[Any] = []
    offset = 0
    total: int | None = None
    for _page in range(DYNAMIC_MAX_PAGES):
        resp = punchplay_request(adapter, "GET", URL_LIST_ITEMS.format(list_id=lid), params={"offset": offset, "limit": DYNAMIC_PAGE})
        if resp.status_code in (401, 403, 404):
            raise PunchPlayListUnavailable(f"The PunchPlay list {lid} is private or no longer exists")
        if resp.status_code != 200:
            raise PunchPlayPlaylistError(f"punchplay_playlists_items_http_{resp.status_code}")
        data = safe_json(resp)
        page = data.get("items") if isinstance(data, Mapping) else None
        if not isinstance(page, list):
            raise PunchPlayPlaylistError("punchplay_playlists_items_invalid_response")
        rows.extend(page)
        total = _as_int(data.get("total")) if data.get("total") is not None else total
        nxt = data.get("nextOffset")
        if nxt is None:
            break
        nxt_i = _as_int(nxt)
        if nxt_i is None or nxt_i <= offset:
            raise PunchPlayPlaylistError("punchplay_playlists_items_bad_offset")
        offset = nxt_i
    else:
        raise PunchPlayPlaylistError("punchplay_playlists_items_page_guard_tripped")
    if total is not None and len(rows) != total:
        raise PunchPlayPlaylistError("punchplay_playlists_items_incomplete")
    return rows


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    lid = str(playlist_id or "").strip()
    if not lid.isdigit():
        raise PunchPlayListUnavailable(f"The PunchPlay list {lid} could not be found")
    own = _find_resource(adapter, lid)
    data = _detail(adapter, lid)
    resource = own or _detail_resource(adapter, data)
    if resource is None:
        raise PunchPlayListUnavailable(f"The PunchPlay list {lid} could not be found")
    if data.get("isDynamicList"):
        rows = _dynamic_rows(adapter, lid)
    else:
        rows = data.get("items")
        count = _as_int(data.get("itemCount"))
        if isinstance(rows, list) and count is not None and len(rows) != count:
            raise PunchPlayPlaylistError("punchplay_playlists_items_incomplete")
    items = _items_from_rows(rows)
    items.sort(key=lambda it: (it.position is None, it.position if it.position is not None else 0))
    _info(FEATURE, "snapshot_done", list_id=lid, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _link_id(ref: Any) -> str:
    raw = str(ref or "").strip()
    if raw.isdigit():
        return raw
    if "://" not in raw and raw.split("/", 1)[0].lower() in _HOSTS:
        raw = f"https://{raw}"
    if "://" in raw:
        parsed = urlparse(raw)
        if (parsed.hostname or "").lower() not in _HOSTS:
            return ""
        path = parsed.path
    else:
        path = raw.split("?", 1)[0].split("#", 1)[0]
    parts = [unquote(p) for p in path.split("/") if p]
    if len(parts) == 2 and parts[0].lower() == "lists" and parts[1].isdigit():
        return parts[1]
    return ""


def resolve(adapter: Any, ref: Any) -> PlaylistResource:
    lid = _link_id(ref)
    if not lid:
        raise PlaylistLinkError("unsupported PunchPlay list link")
    known = _find_resource(adapter, lid)
    if known is not None:
        return known
    res = _detail_resource(adapter, _detail(adapter, lid))
    if res is None:
        raise PunchPlayListUnavailable(f"The PunchPlay list {lid} could not be found")
    return res


def _writable(adapter: Any, playlist_id: Any) -> str:
    lid = str(playlist_id or "").strip()
    res = _find_resource(adapter, lid) if lid.isdigit() else None
    if res is None and lid.isdigit():
        try:
            data = _detail(adapter, lid)
        except PunchPlayListUnavailable:
            data = {}
        if data.get("isCollaborator") and not data.get("isDynamicList") and not data.get("externalSource"):
            res = _resource(adapter, data)
    if res is None or not (res.can_add or res.can_remove):
        raise PlaylistReadOnlyError(f"The PunchPlay list {lid} is read only")
    return res.id


def _payload(item: Mapping[str, Any]) -> dict[str, Any] | None:
    kind = str(item.get("type") or "").strip().lower()
    if kind not in ("movie", "show"):
        return None
    ids_raw = item.get("ids")
    ids: Mapping[str, Any] = ids_raw if isinstance(ids_raw, Mapping) else {}
    tmdb = _as_int(ids.get("tmdb"))
    if tmdb is None:
        return None
    title = str(item.get("title") or item.get("series_title") or "").strip() or str(tmdb)
    return {"kind": kind, "sourceId": tmdb, "title": title[:300]}


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = _writable(adapter, playlist_id)
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    unresolved_keys: list[str] = []
    seen: set[str] = set()
    ok = True
    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        key = canonical_key(raw)
        if not key or key in seen:
            continue
        seen.add(key)
        payload = _payload(raw)
        if payload is None:
            hint = "unsupported_type" if str(raw.get("type") or "").lower() not in ("movie", "show") else "missing_tmdb_id"
            unresolved.append({"item": id_minimal(raw), "key": key, "hint": hint})
            unresolved_keys.append(key)
            continue
        resp = punchplay_request(adapter, "POST", URL_LIST_ITEMS.format(list_id=lid), json=payload)
        body = safe_json(resp)
        if resp.status_code in (200, 201) and isinstance(body, Mapping) and _as_int(body.get("id")) is not None:
            confirmed.append(key)
            continue
        ok = False
        hint = f"http:{resp.status_code}" if resp.status_code not in (200, 201) else "write_not_verified"
        unresolved.append({"item": {"key": key}, "key": key, "hint": hint})
        unresolved_keys.append(key)
        _warn(FEATURE, "write_failed", op="add", list_id=lid, status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
    _info(FEATURE, "write_done", op="add", list_id=lid, confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = _writable(adapter, playlist_id)
    current = get_snapshot(adapter, lid)
    by_key = {it.key: str(it.playlist_item_id) for it in current.items if it.key and it.playlist_item_id}
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    unresolved_keys: list[str] = []
    ok = True
    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        key = canonical_key(raw)
        item_id = by_key.pop(key, None) if key else None
        if not item_id:
            unresolved.append({"item": id_minimal(raw), "key": key, "hint": "missing_remote_item_id"})
            unresolved_keys.append(key)
            continue
        resp = punchplay_request(adapter, "DELETE", URL_LIST_ITEM.format(list_id=lid, item_id=item_id))
        if resp.status_code in (200, 204):
            confirmed.append(key)
            continue
        ok = False
        unresolved.append({"item": {"key": key}, "key": key, "hint": f"http:{resp.status_code}"})
        unresolved_keys.append(key)
        _warn(FEATURE, "write_failed", op="remove", list_id=lid, status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
    _info(FEATURE, "write_done", op="remove", list_id=lid, confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}


def _owned(adapter: Any, playlist_id: Any, *, op: str) -> PlaylistResource:
    res = _find_resource(adapter, str(playlist_id or "").strip())
    allowed = bool(res and res.extra.get("can_rename" if op == "rename" else "can_delete"))
    if res is None or not allowed:
        raise PlaylistReadOnlyError(f"The PunchPlay list cannot be {'renamed' if op == 'rename' else 'deleted'}")
    return res


def create(
    adapter: Any,
    name: str,
    *,
    media_type: str | None = None,
    items: Sequence[Mapping[str, Any]] | None = None,
    dry_run: bool = False,
) -> PlaylistResource:
    nm = str(name or "").strip()[:100]
    if not nm:
        raise ValueError("playlist name required")
    if dry_run:
        return PlaylistResource(provider="PUNCHPLAY", id="", name=nm, instance=_instance_id(adapter), can_add=True, can_remove=True, media_types=MEDIA_TYPES)
    resp = punchplay_request(adapter, "POST", URL_CREATE_LIST, json={"name": nm, "description": "", "isPublic": False})
    data = safe_json(resp)
    if resp.status_code not in (200, 201) or not isinstance(data, Mapping):
        raise PunchPlayPlaylistError(f"punchplay create list failed: http {resp.status_code}")
    res = _resource(adapter, data)
    if res is None:
        raise PunchPlayPlaylistError("punchplay create list returned no id")
    remember_resource(list_resources, adapter, res)
    if items:
        add(adapter, res.id, items)
    _info(FEATURE, "create_done", list_id=res.id)
    return res


def rename(adapter: Any, playlist_id: Any, name: str, *, dry_run: bool = False) -> PlaylistResource:
    nm = str(name or "").strip()[:100]
    if not nm:
        raise ValueError("playlist name required")
    res = _owned(adapter, playlist_id, op="rename")
    if not dry_run:
        resp = punchplay_request(adapter, "PATCH", URL_LIST_DETAILS.format(list_id=res.id), json={"name": nm})
        if resp.status_code != 200:
            raise PunchPlayPlaylistError(f"punchplay rename list failed: http {resp.status_code}")
    return PlaylistResource(provider="PUNCHPLAY", id=res.id, name=nm, instance=res.instance, kind=res.kind, can_add=res.can_add, can_remove=res.can_remove, media_types=res.media_types, extra=dict(res.extra))


def delete(adapter: Any, playlist_id: Any, *, dry_run: bool = False) -> dict[str, Any]:
    res = _owned(adapter, playlist_id, op="delete")
    if dry_run:
        return {"ok": True, "dry_run": True}
    resp = punchplay_request(adapter, "DELETE", URL_LIST_DETAILS.format(list_id=res.id))
    if resp.status_code not in (200, 204):
        raise PunchPlayPlaylistError(f"punchplay delete list failed: http {resp.status_code}")
    _info(FEATURE, "delete_done", list_id=res.id)
    return {"ok": True, "count": 1}
