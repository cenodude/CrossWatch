# CrossWatch - FlickList playlist sync
from __future__ import annotations

from cw_platform.interactive_reads import remember_resource, retained_read

import copy
import time
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import unquote, urlparse

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PLAYLIST_KIND_SMART,
    PlaylistItem,
    PlaylistLinkError,
    PlaylistReadOnlyError,
    PlaylistResource,
    PlaylistSnapshot,
    PlaylistUnavailableError,
)

from ._common import (
    API_V3_BASE,
    FlickListError,
    HISTORY_PAGE_MAX,
    URL_LIST,
    URL_LISTS,
    URL_LIST_ITEMS,
    cfg_section,
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
URL_PUBLIC_LIST = f"{API_V3_BASE}/lists/{{id}}"
URL_PUBLIC_ITEMS = f"{API_V3_BASE}/lists/{{id}}/items"
URL_USER_LISTS = f"{API_V3_BASE}/users/{{username}}/lists"
PUBLIC_PAGE = 100
PUBLIC_MAX_PAGES = 200
_HOSTS = {"flicklist.tv", "www.flicklist.tv"}
_SNAPSHOT_MEMO_TTL = 6 * 3600.0
_SNAPSHOT_MEMO_MAX = 16
_SNAPSHOT_MEMO: dict[tuple[str, str], tuple[float, tuple[str, Any], list[PlaylistItem]]] = {}


class FlickListPlaylistError(RuntimeError):
    pass


class FlickListPlaylistNotFound(FlickListPlaylistError):
    pass


def _instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def _me(adapter: Any) -> str:
    try:
        return str(cfg_section(adapter).get("username") or "").strip().lower()
    except Exception:
        return ""


def _rows(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, Mapping):
        for key in ("items", "data", "results", "lists"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    raise FlickListError("flicklist_playlists_invalid_response")


def _header_page_count(resp: Any, current: int) -> int:
    page_count = header_int(resp, "X-FlickList-Page-Count")
    return max(current, page_count) if page_count is not None else current


def _owner(row: Mapping[str, Any]) -> str:
    owner = row.get("owner")
    return str(owner.get("username") or "").strip() if isinstance(owner, Mapping) else ""


def _resource_from_row(adapter: Any, row: Mapping[str, Any], *, public: bool = False) -> PlaylistResource | None:
    raw_id = str(row.get("id") or row.get("list_id") or "").strip()
    if not raw_id:
        return None
    is_smart = bool(row.get("is_smart") or row.get("smart") or str(row.get("type") or "").lower() == "smart")
    name = str(row.get("name") or row.get("title") or raw_id).strip()
    owner = _owner(row)
    writable = not public and not is_smart
    extra: dict[str, Any] = {
        "raw_id": raw_id,
        "description": str(row.get("description") or "").strip(),
        "item_count": row.get("item_count") or row.get("items_count") or row.get("count"),
        "type": str(row.get("type") or "").strip().lower(),
        "slug": str(row.get("slug") or "").strip(),
        "privacy": str(row.get("privacy") or "").strip().lower(),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }
    if public:
        extra.update(endpoint_type="public_list", public_list=True, owner=owner, can_rename=False, can_delete=False)
    return PlaylistResource(
        provider="FLICKLIST",
        id=raw_id,
        name=f"{name} by {owner}" if public and owner else name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_SMART if is_smart else PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=writable,
        can_remove=writable,
        can_reorder=False,
        media_types=MEDIA_TYPES,
        extra=extra,
    )


@retained_read
def list_resources(adapter: Any) -> list[PlaylistResource]:
    resp = flicklist_request(adapter, "GET", URL_LISTS)
    if not ok_status(resp):
        _warn(FEATURE, "list_resources_failed", status=int(resp.status_code), error=error_of(resp))
        raise FlickListError(f"flicklist_playlists_list_resources_http_{int(resp.status_code)}")
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


def _detail(adapter: Any, list_id: str) -> Mapping[str, Any] | None:
    resp = flicklist_request(adapter, "GET", URL_PUBLIC_LIST.format(id=list_id))
    if int(resp.status_code) in (400, 404):
        _info(FEATURE, "list_lookup_missed", status=int(resp.status_code))
        return None
    if not ok_status(resp):
        raise FlickListPlaylistError(f"flicklist list lookup failed: http {int(resp.status_code)}")
    data = safe_json(resp)
    return data if isinstance(data, Mapping) and str(data.get("id") or "").strip() else None


def _public_resource(adapter: Any, row: Mapping[str, Any]) -> PlaylistResource | None:
    owner = _owner(row).lower()
    me = _me(adapter)
    return _resource_from_row(adapter, row, public=not (me and owner == me))


def _public_list_resource(adapter: Any, list_id: str) -> PlaylistResource:
    row = _detail(adapter, list_id) if list_id.isdigit() else None
    res = _public_resource(adapter, row) if row is not None else None
    if res is not None:
        return res
    return PlaylistResource(
        provider="FLICKLIST",
        id=list_id,
        name=list_id,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        media_types=MEDIA_TYPES,
        extra={"raw_id": list_id, "unresolved": True, "public_list": True, "can_rename": False, "can_delete": False},
    )


def _unavailable(resource: PlaylistResource) -> PlaylistUnavailableError:
    label = resource.name or resource.id
    if resource.extra.get("unresolved"):
        return PlaylistUnavailableError(f"The FlickList list {label} could not be read. It may be private, unlisted or deleted")
    if resource.extra.get("public_list"):
        return PlaylistUnavailableError(f"The FlickList list {label} is private or no longer exists")
    return PlaylistUnavailableError(f"The FlickList list {label} no longer exists")


def _snapshot_stamp(resource: PlaylistResource) -> tuple[str, Any] | None:
    if resource.is_smart:
        return None
    updated = str(resource.extra.get("updated_at") or "").strip()
    return (updated, resource.extra.get("item_count")) if updated else None


def _memo_get(adapter: Any, list_id: str, stamp: tuple[str, Any] | None) -> list[PlaylistItem] | None:
    if stamp is None:
        return None
    hit = _SNAPSHOT_MEMO.get((_instance_id(adapter), list_id))
    if not hit or hit[1] != stamp or (time.time() - hit[0]) > _SNAPSHOT_MEMO_TTL:
        return None
    return copy.deepcopy(hit[2])


def _memo_put(adapter: Any, list_id: str, stamp: tuple[str, Any] | None, items: list[PlaylistItem]) -> None:
    if stamp is None:
        return
    key = (_instance_id(adapter), list_id)
    _SNAPSHOT_MEMO.pop(key, None)
    _SNAPSHOT_MEMO[key] = (time.time(), stamp, copy.deepcopy(items))
    while len(_SNAPSHOT_MEMO) > _SNAPSHOT_MEMO_MAX:
        _SNAPSHOT_MEMO.pop(next(iter(_SNAPSHOT_MEMO)))


def _memo_drop(adapter: Any, list_id: str) -> None:
    _SNAPSHOT_MEMO.pop((_instance_id(adapter), list_id), None)


def _item_from_row(row: Mapping[str, Any]) -> PlaylistItem | None:
    media = read_minimal(row, default_type=row.get("media_type") or row.get("type") or "movie", seasons=True)
    if not media:
        return None
    key = canonical_key(media)
    if not key:
        return None
    item_id = str(row.get("id") or row.get("item_id") or row.get("list_item_id") or "").strip() or None
    return PlaylistItem.from_media(media, playlist_item_id=item_id, provider_media_id=str(row.get("fldb") or media.get("_flicklist_fldb") or ""))


def _own_items(adapter: Any, resource: PlaylistResource) -> list[PlaylistItem]:
    items: list[PlaylistItem] = []
    page = 1
    while True:
        resp = flicklist_request(adapter, "GET", URL_LIST_ITEMS.format(id=resource.id), params={"page": page, "limit": HISTORY_PAGE_MAX})
        if int(resp.status_code) == 404:
            raise _unavailable(resource)
        if not ok_status(resp):
            raise FlickListPlaylistError(error_of(resp) or f"http:{int(resp.status_code)}")
        rows = _rows(safe_json(resp))
        if any(not isinstance(row, Mapping) for row in rows):
            raise FlickListPlaylistError("flicklist list page invalid")
        items.extend(item for item in (_item_from_row(row) for row in rows) if item is not None)
        if page >= _header_page_count(resp, page):
            break
        page += 1
    return items


def _public_items(adapter: Any, resource: PlaylistResource) -> list[PlaylistItem]:
    items: list[PlaylistItem] = []
    raw_rows = 0
    expected: int | None = None
    listed = resource.extra.get("item_count")
    for page in range(1, PUBLIC_MAX_PAGES + 1):
        resp = flicklist_request(adapter, "GET", URL_PUBLIC_ITEMS.format(id=resource.id), params={"page": page, "limit": PUBLIC_PAGE})
        if int(resp.status_code) in (400, 404):
            raise _unavailable(resource)
        if not ok_status(resp):
            raise FlickListPlaylistError(error_of(resp) or f"http:{int(resp.status_code)}")
        rows = safe_json(resp)
        if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
            raise FlickListPlaylistError("flicklist list page invalid")
        page_count = header_int(resp, "X-FlickList-Page-Count")
        count = header_int(resp, "X-FlickList-Item-Count")
        expected = count if count is not None else expected
        if not rows and page_count is not None and page < page_count:
            raise FlickListPlaylistError("flicklist list page incomplete")
        raw_rows += len(rows)
        for row in rows:
            item = _item_from_row(row)
            if item is not None:
                item.position = len(items)
                items.append(item)
        if not rows or (page_count is not None and page >= page_count) or (page_count is None and len(rows) < PUBLIC_PAGE):
            break
    else:
        raise FlickListPlaylistError("flicklist list too large")
    if expected is None and isinstance(listed, int) and not isinstance(listed, bool):
        expected = listed
    if expected is not None and raw_rows != expected:
        raise FlickListPlaylistError("flicklist list read incomplete")
    return items


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise FlickListPlaylistNotFound("missing flicklist list id")
    own = _find_resource(adapter, lid)
    resource = own or _public_list_resource(adapter, lid)
    stamp = _snapshot_stamp(resource)
    cached = _memo_get(adapter, resource.id, stamp)
    if cached is not None:
        _info(FEATURE, "snapshot_unchanged", list_id=resource.id, count=len(cached))
        return PlaylistSnapshot(resource=resource, items=cached, checkpoint=None)
    if own is not None:
        items = _own_items(adapter, resource)
    elif resource.id.isdigit():
        items = _public_items(adapter, resource)
    else:
        raise _unavailable(resource)
    _memo_put(adapter, resource.id, stamp, items)
    _info(FEATURE, "snapshot_done", list_id=resource.id, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _link(ref: Any) -> tuple[str, str]:
    raw = str(ref or "").strip()
    if raw.isdigit():
        return raw, ""
    if "://" not in raw and raw.split("/", 1)[0].lower() in _HOSTS:
        raw = f"https://{raw}"
    if "://" in raw:
        parsed = urlparse(raw)
        if (parsed.hostname or "").lower() not in _HOSTS:
            return "", ""
        path = parsed.path
    else:
        path = raw.split("?", 1)[0].split("#", 1)[0]
    parts = [unquote(p) for p in path.split("/") if p]
    if len(parts) == 4 and parts[0].lower() == "u" and parts[2].lower() == "lists":
        return parts[1], parts[3]
    if len(parts) == 2 and parts[0].lower() == "lists" and parts[1].isdigit():
        return parts[1], ""
    return "", ""


def _user_list(adapter: Any, username: str, slug: str) -> PlaylistResource:
    if username.lower() == _me(adapter):
        for res in list_resources(adapter):
            if res.extra.get("slug") == slug:
                return res
        raise PlaylistUnavailableError(f"The FlickList list {slug} no longer exists")
    for page in range(1, PUBLIC_MAX_PAGES + 1):
        resp = flicklist_request(adapter, "GET", URL_USER_LISTS.format(username=username), params={"page": page, "limit": PUBLIC_PAGE})
        if int(resp.status_code) == 404:
            raise PlaylistUnavailableError(f"The FlickList user {username} does not exist")
        if not ok_status(resp):
            raise FlickListPlaylistError(f"flicklist user lists failed: http {int(resp.status_code)}")
        rows = safe_json(resp)
        if not isinstance(rows, list):
            raise FlickListPlaylistError("flicklist user lists invalid")
        for row in rows:
            if isinstance(row, Mapping) and str(row.get("slug") or "") == slug:
                res = _public_resource(adapter, row)
                if res is not None:
                    return res
        page_count = header_int(resp, "X-FlickList-Page-Count")
        if not rows or page_count is None or page >= page_count:
            break
    raise PlaylistUnavailableError(f"The FlickList list {slug} is private, unlisted or no longer exists")


def resolve(adapter: Any, ref: Any) -> PlaylistResource:
    first, slug = _link(ref)
    if not first:
        raise PlaylistLinkError("unsupported FlickList list link")
    if slug:
        return _user_list(adapter, first, slug)
    known = _find_resource(adapter, first)
    if known is not None:
        return known
    row = _detail(adapter, first)
    res = _public_resource(adapter, row) if row is not None else None
    if res is None:
        raise PlaylistUnavailableError(f"The FlickList list {first} is private, unlisted or no longer exists")
    return res


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
    remember_resource(list_resources, adapter, resource)
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
        payload = write_ident(media, seasons=True)
        if not key or payload is None:
            unresolved.append({"item": media, "hint": "missing_supported_id"})
            continue
        if key in seen:
            continue
        seen.add(key)
        accepted.append((key, payload))
    return accepted, unresolved


def _writable(adapter: Any, playlist_id: Any) -> str:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise FlickListPlaylistNotFound("missing flicklist list id")
    res = _find_resource(adapter, lid)
    if res is None or not (res.can_add or res.can_remove):
        raise PlaylistReadOnlyError(f"The FlickList list {res.name if res else lid} is read only")
    return res.id


def _write_items(adapter: Any, method: str, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = _writable(adapter, playlist_id)
    accepted, unresolved = _accepted(items)
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    ok = True
    if not accepted:
        return {"ok": True, "count": 0, "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}
    _memo_drop(adapter, lid)
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
    res = _find_resource(adapter, lid)
    if res is None:
        raise PlaylistReadOnlyError(f"The FlickList list {lid} is not yours to delete")
    _memo_drop(adapter, lid)
    resp = flicklist_request(adapter, "DELETE", URL_LIST.format(id=lid))
    return {"ok": ok_status(resp) or int(resp.status_code) == 404, "status": int(resp.status_code)}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}
