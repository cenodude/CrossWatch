# /providers/sync/tmdb/_playlists.py
# TMDb list sync (v4 lists)
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import copy
import time
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import unquote, urlparse

from cw_platform.id_map import minimal as id_minimal
from cw_platform.interactive_reads import remember_resource, retained_read
from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PlaylistAuthError,
    PlaylistItem,
    PlaylistLinkError,
    PlaylistReadOnlyError,
    PlaylistResource,
    PlaylistSnapshot,
    PlaylistUnavailableError,
)

from .._log import log as cw_log

from ._common import (
    as_int,
    enrich_ids_dict,
    key_of,
    pick_media_type,
    resolve_tmdb_id,
    year_from_date,
)

FEATURE = "playlists"
MEDIA_TYPES = ("movie", "show")
BATCH_SIZE = 100
MAX_PAGES = 500
_HOSTS = {"themoviedb.org", "www.themoviedb.org"}
_SNAPSHOT_MEMO_TTL = 6 * 3600.0
_SNAPSHOT_MEMO_MAX = 16
_SNAPSHOT_MEMO: dict[tuple[str, str], tuple[float, tuple[str, Any], list[PlaylistItem]]] = {}


def _info(msg: str, **fields: Any) -> None:
    cw_log("TMDB", FEATURE, "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("TMDB", FEATURE, "warn", msg, **fields)


class TMDBPlaylistError(RuntimeError):
    pass


class TMDBPlaylistNotFound(TMDBPlaylistError, PlaylistUnavailableError):
    pass


class TMDBListsTokenRejected(TMDBPlaylistError, PlaylistAuthError):
    pass


def _instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def _ok(resp: Any) -> bool:
    return 200 <= int(getattr(resp, "status_code", 0) or 0) < 300


def _json(resp: Any) -> Any:
    try:
        return resp.json() if (getattr(resp, "content", b"") or b"") else {}
    except Exception:
        return {}


def _error_of(resp: Any) -> str:
    data = _json(resp)
    if isinstance(data, Mapping):
        msg = str(data.get("status_message") or data.get("message") or "").strip()
        if msg:
            return msg
    return f"http:{int(getattr(resp, 'status_code', 0) or 0)}"


def _owner(row: Mapping[str, Any]) -> tuple[str, str]:
    created = row.get("created_by")
    created = created if isinstance(created, Mapping) else {}
    return str(created.get("id") or "").strip(), str(created.get("username") or created.get("name") or "").strip()


def _resource(adapter: Any, row: Mapping[str, Any], fallback_id: str = "", *, public: bool = False) -> PlaylistResource | None:
    raw_id = str(row.get("id") or fallback_id or "").strip()
    if not raw_id:
        return None
    _owner_id, owner = _owner(row)
    name = str(row.get("name") or raw_id).strip()
    extra: dict[str, Any] = {
        "raw_id": raw_id,
        "description": str(row.get("description") or "").strip(),
        "item_count": as_int(row.get("number_of_items") if row.get("number_of_items") is not None else row.get("item_count") if row.get("item_count") is not None else row.get("total_results")),
        "private": not bool(row.get("public")),
        "updated_at": str(row.get("updated_at") or "").strip(),
    }
    if public:
        extra.update(endpoint_type="public_list", public_list=True, owner=owner, can_rename=False, can_delete=False)
    return PlaylistResource(
        provider="TMDB",
        id=raw_id,
        name=f"{name} by {owner}" if public and owner else name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=not public,
        can_remove=not public,
        can_reorder=False,
        media_types=MEDIA_TYPES,
        extra=extra,
    )


def _page(adapter: Any, path: str, page: int, *, missing: str, account: bool = False) -> Mapping[str, Any]:
    resp = adapter.client.v4("GET", path, params={"page": page})
    if account and int(resp.status_code) in (401, 403):
        raise TMDBListsTokenRejected("TMDb rejected the lists token. Reconnect TMDb")
    if int(resp.status_code) in (401, 404):
        raise TMDBPlaylistNotFound(missing)
    if not _ok(resp):
        raise TMDBPlaylistError(_error_of(resp))
    data = _json(resp)
    rows = data.get("results") if isinstance(data, Mapping) else None
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise TMDBPlaylistError(f"TMDb list page invalid ({path})")
    return data


def _pages(adapter: Any, path: str, *, missing: str = "TMDb list not found", account: bool = False) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    page = 1
    total = 1
    rows_seen = 0
    while page <= total:
        data = _page(adapter, path, page, missing=missing, account=account)
        rows = data["results"]
        total = as_int(data.get("total_pages")) or 1
        if total > MAX_PAGES:
            raise TMDBPlaylistError(f"TMDb list too large ({total} pages, limit {MAX_PAGES})")
        if not rows and page < total:
            raise TMDBPlaylistError(f"TMDb list page incomplete ({path})")
        rows_seen += len(rows)
        out.append(data)
        page += 1
    expected = as_int(out[0].get("total_results")) if out else None
    if expected is not None and rows_seen != expected:
        raise TMDBPlaylistError(f"TMDb list read incomplete ({path})")
    return out


@retained_read
def list_resources(adapter: Any) -> list[PlaylistResource]:
    object_id = adapter.client.account_object_id()
    resources: list[PlaylistResource] = []
    for data in _pages(adapter, f"/account/{object_id}/lists", missing="TMDb account lists not found", account=True):
        for row in data["results"]:
            res = _resource(adapter, row)
            if res is not None:
                resources.append(res)
    _info("list_resources_done", count=len(resources))
    return resources


def _find_resource(adapter: Any, lid: str) -> PlaylistResource | None:
    for res in list_resources(adapter):
        if res.id == lid:
            return res
    return None


def _public_resource(adapter: Any, row: Mapping[str, Any], lid: str) -> PlaylistResource | None:
    owner_id, _owner_name = _owner(row)
    try:
        mine = bool(owner_id) and owner_id == str(adapter.client.account_object_id())
    except Exception:
        mine = False
    return _resource(adapter, row, lid, public=not mine)


def _media_from_row(adapter: Any, row: Mapping[str, Any]) -> dict[str, Any] | None:
    tmdb_id = as_int(row.get("id"))
    if tmdb_id is None:
        return None
    kind = str(row.get("media_type") or "").strip().lower()
    if kind == "movie":
        item: dict[str, Any] = {
            "type": "movie",
            "title": row.get("title") or row.get("original_title"),
            "year": year_from_date(row.get("release_date")),
            "ids": {"tmdb": tmdb_id},
        }
        enrich_ids_dict(adapter, item["ids"], media_type="movie", tmdb_id=tmdb_id, feature=FEATURE)
    elif kind == "tv":
        item = {
            "type": "show",
            "title": row.get("name") or row.get("original_name"),
            "year": year_from_date(row.get("first_air_date")),
            "ids": {"tmdb": tmdb_id},
        }
        enrich_ids_dict(adapter, item["ids"], media_type="tv", tmdb_id=tmdb_id, feature=FEATURE)
    else:
        return None
    return id_minimal(item)


def _snapshot_stamp(resource: PlaylistResource) -> tuple[str, Any] | None:
    updated = str(resource.extra.get("updated_at") or "").strip()
    if not updated or resource.extra.get("public_list"):
        return None
    return updated, resource.extra.get("item_count")


def _memo_get(adapter: Any, lid: str, stamp: tuple[str, Any] | None) -> list[PlaylistItem] | None:
    if stamp is None:
        return None
    hit = _SNAPSHOT_MEMO.get((_instance_id(adapter), lid))
    if not hit or hit[1] != stamp or (time.time() - hit[0]) > _SNAPSHOT_MEMO_TTL:
        return None
    return copy.deepcopy(hit[2])


def _memo_put(adapter: Any, lid: str, stamp: tuple[str, Any] | None, items: list[PlaylistItem]) -> None:
    if stamp is None:
        return
    key = (_instance_id(adapter), lid)
    _SNAPSHOT_MEMO.pop(key, None)
    _SNAPSHOT_MEMO[key] = (time.time(), stamp, copy.deepcopy(items))
    while len(_SNAPSHOT_MEMO) > _SNAPSHOT_MEMO_MAX:
        _SNAPSHOT_MEMO.pop(next(iter(_SNAPSHOT_MEMO)))


def _memo_drop(adapter: Any, lid: str) -> None:
    _SNAPSHOT_MEMO.pop((_instance_id(adapter), lid), None)


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise TMDBPlaylistNotFound("missing TMDb list id")
    own = _find_resource(adapter, lid)
    stamp = _snapshot_stamp(own) if own is not None else None
    cached = _memo_get(adapter, lid, stamp)
    if own is not None and cached is not None:
        _info("snapshot_unchanged", list_id=lid, count=len(cached))
        return PlaylistSnapshot(resource=own, items=cached, checkpoint=None)
    if not lid.isdigit():
        raise TMDBPlaylistNotFound(f"The TMDb list {lid} could not be found")
    missing = f"The TMDb list {own.name if own else lid} is private or no longer exists"
    pages = _pages(adapter, f"/list/{lid}", missing=missing)
    head = pages[0] if pages else {}
    resource = own or _public_resource(adapter, head, lid)
    if resource is None:
        raise TMDBPlaylistNotFound(missing)
    items: list[PlaylistItem] = []
    for data in pages:
        for row in data["results"]:
            media = _media_from_row(adapter, row)
            if not media:
                continue
            item = PlaylistItem.from_media(media, position=len(items), provider_media_id=f"{row.get('media_type')}:{row.get('id')}")
            if item.key:
                items.append(item)
    _memo_put(adapter, lid, stamp, items)
    _info("snapshot_done", list_id=lid, count=len(items))
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
    if len(parts) == 2 and parts[0].lower() == "list":
        head = parts[1].split("-", 1)[0]
        return head if head.isdigit() else ""
    return ""


def resolve(adapter: Any, ref: Any) -> PlaylistResource:
    lid = _link_id(ref)
    if not lid:
        raise PlaylistLinkError("unsupported TMDb list link")
    known = _find_resource(adapter, lid)
    if known is not None:
        return known
    data = _page(adapter, f"/list/{lid}", 1, missing=f"The TMDb list {lid} is private or no longer exists")
    res = _public_resource(adapter, data, lid)
    if res is None:
        raise TMDBPlaylistNotFound(f"The TMDb list {lid} is private or no longer exists")
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
        return PlaylistResource(provider="TMDB", id="", name=nm, instance=_instance_id(adapter), can_add=True, can_remove=True, media_types=MEDIA_TYPES)
    resp = adapter.client.v4("POST", "/list", json_body={"name": nm, "iso_639_1": "en", "public": 0})
    data = _json(resp)
    if not _ok(resp) or not isinstance(data, Mapping) or not data.get("id"):
        raise TMDBPlaylistError(_error_of(resp) if not _ok(resp) else "TMDb create list returned no id")
    resource = _resource(adapter, {"id": data.get("id"), "name": nm, "number_of_items": 0, "public": 0})
    if resource is None:
        raise TMDBPlaylistError("TMDb create list returned no id")
    remember_resource(list_resources, adapter, resource)
    if items:
        add(adapter, resource.id, items)
    _info("create_done", list_id=resource.id, name=nm)
    return resource


def _accepted(adapter: Any, items: Sequence[Mapping[str, Any]]) -> tuple[list[tuple[str, dict[str, Any]]], list[dict[str, Any]]]:
    accepted: list[tuple[str, dict[str, Any]]] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        media = id_minimal(raw)
        key = key_of(media)
        kind = pick_media_type(media)
        if not key:
            unresolved.append({"item": media, "hint": "missing_key"})
            continue
        if kind not in ("movie", "tv"):
            unresolved.append({"item": media, "key": key, "hint": "unsupported_type"})
            continue
        tmdb_id = resolve_tmdb_id(adapter, media, want=kind)
        if tmdb_id is None:
            unresolved.append({"item": media, "key": key, "hint": "missing_tmdb_id"})
            continue
        token = f"{kind}:{tmdb_id}"
        if token in seen:
            continue
        seen.add(token)
        accepted.append((key, {"media_type": kind, "media_id": int(tmdb_id)}))
    return accepted, unresolved


def _result_ok(results: Any, payload: Mapping[str, Any]) -> tuple[bool | None, str]:
    if not isinstance(results, list):
        return None, ""
    for row in results:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("media_type") or "") == payload["media_type"] and as_int(row.get("media_id")) == payload["media_id"]:
            err = row.get("error")
            if isinstance(err, list):
                err = "; ".join(str(e) for e in err)
            return bool(row.get("success")), str(err or "").strip()
    return None, ""


def _write_items(adapter: Any, method: str, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise TMDBPlaylistNotFound("missing TMDb list id")
    own = _find_resource(adapter, lid)
    if own is None or not (own.can_add or own.can_remove):
        raise PlaylistReadOnlyError(f"The TMDb list {lid} is read only")
    accepted, unresolved = _accepted(adapter, items)
    confirmed: list[str] = []
    unresolved_keys = [str(u["key"]) for u in unresolved if u.get("key")]
    ok = True
    if accepted:
        _memo_drop(adapter, lid)
    for start in range(0, len(accepted), BATCH_SIZE):
        batch = accepted[start:start + BATCH_SIZE]
        resp = adapter.client.v4(method, f"/list/{lid}/items", json_body={"items": [payload for _key, payload in batch]})
        if not _ok(resp):
            ok = False
            err = _error_of(resp)
            for key, _payload in batch:
                unresolved_keys.append(key)
                unresolved.append({"item": {"key": key}, "key": key, "hint": f"http:{int(resp.status_code)}", "error": err})
            _warn("write_items_failed", method=method, list_id=lid, status=int(resp.status_code), error=err)
            continue
        data = _json(resp)
        results = data.get("results") if isinstance(data, Mapping) else None
        for key, payload in batch:
            hit, err = _result_ok(results, payload)
            already = method == "POST" and hit is False and "already" in err.lower()
            if hit is True or already:
                confirmed.append(key)
                continue
            ok = False
            unresolved_keys.append(key)
            unresolved.append({"item": {"key": key}, "key": key, "hint": "rejected" if hit is False else "write_not_verified", "error": err})
    _info("write_items_done", method=method.lower(), list_id=lid, confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write_items(adapter, "POST", playlist_id, items)


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write_items(adapter, "DELETE", playlist_id, items)


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}
