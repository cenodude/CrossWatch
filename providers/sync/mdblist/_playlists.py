# /providers/sync/mdblist/_playlists.py
# MDBList Module for playlist sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PLAYLIST_KIND_SMART,
    PlaylistItem,
    PlaylistResource,
    PlaylistSnapshot,
)

from .._log import log as cw_log
from . import _watchlist as feat_watchlist
from ._common import cfg_section, has_auth, mdblist_request
from ._watchlist import _as_int, _as_str, _parse_rows_and_total

BASE = "https://api.mdblist.com"
URL_USER_LISTS = f"{BASE}/lists/user"
URL_LIST_ITEMS = f"{BASE}/lists/{{id}}/items"
URL_LIST_ADD = f"{BASE}/lists/{{id}}/items/add"
URL_LIST_REMOVE = f"{BASE}/lists/{{id}}/items/remove"
URL_CREATE_LIST = f"{BASE}/lists/user/add"
URL_UPDATE_LIST = f"{BASE}/lists/{{id}}/update"
URL_USER = f"{BASE}/user"

_PROVIDER = "MDBLIST"
_FEATURE = "playlists"
WATCHLIST_ID = "__watchlist__"
_STATIC_MEDIA_TYPES = ("movie", "show", "season", "episode")
_WRITE_GROUPS = {"movie": "movies", "show": "shows", "season": "seasons", "episode": "episodes"}


def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)


class MDBListPlaylistError(RuntimeError):
    pass


class MDBListDynamicListError(MDBListPlaylistError):
    pass


class MDBListNotOwnedError(MDBListPlaylistError):
    pass


class MDBListNotFoundError(MDBListPlaylistError):
    pass


class MDBListCapacityError(MDBListPlaylistError):
    pass


def _instance_id(adapter: Any) -> str:
    inst = getattr(adapter, "instance_id", None)
    return str(inst).strip() if inst else "default"


def _is_watchlist_id(resource_or_id: Any) -> bool:
    raw = resource_or_id.id if isinstance(resource_or_id, PlaylistResource) else resource_or_id
    lid = str(raw or "").strip().lower()
    return lid in {WATCHLIST_ID, "watchlist", "mdblist:watchlist"}


def _watchlist_resource(adapter: Any) -> PlaylistResource:
    return PlaylistResource(
        provider=_PROVIDER,
        id=WATCHLIST_ID,
        name="Watchlist",
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=False,
        media_types=("movie", "show"),
        extra={"builtin": "watchlist"},
    )


def _apikey(adapter: Any) -> str:
    return _as_str(cfg_section(adapter).get("api_key")) or ""


def _norm_media_type(v: Any) -> str:
    t = str(v or "").strip().lower()
    if t in ("episode", "episodes"):
        return "episode"
    if t in ("season", "seasons"):
        return "season"
    if t in ("show", "shows", "tv", "series"):
        return "show"
    return "movie"


def _normalize_row(row: Mapping[str, Any]) -> dict[str, Any]:
    ids_src = row.get("ids")
    ids_block: Mapping[str, Any] = ids_src if isinstance(ids_src, Mapping) else {}
    ids: dict[str, Any] = {}
    imdb = _as_str(ids_block.get("imdb") or row.get("imdb_id") or row.get("imdb"))
    if imdb:
        ids["imdb"] = imdb
    tmdb = _as_int(ids_block.get("tmdb") or row.get("tmdb_id") or row.get("tmdb"))
    if tmdb is not None:
        ids["tmdb"] = str(tmdb)
    tvdb = _as_int(ids_block.get("tvdb") or row.get("tvdb_id") or row.get("tvdb"))
    if tvdb is not None:
        ids["tvdb"] = str(tvdb)
    trakt = _as_int(ids_block.get("trakt") or row.get("trakt_id") or row.get("trakt"))
    if trakt is not None:
        ids["trakt"] = str(trakt)
    mal = _as_int(ids_block.get("mal") or row.get("mal_id") or row.get("mal"))
    if mal is not None:
        ids["mal"] = str(mal)
    mdb = _as_str(ids_block.get("mdblist") or row.get("mdblist_id") or row.get("mdblist"))
    if mdb:
        ids["mdblist"] = mdb

    typ = _norm_media_type(row.get("mediatype") or row.get("type"))
    out: dict[str, Any] = {"type": typ, "ids": ids}
    title = _as_str(row.get("title") or row.get("name"))
    if title:
        out["title"] = title
    year = _as_int(row.get("release_year") or row.get("year") or row.get("first_air_year"))
    if year is not None:
        out["year"] = year
    return out


def _resource_from_list(adapter: Any, row: Mapping[str, Any]) -> PlaylistResource | None:
    lid = row.get("id")
    if lid is None:
        return None
    dynamic = bool(row.get("dynamic"))
    raw_mediatype = _as_str(row.get("mediatype") or row.get("type"))
    mediatype = _norm_media_type(raw_mediatype) if raw_mediatype else ""
    media_types = (mediatype,) if dynamic and mediatype else _STATIC_MEDIA_TYPES
    return PlaylistResource(
        provider=_PROVIDER,
        id=str(lid),
        name=str(row.get("name") or "").strip() or str(lid),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_SMART if dynamic else PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=not dynamic,
        can_remove=not dynamic,
        can_reorder=False,
        media_types=media_types,
        extra={
            "dynamic": dynamic,
            "static": not dynamic,
            "mediatype": mediatype,
            "item_count": _as_int(row.get("items")),
            "private": bool(row.get("private")),
            "slug": _as_str(row.get("slug")),
            "description": _as_str(row.get("description")),
            "owner": _as_str(row.get("user_name")),
            "updated_at": _as_str(row.get("updated") or row.get("last_updated")),
        },
    )


def list_resources(adapter: Any) -> list[PlaylistResource]:
    if not has_auth(cfg_section(adapter)):
        return []
    r = mdblist_request(adapter, "GET", URL_USER_LISTS, params={"apikey": _apikey(adapter)})
    if not (200 <= r.status_code < 300):
        _warn("http_failed", op="list_resources", status=r.status_code)
        return []
    data = r.json() if (r.text or "").strip() else []
    rows = data if isinstance(data, list) else (data.get("lists") if isinstance(data, Mapping) else [])
    out: list[PlaylistResource] = [_watchlist_resource(adapter)]
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        res = _resource_from_list(adapter, row)
        if res:
            out.append(res)
    _info("list_resources_done", count=len(out))
    return out


def _find_owned_resource(adapter: Any, playlist_id: Any) -> PlaylistResource | None:
    lid = str(playlist_id or "").strip()
    for res in list_resources(adapter):
        if res.id == lid:
            return res
    return None


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    if _is_watchlist_id(playlist_id):
        resource = _watchlist_resource(adapter)
        idx = feat_watchlist.build_index(adapter) or {}
        items = [PlaylistItem.from_media(m, position=i) for i, m in enumerate(idx.values()) if isinstance(m, Mapping)]
        _info("snapshot_done", list_id=WATCHLIST_ID, count=len(items))
        return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)

    lid = str(playlist_id or "").strip()
    resource = _find_owned_resource(adapter, lid)
    if resource is None:
        resource = PlaylistResource(
            provider=_PROVIDER,
            id=lid,
            name=lid,
            instance=_instance_id(adapter),
            can_read=True,
            can_add=False,
            can_remove=False,
            can_reorder=False,
        )

    items: list[PlaylistItem] = []
    offset = 0
    limit = 1000
    apikey = _apikey(adapter)
    pos = 0
    while offset <= 1_000_000:
        r = mdblist_request(
            adapter,
            "GET",
            URL_LIST_ITEMS.format(id=lid),
            params={"apikey": apikey, "limit": limit, "offset": offset, "unified": "true"},
        )
        if not (200 <= r.status_code < 300):
            _warn("http_failed", op="get_snapshot", status=r.status_code, list_id=lid)
            break
        data = r.json() if (r.text or "").strip() else {}
        rows, _total = _parse_rows_and_total(data)
        if not rows:
            break
        for row in rows:
            media = _normalize_row(row)
            rank = _as_int(row.get("rank"))
            items.append(
                PlaylistItem.from_media(
                    media,
                    playlist_item_id=row.get("id"),
                    position=rank if rank is not None else pos,
                    provider_media_id=row.get("id") or (media.get("ids") or {}).get("mdblist"),
                )
            )
            pos += 1
        has_more = None
        try:
            hdr = r.headers.get("X-Has-More")
            if hdr is not None:
                has_more = str(hdr).strip().lower() in ("1", "true", "yes")
        except Exception:
            has_more = None
        if has_more is None:
            pag = data.get("pagination") if isinstance(data, Mapping) else None
            has_more = bool(pag.get("has_more")) if isinstance(pag, Mapping) else (len(rows) >= limit)
        if not has_more:
            break
        offset += len(rows)

    items.sort(key=lambda it: (it.position is None, it.position if it.position is not None else 0))
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
            can_add=True,
            can_remove=True,
            can_reorder=False,
            media_types=_STATIC_MEDIA_TYPES,
        )

    body: dict[str, Any] = {"name": nm}
    r = mdblist_request(
        adapter,
        "POST",
        URL_CREATE_LIST,
        params={"apikey": _apikey(adapter)},
        json=body,
    )
    if r.status_code in (401, 403):
        raise MDBListPlaylistError("mdblist create list unauthorized")
    if not (200 <= r.status_code < 300):
        _warn("create_failed", status=r.status_code, body=((r.text or "")[:180]))
        raise MDBListPlaylistError(f"mdblist create list failed: http {r.status_code}")
    data = r.json() if (r.text or "").strip() else {}
    row: dict[str, Any] = {}
    if isinstance(data, Mapping):
        list_node = data.get("list")
        ids_node = data.get("ids")
        if isinstance(list_node, Mapping):
            row = dict(list_node)
        elif isinstance(ids_node, list) and ids_node:
            row = {"id": ids_node[0], "name": nm}
        else:
            row = dict(data)
    res = _resource_from_list(adapter, row) if row.get("id") is not None else None
    if res is None:
        raise MDBListPlaylistError("mdblist create list returned no id")
    _info("create_done", list_id=res.id, name=res.name)
    return res


def update_metadata(adapter: Any, playlist_id: Any, *, name: str | None = None, private: bool | None = None) -> dict[str, Any]:
    resource = _find_owned_resource(adapter, playlist_id)
    if resource is None:
        raise MDBListNotOwnedError("mdblist list is not owned by the authenticated user")
    if resource.is_smart:
        raise MDBListDynamicListError("cannot update a dynamic mdblist list")
    body: dict[str, Any] = {}
    if name is not None:
        body["name"] = str(name)
    if private is not None:
        body["private"] = bool(private)
    if not body:
        return {"ok": True, "updated": False}
    r = mdblist_request(
        adapter,
        "POST",
        URL_UPDATE_LIST.format(id=resource.id),
        params={"apikey": _apikey(adapter)},
        json=body,
    )
    if not (200 <= r.status_code < 300):
        _warn("update_failed", status=r.status_code, list_id=resource.id)
        raise MDBListPlaylistError(f"mdblist update list failed: http {r.status_code}")
    return {"ok": True, "updated": True}


def _guard_writable(resource: PlaylistResource | None) -> PlaylistResource:
    if resource is None:
        raise MDBListNotOwnedError("mdblist list is not owned by the authenticated user")
    if resource.is_smart:
        raise MDBListDynamicListError("cannot write to a dynamic mdblist list")
    if not (resource.can_add or resource.can_remove):
        raise MDBListPlaylistError("mdblist list is read only")
    return resource


def _accept_items(items: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for raw in items or []:
        m = id_minimal(raw)
        kind = _norm_media_type(m.get("type"))
        ids_raw = m.get("ids")
        ids_src: Mapping[str, Any] = ids_raw if isinstance(ids_raw, Mapping) else {}
        ids: dict[str, Any] = {}
        if kind in ("season", "episode"):
            tmdb = _as_int(ids_src.get("tmdb"))
            if tmdb is not None:
                ids["tmdb"] = tmdb
        else:
            imdb = _as_str(ids_src.get("imdb"))
            if imdb:
                ids["imdb"] = imdb
            for key in ("tmdb", "tvdb", "trakt"):
                v = _as_int(ids_src.get(key))
                if v is not None:
                    ids[key] = v
            mdb = _as_str(ids_src.get("mdblist"))
            if mdb:
                ids["mdblist"] = mdb
        if not ids:
            unresolved.append({"item": m, "hint": "missing_supported_ids"})
            continue
        accepted.append({"type": kind, "ids": ids})
        keys.append(canonical_key(m))
    return accepted, keys, unresolved


def _payload(accepted: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    buckets: dict[str, list[dict[str, Any]]] = {v: [] for v in _WRITE_GROUPS.values()}
    for x in accepted:
        ids = {k: v for k, v in dict(x.get("ids") or {}).items() if v not in (None, "")}
        if not ids:
            continue
        bucket = _WRITE_GROUPS.get(str(x.get("type") or "movie"))
        if bucket:
            buckets[bucket].append(ids)
    return {k: v for k, v in buckets.items() if v}


def _count_bucket(value: Any) -> int:
    if isinstance(value, list):
        return len(value)
    try:
        return int(value or 0)
    except Exception:
        return 0


def _confirmed_keys(items: Sequence[Mapping[str, Any]], unresolved: Sequence[Mapping[str, Any]]) -> list[str]:
    unresolved_keys: set[str] = set()
    for u in unresolved or []:
        obj = u.get("item") if isinstance(u, Mapping) else None
        if isinstance(obj, Mapping):
            try:
                unresolved_keys.add(canonical_key(id_minimal(obj)))
            except Exception:
                pass
    out: list[str] = []
    seen: set[str] = set()
    for it in items or []:
        try:
            k = canonical_key(id_minimal(it))
        except Exception:
            k = ""
        if not k or k in seen or k in unresolved_keys:
            continue
        seen.add(k)
        out.append(k)
    return out


def _write(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]], *, op: str) -> dict[str, Any]:
    if _is_watchlist_id(playlist_id):
        lst = list(items or [])
        count, unresolved = (feat_watchlist.add(adapter, lst) if op == "add" else feat_watchlist.remove(adapter, lst))
        return {"ok": True, "count": int(count or 0), "unresolved": unresolved, "confirmed_keys": _confirmed_keys(lst, unresolved)}

    resource = _guard_writable(_find_owned_resource(adapter, playlist_id))
    accepted, keys, unresolved = _accept_items(items)
    if not accepted:
        _info("write_skipped", op=op, reason="empty_payload", unresolved=len(unresolved))
        return {"ok": True, "count": 0, "unresolved": unresolved, "confirmed_keys": []}

    payload = _payload(accepted)
    if not payload:
        for m in accepted:
            unresolved.append({"item": id_minimal(m), "hint": "missing_write_ids"})
        return {"ok": True, "count": 0, "unresolved": unresolved, "confirmed_keys": []}

    url = URL_LIST_ADD if op == "add" else URL_LIST_REMOVE
    r = mdblist_request(
        adapter,
        "POST",
        url.format(id=resource.id),
        params={"apikey": _apikey(adapter)},
        json=payload,
    )
    if r.status_code == 429:
        _warn("capacity", op=op, status=429)
        for k in keys:
            unresolved.append({"item": {"key": k}, "hint": "rate_limited"})
        return {"ok": False, "count": 0, "unresolved": unresolved, "confirmed_keys": [], "capacity": "rate_limit"}
    if r.status_code in (401, 403):
        raise MDBListPlaylistError("mdblist write unauthorized or forbidden")
    if not (200 <= r.status_code < 300):
        _warn("write_failed", op=op, status=r.status_code, body=((r.text or "")[:180]))
        for k in keys:
            unresolved.append({"item": {"key": k}, "hint": f"http:{r.status_code}"})
        return {"ok": False, "count": 0, "unresolved": unresolved, "confirmed_keys": []}

    body = r.json() if (r.text or "").strip() else {}
    body = body if isinstance(body, Mapping) else {}
    bucket_key = "added" if op == "add" else "deleted"
    bucket = body.get(bucket_key) or body.get("removed") or {}
    applied = 0
    if isinstance(bucket, Mapping):
        applied = sum(_count_bucket(bucket.get(k)) for k in _WRITE_GROUPS.values())
    if applied == 0 and not body.get("not_found") and not body.get("existing"):
        applied = len(accepted)

    nf = body.get("not_found") if isinstance(body.get("not_found"), Mapping) else {}
    nf_keys: set[str] = set()
    for grp, typ in (("movies", "movie"), ("shows", "show"), ("seasons", "season"), ("episodes", "episode")):
        for obj in (nf.get(grp) or []) if isinstance(nf, Mapping) else []:
            if isinstance(obj, Mapping):
                ids = {k: v for k, v in obj.items() if k in ("imdb", "tmdb", "tvdb")}
                if ids:
                    nf_keys.add(canonical_key({"type": typ, "ids": ids}))
                    unresolved.append({"item": id_minimal({"type": typ, "ids": ids}), "hint": "not_found"})

    confirmed = [k for k in keys if k not in nf_keys]
    _info("write_done", op=op, list_id=resource.id, applied=applied, unresolved=len(unresolved))
    return {"ok": True, "count": applied, "unresolved": unresolved, "confirmed_keys": confirmed}


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, list(items or []), op="add")


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, list(items or []), op="remove")


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}


def account(adapter: Any) -> dict[str, Any]:
    if not has_auth(cfg_section(adapter)):
        return {}
    try:
        r = mdblist_request(adapter, "GET", URL_USER, params={"apikey": _apikey(adapter)})
        if 200 <= r.status_code < 300 and (r.text or "").strip():
            data = r.json()
            return dict(data) if isinstance(data, Mapping) else {}
    except Exception:
        return {}
    return {}
