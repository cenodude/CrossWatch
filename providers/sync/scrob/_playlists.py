# providers/sync/scrob/_playlists.py
# CrossWatch - Scrob lists as playlists: own lists, seasons and public lists on the same server
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import unquote, urlparse

from cw_platform.id_map import canonical_key, minimal as id_minimal
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

from ._common import (
    PATH_LIST,
    PATH_LIST_ITEM,
    PATH_LIST_ITEMS,
    PATH_LISTS,
    ScrobError,
    _info,
    _warn,
    cfg_section,
    mapping,
    positive_int,
    safe_json,
    scrob_request,
    year_of,
)
from ._watchlist import watchlist_name

FEATURE = "playlists"
MEDIA_TYPES = ("movie", "show", "season", "episode")


class ScrobPlaylistError(ScrobError):
    pass


class ScrobListUnavailable(ScrobPlaylistError, PlaylistUnavailableError):
    pass


class ScrobListsTokenRejected(ScrobPlaylistError, PlaylistAuthError):
    pass


def _instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def _resource(adapter: Any, row: Mapping[str, Any], *, public: bool = False) -> PlaylistResource | None:
    lid = positive_int(row.get("id"))
    if not lid:
        return None
    name = str(row.get("name") or "").strip() or str(lid)
    builtin = not public and name.lower() == watchlist_name(adapter).lower()
    extra: dict[str, Any] = {
        "item_count": positive_int(row.get("item_count")) or 0,
        "private": str(row.get("privacy_level") or "private").lower() != "public",
        "privacy": str(row.get("privacy_level") or "").lower(),
        "builtin": builtin,
        "can_rename": not public and not builtin,
        "can_delete": not public and not builtin,
    }
    if public:
        extra.update(endpoint_type="public_list", public_list=True)
    return PlaylistResource(
        provider="SCROB",
        id=str(lid),
        name=name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=not public,
        can_remove=not public,
        can_reorder=False,
        media_types=MEDIA_TYPES,
        extra=extra,
    )


@retained_read
def list_resources(adapter: Any) -> list[PlaylistResource]:
    resp = scrob_request(adapter, "GET", PATH_LISTS)
    if resp.status_code in (401, 403):
        raise ScrobListsTokenRejected("Scrob rejected the login. Reconnect Scrob")
    if not (200 <= resp.status_code < 300):
        _warn(FEATURE, "list_resources_failed", status=resp.status_code)
        raise ScrobPlaylistError(f"scrob_playlists_lists_http_{resp.status_code}")
    data = safe_json(resp)
    rows = data.get("lists") if isinstance(data, Mapping) else None
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise ScrobPlaylistError("scrob_playlists_lists_invalid_response")
    out = [res for res in (_resource(adapter, row) for row in rows) if res is not None]
    _info(FEATURE, "list_resources_done", count=len(out))
    return out


def _find_resource(adapter: Any, lid: str) -> PlaylistResource | None:
    for res in list_resources(adapter):
        if res.id == lid:
            return res
    return None


def _detail(adapter: Any, lid: str) -> Mapping[str, Any]:
    resp = scrob_request(adapter, "GET", PATH_LIST.format(list_id=lid))
    if resp.status_code in (403, 404):
        raise ScrobListUnavailable(f"The Scrob list {lid} is private or no longer exists")
    if resp.status_code == 401:
        raise ScrobListsTokenRejected("Scrob rejected the login. Reconnect Scrob")
    if not (200 <= resp.status_code < 300):
        raise ScrobPlaylistError(f"scrob_playlists_detail_http_{resp.status_code}")
    data = safe_json(resp)
    if not isinstance(data, Mapping) or not positive_int(data.get("id")):
        raise ScrobPlaylistError("scrob_playlists_detail_invalid_response")
    return data


def _ids(media: Mapping[str, Any], prefix: str = "") -> dict[str, str]:
    out: dict[str, str] = {}
    tmdb = positive_int(media.get(f"{prefix}tmdb_id"))
    tvdb = positive_int(media.get(f"{prefix}tvdb_id"))
    imdb = str(media.get(f"{prefix}imdb_id") or "").strip()
    if tmdb:
        out["tmdb"] = str(tmdb)
    if tvdb:
        out["tvdb"] = str(tvdb)
    if imdb.startswith("tt"):
        out["imdb"] = imdb
    return out


def _media(row: Mapping[str, Any]) -> dict[str, Any] | None:
    media = row.get("media")
    kind = str(media.get("type") or "").strip().lower() if isinstance(media, Mapping) else ""
    if not isinstance(media, Mapping) or not kind:
        raise ScrobPlaylistError("scrob_playlists_invalid_list_item")
    title = str(media.get("title") or "").strip()
    if kind == "movie":
        ids = _ids(media)
        if not ids:
            raise ScrobPlaylistError("scrob_playlists_invalid_list_item")
        out: dict[str, Any] = {"type": "movie", "ids": ids, "title": title}
        year = year_of(media)
        if year:
            out["year"] = year
        return id_minimal(out)
    if kind == "series":
        ids = _ids(media)
        if not ids:
            raise ScrobPlaylistError("scrob_playlists_invalid_list_item")
        season = media.get("season_number")
        if season is None:
            out = {"type": "show", "ids": ids, "title": title}
            year = year_of(media)
            if year:
                out["year"] = year
            return id_minimal(out)
        if not isinstance(season, int) or season < 0:
            raise ScrobPlaylistError("scrob_playlists_invalid_list_item")
        return id_minimal({"type": "season", "show_ids": ids, "season": season, "series_title": str(media.get("show_title") or title), "ids": {}})
    if kind == "episode":
        show_ids = _ids(media, "show_")
        season, episode = media.get("season_number"), media.get("episode_number")
        if not show_ids or not isinstance(season, int) or season < 0 or not isinstance(episode, int) or episode < 1:
            raise ScrobPlaylistError("scrob_playlists_invalid_list_item")
        return id_minimal({"type": "episode", "show_ids": show_ids, "season": season, "episode": episode, "series_title": str(media.get("show_title") or ""), "ids": _ids(media)})
    if kind == "person":
        return None
    raise ScrobPlaylistError("scrob_playlists_invalid_list_item")


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    lid = str(playlist_id or "").strip()
    if not lid.isdigit():
        raise ScrobListUnavailable(f"The Scrob list {lid} could not be found")
    own = _find_resource(adapter, lid)
    data = _detail(adapter, lid)
    resource = own or _resource(adapter, data, public=not data.get("is_owner"))
    if resource is None:
        raise ScrobListUnavailable(f"The Scrob list {lid} could not be found")
    rows = data.get("items")
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise ScrobPlaylistError("scrob_playlists_items_invalid_response")
    count = data.get("item_count")
    if isinstance(count, int) and not isinstance(count, bool) and count != len(rows):
        raise ScrobPlaylistError("scrob_playlists_items_incomplete")
    items: list[PlaylistItem] = []
    for row in rows:
        media = _media(row)
        if media is None:
            continue
        item = PlaylistItem.from_media(media, playlist_item_id=row.get("id"), position=len(items), provider_media_id=str(mapping(row.get("media")).get("id") or ""))
        if not item.key:
            raise ScrobPlaylistError("scrob_playlists_invalid_list_item")
        items.append(item)
    _info(FEATURE, "snapshot_done", list_id=lid, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _origin(url: str) -> tuple[str, str, int] | None:
    try:
        parsed = urlparse(url)
        scheme = (parsed.scheme or "").lower()
        host = (parsed.hostname or "").lower()
        port = parsed.port or {"http": 80, "https": 443}.get(scheme)
    except ValueError:
        return None
    if scheme not in ("http", "https") or not host or not port:
        return None
    return scheme, host, port


def _link_id(adapter: Any, ref: Any) -> str:
    raw = str(ref or "").strip()
    if raw.isdigit():
        return raw
    if "://" not in raw:
        return ""
    origin = _origin(raw)
    if origin is None or origin != _origin(str(cfg_section(adapter).get("server_url") or "")):
        return ""
    parts = [unquote(p) for p in urlparse(raw).path.split("/") if p]
    if len(parts) == 2 and parts[0].lower() in ("list", "lists") and parts[1].isdigit():
        return parts[1]
    return ""


def resolve(adapter: Any, ref: Any) -> PlaylistResource:
    lid = _link_id(adapter, ref)
    if not lid:
        raise PlaylistLinkError("unsupported Scrob list link")
    known = _find_resource(adapter, lid)
    if known is not None:
        return known
    data = _detail(adapter, lid)
    res = _resource(adapter, data, public=not data.get("is_owner"))
    if res is None:
        raise ScrobListUnavailable(f"The Scrob list {lid} could not be found")
    return res


def _writable(adapter: Any, playlist_id: Any) -> PlaylistResource:
    lid = str(playlist_id or "").strip()
    res = _find_resource(adapter, lid) if lid.isdigit() else None
    if res is None or not (res.can_add or res.can_remove):
        raise PlaylistReadOnlyError(f"The Scrob list {lid} is read only")
    return res


def _payload(item: Mapping[str, Any]) -> dict[str, Any] | None:
    kind = str(item.get("type") or "").strip().lower()
    if kind == "movie":
        ids = mapping(item.get("ids"))
        tmdb = positive_int(ids.get("tmdb"))
        return {"tmdb_id": tmdb, "media_type": "movie"} if tmdb else None
    if kind == "show":
        ids = mapping(item.get("ids"))
        tmdb, tvdb = positive_int(ids.get("tmdb")), positive_int(ids.get("tvdb"))
        if tmdb:
            return {"tmdb_id": tmdb, "media_type": "series"}
        return {"tvdb_id": tvdb, "media_type": "series"} if tvdb else None
    if kind == "season":
        show_ids = mapping(item.get("show_ids"))
        tmdb = positive_int(show_ids.get("tmdb"))
        season = item.get("season")
        if tmdb and isinstance(season, int) and season >= 0:
            return {"tmdb_id": tmdb, "media_type": "series", "season_number": season}
    return None


def _confirmed(body: Any, payload: Mapping[str, Any]) -> bool:
    media = mapping(body.get("media")) if isinstance(body, Mapping) else {}
    if str(media.get("type") or "").lower() != payload["media_type"]:
        return False
    if payload.get("tmdb_id") and positive_int(media.get("tmdb_id")) != payload["tmdb_id"]:
        return False
    if payload.get("tvdb_id") and positive_int(media.get("tvdb_id")) != payload["tvdb_id"]:
        return False
    return media.get("season_number") == payload.get("season_number")


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    res = _writable(adapter, playlist_id)
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
            kind = str(raw.get("type") or "").lower()
            unresolved.append({"item": id_minimal(raw), "key": key, "hint": "unsupported_type" if kind not in ("movie", "show", "season") else "missing_tmdb_id"})
            unresolved_keys.append(key)
            continue
        resp = scrob_request(adapter, "POST", PATH_LIST_ITEMS.format(list_id=res.id), json=payload)
        if resp.status_code == 409 or (resp.status_code in (200, 201) and _confirmed(safe_json(resp), payload)):
            confirmed.append(key)
            continue
        if resp.status_code == 401:
            raise ScrobListsTokenRejected("Scrob rejected the login. Reconnect Scrob")
        ok = False
        hint = "not_found" if resp.status_code == 404 else ("write_not_verified" if resp.status_code in (200, 201) else f"http:{resp.status_code}")
        unresolved.append({"item": {"key": key}, "key": key, "hint": hint})
        unresolved_keys.append(key)
        _warn(FEATURE, "write_failed", op="add", list_id=res.id, status=resp.status_code)
    _info(FEATURE, "write_done", op="add", list_id=res.id, confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    res = _writable(adapter, playlist_id)
    current = get_snapshot(adapter, res.id)
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
        resp = scrob_request(adapter, "DELETE", PATH_LIST_ITEM.format(list_id=res.id, item_id=item_id))
        if resp.status_code in (200, 204, 404):
            confirmed.append(key)
            continue
        if resp.status_code == 401:
            raise ScrobListsTokenRejected("Scrob rejected the login. Reconnect Scrob")
        ok = False
        unresolved.append({"item": {"key": key}, "key": key, "hint": f"http:{resp.status_code}"})
        unresolved_keys.append(key)
    _info(FEATURE, "write_done", op="remove", list_id=res.id, confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}


def _owned(adapter: Any, playlist_id: Any, field: str) -> PlaylistResource:
    lid = str(playlist_id or "").strip()
    res = _find_resource(adapter, lid) if lid.isdigit() else None
    if res is None or not res.extra.get(field):
        raise PlaylistReadOnlyError("The Scrob list cannot be changed")
    return res


def create(
    adapter: Any,
    name: str,
    *,
    media_type: str | None = None,
    items: Sequence[Mapping[str, Any]] | None = None,
    dry_run: bool = False,
) -> PlaylistResource:
    nm = str(name or "").strip()[:255]
    if not nm:
        raise ValueError("playlist name required")
    if dry_run:
        return PlaylistResource(provider="SCROB", id="", name=nm, instance=_instance_id(adapter), can_add=True, can_remove=True, media_types=MEDIA_TYPES)
    resp = scrob_request(adapter, "POST", PATH_LISTS, json={"name": nm, "privacy_level": "private"})
    data = safe_json(resp)
    if resp.status_code not in (200, 201) or not isinstance(data, Mapping):
        raise ScrobPlaylistError(f"scrob create list failed: http {resp.status_code}")
    res = _resource(adapter, data)
    if res is None:
        raise ScrobPlaylistError("scrob create list returned no id")
    remember_resource(list_resources, adapter, res)
    if items:
        add(adapter, res.id, items)
    return res


def rename(adapter: Any, playlist_id: Any, name: str, *, dry_run: bool = False) -> PlaylistResource:
    nm = str(name or "").strip()[:255]
    if not nm:
        raise ValueError("playlist name required")
    res = _owned(adapter, playlist_id, "can_rename")
    if not dry_run:
        resp = scrob_request(adapter, "PATCH", PATH_LIST.format(list_id=res.id), json={"name": nm})
        if not (200 <= resp.status_code < 300):
            raise ScrobPlaylistError(f"scrob rename list failed: http {resp.status_code}")
    return PlaylistResource(provider="SCROB", id=res.id, name=nm, instance=res.instance, can_add=res.can_add, can_remove=res.can_remove, media_types=res.media_types, extra=dict(res.extra))


def delete(adapter: Any, playlist_id: Any, *, dry_run: bool = False) -> dict[str, Any]:
    res = _owned(adapter, playlist_id, "can_delete")
    if dry_run:
        return {"ok": True, "dry_run": True}
    resp = scrob_request(adapter, "DELETE", PATH_LIST.format(list_id=res.id))
    if not (200 <= resp.status_code < 300):
        raise ScrobPlaylistError(f"scrob delete list failed: http {resp.status_code}")
    return {"ok": True, "count": 1}
