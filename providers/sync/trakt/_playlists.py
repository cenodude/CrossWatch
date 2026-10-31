# /providers/sync/trakt/_playlists.py
# TRAKT Module for playlist sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from cw_platform.interactive_reads import remember_resource, retained_read, replaying

import copy
import time
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import quote, unquote, urlparse

from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PLAYLIST_KIND_SMART,
    PlaylistItem,
    PlaylistResource,
    PlaylistSnapshot,
    PlaylistLinkError,
    PlaylistReadOnlyError,
    PlaylistUnavailableError,
)
from cw_platform.run_control import SyncCancelled

from ._common import (
    build_watchlist_body,
    headers_for_adapter,
    key_of,
    normalize_watchlist_row,
    _chunk,
    _record_limit_error,
)
from . import _watchlist as feat_watchlist
from ._watchlist import _batch_payload, _record_not_found
from .._mod_common import request_with_retries
from .._log import log as cw_log
from ._pagination import TraktPager

BASE = "https://api.trakt.tv"
_PROVIDER = "TRAKT"
_FEATURE = "playlists"
_SUPPORTED_TYPES = ("movie", "show", "season", "episode")
WATCHLIST_ID = "__watchlist__"
DISCOVERY_PREFIX = "discovery:trakt:"
SMART_PREFIX = "smart:trakt:"
_DISCOVERY_LIMIT = 100
_TRAKT_HOSTS = {"trakt.tv", "www.trakt.tv", "app.trakt.tv"}
_SNAPSHOT_MEMO_TTL = 6 * 3600.0
_SNAPSHOT_MEMO_MAX = 16

_DISCOVERY_FEEDS: tuple[dict[str, Any], ...] = (
    {"id": "discovery:trakt:movies:trending", "name": "Trending Movies", "media_type": "movie", "path": "/movies/trending", "feed": "trending"},
    {"id": "discovery:trakt:movies:popular", "name": "Popular Movies", "media_type": "movie", "path": "/movies/popular", "feed": "popular"},
    {"id": "discovery:trakt:movies:anticipated", "name": "Anticipated Movies", "media_type": "movie", "path": "/movies/anticipated", "feed": "anticipated"},
    {"id": "discovery:trakt:movies:recommendations", "name": "Recommended Movies", "media_type": "movie", "path": "/recommendations/movies", "feed": "recommendations"},
    {"id": "discovery:trakt:shows:trending", "name": "Trending Shows", "media_type": "show", "path": "/shows/trending", "feed": "trending"},
    {"id": "discovery:trakt:shows:popular", "name": "Popular Shows", "media_type": "show", "path": "/shows/popular", "feed": "popular"},
    {"id": "discovery:trakt:shows:anticipated", "name": "Anticipated Shows", "media_type": "show", "path": "/shows/anticipated", "feed": "anticipated"},
    {"id": "discovery:trakt:shows:recommendations", "name": "Recommended Shows", "media_type": "show", "path": "/recommendations/shows", "feed": "recommendations"},
)

_SETTINGS_MEMO: tuple[float, dict[str, Any] | None] = (0.0, None)
_SNAPSHOT_MEMO: dict[tuple[str, str], tuple[float, tuple[str, Any], list[PlaylistItem]]] = {}


def _dbg(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)


class TraktCapacityError(RuntimeError):
    def __init__(self, message: str, *, scope: str, upgrade_url: str | None = None) -> None:
        super().__init__(message)
        self.scope = scope
        self.upgrade_url = upgrade_url


def _instance_id(adapter: Any) -> str:
    inst = getattr(adapter, "instance_id", None)
    return str(inst).strip() if inst else "default"


def _list_id(resource_or_id: Any) -> str:
    if isinstance(resource_or_id, PlaylistResource):
        return resource_or_id.id
    return str(resource_or_id or "").strip()


def _is_watchlist_id(resource_or_id: Any) -> bool:
    lid = _list_id(resource_or_id).strip().lower()
    return lid in {WATCHLIST_ID, "watchlist", "trakt:watchlist"}


def _discovery_feed(resource_or_id: Any) -> dict[str, Any] | None:
    lid = _list_id(resource_or_id).strip().lower()
    if not lid.startswith(DISCOVERY_PREFIX):
        return None
    for feed in _DISCOVERY_FEEDS:
        if str(feed["id"]).lower() == lid:
            return feed
    return None


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
        media_types=("movies", "shows", "seasons", "episodes"),
        extra={"builtin": "watchlist"},
    )


def _discovery_resource(adapter: Any, feed: Mapping[str, Any]) -> PlaylistResource:
    media_type = str(feed.get("media_type") or "").strip().lower()
    return PlaylistResource(
        provider=_PROVIDER,
        id=str(feed.get("id") or ""),
        name=str(feed.get("name") or feed.get("id") or "").strip(),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_SMART,
        can_read=True,
        can_add=False,
        can_remove=False,
        can_reorder=False,
        media_types=(media_type,) if media_type else (),
        extra={
            "endpoint_type": "discovery",
            "source_kind": "discovery",
            "discovery": True,
            "virtual": True,
            "provider_feed": str(feed.get("feed") or ""),
            "default_limit": _DISCOVERY_LIMIT,
        },
    )


def _discovery_resources(adapter: Any) -> list[PlaylistResource]:
    return [_discovery_resource(adapter, feed) for feed in _DISCOVERY_FEEDS]


def _settings_limits(adapter: Any) -> dict[str, Any]:
    global _SETTINGS_MEMO
    now = time.time()
    ts, cached = _SETTINGS_MEMO
    if cached is not None and (now - ts) < 300.0:
        return cached
    limits: dict[str, Any] = {}
    try:
        r = adapter.client.get(f"{BASE}/users/settings")
        if 200 <= r.status_code < 300 and (r.text or "").strip():
            data = r.json()
            raw = data.get("limits") if isinstance(data, Mapping) else None
            if isinstance(raw, Mapping):
                limits = dict(raw)
    except Exception:
        limits = {}
    _SETTINGS_MEMO = (now, limits)
    return limits


def _list_item_limit(adapter: Any) -> int | None:
    return _list_limit_field(adapter, "item_count")


def _list_count_limit(adapter: Any) -> int | None:
    return _list_limit_field(adapter, "count")


def _list_limit_field(adapter: Any, field: str) -> int | None:
    limits = _settings_limits(adapter)
    lst = limits.get("list") if isinstance(limits, Mapping) else None
    if isinstance(lst, Mapping):
        raw = lst.get(field)
        if raw is None:
            return None
        try:
            v = int(raw)
            return v if v > 0 else None
        except Exception:
            return None
    return None


def _list_stamp_extra(row: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    updated = str(row.get("updated_at") or "").strip()
    if updated:
        out["updated_at"] = updated
    count = row.get("item_count")
    if isinstance(count, int) and not isinstance(count, bool):
        out["item_count"] = count
    return out


def _row_user(row: Mapping[str, Any]) -> tuple[str, str]:
    raw_user = row.get("user")
    user: Mapping[str, Any] = raw_user if isinstance(raw_user, Mapping) else {}
    raw_ids = user.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    name = str(user.get("username") or user.get("name") or "").strip()
    slug = str(ids.get("slug") or user.get("username") or "").strip()
    return name, slug


def _row_list_id(row: Mapping[str, Any]) -> str:
    return str(dict(row.get("ids") or {}).get("trakt") or "").strip()


def _owned_name(row: Mapping[str, Any], fallback: str, owner: str) -> str:
    name = str(row.get("name") or "").strip() or fallback
    return f"{name} by {owner}" if owner else name


def _resource_from_list(adapter: Any, row: Mapping[str, Any]) -> PlaylistResource | None:
    ids = dict(row.get("ids") or {})
    lid = ids.get("trakt")
    if lid is None:
        lid = ids.get("slug")
    lid = str(lid or "").strip()
    if not lid:
        return None
    return PlaylistResource(
        provider=_PROVIDER,
        id=lid,
        name=str(row.get("name") or "").strip() or lid,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=True,
        media_types=("movies", "shows", "seasons", "episodes"),
        extra=_list_stamp_extra(row),
    )


def _public_resource(adapter: Any, row: Any, endpoint_type: str) -> PlaylistResource | None:
    if not isinstance(row, Mapping):
        return None
    lid = _row_list_id(row)
    if not lid:
        return None
    owner, _slug = _row_user(row)
    return PlaylistResource(
        provider=_PROVIDER,
        id=lid,
        name=_owned_name(row, lid, owner),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        media_types=("movies", "shows", "seasons", "episodes"),
        extra={
            "endpoint_type": endpoint_type,
            "public_list": True,
            "owner": owner,
            "can_rename": False,
            "can_delete": False,
            **_list_stamp_extra(row),
        },
    )


def _collab_resource(adapter: Any, row: Any) -> PlaylistResource | None:
    if not isinstance(row, Mapping):
        return None
    lid = _row_list_id(row)
    owner, slug = _row_user(row)
    if not lid or not slug:
        return None
    return PlaylistResource(
        provider=_PROVIDER,
        id=lid,
        name=_owned_name(row, lid, owner),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=True,
        media_types=("movies", "shows", "seasons", "episodes"),
        extra={
            "endpoint_type": "collaboration",
            "owner": owner,
            "owner_slug": slug,
            "can_rename": False,
            "can_delete": False,
            **_list_stamp_extra(row),
        },
    )


def _smart_resource(adapter: Any, row: Any, *, shared: bool) -> PlaylistResource | None:
    if not isinstance(row, Mapping):
        return None
    slug = str(dict(row.get("ids") or {}).get("slug") or "").strip()
    if not slug:
        return None
    owner, _owner_slug = _row_user(row)
    media = str(row.get("media_type") or "").strip().lower()
    media_types = ("movies",) if media == "movies" else ("shows",) if media == "shows" else ("movies", "shows")
    return PlaylistResource(
        provider=_PROVIDER,
        id=f"{SMART_PREFIX}{slug}",
        name=_owned_name(row, slug, owner if shared else ""),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_SMART,
        can_read=True,
        media_types=media_types,
        extra={
            "endpoint_type": "smart",
            "source_kind": "smart",
            "smart_slug": slug,
            "owner": owner,
            "can_rename": False,
            "can_delete": False,
        },
    )


def _get(adapter: Any, path: str, params: Mapping[str, Any] | None = None) -> Any:
    return request_with_retries(
        adapter.client.session,
        "GET",
        f"{BASE}{path}",
        headers=headers_for_adapter(adapter),
        params=dict(params or {}),
        timeout=adapter.cfg.timeout,
        max_retries=adapter.cfg.max_retries,
    )


def _last_page(response: Any, page: int) -> bool:
    headers = {str(k).lower(): v for k, v in (getattr(response, "headers", None) or {}).items()}
    try:
        return page >= int(headers.get("x-pagination-page-count") or 0) > 0
    except (TypeError, ValueError):
        return False


def _optional_rows(adapter: Any, path: str, op: str) -> list[Any]:
    try:
        r = _get(adapter, path)
        if r.status_code != 200:
            _warn("http_failed", op=op, status=r.status_code)
            return []
        data = r.json() if (r.text or "").strip() else []
    except SyncCancelled:
        raise
    except Exception as e:
        _warn("http_failed", op=op, error=str(e))
        return []
    return list(data) if isinstance(data, list) else []


def _liked_resources(adapter: Any, skip_ids: set[str]) -> list[PlaylistResource]:
    out: list[PlaylistResource] = []
    seen = set(skip_ids)
    page = 1
    pager = TraktPager("playlists", 50)
    try:
        while True:
            r = _get(adapter, "/users/me/likes/lists", {"page": page, "limit": 100})
            rows = pager.read(r, page)
            if not rows:
                break
            for row in rows:
                res = _public_resource(adapter, row.get("list") if isinstance(row, Mapping) else None, "liked_list")
                if res and res.id not in seen:
                    seen.add(res.id)
                    out.append(res)
            if _last_page(r, page):
                break
            page += 1
    except SyncCancelled:
        raise
    except Exception as e:
        _warn("liked_lists_failed", error=str(e))
        return []
    return out


def _public_list_resource(adapter: Any, lid: str, *, strict: bool = False) -> PlaylistResource:
    status = 0
    try:
        r = _get(adapter, f"/lists/{quote(lid, safe='')}")
        status = r.status_code
        if status == 200 and (r.text or "").strip():
            res = _public_resource(adapter, r.json(), "public_list")
            if res:
                return res
    except SyncCancelled:
        raise
    except Exception as e:
        if strict:
            raise
        _warn("public_list_lookup_failed", list_id=lid, error=str(e))
    if strict:
        raise PlaylistUnavailableError(f"The Trakt list {lid} is private or no longer exists")
    _dbg("public_list_lookup_missed", list_id=lid, status=status)
    return PlaylistResource(
        provider=_PROVIDER,
        id=lid,
        name=lid,
        instance=_instance_id(adapter),
        can_read=True,
        media_types=("movies", "shows", "seasons", "episodes"),
        extra={"unresolved": True, "can_rename": False, "can_delete": False},
    )


def _smart_public_resource(adapter: Any, slug: str, *, strict: bool = False) -> PlaylistResource:
    try:
        r = _get(adapter, f"/smart-lists/{quote(slug, safe='')}")
        if r.status_code == 200 and (r.text or "").strip():
            res = _smart_resource(adapter, r.json(), shared=True)
            if res:
                return res
    except SyncCancelled:
        raise
    except Exception as e:
        if strict:
            raise
        _warn("smart_list_lookup_failed", slug=slug, error=str(e))
    if strict:
        raise PlaylistUnavailableError(f"The Trakt smart list {slug} is private or no longer exists")
    return PlaylistResource(
        provider=_PROVIDER,
        id=f"{SMART_PREFIX}{slug}",
        name=slug,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_SMART,
        can_read=True,
        media_types=("movies", "shows"),
        extra={"endpoint_type": "smart", "source_kind": "smart", "smart_slug": slug, "can_rename": False, "can_delete": False},
    )


@retained_read
def list_resources(adapter: Any) -> list[PlaylistResource]:
    out: list[PlaylistResource] = [_watchlist_resource(adapter), *_discovery_resources(adapter)]
    r = _get(adapter, "/users/me/lists")
    known: set[str] = set()
    if r.status_code != 200:
        _warn("http_failed", op="list_resources", status=r.status_code)
        raise RuntimeError(f"trakt list discovery failed: http {r.status_code}")
    data = r.json() if (r.text or "").strip() else []
    if not isinstance(data, list):
        raise RuntimeError("trakt list discovery returned an invalid response")
    for row in data:
        if not isinstance(row, Mapping):
            continue
        res = _resource_from_list(adapter, row)
        if res:
            known.add(res.id)
            out.append(res)
    for row in _optional_rows(adapter, "/users/me/lists/collaborations", "collaborations"):
        res = _collab_resource(adapter, row)
        if res and res.id not in known:
            known.add(res.id)
            out.append(res)
    for row in _optional_rows(adapter, "/users/me/smart-lists", "smart_lists"):
        res = _smart_resource(adapter, row, shared=False)
        if res:
            out.append(res)
    out.extend(_liked_resources(adapter, known))
    _info("list_resources_done", count=len(out))
    return out


def _discovery_row(row: Mapping[str, Any], media_type: str) -> dict[str, Any] | None:
    kind = "show" if media_type == "show" else "movie"
    if kind in row:
        wrapped = dict(row)
        wrapped.setdefault("type", kind)
    else:
        wrapped = {"type": kind, kind: row}
    media = normalize_watchlist_row(wrapped)
    ids = media.get("ids") if isinstance(media, Mapping) else None
    if not isinstance(ids, Mapping) or not any(str(v or "").strip() for v in ids.values()):
        return None
    return media


def _discovery_snapshot(adapter: Any, feed: Mapping[str, Any]) -> PlaylistSnapshot:
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    resource = _discovery_resource(adapter, feed)
    media_type = str(feed.get("media_type") or "").strip().lower()
    path = str(feed.get("path") or "").strip()
    items: list[PlaylistItem] = []
    page = 1
    per_page = _DISCOVERY_LIMIT
    pager = TraktPager("playlists", 10)
    while len(items) < _DISCOVERY_LIMIT:
        r = request_with_retries(
            sess,
            "GET",
            f"{BASE}{path}",
            headers=headers,
            params={"page": page, "limit": per_page, "extended": "full"},
            timeout=adapter.cfg.timeout,
            max_retries=adapter.cfg.max_retries,
        )
        rows = pager.read(r, page)
        if not rows:
            break
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            media = _discovery_row(row, media_type)
            if not media:
                continue
            pos = row.get("rank") or len(items)
            items.append(
                PlaylistItem.from_media(
                    media,
                    playlist_item_id=None,
                    position=pos,
                    provider_media_id=(dict(media.get("ids") or {}).get("trakt")),
                )
            )
            if len(items) >= _DISCOVERY_LIMIT:
                break
        page += 1
    _info("snapshot_done", list_id=resource.id, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _find_resource(adapter: Any, lid: str) -> PlaylistResource | None:
    resources = list_resources(adapter)
    for res in resources:
        if res.id == lid:
            return res
    if lid.isdigit():
        return None
    for res in resources:
        if res.name == lid:
            return res
    return None


def _is_own_list(res: PlaylistResource) -> bool:
    return not res.is_smart and not res.extra.get("builtin") and not res.is_discovery and not res.extra.get("endpoint_type")


def _list_base(resource: PlaylistResource | None, lid: str) -> str:
    extra = resource.extra if resource is not None else {}
    owner = str(extra.get("owner_slug") or "").strip()
    if extra.get("endpoint_type") == "collaboration" and owner:
        return f"{BASE}/users/{quote(owner, safe='')}/lists/{lid}"
    return f"{BASE}/users/me/lists/{lid}"


def _items_url(resource: PlaylistResource, lid: str) -> str:
    if resource.is_smart:
        return f"{BASE}/smart-lists/{quote(str(resource.extra.get('smart_slug') or ''), safe='')}/items"
    if resource.extra.get("public_list"):
        return f"{BASE}/lists/{quote(lid, safe='')}/items"
    return f"{_list_base(resource, lid)}/items"


def _write_target(adapter: Any, playlist_id: Any) -> tuple[str, str]:
    lid = _list_id(playlist_id)
    if lid.lower().startswith(SMART_PREFIX):
        raise PlaylistReadOnlyError("Trakt smart lists are read only")
    res = _find_resource(adapter, lid)
    if res is None:
        raise RuntimeError(f"trakt list {lid} not found among writable lists")
    if res.is_smart or res.extra.get("public_list") or not (res.can_add or res.can_remove):
        raise PlaylistReadOnlyError(f"The Trakt list {res.name or res.id} is read only")
    return res.id, _list_base(res, res.id)


def _unavailable(resource: PlaylistResource) -> PlaylistUnavailableError:
    label = resource.name or resource.id
    if resource.extra.get("endpoint_type") == "collaboration":
        return PlaylistUnavailableError(f"You no longer have access to the Trakt list {label}")
    kind = "smart list" if resource.is_smart else "list"
    if resource.extra.get("unresolved"):
        return PlaylistUnavailableError(f"The Trakt list {label} could not be read. It may be private, deleted or no longer shared with you")
    if resource.is_smart or resource.extra.get("public_list"):
        return PlaylistUnavailableError(f"The Trakt {kind} {label} is private or no longer exists")
    return PlaylistUnavailableError(f"The Trakt {kind} {label} no longer exists")


def _snapshot_stamp(resource: PlaylistResource) -> tuple[str, Any] | None:
    if resource.is_smart:
        return None
    updated = str(resource.extra.get("updated_at") or "").strip()
    if not updated:
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


@retained_read
def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    if _is_watchlist_id(playlist_id):
        resource = _watchlist_resource(adapter)
        idx = feat_watchlist.build_index(adapter) or {}
        items = [PlaylistItem.from_media(m, position=i) for i, m in enumerate(idx.values()) if isinstance(m, Mapping)]
        _info("snapshot_done", list_id=WATCHLIST_ID, count=len(items))
        return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)

    discovery = _discovery_feed(playlist_id)
    if discovery is not None:
        return _discovery_snapshot(adapter, discovery)

    lid = _list_id(playlist_id)
    resource = _find_resource(adapter, lid)
    if resource is None:
        if lid.lower().startswith(SMART_PREFIX):
            resource = _smart_public_resource(adapter, lid[len(SMART_PREFIX):])
        else:
            resource = _public_list_resource(adapter, lid)
    lid = resource.id

    stamp = _snapshot_stamp(resource)
    cached = _memo_get(adapter, lid, stamp)
    if cached is not None:
        _info("snapshot_unchanged", list_id=lid, count=len(cached))
        return PlaylistSnapshot(resource=resource, items=cached, checkpoint=None)

    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    items_url = _items_url(resource, lid)
    items: list[PlaylistItem] = []
    page = 1
    per_page = 100 if resource.is_smart else 250
    pager = TraktPager("playlists", 1000)
    while True:
        r = request_with_retries(
            sess,
            "GET",
            items_url,
            headers=headers,
            params={"page": page, "limit": per_page, "extended": "full"},
            timeout=adapter.cfg.timeout,
            max_retries=adapter.cfg.max_retries,
        )
        if page == 1 and r.status_code in (403, 404):
            _warn("list_unavailable", list_id=lid, status=r.status_code)
            raise _unavailable(resource)
        rows = pager.read(r, page)
        if not rows:
            break
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            typ = str(row.get("type") or "").lower()
            if typ not in _SUPPORTED_TYPES:
                continue
            media = normalize_watchlist_row(row)
            items.append(
                PlaylistItem.from_media(
                    media,
                    playlist_item_id=row.get("id"),
                    position=row.get("rank"),
                    provider_media_id=(dict(media.get("ids") or {}).get("trakt")),
                )
            )
        if _last_page(r, page):
            break
        page += 1

    items.sort(key=lambda it: (it.position is None, it.position if it.position is not None else 0))
    _memo_put(adapter, lid, stamp, items)
    _info("snapshot_done", list_id=lid, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _link_parts(ref: Any) -> list[str]:
    raw = str(ref or "").strip()
    if not raw:
        return []
    if "://" not in raw and raw.split("/", 1)[0].lower() in _TRAKT_HOSTS:
        raw = f"https://{raw}"
    if "://" in raw:
        parsed = urlparse(raw)
        if (parsed.hostname or "").lower() not in _TRAKT_HOSTS:
            return []
        path = parsed.path
    else:
        path = raw.split("?", 1)[0]
    return [unquote(p) for p in path.split("/") if p]


def _resolve_user_list(adapter: Any, user: str, slug: str) -> PlaylistResource:
    r = _get(adapter, f"/users/{quote(user, safe='')}/lists/{quote(slug, safe='')}")
    if r.status_code != 200 or not (r.text or "").strip():
        raise PlaylistUnavailableError(f"The Trakt list {slug} is private or no longer exists")
    row = r.json()
    lid = _row_list_id(row) if isinstance(row, Mapping) else ""
    if not lid:
        raise PlaylistUnavailableError(f"The Trakt list {slug} is private or no longer exists")
    known = _find_resource(adapter, lid)
    if known is not None:
        return known
    if str(row.get("privacy") or "public").strip().lower() != "public":
        raise PlaylistUnavailableError(f"The Trakt list {row.get('name') or slug} is not public")
    res = _public_resource(adapter, row, "public_list")
    if res is None:
        raise PlaylistUnavailableError(f"The Trakt list {slug} is private or no longer exists")
    return res


def resolve(adapter: Any, ref: Any) -> PlaylistResource:
    raw = str(ref or "").strip()
    if raw.lower().startswith(SMART_PREFIX):
        return _find_resource(adapter, raw) or _smart_public_resource(adapter, raw[len(SMART_PREFIX):], strict=True)
    parts = _link_parts(raw)
    if len(parts) == 1 and parts[0].isdigit():
        return _find_resource(adapter, parts[0]) or _public_list_resource(adapter, parts[0], strict=True)
    if len(parts) >= 2 and parts[0] == "lists" and parts[1].isdigit():
        return _find_resource(adapter, parts[1]) or _public_list_resource(adapter, parts[1], strict=True)
    if len(parts) >= 4 and parts[0] == "users" and parts[2] == "lists":
        return _resolve_user_list(adapter, parts[1], parts[3])
    smart_slug = ""
    if len(parts) >= 4 and parts[0] == "users" and parts[2] == "smart-lists":
        smart_slug = parts[3]
    elif len(parts) >= 2 and parts[0] == "smart-lists":
        smart_slug = parts[1]
    if smart_slug:
        sid = f"{SMART_PREFIX}{smart_slug}"
        return _find_resource(adapter, sid) or _smart_public_resource(adapter, smart_slug, strict=True)
    raise PlaylistLinkError("unsupported Trakt list link")


def create(adapter: Any, name: str, *, media_type: str | None = None, dry_run: bool = False) -> PlaylistResource:
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
            can_reorder=True,
            media_types=("movies", "shows", "seasons", "episodes"),
        )

    limit = _list_count_limit(adapter)
    if limit is not None:
        existing = len([res for res in list_resources(adapter) if _is_own_list(res)])
        if existing >= limit:
            _record_limit_error("playlists")
            raise TraktCapacityError("trakt list count limit reached", scope="list_count")

    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    r = request_with_retries(
        sess,
        "POST",
        f"{BASE}/users/me/lists",
        headers=headers,
        json={"name": nm, "privacy": "private"},
        timeout=adapter.cfg.timeout,
        max_retries=adapter.cfg.max_retries,
    )
    if r.status_code == 420:
        _record_limit_error("playlists")
        raise TraktCapacityError(
            "trakt list count limit reached",
            scope="list_count",
            upgrade_url=r.headers.get("X-Upgrade-URL"),
        )
    if r.status_code not in (200, 201):
        _warn("write_failed", op="create", status=r.status_code, body=((r.text or "")[:180]))
        raise RuntimeError(f"trakt create list failed: http {r.status_code}")
    data = r.json() if (r.text or "").strip() else {}
    res = _resource_from_list(adapter, data if isinstance(data, Mapping) else {})
    if res is None:
        raise RuntimeError("trakt create list returned no id")
    _info("create_done", list_id=res.id, name=res.name)
    remember_resource(list_resources, adapter, res)
    return res


def rename(adapter: Any, playlist_id: Any, name: str, *, dry_run: bool = False) -> PlaylistResource:
    if _discovery_feed(playlist_id) is not None or _is_watchlist_id(playlist_id):
        raise RuntimeError("trakt built-in and discovery feeds cannot be renamed")
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    lid = _list_id(playlist_id)
    if dry_run:
        return PlaylistResource(
            provider=_PROVIDER,
            id=lid,
            name=nm,
            instance=_instance_id(adapter),
            can_add=True,
            can_remove=True,
            can_reorder=True,
            media_types=("movies", "shows", "seasons", "episodes"),
        )
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    r = request_with_retries(
        sess,
        "PUT",
        f"{BASE}/users/me/lists/{lid}",
        headers=headers,
        json={"name": nm},
        timeout=adapter.cfg.timeout,
        max_retries=adapter.cfg.max_retries,
    )
    if r.status_code not in (200, 201):
        _warn("write_failed", op="rename", status=r.status_code, body=((r.text or "")[:180]))
        raise RuntimeError(f"trakt rename list failed: http {r.status_code}")
    data = r.json() if (r.text or "").strip() else {}
    res = _resource_from_list(adapter, data if isinstance(data, Mapping) else {})
    if res is None:
        res = PlaylistResource(
            provider=_PROVIDER,
            id=lid,
            name=nm,
            instance=_instance_id(adapter),
            can_add=True,
            can_remove=True,
            can_reorder=True,
            media_types=("movies", "shows", "seasons", "episodes"),
        )
    _info("rename_done", list_id=res.id, name=res.name)
    return res


def delete(adapter: Any, playlist_id: Any, *, dry_run: bool = False) -> dict[str, Any]:
    if _discovery_feed(playlist_id) is not None or _is_watchlist_id(playlist_id):
        raise RuntimeError("trakt built-in and discovery feeds cannot be deleted")
    lid = _list_id(playlist_id)
    if dry_run:
        return {"ok": True, "dry_run": True}
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    r = request_with_retries(
        sess,
        "DELETE",
        f"{BASE}/users/me/lists/{lid}",
        headers=headers,
        timeout=adapter.cfg.timeout,
        max_retries=adapter.cfg.max_retries,
    )
    if r.status_code not in (200, 204):
        _warn("write_failed", op="delete", status=r.status_code, body=((r.text or "")[:180]))
        raise RuntimeError(f"trakt delete list failed: http {r.status_code}")
    _info("delete_done", list_id=lid)
    return {"ok": True, "count": 1}


def _confirmed_keys(items: Iterable[Mapping[str, Any]], unresolved: list[dict[str, Any]]) -> list[str]:
    ukeys: set[str] = set()
    for u in unresolved or []:
        obj = u.get("item") if isinstance(u, Mapping) else u
        if isinstance(obj, Mapping):
            try:
                ukeys.add(key_of(obj))
            except Exception:
                pass
    out: list[str] = []
    seen: set[str] = set()
    for it in items or []:
        try:
            k = key_of(it)
        except Exception:
            k = ""
        if not k or k in seen or k in ukeys:
            continue
        seen.add(k)
        out.append(k)
    return out


def _write(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]], *, op: str) -> dict[str, Any]:
    if _discovery_feed(playlist_id) is not None:
        raise RuntimeError("trakt discovery feeds are read only")
    if _is_watchlist_id(playlist_id):
        lst = list(items or [])
        count, unresolved = (feat_watchlist.add(adapter, lst) if op == "add" else feat_watchlist.remove(adapter, lst))
        confirmed = _confirmed_keys(lst, unresolved)
        return {"ok": True, "count": int(count or 0), "unresolved": unresolved, "confirmed_keys": confirmed}

    lid, list_base = _write_target(adapter, playlist_id)
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    batch = 100

    accepted, unresolved = _batch_payload(items)
    if not accepted:
        _info("write_skipped", op=op, reason="empty_payload", unresolved=len(unresolved))
        return {"ok": True, "count": 0, "unresolved": unresolved, "confirmed_keys": []}

    if op == "add":
        limit = _list_item_limit(adapter)
        if limit is not None:
            try:
                current = len(get_snapshot(adapter, lid).items)
            except Exception:
                current = 0
            capacity = max(0, limit - current)
            if capacity <= 0:
                for x in accepted:
                    unresolved.append({"item": x, "hint": "trakt_limit"})
                _record_limit_error("playlists")
                _warn("capacity", op=op, reason="list_item_limit", limit=limit, have=current)
                return {"ok": False, "count": 0, "unresolved": unresolved, "confirmed_keys": [], "capacity": "list_item_count"}
            if capacity < len(accepted):
                overflow = accepted[capacity:]
                accepted = accepted[:capacity]
                for x in overflow:
                    unresolved.append({"item": x, "hint": "trakt_limit"})

    url = f"{list_base}/items"
    if op == "remove":
        url = f"{list_base}/items/remove"

    _memo_drop(adapter, lid)
    ok = 0
    capacity_hit = False
    for sl in _chunk(accepted, batch):
        payload = build_watchlist_body(sl)
        if not payload:
            continue
        r = request_with_retries(
            sess,
            "POST",
            url,
            headers=headers,
            json=payload,
            timeout=adapter.cfg.timeout,
            max_retries=adapter.cfg.max_retries,
        )
        if r.status_code in (200, 201):
            d = r.json() if (r.text or "").strip() else {}
            bucket = d.get("added") if op == "add" else (d.get("deleted") or d.get("removed"))
            bucket = bucket or {}
            ok += sum(int(bucket.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
            _record_not_found(d.get("not_found") or {}, action=op, unresolved=unresolved)
        elif r.status_code == 420:
            capacity_hit = True
            _record_limit_error("playlists")
            _warn("capacity", op=op, status=420, upgrade_url=r.headers.get("X-Upgrade-URL"))
            for x in sl:
                unresolved.append({"item": x, "hint": "trakt_limit"})
            break
        else:
            _warn("write_failed", op=op, status=r.status_code, body=((r.text or "")[:180]))
            for x in sl:
                unresolved.append({"item": x, "hint": f"http:{r.status_code}"})

    confirmed = _confirmed_keys(accepted, unresolved)
    out: dict[str, Any] = {
        "ok": len([u for u in unresolved if u.get("hint") == "trakt_limit"]) == 0 or ok > 0,
        "count": ok,
        "unresolved": unresolved,
        "confirmed_keys": confirmed,
    }
    if capacity_hit:
        out["capacity"] = "list_item_count"
    _info("write_done", op=op, list_id=lid, applied=ok, unresolved=len(unresolved))
    return out


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, list(items or []), op="add")


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, list(items or []), op="remove")


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    if _discovery_feed(playlist_id) is not None:
        return {"ok": False, "count": 0, "reordered": 0, "unsupported": True, "error": "read_only"}
    if _is_watchlist_id(playlist_id):
        return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}

    lid, list_base = _write_target(adapter, playlist_id)
    try:
        snap = get_snapshot(adapter, lid)
    except RuntimeError:
        if replaying():
            return {"ok": False, "count": 0, "error": "playlist_item_ids_unavailable"}
        raise
    key_to_item_id: dict[str, Any] = {}
    for it in snap.items:
        if it.key and it.playlist_item_id and it.key not in key_to_item_id:
            key_to_item_id[it.key] = it.playlist_item_id
    for key, item_id in list(key_to_item_id.items()):
        if key.startswith("tmdb:") and key.endswith("#show"):
            key_to_item_id.setdefault(key.removesuffix("#show"), item_id)

    rank: list[Any] = []
    seen_ids: set[str] = set()
    if replaying() and any(str(key) not in key_to_item_id for key in ordered_keys):
        return {"ok": False, "count": 0, "error": "playlist_item_ids_unavailable"}
    seen: set[str] = set()
    for k in ordered_keys or []:
        ks = str(k or "").strip()
        if not ks or ks in seen:
            continue
        seen.add(ks)
        item_id = key_to_item_id.get(ks)
        if item_id is not None and str(item_id) not in seen_ids:
            rank.append(item_id)
            seen_ids.add(str(item_id))
    for it in snap.items:
        if it.key in seen:
            continue
        if it.playlist_item_id is not None and str(it.playlist_item_id) not in seen_ids:
            rank.append(it.playlist_item_id)
            seen_ids.add(str(it.playlist_item_id))
            seen.add(it.key)

    if not rank:
        return {"ok": True, "count": 0, "reordered": 0}

    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    _memo_drop(adapter, lid)
    r = request_with_retries(
        sess,
        "POST",
        f"{list_base}/items/reorder",
        headers=headers,
        json={"rank": rank},
        timeout=adapter.cfg.timeout,
        max_retries=adapter.cfg.max_retries,
    )
    if r.status_code not in (200, 201):
        _warn("write_failed", op="reorder", status=r.status_code, body=((r.text or "")[:180]))
        return {"ok": False, "count": 0, "reordered": 0, "error": f"http:{r.status_code}"}
    d = r.json() if (r.text or "").strip() else {}
    updated = int(d.get("updated") or 0) if isinstance(d, Mapping) else len(rank)
    _info("reorder_done", list_id=lid, updated=updated)
    return {"ok": True, "count": updated, "reordered": updated}
