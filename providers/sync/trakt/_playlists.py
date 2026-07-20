# /providers/sync/trakt/_playlists.py
# TRAKT Module for playlist sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from typing import Any, Iterable, Mapping, Sequence

from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PlaylistItem,
    PlaylistResource,
    PlaylistSnapshot,
)

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

BASE = "https://api.trakt.tv"
_PROVIDER = "TRAKT"
_FEATURE = "playlists"
_SUPPORTED_TYPES = ("movie", "show", "season", "episode")
WATCHLIST_ID = "__watchlist__"

_SETTINGS_MEMO: tuple[float, dict[str, Any] | None] = (0.0, None)


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
    )


def list_resources(adapter: Any) -> list[PlaylistResource]:
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    out: list[PlaylistResource] = [_watchlist_resource(adapter)]
    r = request_with_retries(
        sess,
        "GET",
        f"{BASE}/users/me/lists",
        headers=headers,
        timeout=adapter.cfg.timeout,
        max_retries=adapter.cfg.max_retries,
    )
    if r.status_code != 200:
        _warn("http_failed", op="list_resources", status=r.status_code)
        return out
    data = r.json() if (r.text or "").strip() else []
    for row in data or []:
        if not isinstance(row, Mapping):
            continue
        res = _resource_from_list(adapter, row)
        if res:
            out.append(res)
    _info("list_resources_done", count=len(out))
    return out


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    if _is_watchlist_id(playlist_id):
        resource = _watchlist_resource(adapter)
        idx = feat_watchlist.build_index(adapter) or {}
        items = [PlaylistItem.from_media(m, position=i) for i, m in enumerate(idx.values()) if isinstance(m, Mapping)]
        _info("snapshot_done", list_id=WATCHLIST_ID, count=len(items))
        return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)

    lid = _list_id(playlist_id)
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)

    resource: PlaylistResource | None = None
    for res in list_resources(adapter):
        if res.id == lid or res.name == lid:
            resource = res
            lid = res.id
            break
    if resource is None:
        resource = PlaylistResource(
            provider=_PROVIDER,
            id=lid,
            name=lid,
            instance=_instance_id(adapter),
            can_add=True,
            can_remove=True,
            can_reorder=True,
            media_types=("movies", "shows", "seasons", "episodes"),
        )

    items: list[PlaylistItem] = []
    page = 1
    per_page = 100
    while page <= 1000:
        r = request_with_retries(
            sess,
            "GET",
            f"{BASE}/users/me/lists/{lid}/items",
            headers=headers,
            params={"page": page, "limit": per_page, "extended": "full"},
            timeout=adapter.cfg.timeout,
            max_retries=adapter.cfg.max_retries,
        )
        if r.status_code != 200:
            _warn("http_failed", op="get_snapshot", status=r.status_code, list_id=lid)
            break
        rows = r.json() if (r.text or "").strip() else []
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
        try:
            page_count = int(r.headers.get("X-Pagination-Page-Count") or 0)
        except Exception:
            page_count = 0
        if page_count and page >= page_count:
            break
        if len(rows) < per_page:
            break
        page += 1

    items.sort(key=lambda it: (it.position is None, it.position if it.position is not None else 0))
    _info("snapshot_done", list_id=lid, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


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
        existing = len(list_resources(adapter))
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
    return res


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
    if _is_watchlist_id(playlist_id):
        lst = list(items or [])
        count, unresolved = (feat_watchlist.add(adapter, lst) if op == "add" else feat_watchlist.remove(adapter, lst))
        confirmed = _confirmed_keys(lst, unresolved)
        return {"ok": True, "count": int(count or 0), "unresolved": unresolved, "confirmed_keys": confirmed}

    lid = _list_id(playlist_id)
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

    url = f"{BASE}/users/me/lists/{lid}/items"
    if op == "remove":
        url = f"{BASE}/users/me/lists/{lid}/items/remove"

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
    if _is_watchlist_id(playlist_id):
        return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}

    lid = _list_id(playlist_id)
    snap = get_snapshot(adapter, lid)
    key_to_item_id: dict[str, Any] = {}
    for it in snap.items:
        if it.key and it.playlist_item_id and it.key not in key_to_item_id:
            key_to_item_id[it.key] = it.playlist_item_id

    rank: list[Any] = []
    seen: set[str] = set()
    for k in ordered_keys or []:
        ks = str(k or "").strip()
        if not ks or ks in seen:
            continue
        seen.add(ks)
        item_id = key_to_item_id.get(ks)
        if item_id is not None:
            rank.append(item_id)
    for it in snap.items:
        if it.key in seen:
            continue
        if it.playlist_item_id is not None:
            rank.append(it.playlist_item_id)
            seen.add(it.key)

    if not rank:
        return {"ok": True, "count": 0, "reordered": 0}

    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    r = request_with_retries(
        sess,
        "POST",
        f"{BASE}/users/me/lists/{lid}/items/reorder",
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
