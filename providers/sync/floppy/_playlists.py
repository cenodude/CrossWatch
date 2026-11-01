# providers/sync/floppy/_playlists.py
# CrossWatch - Floppy custom lists as playlists, with seasons, episodes and custom order
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import time
from collections.abc import Mapping, Sequence
from typing import Any

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.interactive_reads import remember_resource, retained_read
from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PlaylistAuthError,
    PlaylistItem,
    PlaylistReadOnlyError,
    PlaylistResource,
    PlaylistSnapshot,
    PlaylistUnavailableError,
)
from providers.auth._auth_FLOPPY import FloppyAuthError

from ._common import api_delete, api_patch, api_post, api_put, bucket_body, bucket_params, paged, tmdb_enriched_item, watchlist_name, write_bucket

FEATURE = "playlists"
MEDIA_TYPES = ("movie", "show", "season", "episode")
_SNAPSHOT_MEMO_TTL = 6 * 3600.0
_SNAPSHOT_MEMO_MAX = 16
_SNAPSHOT_MEMO: dict[tuple[str, str], tuple[float, tuple[str, Any], list[PlaylistItem]]] = {}


class FloppyPlaylistError(RuntimeError):
    pass


class FloppyListUnavailable(FloppyPlaylistError, PlaylistUnavailableError):
    pass


class FloppyListsTokenRejected(FloppyPlaylistError, PlaylistAuthError):
    pass


def _instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def _owner(row: Mapping[str, Any]) -> str:
    owner_raw = row.get("owner")
    return str(owner_raw.get("username") or "").strip() if isinstance(owner_raw, Mapping) else ""


def _me(rows: Sequence[Mapping[str, Any]]) -> str:
    for row in rows:
        owner = _owner(row)
        if owner and not row.get("collaborators"):
            return owner.lower()
    return ""


def _status(exc: Exception) -> int:
    return int(getattr(exc, "status_code", 0) or 0)


def _resource(adapter: Any, row: Mapping[str, Any], me: str = "") -> PlaylistResource | None:
    lid = str(row.get("id") or "").strip()
    if not lid.isdigit():
        return None
    name = str(row.get("name") or "").strip() or lid
    owner = _owner(row)
    owned = not row.get("collaborators") or bool(me and owner and owner.lower() == me)
    shared = not owned
    builtin = owned and name.lower() == watchlist_name(adapter).lower()
    extra: dict[str, Any] = {
        "item_count": row.get("items_count"),
        "updated_at": str(row.get("latest_update") or "").strip(),
        "private": not bool(row.get("is_public")),
        "owner": owner,
        "builtin": builtin,
        "can_rename": owned and not builtin,
        "can_delete": owned and not builtin,
    }
    if shared:
        extra["endpoint_type"] = "collaboration"
    return PlaylistResource(
        provider="FLOPPY",
        id=lid,
        name=f"{name} by {owner}" if shared and owner else name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=owned,
        media_types=MEDIA_TYPES,
        extra=extra,
    )


def _paged(adapter: Any, path: str) -> list[Mapping[str, Any]]:
    try:
        return paged(adapter, path)
    except FloppyAuthError as exc:
        if _status(exc) in (401, 403):
            raise FloppyListsTokenRejected("Floppy rejected the API token. Reconnect Floppy") from None
        if _status(exc) == 404:
            raise FloppyListUnavailable("The Floppy list no longer exists") from None
        raise


@retained_read
def list_resources(adapter: Any) -> list[PlaylistResource]:
    rows = _paged(adapter, "lists")
    me = _me(rows)
    return [res for res in (_resource(adapter, row, me) for row in rows) if res is not None]


def _find_resource(adapter: Any, lid: str) -> PlaylistResource | None:
    for res in list_resources(adapter):
        if res.id == lid:
            return res
    return None


def _media_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    parts = [p for p in str(row.get("item_id") or "").strip("/").split("/") if p]
    if len(parts) < 3:
        raise FloppyPlaylistError("floppy_playlists_invalid_list_item")
    typ, source, media_id = parts[0].lower(), parts[1].lower(), parts[2]
    if source != "tmdb":
        return None
    if typ not in ("movie", "tv", "anime"):
        return None
    if not media_id.isdigit():
        raise FloppyPlaylistError("floppy_playlists_invalid_list_item")
    inner_raw = row.get("item")
    inner: Mapping[str, Any] = inner_raw if isinstance(inner_raw, Mapping) else {}
    title = str(inner.get("title") or row.get("title") or "").strip()
    if typ == "movie":
        if len(parts) != 3:
            raise FloppyPlaylistError("floppy_playlists_invalid_list_item")
        out: dict[str, Any] = {"type": "movie", "ids": {"tmdb": media_id}}
    elif len(parts) == 3:
        out = {"type": "show", "ids": {"tmdb": media_id}}
    elif len(parts) == 4 and parts[3].isdigit():
        out = {"type": "season", "show_ids": {"tmdb": media_id}, "season": int(parts[3]), "ids": {}}
    elif len(parts) == 5 and parts[3].isdigit() and parts[4].isdigit():
        out = {"type": "episode", "show_ids": {"tmdb": media_id}, "season": int(parts[3]), "episode": int(parts[4]), "ids": {}}
    else:
        raise FloppyPlaylistError("floppy_playlists_invalid_list_item")
    if title:
        out["title" if out["type"] in ("movie", "show") else "series_title"] = title
    return id_minimal(out)


def _snapshot_stamp(resource: PlaylistResource) -> tuple[str, Any] | None:
    updated = str(resource.extra.get("updated_at") or "").strip()
    return (updated, resource.extra.get("item_count")) if updated else None


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
    resource = _find_resource(adapter, lid) if lid.isdigit() else None
    if resource is None:
        raise FloppyListUnavailable(f"The Floppy list {lid} no longer exists")
    stamp = _snapshot_stamp(resource)
    cached = _memo_get(adapter, lid, stamp)
    if cached is not None:
        return PlaylistSnapshot(resource=resource, items=cached, checkpoint=None)
    rows = _paged(adapter, f"lists/{lid}/items")
    items: list[PlaylistItem] = []
    for row in rows:
        media = _media_from_row(row)
        if media is None:
            continue
        item = PlaylistItem.from_media(media, playlist_item_id=row.get("id"), position=len(items), provider_media_id=str(row.get("item_id") or ""))
        if not item.key:
            raise FloppyPlaylistError("floppy_playlists_invalid_list_item")
        items.append(item)
    _memo_put(adapter, lid, stamp, items)
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _writable(adapter: Any, playlist_id: Any) -> PlaylistResource:
    lid = str(playlist_id or "").strip()
    res = _find_resource(adapter, lid) if lid.isdigit() else None
    if res is None or not (res.can_add or res.can_remove):
        raise PlaylistReadOnlyError(f"The Floppy list {lid} is read only")
    return res


def _media_path(adapter: Any, item: Mapping[str, Any]) -> tuple[str, str] | None:
    kind = str(item.get("type") or "").strip().lower()
    if kind == "movie":
        enriched = tmdb_enriched_item(adapter, item)
        tmdb = str((enriched.get("ids") or {}).get("tmdb") or "").strip()
        return (f"media/movie/tmdb/{tmdb}", "") if tmdb.isdigit() else None
    if kind not in ("show", "season", "episode"):
        return None
    show = kind != "show"
    enriched = tmdb_enriched_item(adapter, item, episode_show=show)
    src = enriched.get("show_ids") if show else enriched.get("ids")
    tmdb = str((src or {}).get("tmdb") or "").strip() if isinstance(src, Mapping) else ""
    if not tmdb.isdigit():
        return None
    base = f"media/tv/tmdb/{tmdb}"
    if kind == "season":
        season = item.get("season")
        if not isinstance(season, int) or season < 0:
            return None
        base = f"{base}/{season}"
    elif kind == "episode":
        season, episode = item.get("season"), item.get("episode")
        if not isinstance(season, int) or season < 0 or not isinstance(episode, int) or episode < 1:
            return None
        base = f"{base}/{season}/{episode}"
    return base, write_bucket(adapter, tmdb, item)


def _write(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]], *, op: str) -> dict[str, Any]:
    res = _writable(adapter, playlist_id)
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    unresolved_keys: list[str] = []
    seen: set[str] = set()
    ok = True
    _memo_drop(adapter, res.id)
    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        key = canonical_key(raw)
        if not key or key in seen:
            continue
        seen.add(key)
        target = _media_path(adapter, raw)
        if target is None:
            unresolved.append({"item": id_minimal(raw), "key": key, "hint": "missing_tmdb_id"})
            unresolved_keys.append(key)
            continue
        path, bucket = target
        try:
            if op == "add":
                api_put(adapter, f"{path}/lists/{res.id}", json=bucket_body(bucket) or None)
            else:
                api_delete(adapter, f"{path}/lists/{res.id}", params=bucket_params(bucket))
        except FloppyAuthError as exc:
            status = _status(exc)
            if status in (401, 403):
                raise FloppyListsTokenRejected("Floppy rejected the API token. Reconnect Floppy") from None
            if op == "add" and status == 409:
                confirmed.append(key)
                continue
            ok = False
            unresolved.append({"item": {"key": key}, "key": key, "hint": "not_found" if status == 404 else f"http:{status}"})
            unresolved_keys.append(key)
            continue
        confirmed.append(key)
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, list(items or []), op="add")


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, list(items or []), op="remove")


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    res = _writable(adapter, playlist_id)
    if not res.can_reorder:
        raise PlaylistReadOnlyError(f"The Floppy list {res.name or res.id} cannot be reordered")
    _memo_drop(adapter, res.id)
    snap = get_snapshot(adapter, res.id)
    by_key: dict[str, Any] = {}
    for it in snap.items:
        if it.key and it.playlist_item_id is not None:
            by_key.setdefault(it.key, it.playlist_item_id)
    order: list[int] = []
    used: set[str] = set()
    for key in ordered_keys or []:
        item_id = by_key.get(str(key or "").strip())
        if item_id is not None and str(item_id) not in used:
            used.add(str(item_id))
            order.append(int(item_id))
    for it in snap.items:
        if it.playlist_item_id is not None and str(it.playlist_item_id) not in used:
            used.add(str(it.playlist_item_id))
            order.append(int(it.playlist_item_id))
    current = [int(it.playlist_item_id) for it in snap.items if it.playlist_item_id is not None]
    if not order or order == current:
        return {"ok": True, "count": 0, "reordered": 0}
    api_put(adapter, f"lists/{res.id}/items/order", json={"item_ids": order})
    _memo_drop(adapter, res.id)
    return {"ok": True, "count": len(order), "reordered": len(order)}


def _owned(adapter: Any, playlist_id: Any, field: str) -> PlaylistResource:
    lid = str(playlist_id or "").strip()
    res = _find_resource(adapter, lid) if lid.isdigit() else None
    if res is None or not res.extra.get(field):
        raise PlaylistReadOnlyError("The Floppy list cannot be changed")
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
        return PlaylistResource(provider="FLOPPY", id="", name=nm, instance=_instance_id(adapter), can_add=True, can_remove=True, can_reorder=True, media_types=MEDIA_TYPES)
    data = api_post(adapter, "lists", json={"name": nm, "is_public": False})
    res = _resource(adapter, data) if isinstance(data, Mapping) else None
    if res is None:
        raise FloppyPlaylistError("floppy create list returned no id")
    remember_resource(list_resources, adapter, res)
    if items:
        add(adapter, res.id, items)
    return res


def rename(adapter: Any, playlist_id: Any, name: str, *, dry_run: bool = False) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    res = _owned(adapter, playlist_id, "can_rename")
    if not dry_run:
        api_patch(adapter, f"lists/{res.id}", json={"name": nm})
    return PlaylistResource(provider="FLOPPY", id=res.id, name=nm, instance=res.instance, can_add=res.can_add, can_remove=res.can_remove, can_reorder=res.can_reorder, media_types=res.media_types, extra=dict(res.extra))


def delete(adapter: Any, playlist_id: Any, *, dry_run: bool = False) -> dict[str, Any]:
    res = _owned(adapter, playlist_id, "can_delete")
    if dry_run:
        return {"ok": True, "dry_run": True}
    _memo_drop(adapter, res.id)
    api_delete(adapter, f"lists/{res.id}")
    return {"ok": True, "count": 1}
