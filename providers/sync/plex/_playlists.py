# /providers/sync/plex/_playlists.py
# Plex Module for playlist sync functions
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

from ._common import (
    item_guid_candidates,
    key_of as plex_key_of,
    normalize as plex_normalize,
    resolve_obj_by_guids,
)
from . import _watchlist as feat_watchlist
from .._log import log as cw_log

_PROVIDER = "PLEX"
_FEATURE = "playlists"
_VIDEO_TYPES = {"movie", "episode"}
_COLLECTION_TYPES = {"movie", "show"}
WATCHLIST_ID = "__watchlist__"
COLLECTION_PREFIX = "collection:"


def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)


class PlaylistError(RuntimeError):
    pass


class SmartPlaylistError(PlaylistError):
    pass


class PlaylistNotFound(PlaylistError):
    pass


def _instance_id(adapter: Any) -> str:
    inst = getattr(adapter, "instance_id", None)
    return str(inst).strip() if inst else "default"


def _is_watchlist_id(resource_or_id: Any) -> bool:
    raw = resource_or_id.id if isinstance(resource_or_id, PlaylistResource) else resource_or_id
    lid = str(raw or "").strip().lower()
    return lid in {WATCHLIST_ID, "watchlist", "plex:watchlist"}


def _is_collection_id(resource_or_id: Any) -> bool:
    raw = resource_or_id.id if isinstance(resource_or_id, PlaylistResource) else resource_or_id
    return str(raw or "").strip().lower().startswith(COLLECTION_PREFIX)


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


def _server(adapter: Any) -> Any:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if srv is None:
        raise PlaylistError("plex server not connected")
    return srv


def _is_smart(pl: Any) -> bool:
    try:
        return bool(getattr(pl, "smart", False))
    except Exception:
        return False


def _section_key(section: Any) -> str:
    for attr in ("key", "librarySectionID", "sectionID"):
        v = getattr(section, attr, None)
        if v is not None and str(v).strip():
            return str(v).strip()
    return str(getattr(section, "title", "") or "").strip()


def _section_type(section: Any) -> str:
    return str(getattr(section, "type", "") or getattr(section, "TYPE", "") or "").strip().lower()


def _collection_subtype(section: Any, coll: Any) -> str:
    sub = str(getattr(coll, "subtype", "") or "").strip().lower()
    return sub if sub in _COLLECTION_TYPES else _section_type(section)


def _collection_id(section: Any, coll: Any) -> str:
    sid = _section_key(section)
    rk = getattr(coll, "ratingKey", None) or getattr(coll, "key", None) or getattr(coll, "title", "")
    return f"{COLLECTION_PREFIX}{sid}:{str(rk or '').strip()}"


def _parse_collection_id(value: Any) -> tuple[str, str] | None:
    raw = str(value or "").strip()
    if not raw.lower().startswith(COLLECTION_PREFIX):
        return None
    rest = raw[len(COLLECTION_PREFIX):]
    if ":" not in rest:
        return None
    sid, rid = rest.split(":", 1)
    sid = sid.strip()
    rid = rid.strip()
    if not sid or not rid:
        return None
    return sid, rid


def _playlist_type(pl: Any) -> str:
    return str(getattr(pl, "playlistType", "") or "").lower()


def _resource_from_playlist(adapter: Any, pl: Any) -> PlaylistResource:
    smart = _is_smart(pl)
    rk = getattr(pl, "ratingKey", None)
    pid = str(rk if rk is not None else (getattr(pl, "key", None) or getattr(pl, "title", "")))
    return PlaylistResource(
        provider=_PROVIDER,
        id=pid,
        name=str(getattr(pl, "title", "") or pid),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_SMART if smart else PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=not smart,
        can_remove=not smart,
        can_reorder=not smart,
        media_types=("movie", "episode"),
        extra={"endpoint_type": "playlist", "raw_id": pid},
    )


def _resource_from_collection(adapter: Any, section: Any, coll: Any) -> PlaylistResource | None:
    subtype = _collection_subtype(section, coll)
    if subtype not in _COLLECTION_TYPES:
        return None
    smart = _is_smart(coll)
    cid = _collection_id(section, coll)
    raw = str(getattr(coll, "ratingKey", None) or "").strip()
    return PlaylistResource(
        provider=_PROVIDER,
        id=cid,
        name=str(getattr(coll, "title", "") or raw or cid),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_SMART if smart else PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=not smart,
        can_remove=not smart,
        can_reorder=False,
        media_types=(subtype,),
        extra={
            "endpoint_type": "collection",
            "raw_id": raw,
            "section_id": _section_key(section),
            "section_title": str(getattr(section, "title", "") or ""),
            "subtype": subtype,
            "smart": smart,
            "item_count": getattr(coll, "childCount", None),
        },
    )


def _iter_video_playlists(srv: Any) -> list[Any]:
    try:
        playlists = list(srv.playlists() or [])
    except Exception as e:
        _warn("list_failed", error=str(e))
        return []
    return [pl for pl in playlists if _playlist_type(pl) in ("", "video")]


def _iter_collection_sections(srv: Any) -> list[Any]:
    try:
        sections = list(srv.library.sections() or [])
    except Exception as e:
        _warn("collection_sections_failed", error=str(e))
        return []
    return [sec for sec in sections if _section_type(sec) in _COLLECTION_TYPES]


def _iter_collections(srv: Any) -> list[tuple[Any, Any]]:
    out: list[tuple[Any, Any]] = []
    for sec in _iter_collection_sections(srv):
        try:
            cols = list(sec.collections() or [])
        except Exception as e:
            _warn("collection_list_failed", section=_section_key(sec), error=str(e))
            continue
        for coll in cols:
            if _collection_subtype(sec, coll) in _COLLECTION_TYPES:
                out.append((sec, coll))
    return out


def list_resources(adapter: Any) -> list[PlaylistResource]:
    srv = _server(adapter)
    out = [_watchlist_resource(adapter)]
    out.extend(_resource_from_playlist(adapter, pl) for pl in _iter_video_playlists(srv))
    for section, coll in _iter_collections(srv):
        res = _resource_from_collection(adapter, section, coll)
        if res:
            out.append(res)
    _info("list_resources_done", count=len(out))
    return out


def _find_playlist(srv: Any, playlist_id: Any) -> Any:
    want = str(playlist_id or "").strip()
    if not want:
        raise PlaylistNotFound("missing playlist id")
    for pl in _iter_video_playlists(srv):
        rk = str(getattr(pl, "ratingKey", "") or "")
        if rk and rk == want:
            return pl
    for pl in _iter_video_playlists(srv):
        if str(getattr(pl, "title", "") or "") == want:
            return pl
    raise PlaylistNotFound(f"plex playlist not found: {want}")


def _find_collection(srv: Any, collection_id: Any) -> tuple[Any, Any]:
    parsed = _parse_collection_id(collection_id)
    if not parsed:
        raise PlaylistNotFound("missing plex collection id")
    want_section, want_key = parsed
    for section, coll in _iter_collections(srv):
        if _section_key(section) != want_section:
            continue
        rk = str(getattr(coll, "ratingKey", "") or "")
        key = str(getattr(coll, "key", "") or "")
        title = str(getattr(coll, "title", "") or "")
        if want_key in {rk, key, title}:
            return section, coll
    raise PlaylistNotFound(f"plex collection not found: {collection_id}")


def _obj_key(obj: Any) -> str:
    try:
        return plex_key_of(obj)
    except Exception:
        return ""


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    if _is_watchlist_id(playlist_id):
        resource = _watchlist_resource(adapter)
        idx = feat_watchlist.build_index(adapter) or {}
        items = [PlaylistItem.from_media(m, position=i) for i, m in enumerate(idx.values()) if isinstance(m, Mapping)]
        _info("snapshot_done", list_id=WATCHLIST_ID, count=len(items))
        return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)

    srv = _server(adapter)
    if _is_collection_id(playlist_id):
        section, coll = _find_collection(srv, playlist_id)
        resource = _resource_from_collection(adapter, section, coll)
        if resource is None:
            raise PlaylistNotFound(f"plex collection not supported: {playlist_id}")
        try:
            raw = list(coll.items() or [])
        except Exception as e:
            _warn("collection_snapshot_failed", list_id=resource.id, error=str(e))
            raw = []
        subtype = _collection_subtype(section, coll)
        items = []
        for obj in raw:
            if str(getattr(obj, "type", "") or "").lower() != subtype:
                continue
            items.append(
                PlaylistItem.from_media(
                    plex_normalize(obj),
                    playlist_item_id=getattr(obj, "ratingKey", None),
                    position=None,
                    provider_media_id=getattr(obj, "ratingKey", None),
                )
            )
        _info("snapshot_done", list_id=resource.id, count=len(items))
        return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)

    pl = _find_playlist(srv, playlist_id)
    resource = _resource_from_playlist(adapter, pl)

    items: list[PlaylistItem] = []
    try:
        raw = list(pl.items() or [])
    except Exception as e:
        _warn("snapshot_failed", list_id=resource.id, error=str(e))
        raw = []

    for pos, obj in enumerate(raw):
        typ = str(getattr(obj, "type", "") or "").lower()
        if typ not in _VIDEO_TYPES:
            continue
        media = plex_normalize(obj)
        items.append(
            PlaylistItem.from_media(
                media,
                playlist_item_id=getattr(obj, "playlistItemID", None),
                position=pos,
                provider_media_id=getattr(obj, "ratingKey", None),
            )
        )
    _info("snapshot_done", list_id=resource.id, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _resolve_object(srv: Any, item: Mapping[str, Any], *, accept: set[str] | None = None, allow: set[str] | None = None) -> Any | None:
    guids = item_guid_candidates(item.get("ids") or {}, item.get("show_ids") or {}, item)
    if not guids:
        return None
    return resolve_obj_by_guids(srv, guids, set(allow or set()), set(accept or _VIDEO_TYPES))


def _resolve_items(srv: Any, items: Sequence[Mapping[str, Any]], *, accept: set[str] | None = None, allow: set[str] | None = None, missing_hint: str = "not_found") -> tuple[list[Any], list[str], list[dict[str, Any]]]:
    resolved: list[Any] = []
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        m = id_minimal(raw)
        if isinstance(raw, Mapping) and raw.get("show_ids"):
            m["show_ids"] = raw.get("show_ids")
        key = canonical_key(m)
        if key in seen:
            continue
        seen.add(key)
        obj = _resolve_object(srv, m, accept=accept, allow=allow)
        if obj is None:
            unresolved.append({"item": m, "hint": missing_hint})
            continue
        resolved.append(obj)
        confirmed.append(key)
    return resolved, confirmed, unresolved


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
            can_reorder=True,
            media_types=("movie", "episode"),
        )
    srv = _server(adapter)
    resolved, _confirmed, _unresolved = _resolve_items(srv, list(items or []))
    if not resolved:
        raise PlaylistError("plex requires at least one resolvable item to create a playlist")
    try:
        pl = srv.createPlaylist(nm, items=resolved)
    except Exception as e:
        _warn("create_failed", name=nm, error=str(e))
        raise PlaylistError(f"plex create playlist failed: {e}") from e
    _info("create_done", name=nm)
    return _resource_from_playlist(adapter, pl)


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


def _guard_writable(pl: Any) -> None:
    if _is_smart(pl):
        raise SmartPlaylistError("cannot write to a plex smart playlist")
    if _playlist_type(pl) not in ("", "video"):
        raise PlaylistError("plex playlist is not a video playlist")


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if _is_watchlist_id(playlist_id):
        lst = list(items or [])
        count, unresolved = feat_watchlist.add(adapter, lst)
        return {"ok": True, "count": int(count or 0), "unresolved": unresolved, "confirmed_keys": _confirmed_keys(lst, unresolved)}

    srv = _server(adapter)
    if _is_collection_id(playlist_id):
        section, coll = _find_collection(srv, playlist_id)
        resource = _resource_from_collection(adapter, section, coll)
        if resource is None:
            raise PlaylistNotFound(f"plex collection not supported: {playlist_id}")
        if resource.is_smart:
            raise SmartPlaylistError("cannot write to a plex smart collection")
        subtype = str(resource.extra.get("subtype") or "").strip().lower()
        section_id = str(resource.extra.get("section_id") or "").strip()
        allow = {section_id} if section_id else None
        resolved, confirmed, unresolved = _resolve_items(
            srv,
            list(items or []),
            accept={subtype},
            allow=allow,
            missing_hint="not_in_library",
        )
        added = 0
        if resolved:
            try:
                coll.addItems(resolved)
                added = len(resolved)
            except Exception as e:
                _warn("collection_add_failed", list_id=resource.id, error=str(e))
                for obj in resolved:
                    unresolved.append({"item": plex_normalize(obj), "hint": "add_failed"})
                confirmed = []
                added = 0
        _info("write_done", op="add", list_id=resource.id, applied=added, unresolved=len(unresolved))
        return {"ok": True, "count": added, "unresolved": unresolved, "confirmed_keys": confirmed}

    pl = _find_playlist(srv, playlist_id)
    _guard_writable(pl)

    resolved, confirmed, unresolved = _resolve_items(srv, list(items or []))
    added = 0
    if resolved:
        try:
            pl.addItems(resolved)
            added = len(resolved)
        except Exception as e:
            _warn("add_failed", list_id=str(playlist_id), error=str(e))
            for obj in resolved:
                unresolved.append({"item": plex_normalize(obj), "hint": "add_failed"})
            confirmed = []
            added = 0
    _info("write_done", op="add", list_id=str(playlist_id), applied=added, unresolved=len(unresolved))
    return {"ok": True, "count": added, "unresolved": unresolved, "confirmed_keys": confirmed}


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if _is_watchlist_id(playlist_id):
        lst = list(items or [])
        count, unresolved = feat_watchlist.remove(adapter, lst)
        return {"ok": True, "count": int(count or 0), "unresolved": unresolved, "confirmed_keys": _confirmed_keys(lst, unresolved)}

    srv = _server(adapter)
    if _is_collection_id(playlist_id):
        section, coll = _find_collection(srv, playlist_id)
        resource = _resource_from_collection(adapter, section, coll)
        if resource is None:
            raise PlaylistNotFound(f"plex collection not supported: {playlist_id}")
        if resource.is_smart:
            raise SmartPlaylistError("cannot write to a plex smart collection")
        wanted: set[str] = set()
        for raw in items or []:
            try:
                wanted.add(canonical_key(id_minimal(raw)))
            except Exception:
                continue
        try:
            current = list(coll.items() or [])
        except Exception as e:
            _warn("collection_remove_failed", list_id=resource.id, error=str(e))
            current = []
        present: dict[str, Any] = {}
        for obj in current:
            k = _obj_key(obj)
            if k and k not in present:
                present[k] = obj
        removed = 0
        confirmed: list[str] = []
        unresolved: list[dict[str, Any]] = []
        for key in wanted:
            obj = present.get(key)
            if obj is None:
                unresolved.append({"item": {"key": key}, "hint": "not_in_collection"})
                continue
            try:
                coll.removeItems(obj)
                removed += 1
                confirmed.append(key)
            except Exception as e:
                _warn("collection_remove_item_failed", list_id=resource.id, error=str(e))
                unresolved.append({"item": {"key": key}, "hint": "remove_failed"})
        _info("write_done", op="remove", list_id=resource.id, applied=removed, unresolved=len(unresolved))
        return {"ok": True, "count": removed, "unresolved": unresolved, "confirmed_keys": confirmed}

    pl = _find_playlist(srv, playlist_id)
    _guard_writable(pl)

    wanted: set[str] = set()
    for raw in items or []:
        try:
            wanted.add(canonical_key(id_minimal(raw)))
        except Exception:
            continue

    removed = 0
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    try:
        current = list(pl.items() or [])
    except Exception as e:
        _warn("remove_failed", list_id=str(playlist_id), error=str(e))
        current = []

    present: dict[str, Any] = {}
    for obj in current:
        k = _obj_key(obj)
        if k and k not in present:
            present[k] = obj

    for key in wanted:
        obj = present.get(key)
        if obj is None:
            unresolved.append({"item": {"key": key}, "hint": "not_in_playlist"})
            continue
        try:
            pl.removeItems(obj)
            removed += 1
            confirmed.append(key)
        except Exception as e:
            _warn("remove_item_failed", list_id=str(playlist_id), error=str(e))
            unresolved.append({"item": {"key": key}, "hint": "remove_failed"})

    _info("write_done", op="remove", list_id=str(playlist_id), applied=removed, unresolved=len(unresolved))
    return {"ok": True, "count": removed, "unresolved": unresolved, "confirmed_keys": confirmed}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    if _is_watchlist_id(playlist_id):
        return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}
    if _is_collection_id(playlist_id):
        return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}

    srv = _server(adapter)
    pl = _find_playlist(srv, playlist_id)
    _guard_writable(pl)

    try:
        current = list(pl.items() or [])
    except Exception as e:
        _warn("reorder_failed", list_id=str(playlist_id), error=str(e))
        return {"ok": False, "count": 0, "reordered": 0, "error": str(e)}

    cur_keys = [_obj_key(o) for o in current]
    obj_by_key: dict[str, Any] = {}
    for k, o in zip(cur_keys, current):
        if k and k not in obj_by_key:
            obj_by_key[k] = o

    target = [k for k in (ordered_keys or []) if k in obj_by_key]
    work = [k for k in cur_keys if k]
    moves = 0
    for i, k in enumerate(target):
        if i < len(work) and work[i] == k:
            continue
        after = obj_by_key[target[i - 1]] if i > 0 else None
        try:
            pl.moveItem(obj_by_key[k], after=after)
        except TypeError:
            pl.moveItem(obj_by_key[k], after)
        moves += 1
        try:
            work.remove(k)
        except ValueError:
            pass
        work.insert(i, k)

    _info("reorder_done", list_id=str(playlist_id), moves=moves)
    return {"ok": True, "count": moves, "reordered": moves}
