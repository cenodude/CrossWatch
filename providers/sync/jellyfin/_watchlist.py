# /providers/sync/jellyfin/_watchlist.py
# JELLYFIN Module for watchlist sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from .._log import log as cw_log

import json
import os
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal
from ._common import (
    state_file,
    chunked,
    collection_add_items,
    collection_remove_items,
    create_collection,
    create_playlist,
    find_collection_id_by_name,
    find_playlist_id_by_name,
    playlist_fetch_all,
    collection_fetch_all,
    mark_favorite,
    playlist_add_items,
    playlist_remove_entries,
    resolve_item_id,
    sleep_ms,
    update_userdata,
    key_of as jelly_key_of,
    normalize as jelly_normalize,
    _pair_scope,
    _is_capture_mode,
)

def _unresolved_path() -> str:
    return str(state_file("jellyfin_watchlist.unresolved.json"))




def _trc(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "watchlist", "trace", msg, **fields)


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "watchlist", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "watchlist", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "watchlist", "warn", msg, **fields)


def _load() -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    try:
        with open(_unresolved_path(), "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save(obj: Mapping[str, Any]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        os.makedirs(os.path.dirname(_unresolved_path()), exist_ok=True)
        tmp = _unresolved_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, _unresolved_path())
    except Exception:
        pass


def _freeze(item: Mapping[str, Any], *, reason: str) -> None:
    key = canonical_key(id_minimal(item))
    data = _load()
    ent = data.get(key) or {"feature": "watchlist", "attempts": 0}
    ent.update({"hint": id_minimal(item)})
    ent["attempts"] = int(ent.get("attempts", 0)) + 1
    ent["reason"] = reason
    data[key] = ent
    _save(data)


def _thaw_if_present(keys: Iterable[str]) -> None:
    data = _load()
    changed = False
    for k in list(keys or []):
        if k in data:
            del data[k]
            changed = True
    if changed:
        _save(data)


def _get_playlist_id(adapter: Any, *, create_if_missing: bool) -> str | None:
    cfg = adapter.cfg
    http = adapter.client
    uid = adapter.cfg.user_id
    name = cfg.watchlist_playlist_name
    pid = find_playlist_id_by_name(http, uid, name)
    if pid:
        return pid
    if not create_if_missing:
        return None
    pid = create_playlist(http, uid, name, is_public=False)
    if pid:
        _info("playlist created", name=name, playlist_id=pid)
    return pid


def _get_collection_id(adapter: Any, *, create_if_missing: bool) -> str | None:
    cfg = adapter.cfg
    http = adapter.client
    uid = adapter.cfg.user_id
    name = cfg.watchlist_playlist_name
    cid = find_collection_id_by_name(http, uid, name)
    if cid:
        return cid
    if not create_if_missing:
        return None
    cid = create_collection(http, name)
    if cid:
        _info("collection created", name=name, collection_id=cid)
    return cid


def _is_episode(obj: Mapping[str, Any]) -> bool:
    t = (obj.get("Type") or obj.get("type") or "").strip().lower()
    return t in ("episode",)


def _is_movie_or_show(obj: Mapping[str, Any]) -> bool:
    t = (obj.get("Type") or obj.get("type") or "").strip().lower()
    return t in ("movie", "show", "series")


# index
def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("watchlist") if callable(prog_mk) else None

    cfg = adapter.cfg
    http = adapter.client
    uid = adapter.cfg.user_id

    # Playlist mode
    if cfg.watchlist_mode == "playlist":
        name = cfg.watchlist_playlist_name
        pid = _get_playlist_id(adapter, create_if_missing=False)
        out: dict[str, dict[str, Any]] = {}
        if pid:
            page_size = max(200, int(getattr(cfg, "watchlist_query_limit", 1000)))
            rows, total = playlist_fetch_all(http, pid, page_size=page_size)
            if prog:
                try:
                    prog.tick(0, total=total, force=True)
                except Exception:
                    pass

            done = 0
            for row in rows:
                if not _is_movie_or_show(row):
                    done += 1
                    if prog:
                        try:
                            prog.tick(done, total=total)
                        except Exception:
                            pass
                    continue
                try:
                    m = jelly_normalize(row)
                    out[canonical_key(m)] = m
                except Exception:
                    pass
                done += 1
                if prog:
                    try:
                        prog.tick(done, total=total)
                    except Exception:
                        pass

        _thaw_if_present(out.keys())
        _info("index done", mode="playlist", name=name, count=len(out))
        return out

    # Collection mode
    if cfg.watchlist_mode == "collection":
        name = cfg.watchlist_playlist_name
        cid = _get_collection_id(adapter, create_if_missing=False)
        out: dict[str, dict[str, Any]] = {}
        if cid:
            page_size = max(200, int(getattr(cfg, "watchlist_query_limit", 1000)))
            rows, total = collection_fetch_all(http, uid, cid, page_size=page_size)
            if prog:
                try:
                    prog.tick(0, total=total, force=True)
                except Exception:
                    pass

            done = 0
            for row in rows:
                if not _is_movie_or_show(row):
                    done += 1
                    if prog:
                        try:
                            prog.tick(done, total=total)
                        except Exception:
                            pass
                    continue
                try:
                    m = jelly_normalize(row)
                    out[canonical_key(m)] = m
                except Exception:
                    pass
                done += 1
                if prog:
                    try:
                        prog.tick(done, total=total)
                    except Exception:
                        pass

        _thaw_if_present(out.keys())
        _info("index done", mode="collection", name=name, count=len(out))
        return out

    # Favorites mode
    page_size = max(1, int(getattr(cfg, "watchlist_query_limit", 1000)))
    start = 0
    total: int | None = None

    out: dict[str, dict[str, Any]] = {}
    done = 0

    while True:
        r = http.get(
            f"/Users/{uid}/Items",
            params={
                "IncludeItemTypes": "Movie,Series",
                "Recursive": True,
                "EnableUserData": True,
                "Fields": "ProviderIds,ProductionYear,UserData,Type",
                "Filters": "IsFavorite",
                "SortBy": "DateLastSaved",
                "SortOrder": "Descending",
                "EnableTotalRecordCount": True,
                "StartIndex": start,
                "Limit": page_size,
            },
        )

        try:
            body = r.json() or {}
            rows: list[Mapping[str, Any]] = body.get("Items") or []
            if total is None:
                total = int(body.get("TotalRecordCount") or 0)
                if prog:
                    try:
                        prog.tick(0, total=total, force=True)
                    except Exception:
                        pass
        except Exception:
            rows = []
            if total is None:
                total = 0

        for row in rows:
            try:
                m = jelly_normalize(row)
                out[canonical_key(m)] = m
            except Exception:
                pass
            done += 1
            if prog and total is not None:
                try:
                    prog.tick(done, total=total)
                except Exception:
                    pass
        start += len(rows)
        if not rows or (total is not None and start >= total):
            break

    _thaw_if_present(out.keys())
    _info("index done", mode="favorites", count=len(out))
    return out

# writes
def _favorite(http: Any, uid: str, item_id: str, flag: bool) -> bool:
    try:
        r = (
            http.post(f"/Users/{uid}/FavoriteItems/{item_id}")
            if flag
            else http.delete(f"/Users/{uid}/FavoriteItems/{item_id}")
        )
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False

def _verify_favorite(
    http: Any,
    uid: str,
    iid: str,
    expect: bool,
    *,
    retries: int = 3,
    delay_ms: int = 150,
) -> bool:
    for attempt in range(max(1, retries)):
        try:
            r = http.get(
                f"/Users/{uid}/Items/{iid}",
                params={"Fields": "UserData", "EnableUserData": True},
            )
            if getattr(r, "status_code", 0) == 200:
                ud = (r.json() or {}).get("UserData") or {}
                val = bool(ud.get("IsFavorite"))
                _trc("favorite verify", item_id=iid, is_favorite=val, expect=expect, attempt=attempt + 1)
                if val is expect:
                    return True
            else:
                r2 = http.get(
                    f"/Users/{uid}/Items",
                    params={"Ids": iid, "Fields": "UserData", "EnableUserData": True},
                )
                if getattr(r2, "status_code", 0) == 200:
                    arr = (r2.json() or {}).get("Items") or []
                    if not arr and expect is False:
                        _dbg("favorite verify fallback empty", item_id=iid, expect=expect)
                        return True
                    if arr:
                        ud = arr[0].get("UserData") or {}
                        val = bool(ud.get("IsFavorite"))
                        _trc("favorite verify fallback", item_id=iid, is_favorite=val, expect=expect, attempt=attempt + 1)
                        if val is expect:
                            return True
        except Exception:
            pass
        if attempt + 1 < retries:
            sleep_ms(delay_ms)
    return False


def _filter_watchlist_items(items: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    for it in items or []:
        t = (it.get("type") or "").strip().lower()
        if t in ("movie", "show"):
            out.append(it)
    return out


def _add_favorites(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    http = adapter.client
    uid = adapter.cfg.user_id
    items = _filter_watchlist_items(items)
    ok = 0
    unresolved: list[dict[str, Any]] = []
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))

    for it in items:
        iid = resolve_item_id(adapter, it)
        if not iid:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            _freeze(it, reason="resolve_failed")
            continue

        wrote = mark_favorite(http, uid, iid, True) or _favorite(http, uid, iid, True)
        if not wrote:
            unresolved.append({"item": id_minimal(it), "hint": "favorite_failed"})
            _freeze(it, reason="write_failed")
            sleep_ms(delay)
            continue

        if not _verify_favorite(http, uid, iid, True):
            forced = update_userdata(http, uid, iid, {"IsFavorite": True})
            _warn("favorite force userdata", item_id=iid, forced=forced)
            if not forced:
                unresolved.append({"item": id_minimal(it), "hint": "verify_failed"})
                _freeze(it, reason="verify_or_userdata_failed")
                sleep_ms(delay)
                continue

        ok += 1
        _thaw_if_present([canonical_key(id_minimal(it))])
        sleep_ms(delay)
    return ok, unresolved


def _remove_favorites(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    http = adapter.client
    uid = adapter.cfg.user_id
    items = _filter_watchlist_items(items)
    ok = 0
    unresolved: list[dict[str, Any]] = []
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))

    for it in items:
        iid = resolve_item_id(adapter, it)
        if not iid:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            _freeze(it, reason="resolve_failed")
            continue

        wrote = mark_favorite(http, uid, iid, False) or _favorite(http, uid, iid, False)
        if not wrote:
            unresolved.append({"item": id_minimal(it), "hint": "unfavorite_failed"})
            _freeze(it, reason="write_failed")
            sleep_ms(delay)
            continue

        if not _verify_favorite(http, uid, iid, False):
            forced = update_userdata(http, uid, iid, {"IsFavorite": False})
            _warn("unfavorite force userdata", item_id=iid, forced=forced)
            if not forced:
                unresolved.append({"item": id_minimal(it), "hint": "verify_failed"})
                _freeze(it, reason="verify_or_userdata_failed")
                sleep_ms(delay)
                continue

        ok += 1
        _thaw_if_present([canonical_key(id_minimal(it))])
        sleep_ms(delay)
    return ok, unresolved


def _add_playlist(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    http = adapter.client
    uid = adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    pid = _get_playlist_id(adapter, create_if_missing=True)
    if not pid:
        return 0, [{"item": {}, "hint": "playlist_missing"}]

    items = _filter_watchlist_items(items)
    mids: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for it in items:
        iid = resolve_item_id(adapter, it)
        if iid:
            mids.append(iid)
        else:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            _freeze(it, reason="resolve_failed")

    ok = 0
    for chunk in chunked(mids, qlim):
        if playlist_add_items(http, pid, uid, chunk):
            ok += len(chunk)
        else:
            for _ in chunk:
                unresolved.append({"item": {}, "hint": "playlist_add_failed"})
        sleep_ms(delay)

    if ok:
        _thaw_if_present([canonical_key({"ids": {"jellyfin": x}}) for x in mids])
    return ok, unresolved


def _remove_playlist(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    http = adapter.client
    uid = adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    pid = _get_playlist_id(adapter, create_if_missing=False)
    if not pid:
        return 0, [{"item": {}, "hint": "playlist_missing"}]

    page_size = max(1, int(getattr(cfg, "watchlist_query_limit", 1000)))
    rows, _total = playlist_fetch_all(http, pid, page_size=page_size)

    entry_by_key: dict[str, list[str]] = {}
    keys_by_eid: dict[str, set[str]] = {}

    for row in rows:
        if not _is_movie_or_show(row):
            continue
        key = jelly_key_of(row)
        entry_id = row.get("PlaylistItemId") or row.get("playlistitemid") or row.get("Id")
        if not key or not entry_id:
            continue
        eid = str(entry_id)
        entry_by_key.setdefault(key, []).append(eid)
        keys_by_eid.setdefault(eid, set()).add(key)

    items = _filter_watchlist_items(items)
    eids: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for it in items:
        k = canonical_key(id_minimal(it))
        found = entry_by_key.get(k)
        if found:
            eids.extend(found)
        else:
            unresolved.append({"item": id_minimal(it), "hint": "no_entry_id"})
            _freeze(it, reason="resolve_failed")
    seen: set[str] = set()
    eids = [x for x in eids if not (x in seen or seen.add(x))]
    ok = 0
    for chunk in chunked(eids, qlim):
        if playlist_remove_entries(http, pid, chunk):
            ok += len(chunk)
            thaw_keys: set[str] = set()
            for eid in chunk:
                thaw_keys |= keys_by_eid.get(eid, set())
            if thaw_keys:
                _thaw_if_present(thaw_keys)
        else:
            for _ in chunk:
                unresolved.append({"item": {}, "hint": "playlist_remove_failed"})
        sleep_ms(delay)
    return ok, unresolved

def _add_collection(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    http = adapter.client
    uid = adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    cid = _get_collection_id(adapter, create_if_missing=True)
    if not cid:
        return 0, [{"item": {}, "hint": "collection_missing"}]

    items = _filter_watchlist_items(items)
    mids: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for it in items:
        iid = resolve_item_id(adapter, it)
        if iid:
            mids.append(iid)
        else:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            _freeze(it, reason="resolve_failed")

    ok = 0
    for chunk in chunked(mids, qlim):
        if collection_add_items(http, cid, chunk):
            ok += len(chunk)
        else:
            for _ in chunk:
                unresolved.append({"item": {}, "hint": "collection_add_failed"})
        sleep_ms(delay)

    if ok:
        _thaw_if_present([canonical_key({"ids": {"jellyfin": x}}) for x in mids])
    return ok, unresolved


def _remove_collection(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    http = adapter.client
    uid = adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    cid = _get_collection_id(adapter, create_if_missing=False)
    if not cid:
        return 0, [{"item": {}, "hint": "collection_missing"}]

    page_size = max(200, int(getattr(cfg, "watchlist_query_limit", 1000)))
    rows_all, _total = collection_fetch_all(http, uid, cid, page_size=page_size)

    by_key: dict[str, list[str]] = {}
    for r in rows_all:
        if not _is_movie_or_show(r):
            continue
        k = jelly_key_of(r)
        iid = r.get("Id")
        if k and iid:
            by_key.setdefault(k, []).append(str(iid))

    items = _filter_watchlist_items(items)
    rm_pairs: list[tuple[str, str]] = []
    seen_ids: set[str] = set()
    unresolved: list[dict[str, Any]] = []

    def _push(iid: str, k: str) -> None:
        if iid and iid not in seen_ids:
            rm_pairs.append((iid, k))
            seen_ids.add(iid)

    for it in items:
        k = canonical_key(id_minimal(it))
        hit = by_key.get(k) or []
        if hit:
            for iid in hit:
                _push(iid, k)
            continue

        iid = resolve_item_id(adapter, it)
        if iid:
            _push(str(iid), k)
        else:
            unresolved.append({"item": id_minimal(it), "hint": "no_collection_item"})
            _freeze(it, reason="resolve_failed")

    ok = 0
    for chunk in chunked(rm_pairs, qlim):
        ids = [iid for iid, _k in chunk]
        keys = [k for _iid, k in chunk]
        if collection_remove_items(http, cid, ids):
            ok += len(ids)
            _thaw_if_present(keys)
            _thaw_if_present([canonical_key({"ids": {"jellyfin": x}}) for x in ids])
        else:
            for _ in ids:
                unresolved.append({"item": {}, "hint": "collection_remove_failed"})
        sleep_ms(delay)
    return ok, unresolved

def add(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    if cfg.watchlist_mode == "playlist":
        ok, unresolved = _add_playlist(adapter, items)
    elif cfg.watchlist_mode == "collection":
        ok, unresolved = _add_collection(adapter, items)
    else:
        ok, unresolved = _add_favorites(adapter, items)
    _info("add done", ok=ok, unresolved=len(unresolved), mode=cfg.watchlist_mode)
    return ok, unresolved


def remove(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    if cfg.watchlist_mode == "playlist":
        ok, unresolved = _remove_playlist(adapter, items)
    elif cfg.watchlist_mode == "collection":
        ok, unresolved = _remove_collection(adapter, items)
    else:
        ok, unresolved = _remove_favorites(adapter, items)
    _info("remove done", ok=ok, unresolved=len(unresolved), mode=cfg.watchlist_mode)
    return ok, unresolved
