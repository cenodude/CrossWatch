# /providers/sync/emby/_watchlist.py
# EMBY Module for watchlist synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
import json
import os
from typing import Any, Iterable, Mapping

from ._common import (
    state_file,
    normalize as emby_normalize,
    key_of as emby_key_of,
    mark_favorite,
    update_userdata,
    find_playlist_id_by_name,
    create_playlist,
    playlist_add_items,
    playlist_remove_entries,
    find_collection_id_by_name,
    create_collection,
    collection_add_items,
    collection_remove_items,
    chunked,
    sleep_ms,
    resolve_item_id,
    _fetch_all_playlist_items,
    _fetch_all_collection_items,
    _fetch_all_series_episodes,
    _is_future_episode,
    playlist_as_watchlist_index,
    _series_minimal_from_episode,
    find_seed_item_id,
)

from cw_platform.id_map import minimal as id_minimal, canonical_key
from .._log import log as cw_log

def _unresolved_path() -> str:
    return state_file("emby_watchlist.unresolved.json")




def _dbg(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "watchlist", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "watchlist", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "watchlist", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "watchlist", "error", msg, **fields)


def _load() -> dict[str, Any]:
    try:
        with open(_unresolved_path(), "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save(obj: Mapping[str, Any]) -> None:
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
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    name = cfg.watchlist_playlist_name
    pid = find_playlist_id_by_name(http, uid, name)
    if pid:
        return pid
    if not create_if_missing:
        return None
    pid = create_playlist(http, uid, name, is_public=False)
    if pid:
        _info("playlist_created", name=name, playlist_id=pid)
    return pid


def _get_collection_id(adapter: Any, *, create_if_missing: bool) -> str | None:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    name = cfg.watchlist_playlist_name

    cid = find_collection_id_by_name(http, uid, name)
    if cid:
        return cid

    if not create_if_missing:
        return None

    seed: str | None = None
    for t in ("Movie", "Series"):
        try:
            r = http.get(
                f"/Users/{uid}/Items",
                params={"IncludeItemTypes": t, "Recursive": True, "Limit": 1},
            )
            if getattr(r, "status_code", 0) == 200:
                arr = (r.json() or {}).get("Items") or []
                if arr and arr[0].get("Id"):
                    seed = str(arr[0]["Id"])
                    break
        except Exception:
            pass

    created = create_collection(http, name, [seed] if seed else None)
    if created:
        _info("collection_created", name=name, collection_id=created)
        return created

    cid = find_collection_id_by_name(http, uid, name)
    if cid:
        _info("collection_created_post_lookup", name=name, collection_id=cid)
        return cid

    _warn("collection_create_failed", name=name)
    return None


def _is_episode(obj: Mapping[str, Any]) -> bool:
    t = (obj.get("Type") or obj.get("type") or "").strip().lower()
    return t in ("episode",)


def _is_movie_or_show(obj: Mapping[str, Any]) -> bool:
    t = (obj.get("Type") or obj.get("type") or "").strip().lower()
    return t in ("movie", "show", "series")


def _is_collections_mode(cfg: Any) -> bool:
    m = str(getattr(cfg, "watchlist_mode", "") or "").strip().lower()
    return m in ("collection", "collections")


# Index building
def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("watchlist") if callable(prog_mk) else None

    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id

    if cfg.watchlist_mode == "playlist":
        name = cfg.watchlist_playlist_name
        pid = _get_playlist_id(adapter, create_if_missing=False)
        out: dict[str, dict[str, Any]] = {}
        if pid:
            out = playlist_as_watchlist_index(
                http,
                uid,
                pid,
                limit=max(1, int(getattr(cfg, "watchlist_query_limit", 1000))),
                progress=prog,
            )
        _thaw_if_present(out.keys())
        _info("index_done", count=len(out), mode="playlist", name=name)
        return out

    if _is_collections_mode(cfg):
        name = cfg.watchlist_playlist_name
        cid = _get_collection_id(adapter, create_if_missing=False)
        out: dict[str, dict[str, Any]] = {}
        if cid:
            page_size = max(1, int(getattr(cfg, "watchlist_query_limit", 1000)))
            rows, total = _fetch_all_collection_items(http, uid, cid, page_size=page_size)

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
                    m = emby_normalize(row)
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
        _info("index_done", count=len(out), mode="collections", name=name)
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
                m = emby_normalize(row)
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
    _info("index_done", count=len(out), mode="favorites")
    return out

# Write helpers
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
                _dbg("verify_favorite", item_id=iid, is_favorite=val, expect=expect, attempt=attempt + 1)
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
                        _dbg("verify_favorite_fallback_missing_ok", item_id=iid, expect=expect, attempt=attempt + 1)
                        return True
                    if arr:
                        ud = arr[0].get("UserData") or {}
                        val = bool(ud.get("IsFavorite"))
                        _dbg("verify_favorite_fallback", item_id=iid, is_favorite=val, expect=expect, attempt=attempt + 1)
                        if val is expect:
                            return True
        except Exception:
            pass
        if attempt + 1 < retries:
            sleep_ms(delay_ms)
    return False


def _filter_watchlist_items(
    items: Iterable[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    for it in items or []:
        t = (it.get("type") or "").strip().lower()
        if t in ("movie", "show", "series"):
            out.append(it)
    return out


def _add_favorites(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    frozen = set((_load() or {}).keys())
    ok = 0
    unresolved: list[dict[str, Any]] = []

    for it in items or []:
        t = (it.get("type") or "").strip().lower()
        if t not in ("movie", "show", "series", "episode"):
            continue
        k = canonical_key(id_minimal(it))
        if k in frozen:
            continue
        iid = resolve_item_id(adapter, it)
        if not iid:
            _freeze(it, reason="resolve_failed")
            continue

        try:
            r = http.get(
                f"/Users/{uid}/Items/{iid}",
                params={"Fields": "UserData", "EnableUserData": True},
            )
            if getattr(r, "status_code", 0) == 200 and bool(
                ((r.json() or {}).get("UserData") or {}).get("IsFavorite")
            ):
                _thaw_if_present([k, canonical_key({"ids": {"emby": iid}})])
                continue
        except Exception:
            pass

        if not (mark_favorite(http, uid, iid, True) or _favorite(http, uid, iid, True)):
            unresolved.append({"item": id_minimal(it), "hint": "favorite_failed"})
            _freeze(it, reason="write_failed")
            sleep_ms(delay)
            continue

        if not _verify_favorite(http, uid, iid, True) and not update_userdata(
            http,
            uid,
            iid,
            {"IsFavorite": True},
        ):
            unresolved.append({"item": id_minimal(it), "hint": "verify_failed"})
            _freeze(it, reason="verify_or_userdata_failed")
            sleep_ms(delay)
            continue

        ok += 1
        _thaw_if_present([k, canonical_key({"ids": {"emby": iid}})])
        sleep_ms(delay)

    return ok, unresolved


def _remove_favorites(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
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

        wrote = mark_favorite(http, uid, iid, False) or _favorite(
            http,
            uid,
            iid,
            False,
        )
        if not wrote:
            unresolved.append(
                {"item": id_minimal(it), "hint": "unfavorite_failed"},
            )
            _freeze(it, reason="write_failed")
            sleep_ms(delay)
            continue

        if not _verify_favorite(http, uid, iid, False):
            forced = update_userdata(http, uid, iid, {"IsFavorite": False})
            _dbg("force_unfavorite", item_id=iid, forced=forced)
            if not forced:
                unresolved.append(
                    {"item": id_minimal(it), "hint": "verify_failed"},
                )
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
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    pid = _get_playlist_id(adapter, create_if_missing=True)
    if not pid:
        return 0, [{"item": {}, "hint": "playlist_missing"}]

    page_size = max(1, int(getattr(cfg, "watchlist_query_limit", 1000)))
    rows_now, _total_now = _fetch_all_playlist_items(http, pid, page_size=page_size)
    existing_ids = {str(r.get("Id")) for r in rows_now if r.get("Id")}
    existing_keys = {emby_key_of(r) for r in rows_now if emby_key_of(r)}
    froz = _load() or {}

    to_add: list[str] = []
    meta: dict[str, str | None] = {}
    unresolved: list[dict[str, Any]] = []

    for it in _filter_watchlist_items(items):
        t = (it.get("type") or "").strip().lower()
        k = canonical_key(id_minimal(it))
        if k in froz and (froz[k] or {}).get("reason") != "future_episode":
            continue
        if k in existing_keys:
            _thaw_if_present([k])
            continue

        if t == "movie":
            iid = resolve_item_id(adapter, it)
            if not iid:
                _freeze(it, reason="resolve_failed")
                continue
            if iid in existing_ids:
                _thaw_if_present([k, canonical_key({"ids": {"emby": iid}})])
                continue
            to_add.append(iid)
            meta[iid] = k
            continue

        if t == "episode":
            if _is_future_episode(it):
                _freeze(it, reason="future_episode")
                continue
            iid = resolve_item_id(adapter, it)
            if not iid:
                _freeze(it, reason="resolve_failed")
                continue
            if iid in existing_ids:
                _thaw_if_present([k, canonical_key({"ids": {"emby": iid}})])
                continue
            to_add.append(iid)
            meta[iid] = k
            continue

        if t in ("show", "series"):
            sid = resolve_item_id(
                adapter,
                {
                    "type": "show",
                    "title": it.get("title"),
                    "year": it.get("year"),
                    "ids": it.get("ids", {}),
                },
            )
            if not sid:
                _freeze(it, reason="resolve_failed")
                continue
            eps = _fetch_all_series_episodes(http, uid, sid, page_size=page_size)
            eps = [ep for ep in eps if not _is_future_episode(ep)]
            if not eps:
                _freeze(it, reason="future_episode")
                continue
            added_any = False
            for ep in eps:
                eid = str(ep.get("Id") or "")
                if not eid or eid in existing_ids:
                    continue
                to_add.append(eid)
                meta[eid] = k
                added_any = True
            if not added_any:
                _thaw_if_present([k])
            continue

    if not to_add:
        return 0, unresolved

    ok = 0
    uniq: list[str] = []
    seen: set[str] = set()
    for iid in to_add:
        if iid not in seen and iid not in existing_ids:
            uniq.append(iid)
            seen.add(iid)

    for chunk in chunked(uniq, qlim):
        if not playlist_add_items(http, pid, uid, chunk):
            unresolved.extend(
                {"item": {}, "hint": "playlist_add_failed"} for _ in chunk
            )
            sleep_ms(delay)
            continue
        rows_after, _total_after = _fetch_all_playlist_items(http, pid, page_size=page_size)
        after_ids = {str(r.get("Id")) for r in rows_after if r.get("Id")}
        for iid in chunk:
            if iid in after_ids:
                ok += 1
                k = meta.get(iid)
                keys = [canonical_key({"ids": {"emby": iid}})]
                if k:
                    keys.append(k)
                _thaw_if_present(keys)
            else:
                _freeze({"ids": {"emby": iid}}, reason="playlist_ignored")
        sleep_ms(delay)

    return ok, unresolved


def _remove_playlist(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    pid = _get_playlist_id(adapter, create_if_missing=False)
    if not pid:
        return 0, [{"item": {}, "hint": "playlist_missing"}]

    page_size = max(1, int(getattr(cfg, "watchlist_query_limit", 1000)))
    rows, _total = _fetch_all_playlist_items(http, pid, page_size=page_size)
    entry_by_key: dict[str, list[str]] = {}
    keys_by_eid: dict[str, set[str]] = {}
    series_cache: dict[str, dict[str, Any] | None] = {}

    for row in rows:
        t = (row.get("Type") or row.get("type") or "").strip().lower()
        entry_id = (
            row.get("PlaylistItemId")
            or row.get("playlistitemid")
            or row.get("Id")
        )
        if not entry_id:
            continue
        eid = str(entry_id)
        if t == "movie":
            key = emby_key_of(row)
            if key:
                entry_by_key.setdefault(key, []).append(eid)
                keys_by_eid.setdefault(eid, set()).add(key)
        elif t == "episode":
            m = _series_minimal_from_episode(http, uid, row, series_cache)
            if m:
                key = canonical_key(m)
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


def _add_collections(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    name = cfg.watchlist_playlist_name

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

    cid = _get_collection_id(adapter, create_if_missing=False)
    if not cid:
        if not mids:
            return 0, unresolved + [
                {"item": {}, "hint": "no_items_to_seed_collection"},
            ]
        seed = [mids[0]]
        cid = create_collection(http, name, initial_ids=seed)
        if not cid:
            return 0, unresolved + [
                {"item": {}, "hint": "collection_create_failed"},
            ]
        mids = mids[1:]

    ok = 0
    for chunk in chunked(mids, qlim):
        if collection_add_items(http, cid, chunk):
            ok += len(chunk)
        else:
            for _ in chunk:
                unresolved.append(
                    {"item": {}, "hint": "collection_add_failed"},
                )
        sleep_ms(delay)

    if ok:
        _thaw_if_present(
            [canonical_key({"ids": {"emby": x}}) for x in (mids or [])],
        )
    return ok, unresolved


def _remove_collections(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    cid = _get_collection_id(adapter, create_if_missing=False)
    if not cid:
        return 0, [{"item": {}, "hint": "collection_missing"}]

    page_size = max(1, int(getattr(cfg, "watchlist_query_limit", 1000)))
    rows_all, _total = _fetch_all_collection_items(http, uid, cid, page_size=page_size)
    rows = [r for r in rows_all if _is_movie_or_show(r)]
    by_key: dict[str, str] = {
        emby_key_of(r): str(r.get("Id")) for r in rows if emby_key_of(r)
    }

    items = _filter_watchlist_items(items)
    rm_ids: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for it in items:
        k = canonical_key(id_minimal(it))
        iid = by_key.get(k) or resolve_item_id(adapter, it)
        if iid:
            rm_ids.append(iid)
        else:
            unresolved.append(
                {"item": id_minimal(it), "hint": "no_collection_item"},
            )
            _freeze(it, reason="resolve_failed")

    ok = 0
    for chunk in chunked(rm_ids, qlim):
        if collection_remove_items(http, cid, chunk):
            ok += len(chunk)
            _thaw_if_present(
                [canonical_key({"ids": {"emby": x}}) for x in chunk],
            )
        else:
            for _ in chunk:
                unresolved.append(
                    {"item": {}, "hint": "collection_remove_failed"},
                )
        sleep_ms(delay)
    return ok, unresolved


def add(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    if cfg.watchlist_mode == "playlist":
        ok, unresolved = _add_playlist(adapter, items)
    elif _is_collections_mode(cfg):
        ok, unresolved = _add_collections(adapter, items)
    else:
        ok, unresolved = _add_favorites(adapter, items)
    _info("write_done", op="add", ok=ok, unresolved=len(unresolved), mode=cfg.watchlist_mode)
    return ok, unresolved


def remove(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = adapter.cfg
    if cfg.watchlist_mode == "playlist":
        ok, unresolved = _remove_playlist(adapter, items)
    elif _is_collections_mode(cfg):
        ok, unresolved = _remove_collections(adapter, items)
    else:
        ok, unresolved = _remove_favorites(adapter, items)
    _info("write_done", op="remove", ok=ok, unresolved=len(unresolved), mode=cfg.watchlist_mode)
    return ok, unresolved
