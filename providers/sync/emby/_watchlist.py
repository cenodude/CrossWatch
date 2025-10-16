# /providers/sync/emby/_watchlist.py
from __future__ import annotations
import os, json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from ._common import (
    normalize as emby_normalize,
    key_of as emby_key_of,
    mark_favorite,
    update_userdata,
    find_playlist_id_by_name,
    create_playlist,
    get_playlist_items,
    playlist_add_items,
    playlist_remove_entries,
    find_collection_id_by_name,
    create_collection,
    get_collection_items,
    collection_add_items,
    collection_remove_items,
    chunked,
    sleep_ms,
    resolve_item_id,
    get_series_episodes,
    _is_future_episode,
    playlist_as_watchlist_index,
    _series_minimal_from_episode,
    find_seed_item_id,
)

try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

UNRESOLVED_PATH = "/config/.cw_state/emby_watchlist.unresolved.json"


def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_EMBY_DEBUG"):
        print(f"[EMBY:watchlist] {msg}")


def _load() -> Dict[str, Any]:
    try:
        with open(UNRESOLVED_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save(obj: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(UNRESOLVED_PATH), exist_ok=True)
        tmp = UNRESOLVED_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, UNRESOLVED_PATH)
    except Exception:
        pass


def _freeze(item: Mapping[str, Any], *, reason: str) -> None:
    key = canonical_key(id_minimal(item))
    data = _load(); ent = data.get(key) or {"feature": "watchlist", "attempts": 0}
    ent.update({"hint": id_minimal(item)})
    ent["attempts"] = int(ent.get("attempts", 0)) + 1
    ent["reason"] = reason
    data[key] = ent
    _save(data)


def _thaw_if_present(keys: Iterable[str]) -> None:
    data = _load(); changed = False
    for k in list(keys or []):
        if k in data:
            del data[k]; changed = True
    if changed: _save(data)

def _get_playlist_id(adapter, *, create_if_missing: bool) -> Optional[str]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    name = cfg.watchlist_playlist_name
    pid = find_playlist_id_by_name(http, uid, name)
    if pid: return pid
    if not create_if_missing: return None
    pid = create_playlist(http, uid, name, is_public=False)
    if pid: _log(f"created playlist '{name}' -> {pid}")
    return pid

def _get_collection_id(adapter, *, create_if_missing: bool) -> Optional[str]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    name = cfg.watchlist_playlist_name

    cid = find_collection_id_by_name(http, uid, name)
    if cid:
        return cid

    if not create_if_missing:
        return None

    seed = None
    for t in ("Movie", "Series"):
        try:
            r = http.get(f"/Users/{uid}/Items", params={"IncludeItemTypes": t, "Recursive": True, "Limit": 1})
            if getattr(r, "status_code", 0) == 200:
                arr = (r.json() or {}).get("Items") or []
                if arr and arr[0].get("Id"):
                    seed = str(arr[0]["Id"])
                    break
        except Exception:
            pass

    created = create_collection(http, name, [seed] if seed else None)
    if created:
        _log(f"created collection '{name}' -> {created}")
        return created

    cid = find_collection_id_by_name(http, uid, name)
    if cid:
        _log(f"created collection '{name}' (post-lookup) -> {cid}")
        return cid

    _log(f"create collection failed for '{name}' (no id returned)")
    return None

def _is_episode(obj: Mapping[str, Any]) -> bool:
    t = (obj.get("Type") or obj.get("type") or "").strip().lower()
    return t in ("episode",)


def _is_movie_or_show(obj: Mapping[str, Any]) -> bool:
    t = (obj.get("Type") or obj.get("type") or "").strip().lower()
    return t in ("movie", "show", "series")


def _is_collections_mode(cfg) -> bool:
    m = str(getattr(cfg, "watchlist_mode", "") or "").strip().lower()
    return m in ("collection", "collections")


# ---------- index ----------
def build_index(adapter) -> Dict[str, Dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("watchlist") if callable(prog_mk) else None

    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id

    if cfg.watchlist_mode == "playlist":
        name = cfg.watchlist_playlist_name
        pid = _get_playlist_id(adapter, create_if_missing=False)
        out: Dict[str, Dict[str, Any]] = {}
        if pid:
            out = playlist_as_watchlist_index(
                http, uid, pid,
                limit=max(1, int(getattr(cfg, "watchlist_query_limit", 1000))),
                progress=prog
            )
        _thaw_if_present(out.keys())
        _log(f"index size: {len(out)} (playlist:{name})")
        return out

    if _is_collections_mode(cfg):
        name = cfg.watchlist_playlist_name
        cid = _get_collection_id(adapter, create_if_missing=False)
        out: Dict[str, Dict[str, Any]] = {}
        if cid:
            body = get_collection_items(http, uid, cid)
            rows: List[Mapping[str, Any]] = body.get("Items") or []
            total = int(body.get("TotalRecordCount") or len(rows) or 0)
            if prog:
                try: prog.tick(0, total=total, force=True)
                except Exception: pass
            done = 0
            for row in rows:
                if not _is_movie_or_show(row):
                    done += 1
                    if prog:
                        try: prog.tick(done, total=total)
                        except Exception: pass
                    continue
                try:
                    m = emby_normalize(row)
                    out[canonical_key(m)] = m
                except Exception:
                    pass
                done += 1
                if prog:
                    try: prog.tick(done, total=total)
                    except Exception: pass
        _thaw_if_present(out.keys())
        _log(f"index size: {len(out)} (collections:{name})")
        return out

    r = http.get(f"/Users/{uid}/Items", params={
        "IncludeItemTypes": "Movie,Series",
        "Recursive": True,
        "EnableUserData": True,
        "Fields": "ProviderIds,ProductionYear,UserData,Type",
        "Filters": "IsFavorite",
        "SortBy": "DateLastSaved",
        "SortOrder": "Descending",
        "EnableTotalRecordCount": True,
        "Limit": max(1, int(getattr(cfg, "watchlist_query_limit", 1000))),
    })

    out: Dict[str, Dict[str, Any]] = {}
    rows: List[Mapping[str, Any]] = []
    total = 0
    try:
        body = r.json() or {}
        rows = body.get("Items") or []
        total = int(body.get("TotalRecordCount") or len(rows) or 0)
    except Exception:
        rows, total = [], 0

    if prog:
        try: prog.tick(0, total=total, force=True)
        except Exception: pass

    done = 0
    for row in rows:
        try:
            m = emby_normalize(row)
            out[canonical_key(m)] = m
        except Exception:
            pass
        done += 1
        if prog:
            try: prog.tick(done, total=total)
            except Exception: pass

    _thaw_if_present(out.keys())
    _log(f"index size: {len(out)} (favorites)")
    return out


# ---------- writes ----------

def _favorite(http, uid: str, item_id: str, flag: bool) -> bool:
    try:
        r = http.post(f"/Users/{uid}/FavoriteItems/{item_id}") if flag else http.delete(f"/Users/{uid}/FavoriteItems/{item_id}")
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False


def _verify_favorite(http, uid: str, iid: str, expect: bool, *, retries: int = 3, delay_ms: int = 150) -> bool:
    for attempt in range(max(1, retries)):
        try:
            r = http.get(f"/Users/{uid}/Items/{iid}", params={"Fields": "UserData", "EnableUserData": True})
            if getattr(r, "status_code", 0) == 200:
                ud = ((r.json() or {}).get("UserData") or {})
                val = bool(ud.get("IsFavorite"))
                _log(f"verify item={iid} IsFavorite={val} expect={expect} (attempt {attempt+1})")
                if val is expect: return True
            else:
                r2 = http.get(f"/Users/{uid}/Items", params={"Ids": iid, "Fields": "UserData", "EnableUserData": True})
                if getattr(r2, "status_code", 0) == 200:
                    arr = (r2.json() or {}).get("Items") or []
                    if not arr and expect is False:
                        _log("verify fallback: no item returned, treating as not-favorite OK")
                        return True
                    if arr:
                        ud = (arr[0].get("UserData") or {})
                        val = bool(ud.get("IsFavorite"))
                        _log(f"verify fallback item={iid} IsFavorite={val} expect={expect} (attempt {attempt+1})")
                        if val is expect: return True
        except Exception:
            pass
        if attempt + 1 < retries: sleep_ms(delay_ms)
    return False


def _filter_watchlist_items(items: Iterable[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
    out: List[Mapping[str, Any]] = []
    for it in items or []:
        t = (it.get("type") or "").strip().lower()
        if t in ("movie", "show", "series"):  # include series bt not needed
            out.append(it)
    return out

def _add_favorites(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    frozen = set((_load() or {}).keys())
    ok, unresolved = 0, []

    for it in items or []:
        t = (it.get("type") or "").strip().lower()
        if t not in ("movie", "show", "series", "episode"):
            continue
        k = canonical_key(id_minimal(it))
        if k in frozen:
            continue
        iid = resolve_item_id(adapter, it)
        if not iid:
            _freeze(it, reason="resolve_failed"); continue

        try:
            r = http.get(f"/Users/{uid}/Items/{iid}", params={"Fields": "UserData", "EnableUserData": True})
            if getattr(r, "status_code", 0) == 200 and bool(((r.json() or {}).get("UserData") or {}).get("IsFavorite")):
                _thaw_if_present([k, canonical_key({"ids": {"emby": iid}})]); continue
        except Exception:
            pass

        if not (mark_favorite(http, uid, iid, True) or _favorite(http, uid, iid, True)):
            unresolved.append({"item": id_minimal(it), "hint": "favorite_failed"})
            _freeze(it, reason="write_failed"); sleep_ms(delay); continue

        if not _verify_favorite(http, uid, iid, True) and not update_userdata(http, uid, iid, {"IsFavorite": True}):
            unresolved.append({"item": id_minimal(it), "hint": "verify_failed"})
            _freeze(it, reason="verify_or_userdata_failed"); sleep_ms(delay); continue

        ok += 1
        _thaw_if_present([k, canonical_key({"ids": {"emby": iid}})])
        sleep_ms(delay)

    return ok, unresolved

def _remove_favorites(adapter, items):
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    items = _filter_watchlist_items(items)
    ok = 0; unresolved = []; delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    for it in items:
        iid = resolve_item_id(adapter, it)
        if not iid:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"}); _freeze(it, reason="resolve_failed"); continue

        wrote = (mark_favorite(http, uid, iid, False) or _favorite(http, uid, iid, False))
        if not wrote:
            unresolved.append({"item": id_minimal(it), "hint": "unfavorite_failed"}); _freeze(it, reason="write_failed"); sleep_ms(delay); continue

        if not _verify_favorite(http, uid, iid, False):
            forced = update_userdata(http, uid, iid, {"IsFavorite": False})
            _log(f"force-unfavorite item={iid} forced={forced}")
            if not forced:
                unresolved.append({"item": id_minimal(it), "hint": "verify_failed"}); _freeze(it, reason="verify_or_userdata_failed"); sleep_ms(delay); continue

        ok += 1; _thaw_if_present([canonical_key(id_minimal(it))]); sleep_ms(delay)
    return ok, unresolved

def _add_playlist(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    pid = _get_playlist_id(adapter, create_if_missing=True)
    if not pid:
        return 0, [{"item": {}, "hint": "playlist_missing"}]

    body_now = get_playlist_items(http, pid, start=0, limit=10000)
    rows_now = body_now.get("Items") or []
    existing_ids = {str(r.get("Id")) for r in rows_now if r.get("Id")}
    existing_keys = {emby_key_of(r) for r in rows_now if emby_key_of(r)}
    froz = _load() or {}

    to_add: List[str] = []
    meta: Dict[str, Optional[str]] = {}
    unresolved: List[Dict[str, Any]] = []

    for it in _filter_watchlist_items(items):
        t = (it.get("type") or "").strip().lower()
        k = canonical_key(id_minimal(it))
        if k in froz and (froz[k] or {}).get("reason") != "future_episode":
            continue
        if k in existing_keys:
            _thaw_if_present([k]); continue

        if t == "movie":
            iid = resolve_item_id(adapter, it)
            if not iid: _freeze(it, reason="resolve_failed"); continue
            if iid in existing_ids: _thaw_if_present([k, canonical_key({"ids":{"emby":iid}})]); continue
            to_add.append(iid); meta[iid] = k
            continue

        if t == "episode":
            if _is_future_episode(it): _freeze(it, reason="future_episode"); continue
            iid = resolve_item_id(adapter, it)
            if not iid: _freeze(it, reason="resolve_failed"); continue
            if iid in existing_ids: _thaw_if_present([k, canonical_key({"ids":{"emby":iid}})]); continue
            to_add.append(iid); meta[iid] = k
            continue

        if t in ("show","series"):
            sid = resolve_item_id(adapter, {"type":"show","title":it.get("title"),"year":it.get("year"),"ids":it.get("ids",{})})
            if not sid: _freeze(it, reason="resolve_failed"); continue
            eps = get_series_episodes(http, uid, sid, start=0, limit=10000).get("Items") or []
            eps = [ep for ep in eps if not _is_future_episode(ep)]
            if not eps:
                _freeze(it, reason="future_episode")
                continue
            added_any = False
            for ep in eps:
                eid = str(ep.get("Id") or "")
                if not eid or eid in existing_ids: continue
                to_add.append(eid); meta[eid] = k; added_any = True
            if not added_any:
                _thaw_if_present([k])
            continue

    if not to_add:
        return 0, unresolved

    ok = 0
    uniq = []
    seen = set()
    for iid in to_add:
        if iid not in seen and iid not in existing_ids:
            uniq.append(iid); seen.add(iid)

    for chunk in chunked(uniq, qlim):
        if not playlist_add_items(http, pid, uid, chunk):
            unresolved.extend({"item": {}, "hint": "playlist_add_failed"} for _ in chunk)
            sleep_ms(delay); continue
        body_after = get_playlist_items(http, pid, start=0, limit=10000)
        after_ids = {str(r.get("Id")) for r in (body_after.get("Items") or []) if r.get("Id")}
        for iid in chunk:
            if iid in after_ids:
                ok += 1
                k = meta.get(iid)
                keys = [canonical_key({"ids": {"emby": iid}})]
                if k: keys.append(k)
                _thaw_if_present(keys)
            else:
                _freeze({"ids": {"emby": iid}}, reason="playlist_ignored")
        sleep_ms(delay)

    return ok, unresolved

def _remove_playlist(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    pid = _get_playlist_id(adapter, create_if_missing=False)
    if not pid: return 0, [{"item": {}, "hint": "playlist_missing"}]

    body = get_playlist_items(http, pid, start=0, limit=10000)
    rows = body.get("Items") or []
    entry_by_key: Dict[str, List[str]] = {}
    series_cache: Dict[str, Optional[Dict[str, Any]]] = {}

    for row in rows:
        t = (row.get("Type") or row.get("type") or "").strip().lower()
        entry_id = row.get("PlaylistItemId") or row.get("playlistitemid") or row.get("Id")
        if not entry_id:
            continue
        if t == "movie":
            key = emby_key_of(row)
            if key: entry_by_key.setdefault(key, []).append(str(entry_id))
        elif t == "episode":
            m = _series_minimal_from_episode(http, uid, row, series_cache)
            if m:
                key = canonical_key(m)
                entry_by_key.setdefault(key, []).append(str(entry_id))

    items = _filter_watchlist_items(items)
    eids: List[str] = []; unresolved: List[Dict[str, Any]] = []
    for it in items:
        k = canonical_key(id_minimal(it))
        found = entry_by_key.get(k)
        if found:
            eids.extend(found)
        else:
            unresolved.append({"item": id_minimal(it), "hint": "no_entry_id"})
            _freeze(it, reason="resolve_failed")
    ok = 0
    for chunk in chunked(eids, qlim):
        if playlist_remove_entries(http, pid, chunk):
            ok += len(chunk)
            _thaw_if_present([k for k, v in entry_by_key.items() if v in set(chunk)])
        else:
            for _ in chunk: unresolved.append({"item": {}, "hint": "playlist_remove_failed"})
        sleep_ms(delay)
    return ok, unresolved

def _add_collections(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    # Seed-create collection with first resolved item, then add the rest
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    name = cfg.watchlist_playlist_name

    items = _filter_watchlist_items(items)
    mids: List[str] = []
    unresolved: List[Dict[str, Any]] = []
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
            return 0, unresolved + [{"item": {}, "hint": "no_items_to_seed_collection"}]
        seed = [mids[0]]
        cid = create_collection(http, name, initial_ids=seed)
        if not cid:
            return 0, unresolved + [{"item": {}, "hint": "collection_create_failed"}]
        mids = mids[1:]

    ok = 0
    for chunk in chunked(mids, qlim):
        if collection_add_items(http, cid, chunk):
            ok += len(chunk)
        else:
            for _ in chunk:
                unresolved.append({"item": {}, "hint": "collection_add_failed"})
        sleep_ms(delay)

    if ok:
        _thaw_if_present([canonical_key({"ids": {"emby": x}}) for x in (mids or [])])
    return ok, unresolved

def _remove_collections(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    cfg, http, uid = adapter.cfg, adapter.client, adapter.cfg.user_id
    qlim = int(getattr(cfg, "watchlist_query_limit", 25)) or 25
    delay = int(getattr(cfg, "watchlist_write_delay_ms", 0))
    cid = _get_collection_id(adapter, create_if_missing=False)
    if not cid: return 0, [{"item": {}, "hint": "collection_missing"}]

    body = get_collection_items(http, uid, cid)
    rows = [r for r in (body.get("Items") or []) if _is_movie_or_show(r)]
    by_key: Dict[str, str] = {emby_key_of(r): str(r.get("Id")) for r in rows if emby_key_of(r)}

    items = _filter_watchlist_items(items)
    rm_ids: List[str] = []; unresolved: List[Dict[str, Any]] = []
    for it in items:
        k = canonical_key(id_minimal(it))
        iid = by_key.get(k) or resolve_item_id(adapter, it)
        if iid: rm_ids.append(iid)
        else:
            unresolved.append({"item": id_minimal(it), "hint": "no_collection_item"})
            _freeze(it, reason="resolve_failed")

    ok = 0
    for chunk in chunked(rm_ids, qlim):
        if collection_remove_items(http, cid, chunk):
            ok += len(chunk)
            _thaw_if_present([canonical_key({"ids": {"emby": x}}) for x in chunk])
        else:
            for _ in chunk: unresolved.append({"item": {}, "hint": "collection_remove_failed"})
        sleep_ms(delay)
    return ok, unresolved


def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    cfg = adapter.cfg
    if cfg.watchlist_mode == "playlist":
        ok, unresolved = _add_playlist(adapter, items)
    elif _is_collections_mode(cfg):
        ok, unresolved = _add_collections(adapter, items)
    else:
        ok, unresolved = _add_favorites(adapter, items)
    _log(f"add done: +{ok} / unresolved {len(unresolved)} (mode={cfg.watchlist_mode})")
    return ok, unresolved


def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    cfg = adapter.cfg
    if cfg.watchlist_mode == "playlist":
        ok, unresolved = _remove_playlist(adapter, items)
    elif _is_collections_mode(cfg):
        ok, unresolved = _remove_collections(adapter, items)
    else:
        ok, unresolved = _remove_favorites(adapter, items)
    _log(f"remove done: -{ok} / unresolved {len(unresolved)} (mode={cfg.watchlist_mode})")
    return ok, unresolved
