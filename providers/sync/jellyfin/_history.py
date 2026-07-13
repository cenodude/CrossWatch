# /providers/sync/jellyfin/_history.py
# JELLYFIN Module for history sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from ._common import (
    state_file,
    chunked,
    jf_get_library_roots,
    jf_resolve_library_id,
    jf_scope_history,
    make_logger,
    normalize as jelly_normalize,
    _now_iso_z,
    resolve_item_id,
    sleep_ms,
    _pair_scope,
    _is_capture_mode,
)
from ._routes import items as items_route, played as played_route, user_data as user_data_route, user_params
from cw_platform.id_map import canonical_key, minimal as id_minimal

def _unresolved_path() -> str:
    return str(state_file("jellyfin_history.unresolved.json"))

def _shadow_path() -> str:
    return str(state_file("jellyfin_history.shadow.json"))

def _blackbox_path() -> str:
    return str(state_file("jellyfin_history.jellyfin.blackbox.json"))

def _legacy_blackbox_path() -> str:
    return str(state_file("jellyfin_history.jellyfin-plex.blackbox.json"))

_dbg, _info, _warn = make_logger("history")


# unresolved
def _unres_load() -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    try:
        with open(_unresolved_path(), "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _unres_save(obj: Mapping[str, Any]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        path = _unresolved_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        pass


def _freeze(item: Mapping[str, Any], *, reason: str) -> None:
    key = canonical_key(item)
    data = _unres_load()
    ent = data.get(key) or {"feature": "history", "attempts": 0}
    ent.update({"hint": id_minimal(item)})
    ent["attempts"] = int(ent.get("attempts", 0)) + 1
    ent["reason"] = reason
    data[key] = ent
    _unres_save(data)


def _thaw_if_present(keys: Iterable[str]) -> None:
    data = _unres_load()
    changed = False
    for k in list(keys or []):
        if k in data:
            data.pop(k, None)
            changed = True
    if changed:
        _unres_save(data)


# shadow
def _shadow_load() -> dict[str, dict[str, Any]]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    try:
        with open(_shadow_path(), "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
            out: dict[str, dict[str, Any]] = {}
            if isinstance(raw, Mapping):
                for k, v in raw.items():
                    key = str(k)
                    if isinstance(v, Mapping):
                        cnt = int(v.get("count") or v.get("c") or v.get("n") or 0)
                        iid = str(v.get("iid") or v.get("item_id") or "").strip() or None
                        upd = str(v.get("updated") or v.get("ts") or "").strip() or None
                        hint = v.get("hint") if isinstance(v.get("hint"), Mapping) else None
                        out[key] = {"count": max(0, cnt), "iid": iid, "updated": upd, "hint": hint}
                    else:
                        try:
                            out[key] = {"count": max(0, int(v))}
                        except Exception:
                            out[key] = {"count": 0}
            return out
    except Exception:
        return {}


def _shadow_save(d: Mapping[str, Mapping[str, Any]]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        path = _shadow_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(dict(d or {}), f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        pass


# blackbox
def _bb_write(path: str, d: Mapping[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(dict(d or {}), f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, path)


def _migrate_blackbox() -> None:
    legacy = _legacy_blackbox_path()
    if not os.path.exists(legacy):
        return
    new = _blackbox_path()
    try:
        try:
            with open(legacy, "r", encoding="utf-8") as f:
                old_data = json.load(f) or {}
        except Exception:
            old_data = {}
        new_data: dict[str, Any] = {}
        if os.path.exists(new):
            try:
                with open(new, "r", encoding="utf-8") as f:
                    new_data = json.load(f) or {}
            except Exception:
                new_data = {}
        merged = {**(old_data if isinstance(old_data, dict) else {}), **(new_data if isinstance(new_data, dict) else {})}
        _bb_write(new, merged)
        try:
            os.remove(legacy)
        except Exception:
            pass
    except Exception:
        pass


def _bb_load() -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    _migrate_blackbox()
    try:
        with open(_blackbox_path(), "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _bb_save(d: Mapping[str, Any]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        _bb_write(_blackbox_path(), d)
    except Exception:
        pass


# cfg helpers
def _history_limit(adapter: Any) -> int:
    cfg = getattr(adapter, "cfg", None)
    v = getattr(cfg, "history_query_limit", None)
    if v is None:
        v = getattr(cfg, "watchlist_query_limit", 1000)
    try:
        return max(1, int(v))
    except Exception:
        return 1000

def _history_page_size(adapter: Any) -> int:
    env = (os.environ.get("CW_JELLYFIN_HISTORY_PAGE_SIZE") or "").strip()
    if env:
        try:
            v = int(env)
            return max(50, min(2000, v))
        except Exception:
            pass

    base = int(_history_limit(adapter) or 25)
    if base < 100:
        return 500
    return max(50, min(2000, base))


def _history_delay_ms(adapter: Any) -> int:
    cfg = getattr(adapter, "cfg", None)
    v = getattr(cfg, "history_write_delay_ms", None)
    if v is None:
        v = getattr(cfg, "watchlist_write_delay_ms", 0)
    try:
        return max(0, int(v))
    except Exception:
        return 0


# time utils
def _parse_iso_to_epoch(s: str | None) -> int | None:
    if not s:
        return None
    try:
        t = s.strip()
        if t.endswith("Z"):
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(t)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def _epoch_to_iso_z(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")



# Deep lookup
_lib_anc_cache: dict[str, str | None] = {}


def _lib_id_via_ancestors(http: Any, iid: str, roots: Mapping[str, Any]) -> str | None:
    if not iid:
        return None
    if iid in _lib_anc_cache:
        return _lib_anc_cache[iid]
    try:
        r = http.get(f"/Items/{iid}/Ancestors", params={"Fields": "Id"})
        if getattr(r, "status_code", 0) == 200:
            root_keys = {str(k) for k in roots.keys()}
            for a in (r.json() or []):
                aid = str((a or {}).get("Id") or "")
                if aid in root_keys:
                    _lib_anc_cache[iid] = aid
                    return aid
    except Exception:
        pass
    _lib_anc_cache[iid] = None
    return None


# Series IDs fetcher
_SERIES_META_CACHE: dict[str, dict[str, Any]] = {}


def _prefetch_series_meta(http: Any, uid: str, series_ids: Iterable[str]) -> None:
    missing: list[str] = []
    for sid in series_ids or []:
        ss = str(sid or '').strip()
        if ss and ss not in _SERIES_META_CACHE and ss not in missing:
            missing.append(ss)
    if not missing:
        return
    for batch in chunked(missing, 100):
        ids = ','.join(str(x) for x in batch if x)
        if not ids:
            continue
        try:
            r = http.get(
                items_route(),
                params=user_params(uid, {
                    'Ids': ids,
                    'Fields': 'ProviderIds,ProductionYear,Type,Name',
                }),
            )
            if getattr(r, 'status_code', 0) != 200:
                continue
            body = r.json() or {}
            for row in (body.get('Items') or []):
                if not isinstance(row, Mapping):
                    continue
                rid = str(row.get('Id') or '').strip()
                if rid:
                    _SERIES_META_CACHE[rid] = dict(row)
            for sid in batch:
                ss = str(sid or '').strip()
                if ss and ss not in _SERIES_META_CACHE:
                    _SERIES_META_CACHE[ss] = {}
        except Exception:
            continue


def _series_ids_for(http: Any, uid: str, series_id: str | None) -> dict[str, str]:
    sid = (str(series_id or '').strip()) or ''
    if not sid:
        return {}
    if sid not in _SERIES_META_CACHE:
        _prefetch_series_meta(http, uid, [sid])
    body = _SERIES_META_CACHE.get(sid) or {}
    pids = (body.get('ProviderIds') or {}) if isinstance(body, Mapping) else {}
    out: dict[str, str] = {}
    items: Iterable[tuple[Any, Any]]
    if isinstance(pids, Mapping):
        items = pids.items()
    else:
        items = ()
    for k, v in items:
        kl = str(k).lower()
        sv = str(v).strip()
        if not sv:
            continue
        if kl == 'imdb':
            out['imdb'] = sv if sv.startswith('tt') else f"tt{sv}"
        elif kl == 'tmdb':
            try:
                out['tmdb'] = str(int(sv))
            except Exception:
                pass
        elif kl == 'tvdb':
            try:
                out['tvdb'] = str(int(sv))
            except Exception:
                pass
    return out


# low-level Jellyfin writes
def _mark_played(http: Any, uid: str, item_id: str, *, date_played_iso: str | None) -> bool:
    try:
        params = {"datePlayed": date_played_iso} if date_played_iso else None
        r = http.post(played_route(item_id), params=user_params(uid, params))
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False


def _unmark_played(http: Any, uid: str, item_id: str) -> bool:
    try:
        r = http.delete(played_route(item_id), params=user_params(uid))
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False


def _normalize_watched(http: Any, uid: str, item_id: str, *, date_played_iso: str | None) -> bool:
    payload: dict[str, Any] = {"Played": True, "PlaybackPositionTicks": 0}
    if date_played_iso:
        payload["LastPlayedDate"] = date_played_iso
    try:
        r = http.post(user_data_route(str(item_id)), params=user_params(uid), json=payload)
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False


_ST_WRITTEN = "written"
_ST_ALREADY = "already_correct"
_ST_NEWER = "destination_newer"
_ST_WRITE_FAILED = "write_failed"
_ST_MISSING_DATE = "missing_watched_at"
_ST_RESOLVE_FAILED = "resolve_failed"
_ST_READBACK = "readback_mismatch"


def _set_write_meta(adapter: Any, meta: Mapping[str, Any]) -> None:
    try:
        setattr(adapter, "_history_write_meta", dict(meta))
    except Exception:
        pass


def _result_row(key: str, status: str, *, action: str, reason: str, iid: str | None = None, req_ts: int = 0, dst_ts: int = 0) -> dict[str, Any]:
    row: dict[str, Any] = {"key": key, "status": status, "action": action, "reason": reason}
    if iid:
        row["provider_item_id"] = str(iid)
    if req_ts:
        row["requested_at"] = _epoch_to_iso_z(req_ts)
    if dst_ts:
        row["destination_at"] = _epoch_to_iso_z(dst_ts)
    return row


def _dst_user_state(http: Any, uid: str, iid: str) -> tuple[bool, int]:
    # Delegate to batch variant to keep parsing consistent
    return _dst_user_states(http, uid, [str(iid)]).get(str(iid), (False, 0))




def _dst_user_states(http: Any, uid: str, iids: Iterable[str]) -> dict[str, tuple[bool, int]]:
    out: dict[str, tuple[bool, int]] = {}
    ids: list[str] = []
    seen: set[str] = set()
    for x in iids or []:
        s_id = str(x or '').strip()
        if s_id and s_id not in seen:
            seen.add(s_id)
            ids.append(s_id)
    if not ids:
        return out
    for batch in chunked(ids, 100):
        qids = ','.join(str(x) for x in batch if x)
        if not qids:
            continue
        try:
            r = http.get(
                items_route(),
                params={
                    'userId': uid,
                    'Ids': qids,
                    'Fields': 'UserData',
                    'Recursive': 'false',
                },
            )
            if getattr(r, 'status_code', 0) != 200:
                continue
            body = r.json() or {}
            for row in (body.get('Items') or []):
                if not isinstance(row, Mapping):
                    continue
                iid = str(row.get('Id') or '').strip()
                if not iid:
                    continue
                ud = row.get('UserData') or {}
                played = bool(ud.get('Played') or ud.get('IsPlayed'))
                ts = 0
                for k in ('LastPlayedDate', 'DateLastPlayed', 'LastPlayed'):
                    v = ud.get(k) or row.get(k)
                    if v:
                        ts = _parse_iso_to_epoch(v) or 0
                        if ts:
                            break
                out[iid] = (played, ts)
        except Exception:
            continue
    return out


# event index (watched_at)
def build_index(
    adapter: Any,
    since: Any | None = None,
    limit: int | None = None,
) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history") if callable(prog_mk) else None
    http = adapter.client
    uid = adapter.cfg.user_id
    _SERIES_META_CACHE.clear()
    page_size = _history_page_size(adapter)
    allow_deep = (os.environ.get("CW_JELLYFIN_HISTORY_DEEP_LOOKUP") or "").strip().lower() == "true"
    since_epoch = 0
    if isinstance(since, (int, float)):
        since_epoch = int(since)
    elif isinstance(since, str):
        since_epoch = int(_parse_iso_to_epoch(since) or 0)

    scope_params = jf_scope_history(adapter.cfg) or {}
    scope_libs: list[str] = []
    if isinstance(scope_params, Mapping):
        pid = scope_params.get("ParentId") or scope_params.get("parentId")
        if pid:
            scope_libs = [str(pid)]
        else:
            anc = scope_params.get("ParentIds") or scope_params.get("parentIds")
            if isinstance(anc, (list, tuple)):
                scope_libs = [str(x) for x in anc if x]

    roots = jf_get_library_roots(adapter)
    if roots:
        _dbg("index_fetch_counts", source="library_roots", roots=list(sorted(roots.keys())))

    start = 0
    query_parents: list[str | None] = []
    query_parents.extend(sorted(scope_libs))
    if not query_parents:
        query_parents = [None]
    scope_index = 0
    seen_pages: set[tuple[str, ...]] = set()
    events: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
    page = 0

    while True:
        t0 = time.monotonic()
        params: dict[str, Any] = {
            "SortBy": "DatePlayed",
            "SortOrder": "Descending",
            "IncludeItemTypes": "Movie,Episode",
            "Recursive": "true",
            "Filters": "IsPlayed",
            "Fields": (
                "ProviderIds,Path,ParentId,LibraryId,AncestorIds,"
                "Name,SeriesName,SeriesId,IndexNumber,ParentIndexNumber,UserData"
            ),
            "EnableImages": "false",
            "EnableTotalRecordCount": "false",
            "StartIndex": start,
            "Limit": page_size,
        }

        current_parent = query_parents[scope_index]
        if current_parent:
            params["ParentId"] = current_parent

        r = http.get(items_route(), params=user_params(uid, params))
        body = r.json() or {}
        rows = body.get("Items") or []
        raw_count = len(rows)
        signature = tuple(str(row.get("Id") or "") for row in rows if isinstance(row, Mapping))
        if rows and signature in seen_pages:
            _warn("pagination_repeated_page", source_library_id=current_parent, start_index=start)
            rows = []
            raw_count = 0
        seen_pages.add(signature)
        page += 1
        took_ms = int((time.monotonic() - t0) * 1000)
        _dbg("index_fetch_counts", source="page", page=page, start=start, got=len(rows), limit=page_size, latency_ms=took_ms)
        if not rows:
            scope_index += 1
            if scope_index >= len(query_parents):
                break
            start = 0
            seen_pages.clear()
            continue

        series_ids: set[str] = set()
        for r0 in rows:
            if (r0.get('Type') or '').strip() == 'Episode':
                sid = r0.get('SeriesId')
                if sid:
                    series_ids.add(str(sid))
        if series_ids:
            _prefetch_series_meta(http, uid, sorted(series_ids))

        for row in rows:
            ud = row.get("UserData") or {}
            lp = ud.get("LastPlayedDate") or row.get("DateLastPlayed") or None
            ts = _parse_iso_to_epoch(lp) or 0
            if not ts:
                continue
            if since_epoch and ts <= since_epoch:
                rows = []
                break

            m = jelly_normalize(row)
            lib_id = jf_resolve_library_id(
                row,
                roots,
                scope_libs,
                http,
                allow_deep_lookup=allow_deep,
            )

            m = dict(m)
            m["library_id"] = lib_id

            watched_at = _epoch_to_iso_z(ts)
            typ = (row.get("Type") or "").strip()

            if typ == "Movie":
                event: dict[str, Any] = {
                    "type": "movie",
                    "ids": dict(m.get("ids") or {}),
                    "title": m.get("title"),
                    "year": m.get("year"),
                    "watched_at": watched_at,
                    "watched": True,
                }
            elif typ == "Episode":
                show_ids = _series_ids_for(http, uid, row.get("SeriesId"))
                s = m.get("season")
                e = m.get("episode")
                try:
                    s_i = int(s) if s is not None else 0
                    e_i = int(e) if e is not None else 0
                except Exception:
                    s_i = 0
                    e_i = 0
                ep_title = f"S{s_i:02d}E{e_i:02d}" if s_i > 0 and e_i > 0 else (m.get("title") or row.get("Name"))
                event = {
                    "type": "episode",
                    "ids": dict(m.get("ids") or {}),
                    "title": ep_title,
                    "show_ids": show_ids,
                    "season": m.get("season"),
                    "episode": m.get("episode"),
                    "series_title": m.get("series_title") or row.get("SeriesName"),
                    "watched_at": watched_at,
                    "watched": True,
                }
            else:
                continue

            lib_id = m.get("library_id")
            if lib_id:
                event["library_id"] = lib_id

            base_key = canonical_key(m)
            event.setdefault("_cw_key", base_key)
            jf_iid = m.get("jellyfin_item_id")
            if jf_iid:
                event.setdefault("jellyfin_item_id", str(jf_iid))

            ev_key = f"{base_key}@{ts}"
            out_ev = dict(event)
            if lib_id:
                out_ev["library_id"] = lib_id
            events.append((ts, {"key": ev_key}, out_ev))

        start += raw_count
        if isinstance(limit, int) and limit > 0 and len(events) >= int(limit):
            break
        total = int(body.get("TotalRecordCount") or 0)
        if not rows or raw_count < page_size or (total and start >= total):
            scope_index += 1
            if scope_index >= len(query_parents):
                break
            start = 0
            seen_pages.clear()

    events.sort(key=lambda x: x[0], reverse=True)
    if isinstance(limit, int) and limit > 0:
        events = events[: int(limit)]

    out: dict[str, dict[str, Any]] = {}
    for _, meta, ev in events:
        key = meta["key"]
        cur = out.get(key)
        if cur is None or (cur.get("watched_at") or "") < (ev.get("watched_at") or ""):
            out[key] = ev

    event_bases: set[str] = set()
    for ek in out.keys():
        event_bases.add(ek.split("@", 1)[0])

    shadow = _shadow_load()
    if shadow:
        added = 0
        for k, meta in shadow.items():
            if k in event_bases or k in out:
                continue
            rec = out.setdefault(k, {"watched": True})
            if isinstance(rec, dict):
                rec.setdefault("_cw_key", k)
                rec.setdefault("_cw_presence", True)
                if isinstance(meta, Mapping):
                    iid = meta.get("iid")
                    if iid:
                        rec.setdefault("jellyfin_item_id", str(iid))
                    hint = meta.get("hint")
                    if isinstance(hint, Mapping):
                        for hk in ("type", "title", "year", "season", "episode", "series_title", "ids", "show_ids"):
                            if hk in hint and rec.get(hk) in (None, ""):
                                rec[hk] = hint.get(hk)
            added += 1
        if added:
            _dbg("cache_merged", source="shadow", added=added)

    bb = _bb_load()
    if bb:
        ttl = int(getattr(getattr(adapter, "cfg", None), "blackbox_presence_ttl_seconds", 900) or 900)
        now_ep = int(datetime.now(timezone.utc).timestamp())
        added = 0
        for k, meta in bb.items():
            if k in event_bases or k in out:
                continue
            if isinstance(meta, dict) and str(meta.get("reason", "")).startswith("presence:"):
                since_ep = _parse_iso_to_epoch(meta.get("since")) or 0
                if since_ep and (now_ep - since_ep) <= ttl:
                    rec = out.setdefault(k, {"watched": True})
                    if isinstance(rec, dict):
                        rec.setdefault("_cw_key", k)
                        rec.setdefault("_cw_presence", True)
                        iid = meta.get("iid")
                        if iid:
                            rec.setdefault("jellyfin_item_id", str(iid))
                    added += 1
        if added:
            _dbg("cache_merged", source="blackbox_presence", added=added)

    if os.environ.get("CW_DEBUG") or os.environ.get("CW_JELLYFIN_DEBUG"):
        try:
            cfg_libs = list(
                getattr(adapter.cfg, "history_libraries", None)
                or getattr(adapter.cfg, "libraries", None)
                or [],
            )
        except Exception:
            cfg_libs = []

        lib_counts: dict[str, int] = {}
        for ev in out.values():
            lid = ev.get("library_id") or "NONE"
            s = str(lid)
            lib_counts[s] = lib_counts.get(s, 0) + 1

        _dbg("index_fetch_counts", source="library_distribution", cfg_libraries=cfg_libs, distribution=lib_counts)

    _info("index_done", count=len(out), mode="events+presence")
    return out

# shared write helpers
_COPY_OVER_KEYS: tuple[str, ...] = (
    "type",
    "title",
    "year",
    "watch_type",
    "watched_at",
    "library_id",
    "season",
    "episode",
    "series_title",
    "show_ids",
    "series_year",
)


def _coerce_anime_type(m: dict[str, Any]) -> None:
    t_raw = str(m.get("type") or "").strip().lower()
    if t_raw != "anime":
        return
    if m.get("season") not in (None, "", 0) and m.get("episode") not in (None, "", 0):
        m["type"] = "episode"
    else:
        m["type"] = "show"


def _try_resolve_iid(adapter: Any, m: Mapping[str, Any]) -> str | None:
    try:
        iid = resolve_item_id(adapter, m)
        return str(iid) if iid else None
    except Exception as e:
        _dbg("resolve_miss", error=repr(e))
        return None


def _prepare_want(
    base: Mapping[str, Any],
    *,
    raw_key: str | None = None,
    raw_iid: str | None = None,
) -> tuple[str | None, dict[str, Any] | None, dict[str, Any] | None]:
    base_d: dict[str, Any] = dict(base or {})
    base_ids_raw = base_d.get("ids")
    base_ids = dict(base_ids_raw) if isinstance(base_ids_raw, Mapping) else {}
    has_ids = bool(base_ids) and any(v not in (None, "", 0) for v in base_ids.values())

    nm = jelly_normalize(base_d)
    m: dict[str, Any] = dict(nm)

    if raw_key:
        m["_cw_key"] = raw_key
    if raw_iid:
        m["jellyfin_item_id"] = raw_iid

    if has_ids:
        for key in _COPY_OVER_KEYS:
            if base_d.get(key) not in (None, ""):
                m[key] = base_d[key]

        ids = dict(nm.get("ids") or {})
        for k_id, v_id in base_ids.items():
            if v_id not in (None, "", 0):
                ids[k_id] = v_id
        if ids:
            m["ids"] = ids

    _coerce_anime_type(m)

    key_opt: str | None
    if raw_key:
        key_opt = raw_key
    else:
        try:
            key_opt = canonical_key(m) or canonical_key(base_d)
        except Exception:
            key_opt = None

    if not key_opt:
        _freeze(base_d, reason="missing_ids_for_key")
        return None, None, {"item": id_minimal(base_d), "hint": "missing_ids_for_key"}

    return str(key_opt), m, None


# writes
def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    qlim = int(_history_limit(adapter) or 25)
    delay = _history_delay_ms(adapter)

    tol = int(getattr(adapter.cfg, "history_backdate_tolerance_s", 300) or 300)
    pre_unresolved: list[dict[str, Any]] = []
    wants: dict[str, dict[str, Any]] = {}

    for it in (items or []):
        base = dict(it or {})
        raw_key = str(base.get("_cw_key") or base.get("key") or "").strip() or None
        raw_iid = str(base.get("jellyfin_item_id") or base.get("_jellyfin_item_id") or "").strip() or None
        key, m, err = _prepare_want(base, raw_key=raw_key, raw_iid=raw_iid)
        if err:
            pre_unresolved.append(err)
            continue
        assert key and m
        wants[key] = m

    mids: list[tuple[str, str]] = []
    unresolved: list[dict[str, Any]] = list(pre_unresolved)
    results: list[dict[str, Any]] = []
    confirmed_keys: list[str] = []
    reason_counts: dict[str, int] = {}

    for u in pre_unresolved:
        reason = str(u.get("reason") or u.get("hint") or "unresolved")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        results.append(_result_row(str(u.get("key") or ""), _ST_RESOLVE_FAILED, action="resolve", reason=reason))

    for k, m in wants.items():
        iid = m.get("jellyfin_item_id") or _try_resolve_iid(adapter, m)
        if iid:
            mids.append((k, str(iid)))
        else:
            unresolved.append({"item": id_minimal(m), "key": k, "hint": "resolve_failed", "reason": "resolve_failed"})
            _freeze(m, reason="resolve_failed")
            results.append(_result_row(k, _ST_RESOLVE_FAILED, action="resolve", reason="resolve_failed"))
            reason_counts["resolve_failed"] = reason_counts.get("resolve_failed", 0) + 1

    shadow = _shadow_load()
    bb = _bb_load()
    ok = 0
    stats = {
        "wrote": 0, "forced": 0, "backdated": 0, "skip_newer": 0,
        "skip_played_untimed": 0, "skip_missing_date": 0, "fail_mark": 0,
    }

    def _confirm(k: str, status: str, *, action: str, reason: str, iid: str, req_ts: int, dst_ts: int = 0) -> None:
        confirmed_keys.append(k)
        results.append(_result_row(k, status, action=action, reason=reason, iid=iid, req_ts=req_ts, dst_ts=dst_ts))

    def _fail(k: str, *, hint: str, iid: str | None = None) -> None:
        unresolved.append({"item": id_minimal(wants[k]), "key": k, "hint": hint, "reason": "write_failed"})
        _freeze(wants[k], reason="write_failed")
        results.append(_result_row(k, _ST_WRITE_FAILED, action="write", reason=hint, iid=iid))
        reason_counts["write_failed"] = reason_counts.get("write_failed", 0) + 1

    total = len(mids)
    if total:
        _info("write_start", op="add", count=total)

    processed = 0
    for chunk in chunked(mids, qlim):
        states = _dst_user_states(http, uid, [iid for _, iid in chunk])
        pending_verify: list[tuple[str, str, int]] = []
        for k, iid in chunk:
            src_ts = _parse_iso_to_epoch(wants[k].get("watched_at")) or 0
            if not src_ts:
                unresolved.append({"item": id_minimal(wants[k]), "key": k, "hint": "missing_watched_at", "reason": "missing_watched_at"})
                _freeze(wants[k], reason="missing_watched_at")
                results.append(_result_row(k, _ST_MISSING_DATE, action="skip", reason="missing_watched_at", iid=iid))
                reason_counts["missing_watched_at"] = reason_counts.get("missing_watched_at", 0) + 1
                stats["skip_missing_date"] += 1
                processed += 1
                continue

            src_iso = _epoch_to_iso_z(src_ts)
            played, dst_ts = states.get(iid, (False, 0))

            if played and dst_ts and dst_ts >= (src_ts - tol):
                bb[k] = {"reason": "presence:existing_newer", "since": _now_iso_z(), "iid": iid}
                stats["skip_newer"] += 1
                status = _ST_ALREADY if abs(dst_ts - src_ts) <= tol else _ST_NEWER
                _confirm(k, status, action="skip", reason="existing_newer", iid=iid, req_ts=src_ts, dst_ts=dst_ts)
                processed += 1
                sleep_ms(delay)
                continue

            if played and not dst_ts:
                bb[k] = {"reason": "presence:existing_untimed", "since": _now_iso_z(), "iid": iid}
                stats["skip_played_untimed"] += 1
                _confirm(k, _ST_ALREADY, action="skip", reason="existing_untimed", iid=iid, req_ts=src_ts)
                processed += 1
                sleep_ms(delay)
                continue

            if _mark_played(http, uid, iid, date_played_iso=src_iso):
                ok += 1
                ent = shadow.get(k) or {}
                cur_cnt = int(ent.get("count") or 0) if isinstance(ent, Mapping) else 0
                shadow[k] = {
                    "count": max(0, cur_cnt) + 1,
                    "iid": iid,
                    "updated": _now_iso_z(),
                    "hint": id_minimal(wants[k]),
                }
                bb[k] = {"reason": "presence:shadow", "since": _now_iso_z(), "iid": iid}
                stats["wrote"] += 1
                normalized = _normalize_watched(http, uid, iid, date_played_iso=src_iso)
                _confirm(k, _ST_WRITTEN, action="write", reason="wrote", iid=iid, req_ts=src_ts)
                if not normalized:
                    pending_verify.append((k, iid, src_ts))
            else:
                _fail(k, hint="mark_played_failed", iid=iid)
                stats["fail_mark"] += 1

            processed += 1
            if (processed % 25) == 0:
                _dbg("write_progress", op="add", done=processed, total=total, ok=ok, unresolved=len(unresolved))
            sleep_ms(delay)

        if pending_verify:
            final = _dst_user_states(http, uid, [iid for _, iid, _ in pending_verify])
            for k, iid, req_ts in pending_verify:
                played, _dst = final.get(iid, (False, 0))
                if played:
                    continue
                confirmed_keys[:] = [c for c in confirmed_keys if c != k]
                results[:] = [r for r in results if not (r.get("key") == k and r.get("provider_item_id") == str(iid))]
                unresolved.append({"item": id_minimal(wants[k]), "key": k, "hint": "readback_not_played", "reason": "readback_mismatch"})
                _freeze(wants[k], reason="readback_mismatch")
                results.append(_result_row(k, _ST_READBACK, action="verify", reason="not_played", iid=iid, req_ts=req_ts))
                reason_counts["readback_mismatch"] = reason_counts.get("readback_mismatch", 0) + 1
                shadow.pop(k, None)
                if ok > 0:
                    ok -= 1

    _shadow_save(shadow)
    _bb_save(bb)
    if confirmed_keys:
        _thaw_if_present(confirmed_keys)

    _set_write_meta(adapter, {
        "confirmed_keys": confirmed_keys,
        "unresolved_keys": [str(u.get("key") or "") for u in unresolved if u.get("key")],
        "results": results,
        "reason_counts": reason_counts,
    })
    _info("write_done", op="add", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved), **stats)
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    qlim = int(_history_limit(adapter) or 25)
    delay = _history_delay_ms(adapter)
    force_clear = str(os.getenv("CW_TOOL_CLEAR") or "").strip().lower() in ("1", "true", "yes", "on")
    shadow = _shadow_load()
    pre_unresolved: list[dict[str, Any]] = []
    wants: dict[str, dict[str, Any]] = {}

    for it in (items or []):
        base: dict[str, Any] = dict(it or {})
        if base.get("_cw_tool_clear") or base.get("_cw_force_clear"):
            force_clear = True
        raw_key = str(base.get("_cw_key") or base.get("key") or "").strip() or None
        raw_iid = str(base.get("jellyfin_item_id") or base.get("_jellyfin_item_id") or "").strip() or None
        key, m, err = _prepare_want(base, raw_key=raw_key, raw_iid=raw_iid)
        if err:
            pre_unresolved.append(err)
            continue
        assert key and m
        wants[key] = m

    mids: list[tuple[str, str]] = []
    unresolved: list[dict[str, Any]] = list(pre_unresolved)
    results: list[dict[str, Any]] = []
    confirmed_keys: list[str] = []
    reason_counts: dict[str, int] = {}
    for u in pre_unresolved:
        reason = str(u.get("reason") or u.get("hint") or "unresolved")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        results.append(_result_row(str(u.get("key") or ""), _ST_RESOLVE_FAILED, action="resolve", reason=reason))

    for k, m in wants.items():
        ent = shadow.get(k) or {}
        iid = (m.get("jellyfin_item_id") or (ent.get("iid") if isinstance(ent, Mapping) else None))

        if not iid:
            iid = _try_resolve_iid(adapter, m)

        if iid:
            mids.append((k, str(iid)))
        else:
            unresolved.append({"item": id_minimal(m), "key": k, "hint": "resolve_failed", "reason": "resolve_failed"})
            _freeze(m, reason="resolve_failed")
            results.append(_result_row(k, _ST_RESOLVE_FAILED, action="resolve", reason="resolve_failed"))
            reason_counts["resolve_failed"] = reason_counts.get("resolve_failed", 0) + 1
    ok = 0
    for chunk in chunked(mids, qlim):
        for k, iid in chunk:
            ent = shadow.get(k) or {}
            cur_cnt = int(ent.get("count") or 0) if isinstance(ent, Mapping) else 0
            tracked = (k in shadow) and cur_cnt > 0

            if tracked and cur_cnt > 1 and not force_clear:
                shadow[k] = {
                    **(dict(ent) if isinstance(ent, Mapping) else {}),
                    "count": cur_cnt - 1,
                    "iid": str(ent.get("iid") or iid),
                    "updated": _now_iso_z(),
                }
                confirmed_keys.append(k)
                results.append(_result_row(k, _ST_ALREADY, action="skip", reason="ref_retained", iid=iid))
                sleep_ms(delay)
                continue

            # Force mode (tools/clear) always attempts to unmark.
            if _unmark_played(http, uid, iid):
                ok += 1
                confirmed_keys.append(k)
                results.append(_result_row(k, _ST_WRITTEN, action="write", reason="unmarked", iid=iid))
                if tracked:
                    if force_clear or cur_cnt <= 1:
                        shadow.pop(k, {})
                    else:
                        shadow[k] = {
                            **(dict(ent) if isinstance(ent, Mapping) else {}),
                            "count": cur_cnt - 1,
                            "iid": str(ent.get("iid") or iid),
                            "updated": _now_iso_z(),
                        }
            else:
                unresolved.append({"item": id_minimal(wants[k]), "key": k, "hint": "unmark_played_failed", "reason": "write_failed"})
                _freeze(wants[k], reason="write_failed")
                results.append(_result_row(k, _ST_WRITE_FAILED, action="write", reason="unmark_played_failed", iid=iid))
                reason_counts["write_failed"] = reason_counts.get("write_failed", 0) + 1

                if tracked:
                    shadow[k] = {
                        **(dict(ent) if isinstance(ent, Mapping) else {}),
                        "count": max(1, cur_cnt),
                        "iid": str(ent.get("iid") or iid),
                        "updated": _now_iso_z(),
                    }
            sleep_ms(delay)

    shadow = {k: v for k, v in shadow.items() if int((v or {}).get("count") or 0) > 0}
    _shadow_save(shadow)
    if confirmed_keys:
        _thaw_if_present(confirmed_keys)

    _set_write_meta(adapter, {
        "confirmed_keys": confirmed_keys,
        "unresolved_keys": [str(u.get("key") or "") for u in unresolved if u.get("key")],
        "results": results,
        "reason_counts": reason_counts,
    })
    _info("write_done", op="remove", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
    return ok, unresolved
