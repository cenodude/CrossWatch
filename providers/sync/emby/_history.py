# /providers/sync/emby/_history.py
# EMBY Module for history synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from ._common import (
    state_file,
    chunked,
    emby_scope_history,
    normalize as emby_normalize,
    resolve_item_id,
    _pair_scope,
    _series_minimal_from_episode,
    prefetch_series_minimals,
)
from cw_platform.id_map import canonical_key, minimal as id_minimal
from .._log import log as cw_log

def _unresolved_path() -> str:
    return state_file("emby_history.unresolved.json")

def _shadow_path() -> str:
    return state_file("emby_history.shadow.json")

def _blackbox_path() -> str:
    return state_file("emby_history.emby.blackbox.json")




def _dbg(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "history", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "history", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "history", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "history", "error", msg, **fields)


def sleep_ms(ms: int) -> None:
    try:
        time.sleep(max(0, int(ms)) / 1000.0)
    except Exception:
        pass

# timestamp helpers
def _parse_iso_to_epoch(s: str | None) -> int | None:
    if not s:
        return None
    try:
        t = s.strip()
        if "T" in t and "." in t:
            head, frac = t.split(".", 1)
            tz_pos = next((i for i, c in enumerate(frac) if c in "Z+-"), None)
            frac_only, tz_tail = (frac, "") if tz_pos is None else (frac[:tz_pos], frac[tz_pos:])
            if len(frac_only) > 6:
                frac_only = frac_only[:6]
            t = head + "." + frac_only + tz_tail
        if t.endswith("Z"):
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(t)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        try:
            t2 = re.sub(r"\.\d+", "", t)
            if t2.endswith("Z"):
                dt = datetime.fromisoformat(t2.replace("Z", "+00:00"))
            else:
                dt = datetime.fromisoformat(t2)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return None


def _epoch_to_iso_z(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _epoch_to_emby_dateparam(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y%m%d%H%M%S")


def _now_iso_z() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _played_ts_from_row(row: Mapping[str, Any]) -> int:
    ud = (row.get("UserData") or {}) if isinstance(row, Mapping) else {}
    for v in (
        ud.get("DatePlayed"),
        ud.get("LastPlayedDate"),
        ud.get("LastPlayedAt"),
        row.get("DatePlayed"),
        row.get("DateLastPlayed"),
        row.get("LastPlayedDate"),
    ):
        ts = _parse_iso_to_epoch(v)
        if ts:
            return ts
    return 0


def _played_ts_backfill(http: Any, uid: str, row: Mapping[str, Any]) -> int:
    iid = str(row.get("Id") or "").strip()
    if not iid:
        return 0
    try:
        r = http.get(
            f"/Users/{uid}/Items/{iid}",
            params={"Fields": "UserData", "EnableUserData": True},
        )
        if getattr(r, "status_code", 0) != 200:
            return 0
        body = r.json() or {}
        ud = body.get("UserData") or {}
        ts = _parse_iso_to_epoch(ud.get("LastPlayedDate"))
        return ts or 0
    except Exception:
        return 0



def _prefetch_played_ts(
    http: Any,
    uid: str,
    item_ids: Iterable[Any],
    _cache: dict[str, int],
    *,
    chunk_size: int = 200,
) -> None:
    ids: list[str] = []
    seen = set(_cache.keys())
    for x in item_ids or []:
        s = str(x or '').strip()
        if not s or s in seen:
            continue
        seen.add(s)
        ids.append(s)
    if not ids:
        return
    for batch in chunked(ids, max(1, int(chunk_size))):
        try:
            r = http.get(
                f"/Users/{uid}/Items",
                params={
                    "Ids": ','.join(batch),
                    "Fields": "UserData",
                    "EnableUserData": True,
                },
            )
        except Exception:
            r = None
        if r is None or getattr(r, 'status_code', 0) != 200:
            for iid in batch:
                _cache.setdefault(iid, 0)
            continue
        try:
            arr = (r.json() or {}).get('Items') or []
        except Exception:
            arr = []
        for raw in arr:
            try:
                iid = str((raw or {}).get('Id') or '').strip()
                if not iid:
                    continue
                ts = _played_ts_from_row(raw)
                _cache[iid] = int(ts or 0)
            except Exception:
                pass
        for iid in batch:
            _cache.setdefault(iid, 0)
            
# unresolved tracking
def _unres_load() -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    try:
        with open(_unresolved_path(), "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _unres_save(obj: Mapping[str, Any]) -> None:
    if _pair_scope() is None:
        return
    try:
        path = _unresolved_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
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


# shadow + blackbox
def _shadow_load() -> dict[str, int]:
    if _pair_scope() is None:
        return {}
    try:
        with open(_shadow_path(), "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
            return {str(k): int(v) for k, v in raw.items()}
    except Exception:
        return {}


def _shadow_save(d: Mapping[str, int]) -> None:
    if _pair_scope() is None:
        return
    try:
        path = _shadow_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        pass


def _bb_paths() -> list[str]:
    base = _blackbox_path()
    paths: list[str] = [base]
    try:
        d = os.path.dirname(base) or "."
        for fn in os.listdir(d):
            if fn.startswith("emby_history.emby") and fn.endswith(".blackbox.json"):
                p = os.path.join(d, fn)
                if p not in paths:
                    paths.append(p)
    except Exception:
        pass
    return paths


def _bb_load() -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    merged: dict[str, Any] = {}
    for p in _bb_paths():
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
                if isinstance(obj, dict):
                    merged.update(obj)
        except Exception:
            pass
    return merged


def _bb_save(d: Mapping[str, Any]) -> None:
    if _pair_scope() is None:
        return
    os.makedirs(os.path.dirname(_blackbox_path()), exist_ok=True)
    for p in _bb_paths():
        try:
            tmp = f"{p}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(tmp, p)
        except Exception:
            pass


# config helpers
def _history_limit(adapter: Any) -> int:
    cfg = getattr(adapter, "cfg", None)
    v = getattr(cfg, "history_query_limit", None)
    if v is None:
        v = getattr(cfg, "watchlist_query_limit", 1000)
    try:
        return max(1, int(v))
    except Exception:
        return 1000


def _history_delay_ms(adapter: Any) -> int:
    cfg = getattr(adapter, "cfg", None)
    v = getattr(cfg, "history_write_delay_ms", None)
    if v is None:
        v = getattr(cfg, "watchlist_write_delay_ms", 0)
    try:
        return max(0, int(v))
    except Exception:
        return 0


# Emby ID helpers
_item_meta_cache: dict[str, dict[str, Any] | None] = {}
_item_ids_cache: dict[str, dict[str, str]] = {}

def _item_meta(http: Any, item_id: str | None) -> Mapping[str, Any] | None:
    iid = (str(item_id or '').strip()) or ''
    if not iid:
        return None
    if iid in _item_meta_cache:
        return _item_meta_cache[iid]
    try:
        r = http.get(f"/Items/{iid}", params={"Fields": "ProviderIds,ProductionYear"})
        if getattr(r, 'status_code', 0) != 200:
            _item_meta_cache[iid] = None
            return None
        body = r.json() or {}
        if not isinstance(body, Mapping):
            _item_meta_cache[iid] = None
            return None
        _item_meta_cache[iid] = dict(body)
        return _item_meta_cache[iid]
    except Exception:
        _item_meta_cache[iid] = None
        return None

def _pids_to_ids(pids: Any) -> dict[str, str]:
    p = pids if isinstance(pids, Mapping) else {}
    out: dict[str, str] = {}
    for k, v in p.items():
        kl = str(k).lower()
        sv = str(v).strip()
        if not sv:
            continue
        if kl == 'imdb':
            out['imdb'] = sv if sv.startswith('tt') else f"tt{sv}"
        elif kl in ('tmdb', 'tvdb'):
            try:
                out[kl] = str(int(sv))
            except Exception:
                pass
    return out

def _series_ids_for(http: Any, series_id: str | None) -> dict[str, str]:
    body = _item_meta(http, series_id)
    if not body:
        return {}
    return _pids_to_ids(body.get('ProviderIds'))

def _series_year(http: Any, series_id: str | None) -> int | None:
    body = _item_meta(http, series_id)
    if not body:
        return None
    y = body.get('ProductionYear')
    try:
        return int(y) if y is not None else None
    except Exception:
        return None

def _item_ids_for(http: Any, item_id: str | None) -> dict[str, str]:
    iid = (str(item_id or '').strip()) or ''
    if not iid:
        return {}
    if iid in _item_ids_cache:
        return _item_ids_cache[iid]
    body = _item_meta(http, iid)
    ids = _pids_to_ids(body.get('ProviderIds')) if body else {}
    _item_ids_cache[iid] = ids
    return ids

def _resp_snip(r: Any) -> str:
    try:
        j = r.json()
        s = json.dumps(j, ensure_ascii=False)
        return (s[:180] + "…") if len(s) > 180 else s
    except Exception:
        try:
            t = r.text() if callable(getattr(r, "text", None)) else getattr(r, "text", "")
            s = str(t or "")
            return (s[:180] + "…") if len(s) > 180 else s
        except Exception:
            return "<no-body>"


# library roots
def _emby_library_roots(adapter: Any) -> dict[str, dict[str, Any]]:
    http = adapter.client
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None) or ""
    roots: dict[str, dict[str, Any]] = {}
    try:
        if uid:
            r = http.get(f"/Users/{uid}/Views")
        else:
            r = http.get("/Library/MediaFolders")
    except Exception:
        r = None
    try:
        if r is not None and getattr(r, "status_code", 0) == 200:
            j = r.json() or {}
            items = j.get("Items") or j.get("ItemsList") or j.get("Items") or []
            for it in items:
                lid = it.get("Id") or it.get("Key") or it.get("Id")
                if not lid:
                    continue
                lid_s = str(lid)
                ctyp = (it.get("CollectionType") or it.get("Type") or "").lower()
                if "movie" in ctyp:
                    typ = "movie"
                elif "series" in ctyp or "tv" in ctyp:
                    typ = "show"
                else:
                    typ = ctyp or "lib"
                roots[lid_s] = {"type": typ, "raw": it}
    except Exception:
        pass
    if (os.environ.get("CW_DEBUG") or os.environ.get("CW_EMBY_DEBUG")) and roots:
        _dbg("library_roots", roots=sorted(roots.keys()))
    return roots


_lib_anc_cache: dict[str, str | None] = {}


def _lib_id_via_ancestors(
    http: Any,
    uid: str,
    iid: str,
    roots: Mapping[str, Any],
) -> str | None:
    if not iid:
        return None
    if iid in _lib_anc_cache:
        return _lib_anc_cache[iid]
    try:
        r = http.get(f"/Items/{iid}/Ancestors", params={"Fields": "Id", "UserId": uid})
        if getattr(r, "status_code", 0) == 200:
            root_keys = {str(k) for k in (roots or {}).keys()}
            for a in (r.json() or []):
                aid = str((a or {}).get("Id") or "")
                if aid in root_keys:
                    _lib_anc_cache[iid] = aid
                    return aid
    except Exception:
        pass
    _lib_anc_cache[iid] = None
    return None


# destination writes
def _write_userdata(http: Any, uid: str, item_id: str, *, date_iso: str | None) -> bool:
    payload: dict[str, Any] = {"Played": True, "PlayCount": 1}
    if date_iso:
        payload["LastPlayedDate"] = date_iso
        payload["DatePlayed"] = date_iso
    r = http.post(f"/Users/{uid}/Items/{item_id}/UserData", json=payload)
    ok = getattr(r, "status_code", 0) in (200, 204)
    if not ok:
        _warn("userdata_write_failed", item_id=item_id, status=getattr(r, 'status_code', None), body=_resp_snip(r))
    return ok


def _mark_played(http: Any, uid: str, item_id: str, *, date_played_iso: str | None) -> bool:
    try:
        date_param: str | None = None
        if date_played_iso:
            ts = _parse_iso_to_epoch(date_played_iso)
            if ts is not None:
                date_param = _epoch_to_emby_dateparam(ts)
        r = http.post(
            f"/Users/{uid}/PlayedItems/{item_id}",
            params={"DatePlayed": date_param} if date_param else None,
        )
        if getattr(r, "status_code", 0) in (200, 204):
            return True
        _warn("mark_played_failed", phase="A", item_id=item_id, status=getattr(r, 'status_code', None), body=_resp_snip(r))
        r2 = http.post(
            f"/Users/{uid}/PlayedItems/{item_id}",
            json={"DatePlayed": date_param} if date_param else {},
        )
        if getattr(r2, "status_code", 0) in (200, 204):
            return True
        _warn("mark_played_failed", phase="B", item_id=item_id, status=getattr(r2, 'status_code', None), body=_resp_snip(r2))
        if _write_userdata(http, uid, item_id, date_iso=date_played_iso):
            return True
        return False
    except Exception as e:
        _warn("mark_played_exception", item_id=item_id, error=str(e))
        return False


def _unmark_played(http: Any, uid: str, item_id: str) -> bool:
    try:
        r = http.delete(f"/Users/{uid}/PlayedItems/{item_id}")
        ok = getattr(r, "status_code", 0) in (200, 204)
        if not ok:
            _warn("unmark_played_failed", item_id=item_id, status=getattr(r, 'status_code', None), body=_resp_snip(r))
        return ok
    except Exception as e:
        _warn("unmark_played_exception", item_id=item_id, error=str(e))
        return False


def _dst_user_state(http: Any, uid: str, iid: str) -> tuple[bool, int]:
    try:
        r = http.get(
            f"/Users/{uid}/Items/{iid}",
            params={
                "Fields": "UserData,UserDataPlayCount,UserDataLastPlayedDate",
                "EnableUserData": True,
            },
        )
        if getattr(r, "status_code", 0) != 200:
            _dbg("dst_user_state_http", user_id=uid, item_id=iid, status=getattr(r, 'status_code', None))
            return False, 0
        data = r.json() or {}
        ud = data.get("UserData") or {}
        play_count = int(ud.get("PlayCount") or 0)
        played_flag = bool(ud.get("Played") is True)
        raw_ts = ud.get("LastPlayedDate")
        ts = _parse_iso_to_epoch(raw_ts) or 0
        played = bool(played_flag or play_count > 0)
        if os.environ.get("CW_EMBY_DEBUG"):
            _dbg("dst_user_state", item_id=iid, played=played, ts=ts)
        return played, ts
    except Exception as e:
        _dbg("dst_user_state_exception", item_id=iid, error=str(e))
        return False, 0


# history index
def build_index(adapter: Any, since: Any | None = None, limit: int | None = None) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("history") if callable(prog_mk) else None

    http = adapter.client
    uid = adapter.cfg.user_id

    # Per-run caches
    _item_meta_cache.clear()
    _item_ids_cache.clear()
    _lib_anc_cache.clear()
    series_cache: dict[str, dict[str, Any] | None] = {}
    played_ts_cache: dict[str, int] = {}

    page_size = _history_limit(adapter)
    roots = _emby_library_roots(adapter)
    movie_roots: list[str] = []
    show_roots: list[str] = []
    for lid, meta in (roots or {}).items():
        t = str((meta or {}).get("type") or "").lower()
        if t == "movie":
            movie_roots.append(str(lid))
        elif t == "show":
            show_roots.append(str(lid))

    since_epoch = 0
    if isinstance(since, (int, float)):
        since_epoch = int(since)
    elif isinstance(since, str):
        since_epoch = int(_parse_iso_to_epoch(since) or 0)

    try:
        scope_cfg: Mapping[str, Any] | None = emby_scope_history(adapter.cfg) or {}
    except Exception:
        scope_cfg = {}

    scope_libs: list[str] = []
    if isinstance(scope_cfg, Mapping):
        pid = scope_cfg.get("ParentId")
        if pid:
            scope_libs = [str(pid)]
        else:
            lib_ids = scope_cfg.get("LibraryIds") or scope_cfg.get("LibraryId")
            if isinstance(lib_ids, (list, tuple)):
                scope_libs = [str(x) for x in lib_ids if x]
            elif lib_ids:
                scope_libs = [str(lib_ids)]
            if not scope_libs:
                anc = scope_cfg.get("AncestorIds")
                if isinstance(anc, (list, tuple)):
                    scope_libs = [str(x) for x in anc if x]

    events: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
    presence_keys: set[str] = set()

    def _is_movieish(row: Mapping[str, Any]) -> bool:
        typ = (row.get("Type") or "").strip()
        if typ == "Movie":
            return True
        if typ == "Video":
            vt = str(row.get("VideoType") or "").strip().lower()
            if vt == "movie":
                return True
            pids = (row.get("ProviderIds") or {}) if isinstance(row, Mapping) else {}
            if (
                isinstance(pids, Mapping)
                and (pids.get("Tmdb") or pids.get("tmdb") or pids.get("Imdb") or pids.get("imdb"))
                and not row.get("SeriesId")
            ):
                return True
        return False

    def _scan(
        include_types: str,
        *,
        allow_scope: bool,
        drop_parentid: bool,
        filter_row: Any | None = None,
    ) -> tuple[int, int, int]:
        start = 0
        added_events = 0
        added_presence = 0
        skipped_untimed = 0

        while True:
            params: dict[str, Any] = {
                "IncludeItemTypes": include_types,
                "Recursive": True,
                "EnableUserData": True,
                "Fields": (
                    "ProviderIds,ProductionYear,UserData,Type,MediaType,VideoType,IndexNumber,"
                    "ParentIndexNumber,SeriesName,SeriesId,Name,ParentId,DatePlayed,Path,"
                    "LibraryId,AncestorIds"
                ),
                "Filters": "IsPlayed",
                "SortBy": "DatePlayed",
                "SortOrder": "Descending",
                "StartIndex": start,
                "Limit": page_size,
                "EnableTotalRecordCount": True,
                "UserId": uid,
            }

            scope: Mapping[str, Any] | None
            try:
                scope = emby_scope_history(adapter.cfg) or {}
            except Exception:
                scope = {}

            if allow_scope and isinstance(scope, Mapping):
                for k, v in scope.items():
                    if k == "IncludeItemTypes":
                        continue
                    if drop_parentid and k == "ParentId":
                        continue
                    params[k] = v
                if "IncludeItemTypes" in scope:
                    want = {x.strip() for x in include_types.split(",") if x.strip()}
                    got = {x.strip() for x in str(scope["IncludeItemTypes"]).split(",") if x.strip()}
                    params["IncludeItemTypes"] = ",".join(sorted(want | got))

            r = http.get(f"/Users/{uid}/Items", params=params)
            if getattr(r, "status_code", 0) != 200:
                _warn("query_failed", status=getattr(r, 'status_code', None), body=_resp_snip(r))
                break

            try:
                body = r.json() or {}
                rows = body.get("Items") or []
            except Exception as e:
                _warn("json_parse_failed", error=str(e))
                rows = []

            if not rows:
                break
            try:
                sids = [
                    (row.get("SeriesId") or row.get("ParentId"))
                    for row in rows
                    if (row.get("Type") or "").strip() == "Episode"
                ]
                prefetch_series_minimals(http, uid, sids, series_cache)
            except Exception:
                pass

            try:
                missing_ts: list[str] = []
                for row in rows:
                    if _played_ts_from_row(row):
                        continue
                    ud = row.get('UserData') or {}
                    if ud.get('Played') or ud.get('IsPlayed') or (ud.get('PlayCount') or 0) > 0:
                        iid = str(row.get('Id') or '').strip()
                        if iid:
                            missing_ts.append(iid)
                _prefetch_played_ts(http, uid, missing_ts, played_ts_cache)
            except Exception:
                pass

            stop = False
            for row in rows:
                if callable(filter_row) and not filter_row(row):
                    continue
                ts = _played_ts_from_row(row)
                if not ts:
                    iid = str(row.get('Id') or '').strip()
                    ts = int(played_ts_cache.get(iid) or 0) if iid else 0
                if not ts:
                    ts = _played_ts_backfill(http, uid, row)
                if ts and since_epoch and ts <= since_epoch:
                    stop = True
                    break
                ud = row.get("UserData") or {}
                if not ts:
                    if ud.get("Played") or ud.get("IsPlayed") or (ud.get("PlayCount") or 0) > 0:
                        try:
                            m0 = emby_normalize(row)
                            presence_keys.add(canonical_key(m0))
                            skipped_untimed += 1
                            added_presence += 1
                        except Exception:
                            pass
                    continue

                m = emby_normalize(row)
                watched_at = _epoch_to_iso_z(ts)
                typ = (row.get("Type") or "").strip()

                if _is_movieish(row):
                    ids = dict(m.get("ids") or {})
                    if not ids:
                        ids = _item_ids_for(http, row.get("Id"))
                    movie_title = (m.get("title") or row.get("Name") or "").strip()
                    event: dict[str, Any] = {
                        "type": "movie",
                        "ids": ids,
                        "title": movie_title,
                        "year": m.get("year") or row.get("ProductionYear"),
                        "watched_at": watched_at,
                        "watched": True,
                    }
                elif typ == "Episode":
                    sid = row.get("SeriesId") or row.get("ParentId")
                    smeta = _series_minimal_from_episode(http, uid, row, series_cache)
                    show_ids_raw = (smeta.get("ids") or {}) if isinstance(smeta, Mapping) else {}
                    show_ids: dict[str, str] = {}
                    for k in ("tmdb", "imdb", "tvdb"):
                        vv = str(show_ids_raw.get(k) or "").strip()
                        if not vv:
                            continue
                        if k == "imdb":
                            show_ids["imdb"] = vv if vv.startswith("tt") else f"tt{vv}"
                            continue
                        try:
                            show_ids[k] = str(int(vv))
                        except Exception:
                            pass
                    if not show_ids and sid:
                        # Fallback to direct item lookup (cached) if the series item couldn't be resolved.
                        show_ids = _item_ids_for(http, sid)
                    season = m.get("season")
                    episode = m.get("episode")
                    if season is None:
                        try:
                            season = int(row.get("ParentIndexNumber"))
                        except Exception:
                            pass
                    if episode is None:
                        try:
                            episode = int(row.get("IndexNumber"))
                        except Exception:
                            pass
                    if season is None or episode is None:
                        continue
                    series_title = (
                        m.get("series_title")
                        or row.get("SeriesName")
                        or (smeta.get("title") if isinstance(smeta, Mapping) else "")
                        or ""
                    ).strip()
                    event = {
                        "type": "episode",
                        "season": season,
                        "episode": episode,
                        "series_title": series_title,
                        "title": f"S{int(season):02d}E{int(episode):02d}",
                        "watched_at": watched_at,
                        "watched": True,
                    }
                    if show_ids:
                        event["show_ids"] = dict(show_ids)
                    sy = smeta.get("year") if isinstance(smeta, Mapping) else None
                    if sy is not None:
                        try:
                            event["series_year"] = int(sy)
                        except Exception:
                            pass
                    epi_ids = (m.get("ids") or {}) if isinstance(m.get("ids"), Mapping) else {}
                    if epi_ids:
                        safe_epi: dict[str, str] = {}
                        for k in ("tmdb", "imdb", "tvdb"):
                            vv = str(epi_ids.get(k) or "").strip()
                            if not vv:
                                continue
                            if k == "imdb":
                                safe_epi["imdb"] = vv if vv.startswith("tt") else f"tt{vv}"
                                continue
                            try:
                                safe_epi[k] = str(int(vv))
                            except Exception:
                                pass
                        if safe_epi:
                            event["ids"] = safe_epi
                else:
                    continue

                lib_id: str | None = None
                if scope_libs:
                    lib_id = scope_libs[0]
                else:
                    candidates: list[str] = []
                    mlid = m.get("library_id") if isinstance(m, dict) else None
                    if mlid:
                        candidates.append(str(mlid))
                    lid = row.get("LibraryId")
                    if lid is not None:
                        candidates.append(str(lid))
                    anc = row.get("AncestorIds") or []
                    if isinstance(anc, (list, tuple)):
                        for a in anc:
                            if a is not None:
                                candidates.append(str(a))
                    pid = row.get("ParentId")
                    if pid is not None:
                        candidates.append(str(pid))
                    root_keys = set((roots or {}).keys())
                    for cid in candidates:
                        if cid in root_keys:
                            lib_id = cid
                            break
                    if not lib_id and row.get("Id"):
                        lib_id = _lib_id_via_ancestors(http, uid, str(row["Id"]), roots)
                    if not lib_id:
                        etype = event.get("type")
                        if etype == "movie" and movie_roots:
                            lib_id = movie_roots[0]
                        elif etype == "episode" and show_roots:
                            lib_id = show_roots[0]
                if lib_id:
                    event["library_id"] = str(lib_id)
                ev_key = f"{canonical_key(m)}@{ts}"
                events.append((ts, {"key": ev_key, "base": m}, event))
                added_events += 1

            start += len(rows)
            if stop or len(rows) < page_size:
                break

        if skipped_untimed:
            _dbg("skipped_untimed_items", count=skipped_untimed)
        return added_events, added_presence, skipped_untimed

    _scan("Movie,Video", allow_scope=True, drop_parentid=True, filter_row=_is_movieish)
    _scan("Episode", allow_scope=True, drop_parentid=False, filter_row=None)

    events.sort(key=lambda x: x[0], reverse=True)
    if isinstance(limit, int) and limit > 0:
        events = events[: int(limit)]

    total = len(events)
    out: dict[str, dict[str, Any]] = {}
    if prog:
        try:
            prog.tick(0, total=total, force=True)
        except Exception:
            pass

    done = 0
    for _, meta, event in events:
        out[meta["key"]] = event
        done += 1
        if prog:
            try:
                prog.tick(done, total=total)
            except Exception:
                pass

    event_bases = {ek.split("@", 1)[0] for ek in out.keys()}

    shadow = _shadow_load()
    if shadow:
        added = 0
        for k in list(shadow.keys()):
            if k in event_bases or k in out:
                continue
            out.setdefault(k, {"watched": True})
            added += 1
        if added:
            _dbg("shadow_merged", added=added)

    bb = _bb_load()
    if bb:
        added = 0
        for k, meta in bb.items():
            if k in event_bases or k in out:
                continue
            if isinstance(meta, dict) and str(meta.get("reason", "")).startswith("presence:"):
                out.setdefault(k, {"watched": True})
                added += 1
        if added:
            _dbg("blackbox_presence_merged", added=added)

    if presence_keys:
        added = 0
        for k in presence_keys:
            if k in event_bases or k in out:
                continue
            out.setdefault(k, {"watched": True})
            added += 1
        if added:
            _dbg("presence_merged", added=added)

    if os.environ.get("CW_DEBUG") or os.environ.get("CW_EMBY_DEBUG"):
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
        _dbg("index_library_distribution", cfg_libs=cfg_libs, distribution=lib_counts)

    _info("index_done", count=len(out), mode="events+presence")
    return out


# apply history
def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    qlim = int(_history_limit(adapter) or 25)
    delay = _history_delay_ms(adapter)

    cfg = getattr(adapter, "cfg", None)
    do_force = bool(getattr(cfg, "history_force_overwrite", False))
    do_back = bool(getattr(cfg, "history_backdate", False))
    tol = int(getattr(cfg, "history_backdate_tolerance_s", 300))

    try:
        from ._common import provider_index

        provider_index(adapter)
    except Exception:
        pass

    pre_unresolved: list[dict[str, Any]] = []
    wants: dict[str, dict[str, Any]] = {}
    for it in (items or []):
        base: dict[str, Any] = dict(it or {})
        base_ids_raw = base.get("ids")
        if isinstance(base_ids_raw, Mapping):
            base_ids: dict[str, Any] = dict(base_ids_raw)
        else:
            base_ids = {}
        has_ids = bool(base_ids) and any(v not in (None, "", 0) for v in base_ids.values())
        if has_ids:
            nm = emby_normalize(base)
            m: dict[str, Any] = dict(nm)
            for key in (
                "type",
                "title",
                "year",
                "watch_type",
                "watched_at",
                "library_id",
                "season",
                "episode",
            ):
                if base.get(key) not in (None, ""):
                    m[key] = base[key]
            ids = dict(nm.get("ids") or {})
            for k_id, v_id in base_ids.items():
                if v_id not in (None, "", 0):
                    ids[k_id] = v_id
            if ids:
                m["ids"] = ids
        else:
            m = dict(emby_normalize(base))
        t_raw = str(m.get("type") or "").strip().lower()
        if t_raw == "anime":
            if m.get("season") not in (None, "", 0) and m.get("episode") not in (None, "", 0):
                m["type"] = "episode"
            else:
                m["type"] = "show"

        try:
            k = canonical_key(m) or canonical_key(base)
        except Exception:
            k = None
        if not k:
            pre_unresolved.append({"item": id_minimal(base), "hint": "missing_ids_for_key"})
            _freeze(base, reason="missing_ids_for_key")
            continue
        wants[k] = m

    mids: list[tuple[str, str]] = []
    unresolved: list[dict[str, Any]] = []
    if pre_unresolved:
        unresolved.extend(pre_unresolved)

    for k, m in wants.items():
        try:
            iid = resolve_item_id(adapter, m)
        except Exception as e:
            _warn("resolve_exception", error=str(e))
            iid = None
        if iid:
            mids.append((k, iid))
        else:
            unresolved.append({"item": id_minimal(m), "hint": "resolve_failed"})
            _freeze(m, reason="resolve_failed")

    shadow = _shadow_load()
    bb = _bb_load()
    ok = 0
    stats: dict[str, int] = {
        "wrote": 0,
        "forced": 0,
        "backdated": 0,
        "skip_newer": 0,
        "skip_played_untimed": 0,
        "skip_missing_date": 0,
        "fail_mark": 0,
    }

    total = len(mids)
    if total:
        _info("write_start", op="add", count=total)

    processed = 0
    for chunk in chunked(mids, qlim):
        for k, iid in chunk:
            it = wants[k]
            src_ts = _parse_iso_to_epoch(it.get("watched_at")) or 0
            if not src_ts:
                unresolved.append({"item": id_minimal(it), "hint": "missing_watched_at"})
                _freeze(it, reason="missing_watched_at")
                stats["skip_missing_date"] += 1
                continue

            src_iso = _epoch_to_iso_z(src_ts)

            if do_force:
                _unmark_played(http, uid, iid)
                if _mark_played(http, uid, iid, date_played_iso=src_iso):
                    ok += 1
                    shadow[k] = int(shadow.get(k, 0)) + 1
                    bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                    stats["wrote"] += 1
                    stats["forced"] += 1
                else:
                    unresolved.append({"item": id_minimal(it), "hint": "mark_played_failed"})
                    _freeze(it, reason="write_failed")
                    stats["fail_mark"] += 1
                processed += 1
                if (processed % 25) == 0:
                    _dbg("write_progress", op="add", done=processed, total=total, ok=ok, unresolved=len(unresolved))
                sleep_ms(delay)
                continue

            played, dst_ts = _dst_user_state(http, uid, iid)
            if played and dst_ts and dst_ts >= (src_ts - tol):
                shadow[k] = int(shadow.get(k, 0)) + 1
                bb[k] = {"reason": "presence:existing_newer", "since": _now_iso_z()}
                stats["skip_newer"] += 1
                continue

            if played and not dst_ts:
                if _mark_played(http, uid, iid, date_played_iso=src_iso):
                    ok += 1
                    shadow[k] = int(shadow.get(k, 0)) + 1
                    bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                    stats["wrote"] += 1
                    stats["backdated"] += 1
                    sleep_ms(delay)
                    continue
                if do_back:
                    _unmark_played(http, uid, iid)
                    if _mark_played(http, uid, iid, date_played_iso=src_iso):
                        ok += 1
                        shadow[k] = int(shadow.get(k, 0)) + 1
                        bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                        stats["wrote"] += 1
                        stats["backdated"] += 1
                        sleep_ms(delay)
                        continue
                shadow[k] = int(shadow.get(k, 0)) + 1
                bb[k] = {"reason": "presence:existing_untimed", "since": _now_iso_z()}
                stats["skip_played_untimed"] += 1
                continue

            if do_back and (not dst_ts or dst_ts >= (src_ts + tol)):
                _unmark_played(http, uid, iid)
                if _mark_played(http, uid, iid, date_played_iso=src_iso):
                    ok += 1
                    shadow[k] = int(shadow.get(k, 0)) + 1
                    bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                    stats["wrote"] += 1
                    stats["backdated"] += 1
                else:
                    unresolved.append({"item": id_minimal(it), "hint": "mark_played_failed"})
                    _freeze(it, reason="write_failed")
                    stats["fail_mark"] += 1
                processed += 1
                if (processed % 25) == 0:
                    _dbg("write_progress", op="add", done=processed, total=total, ok=ok, unresolved=len(unresolved))
                sleep_ms(delay)
                continue

            if _mark_played(http, uid, iid, date_played_iso=src_iso):
                ok += 1
                shadow[k] = int(shadow.get(k, 0)) + 1
                bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                stats["wrote"] += 1
            else:
                unresolved.append({"item": id_minimal(it), "hint": "mark_played_failed"})
                _freeze(it, reason="write_failed")
                stats["fail_mark"] += 1

            processed += 1
            if (processed % 25) == 0:
                _dbg("write_progress", op="add", done=processed, total=total, ok=ok, unresolved=len(unresolved))
            sleep_ms(delay)

    _shadow_save(shadow)
    _bb_save(bb)
    if ok:
        _thaw_if_present([k for k, _ in mids])

    _info("write_done", op="add", ok=ok, unresolved=len(unresolved), wrote=stats['wrote'], forced=stats['forced'], backdated=stats['backdated'], skip_newer=stats['skip_newer'], skip_played_untimed=stats['skip_played_untimed'], skip_missing_date=stats['skip_missing_date'], fail_mark=stats['fail_mark'])
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    qlim = int(_history_limit(adapter) or 25)
    delay = _history_delay_ms(adapter)

    pre_unresolved: list[dict[str, Any]] = []
    wants: dict[str, dict[str, Any]] = {}

    for it in (items or []):
        base: dict[str, Any] = dict(it or {})
        base_ids_raw = base.get("ids")
        if isinstance(base_ids_raw, Mapping):
            base_ids: dict[str, Any] = dict(base_ids_raw)
        else:
            base_ids = {}
        has_ids = bool(base_ids) and any(v not in (None, "", 0) for v in base_ids.values())

        if has_ids:
            nm = emby_normalize(base)
            m: dict[str, Any] = dict(nm)
            for key in (
                "type",
                "title",
                "year",
                "watch_type",
                "watched_at",
                "library_id",
                "season",
                "episode",
            ):
                if base.get(key) not in (None, ""):
                    m[key] = base[key]
            ids = dict(nm.get("ids") or {})
            for k_id, v_id in base_ids.items():
                if v_id not in (None, "", 0):
                    ids[k_id] = v_id
            if ids:
                m["ids"] = ids
        else:
            m = dict(emby_normalize(base))
        t_raw = str(m.get("type") or "").strip().lower()
        if t_raw == "anime":
            if m.get("season") not in (None, "", 0) and m.get("episode") not in (None, "", 0):
                m["type"] = "episode"
            else:
                m["type"] = "show"

        try:
            k = canonical_key(m) or canonical_key(base)
        except Exception:
            k = None
        if not k:
            pre_unresolved.append({"item": id_minimal(base), "hint": "missing_ids_for_key"})
            _freeze(base, reason="missing_ids_for_key")
            continue
        wants[k] = m

    mids: list[tuple[str, str]] = []
    unresolved: list[dict[str, Any]] = []
    if pre_unresolved:
        unresolved.extend(pre_unresolved)

    for k, m in wants.items():
        try:
            iid = resolve_item_id(adapter, m)
        except Exception as e:
            _warn("resolve_exception", error=str(e))
            iid = None
        if iid:
            mids.append((k, iid))
        else:
            unresolved.append({"item": id_minimal(m), "hint": "resolve_failed"})
            _freeze(m, reason="resolve_failed")

    shadow = _shadow_load()
    ok = 0

    for chunk in chunked(mids, qlim):
        for k, iid in chunk:
            cur = int(shadow.get(k, 0))
            nxt = max(0, cur - 1)
            shadow[k] = nxt
            if nxt == 0:
                if _unmark_played(http, uid, iid):
                    ok += 1
                else:
                    unresolved.append({"item": id_minimal(wants[k]), "hint": "unmark_played_failed"})
                    _freeze(wants[k], reason="write_failed")
            sleep_ms(delay)

    shadow = {k: v for k, v in shadow.items() if v > 0}
    _shadow_save(shadow)
    if ok:
        _thaw_if_present([k for k, _ in mids])

    _info("write_done", op="remove", ok=ok, unresolved=len(unresolved))
    return ok, unresolved
