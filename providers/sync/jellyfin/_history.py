# /providers/sync/jellyfin/_history.py
# JELLYFIN Module for history sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from .._log import log as cw_log

import json
import os
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from ._common import (
    state_file,
    chunked,
    jf_get_library_roots,
    jf_resolve_library_id,
    jf_scope_history,
    normalize as jelly_normalize,
    resolve_item_id,
    sleep_ms,
    _pair_scope,
)
from cw_platform.id_map import canonical_key, minimal as id_minimal

def _unresolved_path() -> str:
    return str(state_file("jellyfin_history.unresolved.json"))

def _shadow_path() -> str:
    return str(state_file("jellyfin_history.shadow.json"))

def _blackbox_path() -> str:
    return str(state_file("jellyfin_history.jellyfin-plex.blackbox.json"))




def _trc(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "history", "trace", msg, **fields)


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "history", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "history", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "history", "warn", msg, **fields)


# unresolved
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
        os.makedirs(os.path.dirname(_unresolved_path()), exist_ok=True)
        with open(_unresolved_path(), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
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
        os.makedirs(os.path.dirname(_shadow_path()), exist_ok=True)
        with open(_shadow_path(), "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass


# blackbox
def _bb_load() -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    try:
        with open(_blackbox_path(), "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _bb_save(d: Mapping[str, Any]) -> None:
    if _pair_scope() is None:
        return
    try:
        os.makedirs(os.path.dirname(_blackbox_path()), exist_ok=True)
        with open(_blackbox_path(), "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
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


def _now_iso_z() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
def _series_ids_for(http: Any, series_id: str | None) -> dict[str, str]:
    sid = (str(series_id or "").strip()) or ""
    if not sid:
        return {}
    try:
        r = http.get(f"/Items/{sid}", params={"Fields": "ProviderIds,ProductionYear"})
        if getattr(r, "status_code", 0) != 200:
            return {}
        body = r.json() or {}
        pids = (body.get("ProviderIds") or {}) if isinstance(body, Mapping) else {}
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
            if kl == "imdb":
                out["imdb"] = sv if sv.startswith("tt") else f"tt{sv}"
            elif kl == "tmdb":
                try:
                    out["tmdb"] = str(int(sv))
                except Exception:
                    pass
            elif kl == "tvdb":
                try:
                    out["tvdb"] = str(int(sv))
                except Exception:
                    pass
        return out
    except Exception:
        return {}


# low-level Jellyfin writes
def _mark_played(http: Any, uid: str, item_id: str, *, date_played_iso: str | None) -> bool:
    try:
        params = {"datePlayed": date_played_iso} if date_played_iso else None
        r = http.post(f"/Users/{uid}/PlayedItems/{item_id}", params=params)
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False


def _unmark_played(http: Any, uid: str, item_id: str) -> bool:
    try:
        r = http.delete(f"/Users/{uid}/PlayedItems/{item_id}")
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False


def _dst_user_state(http: Any, uid: str, iid: str) -> tuple[bool, int]:
    try:
        r = http.get(f"/Users/{uid}/Items/{iid}", params={"Fields": "UserData"})
        if getattr(r, "status_code", 0) != 200:
            return False, 0
        data = r.json() or {}
        ud = data.get("UserData") or {}
        played = bool(ud.get("Played") or ud.get("IsPlayed"))
        ts = 0
        for k in ("LastPlayedDate", "DateLastPlayed", "LastPlayed"):
            v = ud.get(k) or data.get(k)
            if v:
                ts = _parse_iso_to_epoch(v) or 0
                if ts:
                    break
        return played, ts
    except Exception:
        return False, 0


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
    page_size = _history_limit(adapter)

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
            anc = scope_params.get("AncestorIds") or scope_params.get("ancestorIds")
            if isinstance(anc, (list, tuple)):
                scope_libs = [str(x) for x in anc if x]

    roots = jf_get_library_roots(adapter)
    if roots:
        _dbg("library roots", roots=list(sorted(roots.keys())))

    start = 0
    events: list[tuple[int, dict[str, Any], dict[str, Any]]] = []

    while True:
        params: dict[str, Any] = {
            "UserId": uid,
            "SortBy": "DatePlayed",
            "SortOrder": "Descending",
            "IncludeItemTypes": "Movie,Episode",
            "Recursive": "true",
            "Filters": "IsPlayed",
            "Fields": (
                "ProviderIds,MediaSources,Path,Overview,"
                "ParentId,LibraryId,AncestorIds,"
                "SeriesName,SeriesId,IndexNumber,ParentIndexNumber,DateLastMediaAdded"
            ),
            "StartIndex": start,
            "Limit": page_size,
        }

        if scope_params:
            params.update(scope_params)

        r = http.get(f"/Users/{uid}/Items", params=params)
        body = r.json() or {}
        rows = body.get("Items") or []
        if not rows:
            break

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

            lib_id = jf_resolve_library_id(row, roots, scope_libs, http)
            if not lib_id and row.get("Id"):
                lib_id = _lib_id_via_ancestors(http, str(row["Id"]), roots)

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
                show_ids = _series_ids_for(http, row.get("SeriesId"))
                ep_title = m.get("title") or row.get("Name")
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

            ev_key = f"{canonical_key(m)}@{ts}"
            out_ev = dict(event)
            if lib_id:
                out_ev["library_id"] = lib_id
            events.append((ts, {"key": ev_key, "base": m}, out_ev))

        start += len(body.get("Items") or [])
        if isinstance(limit, int) and limit > 0 and len(events) >= int(limit):
            break
        if not rows:
            break

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
        for k in list(shadow.keys()):
            if k in event_bases or k in out:
                continue
            out.setdefault(k, {"watched": True})
            added += 1
        if added:
            _dbg("shadow merged", added=added)

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
                    out.setdefault(k, {"watched": True})
                    added += 1
        if added:
            _dbg("blackbox presence merged", added=added)

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

        _trc("library distribution", cfg_libraries=cfg_libs, distribution=lib_counts)

    _info("index done", count=len(out), mode="events+presence")
    return out


# writes
def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
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

        nm = jelly_normalize(base)
        m: dict[str, Any] = dict(nm)

        if has_ids:
            for key in (
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
            ):
                if base.get(key) not in (None, ""):
                    m[key] = base[key]

            ids = dict(nm.get("ids") or {})
            for k_id, v_id in base_ids.items():
                if v_id not in (None, "", 0):
                    ids[k_id] = v_id
            if ids:
                m["ids"] = ids

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
            _warn("resolve exception", err=repr(e))
            iid = None

        if iid:
            mids.append((k, iid))
        else:
            unresolved.append({"item": id_minimal(m), "hint": "resolve_failed"})
            _freeze(m, reason="resolve_failed")

    shadow = _shadow_load()
    bb = _bb_load()
    ok = 0
    stats = {
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
        _info("add start", count=total)

    processed = 0
    for chunk in chunked(mids, qlim):
        for k, iid in chunk:
            src_ts = _parse_iso_to_epoch(wants[k].get("watched_at")) or 0
            if not src_ts:
                unresolved.append({"item": id_minimal(wants[k]), "hint": "missing_watched_at"})
                _freeze(wants[k], reason="missing_watched_at")
                stats["skip_missing_date"] += 1
                processed += 1
                continue

            src_iso = _epoch_to_iso_z(src_ts)
            played, dst_ts = _dst_user_state(http, uid, iid)

            if played and dst_ts and src_ts and dst_ts >= src_ts:
                bb[k] = {"reason": "presence:existing_newer", "since": _now_iso_z()}
                stats["skip_newer"] += 1
                processed += 1
                if (processed % 25) == 0:
                    _dbg("add progress", done=processed, total=total, ok=ok, unresolved=len(unresolved))
                sleep_ms(delay)
                continue

            if played and not dst_ts and not src_ts:
                bb[k] = {"reason": "presence:existing_untimed", "since": _now_iso_z()}
                stats["skip_played_untimed"] += 1
                processed += 1
                if (processed % 25) == 0:
                    _dbg("add progress", done=processed, total=total, ok=ok, unresolved=len(unresolved))
                sleep_ms(delay)
                continue

            if _mark_played(http, uid, iid, date_played_iso=src_iso):
                ok += 1
                shadow[k] = int(shadow.get(k, 0)) + 1
                bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                stats["wrote"] += 1
            else:
                unresolved.append({"item": wants[k], "hint": "mark_played_failed"})
                _freeze(wants[k], reason="write_failed")
                stats["fail_mark"] += 1

            processed += 1
            if (processed % 25) == 0:
                _dbg("add progress", done=processed, total=total, ok=ok, unresolved=len(unresolved))
            sleep_ms(delay)

    _shadow_save(shadow)
    _bb_save(bb)
    if ok:
        _thaw_if_present([k for k, _ in mids])
    _info("add done", ok=ok, unresolved=len(unresolved), **stats)
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

        nm = jelly_normalize(base)
        m: dict[str, Any] = dict(nm)

        if has_ids:
            for key in (
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
            ):
                if base.get(key) not in (None, ""):
                    m[key] = base[key]

            ids = dict(nm.get("ids") or {})
            for k_id, v_id in base_ids.items():
                if v_id not in (None, "", 0):
                    ids[k_id] = v_id
            if ids:
                m["ids"] = ids

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
            _warn("resolve exception", err=repr(e))
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
    _info("remove done", ok=ok, unresolved=len(unresolved))
    return ok, unresolved
