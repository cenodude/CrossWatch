# /providers/sync/plex/_history.py
# Plex Module for history synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from pathlib import Path

from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from

from ._common import (
    _as_base_url,
    _xml_to_container,
    as_epoch as _as_epoch,
    candidate_guids_from_ids,
    force_episode_title as _force_episode_title,
    home_scope_enter,
    home_scope_exit,
    iso_from_epoch as _iso,
    minimal_from_history_row,
    normalize as plex_normalize,
    normalize_discover_row,
    plex_headers,
    read_json,
    server_find_rating_key_by_guid,
    sort_guid_candidates,
    state_file,
    unresolved_store,
    write_json,
    emit,
    make_logger,
    _plex_cfg,
)

_UNRES = unresolved_store("history")

def _shadow_path() -> Path:
    return state_file("plex_history.shadow.json")

def _marked_state_path() -> Path:
    return state_file("plex_history.marked_watched.json")

def _load_marked_state() -> dict[str, Any]:
    return read_json(_marked_state_path())

def _save_marked_state(data: Mapping[str, Any]) -> None:
    try:
        write_json(_marked_state_path(), data, indent=0, sort_keys=False, separators=(",", ":"))
    except Exception:
        pass



def _watermark_path() -> Path:
    return state_file("plex_history.watermark.json")

def _wm_key(acct_id: int, uname: str) -> str:
    if acct_id:
        return f"acct:{acct_id}"
    if uname:
        return f"user:{uname.lower()}"
    return "default"

def _load_watermark(key: str) -> int | None:
    try:
        data = read_json(_watermark_path()) or {}
        by_user = data.get("by_user") or {}
        v = by_user.get(key)
        return int(v) if v else None
    except Exception:
        return None

def _save_watermark(key: str, epoch: int) -> None:
    try:
        path = _watermark_path()
        data = read_json(path) or {}
        by_user = dict(data.get("by_user") or {})
        cur = int(by_user.get(key) or 0)
        epoch_i = int(epoch or 0)
        if epoch_i <= 0 or epoch_i <= cur:
            return
        by_user[key] = epoch_i
        out = {"by_user": by_user, "updated_at": _iso(epoch_i)}
        write_json(path, out, indent=0, sort_keys=False, separators=(",", ":"))
    except Exception:
        pass

def _guid_index_path() -> Path:
    return state_file("plex_history.guid_index.json")

def _load_guid_index(srv: Any, allow: set[str]) -> bool:
    try:
        data = read_json(_guid_index_path()) or {}
        mid = str(getattr(srv, "machineIdentifier", "") or "")
        if not mid or data.get("machine_id") != mid:
            return False
        stored_allow = set(str(x) for x in (data.get("allow") or []))
        if stored_allow != set(str(x) for x in (allow or set())):
            return False
        # TTL to avoid stale indices forever.
        ttl_days = int(os.environ.get("CW_PLEX_GUID_INDEX_TTL_DAYS", "0") or "7")
        created = int(data.get("created_epoch") or 0)
        if created and ttl_days > 0 and (int(time.time()) - created) > ttl_days * 86400:
            return False
        movies = data.get("movies") or {}
        shows = data.get("shows") or {}
        if not isinstance(movies, dict) or not isinstance(shows, dict):
            return False
        _GUID_INDEX_MOVIE.update({str(k): str(v) for k, v in movies.items() if k and v})
        _GUID_INDEX_SHOW.update({str(k): str(v) for k, v in shows.items() if k and v})
        return bool(_GUID_INDEX_MOVIE or _GUID_INDEX_SHOW)
    except Exception:
        return False

def _save_guid_index(srv: Any, allow: set[str]) -> None:
    try:
        mid = str(getattr(srv, "machineIdentifier", "") or "")
        if not mid:
            return
        out = {
            "machine_id": mid,
            "allow": sorted(str(x) for x in (allow or set())),
            "created_epoch": int(time.time()),
            "movies": _GUID_INDEX_MOVIE,
            "shows": _GUID_INDEX_SHOW,
        }
        write_json(_guid_index_path(), out, indent=0, sort_keys=False, separators=(",", ":"))
    except Exception:
        pass
_dbg, _info, _warn, _error, _log = make_logger("history")


# PMS GUID index cache (used for strict ID matching).
_GUID_INDEX_MOVIE: dict[str, str] = {}
_GUID_INDEX_SHOW: dict[str, str] = {}

def _meta_guids(meta_obj: Any) -> list[str]:
    vals: list[str] = []
    try:
        if getattr(meta_obj, "guid", None):
            vals.append(str(meta_obj.guid))
        for gg in getattr(meta_obj, "guids", []) or []:
            gid = getattr(gg, "id", None)
            if gid:
                vals.append(str(gid))
    except Exception:
        pass
    return vals

def _build_guid_index(adapter: Any, allow: set[str]) -> None:
    if _GUID_INDEX_MOVIE or _GUID_INDEX_SHOW:
        return
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if srv and _load_guid_index(srv, allow):
        _dbg("guid_index_loaded", movies=len(_GUID_INDEX_MOVIE), shows=len(_GUID_INDEX_SHOW))
        return
    try:
        for sec in adapter.libraries(types=("movie", "show")) or []:
            sid = str(getattr(sec, "key", "") or "").strip()
            if allow and sid and sid not in allow:
                continue
            libtype = "movie" if getattr(sec, "type", "") == "movie" else "show"
            dst = _GUID_INDEX_MOVIE if libtype == "movie" else _GUID_INDEX_SHOW
            try:
                for obj in (sec.all() or []):
                    try:
                        rk = str(getattr(obj, "ratingKey", "") or "").strip()
                        if not rk:
                            continue
                        for g in _meta_guids(obj):
                            gg = str(g or "").strip().lower()
                            if gg and gg not in dst:
                                dst[gg] = rk
                    except Exception:
                        continue
            except Exception:
                continue
        if srv:
            _save_guid_index(srv, allow)
        _dbg("guid_index_done", movies=len(_GUID_INDEX_MOVIE), shows=len(_GUID_INDEX_SHOW))
    except Exception:
        pass

def _pms_find_in_guid_index(libtype: str, candidates: list[str]) -> str | None:
    src = _GUID_INDEX_SHOW if libtype == "show" else _GUID_INDEX_MOVIE
    for g in candidates or []:
        gg = str(g or "").strip().lower()
        if gg and gg in src:
            return src[gg]
    return None


def _emit(evt: dict[str, Any]) -> None:
    emit(evt, default_feature="history")


def _epoch_from_history_entry(entry: Any) -> int | None:
    data = getattr(entry, "_data", None)
    if data is not None and hasattr(data, "get"):
        for k in ("viewedAt", "lastViewedAt"):
            ts = _as_epoch(data.get(k))
            if ts:
                return ts
    for k in ("viewedAt", "viewed_at", "lastViewedAt"):
        ts = _as_epoch(getattr(entry, k, None))
        if ts:
            return ts
    return None

def _account_id_from_history_entry(entry: Any) -> int | None:
    v = getattr(entry, "accountID", None)
    if v is None:
        return None
    try:
        return int(str(v).strip())
    except Exception:
        return None

def _username_from_history_entry(entry: Any) -> str | None:
    data = getattr(entry, "_data", None)
    if isinstance(data, Mapping):
        for attr in ("username", "userName", "accountName", "userTitle", "user"):
            v = data.get(attr)
            if isinstance(v, str):
                s = v.strip()
                if s:
                    return s
            if isinstance(v, Mapping):
                for sub in ("username", "title", "name"):
                    sv = v.get(sub)
                    if isinstance(sv, str):
                        s = sv.strip()
                        if s:
                            return s
    for attr in ("username", "userName", "accountName", "userTitle", "user"):
        v = getattr(entry, attr, None)
        if v is None:
            continue
        if isinstance(v, str):
            s = v.strip()
            return s or None
        for sub in ("username", "title", "name"):
            sv = getattr(v, sub, None)
            if isinstance(sv, str):
                s = sv.strip()
                return s or None
    return None

def _plex_cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    cfg = _plex_cfg(adapter)
    val = cfg.get(key, default) if isinstance(cfg, dict) else default
    return default if val is None else val

def _history_cfg(adapter: Any) -> Mapping[str, Any]:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        hist = plex.get("history") or {}
        return hist if isinstance(hist, dict) else {}
    except Exception:
        return {}

def _history_cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    cfg = _history_cfg(adapter)
    val = cfg.get(key, default) if isinstance(cfg, dict) else default
    return default if val is None else val

def _get_workers(adapter: Any, cfg_key: str, env_key: str, default: int) -> int:
    try:
        n = int(_plex_cfg_get(adapter, cfg_key, 0) or 0)
    except Exception:
        n = 0
    if n <= 0:
        try:
            n = int(os.environ.get(env_key, str(default)))
        except Exception:
            n = default
    return max(1, min(n, 64))

def _allowed_history_sec_ids(adapter: Any) -> set[str]:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        arr = (plex.get("history") or {}).get("libraries") or []
        return {str(int(x)) for x in arr if str(x).strip()}
    except Exception:
        return set()

def _row_section_id(h: Any) -> str | None:
    for attr in ("librarySectionID", "sectionID", "librarySectionId", "sectionId"):
        v = getattr(h, attr, None)
        if v is not None:
            try:
                return str(int(v))
            except Exception:
                pass
    sk = getattr(h, "sectionKey", None) or getattr(h, "librarySectionKey", None)
    if sk:
        m = re.search(r"/library/sections/(\d+)", str(sk))
        if m:
            return m.group(1)
    return None

def _event_key(item: Mapping[str, Any]) -> str:
    return unresolved_store("history").event_key(item)

def _load_shadow() -> dict[str, Any]:
    return read_json(_shadow_path())

def _shadow_load() -> dict[str, Any]:
    return _load_shadow()

def _save_shadow(data: Mapping[str, Any]) -> None:
    write_json(_shadow_path(), data)

def _shadow_add(item: Mapping[str, Any]) -> None:
    try:
        key = _event_key(item)
        if not key:
            return
        data = _load_shadow()
        existing = data.get(key)
        entry: dict[str, Any] = dict(existing) if isinstance(existing, Mapping) else {}
        entry["item"] = id_minimal(item)
        entry["watched_at"] = item.get("watched_at")
        entry["last_seen"] = _iso(int(datetime.now(timezone.utc).timestamp()))
        if "first_seen" not in entry:
            entry["first_seen"] = entry["last_seen"]
        data[key] = entry
        _save_shadow(data)
    except Exception:
        pass

def _has_external_ids(minimal: Mapping[str, Any]) -> bool:
    ids = minimal.get("ids") or {}
    show_ids = minimal.get("show_ids") or {}
    return bool(
        ids.get("imdb")
        or ids.get("tmdb")
        or ids.get("tvdb")
        or ids.get("trakt")
        or show_ids.get("imdb")
        or show_ids.get("tmdb")
        or show_ids.get("tvdb")
        or show_ids.get("trakt")
    )

def _guid_from_minimal(minimal: Mapping[str, Any]) -> str:
    ids = minimal.get("ids") or {}
    guid = minimal.get("guid") or ids.get("guid") or ids.get("plex_guid")
    return str(guid).lower() if guid else ""

def _keep_in_snapshot(adapter: Any, minimal: Mapping[str, Any]) -> bool:
    ignore_local = bool(_plex_cfg_get(adapter, "history_ignore_local_guid", False))
    prefixes = _plex_cfg_get(adapter, "history_ignore_guid_prefixes", ["local://"]) or []
    require_ext = bool(_plex_cfg_get(adapter, "history_require_external_ids", False))
    if require_ext and not _has_external_ids(minimal):
        return False
    if ignore_local:
        guid = _guid_from_minimal(minimal)
        if guid and any(guid.startswith(p.lower()) for p in prefixes):
            return False
    return True


def _marked_section_id(sec: Any) -> str | None:
    for attr in ("librarySectionID", "sectionID", "id", "key"):
        v = getattr(sec, attr, None)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if s.isdigit():
            return s
        m = re.search(r"/library/sections/(\d+)", s)
        if m:
            return m.group(1)
    return None

def _iter_marked_watched_from_library(
    adapter: Any,
    allow: set[str],
    since: int | None = None,
) -> list[tuple[dict[str, Any], int]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return []
    base = _as_base_url(srv)
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token):
        return []

    state = _load_marked_state()
    try:
        last_ts = int((state.get("last_ts") if isinstance(state, dict) else 0) or 0)
    except Exception:
        last_ts = 0
    cutoff = max(int(since or 0), last_ts) if (since is not None or last_ts) else 0

    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"

    def _rows_from(r: Any) -> tuple[list[Mapping[str, Any]], int | None]:
        try:
            ctype = (r.headers.get("content-type") or "").lower()
            data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
            mc = data.get("MediaContainer") or {}
            rows = mc.get("Metadata") or []
            total = mc.get("totalSize")
            total_i = int(total) if total is not None else None
            return [x for x in rows if isinstance(x, Mapping)], total_i
        except Exception:
            return [], None

    page_size = 200
    results: list[tuple[dict[str, Any], int]] = []
    newest = last_ts

    try:
        sections = list(adapter.libraries(types=("movie", "show")) or [])
    except Exception:
        sections = []

    for sec in sections:
        section_id = _marked_section_id(sec) or ""
        if not section_id:
            continue
        if allow and section_id not in allow:
            continue

        section_type = (getattr(sec, "type", "") or "").lower()
        plex_type = 1 if section_type == "movie" else 4 if section_type == "show" else None
        if plex_type is None:
            continue

        start = 0
        while True:
            params = {
                "type": plex_type,
                "unwatched": 0,
                "sort": "lastViewedAt:desc",
                "includeGuids": 1,
                "X-Plex-Container-Start": start,
                "X-Plex-Container-Size": page_size,
            }
            try:
                r = ses.get(f"{base}/library/sections/{section_id}/all", params=params, headers=headers, timeout=15)
            except Exception:
                break
            if not getattr(r, "ok", False):
                break

            rows, total = _rows_from(r)
            if not rows:
                break

            stop = False
            for row in rows:
                ts = _as_epoch(row.get("lastViewedAt") or row.get("viewedAt"))
                if not ts:
                    continue
                ts_i = int(ts)
                if cutoff and ts_i < cutoff:
                    stop = True
                    break
                if ts_i > newest:
                    newest = ts_i
                meta = normalize_discover_row(row, token=token) or {}
                if meta:
                    meta['_cw_marked'] = True
                    meta['watched_at'] = meta.get('watched_at') or _iso(int(ts_i))
                    results.append((meta, ts_i))

            if stop:
                break
            start += len(rows)
            if total is not None and start >= total:
                break
            if len(rows) < page_size:
                break

    if newest and newest != last_ts:
        try:
            st = dict(state) if isinstance(state, dict) else {}
            st["last_ts"] = newest
            _save_marked_state(st)
        except Exception:
            pass

    return results

def _pms_fetch_metadata_row(adapter: Any, rating_key: str) -> Mapping[str, Any] | None:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return None
    base = _as_base_url(srv)
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token and rating_key):
        return None
    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"
    try:
        r = ses.get(f"{base}/library/metadata/{rating_key}", headers=headers, timeout=15)
    except Exception:
        return None
    if not getattr(r, "ok", False):
        return None
    try:
        ctype = (r.headers.get("content-type") or "").lower()
        data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
        mc = data.get("MediaContainer") or {}
        rows = mc.get("Metadata") or []
        if isinstance(rows, list) and rows and isinstance(rows[0], Mapping):
            return rows[0]
    except Exception:
        return None
    return None


def _pms_row_is_watched(row: Mapping[str, Any]) -> bool:
    try:
        vc = row.get("viewCount")
        if vc is None:
            vc = row.get("leafCountViewed")
        return int(vc or 0) > 0
    except Exception:
        return False


def _pms_row_watched_ts(row: Mapping[str, Any]) -> int | None:
    return _as_epoch(row.get("lastViewedAt") or row.get("viewedAt"))
def build_index(adapter: Any, since: int | None = None, limit: int | None = None) -> dict[str, dict[str, Any]]:
    need_home_scope, did_home_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            _info("no_server", reason="account_only")
            return {}
        prog_mk = getattr(adapter, "progress_factory", None)
        prog: Any | None = prog_mk("history") if callable(prog_mk) else None
        fallback_guid = bool(_plex_cfg_get(adapter, "fallback_GUID", False) or _plex_cfg_get(adapter, "fallback_guid", False))
        if fallback_guid:
            _emit({"event": "debug", "msg": "fallback_guid.enabled", "provider": "PLEX", "feature": "history"})

        def _int_or_zero(v: Any) -> int:
            try:
                return int(v or 0)
            except Exception:
                return 0

        cfg_acct_id = _int_or_zero(_plex_cfg_get(adapter, "account_id", 0))
        cli_acct_id = _int_or_zero(getattr(getattr(adapter, "client", None), "user_account_id", None))
        acct_id = cfg_acct_id or cli_acct_id

        cfg_uname = str(_plex_cfg_get(adapter, "username", "") or "").strip().lower()
        cli_uname = str(getattr(getattr(adapter, "client", None), "user_username", "") or "").strip().lower()
        uname = cfg_uname or cli_uname

        wm_key = _wm_key(acct_id, uname)
        wm = _load_watermark(wm_key) if (since is None or int(since or 0) <= 0) else None
                # Treat cursors as *exclusive* to avoid re-reading the boundary event forever.
        if since is not None and int(since or 0) > 0:
            eff_since = int(since) + 1
        elif wm:
            eff_since = int(wm) + 1
        else:
            eff_since = None

        allow = _allowed_history_sec_ids(adapter)
        explicit_user = bool(cfg_acct_id or cfg_uname)

        # Optional cursor debugging (helps diagnose 1-item re-add loops).
        if str(os.environ.get("CW_PLEX_HISTORY_DEBUG_CURSOR", "")).strip().lower() in ("1", "true", "yes"):
            _dbg(
                "cursor",
                since_arg=int(since or 0) if since is not None else None,
                wm=int(wm or 0) if wm else None,
                eff_since=int(eff_since or 0) if eff_since else None,
                wm_key=wm_key,
            )

        base_kwargs: dict[str, Any] = {}
        if cfg_acct_id and (not cli_acct_id or int(cfg_acct_id) != int(cli_acct_id)):
            base_kwargs["accountID"] = int(cfg_acct_id)
        elif not explicit_user and cli_acct_id:
            base_kwargs["accountID"] = int(cli_acct_id)

        if eff_since is not None and eff_since > 0:
            base_kwargs["mindate"] = datetime.fromtimestamp(eff_since, tz=timezone.utc)

        maxresults = _int_or_zero(_history_cfg_get(adapter, "maxresults", 0))
        if maxresults:
            base_kwargs["maxresults"] = int(maxresults)

        def _call_history(**kwargs: Any) -> list[Any]:
            try:
                return list(srv.history(**kwargs) or [])
            except Exception as e:
                if "mindate" in kwargs:
                    _dbg("mindate_fallback_drop", error=str(e))
                    kwargs.pop("mindate", None)
                    return list(srv.history(**kwargs) or [])
                raise

        rows: list[Any] = []
        try:
            if allow:
                for sid in sorted(allow):
                    try:
                        kw = dict(base_kwargs)
                        kw["librarySectionID"] = int(sid)
                        part = _call_history(**kw)
                    except Exception:
                        part = []
                    if not part and "accountID" in kw and not explicit_user:
                        try:
                            kw2 = dict(kw)
                            kw2.pop("accountID", None)
                            part = _call_history(**kw2)
                        except Exception:
                            part = []
                    rows.extend(part)
            else:
                rows = _call_history(**base_kwargs)
                if not rows and "accountID" in base_kwargs and not explicit_user:
                    base_kwargs2 = dict(base_kwargs)
                    base_kwargs2.pop("accountID", None)
                    rows = _call_history(**base_kwargs2)
        except Exception as e:
            _warn("history_fetch_failed", error=str(e))
            rows = []

        max_seen = 0
        for r in rows:
            ts = _epoch_from_history_entry(r) or 0
            if ts and ts > max_seen:
                max_seen = ts

        total = len(rows)

        # Optional cursor debugging: show the rows that are considered "new" for this run.
        if eff_since is not None and str(os.environ.get("CW_PLEX_HISTORY_DEBUG_CURSOR", "")).strip().lower() in ("1", "true", "yes"):
            try:
                new_rows = []
                for rr in rows:
                    ts_i = _epoch_from_history_entry(rr) or 0
                    if ts_i and ts_i >= int(eff_since):
                        new_rows.append(rr)
                sample = []
                for rr in new_rows[:5]:
                    try:
                        sample.append({
                            "type": getattr(rr, "type", None),
                            "title": getattr(rr, "title", None),
                            "ratingKey": getattr(rr, "ratingKey", None),
                            "ts": _epoch_from_history_entry(rr),
                        })
                    except Exception:
                        continue
                _dbg("cursor.new_rows", count=len(new_rows), sample=sample)
            except Exception:
                pass
        if prog:
            prog.tick(0, total=total, force=True)

        out: dict[str, dict[str, Any]] = {}
        for i, raw in enumerate(rows, start=1):
            if prog:
                prog.tick(i, total=total)

            ts = _epoch_from_history_entry(raw)
            if not ts:
                continue
            ts_i = int(ts)

            if eff_since is not None and ts_i < int(eff_since):
                continue

            if allow:
                sid = _row_section_id(raw)
                if sid and sid not in allow:
                    continue

            aid = _account_id_from_history_entry(raw)
            if cfg_acct_id and aid is not None and int(aid) != int(cfg_acct_id):
                continue
            if cfg_uname:
                u = (_username_from_history_entry(raw) or "").strip().lower()
                if u and u != cfg_uname:
                    continue
            if not explicit_user and cli_acct_id and aid is not None and int(aid) != int(cli_acct_id):
                continue

            meta = minimal_from_history_row(raw, token=None, allow_discover=False)
            if not meta and fallback_guid:
                meta = minimal_from_history_row(raw, token=None, allow_discover=True)
            if not meta:
                continue
            if not _keep_in_snapshot(adapter, meta):
                continue

            row = dict(meta)
            _force_episode_title(row)
            row["watched"] = True
            row["watched_at"] = _iso(ts_i)

            key = f"{canonical_key(row)}@{ts_i}"
            out[key] = row
            if limit and len(out) >= int(limit):
                break

        include_marked = bool(_history_cfg_get(adapter, "include_marked_watched", True))
        include_shadow = bool(_history_cfg_get(adapter, "include_shadow", True))

        if include_shadow:
            shadow = _shadow_load()
            for _, entry in (shadow or {}).items():
                item = (entry or {}).get("item")
                watched_at = (entry or {}).get("watched_at")
                ts2 = _as_epoch(watched_at) if watched_at else None
                if not item or not ts2:
                    continue
                row = dict(item)
                row["_cw_marked"] = True
                _force_episode_title(row)
                row["watched"] = True
                row["watched_at"] = _iso(int(ts2))
                key = f"{canonical_key(row)}@{int(ts2)}"
                if key not in out and _keep_in_snapshot(adapter, row):
                    out[key] = row
                    if limit and len(out) >= int(limit):
                        break


        if include_marked and (not limit or len(out) < int(limit)):
            st = _load_marked_state() or {}
            marked0 = st.get("items") or {}
            marked: dict[str, Any] = dict(marked0) if isinstance(marked0, Mapping) else {}
            changed = False

            # Discover newly watched items
            found = 0
            for entry, ts_i in _iter_marked_watched_from_library(adapter, allow, since=eff_since):
                rk = str(ids_from(entry).get("plex") or "")
                if not rk:
                    continue
                prev = marked.get(rk)
                prev_ts = _as_epoch((prev or {}).get("watched_at")) if isinstance(prev, Mapping) else None
                if not prev or (ts_i and (not prev_ts or int(ts_i) != int(prev_ts))):
                    e = dict(entry)
                    e["_cw_marked"] = True
                    e["watched"] = True
                    e["watched_at"] = e.get("watched_at") or _iso(int(ts_i))
                    marked[rk] = e
                    changed = True
                found += 1

            # Validate watched/unwatched toggles directly from PMS metadata.
            for rk, item in list(marked.items()):
                if not isinstance(item, Mapping):
                    continue
                row = _pms_fetch_metadata_row(adapter, str(rk))
                if not row:
                    continue
                is_watched = _pms_row_is_watched(row)
                prev_watched = bool(item.get("watched"))
                ts = _pms_row_watched_ts(row)
                prev_ts = _as_epoch(item.get("watched_at"))
                if is_watched != prev_watched:
                    e = dict(item)
                    e["watched"] = bool(is_watched)
                    if is_watched:
                        if ts and (not prev_ts or int(ts) > int(prev_ts)):
                            use_ts = int(ts)
                        else:
                            use_ts = int(time.time())
                        e["watched_at"] = _iso(use_ts)
                    marked[rk] = e
                    changed = True
                elif is_watched:
                    # Keep watched_at stable; only upgrade if PMS has a newer timestamp.
                    if ts and (not prev_ts or int(ts) > int(prev_ts)):
                        e = dict(item)
                        e["watched"] = True
                        e["watched_at"] = _iso(int(ts))
                        marked[rk] = e
                        changed = True
                else:
                    # Unwatched: keep entry for future re-watch detection.
                    if prev_watched:
                        e = dict(item)
                        e["watched"] = False
                        marked[rk] = e
                        changed = True

            if found or changed:
                st = dict(st) if isinstance(st, Mapping) else {}
                st["items"] = marked
                st["last_updated_at"] = int(time.time())
                _save_marked_state(st)

            for rk, item in (marked or {}).items():
                if not isinstance(item, Mapping) or not item.get("watched"):
                    continue
                row = dict(item)
                _force_episode_title(row)
                ts3 = _as_epoch(row.get("watched_at"))
                if not ts3:
                    continue
                row["watched"] = True
                row["watched_at"] = _iso(int(ts3))
                key = f"{canonical_key(row)}@{int(ts3)}"
                if key not in out and _keep_in_snapshot(adapter, row):
                    out[key] = row
                    if limit and len(out) >= int(limit):
                        break

        if prog:
            prog.done(total=len(out), ok=True)

        if max_seen:
            _save_watermark(wm_key, int(max_seen))

        _info(
            "index_done",
            count=len(out),
            include_marked=include_marked,
            scanned=total,
            token_acct_id=(cli_acct_id or 0),
            selected=(cfg_acct_id or cli_acct_id or 0),
            since=(eff_since or 0),
        )
        return out

    finally:
        home_scope_exit(adapter, did_home_switch)

def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    need_home_scope, did_home_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            unresolved: list[dict[str, Any]] = []
            for item in items or []:
                _UNRES.freeze(item, action="add", reasons=["no_plex_server"])
                unresolved.append({"item": id_minimal(item), "hint": "no_plex_server"})
            _info("write_skipped", op="add", reason="no_server")
            return 0, unresolved

        if need_home_scope and not did_home_switch:
            _info("write_skipped", op="add", reason="home_scope_not_applied", selected=(sel_aid or sel_uname))
            unresolved = []
            for item in items or []:
                unresolved.append({"item": id_minimal(item), "hint": "home_scope_not_applied"})
            return 0, unresolved

        ok = 0
        unresolved: list[dict[str, Any]] = []
        for item in items or []:
            if _UNRES.is_frozen(item):
                _dbg("skip_frozen", title=id_minimal(item).get("title"))
                continue
            ts = _as_epoch(item.get("watched_at"))
            if not ts:
                _UNRES.freeze(item, action="add", reasons=["missing_watched_at"])
                unresolved.append({"item": id_minimal(item), "hint": "missing_watched_at"})
                continue
            rating_key = _resolve_rating_key(adapter, item)
            if not rating_key:
                _UNRES.freeze(item, action="add", reasons=["not_in_library"])
                unresolved.append({"item": id_minimal(item), "hint": "not_in_library"})
                continue
            if _scrobble_with_date(srv, rating_key, ts):
                ok += 1
                _UNRES.unfreeze([_event_key(item)])
                _shadow_add(item)
            else:
                _UNRES.freeze(item, action="add", reasons=["scrobble_failed"])
                unresolved.append({"item": id_minimal(item), "hint": "scrobble_failed"})
        _info("write_done", op="add", ok=ok, unresolved=len(unresolved))
        return ok, unresolved

    finally:
        home_scope_exit(adapter, did_home_switch)

def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    need_home_scope, did_home_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            unresolved: list[dict[str, Any]] = []
            for item in items or []:
                _UNRES.freeze(item, action="remove", reasons=["no_plex_server"])
                unresolved.append({"item": id_minimal(item), "hint": "no_plex_server"})
            _info("write_skipped", op="remove", reason="no_server")
            return 0, unresolved

        if need_home_scope and not did_home_switch:
            _info("write_skipped", op="remove", reason="home_scope_not_applied", selected=(sel_aid or sel_uname))
            unresolved = []
            for item in items or []:
                unresolved.append({"item": id_minimal(item), "hint": "home_scope_not_applied"})
            return 0, unresolved

        ok = 0
        unresolved: list[dict[str, Any]] = []
        for item in items or []:
            if _UNRES.is_frozen(item):
                _dbg("skip_frozen", title=id_minimal(item).get("title"))
                continue
            rating_key = _resolve_rating_key(adapter, item)
            if not rating_key:
                _UNRES.freeze(item, action="remove", reasons=["not_in_library"])
                unresolved.append({"item": id_minimal(item), "hint": "not_in_library"})
                continue
            if _unscrobble(srv, rating_key):
                ok += 1
                _UNRES.unfreeze([_event_key(item)])
            else:
                _UNRES.freeze(item, action="remove", reasons=["unscrobble_failed"])
                unresolved.append({"item": id_minimal(item), "hint": "unscrobble_failed"})
        _info("write_done", op="remove", ok=ok, unresolved=len(unresolved))
        return ok, unresolved

    finally:
        home_scope_exit(adapter, did_home_switch)

def _episode_rk_from_show(show_obj: Any, season: Any, episode: Any) -> str | None:
    def _match(ep: Any) -> str | None:
        try:
            season_ok = season is None or getattr(ep, "parentIndex", None) == season or getattr(ep, "seasonNumber", None) == season
            episode_ok = episode is None or getattr(ep, "index", None) == episode
            if season_ok and episode_ok:
                rk = getattr(ep, "ratingKey", None)
                return str(rk) if rk else None
        except Exception:
            return None
        return None

    try:
        try:
            episodes = show_obj.episodes() or []
        except Exception:
            episodes = []
        for ep in episodes:
            rk = _match(ep)
            if rk:
                return rk
    except Exception:
        pass

    try:
        srv = getattr(show_obj, "_server", None) or getattr(show_obj, "server", None)
        obj_id = getattr(show_obj, "ratingKey", None)
        if not (srv and obj_id and hasattr(srv, "_session")):
            return None

        def _scan_xml(path: str) -> str | None:
            try:
                resp = srv._session.get(
                    srv.url(path),
                    params={"X-Plex-Container-Start": 0, "X-Plex-Container-Size": 5000},
                    timeout=12,
                )
                if not resp.ok:
                    return None
                import xml.etree.ElementTree as ET

                root = ET.fromstring(resp.text or "")
                for ep in root.findall(".//Video"):
                    try:
                        season_ok = season is None or int(ep.attrib.get("parentIndex", "0") or "0") == int(season)
                        episode_ok = episode is None or int(ep.attrib.get("index", "0") or "0") == int(episode)
                        if season_ok and episode_ok:
                            rk = ep.attrib.get("ratingKey")
                            if rk:
                                return str(rk)
                    except Exception:
                        continue
            except Exception:
                return None
            return None

        rk = _scan_xml(f"/library/metadata/{obj_id}/children")
        if rk:
            return rk

        return _scan_xml(f"/library/metadata/{obj_id}/allLeaves")
    except Exception:
        return None

def _extract_show_ids(item: Mapping[str, Any]) -> dict[str, Any]:
    v = item.get("show_ids")
    if isinstance(v, Mapping):
        return dict(v)
    show = item.get("show") or item.get("series") or item.get("grandparent")
    if isinstance(show, Mapping):
        ids = show.get("ids")
        if isinstance(ids, Mapping):
            return dict(ids)
    return {}

def _has_matchable_ids(ids: Mapping[str, Any]) -> bool:
    for k in ("tmdb", "imdb", "tvdb"):
        if ids.get(k):
            return True
    return False

def _type_of_obj(obj: Any) -> str:
    return (getattr(obj, "type", "") or "").lower()

def _section_allowed(obj: Any, allow: set[str]) -> bool:
    if not allow:
        return True
    sid = str(getattr(obj, "librarySectionID", "") or getattr(obj, "sectionID", "") or "").strip()
    return not sid or sid in allow

def _guid_candidates(ids: Mapping[str, Any], show_ids: Mapping[str, Any], item: Mapping[str, Any]) -> list[str]:
    out: list[str] = []
    for g in candidate_guids_from_ids({"ids": ids, "guid": item.get("guid")}) or []:
        if g and g not in out:
            out.append(g)
    for g in candidate_guids_from_ids({"ids": show_ids, "guid": item.get("show_guid") or item.get("grandparentGuid")}) or []:
        if g and g not in out:
            out.append(g)
    for key in ("plex_guid", "grandparentGuid", "show_guid"):
        g = item.get(key)
        if g and str(g) not in out:
            out.append(str(g))
    return sort_guid_candidates(out)

def _resolve_obj_by_guids(srv: Any, guids: list[str], allow: set[str], accept: set[str]) -> Any | None:
    if not guids:
        return None
    rk = server_find_rating_key_by_guid(srv, guids)
    if not rk:
        return None
    try:
        obj = srv.fetchItem(int(rk))
    except Exception:
        obj = None
    if not obj:
        return None
    if accept and _type_of_obj(obj) not in accept:
        return None
    if not _section_allowed(obj, allow):
        return None
    return obj

def _resolve_rating_key(adapter: Any, item: Mapping[str, Any]) -> str | None:
    ids = ids_from(item)
    show_ids = _extract_show_ids(item)
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return None

    rk = ids.get("plex") or None
    if rk:
        try:
            if srv.fetchItem(int(rk)):
                return str(rk)
        except Exception:
            pass

    kind = (item.get("type") or "movie").lower()
    if kind == "anime":
        kind = "episode"
    is_episode = kind == "episode"

    strict = bool(_plex_cfg_get(adapter, "strict_id_matching", False))
    allow = _allowed_history_sec_ids(adapter)

    season = item.get("season") or item.get("season_number")
    episode = item.get("episode") or item.get("episode_number")

    guids = _guid_candidates(ids, show_ids, item)

    if strict:
        if not (guids or _has_matchable_ids(ids) or _has_matchable_ids(show_ids)):
            return None

        if is_episode:
            rk_show = show_ids.get("plex")
            if rk_show:
                try:
                    obj0 = srv.fetchItem(int(rk_show))
                except Exception:
                    obj0 = None
                if obj0 and _section_allowed(obj0, allow):
                    rk0 = _episode_rk_from_show(obj0, season, episode)
                    if rk0:
                        return rk0

            obj = _resolve_obj_by_guids(srv, guids, allow, {"episode"})
            if obj:
                return str(getattr(obj, "ratingKey", None) or "")
            obj2 = _resolve_obj_by_guids(srv, guids, allow, {"show", "season"})
            if obj2:
                rk2 = _episode_rk_from_show(obj2, season, episode)
                return rk2

            _build_guid_index(adapter, allow)
            rk_show_g = _pms_find_in_guid_index("show", guids)
            if rk_show_g:
                try:
                    obj_g = srv.fetchItem(int(rk_show_g))
                except Exception:
                    obj_g = None
                if obj_g and _section_allowed(obj_g, allow):
                    rk_g = _episode_rk_from_show(obj_g, season, episode)
                    if rk_g:
                        return rk_g
            return None

        obj = _resolve_obj_by_guids(srv, guids, allow, {"movie"})
        if obj:
            return str(getattr(obj, "ratingKey", None) or "")
        _build_guid_index(adapter, allow)
        rk_movie_g = _pms_find_in_guid_index("movie", guids)
        if rk_movie_g:
            if not allow:
                return str(rk_movie_g)
            try:
                obj_g = srv.fetchItem(int(rk_movie_g))
            except Exception:
                obj_g = None
            if obj_g and _section_allowed(obj_g, allow):
                return str(getattr(obj_g, "ratingKey", None) or "")
        return None

    title = (item.get("title") or "").strip()
    series_title = (item.get("series_title") or "").strip()
    query_title = series_title if is_episode and series_title else title
    year = item.get("year")

    if not (query_title or guids):
        return None

    sec_types = ("show",) if is_episode else ("movie",)
    hits: list[Any] = []

    obj = _resolve_obj_by_guids(srv, guids, allow, {"movie", "episode", "show", "season"})
    if obj:
        hits.append(obj)

    if not hits and query_title:
        for sec in adapter.libraries(types=sec_types) or []:
            section_id = str(getattr(sec, "key", "")).strip()
            if allow and section_id not in allow:
                continue
            try:
                search_hits = sec.search(title=query_title) or []
                if len(search_hits) == 1:
                    hits.extend(search_hits)
                    break
                hits.extend(search_hits)
            except Exception:
                continue

    if not hits and query_title:
        try:
            mediatype = "episode" if is_episode else "movie"
            search_hits = srv.search(query_title, mediatype=mediatype) or []
            for obj in search_hits:
                if _section_allowed(obj, allow):
                    hits.append(obj)
        except Exception:
            pass

    def _score(obj: Any) -> int:
        score = 0
        try:
            obj_title = (getattr(obj, "grandparentTitle", None) if is_episode else getattr(obj, "title", None)) or ""
            if obj_title.strip().lower() == query_title.lower():
                score += 3
            if not is_episode and year is not None and getattr(obj, "year", None) == year:
                score += 2
            if is_episode:
                s_ok = season is None or getattr(obj, "seasonNumber", None) == season or getattr(obj, "parentIndex", None) == season
                e_ok = episode is None or getattr(obj, "index", None) == episode
                if s_ok and e_ok:
                    score += 2
            meta_ids = (plex_normalize(obj).get("ids") or {})
            for key in ("tmdb", "imdb", "tvdb"):
                if key in meta_ids and key in ids and meta_ids[key] == ids[key]:
                    score += 4
                if key in meta_ids and key in show_ids and meta_ids[key] == show_ids[key]:
                    score += 2
        except Exception:
            pass
        return score

    if not hits:
        return None

    if is_episode:
        ep_hits = [o for o in hits if _type_of_obj(o) == "episode"]
        if ep_hits:
            best_ep = max(ep_hits, key=_score)
            rk_val = getattr(best_ep, "ratingKey", None)
            return str(rk_val) if rk_val else None
        show_hits = [o for o in hits if _type_of_obj(o) in ("show", "season")]
        for show in show_hits:
            rk_val = _episode_rk_from_show(show, season, episode)
            if rk_val:
                return rk_val
        return None

    best = max(hits, key=_score)
    rk_val = getattr(best, "ratingKey", None)
    return str(rk_val) if rk_val else None


def _active_token(srv: Any) -> str | None:
    try:
        ses = getattr(srv, "_session", None)
        tok = (getattr(ses, "headers", {}) or {}).get("X-Plex-Token")
        if tok and str(tok).strip():
            return str(tok).strip()
    except Exception:
        pass
    for attr in ("token", "_token"):
        tok = getattr(srv, attr, None)
        if tok and str(tok).strip():
            return str(tok).strip()
    return None

def _scrobble_with_date(srv: Any, rating_key: Any, epoch: int) -> bool:
    try:
        obj = None
        try:
            obj = srv.fetchItem(int(rating_key))
            obj_type = (getattr(obj, "type", "") or "").lower()
            if obj_type and obj_type not in ("episode", "movie"):
                return False
        except Exception:
            obj = None

        base = _as_base_url(srv)
        ses = getattr(srv, "_session", None)
        tok = _active_token(srv)
        if not (base and ses and tok):
            return False

        url = f"{base}/:/scrobble"
        headers = dict(getattr(ses, "headers", {}) or {})
        headers.update(plex_headers(tok))

        for key_name in ("key", "ratingKey"):
            params = {key_name: int(rating_key), "identifier": "com.plexapp.plugins.library", "viewedAt": int(epoch)}
            try:
                resp = ses.get(url, params=params, headers=headers, timeout=10)
            except Exception as e:
                _warn("scrobble_request_failed", rating_key=str(rating_key), error=str(e))
                continue

            # Plex often returns 200 before the library reflects the new view state. At least that is what i hope...
            if resp.ok:
                return True

            _warn(
                "scrobble_http_failed",
                rating_key=str(rating_key),
                status=resp.status_code,
                body_snippet=(resp.text or "")[:200].replace("\n", " "),
            )

        if obj:
            try:
                obj.markWatched()
                return True
            except Exception:
                pass

        return False

    except Exception as e:
        _warn("scrobble_exception", rating_key=str(rating_key), error=str(e))
        return False

def _unscrobble(srv: Any, rating_key: Any) -> bool:
    try:
        url = srv.url("/:/unscrobble")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library"}
        resp = srv._session.get(url, params=params, timeout=10)
        return resp.ok
    except Exception:
        return False