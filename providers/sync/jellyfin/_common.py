# /providers/sync/jellyfin/_common.py
# JELLYFIN Module for common functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from typing import Any, Mapping, Iterable, Sequence

import json
import os
import re
import time
import shutil
from pathlib import Path

from .._log import log as cw_log

from cw_platform.anime_mapping.service import mapped_or_default_media_type
from cw_platform.id_map import minimal as id_minimal, canonical_key

from ._routes import favorite as favorite_route, user_data as user_data_route, user_params

_DEF_TYPES = {"movie", "show", "episode"}
_IMDB_PAT = re.compile(r"(?:tt)?(\d{5,9})$")
_NUM_PAT = re.compile(r"(\d{1,10})$")


STATE_DIR = Path("/config/.cw_state")


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None




def _is_capture_mode() -> bool:
    v = str(os.getenv("CW_CAPTURE_MODE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def state_file(name: str) -> Path:
    scope = _pair_scope()
    p = Path(name)
    if not scope:
        if p.suffix:
            return STATE_DIR / f"{p.stem}{p.suffix}"
        return STATE_DIR / name
    safe = _safe_scope(scope)
    if p.suffix:
        scoped = STATE_DIR / f"{p.stem}.{safe}{p.suffix}"
        legacy = STATE_DIR / f"{p.stem}{p.suffix}"
    else:
        scoped = STATE_DIR / f"{name}.{safe}"
        legacy = STATE_DIR / name
    if (not _is_capture_mode()) and scoped != legacy and not scoped.exists() and legacy.exists():
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(legacy, scoped)
        except Exception:
            pass
    return scoped

_BAD_NUM = re.compile(r"^\d{13,}$")

CfgLike = Mapping[str, Any] | object


# logging

def _bootstrap_log_level() -> None:
    # Back-compat: CW_JELLYFIN_DEBUG_LEVEL (summary/verbose) -> CW_JELLYFIN_LOG_LEVEL (debug)
    if os.getenv('CW_JELLYFIN_LOG_LEVEL') or os.getenv('CW_LOG_LEVEL') or os.getenv('CW_DEBUG') or os.getenv('CW_JELLYFIN_DEBUG'):
        return
    v = (os.getenv('CW_JELLYFIN_DEBUG_LEVEL') or '').strip().lower()
    if v in ('2', 'v', 'verbose'):
        os.environ['CW_JELLYFIN_LOG_LEVEL'] = 'debug'
    elif v in ('1', 's', 'summary', 'true', 'on'):
        os.environ['CW_JELLYFIN_LOG_LEVEL'] = 'debug'


_bootstrap_log_level()


def _dbg(msg: str, **fields: Any) -> None:
    cw_log('JELLYFIN', 'common', 'debug', msg, **fields)


def _trc(msg: str, **fields: Any) -> None:
    cw_log('JELLYFIN', 'common', 'debug', msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log('JELLYFIN', 'common', 'info', msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log('JELLYFIN', 'common', 'warn', msg, **fields)


def make_logger(feature: str):  # type: ignore[return]
    def _dbg(msg: str, **fields: Any) -> None:
        cw_log('JELLYFIN', feature, 'debug', msg, **fields)

    def _info(msg: str, **fields: Any) -> None:
        cw_log('JELLYFIN', feature, 'info', msg, **fields)

    def _warn(msg: str, **fields: Any) -> None:
        cw_log('JELLYFIN', feature, 'warn', msg, **fields)

    return _dbg, _info, _warn


def _now_iso_z() -> str:
    from datetime import datetime, timezone
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# cfg helpers

def _as_list_str(v: Any) -> list[str]:
    if v is None:
        return []
    it = v if isinstance(v, (list, tuple, set)) else [v]
    out: list[str] = []
    seen: set[str] = set()
    for x in it:
        s = str(x).strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _pluck(cfg: CfgLike, *path: str) -> Any:
    cur: Any = cfg
    for key in path:
        if isinstance(cur, Mapping) and key in cur:
            cur = cur[key]
        else:
            cur = getattr(cur, key, None)
        if cur is None:
            return None
    return cur


# library id via ancestors cache
_jf_lib_anc_cache: dict[str, str | None] = {}


def _jf_lib_id_via_ancestors(http: Any, iid: str, roots: Mapping[str, Any]) -> str | None:
    if not iid:
        return None
    if iid in _jf_lib_anc_cache:
        return _jf_lib_anc_cache[iid]
    try:
        r = http.get(f"/Items/{iid}/Ancestors", params={"Fields": "Id"})
        if getattr(r, "status_code", 0) == 200:
            root_keys = {str(k) for k in (roots or {}).keys()}
            for a in (r.json() or []):
                aid = str((a or {}).get("Id") or "")
                if aid in root_keys:
                    _jf_lib_anc_cache[iid] = aid
                    return aid
    except Exception:
        pass
    _jf_lib_anc_cache[iid] = None
    return None


def jf_library_scope(cfg: CfgLike, feature: str) -> dict[str, Any]:
    jf = _pluck(cfg, "jellyfin") or cfg
    libs = _as_list_str(_pluck(jf, feature, "libraries"))
    if not libs:
        libs_attr = _as_list_str(getattr(cfg, f"{feature}_libraries", None))
        if libs_attr:
            libs = libs_attr
        else:
            sub = getattr(cfg, feature, None)
            if sub is not None:
                libs = _as_list_str(getattr(sub, "libraries", None))
    if not libs:
        return {}
    if len(libs) == 1:
        return {"ParentId": libs[0], "Recursive": True}
    return {"ParentIds": sorted(set(libs)), "Recursive": True}


def with_jf_scope(params: Mapping[str, Any], cfg: CfgLike, feature: str) -> dict[str, Any]:
    out = dict(params or {})
    out.update(jf_library_scope(cfg, feature))
    return out


def jf_scope_history(cfg: CfgLike) -> dict[str, Any]:
    return jf_library_scope(cfg, "history")


def jf_selected_library_ids(cfg: CfgLike, feature: str = "history") -> set[str]:
    scope = jf_scope_history(cfg) if feature == "history" else jf_library_scope(cfg, feature)
    parent = scope.get("ParentId")
    if parent:
        return {str(parent)}
    parents = scope.get("ParentIds") or []
    return {str(value) for value in parents if value}


def jf_scoped_params(params: Mapping[str, Any], cfg: CfgLike, feature: str) -> list[dict[str, Any]]:
    base = {key: value for key, value in dict(params or {}).items() if key not in {"AncestorIds", "ParentIds", "ParentId"}}
    libraries = sorted(jf_selected_library_ids(cfg, feature))
    if not libraries:
        return [base]
    return [{**base, "ParentId": library_id, "Recursive": True} for library_id in libraries]


def jf_get_scoped_items(http: Any, uid: str, params: Mapping[str, Any], cfg: CfgLike, feature: str) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for query in jf_scoped_params(params, cfg, feature):
        response = http.get("/Items", params={**query, "userId": uid})
        if getattr(response, "status_code", 0) == 200:
            rows.extend(row for row in ((response.json() or {}).get("Items") or []) if isinstance(row, Mapping))
    return sorted(rows, key=lambda row: (str(row.get("Id") or ""), str(row.get("LibraryId") or row.get("CollectionFolderId") or "")))


def jf_item_library_ids(item: Mapping[str, Any]) -> set[str]:
    out: set[str] = set()
    for key in ("LibraryId", "CollectionFolderId"):
        value = item.get(key)
        if value:
            out.add(str(value))
    ancestors = item.get("AncestorIds") or []
    if isinstance(ancestors, (list, tuple, set)):
        out.update(str(value) for value in ancestors if value)
    return out


def jf_filter_library_candidates(
    rows: Iterable[Mapping[str, Any]],
    allowed: set[str],
    *,
    trust_query_scope: bool = False,
) -> list[Mapping[str, Any]]:
    candidates = list(rows)
    if not allowed:
        return candidates
    matched = [row for row in candidates if jf_item_library_ids(row) & allowed]
    if matched:
        return matched
    if trust_query_scope and not any(jf_item_library_ids(row) for row in candidates):
        return candidates
    return []


def jf_scope_ratings(cfg: CfgLike) -> dict[str, Any]:
    return jf_library_scope(cfg, "ratings")


def jf_scope_any(cfg: CfgLike) -> dict[str, Any]:
    jf_map = _pluck(cfg, "jellyfin") or cfg
    libs_h = _as_list_str(_pluck(jf_map, "history", "libraries"))
    libs_r = _as_list_str(_pluck(jf_map, "ratings", "libraries"))
    libs: list[str] = []
    seen: set[str] = set()
    for x in libs_h + libs_r:
        if x and x not in seen:
            seen.add(x)
            libs.append(x)

    if not libs and not isinstance(cfg, Mapping):
        libs_h2 = _as_list_str(getattr(cfg, "history_libraries", None))
        libs_r2 = _as_list_str(getattr(cfg, "ratings_libraries", None))
        for x in libs_h2 + libs_r2:
            if x and x not in seen:
                seen.add(x)
                libs.append(x)

        hist_obj = getattr(cfg, "history", None)
        rate_obj = getattr(cfg, "ratings", None)
        if hasattr(hist_obj, "libraries"):
            for x in _as_list_str(getattr(hist_obj, "libraries", None)):
                if x and x not in seen:
                    seen.add(x)
                    libs.append(x)
        if hasattr(rate_obj, "libraries"):
            for x in _as_list_str(getattr(rate_obj, "libraries", None)):
                if x and x not in seen:
                    seen.add(x)
                    libs.append(x)

    if not libs:
        return {}
    if len(libs) == 1:
        return {"ParentId": libs[0], "Recursive": True}
    return {"ParentIds": sorted(set(libs)), "Recursive": True}


# library roots / mapping
def jf_build_library_roots(http: Any, user_id: str) -> dict[str, dict[str, Any]]:
    roots: dict[str, dict[str, Any]] = {}
    if not user_id:
        return roots
    try:
        url = f"/Users/{user_id}/Views"
        r = http.get(url)
        if getattr(r, "status_code", 0) == 200:
            data = r.json() or {}
            for it in data.get("Items") or []:
                lid = str(it.get("Id") or it.get("Key") or "").strip()
                if not lid:
                    continue
                meta = roots.setdefault(
            lid,
                    {
                        "id": lid,
                        "name": (it.get("Name") or it.get("Title") or "").strip() or None,
                        "collection_type": (it.get("CollectionType") or it.get("Type") or "").strip() or None,
                        "paths": [],
                    },
                )
                locs = it.get("Locations") or it.get("Path") or []
                if isinstance(locs, str):
                    locs = [locs]
                for p in locs:
                    s = str(p or "").strip()
                    if s and s not in meta["paths"]:
                        meta["paths"].append(s)
        if not roots:
            r2 = http.get("/Library/MediaFolders")
            if getattr(r2, "status_code", 0) == 200:
                data2 = r2.json() or {}
                for it in data2.get("Items") or []:
                    lid = str(it.get("Id") or it.get("Key") or "").strip()
                    if not lid:
                        continue
                    meta = roots.setdefault(
                        lid,
                        {
                            "id": lid,
                            "name": (it.get("Name") or it.get("Title") or "").strip() or None,
                            "collection_type": (it.get("CollectionType") or it.get("Type") or "").strip() or None,
                            "paths": [],
                        },
                    )
                    locs = it.get("Locations") or it.get("Path") or []
                    if isinstance(locs, str):
                        locs = [locs]
                    for p in locs:
                        s = str(p or "").strip()
                        if s and s not in meta["paths"]:
                            meta["paths"].append(s)
    except Exception:
        pass
    return roots


def jf_get_library_roots(adapter: Any) -> dict[str, dict[str, Any]]:
    roots = getattr(adapter, "_jf_library_roots", None)
    if isinstance(roots, dict) and roots:
        return roots
    http = getattr(adapter, "client", None)
    cfg = getattr(adapter, "cfg", None)
    user_id = getattr(cfg, "user_id", None) if cfg is not None else None
    if not http or not user_id:
        return {}
    roots = jf_build_library_roots(http, str(user_id))
    setattr(adapter, "_jf_library_roots", roots)
    return roots


def jf_resolve_library_id(
    row: Mapping[str, Any],
    roots: Mapping[str, Mapping[str, Any]],
    scope_libs: list[str] | None = None,
    http: Any | None = None,
    *,
    allow_deep_lookup: bool = True,
) -> str | None:
    if scope_libs and len(scope_libs) == 1:
        return str(scope_libs[0])

    candidates: list[str] = []

    lid = row.get("LibraryId")
    if lid:
        candidates.append(str(lid))

    anc = row.get("AncestorIds") or []
    for x in anc:
        if x:
            candidates.append(str(x))

    pid = row.get("ParentId")
    if isinstance(pid, str) and pid:
        candidates.append(pid)

    for cid in candidates:
        if cid in roots:
            return cid

    path = (row.get("Path") or "").strip()
    if path and roots:
        best: str | None = None
        best_len = -1
        lp = path.lower()
        for lib_id, meta in roots.items():
            for root in meta.get("paths") or []:
                rp = str(root or "").rstrip("/\\")
                if not rp:
                    continue
                rpl = rp.lower()
                if lp.startswith(rpl) and len(rp) > best_len:
                    best = lib_id
                    best_len = len(rp)
        if best:
            return best

    # Deep ancestor lookup is expensive
    if allow_deep_lookup and http and row.get("Id"):
        deep = _jf_lib_id_via_ancestors(http, str(row["Id"]), roots)
        if deep:
            return deep

    want: str | None = None
    ctype = (row.get("CollectionType") or "").strip().lower()
    if ctype:
        want = ctype
    else:
        t = (row.get("Type") or "").strip().lower()
        if t == "movie":
            want = "movies"
        elif t in ("series", "episode", "season"):
            want = "tvshows"

    if want:
        for lib_id, meta in roots.items():
            mt = (meta.get("collection_type") or "").strip().lower()
            if mt and mt == want:
                return lib_id

    return None


# type & id helpers
def _norm_type(t: Any) -> str:
    x = str(t or "").strip().lower()
    if x in ("movies", "movie"):
        return "movie"
    if x in ("shows", "show", "series", "tv", "anime", "tv_shows", "tvshows"):
        return "show"
    if x in ("episode", "episodes"):
        return "episode"
    return "movie"


def _lookup_type(it: Mapping[str, Any]) -> str:
    raw = _norm_type(it.get("type"))
    if raw == "episode":
        return raw
    return mapped_or_default_media_type(it)


def looks_like_bad_id(iid: Any) -> bool:
    s = str(iid or "")
    return bool(_BAD_NUM.match(s))


def _ids_from_provider_ids(pids: Mapping[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(pids, Mapping):
        return out
    low = {str(k).lower(): (v if v is not None else "") for k, v in pids.items()}

    v = low.get("imdb")
    if v is not None:
        m = _IMDB_PAT.search(str(v).strip())
        if m:
            out["imdb"] = f"tt{m.group(1)}"

    v = low.get("tmdb")
    if v is not None:
        m = _NUM_PAT.search(str(v).strip())
        if m:
            out["tmdb"] = m.group(1)

    v = low.get("tvdb")
    if v is not None:
        m = _NUM_PAT.search(str(v).strip())
        if m:
            out["tvdb"] = m.group(1)

    v = low.get("mal") or low.get("myanimelist") or low.get("myanimelistid")
    if v is not None:
        m = _NUM_PAT.search(str(v).strip())
        if m:
            out["mal"] = m.group(1)

    v = low.get("anilist") or low.get("anilistid")
    if v is not None:
        m = _NUM_PAT.search(str(v).strip())
        if m:
            out["anilist"] = m.group(1)

    jf = low.get("jellyfin")
    if jf:
        out["jellyfin"] = str(jf)
    return out


def normalize(obj: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(obj, Mapping) and "ids" in obj and "type" in obj:
        out = id_minimal(obj)
        raw = obj.get("jellyfin_item_id") or obj.get("_jellyfin_item_id") or obj.get("Id")
        if raw:
            out["jellyfin_item_id"] = str(raw)
        return out
    t = _norm_type(obj.get("Type") or obj.get("BaseItemKind") or obj.get("type"))
    title = (obj.get("Name") or obj.get("title") or "").strip() or None
    year = obj.get("ProductionYear") if isinstance(obj.get("ProductionYear"), int) else obj.get("year")
    pids = obj.get("ProviderIds") if isinstance(obj.get("ProviderIds"), Mapping) else (obj.get("ids") or {})
    ids = {k: v for k, v in _ids_from_provider_ids(pids).items() if v}
    jf_id = obj.get("Id") or (pids.get("jellyfin") if isinstance(pids, Mapping) else None)
    if jf_id:
        ids["jellyfin"] = str(jf_id)
    row: dict[str, Any] = {"type": t, "title": title, "year": year, "ids": ids}
    
    if jf_id:
        row["jellyfin_item_id"] = str(jf_id)
    if t == "episode":
        series_title = (
            obj.get("SeriesName")
            or obj.get("Series")
            or obj.get("SeriesTitle")
            or obj.get("series_title")
            or ""
        )
        series_title = series_title.strip() or None
        if series_title:
            row["series_title"] = series_title
        s = (
            obj.get("ParentIndexNumber")
            or obj.get("SeasonIndexNumber")
            or obj.get("season")
            or obj.get("season_number")
        )
        e = (
            obj.get("IndexNumber")
            or obj.get("EpisodeIndexNumber")
            or obj.get("episode")
            or obj.get("episode_number")
        )
        try:
            if s is not None:
                row["season"] = int(s)
        except Exception:
            pass
        try:
            if e is not None:
                row["episode"] = int(e)
        except Exception:
            pass
    out = id_minimal(row)
    if jf_id:
        out["jellyfin_item_id"] = str(jf_id)
    return out


def key_of(item: Mapping[str, Any]) -> str:
    return canonical_key(normalize(item))


def map_provider_key(k: str) -> str | None:
    if not k:
        return None
    kl = str(k).strip().lower()
    if kl.startswith("agent:themoviedb"):
        return "tmdb"
    if kl.startswith("agent:imdb"):
        return "imdb"
    if kl.startswith("agent:tvdb"):
        return "tvdb"
    if kl in ("tmdb", "imdb", "tvdb"):
        return kl
    return None


def format_provider_pair(k: str, v: Any) -> str | None:
    kk = map_provider_key(k)
    sv = str(v or "").strip()
    if not kk or not sv:
        return None
    if kk == "imdb":
        m = _IMDB_PAT.search(sv)
        sv = f"tt{m.group(1)}" if m else None
    else:
        m = _NUM_PAT.search(sv)
        sv = str(int(m.group(1))) if m else None
    return f"{kk}.{sv}" if sv else None


def guid_priority_from_cfg(cfg_list: Iterable[str] | None) -> list[str]:
    default = ["tmdb", "imdb", "tvdb", "agent:themoviedb:en", "agent:themoviedb", "agent:imdb"]
    if not cfg_list:
        return default
    seen: set[str] = set()
    out: list[str] = []
    for k in cfg_list:
        k = str(k).strip()
        if k and k not in seen:
            out.append(k)
            seen.add(k)
    for k in default:
        if k not in seen:
            out.append(k)
    return out


def pick_external_id(ids: Mapping[str, Any], priority: Iterable[str]) -> tuple[str, str] | None:
    for k in priority:
        v = ids.get(k)
        if v:
            return k, str(v)
    return None


def all_ext_pairs(it_ids: Mapping[str, Any], priority: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    ext = pick_external_id(dict(it_ids or {}), list(priority))
    if ext:
        p = format_provider_pair(ext[0], ext[1])
        if p and p not in seen:
            out.append(p)
            seen.add(p)
    for k in ("tmdb", "imdb", "tvdb"):
        v = (it_ids or {}).get(k)
        p = format_provider_pair(k, v) if v else None
        if p and p not in seen:
            out.append(p)
            seen.add(p)
    return out


# provider index
def build_provider_index(adapter: Any, *, feature: str | None = None) -> dict[str, list[dict[str, Any]]]:
    cache = getattr(adapter, "_provider_index_cache", None)
    scope_key = tuple(sorted(jf_selected_library_ids(adapter.cfg, feature or "history")))
    if isinstance(cache, dict) and cache and getattr(adapter, "_provider_index_scope", None) == scope_key:
        return cache

    http = adapter.client
    uid = adapter.cfg.user_id
    out: dict[str, list[dict[str, Any]]] = {}
    start = 0
    limit = 500
    total: int | None = None
    parents: list[str | None] = []
    parents.extend(sorted(jf_selected_library_ids(adapter.cfg, feature or "history")))
    if not parents:
        parents = [None]
    parent_index = 0
    seen_pages: set[tuple[str, ...]] = set()

    while True:
        params: dict[str, Any] = {
            "IncludeItemTypes": "Movie,Series",
            "Recursive": True,
            "Fields": "ProviderIds,ProductionYear,Type",
            "StartIndex": start,
            "Limit": limit,
            "EnableTotalRecordCount": True,
        }
        parent_id = parents[parent_index]
        if parent_id:
            params["ParentId"] = parent_id
        r = http.get("/Items", params={**params, "userId": uid})
        body = r.json() or {}
        items = body.get("Items") or []
        signature = tuple(str(row.get("Id") or "") for row in items if isinstance(row, Mapping))
        if items and signature in seen_pages:
            break
        seen_pages.add(signature)
        if total is None:
            total = int(body.get("TotalRecordCount") or 0)
            _dbg('index_fetch_counts', source='provider_index', total=total)
        for row in items:
            pids = row.get("ProviderIds") or {}
            if not pids:
                continue
            low = {str(k).lower(): str(v).strip() for k, v in pids.items() if v}
            imdb_val = low.get("imdb")
            if imdb_val:
                m_imdb = _IMDB_PAT.search(imdb_val)
                if m_imdb:
                    out.setdefault(f"imdb.tt{m_imdb.group(1)}", []).append(row)
            tmdb_val = low.get("tmdb")
            if tmdb_val:
                m_tmdb = _NUM_PAT.search(tmdb_val)
                if m_tmdb:
                    out.setdefault(f"tmdb.{int(m_tmdb.group(1))}", []).append(row)
            tvdb_val = low.get("tvdb")
            if tvdb_val:
                m_tvdb = _NUM_PAT.search(tvdb_val)
                if m_tvdb:
                    out.setdefault(f"tvdb.{int(m_tvdb.group(1))}", []).append(row)
        start += len(items)
        if not items or len(items) < limit or (total is not None and total > 0 and start >= total):
            parent_index += 1
            if parent_index >= len(parents):
                break
            start, total = 0, None
            seen_pages.clear()

    for k, rows in out.items():
        rows.sort(key=lambda r: str(r.get("Id") or ""))
    _dbg('index_done', source='provider_index', count=len(out))
    setattr(adapter, "_provider_index_cache", out)
    setattr(adapter, "_provider_index_scope", scope_key)
    return out


def find_series_in_index(adapter: Any, pairs: Iterable[str]) -> dict[str, Any] | None:
    idx = build_provider_index(adapter)
    for pref in pairs or []:
        rows = idx.get(pref) or []
        for row in rows:
            if (row.get("Type") or "").strip() == "Series":
                return row
    return None


# series/episodes listing
def get_series_episodes(
    http: Any,
    user_id: str,
    series_id: str,
    start: int = 0,
    limit: int = 500,
) -> dict[str, Any]:
    q = {
        "userId": user_id,
        "StartIndex": max(0, int(start)),
        "Limit": max(1, int(limit)),
        "Fields": "IndexNumber,ParentIndexNumber,SeasonId,SeriesId,ProviderIds,ProductionYear,Type",
        "EnableUserData": False,
    }
    r = http.get(f"/Shows/{series_id}/Episodes", params=q)
    if getattr(r, "status_code", 0) != 200:
        return {"Items": [], "TotalRecordCount": 0}
    data = r.json() or {}
    data.setdefault("Items", [])
    data.setdefault("TotalRecordCount", len(data["Items"]))
    return data


# playlists / collections
def find_playlist_id_by_name(http: Any, user_id: str, name: str) -> str | None:
    q = {
        "userId": user_id,
        "includeItemTypes": "Playlist",
        "recursive": True,
        "SearchTerm": name,
    }
    r = http.get("/Items", params=q)
    if getattr(r, "status_code", 0) != 200:
        return None
    items = (r.json() or {}).get("Items") or []
    name_l = (name or "").strip().lower()
    for it in items:
        if (it.get("Name") or "").strip().lower() == name_l:
            return it.get("Id")
    if not items:
        q = {"userId": user_id, "includeItemTypes": "Playlist", "recursive": True}
        r = http.get("/Items", params=q)
        if getattr(r, "status_code", 0) != 200:
            return None
        for it in (r.json() or {}).get("Items") or []:
            if (it.get("Name") or "").strip().lower() == name_l:
                return it.get("Id")
    return None


def create_playlist(http: Any, user_id: str, name: str, is_public: bool = False) -> str | None:
    body = {"Name": name, "UserId": user_id, "IsPublic": bool(is_public)}
    r = http.post("/Playlists", json=body)
    if getattr(r, "status_code", 0) not in (200, 204):
        return None
    data = r.json() or {}
    return data.get("Id") or data.get("PlaylistId") or data.get("id")


def get_playlist_items(http: Any, playlist_id: str, start: int = 0, limit: int = 100) -> dict[str, Any]:
    q = {
        "startIndex": max(0, int(start)),
        "limit": max(1, int(limit)),
        "Fields": "ProviderIds,ProductionYear,Type",
        "EnableUserData": False,
    }
    r = http.get(f"/Playlists/{playlist_id}/Items", params=q)
    if getattr(r, "status_code", 0) != 200:
        return {"Items": [], "TotalRecordCount": 0}
    data = r.json() or {}
    data.setdefault("Items", [])
    data.setdefault("TotalRecordCount", len(data["Items"]))
    return data

def playlist_fetch_all(
    http: Any,
    playlist_id: str,
    *,
    page_size: int = 500,
) -> tuple[list[Mapping[str, Any]], int]:
    out: list[Mapping[str, Any]] = []
    start = 0
    total: int | None = None
    while True:
        body = get_playlist_items(http, playlist_id, start=start, limit=page_size)
        rows: list[Mapping[str, Any]] = body.get("Items") or []
        if total is None:
            total = int(body.get("TotalRecordCount") or 0)
        out.extend(rows)
        start += len(rows)
        if not rows or (total is not None and start >= total):
            break
    return out, int(total or len(out))

def playlist_add_items(http: Any, playlist_id: str, user_id: str, item_ids: Iterable[str]) -> bool:
    ids = ",".join(str(x) for x in item_ids if x)
    if not ids:
        return True
    r = http.post(f"/Playlists/{playlist_id}/Items", params={"ids": ids, "userId": user_id})
    return getattr(r, "status_code", 0) in (200, 204)


def playlist_remove_entries(http: Any, playlist_id: str, entry_ids: Iterable[str]) -> bool:
    eids = ",".join(str(x) for x in entry_ids if x)
    if not eids:
        return True
    r = http.delete(f"/Playlists/{playlist_id}/Items", params={"entryIds": eids})
    return getattr(r, "status_code", 0) in (200, 204)


def find_collection_id_by_name(http: Any, user_id: str, name: str) -> str | None:
    try:
        r = http.get(
            "/Items",
            params={
                "userId": user_id,
                "IncludeItemTypes": "BoxSet",
                "Recursive": True,
                "SearchTerm": name,
                "Limit": 50,
            },
        )
        if getattr(r, "status_code", 0) != 200:
            return None
        items = (r.json() or {}).get("Items") or []
        name_lc = (name or "").strip().lower()
        for row in items:
            if (row.get("Type") == "BoxSet") and (
                str(row.get("Name") or "").strip().lower() == name_lc
            ):
                return str(row.get("Id"))
        r2 = http.get(
            "/Items",
            params={"userId": user_id, "IncludeItemTypes": "BoxSet", "Recursive": True},
        )
        if getattr(r2, "status_code", 0) != 200:
            return None
        for row in (r2.json() or {}).get("Items") or []:
            if (row.get("Type") == "BoxSet") and (
                str(row.get("Name") or "").strip().lower() == name_lc
            ):
                return str(row.get("Id"))
    except Exception:
        pass
    return None


def create_collection(http: Any, name: str) -> str | None:
    try:
        r = http.post("/Collections", params={"Name": name})
        if getattr(r, "status_code", 0) in (200, 201, 204):
            body = r.json() or {}
            cid = body.get("Id") or body.get("id")
            return str(cid) if cid else None
    except Exception:
        pass
    return None


def get_collection_items(http: Any, user_id: str, collection_id: str) -> dict[str, Any]:
    try:
        r = http.get(
            "/Items",
            params={
                "userId": user_id,
                "ParentId": collection_id,
                "Recursive": True,
                "IncludeItemTypes": "Movie,Series",
                "Fields": "ProviderIds,ProductionYear,Type",
                "EnableTotalRecordCount": True,
                "Limit": 10000,
            },
        )
        if getattr(r, "status_code", 0) != 200:
            return {"Items": [], "TotalRecordCount": 0}
        data = r.json() or {}
        data.setdefault("Items", [])
        data.setdefault("TotalRecordCount", len(data["Items"]))
        return data
    except Exception:
        return {"Items": [], "TotalRecordCount": 0}

def collection_fetch_all(
    http: Any,
    user_id: str,
    collection_id: str,
    *,
    page_size: int = 500,
) -> tuple[list[Mapping[str, Any]], int]:
    out: list[Mapping[str, Any]] = []
    start = 0
    total: int | None = None
    while True:
        r = http.get(
            "/Items",
            params={
                "userId": user_id,
                "IncludeItemTypes": "Movie,Series",
                "ParentId": collection_id,
                "Recursive": False,
                "Fields": "ProviderIds,ProductionYear,Type",
                "EnableTotalRecordCount": True,
                "StartIndex": start,
                "Limit": max(1, int(page_size)),
            },
        )
        if getattr(r, "status_code", 0) != 200:
            return out, int(total or len(out))
        body = r.json() or {}
        rows: list[Mapping[str, Any]] = body.get("Items") or []
        if total is None:
            total = int(body.get("TotalRecordCount") or 0)
        out.extend(rows)
        start += len(rows)
        if not rows or (total is not None and start >= total):
            break
    return out, int(total or len(out))

def collection_add_items(http: Any, collection_id: str, item_ids: Iterable[str]) -> bool:
    ids = ",".join(str(x) for x in item_ids if x)
    if not ids:
        return True
    try:
        r = http.post(f"/Collections/{collection_id}/Items", params={"Ids": ids})
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False


def collection_remove_items(http: Any, collection_id: str, item_ids: Iterable[str]) -> bool:
    ids = ",".join(str(x) for x in item_ids if x)
    if not ids:
        return True
    try:
        r = http.delete(f"/Collections/{collection_id}/Items", params={"Ids": ids})
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False


# misc writes
def mark_favorite(http: Any, user_id: str, item_id: str, flag: bool) -> bool:
    path = favorite_route(item_id)
    params = user_params(user_id)
    r = http.post(path, params=params) if flag else http.delete(path, params=params)
    ok = getattr(r, "status_code", 0) in (200, 204)
    if not ok:
        body_snip = "no-body"
        try:
            bj = r.json()
            s = json.dumps(bj, ensure_ascii=False)
            body_snip = (s[:200] + "…") if len(s) > 200 else s
        except Exception:
            try:
                t = r.text() if callable(getattr(r, "text", None)) else getattr(r, "text", "")
                s = str(t or "")
                body_snip = (s[:200] + "…") if len(s) > 200 else s
            except Exception:
                body_snip = "no-body"
        _warn('write_failed', op='favorite', user_id=user_id, item_id=item_id, status=getattr(r,'status_code',None), body=body_snip)
    return ok


def update_userdata(http: Any, user_id: str, item_id: str, payload: Mapping[str, Any]) -> bool:
    try:
        r = http.post(
            user_data_route(item_id),
            params=user_params(user_id),
            json=dict(payload),
        )
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False


# resolver (movie/show/episode)
def _pick_from_candidates(
    cands: Sequence[Mapping[str, Any]],
    *,
    want_type: str | None,
    want_year: int | None,
) -> str | None:
    def score_val(row: Mapping[str, Any]) -> tuple[int, int, str]:
        t = (row.get("Type") or "").strip()
        y = row.get("ProductionYear")
        s = 0
        if want_type:
            if want_type == "movie" and t == "Movie":
                s += 3
            if want_type in ("show", "series") and t == "Series":
                s += 3
            if want_type == "episode" and t == "Episode":
                s += 3
        if isinstance(want_year, int) and isinstance(y, int) and abs(y - want_year) <= 1:
            s += 1
        if row.get("ProviderIds"):
            s += 1
        iid = str(row.get("Id") or "")
        return -s, len(iid), iid

    if not cands:
        return None
    best = min(cands, key=score_val)
    iid = best.get("Id")
    return str(iid) if iid and not looks_like_bad_id(iid) else None


def _direct_query_by_pairs(
    http: Any,
    uid: str,
    pairs: list[str],
    include_types: str,
    scope: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    if not pairs:
        return []
    q: dict[str, Any] = {
        "AnyProviderIdEquals": ",".join(pairs),
        "IncludeItemTypes": include_types,
        "Recursive": True,
        "Fields": "ProviderIds,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesId,ParentId,CollectionFolderId,AncestorIds,LibraryId,Name",
        "Limit": 50,
    }
    scope_values = dict(scope or {})
    parent_ids = [str(value) for value in scope_values.pop("ParentIds", []) if value]
    q.update(scope_values)
    try:
        rows: list[Mapping[str, Any]] = []
        queries = [{**q, "ParentId": value, "Recursive": True} for value in sorted(parent_ids)] or [q]
        for query in queries:
            r = http.get("/Items", params={**query, "userId": uid})
            if getattr(r, "status_code", 0) == 200:
                rows.extend((r.json() or {}).get("Items") or [])
        return rows
    except Exception:
        return []


def _episode_number_matches(row: Mapping[str, Any], season: Any, episode: Any) -> bool:
    row_season = row.get("ParentIndexNumber")
    row_episode = row.get("IndexNumber")
    if row_season is None or row_episode is None or season is None or episode is None:
        return False
    try:
        return (
            int(row_season) == int(season)
            and int(row_episode) == int(episode)
        )
    except (TypeError, ValueError):
        return False


def resolve_item_id(adapter: Any, it: Mapping[str, Any], *, feature: str = "history") -> str | None:
    http = adapter.client
    uid = adapter.cfg.user_id
    selected_libs = jf_selected_library_ids(adapter.cfg, feature)
    setattr(adapter, "_jellyfin_last_resolve_hint", None)
    outside_scope_seen = False

    # Prefer native Jellyfin item id when present.
    raw_iid = it.get("jellyfin_item_id") or it.get("_jellyfin_item_id")
    if raw_iid:
        s = str(raw_iid).strip()
        if s and not looks_like_bad_id(s):
            _dbg('resolve_hit', kind='direct', method='item_field', item_id=s)
            return s

    ids = dict(it.get("ids") or {})
    show_ids = it.get("show_ids") if isinstance(it.get("show_ids"), Mapping) else None

    jf = ids.get("jellyfin")
    if jf and not looks_like_bad_id(jf):
        if selected_libs:
            try:
                response = http.get(
                    f"/Items/{jf}",
                    params={"userId": uid, "Fields": "LibraryId,CollectionFolderId,AncestorIds,ParentId,Type"},
                )
                row = response.json() or {} if getattr(response, "status_code", 0) == 200 else {}
            except Exception:
                row = {}
            if not jf_filter_library_candidates([row] if row else [], selected_libs):
                outside_scope_seen = True
                setattr(adapter, "_jellyfin_last_resolve_hint", "outside_library_scope")
                _dbg('target_candidate_outside_library_scope', item_id=str(jf), allowed_library_ids=sorted(selected_libs), resolution_method='provider_id')
            else:
                return str(jf)
        else:
            _dbg('resolve_hit', kind='direct', method='provider_id', item_id=str(jf))
            return str(jf)

    t = _lookup_type(it)
    title = (it.get("title") or "").strip()
    year = it.get("year")
    season = it.get("season")
    episode = it.get("episode")
    series_title = (it.get("series_title") or "").strip()

    strict = bool(getattr(getattr(adapter, "cfg", None), "strict_id_matching", False))

    prio = guid_priority_from_cfg(getattr(getattr(adapter, "cfg", None), "watchlist_guid_priority", None))
    episode_pairs = all_ext_pairs(ids, prio)
    series_pairs = all_ext_pairs(show_ids, prio) if show_ids else []
    pairs = list(episode_pairs)

    if series_pairs:
        for p in series_pairs:
            if p not in pairs:
                pairs.append(p)

    # Movies
    if t == "movie":
        idx = build_provider_index(adapter) if feature == "history" else build_provider_index(adapter, feature=feature)
        for pref in pairs:
            raw_cands = idx.get(pref) or []
            cands = jf_filter_library_candidates(raw_cands, selected_libs)
            if raw_cands and not cands and selected_libs:
                outside_scope_seen = True
            iid = _pick_from_candidates(cands, want_type="movie", want_year=year)
            if iid:
                _dbg('resolve_hit', kind='movie', method='provider_index', pref=pref, item_id=iid)
                return iid
        if title and not strict:
            try:
                q: dict[str, Any] = {
                    "userId": uid,
                    "recursive": True,
                    "includeItemTypes": "Movie",
                    "SearchTerm": title,
                    "Fields": "ProviderIds,ProductionYear,Type",
                    "Limit": 50,
                }
                t_l = title.lower()
                cand: list[Mapping[str, Any]] = []
                raw_rows = jf_get_scoped_items(http, uid, q, adapter.cfg, feature)
                scoped_rows = jf_filter_library_candidates(raw_rows, selected_libs, trust_query_scope=True)
                if raw_rows and not scoped_rows and selected_libs:
                    outside_scope_seen = True
                for row in scoped_rows:
                    if (row.get("Type") or "") != "Movie":
                        continue
                    nm = (row.get("Name") or "").strip().lower()
                    yr = row.get("ProductionYear")
                    if nm == t_l and ((year is None) or (isinstance(yr, int) and abs(yr - year) <= 1)):
                        cand.append(row)
                cand.sort(key=lambda x: 0 if (x.get("ProviderIds") or {}) else 1)
                for row in cand:
                    iid = row.get("Id")
                    if iid and not looks_like_bad_id(iid):
                        _dbg('resolve_hit', kind='movie', method='search', title=title, year=year, item_id=str(iid))
                        return str(iid)
            except Exception:
                pass
        _dbg('resolve_miss', kind='movie', title=title, year=year)
        if outside_scope_seen:
            setattr(adapter, "_jellyfin_last_resolve_hint", "outside_library_scope")
        return None

    # Shows
    if t in ("show", "series"):
        idx = build_provider_index(adapter) if feature == "history" else build_provider_index(adapter, feature=feature)
        for pref in pairs:
            raw_rows = idx.get(pref) or []
            rows = jf_filter_library_candidates(raw_rows, selected_libs)
            if raw_rows and not rows and selected_libs:
                outside_scope_seen = True
            cands = [row for row in rows if (row.get("Type") or "").strip() == "Series"]
            iid = _pick_from_candidates(cands, want_type="show", want_year=year)
            if iid:
                _dbg('resolve_hit', kind='series', method='provider_index', title=(title or ''), pref=pref, year=year, item_id=str(iid))
                return iid
        if title and not strict:
            try:
                q = {
                    "userId": uid,
                    "recursive": True,
                    "includeItemTypes": "Series",
                    "SearchTerm": title,
                    "Fields": "ProviderIds,ProductionYear,Type",
                    "Limit": 50,
                }
                title_lc = title.lower()
                cand = []
                raw_rows = jf_get_scoped_items(http, uid, q, adapter.cfg, feature)
                scoped_rows = jf_filter_library_candidates(raw_rows, selected_libs, trust_query_scope=True)
                if raw_rows and not scoped_rows and selected_libs:
                    outside_scope_seen = True
                for row in scoped_rows:
                    if (row.get("Type") or "") != "Series":
                        continue
                    nm = (row.get("Name") or "").strip().lower()
                    yr = row.get("ProductionYear")
                    if (nm == title_lc or nm.startswith(title_lc)) and (
                        (year is None) or (isinstance(yr, int) and abs(yr - year) <= 1)
                    ):
                        cand.append(row)
                cand.sort(key=lambda x: 0 if (x.get("ProviderIds") or {}) else 1)
                for row in cand:
                    iid = row.get("Id")
                    if iid and not looks_like_bad_id(iid):
                        _dbg('resolve_hit', kind='series', method='search', title=title, year=year, item_id=str(iid))
                        return str(iid)
            except Exception:
                pass
        _dbg('resolve_miss', kind='series', title=title, year=year)
        if outside_scope_seen:
            setattr(adapter, "_jellyfin_last_resolve_hint", "outside_library_scope")
        return None

    # Episodes
    scope = jf_library_scope(adapter.cfg, feature)
    for pref in episode_pairs:
        rows = _direct_query_by_pairs(http, uid, [pref], "Episode,Series", scope)
        episode_rows: list[Mapping[str, Any]] = []
        seen_episode_ids: set[str] = set()
        filtered_rows = jf_filter_library_candidates(rows, selected_libs, trust_query_scope=True)
        if rows and not filtered_rows and selected_libs:
            outside_scope_seen = True
        for row in filtered_rows:
            iid = str(row.get("Id") or "").strip()
            if (row.get("Type") or "") != "Episode" or not iid or looks_like_bad_id(iid):
                continue
            if iid not in seen_episode_ids:
                seen_episode_ids.add(iid)
                episode_rows.append(row)
        provider_type, _, provider_value = pref.partition(".")
        if len(episode_rows) == 1:
            iid = str(episode_rows[0]["Id"])
            _dbg(
                'resolve_hit',
                kind='episode',
                method='exact_episode_provider_id',
                provider_type=provider_type,
                provider_value=provider_value,
                item_id=iid,
            )
            return iid
        if len(episode_rows) > 1:
            numbered = [
                row
                for row in episode_rows
                if _episode_number_matches(row, season, episode)
            ]
            if len(numbered) == 1:
                iid = str(numbered[0]["Id"])
                _dbg(
                    'resolve_hit',
                    kind='episode',
                    method='episode_provider_id_number_disambiguation',
                    provider_type=provider_type,
                    provider_value=provider_value,
                    season=season,
                    episode=episode,
                    item_id=iid,
                )
                return iid
            _dbg(
                'resolve_miss',
                kind='episode',
                method='ambiguous_episode_provider_id',
                provider_type=provider_type,
                provider_value=provider_value,
                candidate_count=len(episode_rows),
                numbered_candidate_count=len(numbered),
                season=season,
                episode=episode,
            )

    series_row: dict[str, Any] | None = None
    matched_series_pair: str | None = None
    for pref in series_pairs:
        rows = _direct_query_by_pairs(http, uid, [pref], "Series", scope)
        scoped_rows = jf_filter_library_candidates(rows, selected_libs, trust_query_scope=True)
        if rows and not scoped_rows and selected_libs:
            outside_scope_seen = True
        series_rows = [row for row in scoped_rows if (row.get("Type") or "") == "Series"]
        if series_rows:
            series_row = dict(series_rows[0])
            matched_series_pair = pref
            break
    if not series_row and series_pairs:
        idx = build_provider_index(adapter) if feature == "history" else build_provider_index(adapter, feature=feature)
        for pref in series_pairs:
            raw_rows = idx.get(pref) or []
            rows = jf_filter_library_candidates(raw_rows, selected_libs)
            if raw_rows and not rows and selected_libs:
                outside_scope_seen = True
            series_row = next(
                (dict(row) for row in rows if (row.get("Type") or "").strip() == "Series"),
                None,
            )
            if series_row:
                matched_series_pair = pref
                break
    if not series_row and series_title and not strict:
        try:
            q = {
                "userId": uid,
                "recursive": True,
                "includeItemTypes": "Series",
                "SearchTerm": series_title,
                "Fields": "ProviderIds,ProductionYear,Type",
                "Limit": 50,
            }
            t_l = series_title.lower()
            cands = []
            raw_rows = jf_get_scoped_items(http, uid, q, adapter.cfg, feature)
            scoped_rows = jf_filter_library_candidates(raw_rows, selected_libs, trust_query_scope=True)
            if raw_rows and not scoped_rows and selected_libs:
                outside_scope_seen = True
            for row in scoped_rows:
                if (row.get("Type") or "") != "Series":
                    continue
                nm = (row.get("Name") or "").strip().lower()
                if nm == t_l:
                    cands.append(row)
            cands.sort(key=lambda x: 0 if (x.get("ProviderIds") or {}) else 1)
            if cands:
                series_row = cands[0]
                _dbg('resolve_hit', kind='series', method='title_candidate', series_title=series_title)
        except Exception:
            pass

    if series_row and season is not None and episode is not None:
        sid = series_row.get("Id")
        if sid:
            eps = get_series_episodes(http, uid, sid, start=0, limit=10000)
            for row in eps.get("Items") or []:
                s = row.get("ParentIndexNumber")
                e = row.get("IndexNumber")
                if isinstance(s, int) and isinstance(e, int) and s == int(season) and e == int(episode):
                    iid = row.get("Id")
                    if iid and not looks_like_bad_id(iid):
                        _dbg(
                            'resolve_hit',
                            kind='episode',
                            method='show_provider_id_episode_number',
                            provider_type=(matched_series_pair or '').partition('.')[0],
                            provider_value=(matched_series_pair or '').partition('.')[2],
                            season=int(season),
                            episode=int(episode),
                            item_id=str(iid),
                        )
                        return str(iid)

    if title and not strict:
        try:
            q = {
                "userId": uid,
                "recursive": True,
                "includeItemTypes": "Episode",
                "SearchTerm": title,
                "Fields": "ProviderIds,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesId",
                "Limit": 50,
            }
            t_l = title.lower()
            raw_rows = jf_get_scoped_items(http, uid, q, adapter.cfg, feature)
            scoped_rows = jf_filter_library_candidates(raw_rows, selected_libs, trust_query_scope=True)
            if raw_rows and not scoped_rows and selected_libs:
                outside_scope_seen = True
            for row in scoped_rows:
                if (row.get("Type") or "") != "Episode":
                    continue
                nm = (row.get("Name") or "").strip().lower()
                s = row.get("ParentIndexNumber")
                e = row.get("IndexNumber")
                if nm == t_l and ((season is None) or s == season) and ((episode is None) or e == episode):
                    iid = row.get("Id")
                    if iid and not looks_like_bad_id(iid):
                        _dbg('resolve_hit', kind='episode', method='search', title=title, season=season, episode=episode, item_id=str(iid))
                        return str(iid)
        except Exception:
            pass

    _dbg('resolve_miss', kind='episode', title=title, series_title=series_title, season=season, episode=episode)
    if outside_scope_seen:
        setattr(adapter, "_jellyfin_last_resolve_hint", "outside_library_scope")
    return None


def resolve_item_ids(adapter: Any, it: Mapping[str, Any], *, feature: str = "history") -> list[str]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        return []

    # Prefer native Jellyfin item id when present
    raw_iid = it.get("jellyfin_item_id") or it.get("_jellyfin_item_id")
    if raw_iid:
        s = str(raw_iid).strip()
        if s and not looks_like_bad_id(s):
            return [s]

    one = resolve_item_id(adapter, it, feature=feature)
    selected_libs = jf_selected_library_ids(adapter.cfg, feature)

    ids = dict(it.get("ids") or {})
    native_id = str(ids.get("jellyfin") or "").strip()
    if one and native_id and str(one) == native_id:
        return [str(one)]
    show_ids = it.get("show_ids") if isinstance(it.get("show_ids"), Mapping) else None

    t = _lookup_type(it)
    title = (it.get("title") or "").strip()
    year = it.get("year")
    season = it.get("season")
    episode = it.get("episode")
    series_title = (it.get("series_title") or "").strip()

    strict = bool(getattr(getattr(adapter, "cfg", None), "strict_id_matching", False))

    prio = guid_priority_from_cfg(getattr(getattr(adapter, "cfg", None), "watchlist_guid_priority", None))
    pairs = all_ext_pairs(ids, prio)
    if show_ids:
        spairs = all_ext_pairs(show_ids, prio)
        for p in spairs:
            if p not in pairs:
                pairs.append(p)

    idx = build_provider_index(adapter) if feature == "history" else build_provider_index(adapter, feature=feature)

    def _valid(iid: Any) -> str | None:
        s = str(iid or "").strip()
        return s if s and not looks_like_bad_id(s) else None

    found: list[str] = []

    # Movies
    if t == "movie":
        for pref in pairs:
            rows = jf_filter_library_candidates(idx.get(pref) or [], selected_libs)
            cands = [row for row in rows if (row.get("Type") or "") == "Movie"]
            if isinstance(year, int):
                yr = int(year)
                cands_yr = [
                    r for r in cands
                    if isinstance(r.get("ProductionYear"), int) and abs(int(r["ProductionYear"]) - yr) <= 1
                ]
                if cands_yr:
                    cands = cands_yr
            for row in cands:
                iid = _valid(row.get("Id"))
                if iid and iid not in found:
                    found.append(iid)
            if found:
                return found

        if title and not strict:
            try:
                q: dict[str, Any] = {
                    "userId": uid,
                    "recursive": True,
                    "includeItemTypes": "Movie",
                    "SearchTerm": title,
                    "Fields": "ProviderIds,ProductionYear,Type",
                    "Limit": 50,
                }
                t_l = title.lower()
                for row in jf_filter_library_candidates(jf_get_scoped_items(http, uid, q, adapter.cfg, feature), selected_libs, trust_query_scope=True):
                    if (row.get("Type") or "") != "Movie":
                        continue
                    nm = (row.get("Name") or "").strip().lower()
                    yr = row.get("ProductionYear")
                    if nm != t_l:
                        continue
                    if (year is not None) and not (isinstance(yr, int) and abs(int(yr) - int(year)) <= 1):
                        continue
                    iid = _valid(row.get("Id"))
                    if iid and iid not in found:
                        found.append(iid)
            except Exception:
                pass

    # Episodes
    if t == "episode":
        for pref in pairs:
            rows = jf_filter_library_candidates(idx.get(pref) or [], selected_libs)
            cands = [row for row in rows if (row.get("Type") or "") == "Episode"]
            for row in cands:
                try:
                    s_ok = (season is None) or (int(row.get("ParentIndexNumber") or 0) == int(season))
                    e_ok = (episode is None) or (int(row.get("IndexNumber") or 0) == int(episode))
                except Exception:
                    s_ok, e_ok = True, True
                if not (s_ok and e_ok):
                    continue
                iid = _valid(row.get("Id"))
                if iid and iid not in found:
                    found.append(iid)
            if found:
                return found

        if series_title and not strict:
            try:
                q: dict[str, Any] = {
                    "userId": uid,
                    "recursive": True,
                    "includeItemTypes": "Episode",
                    "SearchTerm": series_title,
                    "Fields": "ProviderIds,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesName",
                    "Limit": 200,
                }
                st_l = series_title.lower()
                for row in jf_filter_library_candidates(jf_get_scoped_items(http, uid, q, adapter.cfg, feature), selected_libs, trust_query_scope=True):
                    if (row.get("Type") or "") != "Episode":
                        continue
                    sn = (row.get("SeriesName") or "").strip().lower()
                    if sn != st_l:
                        continue
                    try:
                        s_ok = (season is None) or (int(row.get("ParentIndexNumber") or 0) == int(season))
                        e_ok = (episode is None) or (int(row.get("IndexNumber") or 0) == int(episode))
                    except Exception:
                        s_ok, e_ok = True, True
                    if not (s_ok and e_ok):
                        continue
                    iid = _valid(row.get("Id"))
                    if iid and iid not in found:
                        found.append(iid)
            except Exception:
                pass

    if found:
        return found
    return [one] if one else []


# utilities
def chunked(it: Iterable[Any], n: int) -> Iterable[list[Any]]:
    n = max(1, int(n))
    buf: list[Any] = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf


def sleep_ms(ms: int) -> None:
    m = int(ms or 0)
    if m > 0:
        time.sleep(m / 1000.0)
