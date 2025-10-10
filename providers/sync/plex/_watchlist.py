# /providers/sync/plex/_watchlist.py
# Plex Watchlist via Discover; PMS fallback optional.
# GUID-first, ID-true matching (tmdb/imdb/tvdb), no fuzzy titles.
# Emits snapshot:progress with a fixed total.

from __future__ import annotations
import os, json, time, random
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, List

try:
    from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from, ids_from_guid
except Exception:
    from _id_map import canonical_key, minimal as id_minimal, ids_from, ids_from_guid  # type: ignore

from ._common import (
    DISCOVER, METADATA,
    plex_headers, normalize_discover_row, ids_from_discover_row, _xml_to_container,
    candidate_guids_from_ids, meta_guids, section_find_by_guid, hydrate_external_ids,
)
from .._mod_common import request_with_retries  # instrumented (api:hit etc.)

UNRESOLVED_PATH = "/config/.cw_state/plex_watchlist.unresolved.json"

# ── logging ───────────────────────────────────────────────────────────────────

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:watchlist] {msg}")

# ── time / utils ──────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _sleep_ms(ms: int):
    try:
        if ms and ms > 0:
            time.sleep(ms / 1000.0)
    except Exception:
        pass

def _cfg(adapter) -> Dict[str, Any]:
    c = getattr(adapter, "config", {}) or {}
    return c.get("plex", {}) if isinstance(c, dict) else {}

def _cfg_int(d: Mapping[str, Any], key: str, default: int) -> int:
    try: return int(d.get(key, default))
    except Exception: return default

def _cfg_bool(d: Mapping[str, Any], key: str, default: bool) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    return True if s in ("1","true","yes","on") else False if s in ("0","false","no","off") else default

def _cfg_list(d: Mapping[str, Any], key: str, default: List[str]) -> List[str]:
    v = d.get(key, default)
    if isinstance(v, list):
        return [str(x) for x in v]
    return list(default)

# ── unresolved (hard freeze) ──────────────────────────────────────────────────

def _load_unresolved() -> Dict[str, Any]:
    try:
        with open(UNRESOLVED_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _save_unresolved(data: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(UNRESOLVED_PATH), exist_ok=True)
        tmp = UNRESOLVED_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, UNRESOLVED_PATH)
    except Exception as e:
        _log(f"unresolved.save failed: {e}")

def _freeze_item(item: Mapping[str, Any], *, action: str, reasons: List[str], guids_tried: List[str]) -> None:
    key = canonical_key(item)
    data = _load_unresolved()
    entry = data.get(key) or {"feature": "watchlist", "action": action, "first_seen": _now_iso(), "attempts": 0}
    entry.update({"item": id_minimal(item), "last_attempt": _now_iso()})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    gset = set(entry.get("guids_tried", [])) | set((guids_tried or [])[:8])  # cap for cleanliness
    entry["reasons"] = sorted(rset); entry["guids_tried"] = sorted(gset)
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)

def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved(); changed = False
    for k in list(keys or []):
        if k in data: del data[k]; changed = True
    if changed: _save_unresolved(data)

def _is_frozen(item: Mapping[str, Any]) -> bool:
    return canonical_key(item) in _load_unresolved()

# ── per-run caches ────────────────────────────────────────────────────────────

_HYDRATE_CACHE: Dict[str, Dict[str, Any]] = {}  # ratingKey -> {tmdb/imdb/tvdb}
_GUID_INDEX_MOVIE: Dict[str, Any] = {}  # guid -> obj (movie)
_GUID_INDEX_SHOW: Dict[str, Any] = {}   # guid -> obj (show)

def _hydrate_ids_cached(token: str, rk: str) -> Dict[str, Any]:
    m = _HYDRATE_CACHE.get(rk)
    if m is None:
        try:
            m = hydrate_external_ids(token, rk) or {}
        except Exception:
            m = {}
        _HYDRATE_CACHE[rk] = m
    return m

# ── HTTP helpers (instrumented) ───────────────────────────────────────────────

def _get_container(session, url: str, token: str, *, timeout: float, retries: int, params: Optional[Mapping[str, Any]] = None) -> Optional[Mapping[str, Any]]:
    # Uses platform's instrumented requester; respects retries there.
    try:
        r = request_with_retries(
            session, "GET", url,
            headers=plex_headers(token),
            params=(params or {}),
            timeout=timeout,
            max_retries=int(retries),
        )
        ctype = (r.headers.get("content-type") or "").lower()
        body = r.text or ""
        if r.status_code == 401:
            raise RuntimeError("Unauthorized (bad Plex token)")
        if not r.ok:
            _log(f"GET {url} -> {r.status_code}")
            return None
        if "application/json" in ctype:
            try:
                return r.json()
            except Exception:
                _log("json parse failed; trying xml")
        if "xml" in ctype or body.lstrip().startswith("<"):
            try:
                return _xml_to_container(body)
            except Exception as e:
                _log(f"xml parse failed: {e}")
        _log(f"unknown payload: ctype={ctype or 'n/a'}")
        return None
    except Exception as e:
        _log(f"GET error: {e}")
        return None

def _iter_meta_rows(container: Optional[Mapping[str, Any]]):
    if not container: return
    mc = container.get("MediaContainer") or container
    meta = mc.get("Metadata") if isinstance(mc, Mapping) else None
    if isinstance(meta, list):
        for row in meta:
            if isinstance(row, Mapping): yield row

def _iter_search_rows(container: Optional[Mapping[str, Any]]):
    # Discover /library/search(JSON): SearchResults[].SearchResult[].Metadata
    if not container: return
    mc = container.get("MediaContainer") or {}
    for sr in (mc.get("SearchResults") or []):
        for it in (sr.get("SearchResult") or []):
            md = it.get("Metadata")
            if isinstance(md, Mapping): yield md

# ── GUID helpers ──────────────────────────────────────────────────────────────

def _guid_priority(cfg: Mapping[str, Any]) -> List[str]:
    # Priority list, configurable. Default mirrors PSTS bias.
    return _cfg_list(cfg, "watchlist_guid_priority",
                     ["tmdb","imdb","tvdb","agent:themoviedb:en","agent:themoviedb","agent:imdb"])

def _sort_guid_candidates(guids: List[str], priority: List[str]) -> List[str]:
    if not guids: return []
    def score(g: str) -> Tuple[int,int]:
        s = g.lower()
        order = []
        for p in priority:
            if p == "tmdb" and s.startswith("tmdb://"): order.append(0)
            elif p == "imdb" and s.startswith("imdb://"): order.append(1)
            elif p == "tvdb" and s.startswith("tvdb://"): order.append(2)
            elif p.startswith("agent:themoviedb") and s.startswith("com.plexapp.agents.themoviedb://"):
                order.append(3 if ":en" in p and "?lang=en" in s else 4)
            elif p == "agent:imdb" and s.startswith("com.plexapp.agents.imdb://"):
                order.append(5)
        return (min(order) if order else 99, len(s))
    return sorted(guids, key=score)

def _id_pairs_from_guid(g: str) -> set:
    s = set()
    try:
        for k, v in (ids_from_guid(g) or {}).items():
            if k in ("tmdb","imdb","tvdb") and v: s.add((k, str(v)))
    except Exception:
        pass
    return s

def _expand_queries(title: Optional[str], slug: Optional[str], candidates: List[str]) -> List[str]:
    out: List[str] = []
    def add(v: Optional[str]):
        if v:
            v = str(v).strip()
            if v and v not in out: out.append(v)
    add(title); add(slug)
    for g in candidates or []:
        add(g)
        ids = ids_from_guid(g) or {}
        add(ids.get("imdb")); add(ids.get("tmdb")); add(ids.get("tvdb"))
    cleaned: List[str] = []
    for q in out:
        if "://" in q:
            try: cleaned.append(q.split("://", 1)[1]); continue
            except Exception: pass
        cleaned.append(q)
    res: List[str] = []
    for q in cleaned:
        if q and q not in res: res.append(q)
    return res[: _cfg_int(_cfg_obj, "watchlist_query_limit", 25) if (_cfg_obj := {}) else 25]  # patched later in resolver

# ── Discover resolver (query) → validate IDs (hydrate if needed) ──────────────

def _discover_resolve_rating_key(session, token: str, guid_candidates: List[str], *, title: Optional[str], slug: Optional[str],
                                 timeout: float, retries: int, query_limit: int) -> Optional[str]:
    if not guid_candidates and not title and not slug:
        return None

    # candidates + priority
    pri = _guid_priority(_cfg_obj) if (_cfg_obj := {}) else ["tmdb","imdb","tvdb","agent:themoviedb:en","agent:themoviedb","agent:imdb"]
    targets = [(_g, _id_pairs_from_guid(_g)) for _g in _sort_guid_candidates(guid_candidates or [], pri)]
    queries = _expand_queries(title, slug, guid_candidates)[:max(5, min(25, int(query_limit)))]

    def ids_match(rk: str, row: Mapping[str, Any]) -> bool:
        row_ids = ids_from_discover_row(row) if isinstance(row, Mapping) else {}
        row_pairs = {(k, str(v)) for k, v in row_ids.items() if k in ("tmdb","imdb","tvdb")}
        g = row.get("guid")
        if g: row_pairs |= _id_pairs_from_guid(str(g))
        if row_pairs:
            return any(tgt & row_pairs for _, tgt in targets if tgt)
        # hydration as last resort (expensive)
        ext = _hydrate_ids_cached(token, rk) or {}
        hyd_pairs = {(k, str(v)) for k, v in ext.items() if k in ("tmdb","imdb","tvdb")}
        return bool(hyd_pairs and any(tgt & hyd_pairs for _, tgt in targets if tgt))

    for q in queries:
        cont = _get_container(
            session, f"{DISCOVER}/library/search", token,
            timeout=timeout, retries=retries,
            params={"query": q, "limit": 25, "searchTypes": "movies,tv", "searchProviders": "discover", "includeMetadata": 1},
        )
        if not cont: continue
        for row in _iter_search_rows(cont):
            rk = str(row.get("ratingKey") or "") if isinstance(row, Mapping) else ""
            if rk and ids_match(rk, row):
                _log(f"resolve rk={rk} via DISCOVER/search query={q}")
                return rk
    return None

# ── Discover actions (PUT) with gentle pacing ─────────────────────────────────

def _discover_write_by_rk(session, token: str, rating_key: str, action: str, *, timeout: float, retries: int, delay_ms: int) -> Tuple[bool, int, str, bool]:
    """
    Returns: (ok, status, body, transient) — transient=True for 429/5xx/timeouts.
    """
    if not rating_key: return (False, 0, "no-ratingKey", False)
    path = "addToWatchlist" if action == "add" else "removeFromWatchlist"
    url = f"{DISCOVER}/actions/{path}"
    try:
        _sleep_ms(delay_ms)
        r = request_with_retries(
            session, "PUT", url,
            headers=plex_headers(token),
            params={"ratingKey": rating_key},
            timeout=timeout,
            max_retries=int(retries),
        )
        status = r.status_code
        ok = status in (200, 201, 202, 204)
        body = (r.text or "")[:240]
        transient = status in (408, 429, 500, 502, 503, 504)
        if status == 429:
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    wait = max(0.0, float(ra))
                    _log(f"429 Retry-After={wait}s")
                    time.sleep(min(wait, 5.0))
                except Exception:
                    pass
        _log(f"discover.{action} rk={rating_key} -> {status} {body!r}")
        return ok, status, body, transient
    except Exception as e:
        # Treat request exceptions as transient
        _log(f"discover.{action} error: {e}")
        return False, 0, str(e), True

# ── PMS GUID index (optional fallback) ────────────────────────────────────────

def _build_guid_index(adapter) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    # One pass per section; build guid -> object. Heavy; only when enabled.
    gi_m: Dict[str, Any] = {}
    gi_s: Dict[str, Any] = {}
    for sec in adapter.libraries(types=("movie", "show")) or []:
        try:
            for obj in (sec.all() or []):
                try:
                    gset = set(meta_guids(obj))
                    if not gset: continue
                    for g in gset:
                        (gi_m if sec.type == "movie" else gi_s)[g] = obj
                except Exception:
                    continue
        except Exception as e:
            _log(f"GUID index build error in '{getattr(sec,'title',None)}': {e}")
            continue
    _log(f"GUID index: movies={len(gi_m)}, shows={len(gi_s)}")
    return gi_m, gi_s

def _pms_find_in_index(libtype: str, guid_candidates: List[str]) -> Optional[Any]:
    src = _GUID_INDEX_SHOW if libtype == "show" else _GUID_INDEX_MOVIE
    for g in guid_candidates or []:
        if g in src:
            return src[g]
    return None

# ── public API: index ─────────────────────────────────────────────────────────

def build_index(adapter) -> Dict[str, Dict[str, Any]]:
    """Present-state: {key: minimal}. Also unfreeze items that now exist. Fixed total progress."""
    token = getattr(adapter, "cfg", None) and getattr(adapter.cfg, "token", None)
    if not token:
        raise RuntimeError("Plex token is required for watchlist index")
    session = adapter.client.session
    timeout = float(getattr(adapter.cfg, "timeout", 12.0) or 12.0)
    retries = int(getattr(adapter.cfg, "max_retries", 3) or 3)

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("watchlist") if callable(prog_mk) else None

    cont = _get_container(
        session, f"{DISCOVER}/library/sections/watchlist/all", token,
        timeout=timeout, retries=retries,
        params={"includeCollections": 1, "includeExternalMedia": 1},
    )
    mc = (cont or {}).get("MediaContainer") if isinstance(cont, Mapping) else None
    try:
        total = int((mc or {}).get("size")) if isinstance(mc, Mapping) and str((mc or {}).get("size","")).isdigit() else None
    except Exception:
        total = None

    rows = list(_iter_meta_rows(cont))  # usually small enough to list()
    if total is None:
        total = len(rows)

    if prog:
        try: prog.tick(0, total=total, force=True)
        except Exception: pass

    out: Dict[str, Dict[str, Any]] = {}
    done = 0
    for row in rows:
        m = normalize_discover_row(row, token=token)
        out[canonical_key(m)] = m
        done += 1
        if prog:
            try: prog.tick(done, total=total)
            except Exception: pass

    _unfreeze_keys_if_present(out.keys())
    _log(f"index size: {len(out)}")
    return out

# ── public API: add/remove ────────────────────────────────────────────────────

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, list]:
    """
    Add to Plex Watchlist:
      1) Skip frozen + de-dup by canonical_key.
      2) Resolve ratingKey via Discover (IDs must match); write via actions.
      3) Optional PMS fallback (GUID index) — default off.
      4) Transient errors do NOT freeze; permanent errors freeze.
    """
    token = getattr(adapter, "cfg", None) and getattr(adapter.cfg, "token", None)
    if not token:
        raise RuntimeError("Plex token is required for watchlist writes")
    session = adapter.client.session
    acct = adapter.account()
    cfg = _cfg(adapter)
    allow_pms = _cfg_bool(cfg, "watchlist_allow_pms_fallback", False)  # default off
    timeout = float(getattr(adapter.cfg, "timeout", 12.0) or 12.0)
    retries = int(getattr(adapter.cfg, "max_retries", 3) or 3)
    qlimit = _cfg_int(cfg, "watchlist_query_limit", 25)
    delay_ms = _cfg_int(cfg, "watchlist_write_delay_ms", 0)

    # Build GUID index once if fallback enabled
    if allow_pms and not (_GUID_INDEX_MOVIE or _GUID_INDEX_SHOW):
        gm, gs = _build_guid_index(adapter)
        _GUID_INDEX_MOVIE.update(gm); _GUID_INDEX_SHOW.update(gs)

    ok = 0
    unresolved = []

    seen = set()  # de-dup inputs by canonical key
    for it in items:
        ck = canonical_key(it)
        if ck in seen:
            continue
        seen.add(ck)

        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        guids = _sort_guid_candidates(candidate_guids_from_ids(it), _guid_priority(cfg))
        kind = (it.get("type") or "movie").lower()
        libtype = "show" if kind in ("show", "series", "tv") else "movie"
        title = it.get("title")
        slug = (it.get("ids") or {}).get("slug") if isinstance(it.get("ids"), dict) else None

        if not (guids or title or slug):
            unresolved.append({"item": id_minimal(it), "hint": "no_external_ids"})
            _freeze_item(it, action="add", reasons=["no-external-ids"], guids_tried=guids)
            continue

        rk = _discover_resolve_rating_key(session, token, guids, title=title, slug=slug,
                                          timeout=timeout, retries=retries, query_limit=qlimit)
        if rk:
            ok_flag, status, body, transient = _discover_write_by_rk(session, token, rk, action="add",
                                                                     timeout=timeout, retries=retries, delay_ms=delay_ms)
            if ok_flag:
                ok += 1
                if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                continue
            if transient:
                unresolved.append({"item": id_minimal(it), "hint": f"discover_transient_{status}"})
                continue  # do not freeze
            _log(f"discover.add failed rk={rk} status={status} body={body!r}")

        if allow_pms:
            chosen = _pms_find_in_index(libtype, guids)
            if chosen:
                try:
                    chosen.addToWatchlist(account=acct)
                    ok += 1
                    if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    _log(f"PMS add failed: {e}")
                    unresolved.append({"item": id_minimal(it), "hint": "pms_transient"})
                    continue  # do not freeze on PMS transient

        unresolved.append({"item": id_minimal(it), "hint": "discover+library failed"})
        _freeze_item(it, action="add",
                     reasons=["discover:resolve-or-write-failed" if rk else "discover:resolve-empty",
                              *(["library:guid-index-miss"] if allow_pms else [])],
                     guids_tried=guids)

    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, list]:
    """
    Remove from Plex Watchlist:
      1) Skip frozen + de-dup by canonical_key.
      2) Resolve ratingKey via Discover (IDs must match); write via actions.
      3) Optional PMS fallback (GUID index) — default off.
      4) Transient errors do NOT freeze; permanent errors freeze.
    """
    token = getattr(adapter, "cfg", None) and getattr(adapter.cfg, "token", None)
    if not token:
        raise RuntimeError("Plex token is required for watchlist writes")
    session = adapter.client.session
    acct = adapter.account()
    cfg = _cfg(adapter)
    allow_pms = _cfg_bool(cfg, "watchlist_allow_pms_fallback", False)  # default off
    timeout = float(getattr(adapter.cfg, "timeout", 12.0) or 12.0)
    retries = int(getattr(adapter.cfg, "max_retries", 3) or 3)
    qlimit = _cfg_int(cfg, "watchlist_query_limit", 25)
    delay_ms = _cfg_int(cfg, "watchlist_write_delay_ms", 0)

    if allow_pms and not (_GUID_INDEX_MOVIE or _GUID_INDEX_SHOW):
        gm, gs = _build_guid_index(adapter)
        _GUID_INDEX_MOVIE.update(gm); _GUID_INDEX_SHOW.update(gs)

    ok = 0
    unresolved = []

    seen = set()
    for it in items:
        ck = canonical_key(it)
        if ck in seen:
            continue
        seen.add(ck)

        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        guids = _sort_guid_candidates(candidate_guids_from_ids(it), _guid_priority(cfg))
        kind = (it.get("type") or "movie").lower()
        libtype = "show" if kind in ("show", "series", "tv") else "movie"
        title = it.get("title")
        slug = (it.get("ids") or {}).get("slug") if isinstance(it.get("ids"), dict) else None

        if not (guids or title or slug):
            unresolved.append({"item": id_minimal(it), "hint": "no_external_ids"})
            _freeze_item(it, action="remove", reasons=["no-external-ids"], guids_tried=guids)
            continue

        rk = _discover_resolve_rating_key(session, token, guids, title=title, slug=slug,
                                          timeout=timeout, retries=retries, query_limit=qlimit)
        if rk:
            ok_flag, status, body, transient = _discover_write_by_rk(session, token, rk, action="remove",
                                                                     timeout=timeout, retries=retries, delay_ms=delay_ms)
            if ok_flag:
                ok += 1
                if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                continue
            if transient:
                unresolved.append({"item": id_minimal(it), "hint": f"discover_transient_{status}"})
                continue  # do not freeze
            _log(f"discover.remove failed rk={rk} status={status} body={body!r}")

        if allow_pms:
            chosen = _pms_find_in_index(libtype, guids)
            if chosen:
                try:
                    chosen.removeFromWatchlist(account=acct)
                    ok += 1
                    if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    _log(f"PMS remove failed: {e}")
                    unresolved.append({"item": id_minimal(it), "hint": "pms_transient"})
                    continue  # do not freeze on PMS transient

        unresolved.append({"item": id_minimal(it), "hint": "discover+library failed"})
        _freeze_item(it, action="remove",
                     reasons=["discover:resolve-or-write-failed" if rk else "discover:resolve-empty",
                              *(["library:guid-index-miss"] if allow_pms else [])],
                     guids_tried=guids)

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved
