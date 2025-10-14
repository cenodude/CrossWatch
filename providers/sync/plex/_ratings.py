# /providers/sync/plex/_ratings.py
from __future__ import annotations
import os, json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Set
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from ._common import (
    normalize as plex_normalize,
    candidate_guids_from_ids, section_find_by_guid, meta_guids, minimal_from_history_row,
)
try:
    from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from
except Exception:
    from _id_map import canonical_key, minimal as id_minimal, ids_from  # type: ignore

UNRESOLVED_PATH = "/config/.cw_state/plex_ratings.unresolved.json"

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:ratings] {msg}")
        
def _emit(evt: dict) -> None:
    try:
        feature = str(evt.get("feature") or "?")
        head = []
        if "event"  in evt: head.append(f"event={evt['event']}")
        if "action" in evt: head.append(f"action={evt['action']}")
        tail = [f"{k}={v}" for k, v in evt.items() if k not in {"feature", "event", "action"}]
        line = " ".join(head + tail)
        print(f"[PLEX:{feature}] {line}", flush=True)
    except Exception:
        pass

# ── config / workers ─────────────────────────────────────────────────────────

def _get_rating_workers(adapter) -> int:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        n = int(plex.get("rating_workers", 0) or 0)
    except Exception:
        n = 0
    if n <= 0:
        try: n = int(os.environ.get("CW_PLEX_RATINGS_WORKERS", "12"))
        except Exception: n = 12
    return max(1, min(n, 64))

def _allowed_ratings_sec_ids(adapter) -> Set[str]:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        arr = ((plex.get("ratings") or {}).get("libraries") or [])
        return {str(int(x)) for x in arr if str(x).strip()}
    except Exception:
        return set()

def _plex_cfg(adapter) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    return cfg.get("plex", {}) if isinstance(cfg, dict) else {}

def _plex_cfg_get(adapter, key: str, default: Any = None) -> Any:
    c = _plex_cfg(adapter)
    v = c.get(key, default) if isinstance(c, dict) else default
    return default if v is None else v

# ── tiny helpers ─────────────────────────────────────────────────────────────

def _as_epoch(v: Any) -> Optional[int]:
    if v is None: return None
    if isinstance(v, (int, float)): return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try:
                n = int(s); return n // 1000 if len(s) >= 13 else n
            except Exception:
                return None
        try:
            return int(datetime.fromisoformat(s.replace("Z","+00:00")).timestamp())
        except Exception:
            return None
    if isinstance(v, datetime):
        if v.tzinfo is None: v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp())
    return None

def _iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00","Z")

def _norm_rating(v: Any) -> Optional[int]:
    if v is None: return None
    try: f = float(v)
    except Exception: return None
    if 0 <= f <= 5: f = round(f * 2)
    i = int(round(f))
    if 1 <= i <= 10: return i
    if i == 0: return 0
    return None

def _as_int(v):
    try: return int(v)
    except Exception: return None

def _has_ext_ids(m: Mapping[str, Any]) -> bool:
    ids = (m.get("ids") if isinstance(m, Mapping) else None) or {}
    return bool(ids.get("imdb") or ids.get("tmdb") or ids.get("tvdb"))

# ── unresolved store ─────────────────────────────────────────────────────────

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

def _event_key(it: Mapping[str, Any]) -> str:
    return canonical_key(id_minimal(it))

def _freeze_item(it: Mapping[str, Any], *, action: str, reasons: List[str]) -> None:
    key = _event_key(it)
    data = _load_unresolved()
    now = _iso(int(datetime.now(timezone.utc).timestamp()))
    entry = data.get(key) or {"feature": "ratings", "action": action, "first_seen": now, "attempts": 0}
    entry.update({"item": id_minimal(it), "last_attempt": now})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    entry["reasons"] = sorted(rset); entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry; _save_unresolved(data)

def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved(); changed = False
    for k in list(keys or []):
        if k in data: del data[k]; changed = True
    if changed: _save_unresolved(data)

def _is_frozen(it: Mapping[str, Any]) -> bool:
    return _event_key(it) in _load_unresolved()

# ── ratingKey resolution (GUID-first, then title fallback) ────────────────────

def _resolve_rating_key(adapter, it: Mapping[str, Any]) -> Optional[str]:
    ids = ids_from(it); rk = ids.get("plex")
    if rk: return str(rk)

    srv = getattr(adapter.client, "server", None)
    if not srv: return None

    kind = (it.get("type") or "movie").lower()
    is_episode = kind == "episode"; is_season = kind == "season"
    query_title = (it.get("series_title") if (is_episode or is_season) else it.get("title")) or ""
    query_title = str(query_title).strip()

    allow = _allowed_ratings_sec_ids(adapter)

    guid_candidates = candidate_guids_from_ids(it)
    sec_types = ("show",) if (is_episode or is_season) else ("movie",)
    guid_set = set(guid_candidates or [])
    for sec in adapter.libraries(types=sec_types) or []:
        sid = str(getattr(sec, "key", "")).strip()
        if allow and sid not in allow:
            continue
        obj = section_find_by_guid(sec, guid_candidates)
        if obj:
            rrk = getattr(obj, "ratingKey", None)
            return str(rrk) if rrk else None
        try:
            for cand in (sec.all() or []):
                if set(meta_guids(cand)) & guid_set:
                    rrk = getattr(cand, "ratingKey", None)
                    if rrk: return str(rrk)
        except Exception:
            continue

    if not query_title: return None
    year = it.get("year"); season = it.get("season") or it.get("season_number")
    epno = it.get("episode") or it.get("episode_number")

    hits: List[Any] = []
    for sec in adapter.libraries(types=sec_types) or []:
        sid = str(getattr(sec, "key", "")).strip()
        if allow and sid not in allow:
            continue
        try: hits.extend(sec.search(title=query_title) or [])
        except Exception: continue

    def _score(obj) -> int:
        sc = 0
        try:
            ot = (getattr(obj, "grandparentTitle", None) if (is_episode or is_season) else getattr(obj, "title", None)) or ""
            if ot.strip().lower() == query_title.lower(): sc += 3
            if not (is_episode or is_season) and year is not None and getattr(obj, "year", None) == year: sc += 2
            if is_episode:
                s_ok = (season is None) or (getattr(obj, "seasonNumber", None) == season or getattr(obj, "parentIndex", None) == season)
                e_ok = (epno   is None) or (getattr(obj, "index", None) == epno)
                if s_ok and e_ok: sc += 2
            mids = (plex_normalize(obj).get("ids") or {})
            for k in ("imdb","tmdb","tvdb"):
                if k in mids and k in ids and mids[k] == ids[k]: sc += 4
        except Exception:
            pass
        return sc

    best, best_score = None, -1
    for h in hits:
        sc = _score(h)
        if sc > best_score: best, best_score = h, sc
    rrk = getattr(best, "ratingKey", None) if best else None
    return str(rrk) if rrk else None

def _rate(srv, rating_key: Any, rating_1to10: int) -> bool:
    try:
        url = srv.url("/:/rate")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library", "rating": int(rating_1to10)}
        r = srv._session.get(url, params=params, timeout=10)
        return r.ok
    except Exception:
        return False

# ── per-item fetch (plexapi) ─────────────────────────────────────────────────

_ITEM_PARAMS = {"includePreferences": 1}

def _fetch_one_rating(srv, rk: str) -> Optional[Dict[str, Any]]:
    try:
        it = srv.fetchItem(int(rk))
    except Exception:
        return None

    r = _norm_rating(getattr(it, "userRating", None))
    if not r or r <= 0: return None

    m: Dict[str, Any] = plex_normalize(it) or {}
    if not m: return None

    m["rating"] = r
    ts = _as_epoch(getattr(it, "lastRatedAt", None))
    if ts: m["rated_at"] = _iso(ts)

    t = (getattr(it, "type", None) or m.get("type") or "movie").lower()
    if t in ("movie", "show", "season", "episode"): m["type"] = t
    else: m["type"] = "movie"
    try:
        if m["type"] == "season":
            m["series_title"] = getattr(it, "parentTitle", None) or getattr(it, "grandparentTitle", None)
            m["season"] = getattr(it, "index", None)
        elif m["type"] == "episode":
            m["series_title"] = getattr(it, "grandparentTitle", None)
            m["season"] = getattr(it, "parentIndex", None)
            m["episode"] = getattr(it, "index", None)
    except Exception:
        pass

    return m

# ── index (present-state, threaded per-item) ─────────────────────────────────

def build_index(adapter, limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    srv = getattr(adapter.client, "server", None)
    if not srv: raise RuntimeError("PLEX server not bound")

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("ratings") if callable(prog_mk) else None
    
    plex_cfg = _plex_cfg(adapter)
    if plex_cfg.get("fallback_GUID") or plex_cfg.get("fallback_guid"):
        _emit({"event":"debug","msg":"fallback_guid.enabled","provider":"PLEX","feature":"ratings"})

    out: Dict[str, Dict[str, Any]] = {}
    added = 0; scanned = 0

    allow = _allowed_ratings_sec_ids(adapter)
    keys: List[Tuple[str, str]] = []
    for sec in adapter.libraries(types=("movie", "show")) or []:
        sid = str(getattr(sec, "key", "")).strip()
        if allow and sid not in allow:
            continue
        if (sec.type or "").lower() == "movie":
            try:
                for mv in (sec.all() or []):
                    rk = getattr(mv, "ratingKey", None)
                    if rk: keys.append(("movie", str(rk)))
            except Exception:
                pass
        else:
            try:
                for sh in (sec.all() or []):
                    rk = getattr(sh, "ratingKey", None)
                    if rk: keys.append(("show", str(rk)))
                    try:
                        for sn in (sh.seasons() or []):
                            rk = getattr(sn, "ratingKey", None)
                            if rk: keys.append(("season", str(rk)))
                    except Exception:
                        pass
                    try:
                        for ep in (sh.episodes() or []):
                            rk = getattr(ep, "ratingKey", None)
                            if rk: keys.append(("episode", str(rk)))
                    except Exception:
                        pass
            except Exception:
                pass

    grand_total = len(keys)
    if prog:
        try: prog.tick(0, total=grand_total, force=True)
        except Exception: pass

    workers = _get_rating_workers(adapter)
    fallback_guid = bool(_plex_cfg_get(adapter, "fallback_GUID", False) or _plex_cfg_get(adapter, "fallback_guid", False))
    
    fb_try = 0
    fb_ok  = 0

    def _tick():
        if prog:
            try: prog.tick(scanned, total=grand_total)
            except Exception: pass

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_one_rating, srv, rk): (typ, rk) for typ, rk in keys}
        for fut in as_completed(futs):
            typ, rk = futs[fut]
            scanned += 1
            m = None
            try: m = fut.result()
            except Exception: m = None
            if m:
                if fallback_guid and not _has_ext_ids(m):
                    fb_try += 1
                    _emit({"event":"fallback_guid","provider":"PLEX","feature":"ratings","action":"enrich_try","rk":rk})
                    fb = minimal_from_history_row(m, allow_discover=True)
                    ok = isinstance(fb, Mapping) and ((fb.get("ids") or fb.get("show_ids")))
                    _emit({"event":"fallback_guid","provider":"PLEX","feature":"ratings","action":("enrich_ok" if ok else "enrich_miss"),"rk":rk})
                    if ok:
                        fb_ok += 1
                        ids0 = (m.get("ids") or {})
                        ids1 = (fb.get("ids") or {})
                        if ids1:
                            ids0.update({k: v for k, v in ids1.items() if v})
                            m["ids"] = ids0
                        if "show_ids" in fb:
                            si0 = (m.get("show_ids") or {})
                            si1 = (fb.get("show_ids") or {})
                            if si1:
                                si0.update({k: v for k, v in si1.items() if v})
                                m["show_ids"] = si0

                if typ in ("movie", "show", "season", "episode"): m["type"] = typ
                out[canonical_key(m)] = m; added += 1
            _tick()
            if limit and added >= limit:
                _log(f"index truncated at {limit}")
                if prog:
                    try: prog.done(ok=True, total=grand_total)
                    except Exception: pass
                return out

    if prog:
        try: prog.done(ok=True, total=grand_total)
        except Exception: pass

    _log(f"index size: {len(out)} (added={added}, scanned={scanned}, total={grand_total}, workers={workers})")
    return out

# ── add/remove (guarded) ─────────────────────────────────────────────────────

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        unresolved = []
        for it in items:
            _freeze_item(it, action="add", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(it), "hint": "no_plex_server"})
        _log("add skipped: no PMS bound")
        return 0, unresolved

    ok = 0; unresolved: List[Dict[str, Any]] = []
    for it in items:
        if _is_frozen(it): _log(f"skip frozen: {id_minimal(it).get('title')}"); continue
        rating = _norm_rating(it.get("rating"))
        if rating is None or rating <= 0:
            _freeze_item(it, action="add", reasons=["missing_or_invalid_rating"])
            unresolved.append({"item": id_minimal(it), "hint": "missing_or_invalid_rating"}); continue
        rk = _resolve_rating_key(adapter, it)
        if not rk:
            _freeze_item(it, action="add", reasons=["not_in_library"])
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"}); continue
        if _rate(srv, rk, rating):
            ok += 1; _unfreeze_keys_if_present([_event_key(it)])
        else:
            _freeze_item(it, action="add", reasons=["rate_failed"])
            unresolved.append({"item": id_minimal(it), "hint": "rate_failed"})
    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        unresolved = []
        for it in items:
            _freeze_item(it, action="remove", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(it), "hint": "no_plex_server"})
        _log("remove skipped: no PMS bound")
        return 0, unresolved

    ok = 0; unresolved: List[Dict[str, Any]] = []
    for it in items:
        if _is_frozen(it): _log(f"skip frozen: {id_minimal(it).get('title')}"); continue
        rk = _resolve_rating_key(adapter, it)
        if not rk:
            _freeze_item(it, action="remove", reasons=["not_in_library"])
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"}); continue
        if _rate(srv, rk, 0):
            ok += 1; _unfreeze_keys_if_present([_event_key(it)])
        else:
            _freeze_item(it, action="remove", reasons=["clear_failed"])
            unresolved.append({"item": id_minimal(it), "hint": "clear_failed"})
    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved
