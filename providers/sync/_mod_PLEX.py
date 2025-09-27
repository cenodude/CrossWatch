from __future__ import annotations
# providers/sync/_mod_PLEX.py
# Plex provider (watchlist, ratings, history, playlists) via PlexAPI only.

__VERSION__ = "2.2.1"
__all__ = ["OPS", "PLEXModule", "get_manifest"]

import os, re, json, time
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple, cast
import datetime as _dt

try:
    from cw_platform.metadata import MetadataManager
except Exception:  # flat runs / tests
    from metadata import MetadataManager  # type: ignore

# --- optional host hooks ------------------------------------------------------
try:
    from _logging import log as host_log
except Exception:
    def host_log(*a, **k): pass  # type: ignore
try:
    from _statistics import Stats  # type: ignore
    _stats = Stats()
except Exception:
    _stats = None

# --- PlexAPI (hard requirement) ----------------------------------------------
try:
    import plexapi  # type: ignore
    from plexapi.myplex import MyPlexAccount  # type: ignore
    HAS_PLEXAPI = True
except Exception:  # pragma: no cover
    HAS_PLEXAPI = False
    MyPlexAccount = object  # type: ignore

PROGRESS_EVERY = 50
THROTTLE_EVERY = 200

# --- small state helpers ------------------------------------------------------
def _base_config_dir() -> Path:
    env = os.environ.get("CW_STATE_DIR")
    if env: return Path(env).resolve()
    try:
        from cw_platform.config_base import CONFIG as CONFIG_DIR  # type: ignore
        return Path(CONFIG_DIR)
    except Exception:
        return Path(".").resolve()

def _state_root() -> Path:
    p = _base_config_dir() / ".cw_state"
    try: p.mkdir(parents=True, exist_ok=True)
    except Exception: pass
    return p

def _read_json(path: Path, fallback: Any) -> Any:
    try: return json.loads(path.read_text("utf-8"))
    except Exception: return fallback

def _write_json(path: Path, data: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, path)
    except Exception:
        pass

# snapshots (ratings/history) kept normalized on read/write
def _ratings_shadow_path() -> Path:  return _state_root() / "plex_ratings.shadow.json"
def _history_shadow_path() -> Path:  return _state_root() / "plex_history.shadow.json"
def _ratings_shadow_load() -> Dict[str, Any]: return _read_json(_ratings_shadow_path(), {"items": {}, "ts": 0})
def _ratings_shadow_save(items: Mapping[str, Any]) -> None: _write_json(_ratings_shadow_path(), {"items": dict(items), "ts": int(time.time())})
def _history_shadow_load() -> Dict[str, Any]: return _read_json(_history_shadow_path(), {"items": {}, "ts": 0})
def _history_shadow_save(items: Mapping[str, Any]) -> None: _write_json(_history_shadow_path(), {"items": dict(items), "ts": int(time.time())})

# cursors (for fingerprints)
_CUR_FILE = "plex_cursors.json"
def _cursors_path() -> Path: return _state_root() / _CUR_FILE
def _cursor_key(scope: str) -> str: return f"plex__{scope}.json"
def _cursor_save(scope: str, data: Mapping[str, Any]) -> None:
    entry = dict(data); entry.setdefault("updated_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    cur = _read_json(_cursors_path(), {})
    if not isinstance(cur, dict): cur = {}
    cur[_cursor_key(scope)] = entry
    _write_json(_cursors_path(), cur)
def _cursor_load(scope: str) -> Dict[str, Any]:
    cur = _read_json(_cursors_path(), {})
    return (cur or {}).get(_cursor_key(scope)) or {}

# unresolved/backoff store (per spec)
def _unresolved_path() -> Path: return _state_root() / "plex_unresolved.shadow.json"
def _unresolved_defaults() -> Dict[str, Any]:
    return {"policy": "backoff", "base_hours": 6, "max_days": 30, "max_retries": 8, "ttl_days": 90}
def _unresolved_load() -> Dict[str, Any]:
    obj = _read_json(_unresolved_path(), {"items": {}, "ts": 0})
    if not isinstance(obj, dict): obj = {"items": {}, "ts": 0}
    obj.setdefault("items", {}); obj.setdefault("ts", 0)
    return obj
def _unresolved_save(items: Mapping[str, Any]) -> None:
    _write_json(_unresolved_path(), {"items": dict(items), "ts": int(time.time())})

# --- canonical & normalization -------------------------------------------------
_ID_ORDER = ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid")

def _norm_type(t: Any) -> str:
    x = (str(t or "")).strip().lower()
    if x in ("movies", "movie"): return "movie"
    if x in ("shows", "show", "series"): return "show"
    if x in ("seasons", "season"): return "season"
    if x in ("episodes", "episode"): return "episode"
    return x or "movie"

def _to_utc(dt_obj: Any) -> Optional[str]:
    """Return ISO8601 UTC '...Z'. Accepts datetime or epoch."""
    try:
        if isinstance(dt_obj, _dt.datetime):
            # If tz-aware, trust it; if naive, assume local time.
            if dt_obj.tzinfo and dt_obj.tzinfo.utcoffset(dt_obj) is not None:
                ts = dt_obj.timestamp()
            else:
                ts = time.mktime(dt_obj.timetuple())
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
        if isinstance(dt_obj, (int, float)):
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(dt_obj)))
    except Exception:
        pass
    return None

def _clamp_ts(ts: Any) -> Any:
    if not isinstance(ts, str) or not ts: return ts
    s = ts.strip()
    for sep in ("+", "-"):
        if sep in s[19:]: s = s[:s.find(sep)]; break
    if s.endswith("Z"): s = s[:-1]
    s = s.split(".", 1)[0]
    if len(s) >= 19: s = s[:19]
    return s + "Z"

def _norm_ids(ids: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(ids, Mapping):
        for k in ("imdb", "tmdb", "tvdb", "trakt"):
            v = ids.get(k)
            if v is not None and str(v).strip():
                out[k] = v if k not in ("tmdb","tvdb") else int(v)
    return out

def _norm_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    r = dict(row or {})
    r["type"] = _norm_type(r.get("type"))
    r["ids"]  = _norm_ids(r.get("ids") or {})
    if "watched_at" in r: r["watched_at"] = _clamp_ts(r.get("watched_at"))
    if "rated_at"   in r: r["rated_at"]   = _clamp_ts(r.get("rated_at"))
    return r

def _normalize_shadow_items(items: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for _, v in (items or {}).items():
        nr = _norm_row(v)
        out[canonical_key(nr)] = nr
    return out

def _singular(t: str) -> str:
    x = (t or "").lower()
    if x.startswith("movie"): return "movie"
    if x.startswith("show"): return "show"
    if x.startswith("season"): return "season"
    if x.startswith("episode"): return "episode"
    return "movie"

def canonical_key(item: Mapping[str, Any]) -> str:
    ids = dict((item.get("ids") or {}))
    for k in _ID_ORDER:
        v = ids.get(k)
        if v is not None and str(v) != "":
            return f"{k}:{v}".lower()
    title = (item.get("title") or "").strip().lower()
    year  = item.get("year") or ""
    typ   = _singular(item.get("type") or "")
    return f"{typ}|title:{title}|year:{year}"

def minimal(item: Mapping[str, Any]) -> Dict[str, Any]:
    return {"ids": {k: (item.get("ids") or {}).get(k) for k in _ID_ORDER if (item.get("ids") or {}).get(k)},
            "title": item.get("title"), "year": item.get("year"), "type": _singular(item.get("type") or "") or None}

# --- config helpers ------------------------------------------------------------
def _cfg_unresolved(cfg_root: Mapping[str, Any]) -> Dict[str, Any]:
    pr = dict(cfg_root.get("plex") or {})
    un = dict(pr.get("unresolved") or {})
    d = _unresolved_defaults()
    return {
        "policy": (un.get("policy") or d["policy"]).lower(),
        "base_hours": int(un.get("base_hours") or d["base_hours"]),
        "max_days": int(un.get("max_days") or d["max_days"]),
        "max_retries": int(un.get("max_retries") or d["max_retries"]),
        "ttl_days": int(un.get("ttl_days") or d["ttl_days"]),
    }

def _cfg_token(cfg_root: Mapping[str, Any]) -> str:
    plex_cfg = dict(cfg_root.get("plex") or {})
    token = (plex_cfg.get("account_token") or "").strip()
    if not token: raise ValueError("plex.account_token is required")
    return token

# --- PlexAPI env ---------------------------------------------------------------
@dataclass
class PlexEnv:
    account: MyPlexAccount
    servers: List[Any]  # plexapi.server.PlexServer

def _ensure_account(cfg_root: Mapping[str, Any]) -> MyPlexAccount:
    if not HAS_PLEXAPI: raise RuntimeError("plexapi is required")
    return MyPlexAccount(token=_cfg_token(cfg_root))  # type: ignore

def _section_filters(plex_cfg: Mapping[str, Any]) -> Tuple[set, set]:
    sec_cfg = dict(plex_cfg.get("servers", {}).get("sections") or {})
    def _norm(v: Any) -> str: return str(v).strip().lower()
    include = {_norm(v) for v in (sec_cfg.get("include") or []) if str(v).strip()}
    exclude = {_norm(v) for v in (sec_cfg.get("exclude") or []) if str(v).strip()}
    return include, exclude

def _connect_servers(acct: MyPlexAccount, plex_cfg: Mapping[str, Any]) -> List[Any]:
    wanted_ids: List[str] = list(plex_cfg.get("servers", {}).get("machine_ids") or [])
    servers: List[Any] = []
    for res in acct.resources():
        if "server" not in (res.provides or ""): continue
        if wanted_ids and res.clientIdentifier not in wanted_ids: continue
        try: servers.append(res.connect(timeout=6))
        except Exception: continue
    if not servers:
        for res in acct.resources():
            if "server" in (res.provides or ""):
                try: servers.append(res.connect(timeout=6)); break
                except Exception: continue
    return servers

def _env_from_config(cfg_root: Mapping[str, Any]) -> PlexEnv:
    plex_cfg = dict(cfg_root.get("plex") or {})
    acct = _ensure_account(cfg_root)
    servers = _connect_servers(acct, plex_cfg)
    if not servers: raise RuntimeError("No reachable Plex server via PlexAPI")
    return PlexEnv(account=acct, servers=servers)

# --- id extraction / resolution -----------------------------------------------
_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|tvdb)://(\d+)", re.I)

# in _ids_from_plexobj(obj) — keep current extraction, but also return 'guid'
def _ids_from_plexobj(obj: Any) -> Dict[str, Any]:
    ids: Dict[str, Any] = {}
    try:
        for g in (getattr(obj, "guids", []) or []):
            gid = getattr(g, "id", None)
            if not isinstance(gid, str):
                continue
            # existing imdb/tmdb/tvdb parsing...
            if "imdb" in gid and "imdb" not in ids:
                m = _PAT_IMDB.search(gid);  ids["imdb"] = m.group(1) if m else ids.get("imdb")
            if "tmdb" in gid and "tmdb" not in ids:
                m = _PAT_TMDB.search(gid);  ids["tmdb"] = int(m.group(1)) if m else ids.get("tmdb")
            if "thetvdb" in gid and "tvdb" not in ids:
                m = _PAT_TVDB.search(gid);  ids["tvdb"] = int(m.group(1)) if m else ids.get("tvdb")
        # keep the raw canonical guid too
        if getattr(obj, "guid", None) and "guid" not in ids:
            ids["guid"] = str(getattr(obj, "guid"))
    except Exception:
        pass

    # also try single guid (legacy)
    try:
        gsingle = getattr(obj, "guid", None)
        if isinstance(gsingle, str):
            if "imdb" in gsingle and "imdb" not in ids:
                m = _PAT_IMDB.search(gsingle);  ids["imdb"] = m.group(1) if m else ids.get("imdb")
            if "tmdb" in gsingle and "tmdb" not in ids:
                m = _PAT_TMDB.search(gsingle);  ids["tmdb"] = int(m.group(1)) if m else ids.get("tmdb")
            if "thetvdb" in gsingle and "tvdb" not in ids:
                m = _PAT_TVDB.search(gsingle);  ids["tvdb"] = int(m.group(1)) if m else ids.get("tvdb")
            ids.setdefault("guid", gsingle)
    except Exception:
        pass

    return {k: v for k, v in ids.items() if v is not None}


def _libtype_from_item(item: Mapping[str, Any]) -> str:
    t = (item.get("type") or "").lower()
    if t in ("movie","movies"): return "movie"
    if t in ("show","shows"): return "show"
    if t in ("season","seasons"): return "season"
    if t in ("episode","episodes"): return "episode"
    return "movie"

def _server_find_item(server: Any, q: Mapping[str, Any], libtype: str) -> Optional[Any]:
    ids = (q.get("ids") or q) or {}
    for key in ("imdb", "tmdb", "tvdb"):
        val = ids.get(key)
        if not val: continue
        variants = [f"{key}://{val}"]
        if key == "imdb": variants.append(f"com.plexapp.agents.imdb://{val}")
        elif key == "tmdb": variants.append(f"com.plexapp.agents.tmdb://{val}")
        elif key == "tvdb": variants.append(f"com.plexapp.agents.thetvdb://{val}")
        for g in variants:
            try:
                hits = server.search(guid=g, libtype=libtype) or []
                if hits: return hits[0]
            except Exception: pass
    title = q.get("title"); year = q.get("year")
    try:
        hits = server.search(title=title, year=year, libtype=libtype) or []
        if hits: return hits[0]
    except Exception: pass
    return None

def _resolve_on_servers(env: PlexEnv, q: Mapping[str, Any], mtype: str) -> Optional[Any]:
    for s in env.servers:
        it = _server_find_item(s, q, mtype)
        if it: return it
    return None

# --- unresolved/backoff core ---------------------------------------------------
def _now_iso() -> str: return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _compute_next_due(tries: int, base_hours: int, max_days: int) -> str:
    delay_h = base_hours * (2 ** max(0, tries - 1))
    delay_h = min(delay_h, max_days * 24)
    due = int(time.time() + delay_h * 3600)
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(due))

def _unresolved_key_for(item: Mapping[str, Any]) -> str:
    return canonical_key(_norm_row(item))

def _unresolved_upsert(provider: str, feature: str, item: Mapping[str, Any], *,
                       reason: str, last_error: Optional[str],
                       cfg_root: Mapping[str, Any],
                       hint: Optional[Mapping[str, Any]] = None) -> None:
    # accepts and persists a tiny normalized hint so virtual rows are stable
    pol = _cfg_unresolved(cfg_root)
    sh = _unresolved_load()
    m = dict(sh.get("items") or {})
    key = _unresolved_key_for(item)
    node = dict(m.get(key) or {})
    tries = min(int(node.get("tries") or 0) + 1, int(pol["max_retries"]))
    first_seen = node.get("first_seen_at") or _now_iso()
    next_due = _compute_next_due(tries, pol["base_hours"], pol["max_days"])

    ent = {
        "feature": feature,
        "reason": reason,
        "tries": tries,
        "type": _singular(item.get("type") or "movie"),
        "first_seen_at": first_seen,
        "last_tried_at": _now_iso(),
        "next_due_at": next_due,
    }
    if last_error:
        ent["last_error"] = str(last_error)[:500]

    if hint and isinstance(hint, Mapping):
        safe = {k: hint.get(k) for k in ("type", "title", "year", "ids", "rating", "watched_at") if hint.get(k) is not None}
        if safe:
            ent["hint"] = _norm_row(safe)

    m[key] = ent
    _unresolved_save(m)
    try: host_log(f"{provider}.unresolved.upsert", {"feature": feature, "reason": reason, "tries": tries})
    except Exception: pass

def _unresolved_clear(item: Mapping[str, Any]) -> None:
    sh = _unresolved_load()
    m = dict(sh.get("items") or {})
    key = _unresolved_key_for(item)
    if key in m:
        m.pop(key, None)
        _unresolved_save(m)

def _unresolved_prune_ttl(ttl_days: int) -> None:
    sh = _unresolved_load()
    m = dict(sh.get("items") or {})
    if not m: return
    now = int(time.time()); ttl = max(1, ttl_days) * 86400
    changed = False
    for k, v in list(m.items()):
        t0 = v.get("first_seen_at")
        try:
            if t0 and isinstance(t0, str):
                ts = time.mktime(time.strptime(t0.replace("Z","+0000"), "%Y-%m-%dT%H:%M:%S%z"))
                if (now - int(ts)) > ttl:
                    m.pop(k, None); changed = True
        except Exception:
            pass
    if changed: _unresolved_save(m)

def _unresolved_virtual_for_index(feature: str, policy: str) -> Dict[str, Dict[str, Any]]:
    sh = _unresolved_load()
    now = _now_iso()
    out: Dict[str, Dict[str, Any]] = {}
    for k, v in (sh.get("items") or {}).items():
        if v.get("feature") != feature:
            continue
        due = str(v.get("next_due_at") or "")
        include = (policy == "virtual") or (policy == "backoff" and due > now)
        if not include:
            continue
        # Prefer stored hint; fall back to type-only row
        row: Dict[str, Any] = {"type": (v.get("type") or "movie"), "ids": {}}
        h = v.get("hint")
        if isinstance(h, Mapping):
            for fld in ("title", "year", "ids", "rating", "watched_at"):
                if h.get(fld) is not None:
                    row[fld] = h.get(fld)
        out[k] = _norm_row(row)
    return out

def _unresolved_reset_if_library_grew(feature: str, live_count: int) -> None:
    prev = _cursor_load(f"{feature}.fingerprint") or {}
    prev_cnt = int(prev.get("count") or 0)
    _cursor_save(f"{feature}.fingerprint", {"count": int(live_count)})
    if live_count <= prev_cnt:
        return
    sh = _unresolved_load()
    m = dict(sh.get("items") or {})
    changed = False
    for k, v in list(m.items()):
        if v.get("feature") != feature:
            continue
        v["next_due_at"] = _now_iso()
        v["tries"] = max(0, int(v.get("tries") or 1) - 1)
        changed = True
    if changed:
        _unresolved_save(m)


# --- watchlist (PlexAPI account only) -----------------------------------------
def _watchlist_index(acct: MyPlexAccount) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for kind in ("movie", "show"):
        try: items = acct.watchlist(libtype=kind) or []
        except Exception: items = []
        for it in items:
            ids = _ids_from_plexobj(it)
            if not any(ids.get(k) for k in ("imdb","tmdb","tvdb")): continue
            row = {"type": kind, "title": getattr(it, "title", None), "year": getattr(it, "year", None),
                   "ids": {k: ids.get(k) for k in ("imdb","tmdb","tvdb") if ids.get(k)}}
            nr = _norm_row(row)
            out[canonical_key(nr)] = nr

    if not out: return out
    try:
        mm = MetadataManager(lambda: {}, lambda _cfg: None)
        healed = mm.reconcile_ids(list(out.values()))
        fixed: Dict[str, Dict[str, Any]] = {}
        for r in healed:
            nr = _norm_row({
                "type": r.get("type"), "title": r.get("title"), "year": r.get("year"),
                "ids": {k: (r.get("ids") or {}).get(k) for k in ("imdb","tmdb","tvdb") if (r.get("ids") or {}).get(k)}
            })
            fixed[canonical_key(nr)] = nr
        return fixed
    except Exception:
        return out

def _resolve_discover_item(acct: MyPlexAccount, ids: dict, libtype: str) -> Optional[Any]:
    queries: List[str] = []
    if ids.get("imdb"): queries.append(ids["imdb"])
    if ids.get("tmdb"): queries.append(str(ids["tmdb"]))
    if ids.get("tvdb"): queries.append(str(ids["tvdb"]))
    if ids.get("title"): queries.append(ids["title"])
    queries = list(dict.fromkeys(queries))
    for q in queries:
        try: hits: Sequence[Any] = acct.searchDiscover(q, libtype=libtype) or []  # type: ignore
        except Exception: hits = []
        for md in hits:
            md_ids = _ids_from_plexobj(md)
            if ids.get("imdb") and md_ids.get("imdb") == ids.get("imdb"): return md
            if ids.get("tmdb") and md_ids.get("tmdb") == ids.get("tmdb"): return md
            if ids.get("tvdb") and md_ids.get("tvdb") == ids.get("tvdb"): return md
            if ids.get("title") and ids.get("year"):
                try:
                    same_t = str(md.title).strip().lower() == str(ids["title"]).strip().lower()
                    same_y = int(getattr(md, "year", 0) or 0) == int(ids["year"])
                    if same_t and same_y: return md
                except Exception: pass
    return None

def _watchlist_add(acct: MyPlexAccount, items: Iterable[Mapping[str, Any]], *, cfg_root: Mapping[str, Any]) -> Tuple[int,int]:
    added = 0; suppressed = 0
    for i, it in enumerate(items, 1):
        ids = dict(it.get("ids") or {})
        if "title" not in ids and it.get("title"): ids["title"] = it["title"]
        if "year"  not in ids and it.get("year"):  ids["year"]  = it["year"]
        libtype = "movie" if (it.get("type") or "movie") in ("movie", "movies") else "show"
        md = _resolve_discover_item(acct, ids, libtype)
        if not md:
            _unresolved_upsert("PLEX", "watchlist", it, reason="resolve_failed", last_error=None, cfg_root=cfg_root)
            suppressed += 1; continue
        try:
            cast(Any, md).addToWatchlist(account=acct); added += 1
            _unresolved_clear(it)
        except Exception as e:
            _unresolved_upsert("PLEX", "watchlist", it, reason="write_failed", last_error=str(e), cfg_root=cfg_root)
            suppressed += 1
        if (i % THROTTLE_EVERY) == 0:
            try: time.sleep(0.05)
            except Exception: pass
    return added, suppressed

def _watchlist_remove(acct: MyPlexAccount, items: Iterable[Mapping[str, Any]], *, cfg_root: Mapping[str, Any]) -> Tuple[int,int]:
    removed = 0; suppressed = 0
    for i, it in enumerate(items, 1):
        ids = dict(it.get("ids") or {})
        if "title" not in ids and it.get("title"): ids["title"] = it["title"]
        if "year"  not in ids and it.get("year"):  ids["year"]  = it["year"]
        libtype = "movie" if (it.get("type") or "movie") in ("movie", "movies") else "show"
        md = _resolve_discover_item(acct, ids, libtype)
        if not md:
            _unresolved_upsert("PLEX", "watchlist", it, reason="resolve_failed", last_error=None, cfg_root=cfg_root)
            suppressed += 1; continue
        try:
            cast(Any, md).removeFromWatchlist(account=acct); removed += 1
            _unresolved_clear(it)
        except Exception as e:
            _unresolved_upsert("PLEX", "watchlist", it, reason="write_failed", last_error=str(e), cfg_root=cfg_root)
            suppressed += 1
        if (i % THROTTLE_EVERY) == 0:
            try: time.sleep(0.05)
            except Exception: pass
    return removed, suppressed

# --- ratings ------------------------------------------------------------------
def _rating_row(obj: Any, kind: str) -> Optional[Dict[str, Any]]:
    try: ur = getattr(obj, "userRating", None)
    except Exception: ur = None
    if ur is None: return None
    ids = _ids_from_plexobj(obj)
    if not ids: return None
    row: Dict[str, Any] = {
        "type": _singular(kind), "title": getattr(obj, "title", None), "year": getattr(obj, "year", None),
        "ids": ids, "rating": int(round(float(ur))) if isinstance(ur, (int, float)) else None,
    }
    try:
        ra = getattr(obj, "lastRatedAt", None)
        iso = _to_utc(ra) if ra else None
        if iso: row["rated_at"] = iso
    except Exception:
        pass
    return _norm_row(row)

def _ratings_index_full(env: PlexEnv, cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    plex_cfg = dict(cfg_root.get("plex") or {})
    include, exclude = _section_filters(plex_cfg)
    idx: Dict[str, Dict[str, Any]] = {}
    for s in env.servers:
        try:
            sections = [sec for sec in s.library.sections() if getattr(sec, "type", "") in ("movie", "show")]
            sections = [sec for sec in sections if _match_section(sec, include, exclude)]
            for section in sections:
                if section.type == "movie":
                    try:
                        for mv in section.all():
                            row = _rating_row(mv, "movie")
                            if not row or row.get("rating") is None: continue
                            key = None
                            for k in ("imdb","tmdb","tvdb"):
                                if row["ids"].get(k): key = f"{k}:{row['ids'][k]}".lower(); break
                            if not key: key = canonical_key(row)
                            idx[key] = row
                    except Exception: continue
                if section.type == "show":
                    try:
                        for sh in section.all():
                            r_show = _rating_row(sh, "show")
                            if r_show and r_show.get("rating") is not None:
                                key = None
                                for k in ("imdb","tmdb","tvdb"):
                                    if r_show["ids"].get(k): key = f"{k}:{r_show['ids'][k]}".lower(); break
                                if not key: key = canonical_key(r_show)
                                idx[key] = r_show
                            try:
                                for sn in sh.seasons():
                                    r_season = _rating_row(sn, "season")
                                    if not r_season or r_season.get("rating") is None: continue
                                    key = None
                                    for k in ("imdb","tmdb","tvdb"):
                                        if r_season["ids"].get(k): key = f"{k}:{r_season['ids'][k]}".lower(); break
                                    if not key:
                                        sid = None
                                        for k in ("imdb","tmdb","tvdb"):
                                            if r_show and (r_show.get("ids") or {}).get(k): sid = f"{k}:{r_show['ids'][k]}"; break
                                        snum = getattr(sn, "index", None)
                                        key = f"{(sid or canonical_key(r_show or r_season))}#season:{snum}"
                                    idx[key] = r_season
                            except Exception: pass
                            try:
                                for ep in sh.episodes():
                                    r_ep = _rating_row(ep, "episode")
                                    if not r_ep or r_ep.get("rating") is None: continue
                                    key = None
                                    for k in ("imdb","tmdb","tvdb"):
                                        if r_ep["ids"].get(k): key = f"{k}:{r_ep['ids'][k]}".lower(); break
                                    if not key:
                                        sid = None
                                        for k in ("imdb","tmdb","tvdb"):
                                            if r_show and (r_show.get("ids") or {}).get(k): sid = f"{k}:{r_show['ids'][k]}"; break
                                        snum = getattr(ep, "seasonNumber", getattr(ep, "seasonNumberLocal", None))
                                        enum = getattr(ep, "index", None)
                                        key = f"{(sid or canonical_key(r_show or r_ep))}#s{str(snum).zfill(2)}e{str(enum).zfill(2)}"
                                    idx[key] = r_ep
                            except Exception: pass
                    except Exception: continue
        except Exception: continue
    return idx

def _match_section(sec: Any, include: set, exclude: set) -> bool:
    t = getattr(sec, "title", "") or ""
    k = getattr(sec, "key", "") or ""
    u = getattr(sec, "uuid", "") or ""
    keys = {str(t).strip().lower(), str(k).strip().lower(), str(u).strip().lower()}
    if include and not (keys & include): return False
    if exclude and (keys & exclude): return False
    return True

def _ratings_index(env: PlexEnv, cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    ttl_min = int(((cfg_root.get("ratings") or {}).get("cache") or {}).get("ttl_minutes") or 0)
    ttl_sec = max(0, ttl_min * 60)

    shadow = _ratings_shadow_load()
    items = dict(shadow.get("items") or {}); ts = int(shadow.get("ts") or 0)
    now = int(time.time())
    if ttl_sec > 0 and ts > 0 and (now - ts) < ttl_sec and items:
        return _normalize_shadow_items(items)

    idx = _ratings_index_full(env, cfg_root)
    if not idx:
        return _normalize_shadow_items(items)

    # heal ids; keep rating/rated_at
    mm = MetadataManager(lambda: cfg_root, lambda _cfg: None)
    healed = mm.reconcile_ids(list(idx.values()))
    healed_idx: Dict[str, Dict[str, Any]] = {}
    for r, orig in zip(healed, idx.values()):
        node = dict(r)
        if orig.get("rating") is not None: node["rating"] = orig.get("rating")
        if orig.get("rated_at"): node["rated_at"] = orig.get("rated_at")
        nr = _norm_row(node)
        healed_idx[canonical_key(nr)] = nr

    if healed_idx:
        _ratings_shadow_save(healed_idx)
        return _normalize_shadow_items(healed_idx)

    return _normalize_shadow_items(items)

def _ratings_apply(env: PlexEnv, items: Iterable[Mapping[str, Any]], *, cfg_root: Mapping[str, Any]) -> Tuple[int,int]:
    updated = 0; suppressed = 0
    sh = _ratings_shadow_load()
    shadow_map: Dict[str, Any] = _normalize_shadow_items(dict(sh.get("items") or {}))

    for i, it in enumerate(items, 1):
        tr = it.get("rating")
        if tr is None:
            continue

        libtype = _libtype_from_item(it)
        obj = _resolve_on_servers(env, it, libtype)
        if not obj:
            _unresolved_upsert("PLEX", "ratings", it, reason="resolve_failed", last_error=None, cfg_root=cfg_root)
            suppressed += 1
            continue

        try:
            obj.rate(float(int(tr))); updated += 1
            _unresolved_clear(it)
        except Exception as e:
            _unresolved_upsert("PLEX", "ratings", it, reason="write_failed", last_error=str(e), cfg_root=cfg_root)
            suppressed += 1
            continue

        node = _norm_row({
            "type": _singular(it.get("type") or "movie"),
            "title": it.get("title"),
            "year": it.get("year"),
            "ids": {k: (it.get("ids") or {}).get(k) for k in ("imdb","tmdb","tvdb") if (it.get("ids") or {}).get(k)},
        })

        # Build a stable key (prefer external IDs; otherwise fall back to composite key for S/E)
        key: Optional[str] = None
        ids = node["ids"]
        for k in ("imdb", "tmdb", "tvdb"):
            if ids.get(k):
                key = f"{k}:{ids[k]}".lower()
                break

        if not key and node["type"] in ("season", "episode"):
            show_id = next((f"{k}:{ids[k]}" for k in ("imdb","tmdb","tvdb") if ids.get(k)), None)
            if node["type"] == "season":
                snum = it.get("season") or it.get("season_number") or it.get("index")
                if snum is not None:
                    key = f"{(show_id or canonical_key(node))}#season:{snum}"
            else:  # episode
                snum = it.get("season") or it.get("season_number")
                enum = it.get("episode") or it.get("episode_number") or it.get("index")
                if snum is not None and enum is not None:
                    key = f"{(show_id or canonical_key(node))}#s{str(int(snum)).zfill(2)}e{str(int(enum)).zfill(2)}"

        if not key:
            key = canonical_key(node)

        shadow_map[key] = {
            "type": node["type"],
            "title": node.get("title"),
            "year": node.get("year"),
            "ids": node["ids"],
            "rating": int(tr),
            "rated_at": _now_iso(),
        }

        if (i % THROTTLE_EVERY) == 0:
            try: time.sleep(0.05)
            except Exception: pass

    if updated:
        _ratings_shadow_save(_normalize_shadow_items(shadow_map))
    return updated, suppressed

# --- history ------------------------------------------------------------------
def _history_row(obj: Any, kind: str) -> Optional[Dict[str, Any]]:
    try: vc = getattr(obj, "viewCount", 0) or 0
    except Exception: vc = 0
    if vc <= 0: return None

    ids = _ids_from_plexobj(obj)
    if not ids: return None

    row: Dict[str, Any] = {
        "type": _singular(kind),
        "title": getattr(obj, "title", None),
        "year": getattr(obj, "year", None),
        "ids": ids,
        "watched": True,
    }

    # timestamp (unchanged)
    try:
        wa = getattr(obj, "lastViewedAt", None)
        iso = _to_utc(wa) if wa else None
        if iso: row["watched_at"] = iso
    except Exception:
        pass

    # EPISODE EXTRAS: season/episode & show ids from grandparent guid
    if row["type"] == "episode":
        try:
            row["season"]  = getattr(obj, "seasonNumber", getattr(obj, "seasonNumberLocal", None))
            row["episode"] = getattr(obj, "index", None)
        except Exception:
            pass
        # Extract show ids from grandparentGuid if present (cheap and avoids .show() fetch)
        try:
            gpg = getattr(obj, "grandparentGuid", None)
            if isinstance(gpg, str) and gpg:
                show_ids = {}
                if "imdb" in gpg:
                    m = _PAT_IMDB.search(gpg)
                    if m: show_ids["imdb"] = m.group(1)
                if "tmdb" in gpg:
                    m = _PAT_TMDB.search(gpg)
                    if m: show_ids["tmdb"] = int(m.group(1))
                if "thetvdb" in gpg:
                    m = _PAT_TVDB.search(gpg)
                    if m: show_ids["tvdb"] = int(m.group(1))
                if gpg and "guid" not in show_ids:
                    show_ids["guid"] = gpg
                if show_ids:
                    row["show_ids"] = show_ids
        except Exception:
            pass

    return _norm_row(row)


def _history_index_full(env: PlexEnv, cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    plex_cfg = dict(cfg_root.get("plex") or {})
    include, exclude = _section_filters(plex_cfg)
    idx: Dict[str, Dict[str, Any]] = {}
    for s in env.servers:
        try:
            sections = [sec for sec in s.library.sections() if getattr(sec, "type", "") in ("movie", "show")]
            sections = [sec for sec in sections if _match_section(sec, include, exclude)]
            for section in sections:
                if section.type == "movie":
                    try:
                        for mv in section.all():
                            row = _history_row(mv, "movie")
                            if not row: continue
                            key = None
                            for k in ("imdb","tmdb","tvdb"):
                                if row["ids"].get(k): key = f"{k}:{row['ids'][k]}".lower(); break
                            if not key: key = canonical_key(row)
                            idx[key] = row
                    except Exception: continue
                if section.type == "show":
                    try:
                        for sh in section.all():
                            try:
                                for ep in sh.episodes():
                                    row = _history_row(ep, "episode")
                                    if not row: continue
                                    key = None
                                    for k in ("imdb","tmdb","tvdb"):
                                        if row["ids"].get(k): key = f"{k}:{row['ids'][k]}".lower(); break
                                    if not key: key = canonical_key(row)
                                    idx[key] = row
                            except Exception: pass
                    except Exception: continue
        except Exception: continue
    return idx

def _history_index(env: PlexEnv, cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    ttl_min = int(((cfg_root.get("history") or {}).get("cache") or {}).get("ttl_minutes") or 0)
    ttl_sec = max(0, ttl_min * 60)

    shadow = _history_shadow_load()
    items = dict(shadow.get("items") or {}); ts = int(shadow.get("ts") or 0)
    now = int(time.time())
    if ttl_sec > 0 and ts > 0 and (now - ts) < ttl_sec and items:
        return _normalize_shadow_items(items)

    idx = _history_index_full(env, cfg_root)
    if not idx:
        return _normalize_shadow_items(items)

    # Heal ids (movies as movies; seasons/episodes as shows). Keep watched flags/timestamps.
    mm = MetadataManager(lambda: cfg_root, lambda _cfg: None)
    keys = list(idx.keys())
    rows = []
    for v in idx.values():
        ent = "movie" if (str(v.get("type") or "").startswith("movie")) else "show"
        rows.append({"type": ent, "title": v.get("title"), "year": v.get("year"), "ids": v.get("ids")})

    healed = mm.reconcile_ids(rows)

    healed_idx: Dict[str, Dict[str, Any]] = {}
    for k, v, h in zip(keys, idx.values(), healed):
        merged_ids = dict(v.get("ids") or {}); merged_ids.update((h.get("ids") or {}))
        node = {
            "type": (v.get("type") or "movie"),
            "title": v.get("title"),
            "year": v.get("year"),
            "ids": {kk: vv for kk, vv in merged_ids.items() if vv not in (None, "", [], {})},
        }
        if v.get("watched"): node["watched"] = True
        if v.get("watched_at"): node["watched_at"] = v.get("watched_at")
        nr = _norm_row(node)
        healed_idx[canonical_key(nr)] = nr

    if healed_idx:
        _history_shadow_save(healed_idx)
        return _normalize_shadow_items(healed_idx)

    return _normalize_shadow_items(items)


def _history_change(env: PlexEnv, items: Iterable[Mapping[str, Any]], *, watched: bool, cfg_root: Mapping[str, Any]) -> Tuple[int,int]:
    changed = 0; suppressed = 0

    sh = _history_shadow_load()
    shadow_map: Dict[str, Any] = _normalize_shadow_items(dict(sh.get("items") or {}))
    shadow_touched = False

    for i, it in enumerate(items, 1):
        libtype = _libtype_from_item(it)
        obj = _resolve_on_servers(env, it, libtype)

        raw_wat = it.get("watched_at")
        if isinstance(raw_wat, str) and raw_wat.strip():
            wat_iso = _clamp_ts(raw_wat)
        elif isinstance(raw_wat, (_dt.datetime, int, float)):
            wat_iso = _to_utc(raw_wat) or _now_iso()
        else:
            wat_iso = _now_iso()

        norm_node = _norm_row({
            "type": _singular(it.get("type") or "movie"),
            "title": it.get("title"),
            "year": it.get("year"),
            "ids": {k: (it.get("ids") or {}).get(k) for k in ("imdb","tmdb","tvdb") if (it.get("ids") or {}).get(k)},
            "watched_at": wat_iso if watched else None,
        })

        if not obj:
            _unresolved_upsert("PLEX","history",it,reason="resolve_failed",last_error=None,cfg_root=cfg_root,
                               hint={"type": norm_node["type"], "title": norm_node.get("title"), "year": norm_node.get("year"),
                                     "ids": norm_node.get("ids"), "watched_at": (wat_iso if watched else None)})
            suppressed += 1
            continue

        try:
            obj.markPlayed() if watched else obj.markUnplayed()
            changed += 1
            _unresolved_clear(it)
        except Exception as e:
            _unresolved_upsert("PLEX","history",it,reason="write_failed",last_error=str(e),cfg_root=cfg_root,
                               hint={"type": norm_node["type"], "title": norm_node.get("title"), "year": norm_node.get("year"),
                                     "ids": norm_node.get("ids"), "watched_at": (wat_iso if watched else None)})
            suppressed += 1
            continue

        if watched:
            ids = norm_node["ids"]
            key = next((f"{k}:{ids[k]}".lower() for k in ("imdb","tmdb","tvdb") if ids.get(k)), None)
            if not key: key = canonical_key(norm_node)
            shadow_map[key] = {
                "type": norm_node["type"],
                "title": norm_node.get("title"),
                "year": norm_node.get("year"),
                "ids": norm_node.get("ids"),
                "watched": True,
                "watched_at": wat_iso,
            }
            shadow_touched = True

        if (i % THROTTLE_EVERY) == 0:
            try: time.sleep(0.05)
            except Exception: pass

    if shadow_touched:
        _history_shadow_save(_normalize_shadow_items(shadow_map))

    return changed, suppressed


# --- playlists ----------------------------------------------------------------
def _playlists_index(env: PlexEnv) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for s in env.servers:
        try:
            for pl in s.playlists():
                if getattr(pl, "playlistType", "") not in ("video", "movie", "show"): continue
                key = f"playlist:{pl.ratingKey}".lower()
                row = {"type": "playlist", "title": getattr(pl, "title", None), "ids": {"plex": pl.ratingKey}}
                idx[key] = row
        except Exception: continue
    return idx

def _playlist_add_items(env: PlexEnv, playlist_title: str, items: Iterable[Mapping[str, Any]], mtype_hint: Optional[str]=None, *, cfg_root: Mapping[str, Any]) -> Tuple[int,int]:
    if not env.servers: return (0,0)
    s = env.servers[0]
    libtype = "movie" if (mtype_hint or "movie") in ("movie","movies") else "show"
    plex_items: List[Any] = []; suppressed = 0
    for it in items:
        obj = _resolve_on_servers(env, it, libtype)
        if obj: plex_items.append(obj)
        else:
            _unresolved_upsert("PLEX","playlists",it,reason="resolve_failed",last_error=None,cfg_root=cfg_root)
            suppressed += 1
    if not plex_items: return (0, suppressed)
    try:
        for pl in s.playlists():
            if getattr(pl, "title", "") == playlist_title:
                pl.addItems(plex_items); return (len(plex_items), suppressed)
        s.createPlaylist(playlist_title, plex_items); return (len(plex_items), suppressed)
    except Exception as e:
        for it in items:
            _unresolved_upsert("PLEX","playlists",it,reason="write_failed",last_error=str(e),cfg_root=cfg_root)
        return (0, suppressed + len(plex_items))

def _playlist_remove_items(env: PlexEnv, playlist_title: str, items: Iterable[Mapping[str, Any]], mtype_hint: Optional[str]=None, *, cfg_root: Mapping[str, Any]) -> Tuple[int,int]:
    if not env.servers: return (0,0)
    s = env.servers[0]
    libtype = "movie" if (mtype_hint or "movie") in ("movie","movies") else "show"
    plex_items: List[Any] = []; suppressed = 0
    for it in items:
        obj = _resolve_on_servers(env, it, libtype)
        if obj: plex_items.append(obj)
        else:
            _unresolved_upsert("PLEX","playlists",it,reason="resolve_failed",last_error=None,cfg_root=cfg_root)
            suppressed += 1
    if not plex_items: return (0, suppressed)
    try:
        for pl in s.playlists():
            if getattr(pl, "title", "") == playlist_title:
                pl.removeItems(plex_items); return (len(plex_items), suppressed)
    except Exception as e:
        for it in items:
            _unresolved_upsert("PLEX","playlists",it,reason="write_failed",last_error=str(e),cfg_root=cfg_root)
        return (0, suppressed + len(plex_items))
    return (0, suppressed)

# --- provider protocol ---------------------------------------------------------
class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]: ...
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...

# --- OPS ----------------------------------------------------------------------
class _PlexOPS:
    def name(self) -> str: return "PLEX"
    def label(self) -> str: return "Plex"
    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}
    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": False,
            "ratings": {"types": {"movies": True, "shows": True, "seasons": True, "episodes": True}, "upsert": True, "unrate": True, "from_date": False},
        }

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        pol = _cfg_unresolved(cfg)
        policy = pol["policy"]
        if feature == "watchlist":
            acct = _ensure_account(cfg)
            live = _watchlist_index(acct)
            virt = _unresolved_virtual_for_index("watchlist", policy)
            merged = dict(virt); merged.update(live)
            _cursor_save("watchlist.fingerprint", {"count": len(live)})
            _unresolved_prune_ttl(pol["ttl_days"])
            return merged
        env = _env_from_config(cfg)
        if feature == "ratings":
            live = _ratings_index(env, cfg)
            _unresolved_reset_if_library_grew("ratings", len(live))
            virt = _unresolved_virtual_for_index("ratings", policy)
            merged = dict(virt); merged.update(live)
            _unresolved_prune_ttl(pol["ttl_days"])
            return merged
        if feature == "history":
            live = _history_index(env, cfg)
            _unresolved_reset_if_library_grew("history", len(live))
            virt = _unresolved_virtual_for_index("history", policy)
            merged = dict(virt); merged.update(live)
            _unresolved_prune_ttl(pol["ttl_days"])
            return merged
        if feature == "playlists":
            live = _playlists_index(env)
            _cursor_save("playlists.fingerprint", {"count": len(live)})
            _unresolved_prune_ttl(pol["ttl_days"])
            return live
        return {}

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        items_list = list(items)
        if dry_run: return {"ok": True, "count": len(items_list), "dry_run": True}
        try:
            if feature == "watchlist":
                acct = _ensure_account(cfg)
                n_ok, n_sup = _watchlist_add(acct, items_list, cfg_root=cfg)
                return {"ok": True, "count": n_ok, "unresolved_suppressed": n_sup}
            env = _env_from_config(cfg)
            if feature == "ratings":
                n_ok, n_sup = _ratings_apply(env, items_list, cfg_root=cfg)
                return {"ok": True, "count": n_ok, "unresolved_suppressed": n_sup}
            if feature == "history":
                n_ok, n_sup = _history_change(env, items_list, watched=True, cfg_root=cfg)
                return {"ok": True, "count": n_ok, "unresolved_suppressed": n_sup}
            if feature == "playlists":
                env = _env_from_config(cfg)
                total = 0; suppressed = 0
                for pl in items_list:
                    title = pl.get("playlist") or pl.get("title")
                    if not title: continue
                    mtyp = pl.get("type") or "movie"
                    cnt, sup = _playlist_add_items(env, str(title), pl.get("items") or [], mtype_hint=mtyp, cfg_root=cfg)
                    total += cnt; suppressed += sup
                return {"ok": True, "count": total, "unresolved_suppressed": suppressed}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        return {"ok": True, "count": 0}

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        items_list = list(items)
        if dry_run: return {"ok": True, "count": len(items_list), "dry_run": True}
        try:
            if feature == "watchlist":
                acct = _ensure_account(cfg)
                n_ok, n_sup = _watchlist_remove(acct, items_list, cfg_root=cfg)
                return {"ok": True, "count": n_ok, "unresolved_suppressed": n_sup}
            env = _env_from_config(cfg)
            if feature == "ratings":
                n_ok = 0; suppressed = 0
                for it in items_list:
                    libtype = _libtype_from_item(it)
                    obj = _resolve_on_servers(env, it, libtype)
                    if not obj:
                        _unresolved_upsert("PLEX","ratings",it,reason="resolve_failed",last_error=None,cfg_root=cfg)
                        suppressed += 1; continue
                    try:
                        obj.rate(None); n_ok += 1; _unresolved_clear(it)
                    except Exception as e:
                        _unresolved_upsert("PLEX","ratings",it,reason="write_failed",last_error=str(e),cfg_root=cfg)
                        suppressed += 1
                if n_ok:
                    sh = _ratings_shadow_load(); items_map = dict(sh.get("items") or {})
                    for it in items_list:
                        k = canonical_key(_norm_row(it)); items_map.pop(k, None)
                    _ratings_shadow_save(items_map)
                return {"ok": True, "count": n_ok, "unresolved_suppressed": suppressed}
            if feature == "history":
                n_ok, n_sup = _history_change(env, items_list, watched=False, cfg_root=cfg)
                if n_ok:
                    sh = _history_shadow_load(); items_map = dict(sh.get("items") or {})
                    for it in items_list:
                        k = canonical_key(_norm_row(it)); items_map.pop(k, None)
                    _history_shadow_save(items_map)
                return {"ok": True, "count": n_ok, "unresolved_suppressed": n_sup}
            if feature == "playlists":
                env = _env_from_config(cfg)
                total = 0; suppressed = 0
                for pl in items_list:
                    title = pl.get("playlist") or pl.get("title")
                    if not title: continue
                    mtyp = pl.get("type") or "movie"
                    cnt, sup = _playlist_remove_items(env, str(title), pl.get("items") or [], mtype_hint=mtyp, cfg_root=cfg)
                    total += cnt; suppressed += sup
                return {"ok": True, "count": total, "unresolved_suppressed": suppressed}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        return {"ok": True, "count": 0}

# export
OPS: InventoryOps = _PlexOPS()

# --- manifest -----------------------------------------------------------------
try:
    from providers.sync._base import SyncModule, ModuleInfo, ModuleCapabilities  # type: ignore
except Exception:  # pragma: no cover
    class SyncModule: ...
    @dataclass
    class ModuleCapabilities:
        supports_dry_run: bool = True
        supports_cancel: bool = True
        supports_timeout: bool = True
        status_stream: bool = True
        bidirectional: bool = True
        config_schema: dict | None = None
    @dataclass
    class ModuleInfo:
        name: str
        version: str
        description: str
        vendor: str
        capabilities: ModuleCapabilities

class PLEXModule(SyncModule):
    info = ModuleInfo(
        name="PLEX",
        version=__VERSION__,
        description="Plex via PlexAPI only: watchlist, ratings, history, playlists. With unresolved/backoff to keep diffs clean.",
        vendor="community",
        capabilities=ModuleCapabilities(
            supports_dry_run=True,
            supports_cancel=True,
            supports_timeout=True,
            status_stream=True,
            bidirectional=True,
            config_schema={
                "type": "object",
                "properties": {
                    "plex": {
                        "type": "object",
                        "properties": {
                            "account_token": {"type": "string", "minLength": 1},
                            "servers": {
                                "type": "object",
                                "properties": {
                                    "machine_ids": {"type": "array", "items": {"type": "string"}},
                                    "sections": {
                                        "type": "object",
                                        "properties": {
                                            "include": {"type": "array", "items": {"type": "string"}},
                                            "exclude": {"type": "array", "items": {"type": "string"}},
                                        },
                                        "additionalProperties": False,
                                    },
                                },
                                "additionalProperties": False,
                            },
                            "unresolved": {
                                "type": "object",
                                "properties": {
                                    "policy": {"type": "string", "enum": ["backoff","virtual"]},
                                    "base_hours": {"type": "integer", "minimum": 1},
                                    "max_days": {"type": "integer", "minimum": 1},
                                    "max_retries": {"type": "integer", "minimum": 1},
                                    "ttl_days": {"type": "integer", "minimum": 1},
                                },
                                "additionalProperties": False,
                            },
                        },
                        "required": ["account_token"],
                        "additionalProperties": False,
                    },
                    "ratings": {
                        "type": "object",
                        "properties": {"cache": {"type": "object", "properties": {"ttl_minutes": {"type": "integer", "minimum": 0}}}},
                        "additionalProperties": False,
                    },
                    "history": {
                        "type": "object",
                        "properties": {"cache": {"type": "object", "properties": {"ttl_minutes": {"type": "integer", "minimum": 0}}}},
                        "additionalProperties": False,
                    },
                    "runtime": {"type": "object", "properties": {"debug": {"type": "boolean"}}, "additionalProperties": False},
                },
                "required": ["plex"],
                "additionalProperties": False,
            },
        ),
    )

    @staticmethod
    def supported_features() -> dict:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}

def get_manifest() -> dict:
    return {
        "name": PLEXModule.info.name,
        "label": "Plex",
        "features": PLEXModule.supported_features(),
        "capabilities": {
            "bidirectional": True,
            "ratings": {"types": {"movies": True, "shows": True, "seasons": True, "episodes": True}, "upsert": True, "unrate": True, "from_date": False},
        },
        "version": PLEXModule.info.version,
        "vendor": PLEXModule.info.vendor,
        "description": PLEXModule.info.description,
    }
