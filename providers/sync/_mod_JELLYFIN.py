from __future__ import annotations
# providers/sync/_mod_JELLYFIN.py

__VERSION__ = "0.3.0"
__all__ = ["OPS", "JELLYFINModule", "get_manifest"]

import json, os, time, re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple

import requests

PROGRESS_EVERY = 50
THROTTLE_EVERY = 200
_RUNTIME_DEBUG = False

# --- id normalization helpers (add these) ---
_PAT_IMDB = re.compile(r'(?:tt)?(\d{5,9})$')   # keep imdb as tt########
_PAT_NUM  = re.compile(r'(\d{1,10})$')         # grab trmdb/tvdb numeric suffix

# --- host hooks (optional) ----------------------------------------------------
try:
    from _logging import log as host_log
except Exception:
    def host_log(*a, **k): pass  # type: ignore

try:
    from _statistics import Stats  # type: ignore
    _stats = Stats()
except Exception:
    _stats = None

def _record_http_stat(method: str, url: str, r: Optional[requests.Response], bytes_out: int = 0) -> None:
    if not _stats: return
    try:
        if r is not None:
            elapsed = getattr(r, "elapsed", None)
            ms = int(elapsed.total_seconds() * 1000) if elapsed else 0
            status = int(getattr(r, "status_code", 0) or 0)
            ok = bool(getattr(r, "ok", False))
            content = getattr(r, "content", b"") or b""
            _stats.record_http(
                provider="JELLYFIN",
                fn=method.upper(),
                path=url,
                status=status,
                ok=ok,
                bytes_in=len(content),
                bytes_out=int(bytes_out or 0),
                ms=ms,
            )
        else:
            _stats.record_http(provider="JELLYFIN", fn=method.upper(), path=url, status=0, ok=False, bytes_in=0, bytes_out=int(bytes_out or 0), ms=0)
    except Exception:
        pass

# --- module debug ----------------------------------------------------------
def _set_runtime_debug(cfg_root: Mapping[str, Any]) -> None:
    """Cache debug flag per run."""
    global _RUNTIME_DEBUG
    try:
        _RUNTIME_DEBUG = bool(((cfg_root.get("runtime") or {}).get("debug")))
    except Exception:
        _RUNTIME_DEBUG = False
        
# --- simple persistence (state/shadows) --------------------------------------
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

_CUR_FILE = "jellyfin_cursors.json"

def _cursors_path() -> Path:
    return _state_root() / _CUR_FILE

def _read_json(path: Path, fallback: Any) -> Any:
    try: return json.loads(path.read_text("utf-8"))
    except Exception: return fallback

def _write_json(path: Path, data: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
    except Exception:
        pass

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

def _to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
    try:
        s2 = s.replace("Z", "+00:00") if s.endswith("Z") else s
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _safe_srv(server: str) -> str:
    return (server or "").replace("://", "_").replace("/", "_")

def _cursor_key(server: str, user_id: str, scope: str) -> str:
    return f"jf__{_safe_srv(server)}__{user_id}__{scope}.json"

def _cursor_save(server: str, user_id: str, scope: str, data: Mapping[str, Any]) -> None:
    entry = dict(data); entry.setdefault("updated_at", _now_iso())
    key = _cursor_key(server, user_id, scope)
    all_cur = _read_json(_cursors_path(), {})
    if not isinstance(all_cur, dict): all_cur = {}
    all_cur[key] = entry
    _write_json(_cursors_path(), all_cur)

def _cursor_load(server: str, user_id: str, scope: str) -> Dict[str, Any]:
    key = _cursor_key(server, user_id, scope)
    all_cur = _read_json(_cursors_path(), {})
    if isinstance(all_cur, dict) and key in all_cur and isinstance(all_cur[key], dict):
        return dict(all_cur[key])
    return {}

def _shadow_path(name: str) -> Path: return _state_root() / name
def _shadow_read(name: str, fallback: Any) -> Any: return _read_json(_shadow_path(name), fallback)
def _shadow_write(name: str, data: Any) -> None: _write_json(_shadow_path(name), data)

def _ratings_shadow_load() -> Dict[str, Any]:
    return _shadow_read("jellyfin_ratings.shadow.json", {"items": {}, "ts": 0})

def _ratings_shadow_save(items: Mapping[str, Any]) -> None:
    _shadow_write("jellyfin_ratings.shadow.json", {"items": dict(items), "ts": int(time.time())})

def _history_shadow_load() -> Dict[str, Any]:
    return _shadow_read("jellyfin_history.shadow.json", {"items": {}, "ts": 0})

def _history_shadow_save(items: Mapping[str, Any]) -> None:
    _shadow_write("jellyfin_history.shadow.json", {"items": dict(items), "ts": int(time.time())})

# --- unresolved/backoff shadow -----------------------------------------------
_UNRESOLVED_FILE = "jellyfin_unresolved.shadow.json"

def _unresolved_path() -> Path:
    return _shadow_path(_UNRESOLVED_FILE)

def _unresolved_load() -> Dict[str, Any]:
    obj = _shadow_read(_UNRESOLVED_FILE, {"items": {}, "ts": 0})
    if not isinstance(obj, Mapping): return {"items": {}, "ts": 0}
    items = obj.get("items") or {}
    return {"items": dict(items) if isinstance(items, Mapping) else {}, "ts": int(obj.get("ts") or 0)}

def _unresolved_save(items: Mapping[str, Any]) -> None:
    _shadow_write(_UNRESOLVED_FILE, {"items": dict(items), "ts": int(time.time())})

def _cfg_unresolved(jf_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    raw = dict((jf_cfg or {}).get("unresolved") or {})
    # Defaults: policy backoff, exponential base 6h, cap 30 days, max 8 tries, TTL 90 days
    out = {
        "policy": str(raw.get("policy") or "backoff"),
        "base_hours": int(raw.get("base_hours") or 6),
        "max_days": int(raw.get("max_days") or 30),
        "max_retries": int(raw.get("max_retries") or 8),
        "ttl_days": int(raw.get("ttl_days") or 90),
    }
    if out["policy"] not in ("backoff", "virtual"): out["policy"] = "backoff"
    return out

def _compute_next_due(unres_cfg: Mapping[str, Any], tries: int) -> datetime:
    base_h = max(1, int(unres_cfg.get("base_hours") or 6))
    max_days = max(1, int(unres_cfg.get("max_days") or 30))
    # Exponential: base * 2^(tries-1)
    hours = base_h * (2 ** max(0, tries - 1))
    hours = min(hours, max_days * 24)
    return _utc_now() + timedelta(hours=hours)

def _ttl_expired(unres_cfg: Mapping[str, Any], first_seen_at: Optional[str]) -> bool:
    ttl_days = max(1, int(unres_cfg.get("ttl_days") or 90))
    t0 = _parse_iso(first_seen_at)
    if not t0: return False
    return (_utc_now() - t0) > timedelta(days=ttl_days)

def _ckey_from_row(row: Mapping[str, Any]) -> str:
    return canonical_key(row)

def _parse_ckey(ckey: str) -> Tuple[Dict[str, str], Optional[str], Optional[str], Optional[int]]:
    """
    Best effort: return (ids, type, title, year) from canonical key.
    """
    ids: Dict[str, str] = {}
    if ":" in ckey and not ckey.startswith(("movie|", "show|", "episode|")):
        # id-type canonical (e.g., imdb:tt123, tmdb:123, tvdb:456)
        kind, val = ckey.split(":", 1)
        if kind in ("imdb", "tmdb", "tvdb", "trakt", "plex", "guid", "slug", "jellyfin") and val:
            ids[kind] = val
            return ids, None, None, None
    m = re.match(r"^(movie|show|episode)\|title:(.*)\|year:(.*)$", ckey)
    if not m: return ids, None, None, None
    typ, ttl, yr = m.group(1), (m.group(2) or "").strip(), (m.group(3) or "").strip()
    try: y = int(yr) if yr else None
    except Exception: y = None
    return ids, typ, ttl if ttl else None, y

def _make_virtual_row_from_ckey(ckey: str, type_hint: Optional[str]=None) -> Dict[str, Any]:
    ids, typ, ttl, yr = _parse_ckey(ckey)
    if type_hint and not typ: typ = type_hint
    if not typ: typ = "movie"
    row: Dict[str, Any] = {"type": typ, "ids": ids}
    if ttl: row["title"] = ttl
    if yr is not None: row["year"] = yr
    return _norm_row(row)

# --- unresolved store helpers  -------------------

def _unresolved_upsert(ckey: str, *, feature: str, reason: str, type_hint: Optional[str],
                       last_error: Optional[str], unres_cfg: Mapping[str, Any],
                       hint: Optional[Mapping[str, Any]] = None) -> None:
    store = _unresolved_load()
    items = dict(store["items"])
    now = _now_iso()
    ent = dict(items.get(ckey) or {})
    first = ent.get("first_seen_at") or now
    tries = int(ent.get("tries") or 0) + 1
    next_due = _compute_next_due(unres_cfg, tries)

    ent.update({
        "feature": feature,
        "reason": reason,
        "tries": tries,
        "first_seen_at": first,
        "last_tried_at": now,
        "next_due_at": _to_iso(next_due),
        "type": (type_hint or ent.get("type") or None),
    })
    if last_error:
        ent["last_error"] = str(last_error)[:500]

    if hint and isinstance(hint, Mapping):
        safe = {k: hint.get(k) for k in ("type", "title", "year", "ids", "rating", "watched_at") if hint.get(k) is not None}
        ent["hint"] = _norm_row(safe) if safe else None

    items[ckey] = ent
    _unresolved_save(items)


def _unresolved_remove_if_present(ckey: str) -> None:
    store = _unresolved_load()
    items = dict(store["items"])
    if ckey in items:
        items.pop(ckey, None)
        _unresolved_save(items)

def _unresolved_purge(unres_cfg: Mapping[str, Any]) -> int:
    store = _unresolved_load()
    items = dict(store["items"])
    removed = 0
    for k, v in list(items.items()):
        if _ttl_expired(unres_cfg, v.get("first_seen_at")):
            items.pop(k, None)
            removed += 1
    if removed: _unresolved_save(items)
    return removed

def _unresolved_stats_by_reason_and_type(items: Mapping[str, Any]) -> Dict[str, Dict[str, int]]:
    out: Dict[str, Dict[str, int]] = {}
    for _, v in (items or {}).items():
        r = str(v.get("reason") or "unknown")
        t = str(v.get("type") or "unknown")
        out.setdefault(r, {})
        out[r][t] = out[r].get(t, 0) + 1
    return out

# --- canonical keys & normalization ------------------------------------------
_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug", "jellyfin")

def canonical_key(item: Mapping[str, Any]) -> str:
    ids = dict(item.get("ids") or {})
    for k in ("tmdb", "imdb", "tvdb", "trakt", "guid", "slug", "plex"):
        v = ids.get(k)
        if v is not None and str(v).strip():
            return f"{k}:{str(v).strip().lower()}"

    # Fallback: typed composite key
    t = _norm_type(item.get("type"))
    ttl = str(item.get("series_title") or item.get("series") or item.get("title") or "").strip().lower()
    yr = item.get("year")
    yr_s = str(yr) if yr is not None else ""

    # Season/Episode disambiguation when no portable IDs exist
    if t == "season":
        sn = item.get("season") or item.get("season_number") or item.get("index")
        try:
            if sn is not None:
                sn_i = int(sn)
                return f"{t}|title:{ttl}|year:{yr_s}#season:{sn_i}"
        except Exception:
            pass

    if t == "episode":
        sn = (item.get("season") or item.get("season_number") or
              item.get("parent_index") or item.get("parentIndexNumber"))
        en = (item.get("episode") or item.get("episode_number") or
              item.get("index") or item.get("indexNumber"))
        try:
            if sn is not None and en is not None:
                sn_i = int(sn); en_i = int(en)
                return f"{t}|title:{ttl}|year:{yr_s}#s{sn_i:02d}e{en_i:02d}"
        except Exception:
            pass

    return f"{t}|title:{ttl}|year:{yr_s}"


def _norm_type(t: Any) -> str:
    x = (str(t or "")).strip().lower()
    if x in ("movies", "movie"): return "movie"
    if x in ("shows", "show", "series"): return "show"
    if x in ("episode", "episodes"): return "episode"
    return x or "movie"

def _clamp_ts(ts: Any) -> Any:
    # Normalize to UTC ISO8601 (best-effort).
    if not isinstance(ts, str) or not ts: return ts
    s = ts.strip()
    try:
        s2 = s.replace("Z", "+00:00") if s.endswith("Z") else s
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        s = s.split(".", 1)[0]
        s = (s[:19] if len(s) >= 19 else s)
        return (s + "Z") if not s.endswith("Z") else s

def _norm_ids(ids: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if isinstance(ids, Mapping):
        for k in ("imdb", "tmdb", "tvdb", "trakt", "slug", "guid", "plex"):
            v = ids.get(k)
            if v is not None and str(v).strip():
                out[k] = str(v).strip()
    return out  # don't persist provider-local jellyfin id in shadows

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
        r = _norm_row(v)
        out[canonical_key(r)] = r
    return out

# --- config -------------------------------------------------------------------
def _cfg_jf(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    jf = dict((cfg or {}).get("jellyfin") or {})
    auth = dict((cfg or {}).get("auth") or {})
    legacy = dict(auth.get("jellyfin") or {})
    jf.setdefault("server", legacy.get("server"))
    jf.setdefault("access_token", legacy.get("access_token"))
    jf.setdefault("user_id", legacy.get("user_id"))
    jf.setdefault("device_id", (jf.get("device_id") or "crosswatch"))
    jf.setdefault("verify_ssl", True)
    jf["unresolved"] = _cfg_unresolved(jf)
    wl = dict(jf.get("watchlist") or {})
    wl.setdefault("mode", "favorites")
    wl.setdefault("playlist_name", "Watchlist")
    jf["watchlist"] = wl
    return jf

def _validate_cfg(jf: Mapping[str, Any]) -> None:
    srv = (jf.get("server") or "").strip()
    tok = (jf.get("access_token") or "").strip()
    uid = (jf.get("user_id") or "").strip()
    if not srv or not tok or not uid:
        raise RuntimeError("Jellyfin config invalid: server, access_token, user_id are required")

# --- Jellyfin client (requests-only) -----------------------------------------
class JFClient:
    """Tiny HTTP wrapper. All ops via requests. No apiclient required."""
    def __init__(self, server: str, token: str, device_id: Optional[str] = None, verify_ssl: bool = True):
        base = (server or "").rstrip("/")
        if not base: raise RuntimeError("Jellyfin server missing")
        token = (token or "").strip()
        if not token: raise RuntimeError("Jellyfin access_token missing")

        self.base = base
        self.token = token
        self.device_id = device_id or "crosswatch"

        self.s = requests.Session()
        self.s.verify = bool(verify_ssl)
        auth_val = (f'MediaBrowser Client="CrossWatch", Device="CrossWatch", '
                    f'DeviceId="{self.device_id}", Version="{__VERSION__}", Token="{self.token}"')
        self.s.headers.update({
            "Accept": "application/json",
            "User-Agent": f"CrossWatch/{__VERSION__}",
            "Authorization": auth_val,
            "X-Emby-Authorization": auth_val,
            "X-MediaBrowser-Token": self.token,
        })

    def _url(self, path: str) -> str:
        return self.base + (path if path.startswith("/") else ("/" + path))

    def request(self, method: str, path: str, params: Optional[dict]=None,
                json_payload: Any=None, ok=(200,204)) -> tuple[int, Any]:
        url = self._url(path)
        bytes_out = 0
        if json_payload is not None:
            try: bytes_out = len(json.dumps(json_payload).encode("utf-8"))
            except Exception: bytes_out = 0
        try:
            r = self.s.request(method, url, params=params, json=json_payload, timeout=30)
            _record_http_stat(method, url, r, bytes_out)
            st = int(getattr(r, "status_code", 0) or 0)
            try: data = r.json()
            except Exception: data = None
            return st, data
        except requests.RequestException:
            _record_http_stat(method, url, None, bytes_out)
            return 0, None

    def get(self, path: str, params: Optional[dict]=None):  return self.request("GET", path, params=params)
    def post(self, path: str, params: Optional[dict]=None, json_payload: Any=None, ok=(200,204)): return self.request("POST", path, params=params, json_payload=json_payload, ok=ok)
    def put(self, path: str, params: Optional[dict]=None, json_payload: Any=None, ok=(200,204)):  return self.request("PUT", path, params=params, json_payload=json_payload, ok=ok)
    def delete(self, path: str, params: Optional[dict]=None, ok=(200,204)): return self.request("DELETE", path, params=params, ok=ok)

# --- mapping helpers ----------------------------------------------------------
def _ids_from_provider_ids(pids: Mapping[str, Any]) -> Dict[str, str]:
    """
    Normalize ProviderIds from Jellyfin into portable ids:
      - IMDb -> tt########
      - TMDB -> digits only
      - TVDB -> digits only
    """
    out: Dict[str, str] = {}
    if not isinstance(pids, Mapping):
        return out

    # IMDb
    v = pids.get("Imdb")
    if v is not None:
        m = _PAT_IMDB.search(str(v).strip())
        if m:
            out["imdb"] = f"tt{m.group(1)}"

    # TMDB
    v = pids.get("Tmdb")
    if v is not None:
        s = str(v).strip()
        m = _PAT_NUM.search(s)
        if m:
            out["tmdb"] = m.group(1)

    # TVDB
    v = pids.get("Tvdb")
    if v is not None:
        s = str(v).strip()
        m = _PAT_NUM.search(s)
        if m:
            out["tvdb"] = m.group(1)

    return out

def _add_row_all_keys(dst: Dict[str, Dict[str, Any]], row: Mapping[str, Any]) -> None:
    """Index this row under tmdb/imdb/tvdb keys when available; else fallback canonical."""
    nr = _norm_row(row)
    ids = dict(nr.get("ids") or {})
    added = False
    for k in ("tmdb", "imdb", "tvdb"):
        v = ids.get(k)
        if v is not None and str(v).strip():
            dst[f"{k}:{str(v).strip().lower()}"] = nr
            added = True
    if not added:
        dst[canonical_key(nr)] = nr
        
def _canon_type(kind: str) -> str:
    k = (kind or "").lower()
    if k == "series":  return "show"
    if k == "movie":   return "movie"
    if k == "episode": return "episode"
    return k or "movie"

def _row_rating(item: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    typ = _canon_type(item.get("Type") or item.get("BaseItemKind") or "")
    title = (item.get("Name") or "").strip()
    year = item.get("ProductionYear") if isinstance(item.get("ProductionYear"), int) else None
    ids  = _ids_from_provider_ids(item.get("ProviderIds") or {})

    ud = item.get("UserData") or {}
    if not isinstance(ud, Mapping): 
        return None
    if ud.get("Rating") is None:
        return None
    try:
        rating = float(ud["Rating"])
    except Exception:
        return None

    row: Dict[str, Any] = {
        "type": typ,
        "title": title or None,
        "year": year,
        "ids": {k: v for k, v in ids.items() if v},
        "rating": rating,
        "rated_at": None,  # Jellyfin doesn't expose a reliable "last rated" timestamp
    }

    if typ == "episode":
        # help canonical_key build stable fallback keys
        series_t = (item.get("SeriesName") or item.get("Series") or item.get("SeriesTitle") or "").strip()
        if series_t:
            row["series_title"] = series_t
        s = (item.get("ParentIndexNumber") or item.get("SeasonIndexNumber") or
             item.get("ParentIndex") or item.get("SeasonNumber"))
        e = (item.get("IndexNumber") or item.get("EpisodeIndexNumber") or
             item.get("EpisodeNumber"))
        if isinstance(s, int) or (isinstance(s, str) and s.isdigit()):
            row["season"] = int(s)
        if isinstance(e, int) or (isinstance(e, str) and e.isdigit()):
            row["episode"] = int(e)

    return row


def _row_history(item: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    typ = _canon_type(item.get("Type") or item.get("BaseItemKind") or "")
    if typ not in ("movie", "episode"):
        return None

    title = (item.get("Name") or "").strip()
    year  = item.get("ProductionYear") if isinstance(item.get("ProductionYear"), int) else None
    ids   = _ids_from_provider_ids(item.get("ProviderIds") or {})

    ud = item.get("UserData") or {}
    if not isinstance(ud, Mapping):
        return None
    if not ud.get("Played") and not ud.get("PlayCount") and not ud.get("LastPlayedDate"):
        return None

    # prefer UserData timestamp; fall back to top-level field if present
    watched_at = (ud.get("LastPlayedDate")
                  if isinstance(ud.get("LastPlayedDate"), str)
                  else (item.get("DateLastPlayed") if isinstance(item.get("DateLastPlayed"), str) else None))

    row: Dict[str, Any] = {
        "type": typ,
        "title": title or None,
        "year": year,
        "ids": {k: v for k, v in ids.items() if v},
        "watched": True,
        "watched_at": watched_at,
    }

    if typ == "episode":
        series_t = (item.get("SeriesName") or item.get("Series") or item.get("SeriesTitle") or "").strip()
        if series_t:
            row["series_title"] = series_t
        s = (item.get("ParentIndexNumber") or item.get("SeasonIndexNumber") or
             item.get("ParentIndex") or item.get("SeasonNumber"))
        e = (item.get("IndexNumber") or item.get("EpisodeIndexNumber") or
             item.get("EpisodeNumber"))
        if isinstance(s, int) or (isinstance(s, str) and s.isdigit()):
            row["season"] = int(s)
        if isinstance(e, int) or (isinstance(e, str) and e.isdigit()):
            row["episode"] = int(e)

    return row

def _fields(include_ud: bool = False) -> str:
    base = ["ProviderIds", "ProductionYear"]
    if include_ud:
        base.append("UserData")
    return ",".join(base)

def _page_items(http: JFClient, path: str, params: dict, page_size: int = 500):
    start = 0
    while True:
        q = dict(params); q["startIndex"] = start; q["limit"] = page_size
        st, obj = http.get(path, q)
        if st != 200 or not isinstance(obj, Mapping):
            break
        arr = obj.get("Items") or obj.get("items") or []
        if not isinstance(arr, list): arr = []
        for it in arr:
            yield it
        if len(arr) < page_size: break
        start += page_size

def _jf_items_played(http: JFClient, uid: str, *, fields: str) -> List[Mapping[str, Any]]:
    rows: List[Mapping[str, Any]] = []
    for it in _page_items(http, f"/Users/{uid}/Items", {
        "recursive": True,
        "includeItemTypes": "Movie,Episode",
        "Fields": fields,
        "EnableUserData": True,
        "Filters": "IsPlayed",
        "SortBy": "DatePlayed", "SortOrder": "Descending",
    }):
        rows.append(it)
    return rows


# --- resolver ----------------------------------------------------------------
class Resolver:
    """Resolve CrossWatch item -> Jellyfin item id (strict-by-ID; no fuzzy if IDs exist)."""
    def __init__(self, http: JFClient, user_id: str):
        self.http = http
        self.user_id = user_id

    def _id_pairs(self, want_ids: Mapping[str, Any]) -> List[str]:
        # normalize ids first so queries match what Jellyfin stores
        norm: Dict[str, Optional[str]] = {"imdb": None, "tmdb": None, "tvdb": None}
        if isinstance(want_ids, Mapping):
            if want_ids.get("imdb"):
                m = _PAT_IMDB.search(str(want_ids["imdb"]).strip())
                if m: norm["imdb"] = f"tt{m.group(1)}"
            if want_ids.get("tmdb"):
                m = _PAT_NUM.search(str(want_ids["tmdb"]).strip())
                if m: norm["tmdb"] = m.group(1)
            if want_ids.get("tvdb"):
                m = _PAT_NUM.search(str(want_ids["tvdb"]).strip())
                if m: norm["tvdb"] = m.group(1)

        pairs: List[str] = []
        if norm["imdb"]: pairs.append(f"imdb.{norm['imdb']}")
        if norm["tmdb"]: pairs.append(f"tmdb.{norm['tmdb']}")
        if norm["tvdb"]: pairs.append(f"tvdb.{norm['tvdb']}")
        return pairs

    def _types_for(self, cw_type: str) -> list[str]:
        t = (cw_type or "").lower()
        if t == "movie":
            return ["Movie"]
        if t in ("show", "series"):
            return ["Series"]
        if t == "episode":
            return ["Episode"]
        return ["Movie", "Series", "Episode"]

    def _by_provider_ids(self, cw_item: Mapping[str, Any]) -> List[dict]:
        pairs = self._id_pairs(cw_item.get("ids") or {})
        if not pairs:
            return []
        want_types = self._types_for(cw_item.get("type") or "")
        want_set = set(want_types)
        rows: List[dict] = []
        for pr in pairs:
            q = {
                "recursive": True,
                "includeItemTypes": ",".join(want_types),
                "Fields": "ProviderIds,ProductionYear",
                "userId": self.user_id,
                "AnyProviderIdEquals": pr,
            }
            rows.extend(_page_items(self.http, "/Items", q))
        seen, out = set(), []
        for it in rows:
            typ = (it.get("Type") or it.get("BaseItemKind"))
            if typ not in want_set:
                continue
            iid = it.get("Id")
            if iid and iid not in seen:
                seen.add(iid)
                out.append(it)
        return out

    def _search(self, title: str, year: Optional[int], types: List[str]) -> List[dict]:
        if not title:
            return []
        q = {
            "userId": self.user_id,
            "includeItemTypes": ",".join(types),
            "recursive": True,
            "searchTerm": title,
            "Fields": _fields(True),
        }
        if year:
            q["Years"] = str(year)
        return [it for it in _page_items(self.http, "/Items", q)]

    def _match_by_ids(self, cands: List[dict], want_ids: Mapping[str, Any]) -> Optional[dict]:
        want = {k: str(v) for k, v in (want_ids or {}).items() if v}
        for it in cands:
            got = _ids_from_provider_ids(it.get("ProviderIds") or {})
            # also pass through jellyfin internal id for strict equality
            if it.get("Id"):
                got["jellyfin"] = str(it.get("Id"))
            for k in ("guid", "imdb", "tmdb", "tvdb", "slug", "jellyfin"):
                if want.get(k) and got.get(k) == want.get(k):
                    return it
        return None

    def _has_strong_ids(self, ids: Mapping[str, Any]) -> bool:
        return any((ids or {}).get(k) for k in ("guid", "imdb", "tmdb", "tvdb", "slug"))

    def resolve(self, cw_item: Mapping[str, Any]) -> Optional[str]:
        ids = dict((cw_item or {}).get("ids") or {})
        if ids.get("jellyfin"):
            return str(ids["jellyfin"])

        want_types = self._types_for(cw_item.get("type") or "")
        strong_ids = self._has_strong_ids(ids)

        # 1) Strict by provider IDs (no fuzzy fallback if any strong IDs exist)
        cands = self._by_provider_ids(cw_item)
        if cands:
            best = self._match_by_ids(cands, ids)
            if best:
                return str(best.get("Id") or "") or None
            if strong_ids:
                # IDs exist but no exact provider-id match -> do NOT fallback
                return None

        # 2) No strong IDs -> allow title/year search (still typed)
        if strong_ids:
            return None

        title = (cw_item.get("title") or "").strip()
        year = cw_item.get("year")
        cands = self._search(title, year, want_types)
        if not cands:
            return None

        best = self._match_by_ids(cands, ids)
        if best:
            return str(best.get("Id") or "") or None

        ttl_lc = title.lower()
        for it in cands:
            if (it.get("Name") or "").strip().lower() == ttl_lc:
                y = it.get("ProductionYear")
                if (year is None) or (isinstance(y, int) and int(y) == int(year)):
                    return str(it.get("Id") or "") or None

        for it in cands:
            if (it.get("Type") or it.get("BaseItemKind")) in set(want_types):
                return str(it.get("Id") or "") or None

        return str(cands[0].get("Id") or "") or None


# --- provider protocol --------------------------------------------------------
class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]: ...
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...

# --- playlists ---------------------------------------------------------------
def _find_playlist_id(http: JFClient, uid: str, name: str) -> Optional[str]:
    norm = (name or "").strip()
    if not norm: return None
    for p in _page_items(http, f"/Users/{uid}/Items",
                         {"includeItemTypes": "Playlist", "recursive": False, "Fields": "DateCreated"}):
        if (p.get("Name") or "").strip().lower() == norm.lower():
            pid = str(p.get("Id") or "")
            if pid: return pid
    return None

# --- indices + unresolved merge ----------------------------------------------
def _virtual_row_from_entry(ckey: str, ent: Mapping[str, Any]) -> Dict[str, Any]:
    row = _make_virtual_row_from_ckey(ckey, ent.get("type"))
    hint = ent.get("hint")
    if isinstance(hint, Mapping):
        # copy a few safe fields so snapshots look stable
        for k in ("title", "year", "ids", "rating", "watched_at"):
            if k in hint and hint[k] is not None:
                row[k] = hint[k]
    # history virtuals should look watched
    if (ent.get("feature") == "history") and ("watched" not in row):
        row["watched"] = True
    return _norm_row(row)

def _merge_unresolved_virtuals(dst_map: Dict[str, Dict[str, Any]], *, feature: str, jf_cfg: Mapping[str, Any]) -> Dict[str, int]:
    cfg = jf_cfg.get("unresolved") or {}
    policy = str(cfg.get("policy") or "backoff")
    store = _unresolved_load()
    items = dict(store["items"])
    now = _utc_now()
    suppressed = 0
    retried = 0

    # purge TTL first
    purged = _unresolved_purge(cfg)

    changed = False
    for ckey, ent in list(items.items()):
        if str(ent.get("feature")) != feature:
            continue
        nd = _parse_iso(ent.get("next_due_at"))
        if policy == "virtual":
            # Always treat unresolved as present until TTL
            dst_map[ckey] = _virtual_row_from_entry(ckey, ent)
            suppressed += 1
            continue
        # backoff policy
        if nd and nd > now:
            dst_map[ckey] = _virtual_row_from_entry(ckey, ent)
            suppressed += 1
        else:
            # Due: drop from unresolved so diff will retry
            items.pop(ckey, None)
            changed = True
            retried += 1

    if changed or purged:
        _unresolved_save(items)

    # stats per reason/type for debug
    stats = _unresolved_stats_by_reason_and_type(items)
    _emit("jellyfin.index.debug", {
        "feature": feature,
        "unresolved_suppressed": suppressed,
        "unresolved_retried": retried,
        "unresolved_purged": purged,
        "unresolved_stats": stats,
    })
    return {"suppressed": suppressed, "retried": retried, "purged": purged}

def _fingerprint_changed(prev: Mapping[str, Any], count_now: int) -> bool:
    try:
        prev_count = int(prev.get("count") or 0)
    except Exception:
        prev_count = 0
    delta = max(0, count_now - prev_count)
    threshold = max(25, int(prev_count * 0.05))  # 25 items or 5%
    return delta >= threshold

def _reset_unresolved_on_growth(*, feature: str, types: set[str]) -> int:
    store = _unresolved_load()
    items = dict(store["items"])
    n = 0
    changed = False
    # Reset (remove) only those that failed to resolve previously; cap to avoid stampede.
    for k, v in list(items.items()):
        if str(v.get("feature")) != feature:
            continue
        typ = str(v.get("type") or "")
        if types and typ not in types:
            continue
        if str(v.get("reason")) not in ("resolve_failed", "write_failed"):
            continue
        items.pop(k, None)
        n += 1
        changed = True
        if n >= 500:
            break
    if changed:
        _unresolved_save(items)
    return n

def _watchlist_key(ids: Mapping[str, Any], typ: Optional[str], title: Optional[str], year: Optional[int]) -> str:

    for k in ("imdb", "tmdb", "tvdb", "trakt", "guid", "slug"):
        v = (ids or {}).get(k)
        if v is not None and str(v).strip():
            return f"{k}:{str(v).strip()}".lower()
    t = (typ or "").lower()
    ttl = (title or "").strip().lower()
    yr = year if isinstance(year, int) else (int(year) if (isinstance(year, str) and year.isdigit()) else "")
    return f"{t}|title:{ttl}|year:{yr}"

def _unresolved_migrate_watchlist_episode_to_playlists() -> None:
    """
    Cleanup old unresolved entries that were misfiled as watchlist+episode.
    We just drop them so they don't keep suppressing diffs.
    """
    try:
        store = _unresolved_load()
        items = dict(store.get("items") or {})
        changed = False
        for k, v in list(items.items()):
            if str(v.get("feature")) == "watchlist" and str(v.get("type")) == "episode":
                items.pop(k, None)
                changed = True
        if changed:
            _unresolved_save(items)
    except Exception:
        # never let housekeeping break indexing
        pass

def _idx_watchlist(http: JFClient, cfg: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    uid = cfg.get("user_id") or ""
    if not uid:
        return {}
    mode = (cfg.get("watchlist", {}).get("mode") or "favorites").lower()
    out: Dict[str, Dict[str, Any]] = {}

    def has_cross_id(ids: Mapping[str, Any]) -> bool:
        return any((ids or {}).get(k) for k in ("imdb", "tmdb", "tvdb", "trakt"))

    # clean up old misfiled unresolved entries (see fix #2)
    try:
        _unresolved_migrate_watchlist_episode_to_playlists()
    except Exception:
        pass

    if mode == "favorites":
        params = {
            "includeItemTypes": "Movie,Series",
            "recursive": True,
            "Fields": _fields(True),
            "EnableUserData": True,
            "Filters": "IsFavorite",
            "sortBy": "DateLastSaved",
            "sortOrder": "Descending",
        }
        items = [it for it in _page_items(http, f"/Users/{uid}/Items", params)]
        for it in items:
            typ = _canon_type(it.get("Type") or it.get("BaseItemKind") or "")
            title = (it.get("Name") or "").strip()
            year = it.get("ProductionYear") if isinstance(it.get("ProductionYear"), int) else None
            ids = _ids_from_provider_ids(it.get("ProviderIds") or {})
            if not has_cross_id(ids):
                continue
            nr = _norm_row({"type": typ, "title": title or None, "year": year, "ids": ids})
            out[canonical_key(nr)] = nr

        fp_scope = "watchlist_favorites_fp"
        prev = _cursor_load(cfg["server"], uid, fp_scope)
        _cursor_save(cfg["server"], uid, fp_scope, {"count": len(items), "ts": _now_iso()})
        if _fingerprint_changed(prev, len(items)):
            reset = _reset_unresolved_on_growth(feature="watchlist", types={"movie", "show"})
            _emit("jellyfin.index.debug", {"feature": "watchlist", "growth_reset": reset})

        _merge_unresolved_virtuals(out, feature="watchlist", jf_cfg=cfg)
        _cursor_save(cfg["server"], uid, "watchlist_favorites", {"count": len(items)})
        return out

    # playlist mode
    name = (cfg.get("watchlist", {}).get("playlist_name") or "Watchlist").strip() or "Watchlist"
    pl_id = _find_playlist_id(http, uid, name)
    items: List[Mapping[str, Any]] = []
    if pl_id:
        items = [it for it in _page_items(http, f"/Playlists/{pl_id}/Items", {"userId": uid, "Fields": _fields(True)})]
        for it in items:
            typ = _canon_type(it.get("Type") or it.get("BaseItemKind") or "")
            title = (it.get("Name") or "").strip()
            year = it.get("ProductionYear") if isinstance(it.get("ProductionYear"), int) else None
            ids = _ids_from_provider_ids(it.get("ProviderIds") or {})
            if not has_cross_id(ids):
                continue
            nr = _norm_row({"type": typ, "title": title or None, "year": year, "ids": ids})
            out[canonical_key(nr)] = nr
    else:
        _cursor_save(cfg["server"], uid, "watchlist_playlist", {"count": 0, "playlist": name})

    fp_scope = "watchlist_playlist_fp"
    prev = _cursor_load(cfg["server"], uid, fp_scope)
    _cursor_save(cfg["server"], uid, fp_scope, {"count": len(items), "ts": _now_iso(), "playlist": name})
    if _fingerprint_changed(prev, len(items)):
        reset = _reset_unresolved_on_growth(feature="watchlist", types={"movie", "show"})
        _emit("jellyfin.index.debug", {"feature": "watchlist", "growth_reset": reset})

    _merge_unresolved_virtuals(out, feature="watchlist", jf_cfg=cfg)
    _cursor_save(cfg["server"], uid, "watchlist_playlist", {"count": len(items), "playlist": name})
    return out

def _idx_ratings(http: JFClient, cfg: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    uid = cfg.get("user_id") or ""
    if not uid: return {}
    rows: List[dict] = []
    for it in _page_items(http, "/Items", {
        "userId": uid, "recursive": True,
        "includeItemTypes":"Movie,Series,Episode",
        "Fields": _fields(True), "EnableUserData": True,
        "sortBy":"DateLastSaved", "sortOrder":"Descending"
    }):
        r = _row_rating(it)
        if r: rows.append(r)

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        nr = _norm_row(r)
        out[canonical_key(nr)] = nr

    # fingerprint + reset
    fp_scope = "ratings_fp"
    prev = _cursor_load(cfg["server"], uid, fp_scope)
    _cursor_save(cfg["server"], uid, fp_scope, {"count": len(out), "ts": _now_iso()})
    if _fingerprint_changed(prev, len(out)):
        reset = _reset_unresolved_on_growth(feature="ratings", types={"movie","show","episode"})
        _emit("jellyfin.index.debug", {"feature":"ratings","growth_reset": reset})

    _merge_unresolved_virtuals(out, feature="ratings", jf_cfg=cfg)
    _cursor_save(cfg["server"], uid, "ratings", {"count": len(rows)})
    return out

def _idx_history(http: JFClient, cfg: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    uid = cfg.get("user_id") or ""
    if not uid: return {}
    live = _jf_items_played(http, uid, fields=_fields(True))
    live_map: Dict[str, Dict[str, Any]] = {}
    for it in live:
        r = _row_history(it)
        if not r: continue
        nr = _norm_row(r)
        live_map[canonical_key(nr)] = nr

    # fingerprint + reset on growth
    fp_scope = "history_fp"
    prev = _cursor_load(cfg["server"], uid, fp_scope)
    _cursor_save(cfg["server"], uid, fp_scope, {"count": len(live_map), "ts": _now_iso()})
    if _fingerprint_changed(prev, len(live_map)):
        reset = _reset_unresolved_on_growth(feature="history", types={"movie","episode"})
        _emit("jellyfin.index.debug", {"feature":"history","growth_reset": reset})

    stats = _merge_unresolved_virtuals(live_map, feature="history", jf_cfg=cfg)

    # also keep the rolling shadow (successful writes already merged there)
    sh = _history_shadow_load()
    sh_items = _normalize_shadow_items((sh.get("items") if isinstance(sh, Mapping) else {}) or {})
    merged = dict(sh_items); merged.update(live_map)
    _history_shadow_save(merged)

    _cursor_save(cfg["server"], uid, "history", {"count": len(live)})
    return merged

def _idx_playlists(http: JFClient, cfg: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    uid = cfg.get("user_id") or ""
    if not uid: return {}
    _cursor_save(cfg["server"], uid, "playlists", {"count": 0})
    return {}

# --- events -------------------------------------------------------------------
def _emit(name: str, data: dict) -> None:
    # always send to host logger if present
    try:
        host_log(name, data)
    except Exception:
        pass
    # only echo debug events to console when runtime.debug is true
    try:
        if ".debug" in name and _RUNTIME_DEBUG:
            ev = {"event": "log", "provider": "JELLYFIN"}
            ev.update(data)
            print(json.dumps(ev, separators=(",", ":")), flush=True)
    except Exception:
        pass

# --- writers (apply) ----------------------------------------------------------
def _update_userdata(http: JFClient, *, uid: str, item_id: str, payload: Dict[str, Any]) -> bool:
    st, _ = http.post(f"/Items/{item_id}/UserData", params={"userId": uid}, json_payload=payload, ok=(200,204))
    return st in (200,204)

def _favorite(http: JFClient, *, uid: str, item_id: str, flag: bool) -> bool:
    # 1) dedicated endpoints
    try:
        if flag:
            st, _ = http.post(f"/Users/{uid}/FavoriteItems/{item_id}", json_payload=None, ok=(200,204))
        else:
            st, _ = http.delete(f"/Users/{uid}/FavoriteItems/{item_id}", ok=(200,204))
        if st in (200,204):
            # verify
            st2, obj = http.get(
                f"/Users/{uid}/Items",
                params={"Ids": item_id, "Fields": "UserData", "EnableUserData": True},
            )
            if st2 == 200 and isinstance(obj, dict):
                arr = obj.get("Items") or obj.get("items") or []
                if arr:
                    isfav = bool(((arr[0].get("UserData") or {}).get("IsFavorite")))
                    return isfav if flag else (not isfav)
    except Exception:
        pass

    # 2) fallback to UserData (older servers)
    if _update_userdata(http, uid=uid, item_id=item_id, payload={"IsFavorite": bool(flag)}):
        # verify best-effort
        st2, obj = http.get(
            f"/Users/{uid}/Items",
            params={"Ids": item_id, "Fields": "UserData", "EnableUserData": True},
        )
        if st2 == 200 and isinstance(obj, dict):
            arr = obj.get("Items") or obj.get("items") or []
            if arr:
                isfav = bool(((arr[0].get("UserData") or {}).get("IsFavorite")))
                return isfav if flag else (not isfav)
        # if we can't verify but HTTP said OK, accept
        return True
    return False

def _to_jf_rating(v) -> Optional[float]:
    try: x = float(v)
    except Exception: return None
    if 0.0 <= x <= 5.0: x *= 2.0
    x = max(0.0, min(10.0, x))
    return round(x, 1)

def _rate(http: JFClient, *, uid: str, item_id: str, rating: Optional[float]) -> bool:
    if rating is None:
        st,_ = http.delete(f"/Users/{uid}/UserRating/{item_id}")
        return st in (200,204)
    if _update_userdata(http, uid=uid, item_id=item_id, payload={"Rating": rating}): return True
    st,_ = http.post(f"/Users/{uid}/UserRating/{item_id}", params={"rating": rating}, json_payload=None)
    return st in (200,204)

def _played(http: JFClient, *, uid: str, item_id: str, played: bool, when_iso: Optional[str]=None) -> bool:
    payload: Dict[str, Any] = {"Played": bool(played)}
    if when_iso: payload["LastPlayedDate"] = when_iso
    if _update_userdata(http, uid=uid, item_id=item_id, payload=payload): return True
    if played:
        params = ({"datePlayed": when_iso} if when_iso else None)
        st,_ = http.post(f"/Users/{uid}/PlayedItems/{item_id}", params=params, json_payload=None)
        return st in (200,204)
    st,_ = http.delete(f"/Users/{uid}/PlayedItems/{item_id}")
    return st in (200,204)

# --- playlists: ensure/add/remove --------------------------------------------
def _ensure_playlist(http: JFClient, *, uid: str, name: str) -> Optional[str]:
    norm = (name or "").strip() or "Watchlist"
    pid = _find_playlist_id(http, uid, norm)
    if pid:
        return pid
    # create via JSON body
    st, obj = http.post("/Playlists", json_payload={"Name": norm, "UserId": uid, "MediaType": "Video"}, ok=(200,201))
    if st in (200,201) and isinstance(obj, Mapping):
        pid = str(obj.get("Id") or obj.get("id") or "")
        if pid:
            return pid
    # some servers accept query-style creation
    st2, _ = http.post("/Playlists", params={"name": norm, "userId": uid}, json_payload=None, ok=(200,201,204))
    if st2 in (200,201,204):
        return _find_playlist_id(http, uid, norm)
    # final re-scan
    return _find_playlist_id(http, uid, norm)

def _playlist_add(http: JFClient, *, uid: str, playlist_id: str, item_ids: List[str]) -> bool:
    if not item_ids:
        return True
    st, _ = http.post(f"/Playlists/{playlist_id}/Items", params={"userId": uid, "ids": ",".join(item_ids)}, json_payload=None)
    if st in (200,204):
        return True
    st2, _ = http.post(f"/Playlists/{playlist_id}/Items", json_payload={"Ids": item_ids}, ok=(200,204))
    return st2 in (200,204)

def _playlist_remove(http: JFClient, *, uid: str, playlist_id: str, item_ids: List[str]) -> bool:
    if not item_ids:
        return True
    rev: Dict[str, List[str]] = {}
    for it in _page_items(http, f"/Playlists/{playlist_id}/Items", {"userId": uid}):
        mid = str(it.get("Id") or "")
        eid = str(it.get("PlaylistItemId") or "") or mid
        if mid:
            rev.setdefault(mid, []).append(eid)
    entry_ids: List[str] = []
    for mid in item_ids:
        entry_ids += rev.get(mid, [])
    if entry_ids:
        st, _ = http.delete(f"/Playlists/{playlist_id}/Items", params={"entryIds": ",".join(entry_ids)})
        if st in (200,204):
            return True
    st2, _ = http.delete(f"/Playlists/{playlist_id}/Items", params={"ids": ",".join(item_ids)})
    return st2 in (200,204)

# --- adapter ------------------------------------------------------------------
class _JellyfinOPS:
    def name(self) -> str: return "JELLYFIN"
    def label(self) -> str: return "Jellyfin"
    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}
    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": False,
            "ratings": {"types": {"movies": True, "shows": True, "seasons": False, "episodes": True}, "upsert": True, "unrate": True, "from_date": False},
        }

    def capabilities(self) -> Mapping[str, Any]:
        return {"bidirectional": True, "ratings": {"types": {"movies": True, "shows": True, "episodes": True}, "upsert": True, "unrate": True, "from_date": False}}

    def _http(self, cfg: Mapping[str, Any]) -> JFClient:
        jf = _cfg_jf(cfg); _validate_cfg(jf)
        return JFClient(jf.get("server") or "", jf.get("access_token") or "", jf.get("device_id") or "crosswatch", verify_ssl=bool(jf.get("verify_ssl", True)))

    def _resolver(self, cfg: Mapping[str, Any]) -> Resolver:
        jf = _cfg_jf(cfg); _validate_cfg(jf)
        return Resolver(self._http(cfg), jf.get("user_id") or "")

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        _set_runtime_debug(cfg)
        jf = _cfg_jf(cfg); _validate_cfg(jf)
        http = self._http(cfg); f = (feature or "watchlist").lower()
        try: host_log("build_index", {"provider":"JELLYFIN","feature":f})
        except Exception: pass
        if f == "watchlist": return _idx_watchlist(http, jf)
        if f == "ratings":   return _idx_ratings(http, jf)
        if f == "history":   return _idx_history(http, jf)
        if f == "playlists": return _idx_playlists(http, jf)
        return {}

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        _set_runtime_debug(cfg)
        jf = _cfg_jf(cfg); _validate_cfg(jf)
        http = self._http(cfg); res = self._resolver(cfg)
        uid = jf.get("user_id") or ""; mode = (jf.get("watchlist", {}).get("mode") or "favorites").lower()
        unres_cfg = jf.get("unresolved") or {}

        def _resolve_all(arr):
            out = []
            for it in arr:
                iid = (it.get("ids") or {}).get("jellyfin") or res.resolve(it)
                out.append((it, iid))
            return out

        added, errors = 0, []
        upserts = 0
        f = (feature or "watchlist").lower()
        pairs = list(_resolve_all(items))

        if f == "watchlist":
            if mode == "playlist":
                pl = (jf.get("watchlist", {}).get("playlist_name") or "Watchlist").strip() or "Watchlist"
                if dry_run:
                    ids = [iid for _,iid in pairs if iid]; return {"added": len(ids), "dry_run": True}
                pl_id = _ensure_playlist(http, uid=uid, name=pl)
                if not pl_id: return {"added": 0, "error": f"failed_to_ensure_playlist:{pl}"}
                ids = [iid for _,iid in pairs if iid]
                ok = _playlist_add(http, uid=uid, playlist_id=pl_id, item_ids=ids)
                if ok:
                    for it, iid in pairs:
                        ckey = canonical_key(_norm_row(it))
                        _unresolved_remove_if_present(ckey)
                    return {"added": len(ids)}
                # write failed (bulk): upsert all that had ids
                for it, iid in pairs:
                    ckey = canonical_key(_norm_row(it))
                    if iid:
                        _unresolved_upsert(ckey, feature="watchlist", reason="write_failed", type_hint=it.get("type"), last_error="playlist_add_failed", unres_cfg=unres_cfg, hint=_norm_row(it))
                        upserts += 1
                return {"added": 0, "error": "playlist_add_failed", "unresolved_upserted": upserts}

            for it, iid in pairs:
                ckey = canonical_key(_norm_row(it))
                if not iid:
                    errors.append("resolve_failed")
                    _unresolved_upsert(ckey, feature="watchlist", reason="resolve_failed", type_hint=it.get("type"), last_error=None, unres_cfg=unres_cfg, hint=_norm_row(it))
                    upserts += 1
                    continue
                if dry_run:
                    added += 1
                    continue
                if _favorite(http, uid=uid, item_id=str(iid), flag=True):
                    added += 1
                    _unresolved_remove_if_present(ckey)
                else:
                    errors.append(f"favorite_failed:{iid}")
                    _unresolved_upsert(ckey, feature="watchlist", reason="write_failed", type_hint=it.get("type"), last_error="favorite_failed", unres_cfg=unres_cfg, hint=_norm_row(it))
                    upserts += 1
            out = {"added": added, "unresolved_upserted": upserts}
            return (out | {"errors": errors}) if errors else out

        if f == "ratings":
            total = len(pairs); write_fails = 0; invalids = 0
            succeeded: List[Dict[str, Any]] = []
            for i, (it, iid) in enumerate(pairs, 1):
                ckey = canonical_key(_norm_row(it))
                if not iid:
                    errors.append("resolve_failed")
                    _unresolved_upsert(ckey, feature="ratings", reason="resolve_failed", type_hint=it.get("type"), last_error=None, unres_cfg=unres_cfg, hint=_norm_row(it))
                    upserts += 1
                else:
                    rating = it.get("rating")
                    if rating is None:
                        # nothing to write; just clear unresolved if any
                        _unresolved_remove_if_present(ckey)
                    else:
                        r = _to_jf_rating(rating)
                        if r is None:
                            errors.append(f"invalid_rating:{rating}"); invalids += 1
                        elif dry_run:
                            added += 1
                            succeeded.append({"type": it.get("type"), "title": it.get("title"), "year": it.get("year"), "ids": it.get("ids"), "rating": r, "rated_at": None})
                        else:
                            if _rate(http, uid=uid, item_id=str(iid), rating=r):
                                added += 1
                                _unresolved_remove_if_present(ckey)
                                succeeded.append({"type": it.get("type"), "title": it.get("title"), "year": it.get("year"), "ids": it.get("ids"), "rating": r, "rated_at": None})
                            else:
                                errors.append(f"rating_failed:{iid}"); write_fails += 1
                                _unresolved_upsert(ckey, feature="ratings", reason="write_failed", type_hint=it.get("type"), last_error="rating_failed", unres_cfg=unres_cfg, hint=_norm_row(it))
                                upserts += 1
                if (i % PROGRESS_EVERY) == 0: _emit("jellyfin.ratings.progress", {"done": i, "total": total})
                if (i % THROTTLE_EVERY) == 0:
                    try: time.sleep(0.05)
                    except Exception: pass
            if succeeded and not dry_run:
                sh = _ratings_shadow_load(); items_map = dict((sh or {}).get("items") or {})
                for row in succeeded:
                    nr = _norm_row(row); items_map[canonical_key(nr)] = nr
                _ratings_shadow_save(items_map)
            _emit("jellyfin.ratings.summary", {"total": total, "added": added, "write_failed": write_fails, "invalid": invalids, "unresolved_upserted": upserts})
            out = {"added": added, "unresolved_upserted": upserts}
            return (out | {"errors": errors}) if errors else out

        if f == "history":
            total = len(pairs); write_fails = 0
            succeeded: List[Dict[str, Any]] = []
            for i, (it, iid) in enumerate(pairs, 1):
                ckey = canonical_key(_norm_row(it))
                when = _clamp_ts(it.get("watched_at") or "") or _now_iso()
                if not iid:
                    errors.append("resolve_failed")
                    _unresolved_upsert(ckey, feature="history", reason="resolve_failed", type_hint=it.get("type"), last_error=None, unres_cfg=unres_cfg, hint=_norm_row(it))
                    upserts += 1
                elif dry_run:
                    added += 1
                    succeeded.append({"type": it.get("type"), "title": it.get("title"), "year": it.get("year"),
                                      "ids": it.get("ids"), "watched": True, "watched_at": when})
                else:
                    if _played(http, uid=uid, item_id=str(iid), played=True, when_iso=when):
                        added += 1
                        _unresolved_remove_if_present(ckey)
                        succeeded.append({"type": it.get("type"), "title": it.get("title"), "year": it.get("year"),
                                          "ids": it.get("ids"), "watched": True, "watched_at": when})
                    else:
                        errors.append(f"history_mark_failed:{iid}"); write_fails += 1
                        _unresolved_upsert(ckey, feature="history", reason="write_failed", type_hint=it.get("type"), last_error="history_mark_failed", unres_cfg=unres_cfg, hint=_norm_row(it))
                        upserts += 1
                if (i % PROGRESS_EVERY) == 0: _emit("jellyfin.history.progress", {"done": i, "total": total})
                if (i % THROTTLE_EVERY) == 0:
                    try: time.sleep(0.05)
                    except Exception: pass

            if succeeded and not dry_run:
                sh = _history_shadow_load()
                items_map = _normalize_shadow_items((sh.get("items") if isinstance(sh, Mapping) else {}) or {})
                for row in succeeded:
                    nr = _norm_row(row)
                    items_map[canonical_key(nr)] = nr
                _history_shadow_save(items_map)

            _emit("jellyfin.history.summary", {"total": total, "added": added, "write_failed": write_fails, "unresolved_upserted": upserts})
            out = {"added": added, "unresolved_upserted": upserts}
            return (out | {"errors": errors}) if errors else out

        if f == "playlists":
            total = 0; errs: List[str] = []
            for pl in items:
                name = (pl.get("playlist") or pl.get("title") or "").strip()
                if not name: errs.append("missing_playlist_name"); continue
                pl_id = _ensure_playlist(http, uid=uid, name=name)
                if not pl_id: errs.append(f"failed_to_ensure_playlist:{name}"); continue
                sub_pairs = []
                for sub in (pl.get("items") or []):
                    sub_pairs.append((sub, (sub.get("ids") or {}).get("jellyfin") or res.resolve(sub)))
                ids = [iid for _,iid in sub_pairs if iid]
                if dry_run:
                    total += len(ids); continue
                if not _playlist_add(http, uid=uid, playlist_id=pl_id, item_ids=ids):
                    errs.append(f"playlist_add_failed:{name}")
                    # unresolved tracking for each failed item
                    for sub, iid in sub_pairs:
                        if iid:
                            ckey = canonical_key(_norm_row(sub))
                            _unresolved_upsert(ckey, feature="playlists", reason="write_failed", type_hint=sub.get("type"), last_error="playlist_add_failed", unres_cfg=unres_cfg, hint=_norm_row(sub))
                            upserts += 1
                else:
                    total += len(ids)
                    for sub, _ in sub_pairs:
                        _unresolved_remove_if_present(canonical_key(_norm_row(sub)))
            out = {"added": total, "unresolved_upserted": upserts}
            return (out | {"errors": errs}) if errs else out

        return {"added": 0, "error": f"unknown_feature:{f}"}

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        jf = _cfg_jf(cfg); _validate_cfg(jf)
        _set_runtime_debug(cfg)
        http = self._http(cfg); res = self._resolver(cfg)
        uid = jf.get("user_id") or ""; mode = (jf.get("watchlist", {}).get("mode") or "favorites").lower()
        unres_cfg = jf.get("unresolved") or {}

        def _resolve_all(arr):
            out = []
            for it in arr:
                iid = (it.get("ids") or {}).get("jellyfin") or res.resolve(it)
                out.append((it, iid))
            return out

        removed, errors = 0, []
        upserts = 0
        f = (feature or "watchlist").lower()
        pairs = list(_resolve_all(items))

        if f == "watchlist":
            if mode == "playlist":
                pl = (jf.get("watchlist", {}).get("playlist_name") or "Watchlist").strip() or "Watchlist"
                ids = [iid for _,iid in pairs if iid]
                if dry_run: return {"removed": len(ids), "dry_run": True}
                pl_id = _ensure_playlist(http, uid=uid, name=pl)
                if not pl_id: return {"removed": 0, "error": f"missing_playlist:{pl}"}
                ok = _playlist_remove(http, uid=uid, playlist_id=pl_id, item_ids=ids)
                if ok:
                    for it, _ in pairs:
                        _unresolved_remove_if_present(canonical_key(_norm_row(it)))
                else:
                    # track failed removal as write_failed
                    for it, iid in pairs:
                        if iid:
                            _unresolved_upsert(canonical_key(_norm_row(it)), feature="watchlist", reason="write_failed", type_hint=it.get("type"), last_error="playlist_remove_failed", unres_cfg=unres_cfg, hint=_norm_row(it))
                            upserts += 1
                return {"removed": len(ids) if ok else 0, "unresolved_upserted": upserts} if ok else {"removed": 0, "error": "playlist_remove_failed", "unresolved_upserted": upserts}

            for it, iid in pairs:
                ckey = canonical_key(_norm_row(it))
                if not iid:
                    errors.append("resolve_failed")
                    _unresolved_upsert(ckey, feature="watchlist", reason="resolve_failed", type_hint=it.get("type"), last_error=None, unres_cfg=unres_cfg, hint=_norm_row(it))
                    upserts += 1
                    continue
                if dry_run:
                    removed += 1; continue
                ok = _favorite(http, uid=uid, item_id=str(iid), flag=False)
                if ok:
                    removed += 1
                    _unresolved_remove_if_present(ckey)
                else:
                    errors.append(f"unfavorite_failed:{iid}")
                    _unresolved_upsert(ckey, feature="watchlist", reason="write_failed", type_hint=it.get("type"), last_error="unfavorite_failed", unres_cfg=unres_cfg, hint=_norm_row(it))
                    upserts += 1
            out = {"removed": removed, "unresolved_upserted": upserts}
            return (out | {"errors": errors}) if errors else out

        if f == "ratings":
            total = len(pairs); succeeded_keys: List[str] = []
            for i, (it, iid) in enumerate(pairs, 1):
                ckey = canonical_key(_norm_row(it))
                if not iid:
                    errors.append("resolve_failed")
                    _unresolved_upsert(ckey, feature="ratings", reason="resolve_failed", type_hint=it.get("type"), last_error=None, unres_cfg=unres_cfg, hint=_norm_row(it))
                    upserts += 1
                    continue
                if dry_run:
                    removed += 1; succeeded_keys.append(ckey)
                else:
                    ok = _rate(http, uid=uid, item_id=str(iid), rating=None)
                    if ok:
                        removed += 1; succeeded_keys.append(ckey)
                        _unresolved_remove_if_present(ckey)
                    else:
                        errors.append(f"unrate_failed:{iid}")
                        _unresolved_upsert(ckey, feature="ratings", reason="write_failed", type_hint=it.get("type"), last_error="unrate_failed", unres_cfg=unres_cfg, hint=_norm_row(it))
                        upserts += 1
                if (i % PROGRESS_EVERY) == 0: _emit("jellyfin.ratings.progress", {"done": i, "total": total, "mode": "remove"})
                if (i % THROTTLE_EVERY) == 0:
                    try: time.sleep(0.05)
                    except Exception: pass
            if succeeded_keys and not dry_run:
                sh = _ratings_shadow_load(); items_map = dict((sh or {}).get("items") or {})
                for k in succeeded_keys: items_map.pop(k, None)
                _ratings_shadow_save(items_map)
            out = {"removed": removed, "unresolved_upserted": upserts}
            return (out | {"errors": errors}) if errors else out

        if f == "history":
            total = len(pairs); succeeded_keys: List[str] = []
            for i, (it, iid) in enumerate(pairs, 1):
                ckey = canonical_key(_norm_row(it))
                if not iid:
                    errors.append("resolve_failed")
                    _unresolved_upsert(ckey, feature="history", reason="resolve_failed", type_hint=it.get("type"), last_error=None, unres_cfg=unres_cfg, hint=_norm_row(it))
                    upserts += 1
                    continue
                if dry_run:
                    removed += 1; succeeded_keys.append(ckey)
                else:
                    if _played(http, uid=uid, item_id=str(iid), played=False):
                        removed += 1; succeeded_keys.append(ckey)
                        _unresolved_remove_if_present(ckey)
                    else:
                        errors.append(f"unwatch_failed:{iid}")
                        _unresolved_upsert(ckey, feature="history", reason="write_failed", type_hint=it.get("type"), last_error="unwatch_failed", unres_cfg=unres_cfg, hint=_norm_row(it))
                        upserts += 1
                if (i % PROGRESS_EVERY) == 0: _emit("jellyfin.history.progress", {"done": i, "total": total, "mode": "remove"})
                if (i % THROTTLE_EVERY) == 0:
                    try: time.sleep(0.05)
                    except Exception: pass
            if succeeded_keys and not dry_run:
                sh = _history_shadow_load(); items_map = dict((sh or {}).get("items") or {})
                for k in succeeded_keys: items_map.pop(k, None)
                _history_shadow_save(items_map)
            out = {"removed": removed, "unresolved_upserted": upserts}
            return (out | {"errors": errors}) if errors else out

        if f == "playlists":
            total = 0; errs: List[str] = []
            for pl in items:
                name = (pl.get("playlist") or pl.get("title") or "").strip()
                if not name: errs.append("missing_playlist_name"); continue
                pl_id = _ensure_playlist(http, uid=uid, name=name)
                if not pl_id: errs.append(f"missing_playlist:{name}"); continue
                sub_pairs = [(it, (it.get("ids") or {}).get("jellyfin") or res.resolve(it)) for it in (pl.get("items") or [])]
                ids = [iid for _,iid in sub_pairs if iid]; total += len(ids)
                if dry_run: continue
                if not _playlist_remove(http, uid=uid, playlist_id=pl_id, item_ids=ids):
                    errs.append(f"playlist_remove_failed:{name}")
                    for sub, iid in sub_pairs:
                        if iid:
                            _unresolved_upsert(canonical_key(_norm_row(sub)), feature="playlists", reason="write_failed", type_hint=sub.get("type"), last_error="playlist_remove_failed", unres_cfg=unres_cfg, hint=_norm_row(sub))
                            upserts += 1
                else:
                    for sub, _ in sub_pairs:
                        _unresolved_remove_if_present(canonical_key(_norm_row(sub)))
            out = {"removed": total, "unresolved_upserted": upserts}
            return (out | {"errors": errs}) if errs else out

        return {"removed": 0, "error": f"unknown_feature:{f}"}

# --- export -------------------------------------------------------------------
OPS: InventoryOps = _JellyfinOPS()
_JELLYFINOPS = OPS
_JellyFINOPS = OPS

# --- manifest -----------------------------------------------------------------
try:
    from providers.sync._base import SyncModule, ModuleInfo, ModuleCapabilities  # type: ignore
except Exception:
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

class JELLYFINModule(SyncModule):
    info = ModuleInfo(
        name="JELLYFIN",
        version=__VERSION__,
        description=("Jellyfin connector (requests-only); favorites-or-playlist watchlist, ratings, history. "
                     "Includes unresolved/backoff suppression to keep diffs quiet when libraries mismatch."),
        vendor="community",
        capabilities=ModuleCapabilities(
            supports_dry_run=True, supports_cancel=True, supports_timeout=True, status_stream=True, bidirectional=True,
            config_schema={
                "type": "object",
                "properties": {
                    "jellyfin": {
                        "type": "object",
                        "properties": {
                            "server": {"type": "string", "minLength": 1},
                            "access_token": {"type": "string", "minLength": 1},
                            "user_id": {"type": "string", "minLength": 1},
                            "device_id": {"type": "string"},
                            "verify_ssl": {"type": "boolean", "default": True},
                            "watchlist": {
                                "type": "object",
                                "properties": {
                                    "mode": {"type": "string", "enum": ["favorites", "playlist"], "default": "favorites"},
                                    "playlist_name": {"type": "string", "default": "Watchlist"},
                                },
                                "additionalProperties": False,
                            },
                            "unresolved": {
                                "type": "object",
                                "properties": {
                                    "policy": {"type": "string", "enum": ["backoff","virtual"], "default": "backoff"},
                                    "base_hours": {"type": "integer", "default": 6},
                                    "max_days": {"type": "integer", "default": 30},
                                    "max_retries": {"type": "integer", "default": 8},
                                    "ttl_days": {"type": "integer", "default": 90},
                                },
                                "additionalProperties": False,
                            },
                        },
                        "required": ["server", "access_token", "user_id"],
                        "additionalProperties": False,
                    },
                    "runtime": {
                        "type": "object",
                        "properties": {"debug": {"type": "boolean"}, "state_dir": {"type": "string"}},
                        "additionalProperties": False,
                    },
                },
                "required": ["jellyfin"],
                "additionalProperties": False,
            },
        ),
    )

    @staticmethod
    def supported_features() -> dict:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}

def get_manifest() -> dict:
    return {
        "name": JELLYFINModule.info.name,
        "label": "Jellyfin",
        "features": JELLYFINModule.supported_features(),
        "capabilities": {
            "bidirectional": True,
            "ratings": {"types": {"movies": True, "shows": True, "episodes": True}, "upsert": True, "unrate": True, "from_date": False},
        },
        "version": JELLYFINModule.info.version,
        "vendor": JELLYFINModule.info.vendor,
        "description": JELLYFINModule.info.description,
    }
