from __future__ import annotations
# providers/sync/_mod_JELLYFIN.py

__VERSION__ = "0.1.11"
__all__ = ["OPS", "JELLYFINModule", "get_manifest"]

import json, os, time, random
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple

import requests

PROGRESS_EVERY = 50
THROTTLE_EVERY = 200

# host shims
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
            _stats.record_http(
                provider="JELLYFIN",
                fn=method.upper(),
                path=url,
                status=int(getattr(r, "status_code", 0) or 0),
                ok=bool(getattr(r, "ok", False)),
                bytes_in=len(getattr(r, "content", b"") or b""),
                bytes_out=int(bytes_out or 0),
                ms=int(getattr(r, "elapsed", 0).total_seconds() * 1000) if getattr(r, "elapsed", None) else 0,
            )
        else:
            _stats.record_http(provider="JELLYFIN", fn=method.upper(), path=url, status=0, ok=False, bytes_in=0, bytes_out=int(bytes_out or 0), ms=0)
    except Exception:
        pass

# Global tombstones (optional)
try:
    from _gmt import GlobalTombstoneStore, gmt_is_enabled  # type: ignore
    _HAS_GMT = True
except Exception:
    _HAS_GMT = False
    class GlobalTombstoneStore:  # type: ignore
        def quarantine_ok(self, *a, **k) -> bool: return True
        def remember(self, *a, **k) -> None: pass
    def gmt_is_enabled(cfg: Mapping[str, Any]) -> bool: return True

def _gmt_store_from_cfg(cfg: Mapping[str, Any]) -> Optional[GlobalTombstoneStore]:
    if not _HAS_GMT or not gmt_is_enabled(cfg): return None
    try: return GlobalTombstoneStore()
    except Exception: return None

# state roots
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

def _safe_srv(server: str) -> str:
    return (server or "").replace("://", "_").replace("/", "_")

def _cursor_key(server: str, user_id: str, scope: str) -> str:
    return f"jf__{_safe_srv(server)}__{user_id}__{scope}.json"

def _legacy_file_paths(server: str, user_id: str, scope: str):
    name = _cursor_key(server, user_id, scope)
    yield _state_root() / name
    yield _state_root() / "JELLYFIN" / name

def _cursor_load(server: str, user_id: str, scope: str) -> Dict[str, Any]:
    key = _cursor_key(server, user_id, scope)
    all_cur = _read_json(_cursors_path(), {})
    if isinstance(all_cur, dict) and key in all_cur and isinstance(all_cur[key], dict):
        return dict(all_cur[key])
    for p in _legacy_file_paths(server, user_id, scope):
        data = _read_json(p, {})
        if isinstance(data, dict) and data:
            return data
    return {}

def _cursor_save(server: str, user_id: str, scope: str, data: Mapping[str, Any]) -> None:
    entry = dict(data); entry.setdefault("updated_at", _now_iso())
    key = _cursor_key(server, user_id, scope)
    all_cur = _read_json(_cursors_path(), {})
    if not isinstance(all_cur, dict): all_cur = {}
    all_cur[key] = entry
    _write_json(_cursors_path(), all_cur)

# shadows kept flat in .cw_state
def _shadow_root_path(name: str) -> Path: return _state_root() / name
def _shadow_legacy_path(name: str) -> Path: return _state_root() / "JELLYFIN" / name  # read-only

def _shadow_read(name: str, fallback: Any) -> Any:
    p = _shadow_root_path(name)
    obj = _read_json(p, None)
    if obj is not None: return obj
    return _read_json(_shadow_legacy_path(name), fallback)

def _shadow_write(name: str, data: Any) -> None:
    p = _shadow_root_path(name)
    _write_json(p, data)
    try:
        obj = _read_json(p, None)
        ok = isinstance(obj, dict)
    except Exception:
        ok = False
    if not ok:
        _write_json(_shadow_legacy_path(name), data)

def _ratings_shadow_load() -> Dict[str, Any]:
    return _shadow_read("jellyfin_ratings.shadow.json", {"items": {}, "ts": 0})

def _ratings_shadow_save(items: Mapping[str, Any]) -> None:
    _shadow_write("jellyfin_ratings.shadow.json", {"items": dict(items), "ts": int(time.time())})

def _history_shadow_load() -> Dict[str, Any]:
    return _shadow_read("jellyfin_history.shadow.json", {"items": {}, "ts": 0})

def _history_shadow_save(items: Mapping[str, Any]) -> None:
    _shadow_write("jellyfin_history.shadow.json", {"items": dict(items), "ts": int(time.time())})

# canonical keys
_ID_KEYS = ("imdb", "tmdb", "tvdb", "trakt", "plex", "guid", "slug", "jellyfin")

def canonical_key(item: Mapping[str, Any]) -> str:
    ids = dict(item.get("ids") or {})
    for k in _ID_KEYS:
        v = ids.get(k)
        if v: return f"{k}:{str(v)}".lower()
    t = (item.get("type") or "").lower()
    ttl = str(item.get("title") or "").strip().lower()
    yr = item.get("year") or ""
    return f"{t}|title:{ttl}|year:{yr}"

# normalization
def _norm_type(t: Any) -> str:
    x = (str(t or "")).strip().lower()
    if x in ("movies", "movie"): return "movie"
    if x in ("shows", "show", "series"): return "show"
    if x in ("episode", "episodes"): return "episode"
    return x or "movie"

def _clamp_ts(ts: Any) -> Any:
    if not isinstance(ts, str) or not ts: return ts
    s = ts.strip()
    for sep in ("+", "-"):
        if sep in s[19:]:
            s = s[:s.find(sep)]
            break
    s = s[:-1] if s.endswith("Z") else s
    s = s.split(".", 1)[0]
    if len(s) >= 19: s = s[:19]
    return s + "Z"

def _norm_ids(ids: Any) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if isinstance(ids, Mapping):
        for k in ("imdb", "tmdb", "tvdb", "trakt", "slug", "guid", "plex"):
            v = ids.get(k)
            if v is not None and str(v).strip():
                out[k] = str(v).strip()
    return out  # do not persist provider-local jellyfin id in shadows

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

# config
def _cfg_jf(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    jf = dict((cfg or {}).get("jellyfin") or {})
    auth = dict((cfg or {}).get("auth") or {})
    legacy = dict(auth.get("jellyfin") or {})
    jf.setdefault("server", legacy.get("server"))
    jf.setdefault("access_token", legacy.get("access_token"))
    jf.setdefault("user_id", legacy.get("user_id"))
    jf.setdefault("device_id", (jf.get("device_id") or "crosswatch"))
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

# HTTP
class JFHttp:
    def __init__(self, server: str, token: str, device_id: Optional[str] = None):
        base = (server or "").rstrip("/")
        if not base: raise RuntimeError("Jellyfin server missing")
        token = (token or "").strip()
        if not token: raise RuntimeError("Jellyfin access_token missing")
        self.base = base; self.token = token; self.device_id = device_id or "crosswatch"
        self.s = requests.Session()
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

    def _sleep(self, attempt: int) -> None:
        time.sleep(min(4.0, 0.6*(2**attempt)) + random.uniform(0, 0.25))

    def request(self, method: str, path: str, params: Optional[dict]=None,
                json_payload: Any=None, ok: Tuple[int,...]=(200,204)) -> tuple[int, Any]:
        url = self._url(path)
        body = None; bytes_out = 0
        if json_payload is not None:
            body = json.dumps(json_payload); bytes_out = len(body.encode("utf-8"))
        for attempt in range(0, 4):
            try:
                r = self.s.request(method, url, params=params, data=body,
                                   headers=(None if body is None else {"Content-Type":"application/json"}), timeout=30)
                _record_http_stat(method, url, r, bytes_out)
                st = int(getattr(r, "status_code", 0) or 0)
                if st in ok:
                    try: return st, r.json()
                    except Exception: return st, None
                if st == 429 or (500 <= st < 600):
                    self._sleep(attempt); continue
                try: return st, r.json()
                except Exception: return st, None
            except requests.RequestException:
                _record_http_stat(method, url, None, bytes_out)
                self._sleep(attempt); continue
        return 0, None

    def get(self, path: str, params: Optional[dict]=None):  return self.request("GET", path, params=params)
    def post(self, path: str, params: Optional[dict]=None, json_payload: Any=None, ok: Tuple[int,...]=(200,204)): return self.request("POST", path, params=params, json_payload=json_payload, ok=ok)
    def put(self, path: str, params: Optional[dict]=None, json_payload: Any=None, ok: Tuple[int,...]=(200,204)):  return self.request("PUT", path, params=params, json_payload=json_payload, ok=ok)
    def delete(self, path: str, params: Optional[dict]=None, ok: Tuple[int,...]=(200,204)): return self.request("DELETE", path, params=params, ok=ok)

# mapping helpers
def _ids_from_provider_ids(pids: Mapping[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(pids, Mapping): return out
    for jf_k, cw_k in (("Imdb","imdb"),("Tmdb","tmdb"),("Tvdb","tvdb")):
        v = pids.get(jf_k)
        if isinstance(v, (str,int)) and str(v): out[cw_k] = str(v)
    return out

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
    ids = _ids_from_provider_ids(item.get("ProviderIds") or {})
    if item.get("Id"): ids["jellyfin"] = str(item.get("Id"))
    ud = item.get("UserData") or {}
    if not isinstance(ud, Mapping): return None
    if ud.get("Rating") is None: return None
    row: Dict[str, Any] = {"type": typ, "title": title or None, "year": year, "ids": {k:v for k,v in ids.items() if v}}
    try: row["rating"] = float(ud["Rating"])
    except Exception: return None
    row["rated_at"] = None
    return row

def _row_history(item: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    typ = _canon_type(item.get("Type") or item.get("BaseItemKind") or "")
    if typ not in ("movie", "episode"): return None
    title = (item.get("Name") or "").strip()
    year = item.get("ProductionYear") if isinstance(item.get("ProductionYear"), int) else None
    ids = _ids_from_provider_ids(item.get("ProviderIds") or {})
    if item.get("Id"): ids["jellyfin"] = str(item.get("Id"))
    ud = item.get("UserData") or {}
    if not isinstance(ud, Mapping): return None
    if not ud.get("Played") and not ud.get("PlayCount") and not ud.get("LastPlayedDate"): return None
    watched_at = ud.get("LastPlayedDate") if isinstance(ud.get("LastPlayedDate"), str) else None
    row: Dict[str, Any] = {"type": typ, "title": title or None, "year": year,
                           "ids": {k:v for k,v in ids.items() if v},
                           "watched": True, "watched_at": watched_at}
    return row

def _fields(include_ud: bool=False) -> str:
    base = ["ProviderIds","ProductionYear","DateLastSaved","PrimaryImageAspectRatio"]
    if include_ud: base += ["UserData","DateLastPlayed"]
    return ",".join(base)

def _page_items(http: JFHttp, path: str, params: dict, page_size: int = 500):
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

def _jf_items_played(http: JFHttp, uid: str, *, fields: str) -> List[Mapping[str, Any]]:
    rows: List[Mapping[str, Any]] = []
    for it in _page_items(http, f"/Users/{uid}/Items", {
        "recursive": True,
        "includeItemTypes": "Movie,Episode",
        "fields": fields, "enableUserData": True,
        "Filters": "IsPlayed",
        "sortBy": "DateLastSaved", "sortOrder": "Descending",
    }):
        rows.append(it)
    if not rows:
        for it in _page_items(http, "/Items", {
            "userId": uid, "recursive": True,
            "includeItemTypes": "Movie,Episode",
            "fields": fields, "enableUserData": True,
            "isPlayed": True,
            "sortBy": "DateLastSaved", "sortOrder": "Descending",
        }):
            rows.append(it)
    return rows

# resolver
class Resolver:
    def __init__(self, http: JFHttp, user_id: str):
        self.http = http; self.user_id = user_id

    def _id_pairs(self, want_ids: Mapping[str, Any]) -> List[str]:
        ids = {k:str(v) for k,v in (want_ids or {}).items() if v}
        pairs: List[str] = []
        if ids.get("imdb"): pairs.append(f"imdb.{ids['imdb']}")
        if ids.get("tmdb"): pairs.append(f"tmdb.{ids['tmdb']}")
        if ids.get("tvdb"): pairs.append(f"tvdb.{ids['tvdb']}")
        return pairs

    def _types_for(self, cw_type: str) -> list[str]:
        t = (cw_type or "").lower()
        if t == "movie": return ["Movie"]
        if t in ("show", "series"): return ["Series"]
        if t == "episode": return ["Episode"]
        return ["Movie","Series","Episode"]

    def _by_provider_ids(self, cw_item: Mapping[str, Any]) -> List[dict]:
        pairs = self._id_pairs(cw_item.get("ids") or {})
        if not pairs: return []
        want_types = self._types_for(cw_item.get("type") or "")
        want_set = set(want_types)
        rows: List[dict] = []
        for pr in pairs:
            q = {"recursive": True, "includeItemTypes": ",".join(want_types),
                 "fields": "ProviderIds,ProductionYear", "userId": self.user_id,
                 "AnyProviderIdEquals": pr}
            rows.extend(_page_items(self.http, "/Items", q))
        seen, out = set(), []
        for it in rows:
            if (it.get("Type") or it.get("BaseItemKind")) not in want_set: continue
            iid = it.get("Id")
            if iid and iid not in seen:
                seen.add(iid); out.append(it)
        return out

    def _search(self, title: str, year: Optional[int], types: List[str]) -> List[dict]:
        if not title: return []
        q = {"userId": self.user_id, "includeItemTypes": ",".join(types), "recursive": True,
             "searchTerm": title, "fields": _fields(True)}
        if year: q["years"] = str(year)
        return [it for it in _page_items(self.http, "/Items", q)]

    def _match_by_ids(self, cands: List[dict], want_ids: Mapping[str, Any]) -> Optional[dict]:
        want = {k:str(v) for k,v in (want_ids or {}).items() if v}
        for it in cands:
            got = _ids_from_provider_ids(it.get("ProviderIds") or {})
            for k in ("tmdb","imdb","tvdb"):
                if want.get(k) and got.get(k) == want.get(k):
                    return it
        return None

    def resolve(self, cw_item: Mapping[str, Any]) -> Optional[str]:
        ids = dict((cw_item or {}).get("ids") or {})
        if ids.get("jellyfin"): return str(ids["jellyfin"])

        typ = (cw_item.get("type") or "movie").lower()
        if   typ == "movie":   want_types = ["Movie"]
        elif typ in ("show", "series"): want_types = ["Series"]
        elif typ == "episode": want_types = ["Episode"]
        else:                  want_types = ["Movie","Series","Episode"]

        cands = self._by_provider_ids(cw_item)
        if cands:
            tset = set(want_types)
            cands_typed = [it for it in cands if (it.get("Type") or it.get("BaseItemKind")) in tset]
            pool = cands_typed or cands
            best = self._match_by_ids(pool, ids)
            if best: return str(best.get("Id") or "") or None
            if typ == "episode":
                for it in pool:
                    if (it.get("Type") or it.get("BaseItemKind")) == "Episode":
                        return str(it.get("Id") or "") or None
            if cands_typed: return str(cands_typed[0].get("Id") or "") or None
            if typ != "episode": return str(pool[0].get("Id") or "") or None
            return None

        title = (cw_item.get("title") or "").strip()
        year  = cw_item.get("year")
        cands = self._search(title, year, want_types)
        if not cands: return None

        best = self._match_by_ids(cands, ids)
        if best: return str(best.get("Id") or "") or None

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

# provider protocol
class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]: ...
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...

# playlist discovery helpers (stronger than before)
def _find_playlist_id(http: JFHttp, uid: str, name: str) -> Optional[str]:
    norm = (name or "").strip()
    if not norm: return None

    # A) common
    for p in _page_items(http, f"/Users/{uid}/Items", {"includeItemTypes":"Playlist","recursive":False,"fields":"DateCreated"}):
        if (p.get("Name") or "").strip().lower() == norm.lower():
            pid = str(p.get("Id") or "")
            if pid: return pid

    # B) older servers: Items endpoint
    for p in _page_items(http, "/Items", {"userId":uid,"includeItemTypes":"Playlist","recursive":True}):
        if (p.get("Name") or "").strip().lower() == norm.lower():
            pid = str(p.get("Id") or "")
            if pid: return pid

    # C) via Views: locate Playlists view then search within
    views = [v for v in _page_items(http, f"/Users/{uid}/Views", {})]
    view_id = None
    for v in views:
        nm = (v.get("Name") or "").strip().lower()
        ctype = (v.get("CollectionType") or "").strip().lower()
        if nm == "playlists" or ctype == "playlists":
            view_id = str(v.get("Id") or "")
            if view_id: break
    if view_id:
        for p in _page_items(http, f"/Users/{uid}/Items", {"ParentId": view_id, "includeItemTypes":"Playlist","recursive":False}):
            if (p.get("Name") or "").strip().lower() == norm.lower():
                pid = str(p.get("Id") or "")
                if pid: return pid

    return None

# indices
def _idx_watchlist(http: JFHttp, cfg: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    uid = cfg.get("user_id") or ""
    if not uid: return {}
    mode = (cfg.get("watchlist", {}).get("mode") or "favorites").lower()
    out: Dict[str, Dict[str, Any]] = {}
    if mode == "favorites":
        q1 = {"includeItemTypes":"Movie,Series","recursive":True,"fields":_fields(True),"enableUserData":True,"Filters":"IsFavorite"}
        items = [it for it in _page_items(http, f"/Users/{uid}/Items", q1)]
        if not items:
            q2 = {"userId":uid,"includeItemTypes":"Movie,Series","recursive":True,"fields":_fields(True),"enableUserData":True,"isFavorite":True}
            items = [it for it in _page_items(http, "/Items", q2)]
        for it in items:
            row = _row_rating(it) or {
                "type": _canon_type(it.get("Type") or it.get("BaseItemKind") or ""),
                "title": (it.get("Name") or "").strip() or None,
                "year": it.get("ProductionYear") if isinstance(it.get("ProductionYear"), int) else None,
                "ids": (lambda d: (d.update({"jellyfin": str(it.get("Id"))}) or d) if it.get("Id") else d)(_ids_from_provider_ids(it.get("ProviderIds") or {})),
            }
            nr = _norm_row(row)
            if nr.get("title"): out[canonical_key(nr)] = nr
        _cursor_save(cfg["server"], uid, "watchlist_favorites", {"updated_at": _now_iso(), "count": len(items)})
        return out

    # playlist mode: find playlist robustly
    name = (cfg.get("watchlist", {}).get("playlist_name") or "Watchlist").strip() or "Watchlist"
    pl_id = _find_playlist_id(http, uid, name)
    if not pl_id:
        _cursor_save(cfg["server"], uid, "watchlist_playlist", {"updated_at": _now_iso(), "count": 0, "playlist": name, "note": "missing"})
        return {}

    items = [it for it in _page_items(http, f"/Playlists/{pl_id}/Items", {"userId": uid, "fields": _fields(True)})]
    if not items:
        # older servers sometimes ignore userId here—retry without
        items = [it for it in _page_items(http, f"/Playlists/{pl_id}/Items", {"fields": _fields(True)})]

    for it in items:
        typ = _canon_type(it.get("Type") or it.get("BaseItemKind") or "")
        title = (it.get("Name") or "").strip()
        year = it.get("ProductionYear") if isinstance(it.get("ProductionYear"), int) else None
        ids = _ids_from_provider_ids(it.get("ProviderIds") or {})
        if it.get("Id"): ids["jellyfin"] = str(it.get("Id"))
        nr = _norm_row({"type": typ, "title": title or None, "year": year, "ids": ids})
        if nr.get("title"): out[canonical_key(nr)] = nr

    _cursor_save(cfg["server"], uid, "watchlist_playlist", {"updated_at": _now_iso(), "count": len(items), "playlist": name})
    return out

def _idx_ratings(http: JFHttp, cfg: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    sh = _ratings_shadow_load()
    sh_items = sh.get("items") if isinstance(sh, Mapping) else {}
    if isinstance(sh_items, Mapping) and sh_items:
        return _normalize_shadow_items(sh_items)

    uid = cfg.get("user_id") or ""
    if not uid: return {}
    rows: List[dict] = []
    for it in _page_items(http, "/Items", {
        "userId": uid, "recursive": True,
        "includeItemTypes":"Movie,Series,Episode",
        "fields": _fields(True), "enableUserData": True,
        "sortBy":"DateLastSaved", "sortOrder":"Descending"
    }):
        r = _row_rating(it)
        if r: rows.append(r)
    _cursor_save(cfg["server"], uid, "ratings", {"updated_at": _now_iso(), "count": len(rows)})
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        nr = _norm_row(r)
        out[canonical_key(nr)] = nr
    _ratings_shadow_save(out)
    return out

def _idx_history(http: JFHttp, cfg: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    uid = cfg.get("user_id") or ""
    if not uid: return {}
    live = _jf_items_played(http, uid, fields=_fields(True))
    _cursor_save(cfg["server"], uid, "history", {"updated_at": _now_iso(), "count": len(live)})

    live_map: Dict[str, Dict[str, Any]] = {}
    for it in live:
        r = _row_history(it)
        if not r: continue
        nr = _norm_row(r)
        live_map[canonical_key(nr)] = nr

    sh = _history_shadow_load()
    sh_items = _normalize_shadow_items((sh.get("items") if isinstance(sh, Mapping) else {}) or {})
    merged = dict(sh_items); merged.update(live_map)
    _history_shadow_save(merged)

    try:
        _emit("jellyfin.history.index.debug", {"live": len(live_map), "shadow": len(sh_items), "merged": len(merged)})
    except Exception:
        pass

    return merged

# events
def _emit(name: str, data: dict) -> None:
    try: host_log(name, data)
    except Exception: pass
    try:
        ev_type = "progress" if ".progress" in name else ("summary" if ".summary" in name else "log")
        feature = "history" if ".history." in name else ("ratings" if ".ratings." in name else None)
        ev = {"event": ev_type, "provider": "JELLYFIN"}
        if feature: ev["feature"] = feature
        ev.update(data)
        print(json.dumps(ev, separators=(",",":")), flush=True)
    except Exception:
        pass

# writers (favorites, played, rating, playlist)
def _update_userdata(http: JFHttp, *, uid: str, item_id: str, payload: Dict[str, Any]) -> bool:
    st, _ = http.post(f"/Items/{item_id}/UserData", params={"userId": uid}, json_payload=payload, ok=(200,204))
    if st in (200,204): return True
    st2, _ = http.post(f"/Users/{uid}/Items/{item_id}/UserData", json_payload=payload, ok=(200,204))
    return st2 in (200,204)

def _favorite(http: JFHttp, *, uid: str, item_id: str, flag: bool) -> bool:
    if _update_userdata(http, uid=uid, item_id=item_id, payload={"IsFavorite": bool(flag)}): return True
    if flag:
        st,_ = http.post(f"/Users/{uid}/FavoriteItems/{item_id}"); return st in (200,204)
    st,_ = http.delete(f"/Users/{uid}/FavoriteItems/{item_id}"); return st in (200,204)

def _played(http: JFHttp, *, uid: str, item_id: str, played: bool, when_iso: Optional[str]=None) -> bool:
    payload: Dict[str, Any] = {"Played": bool(played)}
    if when_iso: payload["LastPlayedDate"] = when_iso
    if _update_userdata(http, uid=uid, item_id=item_id, payload=payload): return True
    if played:
        params = ({"datePlayed": when_iso} if when_iso else None)
        st,_ = http.post(f"/Users/{uid}/PlayedItems/{item_id}", params=params, json_payload=None)
        return st in (200,204)
    st,_ = http.delete(f"/Users/{uid}/PlayedItems/{item_id}")
    return st in (200,204)

def _history_shadow_merge(new_items: Mapping[str, Mapping[str, Any]]) -> None:
    sh = _history_shadow_load()
    base = _normalize_shadow_items((sh.get("items") if isinstance(sh, Mapping) else {}) or {})
    add  = _normalize_shadow_items(new_items or {})
    base.update(add)
    _history_shadow_save(base)

def _to_jf_rating(v) -> Optional[float]:
    try: x = float(v)
    except Exception: return None
    if 0.0 <= x <= 5.0: x *= 2.0
    x = max(0.0, min(10.0, x))
    return round(x, 1)

def _rate(http: JFHttp, *, uid: str, item_id: str, rating: Optional[float]) -> bool:
    if _update_userdata(http, uid=uid, item_id=item_id, payload={"Rating": rating}): return True
    try:
        if rating is None:
            st,_ = http.delete(f"/Users/{uid}/UserRating/{item_id}")
        else:
            st,_ = http.post(f"/Users/{uid}/UserRating/{item_id}", params={"rating": rating}, json_payload=None)
        return st in (200,204)
    except Exception:
        return False

# playlists: ensure + multi-path add
def _ensure_playlist(http: JFHttp, *, uid: str, name: str) -> Optional[str]:
    norm = (name or "").strip() or "Watchlist"

    # find existing (3 paths)
    pid = _find_playlist_id(http, uid, norm)
    if pid: return pid

    # create (preferred JSON)
    st, obj = http.post("/Playlists",
                        json_payload={"Name": norm, "UserId": uid, "MediaType": "Video"},
                        ok=(200,201))
    if st in (200,201) and isinstance(obj, Mapping):
        pid = str(obj.get("Id") or obj.get("id") or "")
        if pid: return pid

    # fallback: qs (older servers) with explicit mediaType
    st2, obj2 = http.post("/Playlists", params={"userId": uid, "name": norm, "mediaType": "Video"}, json_payload=None, ok=(200,201))
    if st2 in (200,201) and isinstance(obj2, Mapping):
        pid = str(obj2.get("Id") or obj2.get("id") or "")
        if pid: return pid

    # re-scan
    return _find_playlist_id(http, uid, norm)

def _playlist_add(http: JFHttp, *, uid: str, playlist_id: str, item_ids: List[str]) -> bool:
    if not item_ids: return True

    # 1) qs with userId
    st,_ = http.post(f"/Playlists/{playlist_id}/Items", params={"userId": uid, "ids": ",".join(item_ids)}, json_payload=None)
    if st in (200,204): return True

    # 2) qs without userId
    st2,_ = http.post(f"/Playlists/{playlist_id}/Items", params={"ids": ",".join(item_ids)}, json_payload=None)
    if st2 in (200,204): return True

    # 3) json body
    st3,_ = http.post(f"/Playlists/{playlist_id}/Items", json_payload={"Ids": item_ids}, ok=(200,204))
    return st3 in (200,204)

def _playlist_remove(http: JFHttp, *, uid: str, playlist_id: str, item_ids: List[str]) -> bool:
    if not item_ids: return True
    rev: Dict[str, List[str]] = {}
    for it in _page_items(http, f"/Playlists/{playlist_id}/Items", {"userId": uid}):
        mid = str(it.get("Id") or ""); eid = str(it.get("PlaylistItemId") or "") or mid
        if mid: rev.setdefault(mid, []).append(eid)
    entry_ids: List[str] = []
    for mid in item_ids: entry_ids += rev.get(mid, [])
    if entry_ids:
        st,_ = http.delete(f"/Playlists/{playlist_id}/Items", params={"entryIds": ",".join(entry_ids)})
        if st in (200,204): return True
    st2,_ = http.delete(f"/Playlists/{playlist_id}/Items", params={"ids": ",".join(item_ids)})
    return st2 in (200,204)

# adapter
class _JellyfinOPS:
    def name(self) -> str: return "JELLYFIN"
    def label(self) -> str: return "Jellyfin"

    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}

    def capabilities(self) -> Mapping[str, Any]:
        return {"bidirectional": True, "ratings": {"types": {"movies": True, "shows": True, "episodes": True}, "upsert": True, "unrate": True, "from_date": False}}

    def _http(self, cfg: Mapping[str, Any]) -> JFHttp:
        jf = _cfg_jf(cfg); _validate_cfg(jf)
        return JFHttp(jf.get("server") or "", jf.get("access_token") or "", jf.get("device_id") or "crosswatch")

    def _resolver(self, cfg: Mapping[str, Any]) -> Resolver:
        jf = _cfg_jf(cfg); _validate_cfg(jf)
        return Resolver(self._http(cfg), jf.get("user_id") or "")

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
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
        jf = _cfg_jf(cfg); _validate_cfg(jf)
        http = self._http(cfg); res = self._resolver(cfg)
        uid = jf.get("user_id") or ""; mode = (jf.get("watchlist", {}).get("mode") or "favorites").lower()
        gmt = _gmt_store_from_cfg(cfg)

        def _resolve_all(arr):
            out = []
            for it in arr:
                iid = (it.get("ids") or {}).get("jellyfin") or res.resolve(it)
                out.append((it, iid))
            return out

        added, errors = 0, []
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
                if gmt and not gmt.quarantine_ok("watchlist", ids): return {"added": 0, "error": "gmt_quarantine"}
                ok = _playlist_add(http, uid=uid, playlist_id=pl_id, item_ids=ids)
                if ok: return {"added": len(ids)}
                return {"added": 0, "error": "playlist_add_failed"}

            for it, iid in pairs:
                if not iid: errors.append("resolve_failed"); continue
                if dry_run: added += 1; continue
                if gmt and not gmt.quarantine_ok("watchlist", [iid]): errors.append("gmt_quarantine"); continue
                if _favorite(http, uid=uid, item_id=str(iid), flag=True): added += 1
                else: errors.append(f"favorite_failed:{iid}")
            return {"added": added, "errors": errors} if errors else {"added": added}

        if f == "ratings":
            total = len(pairs); resolv_fails = 0; write_fails = 0; invalids = 0
            succeeded: List[Dict[str, Any]] = []
            for i, (it, iid) in enumerate(pairs, 1):
                if not iid:
                    errors.append("resolve_failed"); resolv_fails += 1
                else:
                    rating = it.get("rating")
                    if rating is None:
                        pass
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
                                succeeded.append({"type": it.get("type"), "title": it.get("title"), "year": it.get("year"), "ids": it.get("ids"), "rating": r, "rated_at": None})
                            else:
                                errors.append(f"rating_failed:{iid}"); write_fails += 1
                if (i % PROGRESS_EVERY) == 0: _emit("jellyfin.ratings.progress", {"done": i, "total": total})
                if (i % THROTTLE_EVERY) == 0:
                    try: time.sleep(0.05)
                    except Exception: pass
            if succeeded and not dry_run:
                sh = _ratings_shadow_load(); items_map = dict((sh or {}).get("items") or {})
                for row in succeeded:
                    nr = _norm_row(row); items_map[canonical_key(nr)] = nr
                _ratings_shadow_save(items_map)
            _emit("jellyfin.ratings.summary", {"total": total, "added": added, "resolve_failed": resolv_fails, "write_failed": write_fails, "invalid": invalids})
            return {"added": added, "errors": errors} if errors else {"added": added}

        if f == "history":
            total = len(pairs); resolv_fails = 0; write_fails = 0
            succeeded: List[Dict[str, Any]] = []
            for i, (it, iid) in enumerate(pairs, 1):
                when = _clamp_ts(it.get("watched_at") or "") or _now_iso()
                if not iid:
                    errors.append("resolve_failed"); resolv_fails += 1
                elif dry_run:
                    added += 1
                    succeeded.append({"type": it.get("type"), "title": it.get("title"), "year": it.get("year"),
                                      "ids": it.get("ids"), "watched": True, "watched_at": when})
                else:
                    if _played(http, uid=uid, item_id=str(iid), played=True, when_iso=when):
                        added += 1
                        succeeded.append({"type": it.get("type"), "title": it.get("title"), "year": it.get("year"),
                                          "ids": it.get("ids"), "watched": True, "watched_at": when})
                    else:
                        errors.append(f"history_mark_failed:{iid}"); write_fails += 1
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

            _emit("jellyfin.history.summary", {"total": total, "added": added, "resolve_failed": resolv_fails, "write_failed": write_fails})
            return {"added": added, "errors": errors} if errors else {"added": added}

        return {"added": 0, "error": f"unknown_feature:{f}"}

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        jf = _cfg_jf(cfg); _validate_cfg(jf)
        http = self._http(cfg); res = self._resolver(cfg)
        uid = jf.get("user_id") or ""; mode = (jf.get("watchlist", {}).get("mode") or "favorites").lower()
        gmt = _gmt_store_from_cfg(cfg)

        def _resolve_all(arr):
            out = []
            for it in arr:
                iid = (it.get("ids") or {}).get("jellyfin") or res.resolve(it)
                out.append((it, iid))
            return out

        removed, errors = 0, []
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
                if ok and gmt:
                    try: gmt.remember("watchlist", ids)
                    except Exception: pass
                return {"removed": len(ids)} if ok else {"removed": 0, "error": "playlist_remove_failed"}

            for _, iid in pairs:
                if not iid: errors.append("resolve_failed"); continue
                if dry_run: removed += 1; continue
                ok = _favorite(http, uid=uid, item_id=str(iid), flag=False)
                if ok:
                    removed += 1
                    if gmt:
                        try: gmt.remember("watchlist", [iid])
                        except Exception: pass
                else:
                    errors.append(f"unfavorite_failed:{iid}")
            return {"removed": removed, "errors": errors} if errors else {"removed": removed}

        if f == "ratings":
            total = len(pairs); succeeded_keys: List[str] = []
            for i, (it, iid) in enumerate(pairs, 1):
                if not iid: errors.append("resolve_failed"); continue
                if dry_run:
                    removed += 1; succeeded_keys.append(canonical_key(_norm_row(it)))
                else:
                    ok = _rate(http, uid=uid, item_id=str(iid), rating=None)
                    if ok:
                        removed += 1; succeeded_keys.append(canonical_key(_norm_row(it)))
                    else:
                        errors.append(f"unrate_failed:{iid}")
                if (i % PROGRESS_EVERY) == 0: _emit("jellyfin.ratings.progress", {"done": i, "total": total, "mode": "remove"})
                if (i % THROTTLE_EVERY) == 0:
                    try: time.sleep(0.05)
                    except Exception: pass
            if succeeded_keys and not dry_run:
                sh = _ratings_shadow_load(); items_map = dict((sh or {}).get("items") or {})
                for k in succeeded_keys: items_map.pop(k, None)
                _ratings_shadow_save(items_map)
            return {"removed": removed, "errors": errors} if errors else {"removed": removed}

        if f == "history":
            total = len(pairs); succeeded_keys: List[str] = []
            for i, (it, iid) in enumerate(pairs, 1):
                if not iid: errors.append("resolve_failed"); continue
                if dry_run:
                    removed += 1; succeeded_keys.append(canonical_key(_norm_row(it)))
                else:
                    if _played(http, uid=uid, item_id=str(iid), played=False):
                        removed += 1; succeeded_keys.append(canonical_key(_norm_row(it)))
                    else:
                        errors.append(f"unwatch_failed:{iid}")
                if (i % PROGRESS_EVERY) == 0: _emit("jellyfin.history.progress", {"done": i, "total": total, "mode": "remove"})
                if (i % THROTTLE_EVERY) == 0:
                    try: time.sleep(0.05)
                    except Exception: pass
            if succeeded_keys and not dry_run:
                sh = _history_shadow_load(); items_map = dict((sh or {}).get("items") or {})
                for k in succeeded_keys: items_map.pop(k, None)
                _history_shadow_save(items_map)
            return {"removed": removed, "errors": errors} if errors else {"removed": removed}

        if f == "playlists":
            total = 0
            for pl in items:
                name = (pl.get("playlist") or pl.get("title") or "").strip()
                if not name: errors.append("missing_playlist_name"); continue
                pl_id = _ensure_playlist(http, uid=uid, name=name)
                if not pl_id: errors.append(f"missing_playlist:{name}"); continue
                sub_pairs = [(it, (it.get("ids") or {}).get("jellyfin") or res.resolve(it)) for it in (pl.get("items") or [])]
                ids = [iid for _,iid in sub_pairs if iid]; total += len(ids)
                if dry_run: continue
                if not _playlist_remove(http, uid=uid, playlist_id=pl_id, item_ids=ids):
                    errors.append(f"playlist_remove_failed:{name}")
            return {"removed": total, "errors": errors} if errors else {"removed": total}

        return {"removed": 0, "error": f"unknown_feature:{f}"}

# --- export
OPS: InventoryOps = _JellyfinOPS()
_JELLYFINOPS = OPS          # legacy alias
_JellyFINOPS = OPS          # extra alias for older callers

# manifest
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
        description=("Jellyfin connector with favorites-or-playlist watchlist, ratings, history, and playlists. "
                     "Robust playlist ensure/find/add across server versions."),
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
                            "watchlist": {
                                "type": "object",
                                "properties": {
                                    "mode": {"type": "string", "enum": ["favorites", "playlist"], "default": "favorites"},
                                    "playlist_name": {"type": "string", "default": "Watchlist"},
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
        return {"watchlist": True, "ratings": False, "history": False, "playlists": True}

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
