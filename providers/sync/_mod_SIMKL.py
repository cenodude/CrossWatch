
from __future__ import annotations
from typing import Any, Dict, Mapping, Optional, List, Tuple, Callable
import time, json, threading
import requests

from ._mod_base import (
    SyncModule, SyncContext, SyncResult, SyncStatus,
    ModuleError, RecoverableModuleError, ConfigError,
    Logger as HostLogger, ProgressEvent,
    ModuleInfo, ModuleCapabilities,
)

__VERSION__ = "0.2.1"

# --- HTTP helpers ---
SIMKL_BASE = "https://api.simkl.com"
SIMKL_OAUTH_TOKEN    = f"{SIMKL_BASE}/oauth/token"
SIMKL_ALL_ITEMS      = f"{SIMKL_BASE}/sync/all-items"
SIMKL_ADD_TO_LIST    = f"{SIMKL_BASE}/sync/add-to-list"
SIMKL_HISTORY_REMOVE = f"{SIMKL_BASE}/sync/history/remove"
SIMKL_ACTIVITIES     = f"{SIMKL_BASE}/sync/activities"
UA = "CrossWatch/Module"

def _headers(simkl_cfg: Mapping[str, Any]) -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {simkl_cfg.get('access_token','')}",
        "simkl-api-key": f"{simkl_cfg.get('client_id','')}",
    }

def _http_get_json(url: str, headers: Mapping[str, str], params: Optional[dict] = None, timeout: int = 45) -> Any:
    r = requests.get(url, headers=headers, params=params or {}, timeout=timeout)
    if not r.ok:
        raise RecoverableModuleError(f"SIMKL GET {url} -> HTTP {r.status_code}: {r.text[:300]}")
    try:
        return r.json()
    except Exception:
        return None

def _http_post_json(url: str, headers: Mapping[str, str], payload: Mapping[str, Any], timeout: int = 45) -> Any:
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if not r.ok:
        raise RecoverableModuleError(f"SIMKL POST {url} -> HTTP {r.status_code}: {r.text[:300]}")
    try:
        return r.json() if r.text else {}
    except Exception:
        return {}

def _token_expired(simkl_cfg: Mapping[str, Any]) -> bool:
    try:
        exp = float(simkl_cfg.get("token_expires_at", 0.0))
    except Exception:
        exp = 0.0
    return time.time() >= (exp - 60)

def _refresh_tokens(full_cfg: Dict[str, Any]) -> Dict[str, Any]:
    s = dict(full_cfg.get("simkl") or {})
    if not s.get("refresh_token"):
        return full_cfg
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": s["refresh_token"],
        "client_id": s.get("client_id", ""),
        "client_secret": s.get("client_secret", ""),
    }
    r = requests.post(SIMKL_OAUTH_TOKEN, json=payload, headers={"User-Agent": UA}, timeout=30)
    if not r.ok:
        raise RecoverableModuleError(f"SIMKL refresh failed: HTTP {r.status_code}: {r.text[:300]}")
    tok = r.json()
    s["access_token"] = tok["access_token"]
    s["refresh_token"] = tok.get("refresh_token", s.get("refresh_token", ""))
    s["token_expires_at"] = time.time() + int(tok.get("expires_in", 3600))
    full_cfg["simkl"] = s
    return full_cfg

# --- Public helpers used by orchestrator ---

def simkl_get_activities(simkl_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    js = _http_get_json(SIMKL_ACTIVITIES, _headers(simkl_cfg)) or {}
    # Flatten minimal fields we care about
    def _sec(j, key):
        sec = j.get(key) or {}
        return {k: sec.get(k) for k in ("all","rated_at","plantowatch","completed","dropped","watching")}
    return {
        "all": (js.get("all") or {}).get("all") if isinstance(js.get("all"), dict) else js.get("all"),
        "movies": _sec(js, "movies"),
        "tv_shows": _sec(js, "tv_shows") if "tv_shows" in js else _sec(js, "shows"),
        "anime": _sec(js, "anime"),
    }

def simkl_ptw_full(simkl_cfg: Mapping[str, Any]) -> Tuple[List[dict], List[dict]]:
    hdrs = _headers(simkl_cfg)
    shows_js  = _http_get_json(f"{SIMKL_ALL_ITEMS}/shows/plantowatch", hdrs)
    movies_js = _http_get_json(f"{SIMKL_ALL_ITEMS}/movies/plantowatch", hdrs)
    return (shows_js or {}).get("shows", []) or [], (movies_js or {}).get("movies", []) or []

def _ids_from_item(it: dict) -> dict:
    node = None
    for k in ("movie", "show", "anime", "ids"):
        if isinstance(it.get(k), dict):
            node = it[k]; break
    if node is None:
        return {}
    ids_block = node.get("ids", node) if ("ids" in node or node is it) else node
    ids: Dict[str, Any] = {}
    for k in ("simkl", "imdb", "tmdb", "tvdb", "slug"):
        v = ids_block.get(k)
        if v is not None:
            ids[k] = v
    if "title" in node: ids["title"] = node.get("title")
    if "year" in node:
        try: ids["year"] = int(node.get("year"))
        except Exception: ids["year"] = node.get("year")
    for k in ("tmdb", "tvdb", "year"):
        if k in ids and ids[k] is not None:
            try: ids[k] = int(ids[k])
            except Exception: pass
    return ids

def _combine_ids(ids: dict) -> dict:
    out = {}
    for k in ("imdb", "tmdb", "tvdb", "slug", "title", "year"):
        if k in ids and ids[k] is not None:
            out[k] = ids[k]
    return out

def _canonical(pair_src: dict) -> Optional[Tuple[str, str]]:
    for k in ("imdb", "tmdb", "tvdb", "slug"):
        v = pair_src.get(k)
        if v is not None:
            return (k, str(v))
    return None

def _key(pair: Tuple[str, str]) -> str:
    return f"{pair[0]}:{pair[1]}"

def build_index_from_simkl(simkl_movies: List[dict], simkl_shows: List[dict]) -> Dict[str, dict]:
    idx: Dict[str, dict] = {}
    for m in simkl_movies or []:
        ids = _combine_ids(_ids_from_item(m))
        pair = _canonical(ids)
        if not pair: continue
        node = (m.get("movie") or m.get("show") or {})
        idx[_key(pair)] = {"type": "movie", "ids": ids, "title": node.get("title"), "year": ids.get("year")}
    for s in simkl_shows or []:
        ids = _combine_ids(_ids_from_item(s))
        pair = _canonical(ids)
        if not pair: continue
        node = (s.get("show") or s.get("movie") or {})
        idx[_key(pair)] = {"type": "show", "ids": ids, "title": node.get("title"), "year": ids.get("year")}
    return idx

# --- Minimal module with write API used by orchestrator ---

class SIMKLModule(SyncModule):
    info = ModuleInfo(
        name="SIMKL",
        version=__VERSION__,
        description="SIMKL PTW read/write via public API.",
        vendor="community",
        capabilities=ModuleCapabilities(
            supports_dry_run=True,
            supports_cancel=True,
            supports_timeout=True,
            status_stream=False,
            bidirectional=True,
            config_schema={
                "type": "object",
                "properties": {
                    "simkl": {
                        "type": "object",
                        "properties": {
                            "client_id": {"type": "string", "minLength": 1},
                            "client_secret": {"type": "string"},
                            "access_token": {"type": "string"},
                            "refresh_token": {"type": "string"},
                            "token_expires_at": {"type": "number"},
                        },
                        "required": ["client_id"],
                    }
                },
                "required": ["simkl"],
            },
        ),
    )

    def __init__(self, config: Mapping[str, Any], logger: HostLogger):
        self._cfg_raw: Dict[str, Any] = dict(config or {})
        self._simkl = dict(self._cfg_raw.get("simkl") or {})
        self._log = logger
        self._cancel = threading.Event()
        self._last_status: Dict[str, Any] = {}

    def set_logger(self, logger: HostLogger) -> None:
        self._log = logger

    def validate_config(self) -> None:
        sid = (self._simkl.get("client_id") or "").strip()
        if not sid:
            raise ConfigError("simkl.client_id is required")

    def reconfigure(self, config: Mapping[str, Any]) -> None:
        self._cfg_raw = dict(config or {})
        self._simkl = dict(self._cfg_raw.get("simkl") or {})

    def get_status(self) -> Mapping[str, Any]:
        return dict(self._last_status)

    def cancel(self) -> None:
        self._cancel.set()

    # token ensure
    def _ensure_token(self) -> None:
        if not self._simkl.get("access_token") or _token_expired(self._simkl):
            self._cfg_raw = _refresh_tokens(self._cfg_raw)
            self._simkl = dict(self._cfg_raw.get("simkl") or {})
            if not self._simkl.get("access_token"):
                raise RecoverableModuleError("SIMKL access_token missing")

    # public write used by orchestrator
    def simkl_add_to_ptw(self, items_by_type: Mapping[str, List[Mapping[str, Any]]], *, dry_run: bool = False) -> Dict[str, Any]:
        self._ensure_token()
        payload: Dict[str, List[Dict[str, Any]]] = {}
        for typ in ("movies", "shows"):
            rows = []
            for it in items_by_type.get(typ, []):
                ids = it.get("ids") or {}
                to  = it.get("to") or "plantowatch"
                if ids:
                    rows.append({"to": to, "ids": _combine_ids(ids)})
            if rows:
                payload[typ] = rows
        if not payload:
            return {"ok": True, "added": 0}
        if dry_run:
            return {"ok": True, "added": sum(len(v) for v in payload.values()), "dry_run": True}
        hdrs = _headers(self._simkl)
        _http_post_json(SIMKL_ADD_TO_LIST, hdrs, payload)
        return {"ok": True, "added": sum(len(v) for v in payload.values())}

    # optional remove
    def simkl_remove_from_history(self, items_by_type: Mapping[str, List[Mapping[str, Any]]], *, dry_run: bool = False) -> Dict[str, Any]:
        self._ensure_token()
        payload: Dict[str, List[Dict[str, Any]]] = {}
        for typ in ("movies", "shows"):
            rows = []
            for it in items_by_type.get(typ, []):
                ids = it.get("ids") or {}
                if ids:
                    rows.append({"ids": _combine_ids(ids)})
            if rows:
                payload[typ] = rows
        if not payload:
            return {"ok": True, "removed": 0}
        if dry_run:
            return {"ok": True, "removed": sum(len(v) for v in payload.values()), "dry_run": True}
        hdrs = _headers(self._simkl)
        _http_post_json(SIMKL_HISTORY_REMOVE, hdrs, payload)
        return {"ok": True, "removed": sum(len(v) for v in payload.values())}

    # no-op run (unused by orchestrator)
    def run_sync(
        self,
        ctx: SyncContext,
        progress: Optional[Callable[[ProgressEvent], None]] = None,
    ) -> SyncResult:
        t0 = time.time()
        return SyncResult(
            status=SyncStatus.SUCCESS,
            started_at=t0,
            finished_at=time.time(),
            duration_ms=int((time.time() - t0)*1000),
            items_total=0, items_added=0, items_removed=0, items_updated=0,
            warnings=[], errors=[], metadata={},
        )



def simkl_allitems_delta(simkl_cfg: Mapping[str, Any], typ: str, status: str, since_iso: str) -> List[dict]:
    """
    Incremental fetch using SIMKL 'date_from' requirement.
    typ: "movies" | "shows"
    status: e.g. "plantowatch", "watching", "completed"
    since_iso: ISO-8601 timestamp
    """
    hdrs = _headers(simkl_cfg)
    base = f"{SIMKL_ALL_ITEMS}/{'movies' if typ=='movies' else 'shows'}/{status}"
    js = _http_get_json(base, hdrs, params={"date_from": since_iso}) or {}
    key = "movies" if typ == "movies" else "shows"
    return js.get(key, []) or []
