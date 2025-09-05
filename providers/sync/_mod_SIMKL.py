from __future__ import annotations
import json
import time
import requests
from typing import Any, Dict, List, Mapping, Optional, Tuple, Callable

from ._mod_base import (
    SyncModule, SyncContext, SyncResult, SyncStatus,
    ConfigError, Logger as HostLogger, ProgressEvent,
    ModuleInfo, ModuleCapabilities,
)

__VERSION__ = "0.2.1"
UA = "CrossWatch/Module"

# --- SIMKL endpoints (STRICT watchlist only — Plantowatch) ---
SIMKL_BASE = "https://api.simkl.com"
SIMKL_ALL_ITEMS_MOVIES_PTW = f"{SIMKL_BASE}/sync/all-items/movies/plantowatch"
SIMKL_ALL_ITEMS_SHOWS_PTW  = f"{SIMKL_BASE}/sync/all-items/shows/plantowatch"
SIMKL_ADD_TO_LIST          = f"{SIMKL_BASE}/sync/add-to-list"
SIMKL_HISTORY_REMOVE       = f"{SIMKL_BASE}/sync/history/remove"


def simkl_headers(simkl_cfg: Mapping[str, Any]) -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {simkl_cfg.get('access_token','')}",
        "simkl-api-key": simkl_cfg.get("client_id",""),
    }


def _read_as_list(j: Any, key: str) -> List[dict]:
    """
    SIMKL may return either a dict {movies:[...]} / {shows:[...]}
    or directly a list [...]. Normalize to list.
    """
    if isinstance(j, list):
        return j
    if isinstance(j, dict):
        arr = j.get(key)
        if isinstance(arr, list):
            return arr
    return []


def ids_from_simkl_item(it: Mapping[str, Any]) -> Dict[str, Any]:
    node = (it.get("movie") or it.get("show") or {}) if isinstance(it, dict) else {}
    ids = dict((node.get("ids") or {}))
    out: Dict[str, Any] = {}
    for k in ("imdb", "tmdb", "tvdb", "slug"):
        v = ids.get(k)
        if v:
            out[k] = str(v)
    y = node.get("year")
    if isinstance(y, int):
        out["year"] = y
    return out


def build_index_from_simkl(movies: List[dict], shows: List[dict]) -> Dict[str, Dict[str, Any]]:
    """
    Build stable keys 'type:idtype:id' with fallback to title/year.
    Values contain: type, ids, title, year.
    """
    idx: Dict[str, Dict[str, Any]] = {}

    def add(typ: str, it: dict) -> None:
        node = (it.get("movie") if typ == "movie" else it.get("show")) or {}
        ids = ids_from_simkl_item(it)
        key = None
        for k in ("imdb", "tmdb", "tvdb", "slug"):
            v = ids.get(k)
            if v:
                key = f"{typ}:{k}:{v}"
                break
        if not key:
            t = str(node.get("title") or "").strip().lower()
            y = node.get("year") or ""
            key = f"{typ}:t:{t}:{y}"
        idx[key] = {
            "type": typ,
            "ids": ids,
            "title": node.get("title") or "",
            "year": node.get("year"),
        }

    for it in movies or []:
        add("movie", it)
    for it in shows or []:
        add("show", it)
    return idx


def simkl_ptw_full(simkl_cfg: Mapping[str, Any], *, debug: bool=False) -> Tuple[List[dict], List[dict]]:
    """
    Strictly fetch the *watchlist* (Plantowatch) only.
    Returns (shows, movies) — orchestrator expects this order.
    Optional: simkl_cfg['date_from'] (ISO8601) to limit results.
    """
    hdr = simkl_headers(simkl_cfg)
    date_from = (simkl_cfg.get("date_from") or "").strip()
    params = {"extended": "full"}
    if date_from:
        params["date_from"] = date_from

    def _get(url: str, label: str, key: str) -> List[dict]:
        try:
            r = requests.get(url, headers=hdr, params=params, timeout=45)
            if debug:
                print(f"[SIMKL] GET {label} -> {r.status_code}")
            if not r.ok:
                if debug:
                    txt = r.text[:200].replace("\\n"," ")
                    print(f"[SIMKL] {label} body: {txt}")
                return []
            out = _read_as_list(r.json() or {}, key)
            if debug:
                print(f"[SIMKL] {label} items={len(out)}")
            return out
        except Exception as e:
            if debug:
                print(f"[SIMKL] {label} error: {e}")
            return []

    movies = _get(SIMKL_ALL_ITEMS_MOVIES_PTW, "movies/plantowatch", "movies")
    shows  = _get(SIMKL_ALL_ITEMS_SHOWS_PTW,  "shows/plantowatch",  "shows")
    return (shows, movies)  # shows first, movies second


class SIMKLModule(SyncModule):
    info = ModuleInfo(
        name="SIMKL",
        version=__VERSION__,
        description="Reads/writes SIMKL Plantowatch (watchlist) via SIMKL API.",
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
                    "simkl": {
                        "type": "object",
                        "properties": {
                            "client_id": {"type":"string","minLength":1},
                            "access_token": {"type":"string","minLength":1},
                            "date_from": {"type":"string"},  # optional ISO8601 filter
                        },
                        "required": ["client_id", "access_token"],
                    },
                    "runtime": {
                        "type": "object",
                        "properties": {"debug": {"type": "boolean"}},
                    },
                },
                "required": ["simkl"],
            },
        ),
    )

    def __init__(self, config: Mapping[str, Any], logger: HostLogger) -> None:
        super().__init__(config, logger)
        self._cfg_raw: Dict[str, Any] = dict(config or {})
        self._simkl_cfg: Dict[str, Any] = dict(self._cfg_raw.get("simkl") or {})
        self._last_status: Dict[str, Any] = {}
        self._cancel_flag = False

    @staticmethod
    def supported_features() -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}

    def validate_config(self) -> None:
        s = self._simkl_cfg
        if not (s.get("client_id") and s.get("access_token")):
            raise ConfigError("simkl.client_id and simkl.access_token are required")

    def reconfigure(self, config: Mapping[str, Any]) -> None:
        self._cfg_raw = dict(config or {})
        self._simkl_cfg = dict(self._cfg_raw.get("simkl") or {})
        self.validate_config()

    def set_logger(self, logger: HostLogger) -> None:
        self._log = logger.bind(module=self.info.name)

    def get_status(self) -> Mapping[str, Any]:
        return dict(self._last_status)

    def cancel(self) -> None:
        self._cancel_flag = True

    # ---- RW APIs used by orchestrator ----
    def simkl_add_to_ptw(self, items_by_type, *, dry_run: bool=False) -> Dict[str, Any]:
        # Ensure every entry has to:"plantowatch"
        def _normalize(payload):
            out = {}
            for bk in ("movies", "shows"):
                arr = []
                for it in (payload.get(bk) or []):
                    entry = dict(it)
                    entry["to"] = entry.get("to") or "plantowatch"
                    ids = dict(entry.get("ids") or {})
                    entry["ids"] = ids
                    arr.append(entry)
                if arr:
                    out[bk] = arr
            return out

        norm = _normalize(items_by_type)
        if dry_run:
            self._log(f"DRY-RUN SIMKL add-to-list: {json.dumps(norm)[:400]}")
            return {"ok": True, "added": sum(len(v) for v in norm.values()), "dry_run": True}
        if not norm:
            return {"ok": True, "added": 0}
        r = requests.post(SIMKL_ADD_TO_LIST, headers=simkl_headers(self._simkl_cfg), json=norm, timeout=45)
        if not r.ok:
            return {"ok": False, "error": f"SIMKL add-to-list failed HTTP {r.status_code}", "status": r.status_code, "text": r.text}
        return {"ok": True, "added": sum(len(v) for v in norm.values())}

    def simkl_remove_from_ptw(self, items_by_type: Mapping[str, List[Mapping[str, Any]]], *, dry_run: bool=False) -> Dict[str, Any]:
        if dry_run:
            self._log(f"DRY-RUN SIMKL history/remove: {json.dumps(items_by_type)[:400]}")
            return {"ok": True, "removed": sum(len(v) for v in items_by_type.values()), "dry_run": True}
        if not items_by_type:
            return {"ok": True, "removed": 0}
        r = requests.post(SIMKL_HISTORY_REMOVE, headers=simkl_headers(self._simkl_cfg), json=items_by_type, timeout=45)
        if not r.ok:
            return {"ok": False, "error": f"SIMKL history/remove failed HTTP {r.status_code}", "status": r.status_code, "text": r.text}
        return {"ok": True, "removed": sum(len(v) for v in items_by_type.values())}

    # ---- Core “read” path (for /api/sync/providers and health) ----
    def run_sync(
        self,
        ctx: SyncContext,
        progress: Optional[Callable[[ProgressEvent], None]] = None,
    ) -> SyncResult:
        t0 = time.time()
        def emit(stage: str, done: int = 0, total: int = 0, note: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> None:
            if progress:
                try:
                    progress(ProgressEvent(stage=stage, done=done, total=total, note=note, meta=dict(meta or {})))
                except Exception:
                    ...
        try:
            self.validate_config()
        except ConfigError as e:
            emit("validate", 0, 0, "config error")
            return SyncResult(status=SyncStatus.FAILED, items_total=0, started_at=t0, finished_at=time.time(), errors=[str(e)])

        dbg = bool(self._cfg_raw.get("runtime", {}).get("debug", False))
        emit("fetch", note="plantowatch")
        shows, movies = simkl_ptw_full(self._simkl_cfg, debug=dbg)
        emit("index")
        idx = build_index_from_simkl(movies, shows)
        total = len(idx)
        self._last_status = {"last_run": time.time(), "watchlist_total": total}
        emit("done", done=total, total=total)
        return SyncResult(status=SyncStatus.SUCCESS, items_total=total, started_at=t0, finished_at=time.time(), metadata={"index_keys": list(idx.keys())})
