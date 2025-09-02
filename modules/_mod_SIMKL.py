# /modules/_mod_SIMKL.py
from __future__ import annotations

__VERSION__ = "0.2.0"

import time, json, threading
from typing import Any, Dict, Mapping, Optional, List, Tuple, Callable

import requests

from ._mod_base import (
    SyncModule, SyncContext, SyncResult, SyncStatus,
    ModuleError, RecoverableModuleError, ConfigError, Logger as HostLogger,
    ProgressEvent, ModuleInfo, ModuleCapabilities,
)

# Root logger adapter
try:
    from _logging import Logger as RootLogger, log as default_root_log  # type: ignore
except Exception:
    RootLogger = None
    default_root_log = None


class _NullLogger:
    def debug(self, msg: str) -> None: print(msg)
    def info(self, msg: str) -> None: print(msg)
    def warn(self, msg: str) -> None: print(msg)
    def warning(self, msg: str) -> None: print(msg)
    def error(self, msg: str) -> None: print(msg)
    def set_context(self, **_: Any) -> None: ...
    def get_context(self) -> Dict[str, Any]: return {}
    def bind(self, **_: Any) -> "_NullLogger": return self
    def child(self, name: str) -> "_NullLogger": return self


class _LoggerAdapter:
    """Accepts your _logging.Logger or a callable; adds bind/child."""
    def __init__(self, logger: Any | None, module_name: str = "SIMKL"):
        self._logger: Any = logger or default_root_log or _NullLogger()
        self._ctx: Dict[str, Any] = {}
        self._module = module_name

    def __call__(
        self,
        message: str,
        *,
        level: str = "INFO",
        module: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None:
        lvl = (level or "INFO").upper()
        tag = {"DEBUG": "[debug]", "INFO": "[i]", "WARN": "[!]", "WARNING": "[!]", "ERROR": "[!]"}.get(lvl, "[i]")
        line = f"{tag} {message}"
        if hasattr(self._logger, "debug") and hasattr(self._logger, "info"):
            if   lvl == "DEBUG": self._logger.debug(line)
            elif lvl in ("WARN", "WARNING"):
                if hasattr(self._logger, "warning"): self._logger.warning(line)
                else: self._logger.warn(line)
            elif lvl == "ERROR": self._logger.error(line)
            else: self._logger.info(line)
            return
        if callable(self._logger):
            try:
                self._logger(line); return
            except Exception:
                pass
        print(line)

    def set_context(self, **ctx: Any) -> None:
        self._ctx.update(ctx)
        if hasattr(self._logger, "set_context"):
            try: self._logger.set_context(**self._ctx)
            except Exception: ...

    def get_context(self) -> Dict[str, Any]:
        if hasattr(self._logger, "get_context"):
            try: return dict(self._logger.get_context())
            except Exception: ...
        return dict(self._ctx)

    def bind(self, **ctx: Any) -> "_LoggerAdapter":
        base = self._logger
        if hasattr(base, "bind"):
            try:
                bound = base.bind(**ctx)
                return _LoggerAdapter(bound, module_name=self._module)
            except Exception:
                ...
        self.set_context(**ctx)
        return self

    def child(self, name: str) -> "_LoggerAdapter":
        base = self._logger
        if hasattr(base, "child"):
            try:
                ch = base.child(name)
                return _LoggerAdapter(ch, module_name=name)
            except Exception:
                ...
        return self


# SIMKL HTTP helpers
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
        raise RecoverableModuleError(f"SIMKL GET {url} → HTTP {r.status_code}: {r.text[:300]}")
    try:
        return r.json()
    except Exception:
        return None


def _http_post_json(url: str, headers: Mapping[str, str], payload: Mapping[str, Any], timeout: int = 45) -> Any:
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if not r.ok:
        raise RecoverableModuleError(f"SIMKL POST {url} → HTTP {r.status_code}: {r.text[:300]}")
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


# Data helpers exposed to orchestrator

def simkl_get_activities(simkl_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    js = _http_get_json(SIMKL_ACTIVITIES, _headers(simkl_cfg)) or {}
    all_raw = js.get("all")
    all_val = all_raw.get("all") if isinstance(all_raw, dict) else all_raw

    def _pick_section(j, *names):
        for n in names:
            sec = j.get(n)
            if isinstance(sec, dict):
                return sec
        return {}

    def _norm(sec: dict) -> dict:
        if not isinstance(sec, dict):
            return {"all": None, "rated_at": None, "plantowatch": None, "completed": None, "dropped": None, "watching": None}
        return {
            "all": sec.get("all"),
            "rated_at": sec.get("rated_at"),
            "plantowatch": sec.get("plantowatch"),
            "completed": sec.get("completed"),
            "dropped": sec.get("dropped"),
            "watching": sec.get("watching"),
        }

    return {
        "all": all_val,
        "movies": _norm(_pick_section(js, "movies")),
        "tv_shows": _norm(_pick_section(js, "tv_shows", "shows")),
        "anime": _norm(_pick_section(js, "anime")),
    }


def simkl_ptw_full(simkl_cfg: Mapping[str, Any]) -> Tuple[List[dict], List[dict]]:
    hdrs = _headers(simkl_cfg)
    shows_js  = _http_get_json(f"{SIMKL_ALL_ITEMS}/shows/plantowatch", hdrs)
    movies_js = _http_get_json(f"{SIMKL_ALL_ITEMS}/movies/plantowatch", hdrs)
    return (shows_js or {}).get("shows", []) or [], (movies_js or {}).get("movies", []) or []


def simkl_allitems_delta(simkl_cfg: Mapping[str, Any], typ: str, status: str, since_iso: str) -> List[dict]:
    hdrs = _headers(simkl_cfg)
    base = f"{SIMKL_ALL_ITEMS}/{'movies' if typ=='movies' else 'shows'}/{status}"
    js = _http_get_json(base, hdrs, params={"date_from": since_iso}) or {}
    key = "movies" if typ == "movies" else "shows"
    return js.get(key, []) or []


def _ids_from_item(it: dict) -> dict:
    node = None
    for k in ("movie", "show", "anime", "ids"):
        if isinstance(it.get(k), dict):
            node = it[k]; break
    if node is None:
        return {}
    ids_block = node.get("ids", node) if ("ids" in node or node is it) else node
    ids = {}
    for k in ("simkl", "imdb", "tmdb", "tvdb", "slug"):
        v = ids_block.get(k)
        if v is not None:
            ids[k] = v
    if "title" in node:
        ids["title"] = node.get("title")
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
    for m in simkl_movies:
        ids = _combine_ids(_ids_from_item(m))
        pair = _canonical(ids)
        if not pair: continue
        node = (m.get("movie") or m.get("show") or {})
        idx[_key(pair)] = {"type": "movie", "ids": ids, "title": node.get("title"), "year": ids.get("year")}
    for s in simkl_shows:
        ids = _combine_ids(_ids_from_item(s))
        pair = _canonical(ids)
        if not pair: continue
        node = (s.get("show") or s.get("movie") or {})
        idx[_key(pair)] = {"type": "show", "ids": ids, "title": node.get("title"), "year": ids.get("year")}
    return idx


class SIMKLModule(SyncModule):
    info = ModuleInfo(
        name="SIMKL",
        version=__VERSION__,
        description="Reads/writes SIMKL PTW and history via public API.",
        vendor="community",
        capabilities=ModuleCapabilities(
            supports_dry_run=True,
            supports_cancel=True,
            supports_timeout=True,
            bidirectional=True,
            status_stream=True,
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
        self._log = _LoggerAdapter(logger, module_name=self.info.name).bind(module=self.info.name)
        self._cancel = threading.Event()
        self._last_status: Dict[str, Any] = {}

    # config lifecycle
    def validate_config(self) -> None:
        sid = (self._simkl.get("client_id") or "").strip()
        at  = (self._simkl.get("access_token") or "").strip()
        rt  = (self._simkl.get("refresh_token") or "").strip()
        if not sid:
            raise ConfigError("simkl.client_id is required")
        if not at and not rt:
            raise ConfigError("simkl.access_token or simkl.refresh_token is required")

    def reconfigure(self, config: Mapping[str, Any]) -> None:
        self._cfg_raw = dict(config or {})
        self._simkl = dict(self._cfg_raw.get("simkl") or {})
        self.validate_config()

    def set_logger(self, logger: HostLogger) -> None:
        self._log = _LoggerAdapter(logger, module_name=self.info.name).bind(module=self.info.name)

    def get_status(self) -> Mapping[str, Any]:
        return dict(self._last_status)

    def cancel(self) -> None:
        self._cancel.set()

    # internals
    def _check_cancel_timeout(self, t0: float, ctx: SyncContext) -> None:
        if self._cancel.is_set() or (ctx.cancel_flag and ctx.cancel_flag[0]):
            raise RecoverableModuleError("cancelled")
        if ctx.timeout_sec is not None and (time.time() - t0) >= ctx.timeout_sec:
            raise RecoverableModuleError("timeout")

    def _ensure_token(self, t0: float, ctx: SyncContext) -> None:
        if not self._simkl.get("access_token") or _token_expired(self._simkl):
            self._log("Refreshing SIMKL token…", level="DEBUG")
            self._cfg_raw["simkl"] = self._simkl
            self._cfg_raw = _refresh_tokens(self._cfg_raw)
            self._simkl = dict(self._cfg_raw.get("simkl") or {})
            if not self._simkl.get("access_token"):
                raise RecoverableModuleError("token refresh did not yield an access_token")
        self._check_cancel_timeout(t0, ctx)

    # public writes
    def simkl_add_to_ptw(self, items_by_type: Mapping[str, List[Mapping[str, Any]]], *, dry_run: bool = False) -> Dict[str, Any]:
        self._ensure_token(time.time(), SyncContext(run_id="internal"))
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
            self._log(f"DRY-RUN add-to-list: {json.dumps(payload)[:400]}", level="DEBUG")
            return {"ok": True, "added": sum(len(v) for v in payload.values()), "dry_run": True}
        hdrs = _headers(self._simkl)
        _http_post_json(SIMKL_ADD_TO_LIST, hdrs, payload)
        return {"ok": True, "added": sum(len(v) for v in payload.values())}

    def simkl_remove_from_history(self, items_by_type: Mapping[str, List[Mapping[str, Any]]], *, dry_run: bool = False) -> Dict[str, Any]:
        self._ensure_token(time.time(), SyncContext(run_id="internal"))
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
            self._log(f"DRY-RUN history/remove: {json.dumps(payload)[:400]}", level="DEBUG")
            return {"ok": True, "removed": sum(len(v) for v in payload.values()), "dry_run": True}
        hdrs = _headers(self._simkl)
        _http_post_json(SIMKL_HISTORY_REMOVE, hdrs, payload)
        return {"ok": True, "removed": sum(len(v) for v in payload.values())}

    # main read path
    def run_sync(
        self,
        ctx: SyncContext,
        progress: Optional[Callable[[ProgressEvent], None]] = None,
    ) -> SyncResult:
        t0 = time.time()
        self._cancel.clear()
        log = self._log.child("run").bind(run_id=ctx.run_id, dry_run=ctx.dry_run)
        log(f"start run_id={ctx.run_id} dry_run={ctx.dry_run}", level="DEBUG")

        def emit(stage: str, done: int = 0, total: int = 0, note: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> None:
            if progress:
                try:
                    progress(ProgressEvent(stage=stage, done=done, total=total, note=note, meta=dict(meta or {})))
                except Exception:
                    ...

        try:
            self.validate_config()
        except ConfigError as e:
            emit("validate", note="config error")
            return self._finish(t0, SyncStatus.FAILED, errors=[str(e)])

        try:
            emit("auth")
            self._ensure_token(t0, ctx)

            emit("activities")
            acts = simkl_get_activities(self._simkl)
            self._check_cancel_timeout(t0, ctx)

            emit("fetch", note="ptw")
            shows_list, movies_list = simkl_ptw_full(self._simkl)
            self._check_cancel_timeout(t0, ctx)

            emit("index")
            idx = build_index_from_simkl(movies_list, shows_list)

            items_total = len(idx)
            meta = {
                "activities": acts,
                "ptw_counts": {
                    "shows": len(shows_list),
                    "movies": len(movies_list),
                    "total": items_total,
                },
                "index_keys": list(idx.keys()),
            }

            self._last_status = {
                "last_run": time.time(),
                "ptw_total": items_total,
                "activities": {
                    "all": acts.get("all"),
                    "movies.plantowatch": (acts.get("movies") or {}).get("plantowatch"),
                    "tv_shows.plantowatch": (acts.get("tv_shows") or {}).get("plantowatch"),
                },
            }

            log(f"SIMKL PTW total={items_total}", level="INFO")
            emit("done", done=items_total, total=items_total)
            return self._finish(t0, SyncStatus.SUCCESS, items_total=items_total, metadata=meta)

        except RecoverableModuleError as e:
            note = "cancelled" if "cancelled" in str(e).lower() else ("timeout" if "timeout" in str(e).lower() else "recoverable")
            emit(note)
            log(str(e), level="WARN")
            status = SyncStatus.CANCELLED if "cancelled" in note else SyncStatus.WARNING
            return self._finish(t0, status, errors=[str(e)])

        except Exception as e:
            emit("error", note="unexpected")
            log(f"unexpected error: {e}", level="ERROR")
            return self._finish(t0, SyncStatus.FAILED, errors=[repr(e)])

    def _finish(
        self,
        t0: float,
        status: SyncStatus,
        *,
        items_total: int = 0,
        items_added: int = 0,
        items_removed: int = 0,
        items_updated: int = 0,
        warnings: Optional[List[str]] = None,
        errors: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SyncResult:
        t1 = time.time()
        return SyncResult(
            status=status,
            started_at=t0,
            finished_at=t1,
            duration_ms=int((t1 - t0) * 1000),
            items_total=items_total,
            items_added=items_added,
            items_removed=items_removed,
            items_updated=items_updated,
            warnings=list(warnings or []),
            errors=list(errors or []),
            metadata=dict(metadata or {}),
        )
