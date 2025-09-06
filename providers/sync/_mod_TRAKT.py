# providers/sync/_mod_TRAKT.py
from __future__ import annotations

# ---- Module -----------------------------------------------------
__VERSION__ = "0.2.1"

import time
from typing import Any, Dict, Iterable, List, Optional, Tuple, Set, Mapping, Callable

import requests

# Auth provider + config I/O
from providers.auth._auth_TRAKT import PROVIDER as TRAKT_AUTH
from crosswatch import load_config, save_config

# Base-interfaces (mirror of _mod_PLEX.py expectations)
from ._mod_base import (
    SyncModule, SyncContext, SyncResult, SyncStatus,
    ModuleError, RecoverableModuleError, ConfigError,
    Logger as HostLogger, ProgressEvent,
    ModuleInfo as BaseModuleInfo, ModuleCapabilities,
)

# ---------- minimal logger adapter (compatible with host logger) ----------
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
    def __init__(self, logger: Any | None, module_name: str = "TRAKT"):
        self._logger: Any = logger or _NullLogger()
        self._ctx: Dict[str, Any] = {}
        self._module = module_name

    def __call__(
        self,
        message: str,
        *,
        level: str = "INFO",
        module: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None
    ) -> None:
        lvl = (level or "INFO").upper()
        if hasattr(self._logger, "debug"):
            if   lvl == "DEBUG": self._logger.debug(message)
            elif lvl in ("WARN", "WARNING"): getattr(self._logger, "warning", getattr(self._logger, "warn"))(message)
            elif lvl == "ERROR": self._logger.error(message)
            else: self._logger.info(message)
        else:
            print(f"[{lvl}] {message}")

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
        self.set_context(**ctx)
        return self

    def child(self, name: str) -> "_LoggerAdapter":
        return self


# ---------- Trakt API helpers ----------
API_BASE = "https://api.trakt.tv"
SYNC_WATCHLIST          = f"{API_BASE}/sync/watchlist"
SYNC_WATCHLIST_MOVIES   = f"{SYNC_WATCHLIST}/movies"
SYNC_WATCHLIST_SHOWS    = f"{SYNC_WATCHLIST}/shows"

def trakt_headers(*, auth: bool = True) -> Dict[str, str]:
    """
    Build Trakt headers using config values.
    Includes client_id as `trakt-api-key` (recommended) and bearer token when auth=True.
    """
    cfg = load_config()
    client_id = (cfg.get("trakt") or {}).get("client_id") or ""
    token = ((cfg.get("auth") or {}).get("trakt") or {}).get("access_token") or ""
    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "trakt-api-version": "2",
    }
    if client_id:
        h["trakt-api-key"] = client_id
    if auth and token:
        h["Authorization"] = f"Bearer {token}"
    return h

def trakt_refresh() -> None:
    """
    Try to refresh the token via the auth provider.
    No exception is raised on failure; callers will handle 401s.
    """
    cfg = load_config()
    try:
        TRAKT_AUTH.refresh(cfg)   # mutates cfg in-place
        save_config(cfg)
    except Exception:
        pass

def _sleep_for_retry(resp: requests.Response, attempt: int) -> None:
    ra = resp.headers.get("Retry-After")
    delay = int(ra) if ra and str(ra).isdigit() else min(60, 2 ** attempt)
    time.sleep(delay)

def _req(method: str, url: str, *, json_body: Optional[Dict]=None, params: Optional[Dict]=None, attempt: int=0) -> requests.Response:
    h = trakt_headers(auth=True)
    r = requests.request(method, url, headers=h, json=json_body, params=params, timeout=30)
    if r.status_code == 401 and attempt == 0:
        trakt_refresh()
        h = trakt_headers(auth=True)
        r = requests.request(method, url, headers=h, json=json_body, params=params, timeout=30)
    if r.status_code == 429:
        _sleep_for_retry(r, attempt)
        if attempt < 5:
            return _req(method, url, json_body=json_body, params=params, attempt=attempt+1)
    r.raise_for_status()
    return r


# ---------- compact RW helper ----------
class _TraktRW:
    """Minimal helper doing watchlist read/write operations."""

    @staticmethod
    def _collect_tmdb_ids(url: str, media_key: str) -> Set[int]:
        page = 1
        out: Set[int] = set()
        while True:
            r = _req("GET", url, params={"page": page, "limit": 100})
            items = r.json() or []
            if not items:
                break
            for it in items:
                ids = ((it.get(media_key) or {}).get("ids") or {})
                tmdb = ids.get("tmdb")
                if isinstance(tmdb, int):
                    out.add(tmdb)
            page += 1
            if len(items) < 100:
                break
        return out

    @classmethod
    def get_watchlist(cls) -> Dict[str, Set[int]]:
        movies = cls._collect_tmdb_ids(SYNC_WATCHLIST_MOVIES, "movie")
        shows  = cls._collect_tmdb_ids(SYNC_WATCHLIST_SHOWS,  "show")
        return {"movie": movies, "show": shows}

    @staticmethod
    def _payload(items: List[Tuple[str, int]]) -> Dict[str, Any]:
        movies = [{"ids": {"tmdb": i}} for t, i in items if t == "movie"]
        shows  = [{"ids": {"tmdb": i}} for t, i in items if t == "show"]
        payload: Dict[str, Any] = {}
        if movies: payload["movies"] = movies
        if shows:  payload["shows"] = shows
        return payload

    @classmethod
    def add_to_watchlist(cls, items: List[Tuple[str, int]]) -> Dict[str, Any]:
        payload = cls._payload(items)
        if not payload:
            return {"added": 0, "errors": []}
        r = _req("POST", SYNC_WATCHLIST, json_body=payload)
        res = r.json() or {}
        added = sum(int(v) for v in (res.get("added") or {}).values())
        return {"added": added, "errors": []}

    @classmethod
    def remove_from_watchlist(cls, items: List[Tuple[str, int]]) -> Dict[str, Any]:
        payload = cls._payload(items)
        if not payload:
            return {"removed": 0, "errors": []}
        r = _req("POST", f"{SYNC_WATCHLIST}/remove", json_body=payload)
        res = r.json() or {}
        removed = sum(int(v) for v in (res.get("deleted") or {}).values())
        return {"removed": removed, "errors": []}


# ---------- module metadata ----------
def _module_info() -> BaseModuleInfo:
    return BaseModuleInfo(
        name="TRAKT",
        version=__VERSION__,
        description="Reads and writes Trakt watchlist via Trakt API.",
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
                    "auth": {
                        "type": "object",
                        "properties": {
                            "trakt": {
                                "type": "object",
                                "properties": {
                                    "access_token": {"type": "string", "minLength": 1},
                                },
                                "required": ["access_token"],
                            }
                        },
                        "required": ["trakt"],
                    },
                    "trakt": {
                        "type": "object",
                        "properties": {
                            "client_id": {"type": "string"},
                            "client_secret": {"type": "string"},
                        },
                    },
                    "runtime": {
                        "type": "object",
                        "properties": {"debug": {"type": "boolean"}},
                    },
                },
                "required": ["auth"],
            },
        ),
    )


# ---------- the Module (mirrors Plex shape) ----------
class TRAKTModule(SyncModule):
    info = _module_info()

    @staticmethod
    def supported_features() -> dict:
        # align with what you actually wire up later
        return {"watchlist": True, "ratings": False, "history": False, "playlists": False}

    def __init__(self, config: Mapping[str, Any], logger: HostLogger):
        self._cfg_raw: Dict[str, Any] = dict(config or {})
        self._log = _LoggerAdapter(logger, module_name=self.info.name).bind(module=self.info.name)
        self._cancel_flag = False
        self._last_status: Dict[str, Any] = {}

    # lifecycle
    def validate_config(self) -> None:
        tok = ((self._cfg_raw.get("auth") or {}).get("trakt") or {}).get("access_token", "")
        if not str(tok).strip():
            raise ConfigError("auth.trakt.access_token is required")

    def reconfigure(self, config: Mapping[str, Any]) -> None:
        self._cfg_raw = dict(config or {})
        self.validate_config()

    def set_logger(self, logger: HostLogger) -> None:
        self._log = _LoggerAdapter(logger, module_name=self.info.name).bind(module=self.info.name)

    def get_status(self) -> Mapping[str, Any]:
        return dict(self._last_status)

    def cancel(self) -> None:
        self._cancel_flag = True

    # core
    def run_sync(
        self,
        ctx: SyncContext,
        progress: Optional[Callable[[ProgressEvent], None]] = None,
    ) -> SyncResult:
        t0 = time.time()
        self._cancel_flag = False
        log = self._log.child("run").bind(run_id=ctx.run_id, dry_run=ctx.dry_run)
        log(f"TRAKT sync start (run_id={ctx.run_id}, dry={ctx.dry_run})", level="DEBUG")

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
            return self._finish(t0, SyncStatus.FAILED, errors=[str(e)])

        try:
            if self._cancel_flag or (ctx.cancel_flag and ctx.cancel_flag[0]):
                raise RecoverableModuleError("cancelled")

            emit("auth")
            _ = trakt_headers(auth=True)  # will include token or fail later

            emit("fetch", note="watchlist")
            wl = _TraktRW.get_watchlist()
            movies_n = len(wl.get("movie", set()))
            shows_n  = len(wl.get("show", set()))
            total = movies_n + shows_n

            if self._cancel_flag or (ctx.cancel_flag and ctx.cancel_flag[0]):
                raise RecoverableModuleError("cancelled")

            emit("done", done=total, total=total)
            self._last_status = {
                "last_run": time.time(),
                "watchlist_total": total,
                "movies": movies_n,
                "shows": shows_n,
            }
            log(f"watchlist totals: movies={movies_n}, shows={shows_n}, total={total}", level="INFO")

            return self._finish(
                t0, SyncStatus.SUCCESS,
                items_total=total,
                metadata={"watchlist_counts": {"movies": movies_n, "shows": shows_n, "total": total}},
            )

        except RecoverableModuleError as e:
            note = "cancelled" if "cancelled" in str(e).lower() else "recoverable"
            emit(note)
            log(str(e), level="WARN")
            status = SyncStatus.CANCELLED if note == "cancelled" else SyncStatus.WARNING
            return self._finish(t0, status, errors=[str(e)])

        except Exception as e:
            emit("error", note="unexpected")
            log(f"unexpected error: {e}", level="ERROR")
            return self._finish(t0, SyncStatus.FAILED, errors=[repr(e)])

    # result builder
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
