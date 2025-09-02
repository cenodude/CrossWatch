# /modules/_mod_PLEX.py
from __future__ import annotations

__VERSION__ = "0.2.0"

import re
import time
import json
import threading
from typing import Any, Dict, Mapping, Optional, List, Tuple, Sequence, cast, TYPE_CHECKING, Callable
from pathlib import Path

import requests

from ._mod_base import (
    SyncModule, SyncContext, SyncResult, SyncStatus,
    ModuleError, RecoverableModuleError, ConfigError,
    Logger as HostLogger, ProgressEvent,
    ModuleInfo, ModuleCapabilities,
)

# ---- root logger integration -------------------------------------------------
try:
    from _logging import Logger as RootLogger, log as default_root_log  # type: ignore
except Exception:  # pragma: no cover
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
    def __init__(self, logger: Any | None, module_name: str = "PLEX"):
        self._logger: Any = logger or default_root_log or _NullLogger()
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
        tag = {"DEBUG": "[debug]", "INFO": "[i]", "WARN": "[!]", "WARNING": "[!]", "ERROR": "[!]"}.get(lvl, "[i]")
        line = f"{tag} {message}"
        if hasattr(self._logger, "debug") and hasattr(self._logger, "info"):
            if   lvl == "DEBUG": self._logger.debug(line)
            elif lvl in ("WARN", "WARNING"): getattr(self._logger, "warning", getattr(self._logger, "warn"))(line)
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
        # best-effort: prefer host bind/child, else keep adapter
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


# ---- constants & regexes -----------------------------------------------------
UA = "Plex-SIMKL-Watchlist-Sync/Module"
DISCOVER_HOST = "https://discover.provider.plex.tv"
PLEX_WATCHLIST_PATH = "/library/sections/watchlist/all"
PLEX_METADATA_PATH = "/library/metadata"

_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|tvdb)://(\d+)", re.I)


# ---- plexapi (optional) ------------------------------------------------------
try:
    import plexapi  # type: ignore
    HAS_PLEXAPI = True
except Exception:  # pragma: no cover
    plexapi = None  # type: ignore
    HAS_PLEXAPI = False

if TYPE_CHECKING:
    from plexapi.myplex import MyPlexAccount  # pragma: no cover


# ---- helpers -----------------------------------------------------------------

def _extract_ids_from_guid_strings(guid_values: List[str]) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    imdb = tmdb = tvdb = None
    for s in guid_values or []:
        s = str(s)
        m = _PAT_IMDB.search(s)
        if m and not imdb:
            imdb = m.group(1)
        m = _PAT_TMDB.search(s)
        if m and not tmdb:
            try: tmdb = int(m.group(1))
            except Exception: pass
        m = _PAT_TVDB.search(s)
        if m and not tvdb:
            try: tvdb = int(m.group(1))
            except Exception: pass
    return imdb, tmdb, tvdb


def _plex_headers(token: str) -> dict:
    return {
        "X-Plex-Token": token,
        "Accept": "application/json",
        "X-Plex-Product": "PlexWatchlistSync",
        "X-Plex-Version": __VERSION__,
        "X-Plex-Client-Identifier": "plex-simkl-bridge",
        "X-Plex-Device": "Python",
        "X-Plex-Device-Name": "plex-simkl-bridge",
        "X-Plex-Platform": "Python",
        "User-Agent": UA,
    }


def _discover_get(path: str, token: str, params: dict, timeout: int = 20) -> Optional[dict]:
    url = f"{DISCOVER_HOST}{path}"
    try:
        r = requests.get(url, headers=_plex_headers(token), params=params, timeout=timeout)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return None


def _discover_metadata_by_ratingkey(token: str, rating_key: str, debug: bool = False) -> Optional[dict]:
    params = {"includeExternalMedia": "1"}
    data = _discover_get(f"{PLEX_METADATA_PATH}/{rating_key}", token, params, timeout=12)
    if not data:
        return None
    md = (data.get("MediaContainer", {}).get("Metadata") or [])
    return md[0] if md else None


def plex_fetch_watchlist_items_via_discover(token: str, page_size: int = 100, debug: bool = False) -> List[Dict[str, Any]]:
    params_base = {"includeCollections": "1", "includeExternalMedia": "1"}
    start = 0
    items: List[dict] = []
    while True:
        params = dict(params_base)
        params["X-Plex-Container-Start"] = str(start)
        params["X-Plex-Container-Size"] = str(page_size)
        data = _discover_get(PLEX_WATCHLIST_PATH, token, params, timeout=20)
        if not data:
            if debug:
                print("[debug] discover watchlist: no data")
            break
        mc = data.get("MediaContainer", {}) or {}
        md = mc.get("Metadata", []) or []
        for it in md:
            title = it.get("title") or it.get("name")
            rating_key = str(it.get("ratingKey") or "") or ""
            mtype = it.get("type") or it.get("metadataType")
            mtype = "show" if (isinstance(mtype, str) and mtype.startswith("show")) or mtype == 2 else "movie"

            guid_values: List[str] = []
            if isinstance(it.get("guid"), str):
                guid_values.append(it["guid"])
            if isinstance(it.get("Guid"), list):
                for gg in it["Guid"]:
                    if isinstance(gg, dict) and "id" in gg:
                        guid_values.append(gg["id"])

            imdb, tmdb, tvdb = _extract_ids_from_guid_strings(guid_values)
            if not any([imdb, tmdb, tvdb]) and rating_key:
                enriched = _discover_metadata_by_ratingkey(token, rating_key, debug=debug)
                if enriched:
                    e_guids: List[str] = []
                    if isinstance(enriched.get("Guid"), list):
                        for gg in enriched["Guid"]:
                            if isinstance(gg, dict) and "id" in gg:
                                e_guids.append(gg["id"])
                    if isinstance(enriched.get("guid"), str):
                        e_guids.append(enriched["guid"])
                    imdb, tmdb, tvdb = _extract_ids_from_guid_strings(e_guids)

            ids: Dict[str, Any] = {}
            if imdb: ids["imdb"] = imdb
            if tmdb is not None: ids["tmdb"] = tmdb
            if tvdb is not None: ids["tvdb"] = tvdb

            items.append({"type": mtype, "title": title, "ids": ids})

        size = int(mc.get("size", len(md)))
        if not md or size < page_size:
            break
        start += size if size else page_size
    return items


def plex_fetch_watchlist_items_via_plexapi(acct: Optional["MyPlexAccount"], debug: bool = False) -> Optional[List[object]]:
    if acct is None:
        return None
    try:
        movies = acct.watchlist(libtype="movie")
        shows = acct.watchlist(libtype="show")
        items = (movies or []) + (shows or [])
        if debug:
            print(f"[debug] plexapi watchlist fetched: {len(items)} items")
        return items
    except Exception as e:
        if debug:
            print(f"[debug] plexapi watchlist fetch failed: {e}")
        return None


def plex_fetch_watchlist_items(acct: Optional["MyPlexAccount"], plex_token: str, debug: bool = False) -> Sequence[object | Dict[str, Any]]:
    items = plex_fetch_watchlist_items_via_plexapi(acct, debug=debug)
    if items is not None:
        return items
    if debug:
        print("[debug] Falling back to Discover HTTP for watchlist read")
    return plex_fetch_watchlist_items_via_discover(plex_token, page_size=100, debug=debug)


def plex_item_to_ids(item: Any) -> Dict[str, Any]:
    if isinstance(item, dict):
        ids = item.get("ids") or {}
        title = item.get("title")
        year = item.get("year")
        out: Dict[str, Any] = {"title": title, "year": year}
        out.update({k: v for k, v in ids.items() if v is not None})
        return {k: v for k, v in out.items() if v is not None}

    title = getattr(item, "title", None)
    year = getattr(item, "year", None)
    guid_values: List[str] = []
    try:
        for g in (getattr(item, "guids", []) or []):
            gid = getattr(g, "id", None)
            if isinstance(gid, str):
                guid_values.append(gid)
    except Exception:
        pass
    gsingle = getattr(item, "guid", None)
    if isinstance(gsingle, str):
        guid_values.append(gsingle)
    imdb, tmdb, tvdb = _extract_ids_from_guid_strings(guid_values)
    out: Dict[str, Any] = {"title": title, "year": year, "imdb": imdb, "tmdb": tmdb, "tvdb": tvdb}
    return {k: v for k, v in out.items() if v}


def item_libtype(item: Any) -> str:
    if isinstance(item, dict):
        return "show" if item.get("type") == "show" else "movie"
    t = getattr(item, "type", "movie")
    return "show" if t == "show" else "movie"


def resolve_discover_item(acct: "MyPlexAccount", ids: dict, libtype: str, debug: bool = False) -> Optional[Any]:
    queries: List[str] = []
    if ids.get("imdb"): queries.append(ids["imdb"])
    if ids.get("tmdb"): queries.append(str(ids["tmdb"]))
    if ids.get("tvdb"): queries.append(str(ids["tvdb"]))
    if ids.get("title"): queries.append(ids["title"])
    queries = list(dict.fromkeys(queries))

    for q in queries:
        try:
            hits: Sequence[Any] = acct.searchDiscover(q, libtype=libtype) or []
        except Exception:
            hits = []
        for md in hits:
            md_ids = plex_item_to_ids(md)
            if ids.get("imdb") and md_ids.get("imdb") == ids.get("imdb"): return md
            if ids.get("tmdb") and md_ids.get("tmdb") == ids.get("tmdb"): return md
            if ids.get("tvdb") and md_ids.get("tvdb") == ids.get("tvdb"): return md
            if ids.get("title") and ids.get("year"):
                try:
                    same_title = str(md_ids.get("title", "")).strip().lower() == str(ids["title"]).strip().lower()
                    same_year = int(md_ids.get("year", 0)) == int(ids["year"])
                    if same_title and same_year: return md
                except Exception:
                    pass
    return None


def plex_add_by_ids(acct: "MyPlexAccount", ids: dict, libtype: str, debug: bool = False) -> bool:
    it = resolve_discover_item(acct, ids, libtype, debug=debug)
    if not it:
        if debug: print(f"[debug] plexapi add: could not resolve {ids}")
        return False
    try:
        cast(Any, it).addToWatchlist(account=acct)
        if debug: print(f"[debug] plexapi add OK: {getattr(it, 'title', ids)}")
        return True
    except Exception as e:
        if debug: print(f"[debug] plexapi add failed: {e}")
        msg = str(e).lower()
        if ("already on the watchlist" in msg or "already on watchlist" in msg or "409" in msg):
            if debug: print("[debug] treat as success: item already present on Plex")
            return True
        return False


def plex_remove_by_ids(acct: "MyPlexAccount", ids: dict, libtype: str, debug: bool = False) -> bool:
    it = resolve_discover_item(acct, ids, libtype, debug=debug)
    if not it:
        if debug: print(f"[debug] plexapi remove: could not resolve {ids}")
        return False
    try:
        cast(Any, it).removeFromWatchlist(account=acct)
        if debug: print(f"[debug] plexapi remove OK: {getattr(it, 'title', ids)}")
        return True
    except Exception as e:
        if debug: print(f"[debug] plexapi remove failed: {e}")
        msg = str(e).lower()
        if "not on the watchlist" in msg or "404" in msg or "not found" in msg:
            if debug: print("[debug] treat as success: item already absent on Plex")
            return True
        return False


def gather_plex_rows(items: Sequence[object | Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[dict] = []
    for it in items:
        libtype = item_libtype(it)
        ids_full = plex_item_to_ids(it)
        ids = {k: v for k, v in ids_full.items() if k in ("imdb", "tmdb", "tvdb", "slug") and v}
        rows.append({"type": libtype, "title": ids_full.get("title"), "year": ids_full.get("year"), "ids": ids})
    return rows


def build_index(rows_movies: List[Dict[str, Any]], rows_shows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    def _canonical(ids: dict) -> Optional[Tuple[str, str]]:
        for k in ("imdb", "tmdb", "tvdb", "slug"):
            v = ids.get(k)
            if v is not None:
                return (k, str(v))
        return None
    def _key(pair: Tuple[str, str]) -> str: return f"{pair[0]}:{pair[1]}"

    idx: Dict[str, dict] = {}
    for r in rows_movies:
        ids = {k: v for k, v in r.get("ids", {}).items() if v is not None}
        pair = _canonical(ids)
        if not pair: continue
        idx[_key(pair)] = {"type": "movie", "ids": ids, "title": r.get("title"), "year": r.get("year")}
    for r in rows_shows:
        ids = {k: v for k, v in r.get("ids", {}).items() if v is not None}
        pair = _canonical(ids)
        if not pair: continue
        idx[_key(pair)] = {"type": "show", "ids": ids, "title": r.get("title"), "year": r.get("year")}
    return idx


# ---- Module ------------------------------------------------------------------
class PLEXModule(SyncModule):
    info = ModuleInfo(
        name="PLEX",
        version=__VERSION__,
        description="Reads and writes Plex watchlist via plexapi/Discover.",
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
                    "plex": {
                        "type": "object",
                        "properties": {
                            "account_token": {"type": "string", "minLength": 1},
                        },
                        "required": ["account_token"],
                    },
                    "runtime": {
                        "type": "object",
                        "properties": {"debug": {"type": "boolean"}},
                    },
                },
                "required": ["plex"],
            },
        ),
    )

    def __init__(self, config: Mapping[str, Any], logger: HostLogger):
        self._cfg_raw: Dict[str, Any] = dict(config or {})
        self._plex_cfg = dict(self._cfg_raw.get("plex") or {})
        self._log = _LoggerAdapter(logger, module_name=self.info.name).bind(module=self.info.name)
        self._cancel = threading.Event()
        self._last_status: Dict[str, Any] = {}
        self._acct: Optional["MyPlexAccount"] = None

    # --- lifecycle

    def validate_config(self) -> None:
        tok = (self._plex_cfg.get("account_token") or "").strip()
        if not tok:
            raise ConfigError("plex.account_token is required")

    def reconfigure(self, config: Mapping[str, Any]) -> None:
        self._cfg_raw = dict(config or {})
        self._plex_cfg = dict(self._cfg_raw.get("plex") or {})
        self.validate_config()

    def set_logger(self, logger: HostLogger) -> None:
        self._log = _LoggerAdapter(logger, module_name=self.info.name).bind(module=self.info.name)

    def get_status(self) -> Mapping[str, Any]:
        return dict(self._last_status)

    def cancel(self) -> None:
        self._cancel.set()

    # --- private

    def _ensure_acct(self) -> None:
        if self._acct is not None:
            return
        token = self._plex_cfg.get("account_token")
        if not HAS_PLEXAPI:
            self._log("plexapi is not installed; write operations are disabled.", level="WARN")
            self._acct = None
            return
        try:
            from plexapi.myplex import MyPlexAccount as _RealMyPlexAccount  # type: ignore
            self._acct = _RealMyPlexAccount(token=token)
        except Exception as e:
            raise RecoverableModuleError(f"Could not authenticate to Plex: {e}")

    def _check_cancel_timeout(self, t0: float, ctx: SyncContext) -> None:
        if self._cancel.is_set() or (ctx.cancel_flag and ctx.cancel_flag[0]):
            raise RecoverableModuleError("cancelled")
        if ctx.timeout_sec is not None and (time.time() - t0) >= ctx.timeout_sec:
            raise RecoverableModuleError("timeout")

    # --- optional RW APIs (used by orchestrator paths)

    def plex_add(self, items_by_type: Mapping[str, List[Mapping[str, Any]]], *, dry_run: bool = False) -> Dict[str, Any]:
        self._ensure_acct()
        if self._acct is None:
            return {"ok": False, "error": "plexapi not available; cannot add"}
        added = 0
        if dry_run:
            self._log(f"DRY-RUN Plex add: {json.dumps(items_by_type)[:400]}", level="DEBUG")
            return {"ok": True, "added": sum(len(v) for v in items_by_type.values()), "dry_run": True}
        acct = cast("MyPlexAccount", self._acct)
        for typ in ("movies", "shows"):
            for it in items_by_type.get(typ, []):
                ids = it.get("ids") or {}
                libtype = "movie" if typ == "movies" else "show"
                if plex_add_by_ids(acct, ids, libtype, debug=False):
                    added += 1
        return {"ok": True, "added": added}

    def plex_remove(self, items_by_type: Mapping[str, List[Mapping[str, Any]]], *, dry_run: bool = False) -> Dict[str, Any]:
        self._ensure_acct()
        if self._acct is None:
            return {"ok": False, "error": "plexapi not available; cannot remove"}
        removed = 0
        if dry_run:
            self._log(f"DRY-RUN Plex remove: {json.dumps(items_by_type)[:400]}", level="DEBUG")
            return {"ok": True, "removed": sum(len(v) for v in items_by_type.values()), "dry_run": True}
        acct = cast("MyPlexAccount", self._acct)
        for typ in ("movies", "shows"):
            for it in items_by_type.get(typ, []):
                ids = it.get("ids") or {}
                libtype = "movie" if typ == "movies" else "show"
                if plex_remove_by_ids(acct, ids, libtype, debug=False):
                    removed += 1
        return {"ok": True, "removed": removed}

    # --- core

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
            emit("validate", 0, 0, "config error")
            return self._finish(t0, SyncStatus.FAILED, errors=[str(e)])

        debug = bool(self._cfg_raw.get("runtime", {}).get("debug", False))
        token = self._plex_cfg.get("account_token", "")

        try:
            emit("auth")
            self._ensure_acct()
            self._check_cancel_timeout(t0, ctx)

            emit("fetch", note="watchlist")
            plex_items = plex_fetch_watchlist_items(self._acct, token, debug=debug)
            self._check_cancel_timeout(t0, ctx)

            emit("parse")
            rows = gather_plex_rows(plex_items)
            rows_movies = [r for r in rows if r["type"] == "movie"]
            rows_shows  = [r for r in rows if r["type"] == "show"]
            self._check_cancel_timeout(t0, ctx)

            emit("index")
            idx = build_index(rows_movies, rows_shows)

            items_total = len(idx)
            meta = {
                "watchlist_counts": {
                    "movies": len(rows_movies),
                    "shows": len(rows_shows),
                    "total": items_total,
                },
                "index_keys": list(idx.keys()),
            }

            self._last_status = {
                "last_run": time.time(),
                "watchlist_total": items_total,
            }

            log(f"PLEX watchlist total={items_total}", level="INFO")
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
