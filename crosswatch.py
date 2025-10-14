# --------------- CrossWatch Web API (FastAPI) ---------------
from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional, Tuple

# --- Core / stdlib
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs
from importlib import import_module

import sys
sys.modules.setdefault("crosswatch", sys.modules[__name__])
import traceback
import json
import os
import re
import secrets
import shutil
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import shlex
import requests
import uvicorn
import asyncio

# --- App routers / features
from _configAPI import router as config_router
from _maintenanceAPI import router as maintenance_router
from _metaAPI import router as meta_router
from _insightAPI import register_insights
from _watchlistAPI import router as watchlist_router
from _schedulingAPI import router as scheduling_router
from _probesAPI import register_probes, PROBE_CACHE as PROBES_CACHE, STATUS_CACHE as PROBES_STATUS_CACHE
from _scrobbleAPI import router as scrobble_router
from _authenticationAPI import register_auth
from _wallAPI import register_wall
from _versionAPI import router as version_router
from _analyzer import router as analyzer_router

from _syncAPI import (
    router as sync_router,
    _is_sync_running,
    _load_state,
    _compute_lanes_from_stats,
    _lane_is_empty,
    _parse_epoch,
)

from _watchlist import build_watchlist, _get_provider_items, _load_hide_set, _save_hide_set
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import Body, FastAPI, Query, Request, Path as FPath
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    Response,
    StreamingResponse,
)
from fastapi.staticfiles import StaticFiles

try:
    from plexapi.myplex import MyPlexAccount
    HAVE_PLEXAPI = True
except Exception:
    HAVE_PLEXAPI = False
    
try:
    from _wallAPI import _load_wall_snapshot, refresh_wall
except Exception:
    pass

from packaging.version import InvalidVersion, Version
from pydantic import BaseModel

from providers.scrobble.scrobble import Dispatcher, from_plex_webhook
from providers.scrobble.trakt.sink import TraktSink
from providers.scrobble.plex.watch import WatchService, autostart_from_config

# Plex → Trakt
try:
    from providers.webhooks.plextrakt import process_webhook as process_webhook
except Exception:
    process_webhook = None

# Jellyfin → Trakt
try:
    from providers.webhooks.jellyfintrakt import process_webhook as process_webhook_jellyfin
except Exception:
    process_webhook_jellyfin = None 

__all__ = ["process_webhook", "process_webhook_jellyfin"]

from _FastAPI import (
    get_index_html,
    register_assets_and_favicons,
    register_ui_root,
)
from _scheduling import SyncScheduler
from _statistics import Stats
from _watchlist import build_watchlist, delete_watchlist_item

from cw_platform.orchestrator import Orchestrator, minimal
from cw_platform.modules_registry import MODULES as _MODULES
from cw_platform.config_base import load_config, save_config, CONFIG as CONFIG_DIR
from cw_platform.orchestrator import canonical_key
from cw_platform import config_base

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# --- Paths & globals
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

STATE_DIR = CONFIG_DIR
STATE_DIR.mkdir(parents=True, exist_ok=True)

STATE_PATH      = (STATE_DIR / "state.json").resolve()
TOMBSTONES_PATH = (STATE_DIR / "tombstones.json").resolve()
LAST_SYNC_PATH  = (STATE_DIR / "last_sync.json").resolve()

REPORT_DIR = (CONFIG_DIR / "sync_reports"); REPORT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR  = (CONFIG_DIR / "cache");        CACHE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATHS = [CONFIG_DIR / "state.json", ROOT / "state.json"]
HIDE_PATH   = (CONFIG_DIR / "watchlist_hide.json")
CW_STATE_DIR = (CONFIG_DIR / ".cw_state"); CW_STATE_DIR.mkdir(parents=True, exist_ok=True)

_METADATA: Any = None
WATCH: Optional[WatchService] = None
DISPATCHER: Optional[Dispatcher] = None
scheduler: Optional[SyncScheduler] = None

STATS = Stats()

_DEBUG_CACHE = {"ts": 0.0, "val": False}
_DEBUG_HTTP_CACHE = {"ts": 0.0, "val": False}
_DEBUG_MODS_CACHE = {"ts": 0.0, "val": False}

RUNNING_PROCS: Dict[str, threading.Thread] = {}
SYNC_PROC_LOCK = threading.Lock()

# --- Debug flag (reads config, cached)
def _is_http_debug_enabled() -> bool:
    try:
        now = time.time()
        if now - _DEBUG_HTTP_CACHE["ts"] > 2.0:
            cfg = load_config()
            _DEBUG_HTTP_CACHE["val"] = bool(((cfg.get("runtime") or {}).get("debug_http") or False))
            _DEBUG_HTTP_CACHE["ts"] = now
        return _DEBUG_HTTP_CACHE["val"]
    except Exception:
        return False
      
def _is_debug_enabled() -> bool:
    try:
        now = time.time()
        if now - _DEBUG_CACHE["ts"] > 2.0:
            cfg = load_config()
            _DEBUG_CACHE["val"] = bool(((cfg.get("runtime") or {}).get("debug") or False))
            _DEBUG_CACHE["ts"] = now
        return _DEBUG_CACHE["val"]
    except Exception:
        return False
    
def _is_static_noise(path: str, status: int) -> bool:
    if path.startswith("/assets/") or path.startswith("/favicon"):
        return True
    if path.endswith((".css", ".js", ".mjs", ".map", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".svg", ".woff", ".woff2", ".ttf")):
        return True

    # Redirects / caching noise
    if status in (301, 302, 303, 304, 307, 308):
        return True

    # Benign 404s: artwork lookups and placeholders
    if status == 404 and (
        path.startswith("/art/") or
        path.startswith("/assets/img/") or
        "/placeholder" in path
    ):
        return True

    # Ignore framework noise
    if request_method := None:
        try:
            pass
        except Exception:
            pass

    return False

def _is_mods_debug_enabled() -> bool:
    try:
        now = time.time()
        if now - _DEBUG_MODS_CACHE["ts"] > 2.0:
            cfg = load_config()
            _DEBUG_MODS_CACHE["val"] = bool(((cfg.get("runtime") or {}).get("debug_mods") or False))
            _DEBUG_MODS_CACHE["ts"] = now
        return _DEBUG_MODS_CACHE["val"]
    except Exception:
        return False

def _apply_debug_env_from_config() -> None:
    """Mirror runtime.debug_mods to CW_DEBUG env."""
    on = _is_mods_debug_enabled()
    if on and not os.environ.get("CW_DEBUG"):
        os.environ["CW_DEBUG"] = "1"
    elif not on and os.environ.get("CW_DEBUG"):
        os.environ.pop("CW_DEBUG", None)
        
_apply_debug_env_from_config()

# --- Next scheduled run helper (reads config, returns epoch)
_SCHED_HINT: Dict[str, int] = {"next_run_at": 0, "last_saved_at": 0}

def _compute_next_run_from_cfg(scfg: dict, now_ts: int | None = None) -> int:
    now = int(time.time()) if now_ts is None else int(now_ts)
    if not scfg or not scfg.get("enabled"):
        return 0

    mode = (scfg.get("mode") or "every_n_hours").lower()

    if mode == "every_n_hours":
        n = max(1, int(scfg.get("every_n_hours") or 1))
        return now + n * 3600

    if mode == "daily_time":
        hh, mm = ("03", "30")
        try:
            hh, mm = (scfg.get("daily_time") or "03:30").split(":")
        except Exception:
            pass

        tz = None
        try:
            tzname = scfg.get("timezone")
            if tzname and ZoneInfo:
                tz = ZoneInfo(tzname)
        except Exception:
            tz = None

        base = datetime.fromtimestamp(now, tz) if tz else datetime.fromtimestamp(now)
        target = base.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        if target.timestamp() <= now:
            target = target + timedelta(days=1)
        return int(target.timestamp())

    # fallback
    return now + 3600

# --- App & routers
app = FastAPI()

@app.middleware("http")
async def conditional_access_logger(request: Request, call_next):
    _apply_debug_env_from_config()
    t0 = time.time()
    client = request.client
    response = None
    err = None
    try:
        response = await call_next(request)
        status = getattr(response, "status_code", 0) or 0
    except Exception as e:
        err = e
        status = 500
    finally:
        if not _is_http_debug_enabled():
            path = request.url.path
            if err is None and _is_static_noise(path, status):
                pass
            else:
                should_log = (err is not None) or (status >= 500)
                if not should_log and _is_debug_enabled() and status >= 400:
                    should_log = True
                if should_log:
                    dt_ms = int((time.time() - t0) * 1000)
                    host = f"{client.host}:{client.port}" if client else "-"
                    path_qs = path + (f"?{request.url.query}" if request.url.query else "")
                    proto = f"HTTP/{request.scope.get('http_version','1.1')}"
                    print(f'{host} - "{request.method} {path_qs} {proto}" {status} ({dt_ms} ms)')
        # else: full access logs handled by uvicorn when debug_http=true

    if err is not None:
        raise err
    return response

app.include_router(config_router)
app.include_router(meta_router)
app.include_router(watchlist_router)
app.include_router(maintenance_router)
app.include_router(scheduling_router)
app.include_router(scrobble_router)
app.include_router(sync_router)
app.include_router(version_router)
app.include_router(analyzer_router)

# --- Static/UI
register_assets_and_favicons(app, ROOT)
register_ui_root(app)

# --- Probes / insights / wall
register_probes(app, load_config)
register_insights(app)
register_wall(app)

# --- Misc util
def get_primary_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80)); return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

# --- Log buffers + ANSI helpers (for UI streaming)
MAX_LOG_LINES = 3000
LOG_BUFFERS: Dict[str, List[str]] = {"SYNC": [], "PLEX": [], "SIMKL": [], "TRBL": [], "TRAKT": []}

ANSI_RE    = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_STRIP = re.compile(r"\x1b\[[0-9;]*m")
_FG_CODES = {"30","31","32","33","34","35","36","37","90","91","92","93","94","95","96","97"}
_BG_CODES = {"40","41","42","43","44","45","46","47","100","101","102","103","104","105","106","107"}

def _escape_html(s: str) -> str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def strip_ansi(s: str) -> str:
    return ANSI_STRIP.sub("", s)

def ansi_to_html(line: str) -> str:
    out, pos = [], 0
    state = {"b": False, "u": False, "fg": None, "bg": None}
    span_open = False

    def state_classes():
        cls = []
        if state["b"]: cls.append("b")
        if state["u"]: cls.append("u")
        if state["fg"]: cls.append(f"c{state['fg']}")
        if state["bg"]: cls.append(f"bg{state['bg']}")
        return cls

    for m in ANSI_RE.finditer(line):
        if m.start() > pos:
            out.append(_escape_html(line[pos:m.start()]))

        codes = [c for c in (m.group(1) or "").split(";") if c != ""]
        if codes:
            for c in codes:
                if c == "0": state.update({"b": False, "u": False, "fg": None, "bg": None})
                elif c == "1": state["b"] = True
                elif c == "22": state["b"] = False
                elif c == "4": state["u"] = True
                elif c == "24": state["u"] = False
                elif c in _FG_CODES: state["fg"] = c
                elif c == "39": state["fg"] = None
                elif c in _BG_CODES: state["bg"] = c
                elif c == "49": state["bg"] = None

            if span_open:
                out.append("</span>")
                span_open = False
            cls = state_classes()
            if cls:
                out.append(f'<span class="{" ".join(cls)}">')
                span_open = True

        pos = m.end()

    if pos < len(line):
        out.append(_escape_html(line[pos:]))

    if span_open:
        out.append("</span>")

    return "".join(out)

def _append_log(tag: str, raw_line: str) -> None:
    html = ansi_to_html(raw_line.rstrip("\n"))
    buf = LOG_BUFFERS.setdefault(tag, [])
    buf.append(html)
    if len(buf) > MAX_LOG_LINES:
        LOG_BUFFERS[tag] = buf[-MAX_LOG_LINES:]

register_auth(app, log_fn=_append_log, probe_cache=PROBES_CACHE)      

# --- Expose buffers for other modules
try:
    app.state.LOG_BUFFERS = LOG_BUFFERS
    app.state.MAX_LOG_LINES = MAX_LOG_LINES
except Exception:
    pass

# --- Host logger (writes to UI buffers)
class _UIHostLogger:
    def __init__(self, tag: str = "SYNC", module_name: str | None = None, base_ctx: dict | None = None):
        self._tag = tag
        self._module = module_name
        self._ctx = dict(base_ctx or {})

    def __call__(self, message: str, *, level: str = "INFO", module: str | None = None, extra: dict | None = None) -> None:
        m = module or self._module or self._ctx.get("module")
        lvl = (level or "INFO").upper()
        if lvl == "DEBUG" and not _is_debug_enabled():
            return
        prefix_mod = f"[{m}]" if m else ""
        try:
            _append_log(self._tag, f"{lvl} {prefix_mod} {message}".strip())
        except Exception:
            print(f"{self._tag}: {lvl} {prefix_mod} {message}")

    def set_context(self, **ctx):
        self._ctx.update(ctx)

    def get_context(self) -> dict:
        return dict(self._ctx)

    def bind(self, **ctx):
        c = dict(self._ctx); c.update(ctx)
        return _UIHostLogger(self._tag, self._module, c)

    def child(self, name: str):
        return _UIHostLogger(self._tag, name, dict(self._ctx))

# --- Orchestrator helpers
def _get_orchestrator() -> Orchestrator:
    cfg = load_config()
    return Orchestrator(config=cfg)

def _json_safe(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)

# --- Startup/Shutdown
@asynccontextmanager
async def _lifespan(app):
    app.state.watch = None
    _apply_debug_env_from_config()

    try:
        fn = globals().get("_on_startup")
        if callable(fn):
            res = fn()
            try:
                import inspect
                if inspect.iscoroutine(res):
                    await res
            except Exception:
                pass
    except Exception as e:
        try: _UIHostLogger("TRAKT", "WATCH")(f"startup hook error: {e}", level="ERROR")
        except Exception: pass

    started = False
    try:
        w = autostart_from_config()
        if w:
            app.state.watch = w
            globals()['WATCH'] = w
            started = bool(getattr(w, "is_alive", lambda: False)())
            _UIHostLogger("TRAKT", "WATCH")(
                "watch autostarted" if started else "watch autostart returned but not alive",
                level="INFO"
            )
        else:
            _UIHostLogger("TRAKT", "WATCH")("autostart_from_config() returned None", level="INFO")
    except Exception as e:
        try: _UIHostLogger("TRAKT", "WATCH")(f"autostart_from_config failed: {e}", level="ERROR")
        except Exception: pass

    if not started:
        try:
            cfg = load_config()
            sc = (cfg.get("scrobble") or {})
            if bool(sc.get("enabled")) and (sc.get("mode") or "").lower() == "watch" and bool((sc.get("watch") or {}).get("autostart")):
                from providers.scrobble.trakt.sink import TraktSink
                from providers.scrobble.plex.watch import make_default_watch
                w2 = make_default_watch(sinks=[TraktSink()])

                try:
                    filters = ((sc.get("watch") or {}).get("filters") or {})
                    if hasattr(w2, "set_filters") and isinstance(filters, dict):
                        w2.set_filters(filters)
                except Exception:
                    pass

                if hasattr(w2, "start_async"):
                    w2.start_async()
                else:
                    import threading
                    threading.Thread(target=w2.start, daemon=True).start()

                app.state.watch = w2
                globals()['WATCH'] = w2
                started = True
                _UIHostLogger("TRAKT", "WATCH")("fallback default watch started", level="INFO")
        except Exception as e:
            try: _UIHostLogger("TRAKT", "WATCH")(f"fallback start failed: {e}", level="ERROR")
            except Exception: pass

    try:
        global scheduler
        if scheduler is not None:
            scheduler.start()  # idempotent
            scfg = (load_config().get("scheduling") or {})
            if bool(scfg.get("enabled")) and hasattr(scheduler, "refresh"):
                scheduler.refresh()
            _UIHostLogger("SYNC")("scheduler: started & refreshed", level="INFO")
    except Exception as e:
        try: _UIHostLogger("SYNC")(f"scheduler startup error: {e}", level="ERROR")
        except Exception: pass

    try:
        yield
    finally:
        try:
            fn2 = globals().get("_on_shutdown")
            if callable(fn2):
                res2 = fn2()
                try:
                    import inspect
                    if inspect.iscoroutine(res2):
                        await res2
                except Exception:
                    pass
        except Exception as e:
            try: _UIHostLogger("TRAKT", "WATCH")(f"shutdown hook error: {e}", level="ERROR")
            except Exception: pass

        try:
            w = getattr(app.state, "watch", None) or (WATCH if 'WATCH' in globals() else None)
            if w:
                w.stop()
                _UIHostLogger("TRAKT", "WATCH")("watch stopped", level="INFO")
            app.state.watch = None
            if 'WATCH' in globals():
                globals()['WATCH'] = None
        except Exception as e:
            try: _UIHostLogger("TRAKT", "WATCH")(f"watch stop failed: {e}", level="ERROR")
            except Exception: pass

app.router.lifespan_context = _lifespan
    
# --- Basic middleware: no-store for API endpoints and MODALS
@app.middleware("http")
async def cache_headers_for_api(request: Request, call_next):
    resp = await call_next(request)
    path = request.url.path

    if path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    elif path == "/assets/js/modals.js" or path.startswith("/assets/js/modals/"):
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp

# --- Logs (with tags for OpenAPI)
@app.get("/api/logs/dump", tags=["logging"])
def logs_dump(channel: str = "TRAKT", n: int = 50):
    return {"channel": channel, "lines": LOG_BUFFERS.get(channel, [])[-n:]}

@app.get("/api/logs/stream", tags=["logging"])
async def api_logs_stream_initial(request: Request, tag: str = Query("SYNC")):
    tag = (tag or "SYNC").upper()

    async def agen():
        buf = LOG_BUFFERS.get(tag, [])
        for line in buf:
            yield f"data: {line}\n\n"
        idx = len(buf)
        while True:
            if await request.is_disconnected():
                break
            new_buf = LOG_BUFFERS.get(tag, [])
            while idx < len(new_buf):
                yield f"data: {new_buf[idx]}\n\n"
                idx += 1
            await asyncio.sleep(0.25)

    return StreamingResponse(agen(), media_type="text/event-stream", headers={"Cache-Control": "no-store"})

# --- Sync runner (called by scheduler)
def _run_pairs_thread(run_id: str, overrides: dict | None = None) -> None:
    overrides = overrides or {}

    def _log(msg: str):
        _append_log("SYNC", msg)

    _log(f"> SYNC start: orchestrator pairs run_id={run_id}")

    try:
        import importlib
        orch_mod = importlib.import_module("cw_platform.orchestrator")
        try:
            orch_mod = importlib.reload(orch_mod)
        except Exception:
            pass

        OrchestratorClass = getattr(orch_mod, "Orchestrator")
        _log(f"[i] Orchestrator module: {getattr(orch_mod, '__file__', '?')}")

        cfg = load_config()
        mgr = OrchestratorClass(config=cfg)

        # dry-run resolution: config OR override
        dry_cfg = bool(((cfg.get("sync") or {}).get("dry_run") or False))
        dry_ovr = bool(overrides.get("dry_run"))
        dry = dry_cfg or dry_ovr

        result = mgr.run_pairs(
            dry_run=dry,
            progress=_append_log.__get__(None, type(_append_log)) if False else _append_log,
            write_state_json=True,
            state_path=STATE_PATH,
            use_snapshot=True,  # kept for compatibility, doesnt do anything now
            overrides=overrides,
        )

        added = int(result.get("added", 0))
        removed = int(result.get("removed", 0))

        try:
            state = _load_state()
            if state:
                STATS.refresh_from_state(state)
                STATS.record_summary(added, removed)
            else:
                _append_log("SYNC", "[!] No state found after sync; stats not updated.")
        except Exception as e:
            _append_log("SYNC", f"[!] Stats update failed: {e}")

        _log(f"[i] Done. Total added: {added}, Total removed: {removed}")
        _log("[SYNC] exit code: 0")

    except Exception as e:
        _append_log("SYNC", f"[!] Sync error: {e}")
        _append_log("SYNC", "[SYNC] exit code: 1")
    finally:
        RUNNING_PROCS.pop("SYNC", None)
        
# --- Scheduler wiring
def _start_sync_from_scheduler() -> bool:
    if _is_sync_running():
        return False
    run_id = str(int(time.time()))
    th = threading.Thread(target=_run_pairs_thread, args=(run_id,), daemon=True)
    th.start()
    RUNNING_PROCS["SYNC"] = th
    return True

scheduler = SyncScheduler(
    load_config, save_config,
    run_sync_fn=_start_sync_from_scheduler,
    is_sync_running_fn=_is_sync_running,
)

# --- Platform / Metadata managers
try:
    from cw_platform.manager import PlatformManager as _PlatformMgr
    _PLATFORM = _PlatformMgr(load_config, save_config)
except Exception as _e:
    _PLATFORM = None
    print("PlatformManager not available:", _e)

try:
    from cw_platform.metadata import MetadataManager as _MetadataMgr
    _METADATA = _MetadataMgr(load_config, save_config)
except Exception as _e:
    _METADATA = None
    print("MetadataManager not available:", _e)

# --- Entrypoint
def main(host: str = "0.0.0.0", port: int = 8787) -> None:
    ip = get_primary_ip()
    print("\nCrossWatch Engine running:")
    print(f"  Local:   http://127.0.0.1:{port}")
    print(f"  Docker:  http://{ip}:{port}")
    print(f"  Bind:    {host}:{port}")
    print(f"  Config:  {CONFIG_DIR / 'config.json'} (JSON)")
    print(f"  Cache:   {CACHE_DIR}")
    print(f"  Reports: {REPORT_DIR}\n")

    cfg = load_config()
    debug = bool((cfg.get("runtime") or {}).get("debug"))
    debug_http = bool((cfg.get("runtime") or {}).get("debug_http"))

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=("debug" if debug else "warning"),
        access_log=debug_http,
    )

if __name__ == "__main__":
    main()
