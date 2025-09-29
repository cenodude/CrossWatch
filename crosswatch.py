# --------------- CrossWatch Web API (FastAPI):  ---------------
# Refactoring project: crosswatch.py (v0.1)
from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional, Tuple

# --------------- Imports ---------------
from contextlib import asynccontextmanager
from datetime import datetime, date, timedelta, timezone

from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs
from importlib import import_module

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

from _maintenanceAPI import router as maintenance_router, compat as troubleshoot_router
from _metaAPI import router as meta_router
from _insightAPI import register_insights
from _watchlistAPI import router as watchlist_router
from _schedulingAPI import router as scheduling_router
from _probesAPI import register_probes, PROBE_CACHE as PROBES_CACHE, STATUS_CACHE as PROBES_STATUS_CACHE
from _scrobbleAPI import router as scrobble_router
from _authenticationAPI import register_auth
from _syncAPI import router as sync_router
from _wallAPI import register_wall

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

from providers.webhooks.trakt import process_webhook
from packaging.version import InvalidVersion, Version
from pydantic import BaseModel

from providers.scrobble.scrobble import Dispatcher, from_plex_webhook
from providers.scrobble.trakt.sink import TraktSink
from providers.scrobble.plex.watch import WatchService, autostart_from_config

from _FastAPI import get_index_html
from _scheduling import SyncScheduler
from _statistics import Stats
from _watchlist import build_watchlist, delete_watchlist_item

from cw_platform.orchestrator import Orchestrator, minimal
from cw_platform.config_base import load_config, save_config, CONFIG as CONFIG_DIR
from cw_platform.orchestrator import canonical_key
from cw_platform import config_base

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# --------------- Constants & basic paths ---------------
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

FEATURE_KEYS = ["watchlist", "ratings", "history", "playlists"]

_METADATA: Any = None
WATCH: Optional[WatchService] = None
DISPATCHER: Optional[Dispatcher] = None
scheduler: Optional[SyncScheduler] = None

STATS = Stats()

_DEBUG_CACHE = {"ts": 0.0, "val": False}
RUNNING_PROCS: Dict[str, threading.Thread] = {}
SYNC_PROC_LOCK = threading.Lock()

_SCHED_HINT: Dict[str, int] = {"next_run_at": 0, "last_saved_at": 0}


# --------------- Helper: compute next schedule run from config ---------------
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

# --------------- Helper: media normalizer---
def _norm_media_type(x: Optional[str]) -> str:
    t = (x or "").strip().lower()
    return "tv" if t in {"tv", "show", "shows", "series", "season", "episode"} else "movie"

# --------------- App & assets ---------------

# --- app setup
app = FastAPI()
ASSETS_DIR = ROOT / "assets"
ASSETS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")

# --- routers
app.include_router(meta_router)
app.include_router(watchlist_router)
app.include_router(maintenance_router)
app.include_router(troubleshoot_router)
app.include_router(scheduling_router)
app.include_router(scrobble_router)
app.include_router(sync_router)

# --- probes
register_probes(app, load_config)

# --- insights
register_insights(app)

# --- wall
register_wall(app)

# --------------- Logging buffers & ANSI helpers ---------------
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
    
    
# --- Module versions API ------------------------------------------------------
from importlib import import_module

_MODULES = {
    "AUTH": {
        "_auth_PLEX":  "providers.auth._auth_PLEX",
        "_auth_SIMKL": "providers.auth._auth_SIMKL",
        "_auth_TRAKT": "providers.auth._auth_TRAKT",
        "_auth_JELLYFIN": "providers.auth._auth_JELLYFIN",
    },
    "SYNC": {
        "_mod_PLEX":   "providers.sync._mod_PLEX",
        "_mod_SIMKL":  "providers.sync._mod_SIMKL",
        "_mod_TRAKT":  "providers.sync._mod_TRAKT",
        "_mod_JELLYFIN":  "providers.sync._mod_JELLYFIN",
    },
}

def _get_module_version(mod_path: str) -> str:
    """Return __VERSION__ (fallback to VERSION/__version__/0.0.0)."""
    try:
        m = import_module(mod_path)
        return str(
            getattr(m, "__VERSION__", getattr(m, "VERSION", getattr(m, "__version__", "0.0.0")))
        )
    except Exception:
        return "0.0.0"

@app.get("/api/modules/versions")
def get_module_versions():
    groups = {}
    for group, mods in _MODULES.items():
        groups[group] = {name: _get_module_version(path) for name, path in mods.items()}
    flat = {name: ver for mods in groups.values() for name, ver in mods.items()}
    return {"groups": groups, "flat": flat}

# --------------- API models ---------------
class MetadataResolveIn(BaseModel):
    entity: str
    ids: dict
    locale: str | None = None
    need: dict | None = None
    strategy: str | None = "first_success"


class PairIn(BaseModel):
    source: str
    target: str
    mode: str | None = None
    enabled: bool | None = None
    features: dict | None = None
    
class PairPatch(BaseModel):
    source: str | None = None
    target: str | None = None
    mode: str | None = None
    enabled: bool | None = None
    features: dict | None = None

# --------------- Orchestrator helpers ---------------
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


def _normalize_features(f: dict | None) -> dict:
    f = dict(f or {})
    for k in FEATURE_KEYS:
        v = f.get(k)
        if isinstance(v, bool):
            f[k] = {"enable": bool(v), "add": bool(v), "remove": False}
        elif isinstance(v, dict):
            v.setdefault("enable", True)
            v.setdefault("add", True)
            v.setdefault("remove", False)
    return f


def _cfg_pairs(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    arr = cfg.get("pairs")
    if not isinstance(arr, list):
        arr = []
        cfg["pairs"] = arr
    return arr


def _gen_id(prefix: str = "pair") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


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


# --------------- App lifespan (startup/shutdown) ---------------
@asynccontextmanager
async def _lifespan(app):
    app.state.watch = None

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
            globals()['WATCH'] = w    # keep old debug endpoints happy
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
        # --- Shutdown hook ---
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

        # --- Stop watcher (supports both storages) ---
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

# --------------- Orchestrator progress & summary ---------------
SUMMARY_LOCK = threading.Lock()
SUMMARY: Dict[str, Any] = {}


def _summary_reset() -> None:
    with SUMMARY_LOCK:
        SUMMARY.clear()
        SUMMARY.update({
            "running": False,
            "started_at": None,
            "finished_at": None,
            "duration_sec": None,
            "cmd": "",
            "version": "",
            "plex_pre": None,
            "simkl_pre": None,
            "plex_post": None,
            "simkl_post": None,
            "result": "",
            "exit_code": None,
            "timeline": {"start": False, "pre": False, "post": False, "done": False},
            "raw_started_ts": None,
        })


def _summary_set(k: str, v: Any) -> None:
    with SUMMARY_LOCK:
        SUMMARY[k] = v


def _summary_set_timeline(flag: str, value: bool = True) -> None:
    with SUMMARY_LOCK:
        SUMMARY.setdefault("timeline", {})
        SUMMARY["timeline"][flag] = value


def _summary_snapshot() -> Dict[str, Any]:
    with SUMMARY_LOCK:
        return dict(SUMMARY)


def _sync_progress_ui(msg: str):
    try:
        _append_log("SYNC", msg)
        try:
            _parse_sync_line(strip_ansi(msg))
        except Exception as e:
            _append_log("SYNC", f"[!] progress-parse failed: {e}")
    except Exception:
        pass


def _orc_progress(event: str, data: dict):
    try:
        payload = json.dumps({"event": event, **(data or {})}, default=str)
    except Exception:
        payload = f"{event} | {data}"
    _append_log("SYNC", payload[:2000])


def _feature_enabled(fmap: dict, name: str) -> tuple[bool, bool]:
    d = dict(fmap.get(name) or {})
    if isinstance(fmap.get(name), bool):
        return bool(fmap[name]), False
    return bool(d.get("enable", False)), bool(d.get("remove", False))


def _item_sig_key(v: dict) -> str:
    try:
        return canonical_key(v)
    except Exception:
        # Fallback (oude logica)
        ids = (v.get("ids") or {})
        for k in ("tmdb", "imdb", "tvdb", "slug"):
            val = ids.get(k)
            if val:
                return f"{k}:{val}".lower()
        t = (str(v.get("title") or v.get("name") or "")).strip().lower()
        y = str(v.get("year") or v.get("release_year") or "")
        typ = (v.get("type") or "").lower()
        return f"{typ}|title:{t}|year:{y}"



def _persist_state_via_orc(orc: Orchestrator, *, feature: str = "watchlist") -> dict:
    snaps = orc.build_snapshots(feature=feature)
    providers: Dict[str, Any] = {}
    wall: List[dict] = []
    seen = set()

    for prov, idx in (snaps or {}).items():
        items_min = {k: minimal(v) for k, v in (idx or {}).items()}
        providers[prov] = {
            feature: {
                "baseline": {"items": items_min},
                "checkpoint": None,
            }
        }
        for item in items_min.values():
            key = _item_sig_key(item)
            if key in seen:
                continue
            seen.add(key)
            wall.append(minimal(item))

    state = {
        "providers": providers,
        "wall": wall,
        "last_sync_epoch": int(time.time()),
    }
    orc.files.save_state(state)
    return state



def _run_pairs_thread(run_id: str, overrides: dict | None = None) -> None:
    overrides = overrides or {}
    _summary_reset()

    LOG_BUFFERS["SYNC"] = []
    _sync_progress_ui("::CLEAR::")
    _sync_progress_ui(f"> SYNC start: orchestrator pairs run_id={run_id}")

    try:
        import importlib
        orch_mod = importlib.import_module("cw_platform.orchestrator")
        try:
            orch_mod = importlib.reload(orch_mod)
        except Exception:
            pass

        OrchestratorClass = getattr(orch_mod, "Orchestrator")
        _sync_progress_ui(f"[i] Orchestrator module: {getattr(orch_mod, '__file__', '?')}")

        cfg = load_config()
        mgr = OrchestratorClass(config=cfg)

        dry_cfg = bool(((cfg.get("sync") or {}).get("dry_run") or False))
        dry_ovr = bool((overrides or {}).get("dry_run"))
        dry = dry_cfg or dry_ovr

        result = mgr.run_pairs(
            dry_run=dry,
            progress=_sync_progress_ui,
            write_state_json=True,
            state_path=STATE_PATH,
            use_snapshot=True,            # kept for API compatibility (ignored by orchestrator)
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

        _sync_progress_ui(f"[i] Done. Total added: {added}, Total removed: {removed}")
        _sync_progress_ui("[SYNC] exit code: 0")

    except Exception as e:
        _sync_progress_ui(f"[!] Sync error: {e}")
        _sync_progress_ui("[SYNC] exit code: 1")
    finally:
        RUNNING_PROCS.pop("SYNC", None)


# --------------- Progress parsing helpers ---------------
def _parse_epoch(v: Any) -> int:
    if v is None: return 0
    try:
        if isinstance(v, (int, float)): return int(v)
        s = str(v).strip()
        if s.isdigit(): return int(s)
        s = s.replace("Z","+00:00")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp())
    except Exception:
        return 0


def _lanes_defaults() -> Dict[str, Dict[str, Any]]:
    def lane():
        return {"added": 0, "removed": 0, "updated": 0,
                "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []}
    return {
        "watchlist": lane(),
        "ratings":   lane(),
        "history":   lane(),
        "playlists": lane(),
    }


def _lanes_enabled_defaults() -> Dict[str, bool]:
    return {"watchlist": True, "ratings": True, "history": True, "playlists": True}


def _compute_lanes_from_stats(since_epoch: int, until_epoch: int):
    feats = _lanes_defaults()
    enabled = _lanes_enabled_defaults()

    with STATS.lock:
        events = list(STATS.data.get("events") or [])
    if not events:
        return feats, enabled

    s = int(since_epoch or 0)
    u = int(until_epoch or 0) or int(time.time())

    def _evt_epoch(e: dict) -> int:
        for k in ("sync_ts", "ingested_ts", "seen_ts", "ts"):
            try:
                v = int(e.get(k) or 0)
                if v:
                    return v
            except Exception:
                pass
        return 0

    rows = [e for e in events if s <= _evt_epoch(e) <= u]
    if not rows:
        return feats, enabled

    rows.sort(key=lambda r: int(r.get("ts") or 0))

    for e in rows:
        # Normalized action/feature
        raw_action = str(e.get("action") or e.get("op") or e.get("change") or "").lower()
        raw_feat   = str(e.get("feature") or e.get("feat") or "").lower()
        action = raw_action.replace(":", "_").replace("-", "_")
        feat   = raw_feat.replace(":", "_").replace("-", "_")

        title = e.get("title") or e.get("key") or "item"
        slim  = {k: e.get(k) for k in ("title","key","type","source","ts") if k in e}

        # WATCHLIST
        if action in ("add","remove") and (feat in ("watchlist","") or "watchlist" in action):
            lane = "watchlist"
            if action == "add":
                feats[lane]["added"] += 1
                feats[lane]["spotlight_add"].append(slim or {"title": title})
            else:
                feats[lane]["removed"] += 1
                feats[lane]["spotlight_remove"].append(slim or {"title": title})
            continue

        # RATINGS
        if action in ("rate","rating","update_rating","unrate") or "rating" in feat:
            lane = "ratings"
            # treat any rating change as updated (UI shows ~)
            feats[lane]["updated"] += 1
            feats[lane]["spotlight_update"].append(slim or {"title": title})
            continue

        # HISTORY
        is_history_feat = feat in ("history","watch","watched") or ("history" in action)
        is_add_like     = any(k in action for k in ("watch","scrobble","checkin","mark_watched","history_add","add_history"))
        is_remove_like  = any(k in action for k in ("unwatch","remove_history","history_remove","delete_watch","del_history"))

        if is_history_feat or is_add_like or is_remove_like:
            lane = "history"
            if is_remove_like:
                feats[lane]["removed"] += 1
                feats[lane]["spotlight_remove"].append(slim or {"title": title})
            else:
                feats[lane]["added"] += 1
                feats[lane]["spotlight_add"].append(slim or {"title": title})
            continue

        if action.startswith("playlist") or "playlist" in feat:
            lane = "playlists"
            if "remove" in action:
                feats[lane]["removed"] += 1
                feats[lane]["spotlight_remove"].append(slim or {"title": title})
            elif "update" in action or "rename" in action:
                feats[lane]["updated"] += 1
                feats[lane]["spotlight_update"].append(slim or {"title": title})
            else:
                feats[lane]["added"] += 1
                feats[lane]["spotlight_add"].append(slim or {"title": title})
            continue

    # keep only last 3 spotlight items
    for lane in feats.values():
        lane["spotlight_add"]    = (lane["spotlight_add"]    or [])[-3:]
        lane["spotlight_remove"] = (lane["spotlight_remove"] or [])[-3:]
        lane["spotlight_update"] = (lane["spotlight_update"] or [])[-3:]

    return feats, enabled

def _parse_sync_line(line: str) -> None:
    s = strip_ansi(line).strip()

    m = re.match(r"^> SYNC start:\s+(?P<cmd>.+)$", s)
    if m:
        if not SUMMARY.get("running"):
            _summary_set("running", True)
            SUMMARY["raw_started_ts"] = time.time()
            _summary_set("started_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        cmd_str = m.group("cmd")
        short_cmd = cmd_str
        try:
            parts = shlex.split(cmd_str)
            script = next((os.path.basename(p) for p in reversed(parts) if p.endswith(".py")), None)
            if script:
                short_cmd = script
            elif parts:
                short_cmd = os.path.basename(parts[0])
        except Exception:
            pass
        _summary_set("cmd", short_cmd)
        _summary_set_timeline("start", True)
        return

    m = re.search(r"Version\s+(?P<ver>[0-9][0-9A-Za-z\.\-\+_]*)", s)
    if m:
        _summary_set("version", m.group("ver"))
        return

    m = re.search(r"Pre-sync counts:\s*(?P<pairs>.+)$", s, re.IGNORECASE)
    if m:
        pairs = re.findall(r"\b([A-Za-z][A-Za-z0-9_-]*)\s*=\s*(\d+)", m.group("pairs"))
        for name, val in pairs:
            key = name.lower()
            try:
                val_i = int(val)
            except Exception:
                continue
            if key in ("plex", "simkl", "trakt"):
                _summary_set(f"{key}_pre", val_i)
        _summary_set_timeline("pre", True)
        return

    m = re.search(r"Post-sync:\s*(?P<rest>.+)$", s, re.IGNORECASE)
    if m:
        rest = m.group("rest")
        pairs = re.findall(r"\b([A-Za-z][A-Za-z0-9_-]*)\s*=\s*(\d+)", rest)
        for name, val in pairs:
            key = name.lower()
            try:
                val_i = int(val)
            except Exception:
                continue
            if key in ("plex", "simkl", "trakt"):
                _summary_set(f"{key}_post", val_i)

        mres = re.search(r"(?:→|->|=>)\s*([A-Za-z]+)", rest)
        if mres:
            _summary_set("result", mres.group(1).upper())
        _summary_set_timeline("post", True)
        return

    m = re.search(r"\[SYNC\]\s+exit code:\s+(?P<code>\d+)", s)
    if m:
        code = int(m.group("code"))
        _summary_set("exit_code", code)

        started = SUMMARY.get("raw_started_ts")
        if started:
            dur = max(0.0, time.time() - float(started))
            _summary_set("duration_sec", round(dur, 2))

        _summary_set("finished_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        _summary_set("running", False)
        _summary_set_timeline("done", True)

        try:
            tl = SUMMARY.get("timeline") or {}
            if tl.get("done"):
                if not tl.get("pre"):
                    _summary_set_timeline("pre", True)
                if not tl.get("post"):
                    _summary_set_timeline("post", True)
        except Exception:
            pass

        try:
            snap0 = _summary_snapshot()
            since = _parse_epoch(snap0.get("raw_started_ts") or snap0.get("started_at"))
            until = _parse_epoch(snap0.get("finished_at")) or int(time.time())

            feats, enabled = _compute_lanes_from_stats(since, until)
            _summary_set("features", feats)
            _summary_set("enabled",  enabled)

            a = r = u = 0
            for k, data in (feats or {}).items():
                if isinstance(enabled, dict) and enabled.get(k) is False:
                    continue
                a += int((data or {}).get("added")   or 0)
                r += int((data or {}).get("removed") or 0)
                u += int((data or {}).get("updated") or 0)
            _summary_set("added_last",   a)
            _summary_set("removed_last", r)
            _summary_set("updated_last", u)
        except Exception:
            pass

        try:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            path = REPORT_DIR / f"sync-{ts}.json"
            with path.open("w", encoding="utf-8") as f:
                json.dump(_summary_snapshot(), f, indent=2)
        except Exception:
            pass


# --------------- Misc state & wall helpers ---------------

def _load_wall_snapshot() -> list[dict]:
    try:
        if STATE_PATH.exists():
            st = json.loads(STATE_PATH.read_text("utf-8"))
            wall = st.get("wall") or []
            return list(wall) if isinstance(wall, list) else []
    except Exception:
        pass
    return []

def refresh_wall():
    state = _load_state()
    return build_watchlist(state, tmdb_ok=True)

def get_primary_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80)); return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def _find_state_path() -> Optional[Path]:
    for p in STATE_PATHS:
        if p.exists(): return p
    return None


def _load_state() -> Dict[str, Any]:
    sp = _find_state_path()
    if not sp: return {}
    try:
        return json.loads(sp.read_text(encoding="utf-8"))
    except Exception:
        return {}

# --------------- Watch logs ---------------
@app.get("/debug/watch/logs")
def debug_watch_logs(tail: int = Query(20, ge=1, le=200), tag: str = Query("TRAKT")) -> JSONResponse:
    buf = LOG_BUFFERS.get(tag.upper(), [])
    tail = max(1, min(int(tail or 20), 200))
    lines = buf[-tail:]
    return JSONResponse({"tag": tag.upper(), "tail": tail, "lines": lines}, headers={"Cache-Control": "no-store"})

# --------------- Middleware ---------------
@app.middleware("http")
async def cache_headers_for_api(request: Request, call_next):
    resp = await call_next(request)
    if request.url.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp

# --------------- Stats endpoints ---------------
@app.get("/api/stats/raw")
def api_stats_raw():
    with STATS.lock:
        return JSONResponse(json.loads(json.dumps(STATS.data)))


@app.get("/api/stats")
def api_stats() -> Dict[str, Any]:
    base = STATS.overview(None) or {}
    try:
        state = _load_state()
        if state:
            now_calc = len(Stats._build_union_map(state))
            if not base.get("now"):
                base["now"] = now_calc
    except Exception:
        pass
    return {"ok": True, **base}

# --------------- Logs ---------------
@app.get("/api/logs/dump")
def logs_dump(channel: str = "TRAKT", n: int = 50):
    return {"channel": channel, "lines": LOG_BUFFERS.get(channel, [])[-n:]}

@app.get("/api/logs/stream")
def api_logs_stream_initial(tag: str = Query("SYNC")):
    tag = (tag or "SYNC").upper()

    def gen():
        buf = LOG_BUFFERS.get(tag, [])
        for line in buf:
            yield f"data: {line}\n\n"
        idx = len(buf)
        while True:
            new_buf = LOG_BUFFERS.get(tag, [])
            while idx < len(new_buf):
                yield f"data: {new_buf[idx]}\n\n"
                idx += 1
            time.sleep(0.25)

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-store"})

# --------------- Icons ---------------
FAVICON_SVG = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">
<defs><linearGradient id="g" x1="0" y1="0" x2="64" y2="64" gradientUnits="userSpaceOnUse">
<stop offset="0" stop-color="#2de2ff"/><stop offset="0.5" stop-color="#7c5cff"/><stop offset="1" stop-color="#ff7ae0"/></linearGradient></defs>
<rect width="64" height="64" rx="14" fill="#0b0b0f"/>
<rect x="10" y="16" width="44" height="28" rx="6" fill="none" stroke="url(#g)" stroke-width="3"/>
<rect x="24" y="46" width="16" height="3" rx="1.5" fill="url(#g)"/>
<circle cx="20" cy="30" r="2.5" fill="url(#g)"/>
<circle cx="32" cy="26" r="2.5" fill="url(#g)"/>
<circle cx="44" cy="22" r="2.5" fill="url(#g)"/>
<path d="M20 30 L32 26 L44 22" fill="none" stroke="url(#g)" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
</svg>"""


@app.get("/favicon.svg", include_in_schema=False)
def favicon_svg():
    return Response(content=FAVICON_SVG, media_type="image/svg+xml")


@app.get("/favicon.ico", include_in_schema=False)
def favicon_ico():
    return Response(content=FAVICON_SVG, media_type="image/svg+xml")

# --------------- Version / update ---------------
STATUS_CACHE = {"ts": 0.0, "data": None}
STATUS_TTL = 3600
PROBE_TTL  = 30

CURRENT_VERSION = os.getenv("APP_VERSION", "v0.1.7")
REPO = os.getenv("GITHUB_REPO", "cenodude/CrossWatch")
GITHUB_API = f"https://api.github.com/repos/{REPO}/releases/latest"


def _norm(v: str) -> str:
    return re.sub(r"^\s*v", "", v.strip(), flags=re.IGNORECASE)


@lru_cache(maxsize=1)
def _cached_latest_release(_marker: int) -> dict:
    headers = {"Accept": "application/vnd.github+json", "User-Agent": "CrossWatch"}
    try:
        r = requests.get(GITHUB_API, headers=headers, timeout=8)
        r.raise_for_status()
        data = r.json()
        tag = data.get("tag_name") or ""
        latest = _norm(tag)
        html_url = data.get("html_url") or f"https://github.com/{REPO}/releases"
        notes = data.get("body") or ""
        published_at = data.get("published_at")
        return {"latest": latest, "html_url": html_url, "body": notes, "published_at": published_at}
    except Exception:
        return {"latest": None, "html_url": f"https://github.com/{REPO}/releases", "body": "", "published_at": None}


def _ttl_marker(seconds=300) -> int:
    return int(time.time() // seconds)


def _is_update_available(current: str, latest: str) -> bool:
    if not latest:
        return False
    try:
        return Version(_norm(latest)) > Version(_norm(current))
    except InvalidVersion:
        return latest != current


@app.get("/api/update")
def api_update():
    cache = _cached_latest_release(_ttl_marker(300))
    cur = _norm(CURRENT_VERSION)
    lat = cache.get("latest") or cur
    update = _is_update_available(cur, lat)
    html_url = cache.get("html_url")
    return {
        "current_version": cur,
        "latest_version": lat,
        "update_available": bool(update),
        "html_url": html_url,
        "url": html_url,
        "body": cache.get("body", ""),
        "published_at": cache.get("published_at"),
    }


@app.get("/api/version")
def get_version():
    cur = _norm(CURRENT_VERSION)
    cache = _cached_latest_release(_ttl_marker(300))
    latest = cache["latest"]
    html_url = cache["html_url"]
    return {
        "current": cur,
        "latest": latest,
        "update_available": _is_update_available(cur, latest),
        "html_url": html_url,
    }


def _ver_tuple(s: str):
    try:
        return tuple(int(p) for p in re.split(r"[^\d]+", s.strip()) if p != "")
    except Exception:
        return (0,)


@app.get("/api/version/check")
def api_version_check():
    cache = _cached_latest_release(_ttl_marker(300))
    cur = CURRENT_VERSION
    lat = cache.get("latest") or cur
    update = _ver_tuple(lat) > _ver_tuple(cur)
    return {
        "current": cur,
        "latest": lat,
        "update_available": bool(update),
        "name": None,
        "url": cache.get("html_url"),
        "notes": "",
        "published_at": None,
    }

# --------------- Start/stop/schedule sync ---------------
def _is_sync_running() -> bool:
    t = RUNNING_PROCS.get("SYNC")
    return bool(t and t.is_alive())


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

INDEX_HTML = get_index_html()

@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)

# --- Ratings spotlight helpers (summary) ---
_R_ACTION_MAP = {
    "add": "add", "rate": "add",
    "remove": "remove", "unrate": "remove",
    "update": "update", "update_rating": "update",
}

def _lane_init():
    return {
        "added": 0, "removed": 0, "updated": 0,
        "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []
    }

def _push_spotlight(lane: dict, kind: str, items: list, max3: bool = True):
    # Keeps up to 3 titles for UI
    key = {"add":"spotlight_add","remove":"spotlight_remove","update":"spotlight_update"}[kind]
    dst = lane[key]
    for it in (items or []):
        title = (it.get("title") or it.get("name") or it.get("key") or str(it))[:200]
        if title and title not in dst:
            dst.append(title)
            if max3 and len(dst) >= 3:
                break

def _ensure_feature(summary_obj: dict, feature: str) -> dict:
    feats = summary_obj.setdefault("features", {})
    lane = feats.setdefault(feature, _lane_init())
    for k, v in _lane_init().items():
        lane.setdefault(k, [] if k.startswith("spotlight_") else 0)
    return lane


# --- Summary helpers (per-feature fill + ratings fallback) ---
def _lane_is_empty(v: dict | None) -> bool:
    if not isinstance(v, dict):
        return True
    has_counts = (v.get("added") or 0) + (v.get("removed") or 0) + (v.get("updated") or 0) > 0
    has_spots  = any(v.get(k) for k in ("spotlight_add","spotlight_remove","spotlight_update"))
    return not (has_counts or has_spots)

def _push_spot_titles(dst: list, items: list, max3: bool = True):
    # Keep up to 3 human titles
    for it in (items or []):
        t = (it.get("title") or it.get("name") or it.get("key") or str(it))[:200]
        if t and t not in dst:
            dst.append(t)
            if max3 and len(dst) >= 3:
                break

def _augment_ratings_from_file(summary_obj: dict) -> None:
    """Fill Ratings lane spotlights from orchestrator snapshot; keep existing counts."""
    feats = summary_obj.setdefault("features", {})
    lane = feats.setdefault("ratings", {
        "added": 0, "removed": 0, "updated": 0,
        "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []
    })

    # Only gate on spotlights (we want to fill them even if counts exist)
    have_spots = bool(lane.get("spotlight_add") or lane.get("spotlight_remove") or lane.get("spotlight_update"))
    if have_spots and all(
        isinstance(lane.get(k), list) and lane.get(k) for k in ("spotlight_add", "spotlight_remove", "spotlight_update")
    ):
        return

    # --- helpers

    def _to_epoch(s: str | None) -> int:
        if not s or not isinstance(s, str):
            return 0
        try:
            ss = s.strip()
            if ss.endswith("Z"): ss = ss[:-1] + "+00:00"
            dt = datetime.fromisoformat(ss)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            return 0

    def _canon_key(it: dict) -> str:
        ids = (it.get("ids") or {})
        for k in ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug"):
            v = ids.get(k)
            if v is not None:
                return f"{k}:{str(v).lower()}"
        t = str(it.get("title") or it.get("name") or "").strip().lower()
        y = str(it.get("year") or "")
        typ = str(it.get("type") or "").strip().lower()
        return f"{typ}|title:{t}|year:{y}"

    def _title_of(it: dict) -> str | None:
        t = it.get("title") or it.get("name") or None
        if not t: return None
        y = it.get("year")
        return f"{t} ({y})" if y else str(t)

    def _push(dst: list, items: list, max3: bool = True):
        seen = set(dst)
        for it in items:
            title = _title_of(it) or (it.get("key") if isinstance(it.get("key"), str) else None)
            if not title or title in seen:
                continue
            dst.append(title); seen.add(title)
            if max3 and len(dst) >= 3:
                break

    # --- find snapshot file
    base_dir_candidates: list[Path] = []
    try:
        # absolute package import
        from cw_platform import config_base as _cb  # type: ignore
        if hasattr(_cb, "CONFIG_BASE"):
            base_dir_candidates.append(Path(_cb.CONFIG_BASE()))
        if hasattr(_cb, "CONFIG"):
            base_dir_candidates.append(Path(_cb.CONFIG))
    except Exception:
        try:
            # relative import fallback
            from . import config_base as _cb  # type: ignore
            if hasattr(_cb, "CONFIG_BASE"):
                base_dir_candidates.append(Path(_cb.CONFIG_BASE()))
            if hasattr(_cb, "CONFIG"):
                base_dir_candidates.append(Path(_cb.CONFIG))
        except Exception:
            pass

    # env override
    if os.getenv("CW_CONFIG_BASE"):
        base_dir_candidates.append(Path(os.getenv("CW_CONFIG_BASE")))
    base_dir_candidates.append(Path(".").resolve())

    # find ratings_changes.json
    snap_file = None
    for bd in base_dir_candidates:
        for p in (bd / "ratings_changes.json", bd / "data" / "ratings_changes.json"):
            if p.exists():
                snap_file = p
                break
        if snap_file:
            break
    if not snap_file:
        return

    # read + normalize payload
    try:
        data = json.loads(snap_file.read_text("utf-8") or "{}")
    except Exception:
        return
    if data.get("feature") != "ratings":
        return

    def _norm_lists(obj: dict) -> tuple[list, list, list]:
        if "to_A" in obj or "to_B" in obj:
            toA = obj.get("to_A") or {}
            toB = obj.get("to_B") or {}
            adds = list((toA.get("adds") or [])) + list((toB.get("adds") or []))
            upds = list((toA.get("updates") or [])) + list((toB.get("updates") or []))
            rems = list((toA.get("removes") or [])) + list((toB.get("removes") or []))
        else:
            adds = list(obj.get("adds") or [])
            upds = list(obj.get("updates") or [])
            rems = list(obj.get("removes") or [])
        return adds, upds, rems

    adds, upds, rems = _norm_lists(data)

    # de-dupe by canonical id; newest first by rated_at
    def _dedupe_and_sort(items: list) -> list:
        by_key = {}
        for it in items:
            by_key[_canon_key(it)] = it
        return sorted(by_key.values(), key=lambda x: _to_epoch(x.get("rated_at")), reverse=True)

    adds = _dedupe_and_sort(adds)
    upds = _dedupe_and_sort(upds)
    rems = _dedupe_and_sort(rems)

    # spotlights (max 3 each)
    if not lane.get("spotlight_add"):    _push(lane["spotlight_add"], adds)
    if not lane.get("spotlight_update"): _push(lane["spotlight_update"], upds)
    if not lane.get("spotlight_remove"): _push(lane["spotlight_remove"], rems)

    # counts: only fill if still zero (keep stats-based counts intact)
    if lane.get("added", 0) == 0:   lane["added"]   = len(adds)
    if lane.get("updated", 0) == 0: lane["updated"] = len(upds)
    if lane.get("removed", 0) == 0: lane["removed"] = len(rems)

# --- helpers: ratings schema (pair-level) ------------------------------------

_ALLOWED_RATING_TYPES: List[str] = ["movies", "shows", "seasons", "episodes"]
_ALLOWED_RATING_MODES: List[str] = ["only_new", "from_date", "all"]

def _ensure_pair_ratings_defaults(cfg: Dict[str, Any]) -> None:
    """
    Ensure each pair has a ratings block with UI-safe defaults (GET path only).
    """
    for p in (cfg.get("pairs") or []):
        rt = p.setdefault("features", {}).setdefault("ratings", {})
        rt.setdefault("enable", False)
        rt.setdefault("add", False)
        rt.setdefault("remove", False)
        rt.setdefault("types", ["movies", "shows"])
        rt.setdefault("mode", "only_new")
        rt.setdefault("from_date", "")

def _normalize_pair_ratings(p: Dict[str, Any]) -> None:
    """
    Validate/normalize pair.features.ratings in-place (POST path).
    Keeps types/mode/from_date in the saved config.
    """
    feats = p.setdefault("features", {})
    rt = feats.setdefault("ratings", {})
    if not isinstance(rt, dict):
        feats["ratings"] = {"enable": False, "add": False, "remove": False,
                            "types": ["movies", "shows"], "mode": "only_new", "from_date": ""}
        return

    # Booleans
    rt["enable"] = bool(rt.get("enable"))
    rt["add"]    = bool(rt.get("add"))
    rt["remove"] = bool(rt.get("remove"))

    # Types
    in_types = rt.get("types", [])
    if isinstance(in_types, str):
        in_types = [in_types]
    in_types = [str(t).strip().lower() for t in in_types if isinstance(t, str)]
    if "all" in in_types:
        norm_types = list(_ALLOWED_RATING_TYPES)
    else:
        norm_types = [t for t in _ALLOWED_RATING_TYPES if t in in_types]
        if not norm_types:
            norm_types = ["movies", "shows"]
    rt["types"] = norm_types

    # Mode + from_date
    mode = str(rt.get("mode", "only_new")).strip().lower()
    if mode not in _ALLOWED_RATING_MODES:
        mode = "only_new"
    rt["mode"] = mode

    fd = str(rt.get("from_date", "") or "").strip()
    if mode == "from_date":
        try:
            d = date.fromisoformat(fd)
        except Exception:
            d = None
        if not d or d > date.today():
            rt["mode"] = "only_new"
            rt["from_date"] = ""
        else:
            rt["from_date"] = d.isoformat()
    else:
        rt["from_date"] = ""

# --- helper: remove legacy ratings keys and migration -----------------
def _prune_legacy_ratings(cfg: Dict[str, Any]) -> None:
    """
    Remove top-level 'ratings' and 'features.ratings' (legacy schema).
    If a legacy enabled flag existed, gently migrate it to pairs that don't
    have ratings configured yet (enable+add only; never override per-pair).
    """
    legacy = dict(cfg.pop("ratings", {}) or {})
    feats  = cfg.setdefault("features", {})
    legacy_feat = dict((feats.pop("ratings", {}) or {}))

    if not legacy and legacy_feat:
        legacy = legacy_feat

    if legacy.get("enabled"):
        for p in (cfg.get("pairs") or []):
            f = p.setdefault("features", {})
            rt = f.setdefault("ratings", {"enable": False, "add": False, "remove": False})
            if not rt.get("enable"):
                rt["enable"] = True
            if "add" not in rt:
                rt["add"] = True

# --------------- Config endpoints ---------------
@app.get("/api/config")
def api_config() -> JSONResponse:
    cfg = load_config()
    _prune_legacy_ratings(cfg)
    _ensure_pair_ratings_defaults(cfg)

    # Redact secrets before sending to the browser
    try:
        cfg = config_base.redact_config(cfg)
    except Exception:
        pass

    return JSONResponse(cfg)


@app.post("/api/config")
def api_config_save(cfg: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    incoming = dict(cfg or {})
    current  = load_config()

    try:
        merged = config_base._deep_merge(current, incoming)
    except Exception:
        merged = {**current, **incoming}

    def _is_blank_or_masked(v: Any) -> bool:
        if v is None:
            return True
        s = str(v).strip()
        return s == "" or s == "••••••••"

    SECRET_PATHS = [
        ("plex", "account_token"),
        ("simkl", "access_token"), ("simkl", "refresh_token"),
        ("trakt", "client_secret"), ("trakt", "access_token"), ("trakt", "refresh_token"),
        ("tmdb", "api_key"),
        ("jellyfin", "access_token"),
    ]

    for path in SECRET_PATHS:
        cur = current
        inc = incoming
        dst = merged
        for k in path[:-1]:
            cur = cur.get(k, {}) if isinstance(cur, dict) else {}
            inc = inc.get(k, {}) if isinstance(inc, dict) else {}
            dst = dst.setdefault(k, {}) if isinstance(dst, dict) else {}
        leaf = path[-1]
        if isinstance(inc, dict) and leaf in inc and _is_blank_or_masked(inc[leaf]):
            dst[leaf] = (cur or {}).get(leaf, "")

    cfg = merged
    sc = cfg.setdefault("scrobble", {})
    sc_enabled = bool(sc.get("enabled", False))
    mode = (sc.get("mode") or "").strip().lower()
    if mode not in ("webhook", "watch"):
        legacy_webhook = bool((cfg.get("webhook") or {}).get("enabled"))
        mode = "webhook" if legacy_webhook else ("watch" if sc_enabled else "")
        if mode:
            sc["mode"] = mode

    if mode == "webhook":
        sc.setdefault("watch", {}).setdefault("autostart", bool(sc.get("watch", {}).get("autostart", False)))
    elif mode == "watch":
        pass
    else:
        sc["enabled"] = False

    # Map to features.watch.enabled
    features = cfg.setdefault("features", {})
    watch_feat = features.setdefault("watch", {})
    autostart = bool(sc.get("watch", {}).get("autostart", False))
    watch_feat["enabled"] = bool(sc_enabled and mode == "watch" and autostart)

    # Normalize pair-level ratings + prune legacy
    _prune_legacy_ratings(cfg)
    for p in (cfg.get("pairs") or []):
        try:
            _normalize_pair_ratings(p)
        except Exception:
            pass

    save_config(cfg)

    # Clear probe cache so status is fresh
    PROBES_CACHE.update({
        "plex": (0.0, False),
        "simkl": (0.0, False),
        "trakt": (0.0, False),
        "jellyfin": (0.0, False),
    })
    PROBES_STATUS_CACHE["ts"] = 0.0
    PROBES_STATUS_CACHE["data"] = None

    try:
        if hasattr(globals().get("scheduler", None), "refresh_ratings_watermarks"):
            globals()["scheduler"].refresh_ratings_watermarks()
    except Exception:
        pass

    try:
        global scheduler
        if scheduler is not None:
            s = (cfg.get("scheduling") or {})
            if bool(s.get("enabled")):
                if hasattr(scheduler, "start"):   scheduler.start()
                if hasattr(scheduler, "refresh"): scheduler.refresh()
            else:
                if hasattr(scheduler, "stop"):    scheduler.stop()
    except Exception:
        pass

    return {"ok": True}

# --------------- Run summary endpoints ---------------
@app.post("/api/run")
def api_run_sync(payload: dict | None = Body(None)) -> Dict[str, Any]:
    with SYNC_PROC_LOCK:
        if _is_sync_running():
            return {"ok": False, "error": "Sync already running"}

        cfg = load_config()
        pairs = list((cfg or {}).get("pairs") or [])
        if not any(p.get("enabled", True) for p in pairs):
            _append_log("SYNC", "[i] No pairs configured — skipping sync.")
            return {"ok": True, "skipped": "no_pairs_configured"}

        run_id = str(int(time.time()))
        th = threading.Thread(
            target=_run_pairs_thread,
            args=(run_id,),
            kwargs={"overrides": (payload or {})},
            daemon=True,
        )
        th.start()
        RUNNING_PROCS["SYNC"] = th
        _append_log("SYNC", f"[i] Triggered sync run {run_id}")
        return {"ok": True, "run_id": run_id}


@app.get("/api/run/summary")
def api_run_summary() -> JSONResponse:
    snap = _summary_snapshot()

    # Compute time window
    since = _parse_epoch(snap.get("raw_started_ts") or snap.get("started_at"))
    until = _parse_epoch(snap.get("finished_at"))
    if not until and snap.get("running"):
        until = int(time.time())

    # Features present?
    feats = snap.get("features") if isinstance(snap.get("features"), dict) else {}
    if not feats:
        # No lanes at all → compute all from stats
        stats_feats, enabled = _compute_lanes_from_stats(since, until)
        snap["features"] = stats_feats
        snap["enabled"] = enabled
    else:
        # Partially hydrate: fill empty lanes and lanes missing entirely
        empty_names = [name for name, lane in feats.items() if _lane_is_empty(lane)]
        stats_feats, enabled = _compute_lanes_from_stats(since, until)
        missing_names = [name for name in stats_feats.keys() if name not in feats]

        for name in set(empty_names + missing_names):
            if stats_feats.get(name):
                feats[name] = stats_feats[name]

        snap["features"] = feats
        snap.setdefault("enabled", enabled or _lanes_enabled_defaults())

    # Ratings fallback from orchestrator snapshot (fills spotlights; keeps counts if present)
    _augment_ratings_from_file(snap)

    # Defensive timeline consistency
    tl = snap.get("timeline") or {}
    if tl.get("done") and not tl.get("post"):
        tl["post"] = True
        tl["pre"] = True
        snap["timeline"] = tl

    return JSONResponse(snap)

@app.get("/api/run/summary/file")
def api_run_summary_file() -> Response:
    js = json.dumps(_summary_snapshot(), indent=2)
    return Response(
        content=js,
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="last_sync.json"'},
    )

@app.get("/api/run/summary/stream")
def api_run_summary_stream() -> StreamingResponse:
    def gen():
        last_key = None
        while True:
            time.sleep(0.25)
            snap = _summary_snapshot()
            _augment_ratings_from_file(snap)  # keep live spotlights in sync
            key = (
                snap.get("running"),
                snap.get("exit_code"),
                snap.get("plex_post"),
                snap.get("simkl_post"),
                snap.get("result"),
                snap.get("duration_sec"),
                (snap.get("timeline", {}) or {}).get("done"),
            )
            if key != last_key:
                last_key = key
                yield f"data: {json.dumps(snap, separators=(',',':'))}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")

# --------------- Platform & MetadataManager instances (if available) ---------------
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


# --------------- Provider item counts ---------------
def _count_provider(cfg: dict, provider: str, feature: str = "watchlist") -> int:
    try:
        orc = Orchestrator(config=cfg)
        snaps = orc.build_snapshots(feature=feature)
        return len(snaps.get(provider.upper(), {}) or {})
    except Exception:
        return 0


def _safe_get(d: dict, *path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
    return cur

def _count_plex(cfg: Dict[str, Any]) -> int:
    return _count_provider(cfg, "PLEX", feature="watchlist")

def _count_simkl(cfg: Dict[str, Any]) -> int:
    return _count_provider(cfg, "SIMKL", feature="watchlist")

def _count_trakt(cfg: Dict[str, Any]) -> int:
    return _count_provider(cfg, "TRAKT", feature="watchlist")

def _count_jellyfin(cfg: Dict[str, Any]) -> int:
    return _count_provider(cfg, "JELLYFIN", feature="watchlist")


# --------------- Main & startup ---------------
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

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=("debug" if debug else "warning"),
        access_log=debug,
    )


if __name__ == "__main__":
    main()
