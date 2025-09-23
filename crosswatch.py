# --------------- CrossWatch Web API (FastAPI): backend for status, auth, scheduling, sync, and state ---------------
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


# --------------- App & assets ---------------
# app = FastAPI(lifespan=_lifespan if "_lifespan" in globals() else None)
app = FastAPI()

ASSETS_DIR = ROOT / "assets"
ASSETS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


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
# ----------------------------------------------------------------------------- 


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


def _compute_lanes_from_stats(since_epoch: int, until_epoch: int) -> Tuple[Dict[str, Any], Dict[str, bool]]:
    feats = _lanes_defaults()
    enabled = _lanes_enabled_defaults()

    with STATS.lock:
        events = list(STATS.data.get("events") or [])
    if not events:
        return feats, enabled

    s = int(since_epoch or 0)
    u = int(until_epoch or 0) or int(time.time())
    rows = [e for e in events if s <= int(e.get("ts") or 0) <= u]
    if not rows:
        return feats, enabled

    rows.sort(key=lambda r: int(r.get("ts") or 0))

    for e in rows:
        action = str(e.get("action") or "").lower()
        title  = e.get("title") or e.get("key") or "item"
        slim   = {k: e.get(k) for k in ("title", "key", "type", "source", "ts") if k in e}

        if action in ("add", "remove"):
            lane = "watchlist"
            if action == "add":
                feats[lane]["added"] += 1
                feats[lane]["spotlight_add"].append(slim)
            else:
                feats[lane]["removed"] += 1
                feats[lane]["spotlight_remove"].append(slim)

        elif action in ("rate", "rating", "update_rating", "unrate"):
            lane = "ratings"
            feats[lane]["updated"] += 1
            feats[lane]["spotlight_update"].append(slim if slim else {"title": title})

        elif action in ("watch", "scrobble", "checkin", "mark_watched"):
            lane = "history"
            feats[lane]["added"] += 1
            feats[lane]["spotlight_add"].append(slim if slim else {"title": title})

        elif action.startswith("playlist"):
            lane = "playlists"
            if "remove" in action:
                feats[lane]["removed"] += 1
                feats[lane]["spotlight_remove"].append(slim if slim else {"title": title})
            elif "update" in action or "rename" in action:
                feats[lane]["updated"] += 1
                feats[lane]["spotlight_update"].append(slim if slim else {"title": title})
            else:
                feats[lane]["added"] += 1
                feats[lane]["spotlight_add"].append(slim if slim else {"title": title})

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
def _load_hide_set() -> set:
    return set()


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
    _ = _load_hide_set()
    return _wall_items_from_state()


def get_primary_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80)); return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()


def _clear_watchlist_hide() -> None:
    try:
        p = HIDE_PATH
        if not p.exists():
            return
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump([], f)
        tmp.replace(p)
        _append_log("SYNC", "[SYNC] watchlist_hide.json cleared")
    except Exception as e:
        _append_log("SYNC", f"[SYNC] failed to clear watchlist_hide.json: {e}")


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


def _pick_added(d: Dict[str, Any]) -> Optional[str]:
    if not isinstance(d, dict):
        return None
    for k in ("added", "added_at", "addedAt", "date_added", "created_at", "createdAt"):
        v = d.get(k)
        if v:
            try:
                if isinstance(v, (int, float)):
                    return datetime.fromtimestamp(int(v), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                return str(v)
            except Exception:
                return str(v)
    dates = d.get("dates") or d.get("meta") or d.get("attributes") or {}
    if isinstance(dates, dict):
        for k in ("added", "added_at", "created", "created_at"):
            v = dates.get(k)
            if v:
                try:
                    if isinstance(v, (int, float)):
                        return datetime.fromtimestamp(int(v), timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    return str(v)
                except Exception:
                    return str(v)
    return None


def _state_items(state: dict | None, provider: str) -> dict:
    if not isinstance(state, dict):
        return {}
    provs = state.get("providers") or {}
    P = (provs.get(provider.upper(), {}) or {})
    items = (((P.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
    if items:
        return items
    return (P.get("items") or {})


def _wall_items_from_state() -> List[Dict[str, Any]]:
    st = _load_state()
    if not st:
        return []

    plex_items   = _state_items(st, "PLEX")
    simkl_items  = _state_items(st, "SIMKL")
    trakt_items  = _state_items(st, "TRAKT")
    jelly_items  = _state_items(st, "JELLYFIN")  # <-- include Jellyfin

    def norm_type(v: dict) -> str:
        return "tv" if str(v.get("type", "").lower()) in ("show", "tv", "series") else "movie"

    def ids_of(v: dict) -> Dict[str, str]:
        ids = dict(v.get("ids") or {})
        for k in ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug"):
            if k not in ids and v.get(k):
                ids[k] = str(v[k])
        return {k: str(val) for k, val in ids.items() if val is not None}

    def sig_title_year(v: dict) -> str:
        t = (v.get("title") or v.get("name") or "").strip().lower()
        t = " ".join(t.split())
        y = v.get("year") or v.get("release_year")
        try:
            y = int(y) if y is not None else None
        except Exception:
            y = None
        return f"{t}|{y or ''}"

    def alias_keys(v: dict) -> List[str]:
        t = norm_type(v); ids = ids_of(v)
        out = []
        for k in ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug"):
            val = ids.get(k)
            if val:
                out.append(f"{t}:{k}:{val}")
        if not out:
            out.append(f"{t}:sig:{sig_title_year(v)}")
        return out

    def primary_key(v: dict) -> str:
        t = norm_type(v); ids = ids_of(v)
        for k in ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid", "slug"):
            val = ids.get(k)
            if val:
                return f"{t}:{k}:{val}"
        return f"{t}:sig:{sig_title_year(v)}"

    buckets: Dict[str, Dict[str, Any]] = {}
    alias2bucket: Dict[str, str] = {}

    def ingest(src: str, rec: dict) -> None:
        keys = alias_keys(rec)
        bucket_key = next((k for k in keys if k in alias2bucket), None)
        if bucket_key is None:
            bucket_key = primary_key(rec)
            if bucket_key in buckets:
                bucket_key = f"{bucket_key}#{len(buckets)}"
            buckets[bucket_key] = {
                "type": norm_type(rec),
                "ids": ids_of(rec),
                "title": rec.get("title") or rec.get("name") or "",
                "year": rec.get("year") or rec.get("release_year"),
                "p": None, "s": None, "t": None, "j": None,  # <-- add 'j' bucket
                "added_epoch": int((rec.get("added_epoch") or rec.get("added_ts") or 0) or 0),
            }
        b = buckets[bucket_key]
        if src == "plex":
            b["p"] = rec
        elif src == "simkl":
            b["s"] = rec
        elif src == "trakt":
            b["t"] = rec
        else:  # jellyfin
            b["j"] = rec
        b["ids"].update(ids_of(rec))
        if not b["title"]:
            b["title"] = rec.get("title") or rec.get("name") or ""
        if not b["year"]:
            b["year"] = rec.get("year") or rec.get("release_year")
        ts = int((rec.get("added_epoch") or rec.get("added_ts") or 0) or 0)
        if ts > b["added_epoch"]:
            b["added_epoch"] = ts
        for a in keys:
            alias2bucket[a] = bucket_key

    for _, v in plex_items.items():
        ingest("plex", v)
    for _, v in simkl_items.items():
        ingest("simkl", v)
    for _, v in trakt_items.items():
        ingest("trakt", v)
    for _, v in jelly_items.items():                 # <-- ingest Jellyfin
        ingest("jellyfin", v)

    out: List[Dict[str, Any]] = []
    for pk, b in buckets.items():
        ids = b["ids"]
        tmdb = ids.get("tmdb")
        try:
            tmdb = int(tmdb) if tmdb is not None and str(tmdb).isdigit() else tmdb
        except Exception:
            pass

        sources = [name for name, it in (("plex", b["p"]), ("simkl", b["s"]), ("trakt", b["t"]), ("jellyfin", b["j"])) if it]
        if len(sources) <= 1:
            status = f"{sources[0]}_only" if sources else "unknown"
        else:
            status = "both"

        p_ts = int((b["p"] or {}).get("added_epoch") or 0)
        s_ts = int((b["s"] or {}).get("added_epoch") or 0)
        t_ts = int((b["t"] or {}).get("added_epoch") or 0)
        j_ts = int((b["j"] or {}).get("added_epoch") or 0)
        # pick newest source
        newest = max((("Plex", p_ts), ("SIMKL", s_ts), ("TRAKT", t_ts), ("JELLYFIN", j_ts)), key=lambda kv: kv[1])[0]

        out.append({
            "key": pk,
            "type": "show" if b["type"] == "tv" else "movie",
            "tmdb": tmdb,
            "title": b["title"],
            "year": b["year"],
            "status": status,
            "added_epoch": int(b.get("added_epoch") or 0),
            "added_src": newest,
            "categories": [],
        })

    out.sort(key=lambda x: int(x.get("added_epoch") or 0), reverse=True)
    return out


# --------------- Plex users & identity ---------------
def _plex_token(cfg: Dict[str, Any]) -> str:
    return ((cfg.get("plex") or {}).get("account_token") or "").strip()


def _plex_client_id(cfg: Dict[str, Any]) -> str:
    return (cfg.get("plex") or {}).get("client_id") or "crosswatch"


def _account(cfg: Dict[str, Any]):
    tok = _plex_token(cfg)
    if not HAVE_PLEXAPI or not tok:
        return None
    try:
        return MyPlexAccount(token=tok)
    except Exception:
        return None


def _resolve_plex_server_uuid(cfg: Dict[str, Any]) -> str:
    # 1) explicit in config
    plex = cfg.get("plex") or {}
    if plex.get("server_uuid"):
        return str(plex["server_uuid"]).strip()

    acc = _account(cfg)
    if not acc:
        return ""

    # 2) pick an owned PMS from resources; prefer host match to plex.server_url
    host_hint = ""
    base = (plex.get("server_url") or "").strip()
    if base:
        try:
            host_hint = urllib.parse.urlparse(base).hostname or ""
        except Exception:
            host_hint = ""

    try:
        servers = [r for r in acc.resources() if "server" in (r.provides or "") and (r.product or "") == "Plex Media Server"]
        owned = [r for r in servers if getattr(r, "owned", False)]

        def matches_host(res) -> bool:
            if not host_hint:
                return False
            for c in (res.connections or []):
                if host_hint in (c.uri or "") or host_hint == (c.address or ""):
                    return True
            return False

        for res in owned:
            if matches_host(res):
                return res.clientIdentifier or ""

        if owned:
            return owned[0].clientIdentifier or ""

        return servers[0].clientIdentifier if servers else ""
    except Exception:
        return ""


def _fetch_owner_and_managed(cfg: Dict[str, Any]) -> Tuple[Optional[dict], List[dict]]:
    acc = _account(cfg)
    if not acc:
        return None, []

    # owner
    owner = {
        "id": str(getattr(acc, "id", "") or ""),
        "username": (getattr(acc, "username", "") or getattr(acc, "title", "") or getattr(acc, "email", "") or "").strip(),
        "title": (getattr(acc, "title", "") or getattr(acc, "username", "") or "").strip(),
        "email": (getattr(acc, "email", "") or "").strip(),
        "type": "owner",
    }

    # managed via PlexAPI Home
    managed: List[dict] = []
    try:
        home = getattr(acc, "home", None)
        if home:
            for u in home.users():
                uid = str(getattr(u, "id", "") or "").strip()
                if not uid:
                    continue
                uname = (getattr(u, "username", "") or getattr(u, "title", "") or "").strip()
                managed.append({
                    "id": uid,
                    "username": uname,
                    "title": (getattr(u, "title", "") or uname).strip(),
                    "email": (getattr(u, "email", "") or "").strip(),
                    "type": "managed",
                })
    except Exception:
        pass

    return owner, managed


def _list_plex_users(cfg: Dict[str, Any]) -> List[dict]:
    users: List[dict] = []
    acc = _account(cfg)

    # friends
    if acc:
        try:
            for u in acc.users():
                users.append({
                    "id": str(getattr(u, "id", "") or ""),
                    "username": (u.username or u.title or u.email or "").strip(),
                    "title": (u.title or u.username or "").strip(),
                    "email": (getattr(u, "email", "") or "").strip(),
                    "type": "friend",
                })
        except Exception:
            pass

    # owner + managed
    owner, managed = _fetch_owner_and_managed(cfg)
    if owner:
        users.append(owner)
    users.extend(managed)

    # prefer owner > managed > friend
    rank = {"owner": 3, "managed": 2, "friend": 1}
    out: Dict[str, dict] = {}
    for u in users:
        uid = str(u.get("id") or "")
        if not uid:
            continue
        cur = out.get(uid)
        if not cur or rank.get(u.get("type", "friend"), 0) >= rank.get(cur.get("type", "friend"), 0):
            out[uid] = u
    return list(out.values())
def _filter_users_with_server_access(cfg: Dict[str, Any], users: List[dict], server_uuid: str) -> List[dict]:
    """
    Lightweight heuristic: friends returned by acc.users() are shared with you (likely have access);
    managed are your home users (treat as having access). Keep both.
    """
    if not users:
        return users
    allowed_types = {"owner", "managed", "friend"}
    out = []
    for u in users:
        if u.get("type") in allowed_types:
            v = dict(u)
            v["has_access"] = True
            out.append(v)
    return out


def _list_pms_servers(cfg: Dict[str, Any]) -> List[dict]:
    acc = _account(cfg)
    if not acc:
        return []

    plex = cfg.get("plex") or {}
    host_hint = ""
    base = (plex.get("server_url") or "").strip()
    if base:
        try:
            host_hint = urllib.parse.urlparse(base).hostname or ""
        except Exception:
            host_hint = ""

    servers = []
    try:
        for r in acc.resources():
            if "server" not in (r.provides or "") or (r.product or "") != "Plex Media Server":
                continue
            conns = []
            for c in (r.connections or []):
                conns.append({
                    "uri": c.uri or "",
                    "address": c.address or "",
                    "port": c.port or "",
                    "protocol": c.protocol or "",
                    "local": bool(getattr(c, "local", False)),
                    "relay": bool(getattr(c, "relay", False)),
                })

            def pick_best() -> str:
                if host_hint:
                    for c in (r.connections or []):
                        if host_hint in (c.uri or "") or host_hint == (c.address or ""):
                            return c.uri or ""
                for c in (r.connections or []):
                    if not c.relay and (c.protocol or "").lower() == "https" and not getattr(c, "local", False):
                        return c.uri or ""
                for c in (r.connections or []):
                    if not c.relay and (c.protocol or "").lower() == "https":
                        return c.uri or ""
                for c in (r.connections or []):
                    if not c.relay:
                        return c.uri or ""
                return (r.connections[0].uri if r.connections else "") or ""

            servers.append({
                "id": r.clientIdentifier or "",
                "name": r.name or r.product or "Plex Media Server",
                "owned": bool(getattr(r, "owned", False)),
                "platform": r.platform or "",
                "product": r.product or "",
                "device": r.device or "",
                "version": r.productVersion or "",
                "connections": conns,
                "best_url": pick_best(),
            })
    except Exception:
        pass

    return servers


# --------------- Routes: Plex ---------------
@app.get("/api/plex/server_uuid")
def api_plex_server_uuid() -> JSONResponse:
    cfg = load_config()
    uid = _resolve_plex_server_uuid(cfg)
    return JSONResponse({"server_uuid": uid or None}, headers={"Cache-Control": "no-store"})


@app.get("/api/plex/users")
def api_plex_users(
    only_with_server_access: bool = Query(False),
    only_home_or_owner: bool = Query(False)
) -> JSONResponse:
    cfg = load_config()
    users = _list_plex_users(cfg)

    if only_with_server_access:
        server_uuid = _resolve_plex_server_uuid(cfg)
        users = _filter_users_with_server_access(cfg, users, server_uuid)

    if only_home_or_owner:
        users = [u for u in users if u.get("type") in ("owner", "managed")]

    return JSONResponse({"users": users, "count": len(users)}, headers={"Cache-Control": "no-store"})


@app.get("/api/plex/pms")
def api_plex_pms() -> JSONResponse:
    cfg = load_config()
    servers = _list_pms_servers(cfg)
    return JSONResponse({"servers": servers, "count": len(servers)}, headers={"Cache-Control": "no-store"})


#----------------watch scrobble
@app.get("/debug/watch/status")
def debug_watch_status():
    w = getattr(app.state, "watch", None) or WATCH
    return {
        "has_watch": bool(w),
        "alive": bool(getattr(w, "is_alive", lambda: False)()),
        "stop_set": bool(getattr(w, "is_stopping", lambda: False)()),
    }

def _ensure_watch_started():
    global WATCH
    w = getattr(app.state, "watch", None) or WATCH
    if w and getattr(w, "is_alive", lambda: False)():
        WATCH = w
        return w
    try:
        w = autostart_from_config()  # honors scrobble.enabled/mode/watch.autostart
    except Exception:
        w = None
    if not w:
        from providers.scrobble.trakt.sink import TraktSink
        from providers.scrobble.plex.watch import make_default_watch
        w = make_default_watch(sinks=[TraktSink()])
        if hasattr(w, "start_async"):
            w.start_async()
        else:
            import threading
            threading.Thread(target=w.start, daemon=True).start()
    app.state.watch = w
    WATCH = w
    return w

@app.post("/debug/watch/start")
def debug_watch_start():
    w = _ensure_watch_started()
    return {"ok": True, "alive": bool(getattr(w, "is_alive", lambda: False)())}

@app.post("/debug/watch/stop")
def debug_watch_stop():
    global WATCH
    w = getattr(app.state, "watch", None) or WATCH
    if w:
        w.stop()
    app.state.watch = None
    WATCH = None
    return {"ok": True, "alive": False}

# --------------- Trakt webhook ---------------
@app.post("/webhook/trakt")
async def webhook_trakt(request: Request):
    logger = _UIHostLogger("TRAKT", "SCROBBLE")

    def log(msg, level="INFO"):
        try:
            logger(msg, level=level, module="SCROBBLE")
        except:
            pass

    ct = (request.headers.get("content-type") or "").lower()
    payload = None

    try:
        if "multipart/form-data" in ct:
            form = await request.form()
            part = form.get("payload")
            if part is None:
                raise ValueError("multipart: no 'payload' part")
            try:
                data = await part.read()
            except Exception:
                try:
                    data = part.file.read()
                except Exception:
                    data = str(part).encode()
            payload = json.loads(data.decode("utf-8", errors="replace"))
            log("parsed multipart payload", "DEBUG")
        else:
            raw = await request.body()
            if "application/x-www-form-urlencoded" in ct:
                d = parse_qs(raw.decode("utf-8", errors="replace"))
                if "payload" not in d or not d["payload"]:
                    raise ValueError("urlencoded: no 'payload' key")
                payload = json.loads(d["payload"][0])
                log("parsed urlencoded payload", "DEBUG")
            else:
                payload = json.loads(raw.decode("utf-8", errors="replace"))
                log("parsed json payload", "DEBUG")
    except Exception as e:
        try:
            raw = await request.body()
            snippet = raw[:200].decode("utf-8", errors="replace")
        except Exception:
            snippet = "<no body>"
        log(f"failed to parse webhook payload: {e} | body[:200]={snippet}", "ERROR")
        return JSONResponse({"ok": True}, status_code=200)

    acc = ((payload.get("Account") or {}).get("title") or "").strip()
    srv = ((payload.get("Server") or {}).get("uuid") or "").strip()
    md = payload.get("Metadata") or {}
    title = md.get("title") or md.get("grandparentTitle") or "?"
    log(f"payload summary user='{acc}' server='{srv}' media='{title}'", "DEBUG")

    try:
        res = process_webhook(payload=payload, headers=dict(request.headers), raw=None, logger=logger)
    except Exception as e:
        log(f"process_webhook raised: {e}", "ERROR")
        return JSONResponse({"ok": True, "error": "internal"}, status_code=200)

    log(f"done action={res.get('action')} status={res.get('status')}", "DEBUG")
    return JSONResponse({"ok": True, **{k: v for k, v in res.items() if k != 'error'}}, status_code=200)


# --------------- Metadata resolver ---------------
@app.post("/api/metadata/resolve")
def api_metadata_resolve(payload: MetadataResolveIn):
    if _METADATA is None:
        return JSONResponse({"ok": False, "error": "MetadataManager not available"}, status_code=500)
    try:
        res = _METADATA.resolve(
            entity=payload.entity,
            ids=payload.ids,
            locale=payload.locale,
            need=payload.need,
            strategy=payload.strategy or "first_success",
        )
        return JSONResponse({"ok": True, "result": res})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    
@app.post("/api/metadata/bulk")
def api_metadata_bulk(
    payload: Dict[str, Any] = Body(
        ...,
        description=(
            "JSON body with items to resolve and optional 'need' flags.\n"
            "Example:\n"
            "{\n"
            '  "items": [{"type":"movie","tmdb":123},{"type":"tv","tmdb":456}],\n'
            '  "need": {"overview": true, "tagline": true, "runtime_minutes": true,'
            '           "videos": true, "genres": true, "score": true,'
            '           "certification": true, "release": true},\n'
            '  "concurrency": 6\n'
            "}"
        ),
    ),
    overview: Literal["none", "short", "full"] = Query(
        "full", description="Override overview handling: none|short|full"
    ),
    locale: Optional[str] = Query(
        None, description="Override metadata locale (e.g., 'nl-NL')"
    ),
) -> JSONResponse:
    """
    Resolve metadata for many TMDb items in one call.
    - Input  : payload.items[] of {type|entity, tmdb}
    - Filters: payload.need (truthy flags) to request only specific fields
    - Limits : respects metadata.bulk_max (default 300) and caps concurrency
    - Output : map keyed by '<type>:<tmdb>' with requested fields
    """
    cfg = load_config()
    st = _load_state()  # optional; only for last_sync_epoch
    api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""
    md_cfg = (cfg.get("metadata") or {})
    bulk_max = int(md_cfg.get("bulk_max", 300))
    default_workers = 6

    # Validate input
    items = (payload or {}).get("items") or []
    if not isinstance(items, list) or not items:
        return JSONResponse(
            {
                "ok": False,
                "error": "Body must include a non-empty 'items' array.",
                "missing_tmdb_key": not bool(api_key),
            },
            status_code=200,
        )

    # Clamp to bulk_max
    items = items[:bulk_max]

    # Determine need flags
    req_need = (payload or {}).get("need") or {
        "overview": True,
        "tagline": True,
        "runtime_minutes": True,
    }
    if overview == "none":
        req_need = dict(req_need, overview=False)
    elif overview in ("short", "full"):
        req_need = dict(req_need, overview=True)

    # Determine locale (explicit -> metadata.locale -> ui.locale)
    eff_locale = (
        locale
        or md_cfg.get("locale")
        or (cfg.get("ui") or {}).get("locale")
        or None
    )

    # Concurrency tuning
    try:
        requested_workers = int((payload or {}).get("concurrency") or default_workers)
    except Exception:
        requested_workers = default_workers
    workers = max(1, min(requested_workers, 12))

    # Helper: safe shortener
    def _shorten(txt: str, limit: int = 280) -> str:
        if not txt or len(txt) <= limit:
            return txt or ""
        cut = txt[:limit].rsplit(" ", 1)[0].rstrip(",.;:!-–—")
        return f"{cut}…"

    # Worker
    def _fetch_one(item: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        typ = (item.get("type") or item.get("entity") or "movie").lower()
        if typ == "show":
            typ = "tv"
        if typ not in ("movie", "tv"):
            typ = "movie"
        tmdb_id = str(item.get("tmdb") or item.get("id") or "").strip()
        key = f"{'show' if typ=='tv' else 'movie'}:{tmdb_id or 'UNKNOWN'}"

        if not tmdb_id:
            return key, {"ok": False, "error": "Missing tmdb id"}

        try:
            meta = get_meta(
                api_key,
                "movie" if typ == "movie" else "show",
                tmdb_id,
                CACHE_DIR,
                need=req_need,
                locale=eff_locale,
            ) or {}
        except Exception as e:
            return key, {"ok": False, "error": f"resolver failed: {e}"}

        if not meta:
            return key, {"ok": False, "error": "no metadata"}
        keep_keys = {
            "type",
            "title",
            "year",
            "ids",
            "runtime_minutes",
            "overview",
            "tagline",
            "images",
            "genres",
            "videos",
            "score",
            "certification",
            "release",
            "detail",
        }
        # Always normalize 'type' to movie|show
        meta_out = {"type": meta.get("type") or ("movie" if typ == "movie" else "show")}
        for k in keep_keys:
            if k in meta and k != "type":
                meta_out[k] = meta[k]

        # Overview shaping
        if overview == "short" and meta_out.get("overview"):
            meta_out["overview"] = _shorten(meta_out["overview"], 280)

        return key, {"ok": True, "meta": meta_out}

    results: Dict[str, Any] = {}
    fetched = 0

    # Fast path: small batches run inline
    if len(items) <= 8:
        for it in items:
            k, v = _fetch_one(it)
            results[k] = v
            if v.get("ok"):
                fetched += 1
    else:
        # Threaded for larger batches
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_fetch_one, it) for it in items]
            for fut in as_completed(futs):
                try:
                    k, v = fut.result()
                except Exception as e:
                    k, v = "unknown:0", {"ok": False, "error": f"worker error: {e}"}
                results[k] = v
                if v.get("ok"):
                    fetched += 1

    return JSONResponse(
        {
            "ok": True,
            "count": len(items),
            "fetched": fetched,
            "missing_tmdb_key": not bool(api_key),
            "results": results,  # "<type>:<tmdb>" -> { ok, meta | error }
            "last_sync_epoch": st.get("last_sync_epoch") if isinstance(st, dict) else None,
        },
        status_code=200,
    )
# --------------- Watch logs ---------------
@app.get("/debug/watch/logs")
def debug_watch_logs(tail: int = Query(20, ge=1, le=200), tag: str = Query("TRAKT")) -> JSONResponse:
    buf = LOG_BUFFERS.get(tag.upper(), [])
    tail = max(1, min(int(tail or 20), 200))
    lines = buf[-tail:]
    return JSONResponse({"tag": tag.upper(), "tail": tail, "lines": lines}, headers={"Cache-Control": "no-store"})

# --------------- Insights & stats (provider-agnostic) ---------------
@app.get("/api/insights")
def api_insights(limit_samples: int = Query(60), history: int = Query(3)) -> JSONResponse:
    """
    Returns:
      - series:   last N (time,count) samples (ascending)
      - history:  last few sync reports (with per-feature breakdown; lanes backfilled from Stats if empty)
                  Each history row also contains 'provider_posts': { <provider>: <value from report> }.
      - watchtime:estimated minutes/hours/days with method=tmdb|fallback|mixed
      - providers:               { <provider>: total }  (watchlist totals for back-compat)
      - providers_by_feature:    { watchlist|ratings|history|playlists -> { <provider>: total } }
      - providers_active:        { <provider>: bool } (from configured pairs)
      - now/week/month + added/removed/new/del from Stats.overview()
    """
    # ---- Samples (keep stable shape)
    with STATS.lock:
        samples = list(STATS.data.get("samples") or [])
    samples.sort(key=lambda r: int(r.get("ts") or 0))
    if int(limit_samples) > 0:
        samples = samples[-int(limit_samples):]
    series = [{"ts": int(r.get("ts") or 0), "count": int(r.get("count") or 0)} for r in samples]

    # ---- Recent sync history (read recent reports)
    rows: list[dict] = []
    try:
        files = sorted(
            REPORT_DIR.glob("sync-*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )[:max(1, int(history))]

        def zero_lane():
            return {"added": 0, "removed": 0, "updated": 0,
                    "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []}

        for p in files:
            try:
                d = json.loads(p.read_text(encoding="utf-8"))

                # Normalized lanes present in the file (may be all zeros)
                feats_in = d.get("features") or {}
                lanes = {
                    "watchlist": feats_in.get("watchlist") or zero_lane(),
                    "ratings":   feats_in.get("ratings")   or zero_lane(),
                    "history":   feats_in.get("history")   or zero_lane(),
                    "playlists": feats_in.get("playlists") or zero_lane(),
                }

                # Compute the time window for this report
                since = _parse_epoch(d.get("raw_started_ts") or d.get("started_at"))
                until = _parse_epoch(d.get("finished_at"))
                if not until:
                    # fallback: use file mtime if finished_at is missing
                    until = int(p.stat().st_mtime)

                # Backfill empty/missing lanes from Stats events within [since, until]
                stats_feats, stats_enabled = _compute_lanes_from_stats(since, until)
                for name in ("watchlist", "ratings", "history", "playlists"):
                    lane = lanes.get(name)
                    if name not in lanes or _lane_is_empty(lane):
                        lanes[name] = stats_feats.get(name) or zero_lane()

                # Enabled map: prefer explicit, else stats-derived, else sane defaults
                enabled = d.get("features_enabled") or d.get("enabled")
                if not isinstance(enabled, dict):
                    enabled = dict(stats_enabled)  # defaults to all True if Stats had nothing

                # Totals: prefer explicit *_last; else derive by summing enabled lanes
                added_total   = d.get("added_last")
                removed_total = d.get("removed_last")
                updated_total = d.get("updated_last")
                if added_total is None or removed_total is None or updated_total is None:
                    a = r = u = 0
                    for k, lane in lanes.items():
                        if enabled.get(k) is False:
                            continue
                        a += int((lane or {}).get("added")   or 0)
                        r += int((lane or {}).get("removed") or 0)
                        u += int((lane or {}).get("updated") or 0)
                    if added_total   is None: added_total   = a
                    if removed_total is None: removed_total = r
                    if updated_total is None: updated_total = u

                # Collect any "<provider>_post" keys dynamically into a compact map.
                provider_posts = {}
                for k, v in d.items():
                    if isinstance(k, str) and k.endswith("_post"):
                        provider_posts[k[:-5]] = v  # key without suffix, e.g. "plex"

                rows.append({
                    "started_at":   d.get("started_at"),
                    "finished_at":  d.get("finished_at"),
                    "duration_sec": d.get("duration_sec"),
                    "result":       d.get("result") or "",
                    "exit_code":    d.get("exit_code"),

                    # Back-compat (UI expects these)
                    "added":        int(added_total or 0),
                    "removed":      int(removed_total or 0),

                    # Per-feature breakdown + enabled map (now filtered by actual lane)
                    "features":         lanes,
                    "features_enabled": enabled,
                    "updated_total":    int(updated_total or 0),

                    # Dynamic provider posts (plus legacy keys remain in 'd' if the UI still reads them)
                    "provider_posts": provider_posts,
                    # Legacy convenience (kept if present in the file)
                    "plex_post":     d.get("plex_post"),
                    "simkl_post":    d.get("simkl_post"),
                    "trakt_post":    d.get("trakt_post"),
                    "jellyfin_post": d.get("jellyfin_post"),
                })
            except Exception:
                continue
    except Exception:
        pass

    # ---- Watchtime (and capture state for provider totals)
    wall = _load_wall_snapshot()
    state = None
    if not wall:
        try:
            orc = _get_orchestrator()
            state = orc.files.load_state()
            if isinstance(state, dict):
                wall = list(state.get("wall") or [])
            if not wall:
                snaps = orc.build_snapshots(feature="watchlist")
                for idx in (snaps or {}).values():
                    wall.extend(list(idx.values()))
        except Exception as e:
            _append_log("SYNC", f"[!] insights: orchestrator init failed: {e}")
            wall = []

    cfg = load_config()
    api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()
    use_tmdb = bool(api_key)

    movies = shows = 0
    total_min = 0
    tmdb_hits = tmdb_misses = 0
    fetch_cap = 50
    fetched = 0

    for meta in wall:
        typ = "movie" if (str(meta.get("type") or "").lower() == "movie") else "tv"
        if typ == "movie": movies += 1
        else:              shows  += 1

        minutes = None
        ids = meta.get("ids") or {}
        tmdb_id = ids.get("tmdb")

        if use_tmdb and tmdb_id and fetched < fetch_cap:
            try:
                minutes = get_runtime(api_key, typ, int(tmdb_id), CACHE_DIR)
                fetched += 1
                if minutes is not None: tmdb_hits += 1
                else:                   tmdb_misses += 1
            except Exception:
                tmdb_misses += 1

        if minutes is None:
            minutes = 115 if typ == "movie" else 45

        total_min += int(minutes)

    method = "tmdb" if tmdb_hits and not tmdb_misses else ("mixed" if tmdb_hits else "fallback")
    watchtime = {
        "movies":  movies,
        "shows":   shows,
        "minutes": total_min,
        "hours":   round(total_min / 60, 1),
        "days":    round(total_min / 1440, 1),
        "method":  method,
    }

    # ---- Build provider universe dynamically (from state + configured pairs)
    providers_set: set[str] = set()
    try:
        # 1) From state providers (UPPERCASE keys)
        if state is None:
            try:
                orc = _get_orchestrator()
                state = orc.files.load_state()
            except Exception:
                state = None
        prov_block = (state or {}).get("providers") or {}
        for up in prov_block.keys():
            if isinstance(up, str):
                providers_set.add(up.strip().lower())

        # 2) From configured pairs (source/target)
        cfg2 = load_config() or {}
        pairs = (cfg2.get("pairs") or cfg2.get("connections") or []) or []
        for p in pairs:
            s = str(p.get("source") or "").strip().lower()
            t = str(p.get("target") or "").strip().lower()
            if s: providers_set.add(s)
            if t: providers_set.add(t)
    except Exception:
        pass

    # Ensure at least the common ones exist if nothing discovered (harmless default)
    if not providers_set:
        providers_set = {"plex", "simkl", "trakt", "jellyfin"}

    # ---- Active map (from pairs)
    active: dict[str, bool] = {k: False for k in providers_set}
    try:
        cfg3 = load_config() or {}
        pairs3 = (cfg3.get("pairs") or cfg3.get("connections") or []) or []
        for p in pairs3:
            s = str(p.get("source") or "").strip().lower()
            t = str(p.get("target") or "").strip().lower()
            if s in active: active[s] = True
            if t in active: active[t] = True
    except Exception:
        pass

    # ---- Provider totals by feature and top-level (watchlist back-compat)
    feature_keys = ["watchlist", "ratings", "history", "playlists"]
    providers_by_feature: dict[str, dict[str, int]] = {feat: {k: 0 for k in providers_set} for feat in feature_keys}
    try:
        for upcase, data in (prov_block or {}).items():
            key = str(upcase or "").strip().lower()
            if key not in providers_set:
                continue
            for feat in feature_keys:
                items = ((((data or {}).get(feat) or {}).get("baseline") or {}).get("items") or {})
                providers_by_feature[feat][key] = int(len(items))
    except Exception:
        pass

    # Back-compat top-level `providers` = watchlist totals
    providers = dict(providers_by_feature.get("watchlist", {}))

    # ---- High-level counters from Stats
    try:
        top = STATS.overview(None) or {}
    except Exception:
        top = {}

    payload = {
        "series":               series,
        "history":              rows,
        "watchtime":            watchtime,
        "providers":            providers,
        "providers_by_feature": providers_by_feature,
        "providers_active":     active,
    }
    # Also expose cumulative and last-run counters
    for k in ("now", "week", "month", "added", "removed", "new", "del"):
        if k in top:
            payload[k] = top[k]

    return JSONResponse(payload)

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

# --------------- Watchlist endpoints ---------------
@app.get("/api/watchlist")
def api_watchlist(
    overview: Literal["none", "short", "full"] = Query(
        "none", description="Attach overview text from TMDb metadata"
    ),
    locale: Optional[str] = Query(
        None, description="Override metadata locale (e.g., 'nl-NL')"
    ),
    limit: int = Query(
        0, ge=0, le=5000, description="Optionally slice the returned list"
    ),
    max_meta: int = Query(
        250, ge=0, le=2000, description="Cap how many items are enriched"
    ),
) -> JSONResponse:
    """
    Returns the merged watchlist. Optional metadata enrichment:
      - overview=none  : no overview (default; fastest)
      - overview=short : ~tweet-length summary
      - overview=full  : full overview text
    Applies a server-side cap (max_meta) to protect latency on large lists.
    """
    cfg = load_config()
    st = _load_state()
    api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""

    if not st:
        return JSONResponse(
            {"ok": False, "error": "No Snapshot found or empty.", "missing_tmdb_key": not bool(api_key)},
            status_code=200,
        )
    try:
        items = build_watchlist(st, tmdb_api_key_present=bool(api_key))
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e), "missing_tmdb_key": not bool(api_key)},
            status_code=200,
        )

    if not items:
        return JSONResponse(
            {"ok": False, "error": "No snapshot data found.", "missing_tmdb_key": not bool(api_key)},
            status_code=200,
        )

    # Optional slice for very large lists
    if limit and isinstance(limit, int) and limit > 0:
        items = items[:limit]

    # Optional overview enrichment using MetadataManager
    enriched = 0
    if overview != "none" and _METADATA is not None and api_key:
        eff_locale = (
            locale
            or (cfg.get("metadata") or {}).get("locale")
            or (cfg.get("ui") or {}).get("locale")
            or None
        )
        for it in items:
            if enriched >= int(max_meta):
                break
            tmdb_id = it.get("tmdb")
            if not tmdb_id:
                continue
            try:
                meta = get_meta(
                    api_key,
                    it.get("type") or "movie",
                    tmdb_id,
                    CACHE_DIR,
                    need={"overview": True, "tagline": True, "title": True, "year": True},
                    locale=eff_locale,
                ) or {}
                desc = meta.get("overview") or ""
                if not desc:
                    continue
                if overview == "short":
                    desc = _shorten(desc, 280)  # helper from metadata block
                it["overview"] = desc
                if overview == "short" and meta.get("tagline"):
                    it["tagline"] = meta["tagline"]
                enriched += 1
            except Exception:
                # Fail soft on resolver hiccups
                continue

    return JSONResponse(
        {
            "ok": True,
            "items": items,
            "missing_tmdb_key": not bool(api_key),
            "last_sync_epoch": st.get("last_sync_epoch"),
            "meta_enriched": enriched,
        },
        status_code=200,
    )

@app.delete("/api/watchlist/{key}")
def api_watchlist_delete(key: str = FPath(...)) -> JSONResponse:
    # single-delete by key (provider = PLEX default)
    sp = STATE_PATH  # <-- use global, no _state_path()
    try:
        if "%" in (key or ""):
            key = urllib.parse.unquote(key)

        result = delete_watchlist_item(
            key=key,
            state_path=sp,
            cfg=load_config(),
            log=_append_log,
        )

        if not isinstance(result, dict) or "ok" not in result:
            result = {"ok": False, "error": "unexpected server response"}

        if result.get("ok"):
            try:
                state = _load_state()
                P = state.get("providers") or {}
                for prov in ("PLEX", "SIMKL", "TRAKT"):
                    items = (((P.get(prov) or {}).get("watchlist") or {}).get("baseline") or {}).get("items") or {}
                    items.pop(key, None)
                STATS.refresh_from_state(state)
            except Exception:
                pass

        status = 200 if result.get("ok") else 400
        return JSONResponse(result, status_code=status)

    except Exception as e:
        _append_log("TRBL", f"[WATCHLIST] ERROR: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# Providers for UI
@app.get("/api/watchlist/providers")
def api_watchlist_providers():
    cfg = load_config()
    from _watchlist import detect_available_watchlist_providers
    return {"providers": detect_available_watchlist_providers(cfg)}


# Delete
@app.post("/api/watchlist/delete")
def api_watchlist_delete_batch(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    """
    Payload: { "keys": ["imdb:tt123", ...], "provider": "ALL"|"PLEX"|"SIMKL"|"TRAKT"|"JELLYFIN" }
    Semantics:
      - ok == True if at least one key deleted successfully (partial success allowed)
      - partial == True if some keys failed
    """
    try:
        keys = payload.get("keys") or []
        provider = (payload.get("provider") or "ALL").upper()

        if not isinstance(keys, list) or not keys:
            return {"ok": False, "error": "keys must be a non-empty array"}

        if provider not in ("ALL", "PLEX", "SIMKL", "TRAKT", "JELLYFIN"):
            return {"ok": False, "error": f"unknown provider '{provider}'"}

        try:
            cfg = load_config()
        except Exception as e:
            return {"ok": False, "error": f"failed to load config: {e}"}

        state_file = STATE_PATH

        results: List[Dict[str, Any]] = []
        ok_count = 0

        for k in keys:
            try:
                r = delete_watchlist_item(
                    key=str(k),
                    state_path=state_file,
                    cfg=cfg,
                    provider=provider,
                    log=_append_log,
                )
                results.append({"key": k, **r})
                if r.get("ok"):
                    ok_count += 1
            except Exception as e:
                tb = traceback.format_exc()
                print(f"[watchlist:delete] key={k} provider={provider} ERROR: {e}\n{tb}")
                results.append({"key": k, "ok": False, "error": str(e)})

        # Best-effort stats refresh
        try:
            state = _load_state()
            STATS.refresh_from_state(state)
        except Exception:
            pass

        return {
            "ok": ok_count > 0,
            "partial": ok_count != len(keys),
            "provider": provider,
            "deleted_ok": ok_count,
            "deleted_total": len(keys),
            "results": results,
        }

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[watchlist:delete] FATAL: {e}\n{tb}")
        return {"ok": False, "error": f"fatal: {e}"}


# Delete Batch
@app.post("/api/watchlist/delete_batch")
def api_watchlist_delete_batch(payload: dict = Body(...)):
    """
    JSON body: { "keys": [ "imdb:tt123", "tmdb:456", ... ], "provider": "PLEX|SIMKL|TRAKT|JELLYFIN|ALL" }
    Deletes selected items on the given provider(s) in one shot.
    """
    try:
        keys = payload.get("keys") or []
        provider = (payload.get("provider") or "ALL").upper().strip()
        if not isinstance(keys, list) or not keys:
            raise HTTPException(status_code=400, detail="keys array required")

        allowed = {"ALL", "PLEX", "SIMKL", "TRAKT", "JELLYFIN"}
        if provider not in allowed:
            raise HTTPException(status_code=400, detail=f"unknown provider '{provider}'")

        cfg = load_config()
        state = _load_state()

        from _watchlist import delete_watchlist_batch as _wl_delete_batch  # local import keeps imports tidy

        results = []
        targets = ["PLEX", "SIMKL", "TRAKT", "JELLYFIN"] if provider == "ALL" else [provider]
        for prov in targets:
            try:
                res = _wl_delete_batch(keys, prov, state, cfg)
                results.append(res | {"provider": prov})
                _append_log("SYNC", f"[WL] batch-delete {len(keys)} on {prov}: OK")
            except Exception as e:
                msg = f"[WL] batch-delete on {prov} failed: {e}"
                _append_log("SYNC", msg)
    
                results.append({"provider": prov, "ok": False, "error": str(e)})

        try:
            if state:
                STATS.refresh_from_state(state)
        except Exception:
            pass

        any_ok = any(r.get("ok") for r in results if isinstance(r, dict))
        return {"ok": any_ok, "results": results}
    except HTTPException:
        raise
    except Exception as e:
        _append_log("SYNC", f"[WL] batch-delete fatal: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    
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

CURRENT_VERSION = os.getenv("APP_VERSION", "v0.1.3")
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

# --------------- Status & connectivity ---------------
_PROBE_CACHE: Dict[str, Tuple[float, bool]] = {
    "plex":  (0.0, False),
    "simkl": (0.0, False),
    "trakt": (0.0, False),
    "jellyfin": (0.0, False),
}

# Extra per-user capability cache (Plex Pass / Trakt VIP)
_USERINFO_CACHE: Dict[str, Tuple[float, dict]] = {
    "plex":  (0.0, {}),
    "trakt": (0.0, {}),
}
USERINFO_TTL = 600  # seconds

def _http_get(url: str, headers: Dict[str, str], timeout: int = 8) -> Tuple[int, bytes]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.getcode(), r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read() if e.fp else b""
    except Exception:
        return 0, b""

def _json_loads(b: bytes) -> dict:
    try:
        return json.loads(b.decode("utf-8", errors="ignore"))
    except Exception:
        return {}

def plex_user_info(cfg: Dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict:
    ts, info = _USERINFO_CACHE["plex"]
    now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict):
        return info

    token = ((cfg.get("plex") or {}).get("account_token") or "").strip()
    if not token:
        _USERINFO_CACHE["plex"] = (now, {})
        return {}

    # 1) Try plexapi if available
    plexpass = None
    plan = None
    status = None
    if HAVE_PLEXAPI:
        try:
            acc = MyPlexAccount(token=token)
            plexpass = bool(getattr(acc, "subscriptionActive", None) or getattr(acc, "hasPlexPass", None))
            plan = getattr(acc, "subscriptionPlan", None) or None
            status = getattr(acc, "subscriptionStatus", None) or None
        except Exception:
            pass

    # 2) Fallback to REST v2 (JSON)
    if plexpass is None:
        headers = {
            "X-Plex-Token": token,
            "X-Plex-Client-Identifier": "crosswatch",
            "X-Plex-Product": "CrossWatch",
            "X-Plex-Version": "1.0",
            "Accept": "application/json",
            "User-Agent": "CrossWatch/1.0",
        }
        code, body = _http_get("https://plex.tv/api/v2/user", headers=headers, timeout=8)
        if code == 200:
            j = _json_loads(body)
            sub = (j.get("subscription") or {})
            plexpass = bool(sub.get("active") or j.get("hasPlexPass"))
            plan = sub.get("plan") or plan
            status = sub.get("status") or status

    out = {}
    if plexpass is not None:
        out["plexpass"] = bool(plexpass)
        out["subscription"] = {"plan": plan, "status": status}
    _USERINFO_CACHE["plex"] = (now, out)
    return out

def trakt_user_info(cfg: Dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict:
    ts, info = _USERINFO_CACHE["trakt"]
    now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict):
        return info

    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {})  # uppercase fallback
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}
    cid = (tr.get("client_id") or auth_tr.get("client_id") or "").strip()
    tok = (auth_tr.get("access_token") or tr.get("access_token") or tr.get("token") or "").strip()
    if not cid or not tok:
        _USERINFO_CACHE["trakt"] = (now, {})
        return {}

    headers = {
        "Authorization": f"Bearer {tok}",
        "trakt-api-key": cid,
        "trakt-api-version": "2",
        "Accept": "application/json",
        "User-Agent": "CrossWatch/1.0",
    }
    code, body = _http_get("https://api.trakt.tv/users/settings", headers=headers, timeout=8)
    out = {}
    if code == 200:
        j = _json_loads(body)
        u = j.get("user") or {}
        # Trakt exposeert soms meerdere VIP flags; pak wat er is.
        vip = bool(u.get("vip") or u.get("vip_og") or u.get("vip_ep"))
        vip_type = "vip"
        if u.get("vip_og"): vip_type = "vip_og"
        if u.get("vip_ep"): vip_type = "vip_ep"
        out = {"vip": vip, "vip_type": vip_type}
    _USERINFO_CACHE["trakt"] = (now, out)
    return out

# --------------- Connectivity Probes ---------------
def probe_plex(cfg: Dict[str, Any], max_age_sec: int = 30) -> bool:
    ts, ok = _PROBE_CACHE["plex"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok

    token = (cfg.get("plex", {}) or {}).get("account_token") or ""
    if not token:
        _PROBE_CACHE["plex"] = (now, False)
        return False

    headers = {
        "X-Plex-Token": token,
        "X-Plex-Client-Identifier": "crosswatch",
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Version": "1.0",
        "Accept": "application/xml",
        "User-Agent": "CrossWatch/1.0",
    }
    code, _ = _http_get("https://plex.tv/users/account", headers=headers, timeout=8)
    ok = (code == 200)
    _PROBE_CACHE["plex"] = (now, ok)
    return ok


def probe_simkl(cfg: Dict[str, Any], max_age_sec: int = 30) -> bool:
    ts, ok = _PROBE_CACHE["simkl"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok

    sk = (cfg.get("simkl") or cfg.get("SIMKL") or {})  # <-- uppercase fallback
    cid = (sk.get("client_id") or "").strip()
    tok = (sk.get("access_token") or sk.get("token") or "").strip()  # token fallback
    if not cid or not tok:
        _PROBE_CACHE["simkl"] = (now, False)
        return False

    headers = {
        "Authorization": f"Bearer {tok}",
        "simkl-api-key": cid,
        "Accept": "application/json",
        "User-Agent": "CrossWatch/1.0",
    }
    code, _ = _http_get("https://api.simkl.com/users/settings", headers=headers, timeout=8)
    ok = (code == 200)
    _PROBE_CACHE["simkl"] = (now, ok)
    return ok

def probe_jellyfin(cfg: Dict[str, Any], max_age_sec: int = 30) -> bool:
    ts, ok = _PROBE_CACHE.get("jellyfin", (0.0, False))
    now = time.time()
    if now - ts < max_age_sec:
        return ok
    jf = (cfg.get("jellyfin") or cfg.get("JELLYFIN") or {})
    ok = bool((jf.get("server") or "").strip() and (jf.get("access_token") or jf.get("token") or "").strip())
    _PROBE_CACHE["jellyfin"] = (now, ok)
    return ok

def probe_trakt(cfg: Dict[str, Any], max_age_sec: int = 30) -> bool:
    ts, ok = _PROBE_CACHE["trakt"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok

    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {})
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}

    cid = (tr.get("client_id") or auth_tr.get("client_id") or "").strip()
    tok = (
        auth_tr.get("access_token")
        or tr.get("access_token")
        or tr.get("token")
    )
    tok = str(tok).strip()

    if not cid or not tok:
        _PROBE_CACHE["trakt"] = (now, False)
        return False

    headers = {
        "Authorization": f"Bearer {tok}",
        "trakt-api-key": cid,
        "trakt-api-version": "2",
        "Accept": "application/json",
        "User-Agent": "CrossWatch/1.0",
    }
    code, _ = _http_get("https://api.trakt.tv/users/settings", headers=headers, timeout=8)
    ok = (code == 200)
    _PROBE_CACHE["trakt"] = (now, ok)
    return ok

def connected_status(cfg: Dict[str, Any]) -> Tuple[bool, bool, bool, bool]:
    plex_ok  = probe_plex(cfg)
    simkl_ok = probe_simkl(cfg)
    trakt_ok = probe_trakt(cfg)
    debug    = bool(cfg.get("runtime", {}).get("debug"))
    return plex_ok, simkl_ok, trakt_ok, debug

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


# --------------- Jellyfin login & status ---------------
@app.post("/api/jellyfin/login")
def api_jellyfin_login(payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    import importlib

    if not isinstance(payload, dict):
        return JSONResponse({"ok": False, "error": "Malformed request"}, 400)

    cfg = load_config()
    jf = cfg.setdefault("jellyfin", {})
    for k in ("server", "username", "password"):
        v = (payload.get(k) or "").strip()
        if v: jf[k] = v
    if not all(jf.get(k) for k in ("server","username","password")):
        return JSONResponse({"ok": False, "error": "Missing: server/username/password"}, 400)

    def _code(msg: str) -> int:
        m = (msg or "").lower()
        if "401" in m or "403" in m or "invalid credential" in m or "unauthor" in m: return 401
        if "timeout" in m: return 504
        if any(x in m for x in ("dns","ssl","connection","refused","unreachable","getaddrinfo","name or service")): return 502
        return 502

    try:
        mod = importlib.import_module("providers.auth._auth_JELLYFIN")
        prov = getattr(mod, "PROVIDER", None)
        if not prov:
            return JSONResponse({"ok": False, "error": "Provider missing"}, 500)

        res = prov.start(cfg, redirect_uri="")
        save_config(cfg)

        if res.get("ok"):
            return JSONResponse({
                "ok": True,
                "user_id": res.get("user_id"),
                "username": jf.get("user") or jf.get("username"),
                "server": jf.get("server"),
            }, 200)

        msg = res.get("error") or "Login failed"
        return JSONResponse({"ok": False, "error": msg}, _code(msg))
    except Exception as e:
        msg = str(e) or "Login failed"
        return JSONResponse({"ok": False, "error": msg}, _code(msg))

@app.get("/api/jellyfin/status")
def api_jellyfin_status() -> Dict[str, Any]:
    cfg = load_config()
    jf = (cfg.get("jellyfin") or {})
    return {
        "connected": bool(jf.get("access_token") and jf.get("server")),
        "user": jf.get("user") or jf.get("username") or None,
    }

# --------------- Trakt PIN request & wait ---------------
def trakt_request_pin() -> dict:
    try:
        from providers.auth._auth_TRAKT import PROVIDER as _TRAKT_PROVIDER
    except Exception:
        _TRAKT_PROVIDER = None
    if _TRAKT_PROVIDER is None:
        raise RuntimeError("Trakt provider not available")

    cfg = load_config()
    res = _TRAKT_PROVIDER.start(cfg, redirect_uri="")
    save_config(cfg)

    pend = (cfg.get("trakt") or {}).get("_pending_device") or {}
    user_code = (pend.get("user_code") or (res or {}).get("user_code"))
    device_code = (pend.get("device_code") or (res or {}).get("device_code"))
    verification_url = (pend.get("verification_url") or (res or {}).get("verification_url") or "https://trakt.tv/activate")
    exp_epoch = int((pend.get("expires_at") or 0) or (time.time() + 600))

    if not user_code or not device_code:
        raise RuntimeError("Trakt PIN could not be issued")

    return {
        "user_code": user_code,
        "device_code": device_code,
        "verification_url": verification_url,
        "expires_epoch": exp_epoch,
    }

# --------------- Trakt wait for token ---------------
def trakt_wait_for_token(device_code: str, timeout_sec: int = 600, interval: float = 2.0) -> str | None:
    try:
        from providers.auth._auth_TRAKT import PROVIDER as _TRAKT_PROVIDER
    except Exception:
        _TRAKT_PROVIDER = None

    deadline = time.time() + max(0, int(timeout_sec))
    sleep_s = max(0.5, float(interval))

    while time.time() < deadline:
        cfg = load_config() or {}
        tok = None
        if _TRAKT_PROVIDER is not None:
            try:
                tok = _TRAKT_PROVIDER.read_token_file(cfg, device_code)
            except Exception:
                tok = None

        if tok:
            try:
                if _TRAKT_PROVIDER is not None:
                    _TRAKT_PROVIDER.finish(cfg, device_code=device_code)
                    save_config(cfg)
                else:
     
                    if isinstance(tok, str):
                        try:
                            tok = json.loads(tok)
                        except Exception:
                            tok = {}
                    if isinstance(tok, dict):
                        tr = cfg.setdefault("trakt", {})
                        tr["access_token"]  = tok.get("access_token")  or tr.get("access_token", "")
                        tr["refresh_token"] = tok.get("refresh_token") or tr.get("refresh_token", "")
                        exp = int(tok.get("created_at") or 0) + int(tok.get("expires_in") or 0)
                        if not exp:
                            exp = int(time.time()) + 90 * 24 * 3600  # conservative default
                        tr["expires_at"] = exp
                        tr["token_type"] = tok.get("token_type") or "bearer"
                        tr["scope"] = tok.get("scope") or tr.get("scope", "public")
                        save_config(cfg)
            except Exception:
                pass
            return "ok"

        if _TRAKT_PROVIDER is not None:
            try:
                _TRAKT_PROVIDER.finish(cfg, device_code=device_code)
                save_config(cfg)
            except Exception:
                pass

        time.sleep(sleep_s)

    return None

# --------------- Trakt PIN request ---------------
@app.post("/api/trakt/pin/new")
def api_trakt_pin_new(payload: dict | None = Body(None)) -> Dict[str, Any]:
    try:
        if payload:
            cid  = str(payload.get("client_id") or "").strip()
            secr = str(payload.get("client_secret") or "").strip()
            if cid or secr:
                cfg = load_config()
                tr = cfg.setdefault("trakt", {})
                if cid:  tr["client_id"] = cid
                if secr: tr["client_secret"] = secr
                save_config(cfg)

        # Request PIN and start background waiter
        info = trakt_request_pin()
        user_code = info["user_code"]
        verification_url = info["verification_url"]
        exp_epoch = int(info["expires_epoch"])
        device_code = info["device_code"]

        def waiter(_device_code: str):
            token = trakt_wait_for_token(_device_code, timeout_sec=600, interval=2.0)
            if token:
                _append_log("TRAKT", "\x1b[92m[TRAKT]\x1b[0m Token acquired and saved.")
                _PROBE_CACHE["trakt"] = (0.0, False)
            else:
                _append_log("TRAKT", "\x1b[91m[TRAKT]\x1b[0m Device code expired or not authorized.")

        threading.Thread(target=waiter, args=(device_code,), daemon=True).start()
        expires_in = max(0, exp_epoch - int(time.time()))

        # Note: frontend expects 'expiresIn' (camelCase)
        return {"ok": True, "user_code": user_code, "verification_url": verification_url, "expiresIn": expires_in}
    except Exception as e:
        _append_log("TRAKT", f"[TRAKT] ERROR: {e}")
        return {"ok": False, "error": str(e)}


# --------------- App status ---------------
from fastapi.responses import JSONResponse

def _prov_configured(cfg: dict, name: str) -> bool:
    """Return True when provider has credentials."""
    name = (name or "").strip().lower()
    if name == "plex":
        return bool((cfg.get("plex") or {}).get("account_token"))
    if name == "trakt":
        return bool((cfg.get("trakt") or {}).get("access_token"))
    if name == "simkl":
        return bool((cfg.get("simkl") or {}).get("access_token"))
    if name == "jellyfin":
        jf = cfg.get("jellyfin") or {}
        return bool((jf.get("server") or "").strip() and (jf.get("access_token") or "").strip())
    return False

def _pair_ready(cfg: dict, pair: dict) -> bool:
    """Ready when pair enabled and both ends configured."""
    if not isinstance(pair, dict):
        return False
    enabled = pair.get("enabled", True) is not False
    def _name(x):
        if isinstance(x, str): return x
        if isinstance(x, dict): return x.get("provider") or x.get("name") or x.get("id") or x.get("type") or ""
        return ""
    a = _name(pair.get("source") or pair.get("a") or pair.get("src") or pair.get("from"))
    b = _name(pair.get("target") or pair.get("b") or pair.get("dst") or pair.get("to"))
    return bool(enabled and _prov_configured(cfg, a) and _prov_configured(cfg, b))

def _safe_probe(fn, cfg, max_age_sec=0):
    """Probe with guard."""
    try:
        return bool(fn(cfg, max_age_sec=max_age_sec))
    except Exception as e:
        print(f"[status] probe {getattr(fn, '__name__', 'fn')} failed: {e}")
        return False

def _safe_userinfo(fn, cfg, max_age_sec=0):
    """User info with guard."""
    try:
        return fn(cfg, max_age_sec=max_age_sec) or {}
    except Exception as e:
        print(f"[status] userinfo {getattr(fn, '__name__', 'fn')} failed: {e}")
        return {}

@app.get("/api/status")
def api_status(fresh: int = Query(0)):
    now = time.time()
    cached = STATUS_CACHE["data"]
    age = (now - STATUS_CACHE["ts"]) if cached else 1e9

    if not fresh and cached and age < STATUS_TTL:
        return JSONResponse(cached, headers={"Cache-Control": "no-store"})

    cfg = load_config() or {}
    pairs = cfg.get("pairs") or []
    any_pair_ready = any(_pair_ready(cfg, p) for p in pairs)

    probe_age = 0 if fresh else PROBE_TTL

    plex_ok  = _safe_probe(probe_plex,  cfg, max_age_sec=probe_age)
    simkl_ok = _safe_probe(probe_simkl, cfg, max_age_sec=probe_age)
    trakt_ok = _safe_probe(probe_trakt, cfg, max_age_sec=probe_age)

    jf_cfg = (cfg.get("jellyfin") or {})
    jelly_ok = bool((jf_cfg.get("server") or "").strip() and (jf_cfg.get("access_token") or "").strip())

    debug = bool(cfg.get("runtime", {}).get("debug"))

    info_plex  = _safe_userinfo(plex_user_info,  cfg, max_age_sec=USERINFO_TTL)  if plex_ok  else {}
    info_trakt = _safe_userinfo(trakt_user_info, cfg, max_age_sec=USERINFO_TTL)  if trakt_ok else {}

    data = {
        "plex_connected":     plex_ok,
        "simkl_connected":    simkl_ok,
        "trakt_connected":    trakt_ok,
        "jellyfin_connected": jelly_ok,
        "debug":              debug,
        "can_run":            bool(any_pair_ready),
        "ts":                 int(now),
        "providers": {
            "PLEX":  {
                "connected": plex_ok,
                **({} if not info_plex else {
                    "plexpass": bool(info_plex.get("plexpass")),
                    "subscription": info_plex.get("subscription") or {}
                })
            },
            "SIMKL": {"connected": simkl_ok},
            "TRAKT": {
                "connected": trakt_ok,
                **({} if not info_trakt else {
                    "vip": bool(info_trakt.get("vip")),
                    "vip_type": info_trakt.get("vip_type")
                })
            },
            "JELLYFIN": {"connected": jelly_ok},
        },
    }

    STATUS_CACHE["ts"]   = now
    STATUS_CACHE["data"] = data
    return JSONResponse(data, headers={"Cache-Control": "no-store"})

@app.post("/api/debug/clear_probe_cache")
def clear_probe_cache():
    for k in list(_PROBE_CACHE.keys()):
        _PROBE_CACHE[k] = (0.0, False)
    STATUS_CACHE["ts"] = 0.0
    STATUS_CACHE["data"] = None
    return {"ok": True}


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
    """
    Persist configuration safely:
      - Deep-merge over current config (no destructive overwrite)
      - Do not blank secrets if client sends empty/masked values
      - Normalize scrobble, strip legacy ratings, normalize pair-level ratings
      - Refresh scheduler without applying destructive changes
    """
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
    _PROBE_CACHE["plex"]  = (0.0, False)
    _PROBE_CACHE["simkl"] = (0.0, False)
    _PROBE_CACHE["trakt"] = (0.0, False)
    _PROBE_CACHE["jellyfin"] = (0.0, False)

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

# --------------- Plex PIN auth ---------------
def plex_request_pin() -> dict:
    cfg = load_config()
    plex = cfg.setdefault("plex", {})
    cid = plex.get("client_id")
    if not cid:
        import secrets as _secrets
        cid = _secrets.token_hex(12)
        plex["client_id"] = cid
        save_config(cfg)

    headers = {
        "Accept": "application/json",
        "User-Agent": "CrossWatch/1.0",
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Version": "1.0",
        "X-Plex-Client-Identifier": cid,
        "X-Plex-Platform": "Web",
    }

    try:
        from providers.auth._auth_PLEX import PROVIDER as _PLEX_PROVIDER
    except Exception:
        _PLEX_PROVIDER = None

    code = None
    pin_id = None
    try:
        if _PLEX_PROVIDER is not None:
            res = _PLEX_PROVIDER.start(cfg, redirect_uri="")
            save_config(cfg)
            code = (res or {}).get("pin")
            pend = (cfg.get("plex") or {}).get("_pending_pin") or {}
            pin_id = pend.get("id")
    except Exception as e:
        raise RuntimeError(f"Plex PIN error: {e}")

    if not code or not pin_id:
        raise RuntimeError("Plex PIN could not be issued")

    expires_epoch = int(time.time()) + 300
    return {"id": pin_id, "code": code, "expires_epoch": expires_epoch, "headers": headers}


def plex_wait_for_token(pin_id: int, headers: dict | None = None, timeout_sec: int = 300, interval: float = 1.0) -> str | None:
    try:
        from providers.auth._auth_PLEX import PROVIDER as _PLEX_PROVIDER
    except Exception:
        _PLEX_PROVIDER = None

    deadline = time.time() + max(0, int(timeout_sec))
    sleep_s = max(0.2, float(interval))

    # Ensure pending pin id is present in config (safety when restarting waiter)
    try:
        cfg0 = load_config()
        plex0 = cfg0.setdefault("plex", {})
        pend = plex0.get("_pending_pin") or {}
        if not pend.get("id") and pin_id:
            plex0["_pending_pin"] = {"id": pin_id}
            save_config(cfg0)
    except Exception:
        pass

    while time.time() < deadline:
        cfg = load_config()
        token = (cfg.get("plex") or {}).get("account_token")
        if token:
            return token
        try:
            if _PLEX_PROVIDER is not None:
                _PLEX_PROVIDER.finish(cfg)
                save_config(cfg)
        except Exception:
            pass
        time.sleep(sleep_s)
    return None


@app.post("/api/plex/pin/new")
def api_plex_pin_new() -> Dict[str, Any]:
    try:
        info = plex_request_pin()
        pin_id = info["id"]
        code = info["code"]
        exp_epoch = int(info["expires_epoch"])
        headers = info["headers"]

        # Persist pending pin info (id + code)
        cfg2 = load_config()
        plex2 = cfg2.setdefault("plex", {})
        plex2["_pending_pin"] = {"id": pin_id, "code": code}
        save_config(cfg2)

        def waiter(_pin_id: int, _headers: Dict[str, str]):
            token = plex_wait_for_token(_pin_id, headers=_headers, timeout_sec=360, interval=1.0)
            if token:
                cfg = load_config()
                cfg.setdefault("plex", {})["account_token"] = token
                save_config(cfg)
                _append_log("PLEX", "\x1b[92m[PLEX]\x1b[0m Token acquired and saved.")
                _PROBE_CACHE["plex"] = (0.0, False)
            else:
                _append_log("PLEX", "\x1b[91m[PLEX]\x1b[0m PIN expired or not authorized.")

        threading.Thread(target=waiter, args=(pin_id, headers), daemon=True).start()
        expires_in = max(0, exp_epoch - int(time.time()))
        return {"ok": True, "code": code, "pin_id": pin_id, "expiresIn": expires_in}
    except Exception as e:
        _append_log("PLEX", f"[PLEX] ERROR: {e}")
        return {"ok": False, "error": str(e)}

# --------------- SIMKL OAuth ---------------
SIMKL_STATE: Dict[str, Any] = {}

@app.post("/api/simkl/authorize")
def api_simkl_authorize(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        origin = (payload or {}).get("origin") or ""
        if not origin:
            return {"ok": False, "error": "origin missing"}

        cfg = load_config()
        simkl = cfg.get("simkl", {}) or {}
        client_id = (simkl.get("client_id") or "").strip()
        client_secret = (simkl.get("client_secret") or "").strip()
        bad_cid = (not client_id) or (client_id.upper() == "YOUR_SIMKL_CLIENT_ID")
        bad_sec = (not client_secret) or (client_secret.upper() == "YOUR_SIMKL_CLIENT_SECRET")
        if bad_cid or bad_sec:
            return {"ok": False, "error": "SIMKL client_id and client_secret must be set in settings first"}

        state = secrets.token_urlsafe(24)
        redirect_uri = f"{origin}/callback"
        SIMKL_STATE["state"] = state
        SIMKL_STATE["redirect_uri"] = redirect_uri

        url = simkl_build_authorize_url(client_id, redirect_uri, state)
        return {"ok": True, "authorize_url": url}
    except Exception as e:
        _append_log("SIMKL", f"[SIMKL] ERROR: {e}")
        return {"ok": False, "error": str(e)}


@app.get("/callback")
def oauth_simkl_callback(request: Request) -> PlainTextResponse:
    try:
        params = dict(request.query_params)
        code = params.get("code")
        state = params.get("state")
        if not code or not state:
            return PlainTextResponse("Missing code or state.", status_code=400)
        if state != SIMKL_STATE.get("state"):
            return PlainTextResponse("State mismatch.", status_code=400)

        redirect_uri = str(SIMKL_STATE.get("redirect_uri") or f"{request.base_url}callback")
        cfg = load_config()
        simkl_cfg = cfg.setdefault("simkl", {})
        client_id = (simkl_cfg.get("client_id") or "").strip()
        client_secret = (simkl_cfg.get("client_secret") or "").strip()
        bad_cid = (not client_id) or (client_id.upper() == "YOUR_SIMKL_CLIENT_ID")
        bad_sec = (not client_secret) or (client_secret.upper() == "YOUR_SIMKL_CLIENT_SECRET")
        if bad_cid or bad_sec:
            return PlainTextResponse("SIMKL client_id/secret missing or placeholders in config.", status_code=400)

        tokens = simkl_exchange_code(client_id, client_secret, code, redirect_uri)
        if not tokens or "access_token" not in tokens:
            return PlainTextResponse("SIMKL token exchange failed.", status_code=400)

        simkl_cfg["access_token"] = tokens["access_token"]
        if tokens.get("refresh_token"):
            simkl_cfg["refresh_token"] = tokens["refresh_token"]
        if tokens.get("expires_in"):
            simkl_cfg["token_expires_at"] = int(time.time()) + int(tokens["expires_in"])
        save_config(cfg)

        _append_log("SIMKL", "\x1b[92m[SIMKL]\x1b[0m Access token saved.")
        _PROBE_CACHE["simkl"] = (0.0, False)
        return PlainTextResponse("SIMKL authorized. You can close this tab and return to the app.", status_code=200)
    except Exception as e:
        _append_log("SIMKL", f"[SIMKL] ERROR: {e}")
        return PlainTextResponse(f"Error: {e}", status_code=500)


def simkl_build_authorize_url(client_id: str, redirect_uri: str, state: str) -> str:
    try:
        from providers.auth._auth_SIMKL import PROVIDER as _SIMKL_PROVIDER
    except Exception:
        _SIMKL_PROVIDER = None

    cfg = load_config()
    cfg.setdefault("simkl", {})["client_id"] = (client_id or cfg.get("simkl", {}).get("client_id") or "").strip()
    url = f"https://simkl.com/oauth/authorize?response_type=code&client_id={cfg['simkl']['client_id']}&redirect_uri={redirect_uri}"
    try:
        if _SIMKL_PROVIDER is not None:
            res = _SIMKL_PROVIDER.start(cfg, redirect_uri=redirect_uri) or {}
            url = res.get("url") or url
            save_config(cfg)
    except Exception:
        pass

    if "state=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}state={state}"
    return url


def simkl_exchange_code(client_id: str, client_secret: str, code: str, redirect_uri: str) -> dict:
    try:
        from providers.auth._auth_SIMKL import PROVIDER as _SIMKL_PROVIDER
    except Exception:
        _SIMKL_PROVIDER = None

    cfg = load_config()
    s = cfg.setdefault("simkl", {})
    s["client_id"] = client_id.strip()
    s["client_secret"] = client_secret.strip()
    try:
        if _SIMKL_PROVIDER is not None:
            _SIMKL_PROVIDER.finish(cfg, redirect_uri=redirect_uri, code=code)
            save_config(cfg)
    except Exception:
        pass

    s = load_config().get("simkl", {}) or {}
    access = s.get("access_token", "")
    refresh = s.get("refresh_token", "")
    exp_at = int(s.get("token_expires_at", 0) or 0)
    expires_in = max(0, exp_at - int(time.time())) if exp_at else 0

    out = {"access_token": access}
    if refresh:
        out["refresh_token"] = refresh
    if expires_in:
        out["expires_in"] = expires_in
    return out


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


# --------------- Wall/state endpoints ---------------
@app.get("/api/state/wall")
def api_state_wall(
    both_only: bool = Query(False),    # keep only status == "both"
    active_only: bool = Query(False),  # drop *_only items for inactive providers
) -> Dict[str, Any]:
    cfg = load_config()
    api_key = ((cfg.get("tmdb") or {}).get("api_key") or "")
    st = _load_state()
    items = _wall_items_from_state() or []

    # Build active providers map from configured pairs (now includes Jellyfin)
    active = {"plex": False, "simkl": False, "trakt": False, "jellyfin": False}
    try:
      pairs = (cfg.get("pairs") or cfg.get("connections") or []) or []
      for p in pairs:
          s = str(p.get("source") or "").strip().lower()
          t = str(p.get("target") or "").strip().lower()
          if s in active: active[s] = True
          if t in active: active[t] = True
    except Exception:
      pass

    # Filters
    def keep_item(it: Dict[str, Any]) -> bool:
        status = str(it.get("status") or "").lower()
        if both_only and status != "both":
            return False
        if active_only and status.endswith("_only"):
            prov = status.replace("_only", "")
            if not active.get(prov, False):
                return False
        return True

    items = [it for it in items if keep_item(it)]

    return {
        "ok": True,
        "items": items,
        "missing_tmdb_key": not bool(api_key),
        "last_sync_epoch": st.get("last_sync_epoch") if isinstance(st, dict) else None,
    }

# --------------- Sync providers discovery ---------------
@app.get("/api/sync/providers")
def api_sync_providers() -> JSONResponse:
    """
    Discover sync providers from providers.sync._mod_* and expose features/capabilities.
    """
    import importlib, pkgutil, dataclasses as _dc, inspect

    HIDDEN = {"BASE"}
    PKG_CANDIDATES = ("providers.sync",)
    FEATURE_KEYS = ("watchlist", "ratings", "history", "playlists")

    def _asdict_dc(obj):
        try:
            if _dc.is_dataclass(obj):
                return _dc.asdict(obj if not isinstance(obj, type) else obj())
        except Exception:
            pass
        return None

    def _norm_features(f: dict | None) -> dict:
        f = dict(f or {})
        out = {}
        for k in FEATURE_KEYS:
            v = f.get(k, False)
            out[k] = bool(v.get("enable", False)) if isinstance(v, dict) else bool(v)
        return out

    def _norm_caps(caps: dict | None) -> dict:
        caps = dict(caps or {})
        return {"bidirectional": bool(caps.get("bidirectional", False))}

    def _manifest_from_module(mod) -> dict | None:
        # 1) get_manifest()
        if hasattr(mod, "get_manifest") and callable(mod.get_manifest):  # type: ignore
            try:
                mf = dict(mod.get_manifest())  # type: ignore
            except Exception:
                mf = None
            if mf:
                if mf.get("hidden") or mf.get("is_template"):
                    return None
                return {
                    "name": (mf.get("name") or "").upper(),
                    "label": mf.get("label") or (mf.get("name") or "").title(),
                    "features": _norm_features(mf.get("features")),
                    "capabilities": _norm_caps(mf.get("capabilities")),
                    "version": mf.get("version"),
                    "vendor": mf.get("vendor"),
                    "description": mf.get("description"),
                }

        # 2) *Module.info + supported_features()
        candidates = [
            cls for _, cls in inspect.getmembers(mod, inspect.isclass)
            if cls.__module__ == mod.__name__ and cls.__name__.endswith("Module")
        ]
        if candidates:
            cls = candidates[0]
            info = getattr(cls, "info", None)
            if info is not None:
                caps = _asdict_dc(getattr(info, "capabilities", None)) or {}
                name = (getattr(info, "name", None) or getattr(cls, "__name__", "").replace("Module", "")).upper()
                label = (getattr(info, "name", None) or name).title()
                hidden = bool(getattr(info, "hidden", False)) or bool(getattr(info, "is_template", False))
                if hidden:
                    return None
                feats = {}
                try:
                    if hasattr(cls, "supported_features") and callable(getattr(cls, "supported_features")):
                        feats = dict(cls.supported_features())
                except Exception:
                    feats = {}
                return {
                    "name": name,
                    "label": label,
                    "features": _norm_features(feats),
                    "capabilities": _norm_caps(caps),
                    "version": getattr(info, "version", None),
                    "vendor": getattr(info, "vendor", None),
                    "description": getattr(info, "description", None),
                }

        # 3) OPS fallback
        ops = getattr(mod, "OPS", None)
        if ops is not None:
            try:
                name = str(ops.name()).upper()
                label = str(getattr(ops, "label")() if hasattr(ops, "label") else name.title())
                feats = dict(ops.features()) if hasattr(ops, "features") else {}
                caps = dict(ops.capabilities()) if hasattr(ops, "capabilities") else {}
                return {
                    "name": name,
                    "label": label,
                    "features": _norm_features(feats),
                    "capabilities": _norm_caps(caps),
                    "version": None,
                    "vendor": None,
                    "description": None,
                }
            except Exception:
                return None
        return None

    items, seen = [], set()
    for pkg_name in PKG_CANDIDATES:
        try:
            pkg = importlib.import_module(pkg_name)
        except Exception:
            continue
        for pkg_path in getattr(pkg, "__path__", []):
            for m in pkgutil.iter_modules([str(pkg_path)]):
                if not m.name.startswith("_mod_"):
                    continue
                prov_key = m.name.replace("_mod_", "").upper()
                if prov_key in HIDDEN:
                    continue
                try:
                    mod = importlib.import_module(f"{pkg_name}.{m.name}")
                except Exception:
                    continue
                mf = _manifest_from_module(mod)
                if not mf:
                    continue
                mf["name"] = (mf["name"] or prov_key).upper()
                mf["label"] = mf.get("label") or mf["name"].title()
                mf["features"] = _norm_features(mf.get("features"))
                mf["capabilities"] = _norm_caps(mf.get("capabilities"))
                if mf["name"] in seen:
                    continue
                seen.add(mf["name"])
                items.append(mf)

    items.sort(key=lambda x: (x.get("label") or x.get("name") or "").lower())
    return JSONResponse(items)


# --------------- Sync pairs endpoints ---------------
@app.get("/api/pairs")
def api_pairs_list() -> JSONResponse:
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        dirty = False
        for it in arr:
            newf = _normalize_features(it.get("features"))
            if newf != (it.get("features") or {}):
                it["features"] = newf
                dirty = True
        if dirty:
            save_config(cfg)
        return JSONResponse(arr)
    except Exception as e:
        try:
            _append_log("TRBL", f"/api/pairs GET failed: {e}")
        except Exception:
            pass
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/pairs")
def api_pairs_add(payload: PairIn) -> Dict[str, Any]:
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)

        item = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        item.setdefault("mode", "one-way")
        item["enabled"] = bool(item.get("enabled", False))  # default OFF
        item["features"] = _normalize_features(item.get("features") or {"watchlist": True})
        item["id"] = _gen_id("pair")
        arr.append(item)
        save_config(cfg)
        return {"ok": True, "id": item["id"]}
    except Exception as e:
        try:
            _append_log("TRBL", f"/api/pairs POST failed: {e}")
        except Exception:
            pass
        return {"ok": False, "error": str(e)}


@app.post("/api/pairs/reorder")
def api_pairs_reorder(order: List[str] = Body(...)) -> dict:
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)

        index_map = {str(p.get("id")): i for i, p in enumerate(arr)}
        wanted_ids = [pid for pid in (order or []) if pid in index_map]
        id_set = set(wanted_ids)

        head = [next(p for p in arr if str(p.get("id")) == pid) for pid in wanted_ids]
        tail = [p for p in arr if str(p.get("id")) not in id_set]
        new_arr = head + tail

        changed = any(a is not b for a, b in zip(arr, new_arr)) or (len(arr) != len(new_arr))
        if changed:
            cfg["pairs"] = new_arr
            save_config(cfg)

        unknown_ids = [pid for pid in (order or []) if pid not in index_map]
        return {
            "ok": True,
            "reordered": changed,
            "count": len(new_arr),
            "unknown_ids": unknown_ids,
            "final_order": [str(p.get("id")) for p in new_arr],
        }
    except Exception as e:
        try:
            _append_log("TRBL", f"/api/pairs/reorder failed: {e}")
        except Exception:
            pass
        return {"ok": False, "error": str(e)}


@app.put("/api/pairs/{pair_id}")
def api_pairs_update(pair_id: str, payload: PairIn) -> Dict[str, Any]:
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        for it in arr:
            if str(it.get("id")) == pair_id:
                upd = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
                for k, v in upd.items():
                    if v is None:
                        continue
                    if k == "features":
                        it["features"] = _normalize_features(v)
                    else:
                        it[k] = v
                save_config(cfg)
                return {"ok": True}
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        try:
            _append_log("TRBL", f"/api/pairs PUT failed: {e}")
        except Exception:
            pass
        return {"ok": False, "error": str(e)}


@app.delete("/api/pairs/{pair_id}")
def api_pairs_delete(pair_id: str) -> Dict[str, Any]:
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        before = len(arr)
        arr[:] = [it for it in arr if str(it.get("id")) != pair_id]
        save_config(cfg)
        return {"ok": True, "deleted": before - len(arr)}
    except Exception as e:
        try:
            _append_log("TRBL", f"/api/pairs DELETE failed: {e}")
        except Exception:
            pass
        return {"ok": False, "error": str(e)}

# --------------- TMDb artwork & metadata (via MetadataManager) ---------------

#-- globals
def _cfg_meta_ttl_secs() -> int:
    """Read TTL hours from config (metadata.ttl_hours); default 6h."""
    try:
        cfg = load_config() or {}
        md = cfg.get("metadata") or {}
        hours = int(md.get("ttl_hours", 6))
        return max(1, hours) * 3600
    except Exception:
        return 6 * 3600

def _meta_cache_enabled() -> bool:
    """Toggle via metadata.meta_cache_enable (default True)."""
    try:
        cfg = load_config() or {}
        md = cfg.get("metadata") or {}
        return bool(md.get("meta_cache_enable", True))
    except Exception:
        return True

def _meta_cache_dir() -> Path:
    d = Path(CACHE_DIR or "./.cache") / "meta"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _meta_cache_path(entity: str, tmdb_id: str | int, locale: str | None) -> Path:
    t = "movie" if str(entity).lower() == "movie" else "show"
    loc = (locale or "en-US").replace("/", "_")
    sub = _meta_cache_dir() / t
    sub.mkdir(parents=True, exist_ok=True)
    return sub / f"{tmdb_id}.{loc}.json"

def _need_satisfied(meta: dict, need: dict | None) -> bool:
    """Return True if cached meta satisfies requested fields in 'need'."""
    if not need:
        return True
    if not isinstance(meta, dict):
        return False
    def has_img(kind: str) -> bool:
        imgs = ((meta.get("images") or {}).get(kind) or [])
        return bool(imgs)
    for k, v in need.items():
        if not v:
            continue
        if k in ("poster", "backdrop", "logo"):
            if not has_img(k):
                return False
        else:
            if not meta.get(k):
                return False
    return True

def _read_meta_cache(p: Path) -> dict | None:
    try:
        if not p.exists():
            return None
        data = json.loads(p.read_text("utf-8"))
        if not isinstance(data, dict):
            return None
        ts = float(data.get("fetched_at") or 0)
        if (time.time() - ts) > _cfg_meta_ttl_secs():
            return None
        return data
    except Exception:
        return None

def _write_meta_cache(p: Path, payload: dict) -> None:
    try:
        tmp = p.with_suffix(p.suffix + ".tmp")
        data = dict(payload)
        data["fetched_at"] = time.time()
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(p)
    except Exception:
        pass

def _prune_meta_cache_if_needed() -> None:
    try:
        cfg = load_config() or {}
        md = cfg.get("metadata") or {}
        cap_mb = int(md.get("meta_cache_max_mb", 0))
        if cap_mb <= 0:
            return
        root = _meta_cache_dir()
        files = list(root.rglob("*.json"))
        total = sum(f.stat().st_size for f in files)
        cap = cap_mb * 1024 * 1024
        if total <= cap:
            return
        files.sort(key=lambda f: f.stat().st_mtime)  # oldest first
        target = int(cap * 0.9)  # prune to 90% of cap to reduce churn
        for f in files:
            try:
                total -= f.stat().st_size
                f.unlink(missing_ok=True)
            except Exception:
                pass
            if total <= target:
                break
    except Exception:
        pass

#-- MetadataManager instance (if available)
def _ttl_bucket(seconds: int) -> int:
    return int(time.time() // max(1, seconds))

@lru_cache(maxsize=4096)
def _resolve_tmdb_cached(ttl_key: int, entity: str, tmdb_id: str, locale: str | None, need_key: tuple) -> dict:
    """LRU wrapper; ttl_key forces periodic refresh."""
    if _METADATA is None:
        return {}
    need = {k: True for k in need_key} if need_key else None
    try:
        return _METADATA.resolve(entity=entity, ids={"tmdb": tmdb_id}, locale=locale, need=need) or {}
    except Exception:
        return {}

def _shorten(txt: str, limit: int = 280) -> str:
    """Human-friendly truncation that keeps whole words."""
    if not txt or len(txt) <= limit:
        return txt or ""
    cut = txt[:limit].rsplit(" ", 1)[0].rstrip(",.;:!-–—")
    return f"{cut}…"

#-- public API
def get_meta(api_key: str, typ: str, tmdb_id: str | int, cache_dir: Path | str, *,
             need: dict | None = None, locale: str | None = None) -> dict:

    if _METADATA is None:
        raise RuntimeError("MetadataManager not available")

    entity = "movie" if str(typ).lower() == "movie" else "show"
    eff_need = need or {"poster": True, "backdrop": True, "logo": False}
    need_key = tuple(sorted(k for k, v in eff_need.items() if v))
    eff_locale = locale  # may be None; provider will fall back to configured locale

    if _meta_cache_enabled():
        p = _meta_cache_path(entity, tmdb_id, eff_locale or "en-US")
        cached = _read_meta_cache(p)
        if cached and _need_satisfied(cached, eff_need):
            return cached  # fresh & satisfies need


    ttl_key = _ttl_bucket(_cfg_meta_ttl_secs())
    res = _resolve_tmdb_cached(ttl_key, entity, str(tmdb_id), eff_locale, need_key) or {}


    if res and _meta_cache_enabled():
        try:
            payload = dict(res)
            payload["locale"] = eff_locale or payload.get("locale") or None
            _write_meta_cache(_meta_cache_path(entity, tmdb_id, eff_locale or "en-US"), payload)
            _prune_meta_cache_if_needed()
        except Exception:
            pass

    return res or {}

def get_runtime(api_key: str, typ: str, tmdb_id: str | int, cache_dir: Path | str) -> int | None:
    """Minimal fetch focusing on runtime; benefits from the same caches."""
    meta = get_meta(api_key, typ, tmdb_id, cache_dir, need={"runtime_minutes": True})
    return meta.get("runtime_minutes")

def _cache_download(url: str, dest_path: Path, timeout: float = 15.0) -> Tuple[Path, str]:
    """Download once and serve from disk; keeps browser cache headers separate."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if not dest_path.exists():
        r = requests.get(url, stream=True, timeout=timeout)
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(64 * 1024):
                if chunk:
                    f.write(chunk)
    ext = dest_path.suffix.lower()
    mime = "image/jpeg" if ext in (".jpg", ".jpeg") else ("image/png" if ext == ".png" else "application/octet-stream")
    return dest_path, mime

@app.get("/art/tmdb/{typ}/{tmdb_id}")
def api_tmdb_art(typ: str = FPath(...), tmdb_id: int = FPath(...), size: str = Query("w342")):
    """Proxy TMDb artwork via local cache to keep the UI snappy and keys private."""
    t = typ.lower()
    if t == "show":
        t = "tv"
    if t not in {"movie", "tv"}:
        return PlainTextResponse("Bad type", status_code=400)

    cfg = load_config()
    api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""
    if not api_key:
        return PlainTextResponse("TMDb key missing", status_code=404)

    try:
        local_path, mime = get_poster_file(api_key, t, tmdb_id, size, CACHE_DIR)
        return FileResponse(
            path=str(local_path),
            media_type=mime,
            headers={
                "Cache-Control": "public, max-age=86400, stale-while-revalidate=86400",
            },
        )
    except Exception as e:
        return PlainTextResponse(f"Poster not available: {e}", status_code=404)

def get_poster_file(api_key: str, typ: str, tmdb_id: str | int, size: str, cache_dir: Path | str) -> tuple[str, str]:
    meta = get_meta(api_key, typ, tmdb_id, cache_dir, need={"poster": True}) or {}
    posters = ((meta.get("images") or {}).get("poster") or [])
    if not posters:
        raise FileNotFoundError("No poster found")
    src_url = posters[0]["url"]
    ext = ".jpg" if (".jpg" in src_url or ".jpeg" in src_url) else ".png"
    size_tag = (size or "w780").lower().strip()
    cache_root = Path(cache_dir or "./.cache") / "posters"
    dest = cache_root / f"{typ}_{tmdb_id}_{size_tag}{ext}"
    path, mime = _cache_download(src_url, dest)
    return str(path), mime


# --------------- Scheduling endpoints ---------------
@app.post("/api/scheduling/replan_now")
def api_scheduling_replan_now() -> Dict[str, Any]:
    cfg = load_config()
    scfg = cfg.get("scheduling") or {}
    nxt = _compute_next_run_from_cfg(scfg)

    _SCHED_HINT["next_run_at"] = int(nxt)
    _SCHED_HINT["last_saved_at"] = int(time.time())

    try:
        global scheduler
        if scheduler is not None:
            if hasattr(scheduler, "stop"):    scheduler.stop()
            if hasattr(scheduler, "start"):   scheduler.start()
            if hasattr(scheduler, "refresh"): scheduler.refresh()
    except Exception as e:
        try:
            _UIHostLogger("SYNC", "SCHED")(f"replan_now worker refresh failed: {e}", level="ERROR")
        except Exception:
            pass

    st = {}
    try:
        st = scheduler.status()
        st["config"] = scfg
        if _SCHED_HINT.get("next_run_at"):
            st["next_run_at"] = int(_SCHED_HINT["next_run_at"])
    except Exception:
        st = {"next_run_at": int(nxt)}

    return {"ok": True, **st}


@app.get("/api/scheduling")
def api_sched_get():
    cfg = load_config()
    return (cfg.get("scheduling") or {})


@app.post("/api/scheduling")
def api_sched_post(payload: dict = Body(...)):
    cfg = load_config()
    cfg["scheduling"] = (payload or {})
    save_config(cfg)
    try:
        nxt = _compute_next_run_from_cfg(cfg["scheduling"] or {})
        _SCHED_HINT["next_run_at"] = int(nxt)
        _SCHED_HINT["last_saved_at"] = int(time.time())
    except Exception:
        nxt = 0

    if (cfg["scheduling"] or {}).get("enabled"):
        scheduler.start()
        scheduler.refresh()
    else:
        scheduler.stop()

    st = scheduler.status()
    try:
        if _SCHED_HINT.get("next_run_at"):
            st["next_run_at"] = int(_SCHED_HINT["next_run_at"])
        st["config"] = cfg.get("scheduling") or {}
    except Exception:
        pass

    return {"ok": True, "next_run_at": st.get("next_run_at", int(nxt) if nxt else 0)}


@app.get("/api/scheduling/status")
def api_sched_status():
    st = scheduler.status()
    try:
        st["config"] = load_config().get("scheduling") or {}
        if _SCHED_HINT.get("next_run_at"):
            st["next_run_at"] = int(_SCHED_HINT["next_run_at"])
    except Exception:
        pass
    return st


# --------------- Troubleshooting endpoints ---------------
def _safe_remove_path(p: Path) -> bool:
    try:
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink(missing_ok=True)
        return True
    except Exception:
        return False

@app.post("/api/troubleshoot/clear-cache")
def api_trbl_clear_cache() -> Dict[str, Any]:
    """Delete contents of CACHE_DIR but keep the directory."""
    deleted_files = 0
    deleted_dirs = 0
    if CACHE_DIR.exists():
        for entry in CACHE_DIR.iterdir():
            try:
                if entry.is_dir():
                    shutil.rmtree(entry, ignore_errors=True)
                    deleted_dirs += 1
                else:
                    entry.unlink(missing_ok=True)
                    deleted_files += 1
            except Exception:
                pass
    _append_log("TRBL", "\x1b[91m[TROUBLESHOOT]\x1b[0m Cleared cache folder.")
    return {"ok": True, "deleted_files": deleted_files, "deleted_dirs": deleted_dirs}

# Clear CrossWatch state files
def _clear_cw_state_files() -> list[str]:
    from pathlib import Path
    root = CW_STATE_DIR  # e.g. CONFIG_DIR / ".cw_state"
    removed: list[str] = []
    if not root.exists():
        return removed
    for p in root.iterdir():
        if p.is_file():
            try:
                p.unlink(missing_ok=True)
                removed.append(p.name)
            except Exception:
                pass
    return removed

@app.post("/api/troubleshoot/reset-stats")
def api_trbl_reset_stats(
    recalc: bool = Body(False),
    purge_file: bool = Body(False)
) -> Dict[str, Any]:
    try:
        STATS.reset()
        if purge_file:
            try:
                STATS.path.unlink(missing_ok=True)  # nuke on-disk file
            except Exception:
                pass
            STATS._load(); STATS._save()

        if recalc:
            state = _load_state()
            if state:
                STATS.refresh_from_state(state)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/api/troubleshoot/reset-state")
def api_trbl_reset_state(
    mode: str = Body("clear_both"),  # clear_both | clear_state | clear_tombstones | clear_tombstone_entries | clear_cw_state_only | rebuild
    keep_ttl: bool = Body(True),
    ttl_override: Optional[int] = Body(None),
    feature: str = Body("watchlist"),
) -> Dict[str, Any]:
    try:
        from pathlib import Path
        import json, os

        # Direct paths — no Orchestrator in clear modes
        state_path           = CONFIG_DIR / "state.json"
        tomb_path            = CONFIG_DIR / "tombstones.json"
        last_path            = CONFIG_DIR / "last_sync.json"
        hide_path            = CONFIG_DIR / "watchlist_hide.json"
        ratings_changes_path = CONFIG_DIR / "ratings_changes.json"

        cleared_files: list[str] = []
        cw_state: Dict[str, Any] = {}

        def _try_unlink(p: Path, label: str):
            try:
                p.unlink(missing_ok=True)
                cleared_files.append(label)
            except Exception:
                pass

        def _ls_cw_files() -> list[str]:
            if not CW_STATE_DIR.exists(): return []
            return sorted([x.name for x in CW_STATE_DIR.iterdir() if x.is_file()])

        if mode in ("clear_state", "clear_both", "clear_cw_state_only"):
            pre = _ls_cw_files()
            removed = _clear_cw_state_files()
            post = _ls_cw_files()
            cw_state = {"path": str(CW_STATE_DIR), "pre": pre, "removed": removed, "post": post}

            if mode != "clear_cw_state_only":
                _try_unlink(state_path, "state.json")
                _try_unlink(last_path, "last_sync.json")
                _try_unlink(ratings_changes_path, "ratings_changes.json")
                _try_unlink(hide_path, "watchlist_hide.json")

        if mode in ("clear_tombstones", "clear_both"):
            _try_unlink(tomb_path, "tombstones.json")

        if mode == "clear_tombstone_entries":
            try:
                t = json.loads(tomb_path.read_text("utf-8")) if tomb_path.exists() else {}
            except Exception:
                t = {}
            t["keys"] = {}
            if isinstance(ttl_override, int) and ttl_override > 0:
                t["ttl_sec"] = ttl_override
            elif not keep_ttl:
                t["ttl_sec"] = 2 * 24 * 3600
            tmp = tomb_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(t, ensure_ascii=False, indent=2), "utf-8")
            os.replace(tmp, tomb_path)

        # Only rebuild path touches Orchestrator (kept for completeness)
        if mode == "rebuild":
            from .orchestrator import Orchestrator  # adjust import if needed
            cfg = load_config()
            orc = Orchestrator(config=cfg)
            state = _persist_state_via_orc(orc, feature=feature)
            STATS.refresh_from_state(state)

        if mode not in ("clear_both","clear_state","clear_tombstones","clear_tombstone_entries","clear_cw_state_only","rebuild"):
            return {"ok": False, "error": f"Unknown mode: {mode}"}

        return {"ok": True, "mode": mode, "cleared": cleared_files, "cw_state": cw_state}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# --------------- Providers registry endpoints ---------------
try:
    from providers.auth.registry import auth_providers_html, auth_providers_manifests
except Exception:
    auth_providers_html = lambda: "<div class='sub'>No providers found.</div>"
    auth_providers_manifests = lambda: []


@app.get("/api/auth/providers")
def api_auth_providers():
    return JSONResponse(auth_providers_manifests())


@app.get("/api/auth/providers/html")
def api_auth_providers_html():
    return HTMLResponse(auth_providers_html())


try:
    from providers.metadata.registry import metadata_providers_html, metadata_providers_manifests
except Exception:
    metadata_providers_html = lambda: "<div class='sub'>No metadata providers found.</div>"
    metadata_providers_manifests = lambda: []


@app.get("/api/metadata/providers")
def api_metadata_providers():
    return JSONResponse(metadata_providers_manifests())


@app.get("/api/metadata/providers/html")
def api_metadata_providers_html():
    return HTMLResponse(metadata_providers_html())


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
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()