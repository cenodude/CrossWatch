from __future__ import annotations

# CrossWatch Web API (FastAPI)
# Compact backend exposing status, auth, scheduling, and watchlist utilities.

def _json_safe(obj):
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    return str(obj)

import requests
import json
import re
import secrets
import socket
import subprocess
import sys
import threading
import time
import uuid
import os
import shutil
import shlex
import urllib.request
import urllib.error
import urllib.parse
from _statistics import Stats
from pydantic import BaseModel
from fastapi import Query
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from fastapi.responses import JSONResponse
from _watchlist import build_watchlist, delete_watchlist_item
from _FastAPI import get_index_html
from functools import lru_cache
from packaging.version import Version, InvalidVersion
from fastapi import APIRouter, HTTPException
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager
from typing import Optional
from typing import Literal

from pathlib import Path
ROOT = Path(__file__).resolve().parent
import sys
sys.path.insert(0, str(ROOT))  # make local packages importable early

import uvicorn
from fastapi import Body, FastAPI, Request, Path as FPath
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    StreamingResponse,
    PlainTextResponse,
    Response,
    FileResponse,
)
from _scheduling import SyncScheduler
from cw_platform.config_base import CONFIG

STATE_PATH = CONFIG / "state.json"
_METADATA = None


class MetadataResolveIn(BaseModel):
    entity: str                  # "movie" | "show"
    ids: dict                    # bv. {"tmdb": "123", "imdb": "tt..."}
    locale: str | None = None
    need: dict | None = None     # bv. {"poster": True}
    strategy: str | None = "first_success"

class PairIn(BaseModel):
    source: str
    target: str
    mode: str | None = None
    enabled: bool | None = None
    features: dict | None = None

def _cfg_pairs(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    arr = cfg.get("pairs")
    if not isinstance(arr, list):
        arr = []
        cfg["pairs"] = arr
    return arr

def _gen_id(prefix: str = "pair") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"

@asynccontextmanager
async def _lifespan(app):
    try:
        if '_on_startup' in globals():
            fn = globals()['_on_startup']
            if getattr(fn, '__code__', None) and 'async' in str(getattr(fn, '__annotations__', {})) or getattr(fn, '__name__', '').startswith('_'):
                res = fn()
                try:
                    import inspect, asyncio
                    if inspect.iscoroutine(res):
                        await res
                except Exception:
                    pass
            else:
                try:
                    fn()
                except Exception:
                    pass
    except Exception:
        pass
    try:
        yield
    finally:
        try:
            if '_on_shutdown' in globals():
                fn2 = globals()['_on_shutdown']
                res2 = fn2()
                try:
                    import inspect, asyncio
                    if inspect.iscoroutine(res2):
                        await res2
                except Exception:
                    pass
        except Exception:
            pass
# App setup
app = FastAPI(lifespan=_lifespan, )

ASSETS_DIR = ROOT / "assets"
ASSETS_DIR.mkdir(parents=True, exist_ok=True)
# Static assets
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")

CURRENT_VERSION = os.getenv("APP_VERSION", "v0.4.5")  # keep in sync with release tag....i think
REPO = os.getenv("GITHUB_REPO", "cenodude/plex-simkl-watchlist-sync")
GITHUB_API = f"https://api.github.com/repos/{REPO}/releases/latest"

router = APIRouter()

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
        
STATS = Stats()

# Version endpoints
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
        "html_url": html_url,  # <-- add this
        "url": html_url,       # <-- keep alias just in case UI expects `url`
        "body": cache.get("body", ""),
        "published_at": cache.get("published_at"),
    }

# Version helpers
def _norm(v: str) -> str:
    return re.sub(r"^\s*v", "", v.strip(), flags=re.IGNORECASE)

@lru_cache(maxsize=1)
def _cached_latest_release(_marker: int) -> dict:
    """
    Cached lookup. _marker allows us to control TTL via a changing integer.
    """
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "Plex-SIMKL-Watchlist-Sync"
    }
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
    except Exception as e:
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

STATUS_CACHE = {"ts": 0.0, "data": None}
STATUS_TTL = 3600  # 60 minutes

CONFIG_BASE = Path("/config") if str(ROOT).startswith("/app") else ROOT
JSON_PATH   = CONFIG_BASE / "config.json"
CONFIG_PATH = JSON_PATH  # always JSON

REPORT_DIR = CONFIG_BASE / "sync_reports"; REPORT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR = CONFIG_BASE / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATHS = [CONFIG_BASE / "state.json", ROOT / "state.json"]

HIDE_PATH   = CONFIG_BASE / "watchlist_hide.json"

SYNC_PROC_LOCK = threading.Lock()
RUNNING_PROCS: Dict[str, subprocess.Popen] = {}
MAX_LOG_LINES = 3000
LOG_BUFFERS: Dict[str, List[str]] = {"SYNC": [], "PLEX": [], "SIMKL": [], "TRBL": []}

class _UIHostLogger:
    def __init__(self, tag: str = "SYNC", module_name: str | None = None, base_ctx: dict | None = None):
        self._tag = tag
        self._module = module_name
        self._ctx = dict(base_ctx or {})

    def __call__(self, message: str, *, level: str = "INFO", module: str | None = None, extra: dict | None = None) -> None:
        m = module or self._module or self._ctx.get("module")
        lvl = (level or "INFO").upper()
        prefix = f"[{m}]" if m else ""
        try:
            _append_log(self._tag, f"{prefix} {message}".strip())
        except Exception:
            print(f"{self._tag}: {prefix} {message}")

    def set_context(self, **ctx):
        self._ctx.update(ctx)

    def get_context(self) -> dict:
        return dict(self._ctx)

    def bind(self, **ctx):
        c = dict(self._ctx); c.update(ctx)
        return _UIHostLogger(self._tag, self._module, c)

    def child(self, name: str):
        return _UIHostLogger(self._tag, name, dict(self._ctx))


SIMKL_STATE: Dict[str, Any] = {}

DEFAULT_CFG: Dict[str, Any] = {
    "plex": {"account_token": ""},
    "simkl": {
        "client_id": "YOUR_SIMKL_CLIENT_ID",
        "client_secret": "YOUR_SIMKL_CLIENT_SECRET",
        "access_token": "",
        "refresh_token": "",
        "token_expires_at": 0,
    },
    "tmdb": {"api_key": ""},
    "sync": {
        "enable_add": True,
        "enable_remove": True,
        "verify_after_write": True,
        "bidirectional": {"enabled": True, "mode": "two-way", "source_of_truth": "plex"},
    },
    "runtime": {"debug": False},
}

def _read_json(p: Path) -> Dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def _write_json(p: Path, data: Dict[str, Any]) -> None:
    tmp = p.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    tmp.replace(p)

def load_config() -> Dict[str, Any]:
    if JSON_PATH.exists():
        try:
            return _read_json(JSON_PATH)
        except Exception:
            pass
    cfg = DEFAULT_CFG.copy()
    save_config(cfg)
    return cfg

def save_config(cfg: Dict[str, Any]) -> None:
    _write_json(JSON_PATH, cfg)

def _is_placeholder(val: str, placeholder: str) -> bool:
    return (val or "").strip().upper() == placeholder.upper()

ANSI_RE    = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_STRIP = re.compile(r"\x1b\[[0-9;]*m")

def _escape_html(s: str) -> str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def strip_ansi(s: str) -> str:
    return ANSI_STRIP.sub("", s)

_FG_CODES = {"30","31","32","33","34","35","36","37","90","91","92","93","94","95","96","97"}
_BG_CODES = {"40","41","42","43","44","45","46","47","100","101","102","103","104","105","106","107"}

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
                if c == "0":                      # full reset
                    state.update({"b": False, "u": False, "fg": None, "bg": None})
                elif c == "1":                    # bold on
                    state["b"] = True
                elif c == "22":                   # bold off
                    state["b"] = False
                elif c == "4":                    # underline on
                    state["u"] = True
                elif c == "24":                   # underline off
                    state["u"] = False
                elif c in _FG_CODES:              # set foreground
                    state["fg"] = c
                elif c == "39":                   # default foreground
                    state["fg"] = None
                elif c in _BG_CODES:              # set background
                    state["bg"] = c
                elif c == "49":                   # default background
                    state["bg"] = None
                else:
                    pass

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

# Logging utilities
def _append_log(tag: str, raw_line: str) -> None:
    html = ansi_to_html(raw_line.rstrip("\n"))
    buf = LOG_BUFFERS.setdefault(tag, [])
    buf.append(html)
    if len(buf) > MAX_LOG_LINES:
        LOG_BUFFERS[tag] = buf[-MAX_LOG_LINES:]

SUMMARY_LOCK = threading.Lock()
SUMMARY: Dict[str, Any] = {}
# Sync summary
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
            "plex_post": None,  # Ensure Plex Post-sync is initialized
            "simkl_post": None,  # Ensure SIMKL Post-sync is initialized
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
        SUMMARY["timeline"][flag] = value

def _summary_snapshot() -> Dict[str, Any]:
    with SUMMARY_LOCK:
        return dict(SUMMARY)
    
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

    m = re.search(r"Pre-sync counts:\s+Plex=(?P<pp>\d+)\s+vs\s+SIMKL=(?P<sp>\d+)\s+\((?P<rel>[^)]+)\)", s)
    if m:
        _summary_set("plex_pre", int(m.group("pp")))
        _summary_set("simkl_pre", int(m.group("sp")))
        _summary_set_timeline("pre", True)
        return

    m = re.search(r"Post-sync:\s+Plex=(?P<pa>\d+)\s+vs\s+SIMKL=(?P<sa>\d+)\s*(?:→|->)\s*(?P<res>[A-Z]+)", s)
    if m:
        _summary_set("plex_post", int(m.group("pa")))   # Store Post-sync Plex count
        _summary_set("simkl_post", int(m.group("sa")))  # Store Post-sync SIMKL count
        _summary_set("result", m.group("res"))          # Store the result (EQUAL or others)
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
            ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            path = REPORT_DIR / f"sync-{ts}.json"
            with path.open("w", encoding="utf-8") as f:
                json.dump(_summary_snapshot(), f, indent=2)
        except Exception:
            pass

def _stream_proc(cmd: List[str], tag: str) -> None:
    try:
        if tag == "SYNC":
            _summary_reset()
            _summary_set("running", True)
            SUMMARY["raw_started_ts"] = time.time()
            _summary_set("started_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
            _summary_set_timeline("start", True)
            try:
                _summary_set("cmd", " ".join(cmd))
            except Exception:
                pass
            try:
                if not _summary_snapshot().get("version"):
                    _summary_set("version", _norm(CURRENT_VERSION))
            except Exception:
                pass

        line0 = f"> {tag} start: {' '.join(cmd)}"
        _append_log(tag, line0)
        if tag == "SYNC":
            _parse_sync_line(line0)

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=str(ROOT),
        )
        RUNNING_PROCS[tag] = proc
        assert proc.stdout is not None

        for line in proc.stdout:
            _append_log(tag, line)
            if tag == "SYNC":
                _parse_sync_line(line)

        rc = proc.wait()
        _append_log(tag, f"[{tag}] exit code: {rc}")

        if tag == "SYNC" and rc == 0:
            _clear_watchlist_hide()

        if tag == "SYNC" and _summary_snapshot().get("exit_code") is None:
            _summary_set("exit_code", rc)
            started = _summary_snapshot().get("raw_started_ts")
            if started:
                dur = max(0.0, time.time() - float(started))
                _summary_set("duration_sec", round(dur, 2))
            _summary_set("finished_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
            _summary_set("running", False)
            _summary_set_timeline("done", True)

            try:
                ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
                path = REPORT_DIR / f"sync-{ts}.json"
                snap = _summary_snapshot()
                tmp = path.with_suffix(".tmp")
                tmp.write_text(json.dumps(snap, indent=2), encoding="utf-8")
                tmp.replace(path)
            except Exception:
                pass

            try:
                try:
                    STATS.refresh_from_state(_load_state())
                except Exception:
                    pass

                ov = STATS.overview(None)
                added_last = int(ov.get("new", 0))
                removed_last = int(ov.get("del", 0))

                reports = sorted(REPORT_DIR.glob("sync-*.json"), key=lambda p: p.stat().st_mtime)
                if reports:
                    latest = reports[-1]
                    data = json.loads(latest.read_text(encoding="utf-8"))
                    data["added_last"] = added_last
                    data["removed_last"] = removed_last
                    tmp2 = latest.with_suffix(".tmp")
                    tmp2.write_text(json.dumps(data, indent=2), encoding="utf-8")
                    tmp2.replace(latest)
            except Exception:
                pass

    except Exception as e:
        _append_log(tag, f"[{tag}] ERROR: {e}")
    finally:
        RUNNING_PROCS.pop(tag, None)

def start_proc_detached(cmd: List[str], tag: str) -> None:
    threading.Thread(target=_stream_proc, args=(cmd, tag), daemon=True).start()

def _load_hide_set() -> set:
    return set()

def refresh_wall():
    state = _load_state()
    hidden_set = _load_hide_set()

    posters = _wall_items_from_state()
    return posters  # Return the posters list directly or implement rendering logic here

def get_primary_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80)); return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

def _clear_watchlist_hide() -> None:
    """After sync, clear watchlist_hide.json if it exists (atomic)."""
    try:
        p = HIDE_PATH
        if not p.exists():
            return
        tmp = p.with_suffix(p.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump([], f)  # empty list
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

def _parse_epoch(v: Any) -> int:
    """Accept integer seconds, float, or ISO 8601 strings (returns epoch seconds)."""
    if v is None: return 0
    try:
        if isinstance(v, (int, float)): return int(v)
        s = str(v).strip()
        if s.isdigit(): return int(s)
        s = s.replace("Z","+00:00")
        try:
            dt = datetime.fromisoformat(s)
            return int(dt.timestamp())
        except Exception:
            return 0
    except Exception:
        return 0

def _pick_added(d: Dict[str, Any]) -> Optional[str]:
    """Find a plausible 'added at' timestamp in various shapes and normalize to UTC Z."""
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

def _tmdb_genres(api_key: str, typ: str, tmdb_id: int, ttl_days: int = 14) -> List[str]:
    """Fetch & cache TMDb genres for movie/tv. Safe fallback to []."""
    try:
        meta_dir = CACHE_DIR / "tmdb_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)
        fpath = meta_dir / f"{typ}-{tmdb_id}.json"

        fresh = False
        if fpath.exists():
            age = time.time() - fpath.stat().st_mtime
            if age < ttl_days * 86400:
                fresh = True

        data = None
        if fresh:
            try:
                data = json.loads(fpath.read_text(encoding="utf-8"))
            except Exception:
                data = None

        if data is None:
            url = f"https://api.themoviedb.org/3/{'tv' if typ=='tv' else 'movie'}/{tmdb_id}?api_key={api_key}&language=en-US"
            with urllib.request.urlopen(url, timeout=8) as resp:
                raw = resp.read()
            fpath.write_bytes(raw)
            data = json.loads(raw.decode("utf-8", errors="ignore"))

        genres = []
        for g in (data.get("genres") or []):
            name = g.get("name")
            if isinstance(name, str) and name.strip():
                genres.append(name.strip())
        return genres[:8]
    except Exception:
        return []

def _wall_items_from_state() -> List[Dict[str, Any]]:
    """Build watchlist preview items from state.json, newest-first."""
    st = _load_state()
    plex_items = (st.get("plex", {}) or {}).get("items", {}) or {}
    simkl_items = (st.get("simkl", {}) or {}).get("items", {}) or {}

    cfg = load_config()
    api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""

    out: List[Dict[str, Any]] = []
    all_keys = set(plex_items.keys()) | set(simkl_items.keys())

    def iso_to_epoch(iso: Optional[str]) -> int:
        if iso is None: return 0
        try:
            s = str(iso).strip()
            if s.isdigit(): return int(s)
            s = s.replace("Z", "+00:00")
            return int(datetime.fromisoformat(s).timestamp())
        except Exception:
            return 0

    for key in all_keys:
        p = plex_items.get(key) or {}
        s = simkl_items.get(key) or {}
        info = p or s
        if not info:
            continue

        typ_raw = (info.get("type") or "").lower()
        typ = "tv" if typ_raw in ("tv", "show") else "movie"

        title = info.get("title") or info.get("name") or ""
        year = info.get("year") or info.get("release_year")
        tmdb_id = (info.get("ids", {}) or {}).get("tmdb") or info.get("tmdb")

        p_when = _pick_added(p)
        s_when = _pick_added(s)
        p_ep = iso_to_epoch(p_when)
        s_ep = iso_to_epoch(s_when)

        if p_ep >= s_ep:
            added_when = p_when
            added_epoch = p_ep
            added_src = "plex" if p else ("simkl" if s else "")
        else:
            added_when = s_when
            added_epoch = s_ep
            added_src = "simkl" if s else ("plex" if p else "")

        status = "both" if key in plex_items and key in simkl_items else ("plex_only" if key in plex_items else "simkl_only")

        categories: List[str] = []
        if api_key and tmdb_id:
            try:
                categories = _tmdb_genres(api_key, typ, int(tmdb_id))
            except Exception:
                categories = []

        out.append({
            "key": key,
            "type": typ,
            "title": title,
            "year": year,
            "tmdb": tmdb_id,
            "status": status,
            "added_epoch": added_epoch,
            "added_when": added_when,
            "added_src": added_src,
            "categories": categories,
        })

    out.sort(key=lambda x: (x.get("added_epoch") or 0, x.get("year") or 0), reverse=True)
    return out

_PROBE_CACHE: Dict[str, Tuple[float, bool]] = {"plex": (0.0, False), "simkl": (0.0, False)}
def _http_get(url: str, headers: Dict[str, str], timeout: int = 8) -> Tuple[int, bytes]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.getcode(), r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read() if e.fp else b""
    except Exception:
        return 0, b""

def probe_plex(cfg: Dict[str, Any], max_age_sec: int = 30) -> bool:
    ts, ok = _PROBE_CACHE["plex"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok
    token = (cfg.get("plex", {}) or {}).get("account_token") or ""
    if not token:
        _PROBE_CACHE["plex"] = (now, False); return False
    headers = {
        "X-Plex-Token": token,
        "X-Plex-Client-Identifier": "plex-simkl-sync-webui",
        "X-Plex-Product": "PlexSimklSync",
        "X-Plex-Version": "1.0",
        "Accept": "application/xml",
        "User-Agent": "Mozilla/5.0",
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
    simkl = cfg.get("simkl", {}) or {}
    cid = (simkl.get("client_id") or "").strip()
    tok = (simkl.get("access_token") or "").strip()
    if not cid or not tok:
        _PROBE_CACHE["simkl"] = (now, False); return False
    headers = {
        "Authorization": f"Bearer {tok}",
        "simkl-api-key": cid,
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    }
    code, _ = _http_get("https://api.simkl.com/users/settings", headers=headers, timeout=8)
    ok = (code == 200)
    _PROBE_CACHE["simkl"] = (now, ok)
    return ok

def connected_status(cfg: Dict[str, Any]) -> Tuple[bool, bool, bool]:
    plex_ok = probe_plex(cfg)
    simkl_ok = probe_simkl(cfg)
    debug = bool(cfg.get("runtime", {}).get("debug"))
    return plex_ok, simkl_ok, debug

def get_tmdb_api_key():
    cfg = getattr(app.state, "cfg", {}) or {}
    return os.getenv("TMDB_API_KEY") or ((cfg.get("tmdb") or {}).get("api_key"))

async def _on_startup():
    try:
        app.state.cfg = load_config() or {}

        global _METADATA
        _METADATA = MetadataManager(load_config, save_config)

        scheduler.ensure_defaults()
        sch = (app.state.cfg.get("scheduling") or {})
        if sch.get("enabled"):
            scheduler.start()
    except Exception:
        pass

    try:
        st = _load_state()
        if not st:
            return
        stats_path = (CONFIG_BASE / "statistics.json")
        if not stats_path.exists() or stats_path.stat().st_size == 0:
            STATS.refresh_from_state(st)
        else:
            STATS.refresh_from_state(st)
    except Exception:
        pass

@app.get("/api/insights")

def api_insights(limit_samples: int = Query(60), history: int = Query(3)) -> JSONResponse:
    """
    Returns:
      - series: last N (time, count) samples from statistics.json (ascending order)
      - history: last few sync reports (date, duration, added_last, removed_last, result)
      - watchtime: estimated minutes/hours/days with method=tmdb|fallback|mixed
    """
    try:
        stats_raw = json.loads((CONFIG / "statistics.json").read_text(encoding="utf-8"))
    except Exception:
        stats_raw = {}
    samples = list(stats_raw.get("samples") or [])
    samples.sort(key=lambda r: int(r.get("ts") or 0))
    if limit_samples > 0:
        samples = samples[-int(limit_samples):]
    series = [{"ts": int(r.get("ts") or 0), "count": int(r.get("count") or 0)} for r in samples]

    rows = []
    try:
        files = sorted(REPORT_DIR.glob("sync-*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:max(1, int(history))]
        for p in files:
            try:
                d = json.loads(p.read_text(encoding="utf-8"))
                rows.append({
                    "started_at": d.get("started_at"),
                    "finished_at": d.get("finished_at"),
                    "duration_sec": d.get("duration_sec"),
                    "result": d.get("result"),
                    "plex_post": d.get("plex_post"),
                    "simkl_post": d.get("simkl_post"),
                    "added": d.get("added_last"),   # may be None on older reports
                    "removed": d.get("removed_last")
                })
            except Exception:
                continue
    except Exception:
        pass

    state = _load_state()
    union = {}
    try:
        union = Stats._union_keys(state) if state else {}
    except Exception:
        union = {}

    plex_items = ((state.get("plex") or {}).get("items") or {}) if state else {}
    simkl_items = ((state.get("simkl") or {}).get("items") or {}) if state else {}

    cfg = load_config()
    api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""
    use_tmdb = bool(api_key)

    movies = shows = 0
    total_min = 0
    tmdb_hits = tmdb_misses = 0

    fetch_cap = 50
    fetched = 0

    for k, meta in (union or {}).items():
        typ = "movie" if (meta.get("type") or "") == "movie" else "tv"
        src = plex_items.get(k) or simkl_items.get(k) or {}
        ids = (src.get("ids") or {})
        tmdb_id = ids.get("tmdb") or src.get("tmdb")

        if typ == "movie": movies += 1
        else: shows += 1

        minutes = None
        if use_tmdb and tmdb_id and fetched < fetch_cap:
            try:
                minutes = get_runtime(api_key, typ, int(tmdb_id), CACHE_DIR)  # <-- use helper from _TMDB.py
                fetched += 1
                if minutes is not None:
                    tmdb_hits += 1
                else:
                    tmdb_misses += 1
            except Exception:
                tmdb_misses += 1

        if minutes is None:
            minutes = 115 if typ == "movie" else 45

        total_min += int(minutes)

    method = "tmdb" if tmdb_hits and not tmdb_misses else ("mixed" if tmdb_hits else "fallback")

    watchtime = {
        "movies": movies,
        "shows": shows,
        "minutes": total_min,
        "hours": round(total_min / 60, 1),
        "days": round(total_min / 60 / 24, 1),
        "method": method
    }

    return JSONResponse({"series": series, "history": rows, "watchtime": watchtime})

@app.get("/api/stats/raw")
def api_stats_raw():
    try:
        p = CONFIG / "statistics.json"
        return JSONResponse(json.loads(p.read_text(encoding="utf-8")))
    except Exception:
        return JSONResponse({"ok": False}, status_code=404)
    
async def _on_shutdown():
    try:
        scheduler.stop()
    except Exception:
        pass

@app.middleware("http")
async def cache_headers_for_api(request: Request, call_next):
    resp = await call_next(request)
    if request.url.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp

@app.get("/api/stats")
def api_stats() -> Dict[str, Any]:
    base = STATS.overview(None)  # don't pass state here; use persisted file

    snap = _summary_snapshot() if callable(globals().get("_summary_snapshot", None)) else {}
    try:
        if bool(snap.get("running")):
            state = _load_state()
            if state:
                base["now"] = len(Stats._union_keys(state))
    except Exception:
        pass

    return base

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

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control":"no-store"})

# Watchlist endpoints
@app.get("/api/watchlist")
def api_watchlist() -> JSONResponse:
    cfg = load_config()
    st = _load_state()
    api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""

    if not st:
        return JSONResponse(
            {"ok": False, "error": "No state.json found or empty.", "missing_tmdb_key": not bool(api_key)},
            status_code=200,
        )
    try:
        items = build_watchlist(st, tmdb_api_key_present=bool(api_key))
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e), "missing_tmdb_key": not bool(api_key)}, status_code=200)

    if not items:
        return JSONResponse(
            {"ok": False, "error": "No state data found.", "missing_tmdb_key": not bool(api_key)},
            status_code=200,
        )

    return JSONResponse(
        {
            "ok": True,
            "items": items,
            "missing_tmdb_key": not bool(api_key),
            "last_sync_epoch": st.get("last_sync_epoch"),
        },
        status_code=200,
    )

@app.delete("/api/watchlist/{key}")
def api_watchlist_delete(key: str = FPath(...)) -> JSONResponse:
    sp = STATE_PATH
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
                for side in ("plex", "simkl"):
                    items = ((state.get(side) or {}).get("items") or {})
                    if key in items:
                        items.pop(key, None)
                STATS.refresh_from_state(state)
            except Exception:
                pass

        status = 200 if result.get("ok") else 400
        return JSONResponse(result, status_code=status)

    except Exception as e:
        _append_log("TRBL", f"[WATCHLIST] ERROR: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
    

@app.get("/favicon.svg", include_in_schema=False)
def favicon_svg():
    return Response(content=FAVICON_SVG, media_type="image/svg+xml")

@app.get("/favicon.ico", include_in_schema=False)
def favicon_ico():
    return Response(content=FAVICON_SVG, media_type="image/svg+xml")

# Scheduler wiring
def _is_sync_running() -> bool:
    p = RUNNING_PROCS.get("SYNC")
    try:
        return p is not None and (p.poll() is None)
    except Exception:
        return False

def _start_sync_from_scheduler() -> bool:
    if _is_sync_running():
        return False
    sync_script = ROOT / "plex_simkl_watchlist_sync.py"
    if not sync_script.exists():
        return False
    cmd = [sys.executable, str(sync_script), "--sync"]
    start_proc_detached(cmd, tag="SYNC")
    return True

scheduler = SyncScheduler(load_config, save_config, run_sync_fn=_start_sync_from_scheduler, is_sync_running_fn=_is_sync_running)

INDEX_HTML = get_index_html()

@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)

# Status endpoints
@app.get("/api/status")
def api_status(fresh: int = Query(0)):
    now = time.time()
    cached = STATUS_CACHE["data"]
    age = (now - STATUS_CACHE["ts"]) if cached else 1e9

    if not fresh and cached and age < STATUS_TTL:
        return JSONResponse(cached, headers={"Cache-Control": "no-store"})

    cfg = load_config()
    plex_ok  = probe_plex(cfg,  max_age_sec=STATUS_TTL)   # pass 3600 to internal probe cache too
    simkl_ok = probe_simkl(cfg, max_age_sec=STATUS_TTL)
    debug    = bool(cfg.get("runtime", {}).get("debug"))
    data = {
        "plex_connected": plex_ok,
        "simkl_connected": simkl_ok,
        "debug": debug,
        "can_run": bool(plex_ok and simkl_ok),
        "ts": int(now),
    }
    STATUS_CACHE["ts"] = now
    STATUS_CACHE["data"] = data
    return JSONResponse(data, headers={"Cache-Control": "no-store"})

@app.get("/api/config")
def api_config() -> JSONResponse:
    return JSONResponse(load_config())

@app.post("/api/config")
def api_config_save(cfg: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    save_config(cfg)
    _PROBE_CACHE["plex"] = (0.0, False)
    _PROBE_CACHE["simkl"] = (0.0, False)
    return {"ok": True}

@app.post("/api/plex/pin/new")
def api_plex_pin_new() -> Dict[str, Any]:
    try:
        info = plex_request_pin()
        pin_id = info["id"]; code = info["code"]; exp_epoch = int(info["expires_epoch"]); headers = info["headers"]
        cfg2 = load_config(); plex2 = cfg2.setdefault('plex', {})
        plex2['_pending_pin'] = {'id': pin_id, 'code': code}; save_config(cfg2)

        def waiter(_pin_id: int, _headers: Dict[str, str]):
            token = plex_wait_for_token(_pin_id, headers=_headers, timeout_sec=360, interval=1.0)
            if token:
                cfg = load_config(); cfg.setdefault("plex", {})["account_token"] = token; save_config(cfg)
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

@app.post("/api/simkl/authorize")
def api_simkl_authorize(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    try:
        origin = (payload or {}).get("origin") or ""
        if not origin:
            return {"ok": False, "error": "origin missing"}
        cfg = load_config(); simkl = cfg.get("simkl", {}) or {}
        client_id = (simkl.get("client_id") or "").strip(); client_secret = (simkl.get("client_secret") or "").strip()
        bad_cid = (not client_id) or _is_placeholder(client_id, "YOUR_SIMKL_CLIENT_ID")
        bad_sec = (not client_secret) or _is_placeholder(client_secret, "YOUR_SIMKL_CLIENT_SECRET")
        if bad_cid or bad_sec:
            return {"ok": False, "error": "SIMKL client_id and client_secret must be set in settings first"}
        state = secrets.token_urlsafe(24); redirect_uri = f"{origin}/callback"
        SIMKL_STATE["state"] = state; SIMKL_STATE["redirect_uri"] = redirect_uri
        url = simkl_build_authorize_url(client_id, redirect_uri, state)
        return {"ok": True, "authorize_url": url}
    except Exception as e:
        _append_log("SIMKL", f"[SIMKL] ERROR: {e}")
        return {"ok": False, "error": str(e)}

@app.get("/callback")
def oauth_simkl_callback(request: Request) -> PlainTextResponse:
    try:
        params = dict(request.query_params); code = params.get("code"); state = params.get("state")
        if not code or not state: return PlainTextResponse("Missing code or state.", status_code=400)
        if state != SIMKL_STATE.get("state"): return PlainTextResponse("State mismatch.", status_code=400)
        redirect_uri = str(SIMKL_STATE.get("redirect_uri") or f"{request.base_url}callback")
        cfg = load_config(); simkl_cfg = cfg.setdefault("simkl", {})
        client_id = (simkl_cfg.get("client_id") or "").strip(); client_secret = (simkl_cfg.get("client_secret") or "").strip()
        bad_cid = (not client_id) or _is_placeholder(client_id, "YOUR_SIMKL_CLIENT_ID")
        bad_sec = (not client_secret) or _is_placeholder(client_secret, "YOUR_SIMKL_CLIENT_SECRET")
        if bad_cid or bad_sec: return PlainTextResponse("SIMKL client_id/secret missing or placeholders in config.", status_code=400)
        tokens = simkl_exchange_code(client_id, client_secret, code, redirect_uri)
        if not tokens or "access_token" not in tokens: return PlainTextResponse("SIMKL token exchange failed.", status_code=400)
        simkl_cfg["access_token"] = tokens["access_token"]
        if tokens.get("refresh_token"): simkl_cfg["refresh_token"] = tokens["refresh_token"]
        if tokens.get("expires_in"): simkl_cfg["token_expires_at"] = int(time.time()) + int(tokens["expires_in"])
        save_config(cfg); _append_log("SIMKL", "\x1b[92m[SIMKL]\x1b[0m Access token saved.")
        _PROBE_CACHE["simkl"] = (0.0, False)
        return PlainTextResponse("SIMKL authorized. You can close this tab and return to the app.", status_code=200)
    except Exception as e:
        _append_log("SIMKL", f"[SIMKL] ERROR: {e}")
        return PlainTextResponse(f"Error: {e}", status_code=500)

# Run & summary endpoints
@app.post("/api/run")
def api_run_sync() -> Dict[str, Any]:
    with SYNC_PROC_LOCK:
        if "SYNC" in RUNNING_PROCS and RUNNING_PROCS["SYNC"] is not None:
            try:
                p = RUNNING_PROCS["SYNC"]
                if hasattr(p, "poll") and p.poll() is None:
                    return {"ok": False, "error": "Sync already running"}
            except Exception:
                pass
        run_id = str(int(time.time()))
        th = threading.Thread(target=_run_pairs_thread, args=(run_id,), daemon=True)
        th.start()
        RUNNING_PROCS["SYNC"] = None  # marker
        _append_log("SYNC", f"[i] Triggered sync run {run_id}")
        return {"ok": True, "run_id": run_id}

def refresh_watchlist_preview():
    print("Triggering refresh of the watchlist preview")

@app.get("/api/run/summary")
def api_run_summary() -> JSONResponse:
    return JSONResponse(_summary_snapshot())

@app.get("/api/run/summary/file")
def api_run_summary_file() -> Response:
    js = json.dumps(_summary_snapshot(), indent=2)
    return Response(content=js, media_type="application/json", headers={"Content-Disposition": 'attachment; filename="last_sync.json"'})

@app.get("/api/run/summary/stream")
def api_run_summary_stream() -> StreamingResponse:
    def gen():
        last_key = None
        while True:
            time.sleep(0.25)
            snap = _summary_snapshot()
            key = (snap.get("running"), snap.get("exit_code"), snap.get("plex_post"), snap.get("simkl_post"),
                   snap.get("result"), snap.get("duration_sec"), (snap.get("timeline", {}) or {}).get("done"))
            if key != last_key:
                last_key = key
                yield f"data: {json.dumps(snap, separators=(',',':'))}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")

@app.get("/api/state/wall")
def api_state_wall() -> Dict[str, Any]:
    cfg = load_config()
    api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""
    st = _load_state()
    items = _wall_items_from_state()
    if not items:
        return {"ok": False, "error": "No state.json found or empty.", "missing_tmdb_key": not bool(api_key)}
    return {
        "ok": True,
        "items": items,
        "missing_tmdb_key": not bool(api_key),
        "last_sync_epoch": st.get("last_sync_epoch"),
    }

# TMDb artwork
@app.get("/art/tmdb/{typ}/{tmdb_id}")
def api_tmdb_art(typ: str = FPath(...), tmdb_id: int = FPath(...), size: str = Query("w342")):
    typ = typ.lower()
    if typ == "show": typ = "tv"
    if typ not in {"movie", "tv"}:
        return PlainTextResponse("Bad type", status_code=400)
    cfg = load_config(); api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""
    if not api_key:
        return PlainTextResponse("TMDb key missing", status_code=404)
    try:
        local_path, mime = get_poster_file(api_key, typ, tmdb_id, size, CACHE_DIR)
        return FileResponse(
            path=str(local_path),
            media_type=mime,
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    except Exception as e:
        return PlainTextResponse(f"Poster not available: {e}", status_code=404)

@app.get("/api/tmdb/meta/{typ}/{tmdb_id}")
def api_tmdb_meta_path(
    typ: Literal["movie", "show", "tv"],
    tmdb_id: int,
    lang: str = Query("en-US")
):
    if _METADATA is None:
        return JSONResponse({"ok": False, "error": "MetadataManager not initialized"}, status_code=500)

    entity = "show" if typ == "tv" else typ  # back-compat

    try:
        res = _METADATA.resolve(
            entity=entity,
            ids={"tmdb": tmdb_id},
            locale=lang,
            need={"poster": True, "backdrop": True, "title": True, "year": True},
            strategy="first_success",
        )
        if not res:
            return JSONResponse({"ok": False, "error": "No metadata"}, status_code=404)
        return {"ok": True, "result": res}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

# Scheduling API
@app.get("/api/scheduling")
def api_sched_get():
    cfg = load_config()
    return (cfg.get("scheduling") or {})

@app.post("/api/scheduling")
def api_sched_post(payload: dict = Body(...)):
    cfg = load_config()
    cfg["scheduling"] = (payload or {})
    save_config(cfg)
    if (cfg["scheduling"] or {}).get("enabled"):
        scheduler.start(); scheduler.refresh()
    else:
        scheduler.stop()
    st = scheduler.status()
    return {"ok": True, "next_run_at": st.get("next_run_at", 0)}

@app.get("/api/scheduling/status")
def api_sched_status():
    return scheduler.status()

def _safe_remove_path(p: Path) -> bool:
    try:
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink(missing_ok=True)
        return True
    except Exception:
        return False

# Troubleshooting
@app.post("/api/troubleshoot/reset-stats")
def api_trbl_reset_stats() -> Dict[str, Any]:
    try:
        with STATS.lock:
            STATS.data = {
                "events": [],
                "samples": [],
                "current": {},
                "counters": {"added": 0, "removed": 0},
                "last_run": {"added": 0, "removed": 0, "ts": 0},
            }
            STATS._save()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    
# Troubleshooting
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

# Troubleshooting
@app.post("/api/troubleshoot/reset-state")
def api_trbl_reset_state() -> Dict[str, Any]:
    """Ask the sync script to rebuild state.json asynchronously (logged under TRBL)."""
    sync_script = ROOT / "plex_simkl_watchlist_sync.py"
    if not sync_script.exists():
        return {"ok": False, "error": "plex_simkl_watchlist_sync.py not found"}
    cmd = [sys.executable, str(sync_script), "--reset-state"]
    start_proc_detached(cmd, tag="TRBL")
    return {"ok": True, "started": True}

# Main entry
def main(host: str = "0.0.0.0", port: int = 8787) -> None:
    ip = get_primary_ip()
    print("\nPlex ⇄ SIMKL Web UI running:")
    print(f"  Local:   http://127.0.0.1:{port}")
    print(f"  Docker:  http://{ip}:{port}")
    print(f"  Bind:    {host}:{port}")
    print(f"  Config:  {CONFIG_PATH} (JSON)")
    print(f"  Cache:   {CACHE_DIR}")
    print(f"  Reports: {REPORT_DIR}\n")
    uvicorn.run(app, host=host, port=port)

try:
    from providers.auth.registry import auth_providers_html, auth_providers_manifests
except Exception as _e:
    auth_providers_html = lambda : "<div class='sub'>No providers found.</div>"
    auth_providers_manifests = lambda : []

@app.get("/api/auth/providers")
def api_auth_providers():
    return JSONResponse(auth_providers_manifests())

@app.get("/api/auth/providers/html")
def api_auth_providers_html():
    return HTMLResponse(auth_providers_html())

try:
    from providers.metadata.registry import metadata_providers_html, metadata_providers_manifests
except Exception as _e:
    metadata_providers_html = lambda : "<div class='sub'>No metadata providers found.</div>"
    metadata_providers_manifests = lambda : []

@app.get("/api/metadata/providers")
def api_metadata_providers():
    return JSONResponse(metadata_providers_manifests())

@app.get("/api/metadata/providers/html")
def api_metadata_providers_html():
    return HTMLResponse(metadata_providers_html())

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

# --- main entry ---
if __name__ == "__main__":
    main()

from typing import Tuple

def plex_request_pin() -> dict:
    cfg = load_config(); plex = cfg.setdefault('plex', {})
    cid = plex.get('client_id')
    if not cid:
        import secrets
        cid = secrets.token_hex(12)
        plex['client_id'] = cid
        save_config(cfg)
    headers = {
        'Accept': 'application/json',
        'User-Agent': 'CrossWatch/1.0',
        'X-Plex-Product': 'CrossWatch',
        'X-Plex-Version': '1.0',
        'X-Plex-Client-Identifier': cid,
        'X-Plex-Platform': 'Web',
    }
    """
    Start Plex PIN flow via provider. Returns legacy-shaped dict.
    """
    try:
        from providers.auth._auth_PLEX import PROVIDER as _PLEX_PROVIDER
    except Exception:
        _PLEX_PROVIDER = None

    cfg = load_config()
    code = None
    pin_id = None
    try:
        if _PLEX_PROVIDER is not None:
            res = _PLEX_PROVIDER.start(cfg, redirect_uri="")
            save_config(cfg)
            code = (res or {}).get("pin")
            pend = (cfg.get("plex") or {}).get("_pending_pin") or {}
            pin_id = pend.get("id")
        elif _PLATFORM is not None:
            res = _PLATFORM.auth_start("PLEX", {})
            cfg = load_config()
            save_config(cfg)
            code = (res or {}).get("pin")
            pend = (cfg.get("plex") or {}).get("_pending_pin") or {}
            pin_id = pend.get("id")
    except Exception as e:
        raise RuntimeError(f"Plex PIN error: {e}")

    if not code or not pin_id:
        raise RuntimeError("Plex PIN could not be issued")

    expires_epoch = int(time.time()) + 300
    return {"id": pin_id, "code": code, "expires_epoch": expires_epoch, "headers": {}}

def plex_wait_for_token(pin_id: int, headers: dict | None = None, timeout_sec: int = 300, interval: float = 1.0) -> str | None:
    """Poll provider.finish() until token appears in config or timeout."""
    try:
        from providers.auth._auth_PLEX import PROVIDER as _PLEX_PROVIDER
    except Exception:
        _PLEX_PROVIDER = None

    deadline = time.time() + max(0, int(timeout_sec))
    sleep_s = max(0.2, float(interval))
    try:
        cfg0 = load_config(); plex0 = cfg0.setdefault('plex', {})
        pend = plex0.get('_pending_pin') or {}
        if not pend.get('id') and pin_id:
            pend = {'id': pin_id}
            plex0['_pending_pin'] = pend
            save_config(cfg0)
    except Exception:
        pass

    while time.time() < deadline:
        cfg = load_config()
        token = (cfg.get('plex') or {}).get('account_token')
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

def get_meta(api_key: str, typ: str, tmdb_id: str | int, cache_dir: Path | str) -> dict:
    """
    Delegate metadata to MetadataManager (TMDb provider normalizes fields).
    """
    if _METADATA is None:
        raise RuntimeError("MetadataManager not available")
    entity = "movie" if str(typ).lower() == "movie" else "show"
    res = _METADATA.resolve(entity=entity, ids={"tmdb": str(tmdb_id)}, locale=None,
                            need={"poster": True, "backdrop": True, "logo": False})
    return res or {}

def get_runtime(api_key: str, typ: str, tmdb_id: str | int, cache_dir: Path | str) -> int | None:
    """
    Read normalized runtime from metadata.
    """
    meta = get_meta(api_key, typ, tmdb_id, cache_dir) or {}
    return meta.get("runtime_minutes")

def _cache_download(url: str, dest_path: Path, timeout: float = 15.0) -> Tuple[Path, str]:
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

def get_poster_file(api_key: str, typ: str, tmdb_id: str | int, size: str, cache_dir: Path | str) -> tuple[str, str]:
    """
    Resolve poster URL via provider and cache locally.
    """
    meta = get_meta(api_key, typ, tmdb_id, cache_dir) or {}
    posters = ((meta.get("images") or {}).get("poster") or [])
    if not posters:
        raise FileNotFoundError("No poster found")
    src_url = posters[0]["url"]
    ext = ".jpg" if ".jpg" in src_url or ".jpeg" in src_url else ".png"
    size_tag = (size or "w780").lower().strip()
    cache_root = Path(cache_dir or "./.cache") / "posters"
    dest = cache_root / f"{typ}_{tmdb_id}_{size_tag}{ext}"
    path, mime = _cache_download(src_url, dest)
    return str(path), mime

def simkl_build_authorize_url(client_id: str, redirect_uri: str, state: str) -> str:
    """
    Build SIMKL OAuth authorize URL using the provider, preserving state.
    """
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
    """
    Exchange SIMKL authorization code for tokens via provider. Returns minimal token dict.
    """
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

@app.get("/api/sync/providers")
def api_sync_providers() -> JSONResponse:
    """
    Return provider list with features/capabilities.
    Hidden/templates are skipped. Only successfully imported providers are listed.
    """
    import importlib, pkgutil, dataclasses as _dc, inspect

    HIDDEN = {"BASE"}
    PKG_CANDIDATES = ("providers.sync",)

    def ensure(items, prov_key):
        return items.setdefault(
            prov_key,
            {
                "name": prov_key,
                "label": prov_key.title(),
                "features": {"watchlist": True, "ratings": True, "history": True, "playlists": True},
                "capabilities": {"bidirectional": False},
            },
        )

    items = {}

    for pkg_name in PKG_CANDIDATES:
        try:
            pkg = importlib.import_module(pkg_name)
        except Exception:
            continue

        for pkg_path in getattr(pkg, "__path__", []):
            for m in pkgutil.iter_modules([str(pkg_path)]):
                name = m.name
                if not name.startswith("_mod_"):
                    continue

                prov_key = name.replace("_mod_", "").upper()
                if prov_key in HIDDEN:
                    continue

                try:
                    mod = importlib.import_module(f"{pkg_name}.{name}")
                except Exception:
                    continue

                candidates = [
                    cls
                    for _, cls in inspect.getmembers(mod, inspect.isclass)
                    if cls.__module__ == mod.__name__
                    and cls.__name__.endswith("Module")
                    and hasattr(cls, "info")
                ]
                if not candidates:
                    continue

                mod_cls = candidates[0]
                info = getattr(mod_cls, "info", None)
                if not info:
                    continue
                if bool(getattr(info, "hidden", False)) or bool(getattr(info, "is_template", False)):
                    continue

                entry = ensure(items, prov_key)
                entry["label"] = getattr(info, "name", prov_key).title()

                caps = getattr(info, "capabilities", None)
                caps_dict = {}
                try:
                    if caps is not None:
                        if _dc.is_dataclass(caps):
                            caps_dict = _dc.asdict(caps)
                        elif isinstance(caps, dict):
                            caps_dict = dict(caps)
                        else:
                            for k in ("bidirectional",):
                                if hasattr(caps, k):
                                    caps_dict[k] = getattr(caps, k)
                except Exception:
                    caps_dict = {}

                bidir = caps_dict.get("bidirectional", getattr(caps, "bidirectional", False))
                if not bidir:
                    modes = getattr(info, "supported_modes", None) or getattr(mod_cls, "supported_modes", None) or []
                    try:
                        bidir = any(str(m).lower() == "two-way" for m in modes)
                    except Exception:
                        pass

                entry["capabilities"]["bidirectional"] = bool(bidir)

    return JSONResponse(list(items.values()))

class PairIn(BaseModel):
    source: str
    target: str
    mode: str | None = None          # e.g., "one-way" | "two-way"
    enabled: bool | None = None
    features: dict | None = None     # e.g., {"watchlist": true}
@app.get("/api/pairs")
def api_pairs_list() -> JSONResponse:
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        return JSONResponse(_json_safe(arr))
    except Exception as e:
        try: _append_log("TRBL", f"/api/pairs GET failed: {e}")
        except Exception: pass
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/pairs")
def api_pairs_add(payload: PairIn) -> Dict[str, Any]:
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        item = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
        item.setdefault("mode", "one-way")
        item["enabled"] = True if item.get("enabled", True) is not False else False
        f = item.get("features") or {"watchlist": True}
        if isinstance(f.get("watchlist"), bool):
            f["watchlist"] = {"add": bool(f["watchlist"]), "remove": False}
        item["features"] = f
        if any(x for x in arr if str(x.get("source","")).upper()==str(item["source"]).upper() and str(x.get("target","")).upper()==str(item["target"]).upper()):
            return {"ok": False, "error": "duplicate"}
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

@app.put("/api/pairs/{pair_id}")
def api_pairs_update(pair_id: str, payload: PairIn) -> Dict[str, Any]:
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        for it in arr:
            if it.get("id") == pair_id:
                upd = payload.model_dump() if hasattr(payload, "model_dump") else payload.dict()
                for k, v in upd.items():
                    if v is None:
                        continue
                    if k == "features":
                        f = v or {}
                        if isinstance(f.get("watchlist"), bool):
                            f["watchlist"] = {"add": bool(f["watchlist"]), "remove": False}
                        it["features"] = f
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
        n = len(arr)
        arr[:] = [it for it in arr if it.get("id") != pair_id]
        save_config(cfg)
        return {"ok": True, "deleted": n - len(arr)}
    except Exception as e:
        try: _append_log("TRBL", f"/api/pairs DELETE failed: {e}")
        except Exception: pass
        return {"ok": False, "error": str(e)}
        


def _safe_get(d: dict, *path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict): return default
        cur = cur.get(k, default)
    return cur

def _count_plex(cfg: Dict[str, Any]) -> int:
    try:
        from providers.sync._mod_PLEX import plex_fetch_watchlist_items, gather_plex_rows
        token = _safe_get(cfg, "plex", "account_token", default="") or ""
        items = plex_fetch_watchlist_items(None, token, debug=False)  # falls back to Discover if plexapi not present
        rows = gather_plex_rows(items)
        return len(rows)
    except Exception as e:
        _append_log("SYNC", f"[!] Plex count failed: {e}")
        return 0

def _count_simkl(cfg: Dict[str, Any]) -> int:
    try:
        from providers.sync._mod_SIMKL import simkl_ptw_full
        s = dict(cfg.get("simkl") or {})
        shows, movies = simkl_ptw_full(s)
        return len(shows) + len(movies)
    except Exception as e:
        _append_log("SYNC", f"[!] SIMKL count failed: {e}")
        return 0

def _build_index_for(cfg: Dict[str, Any], name: str) -> Dict[str, dict]:
    n = name.upper()
    try:
        if n == "PLEX":
            from providers.sync._mod_PLEX import plex_fetch_watchlist_items, gather_plex_rows, build_index
            token = _safe_get(cfg, "plex", "account_token", default="") or ""
            items = plex_fetch_watchlist_items(None, token, debug=False)
            rows = gather_plex_rows(items)
            rows_movies = [r for r in rows if r.get("type") == "movie"]
            rows_shows  = [r for r in rows if r.get("type") == "show"]
            return build_index(rows_movies, rows_shows)
        elif n == "SIMKL":
            from providers.sync._mod_SIMKL import simkl_ptw_full, build_index_from_simkl
            s = dict(cfg.get("simkl") or {})
            shows, movies = simkl_ptw_full(s)
            return build_index_from_simkl(movies, shows)
        else:
            return {}
    except Exception as e:
        _append_log("SYNC", f"[!] Index build for {name} failed: {e}")
        return {}

def _items_by_type_from_keys(idx: Dict[str, dict], keys: list[str]) -> Dict[str, list]:
    out = {"movies": [], "shows": []}
    for k in keys:
        it = idx.get(k) or {}
        typ = it.get("type") or "movie"
        entry = {"ids": it.get("ids") or {}}
        if typ == "show": out["shows"].append(entry)
        else: out["movies"].append(entry)
    return {k:v for k,v in out.items() if v}

def _apply_add_to(name: str, cfg: Dict[str, Any], items_by_type: Dict[str, list]) -> Dict[str, Any]:
    n = name.upper()
    try:
        hostlog = _UIHostLogger("SYNC", module_name=n)
        if n == "SIMKL":
            from providers.sync._mod_SIMKL import SIMKLModule
            mod = SIMKLModule(cfg, hostlog)
            return mod.simkl_add_to_ptw(items_by_type, dry_run=False)
        elif n == "PLEX":
            from providers.sync._mod_PLEX import PLEXModule
            mod = PLEXModule(cfg, hostlog)
            return mod.plex_add(items_by_type, dry_run=False)
        else:
            return {"ok": False, "error": f"Unknown target {name}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def _run_pairs_thread(run_id: str) -> None:
    _summary_reset()
    _parse_sync_line(f"> SYNC start: orchestrator pairs run_id={run_id}")
    _append_log("SYNC", f"[i] Starting sync run {run_id}")

    cfg = load_config()
    pairs = list(cfg.get("pairs") or [])
    if not pairs:
        _append_log("SYNC", "[!] No pairs defined; nothing to do") 
        # Write state snapshot for the UI
        try:
            import importlib
            orch = importlib.import_module("cw_platform.orchestrator")
            snapshot = orch.build_state({}, {})
            orch.write_state(snapshot, STATE_PATH)
            STATS.refresh_from_state(snapshot)
            _append_log("SYNC", "[i] state.json updated (empty snapshot)")
        except Exception as e:
            _append_log("SYNC", f"[!] state.json write failed: {e}")

    # Pre counts
    try:
        plex_pre = _count_plex(cfg)
        simkl_pre = _count_simkl(cfg)
        _parse_sync_line(f"Pre-sync: Plex={plex_pre} vs SIMKL={simkl_pre}")
    except Exception as e:
        _append_log("SYNC", f"[!] Pre-sync count error: {e}")
        plex_pre = simkl_pre = 0

    added_total = 0
    for i, pair in enumerate(pairs, start=1):
        if not pair or not pair.get("enabled", True):
            continue
        if not (pair.get("features", {}).get("watchlist", False)):
            _append_log("SYNC", f"[i] Pair {i}: {pair.get('source')}→{pair.get('target')} — watchlist disabled; skip")
            continue

        src = str(pair.get("source", "")).upper()
        dst = str(pair.get("target", "")).upper()
        mode = (pair.get("mode") or "one-way").lower()

        _append_log("SYNC", f"[i] Pair {i}: {src} → {dst} (mode={mode})")

        idx_src = _build_index_for(cfg, src)
        idx_dst = _build_index_for(cfg, dst)
        keys_src = set(idx_src.keys()); keys_dst = set(idx_dst.keys())
        to_add_keys = sorted(list(keys_src - keys_dst))

        items_by_type = _items_by_type_from_keys(idx_src, to_add_keys)
        if not items_by_type:
            _append_log("SYNC", f"[i] Pair {i}: nothing to add")
        else:
            res = _apply_add_to(dst, cfg, items_by_type)
            if not res.get("ok"):
                _append_log("SYNC", f"[!] Pair {i}: add to {dst} failed: {res.get('error')}")
            else:
                added = int(res.get("added", 0))
                added_total += added
                _append_log("SYNC", f"[i] Pair {i}: added {added} items to {dst}")

        # optional: simple two-way (mirror back) zonder uitgebreide diff
        if mode in ("two-way", "bi-directional", "bidirectional"):
            back_keys = sorted(list(keys_dst - keys_src))
            items_back = _items_by_type_from_keys(idx_dst, back_keys)
            if not items_back:
                _append_log("SYNC", f"[i] Pair {i}: nothing to add {dst}→{src}")
            else:
                res2 = _apply_add_to(src, cfg, items_back)
                if not res2.get("ok"):
                    _append_log("SYNC", f"[!] Pair {i}: add to {src} failed: {res2.get('error')}")
                else:
                    added2 = int(res2.get("added", 0))
                    added_total += added2
                    _append_log("SYNC", f"[i] Pair {i}: added {added2} items to {src}")

    # Post counts
    try:
        plex_post = _count_plex(cfg)
        simkl_post = _count_simkl(cfg)
        result = "EQUAL" if plex_post == simkl_post else "UPDATED"
        _parse_sync_line(f"Post-sync: Plex={plex_post} vs SIMKL={simkl_post} -> {result}")
    except Exception as e:
        _append_log("SYNC", f"[!] Post-sync count error: {e}")

    # Write state snapshot for the UI
    try:
        # runtime import to avoid pylance/module-path noise
        from cw_platform.orchestrator import build_state, write_state
        plex_idx  = _build_index_for(cfg, "PLEX")
        simkl_idx = _build_index_for(cfg, "SIMKL")
        snapshot  = build_state(plex_idx, simkl_idx)
        write_state(snapshot, STATE_PATH)
        STATS.refresh_from_state(snapshot)
        _append_log("SYNC", "[i] state.json updated")
    except Exception as e:
        _append_log("SYNC", f"[!] state.json write failed: {e}")

    _append_log("SYNC", f"[i] Done. Total added: {added_total}")
    _parse_sync_line("[SYNC] exit code: 0")
