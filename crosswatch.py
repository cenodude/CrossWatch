#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web UI backend (FastAPI) for Plex ⇄ SIMKL Watchlist Sync
"""
import requests
import json
import re
import secrets
import socket
import subprocess
import sys
import threading
import time
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
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from contextlib import asynccontextmanager
from typing import Optional

import uvicorn
from pydantic import BaseModel
from fastapi import Body, FastAPI, Request, Path as FPath, Query
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    StreamingResponse,
    PlainTextResponse,
    Response,
    FileResponse,
)
from _scheduling import SyncScheduler

ROOT = Path(__file__).resolve().parent

# --- App (create FIRST, then decorate handlers below) ---

@asynccontextmanager
async def _lifespan(app):
    # call original startup hooks, if present
    try:
        if '_on_startup' in globals():
            fn = globals()['_on_startup']
            if getattr(fn, '__code__', None) and 'async' in str(getattr(fn, '__annotations__', {})) or getattr(fn, '__name__', '').startswith('_'):
                # best-effort await if coroutine
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
app = FastAPI(lifespan=_lifespan, )

# --assets image mapping
ASSETS_DIR = ROOT / "assets"
ASSETS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")

# --- Versioning ---
CURRENT_VERSION = os.getenv("APP_VERSION", "v0.4.5")  # keep in sync with release tag....i think
REPO = os.getenv("GITHUB_REPO", "cenodude/plex-simkl-watchlist-sync")
GITHUB_API = f"https://api.github.com/repos/{REPO}/releases/latest"

router = APIRouter()


# -- Statistics (singleton) ---
STATS = Stats()

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

def _norm(v: str) -> str:
    # strip leading 'v' and spaces
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
        # Fallback: no crash; just say "unknown"
        return {"latest": None, "html_url": f"https://github.com/{REPO}/releases", "body": "", "published_at": None}

def _ttl_marker(seconds=300) -> int:
    # changes every <seconds> to invalidate lru_cache
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

# --- Favicon (SVG) ---
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

# --- /api/status memoization ---
STATUS_CACHE = {"ts": 0.0, "data": None}
STATUS_TTL = 3600  # 60 minutes

# ---------- Paths (Docker-aware) ----------
# If running from /app (typical inside a container), store config, cache, and reports under /config.
CONFIG_BASE = Path("/config") if str(ROOT).startswith("/app") else ROOT
JSON_PATH   = CONFIG_BASE / "config.json"
CONFIG_PATH = JSON_PATH  # always JSON

REPORT_DIR = CONFIG_BASE / "sync_reports"; REPORT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR = CONFIG_BASE / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATHS = [CONFIG_BASE / "state.json", ROOT / "state.json"]

HIDE_PATH   = CONFIG_BASE / "watchlist_hide.json"

# ---------- Globals ----------
SYNC_PROC_LOCK = threading.Lock()
RUNNING_PROCS: Dict[str, subprocess.Popen] = {}
MAX_LOG_LINES = 3000
LOG_BUFFERS: Dict[str, List[str]] = {"SYNC": [], "PLEX": [], "SIMKL": [], "TRBL": []}

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

# ---------- Config read/write (JSON only) ----------
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

# ---------- ANSI → HTML & logs ----------
ANSI_RE    = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_STRIP = re.compile(r"\x1b\[[0-9;]*m")

def _escape_html(s: str) -> str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def strip_ansi(s: str) -> str:
    return ANSI_STRIP.sub("", s)

# Keep combined SGR state (bold, underline, fg color, bg color)
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
        # plain text before escape
        if m.start() > pos:
            out.append(_escape_html(line[pos:m.start()]))

        codes = [c for c in (m.group(1) or "").split(";") if c != ""]
        if codes:
            # apply codes in order (SGR semantics)
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
                    # ignore other SGR codes
                    pass

            # rebuild span for the new state
            if span_open:
                out.append("</span>")
                span_open = False
            cls = state_classes()
            if cls:
                out.append(f'<span class="{" ".join(cls)}">')
                span_open = True

        pos = m.end()

    # tail text
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

# ---------- Sync Summary ----------
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
        # Return the summary with Post-sync counts included
        return dict(SUMMARY)
    
def _parse_sync_line(line: str) -> None:
    s = strip_ansi(line).strip()

    # Match sync start
    m = re.match(r"^> SYNC start:\s+(?P<cmd>.+)$", s)
    if m:
        if not SUMMARY.get("running"):
            _summary_set("running", True)
            SUMMARY["raw_started_ts"] = time.time()
            _summary_set("started_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

        # --- keep only script filename in "cmd" ---
        cmd_str = m.group("cmd")
        short_cmd = cmd_str
        try:
            # split respecting quotes
            parts = shlex.split(cmd_str)
            # find the last token that looks like a python script and strip its path
            script = next((os.path.basename(p) for p in reversed(parts) if p.endswith(".py")), None)
            if script:
                short_cmd = script
            elif parts:
                # fallback: just basename of first token
                short_cmd = os.path.basename(parts[0])
        except Exception:
            pass
        _summary_set("cmd", short_cmd)
        # -----------------------------------------

        _summary_set_timeline("start", True)
        return

    # Match version
    m = re.search(r"Version\s+(?P<ver>[0-9][0-9A-Za-z\.\-\+_]*)", s)
    if m:
        _summary_set("version", m.group("ver"))
        return

    # Match Pre-sync counts (if necessary)
    m = re.search(r"Pre-sync counts:\s+Plex=(?P<pp>\d+)\s+vs\s+SIMKL=(?P<sp>\d+)\s+\((?P<rel>[^)]+)\)", s)
    if m:
        _summary_set("plex_pre", int(m.group("pp")))
        _summary_set("simkl_pre", int(m.group("sp")))
        _summary_set_timeline("pre", True)
        return

    # Match Post-sync counts
    m = re.search(r"Post-sync:\s+Plex=(?P<pa>\d+)\s+vs\s+SIMKL=(?P<sa>\d+)\s*(?:→|->)\s*(?P<res>[A-Z]+)", s)
    if m:
        _summary_set("plex_post", int(m.group("pa")))   # Store Post-sync Plex count
        _summary_set("simkl_post", int(m.group("sa")))  # Store Post-sync SIMKL count
        _summary_set("result", m.group("res"))          # Store the result (EQUAL or others)
        _summary_set_timeline("post", True)
        return

    # Match exit code
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

        # Save the summary to a file
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

            # write report (atomic)
            try:
                ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
                path = REPORT_DIR / f"sync-{ts}.json"
                snap = _summary_snapshot()
                tmp = path.with_suffix(".tmp")
                tmp.write_text(json.dumps(snap, indent=2), encoding="utf-8")
                tmp.replace(path)
            except Exception:
                pass

            # refresh statistics.json, then enrich the latest report with added/removed
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

# Add refresh_wall() function here
def _load_hide_set() -> set:
    # Stub: return an empty set, or implement loading from file if needed
    return set()

def refresh_wall():
    # Reload the state and hidden items list
    state = _load_state()
    hidden_set = _load_hide_set()

    # Re-render the posters, marking those in the hidden set as 'deleted'
    posters = _wall_items_from_state()
    return posters  # Return the posters list directly or implement rendering logic here

# ---------- Misc helpers ----------
def get_primary_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80)); return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

# ---------- watchlist_hide.json helpers ----------
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


# ---------- state.json helpers ----------
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

# ---------- Probes (cached) ----------
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

# Startup
# migrated from on_event
async def _on_startup():
    # 1) scheduler (unchanged)
    try:
        scheduler.ensure_defaults()
        sch = (load_config().get("scheduling") or {})
        if sch.get("enabled"):
            scheduler.start()
    except Exception:
        pass

    # 2) warm statistics.json once at boot (new)
    #    - if state.json exists, compute & persist stats so /api/stats
    #      can serve week/month/added/removed immediately.
    try:
        # Only bother if we have a state
        st = _load_state()
        if not st:
            return

        # Avoid a useless write if the file already exists and is non-empty
        stats_path = (CONFIG_BASE / "statistics.json")
        if not stats_path.exists() or stats_path.stat().st_size == 0:
            STATS.refresh_from_state(st)
        else:
            # Optional: refresh anyway to ensure “generated_at” is recent and
            # samples/events are consistent (safe, atomic)
            STATS.refresh_from_state(st)
    except Exception:
        # Never block startup on stats warm-up
        pass

@app.get("/api/insights")

def api_insights(limit_samples: int = Query(60), history: int = Query(3)) -> JSONResponse:
    """
    Returns:
      - series: last N (time, count) samples from statistics.json (ascending order)
      - history: last few sync reports (date, duration, added_last, removed_last, result)
      - watchtime: estimated minutes/hours/days with method=tmdb|fallback|mixed
    """
    # --- series from statistics.json ---
    try:
        stats_raw = json.loads((CONFIG_BASE / "statistics.json").read_text(encoding="utf-8"))
    except Exception:
        stats_raw = {}
    samples = list(stats_raw.get("samples") or [])
    samples.sort(key=lambda r: int(r.get("ts") or 0))
    if limit_samples > 0:
        samples = samples[-int(limit_samples):]
    series = [{"ts": int(r.get("ts") or 0), "count": int(r.get("count") or 0)} for r in samples]

    # --- history from saved sync reports ---
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

    # --- watch time estimate ---
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

    # cap to avoid huge first-load fetch; cached files will be instant
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
        from pathlib import Path
        # same logic as Stats uses for its default path
        root = Path(__file__).resolve().parent
        base = Path("/config") if str(root).startswith("/app") else root
        p = base / "statistics.json"
        return JSONResponse(json.loads(p.read_text(encoding="utf-8")))
    except Exception:
        return JSONResponse({"ok": False}, status_code=404)
    
# migrated from on_event
async def _on_shutdown():
    try:
        scheduler.stop()
    except Exception:
        pass

@app.middleware("http")
async def cache_headers_for_api(request: Request, call_next):
    resp = await call_next(request)
    # Never cache JSON/API responses in the browser
    if request.url.path.startswith("/api/"):
        resp.headers["Cache-Control"] = "no-store"
        # Optional: also kill intermediary cache
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
    return resp

@app.get("/api/stats")
def api_stats() -> Dict[str, Any]:
    # Persisted stats (from statistics.json)
    base = STATS.overview(None)  # don't pass state here; use persisted file

    # If a sync is actively running, override "now" with a LIVE UNION from state.json
    snap = _summary_snapshot() if callable(globals().get("_summary_snapshot", None)) else {}
    try:
        if bool(snap.get("running")):
            state = _load_state()
            if state:
                # compute union count across Plex ∪ SIMKL using the same canonicalizer as stats
                base["now"] = len(Stats._union_keys(state))
        # No “grace window” after finish; statistics.json is already refreshed at the end of the run
    except Exception:
        pass

    return base

@app.get("/api/logs/stream")
def api_logs_stream_initial(tag: str = Query("SYNC")):
    tag = (tag or "SYNC").upper()

    def gen():
        # dump existing lines first
        buf = LOG_BUFFERS.get(tag, [])
        for line in buf:
            yield f"data: {line}\n\n"
        # then follow new lines
        idx = len(buf)
        while True:
            new_buf = LOG_BUFFERS.get(tag, [])
            while idx < len(new_buf):
                yield f"data: {new_buf[idx]}\n\n"
                idx += 1
            time.sleep(0.25)

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control":"no-store"})

# --- Watchlist API (grid page) ---
@app.get("/api/watchlist")
def api_watchlist() -> JSONResponse:
    cfg = load_config()
    st = _load_state()
    api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""

    # If no state at all: return ok:false but HTTP 200 (frontend expects JSON, not 404)
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
    sp = _find_state_path() or (CONFIG_BASE / "state.json")
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

# -------- Scheduler wiring --------
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

# Instantiate scheduler
scheduler = SyncScheduler(load_config, save_config, run_sync_fn=_start_sync_from_scheduler, is_sync_running_fn=_is_sync_running)

INDEX_HTML = get_index_html()

@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)

@app.get("/api/status")
def api_status(fresh: int = Query(0)):
    now = time.time()
    cached = STATUS_CACHE["data"]
    age = (now - STATUS_CACHE["ts"]) if cached else 1e9

    # If we have a recent cache and not forcing, just return it (0 external calls)
    if not fresh and cached and age < STATUS_TTL:
        return JSONResponse(cached, headers={"Cache-Control": "no-store"})

    # Otherwise (fresh=1 OR cache expired/missing), do at most two external probes
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

# ---- PLEX auth ----
@app.post("/api/plex/pin/new")
def api_plex_pin_new() -> Dict[str, Any]:
    try:
        info = plex_request_pin()
        pin_id = info["id"]; code = info["code"]; exp_epoch = int(info["expires_epoch"]); headers = info["headers"]
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

# ---- SIMKL OAuth ----
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

# ---- Run & Summary ----
@app.post("/api/run")
def api_run_sync() -> Dict[str, Any]:
    sync_script = ROOT / "plex_simkl_watchlist_sync.py"
    if not sync_script.exists():
        return {"ok": False, "error": "plex_simkl_watchlist_sync.py not found"}

    with SYNC_PROC_LOCK:
        if "SYNC" in RUNNING_PROCS and RUNNING_PROCS["SYNC"].poll() is None:
            return {"ok": False, "error": "Sync already running"}
        
        # Start the sync process
        cmd = [sys.executable, str(sync_script), "--sync"]
        start_proc_detached(cmd, tag="SYNC")
        
        # Notify frontend to refresh the Watchlist Preview
        # This could be done by setting a flag or calling a function to refresh the UI.
        refresh_watchlist_preview()  # This is where you can trigger the refresh

        return {"ok": True}

def refresh_watchlist_preview():
    # Trigger a refresh of the Watchlist Preview on the frontend
    # This could send a response to the frontend, or you can call a JavaScript function via SSE/WebSocket
    # Example: Notify frontend to reload the watchlist grid
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

# ---- TMDb & wall ----
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
                # prevent browsers from reusing stale posters
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    except Exception as e:
        return PlainTextResponse(f"Poster not available: {e}", status_code=404)

@app.get("/api/tmdb/meta/{typ}/{tmdb_id}")
def api_tmdb_meta(typ: str = FPath(...), tmdb_id: int = FPath(...)) -> Dict[str, Any]:
    typ = typ.lower()
    if typ == "show": typ = "tv"
    cfg = load_config(); api_key = (cfg.get("tmdb", {}) or {}).get("api_key") or ""
    if not api_key:
        return {"ok": False, "error": "TMDb key missing"}
    try:
        meta = get_meta(api_key, typ, tmdb_id, CACHE_DIR)
        return {"ok": True, **meta}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# --- Scheduling API ---
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

# --- Troubleshoot API ---
def _safe_remove_path(p: Path) -> bool:
    try:
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink(missing_ok=True)
        return True
    except Exception:
        return False

@app.post("/api/troubleshoot/reset-stats")
def api_trbl_reset_stats() -> Dict[str, Any]:
    try:
        with STATS.lock:
            # Reinitialize to known-good defaults
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
        # Return the error so the UI can display it
        return {"ok": False, "error": str(e)}
    
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

@app.post("/api/troubleshoot/reset-state")
def api_trbl_reset_state() -> Dict[str, Any]:
    """Ask the sync script to rebuild state.json asynchronously (logged under TRBL)."""
    sync_script = ROOT / "plex_simkl_watchlist_sync.py"
    if not sync_script.exists():
        return {"ok": False, "error": "plex_simkl_watchlist_sync.py not found"}
    cmd = [sys.executable, str(sync_script), "--reset-state"]
    start_proc_detached(cmd, tag="TRBL")
    return {"ok": True, "started": True}

# ---- Main ----
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

if __name__ == "__main__":
    main()

# ---- Platform Manager ----
try:
    from cw_platform.manager import PlatformManager as _PlatformMgr
    _PLATFORM = _PlatformMgr(load_config, save_config)
except Exception as _e:
    _PLATFORM = None
    print("PlatformManager not available:", _e)

