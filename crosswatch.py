# /crosswatch.py
# CrossWatch main application entry point
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from typing import Any, Dict, List, Optional
from collections.abc import AsyncIterator

from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, quote

import sys
sys.modules.setdefault("crosswatch", sys.modules[__name__])
import os
import re
import socket
import threading
import time
import uvicorn
import asyncio

# Mute Unverified HTTPS request warnings from requests/urllib3
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Internal imports
from api import (
    register as register_api,
    _is_sync_running,
    _load_state,
    api_run_sync,
)

from api.appAuthAPI import (
    COOKIE_NAME as APP_AUTH_COOKIE,
    auth_required as app_auth_required,
    is_authenticated as app_is_authenticated,
    setup_lock_required as app_auth_setup_lock_required,
    register_app_auth,
)

from _logging import log as LOG, BLUE, GREEN, DIM, RESET  # type: ignore

def _c(text: str, color: str) -> str:
    return f"{color}{text}{RESET}" if LOG.use_color else text

from api.versionAPI import CURRENT_VERSION
from services import register as register_services
from fastapi import FastAPI, Query, Request
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    StreamingResponse,
)

from api.wallAPI import _load_wall_snapshot
from providers.webhooks.plextrakt import process_webhook as process_webhook
from providers.webhooks.jellyfintrakt import process_webhook as process_webhook_jellyfin

__all__ = ["process_webhook", "process_webhook_jellyfin"]

from ui_frontend import (
    register_assets_and_favicons,
    register_ui_root,
)
from services.scheduling import SyncScheduler
from services.statistics import Stats

from cw_platform.orchestrator import Orchestrator, minimal
from cw_platform.config_base import load_config, save_config, CONFIG as CONFIG_DIR
from cw_platform.tls import ensure_self_signed_cert, resolve_tls_paths
from cw_platform.orchestrator import canonical_key

from zoneinfo import ZoneInfo

# Paths and globals
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
CW_STATE_DIR = (CONFIG_DIR / ".cw_state"); CW_STATE_DIR.mkdir(parents=True, exist_ok=True)

_METADATA: Any = None
scheduler: Optional[SyncScheduler] = None
_AUTH_RESET_ENV_APPLIED = False

STATS = Stats()

_DEBUG_CACHE = {"ts": 0.0, "val": False}
_DEBUG_HTTP_CACHE = {"ts": 0.0, "val": False}
_DEBUG_MODS_CACHE = {"ts": 0.0, "val": False}

RUNNING_PROCS: Dict[str, threading.Thread] = {}
SYNC_PROC_LOCK = threading.Lock()

# Debug helpers
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


def _resolve_config_scoped_path(raw_path: str) -> Path:
    raw = str(raw_path or "").strip()
    if not raw:
        raise ValueError("Missing path")

    cfg_root = CONFIG_DIR.resolve()
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = cfg_root / candidate
    candidate = candidate.resolve()
    try:
        candidate.relative_to(cfg_root)
    except ValueError as e:
        raise ValueError("Invalid path") from e
    return candidate


def _env_truthy(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _apply_auth_reset_env_once() -> None:
    global _AUTH_RESET_ENV_APPLIED
    if _AUTH_RESET_ENV_APPLIED:
        return
    _AUTH_RESET_ENV_APPLIED = True

    if not _env_truthy("CW_RESET_AUTH_ONCE"):
        return

    cfg_path = CONFIG_DIR / "config.json"
    if not cfg_path.exists():
        print("[BOOT] CW_RESET_AUTH_ONCE ignored: no existing config.json found.")
        return

    try:
        cfg = load_config() or {}
        a = cfg.setdefault("app_auth", {})
        if not isinstance(a, dict):
            a = {}
            cfg["app_auth"] = a

        pwd = a.setdefault("password", {})
        if not isinstance(pwd, dict):
            pwd = {}
            a["password"] = pwd

        sess = a.setdefault("session", {})
        if not isinstance(sess, dict):
            sess = {}
            a["session"] = sess

        a["enabled"] = False
        a["username"] = ""
        a["reset_required"] = True
        pwd["salt"] = ""
        pwd["hash"] = ""
        sess["token_hash"] = ""
        sess["expires_at"] = 0
        a["sessions"] = []
        a["last_login_at"] = 0
        save_config(cfg)
        print("[BOOT] CW_RESET_AUTH_ONCE detected: app authentication was reset. Remove the env var and set a new username/password in the UI.")
    except Exception as exc:
        print(f"[BOOT] CW_RESET_AUTH_ONCE failed: {exc}")

def _is_static_noise(path: str, status: int) -> bool:
    if path.startswith("/assets/") or path.startswith("/favicon"):
        return True
    if path.endswith((".css", ".js", ".mjs", ".map", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".ico", ".svg", ".woff", ".woff2", ".ttf")):
        return True

    # Redirects
    if status in (301, 302, 303, 304, 307, 308):
        return True

    if status == 404 and (
        path.startswith("/art/") or
        path.startswith("/assets/img/") or
        "/placeholder" in path
    ):
        return True
    return False

_SENSITIVE_QUERY_KEYS = {
    "token", "access_token", "refresh_token", "client_secret",
    "api_key", "apikey", "code", "state", "password", "secret",
    "session_id", "x-plex-token",
}

_APP_AUTH_SETUP_ALLOWED_PATHS = {
    "/",
    "/healthz",
    "/login",
    "/logout",
    "/api/health",
    "/api/config/meta",
    "/api/app-auth/status",
    "/api/app-auth/credentials",
}

_PUBLIC_HEALTH_PATHS = {
    "/api/health",
    "/healthz",
}

def _redact_query_string(query: str) -> str:
    q = (query or "").strip()
    if not q:
        return ""
    try:
        pairs = parse_qsl(q, keep_blank_values=True)
    except Exception:
        return ""
    redacted: list[tuple[str, str]] = []
    for k, v in pairs:
        if str(k).strip().lower() in _SENSITIVE_QUERY_KEYS:
            redacted.append((k, "••••"))
        else:
            redacted.append((k, v))
    try:
        return urlencode(redacted, doseq=True)
    except Exception:
        return ""

_SECRET_KV_RE = re.compile(
    r"(?i)(\b(?:api_key|apikey|access_token|refresh_token|client_secret|session_id|token|x-plex-token|password)\b\s*[=:]\s*)([^\s,;&]+)"
)
_URL_QS_RE = re.compile(
    r"(?i)([?&](?:api_key|apikey|access_token|refresh_token|client_secret|session_id|token|x-plex-token)=)([^&\s]+)"
)
_AUTH_BEARER_RE = re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)([^\s,;]+)")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+([^\s,;]+)")
_JSON_DQ_RE = re.compile(
    r'(?i)("(?:api_key|apikey|access_token|refresh_token|client_secret|session_id|token|x-plex-token|password|hash|salt)"\s*:\s*")([^"]*)(")'
)
_JSON_SQ_RE = re.compile(
    r"(?i)('(?:api_key|apikey|access_token|refresh_token|client_secret|session_id|token|x-plex-token|password|hash|salt)'\s*:\s*')([^']*)(')"
)

def _redact_secrets_in_text(s: str) -> str:
    if not s:
        return s
    out = s
    out = _AUTH_BEARER_RE.sub(r"\1••••", out)
    out = _BEARER_RE.sub("Bearer ••••", out)
    out = _URL_QS_RE.sub(r"\1••••", out)
    out = _SECRET_KV_RE.sub(r"\1••••", out)
    out = _JSON_DQ_RE.sub(r"\1••••\3", out)
    out = _JSON_SQ_RE.sub(r"\1••••\3", out)
    return out

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
    on = _is_mods_debug_enabled()
    if on and not os.environ.get("CW_DEBUG"):
        os.environ["CW_DEBUG"] = "1"
    elif not on and os.environ.get("CW_DEBUG"):
        os.environ.pop("CW_DEBUG", None)

# Scheduler: Next run computation
_SCHED_HINT: Dict[str, int] = {"next_run_at": 0, "last_saved_at": 0}

def _compute_next_run_from_cfg(scfg: dict[str, Any] | None, now_ts: int | None = None) -> int:
    now = int(time.time()) if now_ts is None else int(now_ts)
    scfg = scfg or {}
    if not scfg.get("enabled"):
        return 0

    mode = str(scfg.get("mode") or "every_n_hours").lower()

    if mode == "hourly":
        base = datetime.fromtimestamp(now)
        target = base.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return int(target.timestamp())

    if mode == "every_n_hours":
        n = max(1, int(scfg.get("every_n_hours") or 1))
        if n <= 1:
            base = datetime.fromtimestamp(now)
            target = base.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            return int(target.timestamp())
        return now + n * 3600

    if mode == "custom_interval":
        minutes = max(15, int(scfg.get("custom_interval_minutes", 60) or 60))
        return now + minutes * 60

    if mode == "daily_time":
        hh, mm = ("03", "30")
        try:
            hh, mm = (scfg.get("daily_time") or "03:30").split(":")
        except Exception:
            pass

        tz = None
        try:
            tzname = scfg.get("timezone")
            if tzname:
                tz = ZoneInfo(tzname)
        except Exception:
            tz = None

        base = datetime.fromtimestamp(now, tz) if tz else datetime.fromtimestamp(now)
        target = base.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
        if target.timestamp() <= now:
            target = target + timedelta(days=1)
        return int(target.timestamp())
    return now + 3600

# API
_apply_auth_reset_env_once()
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
                    qs = _redact_query_string(request.url.query)
                    path_qs = path + (f"?{qs}" if qs else "")
                    proto = f"HTTP/{request.scope.get('http_version','1.1')}"
                    print(f'{host} - "{request.method} {path_qs} {proto}" {status} ({dt_ms} ms)')

    if err is not None:
        raise err
    return response


@app.middleware("http")
async def app_auth_gate(request: Request, call_next):
    try:
        cfg = load_config()
    except Exception:
        # Fail closed if config can't be loaded 
        if (request.url.path or "").startswith("/api/"):
            return JSONResponse({"ok": False, "error": "Service unavailable"}, status_code=503, headers={"Cache-Control": "no-store"})
        return PlainTextResponse("Service unavailable", status_code=503, headers={"Cache-Control": "no-store"})

    path = request.url.path or "/"
    if path.startswith("/assets/"):
        return await call_next(request)
    
    if path in {"/favicon.ico", "/favicon.svg", "/favicon.png"}:
        return await call_next(request)
    
    if path in {"/manifest.webmanifest", "/sw.js"}:
        return await call_next(request)
    
    if path in _PUBLIC_HEALTH_PATHS:
        return await call_next(request)
    
    if path.startswith("/api/app-auth/") or path in {"/login", "/logout"}:
        return await call_next(request)
    
    # exclude webhooks from auth
    if path.startswith("/webhook/"):
        return await call_next(request)

    # exclude callback paths
    if path == "/callback" or path.startswith("/callback/"):
        return await call_next(request)

    if app_auth_setup_lock_required(cfg):
        if path in _APP_AUTH_SETUP_ALLOWED_PATHS:
            return await call_next(request)
        if path.startswith("/api/"):
            return JSONResponse(
                {"ok": False, "error": "Authentication setup required"},
                status_code=403,
                headers={"Cache-Control": "no-store"},
            )
        return RedirectResponse(url="/", status_code=302)

    if not app_auth_required(cfg):
        return await call_next(request)

    token = request.cookies.get(APP_AUTH_COOKIE)
    if app_is_authenticated(cfg, token):
        return await call_next(request)

    if path.startswith("/api/"):
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    next_url = (request.url.path or "/") + (("?" + request.url.query) if request.url.query else "")
    return RedirectResponse(url="/login?next=" + quote(next_url), status_code=302)

# Static files
register_assets_and_favicons(app, ROOT)
register_ui_root(app)
register_app_auth(app)

# Misc utilities
def get_primary_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80)); return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

# Log buffers
MAX_LOG_LINES = 3000
LOG_BUFFERS: Dict[str, List[str]] = {"SYNC": []}
LOG_BASE_SEQ: Dict[str, int] = {"SYNC": 1}
LOG_NEXT_SEQ: Dict[str, int] = {"SYNC": 1}

ANSI_RE    = re.compile(r"\x1b\[([0-9;]*)m")
ANSI_STRIP = re.compile(r"\x1b\[[0-9;]*m")
_FG_CODES = {"30","31","32","33","34","35","36","37","90","91","92","93","94","95","96","97"}
_BG_CODES = {"40","41","42","43","44","45","46","47","100","101","102","103","104","105","106","107"}

def _escape_html(s: str) -> str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def strip_ansi(s: str) -> str:
    return ANSI_STRIP.sub("", s)

def _norm_log_tag(tag: str | None) -> str:
    t = (tag or "").strip().upper()
    if t in {"JFIN", "JELLY"}:
        return "JELLYFIN"
    if not t:
        return "SYNC"
    return t

def _get_log_buf(tag: str | None) -> List[str]:
    t = _norm_log_tag(tag)
    buf = LOG_BUFFERS.setdefault(t, [])
    if t not in LOG_NEXT_SEQ:
        LOG_NEXT_SEQ[t] = 1
        LOG_BASE_SEQ[t] = 1
    # If another module reset the list, realign base to the next sequence.
    if not buf:
        LOG_BASE_SEQ[t] = int(LOG_NEXT_SEQ.get(t, 1))
    return buf

def _log_lines(tag: str | None, tail: int | None = None) -> List[str]:
    buf = _get_log_buf(tag)
    if not tail:
        return list(buf)
    return buf[-int(tail):]

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
    t = _norm_log_tag(tag)
    safe_line = _redact_secrets_in_text(raw_line)
    html = ansi_to_html(safe_line.rstrip("\n"))
    buf = _get_log_buf(t)
    buf.append(html)
    LOG_NEXT_SEQ[t] = int(LOG_NEXT_SEQ.get(t, 1)) + 1
    if len(buf) > MAX_LOG_LINES:
        drop = len(buf) - MAX_LOG_LINES
        del buf[:drop]
        LOG_BASE_SEQ[t] = int(LOG_BASE_SEQ.get(t, 1)) + drop

def _install_ui_log_forwarder() -> None:
    try:
        from _logging import log as BASE_LOG  # type: ignore
    except Exception:
        return

    def _hook(ctx: dict[str, Any], level: str, msg: str, line: str, extra: Any | None) -> None:
        try:
            tag = str((ctx or {}).get("module") or "")
            _append_log(tag, line)
        except Exception:
            pass

    try:
        if hasattr(BASE_LOG, "set_hook"):
            BASE_LOG.set_hook(_hook)
    except Exception:
        pass

register_api(app, load_config, log_fn=_append_log)
register_services(app, load_config)

# Expose log buffers to app state
try:
    app.state.LOG_BUFFERS = LOG_BUFFERS
    app.state.MAX_LOG_LINES = MAX_LOG_LINES
except Exception:
    pass

# Host logger
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
        module_name = ctx.get("module", self._module)
        return _UIHostLogger(self._tag, module_name, c)

    def child(self, name: str):
        return _UIHostLogger(self._tag, name, dict(self._ctx))

# Orchestrator getter
def _get_orchestrator() -> Orchestrator:
    cfg = load_config()
    return Orchestrator(config=cfg)

# Startup sequence
@asynccontextmanager
async def _lifespan(app: Any) -> AsyncIterator[None]:
    app.state.watch_groups = {}
    app.state.watch_manager = None
    _apply_debug_env_from_config()
    _install_ui_log_forwarder()

    started = False
    try:
        cfg = load_config() or {}
        sc = (cfg.get("scrobble") or {}) or {}
        watch = (sc.get("watch") or {}) if isinstance(sc.get("watch"), dict) else {}
        want_autostart = bool(sc.get("enabled")) and str(sc.get("mode") or "").lower() == "watch" and bool(watch.get("autostart"))

        if want_autostart:
            try:
                from providers.scrobble.watch_manager import start_from_config as _wm_start
                res = _wm_start(app)
                if isinstance(res, dict):
                    started = any(bool(g.get("running")) for g in (res.get("groups") or []))
                _UIHostLogger("WATCH", "WATCH")(
                    "watch routes autostarted" if started else "watch routes autostart returned but not running",
                    level="INFO",
                )
            except Exception as e:
                try:
                    _UIHostLogger("WATCH", "WATCH")(f"watch routes autostart failed: {e}", level="ERROR")
                except Exception:
                    pass
        else:
            if watch.get("autostart") is False:
                _UIHostLogger("WATCH", "WATCH")("Autostart is disabled", level="INFO")
            else:
                _UIHostLogger("WATCH", "WATCH")("watch autostart not requested", level="INFO")
    except Exception as e:
        try:
            _UIHostLogger("WATCH", "WATCH")(f"watch autostart check failed: {e}", level="ERROR")
        except Exception:
            pass


    try:
        global scheduler
        if scheduler is not None:
            cfg_sched = (load_config() or {}).get("scheduling") or {}
            effective_enabled = bool(
                cfg_sched.get("enabled") or ((cfg_sched.get("advanced") or {}).get("enabled"))
            )
            if effective_enabled:
                scheduler.start()
                if hasattr(scheduler, "refresh"):
                    scheduler.refresh()
    except Exception as e:
        try:
            _UIHostLogger("SYNC")(f"scheduler startup error: {e}", level="ERROR")
        except Exception:
            pass
    try:
        yield
    finally:
        try:
            from providers.scrobble.watch_manager import stop_all as _wm_stop
            _wm_stop(app)
        except Exception:
            pass

app.router.lifespan_context = _lifespan

# Middleware: disable caching for API responses
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

# Files listing API - TODO: move to api/files.py
@app.get("/api/files", tags=["files"])
def api_list_files(
    path: str = Query(..., description="Directory path (absolute or config-relative)")
) -> List[Dict[str, Any]]:
    try:
        p = _resolve_config_scoped_path(path)
    except ValueError:
        return []

    try:
        if not p.exists() or not p.is_dir():
            return []
        out: List[Dict[str, Any]] = []
        for child in sorted(p.iterdir()):
            info: Dict[str, Any] = {
                "name": child.name,
                "is_dir": child.is_dir(),
            }
            try:
                info["size"] = child.stat().st_size
            except Exception:
                pass
            out.append(info)
        return out
    except Exception:
        return []

# Logging API - TODO: move to api/logging.py
@app.get("/api/logs/dump", tags=["logging"])
def logs_dump(channel: str = "TRAKT", n: int = 50):
    channel = _norm_log_tag(channel)
    return {"channel": channel, "lines": _log_lines(channel, tail=n)}

@app.get("/api/logs/stream", tags=["logging"])
async def api_logs_stream_initial(request: Request, tag: str = Query("SYNC")):
    tag = _norm_log_tag(tag)

    async def agen():
        buf = _get_log_buf(tag)
        for line in buf:
            yield f"data: {line}\n\n"
        base = int(LOG_BASE_SEQ.get(tag, int(LOG_NEXT_SEQ.get(tag, 1))))
        last_seq = base + len(buf) - 1
        last = time.time()
        while True:
            if await request.is_disconnected():
                break
            new_buf = _get_log_buf(tag)
            base = int(LOG_BASE_SEQ.get(tag, int(LOG_NEXT_SEQ.get(tag, 1))))
            if last_seq < base - 1:
                last_seq = base - 1
            start_seq = max(last_seq + 1, base)
            start_idx = int(start_seq - base)
            if start_idx < 0:
                start_idx = 0
            for i in range(start_idx, len(new_buf)):
                line = new_buf[i]
                yield f"data: {line}\n\n"
                last = time.time()
                last_seq = base + i
            if time.time() - last > 15:
                yield "event: ping\ndata: 1\n\n"
                last = time.time()
            await asyncio.sleep(0.25)

    return StreamingResponse(
        agen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )

@app.get("/api/logs/watcher", tags=["logging"])
async def api_logs_watcher(
    request: Request,
    tail: int = Query(200, ge=1, le=3000),
    tags: str = Query("", description="Optional CSV override"),
):
    cfg = load_config() or {}
    if tags and tags.strip():
        sel = [_norm_log_tag(t) for t in tags.split(",") if t and t.strip()]
    else:
        watch_cfg: dict[str, Any] = {}
        sc = cfg.get("scrobble") or {}
        if isinstance(sc, dict) and isinstance(sc.get("watch"), dict):
            watch_cfg = sc.get("watch") or {}
        routes = watch_cfg.get("routes") if isinstance(watch_cfg, dict) else None
        if isinstance(routes, list) and routes:
            providers = [
                _norm_log_tag(str((r or {}).get("provider") or ""))
                for r in routes
                if isinstance(r, dict) and bool((r or {}).get("enabled", True))
            ]
            sinks = [
                _norm_log_tag(str((r or {}).get("sink") or ""))
                for r in routes
                if isinstance(r, dict) and bool((r or {}).get("enabled", True))
            ]
            sel = [*providers, *sinks]
        else:
            sel = ["WATCH"]

    out: List[str] = []
    for t in sel:
        if t and t not in out:
            out.append(t)
    tags_sel = out or ["SYNC"]

    async def agen():
        last_seq: Dict[str, int] = {}

        for t in tags_sel:
            buf = _get_log_buf(t)
            start = max(0, len(buf) - int(tail))
            for line in buf[start:]:
                safe = (line or "").replace("\n", " ")
                yield f"event: {t}\ndata: {safe}\n\n"
            base = int(LOG_BASE_SEQ.get(t, int(LOG_NEXT_SEQ.get(t, 1))))
            last_seq[t] = base + len(buf) - 1

        last = time.time()
        while True:
            if await request.is_disconnected():
                break

            for t in tags_sel:
                buf = _get_log_buf(t)
                base = int(LOG_BASE_SEQ.get(t, int(LOG_NEXT_SEQ.get(t, 1))))
                seen = int(last_seq.get(t, base - 1))
                if seen < base - 1:
                    seen = base - 1
                start_seq = max(seen + 1, base)
                start_idx = int(start_seq - base)
                if start_idx < 0:
                    start_idx = 0
                for i in range(start_idx, len(buf)):
                    line = buf[i]
                    safe = (line or "").replace("\n", " ")
                    yield f"event: {t}\ndata: {safe}\n\n"
                    last = time.time()
                    seen = base + i
                last_seq[t] = seen

            if time.time() - last > 15:
                yield "event: ping\ndata: 1\n\n"
                last = time.time()

            await asyncio.sleep(0.25)

    return StreamingResponse(
        agen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )

# Scheduler sync starter
def _start_sync_from_scheduler(payload: dict[str, Any] | None = None) -> bool:
    p = dict(payload or {})
    raw_capture = p.get("capture")
    capture: dict[str, Any] = raw_capture if isinstance(raw_capture, dict) else {}
    capture_job_id = str(p.get("capture_job_id") or "").strip()
    scheduler_mode = str(p.get("scheduler_mode") or "").strip().lower()
    if capture_job_id or scheduler_mode == "advanced_capture" or capture:
        try:
            from services.snapshots import create_snapshot, enforce_capture_retention, render_capture_label_template

            provider = str(capture.get("provider") or "").strip().upper()
            instance = str(capture.get("instance") or capture.get("instance_id") or "default").strip() or "default"
            feature = str(capture.get("feature") or "").strip().lower()
            label_template = str(capture.get("label_template") or capture.get("labelTemplate") or "").strip()
            try:
                retention_days = max(0, int(capture.get("retention_days") or 0))
            except Exception:
                retention_days = 0
            try:
                max_captures = max(0, int(capture.get("max_captures") or 0))
            except Exception:
                max_captures = 0
            auto_delete_old = capture.get("auto_delete_old") is True
            if not provider or not feature:
                _append_log("SYNC", "[!] Scheduler capture: missing provider or feature")
                return False

            label = render_capture_label_template(
                label_template,
                provider=provider,
                instance=instance,
                feature=feature,
            )
            res = create_snapshot(provider, feature, label=label, instance_id=instance) or {}
            _append_log(
                "SYNC",
                f"[i] Scheduler capture: saved {provider} {feature} ({instance}) -> {res.get('path') or 'capture created'}",
            )
            if auto_delete_old:
                cleanup = enforce_capture_retention(
                    provider,
                    feature,
                    instance_id=instance,
                    retention_days=retention_days,
                    max_captures=max_captures,
                    auto_delete_old=True,
                ) or {}
                deleted = cleanup.get("deleted") or []
                if deleted:
                    _append_log("SYNC", f"[i] Scheduler capture cleanup: removed {len(deleted)} older capture(s)")
                if cleanup.get("errors"):
                    _append_log("SYNC", f"[!] Scheduler capture cleanup issues: {', '.join(map(str, cleanup.get('errors') or []))}")
            return True
        except Exception as e:
            _append_log("SYNC", f"[!] Scheduler capture failed: {e}")
            return False

    try:
        p.setdefault("source", "scheduler")
        res = api_run_sync(p) or {}
    except Exception as e:
        _append_log("SYNC", f"[!] Scheduler: api_run_sync failed: {e}")
        return False

    if not res.get("ok"):
        return False
    if res.get("skipped"):
        _append_log("SYNC", f"[i] Scheduler: skipped run ({res.get('skipped')})")
        return False

    return True


def scheduler_handle_event(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        if scheduler is None or not hasattr(scheduler, "handle_event"):
            return {"ok": False, "reason": "scheduler_unavailable"}
        return scheduler.handle_event(payload or None)  # type: ignore[union-attr]
    except Exception as e:
        try:
            _append_log("SYNC", f"[!] Scheduler event bridge failed: {e}")
        except Exception:
            pass
        return {"ok": False, "reason": "bridge_error"}


def scheduler_event_from_scrobble(
    ev: Any,
    *,
    source: str = "watcher",
    route_id: str = "",
    provider: str = "",
    provider_instance: str = "",
) -> dict[str, Any]:
    ids = {}
    try:
        ids = dict(getattr(ev, "ids", {}) or {})
    except Exception:
        ids = {}
    progress = getattr(ev, "progress", None)
    event_name = str(getattr(ev, "action", "") or "").strip().lower()
    finished = bool(event_name == "stop" and isinstance(progress, int) and progress >= 95)
    return {
        "source": str(source or "").strip().lower(),
        "route_id": str(route_id or "").strip(),
        "provider": str(provider or "").strip().lower(),
        "provider_instance": str(provider_instance or "").strip(),
        "event": event_name,
        "account": str(getattr(ev, "account", "") or "").strip(),
        "media_type": str(getattr(ev, "media_type", "") or "").strip().lower(),
        "progress": progress if isinstance(progress, int) else None,
        "finished": finished,
        "session_key": str(getattr(ev, "session_key", "") or "").strip(),
        "title": str(getattr(ev, "title", "") or "").strip(),
        "ids": ids,
        "ts": int(time.time()),
    }

scheduler = SyncScheduler(
    load_config, save_config,
    run_sync_fn=_start_sync_from_scheduler,
    is_sync_running_fn=_is_sync_running,
    log_fn=_UIHostLogger("SYNC", "SCHED"),
)

from cw_platform.metadata import MetadataManager as _MetadataMgr

# Metadata manager
_METADATA = _MetadataMgr(load_config, save_config)

# Entry point
def main(host: str = "0.0.0.0", port: int = 8787) -> None:
    cfg = load_config()
    ui = cfg.get("ui")
    if not isinstance(ui, dict):
        ui = {}
    protocol = str(ui.get("protocol") or "http").strip().lower()
    if protocol not in ("http", "https"):
        protocol = "http"

    ip = get_primary_ip() or "127.0.0.1"
    boot = LOG.child("BOOT")
    boot.info(_c(f"CROSSWATCH Engine {CURRENT_VERSION} running:", BLUE))
    boot.info(f"  {_c('Local:', DIM)}   {_c(f'{protocol}://127.0.0.1:{port}', GREEN)}")
    boot.info(f"  {_c('Docker:', DIM)}  {_c(f'{protocol}://{ip}:{port}', GREEN)}")
    boot.info(f"  {_c('Bind:', DIM)}    {_c(f'{host}:{port}', GREEN)}")
    boot.info("")
    boot.info(f"  {_c('Cache:', DIM)}      {CACHE_DIR}")
    boot.info(f"  {_c('CW_STATE:', DIM)}   {CW_STATE_DIR}")
    boot.info(f"  {_c('Reports:', DIM)}    {REPORT_DIR}")
    boot.info(f"  {_c('Last Sync:', DIM)}  {LAST_SYNC_PATH} (JSON)")
    boot.info(f"  {_c('Tombstones:', DIM)} {TOMBSTONES_PATH} (JSON)")
    boot.info(f"  {_c('State:', DIM)}      {STATE_PATH} (JSON)")
    boot.info(f"  {_c('Config:', DIM)}     {CONFIG_DIR / 'config.json'} (JSON)")
    boot.info("")

    debug = bool((cfg.get("runtime") or {}).get("debug"))
    debug_http = bool((cfg.get("runtime") or {}).get("debug_http"))

    uv_args: dict[str, Any] = {
        "host": host,
        "port": port,
        "log_level": ("debug" if debug else "warning"),
        "access_log": debug_http,
    }

    if protocol == "https":
        tls = ui.get("tls")
        if not isinstance(tls, dict):
            tls = {}
        self_signed = bool(tls.get("self_signed", True))
        hostname = str(tls.get("hostname") or "localhost").strip() or "localhost"
        try:
            valid_days = int(tls.get("valid_days") or 825)
        except Exception:
            valid_days = 825

        cert_path, key_path = resolve_tls_paths(cfg, CONFIG_DIR)
        if self_signed:
            alt_dns_raw = tls.get("alt_dns") if isinstance(tls, dict) else None
            alt_ips_raw = tls.get("alt_ips") if isinstance(tls, dict) else None
            alt_dns: list[str] = [str(x) for x in alt_dns_raw] if isinstance(alt_dns_raw, list) else []
            alt_ips: list[str] = [str(x) for x in alt_ips_raw] if isinstance(alt_ips_raw, list) else []
            ensure_self_signed_cert(
                cert_path,
                key_path,
                hostname=hostname,
                valid_days=valid_days,
                alt_dns=[hostname, "localhost", socket.gethostname(), *alt_dns],
                alt_ips=["127.0.0.1", ip, *alt_ips],
            )
        else:
            if not cert_path.exists() or not key_path.exists():
                boot.error(f"[!] HTTPS selected but cert/key not found: {cert_path} / {key_path}")
                raise SystemExit(2)

        uv_args["ssl_certfile"] = str(cert_path)
        uv_args["ssl_keyfile"] = str(key_path)

    uvicorn.run(app, **uv_args)


if __name__ == "__main__":
    main()
