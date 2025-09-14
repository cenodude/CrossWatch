from __future__ import annotations
import time, json
from typing import Any, Dict, Optional, Tuple

import requests

# central logger (optional)
try:
    from modules._logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink

TRAKT_API = "https://api.trakt.tv"
APP_AGENT = "CrossWatch/Scrobble/1.0"

# Runtime override so a freshly refreshed token is used immediately
_TOKEN_OVERRIDE: Optional[str] = None

# ---- config --------------------------------------------------------------------
def _load_config() -> Dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)

def _headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    global _TOKEN_OVERRIDE
    t = (cfg.get("trakt") or {})
    client_id = t.get("client_id") or t.get("api_key") or ""
    token = _TOKEN_OVERRIDE or t.get("access_token") \
            or ((cfg.get("auth") or {}).get("trakt") or {}).get("access_token") or ""
    return {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
        "Authorization": f"Bearer {token}" if token else "",
        "User-Agent": APP_AGENT,
    }

def _post(path: str, body: Dict[str, Any], cfg: Dict[str, Any]) -> requests.Response:
    return requests.post(f"{TRAKT_API}{path}", headers=_headers(cfg), json=body, timeout=10)

def _refresh_trakt_token(cfg: Dict[str, Any]) -> bool:
    global _TOKEN_OVERRIDE
    t = (cfg.get("trakt") or {})
    client_id = t.get("client_id") or t.get("api_key")
    client_secret = t.get("client_secret") or t.get("client_secret_id")
    refresh_token = t.get("refresh_token") \
        or (((cfg.get("auth") or {}).get("trakt") or {}).get("refresh_token"))

    if not (client_id and client_secret and refresh_token):
        if BASE_LOG:
            BASE_LOG("TRAKT", "ERROR", "Cannot refresh token: missing client_id/client_secret/refresh_token")
        return False

    try:
        resp = requests.post(
            f"{TRAKT_API}/oauth/token",
            json={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            headers={"User-Agent": APP_AGENT, "Content-Type": "application/json"},
            timeout=10,
        )
    except Exception as e:
        if BASE_LOG:
            BASE_LOG("TRAKT", "ERROR", f"Token refresh failed (network): {e}")
        return False

    if resp.status_code != 200:
        short = (resp.text or "")[:400]
        if BASE_LOG:
            BASE_LOG("TRAKT", "ERROR", f"Token refresh failed {resp.status_code}: {short}")
        return False

    try:
        data = resp.json()
    except Exception:
        data = {}

    new_access = data.get("access_token")
    new_refresh = data.get("refresh_token") or refresh_token
    if not new_access:
        if BASE_LOG:
            BASE_LOG("TRAKT", "ERROR", "Token refresh response missing access_token")
        return False

    _TOKEN_OVERRIDE = new_access

    try:
        from crosswatch import save_config
        new_cfg = dict(cfg)
        new_cfg.setdefault("trakt", dict(new_cfg.get("trakt") or {}))
        new_cfg["trakt"]["access_token"] = new_access
        new_cfg["trakt"]["refresh_token"] = new_refresh
        save_config(new_cfg)
        if BASE_LOG:
            BASE_LOG("TRAKT", "INFO", "Trakt token refreshed and saved to config")
    except Exception:
        if BASE_LOG:
            BASE_LOG("TRAKT", "INFO", "Trakt token refreshed (runtime only)")
    return True

# ---- id utils ------------------------------------------------------------------
def _extract_ids(event: ScrobbleEvent) -> Dict[str, Any]:
    ids = event.ids or {}
    return {k: ids[k] for k in ("imdb", "tmdb", "tvdb", "trakt") if k in ids and ids[k]}

def _extract_show_ids(event: ScrobbleEvent) -> Dict[str, Any]:
    ids = event.ids or {}
    show = {}
    for k in ("imdb_show", "tmdb_show", "tvdb_show", "trakt_show"):
        if ids.get(k):
            show[k.replace("_show", "")] = ids[k]
    return show

def _clamp_progress(p: int) -> int:
    try:
        p = int(p)
    except Exception:
        p = 0
    return max(0, min(100, p))

def _stop_pause_threshold(cfg: Dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("stop_pause_threshold", 80))
    except Exception:
        return 80

def _regress_tolerance(cfg: Dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("regress_tolerance_percent", 5))
    except Exception:
        return 5

# ---- sink ----------------------------------------------------------------------
class TraktSink(ScrobbleSink):
    """
    - resilient retries & 401 token refresh
    - STOP→PAUSE below threshold
    - progress memory per item across sessions (fixes start-at-0 and resume)
    - accept real rewinds; ignore tiny jitter
    - clamp suspicious 100% on pause/stop using memory
    """

    def __init__(self, logger=None):
        self._logger = logger
        if not self._logger and BASE_LOG and hasattr(BASE_LOG, "child"):
            self._logger = BASE_LOG.child("TRAKT")
        try:
            if self._logger and hasattr(self._logger, "set_level"):
                self._logger.set_level("INFO")
        except Exception:
            pass
        self._last_sent: Dict[str, float] = {}                   # debounce key -> ts
        self._last_prog_session: Dict[Tuple[str, str], int] = {} # (session, media) -> last %
        self._last_prog_global: Dict[str, int] = {}              # media -> last %

    def _log(self, msg: str, level: str = "INFO"):
        if BASE_LOG:
            try:
                BASE_LOG("TRAKT", level.upper(), str(msg))
                return
            except Exception:
                pass
        print(f"{level} [TRAKT] {msg}")

    # identity key for a piece of media
    def _media_key(self, ev: ScrobbleEvent) -> str:
        ids = ev.ids or {}
        parts = []
        for k in ("imdb", "tmdb", "tvdb", "trakt"):
            if ids.get(k):
                parts.append(f"{k}:{ids[k]}")
        if ev.media_type == "episode":
            for k in ("imdb_show", "tmdb_show", "tvdb_show", "trakt_show"):
                if ids.get(k):
                    parts.append(f"{k}:{ids[k]}")
            parts.append(f"S{(ev.season or 0):02d}E{(ev.number or 0):02d}")
        if not parts:
            t = ev.title or ""
            y = ev.year or 0
            if ev.media_type == "episode":
                parts.append(f"{t}|{y}|S{(ev.season or 0):02d}E{(ev.number or 0):02d}")
            else:
                parts.append(f"{t}|{y}")
        return "|".join(parts)

    def _debounced(self, session_key: Optional[str], action: str) -> bool:
        if action == "start":
            return False  # always let START through promptly
        key = f"{session_key}:{action}"
        now = time.time()
        last = self._last_sent.get(key, 0)
        if now - last < 5:
            return True
        self._last_sent[key] = now
        return False

    # Build body using a specific progress
    def _body_with_progress(self, event: ScrobbleEvent, progress: int) -> Dict[str, Any]:
        body: Dict[str, Any] = {"progress": _clamp_progress(progress)}

        if event.media_type == "movie":
            ids = _extract_ids(event)
            if ids:
                body["movie"] = {"ids": ids}
            else:
                m = {"title": event.title}
                if event.year is not None:
                    m["year"] = event.year
                body["movie"] = m
            return body

        # episode
        has_sn = (event.season is not None and event.number is not None)
        show_ids = _extract_show_ids(event)
        ep_ids = _extract_ids(event)

        if has_sn and show_ids:
            body["show"] = {"ids": show_ids}
            body["episode"] = {"season": event.season, "number": event.number}
            return body

        if ep_ids:
            body["episode"] = {"ids": ep_ids}
            return body

        if has_sn:
            s = {"title": event.title}
            if event.year is not None:
                s["year"] = event.year
            body["show"] = s
            body["episode"] = {"season": event.season, "number": event.number}
            return body

        body["episode"] = {"ids": ep_ids} if ep_ids else {"season": event.season, "number": event.number}
        return body

    # HTTP with retries & 401 refresh
    def _send_with_retries(self, path: str, body: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        backoff = 1.0
        tried_refresh = False

        for _ in range(5):
            try:
                r = _post(path, body, cfg)
            except Exception:
                time.sleep(backoff)
                backoff = min(8.0, backoff * 2)
                continue

            if r.status_code == 401 and not tried_refresh:
                self._log("401 Unauthorized. Attempting token refresh...", "WARN")
                if _refresh_trakt_token(cfg):
                    tried_refresh = True
                    continue
                else:
                    return {"ok": False, "status": 401, "resp": "Unauthorized and token refresh failed"}

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra is not None else backoff
                except Exception:
                    wait = backoff
                time.sleep(max(0.5, min(30.0, wait)))
                backoff = min(8.0, backoff * 2)
                continue

            if 500 <= r.status_code < 600:
                time.sleep(backoff)
                backoff = min(8.0, backoff * 2)
                continue

            if r.status_code >= 400:
                short = (r.text or "")[:400]
                if r.status_code == 404:
                    short += " (Trakt couldn’t match the item; missing IDs or mismatched title/year)"
                return {"ok": False, "status": r.status_code, "resp": short}

            try:
                return {"ok": True, "status": r.status_code, "resp": r.json()}
            except Exception:
                return {"ok": True, "status": r.status_code, "resp": (r.text or "")[:400]}

        return {"ok": False, "status": 429, "resp": "rate_limited"}

    # Main send
    def send(self, event: ScrobbleEvent) -> None:
        cfg = _load_config()

        sk = str(event.session_key or "?")
        mk = self._media_key(event)
        p_now = _clamp_progress(event.progress)
        tol = _regress_tolerance(cfg)

        p_prev_session = self._last_prog_session.get((sk, mk), -1)
        p_prev_global = self._last_prog_global.get(mk, -1)

        # Determine p_send
        if event.action == "start":

            # explicit restart: user scrubbed back to start (0–2%)
            explicit_restart = (p_now <= 2 and (p_prev_session >= 10 or p_prev_global >= 10))
            if explicit_restart:
                self._log("restart detected: honoring 0% and clearing memory", "INFO")
                p_send = 0
                self._last_prog_global[mk] = 0
                self._last_prog_session[(sk, mk)] = 0
            else:
                # existing resume logic (avoid overriding a real 0% restart)
                if p_now == 0 and p_prev_global > 0:
                    p_send = p_prev_global
                elif p_prev_global >= 0 and (p_prev_global - p_now) >= tol and p_now > 2:
                    p_send = p_prev_global
                else:
                    p_send = p_now
        else:
            p_base = p_now

            # Clamp suspicious 100% on pause/stop to last in-session progress
            if event.action in ("pause", "stop") and p_base >= 98 and p_prev_session >= 0 and p_prev_session < 95:
                self._log(f"clamp suspicious {event.action} 100% → {p_prev_session}% (session memory)", "INFO")
                p_base = p_prev_session

            if p_prev_session < 0:
                p_send = p_base
            elif p_base >= p_prev_session:
                p_send = p_base
            else:
                # backward seek: accept only if meaningful
                p_send = p_base if (p_prev_session - p_base) >= tol else p_prev_session

        # STOP→PAUSE if under threshold
        threshold = _stop_pause_threshold(cfg)
        effective_action = event.action
        if event.action == "stop" and p_send < threshold:
            self._log(f"convert STOP->PAUSE at {p_send}% (<{threshold}%) to avoid watched", "INFO")
            effective_action = "pause"

        # Update memories
        if p_send != p_prev_session:
            self._last_prog_session[(sk, mk)] = p_send
        if p_send > (p_prev_global if p_prev_global >= 0 else -1):
            self._last_prog_global[mk] = p_send

        if self._debounced(event.session_key, effective_action):
            return

        path = {
            "start": "/scrobble/start",
            "pause": "/scrobble/pause",
            "stop": "/scrobble/stop",
        }[effective_action]

        body = self._body_with_progress(event, p_send)
        self._log(f"→ {path} body={body}")
        res = self._send_with_retries(path, body, cfg)
        if res.get("ok"):
            self._log(f"{path} {res['status']}")
        else:
            self._log(f"{path} {res['status']} err={res.get('resp')}", "ERROR")
