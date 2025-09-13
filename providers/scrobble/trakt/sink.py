from __future__ import annotations
import time, json
from typing import Any, Dict, Optional

import requests

# central logger (optional)
try:
    from modules._logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink

TRAKT_API = "https://api.trakt.tv"
APP_AGENT = "CrossWatch/Scrobble/1.0"

# ---- config --------------------------------------------------------------------
def _load_config() -> Dict[str, Any]:
    """Prefer crosswatch.load_config(); fall back to local config.json."""
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)

def _headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    """Build Trakt headers (strict casing for Authorization)."""
    t = (cfg.get("trakt") or {})
    client_id = t.get("client_id") or t.get("api_key") or ""
    token = t.get("access_token") or ((cfg.get("auth") or {}).get("trakt") or {}).get("access_token") or ""
    return {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
        "Authorization": f"Bearer {token}" if token else "",
        "User-Agent": APP_AGENT,
    }

def _post(path: str, body: Dict[str, Any], cfg: Dict[str, Any]) -> requests.Response:
    return requests.post(f"{TRAKT_API}{path}", headers=_headers(cfg), json=body, timeout=10)

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
    """Early stop -> pause threshold in percent. Default 80."""
    try:
        return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("stop_pause_threshold", 80))
    except Exception:
        return 80

# ---- sink ----------------------------------------------------------------------
class TraktSink(ScrobbleSink):
    """Scrobbles to Trakt with resilient retries and early-stop→pause semantics."""

    def __init__(self, logger=None):
        # Route to /api/logs/dump?channel=TRAKT
        self._logger = logger
        if not self._logger and BASE_LOG and hasattr(BASE_LOG, "child"):
            self._logger = BASE_LOG.child("TRAKT")
        try:
            if self._logger and hasattr(self._logger, "set_level"):
                self._logger.set_level("INFO")
        except Exception:
            pass
        self._last_sent: Dict[str, float] = {}  # debounce key -> ts

    def _log(self, msg: str, level: str = "INFO"):
        if BASE_LOG:
            try:
                BASE_LOG("TRAKT", level.upper(), str(msg))
                return
            except Exception:
                pass
        print(f"{level} [TRAKT] {msg}")


    def _debounced(self, session_key: Optional[str], action: str) -> bool:
        key = f"{session_key}:{action}"
        now = time.time()
        last = self._last_sent.get(key, 0)
        if now - last < 5:
            return True
        self._last_sent[key] = now
        return False

    def _build_body(self, event: ScrobbleEvent) -> Dict[str, Any]:
        p = _clamp_progress(event.progress)
        body: Dict[str, Any] = {"progress": max(1, p)}

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

        # last resort
        body["episode"] = {"ids": ep_ids} if ep_ids else {"season": event.season, "number": event.number}
        return body

    def _send_with_retries(self, path: str, body: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Retry policy:
        - 429: respect Retry-After if present; exponential backoff otherwise.
        - 5xx or network errors: retry with backoff.
        - 4xx (except 429): don't retry.
        """
        backoff = 1.0
        for _ in range(4):
            try:
                r = _post(path, body, cfg)
            except Exception:
                time.sleep(backoff)
                backoff = min(8.0, backoff * 2)
                continue

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

    def send(self, event: ScrobbleEvent) -> None:
        cfg = _load_config()

        p = _clamp_progress(event.progress)
        threshold = _stop_pause_threshold(cfg)  # default 80
        effective_action = event.action
        if event.action == "stop" and p < threshold:
            self._log(f"convert STOP->PAUSE at {p}% (<{threshold}%) to avoid watched", "INFO")
            effective_action = "pause"
        if self._debounced(event.session_key, effective_action):
            return

        path = {
            "start": "/scrobble/start",
            "pause": "/scrobble/pause",
            "stop": "/scrobble/stop",
        }[effective_action]

        body = self._build_body(event)
        self._log(f"→ {path} body={body}")
        res = self._send_with_retries(path, body, cfg)
        if res.get("ok"):
            self._log(f"{path} {res['status']}")
        else:
            self._log(f"{path} {res['status']} err={res.get('resp')}", "ERROR")

