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

# ---- config ----
def _load_config() -> Dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        import json as _json
        with open("config.json", "r", encoding="utf-8") as f:
            return _json.load(f)

def _headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    t = (cfg.get("trakt") or {})
    client_id = t.get("client_id") or t.get("api_key") or ""
    # accept both locations for access token
    token = t.get("access_token") or ((cfg.get("auth") or {}).get("trakt") or {}).get("access_token") or ""
    return {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
        "authorization": f"Bearer {token}" if token else "",
        "User-Agent": APP_AGENT,
    }

def _post(path: str, body: Dict[str, Any], cfg: Dict[str, Any]) -> requests.Response:
    return requests.post(f"{TRAKT_API}{path}", headers=_headers(cfg), json=body, timeout=10)

# ---- id utils ----
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
    try: p = int(p)
    except Exception: p = 0
    return max(0, min(100, p))

# ---- sink ----
class TraktSink(ScrobbleSink):
    """Scrobble to Trakt with PT-like id strategy. Quiet logs (INFO only)."""

    def __init__(self, logger=None):
        self._logger = (logger or (BASE_LOG.child("TRAKT") if BASE_LOG else None))
        # hard set INFO if supported
        try:
            self._logger.set_level("INFO")
        except Exception:
            pass
        self._last_sent: Dict[str, float] = {}  # debounce key -> ts

    def _log(self, msg: str, level: str = "INFO"):
        if self._logger:
            try:
                self._logger(str(msg), level=level.lower())
                return
            except Exception:
                pass
        print(f"{level} [TRAKT] {msg}")

    def _debounced(self, event: ScrobbleEvent) -> bool:
        key = f"{event.session_key}:{event.action}"
        now = time.time()
        last = self._last_sent.get(key, 0)
        if now - last < 5:
            return True
        self._last_sent[key] = now
        return False

    def _build_body(self, event: ScrobbleEvent) -> Dict[str, Any]:
        p = _clamp_progress(event.progress)
        body: Dict[str, Any] = {"progress": max(1, p)}  # avoid 0%

        if event.media_type == "movie":
            ids = _extract_ids(event)
            body["movie"] = {"ids": ids} if ids else {"title": event.title, "year": event.year}
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
            body["show"] = {"title": event.title, "year": event.year}
            body["episode"] = {"season": event.season, "number": event.number}
            return body

        body["episode"] = {"ids": ep_ids} if ep_ids else {"season": event.season, "number": event.number}
        return body

    def _send_with_retries(self, path: str, body: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
        backoff = 1.0
        for _ in range(3):
            r = _post(path, body, cfg)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                wait = float(ra) if ra and str(ra).isdigit() else backoff
                time.sleep(wait); backoff = min(8.0, backoff * 2)
                continue
            if r.status_code >= 400:
                return {"ok": False, "status": r.status_code, "resp": (r.text or "")[:200]}
            try:
                return {"ok": True, "status": r.status_code, "resp": r.json()}
            except Exception:
                return {"ok": True, "status": r.status_code, "resp": (r.text or "")[:200]}
        return {"ok": False, "status": 429, "resp": "rate_limited"}

    def send(self, event: ScrobbleEvent) -> None:
        if self._debounced(event):
            return
        cfg = _load_config()
        path = {"start": "/scrobble/start", "pause": "/scrobble/pause", "stop": "/scrobble/stop"}[event.action]
        body = self._build_body(event)
        res = self._send_with_retries(path, body, cfg)
        if res.get("ok"):
            self._log(f"{path} {res['status']}", "INFO")
        else:
            self._log(f"{path} {res['status']} err={res.get('resp')}", "ERROR")
