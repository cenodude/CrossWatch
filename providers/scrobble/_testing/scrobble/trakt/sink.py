# /providers/scrobble/trakt/sink.py
from __future__ import annotations
# 24-09-2025 Back-to-Basics Editions...and pray i guess..

import time
from typing import Any, Dict, Optional
import requests

from ..scrobble import ScrobbleEvent, cfg, log, debug

class TraktSink:
    def __init__(self, session: Optional[requests.Session] = None):
        self.base = "https://api.trakt.tv"
        self.s = session or requests.Session()
        self.client_id = cfg("providers.trakt.client_id", "")
        self.token = cfg("providers.trakt.token", "")
        self.app_version = cfg("app.version")
        self.app_date = cfg("app.date")
        self.s.headers.update({
            "Content-Type": "application/json",
            "trakt-api-version": "2",
            "trakt-api-key": self.client_id,
            "Authorization": f"Bearer {self.token}",
        })

    def _payload(self, ev: ScrobbleEvent) -> Dict[str, Any]:
        body = dict(ev.payload)
        body["progress"] = round(float(ev.progress or 0.0), 2)
        if self.app_version:
            body["app_version"] = self.app_version
        if self.app_date:
            body["app_date"] = self.app_date
        return body

    def _post(self, path: str, body: Dict[str, Any]) -> None:
        if not self.client_id or not self.token:
            log("Trakt credentials missing; drop scrobble.", "WARN")
            return
        url = self.base + path
        for attempt in range(3):
            try:
                r = self.s.post(url, json=body, timeout=10)
            except Exception as e:
                wait = 1 + attempt * 2
                log(f"Trakt {path} network error: {e} (retry {attempt+1}/3 in {wait}s)", "WARN")
                time.sleep(wait)
                continue

            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                wait = int(ra) if ra and ra.isdigit() else (1 + attempt * 2)
                log(f"Trakt {path} -> {r.status_code}; retry in {wait}s", "WARN")
                time.sleep(wait)
                continue

            if r.status_code == 401:
                log("Trakt auth failed (401). Check token.", "ERROR")
            elif not r.ok:
                log(f"Trakt {path} failed {r.status_code}: {r.text[:200]}", "ERROR")
            return  # success or non-retriable

    def send(self, ev: ScrobbleEvent) -> None:
        # Optional demotion: treat early stops as pause
        if ev.action == "stop":
            thr = cfg("scrobble.stop_demote_threshold", None)
            if isinstance(thr, (int, float)) and ev.progress < float(thr):
                debug(f"demote stop→pause at {ev.progress:.2f}% < {thr}%")
                ev = ScrobbleEvent(
                    action="pause",
                    kind=ev.kind,
                    progress=ev.progress,
                    payload=ev.payload,
                    session_id=ev.session_id,
                    plex_user_id=ev.plex_user_id,
                    plex_username=ev.plex_username,
                    plex_server=ev.plex_server,
                    plex_server_uuid=ev.plex_server_uuid,
                )

        body = self._payload(ev)
        if ev.action == "start":
            self._post("/scrobble/start", body)
        elif ev.action == "pause":
            self._post("/scrobble/pause", body)
        elif ev.action == "stop":
            self._post("/scrobble/stop", body)
        else:
            log(f"Unknown scrobble action: {ev.action}", "ERROR")

__all__ = ["TraktSink"]
