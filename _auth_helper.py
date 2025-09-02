#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Auth helpers for PLEX + SIMKL
- PLEX: re-used logic (pins.json create + pins/{id}.json poll) with SAME headers.
- SIMKL: OAuth Authorization Code flow (authorize URL + token exchange).

Requires: requests
"""

from __future__ import annotations
import calendar
import time
import uuid
from typing import Any, Dict, Optional, Tuple
import urllib.parse as _url

import requests

# ---------------- PLEX  ----------------

__VERSION__ = "0.3.9"
UA = f"Plex-Token-Helper/{__VERSION__}"

PLEX_PIN_CREATE = "https://plex.tv/pins.json"
PLEX_PIN_STATUS = "https://plex.tv/pins/{id}.json"

def _plex_headers(client_id: str) -> Dict[str, str]:
    return {
        "Accept": "application/json",
        "User-Agent": UA,
        "X-Plex-Product": "Plex-Token-Helper",
        "X-Plex-Version": __VERSION__,
        "X-Plex-Client-Identifier": client_id,
        "X-Plex-Device": "Python",
        "X-Plex-Device-Name": "plex-token-helper",
        "X-Plex-Platform": "Python",
    }

def _unwrap_pin(js: dict) -> dict:
    if isinstance(js, dict) and "pin" in js and isinstance(js["pin"], dict):
        return js["pin"]
    return js or {}

def _iso_to_epoch_utc(iso_str: str) -> int:
    try:
        ts = time.strptime(iso_str, "%Y-%m-%dT%H:%M:%SZ")
        return calendar.timegm(ts)
    except Exception:
        return 0

def plex_request_pin(session: Optional[requests.Session] = None) -> Dict[str, Any]:
    s = session or requests.Session()
    client_id = "plex-simkl-bridge-" + uuid.uuid4().hex[:8]
    headers = _plex_headers(client_id)
    r = s.post(PLEX_PIN_CREATE, headers=headers, data={"strong": "true"}, timeout=15)
    r.raise_for_status()
    pin_js = _unwrap_pin(r.json())
    pin_id = pin_js.get("id")
    code = pin_js.get("code")
    exp_iso = pin_js.get("expires_at")
    exp_epoch = _iso_to_epoch_utc(exp_iso) if isinstance(exp_iso, str) else 0
    if not pin_id or not code or not exp_epoch:
        raise RuntimeError(f"Incomplete PIN response: {r.text}")
    return {"id": int(pin_id), "code": code, "expires_epoch": int(exp_epoch), "headers": headers}

def plex_poll_token(pin_id: int, headers: Dict[str, str], session: Optional[requests.Session] = None) -> Tuple[Optional[str], bool]:
    s = session or requests.Session()
    rr = s.get(PLEX_PIN_STATUS.format(id=pin_id), headers=headers, timeout=15)
    if rr.status_code == 404:
        return None, True
    rr.raise_for_status()
    st_pin = _unwrap_pin(rr.json())
    token = st_pin.get("auth_token")
    if token:
        return token, False
    exp_iso = st_pin.get("expires_at")
    exp_epoch = _iso_to_epoch_utc(exp_iso) if isinstance(exp_iso, str) else 0
    expired = bool(exp_epoch and (exp_epoch - int(time.time()) <= 0))
    return None, expired

def plex_wait_for_token(pin_id: int, headers: Dict[str, str], timeout_sec: int = 360, interval: float = 1.0) -> Optional[str]:
    start = time.time()
    while time.time() - start < timeout_sec:
        token, expired = plex_poll_token(pin_id, headers=headers)
        if token:
            return token
        if expired:
            return None
        time.sleep(interval)
    return None


# ---------------- SIMKL (Authorization Code) ----------------

SIMKL_AUTHORIZE = "https://simkl.com/oauth/authorize"
SIMKL_TOKEN     = "https://api.simkl.com/oauth/token"

def simkl_build_authorize_url(client_id: str, redirect_uri: str, state: str) -> str:
    """
    Returns the full authorize URL for SIMKL.
    """
    q = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "state": state,
    }
    return SIMKL_AUTHORIZE + "?" + _url.urlencode(q)

def simkl_exchange_code(client_id: str, client_secret: str, code: str, redirect_uri: str) -> Dict[str, Any]:
    """
    Exchange authorization code for tokens (SIMKL expects JSON, not form).
    """
    payload = {
        "grant_type": "authorization_code",
        "code": code.strip(),
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    headers = {
        "User-Agent": UA,
        "Accept": "application/json",
    }
    r = requests.post(SIMKL_TOKEN, json=payload, headers=headers, timeout=30)
    if not r.ok:
        raise RuntimeError(f"SIMKL token exchange failed: HTTP {r.status_code} {r.text}")
    return r.json()
