# cw_platform/trakt_http.py
# CrossWatch - Shared Trakt request pacing per account
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Iterator, Mapping

import requests
from requests.adapters import HTTPAdapter

from cw_platform.run_control import raise_if_cancelled

API_PREFIX = "https://api.trakt.tv/"
READ_WINDOW_S = 300.0
READ_LIMIT = 500
READ_RESERVE = 25
WRITE_INTERVAL_S = 1.0
MAX_WAIT_BACKGROUND_S = 300.0
MAX_WAIT_FOREGROUND_S = 10.0
MAX_BLOCK_S = 300.0
DISABLE_ENV = "CW_TRAKT_PACING"
READ_LIMIT_ENV = "CW_TRAKT_READ_LIMIT"

_LOG = logging.getLogger("crosswatch.trakt_http")
_GUARD = threading.Lock()
_ACCOUNTS: dict[str, "_Account"] = {}


class _Account:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reads: deque[float] = deque()
        self.next_write = 0.0
        self.blocked = {"GET": 0.0, "POST": 0.0}


def enabled() -> bool:
    return str(os.environ.get(DISABLE_ENV) or "1").strip().lower() not in ("0", "false", "off", "no")


def read_limit() -> int:
    try:
        value = int(os.environ.get(READ_LIMIT_ENV) or READ_LIMIT)
    except (TypeError, ValueError):
        value = READ_LIMIT
    return max(1, value)


def account_key(token: Any = "", client_id: Any = "") -> str:
    value = str(token or "").strip()
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    if value:
        return f"user:{value}"
    return f"app:{str(client_id or '').strip()}"


def _bucket(method: Any) -> str:
    return "GET" if str(method or "GET").upper() == "GET" else "POST"


def _account(key: str) -> _Account:
    with _GUARD:
        account = _ACCOUNTS.get(key)
        if account is None:
            account = _Account()
            _ACCOUNTS[key] = account
        return account


def reset() -> None:
    with _GUARD:
        _ACCOUNTS.clear()


def _sleep(seconds: float, background: bool) -> None:
    end = time.monotonic() + max(0.0, seconds)
    while (left := end - time.monotonic()) > 0:
        if background:
            raise_if_cancelled()
        time.sleep(min(0.5, left))
    if background:
        raise_if_cancelled()


def _reserve(account: _Account, bucket: str, background: bool) -> float:
    with account.lock:
        now = time.monotonic()
        wait = max(0.0, account.blocked[bucket] - now)
        if bucket == "POST":
            slot = max(now + wait, account.next_write)
            account.next_write = slot + WRITE_INTERVAL_S
            return slot - now
        while account.reads and account.reads[0] <= now - READ_WINDOW_S:
            account.reads.popleft()
        allowed = read_limit() - (READ_RESERVE if background else 0)
        if len(account.reads) >= max(1, allowed):
            index = len(account.reads) - max(1, allowed)
            wait = max(wait, account.reads[index] + READ_WINDOW_S - now)
        account.reads.append(now + min(wait, MAX_WAIT_BACKGROUND_S if background else MAX_WAIT_FOREGROUND_S))
        return wait


@contextmanager
def request_gate(method: str, token: Any = "", *, client_id: Any = "", background: bool = False) -> Iterator[None]:
    if not enabled():
        yield
        return
    bucket = _bucket(method)
    wait = _reserve(_account(account_key(token, client_id)), bucket, background)
    wait = min(wait, MAX_WAIT_BACKGROUND_S if background else MAX_WAIT_FOREGROUND_S)
    if wait > 5.0:
        _LOG.info("Trakt %s limit reached, waiting %.0fs", "read" if bucket == "GET" else "write", wait)
    _sleep(wait, background)
    yield


def _header(headers: Any, name: str) -> str:
    if not isinstance(headers, Mapping):
        return ""
    try:
        return str(headers.get(name) or headers.get(name.lower()) or "")
    except Exception:
        return ""


def _retry_after(headers: Any) -> float:
    try:
        value = float(_header(headers, "Retry-After") or 0.0)
    except (TypeError, ValueError):
        value = 0.0
    if value > 0:
        return value
    try:
        until = str(json.loads(_header(headers, "X-Ratelimit") or "{}").get("until") or "")
        return max(0.0, datetime.fromisoformat(until.replace("Z", "+00:00")).timestamp() - time.time())
    except (TypeError, ValueError, AttributeError):
        return 0.0


def record_response(method: str, token: Any, status: Any, headers: Any = None, *, client_id: Any = "") -> None:
    if not enabled():
        return
    try:
        code = int(status)
    except (TypeError, ValueError):
        return
    if code != 429:
        return
    wait = min(MAX_BLOCK_S, _retry_after(headers) or WRITE_INTERVAL_S)
    account = _account(account_key(token, client_id))
    bucket = _bucket(method)
    with account.lock:
        account.blocked[bucket] = max(account.blocked[bucket], time.monotonic() + wait)


def _identity(headers: Any) -> tuple[str, str]:
    return _header(headers, "Authorization"), _header(headers, "trakt-api-key")


def paced_request(sender: Callable[..., Any], method: str, url: str, **kwargs: Any) -> Any:
    token, client_id = _identity(kwargs.get("headers"))
    with request_gate(method, token, client_id=client_id):
        response = sender(url, **kwargs)
    record_response(method, token, getattr(response, "status_code", 0), getattr(response, "headers", None), client_id=client_id)
    return response


class TraktAdapter(HTTPAdapter):
    def __init__(self, *args: Any, background: bool = True, **kwargs: Any) -> None:
        self._background = background
        super().__init__(*args, **kwargs)

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:
        token, client_id = _identity(request.headers)
        method = str(request.method or "GET")
        with request_gate(method, token, client_id=client_id, background=self._background):
            response = super().send(request, **kwargs)
        record_response(method, token, response.status_code, response.headers, client_id=client_id)
        return response


def pace_session(session: requests.Session, *, background: bool = True) -> None:
    session.mount(API_PREFIX, TraktAdapter(background=background))
