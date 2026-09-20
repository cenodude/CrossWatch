# cw_platform/simkl_http.py
# CrossWatch - Shared SIMKL request pacing and connection state
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import time
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Callable, Iterator, Mapping

import requests
from requests.adapters import HTTPAdapter

from cw_platform import config_base

_PACE_LOCK = threading.Lock()
_LOCKS_GUARD = threading.Lock()
_USER_LOCKS: dict[str, Any] = {}
_USER_IDENTITIES: dict[str, str] = {}
_NEXT_REQUEST = 0.0
_NEXT_POST = 0.0
_OUTCOMES: dict[str, bool] = {}


@lru_cache(maxsize=256)
def _derive_token_key(value: str, key: bytes) -> str:
    return hashlib.pbkdf2_hmac(
        "sha256", value.encode("utf-8"), b"crosswatch/simkl/token\x00" + key, 600_000,
    ).hex()


def token_key(token: Any) -> str:
    value = str(token or "").strip()
    if value.lower().startswith("bearer "):
        value = value[7:].strip()
    if not value:
        return ""
    with config_base._CONFIG_LOCK:
        key = config_base._load_config_key(create=True)
    if key is None:
        raise RuntimeError("SIMKL token fingerprint requires the config encryption key")
    return _derive_token_key(value, key)


@contextmanager
def request_gate(method: str, token: str = "") -> Iterator[None]:
    global _NEXT_REQUEST, _NEXT_POST
    key = token_key(token) or "anonymous"
    from providers.sync.simkl._common import account_settings_cached

    settings = account_settings_cached(key, float("inf")) or {}
    account = settings.get("account") or {}
    identity = str(account.get("id") or "") if isinstance(account, Mapping) else ""
    with _LOCKS_GUARD:
        if identity:
            _USER_IDENTITIES[key] = f"user:{identity}"
        key = _USER_IDENTITIES.get(key, key)
        lock = _USER_LOCKS.setdefault(key, threading.RLock())
    with lock:
        _check_quota(token)
        with _PACE_LOCK:
            post = method.upper() != "GET"
            deadline = max(_NEXT_REQUEST, _NEXT_POST if post else 0.0)
            while (delay := deadline - time.monotonic()) > 0:
                time.sleep(delay)
            try:
                yield
            finally:
                now = time.monotonic()
                _NEXT_REQUEST = now + 0.5
                if post:
                    _NEXT_POST = now + 1.0


def _check_quota(token: str) -> None:
    from providers.sync.simkl._common import SIMKLQuotaError, quota_blocked_until

    key = token_key(token)
    with _LOCKS_GUARD:
        identity = _USER_IDENTITIES.get(key, key)
    until = max(quota_blocked_until(key), quota_blocked_until(identity))
    if until:
        raise SIMKLQuotaError(until)


def last_outcome(token: str) -> bool | None:
    key = token_key(token)
    with _LOCKS_GUARD:
        if key in _OUTCOMES:
            return _OUTCOMES[key]
    from providers.sync.simkl._common import STATE_DIR

    try:
        value = json.loads((STATE_DIR / f"simkl.auth.{key}.json").read_text("utf-8"))["ok"]
        if isinstance(value, bool):
            with _LOCKS_GUARD:
                _OUTCOMES[key] = value
        return value if isinstance(value, bool) else None
    except (OSError, ValueError, KeyError, TypeError):
        return None


def transfer_token_state(old_token: str, new_token: str) -> None:
    if not old_token or not new_token or old_token == new_token:
        return
    from providers.sync.simkl._common import account_settings_cached, account_settings_store, block_quota, quota_blocked_until

    old_key, new_key = token_key(old_token), token_key(new_token)
    with _LOCKS_GUARD:
        _USER_IDENTITIES[new_key] = _USER_IDENTITIES.get(old_key, old_key)
    settings = account_settings_cached(old_key, float("inf"))
    if settings is not None:
        account_settings_store(new_key, settings)
    until = quota_blocked_until(old_key)
    if until:
        block_quota(new_key, until - time.time())


def record_outcome(token: str, status: int) -> None:
    key = token_key(token)
    if not key or not (200 <= status < 300 or status == 401):
        return
    ok = status != 401
    if last_outcome(token) is ok:
        return
    with _LOCKS_GUARD:
        _OUTCOMES[key] = ok
    from providers.sync.simkl._common import STATE_DIR

    path = STATE_DIR / f"simkl.auth.{key}.json"
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"ok": ok}), "utf-8")
        os.replace(tmp, path)
    except OSError:
        pass
    probes = sys.modules.get("api.probesAPI")
    if probes is not None:
        probes.STATUS_CACHE["ts"] = 0.0
        probes.STATUS_SCOPE_CACHE.clear()


def _record_response(token: str, url: str, response: requests.Response) -> None:
    from providers.sync.simkl._common import SIMKLQuotaError, block_quota, is_user_limit_response, record_quota

    record_quota(token_key(token), response.headers)
    if is_user_limit_response(response):
        key = token_key(token)
        until = block_quota(key, response.headers.get("Retry-After"))
        with _LOCKS_GUARD:
            identity = _USER_IDENTITIES.get(key, key)
        if identity != key:
            block_quota(identity, until - time.time())
        raise SIMKLQuotaError(until)
    record_outcome(token, response.status_code)
    if response.status_code == 200 and url.split("?", 1)[0].endswith("/users/settings"):
        from providers.sync.simkl._common import account_settings_store

        try:
            data = response.json()
        except ValueError:
            return
        if isinstance(data, Mapping):
            account_settings_store(token_key(token), data)


def paced_request(sender: Callable[..., Any], method: str, url: str, **kwargs: Any) -> Any:
    token = str((kwargs.get("headers") or {}).get("Authorization") or "")
    with request_gate(method, token):
        response = sender(url, **kwargs)
        _record_response(token, url, response)
        return response


class SimklAdapter(HTTPAdapter):
    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:
        token = str(request.headers.get("Authorization") or "")
        with request_gate(str(request.method), token):
            response = super().send(request, **kwargs)
            response.content
            _record_response(token, str(request.url), response)
            return response


def pace_session(session: requests.Session) -> None:
    session.mount("https://api.simkl.com/", SimklAdapter())
