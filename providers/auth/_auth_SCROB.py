# providers/auth/_auth_SCROB.py
# CrossWatch - Scrob Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import base64
import json
import threading
import time
from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, get_provider_block, normalize_instance_id

__VERSION__ = "0.1"
UA = "CrossWatch/1.0"

API_PREFIXES: tuple[str, ...] = ("", "/api/proxy")
DEFAULT_TIMEOUT = 12.0
TOKEN_REFRESH_SKEW = 300
TOKEN_FALLBACK_TTL = 1800

HEALTH_PATH = "health"
PROFILE_PATH = "profile/me"
LOGIN_PATH = "auth/login"
ME_PATH = "auth/me"
VERIFY_2FA_PATH = "auth/2fa/verify-login"

_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_REFRESH_LOCKS_GUARD = threading.Lock()

TERMINAL_LOGIN_REASONS = frozenset({"invalid_credentials", "password_login_disabled", "email_not_confirmed"})
REAUTH_LOGIN_REASONS = frozenset({"totp_required", "invalid_totp_code"})


class ScrobAuthError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "request_failed", status_code: int | None = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.status_code = status_code


def _load_config() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}


def _refresh_lock(instance_id: Any) -> threading.Lock:
    key = normalize_instance_id(instance_id)
    with _REFRESH_LOCKS_GUARD:
        lock = _REFRESH_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _REFRESH_LOCKS[key] = lock
        return lock


def normalize_server_url(value: Any) -> str:
    server = str(value or "").strip().rstrip("/")
    if server and not server.startswith(("http://", "https://")):
        server = "http://" + server
    return server.rstrip("/")


def normalize_api_prefix(value: Any) -> str:
    prefix = str(value or "").strip().strip("/")
    return f"/{prefix}" if prefix else ""


def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return get_provider_block(cfg or {}, "scrob", instance_id)


def is_configured(block: Mapping[str, Any] | None) -> bool:
    data = block or {}
    return bool(
        str(data.get("server_url") or "").strip()
        and str(data.get("api_key") or "").strip()
        and str(data.get("username") or "").strip()
        and str(data.get("password") or "").strip()
    )


def status_for_block(block: Mapping[str, Any] | None, *, instance_id: Any = None) -> AuthStatus:
    inst = normalize_instance_id(instance_id)
    label = "Scrob" if inst == "default" else f"Scrob ({inst})"
    data = block or {}
    expires_at = data.get("expires_at")
    return AuthStatus(
        connected=is_configured(data),
        label=label,
        user=str(data.get("username") or "").strip() or None,
        expires_at=int(expires_at) if isinstance(expires_at, (int, float)) and expires_at else None,
        extra={
            "totp_enabled": bool(data.get("totp_enabled")),
            "reauth_required": needs_reauth(data),
        },
    )


def jwt_expiry(token: Any) -> int:
    parts = str(token or "").split(".")
    if len(parts) != 3:
        return 0
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
    except Exception:
        return 0
    try:
        return int(data.get("exp") or 0)
    except Exception:
        return 0


class ScrobClient:
    def __init__(
        self,
        server_url: str,
        api_key: str,
        *,
        api_prefix: Any = None,
        access_token: Any = None,
        verify_ssl: bool = False,
        timeout: float = DEFAULT_TIMEOUT,
        session: requests.Session | None = None,
    ) -> None:
        self.server_url = normalize_server_url(server_url)
        self.api_key = str(api_key or "").strip()
        self.api_prefix = normalize_api_prefix(api_prefix)
        self.access_token = str(access_token or "").strip()
        self.verify_ssl = bool(verify_ssl)
        self.timeout = float(timeout or DEFAULT_TIMEOUT)
        self.session = session or requests.Session()

    def url_for(self, path: str, *, prefix: str | None = None) -> str:
        base = self.api_prefix if prefix is None else normalize_api_prefix(prefix)
        return f"{self.server_url}{base}/{str(path or '').strip('/')}"

    def request(self, method: str, path: str, *, prefix: str | None = None, **kwargs: Any) -> requests.Response:
        if not self.server_url:
            raise ScrobAuthError("Missing Scrob server URL", reason="server_url_required")
        if not self.api_key:
            raise ScrobAuthError("Missing Scrob API key", reason="api_key_required")

        headers = dict(kwargs.pop("headers", {}) or {})
        headers.setdefault("Accept", "application/json")
        headers.setdefault("User-Agent", UA)
        headers["X-Api-Key"] = self.api_key
        if self.access_token:
            headers["Authorization"] = f"Bearer {self.access_token}"

        params = dict(kwargs.pop("params", {}) or {})
        params.setdefault("api_key", self.api_key)

        try:
            return self.session.request(
                method,
                self.url_for(path, prefix=prefix),
                headers=headers,
                params=params,
                timeout=self.timeout,
                verify=self.verify_ssl,
                allow_redirects=False,
                **kwargs,
            )
        except requests.Timeout as exc:
            raise ScrobAuthError("Scrob request timed out", reason="validation_timeout") from exc
        except requests.exceptions.SSLError as exc:
            raise ScrobAuthError("Scrob SSL validation failed", reason="invalid_ssl") from exc
        except requests.ConnectionError as exc:
            raise ScrobAuthError("Scrob server is unreachable", reason="unreachable") from exc
        except requests.RequestException as exc:
            raise ScrobAuthError("Scrob request failed", reason="validation_failed") from exc

    def raise_for_response(self, response: requests.Response) -> None:
        code = int(response.status_code)
        if code in (401, 403):
            raise ScrobAuthError("Scrob rejected the credentials", reason="unauthorized", status_code=code)
        if code in (301, 302, 303, 307, 308):
            raise ScrobAuthError("Scrob redirected the API call", reason="api_prefix_mismatch", status_code=code)
        if code >= 500:
            raise ScrobAuthError("Scrob server error", reason="server_error", status_code=code)
        if code >= 400:
            raise ScrobAuthError("Scrob request failed", reason=f"validation_http_{code}", status_code=code)

    def request_json(self, method: str, path: str, **kwargs: Any) -> Any:
        response = self.request(method, path, **kwargs)
        self.raise_for_response(response)
        try:
            return response.json()
        except ValueError as exc:
            raise ScrobAuthError("Scrob returned a non JSON response", reason="validation_bad_response", status_code=response.status_code) from exc


def client_from_block(block: Mapping[str, Any] | None, *, session: requests.Session | None = None) -> ScrobClient:
    data = block or {}
    return ScrobClient(
        str(data.get("server_url") or ""),
        str(data.get("api_key") or ""),
        api_prefix=data.get("api_prefix"),
        access_token=data.get("access_token"),
        verify_ssl=bool(data.get("verify_ssl", False)),
        timeout=float(data.get("timeout") or DEFAULT_TIMEOUT),
        session=session,
    )


def detect_api_prefix(
    server_url: str,
    api_key: str,
    *,
    verify_ssl: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
    session: requests.Session | None = None,
) -> str:
    client = ScrobClient(server_url, api_key, verify_ssl=verify_ssl, timeout=timeout, session=session)
    last: ScrobAuthError | None = None
    for prefix in API_PREFIXES:
        try:
            payload = client.request_json("GET", HEALTH_PATH, prefix=prefix)
        except ScrobAuthError as exc:
            last = exc
            if exc.reason in ("server_url_required", "api_key_required", "unreachable", "validation_timeout", "invalid_ssl"):
                raise
            continue
        if isinstance(payload, Mapping) and str(payload.get("status") or "").lower() == "ok":
            return normalize_api_prefix(prefix)
        last = ScrobAuthError("Scrob health check returned an unexpected body", reason="validation_bad_response")
    raise last or ScrobAuthError("Scrob API not found on this server", reason="api_not_found")


def login(
    server_url: str,
    api_key: str,
    username: str,
    password: str,
    *,
    totp_code: str = "",
    api_prefix: Any = None,
    verify_ssl: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    client = ScrobClient(server_url, api_key, api_prefix=api_prefix, verify_ssl=verify_ssl, timeout=timeout, session=session)
    response = client.request(
        "POST",
        LOGIN_PATH,
        data={"grant_type": "password", "username": str(username or ""), "password": str(password or "")},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    code = int(response.status_code)
    if code == 401:
        raise ScrobAuthError("Scrob rejected the username or password", reason="invalid_credentials", status_code=code)
    if code == 403:
        detail = ""
        try:
            body = response.json()
            detail = str((body or {}).get("detail") or "").lower()
        except Exception:
            detail = ""
        reason = "password_login_disabled" if "sso" in detail or "disabled" in detail else "email_not_confirmed"
        raise ScrobAuthError("Scrob refused the password login", reason=reason, status_code=code)
    client.raise_for_response(response)

    try:
        body = response.json()
    except ValueError as exc:
        raise ScrobAuthError("Scrob returned a non JSON login response", reason="validation_bad_response", status_code=code) from exc
    if not isinstance(body, Mapping):
        raise ScrobAuthError("Scrob returned an unexpected login response", reason="validation_bad_response", status_code=code)
    if bool(body.get("requires_2fa")):
        code_text = str(totp_code or "").strip()
        if not code_text:
            raise ScrobAuthError("Scrob account requires two factor authentication", reason="totp_required", status_code=code)
        return verify_totp(client, str(body.get("temp_token") or ""), code_text)

    return _token_from(body, code)


def _token_from(body: Mapping[str, Any], code: int) -> dict[str, Any]:
    token = str(body.get("access_token") or "").strip()
    if not token:
        raise ScrobAuthError("Scrob login returned no access token", reason="validation_bad_response", status_code=code)
    expires_at = jwt_expiry(token) or int(time.time()) + TOKEN_FALLBACK_TTL
    return {"access_token": token, "expires_at": int(expires_at)}


def verify_totp(client: ScrobClient, temp_token: str, totp_code: str) -> dict[str, Any]:
    if not str(temp_token or "").strip():
        raise ScrobAuthError("Scrob did not issue a two factor challenge token", reason="validation_bad_response")
    response = client.request(
        "POST",
        VERIFY_2FA_PATH,
        json={"temp_token": str(temp_token), "code": str(totp_code or "").strip()},
    )
    code = int(response.status_code)
    if code in (400, 401):
        raise ScrobAuthError("Scrob rejected the two factor code", reason="invalid_totp_code", status_code=code)
    client.raise_for_response(response)
    try:
        body = response.json()
    except ValueError as exc:
        raise ScrobAuthError("Scrob returned a non JSON two factor response", reason="validation_bad_response", status_code=code) from exc
    if not isinstance(body, Mapping):
        raise ScrobAuthError("Scrob returned an unexpected two factor response", reason="validation_bad_response", status_code=code)
    return _token_from(body, code)


def needs_reauth(block: Mapping[str, Any] | None) -> bool:
    return bool((block or {}).get("reauth_required"))


def _set_reauth(cfg: dict[str, Any], instance_id: Any, value: bool) -> None:
    block = ensure_instance_block(cfg, "scrob", normalize_instance_id(instance_id))
    block["reauth_required"] = bool(value)
    if value:
        block["access_token"] = ""
        block["expires_at"] = 0


def token_expired(block: Mapping[str, Any] | None, *, skew: int = TOKEN_REFRESH_SKEW) -> bool:
    data = block or {}
    if not str(data.get("access_token") or "").strip():
        return True
    try:
        expires_at = int(data.get("expires_at") or 0)
    except Exception:
        return True
    if expires_at <= 0:
        return True
    return time.time() >= (expires_at - max(0, int(skew)))


def _store_token(cfg: dict[str, Any], instance_id: Any, token: Mapping[str, Any]) -> dict[str, Any]:
    block = ensure_instance_block(cfg, "scrob", instance_id)
    block["access_token"] = str(token.get("access_token") or "")
    block["expires_at"] = int(token.get("expires_at") or 0)
    return block


def clear_token(cfg: dict[str, Any], *, instance_id: Any = None) -> None:
    block = ensure_instance_block(cfg, "scrob", normalize_instance_id(instance_id))
    block["access_token"] = ""
    block["expires_at"] = 0


def refresh_token(cfg: dict[str, Any] | None = None, *, instance_id: Any = None, session: requests.Session | None = None, force: bool = False) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with _refresh_lock(inst):
        cfgd = dict(cfg or _load_config() or {})
        block = _block(cfgd, inst)
        if not is_configured(block):
            raise ScrobAuthError("Scrob is not configured", reason="not_configured")
        if not force and not token_expired(block):
            return {"access_token": str(block.get("access_token") or ""), "expires_at": int(block.get("expires_at") or 0)}

        fresh = dict(load_config() or {})
        fresh_block = _block(fresh, inst)
        if not force and not token_expired(fresh_block):
            return {"access_token": str(fresh_block.get("access_token") or ""), "expires_at": int(fresh_block.get("expires_at") or 0)}

        try:
            token = login(
                str(fresh_block.get("server_url") or block.get("server_url") or ""),
                str(fresh_block.get("api_key") or block.get("api_key") or ""),
                str(fresh_block.get("username") or block.get("username") or ""),
                str(fresh_block.get("password") or block.get("password") or ""),
                api_prefix=fresh_block.get("api_prefix") or block.get("api_prefix"),
                verify_ssl=bool(fresh_block.get("verify_ssl", block.get("verify_ssl", False))),
                timeout=float(fresh_block.get("timeout") or block.get("timeout") or DEFAULT_TIMEOUT),
                session=session,
            )
        except ScrobAuthError as exc:
            if exc.reason in REAUTH_LOGIN_REASONS:
                _set_reauth(fresh, inst, True)
                save_config(fresh)
                if isinstance(cfg, dict):
                    _set_reauth(cfg, inst, True)
            elif exc.reason in TERMINAL_LOGIN_REASONS:
                clear_token(fresh, instance_id=inst)
                save_config(fresh)
            raise

        _store_token(fresh, inst, token)
        _set_reauth(fresh, inst, False)
        save_config(fresh)
        if isinstance(cfg, dict):
            _store_token(cfg, inst, token)
            _set_reauth(cfg, inst, False)
        return token


def access_token_for(
    cfg: Mapping[str, Any] | None,
    *,
    instance_id: Any = None,
    session: requests.Session | None = None,
    required: bool = True,
) -> str:
    inst = normalize_instance_id(instance_id)
    block = _block(cfg or {}, inst)
    if not token_expired(block):
        return str(block.get("access_token") or "")
    try:
        token = refresh_token(dict(cfg or {}), instance_id=inst, session=session)
    except ScrobAuthError:
        if required:
            raise
        return ""
    return str(token.get("access_token") or "")


def request_with_auth(
    session: requests.Session,
    method: str,
    url: str,
    *,
    cfg: Mapping[str, Any] | None,
    instance_id: Any = None,
    timeout: float = DEFAULT_TIMEOUT,
    **kwargs: Any,
) -> requests.Response:
    inst = normalize_instance_id(instance_id)
    block = _block(cfg or {}, inst)
    read_only = str(method or "").upper() in ("GET", "HEAD", "OPTIONS")
    token = access_token_for(cfg, instance_id=inst, session=session, required=not read_only)

    def _send(bearer: str) -> requests.Response:
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.setdefault("Accept", "application/json")
        headers.setdefault("User-Agent", UA)
        headers["X-Api-Key"] = str(block.get("api_key") or "")
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        params = dict(kwargs.pop("params", {}) or {})
        params.setdefault("api_key", str(block.get("api_key") or ""))
        return session.request(
            method,
            url,
            headers=headers,
            params=params,
            timeout=timeout,
            verify=bool(block.get("verify_ssl", False)),
            allow_redirects=False,
            **dict(kwargs),
        )

    response = _send(token)
    if int(response.status_code) == 401:
        try:
            retry = refresh_token(dict(cfg or {}), instance_id=inst, session=session, force=True)
        except ScrobAuthError:
            if read_only:
                return response
            raise
        response = _send(str(retry.get("access_token") or ""))
    return response


def account_identity(client: ScrobClient) -> dict[str, Any]:
    try:
        payload = client.request_json("GET", ME_PATH)
    except ScrobAuthError:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def capability_report(client: ScrobClient, *, identity: Mapping[str, Any] | None = None) -> dict[str, Any]:
    report: dict[str, Any] = {
        "read_history": False,
        "read_ratings": False,
        "read_sessions": False,
        "read_progress": False,
        "read_lists": False,
        "write_history": False,
        "write_ratings": False,
        "write_watchlist": False,
        "write_scrobble": False,
    }
    probes = (
        ("read_history", "history", {"page_size": 1}),
        ("read_ratings", "ratings", None),
        ("read_sessions", "history/now-playing", None),
        ("read_progress", "history/continue-watching", None),
        ("read_lists", "lists", None),
    )
    for key, path, params in probes:
        try:
            client.request_json("GET", path, params=dict(params or {}))
        except ScrobAuthError:
            continue
        report[key] = True

    signed_in = dict(identity) if identity is not None else account_identity(client)
    if signed_in:
        report["write_history"] = True
        report["write_ratings"] = True
        report["write_watchlist"] = True

    report["write_scrobble"] = bool(report["read_history"])
    return report


def validate_credentials(
    server_url: str,
    api_key: str,
    username: str,
    password: str,
    *,
    totp_code: str = "",
    verify_ssl: bool = False,
    timeout: float = DEFAULT_TIMEOUT,
    session: requests.Session | None = None,
) -> tuple[bool, str, dict[str, Any]]:
    try:
        prefix = detect_api_prefix(server_url, api_key, verify_ssl=verify_ssl, timeout=timeout, session=session)
    except ScrobAuthError as exc:
        return False, str(exc.reason or "validation_failed"), {}

    probe = ScrobClient(server_url, api_key, api_prefix=prefix, verify_ssl=verify_ssl, timeout=timeout, session=session)
    try:
        probe.request_json("GET", PROFILE_PATH)
    except ScrobAuthError as exc:
        reason = "invalid_api_key" if exc.reason == "unauthorized" else str(exc.reason or "validation_failed")
        return False, reason, {}

    try:
        token = login(
            server_url,
            api_key,
            username,
            password,
            totp_code=totp_code,
            api_prefix=prefix,
            verify_ssl=verify_ssl,
            timeout=timeout,
            session=session,
        )
    except ScrobAuthError as exc:
        return False, str(exc.reason or "validation_failed"), {}

    client = ScrobClient(
        server_url,
        api_key,
        api_prefix=prefix,
        access_token=token.get("access_token"),
        verify_ssl=verify_ssl,
        timeout=timeout,
        session=session,
    )
    identity = account_identity(client)
    linked_key = str(identity.get("api_key") or "").strip()
    if linked_key and linked_key != str(api_key or "").strip():
        return False, "credentials_mismatch", {}

    detail: dict[str, Any] = {
        "api_prefix": prefix,
        "access_token": token.get("access_token"),
        "expires_at": token.get("expires_at"),
        "account": str(identity.get("username") or "").strip(),
        "totp_enabled": bool(identity.get("totp_enabled")),
        "reauth_required": False,
        "capabilities": capability_report(client, identity=identity),
    }
    return True, "", detail


def account_label(block: Mapping[str, Any] | None, *, session: requests.Session | None = None) -> str:
    if not is_configured(block):
        return ""
    try:
        payload = client_from_block(block, session=session).request_json("GET", PROFILE_PATH)
    except ScrobAuthError:
        return ""
    if isinstance(payload, Mapping):
        name = str(payload.get("display_name") or "").strip()
        if name:
            return name
    return str((block or {}).get("username") or "").strip()


class ScrobAuth(AuthProvider):
    name = "SCROB"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="SCROB",
            label="Scrob",
            flow="api_key",
            fields=[
                {"key": "scrob.server_url", "label": "Server URL", "type": "text", "required": True, "placeholder": "http://localhost:7330"},
                {"key": "scrob.api_key", "label": "API key", "type": "password", "required": True, "placeholder": "********"},
                {"key": "scrob.username", "label": "Username", "type": "text", "required": True, "placeholder": "scrob"},
                {"key": "scrob.password", "label": "Password", "type": "password", "required": True, "placeholder": "********"},
                {"key": "scrob.totp_code", "label": "2FA code", "type": "text", "required": False, "placeholder": "123456"},
                {"key": "scrob.verify_ssl", "label": "Verify SSL", "type": "bool", "required": False, "default": False},
            ],
            actions={"start": False, "finish": True, "refresh": True, "disconnect": True},
            notes="API key comes from Scrob > Connections > API Key. The username and password are the same ones you sign in to Scrob with; Scrob only accepts writes from a signed in session.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": True, "history": True, "progress": True, "playlists": False}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return status_for_block(_block(cfg, normalize_instance_id(instance_id)), instance_id=instance_id)

    def start(self, cfg: MutableMapping[str, Any] | None = None, redirect_uri: str | None = None, *, instance_id: Any = None) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        inst = normalize_instance_id(instance_id)
        server_url = normalize_server_url(payload.get("server_url") or payload.get("scrob.server_url"))
        api_key = str(payload.get("api_key") or payload.get("scrob.api_key") or "").strip()
        username = str(payload.get("username") or payload.get("scrob.username") or "").strip()
        password = str(payload.get("password") or payload.get("scrob.password") or "")
        verify_ssl = bool(payload.get("verify_ssl", payload.get("scrob.verify_ssl", False)))

        ok, reason, detail = validate_credentials(server_url, api_key, username, password, verify_ssl=verify_ssl)
        if not ok:
            raise ValueError(reason)

        block = ensure_instance_block(cfgd, "scrob", inst)
        block["server_url"] = server_url
        block["api_key"] = api_key
        block["username"] = username
        block["password"] = password
        block["verify_ssl"] = verify_ssl
        block["api_prefix"] = str(detail.get("api_prefix") or "")
        block["access_token"] = str(detail.get("access_token") or "")
        block["expires_at"] = int(detail.get("expires_at") or 0)
        block["capabilities"] = dict(detail.get("capabilities") or {})
        block["totp_enabled"] = bool(detail.get("totp_enabled"))
        block["reauth_required"] = False
        save_config(cfgd)
        return self.get_status(cfgd, instance_id=inst)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        try:
            refresh_token(dict(cfg or {}), instance_id=inst, force=True)
        except ScrobAuthError:
            pass
        return self.get_status(_load_config(), instance_id=inst)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        inst = normalize_instance_id(instance_id)
        block = ensure_instance_block(cfgd, "scrob", inst)
        block["server_url"] = ""
        block["api_key"] = ""
        block["username"] = ""
        block["password"] = ""
        block["api_prefix"] = ""
        block["access_token"] = ""
        block["expires_at"] = 0
        block["capabilities"] = {}
        block["totp_enabled"] = False
        block["reauth_required"] = False
        save_config(cfgd)
        return self.get_status(cfgd, instance_id=inst)


def html() -> str:
    return r"""<div class="section" id="sec-scrob">
  <style>
    #sec-scrob .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-scrob .inline{display:flex;gap:8px;align-items:center}
    #sec-scrob .verify{display:flex;gap:8px;align-items:center;margin-top:8px}
    #sec-scrob .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-scrob .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-scrob .msg.hidden{display:none}
    #sec-scrob .hidden{display:none}
    #sec-scrob .btn.danger{background:#a8182e;border-color:rgba(255,107,107,.4)}
    #sec-scrob #scrob_connect{background:linear-gradient(135deg,#6f36cc,#ae45c8);border-color:rgba(111,54,204,.45);box-shadow:0 0 14px rgba(111,54,204,.35);color:#fff}
    #sec-scrob #scrob_connect:hover{filter:brightness(1.06);box-shadow:0 0 18px rgba(174,69,200,.5)}
  </style>
  <div class="head" data-toggle-section="sec-scrob">
    <span class="chev"></span><strong>Scrob</strong>
  </div>
  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="scrob">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Scrob <span class="cw-exp-badge">Experimental</span></div>
            <div class="muted">Connect your Scrob server, API key and account.</div>
          </div>
        </div>
        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>
        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:111,54,204;--cw-auth-c2:174,69,200;--cw-auth-logo:url('/assets/img/SCROB.png')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to Scrob</div>
                <div class="cw-auth-journey-copy">Copy the API key from Scrob &rsaquo; Connections &rsaquo; API Key and use the same URL you open Scrob with. Scrob only accepts writes from a signed in session, so your Scrob username and password are needed as well.</div>
              </div>
            </div>
            <div class="grid2">
              <div>
                <label for="scrob_server">Server URL</label>
                <input id="scrob_server" name="scrob_server" type="text" placeholder="http://localhost:7330" autocomplete="off" spellcheck="false" autocapitalize="off">
              </div>
              <div>
                <label for="scrob_key">API key</label>
                <input id="scrob_key" name="scrob_key" type="password" placeholder="********" autocomplete="off" spellcheck="false" autocapitalize="off" data-lpignore="true" data-1p-ignore="true" data-bwignore="true">
              </div>
            </div>
            <div class="grid2" style="margin-top:10px">
              <div>
                <label for="scrob_username">Username</label>
                <input id="scrob_username" name="scrob_username" type="text" placeholder="scrob" autocomplete="off" spellcheck="false" autocapitalize="off">
              </div>
              <div>
                <label for="scrob_password">Password</label>
                <input id="scrob_password" name="scrob_password" type="password" placeholder="********" autocomplete="off" spellcheck="false" autocapitalize="off" data-lpignore="true" data-1p-ignore="true" data-bwignore="true">
              </div>
            </div>
            <div id="scrob_totp_row" class="hidden" style="margin-top:10px;max-width:240px">
              <label for="scrob_totp">Two factor code</label>
              <input id="scrob_totp" name="scrob_totp" type="text" inputmode="numeric" autocomplete="one-time-code" placeholder="123456" maxlength="10" spellcheck="false">
              <div class="muted" style="margin-top:6px">Scrob issues a session that lasts 7 days and cannot be renewed on its own. Reads and scrobbling keep working after that; enter a new code to resume writes.</div>
            </div>
            <div id="scrob_reauth" class="msg warn hidden" style="margin-top:8px;margin-left:0">
              Two factor session expired. Reads and scrobbling are still running - enter a new code to resume history, ratings and watchlist writes.
            </div>
            <label class="verify"><input id="scrob_verify_ssl" type="checkbox"> Verify SSL</label>
            <div id="scrob_actions_row" class="inline" style="margin-top:10px">
              <button id="scrob_connect" class="btn" type="button">Connect Scrob</button>
              <button id="scrob_disconnect" type="button" hidden aria-hidden="true" tabindex="-1"></button>
              <div id="scrob_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


PROVIDER = ScrobAuth()
