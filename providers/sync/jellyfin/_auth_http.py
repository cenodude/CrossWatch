# providers/sync/jellyfin/_auth_http.py
# CrossWatch - Canonical Jellyfin authentication/connection primitives
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import re
import secrets
from typing import Any
from urllib.parse import urljoin

import requests
from requests import exceptions as rx

UA = os.environ.get("CW_JELLYFIN_UA") or os.environ.get("CW_UA") or "CrossWatch"
CLIENT_VERSION = os.environ.get("CW_JELLYFIN_VERSION") or os.environ.get("CW_VERSION") or "1.0"
CLIENT_NAME = "CrossWatch"
DEVICE_NAME = "CrossWatch"

MINIMUM_SERVER_VERSION = (10, 9)
MINIMUM_SERVER_VERSION_TEXT = "10.9"
QUICK_CONNECT_MIN_VERSION = (10, 8)

HTTP_TIMEOUT_POST = 15
HTTP_TIMEOUT_GET = 10

_VERSION_RE = re.compile(r"^\s*(\d+)\.(\d+)")
_SECRET_RE = re.compile(r'((?:Token|secret|Secret)\s*[=:]\s*)"?[^"&,\s]+', re.IGNORECASE)


class JellyfinAuthError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "error") -> None:
        super().__init__(message)
        self.reason = reason


def redact(text: Any) -> str:
    return _SECRET_RE.sub(r"\1***", str(text or ""))


def clean_base(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "http://" + u
    return u if u.endswith("/") else u + "/"


def new_device_id() -> str:
    return secrets.token_hex(16)


def mb_authorization(token: str | None, device_id: str) -> str:
    base = (
        f'MediaBrowser Client="{CLIENT_NAME}", Device="{DEVICE_NAME}", '
        f'DeviceId="{device_id}", Version="{CLIENT_VERSION}"'
    )
    return f'{base}, Token="{token}"' if token else base


def auth_headers(token: str | None, device_id: str) -> dict[str, str]:
    value = mb_authorization(token, device_id)
    headers: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": UA,
        "Authorization": value,
        "X-Emby-Authorization": value,
    }
    if token:
        headers["X-MediaBrowser-Token"] = token
        headers["X-Emby-Token"] = token
    return headers


def server_version_tuple(value: Any) -> tuple[int, int] | None:
    match = _VERSION_RE.match(str(value or ""))
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))


def validate_server_version(value: Any) -> str:
    version = str(value or "").strip()
    parsed = server_version_tuple(version)
    if parsed is None:
        raise JellyfinAuthError(
            f"Unable to determine Jellyfin server version; CrossWatch requires Jellyfin {MINIMUM_SERVER_VERSION_TEXT} or newer.",
            reason="version_unknown",
        )
    if parsed < MINIMUM_SERVER_VERSION:
        raise JellyfinAuthError(
            f"Jellyfin version too old: detected {version}; CrossWatch requires Jellyfin {MINIMUM_SERVER_VERSION_TEXT} or newer.",
            reason="version_too_old",
        )
    return version


class JellyfinAuthSession:
    """Short-lived authenticated-connection helper used to establish a token.

    Both password and Quick Connect authentication build on this; once a token
    exists the rest of CrossWatch consumes it through the runtime JFClient.
    """

    def __init__(
        self,
        server: str,
        *,
        device_id: str | None = None,
        verify_ssl: bool = True,
        token: str | None = None,
    ) -> None:
        self.base = clean_base(server)
        if not self.base:
            raise JellyfinAuthError("Malformed request: missing server", reason="missing_server")
        self.device_id = (device_id or "").strip() or new_device_id()
        self.verify_ssl = bool(verify_ssl)
        self.token = (token or "").strip() or None
        self._session = requests.Session()
        self._session.verify = self.verify_ssl

    def __enter__(self) -> "JellyfinAuthSession":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def close(self) -> None:
        try:
            self._session.close()
        except Exception:
            pass

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        content_type: str | None = None,
        token: str | None = None,
        expect_json: bool = True,
    ) -> requests.Response:
        url = urljoin(self.base, path.lstrip("/"))
        headers = auth_headers(token if token is not None else self.token, self.device_id)
        if content_type:
            headers["Content-Type"] = content_type
        timeout = HTTP_TIMEOUT_POST if method.upper() == "POST" else HTTP_TIMEOUT_GET
        try:
            return self._session.request(
                method, url, params=params, json=json, headers=headers, timeout=timeout
            )
        except (rx.ConnectTimeout, rx.ReadTimeout):
            raise JellyfinAuthError("Server not reachable: timeout", reason="unreachable")
        except rx.SSLError:
            raise JellyfinAuthError("Server not reachable: ssl", reason="unreachable")
        except rx.ConnectionError:
            raise JellyfinAuthError("Server not reachable: connection", reason="unreachable")
        except rx.InvalidURL:
            raise JellyfinAuthError("Malformed request: server url", reason="missing_server")
        except rx.RequestException as exc:
            raise JellyfinAuthError(
                f"Server not reachable: {exc.__class__.__name__}", reason="unreachable"
            )

    @staticmethod
    def _json(resp: requests.Response) -> dict[str, Any]:
        try:
            data = resp.json()
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _raise_http(resp: requests.Response, default: str, *, reason: str = "error") -> None:
        message = default
        try:
            body = resp.json() or {}
            message = body.get("ErrorMessage") or body.get("Message") or default
        except Exception:
            text = (getattr(resp, "text", "") or "").strip()
            if text:
                message = f"{default}: {redact(text)[:200]}"
        raise JellyfinAuthError(message, reason=reason)

    def system_info(self, *, require_token: bool = True) -> dict[str, Any]:
        resp = self._request("GET", "System/Info" if require_token else "System/Info/Public")
        if not resp.ok:
            self._raise_http(
                resp,
                f"Unable to determine Jellyfin server version; CrossWatch requires Jellyfin {MINIMUM_SERVER_VERSION_TEXT} or newer.",
                reason="version_unknown",
            )
        return self._json(resp)

    def server_version(self, *, require_token: bool = True) -> str:
        info = self.system_info(require_token=require_token)
        return validate_server_version(info.get("Version") or info.get("ServerVersion"))

    def resolve_user(self, token: str) -> tuple[str, str]:
        resp = self._request("GET", "Users/Me", token=token)
        if not resp.ok:
            return "", ""
        info = self._json(resp)
        return str(info.get("Id") or "").strip(), str(info.get("Name") or "").strip()

    def authenticate_by_name(self, username: str, password: str) -> dict[str, Any]:
        resp = self._request(
            "POST",
            "Users/AuthenticateByName",
            json={"Username": username, "Pw": password},
            content_type="application/json",
            token="",
        )
        if resp.status_code in (401, 403):
            raise JellyfinAuthError("Invalid credentials", reason="unauthorized")
        if resp.status_code >= 500:
            raise JellyfinAuthError(f"Server error ({resp.status_code})", reason="unreachable")
        if not resp.ok:
            self._raise_http(resp, "Login failed", reason="login_failed")
        return self._json(resp)

    def quick_connect_enabled(self) -> bool:
        resp = self._request("GET", "QuickConnect/Enabled", token="")
        if not resp.ok:
            return False
        try:
            return bool(resp.json())
        except Exception:
            return False

    def quick_connect_initiate(self) -> dict[str, Any]:
        resp = self._request("POST", "QuickConnect/Initiate", token="")
        if resp.status_code in (401, 403):
            raise JellyfinAuthError("Quick Connect is disabled on this server", reason="disabled")
        if not resp.ok:
            self._raise_http(resp, "Quick Connect initiation failed", reason="initiate_failed")
        data = self._json(resp)
        if not (data.get("Secret") and data.get("Code")):
            raise JellyfinAuthError("Quick Connect initiation failed", reason="initiate_failed")
        return data

    def quick_connect_state(self, secret: str) -> dict[str, Any]:
        resp = self._request("GET", "QuickConnect/Connect", params={"secret": secret}, token="")
        if resp.status_code == 404:
            raise JellyfinAuthError("Quick Connect request expired", reason="expired")
        if not resp.ok:
            self._raise_http(resp, "Quick Connect check failed", reason="connect_failed")
        return self._json(resp)

    def authenticate_with_quick_connect(self, secret: str) -> dict[str, Any]:
        resp = self._request(
            "POST",
            "Users/AuthenticateWithQuickConnect",
            json={"Secret": secret},
            content_type="application/json",
            token="",
        )
        if resp.status_code in (401, 403):
            raise JellyfinAuthError("Quick Connect request not authorized", reason="not_authorized")
        if not resp.ok:
            self._raise_http(resp, "Quick Connect authentication failed", reason="connect_failed")
        return self._json(resp)
