# providers/auth/_auth_KODI.py
# CrossWatch - Kodi Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any, cast

import requests
from requests.auth import HTTPBasicAuth

from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id, resolve_provider_block

from ._auth_base import AuthManifest, AuthProvider, AuthStatus

__VERSION__ = "0.1"

UA = "CrossWatch/1.0 (Kodi)"
MIN_KODI_VERSION = (21, 0, 0)
MIN_JSONRPC_VERSION = (13, 5, 0)


class KodiAuthError(RuntimeError):
    def __init__(self, message: str, *, reason: str = "error", detail: Any = None) -> None:
        super().__init__(message)
        self.reason = reason
        self.detail = detail


def clean_base(url: Any) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    if not value.startswith(("http://", "https://")):
        value = "http://" + value
    return value.rstrip("/")


def _version_tuple(value: Any) -> tuple[int, int, int]:
    if isinstance(value, Mapping):
        return (
            _to_int(value.get("major")),
            _to_int(value.get("minor")),
            _to_int(value.get("patch")),
        )
    parts = [p for p in str(value or "").replace("-", ".").split(".") if p.strip()]
    nums = [_to_int(part) for part in parts[:3]]
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums[:3])  # type: ignore[return-value]


def _version_text(value: Any) -> str:
    major, minor, patch = _version_tuple(value)
    return f"{major}.{minor}.{patch}"


def _to_int(value: Any) -> int:
    try:
        return int(str(value or "0").strip())
    except Exception:
        return 0


def _jsonrpc_version_payload(value: Any) -> Any:
    if isinstance(value, Mapping) and "version" in value:
        return value.get("version")
    return value


def _basic_auth(username: str, password: str) -> HTTPBasicAuth | None:
    if not username and not password:
        return None
    return HTTPBasicAuth(username, password)


def _jsonrpc_error_message(method: str, error: Any, *, request_id: str = "") -> str:
    if isinstance(error, Mapping):
        code = error.get("code")
        msg = str(error.get("message") or "error").strip()
        data = error.get("data")
        suffix = f" data={data}" if data not in (None, "") else ""
        target = request_id or method
        return f"Kodi JSON-RPC error for {target}: {msg} ({code}){suffix}"
    return f"Kodi JSON-RPC error for {request_id or method}: {error}"


def jsonrpc_call(
    server: str,
    method: str,
    *,
    params: Mapping[str, Any] | None = None,
    username: str = "",
    password: str = "",
    verify_ssl: bool = False,
    timeout: float = 12.0,
) -> Any:
    base = clean_base(server)
    if not base:
        raise KodiAuthError("Missing Kodi server URL", reason="missing_server")

    payload: dict[str, Any] = {"jsonrpc": "2.0", "id": method, "method": method}
    if params is not None:
        payload["params"] = dict(params)

    try:
        response = requests.post(
            f"{base}/jsonrpc",
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json", "User-Agent": UA},
            auth=_basic_auth(username, password),
            timeout=timeout,
            verify=bool(verify_ssl),
        )
    except requests.Timeout as exc:
        raise KodiAuthError("Kodi server is unreachable: timeout", reason="unreachable") from exc
    except requests.RequestException as exc:
        raise KodiAuthError("Kodi server is unreachable", reason="unreachable") from exc

    if response.status_code in (401, 403):
        raise KodiAuthError("Kodi rejected the supplied credentials", reason="invalid_credentials")
    if response.status_code >= 400:
        raise KodiAuthError(f"Kodi JSON-RPC HTTP error {response.status_code}", reason="http_error")

    try:
        data = response.json()
    except ValueError as exc:
        raise KodiAuthError("Kodi JSON-RPC returned invalid JSON", reason="invalid_response") from exc

    if not isinstance(data, Mapping):
        raise KodiAuthError("Kodi JSON-RPC returned an invalid response", reason="invalid_response")
    if data.get("error"):
        raise KodiAuthError(_jsonrpc_error_message(method, data.get("error")), reason="jsonrpc_error", detail=data.get("error"))
    if "result" not in data:
        raise KodiAuthError("Kodi JSON-RPC response is missing result", reason="invalid_response")
    return data.get("result")


def jsonrpc_batch_call(
    server: str,
    calls: list[tuple[str, Mapping[str, Any] | None]],
    *,
    username: str = "",
    password: str = "",
    verify_ssl: bool = False,
    timeout: float = 12.0,
) -> dict[str, Any]:
    base = clean_base(server)
    if not base:
        raise KodiAuthError("Missing Kodi server URL", reason="missing_server")
    id_to_method: dict[str, str] = {}
    payload = []
    for i, (method, params) in enumerate(calls):
        rid = f"{i}:{method}"
        id_to_method[rid] = method
        payload.append({"jsonrpc": "2.0", "id": rid, "method": method, **({"params": dict(params)} if params is not None else {})})
    if not payload:
        return {}
    try:
        response = requests.post(
            f"{base}/jsonrpc",
            json=payload,
            headers={"Accept": "application/json", "Content-Type": "application/json", "User-Agent": UA},
            auth=_basic_auth(username, password),
            timeout=timeout,
            verify=bool(verify_ssl),
        )
    except requests.Timeout as exc:
        raise KodiAuthError("Kodi server is unreachable: timeout", reason="unreachable") from exc
    except requests.RequestException as exc:
        raise KodiAuthError("Kodi server is unreachable", reason="unreachable") from exc
    if response.status_code in (401, 403):
        raise KodiAuthError("Kodi rejected the supplied credentials", reason="invalid_credentials")
    if response.status_code >= 400:
        raise KodiAuthError(f"Kodi JSON-RPC HTTP error {response.status_code}", reason="http_error")
    try:
        data = response.json()
    except ValueError as exc:
        raise KodiAuthError("Kodi JSON-RPC returned invalid JSON", reason="invalid_response") from exc
    if not isinstance(data, list):
        raise KodiAuthError("Kodi JSON-RPC returned an invalid batch response", reason="invalid_response")
    out: dict[str, Any] = {}
    for row in data:
        if not isinstance(row, Mapping):
            raise KodiAuthError("Kodi JSON-RPC returned an invalid batch item", reason="invalid_response")
        rid = str(row.get("id") or "")
        if row.get("error"):
            method = id_to_method.get(rid, rid)
            raise KodiAuthError(_jsonrpc_error_message(method, row.get("error"), request_id=rid), reason="jsonrpc_error", detail=row.get("error"))
        if not rid or "result" not in row:
            raise KodiAuthError("Kodi JSON-RPC batch item is missing result", reason="invalid_response")
        out[rid] = row.get("result")
    return out


def verify_connection(
    server: str,
    *,
    username: str = "",
    password: str = "",
    verify_ssl: bool = False,
    timeout: float = 12.0,
) -> dict[str, Any]:
    ping = jsonrpc_call(server, "JSONRPC.Ping", username=username, password=password, verify_ssl=verify_ssl, timeout=timeout)
    if str(ping or "").lower() != "pong":
        raise KodiAuthError("Kodi JSON-RPC ping failed", reason="invalid_response")

    app = jsonrpc_call(
        server,
        "Application.GetProperties",
        params={"properties": ["name", "version"]},
        username=username,
        password=password,
        verify_ssl=verify_ssl,
        timeout=timeout,
    )
    if not isinstance(app, Mapping):
        raise KodiAuthError("Kodi application response is invalid", reason="invalid_response")
    if str(app.get("name") or "").strip().lower() != "kodi":
        raise KodiAuthError("Server is not Kodi", reason="not_kodi")

    kodi_version_raw = app.get("version")
    kodi_version = _version_text(kodi_version_raw)
    if _version_tuple(kodi_version_raw) < MIN_KODI_VERSION:
        raise KodiAuthError("Kodi version too old; CrossWatch requires Kodi 21.0 Omega or newer.", reason="version_too_old")

    rpc = jsonrpc_call(server, "JSONRPC.Version", username=username, password=password, verify_ssl=verify_ssl, timeout=timeout)
    rpc_version_raw = _jsonrpc_version_payload(rpc)
    jsonrpc_version = _version_text(rpc_version_raw)
    if _version_tuple(rpc_version_raw) < MIN_JSONRPC_VERSION:
        raise KodiAuthError("Kodi JSON-RPC version too old; CrossWatch requires 13.5.0 or newer.", reason="jsonrpc_too_old")

    return {
        "kodi_version": kodi_version,
        "jsonrpc_version": jsonrpc_version,
    }


def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return resolve_provider_block(cfg or {}, "kodi", instance_id)


class KodiAuth(AuthProvider):
    name = "KODI"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="KODI",
            label="Kodi",
            flow="credentials",
            fields=[
                {"key": "kodi.server", "label": "Server URL", "type": "text", "required": True},
                {"key": "kodi.username", "label": "Username", "type": "text", "required": False},
                {"key": "kodi.password", "label": "Password", "type": "password", "required": False},
                {"key": "kodi.verify_ssl", "label": "Verify SSL", "type": "bool", "required": False, "default": False},
            ],
            actions={"start": True, "finish": False, "refresh": False, "disconnect": True},
            notes="Kodi uses HTTP JSON-RPC. Username and password are optional HTTP Basic Auth credentials.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"features": {}}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        kodi = _block(cfg, inst)
        connected = bool(str(kodi.get("server") or "").strip() and kodi.get("connection_verified") is True)
        label = "Kodi" if inst == "default" else f"Kodi ({inst})"
        extra: dict[str, Any] = {}
        if kodi.get("kodi_version"):
            extra["kodi_version"] = str(kodi.get("kodi_version"))
        if kodi.get("jsonrpc_version"):
            extra["jsonrpc_version"] = str(kodi.get("jsonrpc_version"))
        if kodi.get("auth_method"):
            extra["auth_method"] = str(kodi.get("auth_method"))
        return AuthStatus(connected=connected, label=label, user=str(kodi.get("username") or "").strip() or None, extra=extra)

    def start(
        self,
        cfg: MutableMapping[str, Any],
        redirect_uri: str | None = None,
        *,
        instance_id: Any = None,
    ) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        kodi = ensure_instance_block(cast(dict[str, Any], cfg), "kodi", inst)

        server = clean_base(kodi.get("server"))
        username = str(kodi.get("username") or "").strip()
        password = str(kodi.get("password") or "")
        verify_ssl = bool(kodi.get("verify_ssl", False))
        timeout = float(kodi.get("timeout", 12.0) or 12.0)
        if not server:
            raise KodiAuthError("Missing Kodi server URL", reason="missing_server")

        detected = verify_connection(
            server,
            username=username,
            password=password,
            verify_ssl=verify_ssl,
            timeout=timeout,
        )

        kodi["server"] = server
        kodi["username"] = username
        kodi["password"] = password
        kodi["verify_ssl"] = verify_ssl
        kodi["kodi_version"] = detected["kodi_version"]
        kodi["jsonrpc_version"] = detected["jsonrpc_version"]
        kodi["auth_method"] = "basic" if (username or password) else "none"
        kodi["connection_verified"] = True

        return {
            "ok": True,
            "kodi_version": detected["kodi_version"],
            "jsonrpc_version": detected["jsonrpc_version"],
            "auth_method": kodi["auth_method"],
        }

    def finish(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        kodi = ensure_instance_block(cast(dict[str, Any], cfg), "kodi", inst)
        kodi["password"] = ""
        for key in ("kodi_version", "jsonrpc_version", "auth_method", "connection_verified"):
            kodi.pop(key, None)
        return self.get_status(cfg, instance_id=inst)


def html() -> str:
    return r"""<div class="section" id="sec-kodi">
  <style>
    #sec-kodi .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-kodi .inline{display:flex;gap:8px;align-items:center}
    #sec-kodi .inp-row{display:flex;gap:12px;align-items:center}
    #sec-kodi .inp-row .grow{flex:1 1 auto}
    #sec-kodi .verify{display:flex;gap:8px;align-items:center;white-space:nowrap}
    #sec-kodi .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-kodi .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-kodi .msg.hidden{display:none}
    #sec-kodi .btn.danger{background:#a8182e;border-color:rgba(255,107,107,.4)}
    #sec-kodi #kodi_connect{background:linear-gradient(135deg,#17b5d1,#1496c8);border-color:rgba(23,181,209,.45);box-shadow:0 0 14px rgba(23,181,209,.32);color:#fff}
  </style>

  <div class="head" data-toggle-section="sec-kodi">
    <span class="chev"></span><strong>Kodi</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="kodi">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Kodi</div>
            <div class="muted">Connect a Kodi media client over HTTP JSON-RPC.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
          <button type="button" class="cw-subtile" data-sub="whitelist">Whitelisting</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="grid2">
              <div style="grid-column:1 / -1">
                <label for="kodi_server">Server URL</label>
                <div class="inp-row">
                <input id="kodi_server" name="kodi_server" class="grow" placeholder="http://host:8080" autocomplete="off" spellcheck="false" autocapitalize="off">
                  <label class="verify"><input id="kodi_verify_ssl" type="checkbox"> Verify SSL</label>
                </div>
              </div>
              <div>
                <label for="kodi_username">Username</label>
                <input id="kodi_username" name="kodi_username" autocomplete="username" placeholder="optional">
              </div>
              <div>
                <label for="kodi_password">Password</label>
                <input id="kodi_password" name="kodi_password" type="password" autocomplete="current-password" placeholder="optional">
              </div>
            </div>

            <div class="inline" style="margin-top:12px;flex-wrap:wrap">
              <button id="kodi_connect" class="btn" type="button">Connect Kodi</button>
              <button id="kodi_disconnect" type="button" hidden aria-hidden="true" tabindex="-1"></button>
              <div id="kodi_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
            </div>
          </div>
          <div class="cw-subpanel" data-sub="whitelist">
            <div class="muted" style="margin-bottom:12px">Limit Kodi sync and scrobbling to selected video sources. Empty means all sources.</div>
            <div id="kodi_libraries">
              <div class="cw-wl">
                <div class="cw-wl-foot">
                  <div class="cw-wl-note">Empty = all libraries.</div>
                  <div class="cw-wl-foot-r">
                    <button type="button" class="cw-wl-load" data-kodi-load-libs><span class="material-symbols-rounded" aria-hidden="true">sync</span>Load libraries</button>
                  </div>
                </div>
              </div>
            </div>
            <select id="kodi_lib_history" class="lm-hidden" multiple></select>
            <select id="kodi_lib_ratings" class="lm-hidden" multiple></select>
            <select id="kodi_lib_progress" class="lm-hidden" multiple></select>
            <select id="kodi_lib_collection" class="lm-hidden" multiple></select>
            <select id="kodi_lib_scrobble" class="lm-hidden" multiple></select>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


PROVIDER = KodiAuth()
__all__ = ["PROVIDER", "KodiAuth", "KodiAuthError", "clean_base", "jsonrpc_call", "jsonrpc_batch_call", "verify_connection", "html", "__VERSION__"]
