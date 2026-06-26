# /api/mobileAPI.py
# CrossWatch - Android companion API facade
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import hashlib
import hmac
import importlib
import io
import json
import secrets
import time
from typing import Any
from urllib.parse import urlparse, urlencode

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from cw_platform.config_base import load_config, save_config
from services.activity import list_events
from services.backups import create_backup

from . import appAuthAPI as app_auth
from .versionAPI import CURRENT_VERSION

router = APIRouter(prefix="/api/mobile", tags=["mobile"])

DEFAULT_SCOPES = ["read", "actions", "diagnostics", "safe-config"]
PAIRING_TTL_SEC = 10 * 60
TOKEN_TTL_SEC = 365 * 24 * 60 * 60
PAIRING_CLAIM_FAIL_LIMIT = 8
PAIRING_CLAIM_FAIL_WINDOW_SEC = 10 * 60

_PAIRING_CLAIM_FAILS: dict[str, list[int]] = {}


def _now() -> int:
    return int(time.time())


def _sha256_hex(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _cfg_mobile(cfg: dict[str, Any]) -> dict[str, Any]:
    block = cfg.setdefault("mobile_auth", {})
    if not isinstance(block, dict):
        block = {}
        cfg["mobile_auth"] = block
    block.setdefault("enabled", True)
    block.setdefault("devices", [])
    block.setdefault("pairings", [])
    return block


def _clean_scopes(values: Any) -> list[str]:
    raw = values if isinstance(values, list) else DEFAULT_SCOPES
    out: list[str] = []
    for item in raw:
        scope = str(item or "").strip().lower()
        if scope in DEFAULT_SCOPES and scope not in out:
            out.append(scope)
    return out or ["read"]


def _public_device(device: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(device.get("id") or "").strip(),
        "name": str(device.get("name") or "").strip() or "Android device",
        "scopes": _clean_scopes(device.get("scopes")),
        "created_at": int(device.get("created_at") or 0),
        "last_seen_at": int(device.get("last_seen_at") or 0),
        "revoked_at": int(device.get("revoked_at") or 0),
    }


def _device_active(device: dict[str, Any]) -> bool:
    exp = int(device.get("expires_at") or 0)
    return bool(str(device.get("token_hash") or "").strip()) and not int(device.get("revoked_at") or 0) and exp > _now()


def _prune_mobile(block: dict[str, Any]) -> None:
    now = _now()
    pairings = block.get("pairings")
    if isinstance(pairings, list):
        block["pairings"] = [
            p for p in pairings
            if isinstance(p, dict) and not p.get("claimed_at") and int(p.get("expires_at") or 0) > now
        ]
    devices = block.get("devices")
    if isinstance(devices, list):
        block["devices"] = [d for d in devices if isinstance(d, dict)]


def _mobile_auth_required(cfg: dict[str, Any]) -> bool:
    block = _cfg_mobile(cfg)
    if block.get("enabled") is False:
        return False
    devices = block.get("devices") if isinstance(block.get("devices"), list) else []
    return app_auth.auth_required(cfg) or bool(devices)


def _bearer_token(request: Request) -> str:
    auth = str(request.headers.get("authorization") or "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


def _find_device_for_token(cfg: dict[str, Any], token: str) -> dict[str, Any] | None:
    if not token:
        return None
    want = _sha256_hex(token)
    block = _cfg_mobile(cfg)
    devices_raw = block.get("devices")
    devices: list[Any] = devices_raw if isinstance(devices_raw, list) else []
    for device in devices:
        if not isinstance(device, dict) or not _device_active(device):
            continue
        got = str(device.get("token_hash") or "").strip()
        if got and hmac.compare_digest(got.encode("utf-8"), want.encode("utf-8")):
            return device
    return None


def _require_web_auth(request: Request, cfg: dict[str, Any]) -> None:
    if not app_auth.auth_required(cfg):
        return
    token = request.cookies.get(app_auth.COOKIE_NAME)
    if not app_auth.is_authenticated(cfg, token):
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not app_auth._origin_allowed(request):  # type: ignore[attr-defined]
        raise HTTPException(status_code=403, detail="Origin mismatch")


def _require_mobile_scope(request: Request, scope: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    cfg = load_config() or {}
    block = _cfg_mobile(cfg)
    _prune_mobile(block)
    if not _mobile_auth_required(cfg):
        return cfg, None

    device = _find_device_for_token(cfg, _bearer_token(request))
    if device is None:
        raise HTTPException(status_code=401, detail="mobile_token_required")

    scopes = set(_clean_scopes(device.get("scopes")))
    want = str(scope or "read").strip().lower()
    if want and want not in scopes:
        raise HTTPException(status_code=403, detail=f"mobile_scope_required:{want}")

    now = _now()
    if now - int(device.get("last_seen_at") or 0) > 60:
        device["last_seen_at"] = now
        save_config(cfg)
    return cfg, device


def _request_base_url(request: Request) -> str:
    proto = str(request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip()
    host = str(request.headers.get("x-forwarded-host") or request.headers.get("host") or "").split(",", 1)[0].strip()
    if proto and host and proto.lower() in ("http", "https"):
        return f"{proto.lower()}://{host}".rstrip("/")
    return str(request.base_url).rstrip("/")


def _safe_server_url(value: Any) -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        return ""
    return f"{parsed.scheme}://{parsed.netloc}".rstrip("/")


def _client_key(request: Request) -> str:
    forwarded = str(request.headers.get("x-forwarded-for") or "").split(",", 1)[0].strip()
    host = forwarded or getattr(getattr(request, "client", None), "host", "") or ""
    return host or "unknown"


def _pairing_claim_failures(request: Request) -> list[int]:
    now = _now()
    key = _client_key(request)
    failures = [
        ts for ts in _PAIRING_CLAIM_FAILS.get(key, [])
        if now - int(ts or 0) <= PAIRING_CLAIM_FAIL_WINDOW_SEC
    ]
    if failures:
        _PAIRING_CLAIM_FAILS[key] = failures
    else:
        _PAIRING_CLAIM_FAILS.pop(key, None)
    return failures


def _pairing_claim_rate_check(request: Request) -> None:
    if len(_pairing_claim_failures(request)) >= PAIRING_CLAIM_FAIL_LIMIT:
        raise HTTPException(status_code=429, detail="pairing_claim_rate_limited")


def _pairing_claim_note_failure(request: Request) -> None:
    key = _client_key(request)
    failures = _pairing_claim_failures(request)
    failures.append(_now())
    _PAIRING_CLAIM_FAILS[key] = failures


def _pairing_claim_clear_failures(request: Request) -> None:
    _PAIRING_CLAIM_FAILS.pop(_client_key(request), None)


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, JSONResponse):
        try:
            return json.loads(bytes(value.body).decode("utf-8"))
        except Exception:
            return {}
    return value if isinstance(value, dict) else {}


def _provider_names(cfg: dict[str, Any]) -> list[str]:
    out: set[str] = set()
    for pair in cfg.get("pairs") or []:
        if not isinstance(pair, dict):
            continue
        for key in ("src", "source", "from", "dst", "target", "to"):
            value = pair.get(key)
            if isinstance(value, str) and value.strip():
                out.add(value.strip().upper())
            elif isinstance(value, dict):
                provider = str(value.get("provider") or value.get("name") or "").strip()
                if provider:
                    out.add(provider.upper())
    configured = [
        "PLEX",
        "JELLYFIN",
        "EMBY",
        "TRAKT",
        "SIMKL",
        "TMDB",
        "MDBLIST",
        "ANILIST",
        "PUBLICMETADB",
        "TAUTULLI",
    ]
    for name in configured:
        block = cfg.get(name.lower()) or cfg.get(name) or {}
        if isinstance(block, dict) and any(str(v or "").strip() for v in block.values()):
            out.add(name)
    return sorted(out)


def _providers_payload(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    names = _provider_names(cfg)
    if not names:
        names = ["PLEX", "TRAKT", "SIMKL"]
    return [
        {
            "name": name.title() if name != "TMDB" else "TMDb",
            "status": "Configured" if name in _provider_names(cfg) else "Not configured",
            "healthy": name in _provider_names(cfg),
        }
        for name in names
    ]


def _activity_payload() -> tuple[list[dict[str, Any]], int]:
    try:
        payload = list_events(limit=12, offset=0) or {}
        raw = payload.get("items") or payload.get("events") or []
    except Exception:
        raw = []
    items: list[dict[str, Any]] = []
    warnings = 0
    for event in raw if isinstance(raw, list) else []:
        if not isinstance(event, dict):
            continue
        level = str(event.get("level") or event.get("status") or "INFO").upper()
        if level in ("WARN", "WARNING", "ERROR", "FAILED"):
            warnings += 1
        title = str(event.get("title") or event.get("summary") or event.get("kind") or "Activity")
        detail = str(event.get("detail") or event.get("message") or event.get("route") or "")
        ts = int(event.get("ts") or event.get("time") or event.get("created_at") or 0) if str(event.get("ts") or event.get("time") or event.get("created_at") or "0").isdigit() else 0
        ago = _ago(ts) if ts else ""
        items.append({"title": title, "detail": detail, "time": ago, "level": "WARN" if level == "WARNING" else level})
    return items, warnings


def _ago(ts: int) -> str:
    delta = max(0, int(time.time()) - int(ts))
    if delta < 60:
        return "now"
    if delta < 3600:
        return f"{delta // 60} min ago"
    if delta < 86400:
        return f"{delta // 3600} h ago"
    return f"{delta // 86400} d ago"


def _watching_label() -> str:
    try:
        from .scrobbleAPI import api_currently_watching

        payload = _as_dict(api_currently_watching())
        data = payload.get("currently_watching")
        if isinstance(data, dict):
            title = str(data.get("title") or data.get("grandparentTitle") or "").strip()
            if title:
                state = str(data.get("state") or "").strip()
                return f"{title} ({state})" if state else title
    except Exception:
        pass
    return "Nothing playing"


def _scheduler_label() -> tuple[str, str]:
    try:
        from .schedulingAPI import sched_status

        payload = sched_status() or {}
        config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
        enabled = bool((config or {}).get("enabled") or ((config or {}).get("advanced") or {}).get("enabled"))
        next_run = int(payload.get("next_run_at") or 0)
        return ("Enabled" if enabled else "Disabled", _ago(next_run) if next_run and next_run < time.time() else ("Scheduled" if next_run else "Not scheduled"))
    except Exception:
        return ("Unknown", "Not scheduled")


def _library_payload() -> list[dict[str, Any]]:
    try:
        from .syncAPI import _load_state

        state = _load_state() or {}
    except Exception:
        state = {}
    wall = state.get("wall") if isinstance(state, dict) else None
    wall_count = len(wall) if isinstance(wall, list) else 0
    return [
        {"title": "Unified watchlist", "value": f"{wall_count} items" if wall_count else "Ready", "detail": "Mobile read-only overview"},
        {"title": "Playback progress", "value": "Open manager", "detail": "Use the web UI for detailed edits"},
        {"title": "Recent activity", "value": "Live", "detail": "Grouped CrossWatch activity feed"},
    ]


@router.post("/pairing/start")
def mobile_pairing_start(request: Request, payload: dict[str, Any] | None = Body(default=None)) -> JSONResponse:
    cfg = load_config() or {}
    _require_web_auth(request, cfg)
    block = _cfg_mobile(cfg)
    _prune_mobile(block)

    body = payload or {}
    device_name = str(body.get("device_name") or body.get("name") or "Android device").strip()[:80] or "Android device"
    scopes = _clean_scopes(body.get("scopes"))
    code = secrets.token_urlsafe(9).replace("-", "").replace("_", "")[:10].upper()
    pairing_id = secrets.token_hex(8)
    now = _now()
    base_url = _safe_server_url(body.get("server_url")) or _request_base_url(request)
    pairing_uri = "crosswatch://pair?" + urlencode({"server": base_url, "code": code})
    pairings = block.setdefault("pairings", [])
    if not isinstance(pairings, list):
        pairings = []
        block["pairings"] = pairings
    pairings.append(
        {
            "id": pairing_id,
            "code_hash": _sha256_hex(code),
            "device_name": device_name,
            "scopes": scopes,
            "created_at": now,
            "expires_at": now + PAIRING_TTL_SEC,
            "pairing_uri": pairing_uri,
            "ip": getattr(getattr(request, "client", None), "host", "") or "",
        }
    )
    save_config(cfg)

    return JSONResponse(
        {
            "ok": True,
            "id": pairing_id,
            "code": code,
            "pairing_uri": pairing_uri,
            "server_url": base_url,
            "scopes": scopes,
            "expires_at": now + PAIRING_TTL_SEC,
        },
        headers={"Cache-Control": "no-store"},
    )


@router.get("/pairing/{pairing_id}/qr.svg")
def mobile_pairing_qr(request: Request, pairing_id: str) -> Response:
    cfg = load_config() or {}
    _require_web_auth(request, cfg)
    block = _cfg_mobile(cfg)
    _prune_mobile(block)
    pairings_raw = block.get("pairings")
    pairings: list[Any] = pairings_raw if isinstance(pairings_raw, list) else []
    pairing = next(
        (
            item for item in pairings
            if isinstance(item, dict)
            and str(item.get("id") or "") == str(pairing_id)
            and int(item.get("expires_at") or 0) > _now()
        ),
        None,
    )
    if pairing is None:
        raise HTTPException(status_code=404, detail="mobile_pairing_not_found")

    pairing_uri = str(pairing.get("pairing_uri") or "").strip()
    if not pairing_uri:
        raise HTTPException(status_code=404, detail="mobile_pairing_uri_missing")

    try:
        qrcode = importlib.import_module("qrcode")
        qrcode_svg = importlib.import_module("qrcode.image.svg")
    except Exception as exc:
        raise HTTPException(status_code=503, detail="mobile_qr_dependency_missing") from exc

    img = qrcode.make(pairing_uri, image_factory=qrcode_svg.SvgPathImage)
    out = io.BytesIO()
    img.save(out)
    return Response(
        content=out.getvalue(),
        media_type="image/svg+xml",
        headers={"Cache-Control": "no-store", "X-CrossWatch-QR": "qrcode"},
    )


@router.post("/pairing/claim")
def mobile_pairing_claim(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    cfg = load_config() or {}
    block = _cfg_mobile(cfg)
    _prune_mobile(block)

    code = str(payload.get("code") or "").strip().upper()
    device_name = str(payload.get("device_name") or payload.get("name") or "Android device").strip()[:80] or "Android device"
    if not code:
        raise HTTPException(status_code=400, detail="pairing_code_required")
    _pairing_claim_rate_check(request)

    code_hash = _sha256_hex(code)
    pairings_raw = block.get("pairings")
    pairings: list[Any] = pairings_raw if isinstance(pairings_raw, list) else []
    pairing = None
    for item in pairings:
        if not isinstance(item, dict):
            continue
        if int(item.get("expires_at") or 0) <= _now():
            continue
        got = str(item.get("code_hash") or "")
        if got and hmac.compare_digest(got.encode("utf-8"), code_hash.encode("utf-8")):
            pairing = item
            break
    if pairing is None:
        _pairing_claim_note_failure(request)
        raise HTTPException(status_code=404, detail="invalid_or_expired_pairing_code")

    token = secrets.token_urlsafe(32)
    device_id = secrets.token_hex(8)
    now = _now()
    scopes = _clean_scopes(pairing.get("scopes"))
    devices = block.setdefault("devices", [])
    if not isinstance(devices, list):
        devices = []
        block["devices"] = devices
    device = {
        "id": device_id,
        "name": device_name or str(pairing.get("device_name") or "Android device"),
        "token_hash": _sha256_hex(token),
        "scopes": scopes,
        "created_at": now,
        "last_seen_at": now,
        "expires_at": now + TOKEN_TTL_SEC,
        "ip": getattr(getattr(request, "client", None), "host", "") or "",
        "ua": str(request.headers.get("user-agent") or "")[:240],
    }
    devices.append(device)
    pairing["claimed_at"] = now
    _prune_mobile(block)
    _pairing_claim_clear_failures(request)
    save_config(cfg)

    return JSONResponse(
        {
            "ok": True,
            "token": token,
            "device": _public_device(device),
            "scopes": scopes,
            "expires_at": now + TOKEN_TTL_SEC,
        },
        headers={"Cache-Control": "no-store"},
    )


@router.get("/devices")
def mobile_devices(request: Request) -> JSONResponse:
    cfg = load_config() or {}
    _require_web_auth(request, cfg)
    block = _cfg_mobile(cfg)
    _prune_mobile(block)
    save_config(cfg)
    devices_raw = block.get("devices")
    devices = [_public_device(d) for d in (devices_raw if isinstance(devices_raw, list) else []) if isinstance(d, dict)]
    return JSONResponse({"ok": True, "devices": devices}, headers={"Cache-Control": "no-store"})


@router.delete("/devices/{device_id}")
def mobile_device_revoke(request: Request, device_id: str) -> dict[str, Any]:
    cfg = load_config() or {}
    _require_web_auth(request, cfg)
    block = _cfg_mobile(cfg)
    found = False
    now = _now()
    devices_raw = block.get("devices")
    devices: list[Any] = devices_raw if isinstance(devices_raw, list) else []
    for device in devices:
        if isinstance(device, dict) and str(device.get("id") or "") == str(device_id):
            device["revoked_at"] = now
            found = True
            break
    if not found:
        raise HTTPException(status_code=404, detail="mobile_device_not_found")
    save_config(cfg)
    return {"ok": True, "revoked": True, "id": device_id}


@router.get("/summary")
def mobile_summary(request: Request) -> JSONResponse:
    cfg, _device = _require_mobile_scope(request, "read")
    activity, warnings = _activity_payload()
    scheduler, next_run = _scheduler_label()
    payload = {
        "server_name": "CrossWatch",
        "version": CURRENT_VERSION,
        "sync_running": False,
        "scheduler": scheduler,
        "next_run": next_run,
        "currently_watching": _watching_label(),
        "warnings": warnings,
        "providers": _providers_payload(cfg),
        "activity": activity,
        "library": _library_payload(),
    }
    try:
        from .syncAPI import _is_sync_running

        payload["sync_running"] = bool(_is_sync_running())
    except Exception:
        pass
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.post("/actions/run")
def mobile_run_sync(request: Request) -> dict[str, Any]:
    _require_mobile_scope(request, "actions")
    from .syncAPI import api_run_sync

    return api_run_sync({})


@router.post("/actions/backup")
def mobile_create_backup(request: Request) -> dict[str, Any]:
    _require_mobile_scope(request, "actions")
    res = create_backup(scope="app_state", label="mobile", trigger="mobile")
    return {"ok": True, "backup": res}


@router.post("/actions/watch/stop")
def mobile_stop_watch(request: Request) -> dict[str, Any]:
    _require_mobile_scope(request, "actions")
    from .scrobbleAPI import debug_watch_stop

    return debug_watch_stop(request)
