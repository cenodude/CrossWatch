# cw_platform/event_archive/audit.py
# CrossWatch - Audit event recording
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import re
import secrets
from collections.abc import Mapping
from typing import Any

from .recorder import make_event, record_events

_LOG = logging.getLogger("crosswatch.event_archive.audit")
_SENSITIVE_KEYS = ("password", "token", "cookie", "secret", "totp", "api_key", "apikey", "pin", "code", "hash", "salt")


def _text(value: Any, limit: int = 240) -> str:
    s = str(value or "").strip()
    return s[:limit]


def _slug(value: Any) -> str:
    s = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:80] or "action"


def _safe_fields(fields: Mapping[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in (fields or {}).items():
        k = _text(key, 80)
        if not k:
            continue
        kl = k.lower()
        if any(part in kl for part in _SENSITIVE_KEYS):
            out[k] = "[redacted]"
        elif isinstance(value, (str, int, float, bool)) or value is None:
            out[k] = _text(value, 500) if isinstance(value, str) else value
        else:
            out[k] = _text(value, 500)
    return out


def _actor(user: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(user, Mapping):
        return {"id": "", "username": "", "role": "unknown", "profile_id": ""}
    is_admin = bool(user.get("is_admin")) or str(user.get("role") or "").lower() == "admin"
    return {
        "id": _text(user.get("id"), 80),
        "username": _text(user.get("username"), 160),
        "role": "admin" if is_admin else "user",
        "profile_id": _text(user.get("profile_id"), 80),
    }


def _request_meta(request: Any) -> dict[str, Any]:
    if request is None:
        return {}
    client = getattr(request, "client", None)
    return {
        "ip": _text(getattr(client, "host", "") or "", 80),
        "user_agent": _text(getattr(getattr(request, "headers", {}), "get", lambda *_: "")("user-agent") or "", 240),
        "method": _text(getattr(request, "method", "") or "", 16),
        "path": _text(getattr(getattr(request, "url", None), "path", "") or "", 240),
    }


def record_audit(
    action: Any,
    *,
    actor: Mapping[str, Any] | None = None,
    request: Any = None,
    status: str = "success",
    target_type: Any = "",
    target_id: Any = "",
    message: Any = "",
    fields: Mapping[str, Any] | None = None,
    severity: str | None = None,
    source_kind: str = "app",
) -> int:
    act = _slug(action)
    st = _slug(status)
    detail = {
        "actor": _actor(actor),
        "target": {"type": _text(target_type, 80), "id": _text(target_id, 120)},
        "status": st,
        "message": _text(message, 500),
        "request": _request_meta(request),
        "fields": _safe_fields(fields),
    }
    event = make_event(
        domain="audit",
        event_type=f"audit_{act}",
        severity=severity or ("error" if st in {"failed", "blocked", "denied"} else "info"),
        feature="security",
        operation=act,
        title=_text(message, 500) or act.replace("_", " ").title(),
        reason_code=None if st == "success" else st,
        source_kind=_text(source_kind, 80) or "app",
        detail=detail,
        hash_extra=secrets.token_hex(12),
    )
    try:
        return record_events([event])
    except Exception as exc:
        _LOG.debug("audit write failed: %s", exc)
        return 0
