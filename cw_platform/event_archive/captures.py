# cw_platform/event_archive/captures.py
# CrossWatch - Capture and backup events for the Events archive
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import re
import secrets
from collections.abc import Mapping
from typing import Any

from .recorder import make_event, record_events

_LOG = logging.getLogger("crosswatch.event_archive")

CAPTURE_DOMAIN = "capture"
BACKUP_DOMAIN = "backup"
_URL_USERINFO = re.compile(r"(?i)(\b[a-z][a-z0-9+.-]*://)[^/@\s]+@")
_BACKUP_SCOPES = {"app_state": "App state", "full": "Full", "config_only": "Settings", "selected": "Selected"}


def _text(value: Any, limit: int = 240) -> str:
    return str(value if value is not None else "").strip()[:limit]


def _safe(value: Any, limit: int) -> str:
    text = _text(value, 2000)
    try:
        from _logging import _redact_log_text

        text = _redact_log_text(text)
    except Exception:
        pass
    return _URL_USERINFO.sub(lambda m: f"{m.group(1)}****@", text)[:limit]


def _trigger(value: Any) -> str:
    return _text(value, 40).lower() or "manual"


def _provider_label(provider: str) -> str:
    try:
        from cw_platform.modules_registry import provider_display_label

        return provider_display_label(provider) or provider
    except Exception:
        return provider


def _write(
    event_type: str,
    *,
    feature: str,
    trigger: Any,
    title: str,
    ok: bool,
    reason: Any = "",
    provider: Any = "",
    instance: Any = "",
    detail: Mapping[str, Any] | None = None,
) -> int:
    prov = _text(provider, 40).upper()
    clean = {k: (_safe(v, 300) if isinstance(v, str) else v) for k, v in dict(detail or {}).items() if v not in (None, "", [], {})}
    row = make_event(
        domain=BACKUP_DOMAIN if feature == "backup" else CAPTURE_DOMAIN,
        event_type=event_type,
        severity="info" if ok else "error",
        feature=feature,
        operation=_trigger(trigger),
        source_provider=prov or None,
        source_instance=(_text(instance, 80) or "default") if prov else None,
        title=_text(title, 300),
        reason_code=None if ok else "failed",
        reason=_safe(reason, 500) or None,
        source_kind=_trigger(trigger),
        detail=clean or None,
        hash_extra=secrets.token_hex(12),
    )
    try:
        return record_events([row])
    except Exception as exc:
        _LOG.debug("capture event write failed: %s", exc)
        return 0


def record_capture(
    provider: Any,
    instance: Any,
    feature: Any,
    *,
    trigger: Any = "manual",
    ok: bool = True,
    count: Any = None,
    label: Any = "",
    path: Any = "",
    reason: Any = "",
) -> int:
    prov = _text(provider, 40).upper()
    feat = _text(feature, 40).lower() or "all"
    name = "All features" if feat == "all" else feat.replace("_", " ").title()
    title = f"{name} capture, {_provider_label(prov)}"
    return _write(
        "capture_completed" if ok else "capture_failed",
        feature="capture", trigger=trigger, ok=ok, reason=reason, provider=prov, instance=instance,
        title=title if ok else f"{title} failed",
        detail={"capture_feature": feat, "items": count, "label": _text(label, 120), "path": _text(path, 300)},
    )


def record_capture_cleanup(provider: Any, instance: Any, feature: Any, removed: int, *, trigger: Any = "manual") -> int:
    if int(removed or 0) <= 0:
        return 0
    prov = _text(provider, 40).upper()
    feat = _text(feature, 40).lower() or "all"
    plural = "capture" if int(removed) == 1 else "captures"
    return _write(
        "capture_cleanup",
        feature="capture", trigger=trigger, ok=True, provider=prov, instance=instance,
        title=f"Removed {int(removed)} old {feat.replace('_', ' ')} {plural}, {_provider_label(prov)}",
        detail={"capture_feature": feat, "removed": int(removed)},
    )


def record_backup(
    scope: Any,
    *,
    trigger: Any = "manual",
    ok: bool = True,
    size: Any = None,
    label: Any = "",
    path: Any = "",
    reason: Any = "",
) -> int:
    sc = _text(scope, 40).lower() or "app_state"
    title = f"{_BACKUP_SCOPES.get(sc, sc.replace('_', ' ').title())} backup"
    return _write(
        "backup_completed" if ok else "backup_failed",
        feature="backup", trigger=trigger, ok=ok, reason=reason,
        title=title if ok else f"{title} failed",
        detail={"scope": sc, "size": size, "label": _text(label, 120), "path": _text(path, 300)},
    )


def record_backup_upload(destination: Any, *, ok: bool, trigger: Any = "manual", reason: Any = "", path: Any = "") -> int:
    kind = _text(destination, 40).lower() or "remote"
    where = {"webdav": "Nextcloud / WebDAV", "local": "mounted folder", "folder": "mounted folder"}.get(kind, kind)
    return _write(
        "backup_uploaded" if ok else "backup_upload_failed",
        feature="backup", trigger=trigger, ok=ok, reason=reason,
        title=f"Backup copied to {where}" if ok else f"Backup copy to {where} failed",
        detail={"destination": kind, "path": _text(path, 300)},
    )


def record_backup_cleanup(removed: int, *, trigger: Any = "manual") -> int:
    if int(removed or 0) <= 0:
        return 0
    plural = "backup" if int(removed) == 1 else "backups"
    return _write(
        "backup_cleanup",
        feature="backup", trigger=trigger, ok=True,
        title=f"Removed {int(removed)} old {plural}",
        detail={"removed": int(removed)},
    )
