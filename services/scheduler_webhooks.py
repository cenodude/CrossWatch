# services/scheduler_webhooks.py
# CrossWatch - outbound callbacks for scheduled sync runs
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

import requests

SCHEDULED_SYNC_MODES = {"standard", "advanced", "advanced_workflow"}
EVENTS = {"start", "success", "failure"}

DEFAULT_WEBHOOKS: dict[str, Any] = {
    "enabled": False,
    "url": "",
    "base_url": "",
    "start_url": "",
    "success_url": "",
    "failure_url": "",
    "payload_format": "crosswatch",
    "notifiarr_channel_id": "",
    "timeout_seconds": 10,
    "alert_on_unresolved": False,
}


def normalize_scheduler_webhooks(value: Any) -> dict[str, Any]:
    src = value if isinstance(value, dict) else {}
    out = dict(DEFAULT_WEBHOOKS)
    out["enabled"] = _as_bool(src.get("enabled"), False)
    out["alert_on_unresolved"] = _as_bool(src.get("alert_on_unresolved") or src.get("alertOnUnresolved"), False)
    out["url"] = _http_url_or_blank(src.get("url") or src.get("default_url"))
    out["base_url"] = _http_url_or_blank(src.get("base_url") or src.get("healthchecks_base_url"))
    for key in ("start_url", "success_url", "failure_url"):
        out[key] = _http_url_or_blank(src.get(key))
    out["payload_format"] = _payload_format(src.get("payload_format") or src.get("format"))
    out["notifiarr_channel_id"] = _digits_or_blank(src.get("notifiarr_channel_id") or src.get("notifiarrChannelId"))
    out["timeout_seconds"] = _as_int(src.get("timeout_seconds"), 10, minimum=1, maximum=60)
    return out


def is_scheduled_sync_context(context: Any) -> bool:
    ctx = context if isinstance(context, dict) else {}
    if str(ctx.get("source") or "").strip().lower() != "scheduler":
        return False
    mode = str(ctx.get("scheduler_mode") or "").strip().lower()
    return mode in SCHEDULED_SYNC_MODES


def callback_url(webhooks: dict[str, Any], event: str) -> str:
    ev = str(event or "").strip().lower()
    if ev not in EVENTS:
        return ""

    if _payload_format(webhooks.get("payload_format") or webhooks.get("format")) == "notifiarr":
        return str(webhooks.get("url") or webhooks.get("default_url") or "").strip()

    explicit_key = {"start": "start_url", "success": "success_url", "failure": "failure_url"}[ev]
    explicit = str(webhooks.get(explicit_key) or "").strip()
    if explicit:
        return explicit

    common = str(webhooks.get("url") or webhooks.get("default_url") or "").strip()
    if common:
        return common

    base = str(webhooks.get("base_url") or webhooks.get("healthchecks_base_url") or "").strip()
    if not base:
        return ""
    if ev == "start":
        return _with_path_suffix(base, "start")
    if ev == "failure":
        return _with_path_suffix(base, "fail")
    return base


def build_payload(
    event: str,
    context: dict[str, Any] | None,
    summary: dict[str, Any] | None,
    payload_format: str = "crosswatch",
) -> dict[str, Any]:
    ctx = dict(context or {})
    snap = dict(summary or {})
    ev = str(event or "").strip().lower()
    exit_code = snap.get("exit_code")
    try:
        exit_code = int(exit_code) if exit_code is not None else None
    except Exception:
        exit_code = None

    payload = {
        "event": ev,
        "status": "ok" if ev == "success" else "failed" if ev == "failure" else "started",
        "run_id": str(ctx.get("run_id") or snap.get("run_id") or ""),
        "scheduler_mode": str(ctx.get("scheduler_mode") or ""),
        "job_id": str(ctx.get("job_id") or ""),
        "workflow_id": str(ctx.get("workflow_id") or ""),
        "workflow_step_id": str(ctx.get("workflow_step_id") or ""),
        "pair_id": str(ctx.get("pair_id") or ctx.get("pair_scope") or ""),
        "started_at": snap.get("started_at"),
        "finished_at": snap.get("finished_at"),
        "duration_sec": snap.get("duration_sec"),
        "exit_code": exit_code,
        "summary": _summary_counts(snap),
    }
    payload = {k: v for k, v in payload.items() if v not in ("", None, {})}
    if _payload_format(payload_format) == "notifiarr":
        return _notifiarr_payload(payload, ctx, snap)
    return payload


def resolve_completion_event(cfg: dict[str, Any] | None, summary: dict[str, Any] | None) -> str:
    snap = summary if isinstance(summary, dict) else {}
    try:
        exit_code = int(snap.get("exit_code") or 0)
    except Exception:
        exit_code = 0
    if exit_code != 0 or snap.get("cancelled"):
        return "failure"

    sch = (cfg or {}).get("scheduling") if isinstance(cfg, dict) else {}
    webhooks = normalize_scheduler_webhooks((sch if isinstance(sch, dict) else {}).get("webhooks"))
    if not webhooks.get("alert_on_unresolved"):
        return "success"

    for key in ("unresolved", "errors"):
        if _as_int(snap.get(key), 0, minimum=0) > 0:
            return "failure"
    return "success"


def notify_scheduler_webhook(
    cfg: dict[str, Any] | None,
    event: str,
    context: dict[str, Any] | None,
    summary: dict[str, Any] | None = None,
    *,
    log_fn: Callable[[str], None] | None = None,
) -> bool:
    if not is_scheduled_sync_context(context):
        return False

    sch = (cfg or {}).get("scheduling") if isinstance(cfg, dict) else {}
    sch = sch if isinstance(sch, dict) else {}
    webhooks = normalize_scheduler_webhooks(sch.get("webhooks"))
    if not webhooks.get("enabled"):
        return False

    url = callback_url(webhooks, event)
    if not url:
        return False

    payload = build_payload(event, context, summary, str(webhooks.get("payload_format") or "crosswatch"))
    if webhooks.get("payload_format") == "notifiarr":
        channel_id = str(webhooks.get("notifiarr_channel_id") or "").strip()
        if channel_id:
            payload.setdefault("discord", {}).setdefault("ids", {})["channel"] = int(channel_id)
    timeout = int(webhooks.get("timeout_seconds") or 10)
    try:
        resp = requests.post(
            url,
            json=payload,
            timeout=timeout,
            headers={"User-Agent": "CrossWatch scheduler webhook"},
        )
        resp.raise_for_status()
        _log(log_fn, f"[i] Scheduler webhook {event}: delivered")
        return True
    except Exception as exc:
        _log(log_fn, f"[!] Scheduler webhook {event}: {type(exc).__name__}")
        return False


def _summary_counts(summary: dict[str, Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for key in ("added", "removed", "updated"):
        direct = summary.get(f"{key}_last")
        if direct is not None:
            out[key] = _as_int(direct, 0, minimum=0)

    features = summary.get("features")
    enabled = summary.get("enabled")
    if isinstance(features, dict):
        totals = {"added": 0, "removed": 0, "updated": 0}
        for name, lane in features.items():
            if isinstance(enabled, dict) and enabled.get(name) is False:
                continue
            if not isinstance(lane, dict):
                continue
            for key in totals:
                totals[key] += _as_int(lane.get(key), 0, minimum=0)
        for key, value in totals.items():
            out[key] = max(out.get(key, 0), value)

    for key in ("unresolved", "errors", "blocked", "skipped"):
        value = summary.get(key)
        if value is not None:
            out[key] = _as_int(value, 0, minimum=0)
    return {k: v for k, v in out.items() if v}


def _notifiarr_payload(payload: dict[str, Any], context: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    event = str(payload.get("event") or "").strip().lower()
    status = str(payload.get("status") or event or "").strip()
    title_event = {
        "start": "started",
        "success": "succeeded",
        "failure": "failed",
    }.get(event, status or "updated")
    color = {
        "start": "4F8CFF",
        "success": "2ECC71",
        "failure": "FF5A5F",
    }.get(event, "7D86C9")

    fields: list[dict[str, Any]] = []
    for title, key in (
        ("Run", "run_id"),
        ("Mode", "scheduler_mode"),
        ("Job", "job_id"),
        ("Workflow", "workflow_id"),
        ("Step", "workflow_step_id"),
        ("Pair", "pair_id"),
        ("Exit", "exit_code"),
        ("Duration", "duration_sec"),
    ):
        value = payload.get(key)
        if value in ("", None, {}):
            continue
        fields.append({"title": title, "text": str(value), "inline": True})

    counts = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    for key in ("added", "removed", "updated", "errors", "blocked", "skipped", "unresolved"):
        value = counts.get(key) if isinstance(counts, dict) else None
        if value:
            fields.append({"title": key.replace("_", " ").title(), "text": str(value), "inline": True})

    description = f"Scheduled sync {title_event}."
    if event == "failure":
        description = "Scheduled sync failed. Check the CrossWatch sync logs for details."
    elif event == "success":
        description = "Scheduled sync completed successfully."

    run_id = str(payload.get("run_id") or context.get("run_id") or summary.get("run_id") or "")
    notification_event = run_id or f"crosswatch-{event or 'scheduler'}"
    return {
        "notification": {
            "update": False,
            "name": "CrossWatch",
            "event": notification_event,
        },
        "discord": {
            "color": color,
            "ping": {
                "pingUser": 0,
                "pingRole": 0,
            },
            "images": {
                "thumbnail": "",
                "image": "",
            },
            "text": {
                "title": f"CrossWatch scheduled sync {title_event}",
                "icon": "",
                "content": "",
                "description": description,
                "fields": fields[:25],
                "footer": "CrossWatch Scheduler",
            },
        },
    }


def _with_path_suffix(url: str, suffix: str) -> str:
    parts = urlsplit(url)
    path = (parts.path or "").rstrip("/") + "/" + suffix.strip("/")
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


def _http_url_or_blank(value: Any) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    try:
        parts = urlsplit(url)
    except Exception:
        return ""
    if parts.scheme.lower() not in {"http", "https"} or not parts.netloc:
        return ""
    return url


def _payload_format(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if text in {"notifiarr", "notifiarr_passthrough"}:
        return "notifiarr"
    return "crosswatch"


def _digits_or_blank(value: Any) -> str:
    text = str(value or "").strip()
    return text if text.isdigit() else ""


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _as_int(value: Any, default: int = 0, *, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    if minimum is not None:
        out = max(int(minimum), out)
    if maximum is not None:
        out = min(int(maximum), out)
    return out


def _log(log_fn: Callable[[str], None] | None, message: str) -> None:
    if not log_fn:
        return
    try:
        log_fn(message)
    except Exception:
        pass
