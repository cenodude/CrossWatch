# services/scheduler_webhooks.py
# CrossWatch - outbound callbacks for scheduled sync runs
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import ipaddress
import socket
import time
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import urlsplit, urlunsplit

import requests

from _logging import log as BASE_LOG

SCHEDULED_SYNC_MODES = {"standard", "advanced", "advanced_workflow"}
EVENTS = {"start", "success", "failure"}
BLOCKED_HOSTNAMES = {"localhost", "localhost.localdomain"}
EMBEDDED_IPV4_NETWORKS = (
    ipaddress.ip_network("64:ff9b::/96"),
    ipaddress.ip_network("::ffff:0:0:0/96"),
    ipaddress.ip_network("::/96"),
)
PAYLOAD_FORMAT_LABELS = {
    "crosswatch": "CrossWatch JSON",
    "notifiarr": "Notifiarr Passthrough",
    "pushover": "Pushover",
    "ntfy": "ntfy",
}
SINGLE_URL_FORMATS = {"notifiarr", "ntfy"}
PUSHOVER_URL = "https://api.pushover.net/1/messages.json"
PUSHOVER_KEY_LENGTH = 30
PUSHOVER_TITLE_MAX = 250
PUSHOVER_MESSAGE_MAX = 1024
PUSHOVER_PRIORITY = {"start": -1, "success": 0, "failure": 1}
NTFY_MESSAGE_MAX_BYTES = 4096
NTFY_PRIORITY = {"start": 2, "success": 3, "failure": 4}
NOTIFIARR_COLORS = {"start": "4F8CFF", "success": "2ECC71", "failure": "FF5A5F"}
HEALTH_EVENTS: dict[str, tuple[str, str, str, str]] = {
    "watcher_stopped": (
        "alert_on_watcher",
        "failure",
        "CrossWatch watcher stopped",
        "These enabled watcher routes are not running:",
    ),
    "watcher_recovered": (
        "alert_on_watcher",
        "success",
        "CrossWatch watcher recovered",
        "These watcher routes are running again:",
    ),
    "provider_auth_lost": (
        "alert_on_provider_auth",
        "failure",
        "CrossWatch provider sign-in lost",
        "Sign in again in CrossWatch for:",
    ),
    "provider_auth_recovered": (
        "alert_on_provider_auth",
        "success",
        "CrossWatch provider sign-in recovered",
        "Sign-in works again for:",
    ),
}
LOG_MODULE = "SCHED"
TEST_TITLE = "CrossWatch test notification"
TEST_MESSAGE = "Scheduler webhooks are set up correctly."

DEFAULT_WEBHOOKS: dict[str, Any] = {
    "enabled": False,
    "url": "",
    "base_url": "",
    "start_url": "",
    "success_url": "",
    "failure_url": "",
    "payload_format": "crosswatch",
    "notifiarr_channel_id": "",
    "pushover_token": "",
    "pushover_user": "",
    "timeout_seconds": 10,
    "alert_on_unresolved": False,
    "alert_on_watcher": False,
    "alert_on_provider_auth": False,
}


def normalize_scheduler_webhooks(value: Any) -> dict[str, Any]:
    src = value if isinstance(value, dict) else {}
    out = dict(DEFAULT_WEBHOOKS)
    out["enabled"] = _as_bool(src.get("enabled"), False)
    out["alert_on_unresolved"] = _as_bool(_first_set(src, "alert_on_unresolved", "alertOnUnresolved"), False)
    out["alert_on_watcher"] = _as_bool(_first_set(src, "alert_on_watcher", "alertOnWatcher"), False)
    out["alert_on_provider_auth"] = _as_bool(_first_set(src, "alert_on_provider_auth", "alertOnProviderAuth"), False)
    out["url"] = _http_url_or_blank(src.get("url") or src.get("default_url"))
    out["base_url"] = _http_url_or_blank(src.get("base_url") or src.get("healthchecks_base_url"))
    for key in ("start_url", "success_url", "failure_url"):
        out[key] = _http_url_or_blank(src.get(key))
    out["payload_format"] = _payload_format(src.get("payload_format") or src.get("format"))
    out["notifiarr_channel_id"] = _digits_or_blank(src.get("notifiarr_channel_id") or src.get("notifiarrChannelId"))
    out["pushover_token"] = _pushover_key_or_blank(src.get("pushover_token") or src.get("pushoverToken"))
    out["pushover_user"] = _pushover_key_or_blank(src.get("pushover_user") or src.get("pushoverUser"))
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

    fmt = _payload_format(webhooks.get("payload_format") or webhooks.get("format"))
    if fmt == "pushover":
        token = _pushover_key_or_blank(webhooks.get("pushover_token"))
        user = _pushover_key_or_blank(webhooks.get("pushover_user"))
        return PUSHOVER_URL if token and user else ""
    if fmt in SINGLE_URL_FORMATS:
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


def health_callback_url(webhooks: dict[str, Any]) -> str:
    fmt = _payload_format(webhooks.get("payload_format") or webhooks.get("format"))
    if fmt == "pushover":
        return callback_url(webhooks, "failure")
    return str(webhooks.get("url") or webhooks.get("default_url") or "").strip()


def payload_format_label(value: Any) -> str:
    return PAYLOAD_FORMAT_LABELS[_payload_format(value)]


def scheduler_webhooks_configured(webhooks: dict[str, Any] | None) -> bool:
    hooks = webhooks if isinstance(webhooks, dict) else {}
    return any(callback_url(hooks, event) for event in sorted(EVENTS))


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
        "notices": [str(note) for note in snap.get("notices") or [] if note][:10] or None,
    }
    payload = {k: v for k, v in payload.items() if v not in ("", None, {})}
    fmt = _payload_format(payload_format)
    if fmt == "notifiarr":
        return _notifiarr_payload(payload, ctx, snap)
    if fmt in {"pushover", "ntfy"}:
        title, description, fields = _notification_text(payload)
        return _push_message(fmt, ev, title, _plain_message(description, fields))
    return payload


def build_health_payload(event: str, items: list[dict[str, Any]], payload_format: str = "crosswatch") -> dict[str, Any]:
    ev = str(event or "").strip().lower()
    _, level, title, lead = HEALTH_EVENTS[ev]
    rows = [(str(item.get("label") or ""), str(item.get("detail") or "")) for item in items]
    fmt = _payload_format(payload_format)
    if fmt in {"pushover", "ntfy"}:
        lines = [f"- {label}: {detail}" if detail else f"- {label}" for label, detail in rows]
        return _push_message(fmt, level, title, "\n".join([lead, *lines]))
    if fmt == "notifiarr":
        fields = [{"title": label, "text": detail or title, "inline": False} for label, detail in rows]
        return _notifiarr_envelope(f"crosswatch-{ev}", level, title, lead, fields, "CrossWatch Health")
    return {
        "event": ev,
        "status": "ok" if level == "success" else "failed",
        "title": title,
        "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "items": [dict(item) for item in items],
    }


def build_test_payload(payload_format: str = "crosswatch") -> dict[str, Any]:
    fmt = _payload_format(payload_format)
    if fmt in {"pushover", "ntfy"}:
        return _push_message(fmt, "success", TEST_TITLE, TEST_MESSAGE)
    if fmt == "notifiarr":
        return _notifiarr_envelope("crosswatch-test", "success", TEST_TITLE, TEST_MESSAGE, [], "CrossWatch Scheduler")
    return {"event": "test", "status": "ok", "title": TEST_TITLE, "message": TEST_MESSAGE}


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
        debug(f"Scheduler webhook {event}: not sent, webhooks are off")
        return False

    url = callback_url(webhooks, event)
    if not url:
        debug(f"Scheduler webhook {event}: not sent, no destination set for this event")
        return False
    fmt = str(webhooks.get("payload_format") or "crosswatch")
    return _deliver(url, event, build_payload(event, context, summary, fmt), webhooks, log_fn)


def notify_health_webhook(
    cfg: dict[str, Any] | None,
    event: str,
    items: list[dict[str, Any]],
    *,
    log_fn: Callable[[str], None] | None = None,
) -> bool:
    ev = str(event or "").strip().lower()
    if ev not in HEALTH_EVENTS or not items:
        return False

    sch = (cfg or {}).get("scheduling") if isinstance(cfg, dict) else {}
    webhooks = normalize_scheduler_webhooks((sch if isinstance(sch, dict) else {}).get("webhooks"))
    if not webhooks.get("enabled") or not webhooks.get(HEALTH_EVENTS[ev][0]):
        return False

    names = ", ".join(str(item.get("label") or "") for item in items)
    url = health_callback_url(webhooks)
    if not url:
        debug(f"Scheduler webhook {ev}: not sent for {names}, no destination set")
        return False
    fmt = str(webhooks.get("payload_format") or "crosswatch")
    debug(f"Scheduler webhook {ev}: reporting {names}")
    return _deliver(url, ev, build_health_payload(ev, items, fmt), webhooks, log_fn)


def send_test_webhook(webhooks: dict[str, Any] | None) -> tuple[bool, str]:
    hooks = normalize_scheduler_webhooks(webhooks)
    url = health_callback_url(hooks) or callback_url(hooks, "success")
    if not url:
        debug("Scheduler webhook test: not sent, no destination set")
        return False, "No destination configured for this payload format."
    logs: list[str] = []
    fmt = str(hooks.get("payload_format") or "crosswatch")
    ok = _deliver(url, "test", build_test_payload(fmt), hooks, logs.append)
    detail = logs[-1].split(": ", 1)[-1] if logs else ""
    return ok, "" if ok else detail or "Delivery failed."


def _deliver(
    url: str,
    event: str,
    payload: dict[str, Any],
    webhooks: dict[str, Any],
    log_fn: Callable[[str], None] | None,
) -> bool:
    fmt = str(webhooks.get("payload_format") or "crosswatch")
    target = f"{payload_format_label(fmt)} ({_url_host(url)})"
    if not scheduler_webhook_url_allowed(url):
        _log(log_fn, f"[!] Scheduler webhook {event}: blocked unsafe URL")
        debug(f"Scheduler webhook {event}: {target} is not a public address, nothing sent")
        return False

    timeout = int(webhooks.get("timeout_seconds") or 10)
    debug(f"Scheduler webhook {event}: sending to {target}, timeout {timeout}s")
    started = time.monotonic()
    try:
        resp = requests.post(
            url,
            timeout=timeout,
            allow_redirects=False,
            **_delivery_kwargs(fmt, payload, webhooks),
        )
        elapsed_ms = int((time.monotonic() - started) * 1000)
        debug(f"Scheduler webhook {event}: {target} answered HTTP {getattr(resp, 'status_code', '?')} in {elapsed_ms} ms")
        resp.raise_for_status()
        if fmt == "pushover" and not _pushover_accepted(resp):
            _log(log_fn, f"[!] Scheduler webhook {event}: rejected by Pushover")
            return False
        _log(log_fn, f"[i] Scheduler webhook {event}: delivered")
        return True
    except Exception as exc:
        _log(log_fn, f"[!] Scheduler webhook {event}: {type(exc).__name__}")
        debug(f"Scheduler webhook {event}: {target} failed with {type(exc).__name__}")
        return False


def debug(message: str) -> None:
    try:
        BASE_LOG(message, level="DEBUG", module=LOG_MODULE)
    except Exception:
        pass


def _url_host(url: str) -> str:
    try:
        return str(urlsplit(url).hostname or "")
    except Exception:
        return ""


def _delivery_kwargs(fmt: str, payload: dict[str, Any], webhooks: dict[str, Any]) -> dict[str, Any]:
    headers = {"User-Agent": "CrossWatch scheduler webhook"}
    if fmt == "pushover":
        data = dict(payload)
        data["token"] = str(webhooks.get("pushover_token") or "")
        data["user"] = str(webhooks.get("pushover_user") or "")
        return {"data": data, "headers": headers}
    if fmt == "ntfy":
        headers["Title"] = str(payload.get("title") or "")
        headers["Priority"] = str(payload.get("priority") or NTFY_PRIORITY["success"])
        headers["Content-Type"] = "text/plain; charset=utf-8"
        return {"data": str(payload.get("message") or "").encode("utf-8"), "headers": headers}
    if fmt == "notifiarr":
        channel_id = str(webhooks.get("notifiarr_channel_id") or "").strip()
        if channel_id:
            payload.setdefault("discord", {}).setdefault("ids", {})["channel"] = int(channel_id)
    return {"json": payload, "headers": headers}


def _pushover_accepted(resp: Any) -> bool:
    try:
        status = resp.json().get("status")
        return resp.status_code == 200 and type(status) is int and status == 1
    except Exception:
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


def _notification_text(payload: dict[str, Any]) -> tuple[str, str, list[tuple[str, str]]]:
    event = str(payload.get("event") or "").strip().lower()
    status = str(payload.get("status") or event or "").strip()
    title_event = {
        "start": "started",
        "success": "succeeded",
        "failure": "failed",
    }.get(event, status or "updated")

    fields: list[tuple[str, str]] = []
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
        fields.append((title, str(value)))

    counts = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    for key in ("added", "removed", "updated", "errors", "blocked", "skipped", "unresolved"):
        value = counts.get(key) if isinstance(counts, dict) else None
        if value:
            fields.append((key.replace("_", " ").title(), str(value)))

    description = f"Scheduled sync {title_event}."
    if event == "failure":
        description = "Scheduled sync failed. Check the CrossWatch sync logs for details."
    elif event == "success":
        description = "Scheduled sync completed successfully."
    return f"CrossWatch scheduled sync {title_event}", description, fields


def _plain_message(description: str, fields: list[tuple[str, str]]) -> str:
    return "\n".join([description, *(f"{title}: {text}" for title, text in fields)])


def _push_message(fmt: str, level: str, title: str, message: str) -> dict[str, Any]:
    if fmt == "pushover":
        return {
            "title": title[:PUSHOVER_TITLE_MAX],
            "message": message[:PUSHOVER_MESSAGE_MAX],
            "priority": PUSHOVER_PRIORITY.get(level, 0),
        }
    body = message.encode("utf-8")[:NTFY_MESSAGE_MAX_BYTES]
    return {
        "title": title,
        "message": body.decode("utf-8", "ignore"),
        "priority": NTFY_PRIORITY.get(level, NTFY_PRIORITY["success"]),
    }


def _notifiarr_payload(payload: dict[str, Any], context: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    event = str(payload.get("event") or "").strip().lower()
    title, description, text_fields = _notification_text(payload)
    fields = [{"title": name, "text": text, "inline": True} for name, text in text_fields]
    run_id = str(payload.get("run_id") or context.get("run_id") or summary.get("run_id") or "")
    notification_event = run_id or f"crosswatch-{event or 'scheduler'}"
    return _notifiarr_envelope(notification_event, event, title, description, fields, "CrossWatch Scheduler")


def _notifiarr_envelope(
    notification_event: str,
    level: str,
    title: str,
    description: str,
    fields: list[dict[str, Any]],
    footer: str,
) -> dict[str, Any]:
    color = NOTIFIARR_COLORS.get(level, "7D86C9")
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
                "title": title,
                "icon": "",
                "content": "",
                "description": description,
                "fields": fields[:25],
                "footer": footer,
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


def scheduler_webhook_url_allowed(value: Any) -> bool:
    url = str(value or "").strip()
    if not url:
        return False
    try:
        parts = urlsplit(url)
        port = parts.port
    except Exception:
        return False

    if parts.scheme.lower() not in {"http", "https"} or not parts.netloc:
        return False
    if parts.username or parts.password:
        return False

    host = str(parts.hostname or "").strip()
    if not host:
        return False
    if _blocked_hostname(host):
        return False

    port = int(port or (443 if parts.scheme.lower() == "https" else 80))
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except Exception:
        return False

    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for info in infos:
        try:
            sockaddr = info[4]
            ip = ipaddress.ip_address(str(sockaddr[0]))
        except Exception:
            return False
        if not _public_ip_allowed(ip):
            return False
        addresses.add(ip)

    return bool(addresses)


def _blocked_hostname(host: str) -> bool:
    clean = host.strip().rstrip(".").lower()
    if clean in BLOCKED_HOSTNAMES:
        return True
    try:
        return not _public_ip_allowed(ipaddress.ip_address(clean))
    except ValueError:
        return False


def _public_ip_allowed(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        return _public_ip_allowed(mapped)
    if isinstance(ip, ipaddress.IPv6Address) and any(ip in network for network in EMBEDDED_IPV4_NETWORKS):
        return _public_ip_allowed(ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF))
    return bool(ip.is_global)


def _payload_format(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_")
    if text in {"notifiarr", "notifiarr_passthrough"}:
        return "notifiarr"
    if text in {"pushover", "ntfy"}:
        return text
    return "crosswatch"


def _pushover_key_or_blank(value: Any) -> str:
    text = str(value or "").strip()
    return text if len(text) == PUSHOVER_KEY_LENGTH and text.isascii() and text.isalnum() else ""


def _digits_or_blank(value: Any) -> str:
    text = str(value or "").strip()
    return text if text.isdigit() else ""


def _first_set(src: dict[str, Any], *keys: str) -> Any:
    return next((src[key] for key in keys if src.get(key) is not None), None)


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
