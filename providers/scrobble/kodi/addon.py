# providers/scrobble/kodi/addon.py
# CrossWatch - Kodi add-on event source
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import hashlib
import hmac
import json
import math
import secrets
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from cw_platform.account_match import media_account_allowed
from cw_platform.app_version import app_version
from cw_platform.config_base import CONFIG
from cw_platform.provider_instances import get_provider_block, normalize_instance_id
from cw_platform.value_coercion import coerce_bool
from providers.scrobble.currently_watching import update_from_event as _cw_update
from providers.scrobble.currently_watching import update_from_payload as _cw_update_payload
from providers.scrobble.routes import build_route_cfg_by_id
from providers.scrobble.scrobble import ScrobbleEvent, mask_account
from providers.scrobble.sources import source_enabled
from providers.sync.kodi._common import EXTERNAL_ID_KEYS, is_placeholder_id, path_scope_status, strip_path_userinfo

ADDON_FRESH_SECONDS = 900.0
TOKEN_PREFIX = "kodiwatcher:"
TOKEN_HEADER = "x-crosswatch-token"
ENDPOINT_PATH = "/webhook/kodiwatcher"
MAX_VIEWERS = 64
MIN_TOKEN_LENGTH = 16
ADDON_ID = "service.crosswatch"
PAIR_PATH = "/webhook/kodiwatcher/pair"
PAIR_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
PAIR_CODE_LENGTH = 6
PAIR_TTL_SECONDS = 600.0
PAIR_MAX_FAILURES = 10
PAIR_MAX_CLIENTS = 1024

_ACTIONS = {"start": "start", "resume": "start", "progress": "start", "pause": "pause", "stop": "stop"}
_PERSISTED = ("last_seen", "addon_version", "device_id", "device_name", "viewers", "pkc_skipped")

_STATE: dict[str, dict[str, Any]] = {}
_STATE_LOCK = threading.Lock()
_STATE_LOADED = False

_PAIR_CODES: dict[str, tuple[str, float]] = {}
_PAIR_FAILURES: dict[str, list[float]] = {}
_PAIR_LOCK = threading.Lock()


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="KODI-WATCH")
            return
        except Exception:
            pass
    try:
        print(f"[KODI-WATCH:{lvl}] {msg}")
    except Exception:
        pass


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any, limit: int = 200) -> str:
    if value is None or isinstance(value, (Mapping, list, tuple, set)):
        return ""
    return str(value).strip()[:limit]


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip())
    except Exception:
        return None
    return int(number) if math.isfinite(number) else None


def _percent(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip())
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return max(0.0, min(100.0, number))


def reported_version() -> str:
    return str(app_version() or "").strip().lstrip("vV")


def feature_enabled(cfg: Mapping[str, Any] | None) -> bool:
    runtime = _dict(_dict(cfg).get("runtime"))
    try:
        return bool(coerce_bool(runtime.get("kodi_addon")))
    except Exception:
        return False


def token_key(instance_id: Any) -> str:
    return f"{TOKEN_PREFIX}{normalize_instance_id(instance_id)}"


def _webhook_ids(cfg: Mapping[str, Any] | None) -> dict[str, Any]:
    return _dict(_dict(_dict(cfg).get("security")).get("webhook_ids"))


def _valid_token(value: Any) -> str:
    token = _text(value, 256)
    return token if len(token) >= MIN_TOKEN_LENGTH and token.isascii() else ""


def instance_token(cfg: Mapping[str, Any] | None, instance_id: Any) -> str:
    return _valid_token(_webhook_ids(cfg).get(token_key(instance_id)))


def instance_enabled(cfg: Mapping[str, Any] | None, instance_id: Any) -> bool:
    return feature_enabled(cfg) and bool(instance_token(cfg, instance_id))


def set_instance_enabled(cfg: dict[str, Any], instance_id: Any, enabled: bool, regenerate: bool = False) -> str:
    sec = cfg.setdefault("security", {})
    if not isinstance(sec, dict):
        sec = {}
        cfg["security"] = sec
    ids = sec.setdefault("webhook_ids", {})
    if not isinstance(ids, dict):
        ids = {}
        sec["webhook_ids"] = ids
    key = token_key(instance_id)
    if not enabled:
        ids.pop(key, None)
        return ""
    if regenerate or not _valid_token(ids.get(key)):
        ids[key] = secrets.token_urlsafe(24).rstrip("=")
    return str(ids[key])


def instance_for_token(cfg: Mapping[str, Any] | None, token: Any) -> str | None:
    got = _text(token, 256)
    if not got:
        return None
    found: str | None = None
    for key, value in _webhook_ids(cfg).items():
        name = str(key or "")
        expected = _valid_token(value)
        if not name.startswith(TOKEN_PREFIX) or not expected:
            continue
        if hmac.compare_digest(expected.encode("utf-8"), got.encode("utf-8")):
            found = normalize_instance_id(name[len(TOKEN_PREFIX):])
    return found


def jsonrpc_connected(cfg: Mapping[str, Any] | None, instance_id: Any) -> bool:
    block = get_provider_block(_dict(cfg), "kodi", normalize_instance_id(instance_id))
    return bool(str(block.get("server") or "").strip() and block.get("connection_verified") is True)


def source_ready(cfg: Mapping[str, Any] | None, instance_id: Any) -> bool:
    return jsonrpc_connected(cfg, instance_id) or instance_enabled(cfg, instance_id)


def clean_base_url(value: Any) -> str:
    base = _text(value, 400).rstrip("/")
    for tail in (PAIR_PATH, ENDPOINT_PATH):
        if base.lower().endswith(tail):
            base = base[: -len(tail)].rstrip("/")
    lowered = base.lower()
    if not lowered.startswith(("http://", "https://")) or not base.split("://", 1)[1]:
        return ""
    if any(ch.isspace() or ch in "?#," for ch in base):
        return ""
    return base


def plain_endpoint(base_url: Any) -> str:
    base = clean_base_url(base_url)
    return f"{base}{ENDPOINT_PATH}" if base else ""


def endpoint_url(base_url: Any, token: Any) -> str:
    base = str(base_url or "").rstrip("/")
    value = _text(token, 256)
    return f"{base}{ENDPOINT_PATH}?token={value}" if value else ""


def normalize_pair_code(value: Any) -> str:
    return "".join(ch for ch in _text(value, 64).upper() if not ch.isspace())


def _prune_pair_locked(now: float) -> None:
    for code, (_inst, expires) in list(_PAIR_CODES.items()):
        if expires <= now:
            _PAIR_CODES.pop(code, None)
    for client, stamps in list(_PAIR_FAILURES.items()):
        kept = [ts for ts in stamps if now - ts < PAIR_TTL_SECONDS]
        if kept:
            _PAIR_FAILURES[client] = kept
        else:
            _PAIR_FAILURES.pop(client, None)


def create_pair_code(instance_id: Any, *, now: float | None = None) -> tuple[str, int]:
    inst = normalize_instance_id(instance_id)
    current = float(now if now is not None else time.time())
    with _PAIR_LOCK:
        _prune_pair_locked(current)
        for code, (owner, _expires) in list(_PAIR_CODES.items()):
            if owner == inst:
                _PAIR_CODES.pop(code, None)
        code = ""
        while not code or code in _PAIR_CODES:
            code = "".join(secrets.choice(PAIR_ALPHABET) for _ in range(PAIR_CODE_LENGTH))
        _PAIR_CODES[code] = (inst, current + PAIR_TTL_SECONDS)
    return code, int(PAIR_TTL_SECONDS)


def active_pair_code(instance_id: Any, *, now: float | None = None) -> tuple[str, int]:
    inst = normalize_instance_id(instance_id)
    current = float(now if now is not None else time.time())
    with _PAIR_LOCK:
        _prune_pair_locked(current)
        for code, (owner, expires) in _PAIR_CODES.items():
            if owner == inst:
                return code, max(0, int(expires - current))
    return "", 0


def clear_pair_codes(instance_id: Any = None) -> None:
    with _PAIR_LOCK:
        if instance_id is None:
            _PAIR_CODES.clear()
            _PAIR_FAILURES.clear()
            return
        inst = normalize_instance_id(instance_id)
        for code, (owner, _expires) in list(_PAIR_CODES.items()):
            if owner == inst:
                _PAIR_CODES.pop(code, None)


def redeem_pair_code(code: Any, client: Any = "", *, now: float | None = None) -> tuple[str | None, str]:
    wanted = normalize_pair_code(code)
    who = _text(client, 80) or "-"
    current = float(now if now is not None else time.time())
    with _PAIR_LOCK:
        _prune_pair_locked(current)
        if len(_PAIR_FAILURES.get(who) or []) >= PAIR_MAX_FAILURES:
            return None, "rate_limited"
        found: str | None = None
        for known in list(_PAIR_CODES):
            if hmac.compare_digest(known.encode("utf-8"), wanted.encode("utf-8")):
                found = known
        if found is None:
            if len(_PAIR_FAILURES) >= PAIR_MAX_CLIENTS and who not in _PAIR_FAILURES:
                _PAIR_FAILURES.pop(next(iter(_PAIR_FAILURES)), None)
            _PAIR_FAILURES.setdefault(who, []).append(current)
            return None, "invalid_code"
        inst, _expires = _PAIR_CODES.pop(found)
        return inst, ""


def pair(cfg: Mapping[str, Any] | None, code: Any, base_url: Any, client: Any = "") -> tuple[int, dict[str, Any]]:
    out: dict[str, Any] = {"ok": False, "crosswatch_version": reported_version()}
    inst, reason = redeem_pair_code(code, client)
    if reason == "rate_limited":
        out["error"] = reason
        return 429, out
    token = instance_token(cfg, inst) if inst is not None and feature_enabled(cfg) else ""
    if inst is None or not token:
        out["error"] = "invalid_code"
        return 401, out
    out.update({"ok": True, "url": plain_endpoint(base_url), "token": token, "instance": _instance_name(_dict(cfg), inst)})
    return 200, out


def link_params(base_url: Any, token: Any) -> list[str]:
    return ["action=link", f"url={plain_endpoint(base_url)}", f"token={_text(token, 256)}"]


def _state_file() -> Path:
    return CONFIG / ".cw_state" / "kodi_addon.json"


def _load_state_locked() -> None:
    global _STATE_LOADED
    if _STATE_LOADED:
        return
    _STATE_LOADED = True
    try:
        data = json.loads(_state_file().read_text(encoding="utf-8"))
    except Exception:
        return
    for inst, row in _dict(data).items():
        if isinstance(row, Mapping):
            _STATE.setdefault(normalize_instance_id(inst), {}).update({k: row[k] for k in _PERSISTED if k in row})


def _save_state_locked() -> None:
    try:
        path = _state_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {inst: {k: row[k] for k in _PERSISTED if k in row} for inst, row in _STATE.items()}
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")
        tmp.replace(path)
    except Exception as exc:
        _log(f"add-on state write failed: {type(exc).__name__}", "DEBUG")


def reset_state() -> None:
    global _STATE_LOADED
    clear_pair_codes()
    with _STATE_LOCK:
        _STATE.clear()
        _STATE_LOADED = True


def forget_instance(instance_id: Any) -> None:
    clear_pair_codes(instance_id)
    with _STATE_LOCK:
        _load_state_locked()
        if _STATE.pop(normalize_instance_id(instance_id), None) is not None:
            _save_state_locked()


def clean_viewers(value: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in value if isinstance(value, (list, tuple)) else []:
        name = _text(item, 80)
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        out.append(name)
        if len(out) >= MAX_VIEWERS:
            break
    return out


def mark_seen(instance_id: Any, payload: Mapping[str, Any] | None = None, *, now: float | None = None) -> None:
    inst = normalize_instance_id(instance_id)
    body = _dict(payload)
    is_ping = _text(body.get("event")).lower() == "ping"
    device = _dict(body.get("device"))
    with _STATE_LOCK:
        _load_state_locked()
        row = _STATE.setdefault(inst, {})
        before = {k: row.get(k) for k in _PERSISTED if k != "last_seen"}
        row["last_seen"] = float(now if now is not None else time.time())
        device_id = _text(device.get("id"), 128)
        if device_id:
            row["device_id"] = device_id
        device_name = _text(device.get("name"), 80)
        if device_name:
            row["device_name"] = device_name
        version = _text(body.get("addon_version"), 40)
        if version:
            row["addon_version"] = version
        if is_ping:
            row["viewers"] = clean_viewers(body.get("viewers"))
            row["pkc_skipped"] = max(0, _int(body.get("pkc_skipped")) or 0)
        after = {k: row.get(k) for k in _PERSISTED if k != "last_seen"}
        if is_ping or before != after:
            _save_state_locked()


def snapshot(instance_id: Any) -> dict[str, Any]:
    with _STATE_LOCK:
        _load_state_locked()
        return dict(_STATE.get(normalize_instance_id(instance_id)) or {})


def is_active(instance_id: Any, *, now: float | None = None) -> bool:
    try:
        last_seen = float(snapshot(instance_id).get("last_seen") or 0.0)
    except Exception:
        return False
    current = float(now if now is not None else time.time())
    return last_seen > 0 and 0 <= current - last_seen < ADDON_FRESH_SECONDS


def device_uuid(instance_id: Any) -> str | None:
    return _text(snapshot(instance_id).get("device_id"), 128) or None


def known_viewers(instance_id: Any) -> list[str]:
    return clean_viewers(snapshot(instance_id).get("viewers"))


def status(cfg: Mapping[str, Any] | None, instance_id: Any, base_url: Any = "", *, now: float | None = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    enabled = instance_enabled(cfg, inst)
    connected = jsonrpc_connected(cfg, inst)
    snap = snapshot(inst) if enabled else {}
    current = float(now if now is not None else time.time())
    last_seen = float(snap.get("last_seen") or 0.0)
    active = enabled and is_active(inst, now=current)
    paired = enabled and last_seen > 0
    if active:
        mode = "addon"
    elif connected:
        mode = "polling"
    else:
        mode = "waiting" if enabled else "off"
    return {
        "ok": True,
        "instance": inst,
        "enabled": enabled,
        "url": endpoint_url(base_url, instance_token(cfg, inst)) if enabled else "",
        "address": clean_base_url(base_url),
        "pair_code": active_pair_code(inst, now=current)[0] if enabled else "",
        "pair_expires_in": active_pair_code(inst, now=current)[1] if enabled else 0,
        "mode": mode,
        "active": active,
        "paired": paired,
        "jsonrpc_connected": connected,
        "last_seen": int(last_seen) if last_seen else None,
        "age_seconds": int(max(0.0, current - last_seen)) if last_seen else None,
        "addon_version": _text(snap.get("addon_version"), 40),
        "device_name": _text(snap.get("device_name"), 80),
        "viewers": clean_viewers(snap.get("viewers")),
        "pkc_skipped": max(0, _int(snap.get("pkc_skipped")) or 0),
    }


def clean_ids(value: Any, media_type: str) -> dict[str, str]:
    ids: dict[str, str] = {}
    for key, raw in _dict(value).items():
        name = str(key or "").strip().lower()
        if isinstance(raw, (Mapping, list, tuple, set)) or is_placeholder_id(raw):
            continue
        base = name
        suffix = ""
        for tail in ("_show", "_episode"):
            if name.endswith(tail):
                base, suffix = name[: -len(tail)], tail
                break
        if base not in EXTERNAL_ID_KEYS:
            continue
        if media_type == "movie" and suffix:
            continue
        if media_type == "episode" and not suffix:
            suffix = "_episode"
        ids.setdefault(f"{base}{suffix}", str(raw).strip()[:64])
    return ids


def _sent_epoch(value: Any) -> int | None:
    text = _text(value, 40)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    try:
        return int(parsed.timestamp())
    except Exception:
        return None


@dataclass(frozen=True)
class AddonEvent:
    event: ScrobbleEvent
    viewers: list[str]
    deliverable: bool
    replayed: bool
    file: str


def build_event(payload: Mapping[str, Any], instance_id: Any, *, force_stop_at: float = 95.0, now: float | None = None) -> tuple[AddonEvent | None, str]:
    inst = normalize_instance_id(instance_id)
    body = _dict(payload)
    name = _text(body.get("event"), 20).lower()
    action = _ACTIONS.get(name)
    if not action:
        return None, "unsupported_event"
    media = _dict(body.get("media"))
    media_type = _text(media.get("type"), 20).lower()
    if media_type not in {"movie", "episode"}:
        return None, "unsupported_media"
    ids = clean_ids(media.get("ids"), media_type)
    if not ids:
        return None, "no_ids"
    season = _int(media.get("season")) if media_type == "episode" else None
    number = _int(media.get("episode")) if media_type == "episode" else None
    if media_type == "episode" and (season is None or number is None):
        return None, "missing_episode_number"

    position_ms = _int(media.get("position_ms"))
    duration_ms = _int(media.get("duration_ms"))
    if position_ms is not None and position_ms < 0:
        position_ms = None
    if duration_ms is not None and duration_ms <= 0:
        duration_ms = None
    percent = _percent(media.get("percent"))
    if percent is None and position_ms is not None and duration_ms:
        percent = max(0.0, min(100.0, (position_ms / float(duration_ms)) * 100.0))
    completed = media.get("completed") is True
    replayed = body.get("replayed") is True

    if percent is not None:
        deliverable = True
        progress = max(1.0, percent) if action == "start" else percent
    elif action == "stop" and completed:
        deliverable = True
        progress = 100.0
    else:
        deliverable = False
        progress = 0.0

    device = _dict(body.get("device"))
    device_id = _text(device.get("id"), 128)
    session_id = _text(body.get("session_id"), 160)
    if not session_id:
        seed = json.dumps([device_id, media_type, sorted(ids.items()), season, number], sort_keys=True)
        session_id = "auto-" + hashlib.sha1(seed.encode("utf-8", "ignore")).hexdigest()[:16]
    file_path = strip_path_userinfo(_text(media.get("file"), 2000))
    title = _text(media.get("title"), 300) or None
    episode_title = _text(media.get("episode_title"), 300) or None
    viewers = clean_viewers(body.get("viewers"))

    raw: dict[str, Any] = {
        "provider": "kodi",
        "provider_instance": inst,
        "item": {
            "type": media_type,
            "title": episode_title if media_type == "episode" else title,
            "showtitle": title if media_type == "episode" else None,
            "file": file_path or None,
        },
        "duration_ms": duration_ms,
        "_cw_preserve_stop": bool(action == "stop" and progress >= float(force_stop_at)),
        "_cw_kodi_addon": {
            "event": name,
            "event_id": _text(body.get("event_id"), 80),
            "viewers": list(viewers),
            "viewers_source": _text(body.get("viewers_source"), 20),
            "source": _text(media.get("source"), 30),
            "device_name": _text(device.get("name"), 80),
            "replayed": replayed,
        },
    }
    if replayed:
        sent = _sent_epoch(body.get("sent_at"))
        current = int(now if now is not None else time.time())
        if sent is not None and 0 < sent <= current:
            raw["_cw_watched_at"] = sent

    event = ScrobbleEvent(
        action=action,  # type: ignore[arg-type]
        media_type=media_type,  # type: ignore[arg-type]
        ids=ids,
        title=title,
        year=_int(media.get("year")),
        season=season,
        number=number,
        progress=progress,
        account=None,
        server_uuid=device_id or device_uuid(inst) or "kodi",
        session_key=f"kodi:{inst}:addon:{session_id}",
        raw=raw,
        position_ms=position_ms,
        duration_ms=duration_ms,
    )
    return AddonEvent(event=event, viewers=viewers, deliverable=deliverable, replayed=replayed, file=file_path), ""


def _watch_block(route_cfg: Mapping[str, Any] | None) -> dict[str, Any]:
    return _dict(_dict(_dict(route_cfg).get("scrobble")).get("watch"))


def route_viewers(route_cfg: Mapping[str, Any] | None, viewers: list[str]) -> list[str]:
    watch = _watch_block(route_cfg)
    if not watch.get("route_enabled", True):
        return []
    whitelist = _dict(watch.get("filters")).get("username_whitelist")
    if not whitelist:
        return [] if _text(watch.get("route_profile_id")) else list(viewers)
    return [name for name in viewers if media_account_allowed(whitelist, name, default_allow=False)]


def route_account(route_cfg: Mapping[str, Any] | None, viewers: list[str]) -> tuple[bool, str | None]:
    if not viewers:
        return True, None
    matched = route_viewers(route_cfg, viewers)
    return (True, matched[0]) if matched else (False, None)


def _group(app: Any, instance_id: Any) -> Any | None:
    groups = getattr(getattr(app, "state", None), "watch_groups", None)
    if not isinstance(groups, dict):
        return None
    inst = normalize_instance_id(instance_id)
    for group in groups.values():
        if str(getattr(group, "provider", "")).lower() == "kodi" and normalize_instance_id(getattr(group, "provider_instance", "")) == inst:
            return group
    return None


def _instance_name(cfg: Mapping[str, Any], instance_id: Any) -> str:
    inst = normalize_instance_id(instance_id)
    block = get_provider_block(_dict(cfg), "kodi", inst)
    label = _text(block.get("label"), 40)
    if label:
        return label
    return _text(snapshot(inst).get("device_name"), 80) or ("Kodi" if inst == "default" else inst)


def _route_rows(cfg: dict[str, Any], group: Any, viewers: list[str]) -> list[dict[str, Any]]:
    from cw_platform.provider_usage import provider_label

    rows: list[dict[str, Any]] = []
    for runner in list(getattr(group, "routes", None) or []):
        route_id = str(getattr(runner, "route_id", "") or "")
        route_cfg = build_route_cfg_by_id(cfg, route_id)
        watch = _watch_block(route_cfg)
        if not isinstance(route_cfg, dict) or not watch.get("route_enabled", True):
            continue
        sink = _text(watch.get("route_sink"), 40).lower()
        rows.append(
            {
                "id": route_id,
                "sink": sink,
                "label": provider_label(sink, watch.get("route_sink_instance") or "default"),
                "viewers": route_viewers(route_cfg, viewers),
            }
        )
    return rows


def _clear_card(event: ScrobbleEvent, instance_id: str) -> None:
    try:
        _cw_update_payload(
            "kodi",
            str(event.media_type),
            event.title or "",
            event.year,
            event.season,
            event.number,
            event.progress,
            True,
            state="stopped",
            clear_on_stop=True,
            ids=event.ids,
            session_key=event.session_key,
            provider_instance=instance_id,
        )
    except Exception:
        pass


def _update_card(item: AddonEvent, event: ScrobbleEvent, instance_id: str, matched: bool) -> None:
    if item.replayed:
        return
    if event.action == "stop" and not (matched and item.deliverable):
        _clear_card(event, instance_id)
        return
    if not matched:
        return
    try:
        if item.deliverable:
            _cw_update("kodi", event, duration_ms=event.duration_ms, provider_instance=instance_id)
            return
        _cw_update_payload(
            "kodi",
            str(event.media_type),
            event.title or "",
            event.year,
            event.season,
            event.number,
            0,
            False,
            duration_ms=event.duration_ms,
            state="paused" if event.action == "pause" else "playing",
            ids=event.ids,
            session_key=event.session_key,
            provider_instance=instance_id,
        )
    except Exception:
        pass


def handle(app: Any, cfg: dict[str, Any], instance_id: Any, payload: Mapping[str, Any]) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    body = _dict(payload)
    name = _text(body.get("event"), 20).lower()
    out: dict[str, Any] = {"ok": True, "ignored": False, "crosswatch_version": reported_version()}

    def ignored(reason: str) -> dict[str, Any]:
        out["ignored"] = True
        out["error"] = reason
        return out

    if name != "ping" and name not in _ACTIONS:
        return ignored("unsupported_event")
    mark_seen(inst, body)

    if name == "ping":
        out["instance"] = _instance_name(cfg, inst)
        group = _group(app, inst) if source_enabled(cfg, "watcher") else None
        out["routes"] = _route_rows(cfg, group, clean_viewers(body.get("viewers"))) if group is not None else []
        if not source_enabled(cfg, "watcher"):
            return ignored("watcher_disabled")
        return out

    if not source_enabled(cfg, "watcher"):
        return ignored("watcher_disabled")
    group = _group(app, inst)
    runners = list(getattr(group, "routes", None) or []) if group is not None else []
    if not runners:
        return ignored("no_routes")

    try:
        force_stop_at = float(_dict(_dict(cfg.get("scrobble")).get("trakt")).get("force_stop_at") or 95)
    except Exception:
        force_stop_at = 95.0
    item, reason = build_event(body, inst, force_stop_at=force_stop_at)
    if item is None:
        return ignored(reason)

    if item.file:
        allowed, scope_reason, _allowed_paths, _blocked_paths = path_scope_status(cfg, "scrobble", item.file, inst)
        if not allowed:
            return ignored(scope_reason or "outside_library_scope")

    matched_event: ScrobbleEvent | None = None
    for runner in runners:
        dispatcher = getattr(runner, "dispatcher", None)
        if dispatcher is None:
            continue
        route_cfg = build_route_cfg_by_id(cfg, str(getattr(runner, "route_id", "") or ""))
        wanted, account = route_account(route_cfg, item.viewers)
        if not wanted:
            continue
        routed = replace(item.event, account=account)
        try:
            if not dispatcher.accepts(routed):
                continue
            if matched_event is None:
                matched_event = routed
            if item.deliverable:
                dispatcher.dispatch(routed)
        except Exception as exc:
            _log(f"add-on route dispatch error: {type(exc).__name__}: {exc}", "ERROR")

    _update_card(item, matched_event or item.event, inst, matched_event is not None)
    if matched_event is None:
        return ignored("no_matching_route")
    _log(
        f"add-on {name} {item.event.media_type} user={mask_account(matched_event.account)} "
        f"p={item.event.progress:.1f} known={item.deliverable} sess={item.event.session_key}",
        "DEBUG",
    )
    return out


__all__ = [
    "ADDON_FRESH_SECONDS",
    "AddonEvent",
    "build_event",
    "clean_ids",
    "device_uuid",
    "endpoint_url",
    "feature_enabled",
    "handle",
    "instance_enabled",
    "instance_for_token",
    "instance_token",
    "is_active",
    "jsonrpc_connected",
    "known_viewers",
    "mark_seen",
    "route_viewers",
    "set_instance_enabled",
    "source_ready",
    "status",
    "token_key",
]
