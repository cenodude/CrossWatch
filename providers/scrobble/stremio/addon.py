# providers/scrobble/stremio/addon.py
# CrossWatch - Stremio add-on event source
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import hmac
import json
import math
import re
import secrets
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from cw_platform.app_version import app_version
from cw_platform.config_base import CONFIG
from cw_platform.provider_instances import get_provider_block, normalize_instance_id
from providers.scrobble.routes import normalize_routes
from providers.scrobble.sources import source_enabled
from providers.sync.stremio._common import ids_from_stremio_id, stremio_id_namespace

ADDON_ID = "app.crosswatch.scrobble"
ADDON_NAME = "CrossWatch"
TOKEN_PREFIX = "stremioaddon:"
BASE_PATH = "/webhook/stremio"
WEB_APP_URL = "https://web.stremio.com"
MANIFEST_FILE = "manifest.json"
LOGO_URL = "https://raw.githubusercontent.com/cenodude/CrossWatch/main/assets/pwa/icon-512.png"
MIN_TOKEN_LENGTH = 16
MEDIA_TYPES = ("movie", "series")
ID_PREFIXES = ("tt", "tmdb:", "tvdb:")
ID_NAMESPACES = {"imdb", "tmdb", "tvdb"}

_ACTIONS = {"start": "start", "pause": "pause", "stop": "stop"}
_PERSISTED = ("last_seen", "last_event")

_STATE: dict[str, dict[str, Any]] = {}
_STATE_LOCK = threading.Lock()
_STATE_LOADED = False


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="STREMIO-WATCH")
            return
        except Exception:
            pass
    try:
        print(f"[STREMIO-WATCH:{lvl}] {msg}")
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


def manifest_version() -> str:
    found = re.search(r"\d+\.\d+\.\d+", str(app_version() or ""))
    return found.group(0) if found else "1.0.0"


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
    return bool(instance_token(cfg, instance_id))


def source_ready(cfg: Mapping[str, Any] | None, instance_id: Any) -> bool:
    return instance_enabled(cfg, instance_id)


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
        if hmac.compare_digest(expected.encode("utf-8"), got.encode("utf-8", "ignore")):
            found = normalize_instance_id(name[len(TOKEN_PREFIX):])
    return found


def clean_base_url(value: Any) -> str:
    base = _text(value, 400).rstrip("/")
    if not base.lower().startswith(("http://", "https://")) or not base.split("://", 1)[1]:
        return ""
    if any(ch.isspace() or ch in "?#," for ch in base):
        return ""
    return base


def manifest_url(base_url: Any, token: Any) -> str:
    base = clean_base_url(base_url)
    value = _valid_token(token)
    return f"{base}{BASE_PATH}/{value}/{MANIFEST_FILE}" if base and value else ""


def install_url(base_url: Any, token: Any) -> str:
    url = manifest_url(base_url, token)
    if not url:
        return ""
    if urlsplit(url).port is not None:
        return "stremio:///addons?addon=" + quote(url, safe="")
    return "stremio://" + url.split("://", 1)[1]


def web_install_url(base_url: Any, token: Any) -> str:
    url = manifest_url(base_url, token)
    return f"{WEB_APP_URL}/#/addons?addon=" + quote(url, safe="") if url else ""


def url_installable(base_url: Any) -> bool:
    base = clean_base_url(base_url)
    if not base:
        return False
    parts = urlsplit(base)
    return parts.scheme == "https" or (parts.hostname or "") in {"127.0.0.1", "localhost"}


def redact_path(path: Any) -> str:
    text = str(path or "")
    prefix = f"{BASE_PATH}/"
    if not text.startswith(prefix):
        return text
    rest = text[len(prefix):].split("/", 1)
    return f"{prefix}***" + (f"/{rest[1]}" if len(rest) > 1 else "")


def _instance_name(cfg: Mapping[str, Any], instance_id: Any) -> str:
    inst = normalize_instance_id(instance_id)
    label = _text(get_provider_block(_dict(cfg), "stremio", inst).get("label"), 40)
    return label or ("" if inst == "default" else inst)


def manifest(cfg: Mapping[str, Any] | None, instance_id: Any) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    label = _instance_name(_dict(cfg), inst)
    slug = re.sub(r"[^a-z0-9]+", "-", inst.lower()).strip("-")
    return {
        "logo": LOGO_URL,
        "id": ADDON_ID if inst == "default" or not slug else f"{ADDON_ID}.{slug}",
        "version": manifest_version(),
        "name": f"{ADDON_NAME} ({label})" if label else ADDON_NAME,
        "description": "Sends what you play in Stremio to CrossWatch, which scrobbles it to your trackers.",
        "resources": [{"name": "player", "types": list(MEDIA_TYPES), "idPrefixes": list(ID_PREFIXES)}],
        "types": list(MEDIA_TYPES),
        "idPrefixes": list(ID_PREFIXES),
        "catalogs": [],
    }


def _state_file() -> Path:
    return CONFIG / ".cw_state" / "stremio_addon.json"


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
    with _STATE_LOCK:
        _STATE.clear()
        _STATE_LOADED = True


def forget_instance(instance_id: Any) -> None:
    with _STATE_LOCK:
        _load_state_locked()
        if _STATE.pop(normalize_instance_id(instance_id), None) is not None:
            _save_state_locked()


def mark_seen(instance_id: Any, event: Any = "", *, now: float | None = None) -> None:
    inst = normalize_instance_id(instance_id)
    with _STATE_LOCK:
        _load_state_locked()
        row = _STATE.setdefault(inst, {})
        row["last_seen"] = float(now if now is not None else time.time())
        name = _text(event, 20).lower()
        if name:
            row["last_event"] = name
        _save_state_locked()


def snapshot(instance_id: Any) -> dict[str, Any]:
    with _STATE_LOCK:
        _load_state_locked()
        return dict(_STATE.get(normalize_instance_id(instance_id)) or {})


def route_count(cfg: Mapping[str, Any] | None, instance_id: Any) -> int:
    inst = normalize_instance_id(instance_id)
    try:
        routes = normalize_routes(json.loads(json.dumps({"scrobble": _dict(_dict(cfg).get("scrobble"))}, default=str)))
    except Exception:
        return 0
    return sum(1 for r in routes if r.get("enabled") and r.get("provider") == "stremio" and r.get("provider_instance") == inst and r.get("sink"))


def status(cfg: Mapping[str, Any] | None, instance_id: Any, base_url: Any = "", *, now: float | None = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    enabled = instance_enabled(cfg, inst)
    token = instance_token(cfg, inst) if enabled else ""
    snap = snapshot(inst) if enabled else {}
    current = float(now if now is not None else time.time())
    try:
        last_seen = float(snap.get("last_seen") or 0.0)
    except Exception:
        last_seen = 0.0
    return {
        "ok": True,
        "instance": inst,
        "enabled": enabled,
        "address": clean_base_url(base_url),
        "installable": url_installable(base_url),
        "manifest_url": manifest_url(base_url, token),
        "install_url": install_url(base_url, token),
        "web_install_url": web_install_url(base_url, token),
        "installed": enabled and last_seen > 0,
        "last_seen": int(last_seen) if last_seen else None,
        "age_seconds": int(max(0.0, current - last_seen)) if last_seen else None,
        "last_event": _text(snap.get("last_event"), 20),
        "routes": route_count(cfg, inst) if enabled else 0,
        "watcher_enabled": source_enabled(cfg, "watcher"),
    }


@dataclass(frozen=True)
class PlayerEvent:
    action: str
    media_type: str
    ids: dict[str, str]
    video_id: str
    season: int | None
    number: int | None
    position_ms: int | None
    duration_ms: int | None
    session_key: str


def split_resource_path(rest: Any) -> tuple[str, str, dict[str, str]]:
    text = str(rest or "").strip("/")
    if text.lower().endswith(".json"):
        text = text[:-5]
    parts = text.split("/")
    if len(parts) < 2:
        return "", "", {}
    args = {str(k): str(v[-1]) for k, v in parse_qs("/".join(parts[2:]), keep_blank_values=True).items() if v}
    return _text(parts[0], 20).lower(), _text(parts[1], 160), args


def _native_ids(value: str, record_type: str) -> dict[str, str]:
    if stremio_id_namespace(value) not in ID_NAMESPACES:
        return {}
    return {k: str(v) for k, v in ids_from_stremio_id(value, record_type).items() if str(v or "").strip()}


def build_event(media_type: Any, video_id: Any, args: Mapping[str, Any] | None, instance_id: Any) -> tuple[PlayerEvent | None, str]:
    inst = normalize_instance_id(instance_id)
    params = _dict(args)
    action = _ACTIONS.get(_text(params.get("action"), 20).lower())
    if not action:
        return None, "unsupported_event"
    kind = _text(media_type, 20).lower()
    vid = _text(video_id, 160)
    season: int | None = None
    number: int | None = None
    if kind == "movie":
        ids = _native_ids(vid, "movie")
        out_type = "movie"
    elif kind == "series":
        parts = vid.rsplit(":", 2)
        if len(parts) != 3:
            return None, "missing_episode_number"
        season, number = _int(parts[1]), _int(parts[2])
        if season is None or number is None or season < 0 or number <= 0:
            return None, "missing_episode_number"
        ids = {f"{k}_show": v for k, v in _native_ids(parts[0], "series").items()}
        out_type = "episode"
    else:
        return None, "unsupported_media"
    if not ids:
        return None, "no_ids"

    position_ms = _int(params.get("currentTime"))
    duration_ms = _int(params.get("duration"))
    if position_ms is not None and position_ms < 0:
        position_ms = None
    if duration_ms is not None and duration_ms <= 0:
        duration_ms = None
    return (
        PlayerEvent(
            action=action,
            media_type=out_type,
            ids=ids,
            video_id=vid,
            season=season,
            number=number,
            position_ms=position_ms,
            duration_ms=duration_ms,
            session_key=f"stremio:{inst}:addon:{vid}",
        ),
        "",
    )


def _group(app: Any, instance_id: Any) -> Any | None:
    groups = getattr(getattr(app, "state", None), "watch_groups", None)
    if not isinstance(groups, dict):
        return None
    inst = normalize_instance_id(instance_id)
    for group in groups.values():
        if str(getattr(group, "provider", "")).lower() == "stremio" and normalize_instance_id(getattr(group, "provider_instance", "")) == inst:
            return group
    return None


def handle(app: Any, cfg: dict[str, Any], instance_id: Any, media_type: Any, video_id: Any, args: Mapping[str, Any] | None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    out: dict[str, Any] = {"success": True}

    action = _text(_dict(args).get("action"), 20).lower() or "-"
    label = f"{_text(media_type, 20) or '-'} {_text(video_id, 160) or '-'} at={_text(_dict(args).get('currentTime'), 20) or '-'} of={_text(_dict(args).get('duration'), 20) or '-'}"

    def ignored(reason: str) -> dict[str, Any]:
        out["success"] = False
        out["ignored"] = reason
        _log(f"add-on {action} {label} ignored; inst={inst} reason={reason}", "INFO")
        return out

    item, reason = build_event(media_type, video_id, args, inst)
    mark_seen(inst, item.action if item is not None else "")
    if item is None:
        return ignored(reason)
    if not source_enabled(cfg, "watcher"):
        return ignored("watcher_disabled")
    ingest: Any = getattr(getattr(_group(app, inst), "watcher", None), "ingest", None)
    if not callable(ingest):
        return ignored("no_routes")
    result: Any = ingest(item)
    accepted, why = (bool(result[0]), str(result[1] or "")) if isinstance(result, tuple) and len(result) == 2 else (bool(result), "")
    if not accepted:
        return ignored(why or "no_matching_route")
    _log(f"add-on {action} {label} accepted; inst={inst}", "INFO")
    return out


__all__ = [
    "ADDON_ID",
    "BASE_PATH",
    "PlayerEvent",
    "build_event",
    "forget_instance",
    "handle",
    "install_url",
    "instance_enabled",
    "instance_for_token",
    "instance_token",
    "manifest",
    "manifest_url",
    "mark_seen",
    "redact_path",
    "set_instance_enabled",
    "source_ready",
    "split_resource_path",
    "status",
    "token_key",
]
