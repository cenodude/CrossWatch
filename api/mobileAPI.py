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
from fastapi.responses import FileResponse, JSONResponse, Response

from cw_platform.config_base import load_config, save_config
from services.activity import list_events
from services.backups import create_backup

from . import appAuthAPI as app_auth
from .versionAPI import CURRENT_VERSION

router = APIRouter(prefix="/api/mobile", tags=["mobile"])

DEFAULT_SCOPES = ["read", "actions", "diagnostics", "safe-config"]
PROVIDER_ORDER = ["ANILIST", "EMBY", "JELLYFIN", "MDBLIST", "PLEX", "PUBLICMETADB", "SIMKL", "TMDB", "TRAKT", "TAUTULLI", "CROSSWATCH"]
PROVIDER_LABELS = {
    "ANILIST": "AniList",
    "EMBY": "Emby",
    "JELLYFIN": "Jellyfin",
    "MDBLIST": "MDBList",
    "PLEX": "Plex",
    "PUBLICMETADB": "PublicMetaDB",
    "SIMKL": "SIMKL",
    "TMDB": "TMDb",
    "TRAKT": "Trakt",
    "TAUTULLI": "Tautulli",
    "CROSSWATCH": "CrossWatch",
}
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


def _web_auth_ok(request: Request, cfg: dict[str, Any]) -> bool:
    if not app_auth.auth_required(cfg):
        return False
    token = request.cookies.get(app_auth.COOKIE_NAME)
    if not app_auth.is_authenticated(cfg, token):
        return False
    try:
        return bool(app_auth._origin_allowed(request))  # type: ignore[attr-defined]
    except Exception:
        return False


def _require_mobile_scope(request: Request, scope: str) -> tuple[dict[str, Any], dict[str, Any] | None]:
    cfg = load_config() or {}
    block = _cfg_mobile(cfg)
    _prune_mobile(block)
    if not _mobile_auth_required(cfg):
        return cfg, None

    device = _find_device_for_token(cfg, _bearer_token(request))
    if device is None:
        if str(scope or "").strip().lower() == "read" and _web_auth_ok(request, cfg):
            return cfg, None
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


def _has_value(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, str):
        return bool(value.strip()) and value.strip().lower() not in {"false", "none", "null"}
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _provider_block(cfg: dict[str, Any], provider: str) -> dict[str, Any]:
    key = provider.lower()
    for cand in (key, provider, provider.upper()):
        node = cfg.get(cand)
        if isinstance(node, dict):
            return node
    return {}


def _profile_configured(provider: str, block: dict[str, Any], cfg: dict[str, Any]) -> bool:
    p = provider.lower()
    b = block if isinstance(block, dict) else {}
    if p == "plex":
        return any(_has_value(b.get(k)) for k in ("account_token", "token", "access_token"))
    if p in {"emby", "jellyfin"}:
        return any(_has_value(b.get(k)) for k in ("access_token", "api_key", "token"))
    if p in {"trakt", "simkl"}:
        return any(_has_value(b.get(k)) for k in ("access_token", "refresh_token"))
    if p == "anilist":
        return any(_has_value(b.get(k)) for k in ("access_token", "token"))
    if p == "mdblist":
        return any(_has_value(b.get(k)) for k in ("api_key", "access_token"))
    if p == "tautulli":
        tb = b or cfg.get("tautulli") or (cfg.get("auth") or {}).get("tautulli") or {}
        return any(_has_value(tb.get(k)) for k in ("api_key", "server_url", "server"))
    if p == "tmdb":
        return _has_value(b.get("api_key")) and _has_value(b.get("session_id") or b.get("session"))
    if p == "crosswatch":
        return b.get("enabled") is not False
    return any(_has_value(b.get(k)) for k in ("access_token", "api_key", "token"))


def _configured_provider_profiles(cfg: dict[str, Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for key in PROVIDER_ORDER:
        p = key.lower()
        base = _provider_block(cfg, p)
        count = 1 if _profile_configured(p, base, cfg) else 0
        insts = base.get("instances")
        if isinstance(insts, dict):
            for block in insts.values():
                if isinstance(block, dict) and _profile_configured(p, block, cfg):
                    count += 1
        if not count and p == "tmdb":
            extra = cfg.get("tmdb_sync") or (cfg.get("auth") or {}).get("tmdb_sync") or {}
            if isinstance(extra, dict) and _profile_configured("tmdb", extra, cfg):
                count = 1
        if count:
            out[key] = count
    return out


def _active_pair_providers(cfg: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    raw_pairs = cfg.get("pairs") or cfg.get("connections") or []
    pairs: list[Any] = raw_pairs if isinstance(raw_pairs, list) else []
    for pair in pairs:
        if not isinstance(pair, dict) or pair.get("enabled") is False:
            continue
        for key in ("source", "target", "src", "dst", "from", "to"):
            value = pair.get(key)
            if isinstance(value, str) and value.strip():
                out.add(value.strip().upper())
            elif isinstance(value, dict):
                provider = str(value.get("provider") or value.get("name") or "").strip()
                if provider:
                    out.add(provider.upper())
    return out


def _iter_feature_items(node: Any) -> list[dict[str, Any]]:
    if not isinstance(node, dict):
        return []
    for container in (node.get("checkpoint"), node.get("baseline"), node.get("present"), node):
        if not isinstance(container, dict):
            continue
        items = container.get("items")
        if isinstance(items, dict):
            return [v for v in items.values() if isinstance(v, dict)]
        if isinstance(items, list):
            return [v for v in items if isinstance(v, dict)]
    return []


def _count_feature(node: Any, feature: str = "watchlist") -> int:
    try:
        if isinstance(node, dict):
            if feature in {"history", "ratings"}:
                return len(_iter_feature_items(node))
            for container in (node.get("checkpoint"), node.get("baseline"), node.get("present"), node):
                if not isinstance(container, dict):
                    continue
                items = container.get("items")
                if isinstance(items, (dict, list)):
                    return len(items)
                if isinstance(items, (int, str)):
                    return int(items)
        if isinstance(node, list):
            return len(node)
        if isinstance(node, (int, str)):
            return int(node)
    except Exception:
        return 0
    return 0


def _media_type(item: dict[str, Any]) -> str:
    raw = str(item.get("media_type") or item.get("type") or item.get("kind") or "").lower()
    if "anime" in raw:
        return "anime"
    if raw in {"episode", "season"}:
        return "shows"
    if raw in {"show", "series", "tv"}:
        return "shows"
    if raw == "movie":
        return "movies"
    title = str(item.get("series_title") or item.get("show_title") or item.get("grandparentTitle") or "")
    if title:
        return "shows"
    return "movies"


def _feature_breakdown(node: Any) -> dict[str, int]:
    out = {"movies": 0, "shows": 0, "anime": 0}
    for item in _iter_feature_items(node):
        key = _media_type(item)
        if key in out:
            out[key] += 1
    return out


def _provider_feature_node(state: dict[str, Any], provider: str, feature: str) -> Any:
    providers = state.get("providers")
    if not isinstance(providers, dict):
        return {}
    node = providers.get(provider) or providers.get(provider.upper()) or providers.get(provider.lower()) or {}
    return node.get(feature) if isinstance(node, dict) else {}


def _providers_payload(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    configured = _configured_provider_profiles(cfg)
    active = _active_pair_providers(cfg)
    try:
        from .syncAPI import _load_state

        state = _load_state() or {}
    except Exception:
        state = {}

    out: list[dict[str, Any]] = []
    for key in PROVIDER_ORDER:
        if key not in configured:
            continue
        feature = "watchlist"
        node = _provider_feature_node(state, key, feature)
        count = _count_feature(node, feature)
        breakdown = _feature_breakdown(node)
        live = key in active or key.lower() in {p.lower() for p in active}
        out.append(
            {
                "key": key,
                "name": PROVIDER_LABELS.get(key, key.title()),
                "label": PROVIDER_LABELS.get(key, key.title()),
                "status": "Live" if live else "Idle",
                "healthy": live,
                "configured": True,
                "profiles": configured.get(key, 1),
                "feature": feature,
                "count": count,
                "breakdown": breakdown,
            }
        )
    return out


def _activity_payload() -> tuple[list[dict[str, Any]], int]:
    try:
        from services.dashboard_widgets import recent_scrobble_widget

        payload = recent_scrobble_widget(limit=12) or {}
        raw = payload.get("items") or []
    except Exception:
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
        method = str(event.get("method") or "").strip().lower()
        detail = str(event.get("detail") or event.get("message") or event.get("route") or "")
        ts_raw = event.get("sort_epoch") or event.get("captured_at") or event.get("watched_at") or event.get("ts") or event.get("time") or event.get("created_at") or 0
        ts = int(ts_raw) if str(ts_raw or "0").isdigit() else 0
        ago = _ago(ts) if ts else ""
        episode_label = str(event.get("episode_label") or "").strip()
        label_parts = []
        if method:
            label_parts.append(method.title())
        if episode_label:
            label_parts.append(episode_label)
        if ago:
            label_parts.append(ago)
        display_detail = " - ".join(label_parts) or detail
        items.append(
            {
                "title": title,
                "detail": display_detail,
                "time": ago,
                "level": "WARN" if level == "WARNING" else level,
                "poster": _mobile_activity_art_path(event),
                "episode_label": episode_label,
                "sources": event.get("sources") if isinstance(event.get("sources"), list) else [],
                "method": method,
            }
        )
    return items, warnings


def _mobile_activity_art_path(event: dict[str, Any]) -> str:
    raw = str(event.get("poster") or "").strip()
    if not raw.startswith("/art/tmdb/"):
        return raw
    season = _int_or_none(event.get("season"))
    episode = _int_or_none(event.get("episode"))
    media_type = str(event.get("type") or event.get("media_type") or "").strip().lower()
    if media_type == "episode" and season is not None and episode is not None:
        base = raw.split("?", 1)[0]
        return f"/api/mobile{base}?kind=still&season={season}&episode={episode}&size=w300"
    return "/api/mobile" + raw


def _ago(ts: int) -> str:
    delta = max(0, int(time.time()) - int(ts))
    if delta < 60:
        return "now"
    if delta < 3600:
        return f"{delta // 60}m ago"
    if delta < 86400:
        return f"{delta // 3600}h ago"
    return f"{delta // 86400}d ago"


def _format_epoch(ts: int) -> str:
    if not ts:
        return "Not scheduled"
    try:
        return time.strftime("%d %b %H:%M", time.localtime(int(ts)))
    except Exception:
        return "Scheduled"


def _format_ms(value: Any) -> str:
    try:
        total = max(0, int(float(value or 0)) // 1000)
    except Exception:
        return ""
    if not total:
        return ""
    hours = total // 3600
    minutes = (total % 3600) // 60
    seconds = total % 60
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _source_label(value: Any) -> str:
    raw = str(value or "").strip().lower()
    labels = {
        "plex": "Plex",
        "plextrakt": "Plex webhook",
        "emby": "Emby",
        "embytrakt": "Emby webhook",
        "jellyfin": "Jellyfin",
        "jellyfintrakt": "Jellyfin webhook",
    }
    return labels.get(raw, raw.title() if raw else "")


def _dict_or_empty(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except Exception:
        return None


def _tmdb_id(item: dict[str, Any]) -> str:
    ids = _dict_or_empty(item.get("ids"))
    media_type = str(item.get("media_type") or item.get("type") or "").lower()
    keys = ("tmdb", "tmdb_id", "tmdb_show") if media_type == "movie" else ("tmdb_show", "tmdb", "tmdb_id")
    for key in keys:
        value = item.get(key) if key in item else ids.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _tmdb_id_from_metadata(cfg: dict[str, Any] | None, item: dict[str, Any]) -> str:
    if not isinstance(cfg, dict):
        return ""
    tmdb_cfg = _dict_or_empty(cfg.get("tmdb"))
    metadata_cfg = _dict_or_empty(cfg.get("metadata"))
    if not str(tmdb_cfg.get("api_key") or metadata_cfg.get("tmdb_api_key") or "").strip():
        return ""
    title = str(item.get("title") or item.get("grandparentTitle") or item.get("name") or "").strip()
    if not title:
        return ""
    try:
        from providers.metadata._meta_TMDB import TmdbProvider

        ids_raw = _dict_or_empty(item.get("ids"))
        ids = {k: str(v) for k, v in ids_raw.items() if v is not None and str(v).strip()}
        if "imdb" not in ids and ids.get("imdb_show"):
            ids["imdb"] = ids["imdb_show"]
        ids["title"] = title
        if item.get("year"):
            ids["year"] = str(item.get("year"))
        media_type = str(item.get("media_type") or item.get("type") or "").lower()
        entity = "movie" if media_type == "movie" else "tv"
        meta = TmdbProvider(load_config, save_config).fetch(entity=entity, ids=ids, need={"poster": False, "backdrop": False})
        meta_ids = _dict_or_empty(meta.get("ids")) if isinstance(meta, dict) else {}
        return str(meta_ids.get("tmdb") or "").strip()
    except Exception:
        return ""


def _mobile_usable_cover(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    lower = raw.lower()
    if lower.startswith(("http://", "https://")):
        return raw
    if lower.startswith(("/api/mobile/", "/api/", "/art/", "/assets/img/")):
        return raw
    return ""


def _art_url(item: dict[str, Any], cfg: dict[str, Any] | None = None, *, kind: str = "poster", size: str = "w342") -> str:
    cover = str(item.get("cover") or item.get("poster") or item.get("thumb") or "").strip()
    art_kind = "backdrop" if str(kind or "").strip().lower() == "backdrop" else "poster"
    usable_cover = _mobile_usable_cover(cover) if art_kind == "poster" else ""
    if usable_cover:
        return usable_cover
    tmdb = _tmdb_id(item) or _tmdb_id_from_metadata(cfg, item)
    if not tmdb:
        return "" if art_kind == "backdrop" else "/assets/img/placeholder_poster.svg"
    media_type = str(item.get("media_type") or item.get("type") or "").lower()
    tmdb_type = "movie" if media_type == "movie" else "tv"
    suffix = f"&kind={art_kind}" if art_kind != "poster" else ""
    return f"/api/mobile/art/tmdb/{tmdb_type}/{tmdb}?size={size}{suffix}"


def _watching_payload(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    try:
        from .scrobbleAPI import api_currently_watching

        payload = _as_dict(api_currently_watching())
        data = payload.get("currently_watching")
        if isinstance(data, dict):
            title = str(data.get("title") or data.get("grandparentTitle") or data.get("name") or "").strip()
            if title:
                state = str(data.get("state") or "").strip()
                media_type = str(data.get("media_type") or data.get("type") or "").lower()
                season = data.get("season")
                episode = data.get("episode")
                subtitle_parts: list[str] = []
                if media_type == "episode" and (season or episode):
                    try:
                        season_num = _int_or_none(season)
                        episode_num = _int_or_none(episode)
                        if season_num is not None and episode_num is not None:
                            subtitle_parts.append(f"S{season_num:02d}E{episode_num:02d}")
                    except Exception:
                        pass
                source = _source_label(data.get("source"))
                if source:
                    subtitle_parts.append(source)
                progress = int(float(data.get("progress") or 0))
                duration_ms = data.get("duration_ms")
                position_ms = int((progress / 100) * int(duration_ms or 0)) if duration_ms and progress else 0
                label = f"{title} ({state})" if state else title
                progress_label = f"{max(0, min(100, progress))}% watched" if progress else "Progress unavailable"
                return {
                    "active": True,
                    "title": title,
                    "subtitle": " | ".join(subtitle_parts),
                    "source": source,
                    "state": state.title() if state else "Playing",
                    "media_type": media_type,
                    "progress": max(0, min(100, progress)),
                    "progress_label": progress_label,
                    "position_ms": position_ms,
                    "duration_ms": int(duration_ms or 0),
                    "position": _format_ms(position_ms),
                    "duration": _format_ms(duration_ms),
                    "poster": _art_url(data, cfg),
                    "backdrop": _art_url(data, cfg, kind="backdrop", size="w780"),
                    "streams_count": int(payload.get("streams_count") or 0),
                    "updated": int(data.get("updated") or 0),
                    "label": label,
                }
    except Exception:
        pass
    return {
        "active": False,
        "title": "Nothing playing",
        "subtitle": "No active watcher session",
        "source": "",
        "state": "Idle",
        "media_type": "",
        "progress": 0,
        "progress_label": "Progress unavailable",
        "position_ms": 0,
        "duration_ms": 0,
        "position": "",
        "duration": "",
        "poster": "/assets/img/placeholder_poster.svg",
        "backdrop": "",
        "streams_count": 0,
        "updated": 0,
        "label": "Nothing playing",
    }


def _watching_label() -> str:
    return str(_watching_payload().get("label") or "Nothing playing")


def _scheduler_label() -> tuple[str, str]:
    try:
        from .schedulingAPI import sched_status

        payload = sched_status() or {}
        config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
        enabled = bool((config or {}).get("enabled") or ((config or {}).get("advanced") or {}).get("enabled"))
        next_run = int(payload.get("next_run_at") or 0)
        return ("Enabled" if enabled else "Disabled", _format_epoch(next_run))
    except Exception:
        return ("Unknown", "Not scheduled")


def _scrobble_source_labels(cfg: dict[str, Any]) -> tuple[str, str]:
    try:
        from providers.scrobble.sources import source_enabled

        watcher = "Enabled" if source_enabled(cfg, "watcher") else "Disabled"
        webhook = "Enabled" if source_enabled(cfg, "webhook") else "Disabled"
        return watcher, webhook
    except Exception:
        return "Unknown", "Unknown"


def _status_cards(sync_running: bool, scheduler: str, watcher: str, webhook: str, next_run: str) -> list[dict[str, Any]]:
    return [
        {"label": "Sync", "value": "Running" if sync_running else "Idle", "tone": "good" if sync_running else "idle"},
        {"label": "Scheduler", "value": scheduler, "tone": "good" if scheduler == "Enabled" else "idle"},
        {"label": "Watcher", "value": watcher, "tone": "good" if watcher == "Enabled" else "idle"},
        {"label": "Webhook", "value": webhook, "tone": "good" if webhook == "Enabled" else "idle"},
        {"label": "Next run", "value": next_run, "tone": "info" if next_run != "Not scheduled" else "idle"},
    ]


def _tmdb_api_key(cfg: dict[str, Any]) -> str:
    tmdb = _dict_or_empty(cfg.get("tmdb"))
    metadata = _dict_or_empty(cfg.get("metadata"))
    return str(tmdb.get("api_key") or metadata.get("tmdb_api_key") or "").strip()


def _mime_for_art_url(url: str, content_type: str = "") -> str:
    ct = str(content_type or "").split(";", 1)[0].strip().lower()
    if ct.startswith("image/"):
        return ct
    clean = str(url or "").split("?", 1)[0].lower()
    if clean.endswith((".jpg", ".jpeg")):
        return "image/jpeg"
    if clean.endswith(".png"):
        return "image/png"
    if clean.endswith(".webp"):
        return "image/webp"
    return "image/jpeg"


def _mobile_tmdb_art_response(
    cfg: dict[str, Any],
    typ: str,
    tmdb_id: int,
    size: str,
    kind: str,
    season: int | None = None,
    episode: int | None = None,
) -> Response:
    from . import metaAPI

    api_key = _tmdb_api_key(cfg)
    if not api_key:
        return Response("TMDb key missing", status_code=404, media_type="text/plain")

    media_type = str(typ or "").strip().lower()
    if media_type == "show":
        media_type = "tv"
    if media_type not in {"movie", "tv"}:
        return Response("Bad type", status_code=400, media_type="text/plain")

    raw_kind = str(kind or "").strip().lower()
    art_kind = "still" if raw_kind in {"still", "episode_still"} else "backdrop" if raw_kind == "backdrop" else "poster"
    try:
        size_tag = metaAPI._sanitize_tmdb_size(size)  # type: ignore[attr-defined]
    except Exception:
        return Response("Invalid size", status_code=400, media_type="text/plain")

    cache_error = ""
    try:
        _, cache_dir, _ = metaAPI._env()  # type: ignore[attr-defined]
        if art_kind == "still":
            if media_type != "tv" or season is None or episode is None:
                return Response("Episode still requires tv, season and episode", status_code=400, media_type="text/plain")
            local_path, mime = metaAPI.get_episode_still_file(api_key, int(tmdb_id), int(season), int(episode), size_tag, cache_dir)
        else:
            local_path, mime = metaAPI.get_art_file(api_key, media_type, int(tmdb_id), size_tag, cache_dir, kind=art_kind)
        if not metaAPI._is_placeholder_art(local_path):  # type: ignore[attr-defined]
            return FileResponse(
                str(local_path),
                media_type=mime,
                headers={
                    "Cache-Control": "public, max-age=86400, stale-while-revalidate=86400",
                    "X-CrossWatch-Mobile-Art": "cache",
                },
            )
    except Exception as exc:
        cache_error = str(exc)[:160]

    try:
        images: list[dict[str, Any]] = []
        if art_kind == "still":
            return _mobile_tmdb_art_response(cfg, typ=typ, tmdb_id=tmdb_id, size=size, kind="poster")
        if art_kind == "poster":
            images = metaAPI._tmdb_fetch_posters(api_key, media_type, str(tmdb_id), metaAPI._cfg_ui_locale())  # type: ignore[attr-defined]
        else:
            _, cache_dir, _ = metaAPI._env()  # type: ignore[attr-defined]
            meta = metaAPI.get_meta(api_key, media_type, str(tmdb_id), cache_dir, need={art_kind: True}) or {}
            images = metaAPI._art_candidates(meta, art_kind)  # type: ignore[attr-defined]
        best = metaAPI._pick_best_image(images, metaAPI._cfg_ui_locale())  # type: ignore[attr-defined]
        src_url = metaAPI._tmdb_size_url(best or {}, size_tag) if best else ""  # type: ignore[attr-defined]
        if src_url:
            import requests

            upstream = requests.get(str(src_url), timeout=20)
            if upstream.ok and upstream.content:
                return Response(
                    content=upstream.content,
                    media_type=_mime_for_art_url(str(src_url), upstream.headers.get("content-type", "")),
                    headers={
                        "Cache-Control": "public, max-age=86400, stale-while-revalidate=86400",
                        "X-CrossWatch-Mobile-Art": "tmdb-proxy",
                    },
                )
    except Exception as exc:
        cache_error = cache_error or str(exc)[:160]

    headers = {"Cache-Control": "no-store"}
    if cache_error:
        headers["X-CrossWatch-Mobile-Art-Error"] = cache_error
    return Response("Art not available", status_code=404, media_type="text/plain", headers=headers)


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
    watcher, webhook = _scrobble_source_labels(cfg)
    now_playing = _watching_payload(cfg)
    sync_running = False
    try:
        from .syncAPI import _is_sync_running

        sync_running = bool(_is_sync_running())
    except Exception:
        pass
    payload = {
        "server_name": "CrossWatch",
        "version": CURRENT_VERSION,
        "sync_running": sync_running,
        "sync_state": "Running" if sync_running else "Idle",
        "scheduler": scheduler,
        "watcher": watcher,
        "webhook": webhook,
        "next_run": next_run,
        "status_cards": _status_cards(sync_running, scheduler, watcher, webhook, next_run),
        "currently_watching": str(now_playing.get("label") or "Nothing playing"),
        "now_playing": now_playing,
        "warnings": warnings,
        "providers": _providers_payload(cfg),
        "activity": activity,
        "library": _library_payload(),
    }
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@router.get("/art/tmdb/{typ}/{tmdb_id}")
def mobile_tmdb_art(
    request: Request,
    typ: str,
    tmdb_id: int,
    size: str = "w342",
    kind: str = "poster",
    season: int | None = None,
    episode: int | None = None,
):
    cfg, _device = _require_mobile_scope(request, "read")
    return _mobile_tmdb_art_response(cfg, typ=typ, tmdb_id=tmdb_id, size=size, kind=kind, season=season, episode=episode)


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
