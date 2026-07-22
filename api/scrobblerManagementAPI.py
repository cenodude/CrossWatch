# /api/scrobblerManagementAPI.py
# CrossWatch - Scrobbler Management API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import list_instance_ids, normalize_instance_id
from cw_platform.provider_usage import WEBHOOK_SOURCE_PROVIDERS, provider_label, webhook_source_enabled
from providers.scrobble.routes import (
    ROUTE_PROVIDERS,
    ROUTE_SINKS,
    normalize_route,
    normalize_route_options,
    normalize_routes,
)
from providers.scrobble.sources import legacy_mode_for_sources, scrobble_sources
from providers.webhooks.config import (
    media_source_connected,
    sink_configured,
    webhook_settings,
    webhook_sink_instance,
    webhook_sinks,
)

from .configAPI import _after_config_save, _apply_watch_runtime, _watch_runtime_changed, _watcher_running
from .scrobbleAPI import _ensure_media_profile_webhook_ids, _ensure_route_ratings_webhook_ids, _gen_webhook_id, debug_watch_status


router = APIRouter(prefix="/api/scrobbler", tags=["scrobbler-management"])

SOURCE_PROVIDERS = tuple(WEBHOOK_SOURCE_PROVIDERS)
SINK_PROVIDERS = tuple(sorted(ROUTE_SINKS))
ALLOWED_FILTER_KEYS = {
    "username_whitelist",
    "server_uuid",
    "server_uuid_whitelist",
    "server_uuid_blacklist",
    "ignore_live_tv_dvr",
}
WEBHOOK_SETTING_KEYS = {
    "enabled",
    "sinks",
    "sink_instances",
    "filters_plex",
    "filters_jellyfin",
    "filters_emby",
    "plex_trakt_ratings",
    "plex_simkl_ratings",
    "plex_mdblist_ratings",
    "pause_debounce_seconds",
    "suppress_start_at",
}


class ValidationFailure(Exception):
    def __init__(self, errors: list[dict[str, str]]) -> None:
        self.errors = errors
        super().__init__("validation_failed")


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _clone(value: Any) -> Any:
    return copy.deepcopy(value)



def _validation_response(exc: ValidationFailure) -> JSONResponse:
    return JSONResponse({"ok": False, "error": "validation_failed", "errors": exc.errors}, status_code=400)


def _err(field: str, code: str, message: str) -> dict[str, str]:
    return {"field": field, "code": code, "message": message}


def _source_provider(provider: Any, field: str = "provider") -> str:
    value = str(provider or "").strip().lower()
    if value not in SOURCE_PROVIDERS:
        raise ValidationFailure([_err(field, "unsupported_provider", "Unsupported source provider")])
    return value


def _sink_provider(provider: Any, field: str = "sink") -> str:
    value = str(provider or "").strip().lower()
    if value not in ROUTE_SINKS:
        raise ValidationFailure([_err(field, "unsupported_provider", "Unsupported destination provider")])
    return value


def _instances(cfg: Mapping[str, Any], provider: str) -> list[str]:
    try:
        return [normalize_instance_id(x) for x in list_instance_ids(cfg, provider)]
    except Exception:
        return ["default"]


def _profile_exists(cfg: Mapping[str, Any], provider: str, instance: str) -> bool:
    return normalize_instance_id(instance) in set(_instances(cfg, provider))


def _require_source_profile(cfg: Mapping[str, Any], provider: str, instance: str) -> None:
    errors: list[dict[str, str]] = []
    if not _profile_exists(cfg, provider, instance):
        errors.append(_err("provider_instance", "profile_not_found", "Source profile does not exist"))
    elif not media_source_connected(cfg, provider, instance):
        errors.append(_err("provider_instance", "profile_not_configured", "Source profile is not configured"))
    if errors:
        raise ValidationFailure(errors)


def _require_sink_profile(cfg: Mapping[str, Any], sink: str, instance: str, field: str = "sink_instance") -> None:
    errors: list[dict[str, str]] = []
    if not _profile_exists(cfg, sink, instance):
        errors.append(_err(field, "profile_not_found", "Destination profile does not exist"))
    elif not sink_configured(cfg, sink, instance):
        errors.append(_err(field, "profile_not_configured", "Destination profile is not configured"))
    if errors:
        raise ValidationFailure(errors)


def _profile_override(cfg: Mapping[str, Any], provider: str, instance: str) -> dict[str, Any]:
    sc = _dict(_dict(cfg).get("scrobble"))
    wh = _dict(sc.get("webhook"))
    profiles = _dict(wh.get("profiles"))
    return _dict(_dict(profiles.get(provider)).get(normalize_instance_id(instance)))


def _profile_override_node(cfg: dict[str, Any], provider: str, instance: str) -> dict[str, Any]:
    sc = cfg.setdefault("scrobble", {})
    if not isinstance(sc, dict):
        sc = {}
        cfg["scrobble"] = sc
    wh = sc.setdefault("webhook", {})
    if not isinstance(wh, dict):
        wh = {}
        sc["webhook"] = wh
    profiles = wh.setdefault("profiles", {})
    if not isinstance(profiles, dict):
        profiles = {}
        wh["profiles"] = profiles
    provider_profiles = profiles.setdefault(provider, {})
    if not isinstance(provider_profiles, dict):
        provider_profiles = {}
        profiles[provider] = provider_profiles
    node = provider_profiles.setdefault(normalize_instance_id(instance), {})
    if not isinstance(node, dict):
        node = {}
        provider_profiles[normalize_instance_id(instance)] = node
    return node


def _profile_override_remove(cfg: dict[str, Any], provider: str, instance: str) -> bool:
    sc = _dict(cfg.get("scrobble"))
    wh = _dict(sc.get("webhook"))
    profiles = wh.get("profiles")
    if not isinstance(profiles, dict):
        return False
    provider_profiles = profiles.get(provider)
    if not isinstance(provider_profiles, dict):
        return False
    inst = normalize_instance_id(instance)
    existed = inst in provider_profiles
    provider_profiles.pop(inst, None)
    if not provider_profiles:
        profiles.pop(provider, None)
    return existed


def _as_list(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        items = [k for k, v in value.items() if v not in (False, None, "")]
    else:
        items = value if isinstance(value, (list, tuple, set)) else str(value or "").split(",")
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _coerce_int(value: Any, field: str) -> int:
    if value is None:
        raise ValidationFailure([_err(field, "invalid_number", "Value must be a number")])
    try:
        return int(value)
    except Exception:
        raise ValidationFailure([_err(field, "invalid_number", "Value must be a number")])


def _normalize_filters(value: Any, provider: str, field: str = "filters") -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValidationFailure([_err(field, "invalid_filter_structure", "Filters must be an object")])
    unknown = sorted(set(value.keys()) - ALLOWED_FILTER_KEYS)
    if unknown:
        raise ValidationFailure([_err(field, "invalid_filter_key", "Unsupported filter field: " + ", ".join(unknown))])
    out: dict[str, Any] = {}
    if "username_whitelist" in value:
        out["username_whitelist"] = _as_list(value.get("username_whitelist"))
    if "server_uuid" in value:
        out["server_uuid"] = str(value.get("server_uuid") or "").strip()
    if "server_uuid_whitelist" in value:
        out["server_uuid_whitelist"] = _as_list(value.get("server_uuid_whitelist"))
    if "server_uuid_blacklist" in value:
        out["server_uuid_blacklist"] = _as_list(value.get("server_uuid_blacklist"))
    if provider == "plex" and "ignore_live_tv_dvr" in value:
        out["ignore_live_tv_dvr"] = bool(value.get("ignore_live_tv_dvr"))
    return out


def _normalize_webhook_settings(cfg: Mapping[str, Any], provider: str, body: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    allowed = WEBHOOK_SETTING_KEYS | {"filters"}
    unknown = sorted(set(str(k) for k in body.keys()) - allowed - {"provider", "provider_instance", "regenerate"})
    if unknown:
        raise ValidationFailure([_err("settings", "unsupported_setting", "Unsupported setting: " + ", ".join(unknown))])
    if "enabled" in body:
        out["enabled"] = bool(body.get("enabled"))
    if "sinks" in body:
        sinks: list[str] = []
        for sink in _as_list(body.get("sinks")):
            key = _sink_provider(sink, "sinks")
            if key not in sinks:
                sinks.append(key)
        out["sinks"] = sinks
    if "sink_instances" in body:
        raw = body.get("sink_instances")
        if not isinstance(raw, Mapping):
            raise ValidationFailure([_err("sink_instances", "invalid_structure", "Destination profiles must be an object")])
        instances: dict[str, str] = {}
        for sink, inst_raw in raw.items():
            key = _sink_provider(sink, f"sink_instances.{sink}")
            inst = normalize_instance_id(inst_raw)
            _require_sink_profile(cfg, key, inst, f"sink_instances.{key}")
            instances[key] = inst
        out["sink_instances"] = instances
    if "filters" in body:
        filter_key = "filters_plex" if provider == "plex" else "filters_jellyfin" if provider == "jellyfin" else "filters_emby"
        out[filter_key] = _normalize_filters(body.get("filters"), provider, "filters")
    for key in ("filters_plex", "filters_jellyfin", "filters_emby"):
        if key in body:
            out[key] = _normalize_filters(body.get(key), provider, key)
    if provider == "plex":
        for key in ("plex_trakt_ratings", "plex_simkl_ratings", "plex_mdblist_ratings"):
            if key in body:
                out[key] = bool(body.get(key))
    for key in ("pause_debounce_seconds", "suppress_start_at"):
        if key in body:
            val = _coerce_int(body.get(key), key)
            if val < 0 or val > 3600:
                raise ValidationFailure([_err(key, "invalid_range", "Value must be between 0 and 3600")])
            out[key] = val
    return out


def _safe_webhook_settings(settings: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key in WEBHOOK_SETTING_KEYS:
        if key in settings:
            if key == "sinks":
                selected: list[str] = []
                for sink in _as_list(settings.get(key)):
                    if sink.strip().lower() in ROUTE_SINKS and sink.strip().lower() not in selected:
                        selected.append(sink.strip().lower())
                out[key] = selected
            else:
                out[key] = _clone(settings.get(key))
    return out


def _base_url(request: Request) -> str:
    base = str(request.base_url).rstrip("/")
    proto = str(request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip().lower()
    if proto == "https" and base.startswith("http://"):
        base = "https://" + base[7:]
    return base


def _profile_endpoint_url(request: Request, provider: str, token: str) -> str:
    path = "plex" if provider == "plex" else "jellyfin" if provider == "jellyfin" else "emby"
    return f"{_base_url(request)}/webhook/{path}?profile={token}"


def _route_ratings_url(request: Request, ratings: Mapping[str, Any]) -> str:
    hook_id = str(ratings.get("webhook_id") or "").strip()
    token = str(ratings.get("webhook_token") or "").strip()
    if not hook_id or not token:
        return ""
    return f"{_base_url(request)}/webhook/plexwatcher?route={hook_id}&token={token}"


def _global_plex_ratings_url(request: Request, cfg: Mapping[str, Any]) -> str:
    token = str(_dict(_dict(cfg.get("security")).get("webhook_ids")).get("plexwatcher") or "").strip()
    if not token:
        return ""
    return f"{_base_url(request)}/webhook/plexwatcher?token={token}"


def _destination_availability(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for sink in SINK_PROVIDERS:
        profiles: list[dict[str, Any]] = []
        for inst in _instances(cfg, sink):
            ready = sink_configured(cfg, sink, inst)
            profiles.append(
                {
                    "provider": sink,
                    "instance": inst,
                    "label": provider_label(sink, inst),
                    "configured": ready,
                    "reason": "" if ready else "not_configured",
                }
            )
        out.append({"provider": sink, "label": provider_label(sink), "profiles": profiles})
    return out


def _eligible_sources(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for provider in SOURCE_PROVIDERS:
        profiles: list[dict[str, Any]] = []
        for inst in _instances(cfg, provider):
            configured = media_source_connected(cfg, provider, inst)
            explicit = bool(_profile_override(cfg, provider, inst))
            profiles.append(
                {
                    "provider": provider,
                    "instance": inst,
                    "label": provider_label(provider, inst),
                    "configured": configured,
                    "eligible": configured,
                    "explicit": explicit,
                    "enabled": webhook_source_enabled(cfg, provider, inst),
                    "reason": "" if configured else "not_configured",
                }
            )
        out.append({"provider": provider, "label": provider_label(provider), "profiles": profiles})
    return out


def _webhook_cards(cfg: dict[str, Any], request: Request) -> list[dict[str, Any]]:
    hooks = {
        f"{h.get('provider')}:{normalize_instance_id(h.get('instance'))}": str(h.get("webhook_token") or "").strip()
        for h in _ensure_media_profile_webhook_ids(cfg)
    }
    sources = scrobble_sources(cfg)
    out: list[dict[str, Any]] = []
    for provider in SOURCE_PROVIDERS:
        for inst in _instances(cfg, provider):
            explicit = _profile_override(cfg, provider, inst)
            settings = webhook_settings(cfg, provider, inst)
            sink_list = webhook_sinks(cfg, provider, inst)
            source_configured = media_source_connected(cfg, provider, inst)
            src_enabled = bool(sources.get("webhook") and webhook_source_enabled(cfg, provider, inst))
            active = bool(src_enabled and source_configured and sink_list)
            if not explicit and not active:
                continue
            if not sink_list:
                continue
            token = hooks.get(f"{provider}:{inst}", "")
            endpoint = _profile_endpoint_url(request, provider, token) if token else ""
            safe_eff = _safe_webhook_settings(settings)
            safe_expl = _safe_webhook_settings(explicit)
            for sink in sink_list:
                sink_inst = webhook_sink_instance(settings, sink)
                ready = sink_configured(cfg, sink, sink_inst)
                out.append(
                    {
                        "provider": provider,
                        "provider_label": provider_label(provider),
                        "provider_instance": inst,
                        "profile_label": provider_label(provider, inst),
                        "sink": sink,
                        "sink_label": provider_label(sink),
                        "sink_instance": sink_inst,
                        "sink_ready": ready,
                        "enabled": src_enabled,
                        "source_configured": source_configured,
                        "explicit": True,
                        "active": bool(src_enabled and source_configured and ready),
                        "endpoint_url": endpoint,
                        "webhook_token": token,
                        "effective_settings": safe_eff,
                        "explicit_settings": safe_expl,
                    }
                )
    return out


def _runtime_status(request: Request, cfg: Mapping[str, Any]) -> dict[str, Any]:
    raw = debug_watch_status(request)
    routes = raw.get("routes") if isinstance(raw, dict) else []
    groups = raw.get("groups") if isinstance(raw, dict) else []
    route_count = len(normalize_routes(dict(cfg or {})))
    running_routes = [r for r in routes if isinstance(r, dict) and r.get("running")] if isinstance(routes, list) else []
    running_groups = [g for g in groups if isinstance(g, dict) and g.get("running")] if isinstance(groups, list) else []
    active_sinks = sorted({str((r or {}).get("sink") or "").strip().lower() for r in running_routes if (r or {}).get("sink")})
    active_sources = sorted({str((g or {}).get("provider") or "").strip().lower() for g in running_groups if (g or {}).get("provider")})
    watch = _dict(_dict(cfg.get("scrobble") if isinstance(cfg, Mapping) else {}).get("watch"))
    return {
        "running": bool(raw.get("alive")) if isinstance(raw, dict) else False,
        "groups": groups if isinstance(groups, list) else [],
        "routes": routes if isinstance(routes, list) else [],
        "running_route_count": len(running_routes),
        "configured_route_count": route_count,
        "active_source_providers": active_sources,
        "active_sinks": active_sinks,
        "autostart": bool(watch.get("autostart")),
        "error": str(raw.get("error") or raw.get("watcher_reload_error") or "") if isinstance(raw, dict) else "",
    }


def _normalized_routes(cfg: dict[str, Any], request: Request) -> list[dict[str, Any]]:
    _ensure_route_ratings_webhook_ids(cfg)
    runtime = _runtime_status(request, cfg)
    running_by_id = {
        str((r or {}).get("id") or ""): bool((r or {}).get("running"))
        for r in runtime.get("routes", [])
        if isinstance(r, dict)
    }
    out: list[dict[str, Any]] = []
    for route in normalize_routes(cfg):
        options = normalize_route_options(route.get("options"))
        ratings = _dict(options.get("ratings"))
        row = dict(route)
        row["options"] = options
        row["runtime"] = {"running": bool(running_by_id.get(str(route.get("id") or "")))}
        row["ratings_webhook_url"] = _route_ratings_url(request, ratings)
        row["source_label"] = provider_label(str(row.get("provider") or ""), row.get("provider_instance"))
        row["sink_label"] = provider_label(str(row.get("sink") or ""), row.get("sink_instance"))
        out.append(row)
    return out


def _summary(cfg: dict[str, Any], request: Request, webhooks: list[dict[str, Any]], routes: list[dict[str, Any]], runtime: dict[str, Any]) -> dict[str, Any]:
    eligible = sum(1 for p in SOURCE_PROVIDERS for i in _instances(cfg, p) if media_source_connected(cfg, p, i))
    active_webhooks = sum(1 for w in webhooks if w.get("active"))
    enabled_routes = sum(1 for r in routes if r.get("enabled"))
    return {
        "active_webhooks": active_webhooks,
        "eligible_profiles": eligible,
        "enabled_routes": enabled_routes,
        "total_routes": len(routes),
        "watcher_running": bool(runtime.get("running")),
    }


def build_overview(cfg: dict[str, Any], request: Request) -> dict[str, Any]:
    cfg = cfg if isinstance(cfg, dict) else {}
    sc0 = cfg.get("scrobble")
    if isinstance(sc0, dict) and isinstance(sc0.get("sources"), Mapping):
        _sync_derived_sources(cfg)
    sources = scrobble_sources(cfg)
    webhooks = _webhook_cards(cfg, request)
    routes = _normalized_routes(cfg, request)
    runtime = _runtime_status(request, cfg)
    sc = _dict(cfg.get("scrobble"))
    watch = _dict(sc.get("watch"))
    wh = _dict(sc.get("webhook"))
    trakt_policy = _dict(sc.get("trakt"))
    global_settings = {
        "enabled": bool(sc.get("enabled")),
        "sources": {
            "webhook": bool(sources.get("webhook")),
            "watcher": bool(sources.get("watcher")),
            "watch": bool(sources.get("watcher")),
        },
        "mode": legacy_mode_for_sources(sources, str(sc.get("mode") or "webhook")),
        "watch_autostart": bool(watch.get("autostart")),
        "global_auto_remove_watchlist": bool(sc.get("delete_plex")),
        "global_auto_remove_types": _clone(sc.get("delete_plex_types") or ["movie"]),
        "watch_defaults": {
            "watched_at": trakt_policy.get("watched_at"),
            "force_stop_at": trakt_policy.get("force_stop_at"),
            "progress_step": trakt_policy.get("progress_step"),
            "pause_debounce_seconds": watch.get("pause_debounce_seconds"),
            "suppress_start_at": watch.get("suppress_start_at"),
        },
        "webhook_defaults": {
            "pause_debounce_seconds": wh.get("pause_debounce_seconds"),
            "suppress_start_at": wh.get("suppress_start_at"),
        },
        "global_plex_ratings": {
            "trakt": bool(watch.get("plex_trakt_ratings")),
            "simkl": bool(watch.get("plex_simkl_ratings")),
            "mdblist": bool(watch.get("plex_mdblist_ratings")),
            "endpoint_url": _global_plex_ratings_url(request, cfg),
        },
    }
    sec = _dict(cfg.get("security"))
    sec_ids = _dict(sec.get("webhook_ids"))
    legacy_webhooks = (
        []
        if bool(sec.get("legacy_webhooks_removed"))
        else [label for label, key in (("Plex", "plextrakt"), ("Jellyfin", "jellyfintrakt"), ("Emby", "embytrakt")) if str(sec_ids.get(key) or "").strip()]
    )
    return {
        "ok": True,
        "source_state": global_settings,
        "eligible_sources": _eligible_sources(cfg),
        "destination_availability": _destination_availability(cfg),
        "webhooks": webhooks,
        "routes": routes,
        "watcher_runtime": runtime,
        "summary": _summary(cfg, request, webhooks, routes, runtime),
        "hybrid_warning": bool(sources.get("webhook") and sources.get("watcher")),
        "legacy_webhooks": legacy_webhooks,
    }


def _sync_derived_sources(cfg: dict[str, Any]) -> None:
    sc = cfg.get("scrobble")
    if not isinstance(sc, dict):
        return
    webhook_on = any(
        media_source_connected(cfg, provider, inst)
        and webhook_source_enabled(cfg, provider, inst)
        and bool(webhook_sinks(cfg, provider, inst))
        for provider in SOURCE_PROVIDERS
        for inst in _instances(cfg, provider)
    )
    watch = sc.get("watch")
    watch = watch if isinstance(watch, dict) else {}
    routes = watch.get("routes")
    routes = routes if isinstance(routes, list) else []
    watcher_on = any(bool(r.get("enabled", True)) for r in routes if isinstance(r, dict))
    sc["sources"] = {"webhook": webhook_on, "watcher": watcher_on, "watch": watcher_on}
    sc["enabled"] = bool(webhook_on or watcher_on)
    sc["mode"] = legacy_mode_for_sources({"webhook": webhook_on, "watcher": watcher_on}, str(sc.get("mode") or "webhook"))


def _save_and_runtime(request: Request, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    _sync_derived_sources(after)
    changed = _watch_runtime_changed(before, after)
    was_running = _watcher_running(request.app) if changed else False
    save_config(after)
    _after_config_save({"CW": None, "probes_cache": None, "probes_status_cache": None, "scheduler": None}, after)
    runtime_result: dict[str, Any] = {}
    if changed:
        runtime_result = _apply_watch_runtime(request.app, after, was_running)
    overview = build_overview(after, request)
    overview["runtime_result"] = runtime_result
    return overview


def _routes_node(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    sc = cfg.setdefault("scrobble", {})
    if not isinstance(sc, dict):
        sc = {}
        cfg["scrobble"] = sc
    watch = sc.setdefault("watch", {})
    if not isinstance(watch, dict):
        watch = {}
        sc["watch"] = watch
    routes = watch.setdefault("routes", [])
    if not isinstance(routes, list):
        routes = []
        watch["routes"] = routes
    return routes


def _route_key(route: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(route.get("provider") or "").strip().lower(),
        normalize_instance_id(route.get("provider_instance")),
        str(route.get("sink") or "").strip().lower(),
        normalize_instance_id(route.get("sink_instance")),
    )


def _validate_route(cfg: Mapping[str, Any], route: dict[str, Any]) -> dict[str, Any]:
    normalized = normalize_route(route, str(route.get("id") or "R1"))
    provider = str(normalized.get("provider") or "").strip().lower()
    sink = str(normalized.get("sink") or "").strip().lower()
    if provider not in ROUTE_PROVIDERS:
        raise ValidationFailure([_err("provider", "unsupported_provider", "Unsupported source provider")])
    if sink not in ROUTE_SINKS:
        raise ValidationFailure([_err("sink", "unsupported_provider", "Unsupported destination provider")])
    _require_source_profile(cfg, provider, str(normalized.get("provider_instance") or "default"))
    _require_sink_profile(cfg, sink, str(normalized.get("sink_instance") or "default"))
    normalized["filters"] = _normalize_filters(normalized.get("filters"), provider, "filters")
    normalized["options"] = normalize_route_options(normalized.get("options"))
    ratings = _dict(_dict(normalized.get("options")).get("ratings"))
    targets = [str(t or "").strip().lower() for t in (ratings.get("targets") or []) if str(t or "").strip()]
    if ratings.get("mode") == "custom":
        if provider != "plex":
            raise ValidationFailure([_err("options.ratings.mode", "plex_only", "Route ratings webhooks are supported for Plex sources only")])
        if not targets:
            raise ValidationFailure([_err("options.ratings.targets", "missing_targets", "Select at least one ratings destination")])
        for target in targets:
            _require_sink_profile(cfg, target, str(normalized.get("sink_instance") or "default"), f"options.ratings.targets.{target}")
    return normalized


def _validate_routes_unique(routes: list[dict[str, Any]]) -> None:
    ids: set[str] = set()
    keys: set[tuple[str, str, str, str]] = set()
    errors: list[dict[str, str]] = []
    for route in routes:
        rid = str(route.get("id") or "").strip()
        if not rid:
            errors.append(_err("id", "missing_id", "Route ID is required"))
        elif rid in ids:
            errors.append(_err("id", "duplicate_route_id", f"Route ID already exists: {rid}"))
        ids.add(rid)
        key = _route_key(route)
        if key in keys:
            errors.append(_err("route", "duplicate_route", "Duplicate routes are not allowed"))
        keys.add(key)
    if errors:
        raise ValidationFailure(errors)


def _next_route_id(routes: list[dict[str, Any]]) -> str:
    used = {str(r.get("id") or "").strip() for r in routes if isinstance(r, dict)}
    i = 1
    while f"R{i}" in used:
        i += 1
    return f"R{i}"


@router.get("/overview")
def api_scrobbler_overview(request: Request) -> JSONResponse:
    cfg = load_config() or {}
    return JSONResponse(build_overview(cfg, request), headers={"Cache-Control": "no-store"})


@router.post("/webhooks/profile")
def api_profile_webhook_save(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        before = load_config() or {}
        after = _clone(before)
        provider = _source_provider(payload.get("provider"))
        instance = normalize_instance_id(payload.get("provider_instance"))
        _require_source_profile(after, provider, instance)
        settings = _normalize_webhook_settings(after, provider, payload)
        node = _profile_override_node(after, provider, instance)
        add_sinks = settings.pop("sinks", None)
        add_insts = settings.pop("sink_instances", None)
        if add_sinks is not None:
            prev_sink = str(payload.get("prev_sink") or "").strip().lower()
            base_settings = webhook_settings(after, provider, instance)
            sinks_now = list(webhook_sinks(after, provider, instance))
            insts_now = {s: webhook_sink_instance(base_settings, s) for s in sinks_now}
            if prev_sink:
                sinks_now = [s for s in sinks_now if s != prev_sink]
                insts_now.pop(prev_sink, None)
            for s in add_sinks:
                if s not in sinks_now:
                    sinks_now.append(s)
            for k, v in (add_insts or {}).items():
                insts_now[k] = v
            node["sinks"] = sinks_now
            node["sink_instances"] = {k: v for k, v in insts_now.items() if k in sinks_now}
        node.update(settings)
        if "enabled" not in node:
            node["enabled"] = True
        _ensure_media_profile_webhook_ids(after, regenerate=bool(payload.get("regenerate")))
        return JSONResponse(_save_and_runtime(request, before, after), headers={"Cache-Control": "no-store"})
    except ValidationFailure as exc:
        return _validation_response(exc)


@router.post("/webhooks/profile/disable")
def api_profile_webhook_disable(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        before = load_config() or {}
        after = _clone(before)
        provider = _source_provider(payload.get("provider"))
        instance = normalize_instance_id(payload.get("provider_instance"))
        sink = str(payload.get("sink") or "").strip().lower()
        if bool(payload.get("remove")):
            if sink:
                settings = webhook_settings(after, provider, instance)
                remaining = [s for s in webhook_sinks(after, provider, instance) if s != sink]
                if remaining:
                    node = _profile_override_node(after, provider, instance)
                    node["sinks"] = remaining
                    node["sink_instances"] = {s: webhook_sink_instance(settings, s) for s in remaining}
                else:
                    _profile_override_remove(after, provider, instance)
                    if webhook_sinks(after, provider, instance):
                        node = _profile_override_node(after, provider, instance)
                        node["sinks"] = []
                        node["sink_instances"] = {}
            else:
                _profile_override_remove(after, provider, instance)
            _ensure_media_profile_webhook_ids(after)
        else:
            node = _profile_override_node(after, provider, instance)
            node["enabled"] = False
        return JSONResponse(_save_and_runtime(request, before, after), headers={"Cache-Control": "no-store"})
    except ValidationFailure as exc:
        return _validation_response(exc)


@router.post("/webhooks/profile/regenerate")
def api_profile_webhook_regenerate(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        before = load_config() or {}
        after = _clone(before)
        provider = _source_provider(payload.get("provider"))
        instance = normalize_instance_id(payload.get("provider_instance"))
        _require_source_profile(after, provider, instance)
        sec = after.setdefault("security", {})
        ids = sec.setdefault("webhook_ids", {})
        ids[f"profile:{provider}:{instance}"] = _gen_webhook_id()
        return JSONResponse(_save_and_runtime(request, before, after), headers={"Cache-Control": "no-store"})
    except ValidationFailure as exc:
        return _validation_response(exc)


@router.post("/webhooks/cleanup-legacy")
def api_cleanup_legacy_webhooks(request: Request) -> JSONResponse:
    before = load_config() or {}
    after = _clone(before)
    sec = after.setdefault("security", {})
    ids = sec.setdefault("webhook_ids", {})
    if isinstance(ids, dict):
        for key in ("plextrakt", "jellyfintrakt", "embytrakt"):
            ids.pop(key, None)
        for key in [k for k in ids if str(k).startswith("profile:") and not _profile_key_exists(after, str(k))]:
            ids.pop(key, None)
    sec["legacy_webhooks_removed"] = True
    return JSONResponse(_save_and_runtime(request, before, after), headers={"Cache-Control": "no-store"})


def _profile_key_exists(cfg: Mapping[str, Any], key: str) -> bool:
    parts = str(key).split(":")
    if len(parts) != 3:
        return True
    _, provider, inst = parts
    provider = provider.strip().lower()
    if provider not in SOURCE_PROVIDERS:
        return False
    return normalize_instance_id(inst) in {normalize_instance_id(i) for i in _instances(cfg, provider)}


@router.post("/routes")
def api_route_create(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        before = load_config() or {}
        after = _clone(before)
        routes = _routes_node(after)
        route_body = dict(payload or {})
        route_body["id"] = str(route_body.get("id") or _next_route_id(routes)).strip()
        route = _validate_route(after, route_body)
        next_routes = [normalize_route(r, f"R{i + 1}") for i, r in enumerate(routes) if isinstance(r, dict)]
        next_routes.append(route)
        _validate_routes_unique(next_routes)
        routes[:] = next_routes
        _ensure_route_ratings_webhook_ids(after)
        return JSONResponse(_save_and_runtime(request, before, after), headers={"Cache-Control": "no-store"})
    except ValidationFailure as exc:
        return _validation_response(exc)


@router.put("/routes/{route_id}")
def api_route_update(request: Request, route_id: str, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        before = load_config() or {}
        after = _clone(before)
        routes = _routes_node(after)
        found = False
        next_routes: list[dict[str, Any]] = []
        for i, raw in enumerate(routes):
            if not isinstance(raw, dict):
                continue
            current = normalize_route(raw, f"R{i + 1}")
            if str(current.get("id") or "") == str(route_id or "").strip():
                merged = {**current, **dict(payload or {})}
                merged["id"] = str(route_id or "").strip()
                current = _validate_route(after, merged)
                found = True
            next_routes.append(current)
        if not found:
            raise ValidationFailure([_err("id", "route_not_found", "Route was not found")])
        _validate_routes_unique(next_routes)
        routes[:] = next_routes
        _ensure_route_ratings_webhook_ids(after, regenerate=bool(payload.get("regenerate_ratings_webhook")))
        return JSONResponse(_save_and_runtime(request, before, after), headers={"Cache-Control": "no-store"})
    except ValidationFailure as exc:
        return _validation_response(exc)


@router.delete("/routes/{route_id}")
def api_route_delete(request: Request, route_id: str) -> JSONResponse:
    try:
        before = load_config() or {}
        after = _clone(before)
        routes = _routes_node(after)
        rid = str(route_id or "").strip()
        next_routes = [
            normalize_route(r, f"R{i + 1}")
            for i, r in enumerate(routes)
            if isinstance(r, dict) and str(normalize_route(r, f"R{i + 1}").get("id") or "") != rid
        ]
        if len(next_routes) == len([r for r in routes if isinstance(r, dict)]):
            raise ValidationFailure([_err("id", "route_not_found", "Route was not found")])
        routes[:] = next_routes
        _ensure_route_ratings_webhook_ids(after)
        return JSONResponse(_save_and_runtime(request, before, after), headers={"Cache-Control": "no-store"})
    except ValidationFailure as exc:
        return _validation_response(exc)


@router.post("/settings")
def api_scrobbler_settings(request: Request, payload: dict[str, Any] = Body(...)) -> JSONResponse:
    try:
        before = load_config() or {}
        after = _clone(before)
        sc = after.setdefault("scrobble", {})
        if not isinstance(sc, dict):
            sc = {}
            after["scrobble"] = sc
        sources_raw = payload.get("sources")
        if isinstance(sources_raw, Mapping):
            sources = {
                "webhook": bool(sources_raw.get("webhook")),
                "watcher": bool(sources_raw.get("watcher", sources_raw.get("watch", False))),
            }
            sc["enabled"] = bool(sources["webhook"] or sources["watcher"])
            sc["sources"] = {"webhook": sources["webhook"], "watcher": sources["watcher"], "watch": sources["watcher"]}
            sc["mode"] = legacy_mode_for_sources(sources, str(sc.get("mode") or "webhook"))
        if "enabled" in payload and "sources" not in payload:
            sc["enabled"] = bool(payload.get("enabled"))
        if "global_auto_remove_watchlist" in payload:
            sc["delete_plex"] = bool(payload.get("global_auto_remove_watchlist"))
        if "global_auto_remove_types" in payload:
            types = [x for x in _as_list(payload.get("global_auto_remove_types")) if x in {"movie", "show", "episode"}]
            sc["delete_plex_types"] = types or ["movie"]
        watch = sc.setdefault("watch", {})
        if not isinstance(watch, dict):
            watch = {}
            sc["watch"] = watch
        if "watch_autostart" in payload:
            watch["autostart"] = bool(payload.get("watch_autostart"))
        ratings_raw = payload.get("global_plex_ratings")
        if isinstance(ratings_raw, Mapping):
            for sink in ("trakt", "simkl", "mdblist"):
                watch[f"plex_{sink}_ratings"] = bool(ratings_raw.get(sink))
        if bool(payload.get("regenerate_global_plex_ratings_webhook")):
            sec = after.setdefault("security", {})
            if not isinstance(sec, dict):
                sec = {}
                after["security"] = sec
            ids = sec.setdefault("webhook_ids", {})
            if not isinstance(ids, dict):
                ids = {}
                sec["webhook_ids"] = ids
            ids["plexwatcher"] = _gen_webhook_id()
        for key in ("pause_debounce_seconds", "suppress_start_at"):
            src_key = f"watch_{key}"
            if src_key in payload:
                val = _coerce_int(payload.get(src_key), src_key)
                max_val = 100 if key == "suppress_start_at" else 3600
                if val < 0 or val > max_val:
                    raise ValidationFailure([_err(src_key, "invalid_range", f"Value must be between 0 and {max_val}")])
                watch[key] = val
        trakt_policy = sc.setdefault("trakt", {})
        if not isinstance(trakt_policy, dict):
            trakt_policy = {}
            sc["trakt"] = trakt_policy
        for key in ("watched_at", "force_stop_at"):
            src_key = f"watch_{key}"
            if src_key in payload:
                val = _coerce_int(payload.get(src_key), src_key)
                if val < 0 or val > 100:
                    raise ValidationFailure([_err(src_key, "invalid_range", "Value must be between 0 and 100")])
                trakt_policy[key] = val
        if "watch_progress_step" in payload:
            val = _coerce_int(payload.get("watch_progress_step"), "watch_progress_step")
            if val < 1 or val > 25:
                raise ValidationFailure([_err("watch_progress_step", "invalid_range", "Value must be between 1 and 25")])
            trakt_policy["progress_step"] = val
        wh = sc.setdefault("webhook", {})
        if not isinstance(wh, dict):
            wh = {}
            sc["webhook"] = wh
        for key in ("pause_debounce_seconds", "suppress_start_at"):
            src_key = f"webhook_{key}"
            if src_key in payload:
                wh[key] = _coerce_int(payload.get(src_key), src_key)
        return JSONResponse(_save_and_runtime(request, before, after), headers={"Cache-Control": "no-store"})
    except ValidationFailure as exc:
        return _validation_response(exc)
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_settings"}, status_code=400)
