# /providers/scrobble/routes.py
# CrossWatch - Multi-Platform Media Monitoring and Scrobbling
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from typing import Any
from cw_platform.provider_instances import get_provider_block, normalize_instance_id, normalize_user_profile_id



DEFAULT_INSTANCE_ID = "default"
ROUTE_PROVIDERS = {"plex", "emby", "jellyfin", "kodi", "scrob"}
ROUTE_SINKS = {"trakt", "simkl", "mdblist", "crosswatch", "floppy", "punchplay", "scrob"}
ROUTE_RATING_SINKS = {"trakt", "simkl", "mdblist", "crosswatch", "floppy", "punchplay", "scrob"}
ROUTE_OPTION_STATES = {"inherit", "on", "off"}
ROUTE_RATINGS_MODES = {"off", "custom"}
ROUTE_SCROBBLE_POLICY_RANGES = {
    "watched_at": (0.0, 100.0),
    "force_stop_at": (0.0, 100.0),
    "progress_step": (1.0, 25.0),
}
ROUTE_SCROBBLE_POLICY_KEYS = set(ROUTE_SCROBBLE_POLICY_RANGES)
ROUTE_WATCH_POLICY_RANGES = {
    "pause_debounce_seconds": (0, 3600),
    "suppress_start_at": (0, 100),
}


def _deep_clone(v: Any) -> Any:
    try:
        import copy
        return copy.deepcopy(v)
    except Exception:
        return v


def _deep_merge(a: Any, b: Any) -> Any:
    if not isinstance(a, dict) or not isinstance(b, dict):
        return _deep_clone(b)
    out: dict[str, Any] = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = _deep_clone(v)
    return out


def _watch_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    sc = cfg.setdefault("scrobble", {})
    return sc.setdefault("watch", {})


def normalize_route_options(options: Any) -> dict[str, Any]:
    raw = options if isinstance(options, dict) else {}
    auto_remove = str(raw.get("auto_remove_watchlist") or "inherit").strip().lower() or "inherit"
    if auto_remove not in ROUTE_OPTION_STATES:
        auto_remove = "inherit"

    ratings_raw = raw.get("ratings")
    ratings_src = ratings_raw if isinstance(ratings_raw, dict) else {}
    ratings_mode = str(ratings_src.get("mode") or "off").strip().lower() or "off"
    if ratings_mode not in ROUTE_RATINGS_MODES:
        ratings_mode = "off"

    targets_raw = ratings_src.get("targets")
    if isinstance(targets_raw, str):
        targets_in = [targets_raw]
    elif isinstance(targets_raw, (list, tuple, set)):
        targets_in = list(targets_raw)
    else:
        targets_in = []

    targets: list[str] = []
    seen: set[str] = set()
    for item in targets_in:
        target = str(item or "").strip().lower()
        if not target or target not in ROUTE_RATING_SINKS or target in seen:
            continue
        seen.add(target)
        targets.append(target)

    webhook_id = str(ratings_src.get("webhook_id") or "").strip()
    webhook_token = str(ratings_src.get("webhook_token") or "").strip()
    scrobble_raw = raw.get("scrobble")
    scrobble_src: dict[str, Any] = scrobble_raw if isinstance(scrobble_raw, dict) else {}
    scrobble: dict[str, float] = {}
    for key, (min_val, max_val) in ROUTE_SCROBBLE_POLICY_RANGES.items():
        if key not in scrobble_src:
            continue
        src_val = scrobble_src.get(key)
        if src_val is None:
            continue
        try:
            val = float(src_val)
        except Exception:
            continue
        if min_val <= val <= max_val:
            scrobble[key] = val

    watch_raw = raw.get("watch")
    watch_src: dict[str, Any] = watch_raw if isinstance(watch_raw, dict) else {}
    watch: dict[str, int] = {}
    for key, (min_val, max_val) in ROUTE_WATCH_POLICY_RANGES.items():
        if key not in watch_src:
            continue
        src_val = watch_src.get(key)
        if src_val is None:
            continue
        try:
            val = int(src_val)
        except Exception:
            continue
        if min_val <= val <= max_val:
            watch[key] = val

    return {
        "auto_remove_watchlist": auto_remove,
        "ratings": {
            "mode": ratings_mode,
            "targets": targets,
            "webhook_id": webhook_id,
            "webhook_token": webhook_token,
        },
        "scrobble": scrobble,
        "watch": watch,
    }


def normalize_route(route: dict[str, Any], fallback_id: str) -> dict[str, Any]:
    r: dict[str, Any] = dict(route or {})
    rid = str(r.get("id") or fallback_id).strip() or fallback_id
    enabled = bool(r.get("enabled", True))

    prov = str(r.get("provider") or "").strip().lower() or "plex"
    if prov not in ROUTE_PROVIDERS:
        prov = "plex"
    prov_inst = str(r.get("provider_instance") or r.get("providerInstance") or DEFAULT_INSTANCE_ID).strip() or DEFAULT_INSTANCE_ID

    prov_inst = normalize_instance_id(prov_inst)
    sink = str(r.get("sink") or "").strip().lower()
    if sink and sink not in ROUTE_SINKS:
        sink = ""
    sink_inst = str(r.get("sink_instance") or r.get("sinkInstance") or DEFAULT_INSTANCE_ID).strip() or DEFAULT_INSTANCE_ID

    sink_inst = normalize_instance_id(sink_inst)
    filters = r.get("filters")
    if not isinstance(filters, dict):
        filters = {}
    options = normalize_route_options(r.get("options"))
    profile_id = normalize_user_profile_id(r.get("profile_id") or r.get("profileId"))

    out = {
        "id": rid,
        "enabled": enabled,
        "provider": prov,
        "provider_instance": prov_inst,
        "sink": sink,
        "sink_instance": sink_inst,
        "filters": filters,
        "options": options,
    }
    if profile_id:
        out["profile_id"] = profile_id
    return out


def normalize_routes(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    w = _watch_cfg(cfg)
    routes = w.get("routes")
    if not isinstance(routes, list):
        return []
    out: list[dict[str, Any]] = []
    for i, raw in enumerate(routes):
        if not isinstance(raw, dict):
            continue
        out.append(normalize_route(raw, f"R{i + 1}"))
    return out


def route_profile_id(cfg: dict[str, Any], route: dict[str, Any]) -> str:
    try:
        from cw_platform.access_policy import route_effective_profile_id

        return route_effective_profile_id(cfg, route)
    except Exception:
        return normalize_user_profile_id((route or {}).get("profile_id"))


def route_assigned_profile_id(cfg: dict[str, Any], route: dict[str, Any]) -> str:
    raw = (route or {}).get("profile_id") or (route or {}).get("profileId")
    try:
        from cw_platform.access_policy import valid_user_profile_id

        return valid_user_profile_id(cfg, raw)
    except Exception:
        return normalize_user_profile_id(raw)


def route_needs_account_filter(cfg: dict[str, Any], route: dict[str, Any]) -> bool:
    if not route_assigned_profile_id(cfg, route):
        return False
    filters = (route or {}).get("filters")
    raw = filters.get("username_whitelist") if isinstance(filters, dict) else None
    values = raw if isinstance(raw, list) else ([raw] if raw else [])
    return not any(str(value or "").strip() for value in values)


def find_route(cfg: dict[str, Any], route_id: str | None) -> dict[str, Any] | None:
    rid = str(route_id or "").strip()
    if not rid:
        return None
    for route in normalize_routes(cfg):
        if str(route.get("id") or "").strip() == rid:
            return route
    return None


def _provider_view(cfg: dict[str, Any], provider: str, instance_id: str) -> dict[str, Any]:
    try:
        merged = get_provider_block(cfg, provider, instance_id)
        if merged:
            base = cfg.get(provider) if isinstance(cfg.get(provider), dict) else {}
            if isinstance(base, dict) and "instances" in base:
                merged["instances"] = base.get("instances")
            return merged
    except Exception:
        pass
    base = cfg.get(provider) if isinstance(cfg.get(provider), dict) else {}
    inst = {}
    if isinstance(base, dict):
        insts = base.get("instances")
        if isinstance(insts, dict):
            inst = insts.get(instance_id) if isinstance(insts.get(instance_id), dict) else {}
    merged = _deep_merge(base, inst)
    if isinstance(base, dict) and "instances" in base:
        merged["instances"] = base.get("instances")
    return merged


def build_route_cfg(cfg: dict[str, Any], route: dict[str, Any]) -> dict[str, Any]:
    r = normalize_route(route, str(route.get("id") or "R1"))
    out: dict[str, Any] = _deep_clone(cfg) if isinstance(cfg, dict) else {}
    w = _watch_cfg(out)

    if r["provider"]:
        out[r["provider"]] = _provider_view(out, r["provider"], r["provider_instance"])
    if r["sink"]:
        out[r["sink"]] = _provider_view(out, r["sink"], r["sink_instance"])
    ratings = (r.get("options") or {}).get("ratings")
    rating_targets = ratings.get("targets") if isinstance(ratings, dict) else []
    for target in rating_targets if isinstance(rating_targets, list) else []:
        target_key = str(target or "").strip().lower()
        if target_key in ROUTE_RATING_SINKS:
            out[target_key] = _provider_view(out, target_key, r["sink_instance"])

    w["filters"] = _deep_clone(r.get("filters") or {})
    w["route_id"] = r["id"]
    w["route_profile_id"] = route_assigned_profile_id(out, r)
    w["route_effective_profile_id"] = route_profile_id(out, r)
    w["route_provider"] = r["provider"]
    w["route_provider_instance"] = r["provider_instance"]
    w["route_sink"] = r["sink"]
    w["route_sink_instance"] = r["sink_instance"]
    w["route_options"] = _deep_clone(r.get("options") or {})
    watch_policy = (w.get("route_options") or {}).get("watch")
    if isinstance(watch_policy, dict):
        for key in ROUTE_WATCH_POLICY_RANGES:
            if key in watch_policy:
                w[key] = watch_policy[key]
    if r["sink"] in ROUTE_SINKS:
        policy = (w.get("route_options") or {}).get("scrobble")
        if isinstance(policy, dict):
            target = out.setdefault("scrobble", {}).setdefault("trakt", {})
            if isinstance(target, dict):
                for key in ROUTE_SCROBBLE_POLICY_KEYS:
                    if key in policy:
                        target[key] = policy[key]
    return out


def build_route_cfg_by_id(cfg: dict[str, Any], route_id: str | None) -> dict[str, Any] | None:
    route = find_route(cfg, route_id)
    if not isinstance(route, dict):
        return None
    return build_route_cfg(cfg, route)
