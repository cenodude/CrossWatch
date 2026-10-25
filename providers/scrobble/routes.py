# /providers/scrobble/routes.py
# CrossWatch - Multi-Platform Media Monitoring and Scrobbling
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from typing import Any
from cw_platform.account_match import exact_media_account_name, media_account_allowed
from cw_platform.provider_instances import (
    get_instance_block,
    get_provider_block,
    is_sync_user_instance,
    list_sync_user_ids,
    normalize_instance_id,
    normalize_user_profile_id,
    split_sync_user_id,
    sync_user_match_name,
)



DEFAULT_INSTANCE_ID = "default"
ROUTE_PROVIDERS = {"plex", "emby", "jellyfin", "kodi", "scrob", "stremio"}
ROUTE_SINGLE_ACCOUNT_PROVIDERS = {"stremio"}
ROUTE_PROFILE_NAME_PROVIDERS = {"kodi"}
ROUTE_SINKS = {"myanimelist", "kitsu", "plex", "jellyfin", "emby", "kodi", "trakt", "simkl", "mdblist", "crosswatch", "floppy", "punchplay", "bingebase", "flicklist", "wetrakr", "anilist", "scrob"}
ROUTE_MEDIA_SINKS = {"plex", "jellyfin", "emby", "kodi"}
ROUTE_RATING_SINKS = {"kitsu", "myanimelist", "trakt", "simkl", "mdblist", "crosswatch", "floppy", "punchplay", "flicklist", "wetrakr", "anilist", "scrob"}
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
    "poll_seconds": (5, 60),
    "idle_poll_seconds": (5, 120),
}
ROUTE_POLL_KEYS = ("poll_seconds", "idle_poll_seconds")
ROUTE_POLL_PROVIDERS = {"emby", "jellyfin", "kodi", "scrob"}
ROUTE_WATCH_BOOLEAN_KEYS = {"unresolved_user_fallback", "anime_mapping", "simkl_rewatches", "plexkodiconnect_support"}
ROUTE_WATCH_STRICT_BOOLEAN_KEYS = {"simkl_rewatches", "plexkodiconnect_support"}


def same_scrobble_endpoint(provider: Any, provider_instance: Any, sink: Any, sink_instance: Any) -> bool:
    source = str(provider or "").strip().lower()
    return bool(source) and source == str(sink or "").strip().lower() and normalize_instance_id(provider_instance) == normalize_instance_id(sink_instance)


def route_is_self_target(route: dict[str, Any]) -> bool:
    return same_scrobble_endpoint(route.get("provider"), route.get("provider_instance"), route.get("sink"), route.get("sink_instance"))


SYNC_USER_ENDPOINTS = {"plex", "emby", "jellyfin"}


def sync_user_endpoints_supported(provider: Any, provider_instance: Any, sink: Any, sink_instance: Any) -> bool:
    prov = str(provider or "").strip().lower()
    snk = str(sink or "").strip().lower()
    if prov not in SYNC_USER_ENDPOINTS or snk not in SYNC_USER_ENDPOINTS:
        return False
    if is_sync_user_instance(provider_instance) or is_sync_user_instance(sink_instance):
        return False
    return not same_scrobble_endpoint(prov, provider_instance, snk, sink_instance)


def _sync_user_name(cfg: dict[str, Any], provider: str, instance: str) -> str:
    entry = get_instance_block(cfg, provider, instance)
    return str(entry.get("username") or entry.get("user") or "").strip()


def _sync_user_account_ids(cfg: dict[str, Any], provider: str, instance: str, key: str) -> list[str]:
    entry = get_instance_block(cfg, provider, instance)
    ids = [key]
    for field in ("user_id", "account_id", "cloud_account_id"):
        value = str(entry.get(field) or "").strip()
        if value and value not in ids:
            ids.append(value)
    return ids


def _unique_by_match_name(cfg: dict[str, Any], provider: str, instances: list[str]) -> dict[str, str]:
    names = {inst: sync_user_match_name(cfg, provider, inst).casefold() for inst in instances}
    counts: dict[str, int] = {}
    for match in names.values():
        if match:
            counts[match] = counts.get(match, 0) + 1
    return {match: inst for inst, match in names.items() if match and counts[match] == 1}


def sync_user_matches(cfg: dict[str, Any], provider: Any, provider_instance: Any, sink: Any, sink_instance: Any) -> list[dict[str, Any]]:
    if not sync_user_endpoints_supported(provider, provider_instance, sink, sink_instance):
        return []
    prov = str(provider).strip().lower()
    snk = str(sink).strip().lower()
    dst_by_name = _unique_by_match_name(cfg, snk, list_sync_user_ids(cfg, snk, normalize_instance_id(sink_instance)))
    src_ids = list_sync_user_ids(cfg, prov, normalize_instance_id(provider_instance))
    src_unique = _unique_by_match_name(cfg, prov, src_ids)
    out: list[dict[str, Any]] = []
    for inst in src_ids:
        ref = split_sync_user_id(inst)
        name = _sync_user_name(cfg, prov, inst)
        match = sync_user_match_name(cfg, prov, inst).casefold()
        dst = dst_by_name.get(match) if match and src_unique.get(match) == inst else None
        if ref is not None and dst:
            out.append({"key": ref[1], "username": name, "sink_instance": dst, "ids": _sync_user_account_ids(cfg, prov, inst, ref[1])})
    return out


def sync_user_whitelist(match: dict[str, Any]) -> list[str]:
    return [f"id:{value}" for value in match.get("ids") or [match["key"]]]


def sync_user_match_for_account(matches: list[dict[str, Any]], accounts: list[Any], *, account_id: Any = "", account_uuid: Any = "", user_id: Any = "") -> dict[str, Any] | None:
    event_ids = {str(value).strip().casefold() for value in (account_id, user_id) if str(value or "").strip()}
    if event_ids:
        hits = [match for match in matches if event_ids & {str(value).casefold() for value in match.get("ids") or [match["key"]]}]
    else:
        names = {exact_media_account_name(name) for name in accounts or [] if exact_media_account_name(name)}
        hits = [match for match in matches if exact_media_account_name(match["username"]) in names]
    return hits[0] if len(hits) == 1 else None


def expand_sync_user_route(cfg: dict[str, Any], route: dict[str, Any]) -> list[dict[str, Any]]:
    if not route.get("match_users"):
        return [route]
    if not sync_user_endpoints_supported(route.get("provider"), route.get("provider_instance"), route.get("sink"), route.get("sink_instance")):
        return [route]
    raw_filters = route.get("filters")
    filters: dict[str, Any] = raw_filters if isinstance(raw_filters, dict) else {}
    allowed = filters.get("username_whitelist") or []
    out: list[dict[str, Any]] = []
    for match in sync_user_matches(cfg, route.get("provider"), route.get("provider_instance"), route.get("sink"), route.get("sink_instance")):
        if allowed and not media_account_allowed(allowed, match["username"], account_id=match["key"], user_id=match["key"], default_allow=False):
            continue
        derived = dict(route)
        derived["id"] = f"{route.get('id')}~{match['key']}"
        derived["sink_instance"] = match["sink_instance"]
        derived["filters"] = {**filters, "username_whitelist": sync_user_whitelist(match)}
        derived["_cw_parent_route_id"] = route.get("id")
        derived["_cw_sync_user"] = match["username"]
        out.append(derived)
    return out


def scrobble_sink_config(cfg: dict[str, Any], provider: str, instance: Any) -> dict[str, Any]:
    inst = normalize_instance_id(instance)
    captured = cfg.get("_cw_scrobble_sink") or {}
    if captured.get("provider") == provider and captured.get("instance") == inst:
        block = _deep_clone(captured.get("config") or {})
    else:
        original = cfg.get("_cw_scrobble_provider_configs") or {}
        base = {**cfg, **original}
        block = _deep_clone(get_provider_block(base, provider, inst))
    block.pop("instances", None)
    if inst == "default" and provider in {"jellyfin", "emby"}:
        auth = (cfg.get("auth") or {}).get(provider) or {}
        for field in ("server", "access_token", "user_id"):
            if field not in block and auth.get(field) is not None:
                block[field] = _deep_clone(auth[field])
    return {**cfg, provider: block}


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
    watch: dict[str, Any] = {}
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
    for key in ROUTE_WATCH_BOOLEAN_KEYS:
        if key in watch_src:
            watch[key] = watch_src.get(key) is True if key in ROUTE_WATCH_STRICT_BOOLEAN_KEYS else bool(watch_src.get(key))

    out: dict[str, Any] = {
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
    destination_raw = raw.get("destination")
    if isinstance(destination_raw, dict):
        libraries_raw = destination_raw.get("libraries")
        libraries: list[str] = []
        for value in libraries_raw if isinstance(libraries_raw, list) else []:
            text = str(value if value is not None else "").strip()
            if text and text not in libraries:
                libraries.append(text)
        destination: dict[str, Any] = {"libraries": libraries}
        if "update_all_copies" in destination_raw:
            destination["update_all_copies"] = destination_raw.get("update_all_copies") is not False
        out["destination"] = destination
    return out


def route_options_has_watch_key(options: Any, key: str) -> bool:
    if not isinstance(options, dict):
        return False
    watch = options.get("watch")
    return isinstance(watch, dict) and key in watch


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
    raw_options = r.get("options")
    options = normalize_route_options(raw_options)
    if prov == "plex" and not route_options_has_watch_key(raw_options, "unresolved_user_fallback"):
        options["watch"]["unresolved_user_fallback"] = True
    if prov not in ROUTE_POLL_PROVIDERS:
        for key in ROUTE_POLL_KEYS:
            options["watch"].pop(key, None)
    if sink not in ROUTE_MEDIA_SINKS:
        options.pop("destination", None)
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
    if r.get("match_users") is True or str(r.get("match_users") or "").strip().lower() in {"1", "true", "yes", "on"}:
        out["match_users"] = True
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


def _loose_profile_name(value: Any) -> str:
    from cw_platform.account_match import normalize_media_account_name

    return normalize_media_account_name(value)


def _exact_profile_name(value: Any) -> str:
    from cw_platform.account_match import exact_media_account_name

    return exact_media_account_name(value)


def route_profile_accepts(cfg: dict[str, Any], name: Any) -> bool | None:
    watch = ((cfg or {}).get("scrobble") or {}).get("watch") if isinstance(cfg, dict) else None
    if not isinstance(watch, dict):
        return None
    if str(watch.get("route_provider") or "").strip().lower() not in ROUTE_PROFILE_NAME_PROVIDERS:
        return None
    profile_id = normalize_user_profile_id(watch.get("route_profile_id"))
    if not profile_id:
        return None
    try:
        from cw_platform.provider_instances import list_user_profiles

        labels = {normalize_user_profile_id(row.get("id")): str(row.get("label") or "").strip() for row in list_user_profiles(cfg)}
    except Exception:
        return None
    label = labels.get(profile_id) or ""
    if not label:
        return None
    wanted = _exact_profile_name(name)
    if not wanted:
        return False
    exact = _exact_profile_name(label)
    others = [other for other in labels.values() if other]
    if sum(1 for other in others if _exact_profile_name(other) == exact) > 1:
        return False
    loose = _loose_profile_name(label)
    if not loose or sum(1 for other in others if _loose_profile_name(other) == loose) > 1:
        return wanted == exact
    return _loose_profile_name(name) == loose


def route_profile_signature(cfg: dict[str, Any]) -> list[str]:
    watch = ((cfg or {}).get("scrobble") or {}).get("watch") if isinstance(cfg, dict) else None
    if not isinstance(watch, dict):
        return []
    if str(watch.get("route_provider") or "").strip().lower() not in ROUTE_PROFILE_NAME_PROVIDERS:
        return []
    profile_id = normalize_user_profile_id(watch.get("route_profile_id"))
    if not profile_id:
        return []
    try:
        from cw_platform.provider_instances import list_user_profiles

        rows = list_user_profiles(cfg)
    except Exception:
        return [profile_id]
    return [profile_id, *sorted(f"{normalize_user_profile_id(row.get('id'))}={str(row.get('label') or '').strip()}" for row in rows)]


def route_needs_account_filter(cfg: dict[str, Any], route: dict[str, Any]) -> bool:
    if not route_assigned_profile_id(cfg, route):
        return False
    provider = str((route or {}).get("provider") or "").strip().lower()
    if provider in ROUTE_SINGLE_ACCOUNT_PROVIDERS or provider in ROUTE_PROFILE_NAME_PROVIDERS:
        return False
    filters = (route or {}).get("filters")
    raw = filters.get("username_whitelist") if isinstance(filters, dict) else None
    values = raw if isinstance(raw, list) else ([raw] if raw else [])
    return not any(str(value or "").strip() for value in values)


def group_poll_seconds(cfg: dict[str, Any], provider: Any, provider_instance: Any, key: str = "poll_seconds") -> int | None:
    prov = str(provider or "").strip().lower()
    if prov not in ROUTE_POLL_PROVIDERS:
        return None
    inst = normalize_instance_id(provider_instance)
    values: list[int] = []
    for route in normalize_routes(cfg):
        if not route.get("enabled") or route.get("provider") != prov or route.get("provider_instance") != inst:
            continue
        if not route.get("sink") or route_is_self_target(route):
            continue
        val = ((route.get("options") or {}).get("watch") or {}).get(key)
        if isinstance(val, int):
            values.append(val)
    return min(values) if values else None


def watch_poll_seconds(cfg: Any, default: float, key: str = "poll_seconds") -> float:
    watch = ((cfg.get("scrobble") or {}).get("watch") or {}) if isinstance(cfg, dict) else {}
    raw = watch.get(key) if isinstance(watch, dict) else None
    if raw is None or isinstance(raw, bool):
        return default
    try:
        val = float(raw)
    except Exception:
        return default
    low, high = ROUTE_WATCH_POLICY_RANGES[key]
    return val if low <= val <= high else default


def find_route(cfg: dict[str, Any], route_id: str | None) -> dict[str, Any] | None:
    rid = str(route_id or "").strip()
    if not rid:
        return None
    routes = normalize_routes(cfg)
    for route in routes:
        if str(route.get("id") or "").strip() == rid:
            return route
    parent_id = rid.split("~", 1)[0]
    if parent_id == rid:
        return None
    for route in routes:
        if str(route.get("id") or "").strip() == parent_id:
            return next((derived for derived in expand_sync_user_route(cfg, route) if derived.get("id") == rid), None)
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

    if r["sink"]:
        out["_cw_scrobble_sink"] = {
            "provider": r["sink"], "instance": r["sink_instance"],
            "config": _deep_clone(scrobble_sink_config(cfg, r["sink"], r["sink_instance"])[r["sink"]]),
        }

    if r["provider"]:
        out[r["provider"]] = _provider_view(out, r["provider"], r["provider_instance"])
    if r["sink"] and r["sink"] != r["provider"]:
        out[r["sink"]] = _provider_view(cfg, r["sink"], r["sink_instance"])
    ratings = (r.get("options") or {}).get("ratings")
    rating_targets = ratings.get("targets") if isinstance(ratings, dict) else []
    for target in rating_targets if isinstance(rating_targets, list) else []:
        target_key = str(target or "").strip().lower()
        if target_key in ROUTE_RATING_SINKS:
            out[target_key] = _provider_view(out, target_key, r["sink_instance"])

    w["filters"] = _deep_clone(r.get("filters") or {})
    w["route_id"] = r["id"]
    w["route_enabled"] = r["enabled"]
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
