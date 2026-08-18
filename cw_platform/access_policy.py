# cw_platform/access_policy.py
# CrossWatch - Managed user profile access policy
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .provider_instances import instances_for_user_profile, list_user_profiles, normalize_instance_id, normalize_user_profile_id, provider_display_key, user_profiles_for_instance

READ_PERMISSION_KEYS = ("dashboard", "watchlist", "playback")


def clean_managed_permissions(raw: Any) -> dict[str, bool]:
    src = raw if isinstance(raw, Mapping) else {}
    return {
        "dashboard": src.get("dashboard") is not False,
        "watchlist": src.get("watchlist") is not False,
        "playback": src.get("playback") is not False,
        "write": bool(src.get("write")),
    }


def managed_user_can_write(user: Mapping[str, Any] | None) -> bool:
    if not isinstance(user, Mapping):
        return False
    if bool(user.get("is_admin")):
        return True
    perms = clean_managed_permissions(user.get("permissions"))
    return bool(perms.get("write"))


def managed_user_permission(user: Mapping[str, Any] | None, key: str) -> bool:
    if not isinstance(user, Mapping):
        return False
    if bool(user.get("is_admin")):
        return True
    perms = clean_managed_permissions(user.get("permissions"))
    return bool(perms.get(str(key or "").strip().lower()))


def managed_profile_id(user: Mapping[str, Any] | None) -> str:
    if not isinstance(user, Mapping) or bool(user.get("is_admin")):
        return ""
    return normalize_user_profile_id(user.get("profile_id"))


def profile_instances_map(cfg: Mapping[str, Any], profile_id: Any) -> dict[str, list[str]]:
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        return {}
    out: dict[str, list[str]] = {}
    for provider, values in instances_for_user_profile(cfg, pid).items():
        prov = provider_display_key(provider)
        clean = [normalize_instance_id(value) for value in (values if isinstance(values, list) else [values])]
        keep = [value for value in clean if value]
        if prov and keep:
            out[prov] = keep
    return out


def managed_profile_instances(cfg: Mapping[str, Any], user: Mapping[str, Any] | None) -> dict[str, list[str]]:
    return profile_instances_map(cfg, managed_profile_id(user))


def origin_owner_instances(cfg: Mapping[str, Any], provider: Any, instance: Any) -> dict[str, list[str]] | None:
    pid = sole_instance_owner_profile_id(cfg, provider, instance)
    if not pid:
        return None
    return profile_instances_map(cfg, pid)


def profile_allows_instance(profile_instances: Mapping[str, list[str]], provider: Any, instance: Any) -> bool:
    prov = provider_display_key(provider)
    inst = normalize_instance_id(instance)
    return bool(prov and inst and inst in list(profile_instances.get(prov) or []))


VIEW_AS_HEADER = "x-cw-view-as"


VIEW_AS_ALL = {"all", "__all__"}


def requested_view_as_profile(request: Any) -> str:
    query_raw = ""
    try:
        query_raw = str(request.query_params.get("user_profile") or "").strip()
    except Exception:
        query_raw = ""
    if query_raw.lower() in VIEW_AS_ALL:
        return ""
    pid = normalize_user_profile_id(query_raw)
    if pid:
        return pid
    header_raw = ""
    try:
        headers = getattr(request, "headers", None)
        if headers is not None:
            header_raw = headers.get(VIEW_AS_HEADER) or ""
    except Exception:
        header_raw = ""
    return normalize_user_profile_id(header_raw)


def impersonated_user(user: Mapping[str, Any], profile_id: str) -> dict[str, Any]:
    return {
        "id": user.get("id"),
        "username": user.get("username"),
        "is_admin": False,
        "profile_id": profile_id,
        "permissions": {"dashboard": True, "watchlist": True, "playback": True, "write": True},
        "view_as": True,
        "view_as_admin_id": user.get("id"),
    }


def request_user(request: Any) -> Mapping[str, Any] | None:
    try:
        state = getattr(request, "state", None)
        user = getattr(state, "cw_user", None) or getattr(state, "user", None)
    except Exception:
        return None
    if not isinstance(user, Mapping):
        return None
    if not bool(user.get("is_admin")):
        return user
    pid = requested_view_as_profile(request)
    if not pid:
        return user
    state = getattr(request, "state", None)
    cached = getattr(state, "cw_view_as", None) if state is not None else None
    if isinstance(cached, tuple) and len(cached) == 2 and cached[0] == pid:
        return cached[1] or user

    def _remember(value: dict[str, Any] | None) -> Mapping[str, Any]:
        try:
            setattr(state, "cw_view_as", (pid, value))
        except Exception:
            pass
        return value or user

    try:
        from .config_base import load_config

        cfg = load_config() or {}
    except Exception:
        return _remember(None)
    if not valid_user_profile_id(cfg, pid):
        return _remember(None)
    return _remember(impersonated_user(user, pid))


def user_can_access_instance(cfg: Mapping[str, Any], user: Mapping[str, Any] | None, provider: Any, instance: Any) -> bool:
    if not isinstance(user, Mapping) or bool(user.get("is_admin")):
        return True
    return profile_allows_instance(managed_profile_instances(cfg, user), provider, instance)


def filter_instances_for_user(cfg: Mapping[str, Any], user: Mapping[str, Any] | None, provider: Any, instances: list[str]) -> list[str]:
    if not isinstance(user, Mapping) or bool(user.get("is_admin")):
        return instances
    prov = provider_display_key(provider)
    allowed = set(managed_profile_instances(cfg, user).get(prov) or [])
    return [normalize_instance_id(instance) for instance in instances if normalize_instance_id(instance) in allowed]


def pair_refs(pair: Mapping[str, Any]) -> tuple[tuple[str, str], tuple[str, str]]:
    src = provider_display_key(pair.get("source"))
    dst = provider_display_key(pair.get("target"))
    src_inst = normalize_instance_id(pair.get("source_instance") or pair.get("src_instance"))
    dst_inst = normalize_instance_id(pair.get("target_instance") or pair.get("dst_instance"))
    return (src, src_inst), (dst, dst_inst)


def profile_allows_pair(profile_instances: Mapping[str, list[str]], pair: Mapping[str, Any]) -> bool:
    (src, src_inst), (dst, dst_inst) = pair_refs(pair)
    return profile_allows_instance(profile_instances, src, src_inst) and profile_allows_instance(profile_instances, dst, dst_inst)


def pair_profile_id(pair: Mapping[str, Any]) -> str:
    return normalize_user_profile_id(pair.get("profile_id") if isinstance(pair, Mapping) else "")


def profile_label_for_id(cfg: Mapping[str, Any], profile_id: Any) -> str:
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        return ""
    for row in list_user_profiles(cfg):
        if normalize_user_profile_id(row.get("id")) == pid:
            return str(row.get("label") or "").strip()
    return ""


def valid_user_profile_id(cfg: Mapping[str, Any], profile_id: Any) -> str:
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        return ""
    for row in list_user_profiles(cfg):
        if normalize_user_profile_id(row.get("id")) == pid:
            return pid
    return ""


def sole_instance_owner_profile_id(cfg: Mapping[str, Any], provider: Any, instance: Any) -> str:
    owners = user_profiles_for_instance(cfg, provider_display_key(provider), normalize_instance_id(instance))
    if len(owners) != 1:
        return ""
    return normalize_user_profile_id(owners[0].get("id") if isinstance(owners[0], Mapping) else "")


def route_effective_profile_id(cfg: Mapping[str, Any], route: Mapping[str, Any]) -> str:
    explicit = valid_user_profile_id(cfg, route.get("profile_id") if isinstance(route, Mapping) else "")
    if explicit:
        return explicit
    provider = route.get("provider") if isinstance(route, Mapping) else ""
    instance = (route.get("provider_instance") or route.get("providerInstance")) if isinstance(route, Mapping) else ""
    return sole_instance_owner_profile_id(cfg, provider, instance)


def webhook_effective_profile_id(cfg: Mapping[str, Any], webhook: Mapping[str, Any]) -> str:
    try:
        from cw_platform.user_profile_resources import webhook_assigned_profile_id, webhook_resource_id
    except Exception:
        webhook_assigned_profile_id = None
        webhook_resource_id = None
    if callable(webhook_assigned_profile_id) and callable(webhook_resource_id):
        explicit = webhook_assigned_profile_id(
            cfg,
            webhook_resource_id(
                webhook.get("provider"),
                webhook.get("provider_instance"),
                webhook.get("sink"),
                webhook.get("sink_instance"),
            ),
        )
        if explicit:
            return explicit
    provider = webhook.get("provider") if isinstance(webhook, Mapping) else ""
    instance = (webhook.get("provider_instance") or webhook.get("providerInstance")) if isinstance(webhook, Mapping) else ""
    return sole_instance_owner_profile_id(cfg, provider, instance)


def media_account_allowlist_for_profile(cfg: Mapping[str, Any], profile_id: Any) -> dict[str, dict[str, list[str]]]:
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        return {}
    try:
        from providers.scrobble.routes import normalize_routes
        routes = normalize_routes(dict(cfg or {}))
    except Exception:
        routes = []
    out: dict[str, dict[str, list[str]]] = {}
    seen: dict[tuple[str, str], set[str]] = {}
    for route in routes:
        if not isinstance(route, Mapping) or route_effective_profile_id(cfg, route) != pid:
            continue
        filters = route.get("filters")
        if not isinstance(filters, Mapping):
            continue
        raw = filters.get("username_whitelist")
        values = raw if isinstance(raw, list) else [raw]
        provider = provider_display_key(route.get("provider"))
        instance = normalize_instance_id(route.get("provider_instance") or route.get("providerInstance"))
        if not provider or not instance:
            continue
        clean = [str(value or "").strip() for value in values]
        clean = [value for value in clean if value]
        if not clean:
            continue
        bucket = out.setdefault(provider, {}).setdefault(instance, [])
        marker = seen.setdefault((provider, instance), set())
        for text in clean:
            key = text.lower()
            if key not in marker:
                marker.add(key)
                bucket.append(text)
    return out


def decorate_pair_profile(cfg: Mapping[str, Any], pair: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(pair)
    pid = pair_profile_id(pair)
    if pid:
        out["profile_id"] = pid
        label = profile_label_for_id(cfg, pid)
        if label:
            out["profile_label"] = label
    else:
        out.pop("profile_id", None)
        out.pop("profile_label", None)
    return out


def user_can_access_pair(cfg: Mapping[str, Any], user: Mapping[str, Any] | None, pair: Mapping[str, Any]) -> bool:
    if not isinstance(user, Mapping) or bool(user.get("is_admin")):
        return True
    pid = managed_profile_id(user)
    assigned = pair_profile_id(pair)
    if not pid or not assigned or assigned != pid:
        return False
    return profile_allows_pair(managed_profile_instances(cfg, user), pair)


def filter_pairs_for_user(cfg: Mapping[str, Any], user: Mapping[str, Any] | None, pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(user, Mapping) or bool(user.get("is_admin")):
        return [decorate_pair_profile(cfg, pair) for pair in pairs if isinstance(pair, Mapping)]
    return [decorate_pair_profile(cfg, pair) for pair in pairs if isinstance(pair, Mapping) and user_can_access_pair(cfg, user, pair)]


def filter_pairs_for_profile(cfg: Mapping[str, Any], profile_id: Any, pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        return [decorate_pair_profile(cfg, pair) for pair in pairs if isinstance(pair, Mapping)]
    instances = profile_instances_map(cfg, pid)
    return [
        decorate_pair_profile(cfg, pair)
        for pair in pairs
        if isinstance(pair, Mapping) and pair_profile_id(pair) == pid and profile_allows_pair(instances, pair)
    ]


def pair_ids_for_user(cfg: Mapping[str, Any], user: Mapping[str, Any] | None) -> set[str]:
    pairs = [pair for pair in cfg.get("pairs", []) if isinstance(pair, dict)] if isinstance(cfg.get("pairs"), list) else []
    return {str(pair.get("id") or "").strip() for pair in filter_pairs_for_user(cfg, user, pairs) if str(pair.get("id") or "").strip()}
