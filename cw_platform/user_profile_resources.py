# cw_platform/user_profile_resources.py
# CrossWatch - User profile resource assignments
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cw_platform.provider_instances import (
    instances_for_user_profile,
    list_user_profiles,
    normalize_instance_id,
    normalize_user_profile_id,
    provider_display_key,
    sanitize_user_profile_label,
    upsert_user_profile,
)
from cw_platform.provider_usage import provider_label

_SYNC_FEATURE_LABELS: tuple[tuple[str, str], ...] = (
    ("watchlist", "watchlist"),
    ("ratings", "ratings"),
    ("history", "history"),
    ("progress", "progress"),
    ("playlists", "playlists"),
)


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _valid_profile_ids(cfg: Mapping[str, Any]) -> set[str]:
    return {normalize_user_profile_id(row.get("id")) for row in list_user_profiles(cfg) if normalize_user_profile_id(row.get("id"))}


def _valid_profile_id(cfg: Mapping[str, Any], value: Any) -> str:
    pid = normalize_user_profile_id(value)
    return pid if pid and pid in _valid_profile_ids(cfg) else ""


def _resource_instance(provider: Any, instance: Any) -> dict[str, str]:
    prov = provider_display_key(provider)
    inst = normalize_instance_id(instance)
    return {"provider": prov, "instance": inst, "label": provider_label(prov.lower(), inst)}


def _resource_instances(*refs: tuple[Any, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for provider, instance in refs:
        row = _resource_instance(provider, instance)
        key = (row["provider"], row["instance"])
        if row["provider"] and key not in seen:
            seen.add(key)
            out.append(row)
    return out


def _sync_pair_features(raw: Mapping[str, Any]) -> list[str]:
    features = raw.get("features")
    if not isinstance(features, Mapping):
        return []
    out: list[str] = []
    for key, label in _SYNC_FEATURE_LABELS:
        value = features.get(key)
        enabled = bool(value)
        if isinstance(value, Mapping):
            enabled = bool(value.get("enable", value.get("enabled", False)))
        if enabled:
            out.append(label)
    return out


def _sync_pair_rows(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    pairs = cfg.get("pairs")
    raw_pairs = pairs if isinstance(pairs, list) else []
    out: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_pairs):
        if not isinstance(raw, Mapping):
            continue
        rid = str(raw.get("id") or f"pair-{index + 1}").strip()
        source = provider_display_key(raw.get("source"))
        target = provider_display_key(raw.get("target"))
        source_instance = normalize_instance_id(raw.get("source_instance") or raw.get("src_instance"))
        target_instance = normalize_instance_id(raw.get("target_instance") or raw.get("dst_instance"))
        if not rid or not source or not target:
            continue
        out.append(
            {
                "id": rid,
                "label": f"{provider_label(source.lower(), source_instance)} -> {provider_label(target.lower(), target_instance)}",
                "source_provider": source,
                "source_instance": source_instance,
                "target_provider": target,
                "target_instance": target_instance,
                "assigned_profile_id": _valid_profile_id(cfg, raw.get("profile_id")),
                "instances": _resource_instances((source, source_instance), (target, target_instance)),
                "features": _sync_pair_features(raw),
            }
        )
    return out


def _watcher_route_rows(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    try:
        from providers.scrobble.routes import normalize_routes
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for route in normalize_routes(dict(cfg or {})):
        if not isinstance(route, Mapping):
            continue
        rid = str(route.get("id") or "").strip()
        source = provider_display_key(route.get("provider"))
        sink = provider_display_key(route.get("sink"))
        source_instance = normalize_instance_id(route.get("provider_instance"))
        sink_instance = normalize_instance_id(route.get("sink_instance"))
        if not rid or not source or not sink:
            continue
        out.append(
            {
                "id": rid,
                "label": f"{provider_label(source.lower(), source_instance)} -> {provider_label(sink.lower(), sink_instance)}",
                "source_provider": source,
                "source_instance": source_instance,
                "target_provider": sink,
                "target_instance": sink_instance,
                "assigned_profile_id": _valid_profile_id(cfg, route.get("profile_id")),
                "instances": _resource_instances((source, source_instance), (sink, sink_instance)),
            }
        )
    return out


def webhook_assignment_root(cfg: Mapping[str, Any] | dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    sc = cfg.get("scrobble") if isinstance(cfg, Mapping) else None
    if not isinstance(sc, dict):
        if create and isinstance(cfg, dict):
            sc = {}
            cfg["scrobble"] = sc
        else:
            return {}
    wh = sc.get("webhook")
    if not isinstance(wh, dict):
        if create:
            wh = {}
            sc["webhook"] = wh
        else:
            return {}
    raw = wh.get("user_profile_assignments")
    if isinstance(raw, dict):
        return raw
    if create:
        wh["user_profile_assignments"] = {}
        return wh["user_profile_assignments"]
    return {}


def webhook_resource_id(provider: Any, provider_instance: Any, sink: Any, sink_instance: Any) -> str:
    return ":".join(
        [
            str(provider or "").strip().lower(),
            normalize_instance_id(provider_instance),
            str(sink or "").strip().lower(),
            normalize_instance_id(sink_instance),
        ]
    )


def webhook_assigned_profile_id(cfg: Mapping[str, Any], resource_id: Any) -> str:
    assignments = webhook_assignment_root(cfg)
    return _valid_profile_id(cfg, assignments.get(str(resource_id or "").strip()))


def webhook_profile_scoped(cfg: Mapping[str, Any], provider: Any, provider_instance: Any) -> bool:
    try:
        from providers.webhooks.config import webhook_settings, webhook_sink_instance, webhook_sinks
    except Exception:
        return False
    try:
        settings = webhook_settings(cfg, str(provider or ""), provider_instance)
        for sink in webhook_sinks(cfg, str(provider or ""), provider_instance):
            resource_id = webhook_resource_id(provider, provider_instance, sink, webhook_sink_instance(settings, sink))
            if webhook_assigned_profile_id(cfg, resource_id):
                return True
    except Exception:
        return False
    return False


def _webhook_route_rows(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    try:
        from providers.webhooks.config import media_source_connected, webhook_settings, webhook_sink_instance, webhook_sinks
        from cw_platform.provider_usage import WEBHOOK_SOURCE_PROVIDERS, webhook_source_enabled
        from providers.scrobble.sources import scrobble_sources
        from cw_platform.provider_instances import list_instance_ids
    except Exception:
        return []
    if not scrobble_sources(cfg).get("webhook"):
        return []
    out: list[dict[str, Any]] = []
    for provider in WEBHOOK_SOURCE_PROVIDERS:
        try:
            instances = list_instance_ids(cfg, provider)
        except Exception:
            instances = ["default"]
        for raw_instance in instances:
            source_instance = normalize_instance_id(raw_instance)
            if not webhook_source_enabled(cfg, provider, source_instance) or not media_source_connected(cfg, provider, source_instance):
                continue
            settings = webhook_settings(cfg, provider, source_instance)
            for sink in webhook_sinks(cfg, provider, source_instance):
                sink_instance = webhook_sink_instance(settings, sink)
                rid = webhook_resource_id(provider, source_instance, sink, sink_instance)
                out.append(
                    {
                        "id": rid,
                        "label": f"{provider_label(provider, source_instance)} -> {provider_label(sink, sink_instance)}",
                        "source_provider": provider_display_key(provider),
                        "source_instance": source_instance,
                        "target_provider": provider_display_key(sink),
                        "target_instance": sink_instance,
                        "assigned_profile_id": webhook_assigned_profile_id(cfg, rid),
                        "instances": _resource_instances((provider, source_instance), (sink, sink_instance)),
                    }
                )
    return out


def profile_resource_summary(cfg: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    return {"sync_pairs": _sync_pair_rows(cfg), "watcher_routes": _watcher_route_rows(cfg), "webhook_routes": _webhook_route_rows(cfg)}


def profile_resource_ids_for_profile(cfg: Mapping[str, Any], profile_id: Any) -> dict[str, list[str]]:
    pid = normalize_user_profile_id(profile_id)
    out: dict[str, list[str]] = {"sync_pairs": [], "watcher_routes": [], "webhook_routes": []}
    if not pid:
        return out
    resources = profile_resource_summary(cfg)
    for key, rows in resources.items():
        out[key] = [str(row.get("id") or "") for row in rows if normalize_user_profile_id(row.get("assigned_profile_id")) == pid]
    return out


def resource_counts_for_profile(cfg: Mapping[str, Any], profile_id: Any) -> dict[str, int]:
    resources = profile_resource_ids_for_profile(cfg, profile_id)
    resource_count = sum(len(values) for values in resources.values())
    providers = set()
    for provider, values in instances_for_user_profile(cfg, profile_id).items():
        if values:
            providers.add(provider_display_key(provider))
    return {"providers": len(providers), "resources": resource_count}


def _instances_from_resources(resources: Mapping[str, list[dict[str, Any]]], selected: Mapping[str, set[str]]) -> dict[str, list[str]]:
    out: dict[str, str] = {}
    for key, ids in selected.items():
        rows = resources.get(key) or []
        for row in rows:
            if str(row.get("id") or "") not in ids:
                continue
            for ref in row.get("instances") or []:
                if not isinstance(ref, Mapping):
                    continue
                prov = provider_display_key(ref.get("provider"))
                inst = normalize_instance_id(ref.get("instance"))
                if prov and inst:
                    out[prov] = inst
    return {provider: [out[provider]] for provider in sorted(out)}


def _merge_instances(*maps: Mapping[str, Any]) -> dict[str, list[str]]:
    out: dict[str, str] = {}
    for raw in maps:
        for provider, values in (raw or {}).items():
            prov = provider_display_key(provider)
            if not prov:
                continue
            seq = values if isinstance(values, list) else [values]
            for value in seq:
                inst = normalize_instance_id(value)
                if inst:
                    out[prov] = inst
    return {provider: [out[provider]] for provider in sorted(out)}


def _id_set(raw: Any) -> set[str]:
    values = raw if isinstance(raw, list) else []
    return {str(value or "").strip() for value in values if str(value or "").strip()}


def apply_profile_resource_assignments(
    cfg: dict[str, Any],
    profile_id: Any,
    *,
    label: Any,
    manual_instances: Mapping[str, Any],
    selected_sync_pairs: Any,
    selected_watcher_routes: Any,
    selected_webhook_routes: Any,
) -> dict[str, Any]:
    pid = normalize_user_profile_id(profile_id)
    clean_label = sanitize_user_profile_label(label)
    if not pid or not clean_label:
        raise ValueError("invalid_user_profile")
    selected = {
        "sync_pairs": _id_set(selected_sync_pairs),
        "watcher_routes": _id_set(selected_watcher_routes),
        "webhook_routes": _id_set(selected_webhook_routes),
    }
    resources = profile_resource_summary(cfg)
    required_instances = _instances_from_resources(resources, selected)
    final_instances = _merge_instances(manual_instances, required_instances)
    profile = upsert_user_profile(cfg, pid, label=clean_label, instances=final_instances, manual_instances=manual_instances)
    valid_sync = {str(row.get("id") or "") for row in resources["sync_pairs"]}
    pairs = cfg.get("pairs")
    if isinstance(pairs, list):
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            rid = str(pair.get("id") or "").strip()
            current = _valid_profile_id(cfg, pair.get("profile_id"))
            if rid in valid_sync and rid in selected["sync_pairs"]:
                pair["profile_id"] = pid
            elif current == pid:
                pair.pop("profile_id", None)
    valid_routes = {str(row.get("id") or "") for row in resources["watcher_routes"]}
    sc = cfg.get("scrobble")
    watch = sc.get("watch") if isinstance(sc, dict) else None
    raw_routes = watch.get("routes") if isinstance(watch, dict) else []
    if isinstance(raw_routes, list):
        for route in raw_routes:
            if not isinstance(route, dict):
                continue
            rid = str(route.get("id") or "").strip()
            current = _valid_profile_id(cfg, route.get("profile_id"))
            if rid in valid_routes and rid in selected["watcher_routes"]:
                route["profile_id"] = pid
            elif current == pid:
                route.pop("profile_id", None)
    valid_webhooks = {str(row.get("id") or "") for row in resources["webhook_routes"]}
    assignments = webhook_assignment_root(cfg, create=True)
    for rid in list(assignments.keys()):
        if rid not in valid_webhooks or _valid_profile_id(cfg, assignments.get(rid)) == pid:
            assignments.pop(rid, None)
    for rid in selected["webhook_routes"]:
        if rid in valid_webhooks:
            assignments[rid] = pid
    if not assignments:
        sc = cfg.get("scrobble")
        wh = sc.get("webhook") if isinstance(sc, dict) else None
        if isinstance(wh, dict):
            wh.pop("user_profile_assignments", None)
    return profile


def clear_profile_resource_assignments(cfg: dict[str, Any], profile_id: Any) -> None:
    pid = normalize_user_profile_id(profile_id)
    if not pid:
        return
    pairs = cfg.get("pairs")
    if isinstance(pairs, list):
        for pair in pairs:
            if isinstance(pair, dict) and _valid_profile_id(cfg, pair.get("profile_id")) == pid:
                pair.pop("profile_id", None)
    sc = cfg.get("scrobble")
    watch = sc.get("watch") if isinstance(sc, dict) else None
    routes = watch.get("routes") if isinstance(watch, dict) else []
    if isinstance(routes, list):
        for route in routes:
            if isinstance(route, dict) and _valid_profile_id(cfg, route.get("profile_id")) == pid:
                route.pop("profile_id", None)
    assignments = webhook_assignment_root(cfg)
    if isinstance(assignments, dict):
        for rid in list(assignments.keys()):
            if _valid_profile_id(cfg, assignments.get(rid)) == pid:
                assignments.pop(rid, None)
        if not assignments:
            wh = sc.get("webhook") if isinstance(sc, dict) else None
            if isinstance(wh, dict):
                wh.pop("user_profile_assignments", None)
