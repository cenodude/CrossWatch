# cw_platform/provider_usage.py
# CrossWatch - Central usage detection for provider profiles referenced by Scrobbling.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cw_platform.provider_instances import list_instance_ids, normalize_instance_id, provider_key
from cw_platform.value_coercion import coerce_bool

__all__ = [
    "find_provider_usage",
    "provider_in_use",
    "usage_conflict_payload",
    "describe_usage",
    "webhook_source_enabled",
    "provider_label",
    "WEBHOOK_SOURCE_PROVIDERS",
]

WEBHOOK_SOURCE_PROVIDERS: tuple[str, ...] = ("plex", "jellyfin", "emby")

_PROVIDER_LABELS = {
    "plex": "Plex",
    "emby": "Emby",
    "jellyfin": "Jellyfin",
    "trakt": "Trakt",
    "simkl": "SIMKL",
    "mdblist": "MDBList",
}


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _label(provider: str, instance: str) -> str:
    key = provider_key(provider)
    name = _PROVIDER_LABELS.get(key, key.upper() or "provider")
    inst = normalize_instance_id(instance)
    return name if inst == "default" else f"{name} {inst}"


def provider_label(provider: str, instance: Any = "default") -> str:
    return _label(provider, instance)


def _raw_routes(cfg: Mapping[str, Any]) -> list[Any]:
    routes = _dict(_dict(_dict(cfg).get("scrobble")).get("watch")).get("routes")
    return list(routes) if isinstance(routes, list) else []


def _watcher_usage(cfg: Mapping[str, Any], provider: str, instance: str) -> list[dict[str, Any]]:
    from providers.scrobble.routes import normalize_route

    out: list[dict[str, Any]] = []
    for index, raw in enumerate(_raw_routes(cfg)):
        if not isinstance(raw, dict):
            continue
        route = normalize_route(raw, f"R{index + 1}")
        route_id = str(route.get("id") or "").strip()
        enabled = bool(route.get("enabled"))
        for role, prov_field, inst_field in (
            ("provider", "provider", "provider_instance"),
            ("sink", "sink", "sink_instance"),
        ):
            if provider_key(str(route.get(prov_field) or "")) != provider:
                continue
            if normalize_instance_id(route.get(inst_field)) != instance:
                continue
            out.append(
                {
                    "feature": "watcher",
                    "role": role,
                    "route_id": route_id,
                    "enabled": enabled,
                    "label": f"Watcher route {route_id}" if route_id else "Watcher route",
                }
            )
    return out


def webhook_source_enabled(cfg: Mapping[str, Any], provider: str, instance: Any = "default") -> bool:
    prov = provider_key(provider)
    inst = normalize_instance_id(instance)
    wh = _dict(_dict(_dict(cfg).get("scrobble")).get("webhook"))

    profile_node = _dict(_dict(_dict(wh.get("profiles")).get(prov)).get(inst))
    if "enabled" in profile_node:
        return coerce_bool(profile_node.get("enabled"))

    if inst == "default":
        provider_node = _dict(_dict(wh.get("providers")).get(prov))
        if "enabled" in provider_node:
            return coerce_bool(provider_node.get("enabled"))

    return True


def _enabled_webhook_sources(cfg: Mapping[str, Any]) -> list[tuple[str, str]]:
    from providers.scrobble.sources import scrobble_sources

    if not scrobble_sources(cfg).get("webhook"):
        return []

    out: list[tuple[str, str]] = []
    for prov in WEBHOOK_SOURCE_PROVIDERS:
        try:
            instances = list_instance_ids(cfg, prov)
        except Exception:
            instances = ["default"]
        for raw_inst in instances:
            inst = normalize_instance_id(raw_inst)
            if webhook_source_enabled(cfg, prov, inst):
                out.append((prov, inst))
    return out


def _webhook_usage(cfg: Mapping[str, Any], provider: str, instance: str) -> list[dict[str, Any]]:
    from providers.webhooks.config import webhook_settings, webhook_sink_instance, webhook_sinks

    out: list[dict[str, Any]] = []
    for source_prov, source_inst in _enabled_webhook_sources(cfg):
        if source_prov == provider and source_inst == instance:
            out.append(
                {
                    "feature": "webhook",
                    "role": "provider",
                    "provider": source_prov,
                    "instance": source_inst,
                    "enabled": True,
                    "label": f"{_label(source_prov, source_inst)} webhook",
                }
            )
            continue

        if provider not in {"trakt", "simkl", "mdblist"}:
            continue

        settings = webhook_settings(cfg, source_prov, source_inst)
        for sink in webhook_sinks(cfg, source_prov, source_inst):
            if provider_key(sink) != provider:
                continue
            if normalize_instance_id(webhook_sink_instance(settings, sink)) != instance:
                continue
            out.append(
                {
                    "feature": "webhook",
                    "role": "sink",
                    "provider": source_prov,
                    "instance": source_inst,
                    "sink": provider,
                    "sink_instance": instance,
                    "enabled": True,
                    "label": f"{_label(source_prov, source_inst)} webhook",
                }
            )
    return out


def find_provider_usage(
    cfg: dict[str, Any],
    provider: str,
    instance_id: str = "default",
) -> list[dict[str, Any]]:
    prov = provider_key(provider)
    if not prov:
        return []
    inst = normalize_instance_id(instance_id)
    cfg_map = cfg if isinstance(cfg, Mapping) else {}

    usages: list[dict[str, Any]] = []
    usages.extend(_watcher_usage(cfg_map, prov, inst))
    usages.extend(_webhook_usage(cfg_map, prov, inst))
    return usages


def provider_in_use(cfg: dict[str, Any], provider: str, instance_id: str = "default") -> bool:
    return bool(find_provider_usage(cfg, provider, instance_id))


def describe_usage(usage: Mapping[str, Any]) -> str:
    feature = str(usage.get("feature") or "").strip().lower()
    role = str(usage.get("role") or "").strip().lower()
    if feature == "watcher":
        route_id = str(usage.get("route_id") or "").strip()
        where = f"Watcher route {route_id}" if route_id else "a Watcher route"
        side = "source" if role == "provider" else "sink"
        state = "" if usage.get("enabled") else " (disabled)"
        return f"{where} ({side}){state}"
    if feature == "webhook":
        source = _label(str(usage.get("provider") or ""), str(usage.get("instance") or "default"))
        if role == "sink":
            return f"{source} webhook (destination)"
        return f"{source} webhook (source)"
    return str(usage.get("label") or "an unknown configuration")


def usage_conflict_payload(
    provider: str,
    instance_id: str,
    usages: list[dict[str, Any]],
) -> dict[str, Any]:
    prov = provider_key(provider)
    inst = normalize_instance_id(instance_id)
    details = [describe_usage(u) for u in usages]
    subject = _label(prov, inst)
    message = (
        f"Cannot delete {subject} because it is used by {', '.join(details)}. "
        "Remove this profile from Watcher or Webhooks first."
        if details
        else f"Cannot delete {subject}."
    )
    return {
        "ok": False,
        "error": "provider_in_use",
        "provider": prov,
        "instance": inst,
        "usages": usages,
        "message": message,
    }
