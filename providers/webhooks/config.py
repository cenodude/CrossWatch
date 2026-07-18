# providers/webhooks/config.py
# CrossWatch - Webhook settings resolver
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from cw_platform.provider_instances import get_provider_block, normalize_instance_id

_SINKS = {"trakt", "simkl", "mdblist"}

_SINK_CREDENTIALS: dict[str, tuple[str, ...]] = {
    "trakt": ("access_token",),
    "simkl": ("access_token",),
    "mdblist": ("api_key", "access_token"),
}

_MEDIA_CREDENTIALS: dict[str, tuple[str, ...]] = {
    "plex": ("account_token", "pms_token"),
    "jellyfin": ("access_token",),
    "emby": ("access_token",),
}

_MISSING = object()


def _dict(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, Mapping) else {}


def _merge(base: Mapping[str, Any] | None, overlay: Mapping[str, Any] | None) -> dict[str, Any]:
    out = copy.deepcopy(dict(base or {}))
    for k, v in dict(overlay or {}).items():
        cur = out.get(k)
        if isinstance(cur, dict) and isinstance(v, Mapping):
            out[k] = _merge(cur, v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def webhook_settings(cfg: Mapping[str, Any] | None, provider: str, provider_instance: Any = None) -> dict[str, Any]:
    sc = _dict(_dict(cfg).get("scrobble"))
    wh = _dict(sc.get("webhook"))
    provider_key = str(provider or "").strip().lower()
    inst = normalize_instance_id(provider_instance)

    out = copy.deepcopy(wh)
    providers = _dict(wh.get("providers"))
    out = _merge(out, _dict(providers.get(provider_key)))

    profiles = _dict(wh.get("profiles"))
    provider_profiles = _dict(profiles.get(provider_key))
    out = _merge(out, _dict(provider_profiles.get(inst)))
    return out


def webhook_sinks(cfg: Mapping[str, Any] | None, provider: str, provider_instance: Any = None) -> list[str]:
    wh = webhook_settings(cfg, provider, provider_instance)
    raw = wh.get("sinks", _MISSING)
    if raw is _MISSING or raw is None:
        return []
    if isinstance(raw, Mapping):
        items = [k for k, v in raw.items() if v not in (False, None, "")]
    else:
        items = raw if isinstance(raw, (list, tuple, set)) else str(raw).split(",")
    out: list[str] = []
    for item in items:
        sink = str(item or "").strip().lower()
        if sink in _SINKS and sink not in out:
            out.append(sink)
    return out


def webhook_sink_instance(settings: Mapping[str, Any] | None, sink: str) -> str:
    wh = settings if isinstance(settings, Mapping) else {}
    instances = wh.get("sink_instances")
    instances = instances if isinstance(instances, Mapping) else {}
    return normalize_instance_id(str(instances.get(str(sink or "").strip().lower()) or "default"))


def sink_configured(cfg: Mapping[str, Any] | None, sink: str, instance_id: Any = None) -> bool:
    key = str(sink or "").strip().lower()
    fields = _SINK_CREDENTIALS.get(key)
    if not fields:
        return False
    inst = normalize_instance_id(instance_id)
    block = get_provider_block(_dict(cfg), key, inst)
    if not block and inst != "default":
        block = get_provider_block(_dict(cfg), key, "default")
    sources = [block]
    if inst == "default":
        sources.append(_dict(_dict(_dict(cfg).get("auth")).get(key)))
    for src in sources:
        for field in fields:
            if str(src.get(field) or "").strip():
                return True
    return False


def configured_webhook_sinks(cfg: Mapping[str, Any] | None, provider: str, provider_instance: Any = None) -> list[str]:
    wh = webhook_settings(cfg, provider, provider_instance)
    return [
        sink
        for sink in webhook_sinks(cfg, provider, provider_instance)
        if sink_configured(cfg, sink, webhook_sink_instance(wh, sink))
    ]


def media_source_connected(cfg: Mapping[str, Any] | None, provider: str, instance_id: Any = None) -> bool:
    key = str(provider or "").strip().lower()
    fields = _MEDIA_CREDENTIALS.get(key)
    if not fields:
        return False
    block = get_provider_block(_dict(cfg), key, normalize_instance_id(instance_id))
    return any(str(block.get(field) or "").strip() for field in fields)


def active_webhook_endpoints(cfg: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    from cw_platform.provider_instances import list_instance_ids
    from cw_platform.provider_usage import WEBHOOK_SOURCE_PROVIDERS, webhook_source_enabled
    from providers.scrobble.sources import source_enabled

    if not source_enabled(cfg, "webhook"):
        return []

    out: list[dict[str, Any]] = []
    for provider in WEBHOOK_SOURCE_PROVIDERS:
        try:
            instances = list_instance_ids(_dict(cfg), provider)
        except Exception:
            instances = ["default"]
        for raw_inst in instances:
            inst = normalize_instance_id(raw_inst)
            if not webhook_source_enabled(_dict(cfg), provider, inst):
                continue
            if not media_source_connected(cfg, provider, inst):
                continue
            selected = webhook_sinks(cfg, provider, inst)
            if not selected:
                continue
            ready = configured_webhook_sinks(cfg, provider, inst)
            out.append(
                {
                    "provider": provider,
                    "instance": inst,
                    "sinks": selected,
                    "ready": ready,
                    "missing": [s for s in selected if s not in ready],
                }
            )
    return out


def describe_active_webhooks(cfg: Mapping[str, Any] | None) -> list[tuple[str, str]]:
    from cw_platform.provider_usage import provider_label
    from providers.scrobble.sources import source_enabled

    if not source_enabled(cfg, "webhook"):
        return []

    endpoints = active_webhook_endpoints(cfg)
    if not endpoints:
        return [("Webhook source enabled but no endpoints are active", "WARN")]

    lines: list[tuple[str, str]] = []
    for ep in endpoints:
        name = provider_label(ep["provider"], ep["instance"])
        ready = [provider_label(s) for s in ep["ready"]]
        missing = [provider_label(s) for s in ep["missing"]]
        if not ep["sinks"]:
            lines.append((f"{name} webhook active -> no destinations selected", "WARN"))
            continue
        target = ", ".join(ready) if ready else "nothing"
        message = f"{name} webhook active -> {target}"
        if missing:
            lines.append((f"{message} | not connected: {', '.join(missing)}", "WARN"))
        else:
            lines.append((message, "INFO"))
    return lines


def apply_webhook_settings(cfg: Mapping[str, Any] | None, provider: str, provider_instance: Any = None) -> dict[str, Any]:
    out = copy.deepcopy(dict(cfg or {}))
    sc = out.setdefault("scrobble", {})
    if not isinstance(sc, dict):
        sc = {}
        out["scrobble"] = sc
    sc["webhook"] = webhook_settings(out, provider, provider_instance)
    return out
