# cw_platform/identity_links.py
# CrossWatch - Identity mappings for sync pairs, watcher routes and webhooks
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from cw_platform.provider_instances import (
    get_instance_block,
    list_sync_user_ids,
    normalize_instance_id,
    split_sync_user_id,
)

IDENTITY_PROVIDERS = frozenset({"plex", "emby", "jellyfin", "silo"})
WEBHOOK_IDENTITY_PROVIDERS = frozenset({"plex", "emby", "jellyfin"})


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def clean_identity_map(raw: Any) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, str] = {}
    taken: set[str] = set()
    for src, dst in raw.items():
        src_key = str(src or "").strip()
        dst_key = str(dst or "").strip()
        if not src_key or not dst_key or "~" in src_key or "~" in dst_key or dst_key in taken:
            continue
        out[src_key] = dst_key
        taken.add(dst_key)
    return out


def identities_by_key(cfg: Mapping[str, Any], provider: str, parent: Any) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for inst in list_sync_user_ids(cfg, provider, normalize_instance_id(parent)):
        ref = split_sync_user_id(inst)
        if ref is None:
            continue
        entry = get_instance_block(cfg, provider, inst)
        name = " ".join(str(entry.get("username") or entry.get("user") or "").split())
        if name:
            out[ref[1]] = {"instance": inst, "username": name}
    return out


def mapped_identities(
    cfg: Mapping[str, Any],
    source: str,
    source_parent: Any,
    target: str,
    target_parent: Any,
    mapping: Any,
) -> list[dict[str, str]]:
    src_users = identities_by_key(cfg, source, source_parent)
    dst_users = identities_by_key(cfg, target, target_parent)
    out: list[dict[str, str]] = []
    for src_key, dst_key in clean_identity_map(mapping).items():
        src = src_users.get(src_key)
        dst = dst_users.get(dst_key)
        if src and dst:
            out.append({
                "key": src_key,
                "username": src["username"],
                "source_instance": src["instance"],
                "target_key": dst_key,
                "target_instance": dst["instance"],
            })
    out.sort(key=lambda row: row["username"].casefold())
    return out


def webhook_identity_sinks(settings: Mapping[str, Any] | None) -> set[str]:
    raw = (settings or {}).get("match_users")
    if not isinstance(raw, Mapping):
        return set()
    return {str(k).strip().lower() for k, v in raw.items() if v is True and str(k).strip().lower() in WEBHOOK_IDENTITY_PROVIDERS}


def webhook_identity_map(settings: Mapping[str, Any] | None, sink: str) -> dict[str, str]:
    raw = (settings or {}).get("user_matches")
    if not isinstance(raw, Mapping):
        return {}
    return clean_identity_map(raw.get(str(sink or "").strip().lower()))


def identity_links(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    from cw_platform.provider_usage import provider_label
    from cw_platform.value_coercion import coerce_bool
    from providers.scrobble.routes import normalize_routes
    from providers.webhooks.config import webhook_settings, webhook_sink_instance

    out: list[dict[str, Any]] = []

    def add(src: Any, src_parent: Any, dst: Any, dst_parent: Any, mapping: Any, kind: str, ref: str, label: str) -> None:
        src_p = str(src or "").strip().lower()
        dst_p = str(dst or "").strip().lower()
        if src_p not in IDENTITY_PROVIDERS or dst_p not in IDENTITY_PROVIDERS:
            return
        for src_key, dst_key in clean_identity_map(mapping).items():
            out.append({
                "source": {"provider": src_p, "instance": normalize_instance_id(src_parent), "key": src_key},
                "target": {"provider": dst_p, "instance": normalize_instance_id(dst_parent), "key": dst_key},
                "kind": kind,
                "id": ref,
                "label": label,
            })

    for idx, pair in enumerate(cfg.get("pairs") or []):
        if isinstance(pair, Mapping) and coerce_bool(pair.get("match_users", False), False):
            add(pair.get("source"), pair.get("source_instance"), pair.get("target"), pair.get("target_instance"), pair.get("user_matches"), "pair", str(pair.get("id") or ""), f"Sync pair {idx + 1}")
    for route in normalize_routes(dict(cfg)):
        if route.get("match_users"):
            rid = str(route.get("id") or "")
            add(route.get("provider"), route.get("provider_instance"), route.get("sink"), route.get("sink_instance"), route.get("user_matches"), "route", rid, f"Watcher route {rid}")
    profiles = _mapping(_mapping(_mapping(cfg.get("scrobble")).get("webhook")).get("profiles"))
    for prov, insts in profiles.items():
        if not isinstance(insts, Mapping):
            continue
        for inst in insts:
            settings = webhook_settings(cfg, str(prov), inst)
            for sink in sorted(webhook_identity_sinks(settings)):
                ref = f"{str(prov).lower()}:{normalize_instance_id(inst)}:{sink}"
                add(prov, inst, sink, webhook_sink_instance(settings, sink), webhook_identity_map(settings, sink), "webhook", ref, f"{provider_label(str(prov))} webhook to {provider_label(sink)}")
    return out
