# services/editor_merged.py
# CrossWatch - Merged Editor view across the provider instances of one profile
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from cw_platform.access_policy import (
    managed_profile_id, pair_refs, profile_instances_map, profile_label_for_id, request_user,
)
from cw_platform.history_events import EVENT_ID_FIELDS, base_key_from_history_event
from cw_platform.id_map import canonical_key, merge_ids, minimal
from cw_platform.orchestrator._pairs import _feature_list_for_pair
from cw_platform.provider_instances import list_user_profiles, normalize_instance_id
from services.editor_removal import LOCAL_ID_NAMESPACES, _tokens

VALUE_FIELDS = {
    "history": "watched_at",
    "ratings": "rating",
    "progress": "progress_at",
    "collection": "collected_at",
}
EVENT_FIELDS = (*EVENT_ID_FIELDS, "_cw_event_key", "_cw_rewatch_sync", "_mdblist_play_id", "play_id")


def _scope(cfg: Mapping[str, Any], request: Any) -> tuple[str, dict[str, list[str]] | None, bool]:
    user = request_user(request)
    chooser = not isinstance(user, Mapping) or bool(user.get("is_admin")) or bool(user.get("view_as"))
    profile_id = managed_profile_id(user)
    if not profile_id:
        return "", None, chooser
    return profile_id, profile_instances_map(cfg, profile_id), chooser


def _in_scope(instances: Mapping[str, list[str]] | None, provider: str, instance: str) -> bool:
    if instances is None:
        return instance == "default"
    return instance in list(instances.get(provider) or [])


def _linked(cfg: Mapping[str, Any], feature: str, instances: Mapping[str, list[str]] | None) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for pair in cfg.get("pairs") or []:
        if not isinstance(pair, Mapping) or pair.get("enabled") is False or feature not in _feature_list_for_pair(pair):
            continue
        left, right = pair_refs(pair)
        inside = [_in_scope(instances, *left), _in_scope(instances, *right)]
        if inside == [True, False]:
            out.add(right)
        elif inside == [False, True]:
            out.add(left)
    return out


def _paired(cfg: Mapping[str, Any], feature: str) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = {("CROSSWATCH", "default")}
    for pair in cfg.get("pairs") or []:
        if not isinstance(pair, Mapping) or pair.get("enabled") is False or feature not in _feature_list_for_pair(pair):
            continue
        out.update(pair_refs(pair))
    return out


def _shared_tokens(key: str, item: Mapping[str, Any]) -> set[str]:
    if str(item.get("type") or "").lower() == "anime":
        item = {**item, "type": "show"}
    tokens = {token for token in _tokens(key, item)
              if token.split("|", 1)[1].split(":", 1)[0] not in LOCAL_ID_NAMESPACES}
    if tokens:
        return tokens
    base = str(base_key_from_history_event(key) or key or "").strip().lower()
    return {f"key|{base}"} if base else set()


def _display_item(feature: str, records: list[tuple[int, str, Mapping[str, Any]]]) -> dict[str, Any]:
    best = max(records, key=lambda record: len(record[2].get("ids") or {}))[2]
    item = minimal(best)
    for field in ("show_ids", "series_year", "simkl_bucket", "anime_type"):
        if best.get(field) not in (None, "", {}):
            item[field] = best.get(field)
    for field in EVENT_FIELDS:
        item.pop(field, None)
    ids: dict[str, str] = {}
    show_ids: dict[str, str] = {}
    for _, _, record in records:
        if isinstance(record.get("ids"), Mapping):
            ids = merge_ids(ids, record["ids"])
        if isinstance(record.get("show_ids"), Mapping):
            show_ids = merge_ids(show_ids, record["show_ids"])
    if str(item.get("type") or "") not in ("episode", "season"):
        item["ids"] = ids or item.get("ids") or {}
    if show_ids:
        item["show_ids"] = show_ids
    field = VALUE_FIELDS.get(feature)
    if field:
        values = [record.get(field) for _, _, record in records if record.get(field) not in (None, "")]
        if values:
            item[field] = max(values, key=str) if field != "rating" else values[0]
    return item


def merge_sources(feature: str, sources: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], dict[str, list[list[Any]]]]:
    parent: list[int] = []
    owners: dict[str, int] = {}
    records: list[tuple[int, str, Mapping[str, Any]]] = []

    def find(node: int) -> int:
        while parent[node] != node:
            parent[node] = parent[parent[node]]
            node = parent[node]
        return node

    for index, items in enumerate(sources):
        for raw_key, raw in (items or {}).items():
            if not isinstance(raw, Mapping):
                continue
            key = str(raw_key or "")
            tokens = _shared_tokens(key, raw)
            if not tokens:
                continue
            node = len(parent)
            parent.append(node)
            records.append((index, key, raw))
            for token in tokens:
                other = owners.get(token)
                if other is None:
                    owners[token] = node
                else:
                    parent[find(node)] = find(other)

    groups: dict[int, list[tuple[int, str, Mapping[str, Any]]]] = {}
    for node, record in enumerate(records):
        groups.setdefault(find(node), []).append(record)

    field = VALUE_FIELDS.get(feature)
    items_out: dict[str, Any] = {}
    presence: dict[str, list[list[Any]]] = {}
    for members in groups.values():
        item = _display_item(feature, members)
        key = str(canonical_key(item) or "").strip().lower() or str(members[0][1]).lower()
        base, serial = key, 1
        while key in items_out:
            serial += 1
            key = f"{base}~{serial}"
        items_out[key] = item
        per_source: dict[int, list[Any]] = {}
        for index, _, record in members:
            entry = per_source.setdefault(index, [index, 0, None])
            entry[1] += 1
            value = record.get(field) if field else None
            if value not in (None, "") and (entry[2] is None or field == "rating" or str(value) > str(entry[2])):
                entry[2] = value
        presence[key] = [per_source[index] for index in sorted(per_source)]
    return items_out, presence


def merged_view(kind: str, request: Any = None) -> dict[str, Any]:
    from api import editorAPI as api
    from services.episode_groups import editor_badges, editor_group_index

    feature = api._normalize_kind(kind)
    cfg = api.load_config() or {}
    profile_id, instances, chooser = _scope(cfg, request)
    raw_state = api._load_current_state_features({feature})
    raw_policy = api._load_policy()

    linked = _linked(cfg, feature, instances) if chooser else set()
    targets: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for target in api._editor_send_targets(cfg, feature):
        provider = str(target.get("provider") or "").upper()
        instance = normalize_instance_id(target.get("instance"))
        inside = _in_scope(instances, provider, instance)
        if not inside and (provider, instance) not in linked:
            continue
        seen.add((provider, instance))
        targets.append({"provider": provider, "instance": instance, "label": target.get("label") or provider,
                        "instance_label": target.get("instance_label") or instance,
                        "display": target.get("display") or provider, "can_send": inside or instances is None,
                        "linked": not inside})

    names = [str(name or "").strip().upper() for name in api._union_providers(raw_state, raw_policy)]
    names.extend(name.upper() for name in api._always_listed_providers())
    for provider in dict.fromkeys(name for name in names if name):
        scoped = ["default"] if instances is None else list(instances.get(provider) or [])
        scoped.extend(instance for name, instance in sorted(linked) if name == provider)
        for instance in scoped:
            if (provider, instance) in seen:
                continue
            seen.add((provider, instance))
            label = api._provider_instance_label(cfg, provider, instance)
            targets.append({"provider": provider, "instance": instance, "label": provider.title(),
                            "instance_label": label,
                            "display": provider.title() if instance == "default" else f"{provider.title()} ({label})",
                            "can_send": False, "linked": (provider, instance) in linked})

    paired = _paired(cfg, feature)
    for target in targets:
        target["verified"] = target["provider"] == "CROSSWATCH" or (target["provider"], target["instance"]) in paired
        owners = api._instance_owner_labels(cfg, target["provider"], target["instance"])
        target["shared_with"] = owners if len(owners) > 1 else []

    sources = [api._load_state_items(feature, target["provider"], target["instance"], raw_state=raw_state)
               for target in targets]
    items, presence = merge_sources(feature, sources)
    group_badges = editor_badges(items, editor_group_index(cfg, request, raw_policy, feature),
        {key: [(targets[entry[0]]["provider"], targets[entry[0]]["instance"]) for entry in rows]
         for key, rows in presence.items()})
    used = {entry[0] for rows in presence.values() for entry in rows}
    keep = [index for index, target in enumerate(targets) if target["can_send"] or index in used]
    remap = {old: new for new, old in enumerate(keep)}
    presence = {key: [[remap[entry[0]], entry[1], entry[2]] for entry in rows] for key, rows in presence.items()}

    profiles = [{"id": str(row.get("id") or ""), "label": str(row.get("label") or row.get("id") or "")}
                for row in list_user_profiles(cfg)] if chooser else []
    return {
        "ok": True,
        "kind": feature,
        "source": "merged",
        "ts": api._state_mtime() if api._state_exists() else None,
        "count": len(items),
        "items": items,
        "presence": presence,
        "episode_groups": group_badges,
        "targets": [targets[index] for index in keep],
        "profile": profile_id,
        "profile_label": profile_label_for_id(cfg, profile_id) if profile_id else "",
        "profiles": profiles,
    }
