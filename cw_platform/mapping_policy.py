# cw_platform/mapping_policy.py
# CrossWatch - Shared and pair-specific mapping policy
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy

from .id_map import keys_for_item
from .local_db.manual_policy import _feature_blocks, _normalize_blocks


def feature_node(policy, provider, instance, feature):
    providers = policy.setdefault("providers", {})
    name = next((key for key in providers if key.upper() == provider.upper()), provider.upper())
    node = providers.setdefault(name, {})
    if instance != "default":
        node = node.setdefault("instances", {}).setdefault(instance, {})
    return node.setdefault(feature, {})


def effective_policy(policy, pair_id=""):
    result = {"version": policy.get("version", 1), "providers": deepcopy(policy.get("providers") or {})}
    scoped = (policy.get("pairs") or {}).get(pair_id) or {}
    for provider, instance, feature, override in _feature_blocks(scoped):
        node = feature_node(result, provider, instance, feature)
        items = node.setdefault("adds", {}).setdefault("items", {})
        records = node.setdefault("mappings", {})
        blocks = list(node.get("blocks") or [])
        for target, record in (override.get("mappings") or {}).items():
            original = record.get("original_key")
            for key, shared in list(records.items()):
                if original and (key == original or shared.get("original_key") == original):
                    items.pop(key, None)
                    records.pop(key, None)
                    if key != target:
                        blocks.append(key)
        items.update(deepcopy((override.get("adds") or {}).get("items") or {}))
        records.update(deepcopy(override.get("mappings") or {}))
        corrected = {alias.lower() for key in (override.get("mappings") or {})
                     for alias in [key, *keys_for_item(items.get(key) or {})]}
        node["blocks"] = _normalize_blocks([
            *(key for key in blocks if key.lower() not in corrected),
            *(override.get("blocks") or []),
        ])
    return result


def update_mappings(raw, edits, *, mappings=None, pair_id="", merge=True):
    effective = effective_policy(raw, pair_id)
    scoped = raw.setdefault("pairs", {}).setdefault(pair_id, {"version": 1, "providers": {}}) if pair_id else raw
    for feature, provider, additions, blocks, instance in edits:
        node = feature_node(scoped, provider, instance, feature)
        items = dict((node.get("adds") or {}).get("items") or {}) if merge else {}
        records = dict(node.get("mappings") or {})
        previous = (feature_node(effective, provider, instance, feature).get("mappings") or {})
        incoming_records = (mappings or {}).get((feature, provider, instance), {})
        for target, incoming in incoming_records.items():
            record = deepcopy(incoming)
            replaced = record.get("original_key")
            if replaced != target:
                items.pop(replaced, None)
                records.pop(replaced, None)
            parent = previous.get(record.get("original_key")) or previous.get(target)
            if isinstance(parent, dict) and parent.get("original"):
                record.update(original=parent["original"], original_key=parent["original_key"])
            for key, old in list(records.items()):
                if key != target and record.get("original_key") and old.get("original_key") == record["original_key"]:
                    items.pop(key, None)
                    records.pop(key, None)
            records[target] = record
            if record.get("original_key") and record["original_key"] != target and record["original_key"] not in keys_for_item(additions.get(target) or {}):
                blocks = [*blocks, record["original_key"]]
        items.update(additions)
        node["adds"] = {"items": items}
        node["blocks"] = _normalize_blocks([*(node.get("blocks") or []), *blocks] if merge else blocks)
        corrected = {alias.lower() for key in incoming_records for alias in [key, *keys_for_item(items.get(key) or {})]}
        node["blocks"] = [key for key in node["blocks"] if key.lower() not in corrected]
        node["mappings"] = {key: value for key, value in records.items() if key in items}
        # A full Editor save can retain a correction without resending its hidden
        # original row. Keep excluding that original while the mapping exists.
        for target, record in node["mappings"].items():
            original = record.get("original_key")
            if original and original != target and original not in keys_for_item(items[target]):
                node["blocks"] = _normalize_blocks([*node["blocks"], original])
