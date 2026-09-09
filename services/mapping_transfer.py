# services/mapping_transfer.py
# CrossWatch - saved mapping and block management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
import json
import math
from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cw_platform.id_map import keys_for_item
from cw_platform.local_db import manual_policy
from cw_platform.mapping_policy import feature_node
from cw_platform.provider_instances import normalize_instance_id
from services.editor_mapping import require_mapping_pair
from services.saved_mappings import correction_block_keys, saved_blocks, saved_corrections

MAX_BYTES = 20 * 1024 * 1024
MAX_RECORDS = 50000


class RuleIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    provider: str = Field(min_length=1, max_length=128)
    instance: str = Field(default="default", max_length=128)
    pair_id: str = Field(default="", max_length=256)
    feature: Literal["watchlist", "history", "ratings", "progress", "collection"]
    key: str = Field(min_length=1, max_length=1024)
    entry_type: Literal["mapping", "block"]

    @field_validator("provider", "instance", "pair_id", "key")
    @classmethod
    def clean_identity(cls, value: str, info):
        value = value.strip()
        if any(ord(char) < 32 for char in value) or (not value and info.field_name in {"provider", "key"}):
            raise ValueError("Invalid rule identity")
        if info.field_name == "provider":
            return value.upper()
        return normalize_instance_id(value) if info.field_name == "instance" else value


def validate_item(item: dict[str, Any] | None) -> dict[str, Any] | None:
    if item is None:
        return None
    json.dumps(item, allow_nan=False)
    strings = {"type", "title", "name", "series_title", "show_title", "watched_at", "last_watched_at",
               "collected_at", "rated_at", "user_rated_at", "progress_at", "progress_at_source",
               "provider_item_id", "provider_event_id"}
    integers = {"year", "season", "episode", "progress_ms", "duration_ms"}
    numbers = {"rating", "user_rating", "progress_percent"}
    for key, value in item.items():
        if key in {"ids", "show_ids"}:
            if not isinstance(value, dict) or len(value) > 100 or any(
                not isinstance(k, str) or len(k) > 128 or type(v) not in (str, int) or len(str(v)) > 1024
                for k, v in value.items()
            ):
                raise ValueError("Invalid item IDs")
        elif value is None and key in strings | integers | numbers | {"watched"}:
            continue
        elif key in strings and isinstance(value, str) and len(value) <= 4096:
            continue
        elif key in integers and type(value) is int and -(2**63) <= value < 2**63:
            continue
        elif key in numbers and isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
            continue
        elif key == "watched" and type(value) is bool:
            continue
        elif key in strings | integers | numbers | {"watched"}:
            raise ValueError("Invalid item field")
    return item


class TransferRule(RuleIdentity):
    item: dict[str, Any] | None = None
    original: dict[str, Any] | None = None
    original_key: str | None = Field(default=None, max_length=1024)
    saved_at: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    origin: str | None = Field(default=None, max_length=128)

    @field_validator("item", "original")
    @classmethod
    def check_item(cls, value):
        return validate_item(value)

    @field_validator("original_key")
    @classmethod
    def check_original_key(cls, value):
        if value is not None and (not value.strip() or any(ord(c) < 32 for c in value)):
            raise ValueError("Invalid original key")
        return value

    @model_validator(mode="after")
    def check_record(self):
        if self.entry_type == "mapping" and not self.item:
            raise ValueError("A mapping needs an item")
        if self.entry_type == "block" and any(value is not None for value in
                (self.item, self.original, self.original_key, self.saved_at, self.origin)):
            raise ValueError("A block only needs its identity")
        return self


class RuleBundle(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    format: Literal["crosswatch-mappings-blocks"]
    version: Literal[1]
    records: list[TransferRule] = Field(max_length=MAX_RECORDS)


def authorize_rules(request, rules):
    from api import editorAPI as api
    cfg, user = api.load_config(), api.request_user(request)
    if user and not user.get("is_admin") and not (user.get("permissions") or {}).get("write"):
        raise HTTPException(403, "Write permission required")
    for rule in rules:
        api._require_instance_scope(cfg, request, rule.provider, rule.instance)
        require_mapping_pair(cfg, request, rule.pair_id, rule.provider, rule.instance, rule.feature)


def rule_node(policy, rule):
    scoped = policy.setdefault("pairs", {}).setdefault(rule.pair_id, {"version": 1, "providers": {}}) if rule.pair_id else policy
    return feature_node(scoped, rule.provider, rule.instance, rule.feature)


def delete_rule(request, rule: RuleIdentity):
    from api import editorAPI as api
    authorize_rules(request, [rule])

    def apply(policy):
        node = rule_node(policy, rule)
        blocks = node.get("blocks") or []
        if rule.entry_type == "block":
            if rule.key.lower() in correction_block_keys(node):
                raise HTTPException(409, "This block belongs to a correction. Delete that mapping instead.")
            if not any(key.lower() == rule.key.lower() for key in blocks):
                raise HTTPException(404, "Block no longer exists")
            node["blocks"] = [key for key in blocks if key.lower() != rule.key.lower()]
            return
        items = (node.get("adds") or {}).get("items") or {}
        if rule.key not in items:
            raise HTTPException(404, "Mapping no longer exists")
        automatic = correction_block_keys(node)
        del items[rule.key]
        (node.get("mappings") or {}).pop(rule.key, None)
        removed = automatic - correction_block_keys(node)
        node["blocks"] = [key for key in blocks if key.lower() not in removed]

    manual_policy.update_policy(api._STATE_BASE, apply)
    return {"ok": True}


def export_rules(request, *, provider="", instance="", feature="", pair_id="", user_profile=""):
    from api import editorAPI as api
    from cw_platform.access_policy import profile_instances_map, profile_allows_instance, user_can_access_pair
    from cw_platform.provider_instances import normalize_user_profile_id

    authorize_rules(request, [])
    cfg, user = api.load_config(), api.request_user(request)
    profile = normalize_user_profile_id(user_profile)
    if user_profile.strip() and not profile:
        raise HTTPException(400, "Invalid profile")
    scope = profile_instances_map(cfg, profile) if profile else {}
    pairs = {str(p.get("id")) for p in cfg.get("pairs", []) if user_can_access_pair(cfg, user, p)}
    policy = api._load_policy()
    records: list[dict[str, Any]] = []
    for row in [*saved_corrections(policy), *saved_blocks(policy)]:
        if (not api.user_can_access_instance(cfg, user, row["provider"], row["instance"])
                or (row["pair_id"] and row["pair_id"] not in pairs)
                or (profile and not profile_allows_instance(scope, row["provider"], row["instance"]))):
            continue
        if ((provider and row["provider"].casefold() != provider.casefold())
                or (instance and row["instance"] != instance) or (feature and row["feature"] != feature)
                or (pair_id and row["pair_id"] != ("" if pair_id == "shared" else pair_id))):
            continue
        identity = RuleIdentity.model_validate({
            **{key: row[key] for key in ("provider", "instance", "feature", "pair_id", "key")},
            "entry_type": row.get("entry_type", "mapping"),
        })
        record = identity.model_dump()
        if identity.entry_type == "mapping":
            node = rule_node(policy, identity)
            record.update(item=deepcopy(node["adds"]["items"][identity.key]))
            record.update({key: row[key] for key in ("original", "original_key", "saved_at", "origin")})
        records.append(record)
    if len(records) > MAX_RECORDS:
        raise HTTPException(400, "Export is too large. Select a source or feature and try again.")
    data = RuleBundle.model_validate(dict(format="crosswatch-mappings-blocks", version=1, records=records)).model_dump(exclude_none=True)
    content = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    if len(content) > MAX_BYTES:
        raise HTTPException(400, "Export is too large. Select a source or feature and try again.")
    return content


def import_rules(request, bundle: RuleBundle):
    from api import editorAPI as api
    authorize_rules(request, bundle.records)

    def apply(policy):
        imported = skipped = 0
        # Import mappings first so their automatic blocks cannot become standalone rules.
        for rule in sorted(bundle.records, key=lambda row: row.entry_type == "block"):
            node = rule_node(policy, rule)
            items = node.setdefault("adds", {}).setdefault("items", {})
            mappings = node.setdefault("mappings", {})
            blocks = node.setdefault("blocks", [])
            if rule.entry_type == "block":
                if rule.key.lower() in {key.lower() for key in blocks}:
                    skipped += 1
                    continue
                blocks.append(rule.key)
            else:
                if (rule.key in items or (rule.original_key and any(
                        record.get("original_key") == rule.original_key for record in mappings.values()))):
                    skipped += 1
                    continue
                items[rule.key] = deepcopy(rule.item)
                metadata = rule.model_dump(include={"original", "original_key", "saved_at", "origin"}, exclude_none=True)
                if metadata:
                    mappings[rule.key] = metadata
                if (rule.original_key and rule.original_key != rule.key
                        and rule.original_key not in keys_for_item(rule.item or {})
                        and rule.original_key.lower() not in {key.lower() for key in blocks}):
                    blocks.append(rule.original_key)
            imported += 1
        return dict(ok=True, imported=imported, skipped=skipped)

    _, result = manual_policy.update_policy(api._STATE_BASE, apply)
    return result
