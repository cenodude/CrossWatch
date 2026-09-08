# services/editor_mapping.py
# CrossWatch - Editor mapping workspace and scope validation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from cw_platform.access_policy import request_user, user_can_access_pair
from cw_platform.provider_instances import normalize_instance_id


def mapping_pairs(cfg, request, provider, instance, feature):
    return [pair for pair in cfg.get("pairs", [])
            if pair.get("id") and user_can_access_pair(cfg, request_user(request), pair)
            and feature in (pair.get("features") or {})
            and any(str(pair.get(side) or "").upper() == provider.upper()
                    and normalize_instance_id(pair.get(f"{side}_instance")) == instance
                    for side in ("source", "target"))]


def require_mapping_pair(cfg, request, pair_id, provider, instance, feature):
    if not pair_id:
        return None
    pair = next((p for p in mapping_pairs(cfg, request, provider, instance, feature)
                 if str(p["id"]) == pair_id), None)
    if pair is None:
        raise HTTPException(404, "Sync pair not found for this provider instance and feature")
    return pair


def scope_options(cfg, request, provider, instance, feature):
    return [dict(id=str(p["id"]), label=str(p.get("name") or p.get("label") or
                 f"Pair {cfg['pairs'].index(p) + 1} · {p.get('source')} → {p.get('target')}"))
            for p in mapping_pairs(cfg, request, provider, instance, feature)]


class MappingRequest(BaseModel):
    provider: str = Field(min_length=1, max_length=128)
    instance: str = Field(default="default", max_length=128)
    pair_id: str = Field(default="", max_length=256)
    feature: Literal["history", "watchlist", "ratings", "progress", "collection"]
    key: str = Field(min_length=1, max_length=1024)
    original: dict[str, Any]
    action: Literal["catalogs", "search", "episodes", "prepare"]
    q: str = Field(default="", max_length=200)
    catalog: Literal["destination", "tmdb"] = "tmdb"
    item: dict[str, Any] = Field(default_factory=dict)


class BlockRequest(BaseModel):
    provider: str = Field(min_length=1, max_length=128)
    instance: str = Field(default="default", max_length=128)
    pair_id: str = Field(default="", max_length=256)
    feature: Literal["history", "watchlist", "ratings", "progress", "collection"]
    key: str = Field(min_length=1, max_length=1024)
    blocked: bool


def update_block(payload, request):
    from api.editorAPI import load_config, _require_instance_scope, _STATE_BASE
    from cw_platform.local_db import manual_policy
    from cw_platform.mapping_policy import feature_node
    from services.saved_mappings import correction_block_keys

    cfg, user = load_config(), request_user(request)
    if user and not user.get("is_admin") and not (user.get("permissions") or {}).get("write"):
        raise HTTPException(403, "Write permission required")
    provider, instance = payload.provider.strip().upper(), normalize_instance_id(payload.instance)
    _require_instance_scope(cfg, request, provider, instance)
    require_mapping_pair(cfg, request, payload.pair_id, provider, instance, payload.feature)
    key = payload.key.strip()
    if not key or any(ord(c) < 32 for c in key):
        raise HTTPException(400, "Enter a valid item key")

    def apply(policy):
        scoped = policy.setdefault("pairs", {}).setdefault(payload.pair_id, {"version": 1, "providers": {}}) if payload.pair_id else policy
        node = feature_node(scoped, provider, instance, payload.feature)
        if not payload.blocked and key.lower() in correction_block_keys(node):
            raise HTTPException(409, "This block belongs to a saved correction. Edit or remove that mapping instead.")
        blocks = [value for value in node.get("blocks") or [] if value.lower() != key.lower()]
        node["blocks"] = [*blocks, key] if payload.blocked else blocks

    manual_policy.update_policy(_STATE_BASE, apply)
    return dict(ok=True, key=key, blocked=payload.blocked)


def handle_mapping(payload, request):
    from api.editorAPI import load_config, _require_instance_scope
    from api.interactiveSyncAPI import prepare_mapping
    from services.interactive_sync_catalogs import search_catalogs
    from services.interactive_sync_mapping import search_candidates
    from services.interactive_sync_episodes import metadata_key, suggest_episodes

    cfg = load_config()
    user = request_user(request)
    if user and not user.get("is_admin") and not (user.get("permissions") or {}).get("write"):
        raise HTTPException(403, "Write permission required")
    provider, instance = payload.provider.upper(), normalize_instance_id(payload.instance)
    _require_instance_scope(cfg, request, provider, instance)
    pair = require_mapping_pair(cfg, request, payload.pair_id, provider, instance, payload.feature)
    destination, target_instance = provider, instance
    if pair:
        side = "target" if str(pair.get("source") or "").upper() == provider and normalize_instance_id(pair.get("source_instance")) == instance else "source"
        destination, target_instance = str(pair[side]).upper(), normalize_instance_id(pair.get(f"{side}_instance"))
    row = dict(id="editor-item", key=payload.key, item=payload.original, feature=payload.feature,
               source=provider, source_instance=instance, provider=destination, instance=target_instance,
               operation="add", metadata_only=not bool(pair))
    if payload.action == "catalogs":
        return {**search_catalogs(cfg, row), "episode_matching": bool(metadata_key(cfg, row)), "row": row}
    if payload.action == "search":
        if len(payload.q.strip()) < 2:
            raise HTTPException(400, "Enter at least two characters")
        return search_candidates(cfg, row, payload.q, catalog=payload.catalog)
    if payload.action == "episodes":
        return suggest_episodes(cfg, [(row, payload.item)])
    key, item, blocks = prepare_mapping(row, payload.item)
    return dict(ok=True, key=key, item=item, blocks=blocks)
