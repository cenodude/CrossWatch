# services/analyzer_mapping.py
# CrossWatch - Mapping corrections from Analyzer pending retries
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import time
from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from cw_platform.access_policy import request_user, user_can_access_pair


class MappingRequest(BaseModel):
    scope: Literal["pair", "shared"] = "pair"
    pair_id: str = Field(min_length=1, max_length=256)
    provider: str = Field(min_length=1, max_length=128)
    feature: Literal["history", "watchlist", "ratings", "progress", "collection"]
    key: str = Field(min_length=1, max_length=1024)
    action: Literal["context", "catalogs", "search", "episodes", "save"] = "context"
    version: str = Field(default="", max_length=64)
    q: str = Field(default="", max_length=200)
    catalog: Literal["destination", "tmdb"] = "destination"
    item: dict[str, Any] = Field(default_factory=dict)


def mapping_row(payload, request):
    from services import analyzer as an

    cfg, user = an._cfg(), request_user(request)
    if user and not user.get("is_admin") and not (user.get("permissions") or {}).get("write"):
        raise HTTPException(403, "Write permission required")
    pair = next((p for p in cfg.get("pairs", []) if an._pair_id(p) == payload.pair_id), None)
    if not pair or not user_can_access_pair(cfg, user, pair):
        raise HTTPException(404, "Sync pair not found")
    analysis = an._cached_analysis(payload.pair_id)
    entries = [entry for entry in analysis.get("attention", {}).get("rows", [])
               if entry.get("unresolved") and entry.get("feature") == payload.feature
               and str(entry.get("provider", "")).casefold() == payload.provider.casefold()
               and payload.key in [entry.get("key"), *(entry.get("keys") or [])]]
    if len(entries) != 1:
        raise HTTPException(409, "This pending item changed. Analyze again before editing its mapping.")
    entry = entries[0]
    aliases = {str(key) for key in [entry.get("key"), *(entry.get("keys") or [])] if key}
    destination = entry.get("target") or payload.provider
    dest_base, dest_instance, explicit_instance = an._split_prov_token_ex(destination)
    directions = an._pair_map({**cfg, "pairs": [pair]}, {})
    candidates = []
    for handle in an._load_state_handles(payload.pair_id, {payload.feature}):
        for source, feature, key, item in an._iter_items(handle["state"]):
            if feature != payload.feature:
                continue
            matches = set(an._alias_keys({**item, "_key": key})) & aliases
            if not matches:
                continue
            for target in directions.get((source, feature), []):
                provider, instance = an._split_prov_token(target)
                if provider != dest_base or (explicit_instance and instance != dest_instance):
                    continue
                src, src_instance = an._split_prov_token(source)
                candidates.append(dict(id="analyzer-item", key=key, item=deepcopy(item), feature=feature,
                                       source=src, source_instance=src_instance, provider=provider,
                                       instance=instance, operation="add"))
    exact = [row for row in candidates if row["key"] == entry.get("key")]
    if exact:
        candidates = exact
    if len(candidates) != 1:
        raise HTTPException(409, "The original source item could not be identified uniquely. Refresh the pair’s snapshots and analyze again.")
    row = candidates[0]
    if row["item"].get("type") not in {"movie", "show", "anime", "season", "episode"}:
        raise HTTPException(400, "This item cannot be mapped")
    version = hashlib.sha256(json.dumps(row, sort_keys=True, default=str).encode()).hexdigest()
    if payload.action != "context" and payload.version != version:
        raise HTTPException(409, "The source item changed. Reopen mapping before continuing.")
    return cfg, row, version


def handle_mapping(payload: MappingRequest, request):
    cfg, row, version = mapping_row(payload, request)
    if payload.action == "context":
        return dict(ok=True, row=row, version=version)
    if payload.action == "catalogs":
        from services.interactive_sync_catalogs import search_catalogs
        from services.interactive_sync_episodes import metadata_key
        return {**search_catalogs(cfg, row), "episode_matching": bool(metadata_key(cfg, row))}
    if payload.action == "search":
        from services.interactive_sync_mapping import search_candidates
        if len(payload.q.strip()) < 2:
            raise HTTPException(400, "Enter at least two characters")
        return search_candidates(cfg, row, payload.q, catalog=payload.catalog)
    if payload.action == "episodes":
        from services.interactive_sync_episodes import suggest_episodes
        return suggest_episodes(cfg, [(row, payload.item)])

    from api.interactiveSyncAPI import prepare_mapping
    from api.editorAPI import _require_instance_scope, _save_policy_manual_batch
    from services.saved_mappings import mapping_details

    _require_instance_scope(cfg, request, row["source"], row["source_instance"])
    key, item, blocks = prepare_mapping(row, payload.item)
    scope = (row["feature"], row["source"], row["source_instance"])
    records = {scope: {key: dict(original_key=row["key"], original=mapping_details(row["item"]),
                               saved_at=int(time.time()), origin="analyzer")}}
    _save_policy_manual_batch([(row["feature"], row["source"], {key: item}, blocks, row["source_instance"])], mappings=records, pair_id=payload.pair_id if payload.scope == "pair" else "")
    return dict(ok=True, key=key)
