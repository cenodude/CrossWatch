# services/assistant_state.py
# CrossWatch - Scoped read-only state evidence for the assistant
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, Request

from cw_platform.access_policy import (
    filter_pairs_for_profile, filter_pairs_for_user, managed_profile_id, pair_refs,
    request_user, user_can_access_instance,
)
from cw_platform.config_base import CONFIG, load_config
from cw_platform.local_db import state as state_db
from cw_platform.pair_scope import pair_feature_scope
from cw_platform.provider_instances import list_user_profiles
from cw_platform.value_coercion import coerce_bool

FEATURES = ("watchlist", "history", "ratings", "progress", "collection")
FIELDS = ("title", "series_title", "show_title", "type", "year", "season", "episode", "rating", "progress", "watched_at", "rated_at", "added_at", "plays")
IDS = ("tmdb", "tvdb", "imdb", "trakt", "simkl", "anilist", "mal", "anidb", "kitsu", "mdblist")
CAUTION = "Saved state, not a live provider read or proof of a successful write. Missing items may be unobserved. Timestamps describe stored state; missing timestamps mean freshness is unknown."
PARAMS = {"pair_id": {"type": "string"}, "profile_id": {"type": "string"}, "feature": {"type": "string", "enum": list(FEATURES)}}
TOOLS = {
    "list_sync_context": ("List accessible profiles and sync pairs with source/target accounts and enabled features. Use to resolve whose data the user means before reading items.",
                          {"type": "object", "properties": {"profile_id": {"type": "string"}}}),
    "search_sync_items": ("Search stored items on both sides of one pair. Returns saved account inventory and pair comparison state separately. Does not contact providers. Use exact pair_id and feature from list_sync_context.",
                          {"type": "object", "properties": {**PARAMS, "query": {"type": "string", "maxLength": 256}, "offset": {"type": "integer", "minimum": 0}, "limit": {"type": "integer", "minimum": 1, "maximum": 25}}, "required": ["pair_id", "feature"]}),
    "explain_sync_item": ("Gather evidence for an exact stored item key in a pair: observed state on both sides, effective saved mappings/blocks and recent matching event groups. Evidence may be incomplete; never infer sync success from presence alone.",
                          {"type": "object", "properties": {**PARAMS, "item_key": {"type": "string", "maxLength": 1024}}, "required": ["pair_id", "feature", "item_key"]}),
}


def _scope(request: Request, args: dict[str, Any]):
    cfg = load_config() or {}
    user = request_user(request)
    profiles = list_user_profiles(cfg)
    allowed_profile = managed_profile_id(user)
    if user and not user.get("is_admin"):
        profiles = [p for p in profiles if p.get("id") == allowed_profile]
    profile = str(args.get("profile_id") or "")
    if profile and not any(p.get("id") == profile for p in profiles):
        raise HTTPException(404, "Profile not found or unavailable")
    pairs = filter_pairs_for_user(cfg, user, [p for p in cfg.get("pairs", []) if isinstance(p, dict)])
    if profile:
        pairs = filter_pairs_for_profile(cfg, profile, pairs)
    return cfg, profiles, pairs


def _features(pair):
    return [name for name, value in (pair.get("features") or {}).items() if name in FEATURES
            and coerce_bool(value.get("enabled", value.get("enable", True)) if isinstance(value, dict) else value, False)]


def _pair_row(pair):
    refs = pair_refs(pair)
    return {"id": pair.get("id"), "label": pair.get("name") or pair.get("label") or f"{refs[0][0]} → {refs[1][0]}",
            "profile_id": pair.get("profile_id") or "", "enabled": coerce_bool(pair.get("enabled", True), True),
            "mode": pair.get("mode"), "features": _features(pair),
            "source": dict(provider=refs[0][0], instance=refs[0][1]), "target": dict(provider=refs[1][0], instance=refs[1][1])}


def _item(key, item):
    item = item if isinstance(item, dict) else {}
    out: dict[str, Any] = {k: v[:300] if isinstance(v, str) else v for k, v in item.items()
           if k in FIELDS and isinstance(v, (str, int, float, bool))}
    out["key"] = str(key)[:1024]
    out["ids"] = {k: str(v)[:100] for k, v in (item.get("ids") or {}).items() if k in IDS and isinstance(v, (str, int))}
    return out


def _block(state, provider, instance, feature):
    node = (state.get("providers") or {}).get(provider) or {}
    if instance != "default":
        node = (node.get("instances") or {}).get(instance) or {}
    return node.get(feature) or {}


def _evidence(cfg, request, pair, feature, query="", key="", offset=0, limit=10):
    for provider, instance in pair_refs(pair):
        if not user_can_access_instance(cfg, request_user(request), provider, instance):
            raise HTTPException(403, "Account unavailable")
    inventory = state_db.load_state_features(CONFIG, {feature})
    index = next(i for i, p in enumerate(cfg["pairs"], 1) if p.get("id") == pair.get("id"))
    scope = pair_feature_scope(cfg, pair, feature, index)
    comparison = state_db.load_pair_state(CONFIG, scope, {feature})
    freshness = {(r["provider"], r["instance"], r["feature"]): r.get("updated_at") for r in state_db.feature_inventory(CONFIG)}
    sides = []
    for side, (provider, instance) in zip(("source", "target"), pair_refs(pair)):
        evidence = []
        for label, state in (("account_inventory", inventory), ("pair_comparison", comparison)):
            block = _block(state, provider, instance, feature)
            items = (block.get("baseline") or {}).get("items") or {}
            rows = []
            for item_key, value in items.items():
                if key and item_key != key:
                    continue
                row = _item(item_key, value)
                if not query or query.casefold() in json.dumps(row, ensure_ascii=False).casefold():
                    rows.append(row)
            rows.sort(key=lambda row: row["key"])
            evidence.append({"source": label, "available": bool(block), "total": len(rows), "offset": offset,
                             "items": rows[offset:offset + limit], "last_sync_epoch": state.get("last_sync_epoch"),
                             "stored_at_ns": freshness.get((provider, instance, feature)) if label == "account_inventory" else None})
        sides.append({"side": side, "provider": provider, "instance": instance, "evidence": evidence})
    return {"pair": _pair_row(pair), "feature": feature, "caution": CAUTION, "sides": sides}


def run(name: str, request: Request, args: dict[str, Any]) -> dict[str, Any]:
    cfg, profiles, pairs = _scope(request, args)
    if name == "list_sync_context":
        return {"profiles": [{"id": p["id"], "label": p.get("label") or p["id"]} for p in profiles],
                "pairs": [_pair_row(p) for p in pairs], "instruction": "Ask which profile or pair if the request is ambiguous. Accounts are provider instances, not login credentials."}
    pair = next((p for p in pairs if str(p.get("id")) == str(args.get("pair_id") or "")), None)
    if pair is None:
        raise HTTPException(404, "Pair not found or unavailable. Use list_sync_context.")
    feature = str(args.get("feature") or "")
    if feature not in _features(pair):
        raise HTTPException(400, "Choose an enabled feature from list_sync_context.")
    if name == "search_sync_items":
        try:
            offset, limit = max(0, int(args.get("offset", 0))), max(1, min(25, int(args.get("limit", 10))))
        except (ValueError, TypeError, OverflowError):
            raise HTTPException(400, "offset and limit must be integers") from None
        return _evidence(cfg, request, pair, feature, query=str(args.get("query") or "")[:256], offset=offset, limit=limit)
    key = str(args.get("item_key") or "").strip()
    if not key or len(key) > 1024:
        raise HTTPException(400, "Use an exact item_key from search_sync_items")
    result = _evidence(cfg, request, pair, feature, key=key)
    from api import editorAPI, eventsAPI
    from cw_platform.mapping_policy import effective_policy
    from services.saved_mappings import saved_blocks, saved_corrections

    policy = effective_policy(editorAPI._load_policy(), str(pair["id"]))
    refs = set(pair_refs(pair))
    rules = [r for rows in (saved_corrections(policy), saved_blocks(policy)) for r in rows
             if (r["provider"], r["instance"]) in refs and r["feature"] == feature and key in (r["key"], r.get("original_key"))]
    result["rules"] = [{"provider": r["provider"], "instance": r["instance"], "key": r["key"],
                        "original_key": r.get("original_key"), "blocked": r.get("excluded", r.get("entry_type") == "block"),
                        "corrected": _item(r["key"], r.get("corrected"))} for r in rules[:25]]
    result["rules_total"] = len(rules)
    pair_key = "-".join(sorted(provider for provider, _ in pair_refs(pair)))
    response = eventsAPI.events_search(item_key=key, pair_key=pair_key, feature=feature, visibility="all", order="newest", view="groups", domain="sync", limit=100, offset=0, request=request)
    if response.status_code != 200:
        result["events"] = {"available": False}
    else:
        data = json.loads(bytes(response.body))
        candidates = data.get("items", [])
        matched = [row for row in candidates if
                   {(str(row.get("source_provider") or "").upper(), str(row.get("source_instance") or "")),
                    (str(row.get("destination_provider") or "").upper(), str(row.get("destination_instance") or ""))} == refs]
        event_fields = ("id", "item_key", "title", "status", "reason_code", "reason_message", "summary", "pair_key", "feature", "first_event_at", "last_event_at", "event_count")
        result["events"] = {"available": True, "candidate_total": data.get("total"), "candidates_scanned": len(candidates), "matching_in_page": len(matched), "items": [
            {k: v[:500] if isinstance(v, str) else v for k, v in row.items() if k in event_fields and isinstance(v, (str, int, float, bool))}
            for row in matched[:10]]}
    result["event_caution"] = "Related events match both account instances, feature and exact item key, not necessarily a unique configured pair. At most 100 recent candidate groups are searched and 10 matches shown. Groups without explicit accounts and different mapped keys are omitted. No event does not prove no sync. Use search_events and Analyzer for additional evidence."
    return result
