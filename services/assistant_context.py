# services/assistant_context.py
# CrossWatch - Validated assistant page context and evidence navigation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from fastapi import HTTPException
from services import assistant_state

PAGES = {"main", "editor", "analyzer", "events", "logs", "settings", "profile", "snapshots", "capture_compare", "interactive_sync", "playlists"}


def validate(request, raw):
    if not isinstance(raw, dict):
        return {}
    result = {}
    if isinstance(raw.get("page"), str) and raw["page"] in PAGES:
        result["page"] = raw["page"]
    profile = str(raw.get("profile_id") or "")[:64]
    pair_id = str(raw.get("pair_id") or "")[:160]
    cfg, profiles, pairs = assistant_state._scope(request, {"profile_id": profile})
    if profile:
        result["profile_id"] = profile
    provider, instance = str(raw.get("provider") or "").upper(), str(raw.get("instance") or "default")
    feature = str(raw.get("feature") or "")
    if not pair_id and provider and feature in assistant_state.FEATURES:
        candidates = [p for p in pairs if (provider, instance) in assistant_state.pair_refs(p) and feature in assistant_state._features(p)]
        if len(candidates) == 1:
            pair_id = str(candidates[0]["id"])
        elif candidates:
            result.update(provider=provider, instance=instance, feature=feature, candidate_pair_ids=[str(p["id"]) for p in candidates[:25]])
            state = assistant_state.state_db.load_state_features(assistant_state.CONFIG, {feature})
            items = (assistant_state._block(state, provider, instance, feature).get("baseline") or {}).get("items") or {}
            key = str(raw.get("item_key") or "")[:1024]
            if key in items:
                result["item_key"] = key
    if pair_id:
        pair = next((p for p in pairs if str(p.get("id")) == pair_id), None)
        if pair is None:
            raise HTTPException(400, "The selected pair is unavailable. Reopen the assistant from the current page.")
        result["pair_id"] = pair_id
        feature = str(raw.get("feature") or "")
        if feature in assistant_state._features(pair):
            result["feature"] = feature
            key = str(raw.get("item_key") or "")[:1024]
            if key:
                evidence = assistant_state._evidence(cfg, request, pair, feature, key=key)
                if any(e["items"] for side in evidence["sides"] for e in side["evidence"]):
                    result["item_key"] = key
    return result


def references(request, name, args, output):
    if name in ("read_logs", "read_log_context"):
        from api.logsAPI import get_session
        row = get_session(request, str(args.get("session_id") or ""))
        return [{"kind": "log", "label": "Open cited log session", "session_id": row["id"], "channel": row["channel"], "run_id": row.get("run_id") or ""}]
    if name == "search_events" and isinstance(output, dict):
        return [{"kind": "event", "label": f"Event group {row['id']}", "group_id": row["id"]}
                for row in output.get("items", [])[:3] if isinstance(row, dict) and isinstance(row.get("id"), int)]
    if name == "explain_sync_item" and isinstance(output, dict):
        return [{"kind": "event", "label": f"Item event group {row['id']}", "group_id": row["id"]}
                for row in output.get("events", {}).get("items", [])[:3] if isinstance(row.get("id"), int)]
    if name == "list_saved_mappings":
        return [{"kind": "mappings", "label": "Open mappings and blocks"}]
    if name == "list_anime_overrides":
        return [{"kind": "anime", "label": "Open custom anime mappings"}]
    if name == "get_analyzer_problems":
        return [{"kind": "analyzer", "label": "Open Analyzer"}]
    if name == "compare_captures":
        return [{"kind": "captures", "label": "Open captures"}]
    if name == "get_scheduler_details":
        return [{"kind": "scheduling", "label": "Open scheduling"}]
    if name == "list_sync_reviews" and isinstance(output, dict):
        return [{"kind": "reviews", "label": "Open Interactive Sync review", "session_id": row["id"]}
                for row in output.get("reviews", [])[:3] if isinstance(row.get("id"), str)]
    return []
