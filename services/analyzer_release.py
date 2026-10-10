# services/analyzer_release.py
# CrossWatch - Release blocked items from Analyzer results
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

from typing import Any, Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from cw_platform.access_policy import request_user, user_can_access_pair
from cw_platform.provider_instances import normalize_instance_id


class ReleaseRequest(BaseModel):
    pair_id: str = Field(min_length=1, max_length=256)
    provider: str = Field(min_length=1, max_length=128)
    feature: Literal["history", "watchlist", "ratings", "progress", "collection"]
    key: str = Field(min_length=1, max_length=1024)
    kind: Literal["retry", "rule"]


def block_ref(manual: Any, token: str, feature: str, key: str) -> dict[str, Any] | None:
    from services import analyzer as an
    from services.saved_mappings import correction_block_keys, deliberate_record

    providers = manual.get("providers") if isinstance(manual, dict) else None
    if not isinstance(providers, dict):
        return None
    for candidate in dict.fromkeys((token, an._provider_base(token))):
        base, instance = an._split_prov_token(candidate)
        for name, node in providers.items():
            if str(name).upper() != base or not isinstance(node, dict):
                continue
            if instance != an._DEFAULT_INSTANCE:
                instances = node.get("instances")
                node = next((blk for inst, blk in (instances if isinstance(instances, dict) else {}).items()
                             if normalize_instance_id(inst) == instance), None)
            block = node.get(feature) if isinstance(node, dict) else None
            if not isinstance(block, dict) or key not in an._block_keys(block.get("blocks")):
                continue
            owned = key.lower() in correction_block_keys(block) and not (deliberate_record(block, key) or {}).get("blocked")
            items = (block.get("adds") or {}).get("items") or {}
            mapping_key = next((target for target, record in (block.get("mappings") or {}).items()
                                if target in items and record.get("original_key") != target
                                and str(record.get("original_key") or "").lower() == key.lower()), None) if owned else None
            return dict(provider=base, instance=instance, key=key, mapping_key=mapping_key)
    return None


def _release_rule(payload, request, analysis, wanted):
    from services import analyzer as an

    rows = [row for row in analysis.get("problems", [])
            if row.get("type") == "blocked_manual" and row.get("feature") == payload.feature
            and an._norm_prov_token(row.get("provider")) == wanted and row.get("key") == payload.key
            and isinstance(row.get("block"), dict)]
    if len(rows) != 1:
        raise HTTPException(409, "This item changed. Analyze again before releasing it.")
    block = rows[0]["block"]
    if block.get("mapping_key"):
        from services.mapping_transfer import RuleIdentity, delete_rule

        delete_rule(request, RuleIdentity(provider=block["provider"], instance=block["instance"],
                                          feature=payload.feature, key=block["mapping_key"], entry_type="mapping"))
        return dict(ok=True, released="mapping")
    from services.editor_mapping import BlockRequest, update_block

    update_block(BlockRequest(provider=block["provider"], instance=block["instance"],
                              feature=payload.feature, key=block["key"], blocked=False), request)
    return dict(ok=True, released="rule")


def _release_retry(payload, analysis, wanted):
    from cw_platform.orchestrator._blackbox import clear_path_keys
    from services import analyzer as an

    entries = [entry for entry in analysis.get("attention", {}).get("rows", [])
               if entry.get("unresolved") and entry.get("retry_blocked") and entry.get("feature") == payload.feature
               and an._norm_prov_token(entry.get("provider")) == wanted
               and payload.key in [entry.get("key"), *(entry.get("keys") or [])]]
    if len(entries) != 1:
        raise HTTPException(409, "This item changed. Analyze again before releasing it.")
    aliases = {str(key) for key in [entries[0].get("key"), *(entries[0].get("keys") or [])] if key}
    _state, _context, allowed, _selected, _timings = an._load_analysis_state(payload.pair_id)
    removed = 0
    for provider, feature, kind, _pending, name, rows in an._iter_unresolved_files(allowed):
        if kind != "blackbox" or feature != payload.feature or an._norm_prov_token(provider) != wanted:
            continue
        keys = [key for key, _item, _rec in rows if key in aliases or key.split("@", 1)[0] in aliases]
        if keys:
            removed += clear_path_keys(an.CWS_DIR / name, keys)["blackbox_removed"]
    if not removed:
        raise HTTPException(409, "This retry block no longer exists. Analyze again.")
    return dict(ok=True, released="retry", removed=removed)


def handle_release(payload: ReleaseRequest, request):
    from services import analyzer as an

    cfg, user = an._cfg(), request_user(request)
    if user and not user.get("is_admin") and not (user.get("permissions") or {}).get("write"):
        raise HTTPException(403, "Write permission required")
    pair = next((p for p in cfg.get("pairs", []) if an._pair_id(p) == payload.pair_id), None)
    if not pair or not user_can_access_pair(cfg, user, pair):
        raise HTTPException(404, "Sync pair not found")
    analysis = an._cached_analysis(payload.pair_id)
    wanted = an._norm_prov_token(payload.provider)
    if payload.kind == "rule":
        return _release_rule(payload, request, analysis, wanted)
    return _release_retry(payload, analysis, wanted)
