# services/episode_groups.py
# CrossWatch - Pair-scoped History episode group management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Literal

from fastapi import HTTPException
from pydantic import BaseModel, Field

from cw_platform.access_policy import request_user, user_can_access_pair
from cw_platform.episode_groups import EpisodeGroup, check_corrections, endpoint, tokens, validate_groups
from cw_platform.local_db import manual_policy
from cw_platform.provider_instances import normalize_instance_id


class GroupRequest(BaseModel):
    pair_id: str = Field(min_length=1, max_length=256)
    action: Literal["save", "delete"] = "save"
    group: EpisodeGroup | None = None
    group_id: str = Field(default="", max_length=64)


def editor_group_index(cfg, request, policy, kind, pair_id=""):
    from api.editorAPI import _require_instance_scope

    index = {}
    if kind != "history":
        return index
    user = request_user(request)
    can_edit = not user or user.get("is_admin") or bool((user.get("permissions") or {}).get("write"))
    for pair in cfg.get("pairs") or []:
        pid = str(pair.get("id") or "")
        if not pid or (pair_id and pid != pair_id) or "history" not in (pair.get("features") or {}):
            continue
        if not user_can_access_pair(cfg, user, pair):
            continue
        sides = {side: (str(pair.get(side) or "").upper(), normalize_instance_id(pair.get(f"{side}_instance")))
                 for side in ("source", "target")}
        try:
            for provider, instance in sides.values():
                _require_instance_scope(cfg, request, provider, instance)
        except HTTPException:
            continue
        for group in ((policy.get("pairs") or {}).get(pid) or {}).get("episode_groups") or []:
            if any(endpoint(group[side]) != sides[side] for side in sides):
                continue
            badge = {**group, "pair_id": pid, "pair_name": pair.get("name") or pair.get("label") or
                     f"{pair['source']} → {pair['target']}", "can_edit": bool(can_edit)}
            for side in sides:
                for member in group[side]["episodes"]:
                    for token in tokens(member):
                        index.setdefault((*sides[side], token), []).append(badge)
    return index


def editor_badges(items, index, sources):
    result = {}
    if not index:
        return result
    for key, item in items.items():
        found = {}
        endpoints = sources.get(key, []) if isinstance(sources, dict) else sources
        for provider, instance in endpoints:
            for token in tokens(item):
                for badge in index.get((str(provider).upper(), normalize_instance_id(instance), token), []):
                    found[(badge["pair_id"], badge["id"])] = badge
        if found:
            result[key] = list(found.values())
    return result


def authorized(request):
    from api.editorAPI import load_config

    cfg, user = load_config(), request_user(request)
    if user and not user.get("is_admin") and not (user.get("permissions") or {}).get("write"):
        raise HTTPException(403, "Write permission required")
    return cfg, [pair for pair in cfg.get("pairs", []) if pair.get("id")
                 and "history" in (pair.get("features") or {}) and user_can_access_pair(cfg, user, pair)]


def list_groups(request):
    from api.editorAPI import _STATE_BASE, _require_instance_scope

    cfg, pairs = authorized(request)
    policy = manual_policy.load_policy(_STATE_BASE)
    result = []
    for pair in pairs:
        sides = {side: dict(provider=str(pair[key]).upper(), instance=normalize_instance_id(pair.get(f"{key}_instance")))
                 for side, key in (("source", "source"), ("target", "target"))}
        try:
            for side in sides.values():
                _require_instance_scope(cfg, request, **side)
        except HTTPException:
            continue
        result.append(dict(id=str(pair["id"]), name=pair.get("name") or pair.get("label") or
                           f"{pair['source']} → {pair['target']}", **sides,
                           groups=((policy.get("pairs") or {}).get(str(pair["id"])) or {}).get("episode_groups") or []))
    return dict(ok=True, pairs=result)


def update_group(payload, request):
    from api.editorAPI import _STATE_BASE, _require_instance_scope

    cfg, pairs = authorized(request)
    pair = next((pair for pair in pairs if str(pair["id"]) == payload.pair_id), None)
    if pair is None:
        raise HTTPException(404, "History sync pair not found")
    endpoints = [(str(pair[side]).upper(), normalize_instance_id(pair.get(f"{side}_instance"))) for side in ("source", "target")]
    for provider, instance in endpoints:
        _require_instance_scope(cfg, request, provider, instance)
    if payload.action == "save":
        if payload.group is None:
            raise HTTPException(400, "An episode group is required")
        group = payload.group.model_dump()
        if [endpoint(group[side]) for side in ("source", "target")] != endpoints:
            raise HTTPException(400, "Episode group endpoints must match the selected sync pair")
        group_id = group["id"]
    else:
        group_id = payload.group_id

    def apply(policy):
        scoped = policy.setdefault("pairs", {}).setdefault(payload.pair_id, {"version": 1, "providers": {}})
        groups = [row for row in scoped.get("episode_groups") or [] if row["id"] != group_id]
        if payload.action == "save":
            groups.append(group)
            if len(groups) > 500:
                raise ValueError("A sync pair supports up to 500 episode groups")
            groups = validate_groups(groups)
            check_corrections(policy, payload.pair_id, groups)
        elif len(groups) == len(scoped.get("episode_groups") or []):
            raise HTTPException(404, "Episode group not found")
        scoped["episode_groups"] = groups

    try:
        manual_policy.update_policy(_STATE_BASE, apply)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return dict(ok=True, id=group_id)
