# /api/providerInstancesAPI.py
# CrossWatch - Provider instance management API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import re
import time
from collections.abc import Mapping
from functools import lru_cache
from typing import Any, cast

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

from api.provider_guard import usage_conflict_response
from cw_platform.config_base import load_config, save_config, update_config
from cw_platform.provider_instances import (
    assign_instance_to_user_profile,
    assign_instance_to_user_profile_id,
    apply_instance_defaults,
    delete_user_profile,
    generate_user_profile_id,
    get_instance_block,
    is_sync_user_instance,
    list_instance_ids,
    list_sync_user_ids,
    split_sync_user_id,
    sync_user_instance_id,
    sync_user_match_name,
    list_user_profiles,
    normalize_instance_id,
    normalize_user_profile_id,
    provider_key,
    provider_instance_uid_for,
    remove_provider_instance_uid,
    remove_instance_from_user_profiles,
    set_instance_label,
    sanitize_instance_label,
    sanitize_user_profile_label,
    upsert_user_profile,
    user_profiles_for_instance,
)
from cw_platform.tracker_storage import remove_crosswatch_storage
from cw_platform.user_profile_resources import (
    apply_profile_resource_assignments,
    clear_profile_resource_assignments,
    profile_resource_ids_for_profile,
    profile_resource_summary,
    resource_counts_for_profile,
)

router = APIRouter(prefix="/api", tags=["provider-instances"])

_CFG_KEY_ALIAS = {
    "tmdb": "tmdb_sync",
    "tmdb_sync": "tmdb_sync",
    "cw": "crosswatch",
    "crosswatch": "crosswatch",
}
_DEFAULT_MAX_PROFILES = 10


def _max_profiles_per_provider(cfg: Mapping[str, Any] | None = None) -> int:
    try:
        src = cfg if isinstance(cfg, Mapping) else (load_config() or {})
        rt = src.get("runtime")
        raw = rt.get("max_profiles_per_provider") if isinstance(rt, Mapping) else None
        total = _DEFAULT_MAX_PROFILES if raw is None else int(raw)
    except Exception:
        total = _DEFAULT_MAX_PROFILES
    return max(1, min(100, total))


def _max_generated_profiles(cfg: Mapping[str, Any] | None = None) -> int:
    return _max_profiles_per_provider(cfg) - 1


_DEFAULT_MAX_SYNC_USERS = 25
_SYNC_USER_PROVIDERS = frozenset({"plex", "emby", "jellyfin"})


def _max_sync_users_per_connection(cfg: Mapping[str, Any] | None = None) -> int:
    try:
        src = cfg if isinstance(cfg, Mapping) else (load_config() or {})
        rt = src.get("runtime")
        raw = rt.get("max_sync_users_per_connection") if isinstance(rt, Mapping) else None
        total = _DEFAULT_MAX_SYNC_USERS if raw is None else int(raw)
    except Exception:
        total = _DEFAULT_MAX_SYNC_USERS
    return max(1, min(100, total))


def _cfg_key(provider: str) -> str:
    k = provider_key(provider)
    return _CFG_KEY_ALIAS.get(k, k)



@lru_cache(maxsize=1)
def _known_providers_from_registry() -> tuple[str, ...]:
    try:
        from cw_platform.modules_registry import MODULES
    except Exception:
        return ()
    out: set[str] = set()
    for group in ("AUTH", "SYNC"):
        grp = MODULES.get(group) or {}
        if not isinstance(grp, dict):
            continue
        for key in grp.keys():
            k = str(key)
            if k.startswith("_auth_"):
                out.add(k[6:].lower())
            elif k.startswith("_mod_"):
                out.add(k[5:].lower())
    return tuple(sorted(out))



def _prov_prefix(provider: str) -> str:
    key = provider_key(provider)
    if key == "crosswatch":
        return "CW"
    return str(provider or "").strip().upper()


def _canonical_profile_id(provider: str, instance_id: Any, cfg: Mapping[str, Any] | None = None) -> str | None:
    inst = normalize_instance_id(instance_id)
    if inst == "default":
        return None
    prov = _prov_prefix(provider)
    m = re.fullmatch(rf"{re.escape(prov)}-P(\d{{2,}})", str(inst).strip().upper())
    if not m:
        return ""
    try:
        num = int(m.group(1))
    except Exception:
        return ""
    if num < 1 or num > _max_generated_profiles(cfg):
        return ""
    return f"{prov}-P{num:02d}"


def _next_profile_id(provider: str, insts: dict[str, Any], cfg: Mapping[str, Any] | None = None) -> str:
    prov = _prov_prefix(provider)
    existing = {str(k).strip().upper() for k in (insts or {}).keys()}
    for n in range(1, _max_generated_profiles(cfg) + 1):
        cand = f"{prov}-P{n:02d}"
        if cand not in existing:
            return cand
    return ""


def _profile_label(block: Any, fallback: str) -> str:
    if isinstance(block, dict):
        label = sanitize_instance_label(block.get("label"))
        if label:
            return f"{fallback} - {label}"
    return fallback


def _friendly_label(block: Any) -> str:
    if isinstance(block, dict):
        return sanitize_instance_label(block.get("label"))
    return ""


def _create_instance(cfg: dict[str, Any], provider: str, insts: dict[str, Any], inst: str, payload: dict[str, Any]) -> None:
    raw_copy_from = str((payload or {}).get("copy_from") or "").strip()
    copy_from = normalize_instance_id(raw_copy_from) if raw_copy_from else ""
    template = (payload or {}).get("template")

    if isinstance(template, dict):
        insts[inst] = copy.deepcopy(template)
    elif copy_from and copy_from != "default":
        src = insts.get(copy_from)
        insts[inst] = copy.deepcopy(src) if isinstance(src, dict) else {}
    else:
        insts[inst] = {}
    label = sanitize_instance_label((payload or {}).get("label"))
    if label:
        insts[inst]["label"] = label
    apply_instance_defaults(cfg, _cfg_key(provider), inst)
    provider_instance_uid_for(cfg, provider, inst, create=True)


def _provider_block(cfg: dict[str, Any], provider: str) -> dict[str, Any]:
    k = _cfg_key(provider)
    raw = cfg.get(k)
    if isinstance(raw, dict):
        return raw
    blk: dict[str, Any] = {}
    cfg[k] = blk
    return blk


def _invalidate_provider_cache(provider: str) -> None:
    try:
        from api.probesAPI import invalidate_provider_caches

        invalidate_provider_caches(_cfg_key(provider))
    except Exception:
        pass


def _strip_instances(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d or {})
    out.pop("instances", None)
    return out


def _provider_instance_configured(cfg: dict[str, Any], provider: str, instance_id: Any) -> bool:
    try:
        from api.probesAPI import _prov_configured

        return bool(_prov_configured(cfg, provider, instance_id))
    except Exception:
        return False


def _instances_map_for(cfg: dict[str, Any], provider: str, *, configured_only: bool = False) -> list[dict[str, Any]]:
    ids = list_instance_ids(cfg, _cfg_key(provider))
    out: list[dict[str, str]] = []
    blk = _provider_block(cfg, provider)
    insts = blk.get("instances")
    inst_map = insts if isinstance(insts, dict) else {}
    for i in ids:
        configured = _provider_instance_configured(cfg, provider, i)
        if configured_only and not configured:
            continue
        block = blk if i == "default" else inst_map.get(i)
        friendly = _friendly_label(block)
        lab = friendly or ("Default" if i == "default" else _profile_label(block, i))
        owners = user_profiles_for_instance(cfg, provider, i)
        user_profile = owners[0] if owners else None
        row: dict[str, Any] = {
            "id": i,
            "uid": provider_instance_uid_for(cfg, provider, i),
            "label": lab,
            "friendly_label": friendly,
            "display_label": friendly or lab,
            "configured": configured,
        }
        if user_profile:
            row["user_profile_id"] = str(user_profile.get("id") or "")
            row["user_profile_label"] = str(user_profile.get("label") or "")
        if owners:
            row["user_profiles"] = [{"id": str(o.get("id") or ""), "label": str(o.get("label") or "")} for o in owners]
        out.append(row)
    return out


def _profile_scope_provider_key(provider: Any) -> str:
    key = provider_key(str(provider or ""))
    return "TMDB" if key == "tmdb" else key.upper()


def _authenticated_request_user(cfg: dict[str, Any], request: Request | None) -> Mapping[str, Any] | None:
    if request is None:
        return None
    try:
        state = getattr(request, "state", None)
        user = getattr(state, "cw_user", None) or getattr(state, "user", None)
        if isinstance(user, Mapping):
            return user
    except Exception:
        pass
    try:
        from api.apiTokensAPI import extract_api_token, resolve_api_token

        user = resolve_api_token(cfg, extract_api_token(request))
        if isinstance(user, Mapping):
            return user
    except Exception:
        pass
    try:
        from api.appAuthAPI import COOKIE_NAME, current_user

        user = current_user(cfg, request.cookies.get(COOKIE_NAME))
        if isinstance(user, Mapping):
            return user
    except Exception:
        pass
    return None


def _managed_user_instance_scope(cfg: dict[str, Any], request: Request | None) -> dict[str, set[str]] | None:
    user = _authenticated_request_user(cfg, request)
    if not isinstance(user, Mapping) or user.get("is_admin"):
        return None
    pid = normalize_user_profile_id(user.get("profile_id"))
    if not pid:
        return {}
    for row in list_user_profiles(cfg):
        if normalize_user_profile_id(row.get("id")) != pid:
            continue
        raw = row.get("instances")
        if not isinstance(raw, dict):
            return {}
        out: dict[str, set[str]] = {}
        for provider, values in raw.items():
            key = _profile_scope_provider_key(provider)
            if isinstance(values, list):
                ids = [normalize_instance_id(v) for v in values]
            else:
                ids = [normalize_instance_id(values)]
            out[key] = {v for v in ids if v}
        return out
    return {}


def _filter_instance_rows_for_scope(provider: Any, rows: list[dict[str, Any]], scope: dict[str, set[str]] | None) -> list[dict[str, Any]]:
    if scope is None:
        return rows
    allowed = scope.get(_profile_scope_provider_key(provider), set())
    return [row for row in rows if normalize_instance_id(row.get("id")) in allowed]


def _decorate_user_profile_rows(cfg: dict[str, Any], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        pid = normalize_user_profile_id(item.get("id"))
        item["resources"] = profile_resource_ids_for_profile(cfg, pid)
        item["resource_counts"] = resource_counts_for_profile(cfg, pid)
        out.append(item)
    return out


def _admin_required_response(request: Request | None, cfg: dict[str, Any]) -> JSONResponse | None:
    try:
        from api.appAuthAPI import _admin_required
    except Exception:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if request is None:
        return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401, headers={"Cache-Control": "no-store"})
    if _admin_api_token_request(request, cfg):
        return None
    return _admin_required(request, cfg)


def _admin_api_token_request(request: Request, cfg: dict[str, Any]) -> bool:
    try:
        from api.apiTokensAPI import extract_api_token, resolve_api_token

        user = resolve_api_token(cfg, extract_api_token(request))
    except Exception:
        return False
    return isinstance(user, Mapping) and bool(user.get("is_admin"))


def _profile_value_error(exc: ValueError) -> str:
    code = str(exc.args[0] if exc.args else "").strip()
    if code in {"invalid_user_profile", "duplicate_user_profile_label"}:
        return code
    return "invalid_user_profile"


@router.get("/provider-instances")
def api_provider_instances_all(request: Request = cast(Request, None), configured_only: bool = False) -> JSONResponse:
    cfg = dict(load_config() or {})
    scope = _managed_user_instance_scope(cfg, request)
    out: dict[str, Any] = {}
    for k, v in list(cfg.items()):
        if not isinstance(v, dict):
            continue
        insts = v.get("instances")
        if isinstance(insts, dict) and insts:
            out_key = "TMDB" if str(k) == "tmdb_sync" else str(k).upper()
            if out_key not in out:
                rows = _instances_map_for(cfg, out_key, configured_only=configured_only)
                rows = _filter_instance_rows_for_scope(out_key, rows, scope)
                if rows or (scope is None and not configured_only):
                    out[out_key] = rows
    for prov in _known_providers_from_registry():
        ck = _cfg_key(prov)
        out_key = "TMDB" if provider_key(prov) == "tmdb" else provider_key(prov).upper()
        if ck in cfg and out_key not in out:
            rows = _instances_map_for(cfg, out_key, configured_only=configured_only)
            rows = _filter_instance_rows_for_scope(out_key, rows, scope)
            if rows or (scope is None and not configured_only):
                out[out_key] = rows
    return JSONResponse(out)


@router.get("/provider-instances/{provider}")
def api_provider_instances_provider(provider: str, request: Request = cast(Request, None)) -> JSONResponse:
    cfg = dict(load_config() or {})
    scope = _managed_user_instance_scope(cfg, request)
    rows = _instances_map_for(cfg, provider)
    rows = _filter_instance_rows_for_scope(provider, rows, scope)
    return JSONResponse(rows)


@router.post("/provider-instances/{provider}/next")
def api_provider_instances_create_next(provider: str, payload: dict[str, Any] = Body(default_factory=dict), request: Request = cast(Request, None)) -> Any:
    cfg = dict(load_config() or {})
    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked
    blk = _provider_block(cfg, provider)
    insts = blk.get("instances")
    if not isinstance(insts, dict):
        insts = {}
        blk["instances"] = insts

    inst = _next_profile_id(provider, insts, cfg)
    if not inst:
        return {"ok": False, "error": "profile_limit_reached", "limit": _max_profiles_per_provider(cfg)}

    if inst in insts and isinstance(insts.get(inst), dict):
        uid = provider_instance_uid_for(cfg, provider, inst, create=True)
        save_config(cfg)
        return {"ok": True, "id": inst, "uid": uid}

    _create_instance(cfg, provider, insts, inst, payload or {})
    save_config(cfg)
    _invalidate_provider_cache(provider)
    return {"ok": True, "id": inst, "uid": provider_instance_uid_for(cfg, provider, inst)}


@router.post("/provider-instances/{provider}/{instance_id}")
def api_provider_instances_create(provider: str, instance_id: str, payload: dict[str, Any] = Body(default_factory=dict), request: Request = cast(Request, None)) -> Any:
    cfg = dict(load_config() or {})
    canon = _canonical_profile_id(provider, instance_id, cfg)
    if canon is None:
        return {"ok": False, "error": "reserved_instance_id"}
    if not canon:
        return {"ok": False, "error": "invalid_instance_id"}
    inst = canon

    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked
    blk = _provider_block(cfg, provider)
    insts = blk.get("instances")
    if not isinstance(insts, dict):
        insts = {}
        blk["instances"] = insts

    if inst in insts and isinstance(insts.get(inst), dict):
        uid = provider_instance_uid_for(cfg, provider, inst, create=True)
        save_config(cfg)
        return {"ok": True, "id": inst, "uid": uid}

    _create_instance(cfg, provider, insts, inst, payload or {})

    save_config(cfg)
    _invalidate_provider_cache(provider)
    return {"ok": True, "id": inst, "uid": provider_instance_uid_for(cfg, provider, inst)}


@router.patch("/provider-instances/{provider}/{instance_id}")
def api_provider_instances_update(provider: str, instance_id: str, payload: dict[str, Any] = Body(default_factory=dict), request: Request = cast(Request, None)) -> Any:
    inst = normalize_instance_id(instance_id)
    cfg = dict(load_config() or {})
    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked
    if inst != "default" and inst not in list_instance_ids(cfg, _cfg_key(provider)):
        return {"ok": False, "error": "not_found"}

    updated = False
    if isinstance(payload, dict) and "label" in payload:
        set_instance_label(cfg, _cfg_key(provider), inst, payload.get("label"))
        updated = True

    if isinstance(payload, dict) and ("user_profile" in payload or "user_profile_label" in payload or "user_profile_id" in payload):
        if "user_profile_label" in payload or "user_profile" in payload:
            raw_user = payload.get("user_profile_label") if "user_profile_label" in payload else payload.get("user_profile")
            assign_instance_to_user_profile(cfg, raw_user, provider, inst)
        else:
            assign_instance_to_user_profile_id(cfg, payload.get("user_profile_id"), provider, inst)
        updated = True

    if not updated:
        return {"ok": False, "error": "nothing_to_update"}

    uid = provider_instance_uid_for(cfg, provider, inst, create=True)
    save_config(cfg)
    _invalidate_provider_cache(provider)
    owners = user_profiles_for_instance(cfg, provider, inst)
    return {"ok": True, "id": inst, "uid": uid, "user_profile": owners[0] if owners else None, "user_profiles": owners}


def _sync_user_rows(cfg: Mapping[str, Any], provider: str, inst: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for user_inst in list_sync_user_ids(cfg, provider, inst):
        ref = split_sync_user_id(user_inst)
        entry = get_instance_block(cfg, provider, user_inst)
        if ref is None:
            continue
        row: dict[str, Any] = {"key": ref[1], "id": user_inst, "username": str(entry.get("username") or ""), "match_as": str(entry.get("match_as") or ""), "match_key": sync_user_match_name(cfg, provider, user_inst).casefold(), "added_at": entry.get("added_at")}
        if provider == "plex":
            row["has_pin"] = bool(str(entry.get("home_pin") or "").strip())
        rows.append(row)
    rows.sort(key=lambda row: row["username"].casefold())
    return rows


def _build_sync_user_entry(provider: str, key: str, raw: Mapping[str, Any], existing: Mapping[str, Any], now: int) -> dict[str, Any]:
    name = " ".join(str(raw.get("username") or existing.get("username") or "").split())[:80]
    if provider == "plex":
        entry: dict[str, Any] = {"username": name, "account_id": int(key) if key.isdigit() else key, "home_pin": str(existing.get("home_pin") or "")}
        if "home_pin" in raw:
            entry["home_pin"] = str(raw.get("home_pin") or "").strip()
        cloud_id = str(raw.get("cloud_account_id") or existing.get("cloud_account_id") or "").strip()
        if cloud_id and cloud_id != key:
            entry["cloud_account_id"] = cloud_id
    else:
        entry = {"user_id": key, "username": name, "user": name}
    source = raw.get("match_as") if "match_as" in raw else existing.get("match_as")
    match_as = " ".join(str(source or "").split())
    if len(match_as) > 40:
        raise ValueError("match_as_too_long")
    if match_as and match_as.casefold() != name.casefold():
        entry["match_as"] = match_as
    entry["added_at"] = existing.get("added_at") or now
    return entry


def _profile_identity(cfg: Mapping[str, Any], provider: str, user_inst: str) -> tuple[str, str]:
    full = sync_user_match_name(cfg, provider, user_inst)
    label = sanitize_user_profile_label(full)
    if not label or label != full:
        return "", ""
    return label, label.casefold()


_SYNC_USER_ERRORS: dict[str, str] = {
    "not_found": "not_found",
    "unsupported_provider": "unsupported_provider",
    "invalid_instance_id": "invalid_instance_id",
    "sync_user_limit_reached": "sync_user_limit_reached",
    "match_as_too_long": "match_as_too_long",
    "invalid_user_profile": "invalid_user_profile",
    "duplicate_user_profile_label": "duplicate_user_profile_label",
}


def _sync_user_error(exc: Exception) -> str:
    raw = str(exc.args[0] if exc.args else "").split(":", 1)[0].strip()
    return _SYNC_USER_ERRORS.get(raw, "sync_users_failed")


def _match_name_suggestions(cfg: Mapping[str, Any], provider: str, inst: str) -> list[str]:
    names: dict[str, str] = {}
    for other in sorted(_SYNC_USER_PROVIDERS):
        for user_inst in list_sync_user_ids(cfg, other):
            ref = split_sync_user_id(user_inst)
            if ref is None or (other == provider and ref[0] == inst):
                continue
            name = sync_user_match_name(cfg, other, user_inst)
            if name and len(name) <= 40:
                names.setdefault(name.casefold(), name)
    for row in list_user_profiles(cfg):
        label = str(row.get("label") or "").strip()
        if label:
            names.setdefault(label.casefold(), label)
    return sorted(names.values(), key=str.casefold)


def _sync_users_target(cfg: Mapping[str, Any], provider: str, inst: str) -> tuple[str, str | None]:
    prov = _cfg_key(provider)
    if prov not in _SYNC_USER_PROVIDERS:
        return prov, "unsupported_provider"
    if is_sync_user_instance(inst):
        return prov, "invalid_instance_id"
    if inst != "default" and inst not in list_instance_ids(cfg, prov):
        return prov, "not_found"
    return prov, None


@router.get("/sync-user-labels")
def api_sync_user_labels(request: Request = cast(Request, None)) -> JSONResponse:
    cfg = dict(load_config() or {})
    scope = _managed_user_instance_scope(cfg, request)
    out: dict[str, dict[str, str]] = {}
    for prov in sorted(_SYNC_USER_PROVIDERS):
        allowed = scope.get(_profile_scope_provider_key(prov), set()) if scope is not None else None
        labels = {
            user_inst: str(get_instance_block(cfg, prov, user_inst).get("username") or "")
            for user_inst in list_sync_user_ids(cfg, prov)
            if allowed is None or user_inst in allowed
        }
        if labels:
            out[prov.upper()] = labels
    return JSONResponse(out, headers={"Cache-Control": "no-store"})


@router.get("/provider-instances/{provider}/{instance_id}/sync-users")
def api_provider_sync_users(provider: str, instance_id: str, request: Request = cast(Request, None)) -> Any:
    inst = normalize_instance_id(instance_id)
    cfg = dict(load_config() or {})
    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked
    prov, error = _sync_users_target(cfg, provider, inst)
    if error:
        return {"ok": False, "error": error}
    rows = _sync_user_rows(cfg, prov, inst)
    return {"ok": True, "users": rows, "count": len(rows), "limit": _max_sync_users_per_connection(cfg), "suggestions": _match_name_suggestions(cfg, prov, inst)}


@router.put("/provider-instances/{provider}/{instance_id}/sync-users")
def api_provider_sync_users_update(provider: str, instance_id: str, payload: dict[str, Any] = Body(default_factory=dict), request: Request = cast(Request, None)) -> Any:
    inst = normalize_instance_id(instance_id)
    cfg = dict(load_config() or {})
    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked
    raw_users = (payload or {}).get("users")
    if not isinstance(raw_users, list):
        return {"ok": False, "error": "invalid_users"}
    wanted: dict[str, Mapping[str, Any]] = {}
    for raw in raw_users:
        if not isinstance(raw, Mapping):
            continue
        key = str(raw.get("key") or "").strip()
        if key and "~" not in key and str(raw.get("username") or "").strip():
            wanted.setdefault(key, raw)

    def _mutate(latest: dict[str, Any]) -> dict[str, Any]:
        prov, error = _sync_users_target(latest, provider, inst)
        if error:
            raise KeyError(error)
        limit = _max_sync_users_per_connection(latest)
        if len(wanted) > limit:
            raise ValueError(f"sync_user_limit_reached:{limit}")
        parent = latest.get(prov) if inst == "default" else ((latest.get(prov) or {}).get("instances") or {}).get(inst)
        if not isinstance(parent, dict):
            raise KeyError("not_found")
        raw_current = parent.get("sync_users")
        current: dict[str, Any] = raw_current if isinstance(raw_current, dict) else {}
        now = int(time.time())
        updated = {key: _build_sync_user_entry(prov, key, raw, current.get(key) or {}, now) for key, raw in wanted.items()}
        for key in set(current) - set(updated):
            user_inst = sync_user_instance_id(inst, key)
            remove_instance_from_user_profiles(latest, prov, user_inst)
            remove_provider_instance_uid(latest, prov, user_inst)
        if updated:
            parent["sync_users"] = updated
        else:
            parent.pop("sync_users", None)
        profiles_by_label = {str(row.get("label") or "").casefold(): str(row.get("id") or "") for row in list_user_profiles(latest)}
        identities = {key: _profile_identity(latest, prov, sync_user_instance_id(inst, key))[1] for key in updated}
        counts: dict[str, int] = {}
        for folded in identities.values():
            if folded:
                counts[folded] = counts.get(folded, 0) + 1
        for key in updated:
            user_inst = sync_user_instance_id(inst, key)
            provider_instance_uid_for(latest, prov, user_inst, create=True)
            folded = identities[key]
            pid = profiles_by_label.get(folded) if folded and counts.get(folded) == 1 else None
            if pid and not user_profiles_for_instance(latest, prov, user_inst):
                assign_instance_to_user_profile_id(latest, pid, prov, user_inst)
        return {"users": _sync_user_rows(latest, prov, inst), "limit": limit}

    try:
        _, result = update_config(_mutate)
    except (KeyError, ValueError) as exc:
        code = _sync_user_error(exc)
        if code == "sync_user_limit_reached":
            return {"ok": False, "error": code, "limit": _max_sync_users_per_connection(load_config() or {})}
        return {"ok": False, "error": code}
    _invalidate_provider_cache(provider)
    return {"ok": True, **result, "count": len(result["users"])}


@router.post("/provider-instances/{provider}/{instance_id}/sync-users/profiles")
def api_provider_sync_users_create_profiles(provider: str, instance_id: str, request: Request = cast(Request, None)) -> Any:
    inst = normalize_instance_id(instance_id)
    cfg = dict(load_config() or {})
    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked

    def _mutate(latest: dict[str, Any]) -> dict[str, Any]:
        prov, error = _sync_users_target(latest, provider, inst)
        if error:
            raise KeyError(error)
        everyone: list[tuple[str, str, str, str, str]] = []
        for other in sorted(_SYNC_USER_PROVIDERS):
            for user_inst in list_sync_user_ids(latest, other):
                ref = split_sync_user_id(user_inst)
                label, folded = _profile_identity(latest, other, user_inst)
                everyone.append((other, ref[0] if ref else "", user_inst, label, folded))
        per_connection: dict[tuple[str, str, str], int] = {}
        for other, parent, _user_inst, _label, folded in everyone:
            if folded:
                per_connection[(other, parent, folded)] = per_connection.get((other, parent, folded), 0) + 1
        ambiguous = {folded for (_other, _parent, folded), count in per_connection.items() if count > 1}
        own = [(label, folded) for other, parent, _user_inst, label, folded in everyone if other == prov and parent == inst]
        skipped = sum(1 for _label, folded in own if not folded or folded in ambiguous)
        names = {folded: label for label, folded in own if folded and folded not in ambiguous}
        profiles_by_label = {str(row.get("label") or "").casefold(): str(row.get("id") or "") for row in list_user_profiles(latest)}
        created = linked = 0
        for folded, label in names.items():
            eligible = [(other, user_inst) for other, _parent, user_inst, _label, name in everyone if name == folded and not user_profiles_for_instance(latest, other, user_inst)]
            if not eligible:
                continue
            pid = profiles_by_label.get(folded)
            if not pid:
                pid = generate_user_profile_id(latest)
                upsert_user_profile(latest, pid, label=label)
                profiles_by_label[folded] = pid
                created += 1
            for other, user_inst in eligible:
                assign_instance_to_user_profile_id(latest, pid, other, user_inst)
                linked += 1
        return {"created": created, "linked": linked, "skipped": skipped}

    try:
        _, result = update_config(_mutate)
    except (KeyError, ValueError) as exc:
        return {"ok": False, "error": _sync_user_error(exc)}
    return {"ok": True, **result}


@router.delete("/provider-instances/{provider}/{instance_id}")
def api_provider_instances_delete(provider: str, instance_id: str, request: Request = cast(Request, None)) -> Any:
    inst = normalize_instance_id(instance_id)
    if inst == "default":
        return {"ok": False, "error": "cannot_delete_default"}

    cfg = dict(load_config() or {})
    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked
    blk = _provider_block(cfg, provider)
    insts = blk.get("instances")
    if not isinstance(insts, dict) or inst not in insts:
        return {"ok": False, "error": "not_found"}

    conflict = usage_conflict_response(cfg, provider_key(provider), inst)
    if conflict is not None:
        return conflict

    cleanup: dict[str, Any] | None = None
    if _cfg_key(provider) == "crosswatch":
        cleanup = remove_crosswatch_storage(cfg, inst)
        if cleanup.get("ok") is False:
            return cleanup

    if _cfg_key(provider) == "stremio":
        from providers.scrobble.stremio import addon as stremio_addon

        stremio_addon.set_instance_enabled(cfg, inst, False)
        stremio_addon.forget_instance(inst)

    insts.pop(inst, None)
    remove_instance_from_user_profiles(cfg, provider, inst)
    remove_provider_instance_uid(cfg, provider, inst)
    save_config(cfg)
    _invalidate_provider_cache(provider)
    return {"ok": True, **({"storage": cleanup} if cleanup else {})}


@router.get("/user-profiles")
def api_user_profiles_all(request: Request = cast(Request, None)) -> JSONResponse:
    cfg = dict(load_config() or {})
    rows = list_user_profiles(cfg)
    include_resources = True
    user = _authenticated_request_user(cfg, request)
    if user and not user.get("is_admin"):
        pid = normalize_user_profile_id(user.get("profile_id"))
        rows = [row for row in rows if row.get("id") == pid]
        include_resources = False
    payload: dict[str, Any] = {"ok": True, "items": _decorate_user_profile_rows(cfg, rows)}
    if include_resources:
        payload["resources"] = profile_resource_summary(cfg)
    return JSONResponse(payload)


@router.get("/user-profiles/{profile_id}")
def api_user_profiles_get(profile_id: str, request: Request = cast(Request, None)) -> JSONResponse:
    cfg = dict(load_config() or {})
    pid = normalize_user_profile_id(profile_id)
    include_resources = True
    try:
        from api.appAuthAPI import auth_required

        if request is not None and auth_required(cfg):
            user = _authenticated_request_user(cfg, request)
            if user is None:
                return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
            if not user.get("is_admin"):
                if normalize_user_profile_id(user.get("profile_id")) != pid:
                    return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
                include_resources = False
    except Exception:
        if request is not None:
            return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    for row in list_user_profiles(cfg):
        if row.get("id") == pid:
            payload: dict[str, Any] = {"ok": True, "profile": _decorate_user_profile_rows(cfg, [row])[0]}
            if include_resources:
                payload["resources"] = profile_resource_summary(cfg)
            return JSONResponse(payload)
    return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)


@router.post("/user-profiles")
def api_user_profiles_create(payload: dict[str, Any] = Body(default_factory=dict), request: Request = cast(Request, None)) -> Any:
    cfg = dict(load_config() or {})
    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked
    label = sanitize_user_profile_label((payload or {}).get("label") or (payload or {}).get("name"))
    if not label:
        return {"ok": False, "error": "invalid_user_profile"}

    def _mutate(latest: dict[str, Any]) -> dict[str, Any]:
        profile_id = generate_user_profile_id(latest)
        resource_payload = payload or {}
        raw_instances = resource_payload.get("instances")
        manual_instances: dict[str, Any] = raw_instances if isinstance(raw_instances, dict) else {}
        if any(key in resource_payload for key in ("sync_pairs", "watcher_routes", "webhook_routes", "resources")):
            raw_resources = resource_payload.get("resources")
            resources: dict[str, Any] = raw_resources if isinstance(raw_resources, dict) else {}
            return apply_profile_resource_assignments(
                latest,
                profile_id,
                label=label,
                manual_instances=manual_instances,
                selected_sync_pairs=resource_payload.get("sync_pairs", resources.get("sync_pairs") if isinstance(resources, dict) else []),
                selected_watcher_routes=resource_payload.get("watcher_routes", resources.get("watcher_routes") if isinstance(resources, dict) else []),
                selected_webhook_routes=resource_payload.get("webhook_routes", resources.get("webhook_routes") if isinstance(resources, dict) else []),
            )
        return upsert_user_profile(
            latest,
            profile_id,
            label=label,
            instances=manual_instances,
        )

    try:
        _, profile = update_config(_mutate)
    except ValueError as exc:
        return {"ok": False, "error": _profile_value_error(exc)}
    return {"ok": True, "profile": profile}


@router.put("/user-profiles/{profile_id}")
def api_user_profiles_update(profile_id: str, payload: dict[str, Any] = Body(default_factory=dict), request: Request = cast(Request, None)) -> Any:
    cfg = dict(load_config() or {})
    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked
    pid = normalize_user_profile_id(profile_id)

    def _mutate(latest: dict[str, Any]) -> dict[str, Any]:
        if not pid or not any(row.get("id") == pid for row in list_user_profiles(latest)):
            raise KeyError("not_found")
        resource_payload = payload or {}
        raw_instances = resource_payload.get("instances")
        manual_instances: dict[str, Any] = raw_instances if isinstance(raw_instances, dict) else {}
        if any(key in resource_payload for key in ("sync_pairs", "watcher_routes", "webhook_routes", "resources")):
            raw_resources = resource_payload.get("resources")
            resources: dict[str, Any] = raw_resources if isinstance(raw_resources, dict) else {}
            return apply_profile_resource_assignments(
                latest,
                pid,
                label=resource_payload.get("label") or resource_payload.get("name"),
                manual_instances=manual_instances,
                selected_sync_pairs=resource_payload.get("sync_pairs", resources.get("sync_pairs") if isinstance(resources, dict) else []),
                selected_watcher_routes=resource_payload.get("watcher_routes", resources.get("watcher_routes") if isinstance(resources, dict) else []),
                selected_webhook_routes=resource_payload.get("webhook_routes", resources.get("webhook_routes") if isinstance(resources, dict) else []),
            )
        return upsert_user_profile(
            latest,
            pid,
            label=resource_payload.get("label") or resource_payload.get("name"),
            instances=resource_payload.get("instances") if isinstance(resource_payload.get("instances"), dict) else None,
        )

    try:
        _, profile = update_config(_mutate)
    except KeyError:
        return {"ok": False, "error": "not_found"}
    except ValueError as exc:
        return {"ok": False, "error": _profile_value_error(exc)}
    return {"ok": True, "profile": profile}


def _managed_usernames_for_profile(cfg: dict[str, Any], profile_id: str) -> list[str]:
    pid = normalize_user_profile_id(profile_id)
    app_auth = cfg.get("app_auth")
    if not isinstance(app_auth, dict):
        return []
    users = app_auth.get("users")
    if not isinstance(users, dict):
        return []
    names: list[str] = []
    for raw in users.values():
        if not isinstance(raw, dict):
            continue
        if normalize_user_profile_id(raw.get("profile_id")) != pid:
            continue
        username = str(raw.get("username") or "").strip()
        names.append(username or "Unnamed user")
    return names


@router.delete("/user-profiles/{profile_id}")
def api_user_profiles_delete(profile_id: str, request: Request = cast(Request, None)) -> Any:
    cfg = dict(load_config() or {})
    blocked = _admin_required_response(request, cfg)
    if blocked is not None:
        return blocked

    def _mutate(latest: dict[str, Any]) -> dict[str, Any]:
        linked_users = _managed_usernames_for_profile(latest, profile_id)
        if linked_users:
            return {"conflict": linked_users}
        clear_profile_resource_assignments(latest, profile_id)
        return {"deleted": delete_user_profile(latest, profile_id)}

    _, result = update_config(_mutate)
    linked_users = result.get("conflict") if isinstance(result, dict) else None
    if isinstance(linked_users, list) and linked_users:
        label = ", ".join(linked_users[:3])
        if len(linked_users) > 3:
            label = f"{label} and {len(linked_users) - 3} more"
        plural = "accounts" if len(linked_users) != 1 else "account"
        return JSONResponse(
            {"ok": False, "error": f"Cannot delete this profile because it is assigned to user {plural}: {label}. Reassign or delete the user first."},
            status_code=409,
        )
    deleted = bool(result.get("deleted")) if isinstance(result, dict) else False
    return {"ok": True, "deleted": deleted}
