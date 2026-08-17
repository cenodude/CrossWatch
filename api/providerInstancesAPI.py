# /api/providerInstancesAPI.py
# CrossWatch - Provider instance management API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, cast
import copy
import re
from functools import lru_cache

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
    list_instance_ids,
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
_MAX_GENERATED_PROFILES = 9


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


def _canonical_profile_id(provider: str, instance_id: Any) -> str | None:
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
    if num < 1 or num > _MAX_GENERATED_PROFILES:
        return ""
    return f"{prov}-P{num:02d}"


def _next_profile_id(provider: str, insts: dict[str, Any]) -> str:
    prov = _prov_prefix(provider)
    existing = {str(k).strip().upper() for k in (insts or {}).keys()}
    for n in range(1, _MAX_GENERATED_PROFILES + 1):
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


def _managed_user_instance_scope(cfg: dict[str, Any], request: Request | None) -> dict[str, set[str]] | None:
    if request is None:
        return None
    try:
        from api.appAuthAPI import COOKIE_NAME, current_user

        user = current_user(cfg, request.cookies.get(COOKIE_NAME))
    except Exception:
        return None
    if not isinstance(user, dict) or user.get("is_admin"):
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
    return _admin_required(request, cfg)


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

    inst = _next_profile_id(provider, insts)
    if not inst:
        return {"ok": False, "error": "profile_limit_reached"}

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
    canon = _canonical_profile_id(provider, instance_id)
    if canon is None:
        return {"ok": False, "error": "reserved_instance_id"}
    if not canon:
        return {"ok": False, "error": "invalid_instance_id"}
    inst = canon

    cfg = dict(load_config() or {})
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
    try:
        from api.appAuthAPI import COOKIE_NAME, current_user

        token = request.cookies.get(COOKIE_NAME) if request is not None else None
        user = current_user(cfg, token)
        if user and not user.get("is_admin"):
            pid = normalize_user_profile_id(user.get("profile_id"))
            rows = [row for row in rows if row.get("id") == pid]
            include_resources = False
    except Exception:
        pass
    payload: dict[str, Any] = {"ok": True, "items": _decorate_user_profile_rows(cfg, rows)}
    if include_resources:
        payload["resources"] = profile_resource_summary(cfg)
    return JSONResponse(payload)


@router.get("/user-profiles/{profile_id}")
def api_user_profiles_get(profile_id: str, request: Request = cast(Request, None)) -> JSONResponse:
    cfg = dict(load_config() or {})
    pid = normalize_user_profile_id(profile_id)
    try:
        from api.appAuthAPI import COOKIE_NAME, auth_required, current_user

        if request is not None and auth_required(cfg):
            token = request.cookies.get(COOKIE_NAME)
            user = current_user(cfg, token)
            if user is None:
                return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
            if not user.get("is_admin") and normalize_user_profile_id(user.get("profile_id")) != pid:
                return JSONResponse({"ok": False, "error": "not_found"}, status_code=404)
    except Exception:
        if request is not None:
            return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
    for row in list_user_profiles(cfg):
        if row.get("id") == pid:
            return JSONResponse({"ok": True, "profile": _decorate_user_profile_rows(cfg, [row])[0], "resources": profile_resource_summary(cfg)})
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
