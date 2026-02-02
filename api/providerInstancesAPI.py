# /api/providerInstancesAPI.py
# CrossWatch - Provider instance management API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
import copy
import re
from functools import lru_cache

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import list_instance_ids, provider_key, normalize_instance_id

router = APIRouter(prefix="/api", tags=["provider-instances"])


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
    return f"{prov}-P{num:02d}"


def _next_profile_id(provider: str, insts: dict[str, Any]) -> str:
    prov = _prov_prefix(provider)
    best = 0
    for k in (insts or {}).keys():
        m = re.fullmatch(rf"{re.escape(prov)}-P(\d{{2,}})", str(k).strip().upper())
        if not m:
            continue
        try:
            best = max(best, int(m.group(1)))
        except Exception:
            pass
    n = best + 1 if best else 1
    cand = f"{prov}-P{n:02d}"
    while cand in insts:
        n += 1
        cand = f"{prov}-P{n:02d}"
    return cand


def _create_instance(insts: dict[str, Any], inst: str, payload: dict[str, Any]) -> None:
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


def _provider_block(cfg: dict[str, Any], provider: str) -> dict[str, Any]:
    k = provider_key(provider)
    raw = cfg.get(k)
    if isinstance(raw, dict):
        return raw
    blk: dict[str, Any] = {}
    cfg[k] = blk
    return blk


def _strip_instances(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d or {})
    out.pop("instances", None)
    return out


def _instances_map_for(cfg: dict[str, Any], provider: str) -> list[dict[str, str]]:
    ids = list_instance_ids(cfg, provider)
    out: list[dict[str, str]] = []
    for i in ids:
        lab = "Default" if i == "default" else i
        out.append({"id": i, "label": lab})
    return out


@router.get("/provider-instances")
def api_provider_instances_all() -> JSONResponse:
    cfg = dict(load_config() or {})
    out: dict[str, Any] = {}
    for k, v in cfg.items():
        if not isinstance(v, dict):
            continue
        insts = v.get("instances")
        if isinstance(insts, dict) and insts:
            out[str(k).upper()] = _instances_map_for(cfg, k)
    # Always include defaults for known providers if present in cfg
    for prov in _known_providers_from_registry():
        k = provider_key(prov)
        if k in cfg and k.upper() not in out:
            out[k.upper()] = _instances_map_for(cfg, k)
    return JSONResponse(out)


@router.get("/provider-instances/{provider}")
def api_provider_instances_provider(provider: str) -> JSONResponse:
    cfg = dict(load_config() or {})
    return JSONResponse(_instances_map_for(cfg, provider))


@router.post("/provider-instances/{provider}/next")
def api_provider_instances_create_next(provider: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    cfg = dict(load_config() or {})
    blk = _provider_block(cfg, provider)
    insts = blk.get("instances")
    if not isinstance(insts, dict):
        insts = {}
        blk["instances"] = insts

    inst = _next_profile_id(provider, insts)
    if inst in insts and isinstance(insts.get(inst), dict):
        return {"ok": True, "id": inst}

    _create_instance(insts, inst, payload or {})
    save_config(cfg)
    return {"ok": True, "id": inst}


@router.post("/provider-instances/{provider}/{instance_id}")
def api_provider_instances_create(provider: str, instance_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> dict[str, Any]:
    canon = _canonical_profile_id(provider, instance_id)
    if canon is None:
        return {"ok": False, "error": "reserved_instance_id"}
    if not canon:
        return {"ok": False, "error": "invalid_instance_id"}
    inst = canon

    cfg = dict(load_config() or {})
    blk = _provider_block(cfg, provider)
    insts = blk.get("instances")
    if not isinstance(insts, dict):
        insts = {}
        blk["instances"] = insts

    if inst in insts and isinstance(insts.get(inst), dict):
        return {"ok": True, "id": inst}

    _create_instance(insts, inst, payload or {})

    save_config(cfg)
    return {"ok": True, "id": inst}


@router.delete("/provider-instances/{provider}/{instance_id}")
def api_provider_instances_delete(provider: str, instance_id: str) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    if inst == "default":
        return {"ok": False, "error": "cannot_delete_default"}

    cfg = dict(load_config() or {})
    blk = _provider_block(cfg, provider)
    insts = blk.get("instances")
    if not isinstance(insts, dict) or inst not in insts:
        return {"ok": False, "error": "not_found"}

    insts.pop(inst, None)
    save_config(cfg)
    return {"ok": True}
