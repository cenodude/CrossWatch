# cw_platform/provider_instances.py
# Provider instance helpers (multi-profile / multi-account).
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import copy

_DEFAULT_INSTANCE = "default"
_INSTANCES_KEY = "instances"


def normalize_instance_id(v: Any) -> str:
    s = str(v or "").strip()
    return _DEFAULT_INSTANCE if not s or s.lower() == _DEFAULT_INSTANCE else s


def provider_key(provider_name: str) -> str:
    return str(provider_name or "").strip().lower()


def get_provider_block(cfg: Mapping[str, Any], provider_name: str, instance_id: Any = None) -> dict[str, Any]:
    key = provider_key(provider_name)
    base = cfg.get(key) if isinstance(cfg, Mapping) else None
    base_block = dict(base or {}) if isinstance(base, Mapping) else {}

    inst = normalize_instance_id(instance_id)
    if inst == _DEFAULT_INSTANCE:
        return base_block

    insts = base_block.get(_INSTANCES_KEY)
    if isinstance(insts, Mapping) and inst in insts and isinstance(insts.get(inst), Mapping):
        return dict(insts.get(inst) or {})

    return {}


def list_instance_ids(cfg: Mapping[str, Any], provider_name: str) -> list[str]:
    key = provider_key(provider_name)
    base = cfg.get(key) if isinstance(cfg, Mapping) else None
    insts = (base or {}).get(_INSTANCES_KEY) if isinstance(base, Mapping) else None

    out: list[str] = [_DEFAULT_INSTANCE]
    if isinstance(insts, Mapping):
        extra = [str(k) for k in insts.keys() if str(k).strip()]
        out.extend(sorted(set(extra)))
    return out


def build_config_view(cfg: Mapping[str, Any], selections: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(cfg or {})
    for prov, inst in (selections or {}).items():
        k = provider_key(str(prov))
        out[k] = copy.deepcopy(get_provider_block(cfg, prov, inst))
    return out


def build_pair_config_view(
    cfg: Mapping[str, Any],
    src: str,
    src_instance: Any,
    dst: str,
    dst_instance: Any,
) -> dict[str, Any]:
    return build_config_view(cfg, {src: src_instance, dst: dst_instance})


def build_provider_config_view(cfg: Mapping[str, Any], provider_name: str, instance_id: Any) -> dict[str, Any]:
    return build_config_view(cfg, {provider_name: instance_id})


def ensure_provider_block(cfg: dict[str, Any], provider_name: str) -> dict[str, Any]:
    key = provider_key(provider_name)
    blk = cfg.get(key)
    if isinstance(blk, dict):
        return blk
    out: dict[str, Any] = {}
    cfg[key] = out
    return out


def ensure_instance_block(cfg: dict[str, Any], provider_name: str, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    base = ensure_provider_block(cfg, provider_name)
    if inst == _DEFAULT_INSTANCE:
        return base
    insts = base.get(_INSTANCES_KEY)
    if not isinstance(insts, dict):
        insts = {}
        base[_INSTANCES_KEY] = insts
    blk = insts.get(inst)
    if isinstance(blk, dict):
        return blk
    out: dict[str, Any] = {}
    insts[inst] = out
    return out
