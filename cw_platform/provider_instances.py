# cw_platform/provider_instances.py
# Provider instance helpers (multi-profile / multi-account).
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
import copy
import re

_DEFAULT_INSTANCE = "default"
_INSTANCES_KEY = "instances"
_CROSSWATCH_PROFILE_ROOT = "profiles"


def _deep_merge(base: dict[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(base or {})
    for k, v in (overlay or {}).items():
        key = str(k)
        cur = out.get(key)
        if isinstance(cur, dict) and isinstance(v, Mapping):
            out[key] = _deep_merge(cur, dict(v))
        else:
            out[key] = copy.deepcopy(v)
    return out


def normalize_instance_id(v: Any) -> str:
    s = str(v or "").strip()
    if not s or s.lower() == _DEFAULT_INSTANCE:
        return _DEFAULT_INSTANCE

    m = re.match(r"^(?P<id>.+?):(?P<n>\d+)$", s)
    if m:
        s = m.group("id").strip()
    return _DEFAULT_INSTANCE if not s or s.lower() == _DEFAULT_INSTANCE else s


def sanitize_instance_label(v: Any, *, max_len: int = 12) -> str:
    s = str(v or "").strip()
    s = " ".join(s.split())
    return s[:max(0, int(max_len or 0))]


def provider_key(provider_name: str) -> str:
    raw = str(provider_name or "").strip().lower()
    return "crosswatch" if raw in {"cw", "crosswatch"} else raw


def _profile_prefix(provider: str) -> str:
    k = provider_key(provider)
    if k == "crosswatch":
        return "CW"
    return str(provider or "").strip().upper()


def _safe_profile_dir_name(instance_id: Any) -> str:
    raw = normalize_instance_id(instance_id)
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._- ")
    return safe or "default"


def _crosswatch_profile_root_dir(base_block: Mapping[str, Any], instance_id: Any) -> str:
    root = str(base_block.get("root_dir") or "/config/.cw_provider").strip() or "/config/.cw_provider"
    inst = _safe_profile_dir_name(instance_id)
    clean_root = root.rstrip("/").rstrip("\\")
    return f"{clean_root}/{_CROSSWATCH_PROFILE_ROOT}/{inst}"


def _with_crosswatch_profile_defaults(
    provider_name: str,
    base_block: Mapping[str, Any],
    instance_id: Any,
    block: dict[str, Any],
    instance_block: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if provider_key(provider_name) != "crosswatch" or normalize_instance_id(instance_id) == _DEFAULT_INSTANCE:
        return block
    out = dict(block or {})
    inst_root = str((instance_block or {}).get("root_dir") or "").strip()
    if not inst_root:
        out["root_dir"] = _crosswatch_profile_root_dir(base_block, instance_id)
    out["connected"] = (instance_block or {}).get("connected") is True
    return out


def _config_key_for(cfg: Mapping[str, Any], provider_name: str) -> str:
    k = provider_key(provider_name)
    # TMDb sync stores auth under tmdb_sync (metadata uses tmdb).
    if k == "tmdb":
        return "tmdb_sync"
    return k


def get_provider_block(cfg: Mapping[str, Any], provider_name: str, instance_id: Any = None) -> dict[str, Any]:
    key = provider_key(provider_name)
    base = cfg.get(key) if isinstance(cfg, Mapping) else None
    base_block = dict(base or {}) if isinstance(base, Mapping) else {}

    inst = normalize_instance_id(instance_id)
    if inst == _DEFAULT_INSTANCE:
        return base_block

    insts = base_block.get(_INSTANCES_KEY)
    if isinstance(insts, Mapping) and inst in insts and isinstance(insts.get(inst), Mapping):
        inst_block = dict(insts.get(inst) or {})
        merged_base = dict(base_block or {})
        merged_base.pop(_INSTANCES_KEY, None)
        return _with_crosswatch_profile_defaults(
            provider_name,
            base_block,
            inst,
            _deep_merge(merged_base, inst_block),
            inst_block,
        )

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
        ck = _config_key_for(cfg, str(prov))
        out[ck] = copy.deepcopy(get_provider_block(cfg, ck, inst))
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


def get_instance_block(
    cfg: Mapping[str, Any] | dict[str, Any],
    provider_name: str,
    instance_id: Any = None,
    *,
    create: bool = False,
) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    key = provider_key(provider_name)

    base = cfg.get(key) if isinstance(cfg, Mapping) else None
    if not isinstance(base, dict):
        if not create or not isinstance(cfg, dict):
            return {}
        base = {}
        cfg[key] = base

    if inst == _DEFAULT_INSTANCE:
        return base

    insts = base.get(_INSTANCES_KEY)
    if not isinstance(insts, dict):
        if not create:
            return {}
        insts = {}
        base[_INSTANCES_KEY] = insts

    blk = insts.get(inst)
    if isinstance(blk, dict):
        return blk

    if not create:
        return {}

    out: dict[str, Any] = {}
    insts[inst] = out
    return out


def apply_instance_defaults(cfg: dict[str, Any], provider_name: str, instance_id: Any = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    block = get_instance_block(cfg, provider_name, inst, create=True)
    if provider_key(provider_name) == "crosswatch" and inst != _DEFAULT_INSTANCE:
        base = cfg.get(provider_key(provider_name))
        base_block = base if isinstance(base, Mapping) else {}
        block.setdefault("root_dir", _crosswatch_profile_root_dir(base_block, inst))
    if "label" in block:
        block["label"] = sanitize_instance_label(block.get("label"))
    return block


def instance_exists(cfg: Mapping[str, Any], provider_name: str, instance_id: Any = None) -> bool:
    inst = normalize_instance_id(instance_id)
    if inst == _DEFAULT_INSTANCE:
        return True
    return bool(get_instance_block(cfg, provider_name, inst, create=False))


def ensure_instance_block(cfg: dict[str, Any], provider_name: str, instance_id: Any = None) -> dict[str, Any]:
    return get_instance_block(cfg, provider_name, instance_id, create=True)
