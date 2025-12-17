# cw_platform/orchestration/_pairs_utils.py
# Utility functions for data pair synchronization.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Mapping, Callable
from typing import Any
import importlib
from collections.abc import Mapping as _Mapping
from ..id_map import canonical_key as _ck, ID_KEYS

def supports_feature(ops, feature: str) -> bool:
    try:
        feats = (ops.capabilities() or {}).get("features", {})
        val = feats.get(feature)
        return True if val is None else bool(val)
    except Exception:
        return True

def resolve_flags(fcfg: Any, sync_cfg: Mapping[str, Any] | None) -> dict[str, bool]:
    fcfg = fcfg if isinstance(fcfg, dict) else {}
    cfg = sync_cfg or {}
    allow_adds = fcfg.get("add")
    if allow_adds is None:
        allow_adds = bool(cfg.get("enable_add", True))
    allow_removals = fcfg.get("remove")
    if allow_removals is None:
        allow_removals = bool(cfg.get("enable_remove", False))
    return {"allow_adds": bool(allow_adds), "allow_removals": bool(allow_removals)}

def apply_verify_supported(ops) -> bool:
    try:
        caps = ops.capabilities() or {}
        return bool(caps.get("verify_after_write", False))
    except Exception:
        return False

def apply_verify_after_write_supported(ops) -> bool:
    return apply_verify_supported(ops)

def health_status(h: Mapping[str, Any] | None) -> str:
    try:
        return str((h or {}).get("status") or "").lower()
    except Exception:
        return ""

def health_feature_ok(h: Mapping[str, Any] | None, feature: str) -> bool:
    try:
        feats = (h or {}).get("features") or {}
        val = feats.get(feature)
        return True if val is None else bool(val)
    except Exception:
        return True

def rate_remaining(h: Mapping[str, Any] | None) -> int | None:
    try:
        api = (h or {}).get("api") or {}
        if not isinstance(api, Mapping):
            return None
        rate = api.get("rate_limit") or {}
        if not isinstance(rate, Mapping):
            return None
        val = rate.get("remaining")
        if val is None:
            return None
        return int(val)
    except Exception:
        return None

def inject_ctx_into_provider(ops, ctx) -> None:
    try:
        try:
            setattr(ops, "ctx", ctx)
        except Exception:
            pass

        modname = getattr(ops, "__module__", None) or ops.__class__.__module__
        if not modname:
            return

        try:
            mod = importlib.import_module(modname)
            setattr(mod, "ctx", ctx)
        except Exception:
            pass

        try:
            base = modname.rsplit(".", 1)[0]
            candidates = {
                f"{base}._mod_common",
                modname.replace("_mod_PLEX", "_mod_common")
                      .replace("_mod_TRAKT", "_mod_common")
                      .replace("_mod_SIMKL", "_mod_common")
                      .replace("_mod_JELLYFIN", "_mod_common"),
            }
            for cname in candidates:
                try:
                    cmod = importlib.import_module(cname)
                    setattr(cmod, "ctx", ctx)
                except Exception:
                    continue
        except Exception:
            pass
    except Exception:
        pass

def pair_key(a: str, b: str, *, mode: str = "two-way", src: str | None = None, dst: str | None = None) -> str:
    try:
        mode = (mode or "two-way").lower()
    except Exception:
        mode = "two-way"

    if mode == "one-way" and src and dst:
        return f"{str(src).upper()}-{str(dst).upper()}"

    A, B = str(a).upper(), str(b).upper()
    return "-".join(sorted([A, B]))

def manual_policy(state: _Mapping[str, Any] | None, provider: str, feature: str) -> tuple[dict[str, Any], set[str]]:
    st = state or {}
    provs = st.get("providers") or {}
    p = provs.get(str(provider).upper()) or provs.get(str(provider).lower()) or {}
    feat_key = str(feature).lower()

    f: Any = {}
    manual = p.get("manual")
    if isinstance(manual, _Mapping):
        f = manual.get(feat_key) or manual.get(str(feature)) or {}
    if not isinstance(f, _Mapping) or not f:
        direct = p.get(feat_key) or p.get(str(feature)) or {}
        if isinstance(direct, _Mapping):
            f = direct

    blocks_raw = f.get("blocks") or []
    adds_raw = ((f.get("adds") or {}).get("items")) or {}

    blocks: set[str] = set()
    if isinstance(blocks_raw, (list, tuple, set)):
        for x in blocks_raw:
            s = str(x).strip()
            if s:
                blocks.add(s.lower())
    elif isinstance(blocks_raw, dict):
        for k in blocks_raw.keys():
            s = str(k).strip()
            if s:
                blocks.add(s.lower())

    adds: dict[str, Any] = {}
    if isinstance(adds_raw, dict):
        for k, v in adds_raw.items():
            kk = str(k).strip()
            if kk:
                adds[kk] = v

    return adds, blocks


def merge_manual_adds(idx: dict[str, Any], adds: _Mapping[str, Any] | None) -> dict[str, Any]:
    if not adds:
        return dict(idx or {})
    out = dict(idx or {})
    for k, v in adds.items():
        kk = str(k).strip()
        if not kk:
            continue
        if kk not in out:
            out[kk] = v
    return out


def filter_manual_block(items: list[dict[str, Any]] | None, blocked: set[str] | None) -> list[dict[str, Any]]:
    if not items:
        return []
    if not blocked:
        return list(items)

    blk = set(blocked)

    def _hit(it: _Mapping[str, Any]) -> bool:
        try:
            if _ck(it).lower() in blk:
                return True
        except Exception:
            pass

        ids = it.get("ids") or {}
        if isinstance(ids, _Mapping):
            for k in ID_KEYS:
                v = ids.get(k)
                if v is None or str(v) == "":
                    continue
                if f"{str(k).lower()}:{str(v).lower()}" in blk:
                    return True

        t = str(it.get("title") or "").strip().lower()
        y = it.get("year")
        if t and y is not None:
            tok = f"title:{t}|year:{str(y).strip()}"
            if tok in blk:
                return True

        return False

    return [it for it in items if not _hit(it)]

_supports_feature = supports_feature
_resolve_flags = resolve_flags
_apply_verify_after_write_supported = apply_verify_after_write_supported
_health_status = health_status
_health_feature_ok = health_feature_ok
_rate_remaining = rate_remaining
_inject_ctx_into_provider = inject_ctx_into_provider