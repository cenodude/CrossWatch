from __future__ import annotations
from typing import Any, Dict, Mapping, Optional
import importlib

def supports_feature(ops, feature: str) -> bool:
    try:
        feats = (ops.capabilities() or {}).get("features", {})
        val = feats.get(feature)
        return True if val is None else bool(val)
    except Exception:
        return True

def resolve_flags(fcfg: Any, sync_cfg: Dict[str, Any]) -> Dict[str, bool]:
    fcfg = fcfg if isinstance(fcfg, dict) else {}
    allow_adds = fcfg.get("add")
    if allow_adds is None:
        allow_adds = bool((sync_cfg or {}).get("enable_add", True))
    allow_removals = fcfg.get("remove")
    if allow_removals is None:
        allow_removals = bool((sync_cfg or {}).get("enable_remove", False))
    return {"allow_adds": bool(allow_adds), "allow_removals": bool(allow_removals)}

def apply_verify_supported(ops) -> bool:
    try:
        caps = ops.capabilities() or {}
        return bool(caps.get("verify_after_write", False))
    except Exception:
        return False

def apply_verify_after_write_supported(ops) -> bool:
    return apply_verify_supported(ops)


def health_status(h: Optional[Mapping[str, Any]]) -> str:
    try:
        return str((h or {}).get("status") or "").lower()
    except Exception:
        return ""

def health_feature_ok(h: Optional[Mapping[str, Any]], feature: str) -> bool:
    try:
        feats = (h or {}).get("features") or {}
        val = feats.get(feature)
        return True if val is None else bool(val)
    except Exception:
        return True


def rate_remaining(h: Optional[Mapping[str, Any]]) -> Optional[int]:
    try:
        api = (h or {}).get("api") or {}
        rate = api.get("rate_limit") or {}
        return int(rate.get("remaining"))
    except Exception:
        return None


def inject_ctx_into_provider(ops, ctx) -> None:
    try:
        try:
            setattr(ops, "ctx", ctx)  # instance attribute hook
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
                      .replace("_mod_JELLYFIN", "_mod_common")
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

# ---------------------------------------------------------------------------
_supports_feature = supports_feature
_resolve_flags = resolve_flags
_apply_verify_after_write_supported = apply_verify_after_write_supported
_health_status = health_status
_health_feature_ok = health_feature_ok
_rate_remaining = rate_remaining
_inject_ctx_into_provider = inject_ctx_into_provider
