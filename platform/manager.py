from __future__ import annotations
import importlib, pkgutil, json, time
from pathlib import Path
from typing import Any, Dict, Mapping, MutableMapping, List
from _logging import log

def _discover_providers() -> dict[str, Any]:
    """Find /auth/_auth_*.py and return {NAME: provider_instance}."""
    out: dict[str, Any] = {}
    try:
        import auth
    except Exception as e:
        log(f"Platform: auth package not found: {e}", level="ERROR", module="PLATFORM")
        return out
    for pkg_path in getattr(auth, "__path__", []):
        for m in pkgutil.iter_modules([str(pkg_path)]):
            if not m.name.startswith("_auth_"):
                continue
            mod = importlib.import_module(f"auth.{m.name}")
            prov = getattr(mod, "PROVIDER", None)
            if prov and isinstance(getattr(prov, "name", None), str):
                out[prov.name.upper()] = prov
    return out

def _merge_constraints(src_caps: dict, dst_caps: dict) -> dict:
    notes: dict[str, Any] = {}
    s_scale = src_caps.get("features", {}).get("ratings", {}).get("scale")
    d_scale = dst_caps.get("features", {}).get("ratings", {}).get("scale")
    if s_scale and d_scale and s_scale != d_scale:
        notes["ratings.scale_mismatch"] = f"{s_scale} vs {d_scale}"
    return notes

def compute_sync_options(src_caps: dict, dst_caps: dict, *, direction: str = "mirror") -> Dict[str, Any]:
    """Compute allowed features for source → target with direction 'mirror' or 'two-way'."""
    result: Dict[str, Any] = {
        "watchlist": False, "collections": False, "ratings": False, "watched": False, "liked_lists": False
    }
    for feat in list(result.keys()):
        s = src_caps.get("features", {}).get(feat, {})
        d = dst_caps.get("features", {}).get(feat, {})
        if direction == "mirror":
            result[feat] = bool(s.get("read") and d.get("write"))
        else:
            result[feat] = bool(s.get("read") and s.get("write") and d.get("read") and d.get("write"))
    ent_src = set(src_caps.get("entity_types", []) or [])
    ent_dst = set(dst_caps.get("entity_types", []) or [])
    result["entity_types"] = sorted(ent_src & ent_dst)
    result["notes"] = _merge_constraints(src_caps, dst_caps)
    return result

class PlatformManager:
    """Unified facade: auth + capabilities + sync profiles (no external managers)."""
    def __init__(self, load_cfg, save_cfg, profiles_path: Path):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.profiles_path = profiles_path
        self.profiles_path.parent.mkdir(parents=True, exist_ok=True)
        self.providers = _discover_providers()

    # ---------- Providers ----------
    def providers_list(self) -> list[dict]:
        """[{ name, manifest, status, capabilities }]"""
        cfg = self.load_cfg()
        out: list[dict] = []
        for name, prov in self.providers.items():
            man = prov.manifest()
            caps = getattr(prov, "capabilities", lambda: {"features": {}, "entity_types": []})()
            st = prov.get_status(cfg)
            out.append({"name": name, "manifest": man.__dict__, "status": st.__dict__, "capabilities": caps})
        log("Platform: providers listed", level="DEBUG", module="PLATFORM")
        return out

    # ---------- Auth ----------
    def auth_start(self, provider: str, redirect_uri: str) -> dict:
        cfg = self.load_cfg()
        prov = self.providers[provider.upper()]
        res = prov.start(cfg, redirect_uri)
        self.save_cfg(cfg)
        log("Platform: auth start", level="INFO", module="PLATFORM", extra={"provider": provider})
        return res

    def auth_finish(self, provider: str, **payload) -> dict:
        cfg = self.load_cfg()
        prov = self.providers[provider.upper()]
        st = prov.finish(cfg, **payload)
        self.save_cfg(cfg)
        log("Platform: auth finish", level="INFO", module="PLATFORM", extra={"provider": provider})
        return st.__dict__

    def auth_refresh(self, provider: str) -> dict:
        cfg = self.load_cfg()
        prov = self.providers[provider.upper()]
        st = prov.refresh(cfg)
        self.save_cfg(cfg)
        log("Platform: auth refresh", level="INFO", module="PLATFORM", extra={"provider": provider})
        return st.__dict__

    def auth_disconnect(self, provider: str) -> dict:
        cfg = self.load_cfg()
        prov = self.providers[provider.upper()]
        st = prov.disconnect(cfg)
        self.save_cfg(cfg)
        log("Platform: auth disconnect", level="INFO", module="PLATFORM", extra={"provider": provider})
        return st.__dict__

    # ---------- Capabilities / Options ----------
    def sync_options(self, source: str, target: str, direction: str = "mirror") -> dict:
        s = self.providers[source.upper()].capabilities()
        t = self.providers[target.upper()].capabilities()
        return compute_sync_options(s, t, direction=direction)

    # ---------- Profiles ----------
    def _read_profiles(self) -> List[dict]:
        if not self.profiles_path.exists():
            return []
        try:
            return json.loads(self.profiles_path.read_text(encoding="utf-8"))
        except Exception:
            return []

    def _write_profiles(self, rows: List[dict]) -> None:
        tmp = self.profiles_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows, indent=2), encoding="utf-8")
        tmp.replace(self.profiles_path)

    def sync_profiles(self) -> List[dict]:
        return self._read_profiles()

    def sync_profiles_upsert(self, profile: dict) -> dict:
        prof = dict(profile)
        prof["direction"] = str(prof.get("direction", "mirror")).lower()
        src = prof["source"].upper()
        dst = prof["target"].upper()
        caps = self.sync_options(src, dst, prof["direction"])
        feats = prof.get("features", {}) or {}
        for k, v in feats.items():
            if v and not caps.get(k, False):
                raise ValueError(f"Feature '{k}' not supported for {src} → {dst} ({prof['direction']})")
        key = prof.get("id") or f"{src}→{dst}"
        prof["id"] = key
        prof["updated_at"] = int(time.time())
        rows = self._read_profiles()
        for i, r in enumerate(rows):
            if r.get("id") == key:
                rows[i] = prof
                break
        else:
            rows.append(prof)
        self._write_profiles(rows)
        log("Platform: profile saved", level="SUCCESS", module="PLATFORM", extra={"id": key})
        return prof
