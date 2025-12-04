from __future__ import annotations
import json, os, importlib, pkgutil
from pathlib import Path
from typing import Any, Dict, List

from cw_platform.config_base import CONFIG

from _logging import log

class PlatformManager:
    def __init__(self, load_cfg, save_cfg, profiles_path: Path | None = None) -> None:
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.profiles_path = profiles_path or (CONFIG / "profiles.json")
        self._providers = self._discover_providers()

    # discovery
    def _discover_providers(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        try:
            import providers.auth as auth_pkg
        except Exception as e:
            log(f"auth package missing: {e}", level="ERROR", module="PLATFORM")
            return out
        for p in getattr(auth_pkg, "__path__", []):
            for m in pkgutil.iter_modules([str(p)]):
                if not m.name.startswith("_auth_"): continue
                mod = importlib.import_module(f"providers.auth.{m.name}")
                prov = getattr(mod, "PROVIDER", None)
                if prov:
                    name = getattr(prov, "name", m.name.replace("_auth_",""))
                    out[name.upper()] = prov
        return out

    # Providers
    def providers_list(self) -> List[dict]:
        items = []
        for name, prov in self._providers.items():
            try:
                man = prov.manifest()
            except Exception:
                man = {"name": name, "label": name.title()}
            try:
                caps = prov.capabilities()
            except Exception:
                caps = {}
            try:
                st = prov.get_status(self.load_cfg())
            except Exception:
                st = {"connected": False}
            items.append({"name": name, "manifest": man, "capabilities": caps, "status": st})
        return items

    # auth actions
    def auth_start(self, provider: str, payload: dict) -> dict:
        prov = self._providers.get(provider.upper())
        if not prov: raise ValueError(f"Unknown provider: {provider}")
        return prov.start(self.load_cfg(), payload or {}, self.save_cfg)

    def auth_finish(self, provider: str, payload: dict | None = None) -> dict:
        prov = self._providers.get(provider.upper())
        if not prov: raise ValueError(f"Unknown provider: {provider}")
        return prov.finish(self.load_cfg(), payload or {}, self.save_cfg)

    def auth_refresh(self, provider: str) -> dict:
        prov = self._providers.get(provider.upper())
        if not prov: raise ValueError(f"Unknown provider: {provider}")
        return prov.refresh(self.load_cfg(), self.save_cfg)

    def auth_disconnect(self, provider: str) -> dict:
        prov = self._providers.get(provider.upper())
        if not prov: raise ValueError(f"Unknown provider: {provider}")
        return prov.disconnect(self.load_cfg(), self.save_cfg)

    # sync options
    def _caps(self, name: str) -> dict:
        prov = self._providers.get(name.upper())
        return prov.capabilities() if prov else {}

    def sync_options(self, source: str, target: str, direction: str = "mirror") -> dict:
        s = self._caps(source) or {}
        t = self._caps(target) or {}
        feats = {k: bool((s.get("features",{}).get(k) or {}).get("read") and (t.get("features",{}).get(k) or {}).get("write")) for k in set((s.get("features") or {}).keys()) | set((t.get("features") or {}).keys())}
        return feats

    # Profiles
    def _read_profiles(self) -> list[dict]:
        try:
            return json.loads(self.profiles_path.read_text(encoding="utf-8"))
        except Exception:
            return []

    def _write_profiles(self, arr: list[dict]) -> None:
        self.profiles_path.parent.mkdir(parents=True, exist_ok=True)
        self.profiles_path.write_text(json.dumps(arr, indent=2), encoding="utf-8")

    def sync_profiles(self) -> list[dict]:
        return self._read_profiles()

    def sync_profiles_save(self, items: list[dict]) -> None:
        self._write_profiles(items)
