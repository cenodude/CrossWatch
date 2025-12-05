# cw_platform/manager.py
# CrossWatch - Platform Manager
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
import json
import pkgutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

from cw_platform.config_base import CONFIG

from _logging import log


class PlatformManager:
    def __init__(
        self,
        load_cfg: Callable[[], dict[str, Any]],
        save_cfg: Callable[[dict[str, Any]], None],
        profiles_path: Path | None = None,
    ) -> None:
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.profiles_path = profiles_path or (CONFIG / "profiles.json")
        self._providers: dict[str, Any] = self._discover_providers()

    # discovery
    def _discover_providers(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        try:
            import providers.auth as auth_pkg
        except Exception as e:  # pragma: no cover
            log(f"auth package missing: {e}", level="ERROR", module="PLATFORM")
            return out

        for p in getattr(auth_pkg, "__path__", []):
            for m in pkgutil.iter_modules([str(p)]):
                if not m.name.startswith("_auth_"):
                    continue
                mod = importlib.import_module(f"providers.auth.{m.name}")
                prov = getattr(mod, "PROVIDER", None)
                if prov:
                    name = getattr(prov, "name", m.name.replace("_auth_", ""))
                    out[str(name).upper()] = prov
        return out

    # Providers
    def providers_list(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
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

            items.append(
                {
                    "name": name,
                    "manifest": man,
                    "capabilities": caps,
                    "status": st,
                }
            )
        return items

    # auth actions
    def auth_start(self, provider: str, payload: dict[str, Any]) -> dict[str, Any]:
        prov = self._providers.get(provider.upper())
        if not prov:
            raise ValueError(f"Unknown provider: {provider}")
        return prov.start(self.load_cfg(), payload or {}, self.save_cfg)

    def auth_finish(
        self,
        provider: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        prov = self._providers.get(provider.upper())
        if not prov:
            raise ValueError(f"Unknown provider: {provider}")
        return prov.finish(self.load_cfg(), payload or {}, self.save_cfg)

    def auth_refresh(self, provider: str) -> dict[str, Any]:
        prov = self._providers.get(provider.upper())
        if not prov:
            raise ValueError(f"Unknown provider: {provider}")
        return prov.refresh(self.load_cfg(), self.save_cfg)

    def auth_disconnect(self, provider: str) -> dict[str, Any]:
        prov = self._providers.get(provider.upper())
        if not prov:
            raise ValueError(f"Unknown provider: {provider}")
        return prov.disconnect(self.load_cfg(), self.save_cfg)

    # sync options
    def _caps(self, name: str) -> dict[str, Any]:
        prov = self._providers.get(name.upper())
        try:
            return prov.capabilities() if prov else {}
        except Exception:
            return {}

    def sync_options(
        self,
        source: str,
        target: str,
        direction: str = "mirror",  # kept for future use
    ) -> dict[str, bool]:
        s = self._caps(source) or {}
        t = self._caps(target) or {}

        s_feats = s.get("features") or {}
        t_feats = t.get("features") or {}

        feats: dict[str, bool] = {}
        for k in set(s_feats.keys()) | set(t_feats.keys()):
            s_meta = (s_feats.get(k) or {})
            t_meta = (t_feats.get(k) or {})
            feats[k] = bool(
                (s_meta.get("read") is True)
                and (t_meta.get("write") is True)
            )
        return feats

    # Profiles
    def _read_profiles(self) -> list[dict[str, Any]]:
        try:
            return json.loads(self.profiles_path.read_text(encoding="utf-8"))
        except Exception:
            return []

    def _write_profiles(self, arr: list[dict[str, Any]]) -> None:
        self.profiles_path.parent.mkdir(parents=True, exist_ok=True)
        self.profiles_path.write_text(
            json.dumps(arr, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    def sync_profiles(self) -> list[dict[str, Any]]:
        return self._read_profiles()

    def sync_profiles_save(self, items: list[dict[str, Any]]) -> None:
        self._write_profiles(items)