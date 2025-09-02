from __future__ import annotations
import importlib, pkgutil
from pathlib import Path
from typing import Any, Dict
from ._auth_base import AuthProvider, AuthStatus, AuthManifest
from _logging import log

def _discover() -> dict[str, AuthProvider]:
    out: dict[str, AuthProvider] = {}
    pkg = __package__  # "auth"
    pkg_path = Path(__file__).parent
    for m in pkgutil.iter_modules([str(pkg_path)]):
        if not m.name.startswith("_auth_"):
            continue
        mod = importlib.import_module(f"{pkg}.{m.name}")
        prov = getattr(mod, "PROVIDER", None)
        if prov and isinstance(getattr(prov, "name", None), str):
            out[prov.name.upper()] = prov
    return out

class AuthManager:
    """Facade for FastAPI routes."""
    def __init__(self, load_cfg, save_cfg):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.registry = _discover()

    # list providers + status + manifest
    def list(self) -> list[dict]:
        cfg = self.load_cfg()
        data: list[dict] = []
        for name, prov in self.registry.items():
            man = prov.manifest()
            st = prov.get_status(cfg)
            data.append({"name": name, "manifest": man.__dict__, "status": st.__dict__})
        log("Auth: list providers", level="DEBUG", module="AUTH")
        return data

    def start(self, provider: str, redirect_uri: str) -> dict:
        cfg = self.load_cfg()
        prov = self.registry[provider.upper()]
        res = prov.start(cfg, redirect_uri)
        self.save_cfg(cfg)
        log("Auth: start", level="INFO", module="AUTH", extra={"provider": provider})
        return res

    def finish(self, provider: str, **payload) -> dict:
        cfg = self.load_cfg()
        prov = self.registry[provider.upper()]
        st = prov.finish(cfg, **payload)
        self.save_cfg(cfg)
        log("Auth: finish", level="INFO", module="AUTH", extra={"provider": provider})
        return st.__dict__

    def refresh(self, provider: str) -> dict:
        cfg = self.load_cfg()
        prov = self.registry[provider.upper()]
        st = prov.refresh(cfg)
        self.save_cfg(cfg)
        log("Auth: refresh", level="INFO", module="AUTH", extra={"provider": provider})
        return st.__dict__

    def disconnect(self, provider: str) -> dict:
        cfg = self.load_cfg()
        prov = self.registry[provider.upper()]
        st = prov.disconnect(cfg)
        self.save_cfg(cfg)
        log("Auth: disconnect", level="INFO", module="AUTH", extra={"provider": provider})
        return st.__dict__
