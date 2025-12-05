# providers/auth/registry.py
# CrossWatch - Auth Providers Registry
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import dataclasses
import importlib
import inspect
import pkgutil
from pathlib import Path
from types import ModuleType
from typing import Any


PKG_NAME: str = __package__ or "providers.auth"
try:
    import providers.auth as _authpkg
    PKG_PATHS = list(getattr(_authpkg, "__path__", []))
except Exception:
    PKG_PATHS: list[str] = []


def _filesystem_module_names() -> list[str]:
    names: set[str] = set()
    for p in PKG_PATHS:
        try:
            base = Path(p)
            for f in base.glob("_auth_*.py"):
                if f.name == "_auth_base.py":
                    continue
                names.add(f.stem)
        except Exception:
            continue
    return sorted(names)


def _pkgutil_module_names() -> list[str]:
    names: list[str] = []
    for _, name, ispkg in pkgutil.iter_modules(PKG_PATHS):
        if ispkg:
            continue
        if not name.startswith("_auth_"):
            continue
        if name == "_auth_base":
            continue
        names.append(name)
    return sorted(names)


def _discover_module_names() -> list[str]:
    s = set(_pkgutil_module_names()) | set(_filesystem_module_names())
    return sorted(s)


def _safe_import(fullname: str) -> ModuleType | None:
    try:
        return importlib.import_module(fullname)
    except Exception:
        return None


def _iter_auth_modules() -> list[ModuleType]:
    importlib.invalidate_caches()
    mods: list[ModuleType] = []
    for modname in _discover_module_names():
        mod = _safe_import(f"{PKG_NAME}.{modname}")
        if mod is not None:
            mods.append(mod)
    return mods


def _provider_from_module(mod: ModuleType) -> Any | None:
    prov = getattr(mod, "PROVIDER", None)
    if prov is not None:
        return prov
    for _, obj in inspect.getmembers(mod, inspect.isclass):
        if hasattr(obj, "manifest"):
            try:
                return obj()  # type: ignore[misc]
            except Exception:
                pass
    return None


def _manifest_to_dict(man: Any) -> dict[str, Any]:
    if dataclasses.is_dataclass(man):
        return dataclasses.asdict(man)  # type: ignore[arg-type]
    if isinstance(man, dict):
        return dict(man)
    d = getattr(man, "__dict__", None)
    return dict(d) if isinstance(d, dict) else {"name": str(man)}


def auth_providers_manifests() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for mod in _iter_auth_modules():
        prov = _provider_from_module(mod)
        if prov is None:
            continue
        try:
            man = prov.manifest()
            out.append(_manifest_to_dict(man))
        except Exception:
            continue
    return out


def _module_html(mod: ModuleType) -> str:
    prov = _provider_from_module(mod)
    if prov is not None and hasattr(prov, "html"):
        try:
            html = prov.html()
            if isinstance(html, str) and html.strip():
                return html
        except Exception:
            pass
    if hasattr(mod, "html"):
        try:
            html = mod.html()  # type: ignore[misc]
            if isinstance(html, str) and html.strip():
                return html
        except Exception:
            pass

    prov_name = getattr(prov, "name", getattr(mod, "__name__", "Auth"))
    label: str | None
    try:
        m = prov.manifest() if prov else None
        label = getattr(m, "label", None) if m else None
    except Exception:
        label = None
    label = label or str(prov_name).title().replace("_", " ")

    return (
        f'<div class="section">'
        f'  <div class="head"><span class="chev"></span><strong>{label}</strong></div>'
        f'  <div class="body"><div class="sub">No custom UI provided for {label}.</div></div>'
        f'</div>'
    )


def auth_providers_html() -> str:
    frags: list[str] = []
    for mod in _iter_auth_modules():
        try:
            frags.append(_module_html(mod))
        except Exception:
            continue
    return "".join(frags)
