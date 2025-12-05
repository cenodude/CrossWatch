# providers/metadata/registry.py
# CrossWatch - Metadata Providers Registry
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
import inspect
import pkgutil
from dataclasses import asdict, is_dataclass
from types import ModuleType
from typing import Any

import providers.metadata as _metapkg

PKG_NAME: str = __package__ or "providers.metadata"
PKG_PATHS: list[str] = list(getattr(_metapkg, "__path__", []))  # type: ignore[attr-defined]

def _iter_meta_modules() -> list[ModuleType]:
    mods: list[ModuleType] = []
    for _, name, ispkg in pkgutil.iter_modules(PKG_PATHS):
        if ispkg:
            continue
        if not name.startswith("_meta_"):
            continue
        try:
            mods.append(importlib.import_module(f"{PKG_NAME}.{name}"))
        except Exception:
            continue
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


def _is_dataclass_instance(obj: Any) -> bool:
    return is_dataclass(obj) and not isinstance(obj, type)


def _coerce_manifest(man: Any) -> dict[str, Any]:
    if isinstance(man, dict):
        src: dict[str, Any] = man
    elif man is None:
        src = {}
    elif _is_dataclass_instance(man):
        src = asdict(man)
    elif hasattr(man, "model_dump"):
        src = man.model_dump()  # type: ignore[assignment]
    elif hasattr(man, "dict"):
        src = man.dict()  # type: ignore[assignment]
    elif hasattr(man, "_asdict"):
        src = dict(man._asdict())  # type: ignore[call-arg]
    else:
        keys = ("id", "name", "enabled", "ready", "ok", "version")
        src = {k: getattr(man, k) for k in keys if hasattr(man, k)}

    out: dict[str, Any] = {}
    for k, v in (src or {}).items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[k] = v
        else:
            out[k] = str(v)
    return out


def metadata_providers_manifests() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for mod in _iter_meta_modules():
        prov = _provider_from_module(mod)
        if prov is None:
            continue
        try:
            man = prov.manifest() if hasattr(prov, "manifest") else {}
            out.append(_coerce_manifest(man))
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
    label = getattr(prov, "name", "Metadata")
    return (
        f'<div class="section"><div class="head"><span class="chev"></span>'
        f'<strong>{label}</strong></div><div class="body"><div class="sub">'
        f'No UI provided for {label}.</div></div></div>'
    )


def metadata_providers_html() -> str:
    frags: list[str] = []
    for mod in _iter_meta_modules():
        try:
            frags.append(_module_html(mod))
        except Exception:
            continue
    return "".join(frags)
