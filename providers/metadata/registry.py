
"""
Metadata provider registry: discovers modules named _meta_*.py and aggregates their manifests and HTML snippets.
"""
from __future__ import annotations
import importlib, pkgutil, inspect
from typing import Any, List, Dict
import providers.metadata as _metapkg

PKG_NAME = __package__  # "providers.metadata"
PKG_PATHS = list(getattr(_metapkg, '__path__', []))  # type: ignore

def _iter_meta_modules():
    for finder, name, ispkg in pkgutil.iter_modules(PKG_PATHS):
        if not name.startswith("_meta_"):
            continue
        yield importlib.import_module(f"{PKG_NAME}.{name}")

def _provider_from_module(mod):
    prov = getattr(mod, "PROVIDER", None)
    if prov is not None:
        return prov
    for _, obj in inspect.getmembers(mod, inspect.isclass):
        if hasattr(obj, "manifest"):
            try:
                return obj()  # type: ignore[call-arg]
            except Exception:
                pass
    return None

def metadata_providers_manifests() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for mod in _iter_meta_modules():
        prov = _provider_from_module(mod)
        if prov is None:
            continue
        try:
            man = prov.manifest() if hasattr(prov, "manifest") else {}
            if hasattr(man, "__dict__"):
                man = getattr(man, "__dict__", man)
            out.append(man)  # type: ignore[arg-type]
        except Exception:
            continue
    return out

def _module_html(mod) -> str:
    prov = _provider_from_module(mod)
    # Provider instance method
    if prov is not None and hasattr(prov, "html"):
        try:
            html = prov.html()
            if isinstance(html, str) and html.strip():
                return html
        except Exception:
            pass
    # Module-level html()
    if hasattr(mod, "html"):
        try:
            html = mod.html()  # type: ignore[call-arg]
            if isinstance(html, str) and html.strip():
                return html
        except Exception:
            pass
    # Fallback minimal card
    label = getattr(prov, "name", "Metadata")
    return f"""<div class="section"><div class="head"><span class="chev"></span><strong>{label}</strong></div><div class="body"><div class="sub">No UI provided for {label}.</div></div></div>"""

def metadata_providers_html() -> str:
    frags: List[str] = []
    for mod in _iter_meta_modules():
        try:
            frags.append(_module_html(mod))
        except Exception:
            continue
    return "".join(frags)
