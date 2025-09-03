
"""
Auth provider registry: discovers modules named _auth_*.py and aggregates their manifests and HTML snippets.
"""
from __future__ import annotations
import importlib, pkgutil, inspect
from pathlib import Path
from typing import Any, List, Dict

PKG_NAME = __package__  # "providers.auth"
import providers.auth as _authpkg
PKG_PATHS = list(getattr(_authpkg, '__path__', []))  # type: ignore

def _iter_auth_modules():
    for finder, name, ispkg in pkgutil.iter_modules(PKG_PATHS):
        if not name.startswith("_auth_"):
            continue
        # skip base
        if name in ("_auth_base",):
            continue
        yield importlib.import_module(f"{PKG_NAME}.{name}")

def _provider_from_module(mod):
    # Preferred: mod.PROVIDER
    prov = getattr(mod, "PROVIDER", None)
    if prov is not None:
        return prov
    # Otherwise, try to find a class with manifest method
    for _, obj in inspect.getmembers(mod, inspect.isclass):
        if hasattr(obj, "manifest"):
            try:
                return obj()  # type: ignore[call-arg]
            except Exception:
                pass
    return None

def auth_providers_manifests() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for mod in _iter_auth_modules():
        prov = _provider_from_module(mod)
        if prov is None:
            continue
        try:
            man = prov.manifest()  # dataclass or dict-like
            # convert dataclass to dict
            if hasattr(man, "__dict__"):
                # dataclass or simple object
                man = getattr(man, "__dict__", man)
            out.append(man)  # type: ignore[arg-type]
        except Exception:
            # ignore faulty providers
            continue
    return out

def _module_html(mod) -> str:
    # Provider instance may expose html()
    prov = _provider_from_module(mod)
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
    # Fallback: generate a tiny default card
    prov_name = getattr(prov, "name", getattr(mod, "__name__", "Auth"))
    label = getattr(getattr(prov, "manifest", lambda: {})(), "label", None) if prov else None
    label = label or str(prov_name).title().replace("_", " ")
    pid = str(prov_name).lower()
    return f"""<div class="section"><div class="head"><span class="chev"></span><strong>{label}</strong></div><div class="body"><div class="sub">No custom UI provided for {label}.</div></div></div>"""

def auth_providers_html() -> str:
    fragments: List[str] = []
    for mod in _iter_auth_modules():
        try:
            fragments.append(_module_html(mod))
        except Exception:
            # skip broken provider
            continue
    return "".join(fragments)
