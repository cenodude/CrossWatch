# Metadata provider registry
from __future__ import annotations
import importlib, pkgutil, inspect
from typing import Any, List, Dict
from dataclasses import is_dataclass, asdict
import providers.metadata as _metapkg

PKG_NAME = __package__  # "providers.metadata"
PKG_PATHS = list(getattr(_metapkg, '__path__', []))  # type: ignore

# Discover providers
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

# Manifest to JSON 
def _coerce_manifest(man):
    if isinstance(man, dict): 
        src = man
    elif man is None:
        src = {}
    elif is_dataclass(man):
        src = asdict(man)
    elif hasattr(man, "model_dump"):
        src = man.model_dump()
    elif hasattr(man, "dict"):
        src = man.dict()
    elif hasattr(man, "_asdict"):
        src = dict(man._asdict())
    else:
        src = {k: getattr(man, k) for k in ("id","name","enabled","ready","ok","version") if hasattr(man, k)}

    out = {}
    for k, v in (src or {}).items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[k] = v
        else:
            out[k] = str(v)
    return out

# Aggregate provider manifests 
def metadata_providers_manifests() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
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

# Render
def _module_html(mod) -> str:
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
            html = mod.html()  # type: ignore[call-arg]
            if isinstance(html, str) and html.strip():
                return html
        except Exception:
            pass
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