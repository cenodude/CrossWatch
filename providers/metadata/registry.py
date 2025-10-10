"""
Metadata provider registry: discovers modules named _meta_*.py and aggregates their manifests and HTML snippets.
"""
from __future__ import annotations
import importlib, pkgutil, inspect
from typing import Any, List, Dict
from dataclasses import is_dataclass, asdict
import providers.metadata as _metapkg

PKG_NAME = __package__  # "providers.metadata"
PKG_PATHS = list(getattr(_metapkg, '__path__', []))  # type: ignore

# ────────────────────────────────────────────────────────────────────────────
# Discover providers (modules with _meta_* prefix)
# ────────────────────────────────────────────────────────────────────────────
def _iter_meta_modules():
    for finder, name, ispkg in pkgutil.iter_modules(PKG_PATHS):
        if not name.startswith("_meta_"):
            continue
        yield importlib.import_module(f"{PKG_NAME}.{name}")

# ────────────────────────────────────────────────────────────────────────────
# Get a provider instance from a module (PROVIDER or class with manifest())
# ────────────────────────────────────────────────────────────────────────────
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

# ────────────────────────────────────────────────────────────────────────────
# Normalize manifest to a plain JSON-safe dict 
# ────────────────────────────────────────────────────────────────────────────
def _coerce_manifest(man):
    if isinstance(man, dict):                          # plain dict
        src = man
    elif man is None:                                  # nothing to show
        src = {}
    elif is_dataclass(man):                            # dataclass
        src = asdict(man)
    elif hasattr(man, "model_dump"):                   # pydantic v2
        src = man.model_dump()
    elif hasattr(man, "dict"):                         # pydantic v1
        src = man.dict()
    elif hasattr(man, "_asdict"):                      # namedtuple-ish
        src = dict(man._asdict())
    else:                                              # object with @property
        src = {k: getattr(man, k) for k in ("id","name","enabled","ready","ok","version") if hasattr(man, k)}

    # keep only JSON-safe primitives
    out = {}
    for k, v in (src or {}).items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            out[k] = v
        else:
            out[k] = str(v)
    return out


# ────────────────────────────────────────────────────────────────────────────
# Aggregate provider manifests (JSON-safe)
# ────────────────────────────────────────────────────────────────────────────
def metadata_providers_manifests() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for mod in _iter_meta_modules():
        prov = _provider_from_module(mod)
        if prov is None:
            continue
        try:
            man = prov.manifest() if hasattr(prov, "manifest") else {}
            out.append(_coerce_manifest(man))  # ← instead of __dict__
        except Exception:
            continue
    return out

# ────────────────────────────────────────────────────────────────────────────
# Render provider HTML (module/instance html() or minimal card)
# ────────────────────────────────────────────────────────────────────────────
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

# ────────────────────────────────────────────────────────────────────────────
# Concatenate provider HTML fragments
# ────────────────────────────────────────────────────────────────────────────
def metadata_providers_html() -> str:
    frags: List[str] = []
    for mod in _iter_meta_modules():
        try:
            frags.append(_module_html(mod))
        except Exception:
            continue
    return "".join(frags)
