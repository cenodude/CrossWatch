
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
    """Yield imported metadata provider modules discovered in this package.

    Discovery strategy:
    - Walk the package paths for entries whose module name starts with "_meta_".
    - Import each matching module and yield the module object.

    Notes
    - Import side effects are accepted here by design; providers typically
      register constants, classes, or functions at import time.
    - Errors during import are allowed to propagate to the caller which can
      decide whether to skip or report them.
    """
    for finder, name, ispkg in pkgutil.iter_modules(PKG_PATHS):
        if not name.startswith("_meta_"):
            continue
        yield importlib.import_module(f"{PKG_NAME}.{name}")

def _provider_from_module(mod):
    """Return a provider instance for a given metadata module, if available.

    Resolution order:
    1) If the module exposes a ``PROVIDER`` attribute, return it as-is.
    2) Otherwise, search for a class attribute on the module that exposes a
       ``manifest`` method and attempt to instantiate it with no arguments.

    Any exceptions raised while instantiating provider classes are suppressed
    and result in this function returning ``None``.

    Parameters
    - mod: The imported module object to inspect.

    Returns
    - A provider instance (object exposing at least ``manifest()``), or ``None``
      if no suitable provider could be resolved.
    """
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
    """Collect manifest dictionaries from all discovered metadata providers.

    For each ``_meta_*`` module, this function resolves a provider instance
    (see ``_provider_from_module``). If the provider exposes a ``manifest``
    method, it is called and the result is coerced to a plain ``dict`` when
    possible (e.g., dataclass or object with ``__dict__``) before being added to
    the output list. Any errors from individual providers are isolated and
    silently skipped to keep the aggregate resilient.

    Returns
    - A list of manifest objects represented as dictionaries. Providers that
      fail to import or compute a manifest are omitted.
    """
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
    """Render an HTML snippet for a metadata module or its provider.

    Rendering order:
    1) If the resolved provider instance has an ``html()`` method, use it.
    2) Otherwise, if the module exposes a top-level ``html()`` function, use it.
    3) As a last resort, return a minimal card informing that no UI is provided.

    Any exceptions raised by provider or module HTML generation are suppressed
    and the next option is attempted, ensuring a best-effort, never-failing
    outcome for the caller.

    Parameters
    - mod: The imported module object to render.

    Returns
    - A non-empty HTML string. If no dedicated UI is available, a concise
      fallback card is returned.
    """
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
    """Concatenate HTML fragments for all discovered metadata providers.

    Iterates over all ``_meta_*`` modules, renders a best-effort HTML snippet
    for each (see ``_module_html``), and concatenates the results into a single
    string suitable for injection into a page or template.

    Returns
    - A string containing the combined HTML for all providers. Any providers
      failing to import or render are skipped without interrupting the rest.
    """
    frags: List[str] = []
    for mod in _iter_meta_modules():
        try:
            frags.append(_module_html(mod))
        except Exception:
            continue
    return "".join(frags)
