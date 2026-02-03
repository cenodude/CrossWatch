# /providers/scrobble/routes.py
# CrossWatch - Multi-Platform Media Monitoring and Scrobbling
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from typing import Any


DEFAULT_INSTANCE_ID = "default"
ROUTE_PROVIDERS = {"plex", "emby", "jellyfin"}
ROUTE_SINKS = {"trakt", "simkl", "mdblist"}


def _deep_clone(v: Any) -> Any:
    try:
        import copy
        return copy.deepcopy(v)
    except Exception:
        return v


def _deep_merge(a: Any, b: Any) -> Any:
    if not isinstance(a, dict) or not isinstance(b, dict):
        return _deep_clone(b)
    out: dict[str, Any] = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = _deep_clone(v)
    return out


def _watch_cfg(cfg: dict[str, Any]) -> dict[str, Any]:
    sc = cfg.setdefault("scrobble", {})
    return sc.setdefault("watch", {})


def legacy_watch_present(cfg: dict[str, Any]) -> bool:
    w = (cfg.get("scrobble") or {}).get("watch") or {}
    prov = str(w.get("provider") or "").strip()
    sink = str(w.get("sink") or "").strip()
    return bool(prov and sink)


def normalize_route(route: dict[str, Any], fallback_id: str) -> dict[str, Any]:
    r: dict[str, Any] = dict(route or {})
    rid = str(r.get("id") or fallback_id).strip() or fallback_id
    enabled = bool(r.get("enabled", True))

    prov = str(r.get("provider") or "").strip().lower() or "plex"
    if prov not in ROUTE_PROVIDERS:
        prov = "plex"
    prov_inst = str(r.get("provider_instance") or r.get("providerInstance") or DEFAULT_INSTANCE_ID).strip() or DEFAULT_INSTANCE_ID

    sink = str(r.get("sink") or "").strip().lower() or "trakt"
    if sink not in ROUTE_SINKS:
        sink = "trakt"
    sink_inst = str(r.get("sink_instance") or r.get("sinkInstance") or DEFAULT_INSTANCE_ID).strip() or DEFAULT_INSTANCE_ID

    filters = r.get("filters")
    if not isinstance(filters, dict):
        filters = {}

    return {
        "id": rid,
        "enabled": enabled,
        "provider": prov,
        "provider_instance": prov_inst,
        "sink": sink,
        "sink_instance": sink_inst,
        "filters": filters,
    }


def normalize_routes(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    w = _watch_cfg(cfg)
    routes = w.get("routes")

    if isinstance(routes, list):
        out: list[dict[str, Any]] = []
        for i, raw in enumerate(routes):
            if not isinstance(raw, dict):
                continue
            out.append(normalize_route(raw, f"R{i + 1}"))
        return out

    # Legacy migration (only used when routes is missing/invalid)
    prov = str(w.get("provider") or "").strip().lower()
    sink_raw = str(w.get("sink") or "").strip().lower()
    if not prov or not sink_raw:
        return []

    sinks = [s.strip() for s in sink_raw.split(",") if s.strip()]
    filters = w.get("filters")
    if not isinstance(filters, dict):
        filters = {}

    out = []
    for i, s in enumerate(sinks):
        out.append(normalize_route({
            "id": f"R{i + 1}",
            "enabled": True,
            "provider": prov,
            "provider_instance": DEFAULT_INSTANCE_ID,
            "sink": s,
            "sink_instance": DEFAULT_INSTANCE_ID,
            "filters": _deep_clone(filters),
        }, f"R{i + 1}"))
    return out


def ensure_routes(cfg: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    w = _watch_cfg(cfg)

    # If routes key exists, respect it even if empty. Only normalize.
    if "routes" in w:
        if isinstance(w.get("routes"), list):
            w["routes"] = normalize_routes(cfg)
        return cfg, False

    routes = normalize_routes(cfg)
    if routes:
        w["routes"] = routes
        w["routes_migrated_from_legacy"] = True
        return cfg, True

    w["routes"] = []
    return cfg, False


def _provider_view(cfg: dict[str, Any], provider: str, instance_id: str) -> dict[str, Any]:
    base = cfg.get(provider) if isinstance(cfg.get(provider), dict) else {}
    inst = {}
    if isinstance(base, dict):
        insts = base.get("instances")
        if isinstance(insts, dict):
            inst = insts.get(instance_id) if isinstance(insts.get(instance_id), dict) else {}
    merged = _deep_merge(base, inst)
    if isinstance(base, dict) and "instances" in base:
        merged["instances"] = base.get("instances")
    return merged


def build_route_cfg(cfg: dict[str, Any], route: dict[str, Any]) -> dict[str, Any]:
    r = normalize_route(route, str(route.get("id") or "R1"))
    out: dict[str, Any] = _deep_clone(cfg) if isinstance(cfg, dict) else {}
    w = _watch_cfg(out)

    out[r["provider"]] = _provider_view(out, r["provider"], r["provider_instance"])
    out[r["sink"]] = _provider_view(out, r["sink"], r["sink_instance"])

    w["filters"] = _deep_clone(r.get("filters") or {})
    w["provider"] = r["provider"]
    w["sink"] = r["sink"]
    return out
