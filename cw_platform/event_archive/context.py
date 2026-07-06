# cw_platform/event_archive/context.py
# CrossWatch - Current-state context enrichment
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable

from .db import get_conn
from . import query as _query

_RELATED_LIMIT = 50


def _safe(fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except Exception:
        return None


def _config_base() -> Path:
    from ..config_base import CONFIG_BASE
    return CONFIG_BASE()


def _state_dir() -> Path:
    return _config_base() / ".cw_state"


def _read_json_file(p: Path) -> Any:
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return None


def _iter_state_files(dst: str | None, feature: str | None, marker: str, exclude: str | None = None):
    d = _state_dir()
    if not d.exists():
        return
    dl = str(dst or "").strip().lower()
    fl = str(feature or "").strip().lower()
    prefix = f"{dl}_{fl}." if (dl and fl) else None
    for p in d.iterdir():
        if not p.is_file():
            continue
        n = p.name.lower()
        if not n.endswith(".json") or marker not in n:
            continue
        if exclude and exclude in n:
            continue
        if prefix and not n.startswith(prefix):
            continue
        yield p


def _item_variants(item_key: Any) -> set[str]:
    ik = str(item_key or "").strip()
    out = {ik}
    if "#" in ik:
        out.add(ik.split("#", 1)[0])
    return {v for v in out if v}


def _P(v: Any) -> str:
    return str(v or "").strip().upper()


def _I(v: Any) -> str:
    try:
        from ..provider_instances import normalize_instance_id
        n = normalize_instance_id(v)
    except Exception:
        n = v
    s = str(n or "").strip().lower()
    return s or "default"


def _disp_inst(v: Any) -> str:
    return "" if _I(v) == "default" else str(v or "").strip()


def _route_label(a: str, ai: str, b: str, bi: str) -> str:
    left = f"{a}{(' ' + ai) if ai else ''}"
    right = f"{b}{(' ' + bi) if bi else ''}"
    return f"{left} → {right}".strip()


def _event_by_id(conn: sqlite3.Connection, event_id: Any) -> dict[str, Any] | None:
    try:
        row = conn.execute(
            f"SELECT {','.join(_query._COLUMNS)} FROM events WHERE id=?",
            [int(event_id)],
        ).fetchone()
    except Exception:
        return None
    return dict(row) if row else None


def build_context(
    *,
    event_id: Any = None,
    item_key: str | None = None,
    provider: str | None = None,
    feature: str | None = None,
    pair_key: str | None = None,
    run_id: str | None = None,
    source_provider: str | None = None,
    destination_provider: str | None = None,
    origin_provider: str | None = None,
    source_instance: str | None = None,
    destination_instance: str | None = None,
    origin_instance: str | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    c = conn or get_conn()
    out: dict[str, Any] = {"ok": True, "event": None, "related": {}, "context": {}}

    event: dict[str, Any] | None = None
    if c is not None and event_id not in (None, ""):
        event = _event_by_id(c, event_id)
    out["event"] = event

    ev = event or {}
    item_key = item_key or ev.get("item_key")
    feature = feature or ev.get("feature")
    pair_key = pair_key or ev.get("pair_key")
    run_id = run_id or ev.get("run_id")
    dst = destination_provider or ev.get("destination_provider") or provider
    provider = provider or dst or ev.get("source_provider")

    route = {
        "pair_key": pair_key,
        "feature": feature,
        "source_provider": source_provider or ev.get("source_provider"),
        "destination_provider": destination_provider or ev.get("destination_provider") or dst,
        "source_instance": source_instance or ev.get("source_instance"),
        "destination_instance": destination_instance or ev.get("destination_instance"),
        "origin_provider": origin_provider or ev.get("origin_provider"),
    }

    if c is not None:
        if item_key:
            out["related"]["item"] = _safe(lambda: _query.by_item(item_key, limit=_RELATED_LIMIT, visibility="all", conn=c).get("items")) or []
        if run_id:
            out["related"]["run"] = _safe(lambda: _query.by_run(run_id, limit=_RELATED_LIMIT, visibility="all", conn=c).get("items")) or []
        if pair_key:
            out["related"]["pair"] = _safe(lambda: _query.search(pair_key=pair_key, limit=_RELATED_LIMIT, visibility="all", conn=c).get("items")) or []

    ctx = out["context"]
    ctx["unresolved_state"] = _safe(lambda: current_unresolved(dst, feature, item_key))
    ctx["blackbox_state"] = _safe(lambda: current_blackbox(dst, feature, pair_key, item_key))
    ctx["tombstone_state"] = _safe(lambda: current_tombstone(feature, pair_key, item_key))
    ctx["pair_state"] = _safe(lambda: current_pair_state(route))
    ctx["manual_block"] = _safe(lambda: current_manual_block(provider, feature, item_key))
    ctx["provider_health"] = _safe(lambda: current_provider_health(route))
    ctx["analyzer_findings"] = _safe(lambda: current_analyzer_findings(item_key, feature))
    ctx["item_state"] = _safe(lambda: current_item_state(provider, feature, item_key))
    return out


def build_group_context(group_id: Any = None, *, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    from . import groups as _groups
    c = conn or get_conn()
    out: dict[str, Any] = {"ok": True, "group": None, "events": [], "context": {}, "related_groups": []}
    g = _safe(lambda: _groups.get_group(group_id, conn=c))
    if not g:
        out["ok"] = False
        return out
    out["group"] = g
    out["events"] = _safe(lambda: _groups.group_events(group_id, order="asc", conn=c).get("items")) or []
    ctx = _safe(lambda: build_context(
        item_key=g.get("item_key"), feature=g.get("feature"), pair_key=g.get("pair_key"),
        source_provider=g.get("source_provider"), destination_provider=g.get("destination_provider"),
        origin_provider=g.get("origin_provider"), source_instance=g.get("source_instance"),
        destination_instance=g.get("destination_instance"), origin_instance=g.get("origin_instance"),
        conn=c,
    ))
    out["context"] = (ctx or {}).get("context") or {}
    if g.get("item_key"):
        rel = _safe(lambda: _groups.list_groups(item_key=g.get("item_key"), visibility="all", limit=20, conn=c).get("items")) or []
        out["related_groups"] = [r for r in rel if str(r.get("id")) != str(g.get("id"))]
    return out


def current_unresolved(dst: str | None, feature: str | None, item_key: str | None) -> dict[str, Any]:
    if not dst or not feature:
        return {"present": False}
    m: dict[str, Any] = {}
    for p in _iter_state_files(dst, feature, ".unresolved.", exclude="unresolved.pending"):
        d = _read_json_file(p)
        if isinstance(d, Mapping):
            for k, v in d.items():
                m[str(k)] = v
    for p in _iter_state_files(dst, feature, "unresolved.pending"):
        d = _read_json_file(p)
        if isinstance(d, Mapping):
            hints = d.get("hints") if isinstance(d.get("hints"), Mapping) else {}
            for k in (d.get("keys") or []):
                m.setdefault(str(k), (hints.get(str(k)) if isinstance(hints, Mapping) else None) or {})
    meta = m.get(str(item_key)) if item_key else None
    return {"present": bool(item_key and str(item_key) in m), "meta": meta, "total": len(m)}


def current_blackbox(dst: str | None, feature: str | None, pair_key: str | None, item_key: str | None) -> dict[str, Any]:
    if not dst or not feature:
        return {"present": False}
    keys: set[str] = set()
    for p in _iter_state_files(dst, feature, ".blackbox."):
        d = _read_json_file(p)
        if isinstance(d, Mapping):
            keys |= {str(k) for k in d.keys()}
    return {"present": bool(item_key and str(item_key) in keys), "total": len(keys)}


def current_tombstone(feature: str | None, pair_key: str | None, item_key: str | None) -> dict[str, Any]:
    from ..orchestrator._state_store import StateStore
    from ..orchestrator._tombstones import keys_for_feature
    if not feature:
        return {"present": False}
    store = StateStore(base_path=_config_base())
    km = keys_for_feature(store, feature, pair=pair_key) or {}
    present = bool(item_key and any(item_key == k or (item_key in k) for k in km))
    return {"present": present, "total": len(km)}


def _pair_features(p: Mapping[str, Any]) -> set[str]:
    feats: set[str] = set()
    sel = str(p.get("feature") or "").lower()
    if sel and sel != "multi":
        feats.add(sel)
    fmap = p.get("features")
    if isinstance(fmap, Mapping):
        for k, v in fmap.items():
            if v is True or (isinstance(v, Mapping) and v.get("enable")):
                feats.add(str(k).lower())
    return feats


def _is_two_way(p: Mapping[str, Any]) -> bool:
    return "two" in str(p.get("mode") or "").lower()


def current_pair_state(route: Mapping[str, Any]) -> dict[str, Any]:
    from ..config_base import load_config
    pair_key = route.get("pair_key")
    src = _P(route.get("source_provider"))
    dst = _P(route.get("destination_provider"))
    feat = str(route.get("feature") or "").lower()
    si = _I(route.get("source_instance"))
    di = _I(route.get("destination_instance"))
    cfg = load_config() or {}
    pairs = [p for p in (cfg.get("pairs") or []) if isinstance(p, Mapping)]

    def match(p: Mapping[str, Any], strategy: str, reverse: bool = False) -> dict[str, Any]:
        a, ai = _P(p.get("source")), _disp_inst(p.get("source_instance"))
        b, bi = _P(p.get("target")), _disp_inst(p.get("target_instance"))
        if reverse:
            a, ai, b, bi = b, bi, a, ai
        return {
            "matched": True, "strategy": strategy, "reversed": reverse,
            "id": p.get("id"), "enabled": bool(p.get("enabled", True)), "mode": p.get("mode"),
            "source": a, "source_instance": ai, "target": b, "target_instance": bi,
            "label": _route_label(a, ai, b, bi), "raw_pair_key": pair_key,
        }

    if pair_key:
        pk = str(pair_key)
        pk_sorted = "-".join(sorted(_P(pk).split("-")))
        for p in pairs:
            if str(p.get("id") or "") == pk:
                return match(p, "pair_id")
        for p in pairs:
            gen = "-".join(sorted([_P(p.get("source")), _P(p.get("target"))]))
            if gen == pk or gen == pk_sorted:
                return match(p, "pair_key")

    if src and dst:
        for p in pairs:
            if feat and (fs := _pair_features(p)) and feat not in fs:
                continue
            ps, pt = _P(p.get("source")), _P(p.get("target"))
            psi, pti = _I(p.get("source_instance")), _I(p.get("target_instance"))
            if ps == src and pt == dst and psi == si and pti == di:
                return match(p, "providers_instances")
            if _is_two_way(p) and ps == dst and pt == src and psi == di and pti == si:
                return match(p, "reverse_providers_instances", reverse=True)
        for p in pairs:
            if feat and (fs := _pair_features(p)) and feat not in fs:
                continue
            ps, pt = _P(p.get("source")), _P(p.get("target"))
            if ps == src and pt == dst:
                return match(p, "providers")
            if _is_two_way(p) and ps == dst and pt == src:
                return match(p, "reverse_providers", reverse=True)

    # match a configured pair where the single known provider participates for this feature.
    known = dst or src
    if known and not (src and dst):
        for p in pairs:
            if feat and (fs := _pair_features(p)) and feat not in fs:
                continue
            ps, pt = _P(p.get("source")), _P(p.get("target"))
            if dst:
                if pt == dst:
                    return match(p, "destination_only")
                if ps == dst and _is_two_way(p):
                    return match(p, "destination_only_reverse", reverse=True)
            else:
                if ps == src:
                    return match(p, "source_only")
                if pt == src and _is_two_way(p):
                    return match(p, "source_only_reverse", reverse=True)

    return {
        "matched": False, "raw_pair_key": pair_key,
        "label": _route_label(src, _disp_inst(route.get("source_instance")), dst, _disp_inst(route.get("destination_instance"))),
    }


def _configured_providers() -> set[str]:
    from ..config_base import load_config
    cfg = load_config() or {}
    out: set[str] = set()
    for p in (cfg.get("pairs") or []):
        if not isinstance(p, Mapping):
            continue
        for k in ("source", "target"):
            v = _P(p.get(k))
            if v:
                out.add(v)
    return out


def _health_entry(node: Mapping[str, Any], inst: str) -> dict[str, Any]:
    raw_insts = node.get("instances")
    insts: Mapping[str, Any] = raw_insts if isinstance(raw_insts, Mapping) else {}
    im: Mapping[str, Any] | None = None
    for k, v in insts.items():
        if _I(k) == inst and isinstance(v, Mapping):
            im = v
            break
    if im is not None and "connected" in im:
        connected = bool(im.get("connected"))
    else:
        connected = bool(node.get("connected"))
    reason = (im.get("reason") if isinstance(im, Mapping) else None) or node.get("reason")
    entry: dict[str, Any] = {
        "configured": True, "connected": connected,
        "status": "ok" if connected else "down",
    }
    if inst != "default":
        entry["instance"] = inst
    if reason:
        entry["reason"] = str(reason)
    return entry


def current_provider_health(route: Mapping[str, Any]) -> dict[str, Any]:
    src, dst, org = _P(route.get("source_provider")), _P(route.get("destination_provider")), _P(route.get("origin_provider"))
    si, di = _I(route.get("source_instance")), _I(route.get("destination_instance"))
    try:
        from api.probesAPI import STATUS_CACHE
    except Exception:
        return {"status_available": False}
    cache = STATUS_CACHE or {}
    data = cache.get("data")
    if not isinstance(data, Mapping) or not data:
        return {"status_available": False}
    raw_providers = data.get("providers")
    providers: Mapping[str, Any] = raw_providers if isinstance(raw_providers, Mapping) else {}
    configured = _safe(_configured_providers) or set()

    checked_at = 0
    for cand in (cache.get("ts"), data.get("ts")):
        try:
            if cand:
                checked_at = int(float(cand))
                break
        except Exception:
            pass
    out: dict[str, Any] = {"status_available": True, "checked_at": checked_at, "providers": {}}
    for name, inst in ((dst, di), (src, si), (org, "default")):
        if not name or name in out["providers"]:
            continue
        node = providers.get(name) or providers.get(name.upper()) or providers.get(name.lower())
        if isinstance(node, Mapping):
            out["providers"][name] = _health_entry(node, inst)
        elif name in configured:
            out["providers"][name] = {"configured": True, "status": "unknown"}
        else:
            out["providers"][name] = {"configured": False, "status": "not_configured"}
    return out


def current_manual_block(provider: str | None, feature: str | None, item_key: str | None) -> dict[str, Any] | None:
    import json
    p = _config_base() / "state.manual.json"
    if not p.exists():
        return {"present": False}
    try:
        raw = json.loads(p.read_text("utf-8"))
    except Exception:
        return {"present": False}
    provs = (raw or {}).get("providers") or {}
    blk = provs.get(str(provider or "").upper()) if provider else None
    if not isinstance(blk, dict):
        return {"present": False}
    feat = blk.get(str(feature or "")) if feature else None
    blocked = (feat or {}).get("block") or (feat or {}).get("blocks") or []
    present = bool(item_key and item_key in blocked)
    return {"present": present, "count": len(blocked) if isinstance(blocked, (list, dict)) else 0}


def current_analyzer_findings(item_key: str | None, feature: str | None) -> dict[str, Any] | None:
    if not item_key:
        return None
    from services.analyzer import _load_state
    state = _load_state(None) or {}
    providers = (state.get("providers") or {}) if isinstance(state, dict) else {}
    variants = _item_variants(item_key)
    feat = str(feature or "")
    hits: list[dict[str, Any]] = []

    def _check(blk: Any, prov: str, instance: str | None) -> None:
        if not isinstance(blk, Mapping):
            return
        fblk = blk.get(feat) if feat else None
        base = ((fblk or {}).get("baseline") or {}).get("items") if isinstance(fblk, Mapping) else None
        if isinstance(base, Mapping) and any(v in base for v in variants):
            hit: dict[str, Any] = {"provider": prov, "present": True}
            if instance:
                hit["instance"] = instance
            hits.append(hit)

    for prov, pblk in providers.items():
        if not isinstance(pblk, Mapping):
            continue
        _check(pblk, str(prov), None)
        insts = pblk.get("instances")
        if isinstance(insts, Mapping):
            for inst_id, iblk in insts.items():
                _check(iblk, str(prov), str(inst_id))
    return {"present_in": hits, "count": len(hits)}


def current_item_state(provider: str | None, feature: str | None, item_key: str | None) -> dict[str, Any] | None:
    if not item_key:
        return None
    finding = current_analyzer_findings(item_key, feature)
    present = bool(finding and finding.get("count"))
    return {"item_key": item_key, "present_somewhere": present}


# title enrichment 
_TITLE_CACHE: dict[str, Any] = {"ts": 0.0, "index": None}
_TITLE_TTL = 8.0


def _baseline_states() -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    base = _config_base()
    for root in (base, base / ".cw_state"):
        try:
            files = sorted(root.glob("state.*.json")) if root.exists() else []
        except Exception:
            files = []
        for p in files:
            if p.name == "state.manual.json":
                continue
            rp = str(p.resolve())
            if rp in seen:
                continue
            seen.add(rp)
            d = _read_json_file(p)
            if isinstance(d, Mapping):
                out.append(d)
    if not out:
        try:
            from services.analyzer import _load_state
            gs = _load_state(None)
            if isinstance(gs, Mapping):
                out.append(gs)
        except Exception:
            pass
    return out


def _title_index() -> dict[str, dict[str, Any]]:
    now = time.monotonic()
    cached = _TITLE_CACHE.get("index")
    if cached is not None and (now - float(_TITLE_CACHE.get("ts") or 0)) < _TITLE_TTL:
        return cached
    idx: dict[str, dict[str, Any]] = {}

    def _score(r: Mapping[str, Any]) -> tuple[int, int]:
        return (
            1 if (r.get("series_title") or r.get("show_title") or r.get("show")) else 0,
            1 if r.get("title") else 0,
        )

    def _add(key: Any, rec: Mapping[str, Any]) -> None:
        k = str(key or "").strip()
        if not k:
            return
        cur = idx.get(k)
        if cur is None or _score(rec) > _score(cur):
            idx[k] = dict(rec)

    def _id_keys(rec: Mapping[str, Any]) -> list[str]:
        ks: list[str] = []
        for grp in ("ids", "show_ids"):
            m = rec.get(grp)
            if isinstance(m, Mapping):
                for ns, v in m.items():
                    if v not in (None, ""):
                        ks.append(f"{str(ns).strip().lower()}:{str(v).strip().lower()}")
        return ks

    def _ingest_items(items: Any) -> None:
        if not isinstance(items, Mapping):
            return
        for k, it in items.items():
            if not isinstance(it, Mapping):
                continue
            _add(k, it)
            for kk in _id_keys(it):
                _add(kk, it)

    def _ingest_provblk(blk: Any) -> None:
        if not isinstance(blk, Mapping):
            return
        for feat in ("history", "watchlist", "ratings", "progress"):
            fblk = blk.get(feat)
            items = ((fblk or {}).get("baseline") or {}).get("items") if isinstance(fblk, Mapping) else None
            _ingest_items(items)

    # analyzer provider baselines
    for state in _baseline_states():
        providers = state.get("providers") if isinstance(state, Mapping) else None
        if not isinstance(providers, Mapping):
            continue
        for _, pblk in providers.items():
            _ingest_provblk(pblk)
            insts = pblk.get("instances") if isinstance(pblk, Mapping) else None
            if isinstance(insts, Mapping):
                for _, iblk in insts.items():
                    _ingest_provblk(iblk)

    # unresolved-state item maps
    for p in _iter_state_files(None, None, "unresolved"):
        d = _read_json_file(p)
        if isinstance(d, Mapping):
            _ingest_items(d.get("items"))

    _TITLE_CACHE["index"] = idx
    _TITLE_CACHE["ts"] = now
    return idx


def resolve_title(item_key: Any, media_type: Any = None) -> dict[str, Any] | None:
    if not item_key:
        return None
    idx = _title_index()
    rec: Mapping[str, Any] | None = None
    for v in _item_variants(item_key):
        cand = idx.get(v) or idx.get(v.lower())
        if isinstance(cand, Mapping):
            rec = cand
            break
    if rec is None:
        return None
    series = str(rec.get("series_title") or rec.get("show_title") or rec.get("show") or "").strip()
    title = str(rec.get("title") or "").strip()
    return {
        "series_title": series or None,
        "title": title or None,
        "year": rec.get("year"),
        "media_type": str(rec.get("type") or media_type or "").lower() or None,
        "season": rec.get("season"),
        "episode": rec.get("episode"),
    }


def _ep_tag(season: Any, episode: Any) -> str:
    try:
        return f"S{int(season):02d}E{int(episode):02d}"
    except Exception:
        return ""


def best_title(item_key: Any, *, title: Any = None, media_type: Any = None,
               season: Any = None, episode: Any = None) -> dict[str, Any] | None:

    if not item_key:
        return None
    info = _safe(lambda: resolve_title(item_key, media_type))
    if not info:
        return None
    cur = str(title or "").strip()
    tag = _ep_tag(season, episode)
    is_episode = str(media_type or info.get("media_type") or "").lower() == "episode" \
        or (season is not None and episode is not None)
    weak = (not cur) or bool(tag and cur.upper() == tag)
    new_title = cur or None
    if weak:
        series = info.get("series_title")
        ititle = info.get("title")
        pick = (series or ititle) if is_episode else (ititle or series)
        if pick:
            new_title = pick
    changed = (new_title or None) != (cur or None) or bool(info.get("year")) or bool(info.get("media_type"))
    if not changed:
        return None
    return {
        "title": new_title,
        "year": info.get("year"),
        "media_type": info.get("media_type"),
    }
