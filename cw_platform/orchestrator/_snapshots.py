from __future__ import annotations
from typing import Any, Dict, Mapping, Tuple

import time, datetime as _dt

from ..id_map import canonical_key
from ._types import InventoryOps
from ..modules_registry import load_sync_ops

def allowed_providers_for_feature(config: Mapping[str, Any], feature: str) -> set[str]:
    allowed: set[str] = set()
    try: pairs = list((config.get("pairs") or []) or [])
    except Exception: pairs = []
    
    def _feat_enabled(fmap: dict, name: str) -> bool:
        v = (fmap or {}).get(name)
        if isinstance(v, bool): return bool(v)
        if isinstance(v, dict): return bool(v.get("enable", v.get("add", True)))
        return False
    for p in pairs:
        try:
            if not p.get("enabled", True): continue
            fmap = dict(p.get("features") or {})
            if not _feat_enabled(fmap, feature): continue
            s = str(p.get("source") or p.get("src") or "").strip().upper()
            t = str(p.get("target") or p.get("dst") or "").strip().upper()
            if s: allowed.add(s)
            if t: allowed.add(t)
        except Exception: continue
    return allowed
    
def provider_configured(config: Mapping[str, Any], name: str) -> bool:
    nm = (name or "").upper()
    ops = load_sync_ops(nm)
    if ops and hasattr(ops, "is_configured"):
        try:
            return bool(ops.is_configured(config))
        except Exception:
            return False
    return False
    
def module_checkpoint(ops: InventoryOps, config: Mapping[str, Any], feature: str) -> str | None:
    acts_fn = getattr(ops, "activities", None)
    if not callable(acts_fn): return None
    try:
        acts = acts_fn(config) or {}
        if feature == "watchlist": return acts.get("watchlist") or acts.get("ptw") or acts.get("updated_at")
        if feature == "ratings":   return acts.get("ratings") or acts.get("updated_at")
        if feature == "history":   return acts.get("history") or acts.get("updated_at")
        return acts.get("updated_at")
    except Exception:
        return None
        
def prev_checkpoint(state: Mapping[str, Any], prov: str, feature: str) -> str | None:
    try:
        return (((state.get("providers") or {}).get(prov, {}) or {}).get(feature, {}) or {}).get("checkpoint")
    except Exception:
        return None
        
def _parse_ts(v) -> int | None:
    if v in (None, "", 0): return None
    try:
        if isinstance(v, (int, float)): return int(v)
        return int(_dt.datetime.fromisoformat(str(v).replace("Z","+00:00").replace(" ", "T")).timestamp())
    except Exception:
        return None
        
def build_snapshots_for_feature(*, feature: str, config: Mapping[str, Any], providers: Mapping[str, InventoryOps], snap_cache: Dict[Tuple[str,str], Tuple[float, Dict[str, Dict[str, Any]]]], snap_ttl_sec: int, dbg, emit_info) -> Dict[str, Dict[str, Dict[str, Any]]]:
    snaps: Dict[str, Dict[str, Any]] = {}
    now = time.time()
    allowed = allowed_providers_for_feature(config, feature)

    for name, ops in providers.items():
        if not ops.features().get(feature, False):
            continue

        if allowed and name.upper() not in allowed:
            # snapshot for this provider not needed for current pairs; skip
            continue

        if not provider_configured(config, name):
            # provider not configured; skip
            continue

        memo_key = (name, feature)
        if snap_ttl_sec > 0:
            ent = snap_cache.get(memo_key)
            if ent and (now - ent[0]) < snap_ttl_sec:
                snaps[name] = ent[1]
                dbg("snapshot.memo", provider=name, feature=feature, count=len(ent[1]))
                continue

        degraded = False
        try:
            idx = ops.build_index(config, feature=feature) or {}
        except Exception as e:
            emit_info(f"[!] snapshot.failed provider={name} feature={feature} error={e}")
            dbg("provider.degraded", provider=name, feature=feature)
            degraded = True
            idx = {}

        if isinstance(idx, list):
            canon = {canonical_key(v): v for v in idx}
        else:
            canon = {canonical_key(v): v for v in idx.values()} if idx else {}
        snaps[name] = canon

        if snap_ttl_sec > 0:
            if degraded or not canon:
                dbg("snapshot.no_cache_empty", provider=name, feature=feature, degraded=bool(degraded))
            else:
                snap_cache[memo_key] = (now, canon)
        dbg("snapshot", provider=name, feature=feature, count=len(canon))
    return snaps
    
def coerce_suspect_snapshot(*, provider: str, ops: InventoryOps, prev_idx: Mapping[str, Any], cur_idx: Mapping[str, Any], feature: str, suspect_min_prev: int, suspect_shrink_ratio: float, suspect_debug: bool, emit, emit_info, prev_cp: str | None, now_cp: str | None) -> tuple[Dict[str, Any], bool, str]:
    try:
        sem = (ops.capabilities() or {}).get("index_semantics", "present")
    except Exception:
        sem = "present"
    if str(sem).lower() != "present":
        return dict(cur_idx), False, "semantics:delta"
    prev_count = len(prev_idx or {}); cur_count = len(cur_idx or {})
    if prev_count < suspect_min_prev:
        return dict(cur_idx), False, "baseline:tiny"
    shrink_limit = max(1, int(prev_count * suspect_shrink_ratio))
    shrunk = (cur_count == 0) or (cur_count <= shrink_limit)
    if not shrunk:
        return dict(cur_idx), False, "ok"
    prev_ts = _parse_ts(prev_cp); now_ts = _parse_ts(now_cp)
    no_progress = ((prev_ts is not None and now_ts is not None and now_ts <= prev_ts) or (prev_ts is not None and now_ts is None) or (prev_cp and now_cp and str(now_cp) == str(prev_cp)))
    if no_progress:
        reason = "suspect:no-progress+shrunk"
        if suspect_debug:
            emit("snapshot:suspect", provider=provider, feature=feature, prev_count=prev_count, cur_count=cur_count, shrink_limit=shrink_limit, prev_checkpoint=prev_cp, now_checkpoint=now_cp, reason=reason)
        return dict(prev_idx), True, reason
    return dict(cur_idx), False, "progressed"