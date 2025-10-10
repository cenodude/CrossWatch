from __future__ import annotations
from typing import Any, Dict, List, Mapping, Optional

# Core orchestrator pieces
from ..id_map import minimal as _minimal, canonical_key as _ck
from ._snapshots import (
    build_snapshots_for_feature,
    coerce_suspect_snapshot,
    module_checkpoint,
    prev_checkpoint,
)
from ._applier import apply_add, apply_remove
from ._tombstones import cascade_removals
from ._unresolved import load_unresolved_keys, record_unresolved
from ._planner import diff, diff_ratings
from ._phantoms import PhantomGuard

# Extracted helpers (compact + reusable)
from ._pairs_utils import (
    supports_feature as _supports_feature,
    resolve_flags as _resolve_flags,
    health_status as _health_status,
    health_feature_ok as _health_feature_ok,
    rate_remaining as _rate_remaining,
    apply_verify_after_write_supported as _apply_verify_after_write_supported,
)
from ._pairs_massdelete import maybe_block_mass_delete as _maybe_block_mass_delete
from ._pairs_blocklist import apply_blocklist  # union: (global+pair) tombstones ∪ unresolved ∪ blackbox

# Blackbox (flapper suppression). Fallback stubs accept **kwargs to stay safe.
try:  # pragma: no cover
    from ._blackbox import load_blackbox_keys, record_attempts, record_success  # type: ignore
except Exception:  # pragma: no cover
    def load_blackbox_keys(dst: str, feature: str) -> set[str]:
        return set()
    def record_attempts(dst: str, feature: str, keys, **kwargs) -> Dict[str, Any]:
        return {"ok": True, "count": 0}
    def record_success(dst: str, feature: str, keys, **kwargs) -> Dict[str, Any]:
        return {"ok": True, "count": 0}


# Compact, tolerant ratings filter
def _ratings_filter_index(idx: Dict[str, Any], fcfg: Mapping[str, Any]) -> Dict[str, Any]:
    # normalize type synonyms
    alias = {"movies":"movie","movie":"movie","shows":"show","show":"show",
             "episodes":"episode","episode":"episode","ep":"episode","eps":"episode"}
    types_raw = [str(t).strip().lower() for t in (fcfg.get("types") or []) if isinstance(t, (str, bytes))]
    types = {alias.get(t, t.rstrip("s")) for t in types_raw if t}
    from_date = str(fcfg.get("from_date") or "").strip()  # YYYY-MM-DD

    def _keep(v: Mapping[str, Any]) -> bool:
        vt = alias.get(str(v.get("type","")).strip().lower(),
                       str(v.get("type","")).strip().lower().rstrip("s"))
        if types and vt not in types:
            return False
        if from_date:
            ra = (v.get("rated_at") or v.get("ratedAt") or "").strip()
            if not ra:
                return True  # no timestamp → keep (don’t over-filter)
            if ra[:10] < from_date:
                return False
        return True

    return {k: v for k, v in idx.items() if _keep(v)}


def run_one_way_feature(
    ctx,
    src: str,
    dst: str,
    *,
    feature: str,
    fcfg: Mapping[str, Any],
    health_map: Mapping[str, Any],
) -> Dict[str, Any]:
    """
    One-way sync driver (src → dst) for a single feature.
    - Present vs delta semantics respected per provider
    - Observed-deletes disabled automatically if a provider says so
    """
    cfg, emit, dbg = ctx.config, ctx.emit, ctx.dbg
    sync_cfg = (cfg.get("sync") or {})
    provs = ctx.providers

    src = str(src).upper()
    dst = str(dst).upper()
    src_ops = provs.get(src)
    dst_ops = provs.get(dst)

    emit("feature:start", src=src, dst=dst, feature=feature)

    if not src_ops or not dst_ops:
        ctx.emit_info(f"[!] Missing provider ops for {src}→{dst}")
        emit("feature:done", src=src, dst=dst, feature=feature)
        return {"ok": False, "added": 0, "removed": 0, "unresolved": 0}

    flags = _resolve_flags(fcfg, sync_cfg)
    allow_adds = flags["allow_adds"]
    allow_removes = flags["allow_removals"]

    # Health guards
    Hs = health_map.get(src) or {}
    Hd = health_map.get(dst) or {}
    ss = _health_status(Hs)
    sd = _health_status(Hd)
    src_down = (ss == "down")
    dst_down = (sd == "down")
    if ss == "auth_failed" or sd == "auth_failed":
        emit("pair:skip", src=src, dst=dst, reason="auth_failed", src_status=ss, dst_status=sd)
        emit("feature:done", src=src, dst=dst, feature=feature)
        return {"ok": False, "added": 0, "removed": 0, "unresolved": 0}

    # Capability + per-feature health guard
    if (not _supports_feature(src_ops, feature)) or (not _supports_feature(dst_ops, feature)) \
       or (not _health_feature_ok(Hs, feature)) or (not _health_feature_ok(Hd, feature)):
        emit("feature:unsupported", src=src, dst=dst, feature=feature,
             src_supported=_supports_feature(src_ops, feature) and _health_feature_ok(Hs, feature),
             dst_supported=_supports_feature(dst_ops, feature) and _health_feature_ok(Hd, feature))
        emit("feature:done", src=src, dst=dst, feature=feature)
        return {"ok": True, "added": 0, "removed": 0, "unresolved": 0}

    # If source is down, skip planning completely (avoids delete storms)
    if src_down:
        emit("writes:skipped", src=src, dst=dst, feature=feature, reason="source_down")
        emit("feature:done", src=src, dst=dst, feature=feature)
        return {"ok": True, "added": 0, "removed": 0, "unresolved": 0}

    # Observed-deletes toggle (config default → clamped by health & capabilities)
    include_observed = bool(sync_cfg.get("include_observed_deletes", True))
    if src_down or dst_down:
        include_observed = False  # safer if either side is down

    # If either provider declares observed_deletes=False, force it off
    def _cap_obsdel(ops) -> Optional[bool]:
        try:
            v = (ops.capabilities() or {}).get("observed_deletes")
            return None if v is None else bool(v)
        except Exception:
            return None

    try:
        if _cap_obsdel(src_ops) is False or _cap_obsdel(dst_ops) is False:
            include_observed = False
            pair_key_dbg = "-".join(sorted([src, dst]))
            emit("debug",
                 msg="observed.deletions.forced_off",
                 feature=feature, pair=pair_key_dbg, reason="provider_capability")
    except Exception:
        pass

    # Rate-based pause hint for apply_* calls
    def _pause_for(pname: str) -> int:
        base = int(getattr(ctx, "apply_chunk_pause_ms", 0) or 0)
        rem = _rate_remaining(health_map.get(pname))
        if rem is not None and rem < 10:
            emit("rate:slow", provider=pname, remaining=rem, base_ms=base, extra_ms=1000)
            return base + 1000
        return base

    # Snapshots (fresh)
    snaps = build_snapshots_for_feature(
        feature=feature,
        config=cfg,
        providers=provs,
        snap_cache=ctx.snap_cache,
        snap_ttl_sec=ctx.snap_ttl_sec,
        dbg=dbg,
        emit_info=ctx.emit_info,
    )
    src_cur = snaps.get(src) or {}
    dst_cur = snaps.get(dst) or {}

    # Previous baselines (legacy layout)
    prev_state = ctx.state_store.load_state() or {}
    prev_provs = (prev_state.get("providers") or {})
    prev_src = dict((((prev_provs.get(src, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})
    prev_dst = dict((((prev_provs.get(dst, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})

    # Suspect guards (protect against drastic shrink snapshots)
    drop_guard = bool(sync_cfg.get("drop_guard", False))
    suspect_min_prev = int((cfg.get("runtime") or {}).get("suspect_min_prev", 20))
    suspect_ratio = float((cfg.get("runtime") or {}).get("suspect_shrink_ratio", 0.10))
    suspect_debug = bool((cfg.get("runtime") or {}).get("suspect_debug", True))

    if drop_guard:
        prev_cp_src = prev_checkpoint(prev_state, src, feature)
        now_cp_src = module_checkpoint(src_ops, cfg, feature)
        eff_src, src_suspect, src_reason = coerce_suspect_snapshot(
            provider=src, ops=src_ops,
            prev_idx=prev_src, cur_idx=src_cur, feature=feature,
            suspect_min_prev=suspect_min_prev, suspect_shrink_ratio=suspect_ratio,
            suspect_debug=suspect_debug, emit=emit, emit_info=ctx.emit_info,
            prev_cp=prev_cp_src, now_cp=now_cp_src,
        )
        if src_suspect:
            dbg("snapshot.guard", provider=src, feature=feature, reason=src_reason)

        prev_cp_dst = prev_checkpoint(prev_state, dst, feature)
        now_cp_dst = module_checkpoint(dst_ops, cfg, feature)
        eff_dst, dst_suspect, dst_reason = coerce_suspect_snapshot(
            provider=dst, ops=dst_ops,
            prev_idx=prev_dst, cur_idx=dst_cur, feature=feature,
            suspect_min_prev=suspect_min_prev, suspect_shrink_ratio=suspect_ratio,
            suspect_debug=suspect_debug, emit=emit, emit_info=ctx.emit_info,
            prev_cp=prev_cp_dst, now_cp=now_cp_dst,
        )
        if dst_suspect:
            dbg("snapshot.guard", provider=dst, feature=feature, reason=dst_reason)
    else:
        eff_src, eff_dst = dict(src_cur), dict(dst_cur)
        now_cp_src = module_checkpoint(src_ops, cfg, feature)
        now_cp_dst = module_checkpoint(dst_ops, cfg, feature)

    # Present vs delta semantics (merge baseline for delta providers)
    try:
        dst_sem = str((dst_ops.capabilities() or {}).get("index_semantics", "present")).lower()
    except Exception:
        dst_sem = "present"
    try:
        src_sem = str((src_ops.capabilities() or {}).get("index_semantics", "present")).lower()
    except Exception:
        src_sem = "present"

    dst_full = (dict(prev_dst) | dict(dst_cur)) if dst_sem == "delta" else dict(eff_dst)
    src_idx = (dict(prev_src) | dict(src_cur)) if src_sem == "delta" else dict(eff_src)

    # Feature-specific pre-filtering (e.g., ratings from_date/types)
    if feature == "ratings":
        src_idx  = _ratings_filter_index(src_idx,  fcfg)
        dst_full = _ratings_filter_index(dst_full, fcfg)
        adds, removes = diff_ratings(src_idx, dst_full)
    else:
        adds, removes = diff(src_idx, dst_full)

    # Honor gates
    if not allow_adds:
        adds = []
    if not allow_removes:
        removes = []

    # Mass-delete guard (on removals)
    removes = _maybe_block_mass_delete(
        removes, baseline_size=len(dst_full),
        allow_mass_delete=bool(sync_cfg.get("allow_mass_delete", True)),
        suspect_ratio=suspect_ratio,
        emit=emit, dbg=dbg, dst_name=dst, feature=feature,
    )

    # Apply union blocklist first
    pair_key = "-".join(sorted([src, dst]))
    adds = apply_blocklist(
        ctx.state_store, adds, dst=dst, feature=feature, pair_key=pair_key, emit=emit
    )

    # Telemetry summary (post-blocklist)
    emit("one:plan", src=src, dst=dst, feature=feature,
         adds=len(adds), removes=len(removes),
         src_count=len(src_idx), dst_count=len(dst_full))

    # Phantom filter (post-blocklist, pre-write) — toggled by blackbox
    bb = ((cfg or {}).get("blackbox") if isinstance(cfg, dict) else getattr(cfg, "blackbox", {})) or {}
    use_phantoms = bool(bb.get("enabled") and bb.get("block_adds", True))
    ttl_days = int(bb.get("cooldown_days") or 0) or None

    guard = PhantomGuard(src, dst, feature, ttl_days=ttl_days, enabled=use_phantoms)
    if use_phantoms and adds:
        adds, _blocked = guard.filter_adds(adds, _ck, _minimal, emit, ctx.state_store, pair_key)

    # Write bookkeeping (now-final adds)
    attempted_keys = {_ck(it) for it in adds}
    key2item = {_ck(it): _minimal(it) for it in adds}

    # Apply additions with unresolved-correction (+ optional verify)
    added_effective = 0
    unresolved_new_total = 0  # report new unresolved upwards
    dry_run_flag = bool(ctx.dry_run or sync_cfg.get("dry_run", False))
    verify_after_write = bool(sync_cfg.get("verify_after_write", False))

    if adds:
        if dst_down:
            record_unresolved(dst, feature, adds, hint="provider_down:add")
            emit("writes:skipped", dst=dst, feature=feature, reason="provider_down", op="add", count=len(adds))
            unresolved_new_total += len(adds)
        else:
            unresolved_before = set(load_unresolved_keys(dst, feature, cross_features=True) or [])
            add_res = apply_add(
                dst_ops=dst_ops,
                cfg=cfg,
                dst_name=dst,
                feature=feature,
                items=adds,
                dry_run=dry_run_flag,
                emit=emit,
                dbg=dbg,
                chunk_size=ctx.apply_chunk_size,
                chunk_pause_ms=_pause_for(dst),
            )
            unresolved_after = set(load_unresolved_keys(dst, feature, cross_features=True) or [])
            new_unresolved = unresolved_after - unresolved_before
            unresolved_new_total += len(new_unresolved)

            confirmed_keys = [k for k in attempted_keys if k not in new_unresolved]

            if verify_after_write and _apply_verify_after_write_supported(dst_ops):
                try:
                    unresolved_again = set(load_unresolved_keys(dst, feature, cross_features=True) or [])
                    confirmed_keys = [k for k in confirmed_keys if k not in unresolved_again]
                except Exception:
                    pass

            prov_count = int((add_res or {}).get("count") or 0)

            # Strict pessimistic policy: if any new unresolved appeared and we didn't verify, count 0 adds.
            strict_pessimist = (not verify_after_write) and bool(new_unresolved)
            if strict_pessimist:
                added_effective = 0
            else:
                added_effective = len(confirmed_keys) if verify_after_write else min(prov_count, len(confirmed_keys))

            if added_effective != prov_count:
                dbg("apply:add:corrected", dst=dst, feature=feature,
                    provider_count=prov_count, effective=added_effective,
                    newly_unresolved=len(new_unresolved))

            # Blackbox + persist unresolved for failed attempts (pass pair+cfg)
            failed_keys = [k for k in attempted_keys if k not in confirmed_keys]
            try:
                if failed_keys:
                    record_attempts(dst, feature, failed_keys, reason="apply:add:failed", op="add",
                                    pair=pair_key, cfg=cfg)
                    failed_items = [key2item.get(k) for k in failed_keys if key2item.get(k)]
                    if failed_items:
                        record_unresolved(dst, feature, failed_items, hint="apply:add:failed")
                if confirmed_keys:
                    record_success(dst, feature, confirmed_keys, pair=pair_key, cfg=cfg)

                # Record only when phantom guard is active and we actually counted them
                if use_phantoms and guard and added_effective and confirmed_keys:
                    guard.record_success(confirmed_keys)
            except Exception:
                pass

            # Only update baseline when we truly consider them added.
            if added_effective and not dry_run_flag:
                for k in confirmed_keys[:added_effective]:
                    v = key2item.get(k)
                    if v:
                        dst_full[k] = v

    # Apply removals
    removed_count = 0
    rem_keys_attempted: List[str] = []
    if removes:
        # canonical keys for everything we intend to remove
        try:
            rem_keys_attempted = [
                _ck(_minimal(it)) for it in removes if _ck(_minimal(it))
            ]
        except Exception:
            rem_keys_attempted = []

        if dst_down:
            record_unresolved(dst, feature, removes, hint="provider_down:remove")
            emit("writes:skipped", dst=dst, feature=feature, reason="provider_down", op="remove", count=len(removes))
        else:
            rem_res = apply_remove(
                dst_ops=dst_ops,
                cfg=cfg,
                dst_name=dst,
                feature=feature,
                items=removes,
                dry_run=dry_run_flag,
                emit=emit,
                dbg=dbg,
                chunk_size=ctx.apply_chunk_size,
                chunk_pause_ms=_pause_for(dst),
            )
            removed_count = int((rem_res or {}).get("count") or 0)

            # Mark BOTH global and pair-scoped tombstones for removed keys + ALL ALIASES.
            if removed_count and not dry_run_flag:
                try:
                    import time as _t
                    now = int(_t.time())
                    t = ctx.state_store.load_tomb() or {}
                    ks = t.setdefault("keys", {})

                    removed_tokens = set()
                    for it in (removes or []):
                        try:
                            # canonical
                            ck = _ck(_minimal(it))
                            if ck:
                                removed_tokens.add(ck)
                            # aliases
                            ids = (it.get("ids") or {})
                            for idk, idv in (ids or {}).items():
                                if idv is None or str(idv) == "":
                                    continue
                                removed_tokens.add(f"{str(idk).lower()}:{str(idv).lower()}")
                        except Exception:
                            continue

                    for tok in removed_tokens:
                        # Global feature tombstone (feature|alias_or_ck)
                        ks.setdefault(f"{feature}|{tok}", now)
                        # Pair-scoped tombstone (feature:SRC-DST|alias_or_ck)
                        ks.setdefault(f"{feature}:{pair_key}|{tok}", now)

                    ctx.state_store.save_tomb(t)
                    emit("debug", msg="tombstones.marked", feature=feature,
                         added=len(removed_tokens), scope="global+pair")
                except Exception:
                    pass
            # This prevents the same “removed:N” from being planned on every run when removal succeeded.
            if not dry_run_flag and removed_count:
                for k in rem_keys_attempted:
                    if k in dst_full:
                        dst_full.pop(k, None)

    # Persist baselines & checkpoints (legacy layout kept intact)
    try:
        st = ctx.state_store.load_state() or {}
        provs_block = st.setdefault("providers", {})

        def _ensure_pf(pmap, prov, feat):
            pprov = pmap.setdefault(prov, {})
            return pprov.setdefault(feat, {"baseline": {"items": {}}, "checkpoint": None})

        def _commit_baseline(pmap, prov, feat, items):
            pf = _ensure_pf(pmap, prov, feat)
            pf["baseline"] = {"items": {k: _minimal(v) for k, v in (items or {}).items()}}

        def _commit_checkpoint(pmap, prov, feat, chk):
            if not chk:
                return
            pf = _ensure_pf(pmap, prov, feat)
            pf["checkpoint"] = chk

        _commit_baseline(provs_block, src, feature, src_idx)
        _commit_baseline(provs_block, dst, feature, dst_full)
        _commit_checkpoint(provs_block, src, feature, now_cp_src)
        _commit_checkpoint(provs_block, dst, feature, now_cp_dst)

        import time as _t
        st["last_sync_epoch"] = int(_t.time())
        ctx.state_store.save_state(st)
    except Exception:
        pass

    # Per-feature cascade (when we actually removed something)
    try:
        if removes:
            cascade_removals(
                ctx.state_store, dbg, feature=feature,
                removed_keys=[k.get("ids", {}).get("imdb") or "" for k in removes if isinstance(k, dict)],
            )
    except Exception:
        pass

    emit("feature:done", src=src, dst=dst, feature=feature)
    # Report unresolved so _pairs.py can aggregate into run:done and stats:overview.
    return {"ok": True, "added": added_effective, "removed": removed_count, "unresolved": int(unresolved_new_total)}
