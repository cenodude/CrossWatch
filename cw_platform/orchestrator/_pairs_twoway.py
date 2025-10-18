from __future__ import annotations
from typing import Any, Dict, List, Mapping, Optional

from ._planner import diff, diff_ratings
try:
    from ._pairs_oneway import _ratings_filter_index as _rate_filter
except Exception:
    def _rate_filter(x, _): return x  # safe fallback

# Core imports
from ..id_map import minimal as _minimal, canonical_key as _ck
from ._snapshots import (
    build_snapshots_for_feature,
    coerce_suspect_snapshot,
    module_checkpoint,
    prev_checkpoint,
)
from ._applier import apply_add, apply_remove
from ._tombstones import keys_for_feature, cascade_removals
from ._unresolved import load_unresolved_keys, record_unresolved
from ._phantoms import PhantomGuard

# Utility imports
from ._pairs_blocklist import apply_blocklist
from ._pairs_massdelete import maybe_block_mass_delete as _maybe_block_massdelete
from ._pairs_utils import (
    supports_feature as _supports_feature,
    resolve_flags as _resolve_flags,
    health_status as _health_status,
    health_feature_ok as _health_feature_ok,
    rate_remaining as _rate_remaining,
    apply_verify_after_write_supported as _apply_verify_after_write_supported,
)

# Blackbox imports
try:  # pragma: no cover
    from ._blackbox import load_blackbox_keys, record_attempts, record_success  # type: ignore
except Exception:  # pragma: no cover
    def load_blackbox_keys(dst: str, feature: str, pair: Optional[str] = None) -> set[str]:
        return set()
    def record_attempts(dst: str, feature: str, keys, **kwargs) -> Dict[str, Any]:
        return {"ok": True, "count": 0}
    def record_success(dst: str, feature: str, keys, **kwargs) -> Dict[str, Any]:
        return {"ok": True, "count": 0}

def _confirmed(res: dict) -> int:
    return int((res or {}).get("confirmed", (res or {}).get("count", 0)) or 0)

def _two_way_sync(
    ctx,
    a: str,
    b: str,
    *,
    feature: str,
    fcfg: Mapping[str, Any],
    health_map: Mapping[str, Any],
    include_observed_override: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Two-way sync for a single feature between providers A and B.
    - Delta-support: unions previous baselines for providers that advertise index_semantics="delta".
    - Observed-deletes clamp: if either side says observed_deletes=False, disable observed deletes pair-wide.
    - Ratings: compute adds via diff_ratings; compute removals only if tomb/observed indicates a real delete.
    """
    import time as _t
    from typing import Any as _Any, Dict as _Dict, List as _List, Mapping as _Mapping

    cfg, emit, info, dbg = ctx.config, ctx.emit, ctx.emit_info, ctx.dbg
    sync_cfg = (cfg.get("sync") or {})
    provs = ctx.providers
    a = str(a).upper()
    b = str(b).upper()

    aops = provs.get(a)
    bops = provs.get(b)
    if not aops or not bops:
        info(f"[!] Missing provider ops for {a}<->{b}")
        return {"ok": False, "adds_to_A": 0, "adds_to_B": 0, "rem_from_A": 0, "rem_from_B": 0}

    flags = _resolve_flags(fcfg, sync_cfg)
    allow_adds = flags["allow_adds"]
    allow_removals = flags["allow_removals"]

    # Capability: verify_after_write
    include_observed_cfg = bool(sync_cfg.get("include_observed_deletes", True))
    include_observed = include_observed_cfg if include_observed_override is None else bool(include_observed_override)
    drop_guard = bool(sync_cfg.get("drop_guard", False))
    allow_mass_delete = bool(sync_cfg.get("allow_mass_delete", True))
    verify_after_write = bool(sync_cfg.get("verify_after_write", False))
    dry_run_flag = bool(ctx.dry_run or sync_cfg.get("dry_run", False))

    # Capability: verify_after_write support
    Ha = health_map.get(a) or {}
    Hb = health_map.get(b) or {}
    sa = _health_status(Ha)
    sb = _health_status(Hb)
    a_down = (sa == "down")
    b_down = (sb == "down")
    a_auth_fail = (sa == "auth_failed")
    b_auth_fail = (sb == "auth_failed")

    if a_auth_fail or b_auth_fail:
        emit("pair:skip", a=a, b=b, feature=feature, reason="auth_failed", a_status=sa, b_status=sb)
        return {"ok": False, "adds_to_A": 0, "adds_to_B": 0, "rem_from_A": 0, "rem_from_B": 0}

    if a_down or b_down:
        include_observed = False  # safer if either side down

    # Capability: observed deletions
    def _cap_obsdel(ops) -> Optional[bool]:
        try:
            v = (ops.capabilities() or {}).get("observed_deletes")
            return None if v is None else bool(v)
        except Exception:
            return None

    try:
        if _cap_obsdel(aops) is False or _cap_obsdel(bops) is False:
            include_observed = False
            emit("debug", msg="observed.deletions.forced_off",
                 feature=feature, a=a, b=b, reason="provider_capability")
    except Exception:
        pass

    # Early exit: unsupported feature or unhealthy provider
    if (not _supports_feature(aops, feature)) or (not _supports_feature(bops, feature)) \
       or (not _health_feature_ok(Ha, feature)) or (not _health_feature_ok(Hb, feature)):
        emit("feature:unsupported", a=a, b=b, feature=feature,
             a_supported=_supports_feature(aops, feature) and _health_feature_ok(Ha, feature),
             b_supported=_supports_feature(bops, feature) and _health_feature_ok(Hb, feature))
        return {"ok": True, "adds_to_A": 0, "adds_to_B": 0, "rem_from_A": 0, "rem_from_B": 0}

    # Pause helper
    def _pause_for(pname: str) -> int:
        base = int(getattr(ctx, "apply_chunk_pause_ms", 0) or 0)
        rem = _rate_remaining(health_map.get(pname))
        if rem is not None and rem < 10:
            emit("rate:slow", provider=pname, remaining=rem, base_ms=base, extra_ms=1000)
            return base + 1000
        return base

    emit("two:start", a=a, b=b, feature=feature, removals=allow_removals)

    #--- Build snapshots + deltas -------------------------------------------
    snaps = build_snapshots_for_feature(
        feature=feature, config=cfg, providers=provs,
        snap_cache=ctx.snap_cache, snap_ttl_sec=ctx.snap_ttl_sec,
        dbg=dbg, emit_info=info,
    )
    A_cur = snaps.get(a) or {}
    B_cur = snaps.get(b) or {}

    prev_state = ctx.state_store.load_state() or {}
    prev_provs = (prev_state.get("providers") or {})
    prevA = dict((((prev_provs.get(a, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})
    prevB = dict((((prev_provs.get(b, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})

    prev_cp_A = prev_checkpoint(prev_state, a, feature)
    prev_cp_B = prev_checkpoint(prev_state, b, feature)
    now_cp_A = module_checkpoint(aops, cfg, feature)
    now_cp_B = module_checkpoint(bops, cfg, feature)

    #-- Drop guards if configured ---------------------------------------
    if drop_guard:
        A_eff_guard, A_suspect, A_reason = coerce_suspect_snapshot(
            provider=a, ops=aops, prev_idx=prevA, cur_idx=A_cur, feature=feature,
            suspect_min_prev=int((cfg.get("runtime") or {}).get("suspect_min_prev", 20)),
            suspect_shrink_ratio=float((cfg.get("runtime") or {}).get("suspect_shrink_ratio", 0.10)),
            suspect_debug=bool((cfg.get("runtime") or {}).get("suspect_debug", True)),
            emit=emit, emit_info=info, prev_cp=prev_cp_A, now_cp=now_cp_A,
        )
        if A_suspect: dbg("snapshot.guard", provider=a, feature=feature, reason=A_reason)
        B_eff_guard, B_suspect, B_reason = coerce_suspect_snapshot(
            provider=b, ops=bops, prev_idx=prevB, cur_idx=B_cur, feature=feature,
            suspect_min_prev=int((cfg.get("runtime") or {}).get("suspect_min_prev", 20)),
            suspect_shrink_ratio=float((cfg.get("runtime") or {}).get("suspect_shrink_ratio", 0.10)),
            suspect_debug=bool((cfg.get("runtime") or {}).get("suspect_debug", True)),
            emit=emit, emit_info=info, prev_cp=prev_cp_B, now_cp=now_cp_B,
        )
        if B_suspect: dbg("snapshot.guard", provider=b, feature=feature, reason=B_reason)
    else:
        emit("drop_guard:skipped", a=a, b=b, feature=feature)
        A_eff_guard, A_suspect = dict(A_cur), False
        B_eff_guard, B_suspect = dict(B_cur), False

    #--------- Prepare effective baselines -----------------------------------
    try:
        a_sem = str((aops.capabilities() or {}).get("index_semantics", "present")).lower()
    except Exception:
        a_sem = "present"
    try:
        b_sem = str((bops.capabilities() or {}).get("index_semantics", "present")).lower()
    except Exception:
        b_sem = "present"

    A_eff = (dict(prevA) | dict(A_cur)) if a_sem == "delta" else dict(A_eff_guard)
    B_eff = (dict(prevB) | dict(B_cur)) if b_sem == "delta" else dict(B_eff_guard)

    # ---------- Observed deletions → tombstones (pair-scoped + alias-aware)
    now = int(_t.time())
    tomb_ttl_days = int((cfg.get("sync") or {}).get("tombstone_ttl_days", 30))
    tomb_ttl_secs = max(1, tomb_ttl_days) * 24 * 3600
    pair_key = "-".join(sorted([a, b]))
    tomb_map = dict(keys_for_feature(ctx.state_store, feature, pair=pair_key) or {})
    tomb = {k for k, ts in tomb_map.items() if not isinstance(ts, int) or (now - int(ts)) <= tomb_ttl_secs}

    bootstrap = (not prevA) and (not prevB) and not tomb
    obsA: set[str] = set()
    obsB: set[str] = set()
    if include_observed and not bootstrap:
        if not A_suspect: obsA = {k for k in prevA.keys() if k not in (A_cur or {})}
        if not B_suspect: obsB = {k for k in prevB.keys() if k not in (B_cur or {})}
        newly = (obsA | obsB) - tomb

        #--- Record new tombstones (pair-scoped + alias-aware) ----------------
        if newly:
            t = ctx.state_store.load_tomb() or {}
            ks = t.setdefault("keys", {})

            def _tokens_for_ck(ck: str) -> set[str]:
                toks = {ck}
                it = (prevA.get(ck) or prevB.get(ck) or {})
                ids = (it.get("ids") or {})
                try:
                    for k, v in (ids or {}).items():
                        if v is None or str(v) == "":
                            continue
                        toks.add(f"{str(k).lower()}:{str(v).lower()}")
                except Exception:
                    pass
                return toks

            write_tokens: set[str] = set()
            for ck in set(newly):
                write_tokens |= _tokens_for_ck(ck)

            for tok in write_tokens:
                ks.setdefault(f"{feature}|{tok}", now)               # global
                ks.setdefault(f"{feature}:{pair_key}|{tok}", now)    # pair

            ctx.state_store.save_tomb(t)

        emit("debug", msg="observed.deletions", a=len(obsA), b=len(obsB), tomb=len(tomb),
             suppressed_on_A=bool(A_suspect), suppressed_on_B=bool(B_suspect))
    elif not include_observed:
        emit("debug", msg="observed.deletions.disabled", feature=feature, pair=pair_key)

    # Prune effective baselines by tombstones + observed deletions
    for k in list(obsA): A_eff.pop(k, None)
    for k in list(obsB): B_eff.pop(k, None)

    # ---------- Alias helpers (used by presence checks and ratings removals)
    def _alias_index(idx: _Dict[str, _Dict[str, _Any]]) -> _Dict[str, str]:
        m: _Dict[str, str] = {}
        for ck, it in (idx or {}).items():
            ids = (it.get("ids") or {})
            for k, v in (ids or {}).items():
                if v is None or str(v) == "": continue
                m[f"{k}:{str(v).lower()}"] = ck
        return m

    def _present(idx: Dict[str, Any], alias: Dict[str, str], it: Mapping[str, Any]) -> bool:
        ck = _ck(it)
        if ck in idx: return True
        ids = (it.get("ids") or {})
        try:
            for k, v in (ids or {}).items():
                if v is None or str(v) == "": continue
                if f"{k}:{str(v).lower()}" in alias: return True
        except Exception: pass
        return False

    A_alias = _alias_index(A_eff)
    B_alias = _alias_index(B_eff)

    #--- Plan additions and removals ----------------------------------------
    add_to_A: List[Dict[str, Any]] = []
    add_to_B: List[Dict[str, Any]] = []
    rem_from_A: List[Dict[str, Any]] = []
    rem_from_B: List[Dict[str, Any]] = []

    if feature == "ratings":
        # Ratings-style features: plan via diff_ratings + tomb/observed rules.
        A_f = _rate_filter(A_eff, fcfg)
        B_f = _rate_filter(B_eff, fcfg)

        up_B, _un_B = diff_ratings(A_f, B_f)   # A → B (adds/updates)
        up_A, _un_A = diff_ratings(B_f, A_f)   # B → A (adds/updates)

        add_to_A = list(_minimal(it) for it in up_A) if allow_adds else []
        add_to_B = list(_minimal(it) for it in up_B) if allow_adds else []

        # Removals: only when (a) missing on the other side AND (b) tomb/observed indicates a true delete.
        if allow_removals:
            for _k, v in A_f.items():
                ck = _ck(v)
                if not _present(B_f, B_alias, v) and (ck in tomb or ck in obsB):
                    rem_from_A.append(_minimal(v))
            for _k, v in B_f.items():
                ck = _ck(v)
                if not _present(A_f, A_alias, v) and (ck in tomb or ck in obsA):
                    rem_from_B.append(_minimal(v))
    else:
        # Non-ratings features: plan via simple presence + tomb/observed rules.
        for _k, v in A_eff.items():
            if _present(B_eff, B_alias, v): continue
            if allow_removals and (_ck(v) in tomb or _ck(v) in obsB): rem_from_A.append(_minimal(v))
            else: add_to_B.append(_minimal(v))
        for _k, v in B_eff.items():
            if _present(A_eff, A_alias, v): continue
            if allow_removals and (_ck(v) in tomb or _ck(v) in obsA): rem_from_B.append(_minimal(v))
            else: add_to_A.append(_minimal(v))

    # Finalize plans based on allow_adds / allow_removals flags
    if not allow_adds: add_to_A.clear(); add_to_B.clear()
    if not allow_removals: rem_from_A.clear(); rem_from_B.clear()

    # Bootstrap mode: disable all removals
    if bootstrap and allow_removals:
        rem_from_A.clear(); rem_from_B.clear()
        dbg("bootstrap.no-delete", a=a, b=b)

    # --- Unresolved guard (drop items already marked unresolved for the target)
    try:
        unresolved_A = set(load_unresolved_keys(a, feature, cross_features=True) or [])
        unresolved_B = set(load_unresolved_keys(b, feature, cross_features=True) or [])

        preA, preB = len(add_to_A), len(add_to_B)
        add_to_A = [it for it in add_to_A if _ck(it) not in unresolved_A]
        add_to_B = [it for it in add_to_B if _ck(it) not in unresolved_B]

        blkA = preA - len(add_to_A)
        blkB = preB - len(add_to_B)
        if blkA:
            emit("debug", msg="blocked.counts", feature=feature, dst=a,
                 pair=f"{a}-{b}", blocked_unresolved=blkA, blocked_total=blkA)
        if blkB:
            emit("debug", msg="blocked.counts", feature=feature, dst=b,
                 pair=f"{a}-{b}", blocked_unresolved=blkB, blocked_total=blkB)
    except Exception:
        pass

    #--- Apply blocklist filtering -----------------------------------------
    add_to_A = apply_blocklist(ctx.state_store, add_to_A, dst=a, feature=feature, pair_key=pair_key, emit=emit)
    add_to_B = apply_blocklist(ctx.state_store, add_to_B, dst=b, feature=feature, pair_key=pair_key, emit=emit)

    #-- Phantom guard filtering -------------------------------------------
    bb = ((cfg or {}).get("blackbox") if isinstance(cfg, dict) else getattr(cfg, "blackbox", {})) or {}
    use_phantoms = bool(bb.get("enabled") and bb.get("block_adds", True))
    bb_ttl_days = int(bb.get("cooldown_days") or 0) or None

    guardA = PhantomGuard(src=b, dst=a, feature=feature, ttl_days=bb_ttl_days, enabled=use_phantoms)
    guardB = PhantomGuard(src=a, dst=b, feature=feature, ttl_days=bb_ttl_days, enabled=use_phantoms)

    if use_phantoms and add_to_A:
        add_to_A, _ = guardA.filter_adds(add_to_A, _ck, _minimal, emit, ctx.state_store, pair_key)
    if use_phantoms and add_to_B:
        add_to_B, _ = guardB.filter_adds(add_to_B, _ck, _minimal, emit, ctx.state_store, pair_key)

    #-- Mass-delete protection ---------------------------------------------
    rem_from_A = _maybe_block_massdelete(
        rem_from_A, baseline_size=len(A_eff),
        allow_mass_delete=allow_mass_delete,
        suspect_ratio=float((cfg.get("runtime") or {}).get("suspect_shrink_ratio", 0.10)),
        emit=emit, dbg=dbg, dst_name=a, feature=feature,
    )
    rem_from_B = _maybe_block_massdelete(
        rem_from_B, baseline_size=len(B_eff),
        allow_mass_delete=allow_mass_delete,
        suspect_ratio=float((cfg.get("runtime") or {}).get("suspect_shrink_ratio", 0.10)),
        emit=emit, dbg=dbg, dst_name=b, feature=feature,
    )

    emit("two:plan", a=a, b=b, feature=feature,
         add_to_A=len(add_to_A), add_to_B=len(add_to_B),
         rem_from_A=len(rem_from_A), rem_from_B=len(rem_from_B))

    #--- Apply removals (with tombstone + alias-aware logic) ----------------
    resA_rem = {"ok": True, "count": 0}
    resB_rem = {"ok": True, "count": 0}
    remA_keys = [_ck(_minimal(it)) for it in (rem_from_A or []) if _ck(_minimal(it))]
    remB_keys = [_ck(_minimal(it)) for it in (rem_from_B or []) if _ck(_minimal(it))]

    # Tombstone marker
    def _mark_tombs(items: List[Dict[str, Any]]) -> None:
        try:
            now_ts = int(_t.time())
            tomb = ctx.state_store.load_tomb() or {}
            ks = tomb.setdefault("keys", {})

            tokens = set()
            for it in (items or []):
                try:
                    ck = _ck(_minimal(it))
                    if ck:
                        tokens.add(ck)
                    for idk, idv in ((it.get("ids") or {}) or {}).items():
                        if idv is None or str(idv) == "":
                            continue
                        tokens.add(f"{str(idk).lower()}:{str(idv).lower()}")
                except Exception:
                    continue

            for tok in tokens:
                ks.setdefault(f"{feature}|{tok}", now_ts)            # global
                ks.setdefault(f"{feature}:{pair_key}|{tok}", now_ts)  # pair

            ctx.state_store.save_tomb(tomb)
            emit("debug", msg="tombstones.marked", feature=feature,
                 added=len(tokens), scope="global+pair")
        except Exception:
            pass

    if rem_from_A:
        if a_down:
            record_unresolved(a, feature, rem_from_A, hint="provider_down:remove")
            emit("writes:skipped", dst=a, feature=feature, reason="provider_down", op="remove", count=len(rem_from_A))
        else:
            emit("two:apply:remove:A:start", dst=a, feature=feature, count=len(rem_from_A))
            resA_rem = apply_remove(
                dst_ops=aops, cfg=cfg, dst_name=a, feature=feature, items=rem_from_A,
                dry_run=dry_run_flag, emit=emit, dbg=dbg,
                chunk_size=ctx.apply_chunk_size, chunk_pause_ms=_pause_for(a),
            )
            prov_count_A = _confirmed(resA_rem)

            # Prune from in-memory baseline up to what the provider confirmed
            if prov_count_A and not dry_run_flag:
                removed_now = 0
                for k in remA_keys:
                    if k in A_eff:
                        A_eff.pop(k, None)
                        removed_now += 1
                        if removed_now >= prov_count_A:
                            break
                _mark_tombs(rem_from_A)

            emit("two:apply:remove:A:done", dst=a, feature=feature,
                 count=_confirmed(resA_rem),
                 attempted=int(resA_rem.get("attempted", 0)),
                 removed=_confirmed(resA_rem),
                 skipped=int(resA_rem.get("skipped", 0)),
                 unresolved=int(resA_rem.get("unresolved", 0)),
                 errors=int(resA_rem.get("errors", 0)),
                 result=resA_rem)

    if rem_from_B:
        if b_down:
            record_unresolved(b, feature, rem_from_B, hint="provider_down:remove")
            emit("writes:skipped", dst=b, feature=feature, reason="provider_down", op="remove", count=len(rem_from_B))
        else:
            emit("two:apply:remove:B:start", dst=b, feature=feature, count=len(rem_from_B))
            resB_rem = apply_remove(
                dst_ops=bops, cfg=cfg, dst_name=b, feature=feature, items=rem_from_B,
                dry_run=dry_run_flag, emit=emit, dbg=dbg,
                chunk_size=ctx.apply_chunk_size, chunk_pause_ms=_pause_for(b),
            )
            prov_count_B = _confirmed(resB_rem)

            # Prune from in-memory baseline up to what the provider confirmed
            if prov_count_B and not dry_run_flag:
                removed_now = 0
                for k in remB_keys:
                    if k in B_eff:
                        B_eff.pop(k, None)
                        removed_now += 1
                        if removed_now >= prov_count_B:
                            break
                _mark_tombs(rem_from_B)

            emit("two:apply:remove:B:done", dst=b, feature=feature,
                 count=_confirmed(resB_rem),
                 attempted=int(resB_rem.get("attempted", 0)),
                 removed=_confirmed(resB_rem),
                 skipped=int(resB_rem.get("skipped", 0)),
                 unresolved=int(resB_rem.get("unresolved", 0)),
                 errors=int(resB_rem.get("errors", 0)),
                 result=resB_rem)

    #--- Apply additions (with blackbox + phantom guard logic) --------------
    resA_add = {"ok": True, "count": 0}
    resB_add = {"ok": True, "count": 0}
    eff_add_A = 0
    eff_add_B = 0
    unresolved_new_A_total = 0
    unresolved_new_B_total = 0

    if add_to_A:
        if a_down:
            record_unresolved(a, feature, add_to_A, hint="provider_down:add")
            emit("writes:skipped", dst=a, feature=feature, reason="provider_down", op="add", count=len(add_to_A))
            unresolved_new_A_total += len(add_to_A)
        else:
            emit("two:apply:add:A:start", dst=a, feature=feature, count=len(add_to_A))
            unresolved_before_A = set(load_unresolved_keys(a, feature, cross_features=True) or [])
            _ = set(load_blackbox_keys(a, feature, pair=pair_key) or [])  # not used directly in strict mode
            keys_A = {_ck(it) for it in add_to_A}
            k2i_A = {_ck(it): _minimal(it) for it in add_to_A}

            resA_add = apply_add(
                dst_ops=aops, cfg=cfg, dst_name=a, feature=feature, items=add_to_A,
                dry_run=dry_run_flag, emit=emit, dbg=dbg,
                chunk_size=ctx.apply_chunk_size, chunk_pause_ms=_pause_for(a),
            )
            unresolved_after_A = set(load_unresolved_keys(a, feature, cross_features=True) or [])
            new_unresolved_A = unresolved_after_A - unresolved_before_A
            unresolved_new_A_total += len(new_unresolved_A)

            confirmed_A = [k for k in keys_A if k not in new_unresolved_A]
            prov_count_A = _confirmed(resA_add)

            if verify_after_write and _apply_verify_after_write_supported(aops):
                try:
                    unresolved_again = set(load_unresolved_keys(a, feature, cross_features=True) or [])
                    confirmed_A = [k for k in confirmed_A if k not in unresolved_again]
                except Exception:  # pragma: no cover
                    pass
                eff_add_A = len(confirmed_A)
            else:
                eff_add_A = 0 if new_unresolved_A else min(prov_count_A, len(confirmed_A))

            if eff_add_A != prov_count_A:
                dbg("two:apply:add:corrected", dst=a, feature=feature,
                    provider_count=prov_count_A, effective=eff_add_A, newly_unresolved=len(new_unresolved_A))

            try:
                failed_A = [k for k in keys_A if k not in confirmed_A]
                if failed_A:
                    record_attempts(a, feature, failed_A,
                                    reason="two:apply:add:failed", op="add",
                                    pair=pair_key, cfg=cfg)
                    failed_items_A = [k2i_A.get(k) for k in failed_A if k2i_A.get(k)]
                    if failed_items_A: record_unresolved(a, feature, failed_items_A, hint="apply:add:failed")
                if confirmed_A:
                    record_success(a, feature, confirmed_A, pair=pair_key, cfg=cfg)
                if use_phantoms and 'guardA' in locals() and guardA and eff_add_A and confirmed_A:
                    guardA.record_success(set(confirmed_A[:eff_add_A]))
            except Exception:
                pass

            if eff_add_A and not dry_run_flag:
                for k in confirmed_A[:eff_add_A]:
                    v = k2i_A.get(k)
                    if v: A_eff[k] = v

            emit("two:apply:add:A:done", dst=a, feature=feature,
                 count=_confirmed(resA_add),
                 attempted=int(resA_add.get("attempted", 0)),
                 added=_confirmed(resA_add),
                 skipped=int(resA_add.get("skipped", 0)),
                 unresolved=int(resA_add.get("unresolved", 0)),
                 errors=int(resA_add.get("errors", 0)),
                 result=resA_add)

    if add_to_B:
        if b_down:
            record_unresolved(b, feature, add_to_B, hint="provider_down:add")
            emit("writes:skipped", dst=b, feature=feature, reason="provider_down", op="add", count=len(add_to_B))
            unresolved_new_B_total += len(add_to_B)
        else:
            emit("two:apply:add:B:start", dst=b, feature=feature, count=len(add_to_B))
            unresolved_before_B = set(load_unresolved_keys(b, feature, cross_features=True) or [])
            _ = set(load_blackbox_keys(b, feature, pair=pair_key) or [])  # not used directly in strict mode
            keys_B = {_ck(it) for it in add_to_B}
            k2i_B = {_ck(it): _minimal(it) for it in add_to_B}

            resB_add = apply_add(
                dst_ops=bops, cfg=cfg, dst_name=b, feature=feature, items=add_to_B,
                dry_run=dry_run_flag, emit=emit, dbg=dbg,
                chunk_size=ctx.apply_chunk_size, chunk_pause_ms=_pause_for(b),
            )
            unresolved_after_B = set(load_unresolved_keys(b, feature, cross_features=True) or [])
            new_unresolved_B = unresolved_after_B - unresolved_before_B
            unresolved_new_B_total += len(new_unresolved_B)

            confirmed_B = [k for k in keys_B if k not in new_unresolved_B]
            prov_count_B = _confirmed(resB_add)

            if verify_after_write and _apply_verify_after_write_supported(bops):
                try:
                    unresolved_again = set(load_unresolved_keys(b, feature, cross_features=True) or [])
                    confirmed_B = [k for k in confirmed_B if k not in unresolved_again]
                except Exception:  # pragma: no cover
                    pass
                eff_add_B = len(confirmed_B)
            else:
                eff_add_B = 0 if new_unresolved_B else min(prov_count_B, len(confirmed_B))

            if eff_add_B != prov_count_B:
                dbg("two:apply:add:corrected", dst=b, feature=feature,
                    provider_count=prov_count_B, effective=eff_add_B, newly_unresolved=len(new_unresolved_B))

            try:
                failed_B = [k for k in keys_B if k not in confirmed_B]
                if failed_B:
                    record_attempts(b, feature, failed_B,
                                    reason="two:apply:add:failed", op="add",
                                    pair=pair_key, cfg=cfg)
                    failed_items_B = [k2i_B.get(k) for k in failed_B if k2i_B.get(k)]
                    if failed_items_B: record_unresolved(b, feature, failed_items_B, hint="apply:add:failed")
                if confirmed_B:
                    record_success(b, feature, confirmed_B, pair=pair_key, cfg=cfg)
                if use_phantoms and 'guardB' in locals() and guardB and eff_add_B and confirmed_B:
                    guardB.record_success(set(confirmed_B[:eff_add_B]))
            except Exception:
                pass

            if eff_add_B and not dry_run_flag:
                for k in confirmed_B[:eff_add_B]:
                    v = k2i_B.get(k)
                    if v: B_eff[k] = v

            emit("two:apply:add:B:done", dst=b, feature=feature,
                 count=_confirmed(resB_add),
                 attempted=int(resB_add.get("attempted", 0)),
                 added=_confirmed(resB_add),
                 skipped=int(resB_add.get("skipped", 0)),
                 unresolved=int(resB_add.get("unresolved", 0)),
                 errors=int(resB_add.get("errors", 0)),
                 result=resB_add)

    #--- Cascade removals in state store -----------------------------------
    try:
        rem_keys = (rem_from_A or []) + (rem_from_B or [])
        cascade_removals(
            ctx.state_store, dbg, feature=feature,
            removed_keys=[k.get("ids", {}).get("imdb") or "" for k in rem_keys if isinstance(k, dict)],
        )
    except Exception:
        pass

    #--- Save new baselines + checkpoints ---------------------------------
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
            if not chk: return
            pf = _ensure_pf(pmap, prov, feat); pf["checkpoint"] = chk

        _commit_baseline(provs_block, a, feature, A_eff)
        _commit_baseline(provs_block, b, feature, B_eff)
        _commit_checkpoint(provs_block, a, feature, now_cp_A)
        _commit_checkpoint(provs_block, b, feature, now_cp_B)

        st["last_sync_epoch"] = int(_t.time())
        ctx.state_store.save_state(st)
    except Exception:
        pass

    emit("two:done", a=a, b=b, feature=feature,
         adds_to_A=eff_add_A, adds_to_B=eff_add_B,
         rem_from_A=_confirmed(resA_rem),
         rem_from_B=_confirmed(resB_rem))

    # NEW: roll-ups for orchestrator aggregation
    skipped_total = int(resA_add.get("skipped", 0)) + int(resB_add.get("skipped", 0)) + \
                    int(resA_rem.get("skipped", 0)) + int(resB_rem.get("skipped", 0))
    errors_total  = int(resA_add.get("errors", 0))  + int(resB_add.get("errors", 0))  + \
                    int(resA_rem.get("errors", 0))  + int(resB_rem.get("errors", 0))
    unresolved_total = int(unresolved_new_A_total) + int(unresolved_new_B_total)

    return {
        "ok": True, "feature": feature, "a": a, "b": b,
        "adds_to_A": eff_add_A, "adds_to_B": eff_add_B,
        "rem_from_A": _confirmed(resA_rem),
        "rem_from_B": _confirmed(resB_rem),
        "resA_add": resA_add, "resB_add": resB_add,
        "resA_remove": resA_rem, "resB_remove": resB_rem,
        "unresolved_to_A": int(unresolved_new_A_total),
        "unresolved_to_B": int(unresolved_new_B_total),
        # NEW: totals so Orchestrator.run_pairs can add them into run:done
        "unresolved": unresolved_total,
        "skipped": skipped_total,
        "errors": errors_total,
    }

def run_two_way_feature(
    ctx,
    src: str,
    dst: str,
    *,
    feature: str,
    fcfg: Mapping[str, Any],
    health_map: Mapping[str, Any],
) -> Dict[str, Any]:

    emit = ctx.emit

    Hs = health_map.get(str(src).upper()) or {}
    Hd = health_map.get(str(dst).upper()) or {}

    ops_src = ctx.providers.get(str(src).upper())
    ops_dst = ctx.providers.get(str(dst).upper())

    caps_off = False
    try:
        caps_off = ((ops_src and (ops_src.capabilities() or {}).get("observed_deletes") is False) or
                    (ops_dst and (ops_dst.capabilities() or {}).get("observed_deletes") is False))
    except Exception:
        caps_off = False

    include_obs_override = False if (_health_status(Hs) == "down"
                                    or _health_status(Hd) == "down"
                                    or caps_off) else None

    emit("feature:start", src=str(src).upper(), dst=str(dst).upper(), feature=feature)
    res = _two_way_sync(
        ctx, str(src).upper(), str(dst).upper(),
        feature=feature, fcfg=fcfg, health_map=health_map,
        include_observed_override=include_obs_override,
    )
    emit("feature:done", src=str(src).upper(), dst=str(dst).upper(), feature=feature)
    return res
