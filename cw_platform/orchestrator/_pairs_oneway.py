# cw_platform/orchestration/_pairs_oneway.py
# One-way synchronization logic for data pairs.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Mapping
from typing import Any


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


from ._pairs_utils import (
    _supports_feature,
    _resolve_flags,
    _health_status,
    _health_feature_ok,
    _rate_remaining,
    _apply_verify_after_write_supported,
    manual_policy as _manual_policy,
    merge_manual_adds as _merge_manual_adds,
    filter_manual_block as _filter_manual_block,
)
from ._pairs_massdelete import maybe_block_mass_delete as _maybe_block_mass_delete
from ._pairs_blocklist import apply_blocklist

# Blackbox imports
try:  # pragma: no cover
    from ._blackbox import load_blackbox_keys, record_attempts, record_success  # type: ignore
except Exception:  # pragma: no cover
    def load_blackbox_keys(dst: str, feature: str) -> set[str]:
        return set()
    def record_attempts(dst: str, feature: str, keys, **kwargs) -> dict[str, Any]:
        return {"ok": True, "count": 0}
    def record_success(dst: str, feature: str, keys, **kwargs) -> dict[str, Any]:
        return {"ok": True, "count": 0}

_PROVIDER_KEY_MAP = {
    "PLEX": "plex",
    "JELLYFIN": "jellyfin",
    "EMBY": "emby",
}

def _effective_library_whitelist(
    cfg: Mapping[str, Any],
    provider_name: str,
    feature: str,
    fcfg: Mapping[str, Any],
) -> list[str]:
    if feature not in ("history", "ratings"):
        return []

    libs: list[str] = []

    lib_cfg = fcfg.get("libraries")
    if isinstance(lib_cfg, dict):
        per = lib_cfg.get(provider_name.upper()) or lib_cfg.get(provider_name.lower())
        if isinstance(per, (list, tuple)):
            libs = [str(x).strip() for x in per if str(x).strip()]
    elif isinstance(lib_cfg, (list, tuple)):
        libs = [str(x).strip() for x in lib_cfg if str(x).strip()]

    if libs:
        return libs

    key = _PROVIDER_KEY_MAP.get(str(provider_name).upper())
    if not key:
        return []

    prov_cfg = cfg.get(key) or {}
    feat_cfg = (prov_cfg.get(feature) or {})
    base_libs = feat_cfg.get("libraries") or []
    if isinstance(base_libs, (list, tuple)):
        return [str(x).strip() for x in base_libs if str(x).strip()]

    return []

def _filter_index_by_libraries(idx: dict[str, Any], libs: list[str], *, allow_unknown: bool = False) -> dict[str, Any]:
    if not libs or not idx:
        return dict(idx)

    allowed = {str(x).strip() for x in libs if str(x).strip()}
    if not allowed:
        return dict(idx)

    out: dict[str, Any] = {}
    for ck, item in idx.items():
        v = item or {}
        lid = (
            v.get("library_id")
            or v.get("libraryId")
            or v.get("library")
            or v.get("section_id")
            or v.get("sectionId")
        )

        if lid is None:
            if allow_unknown:
                out[ck] = v
            continue

        if str(lid).strip() in allowed:
            out[ck] = v

    return out

# Feature-specific filters
def _ratings_filter_index(idx: dict[str, Any], fcfg: Mapping[str, Any]) -> dict[str, Any]:
    alias = {"movies":"movie","movie":"movie","shows":"show","show":"show","anime":"show","animes":"show",
             "episodes":"episode","episode":"episode","ep":"episode","eps":"episode"}
    types_raw = [str(t).strip().lower() for t in (fcfg.get("types") or []) if isinstance(t, (str, bytes))]
    types = {alias.get(t, t.rstrip("s")) for t in types_raw if t}
    from_date = str(fcfg.get("from_date") or "").strip()

    def _keep(v: Mapping[str, Any]) -> bool:
        vt = alias.get(str(v.get("type","")).strip().lower(),
                       str(v.get("type","")).strip().lower().rstrip("s"))
        if types and vt not in types:
            return False
        if from_date:
            ra = (v.get("rated_at") or v.get("ratedAt") or "").strip()
            if not ra:
                return True
            if ra[:10] < from_date:
                return False
        return True

    return {k: v for k, v in idx.items() if _keep(v)}

# One-way sync core
def run_one_way_feature(
    ctx,
    src: str,
    dst: str,
    *,
    feature: str,
    fcfg: Mapping[str, Any],
    health_map: Mapping[str, Any],
) -> dict[str, Any]:
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

    if (not _supports_feature(src_ops, feature)) or (not _supports_feature(dst_ops, feature)) \
       or (not _health_feature_ok(Hs, feature)) or (not _health_feature_ok(Hd, feature)):
        emit("feature:unsupported", src=src, dst=dst, feature=feature,
             src_supported=_supports_feature(src_ops, feature) and _health_feature_ok(Hs, feature),
             dst_supported=_supports_feature(dst_ops, feature) and _health_feature_ok(Hd, feature))
        emit("feature:done", src=src, dst=dst, feature=feature)
        return {"ok": True, "added": 0, "removed": 0, "unresolved": 0}

    if src_down:
        emit("writes:skipped", src=src, dst=dst, feature=feature, reason="source_down")
        emit("feature:done", src=src, dst=dst, feature=feature)
        return {"ok": True, "added": 0, "removed": 0, "unresolved": 0}

    include_observed = bool(sync_cfg.get("include_observed_deletes", True))
    if src_down or dst_down:
        include_observed = False

    def _cap_obsdel(ops) -> bool | None:
        try:
            v = (ops.capabilities() or {}).get("observed_deletes")
            return None if v is None else bool(v)
        except Exception:
            return None

    try:
        if (_cap_obsdel(src_ops) is False) or (_cap_obsdel(dst_ops) is False):
            pair_key_dbg = "-".join(sorted([src, dst]))
            emit("debug",
                 msg="observed.deletions.partial",
                 feature=feature, pair=pair_key_dbg, reason="provider_capability")
    except Exception:
        pass

    def _pause_for(pname: str) -> int:
        base = int(getattr(ctx, "apply_chunk_pause_ms", 0) or 0)
        rem = _rate_remaining(health_map.get(pname))
        if rem is not None and rem < 10:
            emit("rate:slow", provider=pname, remaining=rem, base_ms=base, extra_ms=1000)
            return base + 1000
        return base

    def _bust_snapshot(pname: str) -> None:
        try:
            sc = getattr(ctx, "snap_cache", None)
            if isinstance(sc, dict):
                sc.pop((pname, feature), None)
                sc.pop(pname, None)
        except Exception:
            pass

    def _alias_index(idx: dict[str, dict[str, Any]]) -> dict[str, str]:
        m: dict[str, str] = {}
        for ck, it in (idx or {}).items():
            ids = (it.get("ids") or {})
            for k, v in (ids or {}).items():
                if v is None or str(v) == "": continue
                m[f"{k}:{str(v).lower()}"] = ck
        return m

    def _present(idx: dict[str, Any], alias: dict[str, str], it: Mapping[str, Any]) -> bool:
        ck = _ck(it)
        if ck in idx: return True
        ids = (it.get("ids") or {})
        try:
            for k, v in (ids or {}).items():
                if v is None or str(v) == "": continue
                if f"{k}:{str(v).lower()}" in alias: return True
        except Exception:
            pass
        return False

    pair_providers = {src: src_ops, dst: dst_ops}

    snaps = build_snapshots_for_feature(
        feature=feature,
        config=cfg,
        providers=pair_providers,
        snap_cache=ctx.snap_cache,
        snap_ttl_sec=ctx.snap_ttl_sec,
        dbg=dbg,
        emit_info=ctx.emit_info,
    )

    src_cur = snaps.get(src) or {}
    dst_cur = snaps.get(dst) or {}

    prev_state = ctx.state_store.load_state() or {}
    manual_adds, manual_blocks = _manual_policy(prev_state, src, feature)
    prev_provs = (prev_state.get("providers") or {})
    prev_src = dict((((prev_provs.get(src, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})
    prev_dst = dict((((prev_provs.get(dst, {}) or {}).get(feature, {}) or {}).get("baseline", {}) or {}).get("items") or {})

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

    libs_src: list[str] = _effective_library_whitelist(cfg, src, feature, fcfg)
    libs_dst: list[str] = _effective_library_whitelist(cfg, dst, feature, fcfg)

    allow_unknown_src = (str(src).upper() == "PLEX" and feature == "history")
    allow_unknown_dst = (str(dst).upper() == "PLEX" and feature == "history")

    if libs_src:
        prev_src = _filter_index_by_libraries(prev_src, libs_src, allow_unknown=allow_unknown_src)
        src_cur  = _filter_index_by_libraries(src_cur,  libs_src, allow_unknown=allow_unknown_src)
        eff_src  = _filter_index_by_libraries(eff_src,  libs_src, allow_unknown=allow_unknown_src)

    if libs_dst:
        prev_dst = _filter_index_by_libraries(prev_dst, libs_dst, allow_unknown=allow_unknown_dst)
        dst_cur  = _filter_index_by_libraries(dst_cur,  libs_dst, allow_unknown=allow_unknown_dst)
        eff_dst  = _filter_index_by_libraries(eff_dst,  libs_dst, allow_unknown=allow_unknown_dst)

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

    if feature == "ratings":
        src_idx  = _ratings_filter_index(src_idx,  fcfg)
        dst_full = _ratings_filter_index(dst_full, fcfg)
        if manual_adds:
            src_idx = _merge_manual_adds(src_idx, manual_adds)
        adds, removes = diff_ratings(src_idx, dst_full)
    else:
        if manual_adds:
            src_idx = _merge_manual_adds(src_idx, manual_adds)
        adds, removes = diff(src_idx, dst_full)

    src_alias = _alias_index(src_idx)
    dst_alias = _alias_index(dst_full)

    if feature == "ratings":
        if removes:
            removes = [it for it in removes if not _present(src_idx, src_alias, it)]
            try:
                removes = [it for it in removes if _ck(it) in prev_dst]
            except Exception:
                pass
    else:
        if adds:
            adds = [it for it in adds if not _present(dst_full, dst_alias, it)]
        if removes:
            removes = [it for it in removes if not _present(src_idx, src_alias, it)]
            try:
                removes = [it for it in removes if _ck(it) in prev_dst]
            except Exception:
                pass

    if not allow_adds:
        adds = []
    if not allow_removes:
        removes = []

    removes = _maybe_block_mass_delete(
        removes, baseline_size=len(dst_full),
        allow_mass_delete=bool(sync_cfg.get("allow_mass_delete", True)),
        suspect_ratio=suspect_ratio,
        emit=emit, dbg=dbg, dst_name=dst, feature=feature,
    )

    pair_key = "-".join(sorted([src, dst]))
    if feature != "watchlist":
        adds = apply_blocklist(
            ctx.state_store, adds, dst=dst, feature=feature, pair_key=pair_key, emit=emit
        )

    manual_blocked = 0
    if manual_blocks:
        b_adds, b_rem = len(adds), len(removes)
        adds = _filter_manual_block(adds, manual_blocks)
        removes = _filter_manual_block(removes, manual_blocks)
        manual_blocked = (b_adds - len(adds)) + (b_rem - len(removes))

        if manual_blocked:
            ctx.emit(
                "debug",
                msg="blocked.manual",
                feature=feature,
                pair=f"{src}-{dst}",
                blocked_items=int(manual_blocked),
                blocked_keys=int(len(manual_blocks)),
            )
            ctx.stats_manual_blocked = int(getattr(ctx, "stats_manual_blocked", 0) or 0) + int(manual_blocked)

    try:
        unresolved_known = set(load_unresolved_keys(dst, feature, cross_features=True) or [])
    except Exception:
        unresolved_known = set()

    if unresolved_known and adds:
        _before = len(adds)
        try:
            adds = [it for it in adds if _ck(it) not in unresolved_known]
        except Exception:
            pass
        _blocked = _before - len(adds)
        if _blocked:
            emit("debug", msg="blocked.unresolved", feature=feature, dst=dst, blocked=_blocked)

    emit("one:plan", src=src, dst=dst, feature=feature,
        adds=len(adds), removes=len(removes),
        src_count=len(src_idx), dst_count=len(dst_full))

    bb = ((cfg or {}).get("blackbox") if isinstance(cfg, dict) else getattr(cfg, "blackbox", {})) or {}
    use_phantoms = bool(bb.get("enabled") and bb.get("block_adds", True))
    ttl_days = int(bb.get("cooldown_days") or 0) or None

    guard = PhantomGuard(src, dst, feature, ttl_days=ttl_days, enabled=use_phantoms)
    if use_phantoms and adds:
        adds, _blocked = guard.filter_adds(adds, _ck, _minimal, emit, ctx.state_store, pair_key)

    attempted_keys = {_ck(it) for it in adds}
    key2item = {_ck(it): _minimal(it) for it in adds}

    added_effective = 0
    res_add: dict[str, Any] = {"attempted": 0, "confirmed": 0, "skipped": 0, "unresolved": 0, "errors": 0}
    unresolved_new_total = 0
    dry_run_flag = bool(ctx.dry_run or sync_cfg.get("dry_run", False))
    verify_after_write = bool(sync_cfg.get("verify_after_write", False))

    if adds:
        if dst_down:
            record_unresolved(dst, feature, adds, hint="provider_down:add")
            emit("writes:skipped", dst=dst, feature=feature, reason="provider_down", op="add", count=len(adds))
            unresolved_new_total += len(adds)
        else:
            unresolved_before = set(load_unresolved_keys(dst, feature, cross_features=True) or [])
            _ = set(load_blackbox_keys(dst, feature) or [])
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
            res_add = {
                "attempted": int((add_res or {}).get("attempted", 0)),
                "confirmed": int((add_res or {}).get("confirmed", (add_res or {}).get("count", 0)) or 0),
                "skipped": int((add_res or {}).get("skipped", 0)),
                "unresolved": int((add_res or {}).get("unresolved", 0)),
                "errors": int((add_res or {}).get("errors", 0)),
            }
            new_unresolved = unresolved_after - unresolved_before
            unresolved_new_total += len(new_unresolved)

            confirmed_keys = [k for k in attempted_keys if k not in new_unresolved]

            if verify_after_write and _apply_verify_after_write_supported(dst_ops):
                try:
                    unresolved_again = set(load_unresolved_keys(dst, feature, cross_features=True) or [])
                    confirmed_keys = [k for k in confirmed_keys if k not in unresolved_again]
                except Exception:
                    pass

            prov_confirmed = int((add_res or {}).get("confirmed", (add_res or {}).get("count", 0)) or 0)

            if not dry_run_flag and not new_unresolved and prov_confirmed == 0 and adds:
                try:
                    record_unresolved(dst, feature, adds, hint="apply:add:no_confirmations_fallback")
                    new_unresolved = set(attempted_keys)
                    unresolved_new_total += len(new_unresolved)
                    confirmed_keys = [k for k in attempted_keys if k not in new_unresolved]
                except Exception:
                    pass

            strict_pessimist = (not verify_after_write) and bool(new_unresolved)
            if strict_pessimist:
                added_effective = 0
            else:
                added_effective = len(confirmed_keys) if verify_after_write else min(prov_confirmed, len(confirmed_keys))

            if added_effective != prov_confirmed:
                dbg("apply:add:corrected", dst=dst, feature=feature,
                    provider_count=prov_confirmed, effective=added_effective,
                    newly_unresolved=len(new_unresolved))

            failed_keys = [k for k in attempted_keys if k not in confirmed_keys]
            try:
                if failed_keys:
                    record_attempts(dst, feature, failed_keys, reason="apply:add:failed", op="add",
                                    pair=pair_key, cfg=cfg)
                    failed_items = [key2item[k] for k in failed_keys if k in key2item]
                    if failed_items:
                        record_unresolved(dst, feature, failed_items, hint="apply:add:failed")

                if confirmed_keys:
                    record_success(dst, feature, confirmed_keys, pair=pair_key, cfg=cfg)
                if use_phantoms and guard and added_effective and confirmed_keys:
                    guard.record_success(confirmed_keys)
            except Exception:
                pass

            if added_effective and not dry_run_flag:
                for k in confirmed_keys[:added_effective]:
                    v = key2item.get(k)
                    if v:
                        dst_full[k] = v
                _bust_snapshot(dst)

    removed_count = 0
    rem_keys_attempted: list[str] = []
    res_remove: dict[str, Any] = {"attempted": 0, "confirmed": 0, "skipped": 0, "unresolved": 0, "errors": 0}
    if removes:
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
            removed_count = int((rem_res or {}).get("confirmed", (rem_res or {}).get("count", 0)) or 0)
            res_remove = {
                "attempted": int((rem_res or {}).get("attempted", 0)),
                "confirmed": int((rem_res or {}).get("confirmed", (rem_res or {}).get("count", 0)) or 0),
                "skipped": int((rem_res or {}).get("skipped", 0)),
                "unresolved": int((rem_res or {}).get("unresolved", 0)),
                "errors": int((rem_res or {}).get("errors", 0)),
            }

            if removed_count and not dry_run_flag:
                try:
                    import time as _t
                    now = int(_t.time())
                    t = ctx.state_store.load_tomb() or {}
                    ks = t.setdefault("keys", {})

                    removed_tokens = set()
                    for it in (removes or []):
                        try:
                            ck = _ck(_minimal(it))
                            if ck:
                                removed_tokens.add(ck)
                            ids = (it.get("ids") or {})
                            for idk, idv in (ids or {}).items():
                                if idv is None or str(idv) == "":
                                    continue
                                removed_tokens.add(f"{str(idk).lower()}:{str(idv).lower()}")
                        except Exception:
                            continue

                    for tok in removed_tokens:
                        ks.setdefault(f"{feature}|{tok}", now)
                        ks.setdefault(f"{feature}:{pair_key}|{tok}", now)

                    ctx.state_store.save_tomb(t)
                    emit("debug", msg="tombstones.marked", feature=feature,
                         added=len(removed_tokens), scope="global+pair")
                except Exception:
                    pass
            if not dry_run_flag and removed_count:
                for k in rem_keys_attempted:
                    if k in dst_full:
                        dst_full.pop(k, None)
                _bust_snapshot(dst)

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

    try:
        if removes:
            cascade_removals(
                ctx.state_store, dbg, feature=feature,
                removed_keys=[k.get("ids", {}).get("imdb") or "" for k in removes if isinstance(k, dict)],
            )
    except Exception:
        pass

    emit("feature:done", src=src, dst=dst, feature=feature)

    return {
        "ok": True,
        "added": int(added_effective),
        "removed": int(removed_count),
        "skipped": int((res_add or {}).get("skipped", 0)) + int((res_remove or {}).get("skipped", 0)),
        "unresolved": int((res_add or {}).get("unresolved", 0)) + int((res_remove or {}).get("unresolved", 0)),
        "errors": int((res_add or {}).get("errors", 0)) + int((res_remove or {}).get("errors", 0)),
        "res_add": res_add,
        "res_remove": res_remove,
    }