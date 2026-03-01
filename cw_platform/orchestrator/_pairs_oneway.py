# cw_platform/orchestrator/_pairs_oneway.py
# One-way synchronization logic for data pairs.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Mapping
from typing import Any

import os
import re

from ..provider_instances import normalize_instance_id

from ..id_map import minimal as _minimal, canonical_key as _ck
from ._snapshots import (
    build_snapshots_for_feature,
    coerce_suspect_snapshot,
    module_checkpoint,
    prev_checkpoint,
)
from ._applier import apply_add, apply_remove
from ._chunking import effective_chunk_size
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
    "ANILIST": "anilist",
}


def _index_semantics(ops, feature: str) -> str:
    try:
        caps = ops.capabilities() or {}
    except Exception:
        return "present"
    if not isinstance(caps, Mapping):
        return "present"
    per = caps.get(feature)
    if isinstance(per, Mapping):
        sem = per.get("index_semantics")
        if sem:
            return str(sem).lower()
    return str(caps.get("index_semantics", "present")).lower()


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

# History key helpers
_HISTORY_KEY_RE = re.compile(r"^(?P<base>.+?)@(?P<ts>\d+)(?P<rest>.*)$")

def _history_bucket_sec(a: str, b: str, feature: str) -> int:
    if str(feature) != "history":
        return 0
    a_u = str(a or "").upper()
    b_u = str(b or "").upper()
    return 60 if (a_u == "TRAKT" or b_u == "TRAKT") else 0

def _history_ts_from_key(key: str) -> int | None:
    m = _HISTORY_KEY_RE.match(str(key))
    if not m:
        return None
    try:
        return int(m.group("ts"))
    except Exception:
        return None

def _bucket_ts(ts: int, bucket_sec: int) -> int:
    b = int(bucket_sec or 0)
    if b <= 1:
        return int(ts)
    return (int(ts) // b) * b

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
    src_inst = normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    dst_inst = normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))
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

    Hs = health_map.get(f"{src}#{src_inst}") or health_map.get(src) or {}
    Hd = health_map.get(f"{dst}#{dst_inst}") or health_map.get(dst) or {}
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
        inst = src_inst if pname == src else (dst_inst if pname == dst else "default")
        rem = _rate_remaining(health_map.get(f"{pname}#{inst}") or health_map.get(pname))
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

    def _typed_tokens(it: Mapping[str, Any]) -> set[str]:
        typ = str(it.get("type") or "").strip().lower()
        show_ids_raw = it.get("show_ids") if isinstance(it.get("show_ids"), Mapping) else {}
        ids_raw = it.get("ids") if isinstance(it.get("ids"), Mapping) else {}
        show_ids = dict(show_ids_raw or {})
        ids = dict(ids_raw or {})

        toks: set[str] = set()

        if typ == "episode":
            try:
                s = int(it.get("season") or 0)
                e = int(it.get("episode") or 0)
            except Exception:
                s, e = 0, 0
            has_frag = bool(s > 0 and e > 0)
            if has_frag:
                frag = f"#s{s:02d}e{e:02d}"
    
                for src_ids in (show_ids, ids):
                    for k, v in src_ids.items():
                        if v is None or str(v) == "":
                            continue
                        toks.add(f"{str(k).lower()}:{str(v).lower()}{frag}")

            if not has_frag:
                for k, v in ids.items():
                    if v is None or str(v) == "":
                        continue
                    toks.add(f"{str(k).lower()}:{str(v).lower()}")

        elif typ == "season":
            try:
                s = int(it.get("season") or 0)
            except Exception:
                s = 0
            if s > 0:
                frag = f"#season:{s}"
                for src_ids in (show_ids, ids):
                    for k, v in src_ids.items():
                        if v is None or str(v) == "":
                            continue
                        toks.add(f"{str(k).lower()}:{str(v).lower()}{frag}")

        else:
            for k, v in ids.items():
                if v is None or str(v) == "":
                    continue
                toks.add(f"{str(k).lower()}:{str(v).lower()}")

        return toks

    def _alias_index(idx: dict[str, dict[str, Any]]) -> dict[str, str]:
        m: dict[str, str] = {}
        for ck, it in (idx or {}).items():
            if not isinstance(it, Mapping):
                continue
            for tok in _typed_tokens(it):
                m[tok] = ck
        return m

    def _present(idx: dict[str, Any], alias: dict[str, str], it: Mapping[str, Any]) -> bool:
        ck = _ck(it)
        if ck in idx:
            return True
        for tok in _typed_tokens(it):
            if tok in alias:
                return True
        return False

    def _find_in_idx(idx: dict[str, Any], alias: dict[str, str], it: Mapping[str, Any]) -> Mapping[str, Any] | None:
        ck = _ck(it)
        if ck and ck in idx:
            v = idx.get(ck)
            return v if isinstance(v, Mapping) else None
        for tok in _typed_tokens(it):
            dk = alias.get(tok)
            if not dk:
                continue
            v = idx.get(dk)
            return v if isinstance(v, Mapping) else None
        return None

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

    def _prev_items(pmap: Mapping[str, Any], prov: str, inst: str, feat: str) -> dict[str, Any]:
        try:
            pblk = pmap.get(prov) or {}
            if not isinstance(pblk, Mapping):
                return {}
            if inst != "default":
                insts = pblk.get("instances") or {}
                if not isinstance(insts, Mapping):
                    return {}
                pblk = insts.get(inst) or {}
                if not isinstance(pblk, Mapping):
                    return {}
            fblk = pblk.get(feat) or {}
            if not isinstance(fblk, Mapping):
                return {}
            base = fblk.get("baseline") or {}
            if not isinstance(base, Mapping):
                return {}
            items = base.get("items") or {}
            return dict(items) if isinstance(items, Mapping) else {}
        except Exception:
            return {}

    prev_src = _prev_items(prev_provs, src, src_inst, feature)
    prev_dst = _prev_items(prev_provs, dst, dst_inst, feature)

    drop_guard = bool(sync_cfg.get("drop_guard", False))
    suspect_min_prev = int((cfg.get("runtime") or {}).get("suspect_min_prev", 20))
    suspect_ratio = float((cfg.get("runtime") or {}).get("suspect_shrink_ratio", 0.10))
    suspect_debug = bool((cfg.get("runtime") or {}).get("suspect_debug", True))

    if drop_guard:
        prev_cp_src = prev_checkpoint(prev_state, src, feature, src_inst)
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

        prev_cp_dst = prev_checkpoint(prev_state, dst, feature, dst_inst)
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
        src_suspect = False
        dst_suspect = False
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

    dst_sem = _index_semantics(dst_ops, feature)
    src_sem = _index_semantics(src_ops, feature)

    dst_full = (dict(prev_dst) | dict(dst_cur)) if dst_sem == "delta" else dict(eff_dst)
    src_idx = (dict(prev_src) | dict(src_cur)) if src_sem == "delta" else dict(eff_src)

    remove_mode = str(fcfg.get("remove_mode") or (sync_cfg.get("one_way_remove_mode") or "source_deletes")).strip().lower()
    if remove_mode not in ("source_deletes", "mirror"):
        remove_mode = "source_deletes"

    mirror_removes: list[dict[str, Any]] = []
    if feature == "ratings":
        src_idx  = _ratings_filter_index(src_idx,  fcfg)
        dst_full = _ratings_filter_index(dst_full, fcfg)
        if manual_adds:
            src_idx = _merge_manual_adds(src_idx, manual_adds)
        adds, mirror_removes = diff_ratings(src_idx, dst_full)

    else:
        if manual_adds:
            src_idx = _merge_manual_adds(src_idx, manual_adds)

        bucket_sec = _history_bucket_sec(src, dst, feature)
        if bucket_sec and int(bucket_sec) > 1:
            b = int(bucket_sec)

            def _tsb_from_key(k: str) -> int | None:
                ts = _history_ts_from_key(k)
                return None if ts is None else _bucket_ts(int(ts), b)

            dst_tok_ts: set[tuple[str, int]] = set()
            for dk, dv in (dst_full or {}).items():
                if not isinstance(dv, Mapping):
                    continue
                tsb = _tsb_from_key(str(dk))
                if tsb is None:
                    continue
                for tok in _typed_tokens(dv):
                    if tok:
                        dst_tok_ts.add((tok, tsb))

            src_tok_ts: set[tuple[str, int]] = set()
            for sk, sv in (src_idx or {}).items():
                if not isinstance(sv, Mapping):
                    continue
                tsb = _tsb_from_key(str(sk))
                if tsb is None:
                    continue
                for tok in _typed_tokens(sv):
                    if tok:
                        src_tok_ts.add((tok, tsb))

            adds = []
            for sk, sv in (src_idx or {}).items():
                if not isinstance(sv, Mapping):
                    continue
                tsb = _tsb_from_key(str(sk))
                if tsb is None:

                    if str(sk) not in (dst_full or {}):
                        adds.append(_minimal(sv))
                    continue

                toks = _typed_tokens(sv)
                if toks and any((tok, tsb) in dst_tok_ts for tok in toks):
                    continue
                adds.append(_minimal(sv))

            mirror_removes = []
            for dk, dv in (dst_full or {}).items():
                if not isinstance(dv, Mapping):
                    continue
                tsb = _tsb_from_key(str(dk))
                if tsb is None:
                    if str(dk) not in (src_idx or {}):
                        mirror_removes.append(_minimal(dv))
                    continue

                toks = _typed_tokens(dv)
                if toks and any((tok, tsb) in src_tok_ts for tok in toks):
                    continue
                mirror_removes.append(_minimal(dv))
        else:
            adds, mirror_removes = diff(src_idx, dst_full)

    src_alias = _alias_index(src_idx)
    dst_alias = _alias_index(dst_full)

    if adds:
        if feature not in ("ratings", "history"):
            adds = [it for it in adds if not _present(dst_full, dst_alias, it)]
        elif feature == "history":
            pruned: list[dict[str, Any]] = []
            for it in adds:
                ck = _ck(it) or ""
                if ck and _history_ts_from_key(ck) is None and _present(dst_full, dst_alias, it):
                    continue
                pruned.append(it)
            adds = pruned

    removes: list[dict[str, Any]] = []
    if allow_removes:
        if remove_mode == "mirror":
            removes = list(mirror_removes or [])
            if feature == "ratings":
                if removes:
                    removes = [it for it in removes if not _present(src_idx, src_alias, it)]
                    try:
                        removes = [it for it in removes if _ck(it) in prev_dst]
                    except Exception:
                        pass
            else:
                if removes:
                    removes = [it for it in removes if not _present(src_idx, src_alias, it)]
                    try:
                        removes = [it for it in removes if _ck(it) in prev_dst]
                    except Exception:
                        pass
        else:
            if include_observed and not src_suspect and src_sem != "delta" and prev_src:
                src_obs = dict(src_cur or {})
                if manual_adds:
                    src_obs = _merge_manual_adds(src_obs, manual_adds)
                src_obs_alias = _alias_index(src_obs)

                observed: list[Mapping[str, Any]] = []
                for it in (prev_src or {}).values():
                    if not isinstance(it, Mapping):
                        continue
                    if not _present(src_obs, src_obs_alias, it):
                        observed.append(it)

                if observed:
                    seen: set[str] = set()
                    for it in observed:
                        dv = _find_in_idx(dst_full, dst_alias, it)
                        if not dv:
                            continue
                        rk = _ck(dv) or _ck(it)
                        if rk and rk in seen:
                            continue
                        if rk:
                            seen.add(rk)
                        removes.append(_minimal(dv))

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
        # Ratings use upsert semantics
        if feature == "ratings":
            updates = [it for it in adds if _present(dst_full, dst_alias, it)]
            fresh = [it for it in adds if not _present(dst_full, dst_alias, it)]
            if fresh:
                fresh, _blocked = guard.filter_adds(fresh, _ck, _minimal, emit, ctx.state_store, pair_key)
            adds = updates + fresh
        else:
            adds, _blocked = guard.filter_adds(adds, _ck, _minimal, emit, ctx.state_store, pair_key)

    attempted_keys: list[str] = []
    key2item: dict[str, Any] = {}
    seen: set[str] = set()

    for it in adds:
        k = _ck(it)
        if not k:
            continue
        if k not in seen:
            attempted_keys.append(k)
            seen.add(k)
        key2item.setdefault(k, _minimal(it))

    add_attempted_raw = len(adds)
    add_attempted_unique = len(attempted_keys)
    add_attempted_duplicate_keys = max(0, add_attempted_raw - add_attempted_unique)
    if add_attempted_duplicate_keys:
        dbg(
            "apply:add:deduped",
            dst=dst,
            feature=feature,
            attempted_raw=add_attempted_raw,
            attempted_unique=add_attempted_unique,
            duplicate_canonical_keys=add_attempted_duplicate_keys,
        )

    added_effective = 0
    added_provider_reported = 0
    res_add: dict[str, Any] = {
        "attempted": 0,
        "confirmed": 0,
        "skipped": 0,
        "skipped_exact": 0,
        "skipped_inferred": 0,
        "skipped_reported": 0,
        "skip_basis": "provider_keys",
        "unresolved": 0,
        "errors": 0,
    }
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
                chunk_size=effective_chunk_size(ctx, dst),
                chunk_pause_ms=_pause_for(dst),
            )
            unresolved_after = set(load_unresolved_keys(dst, feature, cross_features=True) or [])
            res_add = {
                "attempted": int((add_res or {}).get("attempted", 0)),
                "confirmed": int((add_res or {}).get("confirmed", (add_res or {}).get("count", 0)) or 0),
                "skipped": int((add_res or {}).get("skipped", 0)),
                "skipped_exact": int((add_res or {}).get("skipped_exact", 0) or 0),
                "skipped_inferred": int((add_res or {}).get("skipped_inferred", 0) or 0),
                "skipped_reported": int((add_res or {}).get("skipped_reported", 0) or 0),
                "skip_basis": str((add_res or {}).get("skip_basis") or "provider_keys"),
                "unresolved": int((add_res or {}).get("unresolved", 0)),
                "errors": int((add_res or {}).get("errors", 0)),
            }
            prov_unresolved_keys_raw = (add_res or {}).get("unresolved_keys")
            prov_unresolved_keys: list[str] = (
                [str(x) for x in prov_unresolved_keys_raw if x] if isinstance(prov_unresolved_keys_raw, list) else []
            )
            prov_unresolved_set: set[str] = set(prov_unresolved_keys)

            new_unresolved = (unresolved_after - unresolved_before) | (prov_unresolved_set - unresolved_before)
            unresolved_new_total += len(new_unresolved)
            still_unresolved = set(attempted_keys) & (unresolved_after | prov_unresolved_set)
            
            prov_confirmed_keys_raw = (add_res or {}).get("confirmed_keys")
            prov_skipped_keys_raw = (add_res or {}).get("skipped_keys")

            prov_confirmed_keys: list[str] = (
                [str(x) for x in prov_confirmed_keys_raw if x] if isinstance(prov_confirmed_keys_raw, list) else []
            )
            prov_skipped_keys: list[str] = (
                [str(x) for x in prov_skipped_keys_raw if x] if isinstance(prov_skipped_keys_raw, list) else []
            )

            skipped_keys_set: set[str] = set(prov_skipped_keys)

            have_exact_keys = bool(prov_confirmed_keys)
            if have_exact_keys:
                attempted_set = set(attempted_keys)
                confirmed_keys = [k for k in prov_confirmed_keys if k in attempted_set]
            else:
                confirmed_keys = [k for k in attempted_keys if k not in still_unresolved]

           
            if verify_after_write and _apply_verify_after_write_supported(dst_ops):
                try:
                    unresolved_again = set(load_unresolved_keys(dst, feature, cross_features=True) or [])
                    confirmed_keys = [k for k in confirmed_keys if k not in unresolved_again]
                except Exception:
                    pass
            
            prov_confirmed = int((add_res or {}).get("confirmed", (add_res or {}).get("count", 0)) or 0)
            added_provider_reported = prov_confirmed
            if have_exact_keys:
                prov_confirmed = min(prov_confirmed or len(confirmed_keys), len(confirmed_keys))
            
            if not dry_run_flag and not new_unresolved and prov_confirmed == 0 and adds:
                try:
                    record_unresolved(dst, feature, adds, hint="apply:add:no_confirmations_fallback")
                    new_unresolved = set(attempted_keys)
                    unresolved_new_total += len(new_unresolved)
                    still_unresolved = set(attempted_keys)
                    confirmed_keys = []
                    skipped_keys_set = set()
                    have_exact_keys = False
                except Exception:
                    pass
            
            ambiguous_partial = (not have_exact_keys) and bool(res_add.get("skipped")) and prov_confirmed and (prov_confirmed < len(confirmed_keys))
            strict_pessimist = (not have_exact_keys) and (not verify_after_write) and bool(still_unresolved)
            if strict_pessimist or ambiguous_partial:
                added_effective = 0
            else:
                added_effective = len(confirmed_keys) if (verify_after_write or have_exact_keys) else min(prov_confirmed, len(confirmed_keys))
            
            if added_effective != prov_confirmed and not have_exact_keys:
                dbg("apply:add:corrected", dst=dst, feature=feature,
                    provider_count=prov_confirmed, effective=added_effective,
                    newly_unresolved=len(new_unresolved))

            if int(res_add.get("skipped_inferred", 0) or 0):
                dbg(
                    "apply:add:skip_inference",
                    dst=dst,
                    feature=feature,
                    skipped=int(res_add.get("skipped", 0) or 0),
                    skipped_exact=int(res_add.get("skipped_exact", 0) or 0),
                    skipped_inferred=int(res_add.get("skipped_inferred", 0) or 0),
                    skip_basis=str(res_add.get("skip_basis") or "provider_keys"),
                )
            
            success_keys = confirmed_keys if (verify_after_write or have_exact_keys) else confirmed_keys[:added_effective]
            failed_keys = [k for k in attempted_keys if k not in set(success_keys) and k not in skipped_keys_set]
            try:
                if failed_keys and not ambiguous_partial:
                    record_attempts(dst, feature, failed_keys, reason="apply:add:failed", op="add",
                        pair=pair_key, cfg=cfg)
                    failed_items = [key2item[k] for k in failed_keys if k in key2item]
                    if failed_items:
                        record_unresolved(dst, feature, failed_items, hint="apply:add:failed")
            
                if success_keys and not ambiguous_partial:
                    record_success(dst, feature, success_keys, pair=pair_key, cfg=cfg)
                if use_phantoms and guard and success_keys and not ambiguous_partial:
                    guard.record_success(success_keys)
            except Exception:
                pass
            if success_keys and not dry_run_flag:
                for k in success_keys:
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
                chunk_size=effective_chunk_size(ctx, dst),
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
                        ks.setdefault(f"{feature}:{pair_key}|{tok}", now)

                    ctx.state_store.save_tomb(t)
                    emit("debug", msg="tombstones.marked", feature=feature,
                         added=len(removed_tokens), scope="pair")
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

        def _ensure_pf(pmap, prov, inst, feat):
            pprov = pmap.setdefault(prov, {})
            if inst != "default":
                insts = pprov.setdefault("instances", {})
                pprov = insts.setdefault(inst, {})
            return pprov.setdefault(feat, {"baseline": {"items": {}}, "checkpoint": None})

        def _commit_baseline(pmap, prov, inst, feat, items):
            pf = _ensure_pf(pmap, prov, inst, feat)
            pkey = _PROVIDER_KEY_MAP.get(str(prov or "").upper(), str(prov or "").strip().lower())

            kept: dict[str, Any] = {}
            for k, v in (items or {}).items():
                if not isinstance(v, Mapping):
                    continue

                if v.get("_cw_persist") is False or v.get("_cw_transient") is True or v.get("_cw_skip_persist") is True:
                    continue

                pobj = v.get(pkey)
                if isinstance(pobj, Mapping) and pobj.get("ignored") is True:
                    continue

                mv = _minimal(v)
                if inst != "default":
                    mv["_cw_instance"] = inst
                kept[str(k)] = mv

            pf["baseline"] = {"items": kept}


        def _commit_checkpoint(pmap, prov, inst, feat, chk):
            if not chk:
                return
            pf = _ensure_pf(pmap, prov, inst, feat)
            pf["checkpoint"] = chk

        _commit_baseline(provs_block, src, src_inst, feature, src_idx)
        _commit_baseline(provs_block, dst, dst_inst, feature, dst_full)
        _commit_checkpoint(provs_block, src, src_inst, feature, now_cp_src)
        _commit_checkpoint(provs_block, dst, dst_inst, feature, now_cp_dst)

        import time as _t
        st["last_sync_epoch"] = int(_t.time())
        ctx.state_store.save_state(st)
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