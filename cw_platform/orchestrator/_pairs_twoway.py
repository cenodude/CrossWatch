# cw_platform/orchestration/_pairs_twoway.py
# Two-way synchronization logic for data pairs.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Mapping
from typing import Any

import os
import re

try:
    from ._pairs_oneway import (
        _history_bucket_sec as _hist_bucket_sec,
        _history_ts_from_key as _hist_ts_from_key,
        _bucket_ts as _hist_bucket_ts,
    )
except Exception:  # pragma: no cover
    _HIST_RE = re.compile(r"^(?P<base>.+?)@(?P<ts>\d+)(?P<rest>.*)$")

    def _hist_bucket_sec(a: str, b: str, feature: str) -> int:
        if str(feature) != "history":
            return 0
        au = str(a or "").upper()
        bu = str(b or "").upper()
        return 60 if (au == "TRAKT" or bu == "TRAKT") else 0

    def _hist_ts_from_key(key: str) -> int | None:
        m = _HIST_RE.match(str(key))
        if not m:
            return None
        try:
            return int(m.group("ts"))
        except Exception:
            return None

    def _hist_bucket_ts(ts: int, bucket_sec: int) -> int:
        b2 = int(bucket_sec or 0)
        if b2 <= 1:
            return int(ts)
        return (int(ts) // b2) * b2

from ..provider_instances import normalize_instance_id

from ._planner import diff_ratings
try:
    from ._pairs_oneway import _ratings_filter_index as _rate_filter
except Exception:
    def _rate_filter(idx: dict[str, Any], fcfg: Mapping[str, Any]) -> dict[str, Any]:
        return idx

from ..id_map import minimal as _minimal, canonical_key as _ck
from ._snapshots import (
    build_snapshots_for_feature,
    coerce_suspect_snapshot,
    module_checkpoint,
    prev_checkpoint,
)
from ._applier import apply_add, apply_remove
from ._chunking import effective_chunk_size
from ._tombstones import keys_for_feature
from ._unresolved import load_unresolved_keys, record_unresolved
from ._phantoms import PhantomGuard  # type: ignore[attr-defined]

from ._pairs_blocklist import apply_blocklist
from ._pairs_massdelete import maybe_block_mass_delete as _maybe_block_massdelete
from ._pairs_utils import (
    supports_feature as _supports_feature,
    resolve_flags as _resolve_flags,
    health_status as _health_status,
    health_feature_ok as _health_feature_ok,
    rate_remaining as _rate_remaining,
    apply_verify_after_write_supported as _apply_verify_after_write_supported,
    manual_policy as _manual_policy,
    merge_manual_adds as _merge_manual_adds,
    filter_manual_block as _filter_manual_block,
)

try:
    from ._blackbox import load_blackbox_keys, record_attempts, record_success  # type: ignore
except Exception:
    def load_blackbox_keys(dst: str, feature: str, pair: str | None = None) -> set[str]:
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

def _minimal_keep_rating(it: Mapping[str, Any]) -> dict[str, Any]:
    out = _minimal(it)
    try:
        if "rating" in it:
            out["rating"] = it.get("rating")
        ra = (it.get("rated_at") or it.get("ratedAt") or it.get("user_rated_at") or "")
        ra = ra.strip() if isinstance(ra, str) else ""
        if ra:
            out["rated_at"] = ra
    except Exception:
        pass
    return out

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
    include_observed_override: bool | None = None,
) -> dict[str, Any]:
    import time as _t

    cfg, emit, info, dbg = ctx.config, ctx.emit, ctx.emit_info, ctx.dbg
    src_inst = normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    dst_inst = normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))
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

    include_observed_cfg = bool(sync_cfg.get("include_observed_deletes", True))
    base_obs = include_observed_cfg if include_observed_override is None else bool(include_observed_override)
    include_obs_A = bool(base_obs)
    include_obs_B = bool(base_obs)
    drop_guard = bool(sync_cfg.get("drop_guard", False))
    allow_mass_delete = bool(sync_cfg.get("allow_mass_delete", True))
    verify_after_write = bool(sync_cfg.get("verify_after_write", False))
    dry_run_flag = bool(ctx.dry_run or sync_cfg.get("dry_run", False))

    Ha = health_map.get(f"{a}#{src_inst}") or health_map.get(a) or {}
    Hb = health_map.get(f"{b}#{dst_inst}") or health_map.get(b) or {}
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
        include_obs_A = False
        include_obs_B = False

    def _cap_obsdel(ops) -> bool | None:
        try:
            v = (ops.capabilities() or {}).get("observed_deletes")
            return None if v is None else bool(v)
        except Exception:
            return None

    try:
        capA = _cap_obsdel(aops)
        capB = _cap_obsdel(bops)
        if (capA is False) or (capB is False):
            emit("debug", msg="observed.deletions.partial",
                 feature=feature, a=a, b=b, a_enabled=include_obs_A, b_enabled=include_obs_B,
                 reason="provider_capability")
    except Exception:
        pass

    if (not _supports_feature(aops, feature)) or (not _supports_feature(bops, feature)) \
       or (not _health_feature_ok(Ha, feature)) or (not _health_feature_ok(Hb, feature)):
        emit("feature:unsupported", a=a, b=b, feature=feature,
             a_supported=_supports_feature(aops, feature) and _health_feature_ok(Ha, feature),
             b_supported=_supports_feature(bops, feature) and _health_feature_ok(Hb, feature))
        return {"ok": True, "adds_to_A": 0, "adds_to_B": 0, "rem_from_A": 0, "rem_from_B": 0}

    def _pause_for(pname: str) -> int:
        base = int(getattr(ctx, "apply_chunk_pause_ms", 0) or 0)
        inst = src_inst if pname == a else (dst_inst if pname == b else "default")
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

    emit("two:start", a=a, b=b, feature=feature, removals=allow_removals)

    pair_providers = {a: aops, b: bops}

    snaps = build_snapshots_for_feature(
        feature=feature, config=cfg, providers=pair_providers,
        snap_cache=ctx.snap_cache, snap_ttl_sec=ctx.snap_ttl_sec,
        dbg=dbg, emit_info=info,
    )
    A_cur = snaps.get(a) or {}
    B_cur = snaps.get(b) or {}

    prev_state = getattr(ctx, "_stable_prev_state", None)
    if not prev_state:
        prev_state = ctx.state_store.load_state() or {}
        try:
            setattr(ctx, "_stable_prev_state", prev_state)
        except Exception:
            pass

    manual_adds_A, manual_blocks_A = _manual_policy(prev_state, a, feature)
    manual_adds_B, manual_blocks_B = _manual_policy(prev_state, b, feature)

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

    prevA = _prev_items(prev_provs, a, src_inst, feature)
    prevB = _prev_items(prev_provs, b, dst_inst, feature)

    prev_cp_A = prev_checkpoint(prev_state, a, feature, src_inst)
    prev_cp_B = prev_checkpoint(prev_state, b, feature, dst_inst)
    now_cp_A = module_checkpoint(aops, cfg, feature)
    now_cp_B = module_checkpoint(bops, cfg, feature)

    if drop_guard:
        A_eff_guard, A_suspect, A_reason = coerce_suspect_snapshot(
            provider=a, ops=aops, prev_idx=prevA, cur_idx=A_cur, feature=feature,
            suspect_min_prev=int((cfg.get("runtime") or {}).get("suspect_min_prev", 20)),
            suspect_shrink_ratio=float((cfg.get("runtime") or {}).get("suspect_shrink_ratio", 0.10)),
            suspect_debug=bool((cfg.get("runtime") or {}).get("suspect_debug", True)),
            emit=emit, emit_info=info, prev_cp=prev_cp_A, now_cp=now_cp_A,
        )
        if A_suspect:
            dbg("snapshot.guard", provider=a, feature=feature, reason=A_reason)
        B_eff_guard, B_suspect, B_reason = coerce_suspect_snapshot(
            provider=b, ops=bops, prev_idx=prevB, cur_idx=B_cur, feature=feature,
            suspect_min_prev=int((cfg.get("runtime") or {}).get("suspect_min_prev", 20)),
            suspect_shrink_ratio=float((cfg.get("runtime") or {}).get("suspect_shrink_ratio", 0.10)),
            suspect_debug=bool((cfg.get("runtime") or {}).get("suspect_debug", True)),
            emit=emit, emit_info=info, prev_cp=prev_cp_B, now_cp=now_cp_B,
        )
        if B_suspect:
            dbg("snapshot.guard", provider=b, feature=feature, reason=B_reason)
    else:
        emit("drop_guard:skipped", a=a, b=b, feature=feature)
        A_eff_guard, A_suspect = dict(A_cur), False
        B_eff_guard, B_suspect = dict(B_cur), False

    a_sem = _index_semantics(aops, feature)
    b_sem = _index_semantics(bops, feature)

    A_eff = (dict(prevA) | dict(A_cur)) if a_sem == "delta" else dict(A_eff_guard)
    B_eff = (dict(prevB) | dict(B_cur)) if b_sem == "delta" else dict(B_eff_guard)

    libs_A = _effective_library_whitelist(cfg, a, feature, fcfg)
    libs_B = _effective_library_whitelist(cfg, b, feature, fcfg)

    allow_unknown_A = (str(a).upper() == "PLEX" and feature == "history")
    allow_unknown_B = (str(b).upper() == "PLEX" and feature == "history")

    if libs_A:
        prevA = _filter_index_by_libraries(prevA, libs_A, allow_unknown=allow_unknown_A)
        A_cur = _filter_index_by_libraries(A_cur, libs_A, allow_unknown=allow_unknown_A)
        A_eff = _filter_index_by_libraries(A_eff, libs_A, allow_unknown=allow_unknown_A)

    if libs_B:
        prevB = _filter_index_by_libraries(prevB, libs_B, allow_unknown=allow_unknown_B)
        B_cur = _filter_index_by_libraries(B_cur, libs_B, allow_unknown=allow_unknown_B)
        B_eff = _filter_index_by_libraries(B_eff, libs_B, allow_unknown=allow_unknown_B)

    now = int(_t.time())
    tomb_ttl_days = int((cfg.get("sync") or {}).get("tombstone_ttl_days", 30))
    tomb_ttl_secs = max(1, tomb_ttl_days) * 24 * 3600
    pair_key = "-".join(sorted([a, b]))
    tomb_map = dict(
        keys_for_feature(
            ctx.state_store, feature, pair=pair_key
        ) or {}
    )
    tomb = {k for k, ts in tomb_map.items() if not isinstance(ts, int) or (now - int(ts)) <= tomb_ttl_secs}

    bootstrap = (not prevA) and (not prevB) and not tomb
    obsA: set[str] = set()
    obsB: set[str] = set()
    if not bootstrap:
        if include_obs_A and not A_suspect:
            obsA = {k for k in prevA.keys() if k not in (A_cur or {})}
        if include_obs_B and not B_suspect:
            obsB = {k for k in prevB.keys() if k not in (B_cur or {})}
        newly = (obsA | obsB) - tomb

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
                ks.setdefault(f"{feature}:{pair_key}|{tok}", now)

            ctx.state_store.save_tomb(t)

        emit("debug", msg="observed.deletions", a=len(obsA), b=len(obsB), tomb=len(tomb),
             suppressed_on_A=bool(A_suspect), suppressed_on_B=bool(B_suspect))
    elif not (include_obs_A or include_obs_B):
        emit("debug", msg="observed.deletions.disabled", feature=feature, pair=pair_key)

    shrinkA = {k for k in prevA.keys() if k not in (A_cur or {})}
    shrinkB = {k for k in prevB.keys() if k not in (B_cur or {})}

    for k in list(obsA):
        A_eff.pop(k, None)
    for k in list(obsB):
        B_eff.pop(k, None)

    if manual_adds_A:
        A_eff = _merge_manual_adds(A_eff, manual_adds_A)
    if manual_adds_B:
        B_eff = _merge_manual_adds(B_eff, manual_adds_B)

    def _typed_tokens(it: Mapping[str, Any]) -> set[str]:
        typ = str(it.get("type") or "").strip().lower()
        if typ in ("episode", "season"):
            ids_raw = it.get("show_ids") or it.get("ids") or {}
        else:
            ids_raw = it.get("ids") or {}
        ids = ids_raw if isinstance(ids_raw, Mapping) else {}
        toks: set[str] = set()

        if typ == "episode":
            try:
                s = int(it.get("season") or 0)
                e = int(it.get("episode") or 0)
            except Exception:
                s, e = 0, 0
            if s > 0 and e > 0:
                frag = f"#s{s:02d}e{e:02d}"
                for k, v in ids.items():
                    if v is None or str(v) == "":
                        continue
                    toks.add(f"{str(k).lower()}:{str(v).lower()}{frag}")

        elif typ == "season":
            try:
                s = int(it.get("season") or 0)
            except Exception:
                s = 0
            if s > 0:
                frag = f"#season:{s}"
                for k, v in ids.items():
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

    def _tokens(it: Mapping[str, Any]) -> set[str]:
        toks: set[str] = set()
        try:
            ck = _ck(it)
            if ck:
                toks.add(ck)
            toks |= _typed_tokens(it)
        except Exception:
            pass
        return toks

    A_alias = _alias_index(A_eff)
    B_alias = _alias_index(B_eff)
    prevA_alias = _alias_index(prevA)
    prevB_alias = _alias_index(prevB)

    tombX = set(tomb)
    try:
        for _tok in list(tomb):
            _ckA = A_alias.get(_tok)
            if _ckA:
                tombX.add(_ckA)
            _ckB = B_alias.get(_tok)
            if _ckB:
                tombX.add(_ckB)
            _ckPA = prevA_alias.get(_tok)
            if _ckPA:
                tombX.add(_ckPA)
            _ckPB = prevB_alias.get(_tok)
            if _ckPB:
                tombX.add(_ckPB)
    except Exception:
        tombX = set(tomb)

    def _prev_had(prev_idx: dict[str, Any], prev_alias: dict[str, str], it: Mapping[str, Any]) -> bool:
        ck = _ck(it)
        if ck in prev_idx:
            return True
        try:
            for tok in _typed_tokens(it):
                if tok in prev_alias:
                    return True
        except Exception:
            pass
        return False

    add_to_A: list[dict[str, Any]] = []
    add_to_B: list[dict[str, Any]] = []
    rem_from_A: list[dict[str, Any]] = []
    rem_from_B: list[dict[str, Any]] = []

    if feature == "ratings":
        A_f = _rate_filter(A_eff, fcfg)
        B_f = _rate_filter(B_eff, fcfg)

        up_B, unrate_B = diff_ratings(A_f, B_f, propagate_timestamp_updates=False)
        up_A, unrate_A = diff_ratings(B_f, A_f, propagate_timestamp_updates=False)

        def _rated_epoch(it: Mapping[str, Any]) -> int | None:
            from datetime import datetime
            for key in ("rated_at", "ratedAt", "user_rated_at", "userRatedAt"):
                v = it.get(key)
                if isinstance(v, str) and v.strip():
                    try:
                        return int(datetime.fromisoformat(v.strip().replace("Z", "+00:00")).timestamp())
                    except Exception:
                        return None
            return None

        bi = sync_cfg.get("bidirectional") or {}
        sot = (bi.get("source_of_truth") or bi.get("sourceOfTruth") or "").strip().upper()
        prefer = sot if sot in (a, b) else a

        upB = {k: it for it in up_B if (k := _ck(it))}
        upA = {k: it for it in up_A if (k := _ck(it))}
        unA = {k: it for it in unrate_A if (k := _ck(it))}
        unB = {k: it for it in unrate_B if (k := _ck(it))}

        addA: list[dict[str, Any]] = []
        addB: list[dict[str, Any]] = []
        remA: list[dict[str, Any]] = []
        remB: list[dict[str, Any]] = []

        for k in (set(upA) | set(upB) | set(unA) | set(unB)):
            a_it = A_f.get(k) or upB.get(k) or {}
            b_it = B_f.get(k) or upA.get(k) or {}

            if k in upA and k in upB:
                ta = _rated_epoch(a_it)
                tb = _rated_epoch(b_it)
                if ta is not None and tb is not None and ta != tb:
                    win = a if ta > tb else b
                else:
                    win = prefer
                if win == a:
                    addB.append(_minimal_keep_rating(upB[k]))
                else:
                    addA.append(_minimal_keep_rating(upA[k]))
                continue

            if k in upB and k in unA:
                if allow_removals and ((_tokens(a_it) & tombX) or (k in tombX)):
                    remA.append(_minimal(unA[k]))
                else:
                    addB.append(_minimal_keep_rating(upB[k]))
                continue

            if k in upA and k in unB:
                if allow_removals and ((_tokens(b_it) & tombX) or (k in tombX)):
                    remB.append(_minimal(unB[k]))
                else:
                    addA.append(_minimal_keep_rating(upA[k]))
                continue

            if k in upB:
                addB.append(_minimal_keep_rating(upB[k]))
            elif k in upA:
                addA.append(_minimal_keep_rating(upA[k]))
            elif allow_removals and k in unA:
                remA.append(_minimal(unA[k]))
            elif allow_removals and k in unB:
                remB.append(_minimal(unB[k]))

        add_to_A = addA if allow_adds else []
        add_to_B = addB if allow_adds else []
        if allow_removals:
            rem_from_A.extend(remA)
            rem_from_B.extend(remB)

    else:
        bucket_sec = _hist_bucket_sec(a, b, feature)
        if bucket_sec and int(bucket_sec) > 1:
            bsec = int(bucket_sec)

            def _tsb_from_key(k: str) -> int | None:
                ts = _hist_ts_from_key(k)
                return None if ts is None else _hist_bucket_ts(int(ts), bsec)

            A_tok_ts: set[tuple[str, int]] = set()
            for ak, av in (A_eff or {}).items():
                if not isinstance(av, Mapping):
                    continue
                tsb = _tsb_from_key(str(ak))
                if tsb is None:
                    continue
                for tok in _typed_tokens(av):
                    if tok:
                        A_tok_ts.add((tok, tsb))

            B_tok_ts: set[tuple[str, int]] = set()
            for bk, bv in (B_eff or {}).items():
                if not isinstance(bv, Mapping):
                    continue
                tsb = _tsb_from_key(str(bk))
                if tsb is None:
                    continue
                for tok in _typed_tokens(bv):
                    if tok:
                        B_tok_ts.add((tok, tsb))

            for ak, v in (A_eff or {}).items():
                if not isinstance(v, Mapping):
                    continue
                tsb = _tsb_from_key(str(ak))
                if tsb is not None:
                    toks = _typed_tokens(v)
                    if toks and any((tok, tsb) in B_tok_ts for tok in toks):
                        continue
                else:
                    if _present(B_eff, B_alias, v):
                        continue

                if allow_removals and ((_tokens(v) & tombX) or (_ck(v) in tombX) or (_ck(v) in obsB) or (_ck(v) in shrinkB)) and (_prev_had(prevB, prevB_alias, v) or (_tokens(v) & tombX) or (_ck(v) in tombX)):
                    rem_from_A.append(_minimal(v))
                else:
                    add_to_B.append(_minimal(v))

            for bk, v in (B_eff or {}).items():
                if not isinstance(v, Mapping):
                    continue
                tsb = _tsb_from_key(str(bk))
                if tsb is not None:
                    toks = _typed_tokens(v)
                    if toks and any((tok, tsb) in A_tok_ts for tok in toks):
                        continue
                else:
                    if _present(A_eff, A_alias, v):
                        continue

                if allow_removals and ((_tokens(v) & tombX) or (_ck(v) in tombX) or (_ck(v) in obsA) or (_ck(v) in shrinkA)) and (_prev_had(prevA, prevA_alias, v) or (_tokens(v) & tombX) or (_ck(v) in tombX)):
                    rem_from_B.append(_minimal(v))
                else:
                    add_to_A.append(_minimal(v))
        else:
            for _k, v in A_eff.items():
                if _present(B_eff, B_alias, v):
                    continue
                if allow_removals and ((_tokens(v) & tombX) or (_ck(v) in tombX) or (_ck(v) in obsB) or (_ck(v) in shrinkB)) and (_prev_had(prevB, prevB_alias, v) or (_tokens(v) & tombX) or (_ck(v) in tombX)):
                    rem_from_A.append(_minimal(v))
                else:
                    add_to_B.append(_minimal(v))
            for _k, v in B_eff.items():
                if _present(A_eff, A_alias, v):
                    continue
                if allow_removals and ((_tokens(v) & tombX) or (_ck(v) in tombX) or (_ck(v) in obsA) or (_ck(v) in shrinkA)) and (_prev_had(prevA, prevA_alias, v) or (_tokens(v) & tombX) or (_ck(v) in tombX)):
                    rem_from_B.append(_minimal(v))
                else:
                    add_to_A.append(_minimal(v))
    if not allow_adds:
        add_to_A.clear()
        add_to_B.clear()
    if not allow_removals:
        rem_from_A.clear()
        rem_from_B.clear()

    if bootstrap and allow_removals:
        rem_from_A.clear()
        rem_from_B.clear()
        dbg("bootstrap.no-delete", a=a, b=b)

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

    if feature != "watchlist":
        add_to_A = apply_blocklist(ctx.state_store, add_to_A, dst=a, feature=feature, pair_key=pair_key, emit=emit)
        add_to_B = apply_blocklist(ctx.state_store, add_to_B, dst=b, feature=feature, pair_key=pair_key, emit=emit)

    manual_blocked = 0
    if manual_blocks_A:
        pre_add, pre_rem = len(add_to_A), len(rem_from_A)
        add_to_A = _filter_manual_block(add_to_A, manual_blocks_A)
        rem_from_A = _filter_manual_block(rem_from_A, manual_blocks_A)
        blk = (pre_add - len(add_to_A)) + (pre_rem - len(rem_from_A))
        if blk:
            emit("debug", msg="blocked.counts", feature=feature, dst=a, pair=f"{a}-{b}",
                 blocked_manual=int(blk), blocked_total=int(blk))
        manual_blocked += blk

    if manual_blocks_B:
        pre_add, pre_rem = len(add_to_B), len(rem_from_B)
        add_to_B = _filter_manual_block(add_to_B, manual_blocks_B)
        rem_from_B = _filter_manual_block(rem_from_B, manual_blocks_B)
        blk = (pre_add - len(add_to_B)) + (pre_rem - len(rem_from_B))
        if blk:
            emit("debug", msg="blocked.counts", feature=feature, dst=b, pair=f"{a}-{b}",
                 blocked_manual=int(blk), blocked_total=int(blk))
        manual_blocked += blk

    if manual_blocked:
        try:
            ctx.stats_manual_blocked = int(getattr(ctx, "stats_manual_blocked", 0) or 0) + int(manual_blocked)
        except Exception:
            pass

    bb = ((cfg or {}).get("blackbox") if isinstance(cfg, dict) else getattr(cfg, "blackbox", {})) or {}
    use_phantoms = bool(bb.get("enabled") and bb.get("block_adds", True))
    bb_ttl_days = int(bb.get("cooldown_days") or 0) or None

    guardA = PhantomGuard(src=b, dst=a, feature=feature, ttl_days=bb_ttl_days, enabled=use_phantoms)
    guardB = PhantomGuard(src=a, dst=b, feature=feature, ttl_days=bb_ttl_days, enabled=use_phantoms)

    if use_phantoms and add_to_A:
        # Ratings use upsert semantics (add == set/update). Do not phantom-block updates.
        if feature == "ratings":
            upd = [it for it in add_to_A if _present(A_eff, A_alias, it)]
            fresh = [it for it in add_to_A if not _present(A_eff, A_alias, it)]
            if fresh:
                fresh, _ = guardA.filter_adds(fresh, _ck, _minimal, emit, ctx.state_store, pair_key)
            add_to_A = upd + fresh
        else:
            add_to_A, _ = guardA.filter_adds(add_to_A, _ck, _minimal, emit, ctx.state_store, pair_key)
    if use_phantoms and add_to_B:
        # Ratings use upsert semantics
        if feature == "ratings":
            upd = [it for it in add_to_B if _present(B_eff, B_alias, it)]
            fresh = [it for it in add_to_B if not _present(B_eff, B_alias, it)]
            if fresh:
                fresh, _ = guardB.filter_adds(fresh, _ck, _minimal, emit, ctx.state_store, pair_key)
            add_to_B = upd + fresh
        else:
            add_to_B, _ = guardB.filter_adds(add_to_B, _ck, _minimal, emit, ctx.state_store, pair_key)

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

    resA_rem: dict[str, Any] = {"ok": True, "count": 0}
    resB_rem: dict[str, Any] = {"ok": True, "count": 0}
    remA_keys = [_ck(_minimal(it)) for it in (rem_from_A or []) if _ck(_minimal(it))]
    remB_keys = [_ck(_minimal(it)) for it in (rem_from_B or []) if _ck(_minimal(it))]

    def _mark_tombs(items: list[dict[str, Any]]) -> None:
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
                ks.setdefault(f"{feature}:{pair_key}|{tok}", now_ts)

            ctx.state_store.save_tomb(tomb)
            emit("debug", msg="tombstones.marked", feature=feature,
                 added=len(tokens), scope="pair")
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
                chunk_size=effective_chunk_size(ctx, a), chunk_pause_ms=_pause_for(a),
            )
            prov_count_A = _confirmed(resA_rem)
            if prov_count_A and not dry_run_flag:
                removed_now = 0
                for k in remA_keys:
                    if k in A_eff:
                        A_eff.pop(k, None)
                        removed_now += 1
                        if removed_now >= prov_count_A:
                            break
                _mark_tombs(rem_from_A)
                _bust_snapshot(a)

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
                chunk_size=effective_chunk_size(ctx, b), chunk_pause_ms=_pause_for(b),
            )
            prov_count_B = _confirmed(resB_rem)
            if prov_count_B and not dry_run_flag:
                removed_now = 0
                for k in remB_keys:
                    if k in B_eff:
                        B_eff.pop(k, None)
                        removed_now += 1
                        if removed_now >= prov_count_B:
                            break
                _mark_tombs(rem_from_B)
                _bust_snapshot(b)

            emit("two:apply:remove:B:done", dst=b, feature=feature,
                 count=_confirmed(resB_rem),
                 attempted=int(resB_rem.get("attempted", 0)),
                 removed=_confirmed(resB_rem),
                 skipped=int(resB_rem.get("skipped", 0)),
                 unresolved=int(resB_rem.get("unresolved", 0)),
                 errors=int(resB_rem.get("errors", 0)),
                 result=resB_rem)

    resA_add: dict[str, Any] = {"ok": True, "count": 0}
    resB_add: dict[str, Any] = {"ok": True, "count": 0}
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
            _ = set(load_blackbox_keys(a, feature, pair=pair_key) or [])
            attempted_A: list[str] = []
            seen_A: set[str] = set()
            k2i_A: dict[str, Any] = {}
            for it in add_to_A:
                k = _ck(_minimal(it))
                if not k or k in seen_A:
                    continue
                seen_A.add(k)
                attempted_A.append(k)
                k2i_A[k] = _minimal(it)
            
            resA_add = apply_add(
                dst_ops=aops, cfg=cfg, dst_name=a, feature=feature, items=add_to_A,
                dry_run=dry_run_flag, emit=emit, dbg=dbg,
                chunk_size=effective_chunk_size(ctx, a), chunk_pause_ms=_pause_for(a),
            )
            unresolved_after_A = set(load_unresolved_keys(a, feature, cross_features=True) or [])
            prov_unresolved_keys_A_raw = (resA_add or {}).get("unresolved_keys")
            prov_unresolved_keys_A: list[str] = (
                [str(x) for x in prov_unresolved_keys_A_raw if x] if isinstance(prov_unresolved_keys_A_raw, list) else []
            )
            prov_unresolved_set_A: set[str] = set(prov_unresolved_keys_A)

            new_unresolved_A = (unresolved_after_A - unresolved_before_A) | (prov_unresolved_set_A - unresolved_before_A)
            unresolved_new_A_total += len(new_unresolved_A)
            still_unresolved_A = set(attempted_A) & (unresolved_after_A | prov_unresolved_set_A)
            
            prov_confirmed_keys_A_raw = (resA_add or {}).get("confirmed_keys")
            prov_skipped_keys_A_raw = (resA_add or {}).get("skipped_keys")

            prov_confirmed_keys_A: list[str] = (
                [str(x) for x in prov_confirmed_keys_A_raw if x] if isinstance(prov_confirmed_keys_A_raw, list) else []
            )
            prov_skipped_keys_A: list[str] = (
                [str(x) for x in prov_skipped_keys_A_raw if x] if isinstance(prov_skipped_keys_A_raw, list) else []
            )

            skipped_keys_A: set[str] = set(prov_skipped_keys_A)

            have_exact_keys_A = bool(prov_confirmed_keys_A)
            if have_exact_keys_A:
                attempted_set_A = set(attempted_A)
                confirmed_A = [k for k in prov_confirmed_keys_A if k in attempted_set_A]
            else:
                confirmed_A = [k for k in attempted_A if k not in still_unresolved_A]

        
            prov_count_A = _confirmed(resA_add)
            if have_exact_keys_A:
                prov_count_A = min(prov_count_A or len(confirmed_A), len(confirmed_A))
            
            ambiguous_partial_A = False
            if verify_after_write and _apply_verify_after_write_supported(aops):
                try:
                    unresolved_again = set(load_unresolved_keys(a, feature, cross_features=True) or [])
                    confirmed_A = [k for k in confirmed_A if k not in unresolved_again]
                except Exception:
                    pass
                eff_add_A = len(confirmed_A)
            else:
                ambiguous_partial_A = (not have_exact_keys_A) and bool((resA_add or {}).get("skipped")) and prov_count_A and (prov_count_A < len(confirmed_A))
                eff_add_A = 0 if still_unresolved_A or ambiguous_partial_A else min(prov_count_A, len(confirmed_A))
            
            if eff_add_A != prov_count_A and not have_exact_keys_A:
                dbg("two:apply:add:corrected", dst=a, feature=feature,
                    provider_count=prov_count_A, effective=eff_add_A, newly_unresolved=len(new_unresolved_A))
            
            success_A = confirmed_A if (verify_after_write or have_exact_keys_A) else confirmed_A[:eff_add_A]
            try:
                failed_A = [k for k in attempted_A if k not in set(success_A) and k not in skipped_keys_A]
                if failed_A and not ambiguous_partial_A:
                    record_attempts(a, feature, failed_A,
                        reason="two:apply:add:failed", op="add",
                        pair=pair_key, cfg=cfg)
                    failed_items_A = [k2i_A[k] for k in failed_A if k in k2i_A]
                    if failed_items_A:
                        record_unresolved(a, feature, failed_items_A, hint="apply:add:failed")
            
                if success_A:
                    record_success(a, feature, success_A, pair=pair_key, cfg=cfg)
                if use_phantoms and 'guardA' in locals() and guardA and success_A:
                    guardA.record_success(set(success_A))
            except Exception:
                pass
            
            if success_A and not dry_run_flag:
                for k in success_A:
                    v = k2i_A.get(k)
                    if v:
                        A_eff[k] = v
                _bust_snapshot(a)
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
            _ = set(load_blackbox_keys(b, feature, pair=pair_key) or [])
            attempted_B: list[str] = []
            seen_B: set[str] = set()
            k2i_B: dict[str, Any] = {}
            for it in add_to_B:
                k = _ck(_minimal(it))
                if not k or k in seen_B:
                    continue
                seen_B.add(k)
                attempted_B.append(k)
                k2i_B[k] = _minimal(it)
            
            resB_add = apply_add(
                dst_ops=bops, cfg=cfg, dst_name=b, feature=feature, items=add_to_B,
                dry_run=dry_run_flag, emit=emit, dbg=dbg,
                chunk_size=effective_chunk_size(ctx, b), chunk_pause_ms=_pause_for(b),
            )
            unresolved_after_B = set(load_unresolved_keys(b, feature, cross_features=True) or [])
            prov_unresolved_keys_B_raw = (resB_add or {}).get("unresolved_keys")
            prov_unresolved_keys_B: list[str] = (
                [str(x) for x in prov_unresolved_keys_B_raw if x] if isinstance(prov_unresolved_keys_B_raw, list) else []
            )
            prov_unresolved_set_B: set[str] = set(prov_unresolved_keys_B)

            new_unresolved_B = (unresolved_after_B - unresolved_before_B) | (prov_unresolved_set_B - unresolved_before_B)
            unresolved_new_B_total += len(new_unresolved_B)
            still_unresolved_B = set(attempted_B) & (unresolved_after_B | prov_unresolved_set_B)
            
            prov_confirmed_keys_B_raw = (resB_add or {}).get("confirmed_keys")
            prov_skipped_keys_B_raw = (resB_add or {}).get("skipped_keys")

            prov_confirmed_keys_B: list[str] = (
                [str(x) for x in prov_confirmed_keys_B_raw if x] if isinstance(prov_confirmed_keys_B_raw, list) else []
            )
            prov_skipped_keys_B: list[str] = (
                [str(x) for x in prov_skipped_keys_B_raw if x] if isinstance(prov_skipped_keys_B_raw, list) else []
            )

            skipped_keys_B: set[str] = set(prov_skipped_keys_B)

            have_exact_keys_B = bool(prov_confirmed_keys_B)
            if have_exact_keys_B:
                attempted_set_B = set(attempted_B)
                confirmed_B = [k for k in prov_confirmed_keys_B if k in attempted_set_B]
            else:
                confirmed_B = [k for k in attempted_B if k not in still_unresolved_B]

        
            prov_count_B = _confirmed(resB_add)
            if have_exact_keys_B:
                prov_count_B = min(prov_count_B or len(confirmed_B), len(confirmed_B))
            
            ambiguous_partial_B = False
            if verify_after_write and _apply_verify_after_write_supported(bops):
                try:
                    unresolved_again = set(load_unresolved_keys(b, feature, cross_features=True) or [])
                    confirmed_B = [k for k in confirmed_B if k not in unresolved_again]
                except Exception:
                    pass
                eff_add_B = len(confirmed_B)
            else:
                ambiguous_partial_B = (not have_exact_keys_B) and bool((resB_add or {}).get("skipped")) and prov_count_B and (prov_count_B < len(confirmed_B))
                eff_add_B = 0 if still_unresolved_B or ambiguous_partial_B else min(prov_count_B, len(confirmed_B))
            
            if eff_add_B != prov_count_B and not have_exact_keys_B:
                dbg("two:apply:add:corrected", dst=b, feature=feature,
                    provider_count=prov_count_B, effective=eff_add_B, newly_unresolved=len(new_unresolved_B))
            
            success_B = confirmed_B if (verify_after_write or have_exact_keys_B) else confirmed_B[:eff_add_B]
            try:
                failed_B = [k for k in attempted_B if k not in set(success_B) and k not in skipped_keys_B]
                if failed_B and not ambiguous_partial_B:
                    record_attempts(b, feature, failed_B,
                        reason="two:apply:add:failed", op="add",
                        pair=pair_key, cfg=cfg)
                    failed_items_B = [k2i_B[k] for k in failed_B if k in k2i_B]
                    if failed_items_B:
                        record_unresolved(b, feature, failed_items_B, hint="apply:add:failed")
            
                if success_B:
                    record_success(b, feature, success_B, pair=pair_key, cfg=cfg)
                if use_phantoms and 'guardB' in locals() and guardB and success_B:
                    guardB.record_success(set(success_B))
            except Exception:
                pass
            
            if success_B and not dry_run_flag:
                for k in success_B:
                    v = k2i_B.get(k)
                    if v:
                        B_eff[k] = v
                _bust_snapshot(b)
            emit("two:apply:add:B:done", dst=b, feature=feature,
                 count=_confirmed(resB_add),
                 attempted=int(resB_add.get("attempted", 0)),
                 added=_confirmed(resB_add),
                 skipped=int(resB_add.get("skipped", 0)),
                 unresolved=int(resB_add.get("unresolved", 0)),
                 errors=int(resB_add.get("errors", 0)),
                 result=resB_add)

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

        _commit_baseline(provs_block, a, src_inst, feature, A_eff)
        _commit_baseline(provs_block, b, dst_inst, feature, B_eff)
        _commit_checkpoint(provs_block, a, src_inst, feature, now_cp_A)
        _commit_checkpoint(provs_block, b, dst_inst, feature, now_cp_B)

        st["last_sync_epoch"] = int(_t.time())
        ctx.state_store.save_state(st)
    except Exception:
        pass

    emit("two:done", a=a, b=b, feature=feature,
         adds_to_A=eff_add_A, adds_to_B=eff_add_B,
         rem_from_A=_confirmed(resA_rem),
         rem_from_B=_confirmed(resB_rem))

    skipped_total = int(resA_add.get("skipped", 0)) + int(resB_add.get("skipped", 0)) + \
                    int(resA_rem.get("skipped", 0)) + int(resB_rem.get("skipped", 0))
    errors_total = int(resA_add.get("errors", 0)) + int(resB_add.get("errors", 0)) + \
                   int(resA_rem.get("errors", 0)) + int(resB_rem.get("errors", 0))
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
) -> dict[str, Any]:

    emit = ctx.emit

    src_inst = normalize_instance_id(os.getenv("CW_PAIR_SRC_INSTANCE"))
    dst_inst = normalize_instance_id(os.getenv("CW_PAIR_DST_INSTANCE"))

    src_u = str(src).upper(); dst_u = str(dst).upper()
    Hs = health_map.get(f"{src_u}#{src_inst}") or health_map.get(src_u) or {}
    Hd = health_map.get(f"{dst_u}#{dst_inst}") or health_map.get(dst_u) or {}

    include_obs_override = None
    if _health_status(Hs) == "down" or _health_status(Hd) == "down":
        include_obs_override = False

    emit("feature:start", src=str(src).upper(), dst=str(dst).upper(), feature=feature)
    res = _two_way_sync(
        ctx, str(src).upper(), str(dst).upper(),
        feature=feature, fcfg=fcfg, health_map=health_map,
        include_observed_override=include_obs_override,
    )
    emit("feature:done", src=str(src).upper(), dst=str(dst).upper(), feature=feature)
    return res
