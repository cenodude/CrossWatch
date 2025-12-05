# cw_platform/orchestration/_pairs.py
# Main orchestration logic for data pair synchronization.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Mapping
from typing import Any

from ._pairs_utils import (
    inject_ctx_into_provider,
    health_status,
    health_feature_ok,
    supports_feature,
)
from ._pairs_metrics import ApiMetrics, persist_api_totals
from ._pairs_oneway import run_one_way_feature
from ._pairs_twoway import run_two_way_feature

try:
    from ._blackbox import prune_once as _bb_prune_once  # type: ignore[attr-defined]
except Exception:
    try:
        from ._blackbox import prune_blackbox as _bb_prune  # type: ignore[attr-defined]

        def _bb_prune_once(cfg: Mapping[str, Any]) -> None:
            try:
                bb_cfg = ((cfg.get("sync") or {}).get("blackbox") or {})
                cooldown = int(bb_cfg.get("cooldown_days", 30))
                _bb_prune(cooldown_days=cooldown)
            except Exception:
                pass
    except Exception:  # last resort: no-op
        def _bb_prune_once(cfg: Mapping[str, Any]) -> None:  # type: ignore[unused-arg]
            return


def _collect_health_for_run(ctx) -> dict[str, Any]:
    emit = ctx.emit
    provs = ctx.providers or {}
    needed: set[str] = set()

    for p in (ctx.config.get("pairs") or []):
        if not p.get("enabled", True):
            continue
        s = str(p.get("source") or "").upper().strip()
        t = str(p.get("target") or "").upper().strip()
        if s:
            needed.add(s)
        if t:
            needed.add(t)

    health_map: dict[str, Any] = {}
    for name in sorted(n for n in needed):
        ops = provs.get(name)
        if not ops:
            continue

        inject_ctx_into_provider(ops, ctx)
        try:
            h = ops.health(ctx.config, emit=emit) or {}
        except TypeError:
            h = ops.health(ctx.config) or {}
        except Exception as e:
            h = {"ok": False, "status": "down", "details": f"health exception: {e}"}

        health_map[name] = h
        emit(
            "health",
            provider=name,
            status=str(h.get("status") or "unknown").lower(),
            ok=bool(h.get("ok", True)),
            latency_ms=h.get("latency_ms"),
            details=h.get("details"),
            features=(h.get("features") or {}),
            api=(h.get("api") or {}),
        )

        try:
            api_map = (h.get("api") or {})
            if isinstance(api_map, Mapping):
                for ep, meta in api_map.items():
                    st = (meta or {}).get("status")
                    if st is not None:
                        emit("api:hit", provider=name, endpoint=f"health:{ep}", status=st)
        except Exception:
            pass

    return health_map


def _feature_list_for_pair(pair: Mapping[str, Any]) -> list[str]:
    selector = str(pair.get("feature") or "").strip().lower()
    fmap = dict(pair.get("features") or {})
    if selector and selector != "multi":
        return [selector]
    if fmap:
        out: list[str] = []
        for fname, fcfg in fmap.items():
            if isinstance(fcfg, dict):
                if bool(fcfg.get("enable", True)):
                    out.append(str(fname))
            elif isinstance(fcfg, bool):
                if fcfg:
                    out.append(str(fname))
            else:
                out.append(str(fname))
        return out
    return ["watchlist", "ratings", "history", "playlists"]


def run_pairs(ctx) -> dict[str, Any]:
    cfg: dict[str, Any] = ctx.config or {}
    sync_cfg = (cfg.get("sync") or {})
    emit_info = ctx.emit_info
    emit_dbg = ctx.dbg

    metrics = ApiMetrics(ctx.emit)
    ctx.emit = metrics.emit
    emit = ctx.emit

    try:
        ttl_days = int(sync_cfg.get("tombstone_ttl_days", 30))
        ctx.tomb_prune(max(1, ttl_days) * 24 * 3600)
    except Exception:
        pass

    health_map = _collect_health_for_run(ctx)

    emit(
        "run:start",
        dry_run=bool(ctx.dry_run or sync_cfg.get("dry_run", False)),
        mode="v3",
    )

    added_total = 0
    removed_total = 0
    unresolved_total = 0
    skipped_total = 0
    errors_total = 0

    pairs = [p for p in (cfg.get("pairs") or []) if p.get("enabled", True)]
    provs = ctx.providers or {}

    features_ran: set[str] = set()

    for i, pair in enumerate(pairs, 1):
        src = str(pair.get("source") or "").upper().strip()
        dst = str(pair.get("target") or "").upper().strip()
        feat_map = dict(pair.get("features") or {})
        mode = str(pair.get("mode") or "one-way").lower().strip()

        selector_raw = str(pair.get("feature") or "").strip().lower()
        used_defaults = (not selector_raw or selector_raw == "multi") and not feat_map

        features = _feature_list_for_pair(pair)
        if not features:
            emit(
                "run:pair:skip",
                src=src,
                dst=dst,
                mode=mode,
                reason="no-features",
            )
            continue

        if used_defaults:
            emit_info(f"No per-feature map set for {src}→{dst}; running defaults: {features}")

        emit(
            "run:pair",
            i=i,
            n=len(pairs),
            src=src,
            dst=dst,
            mode=mode,
            features=features,
        )

        sops = provs.get(src)
        dops = provs.get(dst)
        if not sops or not dops:
            emit_info(f"[!] Missing provider ops for {src}→{dst}")
            continue

        inject_ctx_into_provider(sops, ctx)
        inject_ctx_into_provider(dops, ctx)

        ss = health_status(health_map.get(src) or {})
        sd = health_status(health_map.get(dst) or {})
        if ss == "auth_failed" or sd == "auth_failed":
            emit("pair:skip", src=src, dst=dst, reason="auth_failed", src_status=ss, dst_status=sd)
            continue

        for feature in features:
            fcfg = feat_map.get(feature) or {}
            if isinstance(fcfg, dict) and not bool(fcfg.get("enable", True)):
                continue

            if (not supports_feature(sops, feature)) or (not supports_feature(dops, feature)) \
               or (not health_feature_ok(health_map.get(src), feature)) \
               or (not health_feature_ok(health_map.get(dst), feature)):
                emit(
                    "feature:unsupported",
                    src=src,
                    dst=dst,
                    feature=feature,
                    src_supported=supports_feature(sops, feature)
                    and health_feature_ok(health_map.get(src), feature),
                    dst_supported=supports_feature(dops, feature)
                    and health_feature_ok(health_map.get(dst), feature),
                )
                continue

            features_ran.add(feature)

            if mode == "two-way":
                res = run_two_way_feature(ctx, src, dst, feature=feature, fcfg=fcfg, health_map=health_map)
                added_total += int(res.get("adds_to_A", 0)) + int(res.get("adds_to_B", 0))
                removed_total += int(res.get("rem_from_A", 0)) + int(res.get("rem_from_B", 0))
                unresolved_total += (
                    int(res.get("unresolved", 0))
                    + int(res.get("unresolved_to_A", 0))
                    + int(res.get("unresolved_to_B", 0))
                )
                skipped_total += (
                    int(res.get("skipped", 0))
                    + int(res.get("skipped_to_A", 0))
                    + int(res.get("skipped_to_B", 0))
                )
                errors_total += (
                    int(res.get("errors", 0))
                    + int(res.get("errors_to_A", 0))
                    + int(res.get("errors_to_B", 0))
                )
            else:
                res = run_one_way_feature(ctx, src, dst, feature=feature, fcfg=fcfg, health_map=health_map)
                added_total += int(res.get("added", 0))
                removed_total += int(res.get("removed", 0))
                unresolved_total += int(res.get("unresolved", 0))
                skipped_total += int(res.get("skipped", 0))
                errors_total += int(res.get("errors", 0))

    if "watchlist" in features_ran:
        try:
            from ._tombstones import cascade_removals
            cascade_removals(ctx.state_store, emit_dbg, feature="watchlist", removed_keys=[])
        except Exception:
            pass

        try:
            if hasattr(ctx, "hidefile_clear"):
                ctx.hidefile_clear("watchlist")
        except Exception:
            pass

    try:
        ctx.stats.record_summary(added=added_total, removed=removed_total)
        ctx.emit_rate_warnings()
    except Exception:
        pass

    try:
        overview = ctx.stats.http_overview()
        if overview:
            emit("http:overview", overview=overview)
    except Exception:
        pass

    try:
        import time as _t

        now = int(_t.time())
        ctx.state_store.save_last(
            {
                "started_at": now,
                "finished_at": now,
                "result": {
                    "added": added_total,
                    "removed": removed_total,
                    "skipped": skipped_total,
                    "unresolved": unresolved_total,
                    "errors": errors_total,
                },
            }
        )

        try:
            wall = ctx.stats.overview() or {}
        except Exception:
            wall = {}
        wall["now"] = int(wall.get("now") or 0)
        wall["unresolved"] = int(unresolved_total)

        st = ctx.state_store.load_state() or {}
        st["wall"] = wall
        st["last_sync_epoch"] = now
        ctx.state_store.save_state(st)

        emit("stats:overview", overview=wall)
        emit(
            "debug",
            msg="state.persisted",
            providers=len((ctx.providers or {})),
            wall=(len(wall) if isinstance(wall, dict) else 0),
        )
    except Exception:
        pass

    try:
        totals = metrics.totals()
        emit("api:totals", totals=totals)
        persist_api_totals(ctx, totals)
    except Exception:
        pass

    try:
        _bb_prune_once(cfg)
    except Exception:
        pass

    # restore original emitter
    try:
        ctx.emit = metrics._orig_emit  # type: ignore[attr-defined]
    except Exception:
        pass

    emit(
        "run:done",
        added=added_total,
        removed=removed_total,
        skipped=skipped_total,
        unresolved=unresolved_total,
        errors=errors_total,
        pairs=len(pairs),
        mode="v3",
    )
    return {
        "ok": True,
        "added": added_total,
        "removed": removed_total,
        "skipped": skipped_total,
        "unresolved": unresolved_total,
        "errors": errors_total,
        "pairs": len(pairs),
    }