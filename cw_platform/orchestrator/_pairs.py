from __future__ import annotations
from typing import Any, Dict, Mapping, Optional, Set

# --- compact helpers / drivers ----------------------------------------------
from ._pairs_utils import (
    inject_ctx_into_provider,
    health_status,
    health_feature_ok,
    supports_feature,
)
from ._pairs_metrics import ApiMetrics, persist_api_totals
from ._pairs_oneway import run_one_way_feature
from ._pairs_twoway import run_two_way_feature

# blackbox maintenance (safe fallbacks if module shape differs)
try:
    from ._blackbox import prune_once as _bb_prune_once  # type: ignore
except Exception:
    try:
        from ._blackbox import prune_blackbox as _bb_prune  # type: ignore

        def _bb_prune_once(cfg: Mapping[str, Any]) -> None:
            try:
                bb_cfg = ((cfg.get("sync") or {}).get("blackbox") or {})
                cooldown = int(bb_cfg.get("cooldown_days", 30))
                _bb_prune(cooldown_days=cooldown)
            except Exception:
                pass
    except Exception:  # last resort: no-op
        def _bb_prune_once(cfg: Mapping[str, Any]) -> None:
            return


def _collect_health_for_run(ctx) -> Dict[str, Any]:
    """Ask each provider for health once; make their modules see ctx."""
    emit = ctx.emit
    provs = ctx.providers or {}
    # providers present in configured pairs (enabled only)
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

    health_map: Dict[str, Any] = {}
    for name in sorted(N for N in needed):
        ops = provs.get(name)
        if not ops:
            continue

        # Make provider packages (and their commons) see orchestrator context.
        inject_ctx_into_provider(ops, ctx)

        # Health: try signature with emit first; fall back to simpler one.
        try:
            h = ops.health(ctx.config, emit=emit) or {}
        except TypeError:
            h = ops.health(ctx.config) or {}
        except Exception as e:
            h = {"ok": False, "status": "down", "details": f"health exception: {e}"}

        health_map[name] = h

        # Human-friendly health event for the UI.
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

        # If health includes API statuses, synthesize hits so totals aren’t zero.
        try:
            api_map = (h.get("api") or {})
            for ep, meta in (api_map.items() if isinstance(api_map, Mapping) else []):
                st = (meta or {}).get("status")
                if st is not None:
                    emit("api:hit", provider=name, endpoint=f"health:{ep}", status=st)
        except Exception:
            pass

    return health_map


def _feature_list_for_pair(pair: Mapping[str, Any]) -> list[str]:
    """
    Resolve which features to run:
    - If pair.feature is set and != 'multi' → run just that.
    - Else if pair.features map is present → run the enabled ones.
    - Else → fall back to common defaults.
    """
    selector = str(pair.get("feature") or "").strip().lower()
    fmap = dict(pair.get("features") or {})
    if selector and selector != "multi":
        return [selector]
    if fmap:
        out = []
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


def run_pairs(ctx) -> Dict[str, Any]:
    cfg = ctx.config or {}
    sync_cfg = (cfg.get("sync") or {})
    emit_info = ctx.emit_info
    emit_dbg = ctx.dbg

    # --- wrap emit with API metrics aggregation (human + compact) ------------
    metrics = ApiMetrics(ctx.emit)
    ctx.emit = metrics.emit  # from now on, all emits get counted
    emit = ctx.emit

    # Tombstone housekeeping at start (the helper logs details itself).
    try:
        ttl_days = int(sync_cfg.get("tombstone_ttl_days", 30))
        ctx.tomb_prune(max(1, ttl_days) * 24 * 3600)
    except Exception:
        pass

    # One-shot health pass (+ inject ctx into provider modules so they can log/use it)
    health_map = _collect_health_for_run(ctx)

    # Kickoff marker for the run
    emit(
        "run:start",
        dry_run=bool(ctx.dry_run or sync_cfg.get("dry_run", False)),
        mode="v3",
    )

    added_total = 0
    removed_total = 0
    unresolved_total = 0  # << accumulate unresolved from driver results

    pairs = [p for p in (cfg.get("pairs") or []) if p.get("enabled", True)]
    provs = ctx.providers or {}

    # Track which features actually ran this cycle (for end-of-run cleanups).
    features_ran: Set[str] = set()

    for i, pair in enumerate(pairs, 1):
        src = str(pair.get("source") or "").upper().strip()
        dst = str(pair.get("target") or "").upper().strip()
        feat_map = dict(pair.get("features") or {})
        mode = str(pair.get("mode") or "one-way").lower().strip()

        # Detect whether we’ll fall back to defaults (for clearer logs).
        selector_raw = str(pair.get("feature") or "").strip().lower()
        used_defaults = (not selector_raw or selector_raw == "multi") and not feat_map

        features = _feature_list_for_pair(pair)
        if used_defaults:
            # Friendly, explicit notice so users aren’t surprised by watchlist/others running.
            emit_info(f"No per-feature map set for {src}→{dst}; running defaults: {features}")

        emit("run:pair", i=i, n=len(pairs), src=src, dst=dst, mode=mode, features=features)

        sops = provs.get(src)
        dops = provs.get(dst)
        if not sops or not dops:
            emit_info(f"[!] Missing provider ops for {src}→{dst}")
            continue

        # Ensure both providers still see ctx even if not part of health loop.
        inject_ctx_into_provider(sops, ctx)
        inject_ctx_into_provider(dops, ctx)

        # Auth guard at pair level (saves noisy inner checks).
        ss = health_status(health_map.get(src) or {})
        sd = health_status(health_map.get(dst) or {})
        if ss == "auth_failed" or sd == "auth_failed":
            emit("pair:skip", src=src, dst=dst, reason="auth_failed", src_status=ss, dst_status=sd)
            continue

        for feature in features:
            fcfg = feat_map.get(feature) or {}
            # Local enable knob still respected when features were auto-expanded.
            if isinstance(fcfg, dict) and not bool(fcfg.get("enable", True)):
                continue

            # Capability + per-feature health gate (fast no-op when not supported).
            if (not supports_feature(sops, feature)) or (not supports_feature(dops, feature)) \
               or (not health_feature_ok(health_map.get(src), feature)) \
               or (not health_feature_ok(health_map.get(dst), feature)):
                emit(
                    "feature:unsupported",
                    src=src,
                    dst=dst,
                    feature=feature,
                    src_supported=supports_feature(sops, feature) and health_feature_ok(health_map.get(src), feature),
                    dst_supported=supports_feature(dops, feature) and health_feature_ok(health_map.get(dst), feature),
                )
                continue

            # If we made it here, we are actually running this feature.
            features_ran.add(feature)

            if mode == "two-way":
                res = run_two_way_feature(ctx, src, dst, feature=feature, fcfg=fcfg, health_map=health_map)
                added_total   += int(res.get("adds_to_A", 0)) + int(res.get("adds_to_B", 0))
                removed_total += int(res.get("rem_from_A", 0)) + int(res.get("rem_from_B", 0))
                unresolved_total += (
                    int(res.get("unresolved", 0))
                    + int(res.get("unresolved_to_A", 0))
                    + int(res.get("unresolved_to_B", 0))
                )
            else:
                res = run_one_way_feature(ctx, src, dst, feature=feature, fcfg=fcfg, health_map=health_map)
                added_total   += int(res.get("added", 0))
                removed_total += int(res.get("removed", 0))
                unresolved_total += int(res.get("unresolved", 0))

    # --- end-of-run, before final marker -------------------------------------

    # Gate watchlist-only cleanups behind "did watchlist run at all?"
    if "watchlist" in features_ran:
        try:
            from ._tombstones import cascade_removals  # late import; light touch
            cascade_removals(ctx.state_store, emit_dbg, feature="watchlist", removed_keys=[])
        except Exception:
            pass

        try:
            if hasattr(ctx, "hidefile_clear"):
                ctx.hidefile_clear("watchlist")
                emit("debug", msg="hidefile.cleared", feature="watchlist", scope="end-of-run")
        except Exception:
            pass

    # Feed summary + rate warnings to the UI/stats.
    try:
        # Keep existing signature; unresolved will be carried in the overview payload below.
        ctx.stats.record_summary(added=added_total, removed=removed_total)
        ctx.emit_rate_warnings()
    except Exception:
        pass

    # HTTP overview (if the stats backend can produce it).
    try:
        overview = ctx.stats.http_overview()
        if overview:
            emit("http:overview", overview=overview)
    except Exception:
        pass

    # Persist a small "last run" record and update the wall overview.
    try:
        import time as _t

        now = int(_t.time())
        ctx.state_store.save_last(
            {
                "started_at": now,
                "finished_at": now,
                "result": {"added": added_total, "removed": removed_total, "unresolved": unresolved_total},
            }
        )

        # Pull current overview from stats; keep it light.
        try:
            wall = ctx.stats.overview() or {}
        except Exception:
            wall = {}
        wall["now"] = int(wall.get("now") or 0)  # keep shape stable
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

    # Emit + persist API totals from the aggregator and reset metrics state.
    try:
        totals = metrics.totals()
        emit("api:totals", totals=totals)
        persist_api_totals(ctx, totals)
    except Exception:
        pass

    # Blackbox maintenance window (cooldown-based prune), best-effort.
    try:
        _bb_prune_once(cfg)
    except Exception:
        pass

    # Restore original emit so callers don’t inherit our wrapper.
    try:
        ctx.emit = metrics._emit_original
    except Exception:
        pass

    emit(
        "run:done",
        added=added_total,
        removed=removed_total,
        unresolved=unresolved_total,  # << new field
        pairs=len(pairs),
        mode="v3",
    )
    return {
        "ok": True,
        "added": added_total,
        "removed": removed_total,
        "unresolved": unresolved_total,
        "pairs": len(pairs),
    }
