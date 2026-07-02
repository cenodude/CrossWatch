# cw_platform/orchestration/_pairs.py
# Main orchestration logic for data pair synchronization.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Mapping
from typing import Any
from contextlib import contextmanager
import copy
import os

from ._pairs_utils import (
    inject_ctx_into_provider,
    health_status,
    health_feature_ok,
    supports_feature,
)
from ._pairs_metrics import ApiMetrics, persist_api_totals
from ..provider_instances import build_pair_config_view, build_provider_config_view, normalize_instance_id
from ._pairs_oneway import run_one_way_feature
from ._pairs_twoway import run_two_way_feature
from ..value_coercion import coerce_bool

def _deep_merge_provider_overrides(dst: dict[str, Any], src: Mapping[str, Any]) -> None:
    for k, v in (src or {}).items():
        kk = str(k)
        if kk == "strict_id_matching":
            dst["strict_id_matching"] = coerce_bool(v)
            continue
        cur = dst.get(kk)
        if isinstance(cur, dict) and isinstance(v, Mapping):
            _deep_merge_provider_overrides(cur, v)
        else:
            dst[kk] = v


def _config_with_pair_progress_options(
    cfg: dict[str, Any],
    fcfg: Mapping[str, Any],
    providers: tuple[str, str],
) -> dict[str, Any]:
    lib_cfg = fcfg.get("libraries")
    overrides: dict[str, list[str]] = {}
    provider_keys: list[str] = []
    for provider in providers:
        name = str(provider or "").upper().strip()
        if name not in {"PLEX", "EMBY", "JELLYFIN"}:
            continue
        provider_key = name.lower()
        if provider_key not in provider_keys:
            provider_keys.append(provider_key)
        if isinstance(lib_cfg, Mapping):
            raw = lib_cfg.get(name) or lib_cfg.get(provider_key)
            if isinstance(raw, (list, tuple)):
                values = [str(value).strip() for value in raw if str(value).strip()]
                if values:
                    overrides[provider_key] = values

    has_replay = "replay_enabled" in fcfg
    has_tolerance = "timestamp_tolerance_seconds" in fcfg
    if not provider_keys or (not overrides and not has_replay and not has_tolerance):
        return cfg

    out = copy.deepcopy(cfg)
    for provider_key in provider_keys:
        provider_cfg = out.setdefault(provider_key, {})
        if not isinstance(provider_cfg, dict):
            provider_cfg = {}
            out[provider_key] = provider_cfg
        progress_cfg = provider_cfg.setdefault("progress", {})
        if not isinstance(progress_cfg, dict):
            progress_cfg = {}
            provider_cfg["progress"] = progress_cfg
        if provider_key in overrides:
            progress_cfg["libraries"] = overrides[provider_key]
        if has_replay:
            progress_cfg["replay_enabled"] = coerce_bool(fcfg.get("replay_enabled", False))
        if has_tolerance:
            try:
                tolerance = int(fcfg.get("timestamp_tolerance_seconds", 30))
            except (TypeError, ValueError):
                tolerance = 30
            progress_cfg["timestamp_tolerance_seconds"] = max(0, min(300, tolerance))
    return out



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

    cfg: Mapping[str, Any] = ctx.config or {}
    needed: set[tuple[str, str]] = set()

    for p in (cfg.get("pairs") or []):
        if not p.get("enabled", True):
            continue
        s = str(p.get("source") or "").upper().strip()
        t = str(p.get("target") or "").upper().strip()
        si = normalize_instance_id(p.get("source_instance"))
        ti = normalize_instance_id(p.get("target_instance"))
        if s:
            needed.add((s, si))
        if t:
            needed.add((t, ti))

    health_map: dict[str, Any] = {}

    @contextmanager
    def _health_env(provider: str, instance_id: str):
        suffix = f"{str(provider).upper()}#{normalize_instance_id(instance_id)}"
        key = f"health:{suffix}"
        new = {
            "CW_PAIR_KEY": key,
            "CW_PAIR_SCOPE": key,
            "CW_SYNC_PAIR": key,
            "CW_PAIR": key,
            "CW_PAIR_SRC": str(provider).upper(),
            "CW_PAIR_DST": str(provider).upper(),
            "CW_PAIR_SRC_INSTANCE": normalize_instance_id(instance_id),
            "CW_PAIR_DST_INSTANCE": normalize_instance_id(instance_id),
            "CW_PAIR_MODE": "health",
            "CW_PAIR_FEATURE": "health",
        }
        old = {k: os.environ.get(k) for k in new.keys()}
        try:
            for k, v in new.items():
                os.environ[k] = str(v)
            yield
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    for name, inst in sorted(needed):
        ops = provs.get(name)
        if not ops:
            continue

        cfg_view = build_provider_config_view(cfg, name, inst)

        with _health_env(name, inst):
            inject_ctx_into_provider(ops, ctx)
            try:
                h = ops.health(cfg_view, emit=emit) or {}
            except TypeError:
                try:
                    h = ops.health(cfg_view) or {}
                except Exception as e:
                    h = {"ok": False, "status": "down", "details": f"health exception: {e}"}
            except Exception as e:
                h = {"ok": False, "status": "down", "details": f"health exception: {e}"}

        health_key = f"{str(name).upper()}#{normalize_instance_id(inst)}"
        health_map[health_key] = h
        emit(
            "health",
            provider=name,
            instance=normalize_instance_id(inst),
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
                if coerce_bool(fcfg.get("enable", True), True):
                    out.append(str(fname))
            elif isinstance(fcfg, (bool, str, int, float)):
                if coerce_bool(fcfg):
                    out.append(str(fname))
            else:
                out.append(str(fname))

        if "history" in out and "watchlist" in out:
            hi, wi = out.index("history"), out.index("watchlist")
            if hi > wi:
                out.remove("history")
                out.insert(wi, "history")
        return out
    return ["history", "watchlist", "ratings", "progress", "playlists"]



def _pair_scope_key(pair: Mapping[str, Any], *, i: int, src: str, dst: str, mode: str) -> str:
    mode_norm = str(mode or "one-way").strip().lower()

    si = normalize_instance_id(pair.get("source_instance"))
    ti = normalize_instance_id(pair.get("target_instance"))
    a = f"{str(src).upper()}#{si}"
    b = f"{str(dst).upper()}#{ti}"

    if mode_norm == "two-way":
        base = "-".join(sorted([a, b]))
        mode_norm = "two-way"
    else:
        base = f"{a}-{b}"
        mode_norm = "one-way"

    raw_id = pair.get("id") or pair.get("pair_id") or pair.get("name") or pair.get("label") or ""
    pid = str(raw_id).strip() if raw_id else ""
    if not pid:
        pid = str(i)

    return f"{mode_norm}:{base}:{pid}"


@contextmanager
def _pair_env(pair: Mapping[str, Any], *, i: int, src: str, dst: str, mode: str, feature: str):
    key = _pair_scope_key(pair, i=i, src=src, dst=dst, mode=mode)
    src_inst = normalize_instance_id(pair.get("source_instance"))
    dst_inst = normalize_instance_id(pair.get("target_instance"))
    new = {
        "CW_PAIR_KEY": key,
        "CW_PAIR_SCOPE": key,
        "CW_SYNC_PAIR": key,
        "CW_PAIR": key,
        "CW_PAIR_SRC": str(src).upper(),
        "CW_PAIR_DST": str(dst).upper(),
        "CW_PAIR_SRC_INSTANCE": src_inst,
        "CW_PAIR_DST_INSTANCE": dst_inst,
        "CW_PAIR_MODE": str(mode or "").strip().lower(),
        "CW_PAIR_FEATURE": str(feature or "").strip().lower(),
    }

    old = {k: os.environ.get(k) for k in new.keys()}
    try:
        for k, v in new.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

def run_pairs(ctx) -> dict[str, Any]:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        if str(os.environ.get(k, "")).strip().lower() == "unscoped":
            os.environ.pop(k, None)
            
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
        dry_run=coerce_bool(ctx.dry_run) or coerce_bool(sync_cfg.get("dry_run", False)),
        mode="v3",
    )

    added_total = 0
    added_provider_total = 0
    removed_total = 0
    updated_total = 0
    unresolved_total = 0
    skipped_total = 0
    skipped_exact_total = 0
    skipped_inferred_total = 0
    errors_total = 0
    attempted_add_duplicate_keys_total = 0

    pairs = [p for p in (cfg.get("pairs") or []) if coerce_bool(p.get("enabled", True), True)]
    provs = ctx.providers or {}

    features_ran: set[str] = set()

    for i, pair in enumerate(pairs, 1):
        src = str(pair.get("source") or "").upper().strip()
        dst = str(pair.get("target") or "").upper().strip()
        src_inst = normalize_instance_id(pair.get("source_instance"))
        dst_inst = normalize_instance_id(pair.get("target_instance"))
        pair_cfg_view = build_pair_config_view(cfg, src, src_inst, dst, dst_inst)
        pair_prov = pair.get("providers") or {}
        if isinstance(pair_prov, dict) and pair_prov:
            for pk, pv in pair_prov.items():
                k = str(pk or "").strip().lower()
                if not k:
                    continue
                blk = pair_cfg_view.get(k)
                if not isinstance(blk, dict):
                    blk = {}
                    pair_cfg_view[k] = blk
                if isinstance(pv, Mapping):
                    _deep_merge_provider_overrides(blk, pv)
                elif pv is not None and k in {"plex", "jellyfin", "emby"}:
                    blk["strict_id_matching"] = coerce_bool(pv)

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
            src_instance=src_inst,
            dst_instance=dst_inst,
            mode=mode,
            features=features,
        )

        sops = provs.get(src)
        dops = provs.get(dst)
        if not sops or not dops:
            emit_info(f"[!] Missing provider ops for {src}→{dst}")
            continue

        ss = health_status(health_map.get(f"{src}#{src_inst}") or health_map.get(src) or {})
        sd = health_status(health_map.get(f"{dst}#{dst_inst}") or health_map.get(dst) or {})
        if ss == "auth_failed" or sd == "auth_failed":
            emit("pair:skip", src=src, dst=dst, reason="auth_failed", src_status=ss, dst_status=sd)
            continue

        injected = False

        for feature in features:
            fcfg = feat_map.get(feature) or {}
            if isinstance(fcfg, dict) and not coerce_bool(fcfg.get("enable", True), True):
                continue

            with _pair_env(pair, i=i, src=src, dst=dst, mode=mode, feature=feature):
                prev_cfg = ctx.config
                ctx.config = (
                    _config_with_pair_progress_options(pair_cfg_view, fcfg, (src, dst))
                    if feature == "progress"
                    else pair_cfg_view
                )
                try:
                    if not injected:
                        inject_ctx_into_provider(sops, ctx)
                        inject_ctx_into_provider(dops, ctx)
                        injected = True

                    src_ok = supports_feature(sops, feature) and health_feature_ok(health_map.get(f"{src}#{src_inst}") or health_map.get(src), feature)
                    dst_ok = supports_feature(dops, feature) and health_feature_ok(health_map.get(f"{dst}#{dst_inst}") or health_map.get(dst), feature)
                    if (not src_ok) or (not dst_ok):
                        emit(
                            "feature:unsupported",
                            src=src,
                            dst=dst,
                            feature=feature,
                            src_supported=src_ok,
                            dst_supported=dst_ok,
                        )
                        continue

                    features_ran.add(feature)

                    try:
                        if mode == "two-way":
                            res = run_two_way_feature(ctx, src, dst, feature=feature, fcfg=fcfg, health_map=health_map)
                            updated_total += int(res.get("upd_to_A", 0)) + int(res.get("upd_to_B", 0))
                            added_total += int(res.get("adds_to_A", 0)) + int(res.get("adds_to_B", 0))
                            removed_total += int(res.get("rem_from_A", 0)) + int(res.get("rem_from_B", 0))
                            unresolved_total += int(
                                res.get(
                                    "unresolved",
                                    int(res.get("unresolved_to_A", 0)) + int(res.get("unresolved_to_B", 0)),
                                )
                                or 0
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
                            updated_total += int(res.get("updated", 0))
                            added_total += int(res.get("added", 0))
                            added_provider_total += int(res.get("added_provider_reported", res.get("added", 0)))
                            removed_total += int(res.get("removed", 0))
                            unresolved_total += int(res.get("unresolved", 0))
                            skipped_total += int(res.get("skipped", 0))
                            skipped_exact_total += int(res.get("skipped_exact", 0))
                            skipped_inferred_total += int(res.get("skipped_inferred", 0))
                            attempted_add_duplicate_keys_total += int(res.get("attempted_add_duplicate_keys", 0))
                            errors_total += int(res.get("errors", 0))

                    except Exception as e:
                        import traceback as _tb
                        emit("feature:error", src=src, dst=dst, feature=feature, error=str(e), traceback=_tb.format_exc())
                        errors_total += 1
                        continue
                finally:
                    ctx.config = prev_cfg
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
                    "updated": updated_total,
                    "added": added_total,
                    "added_provider_reported": added_provider_total,
                    "removed": removed_total,
                    "skipped": skipped_total,
                    "skipped_exact": skipped_exact_total,
                    "skipped_inferred": skipped_inferred_total,
                    "attempted_add_duplicate_keys": attempted_add_duplicate_keys_total,
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
        updated=updated_total,
        added=added_total,
        added_provider_reported=added_provider_total,
        removed=removed_total,
        skipped=skipped_total,
        skipped_exact=skipped_exact_total,
        skipped_inferred=skipped_inferred_total,
        attempted_add_duplicate_keys=attempted_add_duplicate_keys_total,
        unresolved=unresolved_total,
        errors=errors_total,
        pairs=len(pairs),
        mode="v3",
    )
    return {
        "ok": True,
        "updated": updated_total,
        "added": added_total,
        "removed": removed_total,
        "skipped": skipped_total,
        "unresolved": unresolved_total,
        "errors": errors_total,
        "pairs": len(pairs),
    }
