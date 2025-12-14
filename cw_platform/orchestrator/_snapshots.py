# cw_platform/orchestrator/_snapshots.py
# snapshot management for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable

import time
import datetime as _dt

from ..id_map import canonical_key
from ._types import InventoryOps
from ..modules_registry import load_sync_ops

SnapIndex = dict[str, dict[str, Any]]
SnapCache = dict[tuple[str, str], tuple[float, SnapIndex]]


def allowed_providers_for_feature(config: Mapping[str, Any], feature: str) -> set[str]:
    allowed: set[str] = set()
    try:
        pairs = list((config.get("pairs") or []) or [])
    except Exception:
        pairs = []

    def _feat_enabled(fmap: dict[str, Any], name: str) -> bool:
        v = (fmap or {}).get(name)
        if isinstance(v, bool):
            return bool(v)
        if isinstance(v, dict):
            return bool(v.get("enable", v.get("add", True)))
        return False

    for p in pairs:
        try:
            if not p.get("enabled", True):
                continue
            fmap = dict(p.get("features") or {})
            if not _feat_enabled(fmap, feature):
                continue
            s = str(p.get("source") or p.get("src") or "").strip().upper()
            t = str(p.get("target") or p.get("dst") or "").strip().upper()
            if s:
                allowed.add(s)
            if t:
                allowed.add(t)
        except Exception:
            continue
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


def _coerce_checkpoint_value(v: Any) -> str | None:
    if v is None:
        return None
    return str(v)


def module_checkpoint(ops: InventoryOps, config: Mapping[str, Any], feature: str) -> str | None:
    acts_fn = getattr(ops, "activities", None)
    if not callable(acts_fn):
        return None

    try:
        raw = acts_fn(config)
    except Exception:
        return None

    acts: Mapping[str, Any]
    if isinstance(raw, Mapping):
        acts = raw
    else:
        return None

    try:
        if feature == "watchlist":
            return (
                _coerce_checkpoint_value(acts.get("watchlist"))
                or _coerce_checkpoint_value(acts.get("ptw"))
                or _coerce_checkpoint_value(acts.get("updated_at"))
            )
        if feature == "ratings":
            return (
                _coerce_checkpoint_value(acts.get("ratings"))
                or _coerce_checkpoint_value(acts.get("updated_at"))
            )
        if feature == "history":
            return (
                _coerce_checkpoint_value(acts.get("history"))
                or _coerce_checkpoint_value(acts.get("updated_at"))
            )
        return _coerce_checkpoint_value(acts.get("updated_at"))
    except Exception:
        return None


def prev_checkpoint(state: Mapping[str, Any], prov: str, feature: str) -> str | None:
    try:
        providers_block = state.get("providers")
        if not isinstance(providers_block, Mapping):
            return None
        prov_block = providers_block.get(prov)
        if not isinstance(prov_block, Mapping):
            return None
        feat_block = prov_block.get(feature)
        if not isinstance(feat_block, Mapping):
            return None
        cp = feat_block.get("checkpoint")
        return _coerce_checkpoint_value(cp)
    except Exception:
        return None


def _parse_ts(v: Any) -> int | None:
    if v in (None, "", 0):
        return None
    try:
        if isinstance(v, (int, float)):
            return int(v)
        return int(
            _dt.datetime.fromisoformat(
                str(v).replace("Z", "+00:00").replace(" ", "T")
            ).timestamp()
        )
    except Exception:
        return None

def _eventish_count(feature: str, idx: Mapping[str, Any]) -> int:
    if feature == "history":
        return sum(
            1
            for v in idx.values()
            if isinstance(v, Mapping) and (v.get("watched_at") or v.get("last_watched_at"))
        )
    if feature == "ratings":
        return sum(
            1
            for v in idx.values()
            if isinstance(v, Mapping)
            and (v.get("rated_at") or v.get("user_rated_at") or v.get("rating") or v.get("user_rating"))
        )
    return len(idx)

def build_snapshots_for_feature(
    *,
    feature: str,
    config: Mapping[str, Any],
    providers: Mapping[str, InventoryOps],
    snap_cache: SnapCache,
    snap_ttl_sec: int,
    dbg: Callable[..., Any],
    emit_info: Callable[[str], Any],
) -> dict[str, SnapIndex]:
    snaps: dict[str, SnapIndex] = {}
    now = time.time()
    allowed = allowed_providers_for_feature(config, feature)

    for name, ops in providers.items():
        try:
            feats_raw = ops.features()  # type: ignore[call-arg]
        except Exception:
            feats_raw = {}

        feats: Mapping[str, Any]
        if isinstance(feats_raw, Mapping):
            feats = feats_raw
        else:
            feats = {}

        if not bool(feats.get(feature, False)):
            continue

        if allowed and name.upper() not in allowed:
            continue

        if not provider_configured(config, name):
            continue

        memo_key = (name, feature)
        if snap_ttl_sec > 0:
            ent = snap_cache.get(memo_key)
            if ent is not None:
                ts, cached_idx = ent
                if (now - ts) < snap_ttl_sec:
                    snaps[name] = cached_idx
                    dbg("snapshot.memo", provider=name, feature=feature, count=_eventish_count(feature, cached_idx), raw_count=len(cached_idx))
                    continue

        degraded = False
        try:
            idx_raw = ops.build_index(config, feature=feature)  # type: ignore[call-arg]
        except Exception as e:
            emit_info(
                f"[!] snapshot.failed provider={name} feature={feature} error={e}"
            )
            dbg("provider.degraded", provider=name, feature=feature)
            degraded = True
            idx_raw = None

        canon: SnapIndex = {}

        if isinstance(idx_raw, list):
            for raw in idx_raw:
                if not isinstance(raw, Mapping):
                    continue
                item = dict(raw)
                key = canonical_key(item)
                if key:
                    canon[key] = item
                    
        elif isinstance(idx_raw, Mapping):
            for k, raw in idx_raw.items():
                if not isinstance(raw, Mapping):
                    continue
                item = dict(raw)
                key = canonical_key(item)
                if not key and isinstance(k, str) and k:
                    key = k.split("@", 1)[0]
                if key:
                    canon[key] = item

        else:
            canon = {}

        snaps[name] = canon

        if snap_ttl_sec > 0:
            if degraded or not canon:
                dbg(
                    "snapshot.no_cache_empty",
                    provider=name,
                    feature=feature,
                    degraded=bool(degraded),
                )
            else:
                snap_cache[memo_key] = (now, canon)
        dbg("snapshot", provider=name, feature=feature, count=_eventish_count(feature, canon), raw_count=len(canon))
    return snaps

def coerce_suspect_snapshot(
    *,
    provider: str,
    ops: InventoryOps,
    prev_idx: Mapping[str, Any],
    cur_idx: Mapping[str, Any],
    feature: str,
    suspect_min_prev: int,
    suspect_shrink_ratio: float,
    suspect_debug: bool,
    emit: Callable[..., Any],
    emit_info: Callable[[str], Any],
    prev_cp: str | None,
    now_cp: str | None,
) -> tuple[dict[str, Any], bool, str]:
    try:
        caps_raw = ops.capabilities()  # type: ignore[call-arg]
    except Exception:
        caps_raw = {}

    if isinstance(caps_raw, Mapping):
        caps: Mapping[str, Any] = caps_raw
    else:
        caps = {}

    sem = caps.get("index_semantics", "present")

    if str(sem).lower() != "present":
        return dict(cur_idx), False, "semantics:delta"

    prev_count = len(prev_idx or {})
    cur_count = len(cur_idx or {})

    if prev_count < suspect_min_prev:
        return dict(cur_idx), False, "baseline:tiny"

    shrink_limit = max(1, int(prev_count * suspect_shrink_ratio))
    shrunk = (cur_count == 0) or (cur_count <= shrink_limit)
    if not shrunk:
        return dict(cur_idx), False, "ok"

    prev_ts = _parse_ts(prev_cp)
    now_ts = _parse_ts(now_cp)

    no_progress = (
        (prev_ts is not None and now_ts is not None and now_ts <= prev_ts)
        or (prev_ts is not None and now_ts is None)
        or (prev_cp and now_cp and str(now_cp) == str(prev_cp))
    )

    if no_progress:
        reason = "suspect:no-progress+shrunk"
        if suspect_debug:
            emit(
                "snapshot:suspect",
                provider=provider,
                feature=feature,
                prev_count=prev_count,
                cur_count=cur_count,
                shrink_limit=shrink_limit,
                prev_checkpoint=prev_cp,
                now_checkpoint=now_cp,
                reason=reason,
            )
        return dict(prev_idx), True, reason

    return dict(cur_idx), False, "progressed"