# /cw_platform/rating_scales.py
# CrossWatch - Provider rating choices for manual writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
from functools import lru_cache
from itertools import combinations
from typing import Any, Callable, Mapping

from cw_platform.modules_registry import load_sync_ops
from cw_platform.provider_instances import build_provider_config_view


def rating_scale(provider: str, cfg: Mapping[str, Any] | None = None) -> dict[str, Any]:
    empty = {"values": [], "max": 10, "display_factor": 1}
    ops = load_sync_ops(provider)
    if ops is None or not ops.features().get("ratings"):
        return empty
    spec = (ops.capabilities().get("ratings") or {}).get("manual_scale")
    if not isinstance(spec, Mapping):
        return empty
    if spec.get("dynamic"):
        resolve = getattr(ops, "rating_scale", None)
        if not callable(resolve):
            return empty
        resolved = resolve(cfg or {})
        return dict(resolved) if isinstance(resolved, Mapping) else empty
    step = spec.get("step")
    if isinstance(step, bool) or not isinstance(step, (int, float)) or not math.isfinite(step) or step < 0.5 or step > 10:
        return empty
    return {"values": [i * step for i in range(1, int(10 / step) + 1) if i * step >= spec.get("min", step)],
            "max": spec.get("max", 10), "display_factor": spec.get("display_factor", 1),
            **({"round_step": spec["round_step"]} if "round_step" in spec else {})}


def rating_supported(provider: str, value: Any, cfg: Mapping[str, Any] | None = None) -> bool:
    if isinstance(value, bool):
        return False
    try:
        return float(value) in rating_scale(provider, cfg)["values"]
    except (TypeError, ValueError):
        return False


def rating_comparator(cfg: Mapping[str, Any]) -> Callable[[Mapping[tuple[str, str], Any]], bool]:
    from cw_platform.orchestrator._planner import _quantize_rating, _rating_step

    @lru_cache(maxsize=None)
    def policy(endpoint: tuple[str, str]) -> tuple[float, bool, dict[str, Any]] | None:
        provider, instance = endpoint
        ops = load_sync_ops(provider)
        if ops is None or not ops.features().get("ratings"):
            return None
        caps = ops.capabilities().get("ratings") or {}
        return (_rating_step(caps.get("step")), caps.get("zero_is_rating") is True,
                rating_scale(provider, build_provider_config_view(cfg, provider, instance)))

    def project(value: float, scale: Mapping[str, Any]) -> float | None:
        if "bands" in scale:
            return next((band["value"] for band in scale["bands"] if band["min"] <= value < band["max"]), None)
        step = scale.get("round_step")
        if step and scale.get("values"):
            return max(scale["values"][0], min(scale["values"][-1], math.floor(value / step + 0.5) * step))
        return value

    def agree(ratings: Mapping[tuple[str, str], Any]) -> bool:
        for (left, a), (right, b) in combinations(ratings.items(), 2):
            if a == b:
                continue
            pa, pb = policy(left), policy(right)
            if pa is None or pb is None:
                return False
            step, zero = max(pa[0], pb[0]), pa[1] and pb[1]
            qa = _quantize_rating(a, step, allow_zero=zero)
            qb = _quantize_rating(b, step, allow_zero=zero)
            if qa is None or qb is None:
                return False
            if not any(project(qa, scale) is not None and project(qa, scale) == project(qb, scale)
                       for scale in (pa[2], pb[2])):
                return False
        return True

    return agree
