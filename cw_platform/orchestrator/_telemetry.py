# cw_platform/orchestrator/_telemetry.py
# telemetry utilities for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable

class Stats:
    def __init__(self, impl: Any | None = None) -> None:
        self.impl: Any = impl or self

    def record_summary(self, *a: Any, **k: Any) -> None:
        pass

    def overview(self, *a: Any, **k: Any) -> dict[str, Any]:
        return {}

    def http_overview(self, *a: Any, **k: Any) -> dict[str, Any]:
        return {}

def maybe_emit_rate_warnings(
    stats: Stats,
    emit: Callable[..., Any],
    thresholds: Mapping[str, int] | None = None,
) -> None:
    try:
        ov = stats.http_overview(hours=24) or {}
        provs = ov.get("providers") or {}
        if not isinstance(provs, Mapping):
            return

        thr_map: Mapping[str, int] = thresholds or {}

        for prov, row_any in provs.items():
            if not isinstance(row_any, Mapping):
                continue

            row: Mapping[str, Any] = row_any
            last = row.get("rate") or {}
            if not isinstance(last, Mapping):
                continue

            remaining = last.get("remaining")
            reset = last.get("reset")
            thr = int(thr_map.get(str(prov), 0) or 0)

            if remaining is not None and thr and int(remaining) <= thr:
                emit(
                    "rate:low",
                    provider=prov,
                    remaining=int(remaining),
                    reset=reset,
                    threshold=thr,
                )
    except Exception:
        pass
