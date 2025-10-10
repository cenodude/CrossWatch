from __future__ import annotations
from typing import Any
class Stats:
    def __init__(self, impl: Any | None = None):
        self.impl = impl or self
    def record_summary(self, *a, **k): pass
    def overview(self, *a, **k): return {}
    def http_overview(self, *a, **k): return {}
def maybe_emit_rate_warnings(stats: Stats, emit, thresholds: dict):
    try:
        ov = stats.http_overview(hours=24) or {}
        provs = (ov.get("providers") or {})
        for prov, row in provs.items():
            last = (row.get("rate") or {})
            remaining = last.get("remaining"); reset = last.get("reset")
            thr = int(thresholds.get(prov, 0) or 0)
            if remaining is not None and thr and int(remaining) <= thr:
                emit("rate:low", provider=prov, remaining=int(remaining), reset=reset, threshold=thr)
    except Exception:
        pass
