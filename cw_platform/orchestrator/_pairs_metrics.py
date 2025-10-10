from __future__ import annotations
from typing import Any, Dict, Mapping, Optional, Callable
import time

# Lightweight API telemetry aggregator used by _pairs.py.
# Plug-and-play:
#   metrics = ApiMetrics(ctx.emit)
#   ctx.emit = metrics.emit   # wrap for the duration of the run
#   ...
#   totals = metrics.totals()
#   ctx.emit("api:totals", totals=totals)
#   persist_api_totals(ctx, totals)  # optional


class ApiMetrics:
    def __init__(self, emit: Callable[..., Any]) -> None:
        self._orig_emit = emit
        self._hits: Dict[str, Dict[str, Any]] = {}  # provider -> buckets

    # ---- public -------------------------------------------------------------

    @property
    def hits(self) -> Dict[str, Dict[str, Any]]:
        return self._hits

    def emit(self, event: str, **kwargs):
        """Wrapped emit that also collects api:* breadcrumbs."""
        try:
            if event == "api:hit":
                self._on_api_hit(kwargs)
            elif event == "api:totals":
                self._on_api_totals(kwargs.get("totals"))
        except Exception:
            # Telemetry must never break the run.
            pass
        return self._orig_emit(event, **kwargs)

    def totals(self) -> Dict[str, Any]:
        """Build a compact, ready-to-emit totals payload."""
        out = {"total": 0, "providers": {}}
        for prov, data in self._hits.items():
            total = int(data.get("total") or 0)
            samples = int(data.get("latency_ms_samples") or 0)
            sum_ms = int(data.get("latency_ms_sum") or 0)
            avg_ms = int(sum_ms / samples) if samples > 0 else None
            out["providers"][prov] = {
                "total": total,
                "by_endpoint": dict(data.get("by_endpoint") or {}),
                "by_feature": dict(data.get("by_feature") or {}),
                "by_method": dict(data.get("by_method") or {}),
                "by_status": dict(data.get("by_status") or {}),
                "latency_ms_avg": avg_ms,
                "latency_ms_samples": samples,
            }
            out["total"] += total
        return out

    # ---- internals ----------------------------------------------------------

    def _prov_entry(self, p: str) -> Dict[str, Any]:
        p = str(p or "UNKNOWN").upper()
        ent = self._hits.setdefault(
            p,
            {
                "total": 0,
                "by_endpoint": {},
                "by_feature": {},
                "by_method": {},
                "by_status": {},
                "latency_ms_sum": 0,
                "latency_ms_samples": 0,
            },
        )
        return ent

    def _on_api_hit(self, kw: Mapping[str, Any]) -> None:
        provider = (
            kw.get("provider")
            or kw.get("dst")
            or kw.get("src")
            or "UNKNOWN"
        )
        ent = self._prov_entry(provider)
        ent["total"] += 1

        ep = kw.get("endpoint")
        if ep:
            key = str(ep)
            ent["by_endpoint"][key] = int(ent["by_endpoint"].get(key, 0)) + 1

        feat = kw.get("feature")
        if feat:
            key = str(feat)
            ent["by_feature"][key] = int(ent["by_feature"].get(key, 0)) + 1

        method = kw.get("method")
        if method:
            m = str(method).upper()
            ent["by_method"][m] = int(ent["by_method"].get(m, 0)) + 1

        status = kw.get("status")
        if status is not None:
            try:
                code = int(status)
                sc = str(code)
                ent["by_status"][sc] = int(ent["by_status"].get(sc, 0)) + 1
                cls = f"{code // 100}xx"
                ent["by_status"][cls] = int(ent["by_status"].get(cls, 0)) + 1
            except Exception:
                pass

        ms = kw.get("ms") or kw.get("latency_ms")
        try:
            if ms is not None:
                ent["latency_ms_sum"] += int(ms)
                ent["latency_ms_samples"] += 1
        except Exception:
            pass

    def _on_api_totals(self, totals: Optional[Mapping[str, Any]]) -> None:
        """
        Allow providers to feed pre-aggregated data:
          {"total": N, "providers": {"TRAKT": {...}, "PLEX": {...}}}
        We merge their buckets into ours.
        """
        if not isinstance(totals, Mapping):
            return
        providers = totals.get("providers") or {}
        if not isinstance(providers, Mapping):
            return
        for pname, pdata in providers.items():
            if not isinstance(pdata, Mapping):
                continue
            self._merge_provider_totals(str(pname), pdata)

    def _merge_provider_totals(self, pname: str, pdata: Mapping[str, Any]) -> None:
        ent = self._prov_entry(pname)
        try:
            ent["total"] += int(pdata.get("total") or 0)
        except Exception:
            pass
        for key in ("by_endpoint", "by_feature", "by_method", "by_status"):
            sub = pdata.get(key) or {}
            if isinstance(sub, Mapping):
                for k, v in sub.items():
                    ent[key][str(k)] = int(ent[key].get(str(k), 0)) + int(v or 0)
        # latency: only additive if the source also provided sum+samples; otherwise ignore
        sum_ms = pdata.get("latency_ms_sum")
        samples = pdata.get("latency_ms_samples")
        try:
            if sum_ms is not None and samples is not None:
                ent["latency_ms_sum"] += int(sum_ms)
                ent["latency_ms_samples"] += int(samples)
        except Exception:
            pass


# Optional: simple persistence helper to mirror legacy behavior.
def persist_api_totals(ctx, totals: Mapping[str, Any], *, ts: Optional[int] = None) -> None:
    try:
        st = ctx.state_store.load_state() or {}
        st.setdefault("metrics", {}).setdefault("api", {})
        st["metrics"]["api"]["last"] = {"ts": int(ts or time.time()), **totals}
        ctx.state_store.save_state(st)
    except Exception:
        # Persistence is best-effort; ignore failures.
        pass
