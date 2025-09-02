from __future__ import annotations
import time
from typing import Any, Dict
from _logging import log
from providers.sync._mod_base import SyncContext, SyncResult, SyncStatus

# Registry mapping (source, target) -> module class
def _registry():
    out = {}
    try:
        from providers.sync._mod_SIMKL import SIMKLModule
        out[("PLEX","SIMKL")] = SIMKLModule
        out[("SIMKL","PLEX")] = SIMKLModule
    except Exception:
        pass
    try:
        from providers.sync._mod_PLEX import PlexModule
        # Add mappings if Plex module handles other pairs
    except Exception:
        pass
    return out

class Orchestrator:
    def __init__(self, load_cfg, save_cfg, platform_mgr):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.platform = platform_mgr
        self._registry = _registry()

    def run_profile(self, profile_id: str, *, dry_run: bool = False, timeout_sec: int | None = 600) -> Dict[str, Any]:
        cfg = self.load_cfg() or {}
        profs = self.platform.sync_profiles()
        prof = next((p for p in profs if p.get("id") == profile_id), None)
        if not prof:
            raise ValueError(f"Profile not found: {profile_id}")
        src = str(prof["source"]).upper()
        dst = str(prof["target"]).upper()
        direction = str(prof.get("direction","mirror")).lower()
        feats = prof.get("features",{}) or {}

        allowed = self.platform.sync_options(src, dst, direction)
        enabled = {k:v for k,v in feats.items() if v and allowed.get(k)}
        if not enabled:
            return {"id": profile_id, "status": "SKIPPED", "reason": "No enabled features"}

        Mod = self._registry.get((src,dst))
        if not Mod:
            raise ValueError(f"No module registered for {src} → {dst}")

        ctx = SyncContext(run_id=f"{profile_id}-{int(time.time())}", dry_run=dry_run, timeout_sec=timeout_sec)
        if not enabled.get("watchlist", False):
            return {"id": profile_id, "status": "SKIPPED", "reason": "Watchlist not enabled"}

        t0 = time.time()
        try:
            mod = Mod(config=cfg, logger=log)
            res: SyncResult = mod.run_sync(ctx)
            dt = time.time() - t0
            snap = (res.metadata or {}).get("snapshot")
            if isinstance(snap, dict):
                cfg["state"] = snap
                self.save_cfg(cfg)
            return {
                "id": profile_id,
                "status": res.status.name,
                "duration_sec": round(dt, 2),
                "items_total": res.items_total,
                "items_added": res.items_added,
                "items_removed": res.items_removed,
                "features": sorted([k for k,v in enabled.items() if v]),
                "run_id": ctx.run_id,
            }
        except Exception as e:
            log(f"Run failed: {e}", level="ERROR", module="ORCH", extra={"id": profile_id})
            return {"id": profile_id, "status": "FAILED", "error": str(e)}
