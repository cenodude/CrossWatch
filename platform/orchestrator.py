from __future__ import annotations
import time
from typing import Any, Dict, Mapping, Optional, Sequence
from _logging import log
from modules._mod_base import SyncContext, SyncResult, SyncStatus
# Import available modules; extend registry as you add new ones
try:
    from modules._mod_SIMKL import SIMKLModule
except Exception:  # pragma: no cover
    SIMKLModule = None  # type: ignore

class Orchestrator:
    """Runs sync profiles by invoking the right module(s)."""

    def __init__(self, load_cfg, save_cfg, platform_mgr):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.platform = platform_mgr
        # Map (source, target) -> module class responsible for the pair
        self.registry: dict[tuple[str, str], Any] = {}
        if SIMKLModule is not None:
            # This handles Plex <-> SIMKL watchlist sync
            self.registry[("PLEX", "SIMKL")] = SIMKLModule
            self.registry[("SIMKL", "PLEX")] = SIMKLModule

    def list_profiles(self) -> list[dict]:
        return self.platform.sync_profiles()

    def _select_module(self, source: str, target: str):
        return self.registry.get((source.upper(), target.upper()))

    def run_profile(self, profile_id: str, *, dry_run: bool = False, timeout_sec: int | None = 600) -> dict:
        """Run a single profile by id. Returns a compact report."""
        cfg = self.load_cfg()
        # fetch profile
        profiles = self.platform.sync_profiles()
        prof = next((p for p in profiles if p.get("id") == profile_id), None)
        if not prof:
            raise ValueError(f"Profile not found: {profile_id}")
        source = str(prof["source"]).upper()
        target = str(prof["target"]).upper()
        direction = str(prof.get("direction", "mirror")).lower()
        features = prof.get("features", {}) or {}

        # compute and validate options server-side again
        allowed = self.platform.sync_options(source, target, direction)
        enabled = {k: v for k, v in features.items() if v and allowed.get(k, False)}
        if not enabled:
            log("Orchestrator: no enabled features for profile", level="WARNING", module="ORCH", extra={"id": profile_id})
            return {"id": profile_id, "status": "SKIPPED", "reason": "No enabled features", "features": features}

        # choose the module for the pair
        Mod = self._select_module(source, target)
        if not Mod:
            raise ValueError(f"No module registered for {source} → {target}")

        # set module config; also inject previous snapshot state if present
        # PlatformManager stores auth tokens in config already.
        ctx = SyncContext(run_id=f"{profile_id}-{int(time.time())}", dry_run=dry_run, timeout_sec=timeout_sec)

        # NOTE: For now, SIMKLModule implements full watchlist sync; we gate on feature selection.
        if not enabled.get("watchlist", False):
            log("Orchestrator: module requires watchlist; nothing to do", level="WARNING", module="ORCH", extra={"id": profile_id})
            return {"id": profile_id, "status": "SKIPPED", "reason": "Watchlist not enabled", "features": features}

        # Run the module
        t0 = time.time()
        try:
            mod = Mod(config=cfg, logger=log)  # modules use flexible logger
            res: SyncResult = mod.run_sync(ctx)
            dt = time.time() - t0
            status = res.status.name
            log("Orchestrator: run finished", level=("SUCCESS" if res.status == SyncStatus.SUCCESS else "WARNING"), module="ORCH",
                extra={"id": profile_id, "duration_ms": res.duration_ms})
            # persist snapshot if provided by the module in metadata
            snap = (res.metadata or {}).get("snapshot")
            if isinstance(snap, dict):
                # store under cfg['state'] so next run can use deltas
                cfg["state"] = snap
                self.save_cfg(cfg)
            return {
                "id": profile_id,
                "status": status,
                "duration_sec": round(dt, 2),
                "items_total": res.items_total,
                "items_added": res.items_added,
                "items_removed": res.items_removed,
                "features": sorted([k for k, v in enabled.items() if v]),
                "run_id": ctx.run_id,
            }
        except Exception as e:  # pragma: no cover
            log(f"Orchestrator: run failed: {e}", level="ERROR", module="ORCH", extra={"id": profile_id})
            return {"id": profile_id, "status": "FAILED", "error": str(e)}
