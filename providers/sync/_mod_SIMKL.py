from __future__ import annotations
from typing import Any, Dict, Mapping, Optional, Callable
import time

# /providers/sync/_mod_SIMKL.py

from ._mod_base import (
    SyncModule, SyncContext, SyncResult, SyncStatus,
    ModuleError, RecoverableModuleError, ConfigError,
    Logger as HostLogger, ProgressEvent,
    ModuleInfo, ModuleCapabilities,
)

__VERSION__ = "0.1.1"

class SIMKLModule(SyncModule):
    info = ModuleInfo(
        name="SIMKL",
        version=__VERSION__,
        description="SIMKL watchlist sync provider.",
        vendor="community",
        capabilities=ModuleCapabilities(
            supports_dry_run=True,
            supports_cancel=True,
            supports_timeout=True,
            status_stream=True,
            bidirectional=True,
            config_schema={
                "type": "object",
                "properties": {
                    "simkl": {
                        "type": "object",
                        "properties": {
                            "access_token": {"type": "string"},
                        },
                    },
                    "runtime": {
                        "type": "object",
                        "properties": {"debug": {"type": "boolean"}},
                    },
                },
            },
        ),
    )

    def __init__(self, config: Optional[Mapping[str, Any]] = None, logger: Optional[Logger] = None) -> None:
        self._logger: Logger = logger or default_log
        self._config: Dict[str, Any] = dict(config or {})
        self._cancelled: bool = False

    def set_logger(self, logger: Logger) -> None:
        self._logger = logger

    def validate_config(self, config: Mapping[str, Any]) -> None:
        # keep permissive; auth flow may inject token later
        if not isinstance(config, Mapping):
            raise ConfigError("config must be a mapping")

    def reconfigure(self, config: Mapping[str, Any]) -> None:
        self._config = dict(config or {})

    def fast_status(self) -> Dict[str, Any]:
        return {"ok": True, "module": "SIMKL"}

    def cancel(self) -> None:
        self._cancelled = True

    def run(
        self,
        ctx: SyncContext,
        config: Optional[Mapping[str, Any]] = None,
        progress: Optional[Callable[[str], None]] = None,
    ) -> SyncResult:
        t0 = time.time()
        self._cancelled = False
        if progress: progress("SIMKL dry run" if ctx.dry_run else "SIMKL run")
        status = SyncStatus.SUCCESS
        return SyncResult(
            status=status,
            started_at=t0,
            finished_at=time.time(),
            duration_ms=int((time.time() - t0) * 1000),
            items_total=0,
            items_added=0,
            items_removed=0,
            items_updated=0,
            warnings=[],
            errors=[],
            metadata={},
        )
