# /modules/_mod_base.py
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, Mapping, Optional, Protocol, Callable  # <- Callable erbij

# ---------- Logging

class Logger(Protocol):
    def __call__(
        self,
        message: str,
        *,
        level: str = "INFO",
        module: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None: ...
    def set_context(self, **ctx: Any) -> None: ...
    def get_context(self) -> Dict[str, Any]: ...
    def bind(self, **ctx: Any) -> "Logger": ...
    def child(self, name: str) -> "Logger": ...

# ---------- Status & results

class SyncStatus(Enum):
    IDLE = auto()
    RUNNING = auto()
    SUCCESS = auto()
    WARNING = auto()
    FAILED = auto()
    CANCELLED = auto()

@dataclass
class SyncContext:
    run_id: str
    dry_run: bool = False
    timeout_sec: Optional[int] = None
    ui_hints: Dict[str, Any] = field(default_factory=dict)
    cancel_flag: list[bool] = field(default_factory=lambda: [False])  # cooperative cancel

@dataclass
class ProgressEvent:
    stage: str
    done: int = 0
    total: int = 0
    note: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SyncResult:
    status: SyncStatus
    started_at: float
    finished_at: float
    duration_ms: int
    items_total: int = 0
    items_added: int = 0
    items_removed: int = 0
    items_updated: int = 0
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

# ---------- Capabilities & meta

@dataclass(frozen=True)
class ModuleCapabilities:
    supports_dry_run: bool = True
    supports_cancel: bool = True
    supports_timeout: bool = True
    bidirectional: bool = False
    status_stream: bool = True
    config_schema: Optional[Dict[str, Any]] = None

@dataclass(frozen=True)
class ModuleInfo:
    name: str
    version: str = "0.1.0"
    description: str = ""
    vendor: str = "community"
    capabilities: ModuleCapabilities = ModuleCapabilities()

# ---------- Errors

class ModuleError(RuntimeError): ...
class RecoverableModuleError(ModuleError): ...
class ConfigError(ModuleError): ...

# ---------- Module protocol

class SyncModule(Protocol):
    info: ModuleInfo

    def __init__(self, config: Mapping[str, Any], logger: Logger) -> None: ...

    def validate_config(self) -> None:
        """Raise ConfigError if config is invalid."""
        ...

    def run_sync(
        self,
        ctx: SyncContext,
        progress: Optional[Callable[[ProgressEvent], None]] = None,
    ) -> SyncResult:
        """
        Execute the sync. Must be deterministic for the given context and config.
        Honor ctx.timeout_sec and ctx.cancel_flag when provided.
        Emit progress() occasionally when available.
        """
        ...

    def get_status(self) -> Mapping[str, Any]:
        """Fast status snapshot for UIs."""
        ...

    def cancel(self) -> None:
        """Best-effort cancel; safe if not running. Should set ctx.cancel_flag[0] = True."""
        ...

    def set_logger(self, logger: Logger) -> None:
        """Replace or wrap the logger at runtime."""
        ...

    def reconfigure(self, config: Mapping[str, Any]) -> None:
        """Apply new config atomically and call validate_config()."""
        ...
