"""Base contracts for sync providers.

Defines the lightweight logging protocol, common status enums, context and
result models, capability metadata, and the `SyncModule` protocol that provider
modules implement. This file intentionally contains no runtime logic.
"""

from __future__ import annotations
from _logging import log

# CrossWatch sync module base contracts
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, Mapping, Optional, Protocol, Callable  # Includes Callable for progress callbacks

# Logging

class Logger(Protocol):
    """Minimal structured logger interface used by sync modules.

    Implementations should accept a message with optional context and produce
    human-readable logs. Context helpers (`set_context`, `bind`, `child`) allow
    attaching structured fields and creating namespaced loggers.
    """
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

# Status & results

class SyncStatus(Enum):
    """High-level lifecycle of a sync run."""
    IDLE = auto()
    RUNNING = auto()
    SUCCESS = auto()
    WARNING = auto()
    FAILED = auto()
    CANCELLED = auto()

@dataclass
class SyncContext:
    """Execution context passed into a sync run.

    - run_id: unique identifier for correlating logs and results
    - dry_run: when True, perform no external writes
    - timeout_sec: optional overall timeout for cooperative checks
    - ui_hints: free-form hints for UIs (e.g., feature labels)
    - cancel_flag: cooperative cancel toggled by `cancel()`
    """
    run_id: str
    dry_run: bool = False
    timeout_sec: Optional[int] = None
    ui_hints: Dict[str, Any] = field(default_factory=dict)
    cancel_flag: list[bool] = field(default_factory=lambda: [False])  # cooperative cancel

@dataclass
class ProgressEvent:
    """Progress signal that modules can emit to UIs during a run."""
    stage: str
    done: int = 0
    total: int = 0
    note: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SyncResult:
    """Final outcome of a sync run with counters and messages."""
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

# Capabilities & meta

@dataclass(frozen=True)
class ModuleCapabilities:
    """Feature flags a module supports; surfaced to UIs and orchestrators."""
    supports_dry_run: bool = True
    supports_cancel: bool = True
    supports_timeout: bool = True
    bidirectional: bool = False
    status_stream: bool = True
    config_schema: Optional[Dict[str, Any]] = None

@dataclass(frozen=True)
class ModuleInfo:
    """Descriptive metadata for a sync module."""
    name: str
    version: str = "0.1.0"
    description: str = ""
    vendor: str = "community"
    capabilities: ModuleCapabilities = ModuleCapabilities()
    hidden: bool = False
    is_template: bool = False


# Errors

class ModuleError(RuntimeError):
    """Base error raised by modules for fatal conditions."""
    ...

class RecoverableModuleError(ModuleError):
    """Error that may be retried or handled without aborting the run."""
    ...

class ConfigError(ModuleError):
    """Configuration problems detected during validation."""
    ...

# Module protocol

class SyncModule(Protocol):
    """Protocol that all sync modules must implement.

    Concrete providers expose `info`, validate configuration, run the sync,
    report status, support cooperative cancellation, and allow runtime
    reconfiguration and logger replacement.
    """
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