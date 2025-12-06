# /providers/sync/_mod_base.py
# CrossWatch base sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol

try:
    from _statistics import Stats  # type: ignore
    _STATS = Stats()
except Exception:
    _STATS = None  # type: ignore


# Log types

class Logger(Protocol):
    def __call__(
        self,
        message: str,
        *,
        level: str = "INFO",
        module: str | None = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None: ...

    def set_context(self, **ctx: Any) -> None: ...

    def get_context(self) -> dict[str, Any]: ...

    def bind(self, **ctx: Any) -> "Logger": ...

    def child(self, name: str) -> "Logger": ...


# Status & results

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
    timeout_sec: int | None = None
    ui_hints: dict[str, Any] = field(default_factory=dict)
    cancel_flag: list[bool] = field(default_factory=lambda: [False])


@dataclass
class ProgressEvent:
    stage: str
    done: int = 0
    total: int = 0
    note: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)


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
    metadata: dict[str, Any] = field(default_factory=dict)


# Capabilities & meta

@dataclass(frozen=True)
class ModuleCapabilities:
    supports_dry_run: bool = True
    supports_cancel: bool = True
    supports_timeout: bool = True
    bidirectional: bool = False
    status_stream: bool = True
    config_schema: dict[str, Any] | None = None


@dataclass(frozen=True)
class ModuleInfo:
    name: str
    version: str = "0.1.0"
    description: str = ""
    vendor: str = "community"
    capabilities: ModuleCapabilities = ModuleCapabilities()
    hidden: bool = False
    is_template: bool = False


# Errors

class ModuleError(RuntimeError): ...
class RecoverableModuleError(ModuleError): ...
class ConfigError(ModuleError): ...


# Module protocol

class SyncModule(Protocol):
    info: ModuleInfo

    def __init__(self, config: Mapping[str, Any], logger: Logger) -> None: ...

    def validate_config(self) -> None: ...

    def run_sync(
        self,
        ctx: SyncContext,
        progress: Callable[[ProgressEvent], None] | None = None,
    ) -> SyncResult: ...

    def get_status(self) -> Mapping[str, Any]: ...

    def cancel(self) -> None: ...

    def set_logger(self, logger: Logger) -> None: ...

    def reconfigure(self, config: Mapping[str, Any]) -> None: ...


def get_stats() -> Any:
    return _STATS


# Time helpers

def iso_to_ts(s: str) -> int:
    if not s:
        return 0
    try:
        import datetime as dt
        s2 = s.replace("Z", "+0000").split(".")[0]
        return int(dt.datetime.strptime(s2, "%Y-%m-%dT%H:%M:%S%z").timestamp())
    except Exception:
        try:
            import datetime as dt
            return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return 0


def ts_to_iso(ts: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts or 0)))


# State dir & JSON I/O

def resolve_state_dir(cfg_root: Mapping[str, Any]) -> Path:
    try:
        p = (cfg_root.get("runtime") or {}).get("state_dir")
        if p:
            return Path(p)
    except Exception:
        pass
    env = os.environ.get("CW_STATE_DIR")
    if env:
        return Path(env)
    if Path("/config").exists():
        return Path("/config")
    return Path.cwd()


def _ensure_dir(p: Path) -> None:
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def atomic_write_json(path: Path, data: Mapping[str, Any]) -> None:
    _ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.write_text(json.dumps(data, ensure_ascii=False), "utf-8")
            os.replace(tmp, path)
        except Exception:
            pass


def read_json_or(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return fallback


# Backoff

def with_backoff(
    req_fn: Callable[..., Any],
    *a: Any,
    retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 10.0,
    **kw: Any,
) -> Any:
    delay = float(base_delay)
    last: Any = None
    for _ in range(max(1, int(retries))):
        try:
            r = req_fn(*a, **kw)
            last = r
        except Exception:
            r = None  # type: ignore[assignment]

        if r is not None:
            try:
                hdr = getattr(r, "headers", {}) or {}
                rem = hdr.get("X-RateLimit-Remaining")
                if rem is not None:
                    try:
                        if int(rem) <= 0:
                            rst = hdr.get("X-RateLimit-Reset")
                            if rst:
                                try:
                                    wait = max(0, int(rst) - int(time.time()))
                                    time.sleep(min(wait, max_delay))
                                except Exception:
                                    time.sleep(delay)
                            else:
                                time.sleep(delay)
                    except Exception:
                        pass

                status = int(getattr(r, "status_code", 0) or 0)
                if status == 429 or (500 <= status < 600):
                    time.sleep(delay)
                    delay = min(delay * 2, max_delay)
                    continue
                return r
            except Exception:
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
                continue
        else:
            time.sleep(delay)
            delay = min(delay * 2, max_delay)
    return last


def record_http(
    *,
    provider: str,
    endpoint: str,
    method: str,
    response: Any | None,
    payload: Any = None,
    count: bool = True,
) -> None:
    stats = get_stats()
    if not stats:
        return
    try:
        status = int(getattr(response, "status_code", 0) or 0)
        ok = bool(getattr(response, "ok", False))
        content = getattr(response, "content", b"") or b""
        bytes_in = len(content) if content is not None else 0

        if isinstance(payload, (bytes, bytearray)):
            bytes_out = len(payload)
        elif payload is None:
            bytes_out = 0
        else:
            try:
                bytes_out = len(json.dumps(payload))
            except Exception:
                bytes_out = 0

        ms = 0
        try:
            elapsed = getattr(response, "elapsed", None)
            if elapsed is not None:
                ms = int(elapsed.total_seconds() * 1000)
        except Exception:
            ms = 0

        rate_remaining: int | None = None
        rate_reset_iso: str | None = None
        try:
            hdr = getattr(response, "headers", {}) or {}
            rem = hdr.get("X-RateLimit-Remaining")
            rst = hdr.get("X-RateLimit-Reset")
            if rem is not None:
                rate_remaining = int(rem)
            if rst:
                rst_i = int(rst)
                rate_reset_iso = ts_to_iso(rst_i) if rst_i > 0 else None
        except Exception:
            pass

        stats.record_http(
            provider=str(provider),
            endpoint=str(endpoint),
            method=str(method).upper(),
            status=status,
            ok=ok,
            bytes_in=bytes_in,
            bytes_out=bytes_out,
            ms=ms,
            rate_remaining=rate_remaining,
            rate_reset_iso=rate_reset_iso,
            count=count,
        )
    except Exception:
        pass