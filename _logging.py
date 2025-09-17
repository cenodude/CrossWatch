"""Lightweight logging utilities for CrossWatch.

This module provides a fast, minimal stdout logger with:
- ANSI colorized tags for readability (optional)
- Timestamp prefixes (optional)
- Structured context binding (module/name and arbitrary fields)
- Optional newline-delimited JSON sink for file-based log collection
- A callable adapter so code can use `log("msg", level="INFO", extra={...})`

Behavior and identifiers are unchanged; comments and docstrings were polished
for clarity and consistency.
"""

from __future__ import annotations
import sys, datetime, json, threading
from typing import Any, Optional, TextIO, Mapping, Dict

RESET = "\033[0m"
DIM = "\033[90m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[33m"
BLUE = "\033[94m"

LEVELS = {"silent": 60, "error": 40, "warn": 30, "info": 20, "debug": 10}
LEVEL_TAG = {"debug": "[debug]", "info": "[i]", "warn": "[!]", "error": "[!]", "success": "[✓]"}

class Logger:
    """Minimal, fast stdout logger with optional JSON sink and context binding.

    Designed for low overhead and simple integration:
    - `debug/info/warn/error/success` methods format and write to stdout
    - Optional JSON sink writes one JSON object per line for easy ingestion
    - Context helpers (`set_context`, `bind`, `child`) attach structured fields
    - Callable adapter supports `log("text", level="INFO", extra={...})`
    """
    def __init__(
        self,
        stream: TextIO = sys.stdout,
        level: str = "info",
        use_color: bool = True,
        show_time: bool = True,
        time_fmt: str = "%Y-%m-%d %H:%M:%S",
        tag_color_map: Optional[dict[str, str]] = None,
        *,
        _context: Optional[Dict[str, Any]] = None,
        _name: Optional[str] = None,
        _json_stream: Optional[TextIO] = None,
        _lock: Optional[threading.Lock] = None,
    ):
        self.stream = stream
        self.level_no = LEVELS.get(level, 20)
        self.use_color = use_color
        self.show_time = show_time
        self.time_fmt = time_fmt
        self.tag_color_map = tag_color_map or {
            "[i]": BLUE,
            "[debug]": YELLOW,
            "[✓]": GREEN,
            "[!]": RED,
        }
        self._context: Dict[str, Any] = dict(_context or {})
        if _name:
            self._context.setdefault("module", _name)
        self._json_stream: Optional[TextIO] = _json_stream
        self._lock = _lock or threading.Lock()

    # Configuration
    def set_level(self, level: str) -> None:
        """Set the log level (silent|error|warn|info|debug)."""
        self.level_no = LEVELS.get(level, self.level_no)

    def enable_color(self, on: bool = True) -> None:
        """Toggle ANSI colors for tag and timestamp rendering."""
        self.use_color = on

    def enable_time(self, on: bool = True) -> None:
        """Toggle timestamp prefixes in human-readable output."""
        self.show_time = on

    def enable_json(self, file_path: str) -> None:
        """Enable newline-delimited JSON logging to the given file path."""
        self._json_stream = open(file_path, "a", encoding="utf-8")

    # Context
    def set_context(self, **ctx: Any) -> None:
        """Merge fields into the logger's structured context."""
        self._context.update(ctx)

    def get_context(self) -> Dict[str, Any]:
        """Return a copy of the current structured context."""
        return dict(self._context)

    def bind(self, **ctx: Any) -> "Logger":
        """Create a child logger with additional context fields merged in."""
        new_ctx = dict(self._context); new_ctx.update(ctx)
        return Logger(
            stream=self.stream,
            level=self.level_name,
            use_color=self.use_color,
            show_time=self.show_time,
            time_fmt=self.time_fmt,
            tag_color_map=dict(self.tag_color_map),
            _context=new_ctx,
            _name=new_ctx.get("module"),
            _json_stream=self._json_stream,
            _lock=self._lock,
        )

    def child(self, name: str) -> "Logger":
        """Create a namespaced child logger (adds module=name to context)."""
        return self.bind(module=name)

    # Formatting
    @property
    def level_name(self) -> str:
        """Return the canonical name of the current log level."""
        for k, v in LEVELS.items():
            if v == self.level_no:
                return k
        return "info"

    def _fmt_text(self, level: str, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> str:
        """Compose a human-readable line with optional color and timestamp."""
        tag = LEVEL_TAG.get(level, "[i]")
        msg = " ".join(str(p) for p in (tag, *parts))
        if self.use_color:
            for t, col in self.tag_color_map.items():
                msg = msg.replace(t, f"{col}{t}{RESET}")
        if self.show_time:
            ts = datetime.datetime.now().strftime(self.time_fmt)
            prefix = f"{DIM}[{ts}]{RESET}" if self.use_color else f"[{ts}]"
            return f"{prefix} {msg}"
        return msg

    def _write_sinks(self, level: str, message_text: str, *, msg: str, extra: Optional[Mapping[str, Any]]) -> None:
        """Write the formatted line to stdout and, if enabled, JSON to file."""
        with self._lock:
            self.stream.write(message_text + "\n")
            self.stream.flush()
            if self._json_stream:
                payload = {
                    "ts": datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                    "level": level,
                    "msg": msg,
                    "ctx": self._context or {},
                }
                if extra:
                    payload["extra"] = dict(extra)
                self._json_stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
                self._json_stream.flush()

    # Public API
    def debug(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        """Log a debug-level message."""
        if self.level_no <= LEVELS["debug"]:
            s = self._fmt_text("debug", *parts, extra=extra)
            self._write_sinks("debug", s, msg=" ".join(str(p) for p in parts), extra=extra)

    def info(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        """Log an info-level message."""
        if self.level_no <= LEVELS["info"]:
            s = self._fmt_text("info", *parts, extra=extra)
            self._write_sinks("info", s, msg=" ".join(str(p) for p in parts), extra=extra)

    def warn(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        """Log a warning-level message."""
        if self.level_no <= LEVELS["warn"]:
            s = self._fmt_text("warn", *parts, extra=extra)
            self._write_sinks("warn", s, msg=" ".join(str(p) for p in parts), extra=extra)

    # Alias for libraries that call .warning
    def warning(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        """Alias for warn()."""
        self.warn(*parts, extra=extra)

    def error(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        """Log an error-level message."""
        if self.level_no <= LEVELS["error"]:
            s = self._fmt_text("error", *parts, extra=extra)
            self._write_sinks("error", s, msg=" ".join(str(p) for p in parts), extra=extra)

    def success(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        """Log a success message (styled like info)."""
        if self.level_no <= LEVELS["info"]:
            s = self._fmt_text("success", *parts, extra=extra)
            self._write_sinks("info", s, msg=" ".join(str(p) for p in parts), extra=extra)

    # Callable adapter: logger("text", level="INFO", extra={...})
    def __call__(
        self,
        message: str,
        *,
        level: str = "INFO",
        module: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Call-style logging entry point used across the codebase.

        Parameters
        - message: text to log
        - level: case-insensitive level name (DEBUG|INFO|WARN|ERROR)
        - module: optional logical name added to context for this call
        - extra: optional mapping of structured metadata to include
        """
        if module:
            self = self.bind(module=module)
        lvl = (level or "INFO").lower()
        if   lvl == "debug":   self.debug(message, extra=extra)
        elif lvl in ("warn", "warning"): self.warn(message, extra=extra)
        elif lvl == "error":   self.error(message, extra=extra)
        else:                  self.info(message, extra=extra)

# default instance
log = Logger()

__all__ = ["Logger", "log", "LEVELS", "RESET", "DIM", "RED", "GREEN", "YELLOW", "BLUE"]
