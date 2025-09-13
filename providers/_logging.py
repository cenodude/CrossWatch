# modules/_logging.py
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
    """Minimal, fast stdout logger with optional JSON file sink and context binding."""
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

    # ----- config
    def set_level(self, level: str) -> None:
        self.level_no = LEVELS.get(level, self.level_no)

    def enable_color(self, on: bool = True) -> None:
        self.use_color = on

    def enable_time(self, on: bool = True) -> None:
        self.show_time = on

    def enable_json(self, file_path: str) -> None:
        self._json_stream = open(file_path, "a", encoding="utf-8")

    # ----- context
    def set_context(self, **ctx: Any) -> None:
        self._context.update(ctx)

    def get_context(self) -> Dict[str, Any]:
        return dict(self._context)

    def bind(self, **ctx: Any) -> "Logger":
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
        return self.bind(module=name)

    # ----- formatters
    @property
    def level_name(self) -> str:
        for k, v in LEVELS.items():
            if v == self.level_no:
                return k
        return "info"

    def _fmt_text(self, level: str, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> str:
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

    # ----- public API
    def debug(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        if self.level_no <= LEVELS["debug"]:
            s = self._fmt_text("debug", *parts, extra=extra)
            self._write_sinks("debug", s, msg=" ".join(str(p) for p in parts), extra=extra)

    def info(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        if self.level_no <= LEVELS["info"]:
            s = self._fmt_text("info", *parts, extra=extra)
            self._write_sinks("info", s, msg=" ".join(str(p) for p in parts), extra=extra)

    def warn(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        if self.level_no <= LEVELS["warn"]:
            s = self._fmt_text("warn", *parts, extra=extra)
            self._write_sinks("warn", s, msg=" ".join(str(p) for p in parts), extra=extra)

    # alias for libraries that call .warning
    def warning(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        self.warn(*parts, extra=extra)

    def error(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        if self.level_no <= LEVELS["error"]:
            s = self._fmt_text("error", *parts, extra=extra)
            self._write_sinks("error", s, msg=" ".join(str(p) for p in parts), extra=extra)

    def success(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        if self.level_no <= LEVELS["info"]:
            s = self._fmt_text("success", *parts, extra=extra)
            self._write_sinks("info", s, msg=" ".join(str(p) for p in parts), extra=extra)

    # callable adapter: logger("text", level="INFO", extra={...})
    def __call__(
        self,
        message: str,
        *,
        level: str = "INFO",
        module: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None:
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
