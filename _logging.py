# _logging.py
# A simple structured logger with colored console output and optional JSON file output.
from __future__ import annotations
import sys, datetime, json, threading, time
from typing import Any, Optional, TextIO, Mapping, Dict

RESET = "\033[0m"
DIM = "\033[90m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[33m"
BLUE = "\033[94m"

LEVELS = {"silent": 60, "error": 40, "warn": 30, "info": 20, "debug": 10}

# ── runtime debug gate (reads config.json, cached briefly) ────────────────
_CFG_CACHE: Dict[str, Any] | None = None
_CFG_TS: float = 0.0

def _debug_enabled() -> bool:
    global _CFG_CACHE, _CFG_TS
    now = time.time()
    if _CFG_CACHE is None or (now - _CFG_TS) > 5.0:
        try:
            with open("config.json", "r", encoding="utf-8") as f:
                _CFG_CACHE = json.load(f)
        except Exception:
            _CFG_CACHE = {}
        _CFG_TS = now
    rt = (_CFG_CACHE.get("runtime") or {})
    return bool(rt.get("debug") or rt.get("debug_mods"))

class Logger:
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
        # colors applied to the level label (optional)
        self.tag_color_map = tag_color_map or {
            "DEBUG": YELLOW,
            "INFO": BLUE,
            "WARN": YELLOW,
            "ERROR": RED,
            "SUCCESS": GREEN,
            "WebHook": GREEN,
        }
        self._context: Dict[str, Any] = dict(_context or {})
        if _name:
            self._context.setdefault("module", _name)
        self._json_stream: Optional[TextIO] = _json_stream
        self._lock = _lock or threading.Lock()

    # Configuration
    def set_level(self, level: str) -> None:
        self.level_no = LEVELS.get(level, self.level_no)

    def enable_color(self, on: bool = True) -> None:
        self.use_color = on

    def enable_time(self, on: bool = True) -> None:
        self.show_time = on

    def enable_json(self, file_path: str) -> None:
        self._json_stream = open(file_path, "a", encoding="utf-8")

    # Context
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

    # Formatting
    @property
    def level_name(self) -> str:
        for k, v in LEVELS.items():
            if v == self.level_no:
                return k
        return "info"

    def _fmt_text(self, display_level: str, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> str:
        # Compose "[MODULE] Level message"
        mod = (self._context.get("module") or "").strip()
        lvl = display_level  # preserve original case (e.g., "WebHook")
        msg = " ".join(str(p) for p in parts)

        if self.use_color:
            col = self.tag_color_map.get(lvl) or self.tag_color_map.get(lvl.upper())
        else:
            col = None

        lvl_disp = f"{col}{lvl}{RESET}" if (col and self.use_color) else lvl
        head = f"[{mod}]" if mod else ""
        line = f"{head} {lvl_disp} {msg}".strip()

        if self.show_time:
            ts = datetime.datetime.now().strftime(self.time_fmt)
            prefix = f"{DIM}[{ts}]{RESET}" if self.use_color else f"[{ts}]"
            return f"{prefix} {line}"
        return line

    def _write_sinks(self, display_level: str, message_text: str, *, msg: str, extra: Optional[Mapping[str, Any]]) -> None:
        with self._lock:
            self.stream.write(message_text + "\n")
            self.stream.flush()
            if self._json_stream:
                payload = {
                    "ts": datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                    "level": display_level,  # keep the original label
                    "msg": msg,
                    "ctx": self._context or {},
                }
                if extra:
                    payload["extra"] = dict(extra)
                self._json_stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
                self._json_stream.flush()

    # Core emission with separate severity vs. display label
    def _emit(self, severity: str, display_level: str, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        sev_no = LEVELS.get(severity, LEVELS["info"])
        if severity == "debug":
            if not _debug_enabled():
                return
        else:
            if self.level_no > sev_no:
                return
        s = self._fmt_text(display_level, *parts, extra=extra)
        self._write_sinks(display_level, s, msg=" ".join(str(p) for p in parts), extra=extra)

    # Public API
    def debug(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        self._emit("debug", "DEBUG", *parts, extra=extra)

    def info(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        self._emit("info", "INFO", *parts, extra=extra)

    def warn(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        self._emit("warn", "WARN", *parts, extra=extra)

    def warning(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        self.warn(*parts, extra=extra)

    def error(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        self._emit("error", "ERROR", *parts, extra=extra)

    def success(self, *parts: Any, extra: Optional[Mapping[str, Any]] = None) -> None:
        self._emit("info", "SUCCESS", *parts, extra=extra)

    # Callable adapter: logger("text", level="INFO", extra={...})
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
        lvl_in = level or "INFO"
        lvl_lc = lvl_in.lower()

        if lvl_lc == "debug":
            self.debug(message, extra=extra)
        elif lvl_lc in ("warn", "warning"):
            self.warn(message, extra=extra)
        elif lvl_lc == "error":
            self.error(message, extra=extra)
        elif lvl_lc == "success":
            self.success(message, extra=extra)
        elif lvl_lc in ("info",):
            self.info(message, extra=extra)
        else:
            # Custom label (e.g., "WebHook"): treat as info severity but preserve label text
            self._emit("info", lvl_in, message, extra=extra)

# default instance
log = Logger()

__all__ = ["Logger", "log", "LEVELS", "RESET", "DIM", "RED", "GREEN", "YELLOW", "BLUE"]
