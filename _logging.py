# _logging.py
# CrossWatch - Logging module
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import datetime
import json
import os
import sys
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any, Optional, TextIO

RESET = "\033[0m"
DIM = "\033[90m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[33m"
BLUE = "\033[94m"

LEVELS: dict[str, int] = {
    "silent": 60,
    "error": 40,
    "warn": 30,
    "info": 20,
    "debug": 10,
}

_CFG_CACHE: dict[str, Any] | None = None
_CFG_TS: float = 0.0
_CFG_TTL: float = 5.0


def _debug_enabled() -> bool:
    global _CFG_CACHE, _CFG_TS
    now = time.time()
    if _CFG_CACHE is None or (now - _CFG_TS) > _CFG_TTL:
        try:
            from cw_platform.config_base import load_config  # type: ignore

            _CFG_CACHE = load_config()
        except Exception:
            _CFG_CACHE = {}
        _CFG_TS = now
    runtime_cfg = (_CFG_CACHE.get("runtime") or {}) if isinstance(_CFG_CACHE, dict) else {}
    return bool(runtime_cfg.get("debug") or runtime_cfg.get("debug_mods"))


def _decide_use_color(requested: bool) -> bool:
    if not requested:
        return False
    if os.getenv("NO_COLOR") is not None:
        return False

    fmt = (os.getenv("CW_LOG_FORMAT") or "").strip().lower()
    if fmt == "json":
        return False

    mode = (os.getenv("CW_LOG_COLOR") or "auto").strip().lower()
    if mode in ("0", "false", "no", "off"):
        return False
    if mode in ("1", "true", "yes", "on"):
        return True

    return True


LogHook = Callable[[dict[str, Any], str, str, str, Mapping[str, Any] | None], None]


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
        _context: Optional[dict[str, Any]] = None,
        _name: Optional[str] = None,
        _json_stream: Optional[TextIO] = None,
        _lock: Optional[threading.Lock] = None,
        _hook: Optional[LogHook] = None,
        _color_requested: Optional[bool] = None,
    ) -> None:
        self.stream = stream
        self.level_no = LEVELS.get(level, 20)

        self._color_requested = use_color if _color_requested is None else _color_requested
        self.use_color = _decide_use_color(self._color_requested)

        self.show_time = show_time
        self.time_fmt = time_fmt
        self.tag_color_map: dict[str, str] = tag_color_map or {
            "DEBUG": YELLOW,
            "INFO": BLUE,
            "WARN": YELLOW,
            "ERROR": RED,
            "SUCCESS": GREEN,
            "WebHook": GREEN,
        }
        self._context: dict[str, Any] = dict(_context or {})
        if _name:
            self._context.setdefault("module", _name)
        self._json_stream: Optional[TextIO] = _json_stream
        self._lock: threading.Lock = _lock or threading.Lock()
        self._hook: Optional[LogHook] = _hook

    def set_level(self, level: str) -> None:
        self.level_no = LEVELS.get(level, self.level_no)

    def enable_color(self, on: bool = True) -> None:
        self._color_requested = on
        self.use_color = _decide_use_color(on)

    def enable_time(self, on: bool = True) -> None:
        self.show_time = on

    def enable_json(self, file_path: str) -> None:
        self._json_stream = open(file_path, "a", encoding="utf-8")

    def set_hook(self, hook: Optional[LogHook]) -> None:
        self._hook = hook

    def set_context(self, **ctx: Any) -> None:
        self._context.update(ctx)

    def get_context(self) -> dict[str, Any]:
        return dict(self._context)

    def bind(self, **ctx: Any) -> Logger:
        new_ctx = dict(self._context)
        new_ctx.update(ctx)
        return Logger(
            stream=self.stream,
            level=self.level_name,
            use_color=self._color_requested,
            show_time=self.show_time,
            time_fmt=self.time_fmt,
            tag_color_map=dict(self.tag_color_map),
            _context=new_ctx,
            _name=new_ctx.get("module"),
            _json_stream=self._json_stream,
            _lock=self._lock,
            _hook=self._hook,
            _color_requested=self._color_requested,
        )

    def child(self, name: str) -> Logger:
        return self.bind(module=name)

    @property
    def level_name(self) -> str:
        for k, v in LEVELS.items():
            if v == self.level_no:
                return k
        return "info"

    def _fmt_text(
        self,
        display_level: str,
        *parts: Any,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> str:
        module_name = (self._context.get("module") or "").strip()
        lvl = display_level
        msg = " ".join(str(p) for p in parts)

        hide_boot_info = (
            module_name.upper() == "BOOT"
            and lvl.upper() == "INFO"
            and (os.getenv("CW_BOOT_SHOW_LEVEL") or "").strip().lower() not in ("1", "true", "yes", "on")
        )

        col: Optional[str]
        if self.use_color:
            col = self.tag_color_map.get(lvl) or self.tag_color_map.get(lvl.upper())
        else:
            col = None

        head = f"[{module_name}]" if module_name else ""

        if hide_boot_info:
            line = "" if not msg.strip() else f"{head} {msg}".strip()
        else:
            lvl_disp = f"{col}{lvl}{RESET}" if (col and self.use_color) else lvl
            line = f"{head} {lvl_disp} {msg}".strip()

        if self.show_time:
            if not line:
                return ""
            ts = datetime.datetime.now().strftime(self.time_fmt)
            prefix = f"{DIM}[{ts}]{RESET}" if self.use_color else f"[{ts}]"
            return f"{prefix} {line}"
        return line

    def _write_sinks(
        self,
        display_level: str,
        message_text: str,
        *,
        msg: str,
        extra: Optional[Mapping[str, Any]],
    ) -> None:
        with self._lock:
            self.stream.write(message_text + "\n")
            self.stream.flush()
            if self._json_stream:
                payload: dict[str, Any] = {
                    "ts": datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
                    "level": display_level,
                    "msg": msg,
                    "ctx": self._context or {},
                }
                if extra:
                    payload["extra"] = dict(extra)
                self._json_stream.write(json.dumps(payload, ensure_ascii=False) + "\n")
                self._json_stream.flush()

        hook = self._hook
        if hook:
            try:
                hook(dict(self._context or {}), display_level, msg, message_text, extra)
            except Exception:
                pass

    def _emit(
        self,
        severity: str,
        display_level: str,
        *parts: Any,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None:
        sev_no = LEVELS.get(severity, LEVELS["info"])
        if severity == "debug":
            if not _debug_enabled():
                return
        else:
            if self.level_no > sev_no:
                return

        self.use_color = _decide_use_color(bool(self._color_requested))

        line = self._fmt_text(display_level, *parts, extra=extra)
        self._write_sinks(
            display_level,
            line,
            msg=" ".join(str(p) for p in parts),
            extra=extra,
        )

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

    def __call__(
        self,
        message: str,
        *,
        level: str = "INFO",
        module: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
    ) -> None:
        logger: Logger = self
        if module:
            logger = self.bind(module=module)
        lvl_in = level or "INFO"
        lvl_lc = lvl_in.lower()

        if lvl_lc == "debug":
            logger.debug(message, extra=extra)
        elif lvl_lc in ("warn", "warning"):
            logger.warn(message, extra=extra)
        elif lvl_lc == "error":
            logger.error(message, extra=extra)
        elif lvl_lc == "success":
            logger.success(message, extra=extra)
        elif lvl_lc == "info":
            logger.info(message, extra=extra)
        else:
            logger._emit("info", lvl_in, message, extra=extra)


log = Logger(show_time=False)

__all__ = ["Logger", "log", "LEVELS", "RESET", "DIM", "RED", "GREEN", "YELLOW", "BLUE"]
