# /providers/sync/_log.py
# CrossWatch  - logging utility
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

_LEVELS: dict[str, int] = {
    "off": 99,
    "error": 40,
    "warn": 30,
    "warning": 30,
    "info": 20,
    "debug": 10,
    "trace": 5,
}

RESET = "\033[0m"
DIM = "\033[90m"
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[33m"
BLUE = "\033[94m"

_LEVEL_COLOR: dict[str, str] = {
    "ERROR": RED,
    "WARN": YELLOW,
    "WARNING": YELLOW,
    "INFO": BLUE,
    "DEBUG": YELLOW,
    "TRACE": DIM,
    "SUCCESS": GREEN,
}


def _env_bool(name: str) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _use_color(fmt: str) -> bool:
    if fmt == "json":
        return False
    if os.getenv("NO_COLOR") is not None:
        return False

    mode = (os.getenv("CW_LOG_COLOR") or "auto").strip().lower()
    if mode in ("0", "false", "no", "off"):
        return False
    if mode in ("1", "true", "yes", "on"):
        return True

    return True


def _c(text: str, color: str, *, on: bool) -> str:
    if not on or not color:
        return text
    return f"{color}{text}{RESET}"


def _level_num(level: str) -> int:
    return _LEVELS.get(str(level or "info").strip().lower(), 20)


def _env_level(provider: str) -> int:
    p = str(provider).strip().upper()
    v = os.getenv(f"CW_{p}_LOG_LEVEL") or os.getenv("CW_LOG_LEVEL") or ""
    if v.strip():
        return _level_num(v)

    if _env_bool("CW_DEBUG") or _env_bool(f"CW_{p}_DEBUG"):
        return _level_num("debug")
    return _level_num("info")


def _one_line(s: Any) -> str:
    t = str(s if s is not None else "")
    return " ".join(t.replace("\n", " ").replace("\r", " ").split())


def _kv(fields: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for k in sorted(fields.keys()):
        v = fields[k]
        if v is None:
            continue
        vs = _one_line(v)
        if vs == "":
            continue

        if any(ch.isspace() for ch in vs) or any(ch in vs for ch in ['"', "=", ":"]):
            vs = json.dumps(vs, ensure_ascii=False)
        parts.append(f"{k}={vs}")
    return " ".join(parts)


def log(provider: str, feature: str, level: str, msg: str, **fields: Any) -> None:
    provider_s = str(provider).strip().upper()
    feature_s = str(feature).strip().lower()
    level_s = str(level).strip().upper()

    if _level_num(level_s) < _env_level(provider_s):
        return

    fmt = (os.getenv("CW_LOG_FORMAT") or "kv").strip().lower()
    use_color = _use_color(fmt)

    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    base = {
        "ts": ts,
        "provider": provider_s,
        "feature": feature_s,
        "level": level_s,
        "msg": _one_line(msg),
    }
    payload = {**base, **fields}

    if fmt == "json":
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return

    head = _c(f"[{base['provider']}:{base['feature']}]", DIM, on=use_color)
    lvl = _c(base["level"], _LEVEL_COLOR.get(base["level"], ""), on=use_color)

    tail = _kv(fields)
    line = f"{head} {lvl} {base['msg']}"
    if tail:
        line = f"{line} {tail}"
    print(line, flush=True)
