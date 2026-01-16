# /providers/sync/_log.py
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Mapping

_LEVELS = {"off": 99, "error": 40, "warn": 30, "warning": 30, "info": 20, "debug": 10, "trace": 5}

def _level_num(level: str) -> int:
    return _LEVELS.get(str(level or "info").strip().lower(), 20)

def _env_level(provider: str) -> int:
    p = str(provider).strip().upper()
    v = os.getenv(f"CW_{p}_LOG_LEVEL") or os.getenv("CW_LOG_LEVEL") or ""
    if v.strip():
        return _level_num(v)

    if os.getenv("CW_DEBUG") or os.getenv(f"CW_{p}_DEBUG"):
        return _level_num("debug")
    return _level_num("off")

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
    if _level_num(level) < _env_level(provider):
        return

    fmt = (os.getenv("CW_LOG_FORMAT") or "kv").strip().lower()
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    base = {
        "ts": ts,
        "provider": str(provider),
        "feature": str(feature),
        "level": str(level).upper(),
        "msg": _one_line(msg),
    }
    payload = {**base, **fields}

    if fmt == "json":
        print(json.dumps(payload, ensure_ascii=False), flush=True)
        return

    tail = _kv(fields)
    line = f"[{base['provider']}:{base['feature']}] {base['level']} {base['msg']}"
    if tail:
        line = f"{line} {tail}"
    print(line, flush=True)
