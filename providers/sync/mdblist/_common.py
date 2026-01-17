# /providers/sync/mdblist/_common.py
# Shared helpers for MDBList sync modules
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, TypeGuard

from .._log import log as cw_log

STATE_DIR = Path("/config/.cw_state")
WATERMARK_PATH = STATE_DIR / "mdblist.watermarks.json"
START_OF_TIME_ISO = "1970-01-01T00:00:00Z"

STATE_DIR.mkdir(parents=True, exist_ok=True)


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def _scoped_watermark_path(path: Path) -> Path:
    if path != WATERMARK_PATH:
        return path
    scope = _pair_scope()
    safe = _safe_scope(scope) if scope else "unscoped"
    return STATE_DIR / f"mdblist.watermarks.{safe}.json"


def state_file(name: str) -> Path:
    scope = _pair_scope()
    safe = _safe_scope(scope) if scope else "unscoped"
    p = Path(name)
    if p.suffix:
        return STATE_DIR / f"{p.stem}.{safe}{p.suffix}"
    return STATE_DIR / f"{name}.{safe}"



def make_logger(tag: str) -> Callable[[str], None]:
    # Back-compat helper. Prefer using cw_log directly with structured fields.
    def _log(msg: str) -> None:
        cw_log("MDBLIST", tag, "debug", msg)
    return _log


def cfg_section(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    if isinstance(cfg, dict) and isinstance(cfg.get("mdblist"), dict):
        return cfg["mdblist"]
    try:
        runtime_cfg = getattr(getattr(adapter, "cfg", None), "config", {}) or {}
        if isinstance(runtime_cfg, dict) and isinstance(runtime_cfg.get("mdblist"), dict):
            return runtime_cfg["mdblist"]
    except Exception:
        pass
    return {}


def cfg_int(data: Mapping[str, Any], key: str, default: int) -> int:
    raw = data.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def cfg_bool(data: Mapping[str, Any], key: str, default: bool) -> bool:
    raw = data.get(key, default)
    if isinstance(raw, bool):
        return raw
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _legacy_path(path: Path) -> Path | None:
    parts = path.stem.split(".")
    if len(parts) < 2:
        return None
    legacy_name = ".".join(parts[:-1]) + path.suffix
    legacy = path.with_name(legacy_name)
    return None if legacy == path else legacy


def _migrate_legacy_json(path: Path) -> None:
    if path.exists():
        return
    if _pair_scope() is None:
        return
    legacy = _legacy_path(path)
    if not legacy or not legacy.exists():
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_bytes(legacy.read_bytes())
        os.replace(tmp, path)
    except Exception:
        pass


def read_json(path: Path) -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    _migrate_legacy_json(path)
    try:
        return json.loads(path.read_text("utf-8") or "{}")
    except Exception:
        return {}


def write_json(path: Path, data: Mapping[str, Any], *, indent: int | None = 2, sort_keys: bool = True) -> None:
    if _pair_scope() is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        if indent is None:
            tmp.write_text(
                json.dumps(dict(data), ensure_ascii=False, separators=(",", ":"), sort_keys=sort_keys),
                "utf-8",
            )
        else:
            tmp.write_text(
                json.dumps(dict(data), ensure_ascii=False, indent=indent, sort_keys=sort_keys),
                "utf-8",
            )
        os.replace(tmp, path)
    except Exception:
        pass


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def iso_ok(value: Any) -> TypeGuard[str]:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def iso_z(value: str) -> str:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def as_epoch(iso: str) -> int | None:
    try:
        return int(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def as_iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def max_iso(a: str | None, b: str | None) -> str | None:
    if not iso_ok(a):
        return iso_z(b) if iso_ok(b) else None
    if not iso_ok(b):
        return iso_z(a)
    az = iso_z(a)
    bz = iso_z(b)
    ta = datetime.fromisoformat(az.replace("Z", "+00:00"))
    tb = datetime.fromisoformat(bz.replace("Z", "+00:00"))
    return az if ta >= tb else bz


def pad_since_iso(iso_ts: str, *, seconds: int = 2) -> str:
    ts = as_epoch(iso_ts)
    if ts is None:
        return iso_ts
    return as_iso(max(0, ts - max(0, int(seconds))))


def get_watermark(feature: str, *, path: Path = WATERMARK_PATH) -> str | None:
    if _pair_scope() is None:
        return None
    p = _scoped_watermark_path(path)
    data = read_json(p)
    v = data.get(feature)
    return iso_z(v) if iso_ok(v) else None


def save_watermark(feature: str, iso_ts: str, *, path: Path = WATERMARK_PATH) -> None:
    if _pair_scope() is None:
        return
    p = _scoped_watermark_path(path)
    data = read_json(p)
    data[feature] = iso_z(iso_ts)
    write_json(p, data)


def update_watermark_if_new(feature: str, iso_ts: str | None, *, path: Path = WATERMARK_PATH) -> str | None:
    if not iso_ok(iso_ts):
        return get_watermark(feature, path=path)
    current = get_watermark(feature, path=path)
    new = max_iso(current, iso_ts)
    if new and new != current:
        save_watermark(feature, new, path=path)
    return new


def coalesce_since(
    feature: str,
    cfg_since: str | None = None,
    *,
    env_any: str | None = None,
    start: str = START_OF_TIME_ISO,
    watermark_path: Path = WATERMARK_PATH,
) -> str:
    env_feature = os.getenv(f"MDBLIST_{feature.upper()}_SINCE")
    env_any_val = os.getenv(env_any) if env_any else None
    for candidate in (get_watermark(feature, path=watermark_path), env_feature, env_any_val, cfg_since, start):
        if iso_ok(candidate):
            return iso_z(candidate)
    return start


__all__ = [
    "STATE_DIR",
    "WATERMARK_PATH",
    "state_file",
    "START_OF_TIME_ISO",
    "make_logger",
    "cfg_section",
    "cfg_int",
    "cfg_bool",
    "read_json",
    "write_json",
    "now_iso",
    "iso_ok",
    "iso_z",
    "as_epoch",
    "as_iso",
    "max_iso",
    "pad_since_iso",
    "get_watermark",
    "save_watermark",
    "update_watermark_if_new",
    "coalesce_since",
]
