# /providers/sync/crosswatch/_common.py
# CrossWatch tracker Module shared helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import shutil
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any

try:
    from .._log import log as cw_log
except Exception:  # pragma: no cover
    def cw_log(provider: str, feature: str, level: str, msg: str, **fields: Any) -> None:  # type: ignore[no-redef]
        pass

from cw_platform.id_map import minimal as id_minimal, canonical_key


_PAIR_SCOPE_ENV: tuple[str, ...] = ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR")


def _truthy_env(name: str) -> bool:
    v = str(os.getenv(name, "")).strip().lower()
    return v in ("1", "true", "yes", "y", "on")


def pair_scoped() -> bool:
    return _truthy_env("CW_CROSSWATCH_PAIR_SCOPED")


def _capture_mode() -> bool:
    return _truthy_env("CW_CAPTURE_MODE")


def _pair_scope() -> str | None:
    if not pair_scoped():
        return "unscoped"

    for k in _PAIR_SCOPE_ENV:
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return "unscoped"


def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def scope_safe() -> str:
    scope = _pair_scope()
    return _safe_scope(scope) if scope else "unscoped"


def scoped_file(root: Path, name: str) -> Path:
    if not pair_scoped():
        return root / name

    safe = scope_safe()
    p = Path(name)

    if p.suffix:
        scoped = root / f"{p.stem}.{safe}{p.suffix}"
        legacy = root / f"{p.stem}{p.suffix}"
    else:
        scoped = root / f"{name}.{safe}"
        legacy = root / name

    # Auto-migrate legacy unscoped state to scoped file
    if not scoped.exists() and legacy.exists():
        try:
            root.mkdir(parents=True, exist_ok=True)
            shutil.copy2(legacy, scoped)
        except Exception:
            pass

    return scoped


def scoped_snapshots_dir(root: Path) -> Path:
    if not pair_scoped():
        return root / "snapshots"
    return root / "snapshots" / scope_safe()


def latest_state_file(root: Path, stem: str) -> Path | None:
    candidates: list[Path] = []
    legacy = root / f"{stem}.json"
    if legacy.exists() and legacy.is_file():
        candidates.append(legacy)

    for p in root.glob(f"{stem}.*.json"):
        if not p.is_file():
            continue
        parts = p.name.split(".")
        if len(parts) != 3:
            continue
        if parts[0] != stem or parts[-1] != "json":
            continue
        candidates.append(p)

    if not candidates:
        return None
    try:
        return max(candidates, key=lambda x: x.stat().st_mtime)
    except Exception:
        return candidates[-1]


def latest_snapshot_file(root: Path, feature: str) -> Path | None:
    snaps = root / "snapshots"
    if not snaps.exists() or not snaps.is_dir():
        return None

    candidates: list[Path] = []
    for p in snaps.rglob(f"*-{feature}.json"):
        if p.is_file():
            candidates.append(p)

    if not candidates:
        return None
    try:
        return max(candidates, key=lambda x: x.stat().st_mtime)
    except Exception:
        return candidates[-1]

# Logging factory

def make_logger(feature: str):  # type: ignore[return]
    def _dbg(msg: str, **fields: Any) -> None:
        cw_log("CROSSWATCH", feature, "debug", msg, **fields)

    def _info(msg: str, **fields: Any) -> None:
        cw_log("CROSSWATCH", feature, "info", msg, **fields)

    def _warn(msg: str, **fields: Any) -> None:
        cw_log("CROSSWATCH", feature, "warn", msg, **fields)

    def _error(msg: str, **fields: Any) -> None:
        cw_log("CROSSWATCH", feature, "error", msg, **fields)

    return _dbg, _info, _warn, _error


# Path helpers

def _root(adapter: Any) -> Path:
    base = getattr(getattr(adapter, "cfg", None), "base_path", None)
    if isinstance(base, Path):
        return base
    if isinstance(base, str) and base:
        return Path(base)
    return Path("/config/.cw_provider")


def _snapshot_dir(adapter: Any) -> Path:
    return scoped_snapshots_dir(_root(adapter))


def _unresolved_path(adapter: Any, stem: str) -> Path:
    return scoped_file(_root(adapter), f"{stem}.unresolved.json")


def _restore_state_path(adapter: Any, stem: str) -> Path:
    return scoped_file(_root(adapter), f"{stem}.restore_state.json")


# Write

def _atomic_write(path: Path, payload: Any) -> None:
    if _capture_mode() or _pair_scope() is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception as e:
        cw_log("CROSSWATCH", "common", "warn", "atomic_write_failed", path=str(path), error=str(e))


# Unresolved state

def _load_unresolved(adapter: Any, stem: str) -> dict[str, Any]:
    if _capture_mode() or _pair_scope() is None:
        return {}
    try:
        return json.loads(_unresolved_path(adapter, stem).read_text("utf-8"))
    except Exception:
        return {}


def _save_unresolved(adapter: Any, data: Mapping[str, Any], stem: str) -> None:
    if _capture_mode() or _pair_scope() is None:
        return
    _atomic_write(_unresolved_path(adapter, stem), dict(data or {}))


def _record_unresolved(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
    stem: str,
) -> list[dict[str, Any]]:
    existing = _load_unresolved(adapter, stem)
    bucket: dict[str, Any] = dict(existing.get("items") or {})
    out: list[dict[str, Any]] = []
    for obj in items:
        try:
            minimal = id_minimal(obj)
        except Exception:
            continue
        key = canonical_key(minimal) or f"obj:{hash(json.dumps(minimal, sort_keys=True))}"
        if key not in bucket:
            bucket[key] = minimal
        out.append(minimal)
    existing["items"] = bucket
    existing["ts"] = int(time.time())
    _save_unresolved(adapter, existing, stem)
    return out


# Snapshot management

def _list_snapshots(adapter: Any, feature: str) -> list[Path]:
    directory = _snapshot_dir(adapter)
    if not directory.exists() or not directory.is_dir():
        return []
    return sorted(
        [
            p
            for p in directory.iterdir()
            if p.is_file() and p.suffix == ".json" and p.name.endswith(f"-{feature}.json")
        ],
        key=lambda p: p.stat().st_mtime,
    )


def _apply_retention(adapter: Any, feature: str) -> None:
    if _capture_mode() or _pair_scope() is None:
        return
    cfg = getattr(adapter, "cfg", None)
    retention_days = int(getattr(cfg, "retention_days", 30) or 0)
    max_snapshots = int(getattr(cfg, "max_snapshots", 64) or 0)
    snaps = _list_snapshots(adapter, feature)
    if not snaps:
        return
    now = time.time()
    keep: list[Path] = []
    for path in snaps:
        try:
            age_days = (now - path.stat().st_mtime) / 86400.0
        except Exception:
            keep.append(path)
            continue
        if retention_days > 0 and age_days > retention_days:
            try:
                path.unlink()
                cw_log("CROSSWATCH", feature, "info", "snapshot_removed",
                       reason="retention", snapshot=path.name, retention_days=retention_days)
            except Exception as e:
                cw_log("CROSSWATCH", feature, "warn", "snapshot_remove_failed",
                       path=str(path), error=str(e))
        else:
            keep.append(path)
    if max_snapshots > 0 and len(keep) > max_snapshots:
        extra = len(keep) - max_snapshots
        for path in keep[:extra]:
            try:
                path.unlink()
                cw_log("CROSSWATCH", feature, "info", "snapshot_removed",
                       reason="max_snapshots", snapshot=path.name, max_snapshots=max_snapshots)
            except Exception as e:
                cw_log("CROSSWATCH", feature, "warn", "snapshot_remove_failed",
                       path=str(path), error=str(e))


def _snapshot_state(
    adapter: Any,
    items: Mapping[str, Any],
    feature: str,
    *,
    reuse_window: int = 0,
) -> None:
    if _capture_mode() or _pair_scope() is None:
        return
    cfg = getattr(adapter, "cfg", None)
    if not getattr(cfg, "auto_snapshot", True):
        return
    directory = _snapshot_dir(adapter)
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    now = time.time()
    path: Path

    if reuse_window > 0:
        snaps = _list_snapshots(adapter, feature)
        if snaps:
            last = snaps[-1]
            try:
                age = now - last.stat().st_mtime
            except Exception:
                age = None
            if age is not None and age <= reuse_window:
                path = last
            else:
                ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
                path = directory / f"{ts}-{feature}.json"
        else:
            ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            path = directory / f"{ts}-{feature}.json"
    else:
        ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        path = directory / f"{ts}-{feature}.json"

    payload = {"ts": int(now), "items": dict(items or {})}
    _atomic_write(path, payload)
    _apply_retention(adapter, feature)


def _maybe_restore(
    adapter: Any,
    feature: str,
    save_fn: Callable[[Any, Mapping[str, Any]], None],
) -> None:
    if _capture_mode():
        return
    cfg = getattr(adapter, "cfg", None)
    restore_id = getattr(cfg, f"restore_{feature}", None)
    if not restore_id:
        return

    marker_path = _restore_state_path(adapter, feature)
    last_id = ""
    try:
        raw = json.loads(marker_path.read_text("utf-8"))
        last_id = str(raw.get("last") or "")
    except Exception:
        last_id = ""

    if last_id and str(restore_id) == last_id:
        return

    snaps = _list_snapshots(adapter, feature)
    if not snaps:
        cw_log("CROSSWATCH", feature, "warn", "restore_failed", reason="no_snapshots")
        return

    chosen: Path | None = None
    restore_id_str = str(restore_id).strip()

    if restore_id_str.lower() in ("latest", "last"):
        chosen = snaps[-1]
    else:
        for path in snaps:
            if path.name == restore_id_str or path.stem == restore_id_str:
                chosen = path
                break

    if not chosen:
        cw_log("CROSSWATCH", feature, "warn", "restore_failed",
               reason="snapshot_not_found", restore_id=restore_id_str)
        return

    try:
        payload = json.loads(chosen.read_text("utf-8"))
        items = dict((payload.get("items") or {}) or {})
        save_fn(adapter, items)
        marker = {"last": restore_id_str, "ts": int(time.time()), "snapshot": chosen.name}
        _atomic_write(marker_path, marker)
        cw_log("CROSSWATCH", feature, "info", "restore_applied", snapshot=chosen.name)
    except Exception as e:
        cw_log("CROSSWATCH", feature, "warn", "restore_failed",
               reason="exception", snapshot=str(chosen), error=str(e))
