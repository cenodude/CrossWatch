# /providers/sync/crosswatch/_common.py
# CrossWatch tracker Module shared helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
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
from cw_platform.tracker_store import RestorePoint, TrackerStore, TrackerUnreadable


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


def chained_source() -> bool:
    if not pair_scoped():
        return True
    return str(os.getenv("CW_PAIR_SRC") or "").strip().upper() == "CROSSWATCH"


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


_IDENTITY_MAP_FIELDS: tuple[str, ...] = ("ids", "show_ids")
_PASSTHROUGH_FIELDS: tuple[str, ...] = (
    "_anime_absolute",
    "_cw_anime_map",
    "_simkl_episode_number",
    "_trakt_number_abs",
    "anime_type",
    "simkl_bucket",
)


def _clean_identity_map(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, Any] = {}
    for key, item in value.items():
        name = str(key or "").strip().lower()
        if not name or item in (None, "") or isinstance(item, bool):
            continue
        if isinstance(item, (Mapping, list, tuple, set)):
            continue
        text = str(item).strip()
        if text:
            out[name] = text
    return out


def preserve_tracker_identity(base: Mapping[str, Any], source: Mapping[str, Any]) -> dict[str, Any]:
    """Keep CrossWatch useful as a chained provider by preserving source identity maps."""
    out = dict(base or {})
    for field in _IDENTITY_MAP_FIELDS:
        raw = _clean_identity_map(source.get(field))
        current = _clean_identity_map(out.get(field))
        merged = dict(raw)
        merged.update(current)
        if merged:
            out[field] = merged
        else:
            out.pop(field, None)
    for field in _PASSTHROUGH_FIELDS:
        value = source.get(field)
        if value not in (None, ""):
            out[field] = value
    return out


def tracker_minimal(item: Mapping[str, Any]) -> dict[str, Any]:
    return preserve_tracker_identity(id_minimal(item), item)


def merge_tracker_identity(existing: Mapping[str, Any] | None, incoming: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(existing, Mapping):
        return dict(incoming or {})
    out = dict(incoming or {})
    for field in _IDENTITY_MAP_FIELDS:
        merged = _clean_identity_map(existing.get(field))
        merged.update(_clean_identity_map(out.get(field)))
        if merged:
            out[field] = merged
    for field in _PASSTHROUGH_FIELDS:
        if out.get(field) in (None, "") and existing.get(field) not in (None, ""):
            out[field] = existing.get(field)
    return out


# Path helpers

def _root(adapter: Any) -> Path:
    base = getattr(getattr(adapter, "cfg", None), "base_path", None)
    if isinstance(base, Path):
        return base
    if isinstance(base, str) and base:
        return Path(base)
    return Path("/config/.cw_provider")


def _store_log(feature: str, level: str, msg: str, **fields: Any) -> None:
    cw_log("CROSSWATCH", feature, level, msg, **fields)


def tracker_store(adapter: Any) -> TrackerStore:
    return TrackerStore(
        _root(adapter),
        scope=scope_safe() if pair_scoped() else None,
        chained=chained_source(),
        log=_store_log,
    )


def readonly(adapter: Any) -> bool:
    cfg = getattr(adapter, "config", None)
    return bool(isinstance(cfg, Mapping) and cfg.get("_cw_readonly"))


def current_state_only(adapter: Any) -> bool:
    cfg = getattr(adapter, "config", None)
    return bool(isinstance(cfg, Mapping) and cfg.get("_cw_current_state_only"))


def read_state(adapter: Any, feature: str, *, snapshot_fallback: bool = True) -> Any | None:
    try:
        found = tracker_store(adapter).read_state(feature, snapshot_fallback=snapshot_fallback)
    except TrackerUnreadable as exc:
        cw_log("CROSSWATCH", feature, "warn", "state_unreadable", files=exc.names)
        raise
    if found is None:
        return None
    if found.restored_from:
        cw_log("CROSSWATCH", feature, "warn", "state_restored_from_snapshot", snapshot=found.restored_from)
    return found.raw


def write_state(adapter: Any, feature: str, items: Mapping[str, Any], *, ts: int | None = None) -> None:
    if _capture_mode() or _pair_scope() is None:
        return
    tracker_store(adapter).write_state(feature, items, ts=ts)


def may_persist(adapter: Any, feature: str) -> bool:
    return not readonly(adapter) and not pair_scoped() and not tracker_store(adapter).has_state(feature)


# Unresolved state

def _load_unresolved(adapter: Any, stem: str) -> dict[str, Any]:
    if _capture_mode() or _pair_scope() is None:
        return {}
    raw = tracker_store(adapter).read_meta(f"{stem}.unresolved")
    return raw if isinstance(raw, dict) else {}


def _save_unresolved(adapter: Any, data: Mapping[str, Any], stem: str) -> None:
    if _capture_mode() or _pair_scope() is None:
        return
    tracker_store(adapter).write_meta(f"{stem}.unresolved", dict(data or {}))


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
    tracker_store(adapter).add_restore_point(
        feature,
        items,
        reuse_window=reuse_window,
        retention_days=int(getattr(cfg, "retention_days", 30) or 0),
        max_snapshots=int(getattr(cfg, "max_snapshots", 14) or 0),
    )


def _maybe_restore(
    adapter: Any,
    feature: str,
    save_fn: Callable[[Any, Mapping[str, Any]], None],
) -> None:
    if _capture_mode() or readonly(adapter):
        return
    cfg = getattr(adapter, "cfg", None)
    restore_id = getattr(cfg, f"restore_{feature}", None)
    if not restore_id:
        return

    store = tracker_store(adapter)
    marker = store.read_meta(f"{feature}.restore_state")
    last_id = str(marker.get("last") or "") if isinstance(marker, Mapping) else ""
    if last_id and str(restore_id) == last_id:
        return

    points = store.restore_points(feature)
    if not points:
        cw_log("CROSSWATCH", feature, "warn", "restore_failed", reason="no_snapshots")
        return

    restore_id_str = str(restore_id).strip()
    chosen: RestorePoint | None = None
    if restore_id_str.lower() in ("latest", "last"):
        chosen = points[-1]
    else:
        chosen = next((p for p in points if restore_id_str in (p.name, Path(p.name).stem)), None)

    if not chosen:
        cw_log("CROSSWATCH", feature, "warn", "restore_failed",
               reason="snapshot_not_found", restore_id=restore_id_str)
        return

    try:
        payload = store.read_restore_point(feature, chosen.name)
        items = dict((payload.get("items") or {}) or {})
        save_fn(adapter, items)
        store.write_meta(f"{feature}.restore_state", {"last": restore_id_str, "ts": int(time.time()), "snapshot": chosen.name})
        cw_log("CROSSWATCH", feature, "info", "restore_applied", snapshot=chosen.name)
    except Exception as e:
        cw_log("CROSSWATCH", feature, "warn", "restore_failed",
               reason="exception", snapshot=chosen.name, error=str(e))
