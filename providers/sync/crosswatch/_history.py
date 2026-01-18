# /providers/sync/crosswatch/_history.py
# CrossWatch tracker Module for History Management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

try:
    from .._log import log as cw_log
except Exception:  # pragma: no cover
    def cw_log(provider: str, feature: str, level: str, msg: str, **fields: Any) -> None:  # type: ignore[no-redef]
        pass

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import scoped_file, scoped_snapshots_dir, _pair_scope


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("CROSSWATCH", "history", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("CROSSWATCH", "history", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("CROSSWATCH", "history", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    cw_log("CROSSWATCH", "history", "error", msg, **fields)


def _root(adapter: Any) -> Path:
    base = getattr(getattr(adapter, "cfg", None), "base_path", None)
    if isinstance(base, Path):
        return base
    if isinstance(base, str) and base:
        return Path(base)
    return Path("/config/.cw_provider")


def _history_path(adapter: Any) -> Path:
    return scoped_file(_root(adapter), "history.json")


def _snapshot_dir(adapter: Any) -> Path:
    return scoped_snapshots_dir(_root(adapter))


def _unresolved_path(adapter: Any) -> Path:
    return scoped_file(_root(adapter), "history.unresolved.json")


def _restore_state_path(adapter: Any) -> Path:
    return scoped_file(_root(adapter), "history.restore_state.json")

def _accepted(obj: Mapping[str, Any]) -> dict[str, Any]:
    base = id_minimal(obj)
    out: dict[str, Any] = dict(base)
    if obj.get("watched") is not None:
        out["watched"] = bool(obj.get("watched"))
    wa = obj.get("watched_at")
    if wa:
        out["watched_at"] = str(wa)
    for k in ("title", "year", "season", "episode", "series_title", "series_year"):
        if k in obj:
            out[k] = obj.get(k)
    si = obj.get("show_ids")
    if isinstance(si, Mapping):
        out["show_ids"] = dict(si)
    return out


def _load_state(adapter: Any) -> dict[str, Any]:
    if _pair_scope() is None:
        return {"ts": 0, "items": {}}
    path = _history_path(adapter)
    try:
        raw = json.loads(path.read_text("utf-8"))
    except Exception:
        return {"ts": 0, "items": {}}

    if isinstance(raw, list):
        items: dict[str, dict[str, Any]] = {}
        for obj in raw:
            if not isinstance(obj, Mapping):
                continue
            key = canonical_key(obj)
            if not key:
                continue
            items[key] = _accepted(obj)
        return {"ts": 0, "items": items}

    if isinstance(raw, Mapping):
        if "items" in raw and isinstance(raw.get("items"), Mapping):
            ts = int(raw.get("ts", 0) or 0)
            items_raw = raw.get("items") or {}
            items: dict[str, dict[str, Any]] = {}
            for key, value in items_raw.items():
                if not isinstance(value, Mapping):
                    continue
                ck = str(key) or canonical_key(value)
                if not ck:
                    continue
                items[ck] = _accepted(value)
            return {"ts": ts, "items": items}

        items: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            if not isinstance(value, Mapping):
                continue
            ck = str(key) or canonical_key(value)
            if not ck:
                continue
            items[ck] = _accepted(value)
        return {"ts": 0, "items": items}

    return {"ts": 0, "items": {}}


def _atomic_write(path: Path, payload: Any) -> None:
    if _pair_scope() is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception as e:
        _warn("atomic_write_failed", path=str(path), error=str(e))


def _save_state(adapter: Any, items: Mapping[str, Mapping[str, Any]]) -> None:
    if _pair_scope() is None:
        return
    payload = {"ts": int(time.time()), "items": dict(items or {})}
    _atomic_write(_history_path(adapter), payload)


def _list_snapshots(adapter: Any) -> list[Path]:
    directory = _snapshot_dir(adapter)
    if not directory.exists() or not directory.is_dir():
        return []
    return sorted(
        [
            p
            for p in directory.iterdir()
            if p.is_file() and p.suffix == ".json" and p.name.endswith("-history.json")
        ],
        key=lambda p: p.stat().st_mtime,
    )


def _apply_retention(adapter: Any) -> None:
    if _pair_scope() is None:
        return
    cfg = getattr(adapter, "cfg", None)
    retention_days = int(getattr(cfg, "retention_days", 30) or 0)
    max_snapshots = int(getattr(cfg, "max_snapshots", 64) or 0)

    snaps = _list_snapshots(adapter)
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
                _info("snapshot_removed", reason="retention", snapshot=path.name, retention_days=retention_days)
            except Exception as e:
                _warn("snapshot_remove_failed", path=str(path), error=str(e))
        else:
            keep.append(path)

    if max_snapshots > 0 and len(keep) > max_snapshots:
        extra = len(keep) - max_snapshots
        for path in keep[:extra]:
            try:
                path.unlink()
                _info("snapshot_removed", reason="max_snapshots", snapshot=path.name, max_snapshots=max_snapshots)
            except Exception as e:
                _warn("snapshot_remove_failed", path=str(path), error=str(e))


def _snapshot_state(adapter: Any, items: Mapping[str, Mapping[str, Any]]) -> None:
    if _pair_scope() is None:
        return
    cfg = getattr(adapter, "cfg", None)
    auto = getattr(cfg, "auto_snapshot", True)
    if not auto:
        return

    directory = _snapshot_dir(adapter)
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    snaps = _list_snapshots(adapter)
    now = time.time()
    reuse_window = 60
    path: Path

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
            path = directory / f"{ts}-history.json"
    else:
        ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        path = directory / f"{ts}-history.json"

    payload = {"ts": int(now), "items": dict(items or {})}
    _atomic_write(path, payload)
    _apply_retention(adapter)


def _load_unresolved(adapter: Any) -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    path = _unresolved_path(adapter)
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {}


def _save_unresolved(adapter: Any, data: Mapping[str, Any]) -> None:
    if _pair_scope() is None:
        return
    _atomic_write(_unresolved_path(adapter), dict(data or {}))


def _record_unresolved(adapter: Any, items: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    existing = _load_unresolved(adapter)
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
    _save_unresolved(adapter, existing)
    return out


def _maybe_restore(adapter: Any) -> None:
    cfg = getattr(adapter, "cfg", None)
    restore_id = getattr(cfg, "restore_history", None)
    if not restore_id:
        return

    marker_path = _restore_state_path(adapter)
    last_id = ""

    try:
        raw = json.loads(marker_path.read_text("utf-8"))
        last_id = str(raw.get("last") or "")
    except Exception:
        last_id = ""

    if last_id and str(restore_id) == last_id:
        return

    snaps = _list_snapshots(adapter)
    if not snaps:
        _warn("restore_failed", reason="no_snapshots")
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
        _warn("restore_failed", reason="snapshot_not_found", restore_id=restore_id_str)
        return

    try:
        payload = json.loads(chosen.read_text("utf-8"))
        items = dict((payload.get("items") or {}) or {})
        _save_state(adapter, items)
        marker = {"last": restore_id_str, "ts": int(time.time()), "snapshot": chosen.name}
        _atomic_write(marker_path, marker)
        _info("restore_applied", snapshot=chosen.name)
    except Exception as e:
        _warn("restore_failed", reason="exception", snapshot=str(chosen), error=str(e))


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    if _pair_scope() is None:
        return {}
    _maybe_restore(adapter)

    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("history") if callable(prog_factory) else None

    state = _load_state(adapter)
    items = dict(state.get("items") or {})
    out: dict[str, dict[str, Any]] = {}

    for key, value in items.items():
        if not isinstance(value, Mapping):
            continue
        ck = canonical_key(value) or str(key)
        if not ck:
            continue
        out[ck] = _accepted(value)

    total = len(out)
    if prog:
        try:
            prog.tick(total, total=total, force=True)
            prog.done()
        except Exception:
            pass

    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    if _pair_scope() is None:
        return 0, []
    src = list(items or [])
    if not src:
        return 0, []

    _maybe_restore(adapter)

    state = _load_state(adapter)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: list[Mapping[str, Any]] = []
    changed = 0

    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        try:
            accepted = _accepted(obj)
        except Exception:
            unresolved_src.append(obj)
            continue
        key = canonical_key(accepted)
        if not key:
            unresolved_src.append(obj)
            continue
        existing = cur.get(key)
        if not accepted.get("watched_at"):
            unresolved_src.append(obj)
            continue
        if existing is None or (str(existing.get("watched_at") or "") <= str(accepted.get("watched_at") or "")):
            cur[key] = accepted
            changed += 1

    if changed:
        _snapshot_state(adapter, cur)
        _save_state(adapter, cur)

    unresolved = _record_unresolved(adapter, unresolved_src) if unresolved_src else []
    return changed, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    if _pair_scope() is None:
        return 0, []
    src = list(items or [])
    if not src:
        return 0, []

    _maybe_restore(adapter)

    state = _load_state(adapter)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: list[Mapping[str, Any]] = []
    changed = 0

    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        try:
            accepted = _accepted(obj)
        except Exception:
            unresolved_src.append(obj)
            continue
        key = canonical_key(accepted)
        if not key:
            unresolved_src.append(obj)
            continue
        if key in cur:
            del cur[key]
            changed += 1

    if changed:
        _snapshot_state(adapter, cur)
        _save_state(adapter, cur)

    unresolved = _record_unresolved(adapter, unresolved_src) if unresolved_src else []
    return changed, unresolved
