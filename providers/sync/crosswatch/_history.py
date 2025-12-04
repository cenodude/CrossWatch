# /providers/sync/crosswatch/_history.py
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Tuple
from cw_platform.id_map import canonical_key, minimal as id_minimal

def _log(msg: str) -> None:
    if os.getenv("CW_DEBUG") or os.getenv("CW_CROSSWATCH_DEBUG"):
        print(f"[CROSSWATCH:history] {msg}")

def _root(adapter) -> Path:
    base = getattr(getattr(adapter, "cfg", None), "base_path", None)
    if isinstance(base, Path):
        return base
    if isinstance(base, str) and base:
        return Path(base)
    return Path("/config/.cw_provider")

def _history_path(adapter) -> Path:
    return _root(adapter) / "history.json"
def _snapshot_dir(adapter) -> Path:
    return _root(adapter) / "snapshots"
def _unresolved_path(adapter) -> Path:
    return _root(adapter) / "history.unresolved.json"
def _restore_state_path(adapter) -> Path:
    return _root(adapter) / "history.restore_state.json"

def _load_state(adapter) -> Dict[str, Any]:
    p = _history_path(adapter)
    try:
        raw = json.loads(p.read_text("utf-8"))
    except Exception:
        return {"ts": 0, "items": {}}

    # Legacy: list of items
    if isinstance(raw, list):
        items: Dict[str, Dict[str, Any]] = {}
        for obj in raw:
            if not isinstance(obj, Mapping):
                continue
            key = canonical_key(obj)
            if not key:
                continue
            items[key] = id_minimal(obj)
        return {"ts": 0, "items": items}

    if isinstance(raw, Mapping):
        if "items" in raw and isinstance(raw.get("items"), Mapping):
            ts = int(raw.get("ts", 0) or 0)
            items_raw = raw.get("items") or {}
            items: Dict[str, Dict[str, Any]] = {}
            for k, v in items_raw.items():
                if not isinstance(v, Mapping):
                    continue
                key = str(k) or canonical_key(v)
                if not key:
                    continue
                items[key] = id_minimal(v)
            return {"ts": ts, "items": items}

        items: Dict[str, Dict[str, Any]] = {}
        for k, v in raw.items():
            if not isinstance(v, Mapping):
                continue
            key = str(k) or canonical_key(v)
            if not key:
                continue
            items[key] = id_minimal(v)
        return {"ts": 0, "items": items}

    return {"ts": 0, "items": {}}

def _atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception as e:
        _log(f"atomic_write failed for {path}: {e}")

def _save_state(adapter, items: Mapping[str, Mapping[str, Any]]) -> None:
    payload = {"ts": int(time.time()), "items": dict(items or {})}
    _atomic_write(_history_path(adapter), payload)

def _list_snapshots(adapter) -> List[Path]:
    d = _snapshot_dir(adapter)
    if not d.exists() or not d.is_dir():
        return []
    return sorted(
        [
            p
            for p in d.iterdir()
            if p.is_file() and p.suffix == ".json" and p.name.endswith("-history.json")
        ],
        key=lambda p: p.stat().st_mtime,
    )

def _apply_retention(adapter) -> None:
    cfg = getattr(adapter, "cfg", None)
    retention_days = int(getattr(cfg, "retention_days", 30) or 0)
    max_snapshots = int(getattr(cfg, "max_snapshots", 64) or 0)

    snaps = _list_snapshots(adapter)
    if not snaps:
        return

    now = time.time()
    keep: List[Path] = []
    for p in snaps:
        try:
            age_days = (now - p.stat().st_mtime) / 86400.0
        except Exception:
            keep.append(p)
            continue
        if retention_days > 0 and age_days > retention_days:
            try:
                p.unlink()
                _log(f"snapshot removed by retention: {p.name}")
            except Exception as e:
                _log(f"snapshot unlink failed: {p} {e}")
        else:
            keep.append(p)

    if max_snapshots > 0 and len(keep) > max_snapshots:
        extra = len(keep) - max_snapshots
        for p in keep[:extra]:
            try:
                p.unlink()
                _log(f"snapshot removed by max_snapshots: {p.name}")
            except Exception as e:
                _log(f"snapshot unlink failed: {p} {e}")

def _snapshot_state(adapter, items: Mapping[str, Mapping[str, Any]]) -> None:
    cfg = getattr(adapter, "cfg", None)
    auto = getattr(cfg, "auto_snapshot", True)
    if not auto:
        return

    d = _snapshot_dir(adapter)
    try:
        d.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    snaps = _list_snapshots(adapter)
    now = time.time()
    reuse_window = 60  # seconds

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
            path = d / f"{ts}-history.json"
    else:
        ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        path = d / f"{ts}-history.json"

    payload = {"ts": int(now), "items": dict(items or {})}
    _atomic_write(path, payload)
    _apply_retention(adapter)

def _load_unresolved(adapter) -> Dict[str, Any]:
    path = _unresolved_path(adapter)
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {}

def _save_unresolved(adapter, data: Mapping[str, Any]) -> None:
    _atomic_write(_unresolved_path(adapter), dict(data or {}))

def _record_unresolved(adapter, items: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    existing = _load_unresolved(adapter)
    bucket: Dict[str, Any] = dict(existing.get("items") or {})
    out: List[Dict[str, Any]] = []

    for obj in items:
        try:
            m = id_minimal(obj)
        except Exception:
            continue
        key = canonical_key(m) or f"obj:{hash(json.dumps(m, sort_keys=True))}"
        if key not in bucket:
            bucket[key] = m
        out.append(m)

    existing["items"] = bucket
    existing["ts"] = int(time.time())
    _save_unresolved(adapter, existing)
    return out

def _maybe_restore(adapter) -> None:
    cfg = getattr(adapter, "cfg", None)
    restore_id = getattr(cfg, "restore_history", None)
    if not restore_id:
        return

    marker_path = _restore_state_path(adapter)
    last_id: str = ""
    try:
        raw = json.loads(marker_path.read_text("utf-8"))
        last_id = str(raw.get("last") or "")
    except Exception:
        last_id = ""

    if last_id and str(restore_id) == last_id:
        return

    snaps = _list_snapshots(adapter)
    if not snaps:
        _log("restore requested but no snapshots present")
        return

    chosen: Path | None = None
    restore_id_str = str(restore_id).strip()

    if restore_id_str.lower() in ("latest", "last"):
        chosen = snaps[-1]
    else:
        for p in snaps:
            if p.name == restore_id_str or p.stem == restore_id_str:
                chosen = p
                break

    if not chosen:
        _log(f"restore requested but snapshot not found: {restore_id_str}")
        return

    try:
        payload = json.loads(chosen.read_text("utf-8"))
        items = dict((payload.get("items") or {}) or {})
        _save_state(adapter, items)
        marker = {"last": restore_id_str, "ts": int(time.time()), "snapshot": chosen.name}
        _atomic_write(marker_path, marker)
        _log(f"restore applied from {chosen.name}")
    except Exception as e:
        _log(f"restore failed from {chosen}: {e}")


def build_index(adapter) -> Dict[str, Dict[str, Any]]:
    _maybe_restore(adapter)

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history") if callable(prog_mk) else None
    state = _load_state(adapter)
    items = dict(state.get("items") or {})
    out: Dict[str, Dict[str, Any]] = {}

    for k, v in items.items():
        if not isinstance(v, Mapping):
            continue
        ck = canonical_key(v) or str(k)
        if not ck:
            continue
        out[ck] = id_minimal(v)

    total = len(out)
    if prog:
        try:
            prog.tick(total, total=total, force=True)
            prog.done()
        except Exception:
            pass
    return out

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    src = list(items or [])
    if not src:
        return 0, []

    _maybe_restore(adapter)

    state = _load_state(adapter)
    cur: Dict[str, Dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: List[Mapping[str, Any]] = []
    changed = 0

    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        try:
            m = id_minimal(obj)
        except Exception:
            unresolved_src.append(obj)
            continue
        key = canonical_key(m)
        if not key:
            unresolved_src.append(obj)
            continue
        existing = cur.get(key)
        if existing != m:
            cur[key] = m
            changed += 1

    if changed:
        _snapshot_state(adapter, cur)
        _save_state(adapter, cur)

    unresolved = _record_unresolved(adapter, unresolved_src) if unresolved_src else []
    return changed, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    src = list(items or [])
    if not src:
        return 0, []

    _maybe_restore(adapter)

    state = _load_state(adapter)
    cur: Dict[str, Dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: List[Mapping[str, Any]] = []
    changed = 0

    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        try:
            m = id_minimal(obj)
        except Exception:
            unresolved_src.append(obj)
            continue
        key = canonical_key(m)
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