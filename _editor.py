# _editor.py
# CrossWatch - Tracker state helpers for history / ratings / watchlist
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Dict, Any, List, Literal, Tuple, IO
from pathlib import Path, PurePosixPath
from datetime import datetime, timezone
from io import BytesIO
import json
import zipfile
import shutil

from cw_platform.config_base import CONFIG

Kind = Literal["watchlist", "history", "ratings"]


def _config_path() -> Path:
    return Path(CONFIG) / "config.json"


def _load_config() -> Dict[str, Any]:
    path = _config_path()
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _cw_cfg() -> Dict[str, Any]:
    cfg = _load_config()
    cw = cfg.get("crosswatch") or {}
    if not isinstance(cw, dict):
        return {}
    return cw


def _root_dir() -> Path:
    cw = _cw_cfg()
    root = cw.get("root_dir") or ".cw_provider"
    p = Path(root)
    if not p.is_absolute():
        p = Path(CONFIG) / p
    return p


def _snapshots_dir() -> Path:
    d = _root_dir() / "snapshots"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _state_path(kind: Kind) -> Path:
    return _root_dir() / f"{kind}.json"


def _parse_ts_from_name(name: str) -> datetime | None:
    try:
        stem = name.split("-", 1)[0]
        return datetime.strptime(stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _snapshot_meta_for_file(path: Path, kind: Kind) -> Dict[str, Any]:
    dt = _parse_ts_from_name(path.name) or datetime.fromtimestamp(
        path.stat().st_mtime, tz=timezone.utc
    )
    return {
        "name": path.name,
        "kind": kind,
        "ts": int(dt.timestamp()),
        "iso": dt.isoformat(),
        "size": path.stat().st_size,
    }


def list_snapshots(kind: Kind) -> List[Dict[str, Any]]:
    snaps_dir = _snapshots_dir()
    suffix = f"-{kind}.json"
    items: List[Tuple[int, Dict[str, Any]]] = []
    for p in snaps_dir.glob(f"*{suffix}"):
        try:
            meta = _snapshot_meta_for_file(p, kind)
            items.append((meta["ts"], meta))
        except Exception:
            continue
    items.sort(key=lambda t: t[0], reverse=True)
    return [m for _, m in items]


def _snapshot_enabled() -> bool:
    cw = _cw_cfg()
    return bool(cw.get("auto_snapshot", True))


def _snapshot_limits() -> Tuple[int, int]:
    cw = _cw_cfg()
    max_snaps = int(cw.get("max_snapshots", 64) or 0)
    retention_days = int(cw.get("retention_days", 30) or 0)
    return max_snaps, retention_days


def _make_snapshot(kind: Kind) -> None:
    if not _snapshot_enabled():
        return
    path = _state_path(kind)
    if not path.exists():
        return
    try:
        payload = path.read_text(encoding="utf-8")
    except Exception:
        return
    if not payload:
        return

    dt = datetime.now(timezone.utc)
    name = dt.strftime("%Y%m%dT%H%M%SZ") + f"-{kind}.json"
    snaps_dir = _snapshots_dir()
    dest = snaps_dir / name
    try:
        dest.write_text(payload, encoding="utf-8")
    except Exception:
        return
    _enforce_snapshot_retention(kind)


def _enforce_snapshot_retention(kind: Kind) -> None:
    max_snaps, retention_days = _snapshot_limits()
    snaps = list_snapshots(kind)
    keep: List[str] = []
    now = datetime.now(timezone.utc)
    for meta in snaps:
        dt = datetime.fromtimestamp(meta["ts"], tz=timezone.utc)
        age_days = (now - dt).days
        if retention_days and age_days > retention_days:
            continue
        keep.append(meta["name"])

    if max_snaps and len(keep) > max_snaps:
        keep = keep[:max_snaps]

    keep_set = set(keep)
    snaps_dir = _snapshots_dir()
    suffix = f"-{kind}.json"
    for p in snaps_dir.glob(f"*{suffix}"):
        if p.name not in keep_set:
            try:
                p.unlink()
            except Exception:
                continue


def load_state(kind: Kind, snapshot: str | None = None) -> Dict[str, Any]:
    kind = kind or "watchlist"  # type: ignore[assignment]
    if kind not in ("watchlist", "history", "ratings"):
        raise ValueError(f"Unsupported kind: {kind!r}")
    if snapshot:
        path = _snapshots_dir() / snapshot
    else:
        path = _state_path(kind)  # type: ignore[arg-type]

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = {}

    items = data.get("items") or {}
    if not isinstance(items, dict):
        items = {}
    ts = data.get("ts")
    if not isinstance(ts, int):
        ts = int(datetime.now(timezone.utc).timestamp())

    return {"items": items, "ts": ts}


def save_state(kind: Kind, items: Dict[str, Any]) -> Dict[str, Any]:
    kind = kind or "watchlist"  # type: ignore[assignment]
    if kind not in ("watchlist", "history", "ratings"):
        raise ValueError(f"Unsupported kind: {kind!r}")

    _make_snapshot(kind)  # type: ignore[arg-type]

    state = {
        "items": items or {},
        "ts": int(datetime.now(timezone.utc).timestamp()),
    }
    path = _state_path(kind)  # type: ignore[arg-type]
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
    return state


TrackerImportStats = Dict[str, Any]


def export_tracker_zip() -> bytes:
    root = _root_dir()
    root.mkdir(parents=True, exist_ok=True)

    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in root.rglob("*.json"):
            try:
                rel = path.relative_to(root)
            except ValueError:
                continue
            zf.write(path, rel.as_posix())
    return buf.getvalue()


def import_tracker_zip(fp: IO[bytes]) -> TrackerImportStats:
    root = _root_dir()
    root.mkdir(parents=True, exist_ok=True)

    stats: TrackerImportStats = {
        "files": 0,
        "overwritten": 0,
        "states": 0,
        "snapshots": 0,
    }

    with zipfile.ZipFile(fp) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            rel = PurePosixPath(info.filename)
            if rel.is_absolute() or ".." in rel.parts:
                continue
            dest = root / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            existed = dest.exists()
            with zf.open(info, "r") as src, dest.open("wb") as out:
                shutil.copyfileobj(src, out)
            stats["files"] += 1
            if existed:
                stats["overwritten"] += 1

            try:
                rel_parts = dest.relative_to(root).parts
            except ValueError:
                rel_parts = ()
            if len(rel_parts) >= 2 and rel_parts[0] == "snapshots":
                stats["snapshots"] += 1
            elif len(rel_parts) == 1 and rel_parts[0] in (
                "watchlist.json",
                "history.json",
                "ratings.json",
            ):
                stats["states"] += 1

    return stats


def _normalize_import_items(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        return {str(k): v for k, v in data.items()}
    if isinstance(data, list):
        out: Dict[str, Any] = {}
        for row in data:
            if not isinstance(row, dict):
                continue
            key = str(row.get("key") or "").strip()
            if not key:
                continue
            payload = {k: v for k, v in row.items() if k != "key"}
            out[key] = payload
        return out
    return {}


def import_tracker_json(payload: bytes, filename: str) -> TrackerImportStats:
    try:
        text = payload.decode("utf-8")
        raw = json.loads(text)
    except Exception as e:
        raise ValueError(f"File is not valid JSON: {e}") from e

    if not isinstance(raw, dict):
        raise ValueError("JSON root must be an object")

    if "items" in raw:
        items = _normalize_import_items(raw.get("items"))
    else:
        items = _normalize_import_items(raw)

    now_ts = int(datetime.now(timezone.utc).timestamp())
    ts_val = raw.get("ts")
    ts = int(ts_val) if isinstance(ts_val, int) else now_ts
    state: Dict[str, Any] = {"items": items, "ts": ts}

    name = (filename or "upload.json").strip()
    lower = name.lower()

    root = _root_dir()
    root.mkdir(parents=True, exist_ok=True)

    target: str
    kind: Kind | None = None

    if lower in ("watchlist.json", "history.json", "ratings.json"):
        kind = lower.split(".")[0]  # type: ignore[assignment]
        dest = _state_path(kind)
        target = "state"
    else:
        for candidate in ("watchlist", "history", "ratings"):
            if lower.endswith(f"-{candidate}.json"):
                kind = candidate  # type: ignore[assignment]
                break
        if kind is None:
            raise ValueError(
                "Could not infer target for JSON file. "
                "Use filenames like 'watchlist.json' or "
                "'YYYYMMDDTHHMMSSZ-watchlist.json'."
            )
        dest = _snapshots_dir() / name
        target = "snapshot"

    dest.parent.mkdir(parents=True, exist_ok=True)
    existed = dest.exists()
    dest.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

    if target == "snapshot" and kind is not None:
        _enforce_snapshot_retention(kind)

    stats: TrackerImportStats = {
        "files": 1,
        "overwritten": 1 if existed else 0,
        "target": target,
        "kind": kind,
        "name": dest.name,
        "states": 1 if target == "state" else 0,
        "snapshots": 1 if target == "snapshot" else 0,
        "mode": "json",
    }
    return stats


def import_tracker_upload(payload: bytes, filename: str | None = None) -> TrackerImportStats:
    name = (filename or "").strip() or "upload.bin"
    buf = BytesIO(payload)

    try:
        is_zip = zipfile.is_zipfile(buf)
    except Exception:
        is_zip = False

    if is_zip:
        buf.seek(0)
        stats = import_tracker_zip(buf)
        stats.setdefault("mode", "zip")
        return stats

    lower = name.lower()
    if lower.endswith(".json"):
        return import_tracker_json(payload, name)

    # Fallback: try JSON anyway for unknown extension
    try:
        return import_tracker_json(payload, name)
    except Exception as e:
        raise ValueError("Unsupported file type; expected a ZIP or JSON file") from e
