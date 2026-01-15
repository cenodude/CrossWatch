# services/editor.py
# CrossWatch - Tracker state helpers for history / ratings / watchlist
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, IO, Literal, cast
import os
import re
import json
import shutil
import zipfile

from cw_platform.config_base import CONFIG, load_config

Kind = Literal["watchlist", "history", "ratings"]

def _cw_cfg() -> dict[str, Any]:
    try:
        cfg = load_config()
    except Exception:
        return {}
    cw = cfg.get("crosswatch") or {}
    return cw if isinstance(cw, dict) else {}

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

def _snapshot_meta_for_file(path: Path, kind: Kind) -> dict[str, Any]:
    dt = _parse_ts_from_name(path.name) or datetime.fromtimestamp(
        path.stat().st_mtime,
        tz=timezone.utc,
    )
    return {
        "name": path.name,
        "kind": kind,
        "ts": int(dt.timestamp()),
        "iso": dt.isoformat(),
        "size": path.stat().st_size,
    }

def list_snapshots(kind: Kind) -> list[dict[str, Any]]:
    snaps_dir = _snapshots_dir()
    suffix = f"-{kind}.json"
    items: list[tuple[int, dict[str, Any]]] = []
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

def _snapshot_limits() -> tuple[int, int]:
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
    keep: list[str] = []
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

def load_state(kind: Kind | None = None, snapshot: str | None = None) -> dict[str, Any]:
    if kind is None:
        kind_val: Kind = "watchlist"
    elif kind in ("watchlist", "history", "ratings"):
        kind_val = kind
    else:
        raise ValueError(f"Unsupported kind: {kind!r}")

    if snapshot:
        path = _snapshots_dir() / snapshot
    else:
        path = _state_path(kind_val)

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

def save_state(kind: Kind | None, items: dict[str, Any]) -> dict[str, Any]:
    if kind is None:
        kind_val: Kind = "watchlist"
    elif kind in ("watchlist", "history", "ratings"):
        kind_val = kind
    else:
        raise ValueError(f"Unsupported kind: {kind!r}")

    _make_snapshot(kind_val)

    state = {
        "items": items or {},
        "ts": int(datetime.now(timezone.utc).timestamp()),
    }
    path = _state_path(kind_val)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass
    return state

TrackerImportStats = dict[str, Any]

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

def _normalize_import_items(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        return {str(k): v for k, v in data.items()}
    if isinstance(data, list):
        out: dict[str, Any] = {}
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
    state: dict[str, Any] = {"items": items, "ts": ts}

    name = (filename or "upload.json").strip()
    lower = name.lower()

    root = _root_dir()
    root.mkdir(parents=True, exist_ok=True)

    target: str
    kind: Kind | None = None

    if lower in ("watchlist.json", "history.json", "ratings.json"):
        base = lower.split(".")[0]  # "watchlist" / "history" / "ratings"
        kind = cast(Kind, base)
        dest = _state_path(kind)
        target = "state"
    else:
        for candidate in ("watchlist", "history", "ratings"):
            if lower.endswith(f"-{candidate}.json"):
                kind = cast(Kind, candidate)
                break
        if kind is None:
            raise ValueError(
                "Could not infer target for JSON file. "
                "Use filenames like 'watchlist.json' or "
                "'YYYYMMDDTHHMMSSZ-watchlist.json'.",
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

def import_tracker_upload(
    payload: bytes,
    filename: str | None = None,
) -> TrackerImportStats:
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

    try:
        return import_tracker_json(payload, name)
    except Exception as e:
        raise ValueError("Unsupported file type; expected a ZIP or JSON file") from e

_PAIR_SCOPE_RE = re.compile(r"^(?P<mode>[^_]+)_(?P<link>[^_]+)_pair_(?P<pid>.+)$")
_PAIR_DATASET_RE = re.compile(
    r"^(?P<prefix>.+)[._-](?P<kind>watchlist|history|ratings)\.(?P<variant>index|shadow)\.(?P<scope>.+)\.json$",
    re.IGNORECASE,
)


def _cw_state_dir() -> Path:
    return Path(CONFIG) / ".cw_state"

def _safe_name(name: str) -> str:
    return PurePosixPath(str(name or "")).name

def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)

def _pair_meta_from_scope(scope: str) -> dict[str, Any]:
    scope_s = str(scope or "").strip()
    mode = None
    src = None
    dst = None
    pair_id = None
    m = _PAIR_SCOPE_RE.match(scope_s)
    if m:
        mode = m.group("mode") or None
        link = m.group("link") or ""
        if "-" in link:
            a, b = link.split("-", 1)
            src = a or None
            dst = b or None
        else:
            src = link or None
        pair_id = m.group("pid") or None
    label = scope_s
    if mode and src and dst:
        label = f"{mode} {src}→{dst}"
    return {"mode": mode, "src": src, "dst": dst, "pair_id": pair_id, "label": label}

def list_pairs() -> dict[str, Any]:
    root = _cw_state_dir()
    if not root.exists():
        return {"pairs": [], "default": ""}

    bucket: dict[str, dict[str, Any]] = {}
    for p in root.glob("*.json"):
        name = p.name
        scope = None

        m = _PAIR_DATASET_RE.match(name)
        if m:
            scope = str(m.group("scope") or "").strip()
        elif ".index." in name or ".shadow." in name:
            sep = ".index." if ".index." in name else ".shadow."
            try:
                scope = name.split(sep, 1)[1].rsplit(".json", 1)[0]
            except Exception:
                scope = None

        if not scope or "pair_" not in scope:
            continue

        try:
            ts = int(p.stat().st_mtime)
        except Exception:
            ts = 0

        cur = bucket.get(scope)
        if not cur or ts > int(cur.get("ts") or 0):
            meta = _pair_meta_from_scope(scope)
            bucket[scope] = {"scope": scope, "ts": ts, **meta}

    pairs = sorted(bucket.values(), key=lambda x: int(x.get("ts") or 0), reverse=True)
    default = str(pairs[0]["scope"]) if pairs else ""
    return {"pairs": pairs, "default": default}

def list_pair_datasets(kind: Kind, pair: str) -> list[dict[str, Any]]:
    root = _cw_state_dir()
    if not root.exists():
        return []

    scope = str(pair or "").strip()
    if not scope:
        return []

    out: list[dict[str, Any]] = []
    for p in root.glob("*.json"):
        m = _PAIR_DATASET_RE.match(p.name)
        if not m:
            continue
        if (m.group("kind") or "").lower() != str(kind).lower():
            continue
        if str(m.group("scope") or "").strip() != scope:
            continue

        prefix = str(m.group("prefix") or "").strip() or p.stem
        variant = str(m.group("variant") or "").lower() or None

        try:
            ts = int(p.stat().st_mtime)
            size = int(p.stat().st_size)
        except Exception:
            ts = 0
            size = 0

        out.append(
            {
                "name": p.name,
                "provider": prefix.upper(),
                "variant": variant,
                "ts": ts,
                "size": size,
            }
        )

    out.sort(key=lambda x: int(x.get("ts") or 0), reverse=True)
    return out

def _resolve_pair_file(kind: Kind, pair: str, dataset: str | None) -> Path | None:
    root = _cw_state_dir()
    if not root.exists():
        return None

    if dataset:
        name = _safe_name(dataset)
        if not name:
            return None
        p = root / name
        return p if p.exists() else None

    dsets = list_pair_datasets(kind, pair)
    if not dsets:
        return None
    return root / str(dsets[0]["name"])

def load_pair_state(kind: Kind, pair: str, dataset: str | None = None) -> dict[str, Any]:
    scope = str(pair or "").strip()
    if not scope:
        raise ValueError("Missing pair scope")

    path = _resolve_pair_file(kind, scope, dataset)
    if not path:
        return {"items": {}, "ts": int(datetime.now(timezone.utc).timestamp()), "file": None, "pair": scope}

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = {}

    items = {}
    if isinstance(data, dict):
        items = data.get("items") or {}
    if not isinstance(items, dict):
        items = {}

    try:
        ts = int(path.stat().st_mtime)
    except Exception:
        ts = int(datetime.now(timezone.utc).timestamp())

    return {"items": items, "ts": ts, "file": path.name, "pair": scope}

def save_pair_state(kind: Kind, pair: str, dataset: str | None, items: dict[str, Any]) -> dict[str, Any]:
    scope = str(pair or "").strip()
    if not scope:
        raise ValueError("Missing pair scope")

    path = _resolve_pair_file(kind, scope, dataset)
    if not path:
        raise ValueError("No dataset found for this pair/kind")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = {}

    if not isinstance(data, dict):
        data = {}
    data["items"] = dict(items or {})
    data["edited_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    _atomic_write_json(path, data)

    try:
        ts = int(path.stat().st_mtime)
    except Exception:
        ts = int(datetime.now(timezone.utc).timestamp())
    return {"items": data["items"], "ts": ts, "file": path.name, "pair": scope}
