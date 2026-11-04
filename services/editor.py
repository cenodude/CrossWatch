# services/editor.py
# CrossWatch - Tracker state helpers for history / ratings / watchlist / progress / collection
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, Literal
import os
import re
import json
import tempfile

from cw_platform.config_base import CONFIG, load_config
from cw_platform.provider_instances import get_provider_block
from cw_platform.tracker_store import TrackerStore, tracker_context

Kind = Literal["watchlist", "history", "ratings", "progress", "collection"]

_JSON_FILE_SUFFIX = ".json"


def _tracker(provider_instance: Any = None) -> tuple[TrackerStore, dict[str, int]]:
    try:
        cfg = load_config() or {}
    except Exception:
        cfg = {}
    inst, root = tracker_context(cfg, CONFIG, provider_instance)
    block = get_provider_block(cfg, "crosswatch", inst) or cfg.get("crosswatch") or {}
    if not isinstance(block, dict):
        block = {}
    limits = {
        "retention_days": int(block.get("retention_days", 30) or 0),
        "max_snapshots": int(block.get("max_snapshots", 64) or 0),
    }
    return TrackerStore(root), limits


def load_state(kind: Kind | None = None, snapshot: str | None = None, provider_instance: Any = None) -> dict[str, Any]:
    return _tracker(provider_instance)[0].load_items(kind or "watchlist", snapshot)


def export_tracker_zip(provider_instance: Any = None) -> bytes:
    return _tracker(provider_instance)[0].export_zip()


def import_tracker_json(payload: bytes, filename: str, provider_instance: Any = None) -> dict[str, Any]:
    store, limits = _tracker(provider_instance)
    return store.import_json(payload, filename, **limits)


def import_tracker_upload(
    payload: bytes,
    filename: str | None = None,
    provider_instance: Any = None,
) -> dict[str, Any]:
    store, limits = _tracker(provider_instance)
    return store.import_upload(payload, filename, **limits)


def _ensure_under_root(root: Path, path: Path) -> Path:
    root_r = root.resolve()
    path_r = path.resolve()
    try:
        path_r.relative_to(root_r)
    except ValueError as e:
        raise ValueError("Invalid path") from e
    return path_r

def _validated_json_filename(name: str, *, label: str) -> str:
    raw = str(name or "").strip()
    if not raw or not raw.lower().endswith(_JSON_FILE_SUFFIX):
        raise ValueError(f"Invalid {label}")

    posix = PurePosixPath(raw)
    win = PureWindowsPath(raw)
    if posix.name != raw or win.name != raw:
        raise ValueError(f"Invalid {label}")
    if posix.is_absolute() or win.is_absolute() or win.drive or win.root:
        raise ValueError(f"Invalid {label}")
    if raw in (".", ".."):
        raise ValueError(f"Invalid {label}")
    return raw

def _resolve_pair_dataset_path(name: str) -> Path:
    raw = _validated_json_filename(name, label="dataset name")
    root = _cw_state_dir()
    return _ensure_under_root(root, root / raw)

def _selected_pair_dataset_path(kind: Kind, pair: str, dataset: str | None) -> Path | None:
    dsets = list_pair_datasets(kind, pair)
    if not dsets:
        return None

    if dataset:
        target = _validated_json_filename(dataset, label="dataset name")
        for meta in dsets:
            if str(meta.get("name") or "") == target:
                try:
                    return _resolve_pair_dataset_path(target)
                except ValueError:
                    return None
        return None

    try:
        return _resolve_pair_dataset_path(str(dsets[0]["name"]))
    except ValueError:
        return None

_PAIR_SCOPE_RE = re.compile(r"^(?P<mode>[^_]+)_(?P<link>[^_]+)_pair_(?P<pid>.+)$")
_PAIR_DATASET_RE = re.compile(
    r"^(?P<prefix>.+)[._-](?P<kind>watchlist|history|ratings|progress|collection)\.(?P<variant>index|shadow)\.(?P<scope>.+)\.json$",
    re.IGNORECASE,
)


def _cw_state_dir() -> Path:
    return Path(CONFIG) / ".cw_state"

def _safe_name(name: str) -> str:
    return PurePosixPath(str(name or "")).name

def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=path.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise


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

    if scope_s.startswith("one-way_"):
        mode = "one-way"
    elif scope_s.startswith("two-way_"):
        mode = "two-way"

    return {"mode": mode, "src": None, "dst": None, "pair_id": None, "label": scope_s}


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

        if not scope:
            continue

        if ("pair_" not in scope) and (not scope.startswith("one-way_")) and (not scope.startswith("two-way_")):
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

    p = _selected_pair_dataset_path(kind, pair, dataset)
    return p if p and p.exists() else None

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

    items: dict[str, Any] = {}
    if isinstance(data, dict):
        raw_items = data.get("items")
        raw_events = data.get("events")

        if isinstance(raw_items, dict) and (raw_items or kind != "history"):
            for k, v in raw_items.items():
                if isinstance(v, dict) and isinstance(v.get("item"), dict):
                    items[str(k)] = v["item"]
                else:
                    items[str(k)] = v
        elif kind == "history" and isinstance(raw_events, dict):
            for k, v in raw_events.items():
                if isinstance(v, dict) and isinstance(v.get("item"), dict):
                    items[str(k)] = v["item"]
                else:
                    items[str(k)] = v

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

    if kind == "history" and isinstance(data.get("events"), dict):
        events: dict[str, Any] = {}
        for k, v in (items or {}).items():
            if not isinstance(v, dict):
                continue
            events[str(k)] = {"item": dict(v)}
        data["events"] = events

        if "items" in data:
            try:
                del data["items"]
            except Exception:
                pass
    else:
        wrapped = False
        raw_items = data.get("items")
        if isinstance(raw_items, dict):
            for _, v in raw_items.items():
                if isinstance(v, dict) and isinstance(v.get("item"), dict):
                    wrapped = True
                    break

        if wrapped and kind == "ratings":
            out_items: dict[str, Any] = {}
            for k, v in (items or {}).items():
                if not isinstance(v, dict):
                    continue
                node: dict[str, Any] = {"item": dict(v)}
                if v.get("rating") is not None:
                    node["rating"] = v.get("rating")
                if v.get("rated_at"):
                    node["rated_at"] = v.get("rated_at")
                out_items[str(k)] = node
            data["items"] = out_items
        else:
            data["items"] = dict(items or {})

    data["edited_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _atomic_write_json(path, data)

    try:
        ts = int(path.stat().st_mtime)
    except Exception:
        ts = int(datetime.now(timezone.utc).timestamp())

    return {"items": dict(items or {}), "ts": ts, "file": path.name, "pair": scope}
