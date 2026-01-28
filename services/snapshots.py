# services/snapshots.py
# CrossWatch - Provider snapshots (watchlist/ratings/history)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import json
import os
import re
import uuid

from cw_platform.config_base import CONFIG, load_config
from cw_platform.modules_registry import MODULES as MR_MODULES, load_sync_ops

Feature = Literal["watchlist", "ratings", "history"]
CreateFeature = Literal["watchlist", "ratings", "history", "all"]
RestoreMode = Literal["merge", "clear_restore"]

SNAPSHOT_KIND = "snapshot"
SNAPSHOT_BUNDLE_KIND = "snapshot_bundle"

def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _registry_sync_providers() -> list[str]:
    return [k.replace("_mod_", "").upper() for k in (MR_MODULES.get("SYNC") or {}).keys()]


def _safe_label(label: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9._ -]+", "", str(label or "").strip())
    s = re.sub(r"\s+", " ", s).strip()
    return s[:60] if s else "snapshot"


def _snapshots_dir() -> Path:
    d = CONFIG / "snapshots"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _date_dir(ts: datetime) -> Path:
    p = _snapshots_dir() / ts.strftime("%Y-%m-%d")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _snap_name(ts: datetime, provider: str, feature: str, label: str) -> str:
    stamp = ts.strftime("%Y%m%dT%H%M%SZ")
    safe = _safe_label(label).replace(" ", "_")
    return f"{stamp}__{provider.upper()}__{feature}__{safe}.json"


def _write_json_atomic(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{uuid.uuid4().hex[:8]}")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False, sort_keys=False), encoding="utf-8")
    os.replace(tmp, path)


def _norm_feature(x: str) -> Feature:
    v = str(x or "").strip().lower()
    if v not in ("watchlist", "ratings", "history"):
        raise ValueError(f"Unsupported feature: {x}")
    return v  # type: ignore[return-value]



def _norm_create_feature(x: str) -> CreateFeature:
    v = str(x or "").strip().lower()
    if v == "all":
        return "all"
    return _norm_feature(v)  # type: ignore[return-value]


def _norm_provider(x: str) -> str:
    v = str(x or "").strip().upper()
    if not v:
        raise ValueError("Provider is required")
    return v


def _ops_or_raise(provider: str):
    ops = load_sync_ops(provider)
    if not ops:
        raise ValueError(f"Unknown provider: {provider}")
    return ops


def _feature_enabled(ops: Any, feature: Feature) -> bool:
    try:
        feats = ops.features() or {}
        v = feats.get(feature)
        return bool(v)
    except Exception:
        return False


def _configured(ops: Any, cfg: Mapping[str, Any]) -> bool:
    fn = getattr(ops, "is_configured", None)
    if not callable(fn):
        return False
    try:
        return bool(fn(cfg))
    except Exception:
        return False


def _type_of_item(it: Mapping[str, Any]) -> str:
    t = str(it.get("type") or it.get("media_type") or it.get("entity") or "").strip().lower()
    if t in ("tv", "show", "shows", "series", "season", "episode", "anime"):
        return "tv"
    if t in ("movie", "movies", "film", "films"):
        return "movie"
    ids = it.get("ids")
    if isinstance(ids, Mapping) and (ids.get("anilist") or ids.get("mal")):
        return "tv"
    return t or "unknown"


def _stats_for(feature: Feature, idx: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    by_type: dict[str, int] = {}
    for it in idx.values():
        if not isinstance(it, Mapping):
            continue
        ty = _type_of_item(it)
        by_type[ty] = by_type.get(ty, 0) + 1

    return {
        "feature": feature,
        "count": len(idx),
        "by_type": dict(sorted(by_type.items(), key=lambda t: (-t[1], t[0]))),
    }


def snapshot_manifest(cfg: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    cfg = cfg or load_config()
    out: list[dict[str, Any]] = []
    for pid in _registry_sync_providers():
        ops = load_sync_ops(pid)
        if not ops:
            continue
        feats = {}
        try:
            raw = ops.features() or {}
            feats = {k: bool(raw.get(k)) for k in ("watchlist", "ratings", "history")}
        except Exception:
            feats = {"watchlist": False, "ratings": False, "history": False}

        out.append(
            {
                "id": pid,
                "label": getattr(ops, "label", lambda: pid)() if callable(getattr(ops, "label", None)) else pid,
                "configured": _configured(ops, cfg),
                "features": feats,
            }
        )

    out.sort(key=lambda d: (not bool(d.get("configured")), str(d.get("id") or "")))
    return out


def _index_dict(idx_raw: Any) -> dict[str, dict[str, Any]]:
    idx: dict[str, dict[str, Any]] = {}
    if not isinstance(idx_raw, Mapping):
        return idx
    for k, v in idx_raw.items():
        if not k or not isinstance(v, Mapping):
            continue
        idx[str(k)] = dict(v)
    return idx


def _create_single_snapshot(
    *,
    ops: Any,
    cfg: Mapping[str, Any],
    pid: str,
    feat: Feature,
    label: str,
    ts: datetime,
) -> dict[str, Any]:
    idx_raw = ops.build_index(cfg, feature=feat) or {}
    idx = _index_dict(idx_raw)
    stats = _stats_for(feat, idx)

    rel = f"{ts.strftime('%Y-%m-%d')}/{_snap_name(ts, pid, feat, label)}"
    path = _snapshots_dir() / rel

    payload: dict[str, Any] = {
        "kind": SNAPSHOT_KIND,
        "created_at": ts.isoformat(),
        "provider": pid,
        "feature": feat,
        "label": _safe_label(label),
        "stats": stats,
        "items": idx,
        "app_version": str(cfg.get("version") or ""),
    }
    _write_json_atomic(path, payload)

    return {
        "ok": True,
        "path": rel,
        "provider": pid,
        "feature": feat,
        "label": payload["label"],
        "created_at": payload["created_at"],
        "stats": stats,
    }



def create_snapshot(
    provider: str,
    feature: CreateFeature | str,
    *,
    label: str = "",
    cfg: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = cfg or load_config()
    pid = _norm_provider(provider)
    feat_any = _norm_create_feature(str(feature or ""))
    ops = _ops_or_raise(pid)

    if not _configured(ops, cfg):
        raise ValueError(f"Provider not configured: {pid}")

    ts = _utc_now()

    if feat_any == "all":
        children: list[dict[str, Any]] = []
        feats_total: dict[str, int] = {}
        total = 0

        for f in ("watchlist", "ratings", "history"):
            feat = _norm_feature(f)
            if not _feature_enabled(ops, feat):
                continue
            try:
                child = _create_single_snapshot(ops=ops, cfg=cfg, pid=pid, feat=feat, label=label, ts=ts)
                children.append({"feature": feat, "path": child["path"], "stats": child["stats"]})
                n = int((child.get("stats") or {}).get("count") or 0)
                feats_total[feat] = n
                total += n
            except Exception as e:
                children.append({"feature": feat, "error": str(e)})

        if not children:
            raise ValueError(f"No snapshot-capable features for provider: {pid}")

        rel = f"{ts.strftime('%Y-%m-%d')}/{_snap_name(ts, pid, 'all', label)}"
        path = _snapshots_dir() / rel
        stats = {"feature": "all", "count": total, "features": feats_total}

        payload: dict[str, Any] = {
            "kind": SNAPSHOT_BUNDLE_KIND,
            "created_at": ts.isoformat(),
            "provider": pid,
            "feature": "all",
            "label": _safe_label(label),
            "stats": stats,
            "children": children,
            "app_version": str(cfg.get("version") or ""),
        }
        _write_json_atomic(path, payload)

        return {"ok": True, "path": rel, "provider": pid, "feature": "all", "label": payload["label"], "created_at": payload["created_at"], "stats": stats, "children": children}

    feat = _norm_feature(str(feat_any))
    if not _feature_enabled(ops, feat):
        raise ValueError(f"Feature not enabled for provider: {pid} / {feat}")

    return _create_single_snapshot(ops=ops, cfg=cfg, pid=pid, feat=feat, label=label, ts=ts)
def list_snapshots() -> list[dict[str, Any]]:
    base = _snapshots_dir()
    out: list[dict[str, Any]] = []

    for p in base.rglob("*.json"):
        try:
            rel = str(p.relative_to(base)).replace("\\", "/")
        except Exception:
            rel = str(p).replace("\\", "/")

        meta = {"path": rel, "size": p.stat().st_size, "mtime": int(p.stat().st_mtime)}
        name = p.name
        parts = name.split("__")
        if len(parts) >= 3:
            meta["stamp"] = parts[0]
            meta["provider"] = parts[1]
            meta["feature"] = parts[2]
            if len(parts) >= 4:
                meta["label"] = parts[3].rsplit(".", 1)[0].replace("_", " ")
        out.append(meta)

    out.sort(key=lambda d: int(d.get("mtime") or 0), reverse=True)
    return out


def read_snapshot(path: str) -> dict[str, Any]:
    base = _snapshots_dir()
    rel = str(path or "").strip().lstrip("/").replace("\\", "/")
    if not rel:
        raise ValueError("Snapshot path is required")
    p = (base / rel).resolve()
    if base.resolve() not in p.parents and p != base.resolve():
        raise ValueError("Invalid snapshot path")
    if not p.exists():
        raise ValueError("Snapshot not found")

    raw = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("Invalid snapshot file")

    raw["path"] = rel
    kind = str(raw.get("kind") or "").strip().lower()
    feat_raw = str(raw.get("feature") or "").strip().lower()

    if kind == SNAPSHOT_BUNDLE_KIND or feat_raw == "all":
        children = raw.get("children")
        if not isinstance(children, list):
            children = []
        stats = raw.get("stats")
        if not isinstance(stats, Mapping):
            feats_total: dict[str, int] = {}
            total = 0
            for c in children:
                if not isinstance(c, Mapping):
                    continue
                f = str(c.get("feature") or "")
                s = c.get("stats")
                n = int(s.get("count") or 0) if isinstance(s, Mapping) else 0
                if f:
                    feats_total[f] = n
                    total += n
            stats = {"feature": "all", "count": total, "features": feats_total}
        raw["stats"] = stats
        return raw

    items = raw.get("items") or {}
    if not isinstance(items, Mapping):
        items = {}

    feat = _norm_feature(raw.get("feature") or "")
    stats = raw.get("stats")
    if not isinstance(stats, Mapping):
        stats = _stats_for(feat, items)  # type: ignore[arg-type]

    raw["stats"] = stats
    return raw
def _chunk(items: Sequence[Mapping[str, Any]], n: int) -> Iterable[list[Mapping[str, Any]]]:
    size = max(1, int(n))
    for i in range(0, len(items), size):
        yield [it for it in items[i : i + size]]


def _restore_single_snapshot(
    path: str,
    *,
    mode: RestoreMode = "merge",
    cfg: Mapping[str, Any] | None = None,
    chunk_size: int = 100,
) -> dict[str, Any]:
    cfg = cfg or load_config()
    snap = read_snapshot(path)
    pid = _norm_provider(str(snap.get("provider") or ""))
    feat = _norm_feature(str(snap.get("feature") or ""))
    ops = _ops_or_raise(pid)

    if not _configured(ops, cfg):
        raise ValueError(f"Provider not configured: {pid}")
    if not _feature_enabled(ops, feat):
        raise ValueError(f"Feature not enabled for provider: {pid} / {feat}")

    snap_items = snap.get("items") or {}
    if not isinstance(snap_items, Mapping):
        snap_items = {}

    cur_raw = ops.build_index(cfg, feature=feat) or {}
    cur: dict[str, dict[str, Any]] = {}
    for k, v in (cur_raw.items() if isinstance(cur_raw, Mapping) else []):
        if not k or not isinstance(v, Mapping):
            continue
        cur[str(k)] = dict(v)

    snap_keys = set(str(k) for k in snap_items.keys())
    cur_keys = set(cur.keys())

    to_add_keys = sorted(snap_keys - cur_keys)
    to_remove_keys: list[str] = []

    if mode == "clear_restore":
        to_remove_keys = sorted(cur_keys)

    add_items = [dict(snap_items[k]) for k in to_add_keys if isinstance(snap_items.get(k), Mapping)]
    rem_items = [dict(cur[k]) for k in to_remove_keys if isinstance(cur.get(k), Mapping)]

    removed = 0
    added = 0
    errors: list[str] = []

    if rem_items:
        for batch in _chunk(rem_items, chunk_size):
            try:
                res = ops.remove(cfg, batch, feature=feat, dry_run=False) or {}
                removed += int(res.get("count") or len(batch))
            except Exception as e:
                errors.append(f"remove_failed: {e}")

    if mode == "clear_restore" and errors:
        return {"ok": False, "provider": pid, "feature": feat, "mode": mode, "removed": removed, "added": added, "errors": errors}

    if add_items:
        for batch in _chunk(add_items, chunk_size):
            try:
                res = ops.add(cfg, batch, feature=feat, dry_run=False) or {}
                added += int(res.get("count") or len(batch))
            except Exception as e:
                errors.append(f"add_failed: {e}")

    return {
        "ok": len(errors) == 0,
        "provider": pid,
        "feature": feat,
        "mode": mode,
        "removed": removed,
        "added": added,
        "current_count": len(cur),
        "snapshot_count": len(snap_items),
        "errors": errors,
    }





def delete_snapshot(path: str, *, delete_children: bool = True) -> dict[str, Any]:
    base = _snapshots_dir()
    rel = str(path or "").strip().lstrip("/").replace("\\", "/")
    if not rel:
        raise ValueError("Snapshot path is required")
    p = (base / rel).resolve()
    if base.resolve() not in p.parents and p != base.resolve():
        raise ValueError("Invalid snapshot path")
    if not p.exists() or not p.is_file():
        raise ValueError("Snapshot not found")

    deleted: list[str] = []
    errors: list[str] = []

    raw: dict[str, Any] | None = None
    try:
        raw_any = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(raw_any, dict):
            raw = raw_any
    except Exception:
        raw = None

    if delete_children and isinstance(raw, dict):
        kind = str(raw.get("kind") or "").strip().lower()
        feat_raw = str(raw.get("feature") or "").strip().lower()
        if kind == SNAPSHOT_BUNDLE_KIND or feat_raw == "all":
            children = raw.get("children")
            if isinstance(children, list):
                for c in children:
                    if not isinstance(c, Mapping):
                        continue
                    child_path = str(c.get("path") or "").strip()
                    if not child_path:
                        continue
                    try:
                        r = delete_snapshot(child_path, delete_children=False)
                        deleted.extend([str(x) for x in (r.get("deleted") or [])])
                        errors.extend([str(x) for x in (r.get("errors") or [])])
                    except Exception as e:
                        errors.append(str(e))

    try:
        p.unlink()
        deleted.append(rel)
    except Exception as e:
        errors.append(str(e))

    try:
        parent = p.parent
        if parent != base and parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
    except Exception:
        pass

    return {"ok": len(errors) == 0, "deleted": deleted, "errors": errors}

def restore_snapshot(
    path: str,
    *,
    mode: RestoreMode = "merge",
    cfg: Mapping[str, Any] | None = None,
    chunk_size: int = 100,
) -> dict[str, Any]:
    cfg = cfg or load_config()
    snap = read_snapshot(path)
    kind = str(snap.get("kind") or "").strip().lower()
    feat_raw = str(snap.get("feature") or "").strip().lower()

    if kind == SNAPSHOT_BUNDLE_KIND or feat_raw == "all":
        pid = _norm_provider(str(snap.get("provider") or ""))
        ops = _ops_or_raise(pid)
        if not _configured(ops, cfg):
            raise ValueError(f"Provider not configured: {pid}")

        children = snap.get("children")
        if not isinstance(children, list):
            children = []

        results: list[dict[str, Any]] = []
        errors: list[str] = []
        for c in children:
            if not isinstance(c, Mapping):
                continue
            child_path = str(c.get("path") or "")
            if not child_path:
                continue
            try:
                results.append(_restore_single_snapshot(child_path, mode=mode, cfg=cfg, chunk_size=chunk_size))
            except Exception as e:
                errors.append(str(e))

        return {"ok": len(errors) == 0 and all(bool(r.get("ok")) for r in results), "provider": pid, "feature": "all", "mode": mode, "children": results, "errors": errors}

    return _restore_single_snapshot(path, mode=mode, cfg=cfg, chunk_size=chunk_size)
def clear_provider_features(
    provider: str,
    features: Iterable[Feature],
    *,
    cfg: Mapping[str, Any] | None = None,
    chunk_size: int = 100,
) -> dict[str, Any]:
    cfg = cfg or load_config()
    pid = _norm_provider(provider)
    ops = _ops_or_raise(pid)
    if not _configured(ops, cfg):
        raise ValueError(f"Provider not configured: {pid}")

    done: dict[str, Any] = {"ok": True, "provider": pid, "results": {}}
    for f in features:
        feat = _norm_feature(f)
        if not _feature_enabled(ops, feat):
            done["results"][feat] = {"ok": True, "skipped": True, "reason": "feature_disabled"}
            continue

        cur_raw = ops.build_index(cfg, feature=feat) or {}
        cur: list[Mapping[str, Any]] = []
        if isinstance(cur_raw, Mapping):
            for v in cur_raw.values():
                if isinstance(v, Mapping):
                    cur.append(dict(v))

        removed = 0
        errors: list[str] = []
        for batch in _chunk(cur, chunk_size):
            try:
                res = ops.remove(cfg, batch, feature=feat, dry_run=False) or {}
                removed += int(res.get("count") or len(batch))
            except Exception as e:
                errors.append(str(e))

        ok = len(errors) == 0
        done["ok"] = done["ok"] and ok
        done["results"][feat] = {"ok": ok, "removed": removed, "errors": errors, "count": len(cur)}

    return done
