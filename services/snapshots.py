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
from cw_platform.provider_instances import build_provider_config_view, list_instance_ids, normalize_instance_id

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


def _snap_name(ts: datetime, provider: str, instance: str, feature: str, label: str) -> str:
    stamp = ts.strftime("%Y%m%dT%H%M%SZ")
    safe = _safe_label(label).replace(" ", "_")
    inst = re.sub(r"[^a-zA-Z0-9._-]+", "", str(instance or "").strip())
    inst = inst if inst else "default"
    return f"{stamp}__{provider.upper()}__{inst}__{feature}__{safe}.json"


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

        insts = list_instance_ids(cfg, pid)
        inst_meta: list[dict[str, Any]] = []
        configured_any = False
        for inst in insts:
            cfg_view = build_provider_config_view(cfg, pid, inst)
            ok = _configured(ops, cfg_view)
            configured_any = configured_any or ok
            inst_meta.append({"id": inst, "label": "Default" if inst == "default" else inst, "configured": ok})

        out.append(
            {
                "id": pid,
                "label": getattr(ops, "label", lambda: pid)() if callable(getattr(ops, "label", None)) else pid,
                "configured": configured_any,
                "features": feats,
                "instances": inst_meta,
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
    instance: str,
    feat: Feature,
    label: str,
    ts: datetime,
) -> dict[str, Any]:
    cfg_view = build_provider_config_view(cfg, pid, instance)
    idx_raw = ops.build_index(cfg_view, feature=feat) or {}
    idx = _index_dict(idx_raw)
    stats = _stats_for(feat, idx)

    inst = normalize_instance_id(instance)
    rel = f"{ts.strftime('%Y-%m-%d')}/{_snap_name(ts, pid, inst, feat, label)}"
    path = _snapshots_dir() / rel

    payload: dict[str, Any] = {
        "kind": SNAPSHOT_KIND,
        "created_at": ts.isoformat(),
        "provider": pid,
        "instance": inst,
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
        "instance": inst,
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
    instance_id: Any | None = None,
    cfg: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = cfg or load_config()
    pid = _norm_provider(provider)
    inst = normalize_instance_id(instance_id)
    feat_any = _norm_create_feature(str(feature or ""))
    ops = _ops_or_raise(pid)

    cfg_view = build_provider_config_view(cfg, pid, inst)

    if not _configured(ops, cfg_view):
        raise ValueError(f"Provider not configured: {pid}#{inst}")

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
                child = _create_single_snapshot(ops=ops, cfg=cfg, pid=pid, instance=inst, feat=feat, label=label, ts=ts)
                children.append({"feature": feat, "path": child["path"], "stats": child["stats"]})
                n = int((child.get("stats") or {}).get("count") or 0)
                feats_total[feat] = n
                total += n
            except Exception as e:
                children.append({"feature": feat, "error": str(e)})

        if not children:
            raise ValueError(f"No snapshot-capable features for provider: {pid}")

        rel = f"{ts.strftime('%Y-%m-%d')}/{_snap_name(ts, pid, inst, 'all', label)}"
        path = _snapshots_dir() / rel
        stats = {"feature": "all", "count": total, "features": feats_total}

        payload: dict[str, Any] = {
            "kind": SNAPSHOT_BUNDLE_KIND,
            "created_at": ts.isoformat(),
            "provider": pid,
            "instance": inst,
            "feature": "all",
            "label": _safe_label(label),
            "stats": stats,
            "children": children,
            "app_version": str(cfg.get("version") or ""),
        }
        _write_json_atomic(path, payload)

        return {"ok": True, "path": rel, "provider": pid, "instance": inst, "feature": "all", "label": payload["label"], "created_at": payload["created_at"], "stats": stats, "children": children}

    feat = _norm_feature(str(feat_any))
    if not _feature_enabled(ops, feat):
        raise ValueError(f"Feature not enabled for provider: {pid} / {feat}")

    return _create_single_snapshot(ops=ops, cfg=cfg, pid=pid, instance=inst, feat=feat, label=label, ts=ts)
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
        if len(parts) >= 5:
            meta["stamp"] = parts[0]
            meta["provider"] = parts[1]
            meta["instance"] = normalize_instance_id(parts[2])
            meta["feature"] = parts[3]
            meta["label"] = parts[4].rsplit(".", 1)[0].replace("_", " ")
        elif len(parts) >= 3:
            meta["stamp"] = parts[0]
            meta["provider"] = parts[1]
            meta["feature"] = parts[2]
            meta["instance"] = "default"
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
    raw["instance"] = normalize_instance_id(raw.get("instance") or raw.get("instance_id") or raw.get("profile"))
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
    instance_id: Any | None = None,
    cfg: Mapping[str, Any] | None = None,
    chunk_size: int = 100,
) -> dict[str, Any]:
    cfg = cfg or load_config()
    snap = read_snapshot(path)
    pid = _norm_provider(str(snap.get("provider") or ""))
    snap_inst = normalize_instance_id(snap.get("instance") or snap.get("instance_id") or snap.get("profile"))
    inst = normalize_instance_id(instance_id) if instance_id else snap_inst
    feat = _norm_feature(str(snap.get("feature") or ""))
    ops = _ops_or_raise(pid)

    cfg_view = build_provider_config_view(cfg, pid, inst)

    if not _configured(ops, cfg_view):
        raise ValueError(f"Provider not configured: {pid}#{inst}")
    if not _feature_enabled(ops, feat):
        raise ValueError(f"Feature not enabled for provider: {pid} / {feat}")

    snap_items = snap.get("items") or {}
    if not isinstance(snap_items, Mapping):
        snap_items = {}

    cur_raw = ops.build_index(cfg_view, feature=feat) or {}
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
                res = ops.remove(cfg_view, batch, feature=feat, dry_run=False) or {}
                removed += int(res.get("count") or len(batch))
            except Exception as e:
                errors.append(f"remove_failed: {e}")

    if mode == "clear_restore" and errors:
        return {"ok": False, "provider": pid, "feature": feat, "mode": mode, "removed": removed, "added": added, "errors": errors}

    if add_items:
        for batch in _chunk(add_items, chunk_size):
            try:
                res = ops.add(cfg_view, batch, feature=feat, dry_run=False) or {}
                added += int(res.get("count") or len(batch))
            except Exception as e:
                errors.append(f"add_failed: {e}")

    return {
        "ok": len(errors) == 0,
        "provider": pid,
        "instance": inst,
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
    instance_id: Any | None = None,
    cfg: Mapping[str, Any] | None = None,
    chunk_size: int = 100,
) -> dict[str, Any]:
    cfg = cfg or load_config()
    snap = read_snapshot(path)
    kind = str(snap.get("kind") or "").strip().lower()
    feat_raw = str(snap.get("feature") or "").strip().lower()

    if kind == SNAPSHOT_BUNDLE_KIND or feat_raw == "all":
        pid = _norm_provider(str(snap.get("provider") or ""))
        snap_inst = normalize_instance_id(snap.get("instance") or snap.get("instance_id") or snap.get("profile"))
        inst = normalize_instance_id(instance_id) if instance_id else snap_inst
        ops = _ops_or_raise(pid)
        if not _configured(ops, build_provider_config_view(cfg, pid, inst)):
            raise ValueError(f"Provider not configured: {pid}#{inst}")

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
                results.append(_restore_single_snapshot(child_path, mode=mode, instance_id=inst, cfg=cfg, chunk_size=chunk_size))
            except Exception as e:
                errors.append(str(e))

        return {"ok": len(errors) == 0 and all(bool(r.get("ok")) for r in results), "provider": pid, "instance": inst, "feature": "all", "mode": mode, "children": results, "errors": errors}

    return _restore_single_snapshot(path, mode=mode, instance_id=instance_id, cfg=cfg, chunk_size=chunk_size)
def clear_provider_features(
    provider: str,
    features: Iterable[Feature],
    *,
    instance_id: Any | None = None,
    cfg: Mapping[str, Any] | None = None,
    chunk_size: int = 100,
) -> dict[str, Any]:
    cfg = cfg or load_config()
    pid = _norm_provider(provider)
    inst = normalize_instance_id(instance_id)
    ops = _ops_or_raise(pid)
    cfg_view = build_provider_config_view(cfg, pid, inst)
    if not _configured(ops, cfg_view):
        raise ValueError(f"Provider not configured: {pid}#{inst}")

    done: dict[str, Any] = {"ok": True, "provider": pid, "instance": inst, "results": {}}
    for f in features:
        feat = _norm_feature(f)
        if not _feature_enabled(ops, feat):
            done["results"][feat] = {"ok": True, "skipped": True, "reason": "feature_disabled"}
            continue

        cur_raw = ops.build_index(cfg_view, feature=feat) or {}
        cur: list[Mapping[str, Any]] = []
        if isinstance(cur_raw, Mapping):
            for v in cur_raw.values():
                if isinstance(v, Mapping):
                    cur.append(dict(v))

        removed = 0
        errors: list[str] = []
        for batch in _chunk(cur, chunk_size):
            try:
                res = ops.remove(cfg_view, batch, feature=feat, dry_run=False) or {}
                removed += int(res.get("count") or len(batch))
            except Exception as e:
                errors.append(str(e))

        ok = len(errors) == 0
        done["ok"] = done["ok"] and ok
        done["results"][feat] = {"ok": ok, "removed": removed, "errors": errors, "count": len(cur)}

    return done


def _brief_item(x: Any) -> dict[str, Any]:
    if not isinstance(x, Mapping):
        return {"value": x}
    out: dict[str, Any] = {}
    for k in ("type", "title", "year", "season", "episode", "status"):
        if k in x:
            out[k] = x.get(k)
    ids = x.get("ids")
    if isinstance(ids, Mapping):
        # Keep common external IDs only; these are the most useful for humans.
        keep = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "anidb", "mal", "anilist", "kitsu")
        out["ids"] = {k: ids.get(k) for k in keep if k in ids}
    return out or dict(x)


def _path_join(base: str, key: str) -> str:
    k = str(key)
    if not base:
        return k
    return base + "." + k


def _diff_any(a: Any, b: Any, *, path: str, out: list[dict[str, Any]], max_depth: int, max_changes: int, depth: int = 0) -> None:
    if len(out) >= max_changes:
        return
    if a == b:
        return
    if depth >= max_depth:
        out.append({"path": path or "<root>", "old": a, "new": b})
        return
    if isinstance(a, Mapping) and isinstance(b, Mapping):
        keys = sorted(set(str(k) for k in a.keys()).union(set(str(k) for k in b.keys())))
        for k in keys:
            if len(out) >= max_changes:
                break
            has_a = k in a
            has_b = k in b
            p = _path_join(path, k)
            if not has_a:
                out.append({"path": p, "old": None, "new": b.get(k)})
                continue
            if not has_b:
                out.append({"path": p, "old": a.get(k), "new": None})
                continue
            _diff_any(a.get(k), b.get(k), path=p, out=out, max_depth=max_depth, max_changes=max_changes, depth=depth + 1)
        return
    if isinstance(a, Sequence) and not isinstance(a, (str, bytes)) and isinstance(b, Sequence) and not isinstance(b, (str, bytes)):
        out.append({"path": path or "<root>", "old": a, "new": b})
        return
    out.append({"path": path or "<root>", "old": a, "new": b})


def diff_snapshots(
    a_path: str,
    b_path: str,
    *,
    limit: int = 200,
    max_depth: int = 4,
    max_changes: int = 25,
) -> dict[str, Any]:
    a = read_snapshot(a_path)
    b = read_snapshot(b_path)

    kind_a = str(a.get("kind") or "").strip().lower()
    kind_b = str(b.get("kind") or "").strip().lower()
    feat_a = str(a.get("feature") or "").strip().lower()
    feat_b = str(b.get("feature") or "").strip().lower()

    if kind_a == SNAPSHOT_BUNDLE_KIND or feat_a == "all":
        raise ValueError("Snapshot A is a bundle. Pick a watchlist/ratings/history snapshot.")
    if kind_b == SNAPSHOT_BUNDLE_KIND or feat_b == "all":
        raise ValueError("Snapshot B is a bundle. Pick a watchlist/ratings/history snapshot.")

    items_a = a.get("items") or {}
    items_b = b.get("items") or {}
    if not isinstance(items_a, Mapping) or not isinstance(items_b, Mapping):
        raise ValueError("Invalid snapshot contents")

    keys_a = set(str(k) for k in items_a.keys())
    keys_b = set(str(k) for k in items_b.keys())

    added_keys = sorted(keys_b - keys_a)
    removed_keys = sorted(keys_a - keys_b)

    common = sorted(keys_a & keys_b)
    updated_keys: list[str] = []
    for k in common:
        if items_a.get(k) != items_b.get(k):
            updated_keys.append(k)

    unchanged = len(common) - len(updated_keys)

    def meta(s: Mapping[str, Any]) -> dict[str, Any]:
        stats_raw = s.get("stats")
        stats: Mapping[str, Any] = stats_raw if isinstance(stats_raw, Mapping) else {}
        return {
            "path": str(s.get("path") or ""),
            "provider": str(s.get("provider") or ""),
            "instance": str(s.get("instance") or s.get("instance_id") or s.get("profile") or "default"),
            "feature": str(s.get("feature") or ""),
            "label": str(s.get("label") or ""),
            "created_at": str(s.get("created_at") or ""),
            "count": int(stats.get("count") or 0),
        }

    lim = max(1, min(int(limit or 200), 2000))

    added = [{"key": k, "item": _brief_item(items_b.get(k))} for k in added_keys[:lim]]
    removed = [{"key": k, "item": _brief_item(items_a.get(k))} for k in removed_keys[:lim]]

    updated: list[dict[str, Any]] = []
    for k in updated_keys[:lim]:
        va = items_a.get(k)
        vb = items_b.get(k)
        changes: list[dict[str, Any]] = []
        _diff_any(va, vb, path="", out=changes, max_depth=max_depth, max_changes=max_changes)
        updated.append({"key": k, "old": _brief_item(va), "new": _brief_item(vb), "changes": changes})

    return {
        "ok": True,
        "a": meta(a),
        "b": meta(b),
        "summary": {
            "total_a": len(keys_a),
            "total_b": len(keys_b),
            "added": len(added_keys),
            "removed": len(removed_keys),
            "updated": len(updated_keys),
            "unchanged": unchanged,
        },
        "added": added,
        "removed": removed,
        "updated": updated,
        "truncated": {
            "added": len(added_keys) > lim,
            "removed": len(removed_keys) > lim,
            "updated": len(updated_keys) > lim,
        },
        "limit": lim,
    }
