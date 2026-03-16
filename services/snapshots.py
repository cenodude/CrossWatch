# services/snapshots.py
# CrossWatch - Provider captures (watchlist/ratings/history/progress)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Literal

import json
import os
import re
import uuid

from cw_platform.config_base import CONFIG, load_config
from cw_platform.modules_registry import MODULES as MR_MODULES, load_sync_ops
from cw_platform.provider_instances import build_provider_config_view, list_instance_ids, normalize_instance_id

Feature = Literal["watchlist", "ratings", "history", "progress"]
CreateFeature = Literal["watchlist", "ratings", "history", "progress", "all"]
RestoreMode = Literal["merge", "clear_restore"]

SNAPSHOT_KIND = "snapshot"
SNAPSHOT_BUNDLE_KIND = "snapshot_bundle"
SNAPSHOT_FEATURES: tuple[Feature, ...] = ("watchlist", "ratings", "history", "progress")


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _registry_sync_providers() -> list[str]:
    return [k.replace("_mod_", "").upper() for k in (MR_MODULES.get("SYNC") or {}).keys()]


def _safe_label(label: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9._ -]+", "", str(label or "").strip())
    s = re.sub(r"\s+", " ", s).strip()
    return s[:60] if s else "capture"


def render_capture_label_template(
    template: str,
    *,
    provider: str,
    instance: Any | None = None,
    feature: str,
    ts: datetime | None = None,
) -> str:
    stamp = ts if isinstance(ts, datetime) else _utc_now()
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    inst = normalize_instance_id(instance)
    raw = str(template or "").strip()
    if not raw:
        raw = "auto-{provider}-{feature}-{date}"

    values = {
        "provider": str(provider or "").strip().upper(),
        "provider_lower": str(provider or "").strip().lower(),
        "instance": inst,
        "feature": str(feature or "").strip().lower(),
        "date": stamp.strftime("%Y-%m-%d"),
        "time": stamp.strftime("%H-%M"),
        "datetime": stamp.strftime("%Y-%m-%d_%H-%M"),
        "stamp": stamp.strftime("%Y%m%dT%H%M%SZ"),
    }
    try:
        rendered = raw.format_map(values)
    except Exception:
        rendered = raw
    return _safe_label(rendered)


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


def _resolve_snapshot_file(path: str, *, must_exist: bool = True) -> tuple[str, Path]:
    raw = str(path or "").strip().replace("\\", "/")
    if not raw:
        raise ValueError("Snapshot path is required")

    posix = PurePosixPath(raw)
    if posix.is_absolute():
        raise ValueError("Invalid snapshot path")

    parts = [part for part in posix.parts if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        raise ValueError("Invalid snapshot path")
    if Path(parts[-1]).suffix.lower() != ".json":
        raise ValueError("Invalid snapshot path")

    rel = "/".join(parts)
    base = _snapshots_dir().resolve()
    target = base.joinpath(*parts).resolve()
    try:
        target.relative_to(base)
    except ValueError as e:
        raise ValueError("Invalid snapshot path") from e

    if must_exist and (not target.exists() or not target.is_file()):
        raise ValueError("Snapshot not found")
    return rel, target


def _norm_feature(x: str) -> Feature:
    v = str(x or "").strip().lower()
    if v not in ("watchlist", "ratings", "history", "progress"):
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


def _build_index_capture_mode(
    *,
    ops: Any,
    cfg_view: Mapping[str, Any],
    pid: str,
    instance: str,
    feat: Feature,
    ts: datetime,
) -> Any:
    prev: dict[str, str | None] = {
        "CW_CAPTURE_MODE": os.environ.get("CW_CAPTURE_MODE"),
        "CW_CAPTURE_PROVIDER": os.environ.get("CW_CAPTURE_PROVIDER"),
        "CW_CAPTURE_INSTANCE": os.environ.get("CW_CAPTURE_INSTANCE"),
        "CW_CAPTURE_FEATURE": os.environ.get("CW_CAPTURE_FEATURE"),
        "CW_CAPTURE_ID": os.environ.get("CW_CAPTURE_ID"),
    }
    os.environ["CW_CAPTURE_MODE"] = "1"
    os.environ["CW_CAPTURE_PROVIDER"] = str(pid or "").strip().upper()
    os.environ["CW_CAPTURE_INSTANCE"] = normalize_instance_id(instance)
    os.environ["CW_CAPTURE_FEATURE"] = str(feat or "").strip().lower()
    os.environ["CW_CAPTURE_ID"] = ts.strftime("%Y%m%dT%H%M%SZ")
    try:
        return ops.build_index(cfg_view, feature=feat) or {}
    finally:
        for key, value in prev.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


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
            feats = {k: bool(raw.get(k)) for k in SNAPSHOT_FEATURES}
        except Exception:
            feats = {"watchlist": False, "ratings": False, "history": False, "progress": False}

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

def _as_int(v: Any) -> int | None:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        return int(v)
    except Exception:
        return None


def _item_kind(it: Mapping[str, Any]) -> str:
    t = str(it.get("type") or it.get("media_type") or it.get("entity") or "").strip().lower()
    if t in ("episode",):
        return "episode"
    if t in ("season",):
        return "season"
    if t in ("tv", "show", "shows", "series", "anime"):
        return "show"
    if t in ("movie", "movies", "film", "films"):
        return "movie"
    if it.get("episode") is not None:
        return "episode"
    if it.get("season") is not None:
        return "season"
    return "unknown"


def _pick_id(
    ids: Mapping[str, Any] | None,
    show_ids: Mapping[str, Any] | None,
    order: Sequence[str],
) -> tuple[str, str, str] | None:
    ids = ids if isinstance(ids, Mapping) else {}
    show_ids = show_ids if isinstance(show_ids, Mapping) else {}

    for k in order:
        v = ids.get(k)
        if v not in (None, "", 0, False):
            return (str(k), str(v), "ids")
    for k in order:
        v = show_ids.get(k)
        if v not in (None, "", 0, False):
            return (str(k), str(v), "show_ids")
    return None


def _epoch_from_item_or_key(item: Mapping[str, Any], orig_key: str) -> int | None:
    dt = item.get("watched_at") or item.get("watchedAt") or item.get("watched")
    parsed = _parse_iso_dt(dt)
    if parsed:
        try:
            return int(parsed.timestamp())
        except Exception:
            pass
    ts = _ts_from_history_key(orig_key)
    return ts or None


def _canonical_item_key(provider: str, feature: Feature, orig_key: str, item: Mapping[str, Any]) -> str:
    pid = str(provider or "").strip().upper()
    kind = _item_kind(item)

    raw_ids = item.get("ids")
    ids = raw_ids if isinstance(raw_ids, Mapping) else {}
    raw_show_ids = item.get("show_ids")
    show_ids = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}

    # Prefer the provider's own native ID
    native: dict[str, list[str]] = {
        "TRAKT": ["trakt"],
        "SIMKL": ["simkl", "simkl_id"],
        "TMDB": ["tmdb"],
        "MDBLIST": ["mdblist"],
        "PLEX": ["plex", "guid"],
        "JELLYFIN": ["jellyfin"],
        "EMBY": ["emby"],
        "ANILIST": ["anilist"],
    }

    if pid in native:
        picked = _pick_id(ids, show_ids, native[pid])
    else:
        picked = _pick_id(ids, show_ids, ["tmdb", "imdb", "tvdb", "trakt", "simkl", "anilist"])

    if not picked:
        return str(orig_key or "").strip()

    id_key, id_val, src = picked
    base = f"{id_key}:{id_val}"

    season = _as_int(item.get("season"))
    episode = _as_int(item.get("episode"))

    if kind == "season" and season is not None:
        if src == "show_ids" or id_key in ("tmdb", "imdb", "tvdb"):
            base = f"{base}#season:{season}"

    if kind == "episode" and season is not None and episode is not None:
        if src == "show_ids" or id_key in ("tmdb", "imdb", "tvdb"):
            base = f"{base}#s{season:02d}e{episode:02d}"

    if feature == "history":
        ts = _epoch_from_item_or_key(item, str(orig_key or ""))
        if ts:
            base = f"{base}@{ts}"

    return base


def _item_score(it: Mapping[str, Any]) -> int:
    score = 0
    raw_ids = it.get("ids")
    ids = raw_ids if isinstance(raw_ids, Mapping) else {}
    raw_show_ids = it.get("show_ids")
    show_ids = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
    score += len([1 for v in ids.values() if v not in (None, "", 0, False)])
    score += len([1 for v in show_ids.values() if v not in (None, "", 0, False)])
    score += len(it.keys())
    return score


def _canonicalize_index(provider: str, feature: Feature, items: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for k, v in items.items():
        if not k or not isinstance(v, Mapping):
            continue
        ck = _canonical_item_key(provider, feature, str(k), v)
        vv = dict(v)
        if ck in out:
            if _item_score(vv) > _item_score(out[ck]):
                out[ck] = vv
            continue
        out[ck] = vv
    return out




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
    idx_raw = _build_index_capture_mode(
        ops=ops,
        cfg_view=cfg_view,
        pid=pid,
        instance=instance,
        feat=feat,
        ts=ts,
    )
    idx = _index_dict(idx_raw)
    idx = _canonicalize_index(pid, feat, idx)
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

        for f in SNAPSHOT_FEATURES:
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


def _snapshot_sort_stamp(meta: Mapping[str, Any]) -> datetime:
    stamp = str(meta.get("stamp") or "").strip()
    if stamp:
        try:
            return datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        except Exception:
            pass
    mtime = int(meta.get("mtime") or 0)
    if mtime > 0:
        try:
            return datetime.fromtimestamp(mtime, tz=timezone.utc)
        except Exception:
            pass
    return datetime.fromtimestamp(0, tz=timezone.utc)


def enforce_capture_retention(
    provider: str,
    feature: CreateFeature | str,
    *,
    instance_id: Any | None = None,
    retention_days: int = 0,
    max_captures: int = 0,
    auto_delete_old: bool = False,
) -> dict[str, Any]:
    pid = _norm_provider(provider)
    feat_any = _norm_create_feature(str(feature or ""))
    inst = normalize_instance_id(instance_id)
    keep_days = max(0, int(retention_days or 0))
    keep_count = max(0, int(max_captures or 0))

    if not auto_delete_old:
        return {"ok": True, "matched": 0, "deleted": [], "errors": [], "applied": False}

    rows = [
        row for row in list_snapshots()
        if str(row.get("provider") or "").strip().upper() == pid
        and normalize_instance_id(row.get("instance") or "default") == inst
        and str(row.get("feature") or "").strip().lower() == feat_any
    ]
    rows.sort(key=_snapshot_sort_stamp, reverse=True)

    cutoff = _utc_now() - timedelta(days=keep_days) if keep_days > 0 else None
    to_delete: list[str] = []
    kept: list[dict[str, Any]] = []

    for row in rows:
        created = _snapshot_sort_stamp(row)
        if cutoff is not None and created < cutoff:
            to_delete.append(str(row.get("path") or ""))
            continue
        kept.append(row)

    if keep_count > 0 and len(kept) > keep_count:
        to_delete.extend(str(row.get("path") or "") for row in kept[keep_count:])

    deleted: list[str] = []
    errors: list[str] = []
    seen: set[str] = set()
    for path in to_delete:
        rel = str(path or "").strip()
        if not rel or rel in seen:
            continue
        seen.add(rel)
        try:
            res = delete_snapshot(rel)
            deleted.extend([str(x) for x in (res.get("deleted") or [])])
            errors.extend([str(x) for x in (res.get("errors") or [])])
        except Exception as e:
            errors.append(str(e))

    return {
        "ok": len(errors) == 0,
        "matched": len(rows),
        "deleted": deleted,
        "errors": errors,
        "applied": True,
    }


def read_snapshot(path: str) -> dict[str, Any]:
    rel, p = _resolve_snapshot_file(path)

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

    snap_items = _canonicalize_index(pid, feat, snap_items)
    cur = _canonicalize_index(pid, feat, cur)

    snap_keys = set(str(k) for k in snap_items.keys())
    cur_keys = set(cur.keys())

    to_add_keys = sorted(snap_keys - cur_keys)
    to_remove_keys: list[str] = []

    if mode == "clear_restore":
        to_remove_keys = sorted(cur_keys)
        to_add_keys = sorted(snap_keys)

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
    rel, p = _resolve_snapshot_file(path)
    base = _snapshots_dir().resolve()

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
    adapter: Any | None = None
    try:
        mk = getattr(ops, "_adapter", None)
        if callable(mk):
            adapter = mk(cfg_view)
    except Exception:
        adapter = None

    for f in features:
        feat = _norm_feature(f)
        if not _feature_enabled(ops, feat):
            done["results"][feat] = {"ok": True, "skipped": True, "reason": "feature_disabled"}
            continue
        if pid == "PLEX" and feat == "progress":
            done["results"][feat] = {"ok": True, "skipped": True, "reason": "unsupported_clear"}
            continue

        cur_raw = (adapter.build_index(feat) if adapter else ops.build_index(cfg_view, feature=feat)) or {}
        cur: list[Mapping[str, Any]] = []
        if isinstance(cur_raw, Mapping):
            for v in cur_raw.values():
                if isinstance(v, Mapping):
                    cur.append(dict(v))

        # Capture mode
        if feat == "history":
            for it in cur:
                if isinstance(it, dict):
                    it.setdefault("_cw_tool_clear", True)

        removed = 0
        unresolved: list[Any] = []
        errors: list[str] = []

        # Prefer a single remove call per feature. 
        try:
            res = (
                adapter.remove(feat, cur, dry_run=False)
                if adapter
                else ops.remove(cfg_view, cur, feature=feat, dry_run=False)
            ) or {}
            if isinstance(res, Mapping) and "count" in res:
                removed = int(res.get("count") or 0)
            else:
                removed = len(cur)
            if isinstance(res, Mapping) and isinstance(res.get("unresolved"), list):
                unresolved = list(res.get("unresolved") or [])
        except Exception as e:
            errors.append(str(e))

        ok = len(errors) == 0
        done["ok"] = done["ok"] and ok
        done["results"][feat] = {
            "ok": ok,
            "removed": removed,
            "count": len(cur),
            "unresolved": unresolved,
            "unresolved_count": len(unresolved),
            "errors": errors,
        }

    return done


def _brief_item(x: Any) -> dict[str, Any]:
    if not isinstance(x, Mapping):
        return {"value": x}
    out: dict[str, Any] = {}
    for k in ("type", "title", "year", "season", "episode", "status"):
        if k in x:
            out[k] = x.get(k)
    t = str(x.get("type") or "").lower()
    is_ep = t == "episode" or ("season" in x and "episode" in x)
    if is_ep:
        for k in ("series_title", "show_title"):
            if k in x:
                out[k] = x.get(k)
    ids = x.get("ids")
    if isinstance(ids, Mapping):
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

    # Ignore second-level for watched timestamps - no second-level precision
    leaf = (str(path or "").rsplit(".", 1)[-1]).lower()
    if leaf in ("watched_at", "watchedat", "progress_at", "progressat", "last_progress_at", "lastprogressat"):
        ma = _dt_minute_bucket(a)
        mb = _dt_minute_bucket(b)
        if ma is not None and mb is not None and ma == mb:
            return
    if leaf in ("watched_ats", "watchedats") and isinstance(a, Sequence) and isinstance(b, Sequence) and not isinstance(a, (str, bytes)) and not isinstance(b, (str, bytes)):
        a_set = set(_iso_minute(x) or str(x) for x in a)
        b_set = set(_iso_minute(x) or str(x) for x in b)
        if a_set == b_set:
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


def _parse_iso_dt(v: Any) -> datetime | None:
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _dt_minute_bucket(v: Any) -> int | None:
    """Returns an epoch-minute bucket for ISO timestamps.

    Used for compare-only logic where second-level differences are noise.
    """

    dt = _parse_iso_dt(v)
    if not dt:
        return None
    try:
        u = dt.astimezone(timezone.utc)
        return int(u.timestamp() // 60)
    except Exception:
        return None


def _iso_minute(v: Any) -> str | None:
    dt = _parse_iso_dt(v)
    if not dt:
        return None
    try:
        u = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
        return u.strftime("%Y-%m-%dT%H:%MZ")
    except Exception:
        return None


def _minute_set(xs: Sequence[Any]) -> set[str]:
    return set(_iso_minute(x) or str(x) for x in xs)


def _cmp_record_minute(rec: Mapping[str, Any]) -> dict[str, Any]:
    """Copy a record for stable comparisons.

    Keeps raw output intact but compares watched timestamps at minute precision.
    """

    out = dict(rec)
    for k in ("watched_at", "watchedAt", "watched"):
        if k in out:
            out[k] = _dt_minute_bucket(out.get(k))
    return out


def _history_base_key(key: str) -> str:
    return str(key or "").split("@", 1)[0]


def _ts_from_history_key(key: str) -> int:
    s = str(key or "")
    if "@" not in s:
        return 0
    tail = s.rsplit("@", 1)[-1].strip()
    try:
        return int(tail)
    except Exception:
        return 0


def _pick_newer_history_item(prev: Any, cand: Any, *, prev_key: str, cand_key: str) -> Any:
    a = _parse_iso_dt((prev or {}).get("watched_at") if isinstance(prev, Mapping) else None)
    b = _parse_iso_dt((cand or {}).get("watched_at") if isinstance(cand, Mapping) else None)
    if a and b:
        return cand if b > a else prev
    if a and not b:
        return prev
    if b and not a:
        return cand
    ta = _ts_from_history_key(prev_key)
    tb = _ts_from_history_key(cand_key)
    return cand if tb > ta else prev


def _history_items_by_base_key(items: Mapping[str, Any]) -> dict[str, Any]:
    grouped: dict[str, dict[str, Any]] = {}
    src: dict[str, str] = {}
    dates: dict[str, set[str]] = {}
    for k, v in items.items():
        raw_key = str(k)
        bk = _history_base_key(raw_key) or raw_key
        if bk not in grouped:
            vv: dict[str, Any] = dict(v) if isinstance(v, Mapping) else {"value": v}
            grouped[bk] = vv
            src[bk] = raw_key
            dates[bk] = set()
            
        dt = None
        if isinstance(v, Mapping):
            dt = v.get("watched_at") or v.get("watchedAt") or v.get("watched")
        if dt:
            dates[bk].add(str(dt))
        else:
            ts = _ts_from_history_key(raw_key)
            if ts:
                try:
                    dates[bk].add(datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
                except Exception:
                    pass

        if bk in src:
            prev = grouped[bk]
            picked = _pick_newer_history_item(prev, v, prev_key=src[bk], cand_key=raw_key)
            if picked is v:
                vv2: dict[str, Any] = dict(v) if isinstance(v, Mapping) else {"value": v}
                grouped[bk] = vv2
                src[bk] = raw_key

    out: dict[str, Any] = {}
    for bk, rep in grouped.items():
        item: dict[str, Any] = dict(rep) if isinstance(rep, Mapping) else {"value": rep}
        all_dates = sorted(dates.get(bk) or set())
        if all_dates:
            item["watched_ats"] = all_dates
            item["watched_at"] = all_dates[-1]
        out[bk] = item
    return out


def _snapshot_meta(s: Mapping[str, Any]) -> dict[str, Any]:
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
        "by_type": dict(stats.get("by_type") or {}) if isinstance(stats.get("by_type"), Mapping) else {},
        "features": dict(stats.get("features") or {}) if isinstance(stats.get("features"), Mapping) else {},
    }


def _bundle_compare_setup(a: Mapping[str, Any], b: Mapping[str, Any], feature: str = "") -> tuple[dict[str, str], dict[str, str], list[str], str]:
    def _child_paths(snap: Mapping[str, Any]) -> dict[str, str]:
        out: dict[str, str] = {}
        children = snap.get("children")
        if not isinstance(children, list):
            return out
        for child in children:
            if not isinstance(child, Mapping):
                continue
            feat = str(child.get("feature") or "").strip().lower()
            path = str(child.get("path") or "").strip()
            if feat in SNAPSHOT_FEATURES and path:
                out[feat] = path
        return out

    order = ("watchlist", "history", "ratings", "progress")
    a_children = _child_paths(a)
    b_children = _child_paths(b)
    features = [feat for feat in order if feat in a_children and feat in b_children]
    features += sorted(set(a_children) & set(b_children) - set(features))
    if not features:
        raise ValueError("No shared child captures found between these full captures.")
    selected = str(feature or "").strip().lower() or features[0]
    if selected != "all" and selected not in features:
        raise ValueError(f"Feature not available in both full captures: {selected}")
    return a_children, b_children, features, selected


def _with_feature_tag(item: Any, feature: str) -> Any:
    if not isinstance(item, Mapping):
        return item
    return {"feature": feature, **dict(item)}




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

    prov_a = str(a.get("provider") or "").strip().upper()
    prov_b = str(b.get("provider") or "").strip().upper()
    inst_a = str(a.get("instance") or a.get("instance_id") or a.get("profile") or "default").strip().lower()
    inst_b = str(b.get("instance") or b.get("instance_id") or b.get("profile") or "default").strip().lower()
    if prov_a != prov_b or inst_a != inst_b:
        raise ValueError("Compare Captures only supports the same provider and instance.")

    bundle_a = kind_a == SNAPSHOT_BUNDLE_KIND or feat_a == "all"
    bundle_b = kind_b == SNAPSHOT_BUNDLE_KIND or feat_b == "all"
    if bundle_a or bundle_b:
        if bundle_a and bundle_b:
            a_children, b_children, features, _ = _bundle_compare_setup(a, b, "all")
            summary = {"total_a": 0, "total_b": 0, "raw_total_a": 0, "raw_total_b": 0, "added": 0, "removed": 0, "updated": 0, "unchanged": 0}
            grouped: dict[str, list[dict[str, Any]]] = {"added": [], "removed": [], "updated": []}
            lim = max(1, min(int(limit or 200), 2000))

            for feat in features:
                child = diff_snapshots(a_children[feat], b_children[feat], limit=lim, max_depth=max_depth, max_changes=max_changes)
                child_summary = child.get("summary")
                if isinstance(child_summary, Mapping):
                    for key in summary:
                        summary[key] += int(child_summary.get(key) or 0)
                for status, payload_key in (("added", "item"), ("removed", "item"), ("updated", None)):
                    for row in child.get(status) or []:
                        if not isinstance(row, Mapping):
                            continue
                        item = row.get(payload_key) if payload_key else None
                        if isinstance(item, Mapping):
                            item = _with_feature_tag(item, feat)
                        grouped[status].append(
                            {"key": f"{feat}:{row.get('key')}", payload_key: item}
                            if payload_key
                            else {
                                "key": f"{feat}:{row.get('key')}",
                                "old": _with_feature_tag(row.get("old"), feat),
                                "new": _with_feature_tag(row.get("new"), feat),
                                "changes": row.get("changes") if isinstance(row.get("changes"), list) else [],
                            }
                        )

            meta_a = _snapshot_meta(a)
            meta_b = _snapshot_meta(b)
            meta_a.update({"feature": "all", "compared_features": features})
            meta_b.update({"feature": "all", "compared_features": features})
            return {
                "ok": True,
                "a": meta_a,
                "b": meta_b,
                "summary": summary,
                "added": grouped["added"][:lim],
                "removed": grouped["removed"][:lim],
                "updated": grouped["updated"][:lim],
                "truncated": {k: len(v) > lim for k, v in grouped.items()},
                "limit": lim,
                "available_features": features,
                "selected_feature": "all",
            }
        raise ValueError("Compare Captures only supports two full captures or two matching feature captures.")

    items_a_raw = a.get("items") or {}
    items_b_raw = b.get("items") or {}
    if not isinstance(items_a_raw, Mapping) or not isinstance(items_b_raw, Mapping):
        raise ValueError("Invalid capture contents")

    if feat_a != feat_b:
        raise ValueError("Compare Captures only supports the same feature.")

    items_a_raw = _canonicalize_index(prov_a, _norm_feature(feat_a), items_a_raw)
    items_b_raw = _canonicalize_index(prov_a, _norm_feature(feat_a), items_b_raw)

    history_multi = False
    if feat_a == "history":
        items_a = _history_items_by_base_key(items_a_raw)
        items_b = _history_items_by_base_key(items_b_raw)
        history_multi = True
    else:
        items_a = dict(items_a_raw)
        items_b = dict(items_b_raw)

    keys_a = set(str(k) for k in items_a.keys())
    keys_b = set(str(k) for k in items_b.keys())

    added_keys = sorted(keys_b - keys_a)
    removed_keys = sorted(keys_a - keys_b)

    common = sorted(keys_a & keys_b)
    updated_keys: list[str] = []
    for k in common:
        va = items_a.get(k)
        vb = items_b.get(k)
        if history_multi:
            oa = va if isinstance(va, Mapping) else {}
            ob = vb if isinstance(vb, Mapping) else {}
            a_tmp = oa.get("watched_ats")
            b_tmp = ob.get("watched_ats")
            a_dates = a_tmp if isinstance(a_tmp, list) else []
            b_dates = b_tmp if isinstance(b_tmp, list) else []
            a_set = set(_iso_minute(x) or str(x) for x in a_dates)
            b_set = set(_iso_minute(x) or str(x) for x in b_dates)
            if a_set != b_set:
                updated_keys.append(k)
                continue
            oa2 = dict(oa)
            ob2 = dict(ob)
            oa2.pop("watched_ats", None)
            ob2.pop("watched_ats", None)
            if _cmp_record_minute(oa2) != _cmp_record_minute(ob2):
                updated_keys.append(k)
            continue
        if va != vb:
            updated_keys.append(k)

    unchanged = len(common) - len(updated_keys)

    lim = max(1, min(int(limit or 200), 2000))

    added = [{"key": k, "item": _brief_item(items_b.get(k))} for k in added_keys[:lim]]
    removed = [{"key": k, "item": _brief_item(items_a.get(k))} for k in removed_keys[:lim]]

    updated: list[dict[str, Any]] = []
    for k in updated_keys[:lim]:
        va = items_a.get(k)
        vb = items_b.get(k)
        changes: list[dict[str, Any]] = []

        if history_multi:
            oa = va if isinstance(va, Mapping) else {}
            ob = vb if isinstance(vb, Mapping) else {}
            a_tmp = oa.get("watched_ats")
            b_tmp = ob.get("watched_ats")
            a_dates = a_tmp if isinstance(a_tmp, list) else []
            b_dates = b_tmp if isinstance(b_tmp, list) else []
            a_set = _minute_set([str(x) for x in a_dates])
            b_set = _minute_set([str(x) for x in b_dates])
            added_dates = sorted(b_set - a_set)
            removed_dates = sorted(a_set - b_set)

            if added_dates:
                changes.append({"path": "watched_ats.added", "old": [], "new": added_dates})
            if removed_dates:
                changes.append({"path": "watched_ats.removed", "old": removed_dates, "new": []})

            oa2 = dict(oa)
            ob2 = dict(ob)
            oa2.pop("watched_ats", None)
            ob2.pop("watched_ats", None)
            if _cmp_record_minute(oa2) != _cmp_record_minute(ob2):
                _diff_any(oa2, ob2, path="", out=changes, max_depth=max_depth, max_changes=max_changes)
        else:
            _diff_any(va, vb, path="", out=changes, max_depth=max_depth, max_changes=max_changes)

        updated.append({"key": k, "old": _brief_item(va), "new": _brief_item(vb), "changes": changes})

    stats_a_raw = a.get("stats")
    stats_b_raw = b.get("stats")
    stats_a: Mapping[str, Any] = stats_a_raw if isinstance(stats_a_raw, Mapping) else {}
    stats_b: Mapping[str, Any] = stats_b_raw if isinstance(stats_b_raw, Mapping) else {}

    return {
        "ok": True,
        "a": _snapshot_meta(a),
        "b": _snapshot_meta(b),
        "summary": {
            "total_a": len(keys_a),
            "total_b": len(keys_b),
            "raw_total_a": int(stats_a.get("count") or len(items_a_raw)),
            "raw_total_b": int(stats_b.get("count") or len(items_b_raw)),
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


def diff_snapshots_extended(
    a_path: str,
    b_path: str,
    *,
    feature: str = "",
    kind: str = "all",
    q: str = "",
    offset: int = 0,
    limit: int = 5000,
    max_depth: int = 6,
    max_changes: int = 250,
) -> dict[str, Any]:

    a = read_snapshot(a_path)
    b = read_snapshot(b_path)

    kind_a = str(a.get("kind") or "").strip().lower()
    kind_b = str(b.get("kind") or "").strip().lower()
    feat_a = str(a.get("feature") or "").strip().lower()
    feat_b = str(b.get("feature") or "").strip().lower()

    prov_a = str(a.get("provider") or "").strip().upper()
    prov_b = str(b.get("provider") or "").strip().upper()
    inst_a = str(a.get("instance") or a.get("instance_id") or a.get("profile") or "default").strip().lower()
    inst_b = str(b.get("instance") or b.get("instance_id") or b.get("profile") or "default").strip().lower()
    if prov_a != prov_b or inst_a != inst_b:
        raise ValueError("Compare Captures only supports the same provider and instance.")

    bundle_a = kind_a == SNAPSHOT_BUNDLE_KIND or feat_a == "all"
    bundle_b = kind_b == SNAPSHOT_BUNDLE_KIND or feat_b == "all"
    selected_feature = str(feature or "").strip().lower()
    if bundle_a or bundle_b:
        if not (bundle_a and bundle_b):
            raise ValueError("Advanced compare supports either two full captures or two matching feature captures.")
        a_children, b_children, available_features, selected_feature = _bundle_compare_setup(a, b, selected_feature)
        a_child = a_children.get(selected_feature)
        b_child = b_children.get(selected_feature)
        if not a_child or not b_child:
            raise ValueError(f"Feature not available in both full captures: {selected_feature}")

        child_res = diff_snapshots_extended(
            a_child,
            b_child,
            feature="",
            kind=kind,
            q=q,
            offset=offset,
            limit=limit,
            max_depth=max_depth,
            max_changes=max_changes,
        )
        meta_a = _snapshot_meta(a)
        meta_b = _snapshot_meta(b)
        meta_a.update({"feature": "all", "compared_features": available_features})
        meta_b.update({"feature": "all", "compared_features": available_features})
        child_res["a"] = meta_a
        child_res["b"] = meta_b
        child_res["available_features"] = available_features
        child_res["selected_feature"] = selected_feature
        return child_res

    items_a_raw = a.get("items") or {}
    items_b_raw = b.get("items") or {}
    if not isinstance(items_a_raw, Mapping) or not isinstance(items_b_raw, Mapping):
        raise ValueError("Invalid capture contents")

    if feat_a != feat_b:
        raise ValueError("Compare Captures only supports the same feature.")

    feat = _norm_feature(feat_a)
    items_a_raw = _canonicalize_index(prov_a, feat, items_a_raw)
    items_b_raw = _canonicalize_index(prov_a, feat, items_b_raw)

    history_multi = False
    if feat_a == "history":
        items_a = _history_items_by_base_key(items_a_raw)
        items_b = _history_items_by_base_key(items_b_raw)
        history_multi = True
    else:
        items_a = dict(items_a_raw)
        items_b = dict(items_b_raw)

    keys_a = set(str(k) for k in items_a.keys())
    keys_b = set(str(k) for k in items_b.keys())
    common = sorted(keys_a & keys_b)

    added_keys = sorted(keys_b - keys_a)
    removed_keys = sorted(keys_a - keys_b)

    updated_keys: list[str] = []
    unchanged_keys: list[str] = []
    for k in common:
        va = items_a.get(k)
        vb = items_b.get(k)
        if history_multi:
            oa = va if isinstance(va, Mapping) else {}
            ob = vb if isinstance(vb, Mapping) else {}
            a_tmp = oa.get("watched_ats")
            b_tmp = ob.get("watched_ats")
            a_dates = a_tmp if isinstance(a_tmp, list) else []
            b_dates = b_tmp if isinstance(b_tmp, list) else []
            a_set = _minute_set([str(x) for x in a_dates])
            b_set = _minute_set([str(x) for x in b_dates])
            if a_set != b_set:
                updated_keys.append(k)
                continue
            oa2 = dict(oa)
            ob2 = dict(ob)
            oa2.pop("watched_ats", None)
            ob2.pop("watched_ats", None)
            if _cmp_record_minute(oa2) != _cmp_record_minute(ob2):
                updated_keys.append(k)
            else:
                unchanged_keys.append(k)
            continue
        if va != vb:
            updated_keys.append(k)
        else:
            unchanged_keys.append(k)

    # Filter helpers
    want = str(kind or "all").strip().lower()
    if want not in ("all", "added", "removed", "updated", "unchanged"):
        want = "all"
    needle = str(q or "").strip().lower()

    def _hay(k: str, it: Any) -> str:
        brief = _brief_item(it)
        parts = [k]
        if isinstance(brief, Mapping):
            for kk in ("title", "series_title", "show_title", "type", "year", "season", "episode", "status"):
                vv = brief.get(kk)
                if vv not in (None, "", 0, False):
                    parts.append(str(vv))
            ids = brief.get("ids")
            if isinstance(ids, Mapping):
                parts.extend([f"{ik}:{iv}" for ik, iv in ids.items() if iv not in (None, "", 0, False)])
        return " ".join(parts).lower()

    def _sort_tuple(k: str, it: Any) -> tuple[Any, ...]:
        b = _brief_item(it)
        title = ""
        year = 0
        season = -1
        episode = -1
        typ = ""
        if isinstance(b, Mapping):
            typ = str(b.get("type") or "")
            title = str(b.get("series_title") or b.get("show_title") or b.get("title") or "")
            year = int(b.get("year") or 0) if str(b.get("year") or "").isdigit() else 0
            season = int(b.get("season") or -1) if str(b.get("season") or "").lstrip("-").isdigit() else -1
            episode = int(b.get("episode") or -1) if str(b.get("episode") or "").lstrip("-").isdigit() else -1
        return (title.lower(), year, season, episode, typ, k)

    def _mk_row(status: str, k: str) -> dict[str, Any]:
        va = items_a.get(k)
        vb = items_b.get(k)
        row: dict[str, Any] = {"key": k, "status": status, "brief": _brief_item(vb if vb is not None else va)}
        if status == "added":
            row["new"] = vb
        elif status == "removed":
            row["old"] = va
        elif status == "unchanged":
            row["item"] = vb
        else:  # updated
            row["old"] = va
            row["new"] = vb
            changes: list[dict[str, Any]] = []
            if history_multi:
                oa = va if isinstance(va, Mapping) else {}
                ob = vb if isinstance(vb, Mapping) else {}
                a_tmp = oa.get("watched_ats")
                b_tmp = ob.get("watched_ats")
                a_dates = a_tmp if isinstance(a_tmp, list) else []
                b_dates = b_tmp if isinstance(b_tmp, list) else []
                a_set = _minute_set([str(x) for x in a_dates])
                b_set = _minute_set([str(x) for x in b_dates])
                add_dates = sorted(b_set - a_set)
                del_dates = sorted(a_set - b_set)
                if add_dates:
                    changes.append({"path": "watched_ats.added", "old": [], "new": add_dates})
                if del_dates:
                    changes.append({"path": "watched_ats.removed", "old": del_dates, "new": []})
                oa2 = dict(oa)
                ob2 = dict(ob)
                oa2.pop("watched_ats", None)
                ob2.pop("watched_ats", None)
                if _cmp_record_minute(oa2) != _cmp_record_minute(ob2):
                    _diff_any(oa2, ob2, path="", out=changes, max_depth=max_depth, max_changes=max_changes)
            else:
                _diff_any(va, vb, path="", out=changes, max_depth=max_depth, max_changes=max_changes)
            row["changes"] = changes
        return row

    # Build full list
    groups: list[tuple[str, list[str]]] = [
        ("added", added_keys),
        ("removed", removed_keys),
        ("updated", updated_keys),
        ("unchanged", unchanged_keys),
    ]

    rows_all: list[dict[str, Any]] = []
    for st, keys in groups:
        if want != "all" and st != want:
            continue

        keys_sorted = sorted(keys, key=lambda kk: _sort_tuple(kk, items_b.get(kk) if st != "removed" else items_a.get(kk)))
        for kk in keys_sorted:
            it = items_b.get(kk) if st != "removed" else items_a.get(kk)
            if needle:
                if needle not in _hay(kk, it):
                    continue
            rows_all.append(_mk_row(st, kk))

    off = max(0, int(offset or 0))
    lim = max(1, min(int(limit or 5000), 20000))
    page_rows = rows_all[off : off + lim]

    stats_a_raw = a.get("stats")
    stats_b_raw = b.get("stats")
    stats_a: Mapping[str, Any] = stats_a_raw if isinstance(stats_a_raw, Mapping) else {}
    stats_b: Mapping[str, Any] = stats_b_raw if isinstance(stats_b_raw, Mapping) else {}

    return {
        "ok": True,
        "a": _snapshot_meta(a),
        "b": _snapshot_meta(b),
        "summary": {
            "total_a": len(keys_a),
            "total_b": len(keys_b),
            "raw_total_a": int(stats_a.get("count") or len(items_a_raw)),
            "raw_total_b": int(stats_b.get("count") or len(items_b_raw)),
            "added": len(added_keys),
            "removed": len(removed_keys),
            "updated": len(updated_keys),
            "unchanged": len(unchanged_keys),
        },
        "query": {"kind": want, "q": needle, "offset": off, "limit": lim},
        "total": len(rows_all),
        "items": page_rows,
        "available_features": [feat],
        "selected_feature": feat,
    }
