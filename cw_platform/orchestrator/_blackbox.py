# cw_platform/orchestration/_blackbox.py
# Blackbox logic for managing state and flap counters.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from pathlib import Path
from collections.abc import Mapping, Iterable
from typing import Any
import json, time
import shutil

from ._scope import scope_safe

STATE_DIR = Path("/config/.cw_state")

def _read_json(p: Path) -> dict[str, Any]:
    try:
        if not p.exists():
            return {}
        return json.loads(p.read_text("utf-8")) or {}
    except Exception:
        return {}

def _write_json(p: Path, obj: dict[str, Any]) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        pass

def _bb_path(dst: str, feature: str, pair: str | None = None, instance: str | None = None) -> Path:
    dst = str(dst).strip().lower()
    feature = str(feature).strip().lower()
    scope = scope_safe(instance, pair=str(pair).strip().lower() if pair else None)
    scoped = STATE_DIR / f"{dst}_{feature}.{scope}.blackbox.json"
    legacy = STATE_DIR / f"{dst}_{feature}.blackbox.json"
    if instance is None and not scoped.exists() and legacy.exists():
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(legacy, scoped)
        except Exception:
            pass
    return scoped

def _flap_path(dst: str, feature: str, pair: str | None = None, instance: str | None = None) -> Path:
    dst = str(dst).strip().lower()
    feature = str(feature).strip().lower()
    scope = scope_safe(instance, pair=str(pair).strip().lower() if pair else None)
    scoped = STATE_DIR / f"{dst}_{feature}.{scope}.flap.json"
    legacy = STATE_DIR / f"{dst}_{feature}.flap.json"
    if instance is None and not scoped.exists() and legacy.exists():
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(legacy, scoped)
        except Exception:
            pass
    return scoped

_DEFAULT_BB: dict[str, Any] = {
    "enabled": True,
    "promote_after": 3,
    "pair_scoped": True,
    "cooldown_days": 30,
    "block_adds": True,
    "block_removes": True,
}

def _load_bb_cfg(cfg: Mapping[str, Any] | None) -> dict[str, Any]:
    try:
        if cfg and isinstance(cfg, Mapping):
            if "sync" in cfg:
                bb = ((cfg.get("sync") or {}).get("blackbox") or {})
                return {**_DEFAULT_BB, **bb}
            if any(k in cfg for k in ("promote_after", "pair_scoped", "enabled")):
                return {**_DEFAULT_BB, **cfg}
        conf_p = Path("/config/config.json")
        if conf_p.exists():
            raw = json.loads(conf_p.read_text("utf-8")) or {}
            bb = ((raw.get("sync") or {}).get("blackbox") or {})
            return {**_DEFAULT_BB, **bb}
    except Exception:
        pass
    return dict(_DEFAULT_BB)

def load_blackbox_keys(dst: str, feature: str, pair: str | None = None, instance: str | None = None) -> set[str]:
    keys: set[str] = set()
    glob = _read_json(_bb_path(dst, feature, instance=instance))
    keys |= set(glob.keys())
    if pair:
        prs = _read_json(_bb_path(dst, feature, pair, instance=instance))
        keys |= set(prs.keys())
    return keys

def load_flap_counters(dst: str, feature: str, instance: str | None = None) -> dict[str, dict[str, Any]]:
    return _read_json(_flap_path(dst, feature, instance=instance))

def inc_flap(dst: str, feature: str, key: str, *, reason: str, op: str, ts: int | None = None, instance: str | None = None) -> int:
    ts = int(ts or time.time())
    path = _flap_path(dst, feature, instance=instance)
    m = _read_json(path)
    row = m.setdefault(key, {})
    row["consecutive"] = int(row.get("consecutive") or 0) + 1
    row["last_reason"] = str(reason or "")
    row["last_op"] = str(op or "")
    row["last_attempt_ts"] = ts
    _write_json(path, m)
    return int(row["consecutive"])

def reset_flap(dst: str, feature: str, key: str, *, ts: int | None = None, instance: str | None = None) -> None:
    ts = int(ts or time.time())
    path = _flap_path(dst, feature, instance=instance)
    m = _read_json(path)
    row = m.setdefault(key, {})
    row["consecutive"] = 0
    row["last_reason"] = "ok"
    row["last_op"] = str(row.get("last_op") or "")
    row["last_success_ts"] = ts
    _write_json(path, m)

def _promote(dst: str, feature: str, key: str, *, reason: str, ts: int, pair: str | None, instance: str | None = None) -> None:
    path = _bb_path(dst, feature, pair, instance=instance)
    data = _read_json(path)
    if key not in data:
        data[key] = {"reason": str(reason or "flapper"), "since": int(ts)}
        _write_json(path, data)

def _normalize_keys(keys: Iterable[str] | None) -> tuple[list[str], list[str]]:
    ordered: list[str] = []
    unique: list[str] = []
    seen: set[str] = set()
    for raw in (keys or []):
        try:
            key = str(raw)
        except Exception:
            continue
        ordered.append(key)
        if key not in seen:
            seen.add(key)
            unique.append(key)
    return ordered, unique


def record_attempts(
    dst: str,
    feature: str,
    keys: Iterable[str],
    *,
    reason: str = "apply:add:failed",
    op: str = "add",
    pair: str | None = None,
    cfg: Mapping[str, Any] | None = None,
    instance: str | None = None,
) -> dict[str, Any]:
    bb = _load_bb_cfg(cfg)
    ordered_keys, unique_keys = _normalize_keys(keys)
    pair_scoped = bool(bb.get("pair_scoped", True))
    scoped_pair = pair if pair_scoped else None
    if not bool(bb.get("enabled", True)):
        return {
            "ok": True,
            "count": len(ordered_keys),
            "promoted": 0,
            "promoted_keys": [],
            "pair": scoped_pair or "global",
            "disabled": True,
        }

    ts = int(time.time())
    flap_path = _flap_path(dst, feature, instance=instance)
    flap_data = _read_json(flap_path)

    promote_after = int(bb.get("promote_after", 3) or 3)
    bb_path = _bb_path(dst, feature, scoped_pair, instance=instance)
    bb_data = _read_json(bb_path)

    flap_changed = False
    bb_changed = False
    promoted = 0
    promoted_keys: list[str] = []

    for key in unique_keys:
        row = flap_data.setdefault(key, {})
        row["consecutive"] = int(row.get("consecutive") or 0) + 1
        row["last_reason"] = str(reason or "")
        row["last_op"] = str(op or "")
        row["last_attempt_ts"] = ts
        flap_changed = True

        should_promote = False
        promote_reason = ""
        cons = int(row.get("consecutive") or 0)
        if cons >= promote_after:
            should_promote = True
            promote_reason = f"flapper:consecutive>={promote_after}"

        if should_promote:
            flap_data.pop(key, None)
            if key not in bb_data:
                bb_data[key] = {"reason": promote_reason or str(reason or "flapper"), "since": int(ts)}
                bb_changed = True
                promoted += 1
                promoted_keys.append(key)

    if flap_changed:
        _write_json(flap_path, flap_data)
    if bb_changed:
        _write_json(bb_path, bb_data)

    return {"ok": True, "count": len(ordered_keys), "promoted": promoted, "promoted_keys": promoted_keys, "pair": scoped_pair or "global"}


def _feature_files(dst: str, feature: str, suffix: str, instance: str | None = None) -> list[Path]:
    prefix = f"{str(dst).strip().lower()}_{str(feature).strip().lower()}."
    try:
        if not STATE_DIR.exists():
            return []
        return [p for p in STATE_DIR.glob(f"{prefix}*") if p.is_file() and p.name.endswith(suffix) and (instance is None or p.name.endswith(f".{scope_safe(instance)}{suffix}"))]
    except Exception:
        return []


def clear_keys(dst: str, feature: str, keys: Iterable[str], instance: str | None = None) -> dict[str, Any]:
    _, unique_keys = _normalize_keys(keys)
    key_set = set(unique_keys)
    if not key_set:
        return {"ok": True, "blackbox_removed": 0, "flap_removed": 0}
    removed = {".blackbox.json": 0, ".flap.json": 0}
    for suffix in removed:
        for path in _feature_files(dst, feature, suffix, instance=instance):
            data = _read_json(path)
            hits = [k for k in data if k in key_set]
            if not hits:
                continue
            for k in hits:
                data.pop(k, None)
            removed[suffix] += len(hits)
            _write_json(path, data)
    return {"ok": True, "blackbox_removed": removed[".blackbox.json"], "flap_removed": removed[".flap.json"]}


def record_success(
    dst: str,
    feature: str,
    keys: Iterable[str],
    *,
    pair: str | None = None,
    cfg: Mapping[str, Any] | None = None,
    instance: str | None = None,
) -> dict[str, Any]:
    ordered_keys, unique_keys = _normalize_keys(keys)
    if not unique_keys:
        return {"ok": True, "count": 0}
    cleared = clear_keys(dst, feature, unique_keys, instance=instance)
    return {"ok": True, "count": len(ordered_keys), "blackbox_removed": cleared["blackbox_removed"]}

def _prune_flap_file(p: Path, *, now: int, cooldown_days: int) -> int:
    data = _read_json(p)
    if not data:
        return 0
    blocked = set(_read_json(p.with_name(p.name[: -len(".flap.json")] + ".blackbox.json")).keys())
    removed = 0
    for k in list(data.keys()):
        raw = data.get(k)
        row: dict[str, Any] = raw if isinstance(raw, dict) else {}
        cons = int(row.get("consecutive") or 0)
        last = int(row.get("last_attempt_ts") or 0)
        stale = not last or (now - last) > (cooldown_days * 86400)
        if cons <= 0 or stale or k in blocked:
            data.pop(k, None)
            removed += 1
    if removed:
        _write_json(p, data)
    return removed


def prune_blackbox(*, cooldown_days: int = 30) -> tuple[int, int]:
    scanned = 0
    removed = 0
    now = int(time.time())
    if not STATE_DIR.exists():
        return (0, 0)
    for p in STATE_DIR.iterdir():
        if not p.is_file():
            continue
        name = p.name
        if not name.endswith(".blackbox.json"):
            continue
        scanned += 1
        data = _read_json(p)
        changed = False
        for k in list(data.keys()):
            since = int((data.get(k) or {}).get("since") or 0)
            if since and (now - since) > (cooldown_days * 86400):
                data.pop(k, None)
                changed = True
                removed += 1
        if changed:
            _write_json(p, data)
    for p in STATE_DIR.iterdir():
        if p.is_file() and p.name.endswith(".flap.json"):
            _prune_flap_file(p, now=now, cooldown_days=cooldown_days)
    return (scanned, removed)
