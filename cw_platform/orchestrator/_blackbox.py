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

def _bb_path(dst: str, feature: str, pair: str | None = None) -> Path:
    dst = str(dst).strip().lower()
    feature = str(feature).strip().lower()
    scope = str(pair).strip().lower() if pair else scope_safe()
    scoped = STATE_DIR / f"{dst}_{feature}.{scope}.blackbox.json"
    legacy = STATE_DIR / f"{dst}_{feature}.blackbox.json"
    if not scoped.exists() and legacy.exists():
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(legacy, scoped)
        except Exception:
            pass
    return scoped

def _flap_path(dst: str, feature: str, pair: str | None = None) -> Path:
    dst = str(dst).strip().lower()
    feature = str(feature).strip().lower()
    scope = str(pair).strip().lower() if pair else scope_safe()
    scoped = STATE_DIR / f"{dst}_{feature}.{scope}.flap.json"
    legacy = STATE_DIR / f"{dst}_{feature}.flap.json"
    if not scoped.exists() and legacy.exists():
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(legacy, scoped)
        except Exception:
            pass
    return scoped

_DEFAULT_BB: dict[str, Any] = {
    "enabled": True,
    "promote_after": 3,
    "unresolved_days": 0,
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

def load_blackbox_keys(dst: str, feature: str, pair: str | None = None) -> set[str]:
    keys: set[str] = set()
    glob = _read_json(_bb_path(dst, feature))
    keys |= set(glob.keys())
    if pair:
        prs = _read_json(_bb_path(dst, feature, pair))
        keys |= set(prs.keys())
    return keys

def load_flap_counters(dst: str, feature: str) -> dict[str, dict[str, Any]]:
    return _read_json(_flap_path(dst, feature))

def inc_flap(dst: str, feature: str, key: str, *, reason: str, op: str, ts: int | None = None) -> int:
    ts = int(ts or time.time())
    path = _flap_path(dst, feature)
    m = _read_json(path)
    row = m.setdefault(key, {})
    row["consecutive"] = int(row.get("consecutive") or 0) + 1
    row["last_reason"] = str(reason or "")
    row["last_op"] = str(op or "")
    row["last_attempt_ts"] = ts
    _write_json(path, m)
    return int(row["consecutive"])

def reset_flap(dst: str, feature: str, key: str, *, ts: int | None = None) -> None:
    ts = int(ts or time.time())
    path = _flap_path(dst, feature)
    m = _read_json(path)
    row = m.setdefault(key, {})
    row["consecutive"] = 0
    row["last_reason"] = "ok"
    row["last_op"] = str(row.get("last_op") or "")
    row["last_success_ts"] = ts
    _write_json(path, m)

def _promote(dst: str, feature: str, key: str, *, reason: str, ts: int, pair: str | None) -> None:
    path = _bb_path(dst, feature, pair)
    data = _read_json(path)
    if key not in data:
        data[key] = {"reason": str(reason or "flapper"), "since": int(ts)}
        _write_json(path, data)

def maybe_promote_to_blackbox(
    dst: str,
    feature: str,
    key: str,
    *,
    cfg: Mapping[str, Any],
    ts: int | None = None,
    pair: str | None = None,
    unresolved_map: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    ts = int(ts or time.time())
    bb = _load_bb_cfg(cfg)
    promote_after = int(bb.get("promote_after", 3) or 3)
    unresolved_days = int(bb.get("unresolved_days", 0) or 0)
    pair_scoped = bool(bb.get("pair_scoped", True))
    if not pair_scoped:
        pair = None

    counters = load_flap_counters(dst, feature)
    row = counters.get(key) or {}
    cons = int(row.get("consecutive") or 0)

    if cons >= promote_after:
        _promote(dst, feature, key, reason=f"flapper:consecutive>={promote_after}", ts=ts, pair=pair)
        return {"promoted": True, "reason": "consecutive", "since": ts}

    if unresolved_days > 0 and unresolved_map:
        meta = unresolved_map.get(key) or {}
        uts = int(meta.get("ts") or 0)
        if uts > 0:
            age_days = (ts - uts) / 86400.0
            if age_days >= unresolved_days:
                _promote(dst, feature, key, reason=f"unresolved_age>={unresolved_days}d", ts=ts, pair=pair)
                return {"promoted": True, "reason": "unresolved_age", "since": ts}

    return {"promoted": False, "reason": None, "since": None}

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
    unresolved_map: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    bb = _load_bb_cfg(cfg)
    ts = int(time.time())
    ordered_keys, unique_keys = _normalize_keys(keys)
    flap_path = _flap_path(dst, feature)
    flap_data = _read_json(flap_path)

    promote_after = int(bb.get("promote_after", 3) or 3)
    unresolved_days = int(bb.get("unresolved_days", 0) or 0)
    pair_scoped = bool(bb.get("pair_scoped", True))
    scoped_pair = pair if pair_scoped else None
    bb_path = _bb_path(dst, feature, scoped_pair)
    bb_data = _read_json(bb_path)

    flap_changed = False
    bb_changed = False
    promoted = 0

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
        elif unresolved_days > 0 and unresolved_map:
            meta = unresolved_map.get(key) or {}
            uts = int(meta.get("ts") or 0)
            if uts > 0:
                age_days = (ts - uts) / 86400.0
                if age_days >= unresolved_days:
                    should_promote = True
                    promote_reason = f"unresolved_age>={unresolved_days}d"

        if should_promote and key not in bb_data:
            bb_data[key] = {"reason": promote_reason or str(reason or "flapper"), "since": int(ts)}
            bb_changed = True
            promoted += 1

    if flap_changed:
        _write_json(flap_path, flap_data)
    if bb_changed:
        _write_json(bb_path, bb_data)

    return {"ok": True, "count": len(ordered_keys), "promoted": promoted, "pair": scoped_pair or "global"}


def record_success(
    dst: str,
    feature: str,
    keys: Iterable[str],
    *,
    pair: str | None = None,
    cfg: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ts = int(time.time())
    ordered_keys, unique_keys = _normalize_keys(keys)
    if not unique_keys:
        return {"ok": True, "count": 0}

    flap_path = _flap_path(dst, feature)
    flap_data = _read_json(flap_path)
    changed = False

    for key in unique_keys:
        row = flap_data.setdefault(key, {})
        row["consecutive"] = 0
        row["last_reason"] = "ok"
        row["last_op"] = str(row.get("last_op") or "")
        row["last_success_ts"] = ts
        changed = True

    if changed:
        _write_json(flap_path, flap_data)

    return {"ok": True, "count": len(ordered_keys)}

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
    return (scanned, removed)