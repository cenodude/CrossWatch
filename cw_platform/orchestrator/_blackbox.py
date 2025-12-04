from __future__ import annotations
from pathlib import Path
from typing import Dict, Set, Optional, Mapping, Any, Tuple, Iterable
import json, time

STATE_DIR = Path("/config/.cw_state")

def _read_json(p: Path) -> dict:
    try:
        if not p.exists():
            return {}
        return json.loads(p.read_text("utf-8")) or {}
    except Exception:
        return {}

def _write_json(p: Path, obj: dict) -> None:
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        pass


# helpers

def _bb_path(dst: str, feature: str, pair: Optional[str] = None) -> Path:
    dst = str(dst).strip().lower()
    feature = str(feature).strip().lower()
    if pair:
        pair = str(pair).strip().lower()
        return STATE_DIR / f"{dst}_{feature}.{pair}.blackbox.json"
    return STATE_DIR / f"{dst}_{feature}.blackbox.json"

def _flap_path(dst: str, feature: str) -> Path:
    dst = str(dst).strip().lower()
    feature = str(feature).strip().lower()
    return STATE_DIR / f"{dst}_{feature}.flap.json"

_DEFAULT_BB = {
    "enabled": True,
    "promote_after": 3,
    "unresolved_days": 0,
    "pair_scoped": True,
    "cooldown_days": 30,
    "block_adds": True,
    "block_removes": True,
}

def _load_bb_cfg(cfg: Optional[Mapping[str, Any]]) -> Mapping[str, Any]:
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

# BB loader
def load_blackbox_keys(dst: str, feature: str, pair: Optional[str] = None) -> Set[str]:
    keys: Set[str] = set()
    glob = _read_json(_bb_path(dst, feature))
    keys |= set(glob.keys())
    if pair:
        prs = _read_json(_bb_path(dst, feature, pair))
        keys |= set(prs.keys())
    return keys

def load_flap_counters(dst: str, feature: str) -> Dict[str, dict]:
    return _read_json(_flap_path(dst, feature))

# Flap protection
def inc_flap(dst: str, feature: str, key: str, *, reason: str, op: str, ts: Optional[int] = None) -> int:
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

def reset_flap(dst: str, feature: str, key: str, *, ts: Optional[int] = None) -> None:
    ts = int(ts or time.time())
    path = _flap_path(dst, feature)
    m = _read_json(path)
    row = m.setdefault(key, {})
    row["consecutive"] = 0
    row["last_reason"] = "ok"
    row["last_op"] = str(row.get("last_op") or "")
    row["last_success_ts"] = ts
    _write_json(path, m)


# BB Promotion
def _promote(dst: str, feature: str, key: str, *, reason: str, ts: int, pair: Optional[str]) -> None:
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
    ts: Optional[int] = None,
    pair: Optional[str] = None,
    unresolved_map: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
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

    # 1: consecutive failures
    if cons >= promote_after:
        _promote(dst, feature, key, reason=f"flapper:consecutive>={promote_after}", ts=ts, pair=pair)
        return {"promoted": True, "reason": "consecutive", "since": ts}

    # 2: unresolved age
    if unresolved_days > 0 and unresolved_map:
        meta = unresolved_map.get(key) or {}
        uts = int(meta.get("ts") or 0)
        if uts > 0:
            age_days = (ts - uts) / 86400.0
            if age_days >= unresolved_days:
                _promote(dst, feature, key, reason=f"unresolved_age>={unresolved_days}d", ts=ts, pair=pair)
                return {"promoted": True, "reason": "unresolved_age", "since": ts}

    return {"promoted": False, "reason": None, "since": None}

# HL Wrappers
def record_attempts(
    dst: str,
    feature: str,
    keys: Iterable[str],
    *,
    reason: str = "apply:add:failed",
    op: str = "add",
    pair: Optional[str] = None,
    cfg: Optional[Mapping[str, Any]] = None,
    unresolved_map: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Dict[str, Any]:
    bb = _load_bb_cfg(cfg)
    ts = int(time.time())
    promoted = 0
    count = 0
    for k in (keys or []):
        try:
            k = str(k)
            count += 1
            inc_flap(dst, feature, k, reason=reason, op=op, ts=ts)
            res = maybe_promote_to_blackbox(
                dst, feature, k, cfg=bb, ts=ts, pair=pair, unresolved_map=unresolved_map,
            )
            if res.get("promoted"):
                promoted += 1
        except Exception:
            continue
    return {"ok": True, "count": count, "promoted": promoted, "pair": pair or "global"}

def record_success(
    dst: str,
    feature: str,
    keys: Iterable[str],
    *,
    pair: Optional[str] = None,   # kept for symmetry; no-op here
    cfg: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    ts = int(time.time())
    count = 0
    for k in (keys or []):
        try:
            k = str(k)
            count += 1
            reset_flap(dst, feature, k, ts=ts)
        except Exception:
            continue
    return {"ok": True, "count": count}

# BB Maintenance
def prune_blackbox(*, cooldown_days: int = 30) -> Tuple[int, int]:
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
