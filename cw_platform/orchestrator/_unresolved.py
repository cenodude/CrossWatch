# cw_platform/orchestrator/_unresolved.py
# unresolved item management for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path
from collections.abc import Iterable, Mapping
from typing import Any
import json
import logging
import time

__all__ = [
    "load_unresolved_keys",
    "load_unresolved_map",
    "load_unresolved_items",
    "record_unresolved",
    "clear_unresolved",
]

STATE_DIR = Path("/config/.cw_state")
_LOG = logging.getLogger("crosswatch.orchestrator.unresolved")
_GENERIC_FAILURE_HINTS = {
    "apply:add:failed",
    "two:apply:add:failed",
}

from ._scope import scoped_file, scope_safe

try:
    from ..id_map import canonical_key as _ck, minimal as _minimal  # type: ignore[attr-defined]
except Exception:
    _ck = None  # type: ignore[assignment]
    _minimal = None  # type: ignore[assignment]


# Helpers
def _read_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _atomic_write(path: Path, data: Mapping[str, Any]) -> tuple[bool, str | None]:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        tmp.replace(path)
        return True, None
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        _LOG.error("unresolved_state_write_failed path=%s error=%s", path, error)
        return False, error


def _blocking_path(dst: str, feature: str) -> Path:
    dst_lower = str(dst).strip().lower()
    feat_lower = str(feature).strip().lower()
    return scoped_file(STATE_DIR, f"{dst_lower}_{feat_lower}.unresolved.json")


def _pending_path(dst: str, feature: str) -> Path:
    dst_lower = str(dst).strip().lower()
    feat_lower = str(feature).strip().lower()
    return scoped_file(STATE_DIR, f"{dst_lower}_{feat_lower}.unresolved.pending.json")


# Blocking
def load_unresolved_keys(
    dst: str,
    feature: str | None = None,
    *,
    cross_features: bool = True,
) -> set[str]:
    keys: set[str] = set()
    if not dst:
        return keys

    dst_lower = str(dst).strip().lower()
    scope = scope_safe()

    if feature and not cross_features:
        p = _blocking_path(dst_lower, feature)
        if p.exists():
            keys |= set(_read_json(p).keys())
        return keys

    if not STATE_DIR.exists():
        return keys

    prefix = f"{dst_lower}_"
    suffix = ".unresolved.json"
    pending_suffix = ".unresolved.pending.json"
    scoped1 = f".unresolved.{scope}.json"
    scoped2 = f".{scope}.unresolved.json"
    scopedp1 = f".unresolved.pending.{scope}.json"
    scopedp2 = f".{scope}.unresolved.pending.json"
    for p in STATE_DIR.iterdir():
        if p.is_file():
            name = p.name
            if not name.startswith(prefix):
                continue
            is_blocking = (name.endswith(scoped1) or name.endswith(scoped2) or name.endswith(suffix))
            is_pending = (name.endswith(scopedp1) or name.endswith(scopedp2) or name.endswith(pending_suffix))
            if not (is_blocking or is_pending):
                continue

            # Migrate legacy (unscoped) files to scoped when needed.
            rp = p
            if (name.endswith(suffix) or name.endswith(pending_suffix)) and not (
                name.endswith(scoped1) or name.endswith(scoped2) or name.endswith(scopedp1) or name.endswith(scopedp2)
            ):
                rp = scoped_file(STATE_DIR, name)

            data = _read_json(rp)

            if is_pending and isinstance(data, dict):
                lst = data.get("keys")
                if isinstance(lst, list):
                    keys |= {str(x) for x in lst if x}
                else:
                    im = data.get("items")
                    if isinstance(im, dict):
                        keys |= {str(k) for k in im.keys()}
            else:
                keys |= {str(k) for k in (data or {}).keys()}
    return keys


def load_unresolved_map(
    dst: str,
    feature: str | None = None,
    *,
    cross_features: bool = True,
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not dst:
        return out

    dst_lower = str(dst).strip().lower()
    scope = scope_safe()

    if feature and not cross_features:
        blocking = _read_json(_blocking_path(dst_lower, feature))
        for k, v in blocking.items():
            out[str(k)] = v if isinstance(v, dict) else {}

        pending = _read_json(_pending_path(dst_lower, feature))
        raw_hints = pending.get("hints") if isinstance(pending, dict) else None
        hints = raw_hints if isinstance(raw_hints, dict) else {}
        raw_keys = pending.get("keys") if isinstance(pending, dict) else None
        keys = raw_keys if isinstance(raw_keys, list) else []
        for k in keys:
            if not k:
                continue
            v = hints.get(k) or {}
            out[str(k)] = v if isinstance(v, dict) else {}
        return out

    if not STATE_DIR.exists():
        return out

    prefix = f"{dst_lower}_"
    suffix = ".unresolved.json"
    pending_suffix = ".unresolved.pending.json"
    scoped1 = f".unresolved.{scope}.json"
    scoped2 = f".{scope}.unresolved.json"
    scopedp1 = f".unresolved.pending.{scope}.json"
    scopedp2 = f".{scope}.unresolved.pending.json"
    for p in STATE_DIR.iterdir():
        if p.is_file():
            name = p.name
            if not name.startswith(prefix):
                continue
            is_blocking = (name.endswith(scoped1) or name.endswith(scoped2) or name.endswith(suffix))
            is_pending = (name.endswith(scopedp1) or name.endswith(scopedp2) or name.endswith(pending_suffix))
            if not (is_blocking or is_pending):
                continue

            rp = p
            if (name.endswith(suffix) or name.endswith(pending_suffix)) and not (
                name.endswith(scoped1) or name.endswith(scoped2) or name.endswith(scopedp1) or name.endswith(scopedp2)
            ):
                rp = scoped_file(STATE_DIR, name)

            data = _read_json(rp)
            if is_pending and isinstance(data, dict):
                raw_hints = data.get("hints")
                hints = raw_hints if isinstance(raw_hints, dict) else {}
                raw_keys = data.get("keys")
                keys = raw_keys if isinstance(raw_keys, list) else []
                for k in keys:
                    if not k:
                        continue
                    v = (hints or {}).get(k) or {}
                    out[str(k)] = v if isinstance(v, dict) else {}
            else:
                for k, v in (data or {}).items():
                    out[str(k)] = v if isinstance(v, dict) else {}
    return out


def load_unresolved_items(dst: str | None = None) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    if not STATE_DIR.exists():
        return []
    dst_lower = str(dst or "").strip().lower()
    for p in sorted(STATE_DIR.iterdir()):
        if not p.is_file():
            continue
        name = p.name
        if ".unresolved" not in name or not name.endswith(".json"):
            continue
        if dst_lower and not name.startswith(f"{dst_lower}_"):
            continue

        data = _read_json(p)
        if not isinstance(data, dict):
            continue
        raw_items = data.get("items")
        items: dict[str, Any] = raw_items if isinstance(raw_items, dict) else {}
        raw_hints = data.get("hints")
        hints: dict[str, Any] = raw_hints if isinstance(raw_hints, dict) else {}
        keys = data.get("keys")
        key_iter = keys if isinstance(keys, list) else list(items.keys())

        feature = ""
        try:
            base = name.split(".unresolved", 1)[0]
            if "_" in base:
                feature = base.split("_", 1)[1]
        except Exception:
            feature = ""

        for raw_ck in key_iter:
            ck = str(raw_ck or "").strip()
            if not ck or ck in out:
                continue
            hint = hints.get(ck) if isinstance(hints, dict) else None
            reason = str(hint.get("reason") or "").strip() if isinstance(hint, Mapping) else ""
            rec: dict[str, Any] = {"key": ck, "reason": reason, "feature": feature}
            item = items.get(ck) if isinstance(items, dict) else None
            if isinstance(item, Mapping):
                rec["item"] = dict(item)
            out[ck] = rec
    return list(out.values())


# Write helpers
def _to_ck_and_min(
    item: str | Mapping[str, Any],
) -> tuple[str, dict[str, Any] | None]:
    if isinstance(item, str):
        return item, None
    if not isinstance(item, Mapping):
        return "", None

    d: dict[str, Any] = dict(item)
    ck = ""

    if _ck:
        try:
            ck = _ck(d) or ""
        except Exception:
            ck = ""

    if not ck:
        ids = d.get("ids") or {}
        for k in ("tmdb", "imdb", "tvdb", "trakt", "ani", "mal", "anilist", "kitsu", "anidb"):
            v = ids.get(k)
            if v:
                ck = f"{k}:{str(v).lower()}"
                break
        if not ck:
            ck = str(d.get("id") or d.get("title") or "").strip().lower()

    min_item: dict[str, Any] | None
    if _minimal:
        try:
            min_item = _minimal(d)  # type: ignore[assignment]
        except Exception:
            min_item = d
    else:
        min_item = d

    return ck, min_item


def record_unresolved(
    dst: str,
    feature: str,
    items: Iterable[str | Mapping[str, Any]],
    *,
    hint: str = "provider_down",
) -> dict[str, Any]:
    path = _pending_path(dst, feature)
    now = int(time.time())

    data: dict[str, Any] = {"keys": [], "items": {}, "hints": {}}
    cur = _read_json(path)
    if cur:
        try:
            data["keys"] = list(set(cur.get("keys") or []))
            data["items"] = dict(cur.get("items") or {})
            data["hints"] = dict(cur.get("hints") or {})
        except Exception:
            pass

    existing: set[str] = set(data["keys"])
    added = 0

    for it in (items or []):
        item_hint = None
        if isinstance(it, Mapping):
            raw_hint = it.get("_cw_unresolved_hint") or it.get("hint") or it.get("reason")
            item_hint = str(raw_hint).strip() if raw_hint is not None else None
        ck, min_item = _to_ck_and_min(it)
        if not ck:
            continue
        if ck not in existing:
            data["keys"].append(ck)
            existing.add(ck)
            if min_item is not None:
                data["items"][ck] = min_item
            added += 1

        effective_hint = item_hint or hint
        if effective_hint:
            hints = data.setdefault("hints", {})
            existing_hint = hints.get(ck) if isinstance(hints, dict) else None
            existing_reason = ""
            if isinstance(existing_hint, Mapping):
                existing_reason = str(existing_hint.get("reason") or "").strip()
            new_reason = str(effective_hint).strip()
            if (
                existing_reason
                and existing_reason not in _GENERIC_FAILURE_HINTS
                and new_reason in _GENERIC_FAILURE_HINTS
            ):
                continue
            hints[ck] = {"reason": new_reason, "ts": now}

    ok, error = _atomic_write(path, data)
    return {"ok": ok, "count": added if ok else 0, "path": str(path), **({"error": error} if error else {})}


def clear_unresolved(
    dst: str,
    feature: str,
    keys: Iterable[str],
) -> dict[str, Any]:
    key_set = {str(k) for k in (keys or []) if k}
    if not dst or not key_set:
        return {"ok": True, "count": 0}

    removed = 0

    pend = _pending_path(dst, feature)
    pdata = _read_json(pend)
    if pdata:
        pchanged = False
        klist = pdata.get("keys")
        if isinstance(klist, list):
            kept = [k for k in klist if str(k) not in key_set]
            if len(kept) != len(klist):
                removed += len(klist) - len(kept)
                pdata["keys"] = kept
                pchanged = True
        for bucket in ("items", "hints"):
            sub = pdata.get(bucket)
            if isinstance(sub, dict):
                for k in [k for k in sub.keys() if str(k) in key_set]:
                    sub.pop(k, None)
                    pchanged = True
        if pchanged:
            _atomic_write(pend, pdata)

    blk = _blocking_path(dst, feature)
    bdata = _read_json(blk)
    if bdata:
        bchanged = False
        for k in [k for k in bdata.keys() if str(k) in key_set]:
            bdata.pop(k, None)
            removed += 1
            bchanged = True
        if bchanged:
            _atomic_write(blk, bdata)

    return {"ok": True, "count": removed}
