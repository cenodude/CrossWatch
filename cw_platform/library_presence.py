# cw_platform/library_presence.py
# CrossWatch - Destination library presence contract
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

PRESENT = "present"
ABSENT = "absent"
UNKNOWN = "unknown"

OPTION = "library_only"
REASON = "not_in_library"

_STATUSES = frozenset({PRESENT, ABSENT, UNKNOWN})
_EXTERNAL_ID_KEYS = ("tmdb", "imdb", "tvdb")
_EPISODE_TYPES = frozenset({"episode", "anime"})


def verdict(status: str, *, item_id: Any = None, reason: Any = "") -> dict[str, Any]:
    out: dict[str, Any] = {"status": status if status in _STATUSES else UNKNOWN}
    if item_id not in (None, ""):
        out["item_id"] = str(item_id)
    if reason:
        out["reason"] = str(reason)
    return out


def unknown(reason: Any = "") -> dict[str, Any]:
    return verdict(UNKNOWN, reason=reason)


def all_unknown(items: Sequence[Any], reason: Any = "") -> list[dict[str, Any]]:
    return [unknown(reason) for _ in items]


def external_ids(ids: Any, keys: Iterable[str] = _EXTERNAL_ID_KEYS) -> dict[str, str]:
    if not isinstance(ids, Mapping):
        return {}
    out: dict[str, str] = {}
    for key in keys:
        value = str(ids.get(key) or "").strip()
        if value and value != "0":
            out[key] = value
    return out


def is_episode(item: Mapping[str, Any]) -> bool:
    kind = str(item.get("type") or "").strip().lower()
    if kind == "episode":
        return True
    return kind in _EPISODE_TYPES and item.get("season") not in (None, "") and item.get("episode") not in (None, "")


def checkable(item: Any, *, native_keys: Iterable[str] = (), has_ids: Callable[[Any], bool] | None = None) -> bool:
    if not isinstance(item, Mapping):
        return False
    usable = has_ids or (lambda ids: bool(external_ids(ids)))
    raw_ids = item.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    for key in native_keys:
        if str(item.get(key) or ids.get(key) or "").strip():
            return False
    if is_episode(item):
        if item.get("season") in (None, "") or item.get("episode") in (None, ""):
            return False
        return bool(usable(item.get("show_ids")))
    return bool(usable(ids))


def requested(feature: Any, fcfg: Any) -> bool:
    return str(feature or "").strip().lower() == "history" and bool((fcfg or {}).get(OPTION))


def supported(ops: Any, feature: Any) -> bool:
    if not callable(getattr(ops, "library_presence", None)):
        return False
    try:
        caps = ops.capabilities() or {}
    except Exception:
        return False
    per = caps.get("library_presence") if isinstance(caps, Mapping) else None
    if not isinstance(per, Mapping):
        return False
    features = per.get("features") or ()
    return str(feature or "").strip().lower() in {str(f).strip().lower() for f in features}


def split_absent(
    ops: Any,
    cfg: Mapping[str, Any],
    feature: str,
    items: Sequence[Any],
    *,
    call: Any = None,
) -> tuple[list[Any], list[tuple[Any, str]], str]:
    rows = list(items or [])
    if not rows:
        return rows, [], ""
    try:
        fn = ops.library_presence
        raw = call(fn, cfg, rows, feature=feature) if call is not None else fn(cfg, rows, feature=feature)
    except Exception as exc:
        return rows, [], f"{type(exc).__name__}"
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)) or len(raw) != len(rows):
        return rows, [], "invalid_result"
    kept: list[Any] = []
    skipped: list[tuple[Any, str]] = []
    for item, row in zip(rows, raw):
        status = row.get("status") if isinstance(row, Mapping) else None
        if status == ABSENT:
            skipped.append((item, str(row.get("reason") or REASON)))
        else:
            kept.append(item)
    return kept, skipped, ""
