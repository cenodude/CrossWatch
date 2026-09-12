# cw_platform/orchestrator/_history_rewatches.py
# CrossWatch - rewatch-aware history planning helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Callable, Mapping
from typing import Any

from ..history_events import (
    base_key_from_history_event,
    history_epoch_from_item,
    history_epoch_from_key,
    history_event_key,
    history_sync_key,
    is_history_event_key,
    minimal_history_item,
)

HISTORY_TIMESTAMP_TOLERANCE_SECONDS = 60


def history_timestamp_tolerance_seconds(config: Mapping[str, Any]) -> int:
    """Read the shared runtime window; malformed values use the default."""
    runtime = config.get("runtime")
    value = HISTORY_TIMESTAMP_TOLERANCE_SECONDS
    if isinstance(runtime, Mapping):
        value = runtime.get("history_timestamp_tolerance_seconds", value)
    if isinstance(value, bool):
        return HISTORY_TIMESTAMP_TOLERANCE_SECONDS
    try:
        return max(0, int(str(value).strip()))
    except (TypeError, ValueError):
        return HISTORY_TIMESTAMP_TOLERANCE_SECONDS


def history_rewatches_requested(feature: str, fcfg: Mapping[str, Any]) -> bool:
    return str(feature or "").strip().lower() == "history" and bool((fcfg or {}).get("rewatches"))


def config_with_history_rewatches(config: Mapping[str, Any], enabled: bool) -> dict[str, Any]:
    out = dict(config or {})
    if enabled:
        out["_cw_history_rewatches"] = True
    else:
        out.pop("_cw_history_rewatches", None)
    return out


def history_rewatch_supported(provider: str, ops: Any, op: str) -> bool:
    try:
        caps = ops.capabilities() or {}
    except Exception:
        caps = {}
    hist = caps.get("history") if isinstance(caps, Mapping) else None
    if isinstance(hist, Mapping):
        rw = hist.get("rewatches")
        if isinstance(rw, Mapping):
            value = rw.get(op)
            if value is None and op == "read":
                value = rw.get("enabled")
            if value is not None:
                return bool(value)
        if isinstance(rw, bool):
            return rw
        event_history = hist.get("event_history")
        if event_history is not None:
            return bool(event_history)
    return str(provider or "").strip().upper() in {"TRAKT", "SIMKL", "PUBLICMETADB", "FLOPPY", "CROSSWATCH"}


def history_rewatch_pair_enabled(
    feature: str,
    fcfg: Mapping[str, Any],
    a: str,
    aops: Any,
    b: str,
    bops: Any,
    *,
    bidirectional: bool = False,
) -> bool:
    if not history_rewatches_requested(feature, fcfg):
        return False
    if bidirectional:
        return (
            history_rewatch_supported(a, aops, "read")
            and history_rewatch_supported(a, aops, "write")
            and history_rewatch_supported(b, bops, "read")
            and history_rewatch_supported(b, bops, "write")
        )
    return (
        history_rewatch_supported(a, aops, "read")
        and history_rewatch_supported(b, bops, "read")
        and history_rewatch_supported(b, bops, "write")
    )


def collapse_history_latest(idx: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    score: dict[str, int] = {}
    for key, value in (idx or {}).items():
        if not isinstance(value, Mapping):
            continue
        base = base_key_from_history_event(key)
        if not base:
            base = history_sync_key(value, key, event_mode=False)
        if not base:
            continue
        item = minimal_history_item(value, key, event_mode=False)
        item.pop("_cw_event_key", None)
        item.pop("_cw_rewatch_sync", None)
        ts = history_epoch_from_item(value) or history_epoch_from_key(key) or 0
        if base not in out or ts >= score.get(base, 0):
            out[base] = item
            score[base] = ts
    return out


def filter_history_events(idx: Mapping[str, Any], *, event_mode: bool) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for key, value in (idx or {}).items():
        if not isinstance(value, Mapping):
            continue
        if not (value.get("watched_at") or value.get("last_watched_at")):
            continue
        item = minimal_history_item(value, key, event_mode=event_mode)
        sync_key = history_sync_key(item, key, event_mode=event_mode)
        if sync_key:
            out[sync_key] = item
    return out


def history_event_matches(
    src_idx: Mapping[str, Any],
    dst_idx: Mapping[str, Any],
    typed_tokens: Callable[[Mapping[str, Any]], set[str]],
    *,
    tolerance_seconds: int = HISTORY_TIMESTAMP_TOLERANCE_SECONDS,
) -> dict[str, str]:
    """Pair viewings without changing timestamps or reusing a destination row.

    Prefer exact event identities, then the unique nearest timestamp on both
    sides. Equally close alternatives remain unmatched. Index by identity and
    time so unrelated library rows never require a pairwise comparison.
    """
    tolerance = max(0, int(tolerance_seconds))

    def records(idx: Mapping[str, Any]) -> dict[str, tuple[int, str, set[str]]]:
        out = {}
        for key, item in idx.items():
            if not isinstance(item, Mapping):
                continue
            epoch = history_epoch_from_item(item)
            if epoch is None:
                epoch = history_epoch_from_key(key)
            if epoch is None:
                continue
            typ = str(item.get("type") or "").strip().lower()
            typ = "show" if typ == "tv" else typ
            event_key = history_event_key(item, key)
            tokens = {f"{typ}|{token}" for token in typed_tokens(item)}
            out[key] = (epoch, event_key, tokens)
        return out

    sources, targets = records(src_idx), records(dst_idx)
    sparse_events = {event_key for _, event_key, tokens in (*sources.values(), *targets.values())
                     if not tokens and is_history_event_key(event_key)}
    for idx, rows in ((src_idx, sources), (dst_idx, targets)):
        for key, (_, event_key, tokens) in rows.items():
            if event_key in sparse_events:
                # Only sparse records fall back to exact event identity.
                # Explicit provider mappings remain authoritative.
                typ = str(idx[key].get("type") or "").strip().lower()
                typ = "show" if typ == "tv" else typ
                tokens.add(f"{typ}|event:{event_key}")
    postings: dict[str, list[tuple[int, str]]] = {}
    for key, (epoch, _, tokens) in targets.items():
        for token in tokens:
            postings.setdefault(token, []).append((epoch, key))
    for rows in postings.values():
        rows.sort()

    candidates: dict[str, dict[str, tuple[int, int]]] = {}
    reverse: dict[str, dict[str, tuple[int, int]]] = {}
    for key, (epoch, event_key, tokens) in sources.items():
        peers = {}
        for token in tokens:
            rows = postings.get(token, [])
            lo = bisect_left(rows, epoch - tolerance, key=lambda row: row[0])
            hi = bisect_right(rows, epoch + tolerance, key=lambda row: row[0])
            for other_epoch, other_key in rows[lo:hi]:
                score = (int(event_key != targets[other_key][1]), abs(epoch - other_epoch))
                peers[other_key] = score
        if peers:
            candidates[key] = peers
            for other_key, score in peers.items():
                reverse.setdefault(other_key, {})[key] = score

    def nearest(peers: Mapping[str, tuple[int, int]]) -> str | None:
        if not peers:
            return None
        score = min(peers.values())
        best = [key for key, value in peers.items() if value == score]
        return best[0] if len(best) == 1 else None

    matches: dict[str, str] = {}
    while candidates:
        source_choices = {key: nearest(peers) for key, peers in candidates.items()}
        target_choices = {key: nearest(peers) for key, peers in reverse.items()}
        pairs = [(key, peer) for key, peer in source_choices.items()
                 if peer is not None and target_choices.get(peer) == key]
        if not pairs:
            break
        for key, peer in pairs:
            matches[key] = peer
            for other_key in candidates.pop(key):
                reverse[other_key].pop(key, None)
            for source_key in reverse.pop(peer):
                candidates[source_key].pop(peer, None)
        candidates = {key: peers for key, peers in candidates.items() if peers}
        reverse = {key: peers for key, peers in reverse.items() if peers}
    return matches


def compare_history_events(
    src_idx: Mapping[str, Any],
    dst_idx: Mapping[str, Any],
    typed_tokens: Callable[[Mapping[str, Any]], set[str]],
    *,
    tolerance_seconds: int = HISTORY_TIMESTAMP_TOLERANCE_SECONDS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    """Return missing viewings and the matched original source/target keys."""
    adds: list[dict[str, Any]] = []
    removes: list[dict[str, Any]] = []
    matches = history_event_matches(src_idx, dst_idx, typed_tokens, tolerance_seconds=tolerance_seconds)
    matched_targets = set(matches.values())
    for key, value in (src_idx or {}).items():
        if isinstance(value, Mapping) and key not in matches:
            adds.append(minimal_history_item(value, key, event_mode=True))
    for key, value in (dst_idx or {}).items():
        if isinstance(value, Mapping) and key not in matched_targets:
            removes.append(minimal_history_item(value, key, event_mode=True))
    return adds, removes, matches


def history_event_diff(
    src_idx: Mapping[str, Any],
    dst_idx: Mapping[str, Any],
    typed_tokens: Callable[[Mapping[str, Any]], set[str]],
    *,
    tolerance_seconds: int = HISTORY_TIMESTAMP_TOLERANCE_SECONDS,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    adds, removes, _ = compare_history_events(
        src_idx, dst_idx, typed_tokens, tolerance_seconds=tolerance_seconds,
    )
    return adds, removes
