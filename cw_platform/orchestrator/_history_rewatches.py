# cw_platform/orchestrator/_history_rewatches.py
# CrossWatch - rewatch-aware history planning helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations


from bisect import bisect_left, bisect_right
from collections.abc import Callable, Mapping
from typing import Any

from ..history_events import (
    has_history_watch,
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
        if not has_history_watch(value) or (event_mode and not (value.get("watched_at") or value.get("last_watched_at"))):
            continue
        item = minimal_history_item(value, key, event_mode=event_mode)
        sync_key = history_sync_key(item, key, event_mode=event_mode)
        if sync_key:
            out[sync_key] = item
    return out


def _history_type(item: Mapping[str, Any]) -> str:
    typ = str(item.get("type") or "").strip().lower()
    return "show" if typ == "tv" else typ


def _history_records(
    idx: Mapping[str, Any],
    typed_tokens: Callable[[Mapping[str, Any]], set[str]],
) -> dict[str, tuple[int, str, set[str], str]]:
    out = {}
    for key, item in idx.items():
        if not isinstance(item, Mapping):
            continue
        epoch = history_epoch_from_item(item)
        if epoch is None:
            epoch = history_epoch_from_key(key)
        if epoch is None:
            continue
        typ = _history_type(item)
        event_key = history_event_key(item, key)
        tokens = {f"{typ}|{token}" for token in typed_tokens(item)}
        out[key] = (epoch, event_key, tokens, typ)
    return out


class HistoryEventIndex:
    def __init__(
        self,
        dst_idx: Mapping[str, Any],
        typed_tokens: Callable[[Mapping[str, Any]], set[str]],
        *,
        tolerance_seconds: int = HISTORY_TIMESTAMP_TOLERANCE_SECONDS,
    ) -> None:
        self.typed_tokens = typed_tokens
        self.tolerance = max(0, int(tolerance_seconds))
        self.targets = _history_records(dst_idx, typed_tokens)
        self.sparse = {event_key for _, event_key, tokens, _ in self.targets.values()
                       if not tokens and is_history_event_key(event_key)}
        self.by_event: dict[str, list[str]] = {}
        self.postings: dict[str, list[tuple[int, str]]] = {}
        for key, (epoch, event_key, tokens, typ) in self.targets.items():
            self.by_event.setdefault(event_key, []).append(key)
            if event_key in self.sparse:
                tokens.add(f"{typ}|event:{event_key}")
            for token in tokens:
                self.postings.setdefault(token, []).append((epoch, key))
        for rows in self.postings.values():
            rows.sort()

    def match(self, src_idx: Mapping[str, Any]) -> dict[str, str]:
        tolerance = self.tolerance
        targets = self.targets
        sources = _history_records(src_idx, self.typed_tokens)
        src_sparse = {event_key for _, event_key, tokens, _ in sources.values()
                      if not tokens and is_history_event_key(event_key)}
        extra: dict[str, list[tuple[int, str]]] = {}
        for event_key in src_sparse - self.sparse:
            for key in self.by_event.get(event_key, ()):
                epoch, _, _, typ = targets[key]
                extra.setdefault(f"{typ}|event:{event_key}", []).append((epoch, key))
        for token, rows in extra.items():
            rows.extend(self.postings.get(token, ()))
            rows.sort()

        candidates: dict[str, dict[str, tuple[int, int]]] = {}
        reverse: dict[str, dict[str, tuple[int, int]]] = {}
        for key, (epoch, event_key, tokens, typ) in sources.items():
            if event_key in self.sparse or event_key in src_sparse:
                tokens.add(f"{typ}|event:{event_key}")
            peers = {}
            for token in tokens:
                rows = extra.get(token) or self.postings.get(token, [])
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


def history_event_matches(
    src_idx: Mapping[str, Any],
    dst_idx: Mapping[str, Any],
    typed_tokens: Callable[[Mapping[str, Any]], set[str]],
    *,
    tolerance_seconds: int = HISTORY_TIMESTAMP_TOLERANCE_SECONDS,
) -> dict[str, str]:
    return HistoryEventIndex(dst_idx, typed_tokens, tolerance_seconds=tolerance_seconds).match(src_idx)


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
