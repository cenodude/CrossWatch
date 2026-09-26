# providers/sync/wetrakr/_progress.py
# CrossWatch - WeTrakr resume progress reads and verified pause updates
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
import os
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.app_version import app_version
from cw_platform.run_control import raise_if_cancelled
from providers.sync._log import log
from providers.sync._mod_common import build_op_result
from providers.sync._progress_policy import as_epoch, decide_progress_write
from ._common import WeTrakrSyncError, body_of, identifier, int_value, item_key, matching, media_item, pages, request, write_lock


def number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError, OverflowError):
        return None


def ignored_reason(response: Any) -> str | None:
    if isinstance(response, Mapping) and response.get("ignored") is True:
        return str(response.get("reason") or "ignored_by_provider")
    return None


def build_index(adapter: Any, *, force: bool = False) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    completed = 0
    progress = getattr(adapter, "_read_progress", None)

    def on_page(count: int) -> None:
        raise_if_cancelled()
        if progress is not None:
            progress.tick(completed + count)
        log("WETRAKR", "progress", "info", "index_page", count=completed + count)

    for kind in ("movie", "episode"):
        rows = pages(adapter, f"/sync/tracking/playing/{kind}s", on_page=on_page)
        for row in rows:
            item = media_item(row, kind)
            playback = row.get("playback")
            if not isinstance(playback, Mapping):
                raise WeTrakrSyncError("missing_playback")
            percent = number(playback.get("progress_percent"))
            seconds = number(playback.get("runtime_seconds"))
            if percent is None or not 0 <= percent <= 100 or playback.get("status") not in ("playing", "paused"):
                raise WeTrakrSyncError("invalid_playback")
            if seconds is None or seconds <= 0:
                runtime = number(row.get("runtime"))
                seconds = runtime * 60 if runtime is not None else None
            if seconds is not None and seconds > 0:
                item.update(progress_ms=round(seconds * 1000 * percent / 100), duration_ms=round(seconds * 1000))
            item.update(progress_percent=percent, progress_at=playback.get("tracked_at"),
                        _wetrakr_playback_status=playback["status"])
            key = item_key(adapter, "progress", item)
            if key in out:
                raise WeTrakrSyncError("duplicate_media_identity")
            out[key] = item
        completed += len(rows)
    log("WETRAKR", "progress", "info", "index_done", count=len(out))
    return out


def scrobble_ids(value: Any) -> dict[str, Any]:
    raw = value if isinstance(value, Mapping) else {}
    selected = identifier({key: raw[key] for key in ("tmdb", "imdb", "tvdb") if key in raw})
    return selected["ids"]


def percent_of(item: Mapping[str, Any]) -> float | None:
    position, duration = number(item.get("progress_ms")), number(item.get("duration_ms"))
    if position is not None and duration is not None and duration > 0:
        return position * 100 / duration
    return number(item.get("progress_percent"))


def payload(item: Mapping[str, Any], *, clear: bool = False) -> dict[str, Any]:
    percent = 0 if clear else percent_of(item)
    if not clear and (percent is None or not 0 < percent < 100):
        raise WeTrakrSyncError("invalid_progress")
    body: dict[str, Any] = {"progress": percent, "app_version": app_version()}
    if item.get("type") == "movie":
        body["movie"] = {"ids": scrobble_ids(item.get("ids"))}
    elif item.get("type") == "episode":
        season, episode = int_value(item.get("season")), int_value(item.get("episode"))
        if season < 0 or episode < 1:
            raise WeTrakrSyncError("invalid_episode_coordinates")
        body["show"] = {"ids": scrobble_ids(item.get("show_ids"))}
        body["episode"] = {"season": season, "number": episode}
    else:
        raise WeTrakrSyncError("unsupported_media_type")
    return body


def same_origin() -> bool:
    return (str(os.getenv("CW_PAIR_SRC") or "").upper() == "WETRAKR"
            and str(os.getenv("CW_PAIR_DST") or "").upper() == "WETRAKR"
            and str(os.getenv("CW_PAIR_SRC_INSTANCE") or "default") == str(os.getenv("CW_PAIR_DST_INSTANCE") or "default"))


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    selected: dict[str, Mapping[str, Any]] = {}
    bodies: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    confirmed: list[str] = []
    for item in items:
        key = item_key(adapter, "progress", item)
        try:
            bodies[key] = payload(item)
            selected[key] = item
        except WeTrakrSyncError as exc:
            unresolved.append({"key": key, "reason": exc.reason})
    if dry_run or not selected:
        return build_op_result(ok=not unresolved, count=len(selected) if dry_run else 0, dry_run=dry_run,
                               unresolved=unresolved, unresolved_keys=[row["key"] for row in unresolved])
    with write_lock(adapter):
        try:
            current = adapter.build_index("progress", force_refresh=True)
            matches = matching(adapter, "progress", selected, current)
        except WeTrakrSyncError as exc:
            return build_op_result(ok=False, error=exc.reason, retry_after=exc.retry_after,
                                   unresolved=unresolved, unresolved_keys=list(selected) + [row["key"] for row in unresolved])
        pending: dict[str, Mapping[str, Any]] = {}
        for key, item in selected.items():
            target = current.get(matches.get(key, ""), {})
            decision = decide_progress_write(
                active_session=target.get("_wetrakr_playback_status") == "playing",
                source_timestamp=item.get("progress_at"), target_timestamp=target.get("progress_at"),
                source_progress_ms=bodies[key]["progress"] * 1000, source_duration_ms=100000,
                target_progress_ms=(percent_of(target) or 0) * 1000, target_duration_ms=100000,
                target_watched=False, same_origin=same_origin(), replay_enabled=True)
            if not decision.apply:
                results.append({"status": "skipped", "reason": decision.reason, "canonical_key": key})
            else:
                pending[key] = item
        attempted: dict[str, Mapping[str, Any]] = {}
        error: WeTrakrSyncError | None = None
        log("WETRAKR", "progress", "debug", "write_prepare", op="add", count=len(pending))
        for key, item in pending.items():
            raise_if_cancelled()
            if error:
                unresolved.append({"key": key, "reason": error.reason})
                continue
            attempted[key] = item
            try:
                response = body_of(request(adapter, "POST", "/scrobble/pause", json=bodies[key]))
                reason = ignored_reason(response)
                if reason is not None:
                    attempted.pop(key)
                    results.append({"status": "skipped", "reason": reason, "canonical_key": key})
            except WeTrakrSyncError as exc:
                error = exc
        if attempted:
            try:
                after = adapter.build_index("progress", force_refresh=True)
                matches = matching(adapter, "progress", attempted, after)
                for key, item in attempted.items():
                    target = after.get(matches.get(key, ""), {})
                    target_percent = percent_of(target)
                    if target_percent is not None and abs(bodies[key]["progress"] - target_percent) <= 0.1:
                        confirmed.append(key)
                    else:
                        unresolved.append({"key": key, "reason": error.reason if error else "write_not_verified"})
            except WeTrakrSyncError:
                unresolved.extend({"key": key, "reason": "verification_failed"} for key in attempted)
    log("WETRAKR", "progress", "info", "write_done", op="add", ok=not unresolved, applied=len(confirmed), unresolved=len(unresolved), skipped=len(results))
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed,
                           unresolved=unresolved, unresolved_keys=[row["key"] for row in unresolved], results=results, skipped=len(results))


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    selected: dict[str, Mapping[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    confirmed: list[str] = []
    results: list[dict[str, Any]] = []
    for item in items:
        key = item_key(adapter, "progress", item)
        try:
            payload(item, clear=True)
            selected[key] = item
        except WeTrakrSyncError as exc:
            unresolved.append({"key": key, "reason": exc.reason})
    if dry_run or not selected:
        return build_op_result(ok=not unresolved, count=len(selected) if dry_run else 0, dry_run=dry_run,
                               unresolved=unresolved, unresolved_keys=[row["key"] for row in unresolved])
    with write_lock(adapter):
        try:
            before = adapter.build_index("progress", force_refresh=True)
            matches = matching(adapter, "progress", selected, before)
        except WeTrakrSyncError as exc:
            return build_op_result(ok=False, error=exc.reason, retry_after=exc.retry_after,
                                   unresolved=unresolved, unresolved_keys=list(selected) + [row["key"] for row in unresolved])
        pending: dict[str, list[str]] = {}
        for key, item in selected.items():
            target_key = matches.get(key)
            if target_key is None:
                confirmed.append(key)
                continue
            target = before[target_key]
            source_stamp, target_stamp = as_epoch(item.get("progress_at")), as_epoch(target.get("progress_at"))
            reason = "active_session" if target.get("_wetrakr_playback_status") == "playing" else ""
            if not reason and same_origin():
                reason = "same_origin"
            if not reason and source_stamp is not None and target_stamp is not None and target_stamp > source_stamp:
                reason = "target_newer"
            if reason:
                results.append({"status": "skipped", "reason": reason, "canonical_key": key})
            else:
                pending.setdefault(target_key, []).append(key)
        attempted: dict[str, Mapping[str, Any]] = {}
        error: WeTrakrSyncError | None = None
        log("WETRAKR", "progress", "debug", "write_prepare", op="remove", count=len(pending))
        for target_key, keys in pending.items():
            raise_if_cancelled()
            if error:
                unresolved.extend({"key": key, "reason": error.reason} for key in keys)
                continue
            try:
                body = payload(before[target_key], clear=True)
            except WeTrakrSyncError as exc:
                unresolved.extend({"key": key, "reason": exc.reason} for key in keys)
                continue
            attempted.update({key: selected[key] for key in keys})
            try:
                response = body_of(request(adapter, "POST", "/scrobble/pause", json=body))
                reason = ignored_reason(response)
                if reason is not None:
                    for key in keys:
                        attempted.pop(key)
                        results.append({"status": "skipped", "reason": reason, "canonical_key": key})
            except WeTrakrSyncError as exc:
                error = exc
        if attempted:
            try:
                after = adapter.build_index("progress", force_refresh=True)
                remaining = matching(adapter, "progress", attempted, after)
                for key in attempted:
                    if key not in remaining:
                        confirmed.append(key)
                    else:
                        unresolved.append({"key": key, "reason": error.reason if error else "write_not_verified"})
            except WeTrakrSyncError:
                unresolved.extend({"key": key, "reason": "verification_failed"} for key in attempted)
    log("WETRAKR", "progress", "info", "write_done", op="remove", ok=not unresolved, applied=len(confirmed), unresolved=len(unresolved), skipped=len(results))
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed,
                           unresolved=unresolved, unresolved_keys=[row["key"] for row in unresolved], results=results, skipped=len(results))
