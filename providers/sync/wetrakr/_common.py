# providers/sync/wetrakr/_common.py
# CrossWatch - WeTrakr shared HTTP, tracking snapshots, identifiers and writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from cw_platform.config_base import CONFIG_BASE
from cw_platform.history_events import history_epoch_from_item, history_sync_key
from cw_platform.id_map import canonical_key
from cw_platform.orchestrator._history_rewatches import history_event_matches, history_timestamp_tolerance_seconds
from cw_platform.run_control import raise_if_cancelled
from providers.auth import _auth_WETRAKR as auth
from providers.sync._log import log
from providers.sync._mod_common import SimpleRateLimiter, build_op_result

SCHEMA = 2
MAX_CACHE_AGE = 3600
WRITE_BATCH_SIZE = 500

_STATES: dict[str, dict[str, Any]] = {}
_STATES_LOCK = threading.Lock()
_WRITE_LOCKS: dict[str, Any] = {}


class WeTrakrSyncError(RuntimeError):
    def __init__(self, reason: str, *, status: int = 0, retry_after: float = 0):
        super().__init__(reason)
        self.reason = reason
        self.status_code = status
        self.retry_after = retry_after


def int_value(value: Any, default: int = -1) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(str(value))
    except (ValueError, TypeError):
        return default


def account_key(adapter: Any) -> str:
    block = auth.provider_block(adapter.config, adapter.instance_id)
    identity = str(block.get("user_id") or block.get("access_token") or adapter.instance_id)
    return hashlib.sha256(f"{auth.app_client_id()}|{identity}".encode()).hexdigest()


def write_lock(adapter: Any) -> Any:
    with _STATES_LOCK:
        return _WRITE_LOCKS.setdefault(account_key(adapter), threading.RLock())


def _state(adapter: Any) -> dict[str, Any]:
    with _STATES_LOCK:
        return _STATES.setdefault(account_key(adapter), {
            "limiter": SimpleRateLimiter(rates_per_sec={"GET": 3, "WRITE": 1}),
            "lock": threading.RLock(), "blocked": {}, "minute": {},
        })


def retry_seconds(value: Any) -> float:
    try:
        return max(0, min(86400, float(value)))
    except (TypeError, ValueError):
        try:
            return max(0, min(86400, parsedate_to_datetime(str(value)).timestamp() - time.time()))
        except (ValueError, TypeError, OverflowError):
            return 60


def request(adapter: Any, method: str, path: str, **kwargs: Any) -> Any:
    if not path.startswith("/") or path.startswith("//"):
        raise WeTrakrSyncError("invalid_api_path")
    state = _state(adapter)
    bucket = "GET" if method.upper() == "GET" else "WRITE"
    with state["lock"]:
        raise_if_cancelled()
        blocked = max(state["blocked"].get(bucket, 0), state["blocked"].get("DAILY", 0))
        if blocked > time.time():
            raise WeTrakrSyncError("rate_limited", status=429, retry_after=blocked - time.time())
        state["limiter"].wait(bucket)
        try:
            response = auth.request_with_auth(adapter.session, method, auth.API_BASE + path,
                                              cfg=adapter.config, instance_id=adapter.instance_id,
                                              timeout=20, max_retries=1, **kwargs)
        except (requests.RequestException, auth.WeTrakrAuthError) as exc:
            raise WeTrakrSyncError("request_failed") from exc
        headers = {str(k).lower(): str(v) for k, v in response.headers.items()}
        state["minute"][bucket] = {k: headers[k] for k in ("ratelimit-limit", "ratelimit-remaining", "ratelimit-reset") if k in headers}
        if response.status_code == 429:
            try:
                body = response.json()
            except ValueError:
                body = {}
            error = body.get("error") if isinstance(body, Mapping) else ""
            code = error.get("code") if isinstance(error, Mapping) else error
            daily = code == "QUOTA_EXCEEDED"
            delay = retry_seconds(headers.get("retry-after"))
            if daily:
                delay = max(delay, retry_seconds(headers.get("x-quota-reset")))
            delay = max(1, delay)
            state["blocked"]["DAILY" if daily else bucket] = time.time() + delay
            log("WETRAKR", "http", "warn", "rate_limited", daily=daily, retry_after=delay)
            raise WeTrakrSyncError("daily_quota_exceeded" if daily else "rate_limited", status=429, retry_after=delay)
        if not 200 <= response.status_code < 300:
            raise WeTrakrSyncError("request_rejected", status=response.status_code)
        if int_value(headers.get("ratelimit-remaining")) == 0 and "ratelimit-reset" in headers:
            state["blocked"][bucket] = time.time() + max(1, retry_seconds(headers["ratelimit-reset"]))
        return response


def body_of(response: Any) -> Any:
    try:
        return response.json()
    except (ValueError, TypeError) as exc:
        raise WeTrakrSyncError("invalid_json") from exc


def pages(adapter: Any, path: str, *, from_date: str | None = None,
          on_page: Callable[[int], None] | None = None) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    expected_total: int | None = None
    expected_pages: int | None = None
    for page in range(1, 10001):
        params: dict[str, Any] = {"page": page, "limit": 100}
        if from_date is not None:
            params["from_date"] = from_date
        response = request(adapter, "GET", path, params=params)
        rows = body_of(response)
        if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
            raise WeTrakrSyncError("invalid_page")
        headers = {str(k).lower(): v for k, v in response.headers.items()}
        total_pages = int_value(headers.get("x-pagination-page-count"))
        total = int_value(headers.get("x-pagination-item-count"))
        returned_page = int_value(headers.get("x-pagination-page"), page)
        if total_pages < 0 or returned_page != page or total_pages > 10000:
            raise WeTrakrSyncError("invalid_pagination")
        if expected_pages is not None and (total_pages != expected_pages or total != expected_total):
            raise WeTrakrSyncError("snapshot_changed")
        expected_pages, expected_total = total_pages, total
        if not rows and (page < total_pages or total > len(out)):
            raise WeTrakrSyncError("incomplete_snapshot")
        signature = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
        if rows and signature in seen:
            raise WeTrakrSyncError("repeated_page")
        seen.add(signature)
        out.extend(rows)
        if on_page is not None:
            on_page(len(out))
        if page >= total_pages:
            if total >= 0 and len(out) != total:
                raise WeTrakrSyncError("incomplete_snapshot")
            return out
    raise WeTrakrSyncError("pagination_limit")


def cache_path(adapter: Any, feature: str) -> Path:
    mode = bool(feature == "history" and adapter.config.get("_cw_history_rewatches"))
    identity = f"{account_key(adapter)}|{adapter.instance_id}|{feature}|{mode}"
    digest = hashlib.sha256(identity.encode()).hexdigest()
    return CONFIG_BASE() / "state" / "wetrakr" / f"{digest}.json"


def _read(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict) and data.get("schema") == SCHEMA and isinstance(data.get("sections"), dict):
            return data
    except (OSError, ValueError):
        pass
    return {}


def _save(path: Path, sections: Mapping[str, Any]) -> None:
    temporary: str | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, suffix=".tmp", delete=False) as handle:
            temporary = handle.name
            json.dump({"schema": SCHEMA, "sections": sections}, handle)
        os.replace(temporary, path)
        temporary = None
    except OSError:
        log("WETRAKR", "cache", "warn", "snapshot_cache_write_failed")
    finally:
        if temporary is not None:
            try:
                Path(temporary).unlink(missing_ok=True)
            except OSError:
                pass


def _activities(adapter: Any) -> dict[str, Any]:
    try:
        data = body_of(request(adapter, "GET", "/sync/last_activities"))
    except WeTrakrSyncError as exc:
        if exc.status_code in (401, 403, 429) or exc.retry_after:
            raise
        log("WETRAKR", "cache", "warn", "activities_unavailable_full_refresh")
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def _stamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return stamp if stamp.tzinfo is not None else None
    except ValueError:
        return None


def refresh_strategy(cached: Mapping[str, Any], current: Mapping[str, Any], feature: str, *, force: bool = False) -> str:
    rows = cached.get("rows")
    checked = cached.get("checked_at")
    fresh = isinstance(checked, (int, float)) and 0 <= time.time() - checked < MAX_CACHE_AGE
    valid = isinstance(rows, list) and all(isinstance(row, dict) for row in rows)
    previous = cached.get("activity")
    if force or not fresh or not valid or not isinstance(previous, Mapping):
        return "full"
    before, after = _stamp(previous.get("all")), _stamp(current.get("all"))
    if before is None or after is None or after < before:
        return "full"
    if current == previous:
        return "cached"
    if after == before:
        return "full"
    for key, value in previous.items():
        stamp = _stamp(value)
        next_stamp = _stamp(current.get(key))
        if stamp is not None and (next_stamp is None or next_stamp < stamp):
            return "full"
    removed = "last_removed_at" if feature == "ratings" else "last_tracking_removed_at"
    if _stamp(previous.get(removed)) is None or _stamp(current.get(removed)) != _stamp(previous[removed]):
        return "full"
    field = {"watchlist": "last_tracking_planning_at", "history": "last_tracking_watched_at", "ratings": "last_updated_at"}[feature]
    old_changed, changed = _stamp(previous.get(field)), _stamp(current.get(field))
    if old_changed is None or changed is None or not old_changed < changed <= after:
        return "full"
    return "delta"


def _merge_rows(previous: list[Mapping[str, Any]], changed: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    merged: dict[str, Mapping[str, Any]] = {}
    for batch in (previous, changed):
        seen: set[str] = set()
        for row in batch:
            key = str(row.get("id") or "").strip()
            if not key or key in seen:
                raise WeTrakrSyncError("invalid_delta_identity")
            seen.add(key)
            merged[key] = row
    return list(merged.values())


def tracking_rows(adapter: Any, feature: str, *, force: bool = False) -> dict[str, list[Mapping[str, Any]]]:
    kinds = ("movie", "show", "season", "episode") if feature == "ratings" else ("movie", "show") if feature == "watchlist" else ("movie", "episode")
    status = "planning" if feature == "watchlist" else "watched"
    event_path = "history/" if feature == "history" and adapter.config.get("_cw_history_rewatches") else ""
    with write_lock(adapter):
        path = cache_path(adapter, feature)
        cached = _read(path).get("sections", {})
        before = _activities(adapter)
        sections: dict[str, Any] = {}
        result: dict[str, list[Mapping[str, Any]]] = {}
        refreshed = False
        completed = 0
        progress = getattr(adapter, "_read_progress", None)

        def page_progress(count: int) -> None:
            raise_if_cancelled()
            if progress is not None:
                progress.tick(completed + count)
            log("WETRAKR", feature, "info", "index_page", count=completed + count)

        for kind in kinds:
            raise_if_cancelled()
            section = f"{kind}s"
            old = cached.get(section)
            old = old if isinstance(old, Mapping) else {}
            block = before.get("ratings" if feature == "ratings" else section)
            activity = dict(block) if isinstance(block, Mapping) else {}
            strategy = refresh_strategy(old, activity, feature, force=force)
            endpoint = f"/sync/ratings/{section}" if feature == "ratings" else f"/sync/tracking/{status}/{event_path}{section}"
            if strategy == "cached":
                rows = old["rows"]
                checked = old["checked_at"]
            else:
                checkpoint = _stamp(old.get("activity", {}).get("all")) if strategy == "delta" else None
                if checkpoint is not None:
                    since = (checkpoint - timedelta(seconds=2)).isoformat().replace("+00:00", "Z")
                    changed = pages(adapter, endpoint, from_date=since, on_page=page_progress)
                    if changed:
                        rows = _merge_rows(old["rows"], changed)
                        checked = old["checked_at"]
                    else:
                        strategy = "full"
                        rows = pages(adapter, endpoint, on_page=page_progress)
                        checked = time.time()
                else:
                    rows = pages(adapter, endpoint, on_page=page_progress)
                    checked = time.time()
                refreshed = True
            sections[section] = {"activity": activity, "rows": rows, "checked_at": checked}
            result[kind] = rows
            completed += len(rows)
            if progress is not None:
                progress.tick(completed, force=True)
            log("WETRAKR", feature, "debug", "snapshot_section", section=section, strategy=strategy, count=len(rows))
        if refreshed:
            after = _activities(adapter)
            for kind in kinds:
                section = f"{kind}s"
                activity_section = "ratings" if feature == "ratings" else section
                old_activity, new_activity = before.get(activity_section), after.get(activity_section)
                old_activity = dict(old_activity) if isinstance(old_activity, Mapping) else {}
                new_activity = dict(new_activity) if isinstance(new_activity, Mapping) else {}
                has_stamp = _stamp(old_activity.get("all")) is not None or _stamp(new_activity.get("all")) is not None
                if has_stamp and old_activity != new_activity:
                    raise WeTrakrSyncError("snapshot_changed")
                if _stamp(new_activity.get("all")) is None:
                    sections[section]["activity"] = {}
            adapter._pending_snapshot = (path, sections)
        return result


def commit_snapshot(adapter: Any) -> None:
    pending = getattr(adapter, "_pending_snapshot", None)
    adapter._pending_snapshot = None
    if pending:
        _save(*pending)


def media_ids(row: Mapping[str, Any]) -> dict[str, str]:
    raw = row.get("ids")
    raw = raw if isinstance(raw, Mapping) else {}
    out: dict[str, str] = {}
    for source in ("tmdb", "imdb", "tvdb"):
        value = raw.get(source)
        if isinstance(value, Mapping):
            value = value.get("id")
        if value is not None and str(value).strip():
            out[source] = str(value).strip()
    if int_value(row.get("id")) > 0:
        out["wetrakr"] = str(row["id"])
    return out


def media_item(row: Mapping[str, Any], kind: str) -> dict[str, Any]:
    if row.get("type") != kind:
        raise WeTrakrSyncError("invalid_media_type")
    item: dict[str, Any] = {"type": kind, "ids": media_ids(row), "title": row.get("title")}
    date = str(row.get("release_date") or row.get("first_air_date") or "")
    if int_value(date[:4]) > 0:
        item["year"] = int(date[:4])
    if kind in ("season", "episode"):
        show = row.get("show")
        if not isinstance(show, Mapping):
            raise WeTrakrSyncError("missing_episode_show")
        item.update(show_ids=media_ids(show), series_title=show.get("title"),
                    season=int_value(row.get("number") if kind == "season" else row.get("season_number")))
        if kind == "episode":
            item["episode"] = int_value(row.get("number"))
        if not item["show_ids"] or item["season"] < 0 or (kind == "episode" and item["episode"] < 1):
            raise WeTrakrSyncError("invalid_episode_coordinates")
    if not item["ids"] or canonical_key(item) == "unknown:":
        raise WeTrakrSyncError("missing_media_identity")
    return item


def item_key(adapter: Any, feature: str, item: Mapping[str, Any]) -> str:
    if feature == "history":
        return history_sync_key(item, item.get("_cw_event_key"), event_mode=bool(adapter.config.get("_cw_history_rewatches")))
    return canonical_key(item)


def identity_tokens(item: Mapping[str, Any]) -> set[str]:
    kind = str(item.get("type") or "")
    ids = item.get("show_ids") if kind in ("season", "episode") else item.get("ids")
    if not isinstance(ids, Mapping):
        return set()
    suffix = f":s{item.get('season')}e{item.get('episode')}" if kind == "episode" else f":s{item.get('season')}" if kind == "season" else ""
    return {f"{kind}:{source}:{value}{suffix}" for source, value in ids.items() if source in ("tmdb", "imdb", "tvdb", "wetrakr") and value}


def matching(adapter: Any, feature: str, source: Mapping[str, Any], dest: Mapping[str, Any]) -> dict[str, str]:
    if feature == "history" and adapter.config.get("_cw_history_rewatches"):
        return history_event_matches(source, dest, identity_tokens, tolerance_seconds=history_timestamp_tolerance_seconds(adapter.config))
    lookup: dict[str, set[str]] = {}
    for key, item in dest.items():
        for token in identity_tokens(item):
            lookup.setdefault(token, set()).add(key)
    matches = {}
    for key, item in source.items():
        peers = set().union(*(lookup.get(token, set()) for token in identity_tokens(item)))
        if len(peers) > 1:
            raise WeTrakrSyncError("ambiguous_media_identity")
        if peers:
            matches[key] = next(iter(peers))
    return matches


def identifier(ids: Any) -> dict[str, Any]:
    if not isinstance(ids, Mapping):
        raise WeTrakrSyncError("missing_supported_id")
    for source in ("wetrakr", "tmdb", "imdb", "tvdb"):
        value = ids.get(source)
        if source == "imdb" and isinstance(value, str) and value.startswith("tt") and value[2:].isdigit():
            return {"ids": {source: value}}
        if source != "imdb" and int_value(value) > 0:
            return {"id": int_value(value)} if source == "wetrakr" else {"ids": {source: int_value(value)}}
    raise WeTrakrSyncError("missing_supported_id")


def payload_item(item: Mapping[str, Any], feature: str, *, include_date: bool = True) -> tuple[str, dict[str, Any]]:
    kind = item.get("type")
    if feature == "ratings":
        if kind not in ("movie", "show", "season", "episode"):
            raise WeTrakrSyncError("unsupported_media_type")
        if kind in ("season", "episode"):
            if int_value(item.get("season")) < 0 or (kind == "episode" and int_value(item.get("episode")) < 1):
                raise WeTrakrSyncError("invalid_episode_coordinates")
            identifier(item.get("show_ids"))
        row = identifier(item.get("ids")) if kind not in ("season", "episode") or item.get("ids") else {}
        if include_date:
            rating = int_value(item.get("rating"))
            if not 1 <= rating <= 10:
                raise WeTrakrSyncError("invalid_rating")
            row["rating"] = rating
        return f"{kind}s", row
    allowed = ("movie", "show") if feature == "watchlist" else ("movie", "episode")
    if kind not in allowed:
        raise WeTrakrSyncError("unsupported_media_type")
    fields: dict[str, Any] = {"status": "planning" if feature == "watchlist" else "watched"}
    if feature == "history" and include_date:
        stamp = history_epoch_from_item(item)
        if stamp is None or item.get("_wetrakr_watched_at_unknown"):
            fields["tracked_at_unknown"] = True
        else:
            fields["tracked_at"] = datetime.fromtimestamp(stamp, timezone.utc).isoformat().replace("+00:00", "Z")
    if kind == "episode":
        season, episode = int_value(item.get("season")), int_value(item.get("episode"))
        if season < 0 or episode < 1:
            raise WeTrakrSyncError("invalid_episode_coordinates")
        show = identifier(item.get("show_ids"))
        return "shows", {**show, "seasons": [{"number": season, "episodes": [{"number": episode, **fields}]}]}
    return f"{kind}s", {**identifier(item.get("ids")), **fields}


def resolve_child(adapter: Any, item: Mapping[str, Any]) -> dict[str, Any]:
    ids = item.get("ids")
    if isinstance(ids, Mapping) and int_value(ids.get("wetrakr")) > 0:
        return dict(item)
    season, episode = int_value(item.get("season")), int_value(item.get("episode"))
    kind = item.get("type")
    if season < 0 or (kind == "episode" and episode < 1):
        raise WeTrakrSyncError("invalid_episode_coordinates")
    show = identifier(item.get("show_ids"))
    cache = getattr(adapter, "_resolved_shows", {})
    cache_key = json.dumps(show, sort_keys=True)
    show_id = show.get("id") or cache.get(cache_key)
    if not show_id:
        source, value = next(iter(show["ids"].items()))
        data = body_of(request(adapter, "GET", f"/media/external/{source}/{quote(str(value), safe='')}", params={"type": "show"}))
        if not isinstance(data, Mapping) or data.get("type") != "show" or int_value(data.get("id")) <= 0:
            raise WeTrakrSyncError("show_not_resolved")
        show_id = data["id"]
        cache[cache_key] = show_id
        adapter._resolved_shows = cache
    path = f"/shows/{show_id}/seasons/{season}"
    if kind == "episode":
        path += f"/episodes/{episode}"
    row = body_of(request(adapter, "GET", path))
    if (not isinstance(row, Mapping) or row.get("type") != kind or int_value(row.get("id")) <= 0
            or int_value(row.get("season_number") if kind == "episode" else row.get("number")) != season
            or (kind == "episode" and int_value(row.get("number")) != episode)
            or int_value(row.get("media_id")) != int_value(show_id)):
        raise WeTrakrSyncError("child_not_resolved")
    return {**item, "ids": {**(ids if isinstance(ids, Mapping) else {}), "wetrakr": str(row["id"])}}


def write_matches(feature: str, item: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    return feature != "ratings" or int_value(item.get("rating")) == int_value(target.get("rating"))


def write_items(adapter: Any, feature: str, items: Iterable[Mapping[str, Any]], *, remove: bool, dry_run: bool) -> dict[str, Any]:
    selected: dict[str, Mapping[str, Any]] = {}
    unresolved: list[dict[str, str]] = []
    confirmed: list[str] = []
    event_mode = feature == "history" and bool(adapter.config.get("_cw_history_rewatches"))
    for item in items:
        key = item_key(adapter, feature, item)
        try:
            payload_item(item, feature, include_date=not remove)
            if event_mode and (history_epoch_from_item(item) is None or item.get("_wetrakr_watched_at_unknown")):
                raise WeTrakrSyncError("missing_watch_timestamp")
            selected[key] = item
        except (WeTrakrSyncError, ValueError, OverflowError, OSError) as exc:
            reason = exc.reason if isinstance(exc, WeTrakrSyncError) else "invalid_watch_timestamp"
            unresolved.append({"key": key, "reason": reason})
            log("WETRAKR", feature, "warn", "item_unresolved_before_write", key=key, reason=reason)
    if dry_run:
        return build_op_result(ok=not unresolved, count=len(selected), unresolved=unresolved,
                               unresolved_keys=[r["key"] for r in unresolved], dry_run=True)
    if not selected:
        return build_op_result(ok=not unresolved, unresolved=unresolved, unresolved_keys=[r["key"] for r in unresolved])
    with write_lock(adapter):
        try:
            before = adapter.build_index(feature, force_refresh=True)
            matches = matching(adapter, feature, selected, before)
        except WeTrakrSyncError as exc:
            return build_op_result(ok=False, unresolved_keys=list(selected) + [r["key"] for r in unresolved], unresolved=unresolved,
                                   error=exc.reason, retry_after=exc.retry_after)
        pending: dict[str, Mapping[str, Any]] = {}
        for key, item in selected.items():
            if event_mode and key not in matches:
                stamp = history_epoch_from_item(item)
                tolerance = history_timestamp_tolerance_seconds(adapter.config)
                peers = [row for row in before.values() if identity_tokens(row) & identity_tokens(item)
                         and stamp is not None and (other := history_epoch_from_item(row)) is not None and abs(stamp - other) <= tolerance]
                if peers:
                    unresolved.append({"key": key, "reason": "ambiguous_watch_event"})
                    continue
            if (remove and key not in matches) or (not remove and key in matches and write_matches(feature, item, before[matches[key]])):
                confirmed.append(key)
            else:
                try:
                    pending[key] = resolve_child(adapter, item) if feature == "ratings" and item.get("type") in ("season", "episode") and not remove else item
                except WeTrakrSyncError as exc:
                    unresolved.append({"key": key, "reason": exc.reason})
        log("WETRAKR", feature, "info", "write_prepare", action="remove" if remove else "add", count=len(pending))
        keys = list(pending)
        for start in range(0, len(keys), WRITE_BATCH_SIZE):
            batch = keys[start:start + WRITE_BATCH_SIZE]
            error: WeTrakrSyncError | None = None
            try:
                if event_mode and remove:
                    for key in batch:
                        event_id = str(before[matches[key]].get("_wetrakr_history_id") or "")
                        if not event_id:
                            raise WeTrakrSyncError("missing_history_id")
                        body_of(request(adapter, "DELETE", f"/sync/tracklogs/{quote(event_id, safe='')}"))
                else:
                    payload: dict[str, list[dict[str, Any]]] = {}
                    for key in batch:
                        item = before[matches[key]] if remove else pending[key]
                        group, row = payload_item(item, feature, include_date=not remove)
                        payload.setdefault(group, []).append(row)
                    path = "/sync/ratings" if feature == "ratings" else "/sync/tracking"
                    if remove:
                        path += "/remove/all" if feature == "history" else "/remove"
                    body_of(request(adapter, "POST", path, json=payload))
            except WeTrakrSyncError as exc:
                error = exc
            try:
                after = adapter.build_index(feature, force_refresh=True)
                verified = matching(adapter, feature, {k: pending[k] for k in batch}, after)
                for key in batch:
                    removed_event = event_mode and remove and not any(
                        row.get("_wetrakr_history_id") == before[matches[key]].get("_wetrakr_history_id") for row in after.values())
                    matches_value = key in verified and write_matches(feature, pending[key], after[verified[key]])
                    if removed_event or (not (event_mode and remove) and (key not in verified if remove else matches_value)):
                        confirmed.append(key)
                    else:
                        unresolved.append({"key": key, "reason": error.reason if error else "write_not_verified"})
            except WeTrakrSyncError as exc:
                error = error or exc
                unresolved.extend({"key": key, "reason": "verification_failed"} for key in batch)
            if error:
                unresolved.extend({"key": key, "reason": error.reason} for key in keys[start + WRITE_BATCH_SIZE:])
                break
        log("WETRAKR", feature, "info", "write_done", confirmed=len(confirmed), unresolved=len(unresolved))
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed,
                           unresolved=unresolved, unresolved_keys=[r["key"] for r in unresolved])
