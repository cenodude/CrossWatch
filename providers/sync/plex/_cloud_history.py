# providers/sync/plex/_cloud_history.py
# Plex account history reads and server-history deduplication.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any, Mapping

from cw_platform.run_control import cancel_requested, SyncCancelled
from . import _common as common
from .._log import log as cw_log
from ._discover_history import Writer, CloudError, _HISTORY_QUERY, _LOCK

_CACHE_NAME = "plex_cloud_history.metadata.json"
_CACHE_VERSION = 1
_MATCH_TTL = 14 * 86400
_MISS_TTL = 86400
_RATE_WAIT_BUDGET = 300


def enabled(adapter: Any) -> bool:
    return common.plex_feature_cfg(adapter, "history").get("read_cloud_history") is True


def _cancel() -> None:
    if cancel_requested():
        raise SyncCancelled()


def _tokens(item: Mapping[str, Any]) -> set[tuple]:
    kind = item.get("type")
    ids = item.get("show_ids" if kind == "episode" else "ids") or {}
    return {(kind, provider, str(value), item.get("season") if kind == "episode" else None,
             item.get("episode") if kind == "episode" else None)
            for provider, value in ids.items() if provider in {"tmdb", "tvdb", "imdb"} and value}


def _fresh(entry: Any, now: float) -> bool:
    if not isinstance(entry, dict) or not isinstance(entry.get("at"), (int, float)):
        return False
    item = entry.get("item")
    if item is not None and not isinstance(item, dict):
        return False
    return now - float(entry["at"]) < (_MATCH_TTL if item else _MISS_TTL)


def _load_cache(now: float) -> dict[str, dict[str, Any]]:
    with _LOCK:
        data = common.global_state_read(_CACHE_NAME)
    items = data.get("items") if data.get("version") == _CACHE_VERSION else None
    return {str(k): v for k, v in items.items() if _fresh(v, now)} if isinstance(items, dict) else {}


def _save_cache(found: Mapping[str, dict[str, Any]], now: float) -> None:
    if not found:
        return
    with _LOCK:
        merged = _load_cache(now)
        merged.update(found)
        common.global_state_write(_CACHE_NAME, {"version": _CACHE_VERSION, "items": merged})


def _patient(writer: Any, waited: list[float]):
    last = [0.0]

    def call(fn: Any, *args: Any, **kwargs: Any) -> Any:
        while True:
            try:
                result = fn(*args, **kwargs)
                last[0] = 0.0
                return result
            except CloudError as exc:
                until = float(getattr(writer, "blocked_until", 0) or 0)
                delay = until - time.time()
                if str(exc) != "plex_discover_rate_limited" or delay <= 0:
                    raise
                if last[0]:
                    delay = max(delay, min(300.0, last[0] * 1.5))
                if waited[0] + delay > _RATE_WAIT_BUDGET:
                    raise
                last[0] = delay
                cw_log("PLEX", "history", "info", "cloud_history_rate_limited", wait_s=round(delay))
                end = time.time() + delay
                while time.time() < end:
                    _cancel()
                    time.sleep(min(1.0, max(0.0, end - time.time())))
                waited[0] += delay
                writer.blocked_until = 0.0
    return call


def read(adapter: Any) -> tuple[list[dict[str, Any]], int]:
    writer = Writer(adapter)
    now = time.time()
    cached = _load_cache(now)
    found: dict[str, dict[str, Any]] = {}
    call = _patient(writer, [0.0])
    try:
        _cancel()
        if not writer.token:
            raise CloudError("plex_discover_account_token_required")
        account = writer._request("GET", "https://plex.tv/api/v2/user")
        cli = getattr(adapter, "client", None)
        cfg_id = common.plex_cfg_get(adapter, "account_id", None)
        selected_id = getattr(cli, "selected_account_id", None) or cfg_id
        expected = (getattr(cli, "selected_username", None) or common.plex_cfg_get(adapter, "username", "")
                    or getattr(cli, "user_username", None))
        if not writer.token or account.get("restricted") or not account.get("uuid"):
            raise CloudError("plex_discover_account_unsupported")
        token_id = getattr(cli, "token_account_id", None) or getattr(cli, "user_account_id", None)
        if selected_id and (not token_id or str(selected_id) != str(token_id)):
            raise CloudError("plex_discover_account_unsupported")
        if expected and str(expected).casefold() != str(account.get("username") or "").casefold():
            raise CloudError("plex_discover_account_mismatch")
        writer.uuid = str(account["uuid"])
        metadata: dict[str, dict[str, Any] | None] = {}

        def fetch(key: str) -> dict[str, Any] | None:
            if key not in metadata:
                _cancel()
                try:
                    data = call(writer._metadata, key, params={"includeGuids": 1})
                except CloudError as exc:
                    if str(exc) != "plex_discover_endpoint_unavailable":
                        raise
                    metadata[key] = None
                    return None
                rows = data.get("Metadata")
                if rows == []:
                    metadata[key] = None
                    return None
                if not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict):
                    raise CloudError("plex_discover_invalid_response")
                row = rows[0]
                if str(row.get("ratingKey") or "") != key:
                    raise CloudError("plex_discover_invalid_response")
                metadata[key] = row
            return metadata[key]

        after = None
        cursors: set[str] = set()
        seen: set[str] = set()
        items = []
        unmatched = 0
        normalized: dict[str, dict[str, Any] | None] = {}
        while True:
            _cancel()
            data = call(writer._gql, _HISTORY_QUERY, {"id": writer.uuid, "after": after})
            history = (data.get("user") or {}).get("watchHistory")
            if not isinstance(history, dict) or not isinstance(history.get("nodes"), list):
                raise CloudError("plex_discover_history_unavailable")
            nodes = history["nodes"]
            seen_before = len(seen)
            for node in nodes:
                if not isinstance(node, dict):
                    raise CloudError("plex_discover_invalid_response")
                activity = str(node.get("id") or "")
                stamp = common.as_epoch(node.get("date"))
                ref = node.get("metadataItem") or {}
                key = str(ref.get("id") or "")
                if not activity or stamp is None:
                    raise CloudError("plex_discover_invalid_response")
                if activity in seen:
                    continue
                seen.add(activity)
                if not key:
                    unmatched += 1
                    continue
                if key not in normalized and key in cached:
                    normalized[key] = cached[key]["item"]
                if key not in normalized:
                    row = fetch(key)
                    item = None
                    if row and row.get("type") in {"movie", "episode"}:
                        item = common.normalize_discover_row(row, token="", hydrate_item_ids=False)
                        if item.get("type") == "episode":
                            show_key = str(row.get("grandparentRatingKey") or "")
                            show = fetch(show_key) if show_key else None
                            item["show_ids"] = common.ids_from_discover_row(show) if show else {}
                            if item.get("season") is None or item.get("episode") is None:
                                item = None
                        if item:
                            for field in ("ids", "show_ids"):
                                if field in item:
                                    item[field] = {k: v for k, v in item[field].items() if k in {"tmdb", "tvdb", "imdb"} and v}
                            if not _tokens(item):
                                item = None
                    normalized[key] = item
                    found[key] = {"at": now, "item": item}
                template = normalized[key]
                if not template:
                    unmatched += 1
                    continue
                items.append(dict(template, watched=True,
                                  watched_at=datetime.fromtimestamp(stamp, timezone.utc).isoformat(),
                                  _plex_cloud_history=True, _plex_cloud_activity_id=activity))
            info = history.get("pageInfo")
            if not isinstance(info, dict) or not isinstance(info.get("hasNextPage"), bool):
                raise CloudError("plex_discover_history_incomplete")
            if not info["hasNextPage"]:
                return items, unmatched
            cursor = info.get("endCursor")
            if len(seen) == seen_before or not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise CloudError("plex_discover_history_incomplete")
            cursors.add(cursor)
            after = cursor
    finally:
        writer.close()
        _save_cache(found, now)


def merge(server: Mapping[str, dict[str, Any]], cloud: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    from ._history import _event_key

    result = dict(server)
    observed: dict[tuple, list[int]] = {}
    for item in server.values():
        stamp = common.as_epoch(item.get("watched_at"))
        if stamp is not None:
            for token in _tokens(item):
                observed.setdefault(token, []).append(stamp)
    for item in cloud:
        stamp = common.as_epoch(item.get("watched_at"))
        if stamp is None:
            raise CloudError("plex_discover_invalid_response")
        if any(abs(stamp - other) <= 60 for token in _tokens(item) for other in observed.get(token, [])):
            continue
        key = _event_key(item)
        if key in result:
            continue
        result[key] = item
        for token in _tokens(item):
            observed.setdefault(token, []).append(stamp)
    return result
