# providers/sync/plex/_cloud_history.py
# Plex account history reads and server-history deduplication.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from cw_platform.run_control import cancel_requested, SyncCancelled
from . import _common as common
from ._discover_history import Writer, CloudError, _HISTORY_QUERY


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


def read(adapter: Any) -> tuple[list[dict[str, Any]], int]:
    writer = Writer(adapter)
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
                    data = writer._metadata(key, params={"includeGuids": 1})
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
        for _ in range(10000):
            _cancel()
            data = writer._gql(_HISTORY_QUERY, {"id": writer.uuid, "after": after})
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
        raise CloudError("plex_discover_history_incomplete")
    finally:
        writer.close()


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
