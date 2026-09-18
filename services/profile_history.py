# /services/profile_history.py
# CrossWatch - Profile history timeline
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import threading
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Callable, Mapping

from services import dashboard_widgets as dw

SOURCES = ("synced", "scrobble", "ratings", "watchlist")
PAGE_SIZE_MAX = 96
_TYPES = {
    "synced": ("movie", "episode"),
    "scrobble": ("movie", "episode"),
    "ratings": ("movie", "show", "season", "episode"),
    "watchlist": ("movie", "show", "anime"),
}
_KNOWN_TYPES = ("movie", "show", "season", "episode", "anime")
_TYPE_ALIASES = {"tv": "show", "series": "show", "shows": "show"}
_UNDATED_SOURCES = {"ratings", "watchlist"}
_COVERAGE_SOURCES = {"synced", "ratings", "watchlist"}
_SCROBBLE_KINDS = {"scrobble", "history_sync"}
_ROW_FIELDS = (
    "id",
    "key",
    "aliases",
    "type",
    "art_type",
    "title",
    "year",
    "season",
    "episode",
    "episode_label",
    "rating",
    "ids",
    "tmdb",
    "poster",
    "cover",
    "sources",
    "source",
    "targets",
    "method",
    "event",
    "watch_count",
    "watch_epochs",
)
_INDEX_CACHE: "OrderedDict[tuple[Any, ...], dict[str, Any]]" = OrderedDict()
_INDEX_CACHE_MAX = 12
_CACHE_LOCK = threading.Lock()
_BUILD_LOCK = threading.Lock()


def normalize_source(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"scrobble", "scrobbles"}:
        return "scrobble"
    if raw in {"ratings", "rating"}:
        return "ratings"
    if raw == "watchlist":
        return "watchlist"
    return "synced"


def clear_history_cache() -> None:
    with _CACHE_LOCK:
        _INDEX_CACHE.clear()


def month_key(epoch: int) -> str:
    try:
        return datetime.fromtimestamp(int(epoch), tz=timezone.utc).strftime("%Y-%m")
    except (OverflowError, OSError, ValueError):
        return ""


def _resolve(value: Any) -> Any:
    return value() if callable(value) else value


def _row_endpoints(row: Mapping[str, Any]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for ref in row.get("sources") or []:
        if not isinstance(ref, Mapping):
            continue
        provider = str(ref.get("provider") or "").strip().upper()
        if not provider:
            continue
        endpoint = (provider, str(ref.get("instance") or "default").strip() or "default")
        if endpoint not in out:
            out.append(endpoint)
    return out


def _ref(endpoint: tuple[str, str]) -> dict[str, str]:
    return {"provider": endpoint[0], "instance": endpoint[1]}


def _row_type(row: Mapping[str, Any]) -> str:
    typ = str(row.get("type") or "").strip().lower()
    typ = _TYPE_ALIASES.get(typ, typ)
    return typ if typ in _KNOWN_TYPES else "other"


def _watchlist_rows(items: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, Mapping):
            continue
        raw_sources = item.get("sources_by_provider")
        by_provider: Mapping[str, Any] = raw_sources if isinstance(raw_sources, Mapping) else {}
        sources = [
            {"provider": str(provider).upper(), "instance": str(instance or "default")}
            for provider, instances in by_provider.items()
            for instance in (instances if isinstance(instances, list) and instances else ["default"])
        ] or [{"provider": str(provider).upper(), "instance": "default"} for provider in item.get("sources") or []]
        rows.append({**item, "sources": sources, "sort_epoch": dw._as_int(item.get("added_epoch")) or 0})
    return rows


def _synced_rows(state: Any, tracker_items: Any, user_filter: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    alias_map = dw._history_alias_representatives()
    state_rows = dw._latest_history_state_rows(state or {}, user_filter=user_filter)
    tracker_rows = dw._latest_history_tracker_rows(tracker_items or {}, user_filter=user_filter)
    return dw._merge_history_rows(state_rows, tracker_rows, alias_map=alias_map)


def _scrobble_rows(user_filter: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    payload = dw.list_events(limit=dw.ACTIVITY_DEFAULT_LIMIT, offset=0, status="ok", kind="all", group_routes=True)
    rows: list[dict[str, Any]] = []
    for item in payload.get("items") or []:
        if not isinstance(item, Mapping) or str(item.get("kind") or "").strip().lower() not in _SCROBBLE_KINDS:
            continue
        row = dw._activity_row(item)
        if dw._sources_match_user(row.get("sources"), user_filter):
            rows.append(row)
    return rows


def _rating_rows(state: Any, tracker_items: Any, user_filter: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    aliases: dict[str, str] = {}

    def put(row: dict[str, Any]) -> None:
        match_key = str(row["key"])
        for alias in dw._rating_aliases(row):
            if alias in aliases:
                match_key = aliases[alias]
                break
        prev = rows.get(match_key)
        if prev is None:
            rows[match_key] = row
        else:
            values = dict(prev["_values"])
            for endpoint, value in row["_values"].items():
                current = values.get(endpoint)
                if current is None or value[1] >= current[1]:
                    values[endpoint] = value
            merged = dw._merge_rating_row(prev, row)
            merged["_values"] = values
            rows[match_key] = merged
        for alias in dw._rating_aliases(rows[match_key]):
            aliases[alias] = match_key

    def add(raw_key: Any, item: dict[str, Any], sources: list[dict[str, str]], from_tracker: bool) -> None:
        if dw._rating_value(item) is None:
            return
        row = dw._rating_row(str(raw_key), item, sources)
        if not row:
            return
        if from_tracker:
            row[dw._RATING_TRACKER_FLAG] = True
        epoch = int(row.get("sort_epoch") or 0)
        row["_values"] = {endpoint: (row["rating"], epoch) for endpoint in _row_endpoints(row)}
        put(row)

    for raw_key, raw_item in (tracker_items or {}).items():
        item = dw._unwrap_rating_item(raw_item)
        sources = dw._sources_from_item(item)
        if dw._sources_match_user(sources, user_filter):
            add(raw_key, item, sources, True)

    for provider, instance, block in dw._history_provider_blocks(state or {}, user_filter):
        ref = dw._provider_ref(provider, instance)
        for raw_key, raw_item in dw._feature_items(block, "ratings").items():
            add(raw_key, dw._unwrap_rating_item(raw_item), [ref], False)

    return list(rows.values())


def build_history_index(
    source: Any,
    *,
    state: Any = None,
    tracker_items: Any = None,
    user_filter: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    wanted = normalize_source(source)
    if wanted == "scrobble":
        raw_rows = _scrobble_rows(user_filter)
    elif wanted == "ratings":
        raw_rows = _rating_rows(_resolve(state), _resolve(tracker_items), user_filter)
    elif wanted == "watchlist":
        raw_rows = _watchlist_rows(_resolve(state))
    else:
        raw_rows = _synced_rows(_resolve(state), _resolve(tracker_items), user_filter)
    rows: list[dict[str, Any]] = []
    endpoints: set[tuple[str, str]] = set()
    minutes = 0.0
    for raw in raw_rows:
        epoch = max(0, int(raw.get("sort_epoch") or raw.get("watched_at") or 0))
        if epoch <= 0 and wanted not in _UNDATED_SOURCES:
            continue
        row = {key: raw[key] for key in _ROW_FIELDS if raw.get(key) not in (None, "", [], {})}
        row["sort_epoch"] = epoch
        row["_month"] = month_key(epoch) if epoch else ""
        row["_type"] = _row_type(raw)
        values = raw.get("_values") if wanted == "ratings" else None
        if isinstance(values, Mapping) and values:
            row["_endpoints"] = list(values)
            row["_ratings"] = {endpoint: value[0] for endpoint, value in values.items()}
            row["_mismatch"] = len(set(row["_ratings"].values())) > 1
        else:
            row["_endpoints"] = _row_endpoints(raw)
            row["_mismatch"] = False
        endpoints.update(row["_endpoints"])
        if wanted == "scrobble":
            minutes += dw._duration_minutes(raw)
        rows.append(row)
    rows.sort(key=lambda row: row["sort_epoch"], reverse=True)

    ordered = sorted(endpoints)
    counts: dict[str, Any] = {"all": len(rows), **{typ: 0 for typ in _TYPES[wanted]}}
    provider_counts: dict[str, int] = {}
    rating_counts = {str(value): 0 for value in range(1, 11)}
    rating_total = 0
    full = 0
    mismatch = 0
    watches = 0
    for row in rows:
        watches += max(1, dw._as_int(row.get("watch_count")) or 1)
        if row["_type"] in counts:
            counts[row["_type"]] += 1
        for provider in {endpoint[0] for endpoint in row["_endpoints"]}:
            key = provider.lower()
            provider_counts[key] = provider_counts.get(key, 0) + 1
        if ordered and len(row["_endpoints"]) >= len(ordered):
            full += 1
        if row["_mismatch"]:
            mismatch += 1
        rating = dw._as_int(row.get("rating"))
        if wanted == "ratings" and rating is not None and 1 <= rating <= 10:
            rating_counts[str(rating)] += 1
            rating_total += rating
    if wanted == "ratings":
        counts["ratings"] = rating_counts
    return {
        "source": wanted,
        "rows": rows,
        "endpoints": ordered,
        "counts": counts,
        "provider_counts": provider_counts,
        "full": full,
        "mismatch": mismatch,
        "rating_total": rating_total,
        "minutes": minutes,
        "watches": watches,
    }


def cached_history_index(cache_key: tuple[Any, ...] | None, builder: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    if cache_key is None:
        return builder()
    with _CACHE_LOCK:
        hit = _INDEX_CACHE.get(cache_key)
        if hit is not None:
            _INDEX_CACHE.move_to_end(cache_key)
            return hit
    with _BUILD_LOCK:
        with _CACHE_LOCK:
            hit = _INDEX_CACHE.get(cache_key)
        if hit is not None:
            return hit
        index = builder()
        with _CACHE_LOCK:
            _INDEX_CACHE[cache_key] = index
            _INDEX_CACHE.move_to_end(cache_key)
            while len(_INDEX_CACHE) > _INDEX_CACHE_MAX:
                _INDEX_CACHE.popitem(last=False)
    return index


def _page_size(value: Any) -> int:
    return max(1, min(PAGE_SIZE_MAX, dw._as_int(value) or 48))


def _search_text(row: Mapping[str, Any]) -> str:
    raw_ids = row.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    parts = [
        row.get("title"),
        row.get("year"),
        row.get("episode_label"),
        row.get("key"),
        *(f"{name}:{value}" for name, value in ids.items() if not isinstance(value, Mapping)),
        *(endpoint[0] for endpoint in row["_endpoints"]),
    ]
    return " ".join(str(part) for part in parts if part not in (None, "")).lower()


def _public_row(row: Mapping[str, Any], source: str, endpoints: list[tuple[str, str]]) -> dict[str, Any]:
    out = {key: value for key, value in row.items() if not key.startswith("_") and key != "art_reason"}
    if source in _COVERAGE_SOURCES:
        present = row["_endpoints"]
        ratings = row.get("_ratings") or {}
        out["present"] = [
            {**_ref(endpoint), "rating": ratings[endpoint]} if endpoint in ratings else _ref(endpoint)
            for endpoint in present
        ]
        out["missing"] = [_ref(endpoint) for endpoint in endpoints if endpoint not in present]
    if source == "ratings":
        out["agree"] = not row["_mismatch"]
    return out


def build_history_payload(
    index: Mapping[str, Any],
    *,
    media_type: str = "all",
    provider: str = "",
    coverage: str = "all",
    rating: Any = "",
    search: str = "",
    month: str = "",
    page: int = 1,
    page_size: int = 48,
    resolve_art: bool = True,
    include_keys: bool = False,
) -> dict[str, Any]:
    source = normalize_source(index.get("source"))
    endpoints = [tuple(endpoint) for endpoint in index.get("endpoints") or []]
    endpoint_total = len(endpoints)
    rows = list(index.get("rows") or [])

    wanted_type = str(media_type or "all").strip().lower()
    if wanted_type in _TYPES[source]:
        rows = [row for row in rows if row["_type"] == wanted_type]
    wanted_provider = str(provider or "").strip().upper()
    if wanted_provider:
        rows = [row for row in rows if any(endpoint[0] == wanted_provider for endpoint in row["_endpoints"])]
    wanted_coverage = str(coverage or "all").strip().lower()
    if source in _COVERAGE_SOURCES and endpoint_total and wanted_coverage in {"partial", "full"}:
        want_full = wanted_coverage == "full"
        rows = [row for row in rows if (len(row["_endpoints"]) >= endpoint_total) is want_full]
    elif source == "ratings" and wanted_coverage == "mismatch":
        rows = [row for row in rows if row["_mismatch"]]
    wanted_rating = dw._as_int(rating) if str(rating or "").strip() else None
    if source == "ratings" and wanted_rating is not None:
        rows = [row for row in rows if dw._as_int(row.get("rating")) == wanted_rating]
    needle = str(search or "").strip().lower()
    if needle:
        rows = [row for row in rows if needle in _search_text(row)]

    month_counts: dict[str, int] = {}
    for row in rows:
        if row["_month"]:
            month_counts[row["_month"]] = month_counts.get(row["_month"], 0) + 1

    size = _page_size(page_size)
    total = len(rows)
    page_count = max(1, -(-total // size))
    current = max(1, dw._as_int(page) or 1)
    wanted_month = str(month or "").strip()
    if wanted_month:
        for position, row in enumerate(rows):
            if row["_month"] and row["_month"] <= wanted_month:
                current = position // size + 1
                break
    current = min(current, page_count)
    start = (current - 1) * size
    selected = rows[start:start + size]
    if resolve_art and selected:
        dw._resolve_missing_art_rows(selected, size="w342")

    counts = dict(index.get("counts") or {"all": 0})
    plays = int(counts.get("all") or 0)
    stats: dict[str, Any] = {
        "plays": plays,
        "movies": counts.get("movie", 0),
        "episodes": counts.get("episode", 0),
        "endpoints": endpoint_total,
    }
    if source == "synced":
        stats["watches"] = int(index.get("watches") or plays)
    if source in _COVERAGE_SOURCES:
        stats["full"] = int(index.get("full") or 0)
        stats["partial"] = max(0, plays - stats["full"])
    if source == "ratings":
        stats["mismatch"] = int(index.get("mismatch") or 0)
        stats["average"] = round(int(index.get("rating_total") or 0) / plays, 1) if plays else 0
    if source == "scrobble":
        stats["hours"] = round(float(index.get("minutes") or 0.0) / 60.0, 1)

    payload: dict[str, Any] = {
        "ok": True,
        "source": source,
        "items": [_public_row(row, source, endpoints) for row in selected],
        "total": total,
        "page": current,
        "page_size": size,
        "page_count": page_count,
        "has_more": start + size < total,
        "counts": counts,
        "stats": stats,
        "providers": [{"provider": key, "count": value} for key, value in sorted((index.get("provider_counts") or {}).items())],
        "endpoints": [_ref(endpoint) for endpoint in endpoints],
        "months": [{"month": key, "count": month_counts[key]} for key in sorted(month_counts, reverse=True)],
    }
    if include_keys:
        payload["selection"] = [selection_entry(row) for row in rows[:SELECTION_MAX] if row.get("key")]
        payload["selection_truncated"] = total > SELECTION_MAX
    return payload


_LOOKUP_KEY = "_tmdb_lookup"


def _tmdb_tokens(*values: Any) -> set[str]:
    return {str(value).strip() for value in values if value not in (None, "", 0) and str(value).strip()}


def _row_tmdb_keys(row: Mapping[str, Any]) -> set[str]:
    raw_ids = row.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    raw_show = ids.get("show_ids")
    show_ids: Mapping[str, Any] = raw_show if isinstance(raw_show, Mapping) else {}
    typ = row.get("_type")
    if typ == "movie":
        return {f"movie:{token}" for token in _tmdb_tokens(ids.get("tmdb"), row.get("tmdb"))}
    if typ in {"show", "anime"}:
        return {f"show:{token}" for token in _tmdb_tokens(ids.get("tmdb"), row.get("tmdb"), ids.get("tmdb_show"))}
    if typ in {"season", "episode"}:
        return {f"show:{token}" for token in _tmdb_tokens(show_ids.get("tmdb"), ids.get("tmdb_show"), row.get("tmdb"))}
    return set()


def rows_for_tmdb(index: dict[str, Any], media: str, tmdb: Any) -> list[dict[str, Any]]:
    lookup = index.get(_LOOKUP_KEY)
    if not isinstance(lookup, dict):
        lookup = {}
        for row in index.get("rows") or []:
            for key in _row_tmdb_keys(row):
                lookup.setdefault(key, []).append(row)
        index[_LOOKUP_KEY] = lookup
    wanted = "movie" if str(media or "").strip().lower() == "movie" else "show"
    return list(lookup.get(f"{wanted}:{str(tmdb or '').strip()}") or [])


def title_presence(index: dict[str, Any], media: str, tmdb: Any) -> dict[str, Any] | None:
    rows = rows_for_tmdb(index, media, tmdb)
    if not rows:
        return None
    source = normalize_source(index.get("source"))
    endpoints = [tuple(endpoint) for endpoint in index.get("endpoints") or []]
    present: list[tuple[str, str]] = []
    for row in rows:
        for endpoint in row.get("_endpoints") or []:
            if endpoint not in present:
                present.append(endpoint)
    out: dict[str, Any] = {
        "count": len(rows),
        "last_epoch": max(int(row.get("sort_epoch") or 0) for row in rows),
        "present": [_ref(endpoint) for endpoint in present],
        "missing": [_ref(endpoint) for endpoint in endpoints if endpoint not in present],
    }
    if source == "ratings":
        latest = max(rows, key=lambda row: int(row.get("sort_epoch") or 0))
        ratings = latest.get("_ratings") or {}
        latest_endpoints = latest.get("_endpoints") or []
        out["rating"] = latest.get("rating")
        out["agree"] = not latest.get("_mismatch")
        out["present"] = [
            {**_ref(endpoint), "rating": ratings[endpoint]} if endpoint in ratings else _ref(endpoint)
            for endpoint in latest_endpoints
        ]
        out["missing"] = [_ref(endpoint) for endpoint in endpoints if endpoint not in latest_endpoints]
    episodes: dict[tuple[int, int], int] = {}
    for row in rows:
        season = dw._as_int(row.get("season"))
        episode = dw._as_int(row.get("episode"))
        if row.get("_type") != "episode" or season is None or episode is None:
            continue
        epoch = int(row.get("sort_epoch") or 0)
        episodes[(season, episode)] = max(episodes.get((season, episode), 0), epoch)
    if episodes:
        out["episodes"] = [
            {"season": season, "episode": episode, "epoch": epoch}
            for (season, episode), epoch in sorted(episodes.items())
        ]
    return out


SELECTION_MAX = 1000
_SELECTION_FIELDS = ("type", "title", "year", "season", "episode", "ids")


def selection_entry(row: Mapping[str, Any]) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "key": row["key"],
        "aliases": list(row.get("aliases") or []),
        "present": [_ref(endpoint) for endpoint in row.get("_endpoints") or []],
    }
    for field in _SELECTION_FIELDS:
        if row.get(field) not in (None, "", [], {}):
            entry[field] = row[field]
    return entry
