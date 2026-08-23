# providers/sync/stremio/_common.py
# CrossWatch Stremio sync helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import copy
import os
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any

from cw_platform.id_map import ID_KEYS, canonical_key, ids_from, merge_ids, minimal as id_minimal
from cw_platform.provider_instances import resolve_provider_block
from providers.auth._auth_STREMIO import is_configured as auth_is_configured
from providers.sync._log import log as cw_log

COLLECTION = "libraryItem"
DEFAULT_STREMIO_PROFILE_ID = "default"


def is_capture_mode() -> bool:
    return str(os.getenv("CW_CAPTURE_MODE") or "").strip().lower() in {"1", "true", "yes", "on"}


def configured_block(cfg: Mapping[str, Any] | None, instance_id: Any = "default") -> dict[str, Any]:
    return resolve_provider_block(cfg or {}, "stremio", instance_id)


def is_configured(cfg: Mapping[str, Any] | None, instance_id: Any = "default") -> bool:
    return auth_is_configured(configured_block(cfg, instance_id))


def stremio_profile_id(adapter: Any = None) -> str:
    value = getattr(adapter, "stremio_profile_id", DEFAULT_STREMIO_PROFILE_ID) if adapter is not None else DEFAULT_STREMIO_PROFILE_ID
    return str(value or DEFAULT_STREMIO_PROFILE_ID).strip() or DEFAULT_STREMIO_PROFILE_ID


def now_ms() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return iso_from_epoch_ms(now_ms()) or datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def positive_int(value: Any) -> int | None:
    number = to_int(value)
    return number if number is not None and number > 0 else None


def epoch_ms(value: Any) -> int | None:
    number = to_int(value)
    if number is not None:
        return number * 1000 if 0 < number < 10_000_000_000 else number
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


def iso_from_epoch_ms(value: Any) -> str | None:
    ms = epoch_ms(value)
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except Exception:
        return None


def imdb_id(value: Any) -> str | None:
    raw = str(value or "").strip()
    if raw.startswith("tt") and raw[2:].isdigit():
        return raw
    return None


def imdb_ids_from_item(item: Mapping[str, Any]) -> dict[str, str]:
    ids = merge_ids(ids_from(item), ids_from(id_minimal(item)))
    show_ids = item.get("show_ids")
    if isinstance(show_ids, Mapping):
        ids = merge_ids(ids, {str(k): v for k, v in show_ids.items()})
    direct = imdb_id(item.get("_stremio_id") or item.get("stremio_id") or item.get("content_id"))
    if direct:
        ids["imdb"] = direct
    return ids


def stremio_id_for_item(item: Mapping[str, Any]) -> str | None:
    direct = imdb_id(item.get("_stremio_id") or item.get("stremio_id") or item.get("content_id"))
    if direct:
        return direct
    typ = str(item.get("type") or "").strip().lower()
    if typ in {"episode", "episodes", "show", "series", "tv"}:
        show_ids = item.get("show_ids")
        if isinstance(show_ids, Mapping):
            direct = imdb_id(show_ids.get("imdb"))
            if direct:
                return direct
            tmdb = str(show_ids.get("tmdb") or "").strip()
            if tmdb:
                return f"tmdb:{tmdb}"
            tvdb = str(show_ids.get("tvdb") or "").strip()
            if tvdb:
                return f"tvdb:{tvdb}"
    ids = imdb_ids_from_item(item)
    direct = imdb_id(ids.get("imdb"))
    if direct:
        return direct
    tmdb = str(ids.get("tmdb") or "").strip()
    if tmdb and typ in {"movie", "show", "series", "tv"}:
        return f"tmdb:{tmdb}"
    tvdb = str(ids.get("tvdb") or "").strip()
    if tvdb and typ in {"show", "series", "tv"}:
        return f"tvdb:{tvdb}"
    return None


def stremio_record_id_for_item(item: Mapping[str, Any]) -> str | None:
    rid = str(item.get("_stremio_record_id") or "").strip()
    if not rid:
        return None
    typ = str(item.get("type") or "").strip().lower()
    record_type = "series" if typ in {"episode", "episodes", "show", "series", "tv"} else "movie"
    native = ids_from_stremio_id(rid, record_type)
    if not native:
        return None
    if imdb_id(rid):
        return rid
    own = imdb_ids_from_item(item)
    for key, value in native.items():
        if str(own.get(key) or "").strip() == str(value).strip():
            return rid
    return None


def stremio_write_id_for_item(item: Mapping[str, Any]) -> str | None:
    return stremio_record_id_for_item(item) or stremio_id_for_item(item)


def metahub_poster_url(stremio_id: Any) -> str:
    found = imdb_id(stremio_id)
    return f"https://images.metahub.space/poster/small/{found}/img" if found else ""


def poster_url_from_item(item: Mapping[str, Any], stremio_id: Any = None) -> str:
    poster = str(item.get("poster") or item.get("poster_url") or item.get("posterUrl") or "").strip()
    if poster.startswith(("http://", "https://")):
        return poster
    return metahub_poster_url(stremio_id or stremio_id_for_item(item))


def video_id_for_episode(item: Mapping[str, Any], show_id: str) -> str | None:
    direct = str(item.get("_stremio_video_id") or item.get("video_id") or "").strip()
    if direct and direct.startswith(f"{show_id}:"):
        return direct
    season = positive_int(item.get("season"))
    episode = positive_int(item.get("episode"))
    return f"{show_id}:{season}:{episode}" if season and episode else None


def tmdb_metadata_provider(adapter: Any) -> Any | None:
    cached = getattr(adapter, "_stremio_tmdb_provider", None)
    if cached is not None:
        return cached
    cfg = getattr(adapter, "config", None) or getattr(adapter, "raw_cfg", None) or {}
    if not isinstance(cfg, Mapping):
        return None
    tmdb_raw = cfg.get("tmdb")
    metadata_raw = cfg.get("metadata")
    tmdb: Mapping[str, Any] = tmdb_raw if isinstance(tmdb_raw, Mapping) else {}
    metadata: Mapping[str, Any] = metadata_raw if isinstance(metadata_raw, Mapping) else {}
    if not str(tmdb.get("api_key") or metadata.get("tmdb_api_key") or "").strip():
        return None
    try:
        from providers.metadata._meta_TMDB import TmdbProvider

        provider = TmdbProvider(lambda: dict(cfg), lambda _cfg: None)
        setattr(adapter, "_stremio_tmdb_provider", provider)
        return provider
    except Exception:
        return None


def canonical_item_key(item: Mapping[str, Any]) -> str:
    return canonical_key(id_minimal(item))


def state_of(record: Mapping[str, Any]) -> Mapping[str, Any]:
    state = record.get("state")
    return state if isinstance(state, Mapping) else {}


def record_id(record: Mapping[str, Any]) -> str:
    return str(record.get("_id") or "").strip()


def is_bare_numeric_id(value: Any) -> bool:
    raw = str(value or "").strip()
    return bool(raw) and raw.isdigit()


def stremio_id_namespace(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "missing"
    if imdb_id(raw):
        return "imdb"
    if ":" in raw:
        prefix = raw.split(":", 1)[0].strip().lower()
        return prefix or "unknown"
    if raw.isdigit():
        return "tmdb_bare"
    return "unknown"


def ids_from_stremio_id(value: Any, record_type: Any = None) -> dict[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return {}
    iid = imdb_id(raw)
    if iid:
        return {"imdb": iid}
    if ":" in raw:
        prefix, ident = raw.split(":", 1)
        prefix = prefix.strip().lower()
        ident = ident.strip()
        if prefix in ID_KEYS and ident:
            return {prefix: ident}
        return {}
    typ = str(record_type or "").strip().lower()
    if raw.isdigit() and typ in {"movie", "series", "show", "tv"}:
        return {"tmdb": raw}
    return {}


def _normalized_title(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return "".join(ch for ch in raw if ch.isalnum())


def _titles_agree(left: Any, right: Any) -> bool:
    a, b = _normalized_title(left), _normalized_title(right)
    if not a or not b:
        return False
    return a == b or a.startswith(b) or b.startswith(a)


def verify_bare_numeric_id(adapter: Any, record: Mapping[str, Any]) -> tuple[bool, str | None]:
    rid = record_id(record)
    if not is_bare_numeric_id(rid):
        return True, None
    typ = str(record.get("type") or "").strip().lower()
    entity = "tv" if typ in {"series", "show", "tv"} else "movie"
    cache_key = f"{entity}:{rid}:{_normalized_title(record.get('name'))}"
    cache = getattr(adapter, "_stremio_bare_id_checks", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(adapter, "_stremio_bare_id_checks", cache)
    if cache_key in cache:
        return cache[cache_key]
    provider = tmdb_metadata_provider(adapter)
    if provider is None:
        result: tuple[bool, str | None] = (False, "bare_numeric_id_unverified")
        cache[cache_key] = result
        return result
    try:
        detail = provider.fetch(entity=entity, ids={"tmdb": rid}, need={"ids": True})
    except Exception:
        detail = None
    title = str((detail or {}).get("title") or "").strip() if isinstance(detail, Mapping) else ""
    if not title:
        result = (False, "bare_numeric_id_unverified")
    elif _titles_agree(title, record.get("name")):
        result = (True, None)
    else:
        result = (False, "bare_numeric_id_mismatch")
    cache[cache_key] = result
    return result


def ids_from_stremio_record(record: Mapping[str, Any]) -> dict[str, str]:
    return ids_from_stremio_id(record_id(record), record.get("type"))


def native_record_ids(adapter: Any, record: Mapping[str, Any]) -> tuple[dict[str, str], str | None]:
    ids = ids_from_stremio_record(record)
    if not ids:
        return {}, "unsupported_stremio_id"
    ok, reason = verify_bare_numeric_id(adapter, record)
    if not ok:
        return {}, reason or "bare_numeric_id_unverified"
    return ids, None


def _item_common_from_record(record: Mapping[str, Any], item_type: str) -> dict[str, Any] | None:
    rid = record_id(record)
    ids = ids_from_stremio_record(record)
    if not ids:
        return None
    item: dict[str, Any] = {
        "type": item_type,
        "ids": ids,
        "_stremio_id": rid,
        "_stremio_record_id": rid,
        "_stremio_record_namespace": stremio_id_namespace(rid),
    }
    title = str(record.get("name") or "").strip()
    if title:
        item["title"] = title
    poster = str(record.get("poster") or "").strip()
    if poster:
        item["poster"] = poster
    return item


def reset_read_drop_report(adapter: Any, feature: str) -> None:
    reports = getattr(adapter, "_stremio_read_drops", None)
    if not isinstance(reports, dict):
        reports = {}
        setattr(adapter, "_stremio_read_drops", reports)
    reports[str(feature or "").strip().lower()] = []


def record_read_drop(adapter: Any, feature: str, record: Mapping[str, Any], reason: str, **fields: Any) -> None:
    feature_s = str(feature or "").strip().lower()
    rid = record_id(record)
    entry = {
        "record_id": rid,
        "record_type": str(record.get("type") or "").strip().lower(),
        "namespace": stremio_id_namespace(rid),
        "reason": str(reason or "unsupported_stremio_id"),
    }
    title = str(record.get("name") or "").strip()
    if title:
        entry["title"] = title
    entry.update({k: v for k, v in fields.items() if v is not None})
    reports = getattr(adapter, "_stremio_read_drops", None)
    if not isinstance(reports, dict):
        reports = {}
        setattr(adapter, "_stremio_read_drops", reports)
    bucket = reports.setdefault(feature_s, [])
    if isinstance(bucket, list):
        bucket.append(entry)
    cw_log("STREMIO", feature_s or "index", "debug", "index_row_dropped", **entry)


def read_drop_unresolved_items(adapter: Any, feature: str) -> list[dict[str, Any]]:
    reports = getattr(adapter, "_stremio_read_drops", None)
    if not isinstance(reports, Mapping):
        return []
    rows = reports.get(str(feature or "").strip().lower())
    if not isinstance(rows, list):
        return []

    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        rid = str(row.get("record_id") or "").strip()
        if not rid:
            continue
        record_type = str(row.get("record_type") or "").strip().lower()
        item_type = "movie" if record_type == "movie" else "show" if record_type in {"series", "show"} else record_type or "movie"
        namespace = str(row.get("namespace") or "").strip().lower()
        ids: dict[str, str] = ids_from_stremio_id(rid, record_type)
        if not ids:
            ids["slug"] = f"stremio:{rid}"

        reason = str(row.get("reason") or "unsupported_stremio_id").strip()
        item: dict[str, Any] = {
            "type": item_type,
            "ids": ids,
            "_stremio_record_id": rid,
            "_stremio_record_namespace": namespace,
            "_stremio_drop_reason": reason,
            "_cw_unresolved_hint": f"stremio_read:{reason}",
        }
        title = str(row.get("title") or "").strip()
        if title:
            item["title"] = title
        out.append(item)
    return out


def read_drop_summary(adapter: Any, feature: str) -> dict[str, Any]:
    reports = getattr(adapter, "_stremio_read_drops", None)
    if not isinstance(reports, Mapping):
        return {"dropped": 0, "drop_reasons": None, "drop_namespaces": None}
    rows = reports.get(str(feature or "").strip().lower())
    if not isinstance(rows, list):
        return {"dropped": 0, "drop_reasons": None, "drop_namespaces": None}
    reasons = sorted({str(row.get("reason") or "") for row in rows if isinstance(row, Mapping) and row.get("reason")})
    namespaces = sorted({str(row.get("namespace") or "") for row in rows if isinstance(row, Mapping) and row.get("namespace")})
    return {
        "dropped": len(rows),
        "drop_reasons": ",".join(reasons) or None,
        "drop_namespaces": ",".join(namespaces) or None,
    }


def _extract_records(data: Any) -> list[Mapping[str, Any]]:
    source = data.get("result") if isinstance(data, Mapping) and "result" in data else data
    if isinstance(source, list):
        return [row for row in source if isinstance(row, Mapping)]
    if isinstance(source, Mapping):
        if isinstance(source.get("items"), list):
            return [row for row in source["items"] if isinstance(row, Mapping)]
        if isinstance(source.get(COLLECTION), list):
            return [row for row in source[COLLECTION] if isinstance(row, Mapping)]
        if all(isinstance(v, Mapping) for v in source.values()):
            return [v for v in source.values() if isinstance(v, Mapping)]
    return []


def datastore_get(adapter: Any, *, ids: list[str] | None = None, all_records: bool = False) -> list[Mapping[str, Any]]:
    payload: dict[str, Any] = {"collection": COLLECTION, "ids": list(ids or [])}
    if all_records:
        payload["all"] = True
    return _extract_records(adapter.client.request_json("datastoreGet", payload))


def datastore_meta(adapter: Any) -> list[Mapping[str, Any]]:
    return _extract_records(adapter.client.request_json("datastoreMeta", {"collection": COLLECTION}))


def datastore_put(adapter: Any, changes: list[Mapping[str, Any]]) -> Any:
    return adapter.client.request_json("datastorePut", {"collection": COLLECTION, "changes": [dict(x) for x in changes]})


def library_records(adapter: Any, *, incremental: bool = False) -> list[Mapping[str, Any]]:
    cache_attr = f"_stremio_mtime_cache_{stremio_profile_id(adapter)}"
    if is_capture_mode():
        return datastore_get(adapter, ids=[], all_records=True)
    if not incremental:
        rows = datastore_get(adapter, ids=[], all_records=True)
        setattr(adapter, cache_attr, {record_id(row): epoch_ms(row.get("_mtime")) or 0 for row in rows if record_id(row)})
        return rows
    meta = datastore_meta(adapter)
    old = getattr(adapter, cache_attr, {})
    old_map = old if isinstance(old, Mapping) else {}
    changed = [record_id(row) for row in meta if record_id(row) and (epoch_ms(row.get("_mtime")) or 0) > (to_int(old_map.get(record_id(row))) or 0)]
    if not changed:
        return []
    rows = datastore_get(adapter, ids=changed, all_records=False)
    new_map = dict(old_map)
    for row in meta:
        rid = record_id(row)
        if rid:
            new_map[rid] = epoch_ms(row.get("_mtime")) or 0
    setattr(adapter, cache_attr, new_map)
    return rows


def default_record(stremio_id: str, item_type: str, item: Mapping[str, Any] | None = None, *, timestamp: Any = None) -> dict[str, Any]:
    ts = iso_from_epoch_ms(timestamp) or now_iso()
    typ = "series" if item_type in {"series", "show", "episode", "episodes"} else "movie"
    src = item or {}
    poster_value = poster_url_from_item(src, stremio_id)
    return {
        "_id": stremio_id,
        "name": str(src.get("series_title") or src.get("show_title") or src.get("title") or stremio_id),
        "type": typ,
        "poster": poster_value,
        "posterShape": src.get("poster_shape") or "poster",
        "removed": True,
        "temp": True,
        "_ctime": ts,
        "_mtime": ts,
        "state": {
            "lastWatched": "",
            "timeWatched": 0,
            "timeOffset": 0,
            "overallTimeWatched": 0,
            "timesWatched": 0,
            "flaggedWatched": 0,
            "duration": 0,
            "video_id": "",
            "watched": "",
            "noNotif": False,
            "season": 0,
            "episode": 0,
        },
        "behaviorHints": {
            "defaultVideoId": None,
            "featuredVideoId": None,
            "hasScheduledVideos": False,
        },
    }


def read_merge_write(
    adapter: Any,
    stremio_id: str,
    item_type: str,
    item: Mapping[str, Any],
    mutate: Callable[[dict[str, Any]], None],
) -> dict[str, Any]:
    rows = datastore_get(adapter, ids=[stremio_id], all_records=False)
    record = copy.deepcopy(dict(rows[0])) if rows else default_record(stremio_id, item_type, item)
    state = record.get("state")
    if not isinstance(state, dict):
        record["state"] = {}
    mutate(record)
    datastore_put(adapter, [record])
    return record


def item_from_movie_record(record: Mapping[str, Any]) -> dict[str, Any] | None:
    return _item_common_from_record(record, "movie")


def item_from_series_record(record: Mapping[str, Any]) -> dict[str, Any] | None:
    return _item_common_from_record(record, "show")


def item_from_episode(show_id: str, season: Any, episode: Any, record: Mapping[str, Any], video: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
    sn = positive_int(season)
    ep = positive_int(episode)
    ids = ids_from_stremio_id(show_id, "series")
    if not ids or not sn or not ep:
        return None
    item: dict[str, Any] = {
        "type": "episode",
        "show_ids": dict(ids),
        "ids": dict(ids),
        "season": sn,
        "episode": ep,
        "_stremio_id": str(show_id or "").strip(),
        "_stremio_record_id": record_id(record),
        "_stremio_record_namespace": stremio_id_namespace(record_id(record)),
    }
    title = str((video or {}).get("title") or "").strip()
    if title:
        item["title"] = title
    series_title = str(record.get("name") or "").strip()
    if series_title:
        item["series_title"] = series_title
    video_id = str((video or {}).get("id") or "").strip()
    if video_id:
        item["_stremio_video_id"] = video_id
    poster = str((video or {}).get("thumbnail") or record.get("poster") or "").strip()
    if poster:
        item["poster"] = poster
    return item
