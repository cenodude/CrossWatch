# providers/sync/flicklist/_common.py
# CrossWatch - FlickList sync helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Any, Iterator, Mapping, Sequence

import requests

from cw_platform.id_map import canonical_key, ids_from, minimal as id_minimal
from providers.auth._auth_FLICKLIST import API_V3_BASE, is_configured as auth_configured, request_with_auth
from providers.sync._log import log as cw_log

URL_ME = f"{API_V3_BASE}/me"
URL_WATCHLIST = f"{API_V3_BASE}/sync/watchlist"
URL_RATINGS = f"{API_V3_BASE}/sync/ratings"
URL_HISTORY = f"{API_V3_BASE}/sync/history"
URL_WATCHED_MOVIES = f"{API_V3_BASE}/sync/watched/movies"
URL_WATCHED_SHOWS = f"{API_V3_BASE}/sync/watched/shows"
URL_PLAYBACK = f"{API_V3_BASE}/sync/playback"
URL_PLAYBACK_ITEM = f"{API_V3_BASE}/sync/playback/{{id}}"
URL_LISTS = f"{API_V3_BASE}/sync/lists"
URL_LIST = f"{API_V3_BASE}/sync/lists/{{id}}"
URL_LIST_ITEMS = f"{API_V3_BASE}/sync/lists/{{id}}/items"
URL_SCROBBLE = f"{API_V3_BASE}/scrobble/{{action}}"

BULK_MAX = 1000
HISTORY_PAGE_MAX = 500
HISTORY_PAGE_DEFAULT = 100
DEFAULT_TIMEOUT = 20.0
DEFAULT_GET_PER_SEC = 0.2
DEFAULT_POST_PER_SEC = 0.2


class FlickListError(RuntimeError):
    pass


def _log(feature: str, level: str, event: str, **fields: Any) -> None:
    cw_log("FLICKLIST", feature, level, event, **fields)


def _dbg(feature: str, event: str, **fields: Any) -> None:
    _log(feature, "debug", event, **fields)


def _info(feature: str, event: str, **fields: Any) -> None:
    _log(feature, "info", event, **fields)


def _warn(feature: str, event: str, **fields: Any) -> None:
    _log(feature, "warn", event, **fields)


def cfg_section(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", None) or {}
    block = cfg.get("flicklist") if isinstance(cfg, Mapping) else None
    return block if isinstance(block, Mapping) else {}


def instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def cfg_float(data: Mapping[str, Any], key: str, default: float) -> float:
    try:
        return float(data.get(key, default))
    except Exception:
        return default


def cfg_int(data: Mapping[str, Any], key: str, default: int) -> int:
    try:
        return int(data.get(key, default))
    except Exception:
        return default


def has_auth(block: Mapping[str, Any] | None) -> bool:
    return auth_configured(block)


def flicklist_request(adapter: Any, method: str, url: str, **kwargs: Any) -> requests.Response:
    cfg = getattr(adapter, "config", None) or {}
    section = cfg_section(adapter)
    timeout = kwargs.pop("timeout", cfg_float(section, "timeout", DEFAULT_TIMEOUT))
    session = getattr(adapter, "session", None) or requests.Session()
    return request_with_auth(
        session,
        method,
        url,
        cfg=cfg,
        instance_id=instance_id(adapter),
        timeout=timeout,
        **kwargs,
    )


def ok_status(resp: requests.Response) -> bool:
    return 200 <= int(getattr(resp, "status_code", 0) or 0) < 300


def safe_json(resp: requests.Response) -> Any:
    try:
        if not (getattr(resp, "text", "") or "").strip():
            return None
        return resp.json()
    except Exception:
        return None


def error_of(resp: requests.Response) -> str:
    data = safe_json(resp)
    if not isinstance(data, Mapping):
        return ""
    detail = data.get("detail")
    if isinstance(detail, list):
        return "; ".join(str((x or {}).get("msg") if isinstance(x, Mapping) else x) for x in detail)[:200]
    return str(data.get("error") or data.get("message") or detail or "").strip()[:200]


def as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def header_int(resp: requests.Response, name: str) -> int | None:
    try:
        headers = getattr(resp, "headers", {}) or {}
        value = headers.get(name) or headers.get(name.lower()) or headers.get(name.upper())
    except Exception:
        return None
    return as_int(value)


def positive_int(value: Any) -> int | None:
    out = as_int(value)
    return out if out is not None and out > 0 else None


def as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except Exception:
        return None


def imdb_id(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if not text.startswith("tt"):
        text = f"tt{text.lstrip('t')}"
    return text if text[2:].isdigit() else None


def iso_z(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            secs = float(value) / 1000.0 if float(value) > 1e11 else float(value)
            return datetime.fromtimestamp(secs, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00").replace(" ", "T"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    except Exception:
        return None


def not_future(value: str | None) -> str | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return value
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.000Z") if dt > now else value


def epoch_of(value: Any) -> int | None:
    text = iso_z(value)
    if not text:
        return None
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def norm_read_type(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"tv", "show", "shows", "series"}:
        return "show"
    if text in {"episode", "episodes"}:
        return "episode"
    if text in {"season", "seasons"}:
        return "season"
    return "movie"


def norm_write_type(value: Any) -> str:
    text = norm_read_type(value)
    return "show" if text in {"show", "season", "episode"} else "movie"


def ids_from_flicklist(raw: Mapping[str, Any] | None) -> dict[str, str]:
    src = raw if isinstance(raw, Mapping) else {}
    out: dict[str, str] = {}
    fldb = str(src.get("fldb") or "").strip()
    if fldb:
        out["fldb"] = fldb
    slug = str(src.get("slug") or "").strip()
    if slug:
        out["slug"] = slug
    tmdb = positive_int(src.get("tmdb"))
    if tmdb:
        out["tmdb"] = str(tmdb)
    imdb = imdb_id(src.get("imdb"))
    if imdb:
        out["imdb"] = imdb
    tvdb = positive_int(src.get("tvdb"))
    if tvdb:
        out["tvdb"] = str(tvdb)
    anilist = positive_int(src.get("anilist"))
    if anilist:
        out["anilist"] = str(anilist)
    return out


def _show_ids_for_write(src: Mapping[str, Any]) -> Mapping[str, Any] | None:
    show_ids = src.get("show_ids") if isinstance(src.get("show_ids"), Mapping) else None
    if show_ids:
        return show_ids

    ids = src.get("ids") if isinstance(src.get("ids"), Mapping) else {}
    prefixed: dict[str, Any] = {}
    for source in (ids, src):
        if not isinstance(source, Mapping):
            continue
        for key in ("fldb", "tmdb", "imdb", "tvdb"):
            value = source.get(f"{key}_show")
            if value:
                prefixed[key] = value
    return prefixed or None


def _write_ids_from(src: Mapping[str, Any], *, prefer_show: bool = False) -> dict[str, Any]:
    if prefer_show:
        ids = _show_ids_for_write(src) or {}
    else:
        ids = src.get("ids") if isinstance(src.get("ids"), Mapping) else src
    out: dict[str, Any] = {}
    fldb = str((ids or {}).get("fldb") or src.get("_flicklist_fldb") or "").strip()
    if fldb:
        out["fldb"] = fldb
        return out
    tmdb = positive_int((ids or {}).get("tmdb"))
    if tmdb:
        out["tmdb"] = tmdb
    imdb = imdb_id((ids or {}).get("imdb"))
    if imdb:
        out["imdb"] = imdb
    tvdb = positive_int((ids or {}).get("tvdb"))
    if tvdb:
        out["tvdb"] = tvdb
    return out


def write_ident(item: Mapping[str, Any]) -> dict[str, Any] | None:
    typ = norm_write_type(item.get("type"))
    is_episode = norm_read_type(item.get("type")) == "episode" or item.get("season") is not None or item.get("episode") is not None
    ids = _write_ids_from(item, prefer_show=is_episode)
    if not ids:
        return None
    payload: dict[str, Any] = {"ids": ids, "media_type": typ}
    if is_episode:
        season = as_int(item.get("season") if item.get("season") is not None else item.get("season_number"))
        episode = as_int(item.get("episode") if item.get("episode") is not None else item.get("episode_number"))
        if season is None or episode is None:
            return None
        payload["media_type"] = "show"
        payload["season"] = season
        payload["episode"] = episode
    return payload


def read_minimal(row: Mapping[str, Any], *, default_type: str = "movie") -> dict[str, Any] | None:
    ids = ids_from_flicklist(row.get("ids") if isinstance(row.get("ids"), Mapping) else {})
    if not ids:
        return None
    typ = norm_read_type(row.get("type") or row.get("media_type") or default_type)
    out: dict[str, Any] = {"type": typ, "ids": {k: v for k, v in ids.items() if k != "fldb"}}
    if "fldb" in ids:
        out["_flicklist_fldb"] = ids["fldb"]
    title = str(row.get("title") or "").strip()
    if title:
        out["title"] = title
    year = as_int(row.get("year"))
    if year:
        out["year"] = year
    season = as_int(row.get("season_number") if row.get("season_number") is not None else row.get("season"))
    episode = as_int(row.get("episode_number") if row.get("episode_number") is not None else row.get("episode"))
    if typ == "episode" or (season is not None and episode is not None):
        out["type"] = "episode"
        if season is not None:
            out["season"] = season
        if episode is not None:
            out["episode"] = episode
        # FlickList read rows carry show-level ids for episodes.
        out["show_ids"] = {k: v for k, v in ids.items() if k in {"tmdb", "imdb", "tvdb", "anilist", "slug"}}
        ep_name = str(row.get("episode_name") or "").strip()
        if ep_name:
            out["title"] = ep_name
        if title:
            out["series_title"] = title
    return id_minimal(out)


def key_of(item: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(item)) or "").strip()
    except Exception:
        return ""


def event_key(item: Mapping[str, Any]) -> str:
    base = key_of(item)
    if not base:
        return ""
    ts = epoch_of(item.get("watched_at"))
    if ts is not None:
        return f"{base}@{ts}"
    rid = str(item.get("_flicklist_history_id") or "").strip()
    return f"{base}@id:{rid}" if rid else base


def read_shape(resp: Any, data: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"status": int(getattr(resp, "status_code", 0) or 0)}
    try:
        headers = getattr(resp, "headers", {}) or {}
        out["content_type"] = str(headers.get("Content-Type") or "").split(";", 1)[0]
        for name, label in (
            ("X-FlickList-Item-Count", "item_count"),
            ("X-FlickList-Page-Count", "page_count"),
            ("X-FlickList-Limit", "limit"),
            ("X-FlickList-Page", "page"),
        ):
            value = header_int(resp, name)
            if value is not None:
                out[label] = value
    except Exception:
        pass
    if isinstance(data, list):
        out["body"] = f"list[{len(data)}]"
    elif isinstance(data, Mapping):
        out["body"] = "object"
        out["body_keys"] = ",".join(sorted(str(k) for k in data.keys())[:12])
    elif data is None:
        out["body"] = "empty"
    else:
        out["body"] = type(data).__name__
    try:
        text = str(getattr(resp, "text", "") or "")
        out["preview"] = text[:200]
        out["length"] = len(text)
    except Exception:
        pass
    return out


def chunk(items: Sequence[Mapping[str, Any]], size: int = BULK_MAX) -> Iterator[list[Mapping[str, Any]]]:
    n = max(1, min(int(size or BULK_MAX), BULK_MAX))
    for idx in range(0, len(items), n):
        yield [dict(x) for x in items[idx : idx + n]]


def write_batch_size(adapter: Any, feature: str | None = None) -> int:
    section = cfg_section(adapter) or {}
    keys = []
    feat = str(feature or "").strip().lower()
    if feat:
        keys.append(f"{feat}_write_batch_size")
    keys.append("write_batch_size")
    for key in keys:
        raw = section.get(key)
        if raw is None:
            continue
        n = cfg_int(section, key, BULK_MAX)
        return max(1, min(n, BULK_MAX))
    return BULK_MAX


def half_point(value: Any) -> float | None:
    num = as_float(value)
    if num is None or num <= 0:
        return None
    return max(0.5, min(10.0, math.floor(num * 2.0 + 0.5) / 2.0))


def percent_of(item: Mapping[str, Any]) -> float | None:
    pct = as_float(item.get("progress_percent") if item.get("progress_percent") is not None else item.get("percent"))
    if pct is not None:
        return max(0.0, min(100.0, pct))
    pos = as_float(item.get("progress_ms") if item.get("progress_ms") is not None else item.get("viewOffset"))
    dur = as_float(item.get("duration_ms"))
    if pos is not None and dur and dur > 0:
        return max(0.0, min(100.0, (pos / dur) * 100.0))
    return None


_WRITE_ID_ORDER = ("fldb", "tmdb", "imdb", "tvdb")
_ADD_COUNTERS = ("added", "existing", "restored")
_REMOVE_COUNTERS = ("removed",)


def _norm_write_ids(raw: Any) -> dict[str, str]:
    src = raw if isinstance(raw, Mapping) else {}
    out: dict[str, str] = {}
    for key in _WRITE_ID_ORDER:
        value = src.get(key)
        if value in (None, "", False):
            continue
        text = str(value).strip().lower()
        if text and text != "null":
            out[key] = text
    return out


def _ident_tokens(row: Mapping[str, Any]) -> set[str]:
    ids = _norm_write_ids(row.get("ids"))
    season = as_int(row.get("season"))
    episode = as_int(row.get("episode"))
    frag = f"#s{season}e{episode}" if season is not None and episode is not None else ""
    return {f"{key}:{value}{frag}" for key, value in ids.items()}


def write_counters(body: Any, method: str) -> dict[str, Any]:
    data = body if isinstance(body, Mapping) else {}
    names = _REMOVE_COUNTERS if str(method).upper() == "DELETE" else _ADD_COUNTERS
    out: dict[str, Any] = {}
    for name in names:
        value = as_int(data.get(name))
        if value is not None:
            out[name] = value
    misses = data.get("not_found")
    out["not_found"] = len(misses) if isinstance(misses, list) else 0
    return out


def has_write_counters(body: Any, method: str) -> bool:
    if not isinstance(body, Mapping):
        return False
    names = _REMOVE_COUNTERS if str(method).upper() == "DELETE" else _ADD_COUNTERS
    return any(name in body for name in names) or "not_found" in body


def classify_write(
    sent: Sequence[tuple[str, Mapping[str, Any]]],
    body: Any,
    *,
    method: str = "POST",
) -> tuple[list[str], list[str], list[dict[str, Any]], dict[str, Any]]:
    counters = write_counters(body, method)
    data = body if isinstance(body, Mapping) else {}
    misses = data.get("not_found")
    misses = misses if isinstance(misses, list) else []

    tokens = [(key, _ident_tokens(payload)) for key, payload in sent]
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    unmatched = 0
    claimed: set[str] = set()

    for row in misses:
        if not isinstance(row, Mapping):
            unmatched += 1
            continue
        want = _ident_tokens(row)
        found = ""
        for key, have in tokens:
            if key in claimed or not want or not have:
                continue
            if want & have:
                found = key
                break
        if not found:
            unmatched += 1
            continue
        claimed.add(found)
        unresolved_keys.append(found)
        unresolved.append({"key": found, "status": "not_found", "reason": str(row.get("reason") or "") or None})

    applied = sum(int(counters.get(name) or 0) for name in (_REMOVE_COUNTERS if str(method).upper() == "DELETE" else _ADD_COUNTERS) if name in counters)
    expected = len(sent) - len(misses)
    if applied != expected:
        counters["counter_gap"] = expected - applied

    if unmatched:
        unresolved_keys = [key for key, _ in sent if key]
        unresolved = [{"key": key, "status": "not_found_unmatched", "applied": applied, "not_found": len(misses), "sent": len(sent)} for key in unresolved_keys]
        return [], unresolved_keys, unresolved, counters

    blocked = set(unresolved_keys)
    confirmed = [key for key, _ in sent if key and key not in blocked]
    return confirmed, unresolved_keys, unresolved, counters


__all__ = [
    "HISTORY_PAGE_DEFAULT",
    "BULK_MAX",
    "DEFAULT_GET_PER_SEC",
    "DEFAULT_POST_PER_SEC",
    "DEFAULT_TIMEOUT",
    "FlickListError",
    "URL_HISTORY",
    "URL_LIST",
    "URL_LISTS",
    "URL_LIST_ITEMS",
    "URL_ME",
    "URL_PLAYBACK",
    "URL_PLAYBACK_ITEM",
    "URL_RATINGS",
    "URL_SCROBBLE",
    "URL_WATCHED_MOVIES",
    "URL_WATCHED_SHOWS",
    "URL_WATCHLIST",
    "as_float",
    "as_int",
    "cfg_float",
    "cfg_int",
    "cfg_section",
    "chunk",
    "classify_write",
    "has_write_counters",
    "write_counters",
    "error_of",
    "event_key",
    "flicklist_request",
    "half_point",
    "has_auth",
    "ids_from",
    "ids_from_flicklist",
    "instance_id",
    "iso_z",
    "key_of",
    "norm_read_type",
    "norm_write_type",
    "not_future",
    "now_iso",
    "ok_status",
    "percent_of",
    "positive_int",
    "read_minimal",
    "read_shape",
    "safe_json",
    "write_ident",
    "write_batch_size",
    "_dbg",
    "_info",
    "_warn",
]
