# /providers/sync/scrob/_common.py
# Shared helpers for Scrob sync modules
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import requests

from .._log import log as cw_log
from .._mod_common import SimpleRateLimiter
from providers.auth._auth_SCROB import (
    ScrobAuthError,
    is_configured as auth_configured,
    normalize_api_prefix,
    normalize_server_url,
    request_with_auth as scrob_request_with_auth,
)

PATH_HISTORY = "history"
PATH_HISTORY_EVENT = "history/event/{event_id}"
PATH_HISTORY_ITEM = "history/item"
PATH_NOW_PLAYING = "history/now-playing"
PATH_CONTINUE_WATCHING = "history/continue-watching"
PATH_RATINGS = "ratings"
PATH_LISTS = "lists"
PATH_LIST = "lists/{list_id}"
PATH_LIST_ITEMS = "lists/{list_id}/items"
PATH_LIST_ITEM = "lists/{list_id}/items/{item_id}"
PATH_WEBHOOK_KODI = "webhooks/kodi"
PATH_WEBHOOK_KODI_RATING = "webhooks/kodi/rating"
PATH_WEBHOOK_JELLYFIN = "webhooks/jellyfin"

HISTORY_PAGE_MAX = 100
DEFAULT_TIMEOUT = 12.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_GET_PER_SEC = 10.0
DEFAULT_POST_PER_SEC = 5.0

MEDIA_TYPE_MOVIE = "movie"
MEDIA_TYPE_EPISODE = "episode"
MEDIA_TYPE_SERIES = "series"

_LIMITERS: dict[str, SimpleRateLimiter] = {}


class ScrobError(RuntimeError):
    pass


def _dbg(feature: str, msg: str, **fields: Any) -> None:
    cw_log("SCROB", feature, "debug", msg, **fields)


def _info(feature: str, msg: str, **fields: Any) -> None:
    cw_log("SCROB", feature, "info", msg, **fields)


def _warn(feature: str, msg: str, **fields: Any) -> None:
    cw_log("SCROB", feature, "warn", msg, **fields)


def mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def cfg_section(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", None)
    if isinstance(cfg, Mapping) and isinstance(cfg.get("scrob"), Mapping):
        return cfg["scrob"]
    raw = getattr(adapter, "raw_cfg", None)
    if isinstance(raw, Mapping) and isinstance(raw.get("scrob"), Mapping):
        return raw["scrob"]
    return {}


def instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def has_auth(block: Mapping[str, Any] | None) -> bool:
    return auth_configured(block)


def cfg_int(data: Mapping[str, Any], key: str, default: int) -> int:
    raw = data.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def cfg_float(data: Mapping[str, Any], key: str, default: float) -> float:
    raw = data.get(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def base_url(section: Mapping[str, Any]) -> str:
    server = normalize_server_url(section.get("server_url"))
    prefix = normalize_api_prefix(section.get("api_prefix"))
    return f"{server}{prefix}"


def url_for(section: Mapping[str, Any], path: str) -> str:
    return f"{base_url(section)}/{str(path or '').strip('/')}"


def _limiter(inst: str, section: Mapping[str, Any]) -> SimpleRateLimiter:
    limiter = _LIMITERS.get(inst)
    if limiter is None:
        rl = mapping(section.get("rate_limit"))
        limiter = SimpleRateLimiter(
            rates_per_sec={
                "GET": cfg_float(rl, "get_per_sec", DEFAULT_GET_PER_SEC),
                "POST": cfg_float(rl, "post_per_sec", DEFAULT_POST_PER_SEC),
                "PATCH": cfg_float(rl, "post_per_sec", DEFAULT_POST_PER_SEC),
                "DELETE": cfg_float(rl, "post_per_sec", DEFAULT_POST_PER_SEC),
            }
        )
        _LIMITERS[inst] = limiter
    return limiter


def scrob_request(adapter: Any, method: str, path: str, **kwargs: Any) -> requests.Response:
    cfg = getattr(adapter, "config", None) or getattr(adapter, "raw_cfg", None) or {}
    section = cfg_section(adapter)
    session = getattr(adapter, "session", None)
    if session is None:
        client = getattr(adapter, "client", None)
        session = getattr(client, "session", None) or requests.Session()
    inst = instance_id(adapter)
    timeout = kwargs.pop("timeout", cfg_float(section, "timeout", DEFAULT_TIMEOUT))

    _limiter(inst, section).wait(str(method).upper())
    return scrob_request_with_auth(
        session,
        method,
        url_for(section, path),
        cfg=cfg,
        instance_id=inst,
        timeout=timeout,
        **kwargs,
    )


def ok_status(resp: requests.Response) -> bool:
    return 200 <= int(getattr(resp, "status_code", 0) or 0) < 300


def error_of(resp: requests.Response) -> str:
    try:
        body = resp.json() or {}
    except Exception:
        return ""
    if not isinstance(body, Mapping):
        return ""
    detail = body.get("detail")
    if isinstance(detail, list):
        return "; ".join(str(x.get("msg") if isinstance(x, Mapping) else x) for x in detail)[:200]
    return str(detail or body.get("error") or "").strip()[:200]


def safe_json(resp: requests.Response) -> Any:
    try:
        if not (getattr(resp, "text", "") or "").strip():
            return None
        return resp.json()
    except Exception:
        return None


def as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def positive_int(value: Any) -> int | None:
    out = as_int(value)
    return out if out is not None and out > 0 else None


def as_imdb(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if not text.startswith("tt"):
        text = f"tt{text.lstrip('t')}"
    rest = text[2:]
    return text if rest.isdigit() and rest else None


def iso_z(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            secs = float(value)
            if secs > 1e11:
                secs = secs / 1000.0
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


def ids_for_scrob(item: Mapping[str, Any]) -> dict[str, Any]:
    ids = item.get("ids") if isinstance(item.get("ids"), Mapping) else item
    out: dict[str, Any] = {}
    tmdb = positive_int((ids or {}).get("tmdb"))
    if tmdb:
        out["tmdb_id"] = tmdb
    imdb = as_imdb((ids or {}).get("imdb"))
    if imdb:
        out["imdb_id"] = imdb
    tvdb = positive_int((ids or {}).get("tvdb"))
    if tvdb:
        out["tvdb_id"] = tvdb
    return out


def show_ids_for_scrob(item: Mapping[str, Any]) -> dict[str, Any]:
    show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else {}
    return ids_for_scrob({"ids": show_ids})


def media_type_of(item: Mapping[str, Any]) -> str:
    raw = str(item.get("type") or "").strip().lower()
    if raw in ("episode", "ep"):
        return MEDIA_TYPE_EPISODE
    if raw in ("show", "series", "tv"):
        return MEDIA_TYPE_SERIES
    return MEDIA_TYPE_MOVIE


def media_to_ids(media: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    tmdb = positive_int(media.get("tmdb_id"))
    if tmdb:
        out["tmdb"] = str(tmdb)
    return out


def media_show_ids(media: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    tmdb = positive_int(media.get("show_tmdb_id"))
    if tmdb:
        out["tmdb"] = str(tmdb)
    tvdb = positive_int(media.get("show_tvdb_id"))
    if tvdb:
        out["tvdb"] = str(tvdb)
    return out


def year_of(media: Mapping[str, Any]) -> int | None:
    raw = str(media.get("release_date") or "").strip()
    if len(raw) >= 4 and raw[:4].isdigit():
        return int(raw[:4])
    return None


def item_from_media(media: Mapping[str, Any]) -> dict[str, Any] | None:
    raw_type = str(media.get("type") or "").strip().lower()
    if raw_type == MEDIA_TYPE_EPISODE:
        season = as_int(media.get("season_number"))
        episode = as_int(media.get("episode_number"))
        show_ids = media_show_ids(media)
        if season is None or episode is None or not show_ids:
            return None
        out: dict[str, Any] = {
            "type": MEDIA_TYPE_EPISODE,
            "ids": media_to_ids(media),
            "show_ids": show_ids,
            "season": season,
            "episode": episode,
        }
        series_title = str(media.get("show_title") or "").strip()
        if series_title:
            out["series_title"] = series_title
        title = str(media.get("title") or "").strip()
        if title:
            out["title"] = title
        return out

    ids = media_to_ids(media)
    if not ids:
        return None
    out = {"type": MEDIA_TYPE_MOVIE, "ids": ids}
    title = str(media.get("title") or "").strip()
    if title:
        out["title"] = title
    year = year_of(media)
    if year:
        out["year"] = year
    return out


def paged_get(adapter: Any, path: str, *, feature: str, page_size: int, max_pages: int, params: Mapping[str, Any] | None = None) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    page = 1
    size = max(1, min(int(page_size or HISTORY_PAGE_MAX), HISTORY_PAGE_MAX))
    while True:
        if max_pages and page > max_pages:
            _warn(feature, "index_page_cap_hit", max_pages=max_pages)
            break
        query = dict(params or {})
        query.update({"page": page, "page_size": size})
        resp = scrob_request(adapter, "GET", path, params=query)
        if not ok_status(resp):
            _warn(feature, "fetch_failed", status=int(resp.status_code), error=error_of(resp), page=page)
            break
        data = safe_json(resp)
        if not isinstance(data, Mapping):
            break
        chunk = data.get("results")
        chunk = [r for r in chunk if isinstance(r, Mapping)] if isinstance(chunk, list) else []
        rows.extend(chunk)
        total_pages = as_int(data.get("total_pages")) or 1
        if not chunk or page >= total_pages:
            break
        page += 1
    return rows


def webhook_post(adapter: Any, path: str, payload: Mapping[str, Any]) -> requests.Response:
    section = cfg_section(adapter)
    session = getattr(adapter, "session", None) or requests.Session()
    inst = instance_id(adapter)
    _limiter(inst, section).wait("POST")
    api_key = str(section.get("api_key") or "").strip()
    if not api_key:
        raise ScrobError("Scrob API key is required for webhook delivery")
    return session.post(
        url_for(section, path),
        params={"api_key": api_key},
        json=dict(payload),
        headers={"Accept": "application/json", "X-Api-Key": api_key, "User-Agent": "CrossWatch/1.0"},
        timeout=cfg_float(section, "timeout", DEFAULT_TIMEOUT),
        verify=bool(section.get("verify_ssl", False)),
        allow_redirects=False,
    )


__all__ = [
    "ScrobAuthError",
    "ScrobError",
    "PATH_HISTORY",
    "PATH_HISTORY_EVENT",
    "PATH_HISTORY_ITEM",
    "PATH_NOW_PLAYING",
    "PATH_CONTINUE_WATCHING",
    "PATH_RATINGS",
    "PATH_LISTS",
    "PATH_LIST",
    "PATH_LIST_ITEMS",
    "PATH_LIST_ITEM",
    "PATH_WEBHOOK_KODI",
    "PATH_WEBHOOK_KODI_RATING",
    "PATH_WEBHOOK_JELLYFIN",
    "HISTORY_PAGE_MAX",
    "as_imdb",
    "as_int",
    "base_url",
    "cfg_float",
    "cfg_int",
    "cfg_section",
    "epoch_of",
    "error_of",
    "has_auth",
    "ids_for_scrob",
    "instance_id",
    "iso_z",
    "item_from_media",
    "mapping",
    "media_show_ids",
    "media_to_ids",
    "media_type_of",
    "not_future",
    "now_iso",
    "ok_status",
    "paged_get",
    "positive_int",
    "safe_json",
    "scrob_request",
    "show_ids_for_scrob",
    "url_for",
    "webhook_post",
    "year_of",
    "_dbg",
    "_info",
    "_warn",
]
