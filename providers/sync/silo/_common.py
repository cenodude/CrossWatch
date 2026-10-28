# /providers/sync/silo/_common.py
# Shared helpers for Silo sync modules
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import quote

import requests

from cw_platform.collection_media import stream_media_info
from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.path_matching import PathIndex, ids_conflict, path_keys
from cw_platform.value_coercion import coerce_bool
from providers.auth._auth_SILO import api_url, normalize_server_url, provider_block, request_with_auth

from .._log import log as cw_log
from .._mod_common import SimpleRateLimiter

STATE_DIR = Path("/config/.cw_state")
try:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass

PAGE_LIMIT = 200
PAGE_GUARD = 5000
WRITE_BATCH = 100
DEFAULT_TIMEOUT = 20.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_GET_PER_SEC = 20.0
DEFAULT_POST_PER_SEC = 10.0
LIBRARY_TTL_SEC = 600.0
DETAIL_TTL_SEC = 30 * 86400
FILES_STATE = "files"

_MOVIE_RE = re.compile(r"^movie-(tmdb|imdb|tvdb)-([a-z0-9]+)$")
_SERIES_RE = re.compile(r"^series-(tvdb|tmdb|imdb)-([a-z0-9]+)$")
_EPISODE_RE = re.compile(r"^episode-(tvdb|tmdb|imdb)-([a-z0-9]+)-(\d+)-(\d+)$")

_LIMITERS: dict[str, SimpleRateLimiter] = {}
_LIBRARIES: dict[str, tuple[float, "Library"]] = {}
_SHARED_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()


class SiloSyncError(RuntimeError):
    pass


def _dbg(feature: str, msg: str, **fields: Any) -> None:
    cw_log("SILO", feature, "debug", msg, **fields)


def _info(feature: str, msg: str, **fields: Any) -> None:
    cw_log("SILO", feature, "info", msg, **fields)


def _warn(feature: str, msg: str, **fields: Any) -> None:
    cw_log("SILO", feature, "warn", msg, **fields)


def key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def iso_z(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def epoch_of(value: Any) -> int | None:
    iso = iso_z(value)
    if not iso:
        return None
    return int(datetime.strptime(iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp())


def cfg_section(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", None) or {}
    block = provider_block(cfg, getattr(adapter, "instance_id", None)) if isinstance(cfg, Mapping) else {}
    return block if isinstance(block, Mapping) else {}


def account_scope(adapter: Any) -> str:
    sec = cfg_section(adapter)
    raw = f"{normalize_server_url(sec.get('server_url'))}|{sec.get('user_id') or ''}|{sec.get('profile_id') or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def server_scope(adapter: Any) -> str:
    raw = normalize_server_url(cfg_section(adapter).get("server_url"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def state_path(name: str, scope: str) -> Path:
    return STATE_DIR / f"silo_{name}.{scope}.json"


def read_state(name: str, scope: str) -> dict[str, Any]:
    try:
        with open(state_path(name, scope), "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_state(name: str, scope: str, data: Mapping[str, Any]) -> None:
    path = state_path(name, scope)
    with _STATE_LOCK:
        try:
            tmp = Path(str(path) + ".tmp")
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(dict(data), fh, ensure_ascii=False)
            tmp.replace(path)
        except Exception:
            pass


def shared_limiter(adapter: Any) -> SimpleRateLimiter:
    sec = cfg_section(adapter)
    rl = sec.get("rate_limit") if isinstance(sec.get("rate_limit"), Mapping) else {}
    get_rps = float((rl or {}).get("get_per_sec") or DEFAULT_GET_PER_SEC)
    post_rps = float((rl or {}).get("post_per_sec") or DEFAULT_POST_PER_SEC)
    key = f"{server_scope(adapter)}|{sec.get('user_id') or ''}|{get_rps}|{post_rps}"
    with _SHARED_LOCK:
        limiter = _LIMITERS.get(key)
        if limiter is None:
            limiter = SimpleRateLimiter(rates_per_sec={"GET": get_rps, "POST": post_rps})
            _LIMITERS[key] = limiter
        return limiter


def label_silo(method: str, url: str, kw: Mapping[str, Any]) -> str:
    path = url.split("/api/v2/", 1)[-1].split("?", 1)[0]
    head = path.split("/", 1)[0] or "unknown"
    m = method.upper()
    if head == "catalog":
        return "library:index"
    if head in ("watchlist", "ratings"):
        return head if m == "GET" else f"{head}:{'remove' if m == 'DELETE' else 'add'}"
    if head == "progress":
        return "progress" if m == "GET" else "progress:remove"
    if head == "sync":
        return "progress:write"
    if head == "watched":
        return "history:add" if m == "POST" else "history:remove"
    if head == "history":
        return "history"
    return head


def silo_request(adapter: Any, method: str, path: str, *, params: Mapping[str, Any] | None = None, json_body: Any = None,
                 headers: Mapping[str, str] | None = None) -> requests.Response:
    sec = cfg_section(adapter)
    url = api_url(str(sec.get("server_url") or ""), path)
    kwargs: dict[str, Any] = {}
    if params:
        kwargs["params"] = dict(params)
    if json_body is not None:
        kwargs["json"] = json_body
    if headers:
        kwargs["headers"] = dict(headers)
    return request_with_auth(
        adapter.session,
        method,
        url,
        cfg=adapter.config,
        instance_id=adapter.instance_id,
        timeout=float(sec.get("timeout") or DEFAULT_TIMEOUT),
        max_retries=int(sec.get("max_retries") or DEFAULT_MAX_RETRIES),
        **kwargs,
    )


def has_more_pages(data: Mapping[str, Any]) -> bool:
    page = data.get("page")
    return bool(page.get("has_more")) if isinstance(page, Mapping) else False


def safe_json(resp: requests.Response) -> Any:
    try:
        if not (resp.text or "").strip():
            return None
        return resp.json()
    except Exception:
        return None


def problem_of(resp: requests.Response) -> str:
    body = safe_json(resp)
    if isinstance(body, Mapping):
        raw = str(body.get("type") or "")
        return raw.rsplit("/", 1)[-1] if raw else ""
    return ""


def pages(adapter: Any, path: str, *, feature: str, params: Mapping[str, Any] | None = None, allow_disabled: bool = False) -> list[dict[str, Any]] | None:
    out: list[dict[str, Any]] = []
    cursor: str | None = None
    seen: set[str] = set()
    for _ in range(PAGE_GUARD):
        query: dict[str, Any] = {"limit": PAGE_LIMIT, **dict(params or {})}
        if cursor:
            query["cursor"] = cursor
        resp = silo_request(adapter, "GET", path, params=query)
        if resp.status_code == 409 and allow_disabled and problem_of(resp) in ("capability_disabled", "capability_not_configured"):
            _dbg(feature, "capability_unavailable", path=path, reason=problem_of(resp))
            return None
        if resp.status_code != 200:
            _warn(feature, "read_failed", path=path, status=resp.status_code, problem=problem_of(resp) or None)
            raise SiloSyncError(f"silo_{feature}_http_{resp.status_code}")
        data = safe_json(resp)
        if not isinstance(data, Mapping) or not isinstance(data.get("items"), list):
            raise SiloSyncError(f"silo_{feature}_invalid_response")
        out.extend(dict(row) for row in data["items"] if isinstance(row, Mapping))
        raw_page = data.get("page")
        page: Mapping[str, Any] = raw_page if isinstance(raw_page, Mapping) else {}
        if not page.get("has_more"):
            return out
        nxt = str(page.get("next_cursor") or "")
        if not nxt or nxt in seen:
            raise SiloSyncError(f"silo_{feature}_invalid_continuation")
        seen.add(nxt)
        cursor = nxt
    raise SiloSyncError(f"silo_{feature}_page_guard_tripped")


def _anchor_ids(provider: str, value: str) -> dict[str, str]:
    if provider == "imdb":
        return {"imdb": value} if value.startswith("tt") else {}
    return {provider: value} if value.isdigit() else {}


def ids_from_content_id(content_id: str) -> tuple[str, dict[str, str]]:
    cid = str(content_id or "").strip().lower()
    m = _MOVIE_RE.match(cid)
    if m:
        return "movie", _anchor_ids(m.group(1), m.group(2))
    m = _SERIES_RE.match(cid)
    if m:
        return "show", _anchor_ids(m.group(1), m.group(2))
    return "", {}


def series_id_of(content_id: str) -> str:
    m = _EPISODE_RE.match(str(content_id or "").strip().lower())
    return f"series-{m.group(1)}-{m.group(2)}" if m else ""


class Library:
    def __init__(self) -> None:
        self.movies: dict[str, dict[str, Any]] = {}
        self.series: dict[str, dict[str, Any]] = {}
        self.episodes: dict[str, dict[str, Any]] = {}
        self.episode_by_coord: dict[tuple[str, int, int], str] = {}
        self.movie_by_id: dict[str, str] = {}
        self.series_by_id: dict[str, str] = {}
        self.detail_passes: set[str] = set()
        self.scoped = False
        self.library_of: dict[str, str] | None = None
        self.library_ids: set[str] = set()
        self.path_index: Any = None
        self.title_index: dict[tuple[str, str], list[str]] | None = None
        self.views: dict[frozenset[str], "Library"] = {}

    def subset(self, keep: set[str]) -> "Library":
        out = Library()
        out.scoped = True
        for cid, rec in self.movies.items():
            if cid in keep:
                out.movies[cid] = rec
                for k, v in rec["ids"].items():
                    out.movie_by_id.setdefault(f"{k}:{v}", cid)
        for cid, rec in self.series.items():
            if cid in keep:
                out.series[cid] = rec
                for k, v in rec["ids"].items():
                    out.series_by_id.setdefault(f"{k}:{v}", cid)
        for cid, rec in self.episodes.items():
            if cid in keep:
                out.episodes[cid] = rec
                out.episode_by_coord.setdefault((rec["series_id"], rec["season"], rec["episode"]), cid)
        return out

    def add_movie(self, row: Mapping[str, Any]) -> None:
        cid = str(row.get("content_id") or "")
        _kind, ids = ids_from_content_id(cid)
        rec = {"content_id": cid, "ids": dict(ids), "title": str(row.get("title") or "").strip(), "year": as_int(row.get("year")), "runtime": as_int(row.get("runtime")),
               "added_at": iso_z(row.get("added_at"))}
        self.movies[cid] = rec
        for k, v in ids.items():
            self.movie_by_id.setdefault(f"{k}:{v}", cid)

    def add_series(self, row: Mapping[str, Any]) -> None:
        cid = str(row.get("content_id") or "")
        _kind, ids = ids_from_content_id(cid)
        rec = {"content_id": cid, "ids": dict(ids), "title": str(row.get("title") or "").strip(), "year": as_int(row.get("year")),
               "added_at": iso_z(row.get("added_at"))}
        self.series[cid] = rec
        for k, v in ids.items():
            self.series_by_id.setdefault(f"{k}:{v}", cid)

    def add_episode(self, row: Mapping[str, Any]) -> None:
        cid = str(row.get("content_id") or "")
        sid = str(row.get("series_id") or "") or series_id_of(cid)
        s, e = as_int(row.get("season_number")), as_int(row.get("episode_number"))
        if not cid or not sid or s is None or e is None:
            return
        self.episodes[cid] = {
            "content_id": cid,
            "series_id": sid,
            "season": s,
            "episode": e,
            "title": str(row.get("title") or "").strip(),
            "series_title": str(row.get("series_title") or "").strip(),
            "runtime": as_int(row.get("runtime")),
            "added_at": iso_z(row.get("added_at")),
        }
        self.episode_by_coord.setdefault((sid, s, e), cid)

    def merge_series_ids(self, cid: str, ids: Mapping[str, Any]) -> None:
        rec = self.series.get(cid)
        if rec is None:
            return
        for k in ("tvdb", "tmdb", "imdb"):
            v = str(ids.get(k) or "").strip()
            if v and k not in rec["ids"]:
                rec["ids"][k] = v
            if v:
                self.series_by_id.setdefault(f"{k}:{v}", cid)

    def merge_movie_ids(self, cid: str, ids: Mapping[str, Any]) -> None:
        rec = self.movies.get(cid)
        if rec is None:
            return
        for k in ("tmdb", "imdb", "tvdb"):
            v = str(ids.get(k) or "").strip()
            if v and k not in rec["ids"]:
                rec["ids"][k] = v
            if v:
                self.movie_by_id.setdefault(f"{k}:{v}", cid)


def selected_libraries(adapter: Any, feature: str) -> set[str]:
    block = cfg_section(adapter).get(feature)
    raw = block.get("libraries") if isinstance(block, Mapping) else None
    return {str(v).strip() for v in raw if str(v).strip()} if isinstance(raw, list) else set()


def list_libraries(adapter: Any) -> list[dict[str, str]]:
    resp = silo_request(adapter, "GET", "/user/libraries")
    if resp.status_code != 200:
        raise SiloSyncError(f"silo_libraries_http_{resp.status_code}")
    data = safe_json(resp)
    rows = data.get("items") if isinstance(data, Mapping) else None
    if not isinstance(rows, list):
        raise SiloSyncError("silo_libraries_invalid_response")
    if has_more_pages(data):
        raise SiloSyncError("silo_libraries_unexpected_pages")
    out: list[dict[str, str]] = []
    for row in rows:
        lid = str(row.get("id") or "").strip() if isinstance(row, Mapping) else ""
        if lid:
            out.append({"key": lid, "title": str(row.get("name") or lid).strip(), "type": str(row.get("type") or "").strip()})
    return out


def _membership(adapter: Any, lib: Library) -> dict[str, str]:
    if lib.library_of is None:
        found: dict[str, str] = {}
        rows = list_libraries(adapter)
        lib.library_ids = {row["key"] for row in rows}
        for row in rows:
            for typ in ("movie", "series", "episode"):
                params = {"type": typ, "skip_total": "true", "library_id": row["key"]}
                for item in pages(adapter, "/catalog", feature="library", params=params) or []:
                    cid = str(item.get("content_id") or "")
                    if cid:
                        found.setdefault(cid, row["key"])
        lib.library_of = found
        _info("library", "membership_done", libraries=len(set(found.values())), items=len(found))
    return lib.library_of


def library(adapter: Any, *, refresh: bool = False, feature: str | None = None, libraries: set[str] | None = None) -> Library:
    full = _full_library(adapter, refresh=refresh)
    allowed = frozenset(libraries or (selected_libraries(adapter, feature) if feature else set()))
    if not allowed:
        return full
    view = full.views.get(allowed)
    if view is None:
        membership = _membership(adapter, full)
        missing = sorted(allowed - full.library_ids)
        if missing:
            _warn("library", "whitelisted_library_missing", scope_feature=feature, missing=missing)
            raise SiloSyncError("silo_whitelisted_library_missing")
        view = full.subset({cid for cid, lid in membership.items() if lid in allowed})
        full.views[allowed] = view
    return view


def _full_library(adapter: Any, *, refresh: bool = False) -> Library:
    key = f"{server_scope(adapter)}|{account_scope(adapter)}"
    now = time.monotonic()
    with _SHARED_LOCK:
        hit = _LIBRARIES.get(key)
    if hit and not refresh and now - hit[0] < LIBRARY_TTL_SEC:
        return hit[1]
    lib = Library()
    for row in pages(adapter, "/catalog", feature="library", params={"type": "movie", "skip_total": "true"}) or []:
        lib.add_movie(row)
    for row in pages(adapter, "/catalog", feature="library", params={"type": "series", "skip_total": "true"}) or []:
        lib.add_series(row)
    for row in pages(adapter, "/catalog", feature="library", params={"type": "episode", "skip_total": "true"}) or []:
        lib.add_episode(row)
    _apply_cached_details(adapter, lib)
    _info("library", "index_done", movies=len(lib.movies), series=len(lib.series), episodes=len(lib.episodes))
    with _SHARED_LOCK:
        _LIBRARIES[key] = (now, lib)
    return lib


def forget_library(adapter: Any) -> None:
    key = f"{server_scope(adapter)}|{account_scope(adapter)}"
    with _SHARED_LOCK:
        _LIBRARIES.pop(key, None)


def _apply_cached_details(adapter: Any, lib: Library) -> None:
    cache = read_state("details", server_scope(adapter))
    for cid, rec in cache.items():
        ids = rec.get("ids") if isinstance(rec, Mapping) else None
        if not isinstance(ids, Mapping):
            continue
        if cid in lib.series:
            lib.merge_series_ids(cid, ids)
        elif cid in lib.movies:
            lib.merge_movie_ids(cid, ids)


def ensure_details(adapter: Any, lib: Library, content_ids: list[str], *, feature: str) -> int:
    scope = server_scope(adapter)
    cache = read_state("details", scope)
    now = int(time.time())
    fetched = 0
    for cid in content_ids:
        rec = cache.get(cid)
        if isinstance(rec, Mapping) and now - int(rec.get("at") or 0) < DETAIL_TTL_SEC:
            continue
        resp = silo_request(adapter, "GET", f"/catalog/items/{quote(cid, safe='')}")
        if resp.status_code != 200:
            _dbg(feature, "detail_lookup_failed", content_id=cid, status=resp.status_code)
            continue
        data = safe_json(resp)
        if not isinstance(data, Mapping):
            continue
        ids = {k: str(data.get(f"{k}_id") or "").strip() for k in ("tmdb", "imdb", "tvdb")}
        ids = {k: v for k, v in ids.items() if v}
        cache[cid] = {"ids": ids, "at": now}
        fetched += 1
    if fetched:
        write_state("details", scope, cache)
        _dbg(feature, "detail_lookups", fetched=fetched)
    _apply_cached_details(adapter, lib)
    return fetched


def item_detail(adapter: Any, content_id: str, *, feature: str) -> dict[str, Any]:
    resp = silo_request(adapter, "GET", f"/catalog/items/{quote(content_id, safe='')}")
    if resp.status_code != 200:
        _dbg(feature, "detail_lookup_failed", content_id=content_id, status=resp.status_code)
        return {}
    data = safe_json(resp)
    return dict(data) if isinstance(data, Mapping) else {}


def item_ids(adapter: Any, content_id: str, *, feature: str) -> dict[str, str]:
    scope = server_scope(adapter)
    cache = read_state("details", scope)
    rec = cache.get(content_id)
    if isinstance(rec, Mapping) and isinstance(rec.get("ids"), Mapping) and int(time.time()) - int(rec.get("at") or 0) < DETAIL_TTL_SEC:
        return dict(rec["ids"])
    data = item_detail(adapter, content_id, feature=feature)
    if not data:
        return {}
    ids = {k: str(data.get(f"{k}_id") or "").strip() for k in ("tmdb", "imdb", "tvdb")}
    ids = {k: v for k, v in ids.items() if v}
    cache[content_id] = {"ids": ids, "at": int(time.time())}
    write_state("details", scope, cache)
    return ids


def ids_of_value(ids: Mapping[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for k in ("tmdb", "imdb", "tvdb"):
        v = str((ids or {}).get(k) or "").strip()
        if not v:
            continue
        if k == "imdb" and not v.startswith("tt"):
            continue
        if k != "imdb" and not v.isdigit():
            continue
        out[k] = v
    return out


def _norm_type(item: Mapping[str, Any]) -> str:
    raw = str(item.get("type") or "").strip().lower()
    if raw in ("movie", "movies", "film"):
        return "movie"
    if raw in ("episode", "episodes"):
        return "episode"
    if raw in ("season", "seasons"):
        return "season"
    return "show"


def _lookup(table: Mapping[str, str], ids: Mapping[str, str], order: tuple[str, ...]) -> str:
    for k in order:
        v = ids.get(k)
        if v and f"{k}:{v}" in table:
            return table[f"{k}:{v}"]
    return ""


def resolve_series(adapter: Any, lib: Library, show_ids: Mapping[str, Any] | None, *, feature: str) -> str:
    ids = ids_of_value(show_ids)
    if not ids:
        return ""
    cid = _lookup(lib.series_by_id, ids, ("tvdb", "tmdb", "imdb"))
    if cid:
        return cid
    if "series" in lib.detail_passes or not ({"tmdb", "imdb"} & set(ids)):
        return ""
    lib.detail_passes.add("series")
    pending = [c for c, rec in lib.series.items() if not ({"tmdb", "imdb"} & set(rec["ids"]))]
    if pending:
        ensure_details(adapter, lib, pending, feature=feature)
    return _lookup(lib.series_by_id, ids, ("tvdb", "tmdb", "imdb"))


def resolve_movie(adapter: Any, lib: Library, ids_in: Mapping[str, Any] | None, *, feature: str) -> str:
    ids = ids_of_value(ids_in)
    if not ids:
        return ""
    cid = _lookup(lib.movie_by_id, ids, ("tmdb", "imdb", "tvdb"))
    if cid:
        return cid
    if "movies" in lib.detail_passes:
        return ""
    lib.detail_passes.add("movies")
    pending = [c for c in lib.movies if c.startswith(("movie-imdb-", "movie-tvdb-"))]
    if pending:
        ensure_details(adapter, lib, pending, feature=feature)
    return _lookup(lib.movie_by_id, ids, ("tmdb", "imdb", "tvdb"))


def _streams(version: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for v in version.get("video_tracks") or []:
        if isinstance(v, Mapping):
            out.append({"Type": "Video", "Width": v.get("width"), "Height": v.get("height"), "IsInterlaced": bool(v.get("interlaced")),
                        "VideoRangeType": "HDR10Plus" if v.get("hdr10_plus") else v.get("video_range_type"), "ColorTransfer": v.get("color_transfer")})
    for a in version.get("audio_tracks") or []:
        if isinstance(a, Mapping):
            out.append({"Type": "Audio", "Codec": a.get("codec"), "Profile": a.get("profile"), "Title": a.get("title"), "Language": a.get("language"),
                        "Channels": a.get("channels"), "ChannelLayout": a.get("layout"), "BitRate": a.get("bitrate"), "IsDefault": a.get("default") is True})
    return out


def media_info_of(detail: Mapping[str, Any]) -> dict[str, Any]:
    versions = [v for v in detail.get("versions") or [] if isinstance(v, Mapping)]
    if not versions:
        return {}
    best = max(versions, key=lambda v: (max((int(t.get("width") or 0) for t in v.get("video_tracks") or [] if isinstance(t, Mapping)), default=0),
                                         int(v.get("bitrate") or 0)))
    return stream_media_info({"MediaStreams": _streams(best)})


def paths_of(detail: Mapping[str, Any]) -> list[str]:
    found = {str(v.get("file_path") or "").strip() for v in detail.get("versions") or [] if isinstance(v, Mapping)}
    return sorted(p for p in found if p)


def file_details(adapter: Any, rows: Iterable[tuple[str, str | None]], *, feature: str, strict: bool) -> dict[str, dict[str, Any]]:
    scope = server_scope(adapter)
    cache = read_state(FILES_STATE, scope)
    now = int(time.time())
    fetched = failed = 0
    out: dict[str, dict[str, Any]] = {}
    for cid, stamp in rows:
        rec = cache.get(cid)
        if isinstance(rec, Mapping) and rec.get("stamp") == stamp and now - int(rec.get("at") or 0) < DETAIL_TTL_SEC:
            out[cid] = dict(rec)
            continue
        detail = item_detail(adapter, cid, feature=feature)
        if not detail:
            failed += 1
            if isinstance(rec, Mapping):
                out[cid] = dict(rec)
            elif strict:
                if fetched:
                    write_state(FILES_STATE, scope, cache)
                raise SiloSyncError(f"silo_{feature}_file_details_failed")
            continue
        cache[cid] = {"stamp": stamp, "at": now, "media": media_info_of(detail), "paths": paths_of(detail)}
        out[cid] = dict(cache[cid])
        fetched += 1
    if fetched:
        write_state(FILES_STATE, scope, cache)
        _dbg(feature, "file_details_fetched", fetched=fetched)
    if failed:
        _warn(feature, "file_details_lookup_failed", count=failed)
    return out


def _stamp(lib: Library, cid: str) -> str | None:
    rec = lib.movies.get(cid) or lib.episodes.get(cid) or {}
    return rec.get("added_at")


def attach_paths(adapter: Any, lib: Library, rows: Iterable[tuple[str, dict[str, Any]]], *, feature: str) -> None:
    playable = [(cid, item) for cid, item in rows if item.get("type") in ("movie", "episode")]
    if not playable:
        return
    details = file_details(adapter, [(cid, _stamp(lib, cid)) for cid, _item in playable], feature=feature, strict=True)
    for cid, item in playable:
        paths = (details.get(cid) or {}).get("paths") or []
        if paths:
            item["media_paths"] = list(paths)


def path_index(adapter: Any, lib: Library, *, feature: str) -> PathIndex:
    cached = getattr(lib, "path_index", None)
    if cached is not None:
        return cached
    for name, pending in (("series", [c for c, rec in lib.series.items() if not ({"tmdb", "imdb"} & set(rec["ids"]))]),
                          ("movies", [c for c in lib.movies if c.startswith(("movie-imdb-", "movie-tvdb-"))])):
        if name not in lib.detail_passes:
            lib.detail_passes.add(name)
            if pending:
                ensure_details(adapter, lib, pending, feature=feature)
    cids = list(lib.movies) + list(lib.episodes)
    details = file_details(adapter, [(cid, _stamp(lib, cid)) for cid in cids], feature=feature, strict=True)
    rows: list[tuple[str, Mapping[str, Any]]] = []
    for cid in cids:
        item = minimal_for(lib, cid)
        paths = (details.get(cid) or {}).get("paths") or []
        if item and paths:
            rows.append((cid, {**item, "media_paths": list(paths)}))
    index = PathIndex(rows)
    lib.path_index = index
    _info(feature, "path_index_done", items=len(rows))
    return index


def resolve_by_path(adapter: Any, lib: Library, item: Mapping[str, Any], *, feature: str) -> tuple[str, str]:
    if not getattr(adapter, "path_matching", False) or not path_keys(item):
        return "", "path_not_found"
    cid, reason = path_index(adapter, lib, feature=feature).resolve(item)
    if reason != "path_not_found":
        (_info if cid else _warn)(feature, reason, resolution_method="file_path", item_id=cid, title=item.get("title"))
    return cid or "", reason


def _by_path(adapter: Any, lib: Library, item: Mapping[str, Any], feature: str) -> str:
    if not getattr(adapter, "path_matching", False):
        return ""
    return resolve_by_path(adapter, lib, item, feature=feature)[0]


def strict_ids(adapter: Any) -> bool:
    return coerce_bool(cfg_section(adapter).get("strict_id_matching", True), True)


def _norm_title(value: Any) -> str:
    return " ".join(str(value or "").casefold().split())


def _title_index(lib: Library) -> dict[tuple[str, str], list[str]]:
    if lib.title_index is None:
        index: dict[tuple[str, str], list[str]] = {}
        for kind, table in (("movie", lib.movies), ("show", lib.series)):
            for cid, rec in table.items():
                title = _norm_title(rec.get("title"))
                if title:
                    index.setdefault((kind, title), []).append(cid)
        lib.title_index = index
    return lib.title_index


def _by_title(adapter: Any, lib: Library, kind: str, title: Any, year: Any, ids: Any, *, feature: str) -> str:
    name = _norm_title(title)
    if not name or strict_ids(adapter):
        return ""
    want = as_int(year)
    table = lib.movies if kind == "movie" else lib.series
    found = []
    for cid in _title_index(lib).get((kind, name), []):
        have = as_int(table[cid].get("year"))
        if want and not (have and abs(have - want) <= 1):
            continue
        if ids_conflict({"ids": ids_of_value(ids if isinstance(ids, Mapping) else {})}, {"ids": table[cid]["ids"]}):
            continue
        found.append(cid)
    if len(found) == 1:
        _dbg(feature, "resolve_hit", kind=kind, method="title", title=title, year=want, item_id=found[0])
        return found[0]
    if found:
        _dbg(feature, "resolve_miss", kind=kind, method="title_ambiguous", title=title, year=want, candidates=len(found))
    return ""


def resolve_item(adapter: Any, lib: Library, item: Mapping[str, Any], *, feature: str) -> tuple[str, str]:
    typ = _norm_type(item)
    ids = item.get("ids") if isinstance(item.get("ids"), Mapping) else {}
    if typ == "movie":
        cid = resolve_movie(adapter, lib, ids, feature=feature)
        return cid or _by_path(adapter, lib, item, feature) or _by_title(adapter, lib, "movie", item.get("title"), item.get("year"), ids, feature=feature), "movie"
    if typ == "episode":
        show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else {}
        s, e = as_int(item.get("season")), as_int(item.get("episode"))
        sid = resolve_series(adapter, lib, show_ids, feature=feature)
        cid = lib.episode_by_coord.get((sid, s, e), "") if sid and s is not None and e is not None else ""
        if not cid:
            cid = _by_path(adapter, lib, item, feature)
        if not cid and s is not None and e is not None:
            show = _by_title(adapter, lib, "show", item.get("series_title") or item.get("show_title"), item.get("show_year"), show_ids, feature=feature)
            cid = lib.episode_by_coord.get((show, s, e), "") if show else ""
        return cid, "episode"
    if typ == "season":
        return "", "season"
    return resolve_series(adapter, lib, ids, feature=feature) or _by_title(adapter, lib, "show", item.get("title"), item.get("year"), ids, feature=feature), "show"


def minimal_for(lib: Library, content_id: str) -> dict[str, Any] | None:
    cid = str(content_id or "")
    if cid in lib.movies:
        rec = lib.movies[cid]
        if not rec["ids"]:
            return None
        out: dict[str, Any] = {"type": "movie", "ids": dict(rec["ids"])}
        if rec["title"]:
            out["title"] = rec["title"]
        if rec["year"]:
            out["year"] = rec["year"]
        return out
    if cid in lib.series:
        rec = lib.series[cid]
        if not rec["ids"]:
            return None
        out = {"type": "show", "ids": dict(rec["ids"])}
        if rec["title"]:
            out["title"] = rec["title"]
        if rec["year"]:
            out["year"] = rec["year"]
        return out
    if cid in lib.episodes:
        rec = lib.episodes[cid]
        series = lib.series.get(rec["series_id"])
        show_ids = dict(series["ids"]) if series else ids_from_content_id(rec["series_id"])[1]
        if not show_ids:
            return None
        out = {"type": "episode", "show_ids": show_ids, "ids": {}, "season": rec["season"], "episode": rec["episode"]}
        series_title = rec["series_title"] or (series["title"] if series else "")
        if series_title:
            out["series_title"] = series_title
        if rec["title"]:
            out["title"] = rec["title"]
        return out
    if lib.scoped:
        return None
    kind, ids = ids_from_content_id(cid)
    if kind and ids:
        return {"type": kind, "ids": ids}
    m = _EPISODE_RE.match(cid.lower())
    if m:
        show_ids = _anchor_ids(m.group(1), m.group(2))
        if show_ids:
            return {"type": "episode", "show_ids": show_ids, "ids": {}, "season": int(m.group(3)), "episode": int(m.group(4))}
    return None


def runtime_seconds(lib: Library, content_id: str) -> int:
    rec = lib.movies.get(content_id) or lib.episodes.get(content_id) or {}
    minutes = as_int(rec.get("runtime"))
    return minutes * 60 if minutes and minutes > 0 else 0


def chunks(rows: list[Any], size: int = WRITE_BATCH) -> list[list[Any]]:
    return [rows[i:i + size] for i in range(0, len(rows), max(1, size))]


def unresolved_entry(key: str, status: str, item: Mapping[str, Any] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"key": key, "status": status}
    if item is not None:
        out["item"] = id_minimal(item)
    return out
