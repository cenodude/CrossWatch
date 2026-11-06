# /providers/sync/tofa/_common.py
# Shared helpers for Tofa sync modules
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import quote

from cw_platform.run_control import raise_if_cancelled

import requests

from cw_platform.collection_media import stream_media_info
from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.auth._auth_TOFA import api_url, normalize_server_url, provider_block, request_with_auth

from .._log import log as cw_log
from .._mod_common import SimpleRateLimiter

STATE_DIR = Path("/config/.cw_state")
try:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass

PAGE_LIMIT = 1000
WRITE_BATCH = 500
WATCHLIST_PAGE = 200
DEFAULT_TIMEOUT = 20.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_GET_PER_SEC = 10.0
DEFAULT_POST_PER_SEC = 5.0
LIBRARY_TTL_SEC = 600.0
DETAIL_TTL_SEC = 30 * 86400
FULL_RESYNC_SEC = 24 * 3600
FEED_PAGE = 500
DETAILS_STATE = "details"
MIN_POSITION_MS = 60000
REQUIRED_CAPABILITIES = ("sync.user_state", "watch.bulk_status")
WATCHED_RULE_CHOICES = (70, 75, 80, 85, 90, 95)

_LIMITERS: dict[str, SimpleRateLimiter] = {}
_LIBRARIES: dict[str, tuple[float, "Library"]] = {}
_SHARED_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()


class TofaSyncError(RuntimeError):
    pass


def _dbg(feature: str, msg: str, **fields: Any) -> None:
    cw_log("TOFA", feature, "debug", msg, **fields)


def _info(feature: str, msg: str, **fields: Any) -> None:
    cw_log("TOFA", feature, "info", msg, **fields)


def _warn(feature: str, msg: str, **fields: Any) -> None:
    cw_log("TOFA", feature, "warn", msg, **fields)


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


def cfg_section(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", None) or {}
    block = provider_block(cfg, getattr(adapter, "instance_id", None)) if isinstance(cfg, Mapping) else {}
    return block if isinstance(block, Mapping) else {}


def server_scope(adapter: Any) -> str:
    raw = normalize_server_url(cfg_section(adapter).get("server_url"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def is_capture_mode() -> bool:
    return str(os.getenv("CW_CAPTURE_MODE") or "").strip().lower() in {"1", "true", "yes", "on"}


def account_scope(adapter: Any) -> str:
    sec = cfg_section(adapter)
    raw = f"{normalize_server_url(sec.get('server_url'))}|{sec.get('user_id') or ''}|{sec.get('profile_id') or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def state_path(name: str, scope: str) -> Path:
    return STATE_DIR / f"tofa_{name}.{scope}.json"


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
    key = f"{server_scope(adapter)}|{get_rps}|{post_rps}"
    with _SHARED_LOCK:
        limiter = _LIMITERS.get(key)
        if limiter is None:
            limiter = SimpleRateLimiter(rates_per_sec={"GET": get_rps, "POST": post_rps})
            _LIMITERS[key] = limiter
        return limiter


def label_tofa(method: str, url: str, kw: Mapping[str, Any]) -> str:
    path = url.split("/api/v1/", 1)[-1].split("?", 1)[0]
    m = method.upper()
    if path.startswith("media/files") or path == "libraries":
        return "library:index"
    if path.startswith("users/me/watchlist"):
        return "watchlist" if m == "GET" else "watchlist:write"
    if path.startswith("users/me/watch-state"):
        return "history"
    if path.startswith("media/watched/batch"):
        return "history:write"
    if path.endswith("/progress"):
        return "progress" if m == "GET" else "progress:write"
    if path.startswith("media/"):
        return "library:detail"
    return path.split("/", 1)[0] or "unknown"


def tofa_request(adapter: Any, method: str, path: str, *, params: Mapping[str, Any] | None = None, json_body: Any = None) -> requests.Response:
    sec = cfg_section(adapter)
    url = api_url(str(sec.get("server_url") or ""), path)
    kwargs: dict[str, Any] = {}
    if params:
        kwargs["params"] = dict(params)
    if json_body is not None:
        kwargs["json"] = json_body
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


def safe_json(resp: requests.Response) -> Any:
    try:
        if not (resp.text or "").strip():
            return None
        return resp.json()
    except Exception:
        return None


def error_of(resp: requests.Response) -> str:
    body = safe_json(resp)
    if isinstance(body, Mapping):
        return str(body.get("code") or body.get("error") or "")
    return ""


def failed_ids_of(resp: requests.Response) -> set[str]:
    body = safe_json(resp)
    raw = body.get("failed_ids") if isinstance(body, Mapping) else None
    return {str(v) for v in raw if str(v or "").strip()} if isinstance(raw, list) else set()


def _raise_for(resp: requests.Response, feature: str, path: str) -> None:
    if resp.status_code == 200:
        return
    _warn(feature, "read_failed", path=path, status=resp.status_code, code=error_of(resp) or None)
    raise TofaSyncError(f"tofa_{feature}_http_{resp.status_code}")


def valid_file_row(row: Mapping[str, Any]) -> bool:
    return bool(str(row.get("media_file_id") or "").strip() and str(row.get("media_id") or "").strip()) and row.get("media_type") in ("movie", "tv")


def valid_state_row(row: Mapping[str, Any]) -> bool:
    return bool(str(row.get("media_file_id") or "").strip()) and isinstance(row.get("completed"), bool)


def valid_change(row: Mapping[str, Any]) -> bool:
    kind = row.get("kind")
    if kind == "watched":
        return valid_state_row(row)
    if kind == "watchlist":
        return isinstance(row.get("on_watchlist"), bool) and bool(as_int(row.get("tmdb_id"))) and row.get("media_type") in ("movie", "tv")
    return bool(kind)


def cursor_pages(adapter: Any, path: str, *, feature: str, params: Mapping[str, Any] | None = None,
                 valid: Callable[[Mapping[str, Any]], bool] | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    after: str | None = None
    seen: set[str] = set()
    while True:
        raise_if_cancelled()
        query: dict[str, Any] = {"limit": PAGE_LIMIT, **dict(params or {})}
        if after:
            query["after"] = after
        resp = tofa_request(adapter, "GET", path, params=query)
        _raise_for(resp, feature, path)
        data = safe_json(resp)
        if not isinstance(data, Mapping) or not isinstance(data.get("items"), list) or not all(isinstance(r, Mapping) for r in data["items"]):
            raise TofaSyncError(f"tofa_{feature}_invalid_response")
        if valid is not None and not all(valid(r) for r in data["items"]):
            raise TofaSyncError(f"tofa_{feature}_invalid_response")
        out.extend(dict(row) for row in data["items"])
        nxt = str(data.get("next_after") or "")
        if not nxt:
            return out
        if nxt in seen or not data["items"]:
            raise TofaSyncError(f"tofa_{feature}_invalid_continuation")
        seen.add(nxt)
        after = nxt


def capabilities(adapter: Any) -> set[str]:
    resp = tofa_request(adapter, "GET", "/system/info")
    _raise_for(resp, "health", "/system/info")
    data = safe_json(resp)
    caps = data.get("capabilities") if isinstance(data, Mapping) else None
    return {str(c) for c in caps} if isinstance(caps, list) else set()


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


def _row_ids(row: Mapping[str, Any]) -> dict[str, str]:
    return ids_of_value({"tmdb": row.get("tmdb_id"), "imdb": row.get("imdb_id"), "tvdb": row.get("tvdb_id")})


class Library:
    def __init__(self) -> None:
        self.movies: dict[str, dict[str, Any]] = {}
        self.shows: dict[str, dict[str, Any]] = {}
        self.episodes: dict[tuple[str, int, int], dict[str, Any]] = {}
        self.movie_by_id: dict[str, str] = {}
        self.show_by_id: dict[str, str] = {}
        self.file_of: dict[str, tuple[str, Any, str]] = {}
        self.library_ids: set[str] = set()
        self.views: dict[frozenset[str], "Library"] = {}
        self.rows_by_library: dict[str, list[dict[str, Any]]] = {}

    def add_row(self, row: Mapping[str, Any]) -> None:
        fid = str(row.get("media_file_id") or "")
        mid = str(row.get("media_id") or "")
        if not fid or not mid:
            return
        ids = _row_ids(row)
        edition = str(row.get("edition") or "")
        title = str(row.get("title") or "").strip()
        if str(row.get("media_type") or "") == "movie":
            rec = self.movies.setdefault(mid, {"media_id": mid, "ids": {}, "title": title, "editions": {}})
            for k, v in ids.items():
                rec["ids"].setdefault(k, v)
                self.movie_by_id.setdefault(f"{k}:{v}", mid)
            rec["editions"].setdefault(edition, []).append(fid)
            self.file_of[fid] = ("movie", mid, edition)
            return
        s, e = as_int(row.get("season_number")), as_int(row.get("episode_number"))
        show = self.shows.setdefault(mid, {"media_id": mid, "ids": {}, "title": title})
        for k, v in ids.items():
            show["ids"].setdefault(k, v)
            self.show_by_id.setdefault(f"{k}:{v}", mid)
        if s is None or e is None:
            return
        coord = (mid, s, e)
        ep = self.episodes.setdefault(coord, {"media_id": mid, "season": s, "episode": e, "episode_id": str(row.get("episode_id") or ""),
                                              "tmdb_episode_id": as_int(row.get("tmdb_episode_id")), "editions": {}})
        ep["editions"].setdefault(edition, []).append(fid)
        self.file_of[fid] = ("episode", coord, edition)


def build_library(rows_by_library: Mapping[str, list[dict[str, Any]]], allowed: Iterable[str] | None = None) -> Library:
    lib = Library()
    keep = set(allowed or rows_by_library.keys())
    lib.library_ids = set(rows_by_library.keys())
    lib.rows_by_library = dict(rows_by_library)
    for lid, rows in rows_by_library.items():
        if lid not in keep:
            continue
        for row in rows:
            lib.add_row(row)
    return lib


def selected_libraries(adapter: Any, feature: str) -> set[str]:
    block = cfg_section(adapter).get(feature)
    raw = block.get("libraries") if isinstance(block, Mapping) else None
    return {str(v).strip() for v in raw if str(v).strip()} if isinstance(raw, list) else set()


def list_libraries(adapter: Any) -> list[dict[str, str]]:
    resp = tofa_request(adapter, "GET", "/libraries")
    if resp.status_code != 200:
        raise TofaSyncError(f"tofa_libraries_http_{resp.status_code}")
    data = safe_json(resp)
    if not isinstance(data, list) or not all(isinstance(r, Mapping) for r in data):
        raise TofaSyncError("tofa_libraries_invalid_response")
    out: list[dict[str, str]] = []
    for row in data:
        lid = str(row.get("id") or "").strip()
        if lid:
            out.append({"key": lid, "title": str(row.get("name") or lid).strip(), "type": str(row.get("media_type") or "").strip()})
    return out


def _full_library(adapter: Any, *, refresh: bool = False) -> Library:
    key = account_scope(adapter)
    now = time.monotonic()
    with _SHARED_LOCK:
        hit = _LIBRARIES.get(key)
    if hit and not refresh and not is_capture_mode() and now - hit[0] < LIBRARY_TTL_SEC:
        return hit[1]
    rows_by_library: dict[str, list[dict[str, Any]]] = {}
    for row in list_libraries(adapter):
        rows_by_library[row["key"]] = cursor_pages(adapter, "/media/files", feature="library", params={"library_id": row["key"]}, valid=valid_file_row)
    lib = build_library(rows_by_library)
    _info("library", "index_done", libraries=len(rows_by_library), movies=len(lib.movies), shows=len(lib.shows),
          episodes=len(lib.episodes), files=len(lib.file_of))
    with _SHARED_LOCK:
        _LIBRARIES[key] = (now, lib)
    return lib


def library(adapter: Any, *, refresh: bool = False, feature: str | None = None, libraries: set[str] | None = None) -> Library:
    full = _full_library(adapter, refresh=refresh)
    allowed = frozenset(libraries or (selected_libraries(adapter, feature) if feature else set()))
    if not allowed:
        return full
    view = full.views.get(allowed)
    if view is None:
        missing = sorted(allowed - full.library_ids)
        if missing:
            _warn("library", "whitelisted_library_missing", scope_feature=feature, missing=missing)
            raise TofaSyncError("tofa_whitelisted_library_missing")
        view = build_library(full.rows_by_library, allowed)
        full.views[allowed] = view
    return view


def forget_library(adapter: Any) -> None:
    with _SHARED_LOCK:
        _LIBRARIES.pop(account_scope(adapter), None)


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


def resolve_item(lib: Library, item: Mapping[str, Any]) -> tuple[Any, str]:
    typ = _norm_type(item)
    ids = ids_of_value(item.get("ids") if isinstance(item.get("ids"), Mapping) else {})
    if typ == "movie":
        return _lookup(lib.movie_by_id, ids, ("tmdb", "imdb", "tvdb")), "movie"
    if typ == "episode":
        show_ids = ids_of_value(item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else {})
        mid = _lookup(lib.show_by_id, show_ids, ("tvdb", "tmdb", "imdb"))
        s, e = as_int(item.get("season")), as_int(item.get("episode"))
        coord = (mid, s, e) if mid and s is not None and e is not None else None
        return (coord if coord in lib.episodes else ""), "episode"
    if typ == "season":
        return "", "season"
    return _lookup(lib.show_by_id, ids, ("tvdb", "tmdb", "imdb")), "show"


def editions_of(lib: Library, kind: str, target: Any) -> dict[str, list[str]]:
    rec = lib.movies.get(target) if kind == "movie" else lib.episodes.get(target) if kind == "episode" else None
    return dict(rec.get("editions") or {}) if isinstance(rec, Mapping) else {}


def default_edition_files(lib: Library, kind: str, target: Any) -> list[str]:
    editions = editions_of(lib, kind, target)
    if not editions:
        return []
    key = "" if "" in editions else sorted(editions)[0]
    return list(editions[key])


def all_files(lib: Library, kind: str, target: Any) -> list[str]:
    return [fid for files in editions_of(lib, kind, target).values() for fid in files]


def minimal_for(lib: Library, kind: str, target: Any) -> dict[str, Any] | None:
    if kind == "movie":
        rec = lib.movies.get(target)
        if not rec or not rec["ids"]:
            return None
        out: dict[str, Any] = {"type": "movie", "ids": dict(rec["ids"])}
        if rec["title"]:
            out["title"] = rec["title"]
        return out
    if kind == "show":
        rec = lib.shows.get(target)
        if not rec or not rec["ids"]:
            return None
        out = {"type": "show", "ids": dict(rec["ids"])}
        if rec["title"]:
            out["title"] = rec["title"]
        return out
    if kind == "episode":
        ep = lib.episodes.get(target)
        show = lib.shows.get(ep["media_id"]) if ep else None
        if not ep or not show or not show["ids"]:
            return None
        out = {"type": "episode", "show_ids": dict(show["ids"]), "ids": {}, "season": ep["season"], "episode": ep["episode"]}
        if show["title"]:
            out["series_title"] = show["title"]
        return out
    return None


def watch_state(adapter: Any, *, completed_only: bool, feature: str) -> dict[str, dict[str, Any]]:
    params = {"completed_only": "true"} if completed_only else {}
    rows = cursor_pages(adapter, "/users/me/watch-state", feature=feature, params=params, valid=valid_state_row)
    return {str(r["media_file_id"]): r for r in rows}


def completed_editions(lib: Library, kind: str, target: Any, state: Mapping[str, Mapping[str, Any]]) -> list[tuple[str, str | None]]:
    out: list[tuple[str, str | None]] = []
    for edition, files in editions_of(lib, kind, target).items():
        rows = [state.get(fid) for fid in files]
        if files and all(r is not None and r.get("completed") for r in rows):
            stamps = [s for s in (iso_z((r or {}).get("completed_at")) for r in rows) if s]
            out.append((edition, max(stamps) if stamps else None))
    return out


def _dimensions(f: Mapping[str, Any]) -> tuple[int | None, int | None]:
    raw = str(f.get("resolution") or "").lower()
    if "x" in raw:
        w, _sep, h = raw.partition("x")
        if as_int(w) and as_int(h):
            return as_int(w), as_int(h)
    return as_int(f.get("width") or f.get("active_width")), as_int(f.get("height") or f.get("active_height"))


def _video_range(f: Mapping[str, Any]) -> str:
    raw = str(f.get("video_range") or "").strip().upper().replace(" ", "")
    if f.get("dolby_vision_profile") is not None or raw.startswith(("DV", "DOVI", "DOLBY")):
        return "DOVI"
    if raw in ("HDR10+", "HDR10PLUS"):
        return "HDR10Plus"
    if raw == "HLG":
        return "HLG"
    return "HDR10" if raw.startswith("HDR") else ""


def file_media_info(f: Mapping[str, Any]) -> dict[str, Any]:
    width, height = _dimensions(f)
    streams: list[dict[str, Any]] = [{"Type": "Video", "Width": width, "Height": height, "IsInterlaced": bool(f.get("interlaced")),
                                      "VideoRangeType": _video_range(f), "ColorTransfer": f.get("color_transfer")}]
    for i, a in enumerate(f.get("audio_tracks") or []):
        if not isinstance(a, Mapping):
            continue
        stream = {"Type": "Audio", "Codec": a.get("codec"), "Profile": a.get("profile"), "Title": a.get("title"), "Language": a.get("language"),
                  "Channels": a.get("channels"), "ChannelLayout": a.get("channel_layout"), "IsDefault": i == 0}
        if a.get("atmos") is True:
            stream["AudioSpatialFormat"] = "DolbyAtmos"
        elif "DTS:X" in str(a.get("profile") or "").upper():
            stream["AudioSpatialFormat"] = "DTSX"
        streams.append(stream)
    return stream_media_info({"MediaStreams": streams, "Video3DFormat": str(f.get("stereo_3d") or "")})


def _detail_files(data: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    files = [f for f in data.get("files") or [] if isinstance(f, Mapping)]
    for season in data.get("seasons") or []:
        for ep in (season or {}).get("episodes") or []:
            files.extend(f for f in (ep or {}).get("files") or [] if isinstance(f, Mapping))
    return files


def _files_by_media(lib: Library) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for fid, (kind, target, _e) in lib.file_of.items():
        out.setdefault(str(target if kind == "movie" else target[0]), []).append(fid)
    return {mid: sorted(fids) for mid, fids in out.items()}


def media_details(adapter: Any, lib: Library, media_ids: Iterable[str], *, feature: str, strict: bool = False) -> dict[str, dict[str, Any]]:
    scope = server_scope(adapter)
    cache = read_state(DETAILS_STATE, scope)
    now = int(time.time())
    out: dict[str, dict[str, Any]] = {}
    fetched = failed = 0
    by_media = _files_by_media(lib)
    for mid in dict.fromkeys(str(m) for m in media_ids if m):
        files = by_media.get(mid, [])
        rec = cache.get(mid)
        if isinstance(rec, Mapping) and rec.get("files") == files and now - int(rec.get("at") or 0) < DETAIL_TTL_SEC:
            out[mid] = dict(rec)
            continue
        resp = tofa_request(adapter, "GET", f"/media/{quote(mid, safe='')}")
        data = safe_json(resp) if resp.status_code == 200 else None
        if not isinstance(data, Mapping):
            failed += 1
            _dbg(feature, "detail_lookup_failed", media_id=mid, status=resp.status_code)
            if strict:
                if fetched:
                    write_state(DETAILS_STATE, scope, cache)
                raise TofaSyncError(f"tofa_{feature}_detail_failed")
            if isinstance(rec, Mapping):
                out[mid] = dict(rec)
            continue
        per_file: dict[str, dict[str, Any]] = {}
        for f in _detail_files(data):
            fid = str(f.get("id") or "")
            if not fid:
                continue
            width, _height = _dimensions(f)
            per_file[fid] = {"duration_ms": as_int(f.get("duration_ms")), "created_at": iso_z(f.get("created_at")),
                             "width": width or 0, "bitrate": as_int(f.get("bitrate")) or 0, "media": file_media_info(f)}
        cache[mid] = {"at": now, "files": files, "added_at": iso_z(data.get("added_at")), "file": per_file}
        out[mid] = dict(cache[mid])
        fetched += 1
    if fetched:
        write_state(DETAILS_STATE, scope, cache)
        _dbg(feature, "detail_lookups", fetched=fetched)
    if failed:
        _warn(feature, "detail_lookup_failed", count=failed)
    return out


def media_id_of(lib: Library, kind: str, target: Any) -> str:
    if kind == "movie":
        return str(target)
    if kind == "episode":
        return str((lib.episodes.get(target) or {}).get("media_id") or "")
    return ""


def durations(adapter: Any, lib: Library, targets: Iterable[tuple[str, Any]], *, feature: str) -> dict[str, int]:
    targets = list(targets)
    details = media_details(adapter, lib, [media_id_of(lib, kind, target) for kind, target in targets], feature=feature)
    out: dict[str, int] = {}
    for kind, target in targets:
        files = (details.get(media_id_of(lib, kind, target)) or {}).get("file") or {}
        for fid in all_files(lib, kind, target):
            ms = as_int((files.get(fid) or {}).get("duration_ms"))
            if ms and ms > 0:
                out[fid] = ms
    return out


def feed_changes(adapter: Any, cursor: str, *, feature: str) -> tuple[list[dict[str, Any]], str, bool]:
    rows: list[dict[str, Any]] = []
    seen = {cursor}
    while True:
        raise_if_cancelled()
        resp = tofa_request(adapter, "GET", "/users/me/state-changes", params={"cursor": cursor, "limit": FEED_PAGE})
        _raise_for(resp, feature, "/users/me/state-changes")
        data = safe_json(resp)
        if not isinstance(data, Mapping) or not isinstance(data.get("changes"), list) or not str(data.get("next_cursor") or ""):
            raise TofaSyncError(f"tofa_{feature}_invalid_feed")
        if data.get("requires_full_sync"):
            return [], "", True
        if not all(isinstance(r, Mapping) and valid_change(r) for r in data["changes"]):
            raise TofaSyncError(f"tofa_{feature}_invalid_feed")
        rows.extend(dict(r) for r in data["changes"])
        nxt = str(data["next_cursor"])
        if not data.get("has_more"):
            return rows, nxt, False
        if nxt in seen or not data["changes"]:
            raise TofaSyncError(f"tofa_{feature}_invalid_continuation")
        seen.add(nxt)
        cursor = nxt


def _snapshot(adapter: Any, name: str, *, feature: str, kind: str, full: Callable[[], tuple[dict[str, Any], str]],
              apply: Callable[[dict[str, Any], Mapping[str, Any]], None]) -> dict[str, Any]:
    if is_capture_mode():
        rows, _head = full()
        _info(feature, "full_read", rows=len(rows), capture=True)
        return rows
    scope = account_scope(adapter)
    state = read_state(name, scope)
    now = int(time.time())
    rows = state.get("rows")
    cursor = str(state.get("cursor") or "")
    if cursor and isinstance(rows, dict) and now - int(state.get("full_at") or 0) < FULL_RESYNC_SEC:
        try:
            changes, nxt, reset = feed_changes(adapter, cursor, feature=feature)
        except (TofaSyncError, requests.RequestException) as exc:
            _warn(feature, "feed_read_failed_full_read", error=str(exc) or type(exc).__name__)
            changes, nxt, reset = [], "", True
        if not reset:
            applied = 0
            for change in changes:
                if change.get("kind") == kind:
                    apply(rows, change)
                    applied += 1
            write_state(name, scope, {"cursor": nxt, "rows": rows, "full_at": int(state.get("full_at") or now)})
            _info(feature, "feed_applied", changes=len(changes), applied=applied, rows=len(rows))
            return rows
        _info(feature, "feed_full_read", reason="requires_full_sync")
    rows, head = full()
    write_state(name, scope, {"cursor": head, "rows": rows, "full_at": now})
    _info(feature, "full_read", rows=len(rows))
    return rows


def _full_completed(adapter: Any, feature: str) -> tuple[dict[str, Any], str]:
    out: dict[str, Any] = {}
    head = ""
    after: str | None = None
    seen: set[str] = set()
    while True:
        raise_if_cancelled()
        query: dict[str, Any] = {"limit": PAGE_LIMIT, "completed_only": "true"}
        if after:
            query["after"] = after
        resp = tofa_request(adapter, "GET", "/users/me/watch-state", params=query)
        _raise_for(resp, feature, "/users/me/watch-state")
        data = safe_json(resp)
        if not isinstance(data, Mapping) or not isinstance(data.get("items"), list) or not all(isinstance(r, Mapping) for r in data["items"]):
            raise TofaSyncError(f"tofa_{feature}_invalid_response")
        head = head or str(data.get("changes_cursor") or "")
        if not all(valid_state_row(r) for r in data["items"]):
            raise TofaSyncError(f"tofa_{feature}_invalid_response")
        for row in data["items"]:
            if row["completed"]:
                out[str(row["media_file_id"])] = {"completed": True, "completed_at": iso_z(row.get("completed_at"))}
        nxt = str(data.get("next_after") or "")
        if not nxt:
            if not head:
                raise TofaSyncError(f"tofa_{feature}_missing_cursor")
            return out, head
        if nxt in seen or not data["items"]:
            raise TofaSyncError(f"tofa_{feature}_invalid_continuation")
        seen.add(nxt)
        after = nxt


def _apply_watched(rows: dict[str, Any], change: Mapping[str, Any]) -> None:
    fid = str(change.get("media_file_id") or "")
    if not fid:
        return
    if change.get("completed") is True:
        rows[fid] = {"completed": True, "completed_at": iso_z(change.get("completed_at"))}
    elif change.get("completed") is False:
        rows.pop(fid, None)


def completed_state(adapter: Any, *, feature: str) -> dict[str, Any]:
    return _snapshot(adapter, "feed_watched", feature=feature, kind="watched",
                     full=lambda: _full_completed(adapter, feature), apply=_apply_watched)


def watchlist_key(media_type: Any, tmdb: Any) -> str:
    return f"{'movie' if str(media_type or '').lower() == 'movie' else 'tv'}:{as_int(tmdb) or ''}"


def _apply_watchlist(rows: dict[str, Any], change: Mapping[str, Any]) -> None:
    tmdb = as_int(change.get("tmdb_id"))
    if not tmdb:
        return
    key = watchlist_key(change.get("media_type"), tmdb)
    if not change.get("on_watchlist"):
        rows.pop(key, None)
        return
    row = dict(rows.get(key) or {})
    row.update({"media_type": "movie" if key.startswith("movie:") else "tv", "tmdb_id": tmdb})
    for field in ("added_at", "media_id"):
        if change.get(field):
            row[field] = change[field]
    rows[key] = row


def watchlist_state(adapter: Any, *, feature: str, full: Callable[[], tuple[dict[str, Any], str]]) -> dict[str, Any]:
    return _snapshot(adapter, "feed_watchlist", feature=feature, kind="watchlist", full=full, apply=_apply_watchlist)


def feed_head(adapter: Any, *, feature: str) -> str:
    resp = tofa_request(adapter, "GET", "/users/me/watch-state", params={"completed_only": "true", "limit": 1})
    _raise_for(resp, feature, "/users/me/watch-state")
    data = safe_json(resp)
    head = str(data.get("changes_cursor") or "") if isinstance(data, Mapping) else ""
    if not head:
        raise TofaSyncError(f"tofa_{feature}_missing_cursor")
    return head


def chunks(rows: list[Any], size: int = WRITE_BATCH) -> list[list[Any]]:
    return [rows[i:i + size] for i in range(0, len(rows), max(1, size))]


def unresolved_entry(key: str, status: str, item: Mapping[str, Any] | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {"key": key, "status": status}
    if item is not None:
        out["item"] = id_minimal(item)
    return out


def _preferences(adapter: Any) -> dict[str, Any]:
    resp = tofa_request(adapter, "GET", "/users/me")
    _raise_for(resp, "watched_rule", "/users/me")
    data = safe_json(resp)
    prefs = data.get("preferences") if isinstance(data, Mapping) else None
    if not isinstance(prefs, Mapping):
        raise TofaSyncError("tofa_watched_rule_invalid_response")
    return dict(prefs)


def watched_rule(adapter: Any) -> dict[str, Any]:
    resp = tofa_request(adapter, "GET", "/system/info")
    _raise_for(resp, "watched_rule", "/system/info")
    info = safe_json(resp)
    raw_default = info.get("watched_rule") if isinstance(info, Mapping) else None
    default: Mapping[str, Any] = raw_default if isinstance(raw_default, Mapping) else {}
    playback = _preferences(adapter).get("playback")
    raw_own = playback.get("watched_rule") if isinstance(playback, Mapping) else None
    own: Mapping[str, Any] = raw_own if isinstance(raw_own, Mapping) else {}
    custom = bool(own.get("custom"))
    rule = own if custom else default
    return {
        "percent": as_int(rule.get("percent")) or as_int(default.get("percent")) or 90,
        "credits": bool(rule.get("credits", default.get("credits", True))),
        "earlier_episodes": bool(rule.get("earlier_episodes", default.get("earlier_episodes", True))),
        "custom": custom,
        "default_percent": as_int(default.get("percent")) or 90,
        "choices": list(WATCHED_RULE_CHOICES),
    }


def set_watched_rule(adapter: Any, percent: Any) -> dict[str, Any]:
    value = as_int(percent)
    if value not in WATCHED_RULE_CHOICES:
        raise TofaSyncError("tofa_watched_rule_invalid_percent")
    current = watched_rule(adapter)
    playback = dict(_preferences(adapter).get("playback") or {})
    playback["watched_rule"] = {"custom": True, "percent": value, "credits": current["credits"], "earlier_episodes": current["earlier_episodes"]}
    resp = tofa_request(adapter, "PUT", "/users/me/preferences", json_body={"preferences": {"playback": playback}})
    _raise_for(resp, "watched_rule", "/users/me/preferences")
    after = watched_rule(adapter)
    if after["percent"] != value or not after["custom"]:
        raise TofaSyncError("tofa_watched_rule_not_saved")
    _info("watched_rule", "watched_rule_set", percent=value)
    return after
