# /providers/sync/plex/_history.py
# Plex Module for history synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from pathlib import Path
from hashlib import sha256
from threading import local
from xml.etree import ElementTree as ET

from cw_platform.log_context import log_run_id

from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from, ids_from_guid

from ._common import (
    _as_base_url,
    _pair_scope,
    _fb_cache_flush,
    _xml_to_container,
    active_pms_token,
    as_epoch as _as_epoch,
    episode_rating_key_from_show,
    extract_show_ids,
    force_episode_title as _force_episode_title,
    has_external_ids,
    home_scope_enter,
    home_scope_exit,
    iso_from_epoch as _iso,
    item_guid_candidates,
    minimal_from_history_row,
    normalize as plex_normalize,
    normalize_discover_row,
    object_type,
    plex_cfg_get,
    plex_feature_library_ids,
    plex_headers,
    read_json,
    plex_worker_count,
    resolve_obj_by_guids,
    section_allowed,
    native_item_matches,
    state_file,
    write_json,
    emit,
    make_logger,
)


def _event_key(item: Mapping[str, Any]) -> str:
    try:
        base = canonical_key(id_minimal(item)) or canonical_key(item) or ""
    except Exception:
        base = ""
    ts = _as_epoch(item.get("watched_at"))
    return f"{base}@{ts}" if (base and ts) else (base or "")


def _shadow_path() -> Path:
    return state_file("plex_history.shadow.json")

def _wm_key(acct_id: int, uname: str) -> str:
    if acct_id:
        return f"acct:{acct_id}"
    if uname:
        return f"user:{uname.lower()}"
    return "default"

_dbg, _info, _warn, _error, _log = make_logger("history")

# A sync thread owns its indexes. Persisted GUID snapshots are no longer read:
# a new run must see library changes instead of loading an indefinitely old file.
_INDEX_CACHE = local()
_GUID_INDEX_TTL_SEC = 300
_ALLOWED_HISTORY_TYPES = frozenset({"movie", "episode"})


def _allowed_history_type(row: Any) -> bool:
    return str(getattr(row, "type", "") or "").strip().lower() in _ALLOWED_HISTORY_TYPES


def _index_cache_context(adapter: Any, allow: set[str], feature: str) -> tuple[tuple[Any, ...], bool]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    config = getattr(adapter, "config", {}) or {}
    pair_scope = (config.get("_cw_pair_scope") if isinstance(config, Mapping) else None) or _pair_scope()
    pair_scope = str(pair_scope or "").strip()
    run_id = log_run_id.get()
    base = _as_base_url(srv) or ""
    token = str(active_pms_token(adapter) or "")
    identity = sha256(token.encode()).digest()
    share = bool(run_id and base and token and pair_scope and pair_scope.lower() not in {"unscoped", "default", "none"})
    owner = None
    if not share:
        owner = getattr(adapter, "_plex_index_owner", None)
        if owner is None:
            owner = object()
            setattr(adapter, "_plex_index_owner", owner)
    key = (run_id, pair_scope, feature, base, str(getattr(srv, "machineIdentifier", "") or ""),
           _user_scope_key(adapter), identity, tuple(sorted(str(x) for x in allow)), owner)
    return key, share


def _clear_guid_index() -> None:
    _INDEX_CACHE.guid = None


def _cached_guid_index(adapter: Any, allow: set[str], *, feature: str = "history") -> dict[str, Any] | None:
    key, share = _index_cache_context(adapter, allow, feature)
    entry = getattr(_INDEX_CACHE, "guid", None)
    if entry is not None and entry["key"] == key and (share or time.monotonic() - entry["ts"] < _GUID_INDEX_TTL_SEC):
        return entry
    return None


def _row_guids(row: Mapping[str, Any]) -> list[str]:
    vals: list[str] = []
    g = row.get("guid")
    if g:
        vals.append(str(g))
    for gg in row.get("Guid") or []:
        gid = gg.get("id") if isinstance(gg, Mapping) else None
        if gid:
            vals.append(str(gid))
    return vals


def _fetch_section_guid_rows(srv: Any, section_id: str, plex_type: int) -> tuple[list[Mapping[str, Any]], int]:
    base = _as_base_url(srv)
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token and section_id):
        raise RuntimeError("plex_guid_index_server_unavailable")

    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"

    page_size = 1000
    out: list[Mapping[str, Any]] = []
    seen_pages: set[tuple[str, ...]] = set()
    start = 0
    made = 0

    while True:
        params = {
            "type": plex_type,
            "includeGuids": 1,
            "X-Plex-Container-Start": start,
            "X-Plex-Container-Size": page_size,
        }
        try:
            r = ses.get(f"{base}/library/sections/{section_id}/all", params=params, headers=headers, timeout=20)
        except Exception as exc:
            raise RuntimeError("plex_guid_index_request_failed") from exc
        made += 1
        if not getattr(r, "ok", False):
            raise RuntimeError(f"plex_guid_index_http_{getattr(r, 'status_code', 0)}")
        try:
            ctype = (r.headers.get("content-type") or "").lower()
            data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
            mc = data.get("MediaContainer") or {}
            rows = [x for x in (mc.get("Metadata") or []) if isinstance(x, Mapping)]
            total = mc.get("totalSize")
            total_i = int(total) if total is not None else None
        except Exception as exc:
            raise RuntimeError("plex_guid_index_parse_failed") from exc
        if not rows:
            if total_i is not None and start < total_i:
                raise RuntimeError("plex_guid_index_incomplete_page")
            break
        signature = tuple(str(row.get("ratingKey") or row.get("key") or "") for row in rows)
        if signature in seen_pages:
            raise RuntimeError("plex_guid_index_repeated_page")
        seen_pages.add(signature)
        out.extend(rows)
        start += len(rows)
        if total_i is not None and start >= total_i:
            break
        if total_i is None and len(rows) < page_size:
            break

    return out, made


def _build_guid_index(adapter: Any, allow: set[str], *, force: bool = False, feature: str = "history") -> dict[str, Any]:
    cached = None if force else _cached_guid_index(adapter, allow, feature=feature)
    if cached is not None:
        return cached
    _clear_guid_index()
    srv = getattr(getattr(adapter, "client", None), "server", None)
    key, _ = _index_cache_context(adapter, allow, feature)
    entry: dict[str, Any] = {"key": key, "movies": {}, "shows": {}}
    requests_made = 0
    for sec in adapter.libraries(types=("movie", "show")) or []:
        sid = str(getattr(sec, "key", "") or "").strip()
        if allow and sid and sid not in allow:
            continue
        is_movie = getattr(sec, "type", "") == "movie"
        dst = entry["movies" if is_movie else "shows"]
        rows, n = _fetch_section_guid_rows(srv, sid, 1 if is_movie else 2)
        requests_made += n
        for row in rows:
            rk = str(row.get("ratingKey") or "").strip()
            if not rk:
                continue
            for g in _row_guids(row):
                gg = str(g or "").strip().lower()
                if gg and gg not in dst:
                    dst[gg] = rk
    entry["ts"] = time.monotonic()
    _INDEX_CACHE.guid = entry
    _dbg("index_fetch_counts", source="guid_index", movies=len(entry["movies"]), shows=len(entry["shows"]), requests=requests_made)
    return entry


def _pms_find_in_guid_index(libtype: str, candidates: list[str]) -> str | None:
    entry = getattr(_INDEX_CACHE, "guid", None) or {}
    src = entry.get("shows" if libtype == "show" else "movies", {})
    for g in candidates or []:
        gg = str(g or "").strip().lower()
        if gg and gg in src:
            return src[gg]
    return None


CLASS_IN_CATALOG_WATCHED = "in_catalog_watched"
CLASS_IN_CATALOG_UNWATCHED = "in_catalog_unwatched"
CLASS_SHOW_MATCHED_EPISODE_MISSING = "show_matched_episode_missing"
CLASS_NOT_IN_PLEX_CATALOG = "not_in_plex_catalog"
CLASS_RESOLVE_AMBIGUOUS = "resolve_ambiguous"

DATE_EXACT = "confirmed_watched_exact_date"
DATE_MISMATCH = "confirmed_watched_date_mismatch"
DATE_NO_DATE = "confirmed_watched_no_date"
DATE_WRITE_FAILED = "write_failed"

_TOKEN_KEYS = ("tmdb", "imdb", "tvdb", "plex")

# Internal tuning defaults.
_DATE_TOLERANCE_SEC = 60
_CATALOG_MEM_TTL_SEC = 90


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name, "") or "").strip().lower() in ("1", "true", "yes", "on")


def _id_tokens(ids: Mapping[str, Any] | None) -> set[str]:
    out: set[str] = set()
    if not isinstance(ids, Mapping):
        return out
    for k in _TOKEN_KEYS:
        v = ids.get(k)
        if v is None or str(v).strip() == "":
            continue
        out.add(f"{k}:{str(v).strip().lower()}")
    return out


def _item_show_tokens(item: Mapping[str, Any]) -> set[str]:
    toks = _id_tokens(extract_show_ids(item))
    kind = (item.get("type") or "").strip().lower()
    if not toks and kind in ("show", "season", "episode", "anime"):
        toks = _id_tokens(ids_from(item))
    external = {tok for tok in toks if not tok.startswith("plex:")}
    return external or toks


def _item_se(item: Mapping[str, Any]) -> tuple[int | None, int | None]:
    s = item.get("season") if item.get("season") is not None else item.get("season_number")
    e = item.get("episode") if item.get("episode") is not None else item.get("episode_number")
    try:
        s = int(s) if s is not None else None
    except Exception:
        s = None
    try:
        e = int(e) if e is not None else None
    except Exception:
        e = None
    return s, e


class HistoryCatalog:
    __slots__ = ("by_rk", "movie_tokens", "show_tokens", "episode_index", "movie_title_year", "live_complete")

    def __init__(self) -> None:
        self.live_complete = False
        self.by_rk: dict[str, dict[str, Any]] = {}
        self.movie_tokens: dict[str, str] = {}
        self.show_tokens: dict[str, str] = {}
        self.episode_index: dict[tuple[str, int, int], set[str]] = {}
        self.movie_title_year: dict[tuple[str, int | None], set[str]] = {}

    def add(self, entry: Mapping[str, Any]) -> None:
        rk = str(entry.get("rk") or entry.get("rating_key") or "").strip()
        if not rk:
            return
        kind = (entry.get("type") or "").strip().lower()
        e: dict[str, Any] = {
            "rk": rk,
            "type": "episode" if kind in ("episode", "anime") else ("show" if kind in ("show", "season") else "movie"),
            "title": entry.get("title"),
            "series_title": entry.get("series_title") or entry.get("show_title"),
            "year": entry.get("year"),
            "library_id": entry.get("library_id"),
            "ids": dict(entry.get("ids") or {}),
            "guids": [str(g).lower() for g in (entry.get("guids") or []) if g],
            "show_rk": (str(entry.get("show_rk")).strip() if entry.get("show_rk") else None),
            "show_ids": dict(entry.get("show_ids") or {}),
            "season": entry.get("season"),
            "episode": entry.get("episode"),
            "watched": bool(entry.get("watched")),
            "write_accepted": bool(entry.get("write_accepted")),
            "view_count": entry.get("view_count"),
            "last_viewed_at": entry.get("last_viewed_at"),
            "added_at": entry.get("added_at"),
        }
        self.by_rk[rk] = e
        if e["type"] == "movie":
            for tok in _id_tokens(e["ids"]):
                self.movie_tokens.setdefault(tok, rk)
            ty = self._title_year(e["title"], e["year"])
            if ty:
                self.movie_title_year.setdefault(ty, set()).add(rk)
        elif e["type"] == "show":
            for tok in _id_tokens(e["ids"]):
                self.show_tokens.setdefault(tok, rk)
        else:
            show_toks = _id_tokens(e["show_ids"])
            for tok in show_toks:
                if e["show_rk"]:
                    self.show_tokens.setdefault(tok, e["show_rk"])
            s, ep = e["season"], e["episode"]
            try:
                s_i = int(s) if s is not None else None
                e_i = int(ep) if ep is not None else None
            except Exception:
                s_i = e_i = None
            if s_i is not None and e_i is not None:
                for tok in show_toks:
                    self.episode_index.setdefault((tok, s_i, e_i), set()).add(rk)

    @staticmethod
    def _title_year(title: Any, year: Any) -> tuple[str, int | None] | None:
        t = str(title or "").strip().lower()
        if not t:
            return None
        try:
            y = int(year) if year is not None else None
        except Exception:
            y = None
        return (t, y)

    def _has_show(self, show_tokens: set[str]) -> bool:
        if any(tok in self.show_tokens for tok in show_tokens):
            return True
        for (tok, _s, _e) in self.episode_index.keys():
            if tok in show_tokens:
                return True
        return False

    def resolve(self, item: Mapping[str, Any], *, strict: bool = False) -> tuple[str | None, str]:
        kind = (item.get("type") or "movie").strip().lower()
        if kind == "anime":
            kind = "episode"

        if kind in ("episode", "season", "show"):
            show_tokens = _item_show_tokens(item)
            s, ep = _item_se(item)
            if s is not None and ep is not None and show_tokens:
                rks: set[str] = set()
                for tok in show_tokens:
                    rks |= self.episode_index.get((tok, int(s), int(ep)), set())
                if len(rks) == 1:
                    rk = next(iter(rks))
                    e = self.by_rk.get(rk) or {}
                    return rk, (CLASS_IN_CATALOG_WATCHED if e.get("watched") else CLASS_IN_CATALOG_UNWATCHED)
                if len(rks) > 1:
                    return None, CLASS_RESOLVE_AMBIGUOUS
            if show_tokens and self._has_show(show_tokens):
                return None, CLASS_SHOW_MATCHED_EPISODE_MISSING
            return None, CLASS_NOT_IN_PLEX_CATALOG

        tokens = _id_tokens(ids_from(item))
        external = {tok for tok in tokens if not tok.startswith("plex:")}
        tokens = external or tokens
        hit_rks = {self.movie_tokens[tok] for tok in tokens if tok in self.movie_tokens}
        if len(hit_rks) == 1:
            rk = next(iter(hit_rks))
            e = self.by_rk.get(rk) or {}
            return rk, (CLASS_IN_CATALOG_WATCHED if e.get("watched") else CLASS_IN_CATALOG_UNWATCHED)
        if len(hit_rks) > 1:
            return None, CLASS_RESOLVE_AMBIGUOUS
        if not strict:
            ty = self._title_year(item.get("title"), item.get("year"))
            if ty:
                cand = self.movie_title_year.get(ty) or set()
                if len(cand) == 1:
                    rk = next(iter(cand))
                    e = self.by_rk.get(rk) or {}
                    return rk, (CLASS_IN_CATALOG_WATCHED if e.get("watched") else CLASS_IN_CATALOG_UNWATCHED)
                if len(cand) > 1:
                    return None, CLASS_RESOLVE_AMBIGUOUS
        return None, CLASS_NOT_IN_PLEX_CATALOG

    def trace(self, item: Mapping[str, Any], *, strict: bool = False) -> dict[str, Any]:
        rk, klass = self.resolve(item, strict=strict)
        entry = self.by_rk.get(rk) if rk else None
        kind = (item.get("type") or "movie").strip().lower()
        info: dict[str, Any] = {
            "source_key": canonical_key(item),
            "source_ids": dict(ids_from(item) or {}),
            "type": kind,
            "season": item.get("season"),
            "episode": item.get("episode"),
            "classification": klass,
            "plex_rating_key": rk,
            "plex_view_count": (entry or {}).get("view_count") if entry else None,
            "plex_last_viewed_at": (entry or {}).get("last_viewed_at") if entry else None,
            "snapshot_present": bool(entry and entry.get("watched")),
        }
        if kind in ("episode", "season", "show", "anime"):
            info["source_show_ids"] = dict(extract_show_ids(item) or {})
            info["plex_show_rating_key"] = (entry or {}).get("show_rk") if entry else None
        desired = _as_epoch(item.get("watched_at")) if item.get("watched_at") else None
        info["source_watched_at"] = item.get("watched_at")
        ds, delta = _date_status(desired, (entry or {}).get("last_viewed_at") if entry else None, _DATE_TOLERANCE_SEC)
        info["date_status"] = ds if (entry and entry.get("watched")) else None
        info["date_delta_seconds"] = delta if (entry and entry.get("watched")) else None
        return info

    def presence(self) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for e in self.by_rk.values():
            if not e.get("watched"):
                continue
            row = _catalog_entry_to_minimal(e)
            ts = _as_epoch(e.get("last_viewed_at")) if e.get("last_viewed_at") else None
            if not ts and e.get("view_count"):
                row["watched_at_missing"] = True
                ts = 0
            row["watched"] = True
            row["watched_at"] = _iso(int(ts)) if ts else None
            _force_episode_title(row)
            key = f"{canonical_key(row)}@{int(ts or 0)}"
            out[key] = row
        return out


def _catalog_entry_to_minimal(e: Mapping[str, Any]) -> dict[str, Any]:
    kind = (e.get("type") or "movie").strip().lower()
    row: dict[str, Any] = {"type": "episode" if kind == "episode" else ("show" if kind == "show" else "movie")}
    ids = dict(e.get("ids") or {})
    if e.get("rk"):
        ids.setdefault("plex", str(e.get("rk")))
    row["ids"] = {k: v for k, v in ids.items() if v}
    if e.get("title") is not None:
        row["title"] = e.get("title")
    if e.get("year") is not None:
        row["year"] = e.get("year")
    if e.get("library_id") is not None:
        row["library_id"] = e.get("library_id")
    if kind == "episode":
        if e.get("series_title"):
            row["series_title"] = e.get("series_title")
        if e.get("show_ids"):
            row["show_ids"] = dict(e.get("show_ids") or {})
        if e.get("season") is not None:
            row["season"] = e.get("season")
        if e.get("episode") is not None:
            row["episode"] = e.get("episode")
    return row


def build_catalog_from_entries(entries: Iterable[Mapping[str, Any]]) -> HistoryCatalog:
    cat = HistoryCatalog()
    for entry in entries or []:
        try:
            cat.add(entry)
        except Exception:
            continue
    return cat


def _emit(evt: dict[str, Any]) -> None:
    emit(evt, default_feature="history")


class _WriteProgress:
    __slots__ = ("phase", "total", "_last_at", "_last_done", "_step", "_interval")

    def __init__(self, phase: str, total: int, *, step: int = 100, interval_s: float = 3.0) -> None:
        self.phase = phase
        self.total = int(total or 0)
        self._last_at = time.time()
        self._last_done = -1
        self._step = max(1, int(step))
        self._interval = float(interval_s)

    def tick(self, done: int, *, force: bool = False) -> None:
        d = int(done or 0)
        if not force:
            now = time.time()
            if d == self._last_done:
                return
            if (d % self._step) and (now - self._last_at) < self._interval:
                return
            self._last_at = now
        self._last_done = d
        _emit({
            "event": "plex.write", "action": "progress", "feature": "history", "level": "info",
            "phase": self.phase, "done": d, "total": self.total,
        })


def _epoch_from_history_entry(entry: Any) -> int | None:
    data = getattr(entry, "_data", None)
    if data is not None and hasattr(data, "get"):
        for k in ("viewedAt", "lastViewedAt"):
            ts = _as_epoch(data.get(k))
            if ts:
                return ts
    for k in ("viewedAt", "viewed_at", "lastViewedAt"):
        ts = _as_epoch(getattr(entry, k, None))
        if ts:
            return ts
    return None

def _account_id_from_history_entry(entry: Any) -> int | None:
    v = getattr(entry, "accountID", None)
    if v is None:
        return None
    try:
        return int(str(v).strip())
    except Exception:
        return None

def _username_from_history_entry(entry: Any) -> str | None:
    data = getattr(entry, "_data", None)
    if isinstance(data, Mapping):
        for attr in ("username", "userName", "accountName", "userTitle", "user"):
            v = data.get(attr)
            if isinstance(v, str):
                s = v.strip()
                if s:
                    return s
            if isinstance(v, Mapping):
                for sub in ("username", "title", "name"):
                    sv = v.get(sub)
                    if isinstance(sv, str):
                        s = sv.strip()
                        if s:
                            return s
    for attr in ("username", "userName", "accountName", "userTitle", "user"):
        v = getattr(entry, attr, None)
        if v is None:
            continue
        if isinstance(v, str):
            s = v.strip()
            return s or None
        for sub in ("username", "title", "name"):
            sv = getattr(v, sub, None)
            if isinstance(sv, str):
                s = sv.strip()
                return s or None
    return None

def _history_cfg(adapter: Any) -> Mapping[str, Any]:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        hist = plex.get("history") or {}
        return hist if isinstance(hist, dict) else {}
    except Exception:
        return {}

def _history_cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    cfg = _history_cfg(adapter)
    val = cfg.get(key, default) if isinstance(cfg, dict) else default
    return default if val is None else val

def _history_force_full(adapter: Any) -> bool:
    return _env_truthy("CW_PLEX_HISTORY_FORCE") or bool(_history_cfg_get(adapter, "force_full", False))

def _row_section_id(h: Any) -> str | None:
    for attr in ("librarySectionID", "sectionID", "librarySectionId", "sectionId"):
        v = getattr(h, attr, None)
        if v is not None:
            try:
                return str(int(v))
            except Exception:
                pass
    sk = getattr(h, "sectionKey", None) or getattr(h, "librarySectionKey", None)
    if sk:
        m = re.search(r"/library/sections/(\d+)", str(sk))
        if m:
            return m.group(1)
    return None

def _load_shadow() -> dict[str, Any]:
    return read_json(_shadow_path())

def _save_shadow(data: Mapping[str, Any]) -> None:
    write_json(_shadow_path(), data)

def _shadow_add_batch(items: list[Mapping[str, Any]]) -> None:
    if not items:
        return
    try:
        data = _load_shadow()
        now_iso = _iso(int(datetime.now(timezone.utc).timestamp()))
        for item in items:
            key = _event_key(item)
            if not key:
                continue
            existing = data.get(key)
            entry: dict[str, Any] = dict(existing) if isinstance(existing, Mapping) else {}
            entry["item"] = id_minimal(item)
            entry["watched_at"] = item.get("watched_at")
            entry["last_seen"] = now_iso
            if "first_seen" not in entry:
                entry["first_seen"] = now_iso
            data[key] = entry
        _save_shadow(data)
    except Exception:
        pass

def _shadow_remove(item: Mapping[str, Any]) -> None:
    try:
        data = _load_shadow() or {}
        if not isinstance(data, Mapping) or not data:
            return

        exact = _event_key(item)
        try:
            base = canonical_key(id_minimal(item)) or canonical_key(item) or ""
        except Exception:
            base = ""

        changed = False
        for key in list(data.keys()):
            key_s = str(key or "")
            if exact and key_s == exact:
                del data[key]
                changed = True
                continue
            if base and (key_s == base or key_s.startswith(f"{base}@")):
                del data[key]
                changed = True

        if changed:
            _save_shadow(data)
    except Exception:
        pass

def _has_external_ids(minimal: Mapping[str, Any]) -> bool:
    ids = minimal.get("ids") or {}
    show_ids = minimal.get("show_ids") or {}
    return bool(
        ids.get("imdb")
        or ids.get("tmdb")
        or ids.get("tvdb")
        or ids.get("trakt")
        or show_ids.get("imdb")
        or show_ids.get("tmdb")
        or show_ids.get("tvdb")
        or show_ids.get("trakt")
    )

def _guid_from_minimal(minimal: Mapping[str, Any]) -> str:
    ids = minimal.get("ids") or {}
    guid = minimal.get("guid") or ids.get("guid") or ids.get("plex_guid")
    return str(guid).lower() if guid else ""

def _keep_in_snapshot(adapter: Any, minimal: Mapping[str, Any]) -> bool:
    ignore_local = bool(plex_cfg_get(adapter, "history_ignore_local_guid", False))
    prefixes = plex_cfg_get(adapter, "history_ignore_guid_prefixes", ["local://"]) or []
    require_ext = bool(plex_cfg_get(adapter, "history_require_external_ids", False))
    if require_ext and not _has_external_ids(minimal):
        return False
    if ignore_local:
        guid = _guid_from_minimal(minimal)
        if guid and any(guid.startswith(p.lower()) for p in prefixes):
            return False
    return True


def _marked_section_id(sec: Any) -> str | None:
    for attr in ("librarySectionID", "sectionID", "id", "key"):
        v = getattr(sec, attr, None)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if s.isdigit():
            return s
        m = re.search(r"/library/sections/(\d+)", s)
        if m:
            return m.group(1)
    return None

def _episode_play_count_supported(adapter: Any, section_id: str, allow: set[str]) -> bool:
    key, shared = _index_cache_context(adapter, allow, "history")
    cached = getattr(_INDEX_CACHE, "watched_filters", None)
    if cached is None or cached["key"] != key or (not shared and time.monotonic() - cached["ts"] >= _CATALOG_MEM_TTL_SEC):
        cached = {"key": key, "ts": time.monotonic(), "sections": {}}
        _INDEX_CACHE.watched_filters = cached
    if section_id in cached["sections"]:
        return cached["sections"][section_id]
    srv = getattr(getattr(adapter, "client", None), "server", None)
    token = active_pms_token(adapter)
    if srv is None or not token:
        cached["sections"][section_id] = False
        return False
    supported = False
    try:
        headers = plex_headers(token)
        headers["Accept"] = "application/json"
        response = srv._session.get(
            f"{_as_base_url(srv)}/library/sections/{section_id}/all",
            params={"includeMeta": 1, "includeAdvanced": 1, "X-Plex-Container-Start": 0, "X-Plex-Container-Size": 0},
            headers=headers, timeout=15,
        )
        if response.ok:
            if "json" in str(response.headers.get("content-type", "")).lower():
                meta = response.json()["MediaContainer"].get("Meta", {})
                field = any(f.get("key") == "episode.viewCount" and f.get("type") == "integer"
                            for kind in meta.get("Type", []) for f in kind.get("Field", []))
                operator = any(op.get("key") == ">>=" for kind in meta.get("FieldType", [])
                               if kind.get("type") == "integer" for op in kind.get("Operator", []))
            else:
                meta = ET.fromstring(response.text).find("Meta")
                field = meta is not None and meta.find("./Type/Field[@key='episode.viewCount'][@type='integer']") is not None
                operator = meta is not None and meta.find("./FieldType[@type='integer']/Operator[@key='>>=']") is not None
            supported = bool(field and operator)
    except Exception:
        pass
    cached["sections"][section_id] = supported
    return supported


def _iter_marked_watched_from_library(
    adapter: Any,
    allow: set[str],
) -> list[tuple[dict[str, Any], int]]:
    setattr(adapter, "_plex_live_scan_complete", False)
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return []
    base = _as_base_url(srv)
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token):
        return []

    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"

    def _rows_from(r: Any) -> tuple[list[Mapping[str, Any]], int | None]:
        try:
            ctype = (r.headers.get("content-type") or "").lower()
            data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
            mc = data.get("MediaContainer")
            if not isinstance(mc, Mapping):
                raise ValueError("invalid_media_container")
            rows = mc.get("Metadata")
            if rows is None and (mc.get("size") == 0 or mc.get("totalSize") == 0):
                rows = []
            if not isinstance(rows, list) or any(not isinstance(row, Mapping) or not row.get("ratingKey") for row in rows):
                raise ValueError("invalid_metadata")
            total = mc.get("totalSize")
            total_i = int(total) if total is not None else None
            return [x for x in rows if isinstance(x, Mapping)], total_i
        except Exception as exc:
            raise RuntimeError("plex_watched_scan_invalid_response") from exc

    def _int0(v: Any) -> int:
        try:
            return int(v or 0)
        except Exception:
            return 0

    page_size = 200
    results: list[tuple[dict[str, Any], int]] = []
    summary = {
        "sections_scanned": 0, "movie_sections": 0, "show_sections": 0,
        "watched_rows_seen": 0, "watched_rows_returned": 0,
        "skipped_no_timestamp": 0, "skipped_no_rating_key": 0, "skipped_normalize_failed": 0,
    }

    try:
        sections = list(adapter.libraries(types=("movie", "show")) or [])
    except Exception as exc:
        raise RuntimeError("plex_watched_libraries_failed") from exc

    available = {_marked_section_id(sec) for sec in sections if str(getattr(sec, "type", "")).lower() in ("movie", "show")}
    missing = allow - available
    if missing:
        _warn("history_libraries_unavailable", library_ids=sorted(missing))
        if not allow.intersection(available):
            raise RuntimeError(f"plex_history_no_accessible_selected_libraries: {','.join(sorted(missing))}")

    def _scan_section(section_id: str, plex_type: int, section_type: str, section_title: str, *, use_unwatched_filter: bool, play_count_filter: bool = False) -> int:
        start = 0
        seen_pages: set[tuple[str, ...]] = set()
        page_retries = 0
        sec = {"seen": 0, "vc": 0, "lva": 0, "no_ts": 0, "norm_fail": 0, "no_rk": 0, "ret": 0}
        last_status: Any = None
        last_total: Any = None
        while True:
            params: dict[str, Any] = {
                "type": plex_type,
                "sort": "lastViewedAt:desc",
                "includeGuids": 1,
                "X-Plex-Container-Start": start,
                "X-Plex-Container-Size": page_size,
            }
            if use_unwatched_filter:
                params["episode.viewCount>>" if play_count_filter else "unwatched"] = 0
            try:
                r = ses.get(f"{base}/library/sections/{section_id}/all", params=params, headers=headers, timeout=15)
            except Exception as exc:
                raise RuntimeError("plex_watched_scan_request_failed") from exc
            last_status = getattr(r, "status_code", None)
            if not getattr(r, "ok", False):
                if play_count_filter and start == 0 and last_status in (400, 422):
                    return _scan_section(section_id, plex_type, section_type, section_title, use_unwatched_filter=False)
                raise RuntimeError(f"plex_watched_scan_http_{last_status}")
            rows, total = _rows_from(r)
            last_total = total
            if not rows:
                if total is not None and start < total:
                    raise RuntimeError("plex_watched_scan_incomplete_page")
                break
            signature = tuple(str(row.get("ratingKey") or row.get("key") or "") for row in rows)
            if signature in seen_pages:
                if page_retries == 0:
                    page_retries += 1
                    continue
                raise RuntimeError("plex_watched_scan_repeated_page")
            page_retries = 0
            seen_pages.add(signature)

            for row in rows:
                view_count = max(_int0(row.get("viewCount")), _int0(row.get("leafCountViewed")))
                ts = _as_epoch(row.get("lastViewedAt") or row.get("viewedAt"))
                watched = view_count > 0
                if not watched:
                    continue
                sec["seen"] += 1
                if view_count > 0:
                    sec["vc"] += 1
                if ts:
                    sec["lva"] += 1
                ts_i = int(ts) if ts else 0
                meta = normalize_discover_row(row, token=token) or {}
                if not meta:
                    raise RuntimeError("plex_watched_scan_normalize_failed")
                if not (meta.get("ids") or {}).get("plex"):
                    raise RuntimeError("plex_watched_scan_missing_identity")
                meta['_cw_marked'] = True
                meta['_cw_view_count'] = view_count
                if ts_i:
                    meta['watched_at'] = meta.get('watched_at') or _iso(int(ts_i))
                else:
                    meta['watched_at'] = None
                    meta['_cw_watched_at_missing'] = True
                    sec["no_ts"] += 1
                results.append((meta, ts_i))
                sec["ret"] += 1

            start += len(rows)
            if total is not None and start >= total:
                break
            if len(rows) < page_size and total is None:
                break

        _dbg(
            "live_watched.section",
            section_id=section_id, section_title=section_title, section_type=section_type,
            plex_type=plex_type, request_url=f"/library/sections/{section_id}/all",
            unwatched_filter=bool(use_unwatched_filter),
            http_status=last_status, totalSize=last_total,
            rows_seen=sec["seen"], rows_with_viewCount=sec["vc"], rows_with_lastViewedAt=sec["lva"],
            rows_skipped_no_timestamp=sec["no_ts"], rows_normalized=sec["ret"], rows_returned=sec["ret"],
            allow_filter_hit=False,
        )
        summary["watched_rows_seen"] += sec["seen"]
        summary["watched_rows_returned"] += sec["ret"]
        summary["skipped_no_timestamp"] += sec["no_ts"]
        summary["skipped_no_rating_key"] += sec["no_rk"]
        summary["skipped_normalize_failed"] += sec["norm_fail"]
        return sec["ret"]

    for sec_obj in sections:
        section_id = _marked_section_id(sec_obj) or ""
        section_type = (getattr(sec_obj, "type", "") or "").lower()
        section_title = str(getattr(sec_obj, "title", "") or "")
        if not section_id:
            continue
        if allow and section_id not in allow:
            _dbg("live_watched.section", section_id=section_id, section_title=section_title,
                 section_type=section_type, allow_filter_hit=True, rows_returned=0)
            continue

        plex_type = 1 if section_type == "movie" else 4 if section_type == "show" else None
        if plex_type is None:
            continue

        summary["sections_scanned"] += 1
        if plex_type == 1:
            summary["movie_sections"] += 1
        else:
            summary["show_sections"] += 1

        play_count_filter = plex_type == 4 and _episode_play_count_supported(adapter, section_id, allow)
        ret = _scan_section(section_id, plex_type, section_type, section_title, use_unwatched_filter=True, play_count_filter=play_count_filter)
        if ret == 0 and plex_type == 4 and not play_count_filter:
            _scan_section(section_id, plex_type, section_type, section_title, use_unwatched_filter=False)

    setattr(adapter, "_plex_live_scan_complete", True)

    _emit({
        "event": "plex.presence", "action": "live_watched_summary", "feature": "history",
        "level": "debug", "full": True, **summary,
    })
    return results


def _live_watched_entry(meta: Mapping[str, Any], ts: int) -> dict[str, Any] | None:
    ids = dict(meta.get("ids") or {})
    rk = ids.get("plex")
    if not rk:
        return None
    show_ids = dict(meta.get("show_ids") or {})
    return {
        "rk": str(rk),
        "type": meta.get("type") or "movie",
        "title": meta.get("title"),
        "series_title": meta.get("series_title") or meta.get("show_title"),
        "year": meta.get("year"),
        "library_id": meta.get("library_id"),
        "ids": ids,
        "show_ids": show_ids,
        "show_rk": show_ids.get("plex"),
        "season": meta.get("season"),
        "episode": meta.get("episode"),
        "watched": True,
        "view_count": meta.get("_cw_view_count"),
        "last_viewed_at": int(ts) if ts else None,
    }


def _iter_live_watched(adapter: Any, allow: set[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for meta, ts in _iter_marked_watched_from_library(adapter, allow):
        entry = _live_watched_entry(meta, ts)
        if entry:
            out.append(entry)
    return out


def _populate_catalog_episode_leaves(adapter: Any, allow: set[str], cat: HistoryCatalog) -> int:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return 0
    base = _as_base_url(srv)
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token):
        return 0

    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"

    try:
        page_size = int(_history_cfg_get(adapter, "episode_catalog_page_size", 1000) or 1000)
    except Exception:
        page_size = 1000
    page_size = max(100, min(page_size, 2000))

    try:
        sections = list(adapter.libraries(types=("show",)) or [])
    except Exception:
        sections = []

    added = 0
    scanned = 0
    t0 = time.time()
    for sec_obj in sections:
        section_id = _marked_section_id(sec_obj) or ""
        if not section_id or (allow and section_id not in allow):
            continue
        start = 0
        seen_pages: set[tuple[str, ...]] = set()
        while True:
            params = {
                "type": 4,
                "sort": "episode.addedAt",
                "includeGuids": 1,
                "X-Plex-Container-Start": start,
                "X-Plex-Container-Size": page_size,
            }
            try:
                r = ses.get(f"{base}/library/sections/{section_id}/all", params=params, headers=headers, timeout=20)
            except Exception:
                break
            if not getattr(r, "ok", False):
                break
            try:
                ctype = (r.headers.get("content-type") or "").lower()
                data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
                mc = data.get("MediaContainer") or {}
                rows0 = mc.get("Metadata") or []
                rows = [x for x in rows0 if isinstance(x, Mapping)]
                total = mc.get("totalSize")
                total_i = int(total) if total is not None else None
            except Exception:
                rows = []
                total_i = None
            if not rows:
                break
            signature = tuple(str(row.get("ratingKey") or row.get("key") or "") for row in rows)
            if signature in seen_pages:
                break
            seen_pages.add(signature)
            scanned += len(rows)
            _emit({
                "event": "plex.catalog", "action": "episode_leaves_progress", "feature": "history", "level": "info",
                "section_id": section_id, "scanned": scanned, "total": total_i,
            })
            for row in rows:
                meta = normalize_discover_row(row, token=token, hydrate_item_ids=False) or {}
                ids = dict(meta.get("ids") or {})
                rk = str(ids.get("plex") or row.get("ratingKey") or "").strip()
                if not rk:
                    continue
                existing = cat.by_rk.get(rk) or {}
                entry = {
                    "rk": rk,
                    "type": "episode",
                    "title": meta.get("title") or row.get("grandparentTitle") or row.get("title"),
                    "series_title": meta.get("series_title") or meta.get("show_title") or row.get("grandparentTitle"),
                    "library_id": meta.get("library_id"),
                    "ids": ids,
                    "show_ids": dict(meta.get("show_ids") or {}),
                    "show_rk": (meta.get("show_ids") or {}).get("plex") if isinstance(meta.get("show_ids"), Mapping) else None,
                    "season": meta.get("season"),
                    "episode": meta.get("episode"),
                    "watched": bool(existing.get("watched")),
                    "write_accepted": bool(existing.get("write_accepted")),
                    "view_count": existing.get("view_count"),
                    "last_viewed_at": existing.get("last_viewed_at"),
                }
                cat.add(entry)
                added += 1
            start += len(rows)
            if total_i is not None and start >= total_i:
                break
            if len(rows) < page_size:
                break

    _emit({
        "event": "plex.catalog", "action": "episode_leaves", "feature": "history", "level": "debug",
        "scanned": scanned, "added": added, "duration_ms": int((time.time() - t0) * 1000),
    })
    return added


def _build_history_catalog(adapter: Any, allow: set[str], *, force: bool = False, live: bool = True) -> HistoryCatalog:
    t0 = time.time()
    allow_list = sorted(str(x) for x in (allow or set()))
    _emit({"event": "plex.catalog", "action": "start", "feature": "history", "level": "debug",
           "force": bool(force), "live": bool(live), "allow": allow_list})
    cat = HistoryCatalog()
    if force:
        _clear_guid_index()
    index = _build_guid_index(adapter, allow, force=force)
    for guid, rk in list(index["movies"].items()):
        ids = ids_from_guid(str(guid))
        if ids:
            cat.add({"rk": rk, "type": "movie", "ids": ids, "watched": False})
    for guid, rk in list(index["shows"].items()):
        ids = ids_from_guid(str(guid))
        if ids:
            cat.add({"rk": rk, "type": "show", "ids": ids, "watched": False})
    watched_movies = 0
    watched_eps = 0
    if live:
        setattr(adapter, "_plex_live_scan_complete", False)
        for entry in _iter_live_watched(adapter, allow):
            cat.add(entry)
            if (entry.get("type") or "") == "episode":
                watched_eps += 1
            else:
                watched_movies += 1
        cat.live_complete = bool(getattr(adapter, "_plex_live_scan_complete", False))
    _emit({
        "event": "plex.catalog", "action": "done", "feature": "history", "level": "debug",
        "force": bool(force), "live": bool(live), "allow": allow_list,
        "guid_movies": len(index["movies"]), "guid_shows": len(index["shows"]),
        "watched_movies": watched_movies, "watched_episodes": watched_eps,
        "catalog_entries": len(cat.by_rk), "duration_ms": int((time.time() - t0) * 1000),
    })
    return cat


def _user_scope_key(adapter: Any) -> str:
    def _i(v: Any) -> int:
        try:
            return int(v or 0)
        except Exception:
            return 0
    cli = getattr(adapter, "client", None)
    acct = _i(plex_cfg_get(adapter, "account_id", 0)) or _i(getattr(cli, "user_account_id", None))
    uname = str(plex_cfg_get(adapter, "username", "") or "").strip().lower() \
        or str(getattr(cli, "user_username", "") or "").strip().lower()
    return _wm_key(acct, uname)


def _playback_cache_path(adapter: Any, allow: set[str]) -> Path | None:
    key, _ = _index_cache_context(adapter, allow, "history")
    if not key[1] or str(key[1]).lower() in {"unscoped", "default", "none"} or not key[3] or not active_pms_token(adapter):
        return None
    options = {name: plex_cfg_get(adapter, name, None) for name in (
        "history_ignore_local_guid", "history_ignore_guid_prefixes", "history_require_external_ids",
    )}
    options["fallback_guid"] = bool(plex_cfg_get(adapter, "fallback_GUID", False) or plex_cfg_get(adapter, "fallback_guid", False))
    owner = sha256(json.dumps(key[1:7], default=str).encode()).hexdigest()[:32]
    identity = json.dumps([key[1:-1], options], default=str, sort_keys=True)
    digest = sha256(identity.encode()).hexdigest()[:32]
    return state_file(f"plex_history.playback.{owner}.{digest}.json")


def _load_playback_cache(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    data = read_json(path)
    if not isinstance(data, Mapping) or data.get("version") != 1 or not isinstance(data.get("items"), dict):
        return None
    cursor = data.get("cursor")
    if not isinstance(cursor, int) or cursor < 0:
        return None
    if not isinstance(data.get("full_refreshed", 0), int) or data.get("full_refreshed", 0) < 0:
        return None
    for key, row in data["items"].items():
        if not isinstance(row, dict) or not _as_epoch(row.get("watched_at")) or _event_key(row) != key:
            return None
    return dict(data)


_PLAYBACK_OVERLAP_SECONDS = 120


def _prune_playback_caches(path: Path, now: float) -> None:
    prefix = ".".join(path.name.split(".")[:3]) + "."
    try:
        for candidate in path.parent.glob(prefix + "*.json"):
            if candidate != path and now - candidate.stat().st_mtime > 30 * 86400:
                candidate.unlink()
    except OSError as exc:
        _dbg("playback_cache_cleanup_failed", error_type=type(exc).__name__)


def _catalog_cache_key(adapter: Any, allow: set[str]) -> tuple[Any, ...]:
    return _index_cache_context(adapter, allow, "history")[0]


def _store_history_catalog(adapter: Any, allow: set[str], cat: HistoryCatalog) -> None:
    _INDEX_CACHE.catalog = {"cat": cat, "ts": time.monotonic(), "key": _catalog_cache_key(adapter, allow)}


def _get_history_catalog(adapter: Any, allow: set[str], *, force: bool = False) -> HistoryCatalog:
    key, shared = _index_cache_context(adapter, allow, "history")
    cached = getattr(_INDEX_CACHE, "catalog", None)
    if not force and cached is not None and cached["key"] == key and (shared or time.monotonic() - cached["ts"] < _CATALOG_MEM_TTL_SEC):
        return cached["cat"]
    _INDEX_CACHE.catalog = None
    cat = _build_history_catalog(adapter, allow, force=force)
    _store_history_catalog(adapter, allow, cat)
    return cat


def _date_status(desired_ts: int | None, confirmed_ts: int | None, tol: int) -> tuple[str, int | None]:
    if not confirmed_ts:
        return DATE_NO_DATE, None
    if not desired_ts:
        return DATE_EXACT, 0
    delta = int(confirmed_ts) - int(desired_ts)
    return (DATE_EXACT, delta) if abs(delta) <= int(tol) else (DATE_MISMATCH, delta)


def _new_write_meta() -> dict[str, Any]:
    return {
        "accepted_keys": [],
        "presence_confirmed_keys": [],
        "live_confirmed_keys": [],
        "accepted_not_seen_live_keys": [],
        "date_confirmed_keys": [],
        "date_mismatch_keys": [],
        "unresolved_keys": [],
        "reason_counts": {},
    }


def _set_write_meta(adapter: Any, meta: Mapping[str, Any]) -> None:
    try:
        setattr(adapter, "_plex_history_write_meta", dict(meta))
    except Exception:
        pass

def _pms_fetch_metadata_row(adapter: Any, rating_key: str, *, strict: bool = False) -> Mapping[str, Any] | None:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        if strict:
            raise RuntimeError("plex_history_metadata_unavailable")
        return None
    base = _as_base_url(srv)
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token and rating_key):
        if strict:
            raise RuntimeError("plex_history_metadata_unavailable")
        return None
    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"
    try:
        r = ses.get(f"{base}/library/metadata/{rating_key}", headers=headers, timeout=15)
    except Exception as exc:
        if strict:
            raise RuntimeError("plex_history_metadata_request_failed") from exc
        return None
    if not getattr(r, "ok", False):
        if strict and getattr(r, "status_code", 0) != 404:
            raise RuntimeError(f"plex_history_metadata_http_{getattr(r, 'status_code', 0)}")
        return None
    try:
        ctype = (r.headers.get("content-type") or "").lower()
        data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
        mc = data.get("MediaContainer") or {}
        rows = mc.get("Metadata") or []
        if isinstance(rows, list) and rows and isinstance(rows[0], Mapping):
            if strict and str(rows[0].get("ratingKey") or "") != str(rating_key):
                raise ValueError("unexpected_rating_key")
            return rows[0]
    except Exception as exc:
        if strict:
            raise RuntimeError("plex_history_metadata_invalid_response") from exc
        return None
    if strict:
        raise RuntimeError("plex_history_metadata_invalid_response")
    return None


def _pms_row_is_watched(row: Mapping[str, Any]) -> bool:
    try:
        vc = row.get("viewCount")
        if vc is None:
            vc = row.get("leafCountViewed")
        return int(vc or 0) > 0
    except Exception:
        return False


def _history_access_denied(error: Exception) -> bool:
    from plexapi.exceptions import BadRequest, Unauthorized

    status = getattr(getattr(error, "response", None), "status_code", None)
    return (isinstance(error, (PermissionError, Unauthorized)) or status in (401, 403)
            or (isinstance(error, BadRequest) and str(error).startswith("(403)")))


def build_index(adapter: Any, since: int | None = None, limit: int | None = None, *, force: bool = False) -> dict[str, dict[str, Any]]:
    need_home_scope, did_home_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            _info("index_skipped", reason="account_only")
            return {}
        prog_mk = getattr(adapter, "progress_factory", None)
        prog: Any | None = prog_mk("history") if callable(prog_mk) else None
        fallback_guid = bool(plex_cfg_get(adapter, "fallback_GUID", False) or plex_cfg_get(adapter, "fallback_guid", False))
        if fallback_guid:
            _emit({"event": "debug", "msg": "fallback_guid.enabled", "provider": "PLEX", "feature": "history"})

        def _int_or_zero(v: Any) -> int:
            try:
                return int(v or 0)
            except Exception:
                return 0

        cfg_acct_id = _int_or_zero(plex_cfg_get(adapter, "account_id", 0))
        cli_acct_id = _int_or_zero(getattr(getattr(adapter, "client", None), "user_account_id", None))
        acct_id = cfg_acct_id or cli_acct_id

        cfg_uname = str(plex_cfg_get(adapter, "username", "") or "").strip().lower()
        cli_uname = str(getattr(getattr(adapter, "client", None), "user_username", "") or "").strip().lower()
        uname = cfg_uname or cli_uname

        wm_key = _wm_key(acct_id, uname)
        if since is not None and int(since or 0) > 0:
            eff_since = int(since)
        else:
            eff_since = None

        allow = plex_feature_library_ids(adapter, "history")
        explicit_user = bool(cfg_acct_id or cfg_uname)

        force = bool(force) or _history_force_full(adapter)
        if force:
            eff_since = None

        scope_ok = not (need_home_scope and not did_home_switch)
        if not scope_ok:
            _warn("home_scope_not_applied", op="build_index", selected=(sel_aid or sel_uname))

        maxresults = _int_or_zero(_history_cfg_get(adapter, "maxresults", 0))
        cache_path = _playback_cache_path(adapter, allow) if scope_ok and not limit and not maxresults and not since else None
        playback_cache = _load_playback_cache(cache_path)
        now = int(time.time())
        refreshed = (playback_cache or {}).get("full_refreshed", 0)
        refresh_full = force or not playback_cache
        if not since:
            eff_since = max(0, int(playback_cache["cursor"]) - _PLAYBACK_OVERLAP_SECONDS) if playback_cache and not refresh_full else None

        cat = _build_history_catalog(adapter, allow, force=force, live=scope_ok)
        include_marked = bool(_history_cfg_get(adapter, "include_marked_watched", True))
        if include_marked and scope_ok and not cat.live_complete:
            raise RuntimeError("plex_history_incomplete_watched_catalog")
        if scope_ok:
            _store_history_catalog(adapter, allow, cat)

        # Optional cursor debugging (helps diagnose 1-item re-add loops).
        if str(os.environ.get("CW_PLEX_HISTORY_DEBUG_CURSOR", "")).strip().lower() in ("1", "true", "yes"):
            _dbg(
                "cursor",
                since_arg=int(since or 0) if since is not None else None,
                eff_since=int(eff_since or 0) if eff_since else None,
                wm_key=wm_key,
            )

        base_kwargs: dict[str, Any] = {}
        if cfg_acct_id and (not cli_acct_id or int(cfg_acct_id) != int(cli_acct_id)):
            base_kwargs["accountID"] = int(cfg_acct_id)
        elif not explicit_user and cli_acct_id:
            base_kwargs["accountID"] = int(cli_acct_id)

        if eff_since is not None and eff_since > 0:
            base_kwargs["mindate"] = datetime.fromtimestamp(eff_since, tz=timezone.utc)

        if maxresults:
            base_kwargs["maxresults"] = int(maxresults)

        full_scopes: set[str | None] = set()

        def _call_history(**kwargs: Any) -> list[Any]:
            try:
                result = list(srv.history(**kwargs) or [])
            except Exception as e:
                if "mindate" in kwargs:
                    _dbg("mindate_fallback_drop", error=str(e))
                    kwargs.pop("mindate", None)
                    result = list(srv.history(**kwargs) or [])
                else:
                    raise
            if "mindate" not in kwargs:
                sid = kwargs.get("librarySectionID")
                full_scopes.add(str(sid) if sid is not None else None)
            return result

        rows: list[Any] = []
        history_complete = True
        history_errors: list[Exception] = []
        try:
            if allow:
                for sid in sorted(allow):
                    try:
                        kw = dict(base_kwargs)
                        kw["librarySectionID"] = int(sid)
                        part = _call_history(**kw)
                    except Exception as exc:
                        history_complete = False
                        history_errors.append(exc)
                        part = []
                    if not part and "accountID" in kw and not explicit_user:
                        try:
                            kw2 = dict(kw)
                            kw2.pop("accountID", None)
                            part = _call_history(**kw2)
                        except Exception as exc:
                            history_complete = False
                            history_errors.append(exc)
                            part = []
                    rows.extend(part)
            else:
                rows = _call_history(**base_kwargs)
                if not rows and "accountID" in base_kwargs and not explicit_user:
                    base_kwargs2 = dict(base_kwargs)
                    base_kwargs2.pop("accountID", None)
                    rows = _call_history(**base_kwargs2)
        except Exception as e:
            _warn("http_failed", op="build_index", error=str(e))
            history_complete = False
            history_errors.append(e)
            rows = []

        if history_errors and not playback_cache and any(not _history_access_denied(exc) for exc in history_errors):
            raise RuntimeError("plex_history_playback_unavailable_without_cache") from history_errors[0]

        total = len(rows)
        workers = plex_worker_count(adapter, "history_workers", "CW_PLEX_HISTORY_WORKERS", 12)

        # Optional cursor debugging: show the rows that are considered "new" for this run.
        if eff_since is not None and str(os.environ.get("CW_PLEX_HISTORY_DEBUG_CURSOR", "")).strip().lower() in ("1", "true", "yes"):
            try:
                new_rows = []
                for rr in rows:
                    ts_i = _epoch_from_history_entry(rr) or 0
                    if ts_i and ts_i >= int(eff_since):
                        new_rows.append(rr)
                sample = []
                for rr in new_rows[:5]:
                    try:
                        sample.append({
                            "type": getattr(rr, "type", None),
                            "title": getattr(rr, "title", None),
                            "ratingKey": getattr(rr, "ratingKey", None),
                            "ts": _epoch_from_history_entry(rr),
                        })
                    except Exception:
                        continue
                _dbg("cursor.new_rows", count=len(new_rows), sample=sample)
            except Exception:
                pass
        if prog:
            prog.tick(0, total=total, force=True)

        out: dict[str, dict[str, Any]] = {}
        def _process_history_row(raw: Any) -> tuple[str, dict[str, Any]] | None:
            if not _allowed_history_type(raw):
                return None

            ts = _epoch_from_history_entry(raw)
            if not ts:
                return None
            ts_i = int(ts)

            if since and not force and eff_since is not None and ts_i <= int(eff_since):
                return None

            if allow:
                sid = _row_section_id(raw)
                if sid and sid not in allow:
                    return None

            aid = _account_id_from_history_entry(raw)
            if cfg_acct_id and aid is not None and int(aid) != int(cfg_acct_id):
                return None
            if cfg_uname:
                u = (_username_from_history_entry(raw) or "").strip().lower()
                if u and u != cfg_uname:
                    return None
            if not explicit_user and cli_acct_id and aid is not None and int(aid) != int(cli_acct_id):
                return None

            rk = str(getattr(raw, "ratingKey", None) or "").strip()
            # History retains titles from playback time. Use the same current
            # metadata as incremental presence reads when this library item is
            # available, so targeted destination searches stay stable across runs.
            catalog_entry = cat.by_rk.get(rk) if scope_ok else None
            if (
                catalog_entry
                and (catalog_entry.get("title") or catalog_entry.get("series_title"))
                and (has_external_ids(catalog_entry.get("ids") or {}) or has_external_ids(catalog_entry.get("show_ids") or {}))
            ):
                meta = _catalog_entry_to_minimal(catalog_entry)
            else:
                meta = minimal_from_history_row(raw, token=None, allow_discover=False)
            if not meta and fallback_guid:
                meta = minimal_from_history_row(raw, token=None, allow_discover=True)
            if not meta:
                return None
            if not _keep_in_snapshot(adapter, meta):
                return None

            row = dict(meta)
            _force_episode_title(row)
            row["watched"] = True
            row["watched_at"] = _iso(ts_i)
            return f"{canonical_key(row)}@{ts_i}", row

        row_iter: Iterable[tuple[str, dict[str, Any]] | None]
        if workers > 1 and len(rows) > 1:
            executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="plex-history")
            try:
                row_iter = executor.map(_process_history_row, rows)
                for i, result in enumerate(row_iter, start=1):
                    if prog:
                        prog.tick(i, total=total)
                    if not result:
                        continue
                    key, row = result
                    out[key] = row
                    if limit and len(out) >= int(limit):
                        break
            finally:
                executor.shutdown(wait=True, cancel_futures=False)
                _fb_cache_flush()
        else:
            for i, raw in enumerate(rows, start=1):
                if prog:
                    prog.tick(i, total=total)
                result = _process_history_row(raw)
                if not result:
                    continue
                key, row = result
                out[key] = row
                if limit and len(out) >= int(limit):
                    break
            _fb_cache_flush()

        reconciled = history_complete and not limit and not maxresults and full_scopes == (set(allow) or {None})
        recorded = dict(playback_cache["items"]) if playback_cache and not reconciled else {}
        recorded.update(out)
        playback_cursor = max((_as_epoch(row.get("watched_at")) or 0 for row in recorded.values()), default=0)
        out = {}
        live_rows: dict[str, Mapping[str, Any] | None] = {}
        if include_marked and scope_ok:
            rating_keys = sorted({str((row.get("ids") or {}).get("plex")) for row in recorded.values()
                                  if (row.get("ids") or {}).get("plex")
                                  and str(row["ids"]["plex"]) not in cat.by_rk})
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="plex-history-state") as executor:
                fetched = executor.map(lambda rk: _pms_fetch_metadata_row(adapter, rk, strict=True), rating_keys)
                live_rows.update(zip(rating_keys, fetched))
        for saved in recorded.values():
            rk = str((saved.get("ids") or {}).get("plex") or "")
            entry = cat.by_rk.get(rk) if scope_ok else None
            if include_marked and scope_ok and rk:
                if entry is not None and not entry.get("watched"):
                    continue
                live_row = live_rows.get(rk)
                if live_row is not None and not _pms_row_is_watched(live_row):
                    continue
            if (entry and (entry.get("title") or entry.get("series_title"))
                    and (has_external_ids(entry.get("ids") or {}) or has_external_ids(entry.get("show_ids") or {}))):
                row = _catalog_entry_to_minimal(entry)
                row.update(watched=True, watched_at=saved["watched_at"])
                _force_episode_title(row)
            else:
                row = dict(saved)
            if _keep_in_snapshot(adapter, row):
                out[_event_key(row)] = row
                if limit and len(out) >= int(limit):
                    break

        base_present: set[str] = set()
        playback_rating_keys = {str((row.get("ids") or {}).get("plex")) for row in out.values()
                                if (row.get("ids") or {}).get("plex")}
        for r in out.values():
            try:
                base_present.add(canonical_key(r))
            except Exception:
                pass

        presence = cat.presence() if include_marked and scope_ok else {}
        presence_added = 0
        presence_dropped_keep = 0
        for key, row in presence.items():
            if limit and len(out) >= int(limit):
                break
            rk = str((row.get("ids") or {}).get("plex") or "")
            if rk and rk in playback_rating_keys:
                continue
            live_row = live_rows.get(rk)
            if live_row is not None and not _pms_row_is_watched(live_row):
                continue
            if key in out:
                continue
            try:
                base = canonical_key(row)
            except Exception:
                base = ""
            if base and base in base_present:
                continue
            if not _keep_in_snapshot(adapter, row):
                presence_dropped_keep += 1
                continue
            out[key] = row
            presence_added += 1
            if base:
                base_present.add(base)

        _emit({
            "event": "plex.presence", "action": "done", "feature": "history", "level": "debug",
            "force": bool(force), "presence_entries": len(presence),
            "presence_added": presence_added, "presence_dropped_keep": presence_dropped_keep,
            "snapshot_entries": len(out),
        })
        _maybe_trace_snapshot(cat, allow, out)

        if cache_path is not None and history_complete:
            cache_data = {"version": 1, "cursor": playback_cursor, "items": recorded,
                          "full_refreshed": now if reconciled else refreshed}
            if cache_data != playback_cache:
                write_json(cache_path, cache_data, indent=0, sort_keys=False, separators=(",", ":"))
            if reconciled:
                _prune_playback_caches(cache_path, now)

        if prog:
            prog.done(total=len(out), ok=True)

        _info(
            "index_done",
            count=len(out),
            workers=workers,
            include_marked=include_marked,
            scanned=total,
            token_acct_id=(cli_acct_id or 0),
            selected=(cfg_acct_id or cli_acct_id or 0),
            since=(eff_since or 0),
        )
        _emit({"event": "plex.snapshot", "action": "return", "feature": "history", "level": "info",
               "force": bool(force), "count": len(out)})
        return out

    finally:
        home_scope_exit(adapter, did_home_switch)

def _bump_reason(meta: dict[str, Any], reason: str) -> None:
    rc = meta.setdefault("reason_counts", {})
    rc[reason] = int(rc.get(reason, 0)) + 1


def _trace_key() -> str:
    return str(os.environ.get("CW_PLEX_TRACE_KEY", "") or "").strip().lower()


def _maybe_trace(cat: HistoryCatalog, item: Mapping[str, Any], *, strict: bool, shadow_ignored: bool) -> None:
    tk = _trace_key()
    if not tk:
        return
    try:
        if str(canonical_key(item) or "").strip().lower() != tk:
            return
        info = cat.trace(item, strict=strict)
        info["shadow_ignored_as_truth"] = bool(shadow_ignored)
        _emit({"event": "plex.trace", "action": "history", "feature": "history", "level": "info", **info})
    except Exception:
        pass


def _parse_trace_item(tk: str) -> dict[str, Any] | None:
    m = re.match(r"^(?P<prefix>[a-z]+):(?P<id>[^#]+)(#s(?P<s>\d+)e(?P<e>\d+))?$", tk)
    if not m:
        return None
    prefix, idv, s, e = m.group("prefix"), m.group("id"), m.group("s"), m.group("e")
    if s is not None and e is not None:
        return {"type": "episode", "show_ids": {prefix: idv}, "season": int(s), "episode": int(e)}
    return {"type": "movie", "ids": {prefix: idv}}


def _maybe_trace_snapshot(cat: HistoryCatalog, allow: set[str], snapshot: Mapping[str, Any]) -> None:
    tk = _trace_key()
    if not tk:
        return
    try:
        item = _parse_trace_item(tk)
        if not item:
            return
        info = cat.trace(item, strict=False)
        info["selected_libraries"] = sorted(str(x) for x in (allow or set()))
        show_toks = _item_show_tokens(item)
        info["show_tokens"] = sorted(show_toks)
        info["show_token_in_catalog"] = bool(show_toks and cat._has_show(show_toks))
        s, ep = _item_se(item)
        if s is not None and ep is not None:
            rks: set[str] = set()
            for tok in show_toks:
                rks |= cat.episode_index.get((tok, int(s), int(ep)), set())
            info["episode_in_index"] = bool(rks)
            info["matched_episode_rk"] = sorted(rks)
        pres = cat.presence()
        info["included_in_presence"] = any(str(k).split("@", 1)[0].lower() == tk for k in pres)
        skey = next((k for k in snapshot if str(k).split("@", 1)[0].lower() == tk), None)
        info["in_snapshot"] = skey is not None
        info["snapshot_key"] = skey
        info["dropped_by_keep_in_snapshot"] = bool(info["included_in_presence"] and skey is None)
        _emit({"event": "plex.trace", "action": "snapshot", "feature": "history", "level": "info", **info})
    except Exception:
        pass


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    need_home_scope, did_home_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    meta = _new_write_meta()
    _set_write_meta(adapter, meta)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            unresolved: list[dict[str, Any]] = []
            for item in items or []:
                k = canonical_key(item) or ""
                unresolved.append({"item": id_minimal(item), "key": k, "hint": "no_plex_server", "reason": "no_plex_server"})
                meta["unresolved_keys"].append(k)
                _bump_reason(meta, "no_plex_server")
            _set_write_meta(adapter, meta)
            _info("write_skipped", op="add", reason="no_server")
            return 0, unresolved

        if need_home_scope and not did_home_switch:
            _info("write_skipped", op="add", reason="home_scope_not_applied", selected=(sel_aid or sel_uname))
            unresolved = []
            for item in items or []:
                k = canonical_key(item) or ""
                unresolved.append({"item": id_minimal(item), "key": k, "hint": "home_scope_not_applied", "reason": "home_scope_not_applied"})
                meta["unresolved_keys"].append(k)
                _bump_reason(meta, "home_scope_not_applied")
            _set_write_meta(adapter, meta)
            return 0, unresolved

        allow = plex_feature_library_ids(adapter, "history")
        write_strict = True
        force = _history_force_full(adapter)
        cat = _get_history_catalog(adapter, allow, force=force)
        tol = _DATE_TOLERANCE_SEC

        ok = 0
        unresolved: list[dict[str, Any]] = []
        shadow_batch: list[Mapping[str, Any]] = []
        to_scrobble: list[tuple[Mapping[str, Any], str, str, int]] = []
        episode_catalog_filled = False

        def _ensure_episode_catalog() -> None:
            nonlocal episode_catalog_filled
            if episode_catalog_filled:
                return
            episode_catalog_filled = True
            if _populate_catalog_episode_leaves(adapter, allow, cat):
                _store_history_catalog(adapter, allow, cat)

        item_list = list(items or [])
        resolve_progress = _WriteProgress("resolve", len(item_list))
        resolve_progress.tick(0, force=True)

        for resolved_n, item in enumerate(item_list, 1):
            resolve_progress.tick(resolved_n)
            key = canonical_key(item) or ""
            ts = _as_epoch(item.get("watched_at"))
            if not ts:
                unresolved.append({"item": id_minimal(item), "key": key, "hint": "missing_watched_at", "reason": "missing_watched_at"})
                meta["unresolved_keys"].append(key)
                _bump_reason(meta, "missing_watched_at")
                continue

            rk, klass = cat.resolve(item, strict=write_strict)
            _maybe_trace(cat, item, strict=write_strict, shadow_ignored=True)

            if klass == CLASS_IN_CATALOG_WATCHED and rk:
                entry = cat.by_rk.get(rk) or {}
                if entry.get("write_accepted"):
                    meta["accepted_keys"].append(key)
                    meta["accepted_not_seen_live_keys"].append(key)
                    ok += 1
                    continue
                ds, _delta = _date_status(int(ts), entry.get("last_viewed_at"), tol)
                meta["accepted_keys"].append(key)
                meta["presence_confirmed_keys"].append(key)
                meta["live_confirmed_keys"].append(key)
                if ds == DATE_MISMATCH:
                    meta["date_mismatch_keys"].append(key)
                elif ds == DATE_EXACT:
                    meta["date_confirmed_keys"].append(key)
                ok += 1
                continue

            if not rk and klass == CLASS_SHOW_MATCHED_EPISODE_MISSING:
                _ensure_episode_catalog()
                rk, klass = cat.resolve(item, strict=write_strict)

            if not rk and klass == CLASS_RESOLVE_AMBIGUOUS:
                rk = _resolve_rating_key(adapter, item, strict=write_strict)

            if not rk:
                reason = klass if klass in (
                    CLASS_SHOW_MATCHED_EPISODE_MISSING, CLASS_NOT_IN_PLEX_CATALOG, CLASS_RESOLVE_AMBIGUOUS
                ) else CLASS_NOT_IN_PLEX_CATALOG
                unresolved.append({"item": id_minimal(item), "key": key, "hint": reason, "reason": reason})
                meta["unresolved_keys"].append(key)
                _bump_reason(meta, reason)
                continue

            to_scrobble.append((item, key, str(rk), int(ts)))

        resolve_progress.tick(len(item_list), force=True)

        write_workers = 0
        write_ms = 0
        if to_scrobble:
            write_workers = plex_worker_count(adapter, "history_workers", "CW_PLEX_HISTORY_WORKERS", 12)
            _ensure_session_pool(srv, write_workers)
            write_progress = _WriteProgress("scrobble", len(to_scrobble))
            write_progress.tick(0, force=True)
            _t_write = time.time()
            if write_workers > 1 and len(to_scrobble) > 1:
                scrobble_ok = []
                with ThreadPoolExecutor(max_workers=write_workers, thread_name_prefix="plex-scrobble") as ex:
                    for done_n, success in enumerate(
                        ex.map(lambda t: _scrobble_with_date(srv, t[2], t[3]), to_scrobble), 1
                    ):
                        scrobble_ok.append(success)
                        write_progress.tick(done_n)
            else:
                scrobble_ok = []
                for done_n, (_item, _key, rk, ts) in enumerate(to_scrobble, 1):
                    scrobble_ok.append(_scrobble_with_date(srv, rk, ts))
                    write_progress.tick(done_n)
            write_progress.tick(len(to_scrobble), force=True)
            write_ms = int((time.time() - _t_write) * 1000)

            for (item, key, _rk, _ts), success in zip(to_scrobble, scrobble_ok):
                if success:
                    ok += 1
                    meta["accepted_keys"].append(key)
                    meta["accepted_not_seen_live_keys"].append(key)
                    shadow_batch.append(item)
                    entry = cat.by_rk.get(_rk)
                    if entry is not None:
                        entry["watched"] = True
                        entry["write_accepted"] = True
                        entry["last_viewed_at"] = None
                else:
                    unresolved.append({"item": id_minimal(item), "key": key, "hint": "scrobble_failed", "reason": DATE_WRITE_FAILED})
                    meta["unresolved_keys"].append(key)
                    _bump_reason(meta, DATE_WRITE_FAILED)

        _shadow_add_batch(shadow_batch)
        _set_write_meta(adapter, meta)
        _emit({
            "event": "plex.write", "action": "summary", "feature": "history", "level": "info",
            "attempted": ok + len(unresolved), "accepted": len(meta["accepted_keys"]),
            "presence_confirmed": len(meta["presence_confirmed_keys"]),
            "accepted_not_seen_live": len(meta["accepted_not_seen_live_keys"]),
            "unresolved": len(unresolved), "reason_counts": dict(meta["reason_counts"]),
            "scrobbled": len(to_scrobble), "workers": write_workers, "duration_ms": write_ms,
        })
        _info("write_done", op="add", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
        return ok, unresolved

    finally:
        home_scope_exit(adapter, did_home_switch)

def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    need_home_scope, did_home_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            unresolved: list[dict[str, Any]] = []
            for item in items or []:
                unresolved.append({"item": id_minimal(item), "key": canonical_key(item) or "", "hint": "no_plex_server", "reason": "no_plex_server"})
            _info("write_skipped", op="remove", reason="no_server")
            return 0, unresolved

        if need_home_scope and not did_home_switch:
            _info("write_skipped", op="remove", reason="home_scope_not_applied", selected=(sel_aid or sel_uname))
            unresolved = []
            for item in items or []:
                unresolved.append({"item": id_minimal(item), "key": canonical_key(item) or "", "hint": "home_scope_not_applied", "reason": "home_scope_not_applied"})
            return 0, unresolved

        ok = 0
        unresolved: list[dict[str, Any]] = []
        for item in items or []:
            key = canonical_key(item) or ""
            rating_key = _resolve_rating_key(adapter, item)
            if not rating_key:
                unresolved.append({"item": id_minimal(item), "key": key, "hint": "not_in_library", "reason": "not_in_library"})
                continue
            if _unscrobble(srv, rating_key):
                ok += 1
                _shadow_remove(item)
                cached = getattr(_INDEX_CACHE, "catalog", None)
                allow = plex_feature_library_ids(adapter, "history")
                if cached is not None and cached["key"] == _catalog_cache_key(adapter, allow):
                    entry = cached["cat"].by_rk.get(str(rating_key))
                    if entry is not None:
                        entry["watched"] = False
                        entry.pop("write_accepted", None)
                        entry["last_viewed_at"] = None
            else:
                unresolved.append({"item": id_minimal(item), "key": key, "hint": "unscrobble_failed", "reason": "unscrobble_failed"})
        _info("write_done", op="remove", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
        return ok, unresolved

    finally:
        home_scope_exit(adapter, did_home_switch)

def _has_matchable_ids(ids: Mapping[str, Any]) -> bool:
    return has_external_ids(ids)

def _resolve_rating_key(adapter: Any, item: Mapping[str, Any], *, strict: bool | None = None) -> str | None:
    ids = ids_from(item)
    show_ids = extract_show_ids(item)
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return None

    allow = plex_feature_library_ids(adapter, "history")
    rk = ids.get("plex") or None
    if rk:
        try:
            obj_rk = srv.fetchItem(int(rk))
            if obj_rk and section_allowed(obj_rk, allow) and native_item_matches(obj_rk, item):
                return str(rk)
        except Exception:
            pass

    kind = (item.get("type") or "movie").lower()
    if kind == "anime":
        kind = "episode"
    is_episode = kind == "episode"

    strict = bool(plex_cfg_get(adapter, "strict_id_matching", False)) if strict is None else bool(strict)

    season = item.get("season") if item.get("season") is not None else item.get("season_number")
    episode = item.get("episode") if item.get("episode") is not None else item.get("episode_number")

    guids = item_guid_candidates(ids, show_ids, item)

    if strict:
        if not (guids or _has_matchable_ids(ids) or _has_matchable_ids(show_ids)):
            return None

        if is_episode:
            rk_show = show_ids.get("plex")
            if rk_show and _has_matchable_ids(show_ids):
                try:
                    obj0 = srv.fetchItem(int(rk_show))
                except Exception:
                    obj0 = None
                if (obj0 and section_allowed(obj0, allow)
                        and native_item_matches(obj0, {"type": "show", "ids": show_ids})):
                    rk0 = episode_rating_key_from_show(obj0, season, episode)
                    if rk0:
                        return rk0

            obj = resolve_obj_by_guids(srv, guids, allow, {"episode"})
            if obj and native_item_matches(obj, item):
                return str(getattr(obj, "ratingKey", None) or "")
            obj2 = resolve_obj_by_guids(srv, guids, allow, {"show", "season"})
            if obj2:
                rk2 = episode_rating_key_from_show(obj2, season, episode)
                return rk2

            _build_guid_index(adapter, allow)
            rk_show_g = _pms_find_in_guid_index("show", guids)
            if rk_show_g:
                try:
                    obj_g = srv.fetchItem(int(rk_show_g))
                except Exception:
                    obj_g = None
                if obj_g and section_allowed(obj_g, allow):
                    rk_g = episode_rating_key_from_show(obj_g, season, episode)
                    if rk_g:
                        return rk_g
            return None

        obj = resolve_obj_by_guids(srv, guids, allow, {"movie"})
        if obj:
            return str(getattr(obj, "ratingKey", None) or "")
        _build_guid_index(adapter, allow)
        rk_movie_g = _pms_find_in_guid_index("movie", guids)
        if rk_movie_g:
            if not allow:
                return str(rk_movie_g)
            try:
                obj_g = srv.fetchItem(int(rk_movie_g))
            except Exception:
                obj_g = None
            if obj_g and section_allowed(obj_g, allow):
                return str(getattr(obj_g, "ratingKey", None) or "")
        return None

    title = (item.get("title") or "").strip()
    series_title = (item.get("series_title") or "").strip()
    query_title = series_title if is_episode and series_title else title
    year = item.get("year")

    if not (query_title or guids):
        return None

    sec_types = ("show",) if is_episode else ("movie",)
    hits: list[Any] = []

    obj = resolve_obj_by_guids(srv, guids, allow, {"movie", "episode", "show", "season"})
    if obj:
        hits.append(obj)

    if not hits and query_title:
        for sec in adapter.libraries(types=sec_types) or []:
            section_id = str(getattr(sec, "key", "")).strip()
            if allow and section_id not in allow:
                continue
            try:
                search_hits = sec.search(title=query_title) or []
                if len(search_hits) == 1:
                    hits.extend(search_hits)
                    break
                hits.extend(search_hits)
            except Exception:
                continue

    if not hits and query_title:
        try:
            mediatype = "episode" if is_episode else "movie"
            search_hits = srv.search(query_title, mediatype=mediatype) or []
            for obj in search_hits:
                if section_allowed(obj, allow):
                    hits.append(obj)
        except Exception:
            pass

    def _score(obj: Any) -> int:
        score = 0
        try:
            obj_title = (getattr(obj, "grandparentTitle", None) if is_episode else getattr(obj, "title", None)) or ""
            if obj_title.strip().lower() == query_title.lower():
                score += 3
            if not is_episode and year is not None and getattr(obj, "year", None) == year:
                score += 2
            if is_episode:
                s_ok = season is None or getattr(obj, "seasonNumber", None) == season or getattr(obj, "parentIndex", None) == season
                e_ok = episode is None or getattr(obj, "index", None) == episode
                if s_ok and e_ok:
                    score += 2
            meta_ids = (plex_normalize(obj).get("ids") or {})
            for key in ("tmdb", "imdb", "tvdb"):
                if key in meta_ids and key in ids and meta_ids[key] == ids[key]:
                    score += 4
                if key in meta_ids and key in show_ids and meta_ids[key] == show_ids[key]:
                    score += 2
        except Exception:
            pass
        return score

    if not hits:
        return None

    if is_episode:
        ep_hits = [o for o in hits if object_type(o) == "episode"]
        if ep_hits:
            best_ep = max(ep_hits, key=_score)
            rk_val = getattr(best_ep, "ratingKey", None)
            return str(rk_val) if rk_val else None
        show_hits = [o for o in hits if object_type(o) in ("show", "season")]
        for show in show_hits:
            rk_val = episode_rating_key_from_show(show, season, episode)
            if rk_val:
                return rk_val
        return None

    best = max(hits, key=_score)
    rk_val = getattr(best, "ratingKey", None)
    return str(rk_val) if rk_val else None


def _ensure_session_pool(srv: Any, workers: int) -> None:
    try:
        ses = getattr(srv, "_session", None)
        if ses is None:
            return
        want = max(10, int(workers or 0) + 2)
        if int(getattr(ses, "_cw_pool_size", 0) or 0) >= want:
            return
        from requests.adapters import HTTPAdapter
        for scheme in ("https://", "http://"):
            try:
                ses.mount(scheme, HTTPAdapter(pool_connections=want, pool_maxsize=want, max_retries=0))
            except Exception:
                continue
        setattr(ses, "_cw_pool_size", want)
    except Exception:
        pass


def _scrobble_with_date(srv: Any, rating_key: Any, epoch: int) -> bool:
    try:
        base = _as_base_url(srv)
        ses = getattr(srv, "_session", None)
        tok = active_pms_token(srv)
        if not (base and ses and tok):
            return False

        url = f"{base}/:/scrobble"
        headers = dict(getattr(ses, "headers", {}) or {})
        headers.update(plex_headers(tok))

        for key_name in ("key", "ratingKey"):
            params = {key_name: int(rating_key), "identifier": "com.plexapp.plugins.library", "viewedAt": int(epoch)}
            try:
                resp = ses.get(url, params=params, headers=headers, timeout=10)
            except Exception as e:
                _warn("http_failed", op="scrobble", rating_key=str(rating_key), error=str(e))
                continue

            # Plex often returns 200 before the library reflects the new view state. At least that is what i hope...
            if resp.ok:
                return True

            _warn("write_failed", op="scrobble", rating_key=str(rating_key), status=resp.status_code, body_snippet=(resp.text or "")[:200].replace("\n", " "))

        try:
            obj = srv.fetchItem(int(rating_key))
            obj_type = (getattr(obj, "type", "") or "").lower()
            if obj is not None and (not obj_type or obj_type in ("episode", "movie")):
                obj.markWatched()
                return True
        except Exception:
            pass

        return False

    except Exception as e:
        _warn("write_failed", op="scrobble", rating_key=str(rating_key), error=str(e))
        return False

def _unscrobble(srv: Any, rating_key: Any) -> bool:
    try:
        token = active_pms_token(srv)
        if not token:
            return False
        url = srv.url("/:/unscrobble")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library"}
        resp = srv._session.get(url, params=params, headers=plex_headers(token), timeout=10)
        return resp.ok
    except Exception:
        return False
