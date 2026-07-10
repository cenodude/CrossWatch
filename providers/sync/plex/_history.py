# /providers/sync/plex/_history.py
# Plex Module for history synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from pathlib import Path

from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from, ids_from_guid

from ._common import (
    _as_base_url,
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
    meta_guids,
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

def _marked_state_path() -> Path:
    return state_file("plex_history.marked_watched.json")

def _load_marked_state() -> dict[str, Any]:
    return read_json(_marked_state_path())

def _save_marked_state(data: Mapping[str, Any]) -> None:
    try:
        write_json(_marked_state_path(), data, indent=0, sort_keys=False, separators=(",", ":"))
    except Exception:
        pass



def _watermark_path() -> Path:
    return state_file("plex_history.watermark.json")

def _wm_key(acct_id: int, uname: str) -> str:
    if acct_id:
        return f"acct:{acct_id}"
    if uname:
        return f"user:{uname.lower()}"
    return "default"

def _load_watermark(key: str) -> int | None:
    try:
        data = read_json(_watermark_path()) or {}
        by_user = data.get("by_user") or {}
        v = by_user.get(key)
        return int(v) if v else None
    except Exception:
        return None

def _save_watermark(key: str, epoch: int) -> None:
    try:
        path = _watermark_path()
        data = read_json(path) or {}
        by_user = dict(data.get("by_user") or {})
        cur = int(by_user.get(key) or 0)
        epoch_i = int(epoch or 0)
        if epoch_i <= 0 or epoch_i <= cur:
            return
        by_user[key] = epoch_i
        out = {"by_user": by_user, "updated_at": _iso(epoch_i)}
        write_json(path, out, indent=0, sort_keys=False, separators=(",", ":"))
    except Exception:
        pass

def _guid_index_path() -> Path:
    return state_file("plex_history.guid_index.json")

def _load_guid_index(srv: Any, allow: set[str]) -> bool:
    try:
        data = read_json(_guid_index_path()) or {}
        mid = str(getattr(srv, "machineIdentifier", "") or "")
        if not mid or data.get("machine_id") != mid:
            return False
        stored_allow = set(str(x) for x in (data.get("allow") or []))
        if stored_allow != set(str(x) for x in (allow or set())):
            return False
        # TTL to avoid stale indices forever.
        ttl_days = int(os.environ.get("CW_PLEX_GUID_INDEX_TTL_DAYS", "0") or "7")
        created = int(data.get("created_epoch") or 0)
        if created and ttl_days > 0 and (int(time.time()) - created) > ttl_days * 86400:
            return False
        movies = data.get("movies") or {}
        shows = data.get("shows") or {}
        if not isinstance(movies, dict) or not isinstance(shows, dict):
            return False
        _GUID_INDEX_MOVIE.update({str(k): str(v) for k, v in movies.items() if k and v})
        _GUID_INDEX_SHOW.update({str(k): str(v) for k, v in shows.items() if k and v})
        return bool(_GUID_INDEX_MOVIE or _GUID_INDEX_SHOW)
    except Exception:
        return False

def _save_guid_index(srv: Any, allow: set[str]) -> None:
    try:
        mid = str(getattr(srv, "machineIdentifier", "") or "")
        if not mid:
            return
        out = {
            "machine_id": mid,
            "allow": sorted(str(x) for x in (allow or set())),
            "created_epoch": int(time.time()),
            "movies": _GUID_INDEX_MOVIE,
            "shows": _GUID_INDEX_SHOW,
        }
        write_json(_guid_index_path(), out, indent=0, sort_keys=False, separators=(",", ":"))
    except Exception:
        pass
_dbg, _info, _warn, _error, _log = make_logger("history")


# PMS GUID index cache (used for strict ID matching).
_GUID_INDEX_MOVIE: dict[str, str] = {}
_GUID_INDEX_SHOW: dict[str, str] = {}
_GUID_INDEX_KEY: str | None = None


def _guid_index_key(srv: Any, allow: set[str]) -> str:
    mid = str(getattr(srv, "machineIdentifier", "") or "").strip()
    libs = ",".join(sorted(str(x) for x in (allow or set())))
    return f"{mid}|{libs}"


def _clear_guid_index() -> None:
    global _GUID_INDEX_KEY
    _GUID_INDEX_MOVIE.clear()
    _GUID_INDEX_SHOW.clear()
    _GUID_INDEX_KEY = None


def _build_guid_index(adapter: Any, allow: set[str], *, force: bool = False) -> None:
    global _GUID_INDEX_KEY
    srv = getattr(getattr(adapter, "client", None), "server", None)
    key = _guid_index_key(srv, allow)
    if (not force) and _GUID_INDEX_KEY == key and (_GUID_INDEX_MOVIE or _GUID_INDEX_SHOW):
        return
    _clear_guid_index()
    if (not force) and srv and _load_guid_index(srv, allow):
        _GUID_INDEX_KEY = key
        _dbg("index_cache_hit", source="guid_index", movies=len(_GUID_INDEX_MOVIE), shows=len(_GUID_INDEX_SHOW))
        return
    try:
        for sec in adapter.libraries(types=("movie", "show")) or []:
            sid = str(getattr(sec, "key", "") or "").strip()
            if allow and sid and sid not in allow:
                continue
            libtype = "movie" if getattr(sec, "type", "") == "movie" else "show"
            dst = _GUID_INDEX_MOVIE if libtype == "movie" else _GUID_INDEX_SHOW
            try:
                for obj in (sec.all() or []):
                    try:
                        rk = str(getattr(obj, "ratingKey", "") or "").strip()
                        if not rk:
                            continue
                        for g in meta_guids(obj):
                            gg = str(g or "").strip().lower()
                            if gg and gg not in dst:
                                dst[gg] = rk
                    except Exception:
                        continue
            except Exception:
                continue
        if srv:
            _save_guid_index(srv, allow)
        _GUID_INDEX_KEY = key
        _dbg("index_fetch_counts", source="guid_index", movies=len(_GUID_INDEX_MOVIE), shows=len(_GUID_INDEX_SHOW))
    except Exception:
        pass

def _pms_find_in_guid_index(libtype: str, candidates: list[str]) -> str | None:
    src = _GUID_INDEX_SHOW if libtype == "show" else _GUID_INDEX_MOVIE
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
_FALLBACK_EMPTY_PAGES_DEFAULT = 3
_CATALOG_MEM_TTL_SEC = 90


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name, "") or "").strip().lower() in ("1", "true", "yes", "on")


def _fallback_empty_pages() -> int:
    try:
        return max(1, int(os.environ.get("CW_PLEX_WATCHED_FALLBACK_EMPTY_PAGES", "") or _FALLBACK_EMPTY_PAGES_DEFAULT))
    except Exception:
        return _FALLBACK_EMPTY_PAGES_DEFAULT


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
    return toks


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
    __slots__ = ("by_rk", "movie_tokens", "show_tokens", "episode_index", "movie_title_year")

    def __init__(self) -> None:
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

def _marked_set_unwatched(rating_key: Any, item: Mapping[str, Any]) -> None:
    try:
        rk = str(rating_key or ids_from(item).get("plex") or "").strip()
        if not rk:
            return
        st = _load_marked_state() or {}
        marked0 = st.get("items") or {}
        marked: dict[str, Any] = dict(marked0) if isinstance(marked0, Mapping) else {}
        prev = marked.get(rk)
        entry: dict[str, Any] = dict(prev) if isinstance(prev, Mapping) else dict(id_minimal(item))
        entry["watched"] = False
        marked[rk] = entry
        st = dict(st) if isinstance(st, Mapping) else {}
        st["items"] = marked
        st["last_updated_at"] = int(time.time())
        _save_marked_state(st)
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

def _iter_marked_watched_from_library(
    adapter: Any,
    allow: set[str],
    since: int | None = None,
    *,
    full: bool = False,
) -> list[tuple[dict[str, Any], int]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return []
    base = _as_base_url(srv)
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token):
        return []

    state = _load_marked_state()
    try:
        last_ts = int((state.get("last_ts") if isinstance(state, dict) else 0) or 0)
    except Exception:
        last_ts = 0
    cutoff = 0 if full else (max(int(since or 0), last_ts) if (since is not None or last_ts) else 0)

    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"

    def _rows_from(r: Any) -> tuple[list[Mapping[str, Any]], int | None]:
        try:
            ctype = (r.headers.get("content-type") or "").lower()
            data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
            mc = data.get("MediaContainer") or {}
            rows = mc.get("Metadata") or []
            total = mc.get("totalSize")
            total_i = int(total) if total is not None else None
            return [x for x in rows if isinstance(x, Mapping)], total_i
        except Exception:
            return [], None

    def _int0(v: Any) -> int:
        try:
            return int(v or 0)
        except Exception:
            return 0

    page_size = 200
    results: list[tuple[dict[str, Any], int]] = []
    newest = last_ts
    summary = {
        "sections_scanned": 0, "movie_sections": 0, "show_sections": 0,
        "watched_rows_seen": 0, "watched_rows_returned": 0,
        "skipped_no_timestamp": 0, "skipped_no_rating_key": 0, "skipped_normalize_failed": 0,
    }

    try:
        sections = list(adapter.libraries(types=("movie", "show")) or [])
    except Exception:
        sections = []

    def _scan_section(section_id: str, plex_type: int, section_type: str, section_title: str, *, use_unwatched_filter: bool) -> int:
        nonlocal newest
        start = 0
        seen_pages: set[tuple[str, ...]] = set()
        sec = {"seen": 0, "vc": 0, "lva": 0, "no_ts": 0, "norm_fail": 0, "no_rk": 0, "ret": 0}
        last_status: Any = None
        last_total: Any = None
        empty_streak = 0
        while True:
            params: dict[str, Any] = {
                "type": plex_type,
                "sort": "lastViewedAt:desc",
                "includeGuids": 1,
                "X-Plex-Container-Start": start,
                "X-Plex-Container-Size": page_size,
            }
            if use_unwatched_filter:
                params["unwatched"] = 0
            try:
                r = ses.get(f"{base}/library/sections/{section_id}/all", params=params, headers=headers, timeout=15)
            except Exception:
                break
            last_status = getattr(r, "status_code", None)
            if not getattr(r, "ok", False):
                break
            rows, total = _rows_from(r)
            last_total = total
            if not rows:
                break
            signature = tuple(str(row.get("ratingKey") or row.get("key") or "") for row in rows)
            if signature in seen_pages:
                break
            seen_pages.add(signature)

            stop = False
            page_watched = 0
            for row in rows:
                view_count = max(_int0(row.get("viewCount")), _int0(row.get("leafCountViewed")))
                ts = _as_epoch(row.get("lastViewedAt") or row.get("viewedAt"))
                watched = view_count > 0 or bool(ts)
                if not watched:
                    continue
                page_watched += 1
                sec["seen"] += 1
                if view_count > 0:
                    sec["vc"] += 1
                if ts:
                    sec["lva"] += 1
                ts_i = int(ts) if ts else 0
                if (not full) and cutoff and ts_i and ts_i < cutoff:
                    stop = True
                    break
                if ts_i and ts_i > newest:
                    newest = ts_i
                meta = normalize_discover_row(row, token=token) or {}
                if not meta:
                    sec["norm_fail"] += 1
                    continue
                if not (meta.get("ids") or {}).get("plex"):
                    sec["no_rk"] += 1
                    continue
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

            if stop:
                break
            start += len(rows)
            if total is not None and start >= total:
                break
            if len(rows) < page_size:
                break
            if not use_unwatched_filter:
                empty_streak = empty_streak + 1 if page_watched == 0 else 0
                if empty_streak >= _fallback_empty_pages():
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

        ret = _scan_section(section_id, plex_type, section_type, section_title, use_unwatched_filter=True)
        if ret == 0 and plex_type == 4:
            _scan_section(section_id, plex_type, section_type, section_title, use_unwatched_filter=False)

    if newest and newest != last_ts:
        try:
            st = dict(state) if isinstance(state, dict) else {}
            st["last_ts"] = newest
            _save_marked_state(st)
        except Exception:
            pass

    _emit({
        "event": "plex.presence", "action": "live_watched_summary", "feature": "history",
        "level": "debug", "full": bool(full), **summary,
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


def _iter_live_watched(adapter: Any, allow: set[str], *, full: bool = True) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for meta, ts in _iter_marked_watched_from_library(adapter, allow, full=full):
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
            for row in rows:
                meta = normalize_discover_row(row, token=token) or {}
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
    _build_guid_index(adapter, allow, force=force)
    for guid, rk in list(_GUID_INDEX_MOVIE.items()):
        ids = ids_from_guid(str(guid))
        if ids:
            cat.add({"rk": rk, "type": "movie", "ids": ids, "watched": False})
    for guid, rk in list(_GUID_INDEX_SHOW.items()):
        ids = ids_from_guid(str(guid))
        if ids:
            cat.add({"rk": rk, "type": "show", "ids": ids, "watched": False})
    watched_movies = 0
    watched_eps = 0
    if live:
        for entry in _iter_live_watched(adapter, allow, full=True):
            cat.add(entry)
            if (entry.get("type") or "") == "episode":
                watched_eps += 1
            else:
                watched_movies += 1
    _emit({
        "event": "plex.catalog", "action": "done", "feature": "history", "level": "debug",
        "force": bool(force), "live": bool(live), "allow": allow_list,
        "guid_movies": len(_GUID_INDEX_MOVIE), "guid_shows": len(_GUID_INDEX_SHOW),
        "watched_movies": watched_movies, "watched_episodes": watched_eps,
        "catalog_entries": len(cat.by_rk), "duration_ms": int((time.time() - t0) * 1000),
    })
    return cat


_CATALOG_CACHE: dict[str, Any] = {"cat": None, "ts": 0.0, "key": None}


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


def _catalog_cache_key(adapter: Any, allow: set[str]) -> str:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    mid = str(getattr(srv, "machineIdentifier", "") or "")
    return f"{mid}|{_user_scope_key(adapter)}|{','.join(sorted(str(x) for x in (allow or set())))}"


def _store_history_catalog(adapter: Any, allow: set[str], cat: HistoryCatalog) -> None:
    _CATALOG_CACHE.update({"cat": cat, "ts": time.time(), "key": _catalog_cache_key(adapter, allow)})


def _get_history_catalog(adapter: Any, allow: set[str], *, force: bool = False) -> HistoryCatalog:
    key = _catalog_cache_key(adapter, allow)
    now = time.time()
    if (not force) and _CATALOG_CACHE.get("cat") is not None and _CATALOG_CACHE.get("key") == key \
            and (now - float(_CATALOG_CACHE.get("ts") or 0)) < _CATALOG_MEM_TTL_SEC:
        return _CATALOG_CACHE["cat"]  # type: ignore[return-value]
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

def _pms_fetch_metadata_row(adapter: Any, rating_key: str) -> Mapping[str, Any] | None:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return None
    base = _as_base_url(srv)
    ses = getattr(srv, "_session", None)
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    if not (base and ses and token and rating_key):
        return None
    headers = dict(getattr(ses, "headers", {}) or {})
    headers.update(plex_headers(token))
    headers["Accept"] = "application/json"
    try:
        r = ses.get(f"{base}/library/metadata/{rating_key}", headers=headers, timeout=15)
    except Exception:
        return None
    if not getattr(r, "ok", False):
        return None
    try:
        ctype = (r.headers.get("content-type") or "").lower()
        data = (r.json() or {}) if "application/json" in ctype else _xml_to_container(r.text or "")
        mc = data.get("MediaContainer") or {}
        rows = mc.get("Metadata") or []
        if isinstance(rows, list) and rows and isinstance(rows[0], Mapping):
            return rows[0]
    except Exception:
        return None
    return None


def _pms_row_is_watched(row: Mapping[str, Any]) -> bool:
    try:
        vc = row.get("viewCount")
        if vc is None:
            vc = row.get("leafCountViewed")
        return int(vc or 0) > 0
    except Exception:
        return False


def _pms_row_watched_ts(row: Mapping[str, Any]) -> int | None:
    return _as_epoch(row.get("lastViewedAt") or row.get("viewedAt"))
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
        wm = _load_watermark(wm_key) if (since is None or int(since or 0) <= 0) else None
                # Treat cursors as *exclusive* to avoid re-reading the boundary event forever.
        if since is not None and int(since or 0) > 0:
            eff_since = int(since) + 1
        elif wm:
            eff_since = int(wm) + 1
        else:
            eff_since = None

        allow = plex_feature_library_ids(adapter, "history")
        explicit_user = bool(cfg_acct_id or cfg_uname)

        force = bool(force) or _history_force_full(adapter)
        if force:
            wm = None
            eff_since = None

        scope_ok = not (need_home_scope and not did_home_switch)
        if not scope_ok:
            _warn("home_scope_not_applied", op="build_index", selected=(sel_aid or sel_uname))

        cat = _build_history_catalog(adapter, allow, force=force, live=scope_ok)
        if scope_ok:
            _store_history_catalog(adapter, allow, cat)

        # Optional cursor debugging (helps diagnose 1-item re-add loops).
        if str(os.environ.get("CW_PLEX_HISTORY_DEBUG_CURSOR", "")).strip().lower() in ("1", "true", "yes"):
            _dbg(
                "cursor",
                since_arg=int(since or 0) if since is not None else None,
                wm=int(wm or 0) if wm else None,
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

        maxresults = _int_or_zero(_history_cfg_get(adapter, "maxresults", 0))
        if maxresults:
            base_kwargs["maxresults"] = int(maxresults)

        def _call_history(**kwargs: Any) -> list[Any]:
            try:
                return list(srv.history(**kwargs) or [])
            except Exception as e:
                if "mindate" in kwargs:
                    _dbg("mindate_fallback_drop", error=str(e))
                    kwargs.pop("mindate", None)
                    return list(srv.history(**kwargs) or [])
                raise

        rows: list[Any] = []
        try:
            if allow:
                for sid in sorted(allow):
                    try:
                        kw = dict(base_kwargs)
                        kw["librarySectionID"] = int(sid)
                        part = _call_history(**kw)
                    except Exception:
                        part = []
                    if not part and "accountID" in kw and not explicit_user:
                        try:
                            kw2 = dict(kw)
                            kw2.pop("accountID", None)
                            part = _call_history(**kw2)
                        except Exception:
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
            rows = []

        total = len(rows)
        max_seen = 0
        workers = plex_worker_count(adapter, "history_workers", "CW_PLEX_HISTORY_WORKERS", 12)
        include_marked = bool(_history_cfg_get(adapter, "include_marked_watched", True))

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
            ts = _epoch_from_history_entry(raw)
            if not ts:
                return None
            ts_i = int(ts)

            if eff_since is not None and ts_i < int(eff_since):
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

            meta = minimal_from_history_row(raw, token=None, allow_discover=False)
            if not meta and fallback_guid:
                meta = minimal_from_history_row(raw, token=None, allow_discover=True)
            if not meta:
                return None
            if not _keep_in_snapshot(adapter, meta):
                return None

            if include_marked:
                rk = str((meta.get("ids") or {}).get("plex") or getattr(raw, "ratingKey", None) or "").strip()
                if rk:
                    live_row = _pms_fetch_metadata_row(adapter, rk)
                    if live_row is not None:
                        if not _pms_row_is_watched(live_row):
                            return None
                        live_ts = _pms_row_watched_ts(live_row)
                        if live_ts:
                            ts_i = int(live_ts)

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

        base_present: set[str] = set()
        for r in out.values():
            try:
                base_present.add(canonical_key(r))
            except Exception:
                pass

        presence = cat.presence()
        presence_added = 0
        presence_dropped_keep = 0
        for key, row in presence.items():
            if limit and len(out) >= int(limit):
                break
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

        if out:
            max_seen = max((_as_epoch(r.get("watched_at")) or 0) for r in out.values())

        if include_marked and scope_ok and not force and (not limit or len(out) < int(limit)):
            st = _load_marked_state() or {}
            marked0 = st.get("items") or {}
            marked: dict[str, Any] = dict(marked0) if isinstance(marked0, Mapping) else {}
            changed = False

            # Discover newly watched items
            found = 0
            for entry, ts_i in _iter_marked_watched_from_library(adapter, allow, since=eff_since):
                rk = str(ids_from(entry).get("plex") or "")
                if not rk:
                    continue
                prev = marked.get(rk)
                prev_ts = _as_epoch((prev or {}).get("watched_at")) if isinstance(prev, Mapping) else None
                if not prev or (ts_i and (not prev_ts or int(ts_i) != int(prev_ts))):
                    e = dict(entry)
                    e["_cw_marked"] = True
                    e["watched"] = True
                    if ts_i:
                        e["watched_at"] = e.get("watched_at") or _iso(int(ts_i))
                    else:
                        e["watched_at"] = None
                        e["watched_at_missing"] = True
                    marked[rk] = e
                    changed = True
                found += 1

            # Validate watched/unwatched toggles directly from PMS metadata.
            def _fetch_marked_meta(rk_item: tuple[str, Any]) -> tuple[str, Any, Mapping[str, Any] | None]:
                rk_, item_ = rk_item
                if not isinstance(item_, Mapping):
                    return rk_, item_, None
                return rk_, item_, _pms_fetch_metadata_row(adapter, str(rk_))

            marked_pairs = list(marked.items())
            meta_workers = plex_worker_count(adapter, "marked_meta_workers", "CW_PLEX_MARKED_META_WORKERS", 8)
            with ThreadPoolExecutor(max_workers=meta_workers, thread_name_prefix="plex-marked-meta") as meta_exec:
                fetched_rows = list(meta_exec.map(_fetch_marked_meta, marked_pairs))

            for rk, item, row in fetched_rows:
                if not isinstance(item, Mapping):
                    continue
                if not row:
                    continue
                is_watched = _pms_row_is_watched(row)
                prev_watched = bool(item.get("watched"))
                ts = _pms_row_watched_ts(row)
                prev_ts = _as_epoch(item.get("watched_at"))
                if is_watched != prev_watched:
                    e = dict(item)
                    e["watched"] = bool(is_watched)
                    if is_watched:
                        if ts and (not prev_ts or int(ts) > int(prev_ts)):
                            use_ts = int(ts)
                        else:
                            use_ts = int(time.time())
                        e["watched_at"] = _iso(use_ts)
                    marked[rk] = e
                    changed = True
                elif is_watched:
                    # Keep watched_at stable; only upgrade if PMS has a newer timestamp.
                    if ts and (not prev_ts or int(ts) > int(prev_ts)):
                        e = dict(item)
                        e["watched"] = True
                        e["watched_at"] = _iso(int(ts))
                        marked[rk] = e
                        changed = True
                else:
                    # Unwatched: keep entry for future re-watch detection.
                    if prev_watched:
                        e = dict(item)
                        e["watched"] = False
                        marked[rk] = e
                        changed = True

            if found or changed:
                st = dict(st) if isinstance(st, Mapping) else {}
                st["items"] = marked
                st["last_updated_at"] = int(time.time())
                _save_marked_state(st)

            for rk, item in (marked or {}).items():
                if not isinstance(item, Mapping) or not item.get("watched"):
                    continue
                row = dict(item)
                _force_episode_title(row)
                ts3 = _as_epoch(row.get("watched_at"))
                if not ts3:
                    continue
                row["watched"] = True
                row["watched_at"] = _iso(int(ts3))
                key = f"{canonical_key(row)}@{int(ts3)}"
                if key not in out and _keep_in_snapshot(adapter, row):
                    out[key] = row
                    if limit and len(out) >= int(limit):
                        break

        if prog:
            prog.done(total=len(out), ok=True)

        if max_seen:
            _save_watermark(wm_key, int(max_seen))

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
        strict = bool(plex_cfg_get(adapter, "strict_id_matching", False))
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

        for item in items or []:
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
                ds, _delta = _date_status(int(ts), entry.get("last_viewed_at"), tol)
                meta["accepted_keys"].append(key)
                meta["presence_confirmed_keys"].append(key)
                meta["live_confirmed_keys"].append(key)
                if ds == DATE_MISMATCH:
                    meta["date_mismatch_keys"].append(key)
                else:
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

        write_workers = 0
        write_ms = 0
        if to_scrobble:
            write_workers = plex_worker_count(adapter, "history_workers", "CW_PLEX_HISTORY_WORKERS", 12)
            _ensure_session_pool(srv, write_workers)
            _t_write = time.time()
            if write_workers > 1 and len(to_scrobble) > 1:
                with ThreadPoolExecutor(max_workers=write_workers, thread_name_prefix="plex-scrobble") as ex:
                    scrobble_ok = list(ex.map(lambda t: _scrobble_with_date(srv, t[2], t[3]), to_scrobble))
            else:
                scrobble_ok = [_scrobble_with_date(srv, rk, ts) for (_item, _key, rk, ts) in to_scrobble]
            write_ms = int((time.time() - _t_write) * 1000)

            for (item, key, _rk, _ts), success in zip(to_scrobble, scrobble_ok):
                if success:
                    ok += 1
                    meta["accepted_keys"].append(key)
                    meta["accepted_not_seen_live_keys"].append(key)
                    shadow_batch.append(item)
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
                _marked_set_unwatched(rating_key, item)
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

    rk = ids.get("plex") or None
    if rk:
        try:
            if srv.fetchItem(int(rk)):
                return str(rk)
        except Exception:
            pass

    kind = (item.get("type") or "movie").lower()
    if kind == "anime":
        kind = "episode"
    is_episode = kind == "episode"

    strict = bool(plex_cfg_get(adapter, "strict_id_matching", False)) if strict is None else bool(strict)
    allow = plex_feature_library_ids(adapter, "history")

    season = item.get("season") or item.get("season_number")
    episode = item.get("episode") or item.get("episode_number")

    guids = item_guid_candidates(ids, show_ids, item)

    if strict:
        if not (guids or _has_matchable_ids(ids) or _has_matchable_ids(show_ids)):
            return None

        if is_episode:
            rk_show = show_ids.get("plex")
            if rk_show:
                try:
                    obj0 = srv.fetchItem(int(rk_show))
                except Exception:
                    obj0 = None
                if obj0 and section_allowed(obj0, allow):
                    rk0 = episode_rating_key_from_show(obj0, season, episode)
                    if rk0:
                        return rk0

            obj = resolve_obj_by_guids(srv, guids, allow, {"episode"})
            if obj:
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
        url = srv.url("/:/unscrobble")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library"}
        resp = srv._session.get(url, params=params, timeout=10)
        return resp.ok
    except Exception:
        return False
