# /providers/sync/mdblist/_history.py
# MDBList watched history sync module (delta semantics)
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import re
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, TypeGuard

from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log

from .._mod_common import request_with_retries

from ._common import (
    START_OF_TIME_ISO,
    STATE_DIR,
    state_file,
    as_epoch,
    as_iso,
    cfg_int,
    cfg_section,
    get_watermark,
    iso_ok,
    iso_z,
    max_iso,
    now_iso,
    read_json,
    write_json,
    save_watermark,
    update_watermark_if_new,
)


BASE = "https://api.mdblist.com"
URL_LAST_ACTIVITIES = f"{BASE}/sync/last_activities"
URL_LIST = f"{BASE}/sync/watched"
URL_UPSERT = f"{BASE}/sync/watched"
URL_REMOVE = f"{BASE}/sync/watched/remove"

IMDB_RE = re.compile(r'^tt\d+$')
EP_TAG_RE = re.compile(r'^S\d{2}E\d{2}$')

def _imdb_ok(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s if IMDB_RE.match(s) else None


def _type_norm(value: object) -> str:
    t = str(value or "").strip().lower()
    if t.endswith("s") and t in ("movies", "shows", "seasons", "episodes"):
        t = t[:-1]
    return t or "movie"


def _sync_visible(item: Mapping[str, Any]) -> bool:
    return _type_norm(item.get("type")) in ("movie", "show", "season", "episode")



def _dbg(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "history", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "history", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "history", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "history", "error", msg, **fields)


def _log(msg: str, **fields: Any) -> None:
    _dbg(msg, **fields)


def _cache_path() -> Path:
    return state_file("mdblist_history.index.json")

_cfg = cfg_section
_cfg_int = cfg_int
_now_iso = now_iso
_iso_ok = iso_ok
_iso_z = iso_z
_as_epoch = as_epoch
_as_iso = as_iso
_max_iso = max_iso


def _migrate_cache(items: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    out: dict[str, Any] = {}
    changed = False
    for k, v in (items or {}).items():
        if not isinstance(v, Mapping):
            changed = True
            continue
        item = dict(v)
        if not _sync_visible(item):
            changed = True
            continue

        typ = _type_norm(item.get("type"))
        if typ == "episode":
            st = str(item.get("series_title") or "").strip()
            if not st:
                t = str(item.get("title") or "").strip()
                if t and not EP_TAG_RE.match(t):
                    item["series_title"] = t
                    changed = True
            try:
                s = int(item.get("season") or 0)
                e = int(item.get("episode") or 0)
            except Exception:
                s = 0
                e = 0
            if s > 0 and e > 0:
                tag = f"S{s:02d}E{e:02d}"
                if str(item.get("title") or "") != tag:
                    item["title"] = tag
                    changed = True
        elif typ == "season":
            st = str(item.get("series_title") or "").strip()
            if not st:
                t = str(item.get("title") or "").strip()
                if t:
                    item["series_title"] = t
                    changed = True
        ek = _event_key(item)
        if not ek:
            changed = True
            continue
        if ek != str(k):
            changed = True
        cur = out.get(ek)
        if not cur:
            out[ek] = item
            continue
        w_new = str(item.get('watched_at') or '')
        w_old = str(cur.get('watched_at') or '')
        if w_new >= w_old:
            out[ek] = item
            changed = True
    normalized, dropped = _normalize_rollups(out)
    if dropped["shows"] or dropped["seasons"]:
        changed = True
        _dbg(
            "index_reconcile",
            reason="rollups_pruned",
            shows=dropped["shows"],
            seasons=dropped["seasons"],
            scope="cache",
        )
    if len(normalized) != len(items):
        changed = True
    return normalized, changed


def _load_cache() -> dict[str, Any]:
    try:
        p = _cache_path()
        doc = read_json(p)
        if not isinstance(doc, dict):
            return {}
        items = dict(doc.get('items') or {})
        if not items:
            return {}
        migrated, changed = _migrate_cache(items)
        if changed:
            _save_cache(migrated)
        return migrated
    except Exception:
        return {}


def _save_cache(items: Mapping[str, Any]) -> None:
    try:
        doc = {"generated_at": _now_iso(), "items": dict(items)}
        write_json(_cache_path(), doc)
    except Exception as e:
        _warn("cache_save_failed", error=str(e))


def _fetch_last_activities(adapter: Any, *, timeout: float, retries: int) -> dict[str, Any] | None:
    client = getattr(adapter, "client", None)
    if client and hasattr(client, "last_activities"):
        try:
            data = client.last_activities()
            if isinstance(data, Mapping) and "error" not in data and "status" not in data:
                return dict(data)
        except Exception:
            pass
        return None

    cfg = _cfg(adapter)
    apikey = str(cfg.get("api_key") or "").strip()
    if not apikey:
        return None

    sess = adapter.client.session
    try:
        r = request_with_retries(
            sess,
            "GET",
            URL_LAST_ACTIVITIES,
            params={"apikey": apikey},
            timeout=timeout,
            max_retries=retries,
        )
        if 200 <= r.status_code < 300:
            data = r.json() if (r.text or "").strip() else {}
            return dict(data) if isinstance(data, Mapping) else None
    except Exception:
        return None
    return None


def _base_key(item: Mapping[str, Any]) -> str:
    if (
        item.get("type") in ("episode", "season")
        and isinstance(item.get("show_ids"), Mapping)
        and item.get("season") is not None
    ):
        base_obj: dict[str, Any] = {
            "type": str(item.get("type")),
            "show_ids": dict(item.get("show_ids") or {}),
            "season": item.get("season"),
        }
        if item.get("type") == "episode" and item.get("episode") is not None:
            base_obj["episode"] = item.get("episode")
        return canonical_key(id_minimal(base_obj))
    return canonical_key(id_minimal(item))


def _event_key(item: Mapping[str, Any]) -> str | None:
    w = item.get("watched_at") or item.get("last_watched_at")
    if not _iso_ok(w):
        return None
    ts = _as_epoch(_iso_z(w))
    if not ts:
        return None
    return f"{_base_key(item)}@{ts}"


def _merge_event(dst: dict[str, Any], item: Mapping[str, Any]) -> str | None:
    ek = _event_key(item)
    if not ek:
        return None
    cur = dst.get(ek)
    if not cur:
        dst[ek] = dict(item)
        return ek
    w_new = str(item.get("watched_at") or "")
    w_old = str(cur.get("watched_at") or "")
    if w_new >= w_old:
        merged = dict(cur)
        merged.update(dict(item))
        dst[ek] = merged
    return ek


def _journal_item(row: Mapping[str, Any]) -> dict[str, Any] | None:
    if str(row.get("category") or "").strip().lower() != "watched":
        return None
    typ = _type_norm(row.get("item_type"))
    ids = _ids_pick(row.get("ids") or {})
    if not ids:
        return None
    out: dict[str, Any] = {"type": typ}
    if typ in ("season", "episode"):
        out["show_ids"] = ids
        out["ids"] = ids
        out["season"] = row.get("season")
        if typ == "episode":
            out["episode"] = row.get("episode")
    else:
        out["ids"] = ids
    title = str(row.get("title") or row.get("name") or "").strip()
    show_title = str(row.get("series_title") or row.get("show_title") or row.get("show") or "").strip()
    if typ == "show" and title:
        out["title"] = title
    elif typ == "season":
        if show_title:
            out["series_title"] = show_title
        elif title:
            out["series_title"] = title
    elif typ == "episode":
        if show_title:
            out["series_title"] = show_title
        elif title and not re.match(r"^s\d{1,3}e\d{1,3}$", title, flags=re.I):
            out["series_title"] = title
        if title:
            out["title"] = title
    status = str(row.get("status") or "").strip().lower()
    value_at = row.get("value_at") or row.get("action_at")
    if status == "added" and _iso_ok(value_at):
        out["watched_at"] = _iso_z(value_at)
    return out


def _remove_base_entries(items: dict[str, Any], base_item: Mapping[str, Any]) -> int:
    typ = _type_norm(base_item.get("type"))
    show_key = _show_rollup_key(base_item) if typ in ("show", "season", "episode") else None
    try:
        season_no = int(base_item.get("season") or 0) if typ == "season" else 0
    except Exception:
        season_no = 0
    try:
        episode_no = int(base_item.get("episode") or 0) if typ == "episode" else 0
    except Exception:
        episode_no = 0

    try:
        target = _base_key(base_item)
    except Exception:
        return 0
    if not target:
        return 0
    removed = 0
    for key, value in list((items or {}).items()):
        try:
            if not isinstance(value, Mapping):
                current = ""
                cur_typ = ""
                cur_show_key = None
                cur_season_no = 0
            else:
                current = _base_key(value)
                cur_typ = _type_norm(value.get("type"))
                cur_show_key = _show_rollup_key(value)
                try:
                    cur_season_no = int(value.get("season") or 0) if cur_typ in ("season", "episode") else 0
                except Exception:
                    cur_season_no = 0
                try:
                    cur_episode_no = int(value.get("episode") or 0) if cur_typ == "episode" else 0
                except Exception:
                    cur_episode_no = 0
        except Exception:
            current = ""
            cur_typ = ""
            cur_show_key = None
            cur_season_no = 0
            cur_episode_no = 0

        remove = bool(current and current == target)
        if not remove and typ == "show" and show_key and cur_show_key == show_key:
            remove = True
        if (
            not remove
            and typ == "season"
            and show_key
            and season_no > 0
            and cur_show_key == show_key
            and cur_typ in ("season", "episode")
            and cur_season_no == season_no
        ):
            remove = True
        if (
            not remove
            and typ == "episode"
            and show_key
            and season_no >= 0
            and episode_no > 0
            and cur_show_key == show_key
            and cur_typ == "episode"
            and cur_season_no == season_no
            and cur_episode_no == episode_no
        ):
            remove = True

        if remove:
            items.pop(key, None)
            removed += 1
    return removed


def _apply_journal_rows(
    cached: Mapping[str, Any],
    rows: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, int], str | None]:
    merged: dict[str, Any] = {
        str(k): dict(v) for k, v in (cached or {}).items() if isinstance(v, Mapping)
    }
    stats = {"removed": 0}
    latest_seen: str | None = None

    def _sort_key(row: Mapping[str, Any]) -> str:
        return str(row.get("action_at") or row.get("value_at") or "")

    for row in sorted((r for r in (rows or []) if isinstance(r, Mapping)), key=_sort_key):
        item = _journal_item(row)
        if not item:
            continue
        latest_seen = _max_iso(latest_seen, row.get("action_at") or row.get("value_at"))
        status = str(row.get("status") or "").strip().lower()
        if status == "removed":
            stats["removed"] += _remove_base_entries(merged, item)
    return merged, stats, latest_seen

def _show_rollup_key(item: Mapping[str, Any]) -> str | None:
    ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else item.get("ids")
    if not isinstance(ids, Mapping):
        return None
    picked = _ids_pick(ids)
    for k in ("tmdb", "imdb", "trakt", "tvdb", "kitsu"):
        v = picked.get(k)
        if v is not None and v != "":
            return f"{k}:{v}"
    return None


def _normalize_rollups(items: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    src: dict[str, Any] = {
        str(k): dict(v) for k, v in (items or {}).items() if isinstance(v, Mapping)
    }
    if not src:
        return {}, {"shows": 0, "seasons": 0}

    episode_children: set[tuple[str, int]] = set()
    episode_show_children: set[str] = set()
    season_children: set[str] = set()

    for item in src.values():
        typ = _type_norm(item.get("type"))
        show_key = _show_rollup_key(item)
        if not show_key:
            continue
        if typ == "season":
            season_children.add(show_key)
            continue
        if typ != "episode":
            continue
        episode_show_children.add(show_key)
        season_children.add(show_key)
        try:
            season_no = int(item.get("season") or 0)
        except Exception:
            season_no = 0
        if season_no > 0:
            episode_children.add((show_key, season_no))

    out: dict[str, Any] = {}
    dropped = {"shows": 0, "seasons": 0}

    for key, item in src.items():
        typ = _type_norm(item.get("type"))
        if typ == "season":
            show_key = _show_rollup_key(item)
            try:
                season_no = int(item.get("season") or 0)
            except Exception:
                season_no = 0
            if show_key and show_key in episode_show_children:
                dropped["seasons"] += 1
                continue
            if show_key and season_no > 0 and (show_key, season_no) in episode_children:
                dropped["seasons"] += 1
                continue
        elif typ == "show":
            show_key = _show_rollup_key(item)
            if show_key and show_key in season_children:
                dropped["shows"] += 1
                continue
        out[key] = item

    return out, dropped

def _ids_pick(obj: Mapping[str, Any]) -> dict[str, Any]:
    ids_raw: dict[str, Any] = dict(obj.get('ids') or {})
    out: dict[str, Any] = {}
    for k in ('tmdb', 'imdb', 'tvdb', 'trakt', 'kitsu', 'mdblist'):
        v = ids_raw.get(k) or obj.get(k) or obj.get(f'{k}_id')
        if v is None or v == '':
            continue
        if k == 'imdb':
            vv = _imdb_ok(v)
            if not vv:
                continue
            out[k] = vv
            continue
        if k == 'mdblist':
            out[k] = str(v).strip().lower()
            continue
        if k in ('tmdb', 'tvdb', 'trakt'):
            try:
                out[k] = int(v)
            except Exception:
                continue
            continue
        out[k] = str(v)
    if out.get('imdb') and out.get('tmdb') and out.get('tvdb'):
        out.pop('tvdb', None)
    return out


def _row_movie(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        mv = row.get("movie") or {}
        ids = _ids_pick(mv)
        if not ids:
            return None
        out: dict[str, Any] = {"type": "movie", "ids": ids}
        title = str(mv.get("title") or mv.get("name") or "").strip()
        if title:
            out["title"] = title
        y = mv.get("year") or mv.get("release_year")
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if year:
            out["year"] = year
        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)
        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _row_show(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        sh = row.get("show") or {}
        ids = _ids_pick(sh)
        if not ids:
            return None
        out: dict[str, Any] = {"type": "show", "ids": ids}
        title = str(sh.get("title") or sh.get("name") or "").strip()
        if title:
            out["title"] = title
        y = sh.get("year") or sh.get("first_air_year")
        if not y:
            fa = str(sh.get("first_air_date") or sh.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit():
                y = int(fa[:4])
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if year:
            out["year"] = year
        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)
        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _row_season(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        sv = row.get("season") or {}
        show = sv.get("show") or {}
        sids = _ids_pick(sv)
        sh_ids = _ids_pick(show)
        ids = sids or sh_ids
        if not ids:
            return None

        out: dict[str, Any] = {"type": "season", "ids": ids, "season": sv.get("number")}
        if sh_ids:
            out["show_ids"] = sh_ids
        show_title = str(show.get("title") or show.get("name") or "").strip()
        if not show_title:
            show_title = str(row.get("title") or "").strip()
        if show_title:
            out["series_title"] = show_title

        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)

        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _row_episode(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        ev = row.get("episode") or {}
        show = ev.get("show") or {}
        eids = _ids_pick(ev)
        sh_ids = _ids_pick(show)
        ids = eids or sh_ids
        if not ids:
            return None

        num = ev.get("number") if ev.get("number") is not None else ev.get("episode")
        out: dict[str, Any] = {
            "type": "episode",
            "ids": ids,
            "season": ev.get("season"),
            "episode": num,
        }
        if sh_ids:
            out["show_ids"] = sh_ids
        show_title = str(show.get("title") or show.get("name") or "").strip()
        if not show_title:
            show_title = str(row.get("title") or "").strip()
        if show_title:
            out["series_title"] = show_title

        try:
            s = int(out.get("season") or 0)
            e = int(out.get("episode") or 0)
        except Exception:
            s = 0
            e = 0
        if s > 0 and e > 0:
            out["title"] = f"S{s:02d}E{e:02d}"
        elif show_title:
            out["title"] = show_title

        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)

        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def build_index(
    adapter: Any,
    *,
    per_page: int = 1000,
    max_pages: int = 250,
) -> dict[str, dict[str, Any]]:
    cfg = _cfg(adapter)
    cached_raw = _load_cache()
    cached: dict[str, dict[str, Any]] = {
        str(k): dict(v) for k, v in (cached_raw or {}).items() if isinstance(v, Mapping)
    }

    apikey = str(cfg.get("api_key") or "").strip()
    if not apikey:
        if cached:
            _dbg("index_cache_hit", source="cache", reason="missing_api_key", count=len(cached))
            _info("index_done", count=len(cached), source="cache")
        else:
            _dbg("index_reconcile", reason="missing_api_key", strategy="empty")
            _info("index_done", count=0, source="empty")
        return cached

    per_page = _cfg_int(cfg, "history_per_page", per_page)
    per_page = max(1, min(int(per_page), 5000))
    max_pages = _cfg_int(cfg, "history_max_pages", max_pages)
    max_pages = max(1, min(int(max_pages), 2000))

    sess = adapter.client.session
    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries

    acts = _fetch_last_activities(adapter, timeout=timeout, retries=retries) or {}
    acts_candidates = []
    if isinstance(acts, Mapping):
        acts_candidates.extend([
            acts.get("watched_at"),
            acts.get("season_watched_at"),
            acts.get("episode_watched_at"),
            acts.get("history"),
            acts.get("updated_at"),
        ])
    acts_watched_iso: str | None = None
    for candidate in acts_candidates:
        if _iso_ok(candidate):
            acts_watched_iso = _max_iso(acts_watched_iso, _iso_z(candidate))

    journal_ts = acts.get("journal_at") if isinstance(acts, Mapping) else None
    journal_iso = _iso_z(journal_ts) if _iso_ok(journal_ts) else None
    wm = get_watermark("history")
    journal_wm = get_watermark("history_journal")
    force_baseline = False

    if acts_watched_iso and journal_iso and wm and journal_wm:
        a = _as_epoch(acts_watched_iso) or 0
        b = _as_epoch(wm) or 0
        jn = _as_epoch(journal_iso) or 0
        jw = _as_epoch(journal_wm) or 0
        if a <= b and jn <= jw:
            _dbg("index_cache_hit", source="cache", reason="activities_unchanged", watched_at=acts_watched_iso, journal_at=journal_iso, watermark=wm, journal_watermark=journal_wm, count=len(cached))
            _info("index_done", count=len(cached), source="cache")
            return cached
        if cached and jn > jw:
            journal_limit = max(50, min(per_page, 1000))
            journal = adapter.fetch_journal(since=journal_wm, limit=journal_limit, category="watched")
            journal_oldest = str(journal.get("journal_oldest_at") or "").strip()
            if _iso_ok(journal_oldest) and (_as_epoch(journal_wm) or 0) < (_as_epoch(_iso_z(journal_oldest)) or 0):
                _warn("index_reconcile", reason="journal_window_stale", strategy="full_replace", journal_oldest_at=journal_oldest, journal_watermark=journal_wm)
                force_baseline = True
            if not force_baseline:
                rows = journal.get("rows") if isinstance(journal.get("rows"), list) else []
                has_adds = any(
                    isinstance(row, Mapping)
                    and str(row.get("category") or "").strip().lower() == "watched"
                    and str(row.get("status") or "").strip().lower() == "added"
                    for row in rows
                )
                if has_adds:
                    _dbg(
                        "index_reconcile",
                        reason="journal_contains_adds",
                        strategy="full_merge",
                        rows=len(rows),
                        journal_at=journal_iso,
                        journal_watermark=journal_wm,
                    )
                else:
                    merged_journal, journal_stats, latest_journal_seen = _apply_journal_rows(cached, rows)
                    normalized_journal, dropped_journal = _normalize_rollups(merged_journal)
                    if dropped_journal["shows"] or dropped_journal["seasons"]:
                        _dbg("index_reconcile", reason="rollups_pruned", shows=dropped_journal["shows"], seasons=dropped_journal["seasons"], scope="journal")
                    _save_cache(normalized_journal)
                    update_watermark_if_new("history", _max_iso(latest_journal_seen, acts_watched_iso))
                    update_watermark_if_new("history_journal", journal_iso)
                    _dbg("index_reconcile", reason="journal_applied", strategy="journal", rows=len(rows), removed=journal_stats["removed"], journal_at=journal_iso, journal_watermark=journal_wm)
                    _info("index_done", count=len(normalized_journal), source="journal")
                    return normalized_journal

    if acts_watched_iso and journal_iso and (not journal_wm) and cached:
        force_baseline = True

    if acts_watched_iso and (not wm) and cached and not force_baseline:
        save_watermark("history", acts_watched_iso)
        if journal_iso:
            save_watermark("history_journal", journal_iso)
        _dbg("index_cache_hit", source="cache", reason="baseline_watermark_set", watermark=acts_watched_iso, count=len(cached))
        _info("index_done", count=len(cached), source="cache")
        return cached

    since_req: str | None = None

    if acts_watched_iso:
        _dbg("index_reconcile", reason="activities_changed", strategy="full_merge", watched_at=acts_watched_iso, journal_at=journal_iso or "-", watermark=wm or "-", journal_watermark=journal_wm or "-")
    else:
        _dbg("index_reconcile", reason="baseline_fetch", strategy="baseline", journal_at=journal_iso or "-")

    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("history") if callable(prog_factory) else None

    out: dict[str, dict[str, Any]] = {}
    latest_seen: str | None = None
    offset = 0
    pages = 0
    tick = 0
    complete_fetch = True
    while True:
        params: dict[str, Any] = {"apikey": apikey, "offset": offset, "limit": per_page}
        if since_req:
            params["since"] = since_req
        try:
            r = request_with_retries(
                sess,
                "GET",
                URL_LIST,
                params=params,
                timeout=timeout,
                max_retries=retries,
            )
        except Exception as e:
            _warn("http_failed", op="index", method="GET", url=URL_LIST, offset=offset, error=f"{type(e).__name__}: {e}")
            complete_fetch = False
            break

        if r.status_code != 200:
            _warn("http_failed", op="index", method="GET", url=URL_LIST, status=r.status_code, offset=offset, body=(r.text or '')[:160])
            complete_fetch = False
            break

        data = r.json() if (r.text or "").strip() else {}
        buckets = {
            "movies": data.get("movies") or [],
            "shows": data.get("shows") or [],
            "seasons": data.get("seasons") or [],
            "episodes": data.get("episodes") or [],
        }
        added = 0
        for row in buckets["movies"]:
            m = _row_movie(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1
        for row in buckets["shows"]:
            m = _row_show(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1
        for row in buckets["seasons"]:
            m = _row_season(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1
        for row in buckets["episodes"]:
            m = _row_episode(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1

        tick += added
        if prog and added:
            try:
                prog.tick(tick, total=max(tick, tick + 1))
            except Exception:
                pass

        pages += 1
        if pages >= max_pages:
            _warn("index_reconcile", reason="safety_cap_hit", strategy="delta", max_pages=max_pages)
            complete_fetch = False
            break

        pag = data.get("pagination") if isinstance(data, Mapping) else None
        if isinstance(pag, Mapping) and pag.get("has_more") is False:
            break

        rows_total = sum(len(v) for v in buckets.values() if isinstance(v, list))
        if rows_total == 0:
            break
        offset += per_page
        
    normalized_out, dropped_out = _normalize_rollups(out)
    if dropped_out["shows"] or dropped_out["seasons"]:
        _dbg(
            "index_reconcile",
            reason="rollups_pruned",
            shows=dropped_out["shows"],
            seasons=dropped_out["seasons"],
            scope="delta",
        )

    if normalized_out and (force_baseline or complete_fetch):
        # /sync/watched is fetched as a full current snapshot here.
        merged_base = {str(k): dict(v) for k, v in normalized_out.items()}
    else:
        merged_base = dict(cached)
        if normalized_out:
            for k, v in normalized_out.items():
                merged_base[str(k)] = dict(v)

    merged, dropped_merged = _normalize_rollups(merged_base)
    if dropped_merged["shows"] or dropped_merged["seasons"]:
        _dbg(
            "index_reconcile",
            reason="rollups_pruned",
            shows=dropped_merged["shows"],
            seasons=dropped_merged["seasons"],
            scope="merged",
        )

    if out or force_baseline or dropped_merged["shows"] or dropped_merged["seasons"]:
        _save_cache(merged)

    update_watermark_if_new("history", latest_seen or acts_watched_iso)
    if journal_iso:
        update_watermark_if_new("history_journal", journal_iso)

    if prog:
        try:
            prog.tick(len(out), total=len(out))
        except Exception:
            pass

    _dbg("index_fetch_counts", count=len(out), latest_seen=latest_seen or "-", watermark=get_watermark("history") or "-", source="current")
    _info("index_done", count=len(merged), source="current")
    return merged


def _stable_show_key(ids: Mapping[str, Any]) -> str:
    keep = {k: ids.get(k) for k in ("tmdb", "imdb", "tvdb", "trakt") if ids.get(k) is not None}
    return json.dumps(keep, sort_keys=True)


def _bucketize(items: Iterable[Mapping[str, Any]], *, unwatch: bool) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows_nested: dict[str, dict[str, Any]] = {}
    shows_plain: dict[str, dict[str, Any]] = {}

    nested_show_keys: set[str] = set()

    accepted: list[dict[str, Any]] = []
    
    def _carry_meta_for_mdblist(src: Mapping[str, Any], dst: dict[str, Any]) -> None:
        typ = str(src.get("type") or "").strip().lower()
        if typ.endswith("s") and typ in ("movies", "shows", "seasons", "episodes"):
            typ = typ[:-1]
        if typ in ("season", "episode"):
            title = src.get("series_title") or src.get("title")
            y = src.get("series_year") if src.get("year") is None else src.get("year")
        else:
            title = src.get("title") or src.get("series_title")
            y = src.get("year")
        if isinstance(title, str):
            title = title.strip()
        if title:
            dst["title"] = title
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if year:
            dst["year"] = year


    for raw in items or []:
        m = dict(raw or {})
        typ_raw = m.get("type")
        typ = str(typ_raw or "movie").strip().lower()
        if typ.endswith("s") and typ in ("movies", "shows", "seasons", "episodes"):
            typ = typ[:-1]
        if typ not in ("movie", "show", "season", "episode"):
            _dbg("write_item_skipped", reason="unknown_type", item=id_minimal({"type": str(typ_raw or "unknown"), "ids": _ids_pick(m)}))
            continue
        ids = _ids_pick(m)
        show_ids = _ids_pick(m.get("show_ids") or {}) if isinstance(m.get("show_ids"), Mapping) else {}

        watched_at = m.get("watched_at") or m.get("last_watched_at")
        watched_iso = _iso_z(watched_at) if _iso_ok(watched_at) else None
        if not watched_iso and not unwatch:
            _dbg("write_item_skipped", reason="missing_watched_at", item=id_minimal({"type": typ, "ids": ids or show_ids}))
            continue

        if typ == "movie":
            if not ids:
                continue
            row: dict[str, Any] = {"ids": ids}
            if watched_iso:
                row["watched_at"] = watched_iso
            _carry_meta_for_mdblist(m, row)
            movies.append(row)
            acc = {"type": "movie", "ids": ids, **({"watched_at": watched_iso} if watched_iso else {})}
            _carry_meta_for_mdblist(m, acc)
            accepted.append(acc)
            continue

        if typ == "show":
            if not ids:
                continue
            key = _stable_show_key(ids)
            sh = shows_plain.get(key) or {"ids": ids}
            if watched_iso:
                sh["watched_at"] = watched_iso
            _carry_meta_for_mdblist(m, sh)
            shows_plain[key] = sh
            acc = {"type": "show", "ids": ids, **({"watched_at": watched_iso} if watched_iso else {})}
            _carry_meta_for_mdblist(m, acc)
            accepted.append(acc)
            continue

        season_num = m.get("season")
        if season_num is None:
            continue
        try:
            s = int(season_num)
        except Exception:
            continue

        sh_ids = show_ids or ids
        if not sh_ids:
            continue
        skey = _stable_show_key(sh_ids)
        nested_show_keys.add(skey)
        show_obj = shows_nested.get(skey) or {"ids": sh_ids, "seasons": []}
        _carry_meta_for_mdblist(m, show_obj)
        if not isinstance(show_obj.get("seasons"), list):
            show_obj["seasons"] = []
        seasons_list: list[dict[str, Any]] = show_obj["seasons"]
        season_obj: dict[str, Any] | None = next((x for x in seasons_list if int(x.get("number") or -1) == s), None)
        if not season_obj:
            season_obj = {"number": s}
            seasons_list.append(season_obj)

        if typ == "season":
            if watched_iso:
                season_obj["watched_at"] = watched_iso
            shows_nested[skey] = show_obj
            accepted.append(
                {
                    "type": "season",
                    "ids": ids or sh_ids,
                    "show_ids": sh_ids,
                    "season": s,
                    **({"watched_at": watched_iso} if watched_iso else {}),
                }
            )
            _carry_meta_for_mdblist(m, accepted[-1])
            continue

        ep_num = m.get("episode") if m.get("episode") is not None else m.get("number")
        if ep_num is None:
            continue
        try:
            e = int(ep_num)
        except Exception:
            continue
        ep: dict[str, Any] = {"number": e}
        if watched_iso:
            ep["watched_at"] = watched_iso
        episodes_list = season_obj.get("episodes")
        if not isinstance(episodes_list, list):
            episodes_list = []
            season_obj["episodes"] = episodes_list
        episodes_list.append(ep)
        shows_nested[skey] = show_obj
        accepted.append(
            {
                "type": "episode",
                "ids": ids or sh_ids,
                "show_ids": sh_ids,
                "season": s,
                "episode": e,
                **({"watched_at": watched_iso} if watched_iso else {}),
            }
        )
        _carry_meta_for_mdblist(m, accepted[-1])

    skipped_nested = 0
    if shows_plain and nested_show_keys:
        for k in list(shows_plain.keys()):
            if k in nested_show_keys:
                shows_plain.pop(k, None)
                skipped_nested += 1

    skipped_meta = 0
    if not unwatch and shows_plain:
        for k, v in list(shows_plain.items()):
            if (not v.get("title") and not v.get("year")) and not v.get("ids"):
                shows_plain.pop(k, None)
                skipped_meta += 1

    if skipped_nested or skipped_meta:
        _dbg("write_prepare", skipped_nested=skipped_nested, skipped_meta=skipped_meta)

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if shows_nested:
        for grp in shows_nested.values():
            seasons_list2 = grp.get("seasons")
            if isinstance(seasons_list2, list):
                grp["seasons"] = sorted(seasons_list2, key=lambda x: int(x.get("number") or 0))
                for sp in grp["seasons"]:
                    eps = sp.get("episodes")
                    if isinstance(eps, list):
                        sp["episodes"] = sorted(eps, key=lambda x: int(x.get("number") or 0))
        body["shows_nested"] = list(shows_nested.values())
    if shows_plain:
        body["shows_plain"] = list(shows_plain.values())
    body = {k: v for k, v in body.items() if v}
    return body, accepted


def _chunk(seq: list[Any], n: int) -> Iterable[list[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _write(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
    *,
    unwatch: bool = False,
) -> tuple[int, list[dict[str, Any]]]:
    cfg = _cfg(adapter)
    apikey = str(cfg.get("api_key") or "").strip()
    items_list = list(items or [])
    if not apikey:
        unresolved = [{"item": id_minimal(it), "hint": "missing_api_key"} for it in items_list]
        _info("write_skipped", op="remove" if unwatch else "add", reason="missing_api_key", unresolved=len(unresolved))
        return 0, unresolved

    sess = adapter.client.session
    tmo = adapter.cfg.timeout
    rr = adapter.cfg.max_retries

    chunk_size = _cfg_int(cfg, "history_chunk_size", 25)
    delay_ms = _cfg_int(cfg, "history_write_delay_ms", 600)
    max_backoff_ms = _cfg_int(cfg, "history_max_backoff_ms", 8000)

    body, accepted = _bucketize(items_list, unwatch=unwatch)
    if not body:
        _info("write_skipped", op="remove" if unwatch else "add", reason="empty_payload", unresolved=0)
        return 0, []

    ok = 0
    unresolved: list[dict[str, Any]] = []

    def _apply_success(payload_rows: list[dict[str, Any]], bucket_name: str, data: Mapping[str, Any], *, is_unwatch: bool) -> int:
        kinds = ("movies", "shows", "seasons", "episodes")
        if is_unwatch:
            removed = data.get("removed") or data.get("deleted") or data.get("unwatched") or {}
            n = sum(int(removed.get(k) or 0) for k in kinds)
            if n <= 0:
                _dbg("write_prepare", op="remove", reason="noop_response", bucket=bucket_name, rows=len(payload_rows))
            return n

        not_found = data.get("not_found") or {}
        for kind in kinds:
            rows_nf = not_found.get(kind) or []
            if not isinstance(rows_nf, list):
                continue
            item_type = kind[:-1]
            for row_nf in rows_nf:
                if not isinstance(row_nf, Mapping):
                    continue
                ids_nf = row_nf.get("ids") or {}
                unresolved.append({"item": id_minimal({"type": item_type, "ids": ids_nf}), "hint": "not_found"})

        updated = data.get("updated") or {}
        added = data.get("added") or {}
        existing = data.get("existing") or {}
        n = 0
        n += sum(int(updated.get(k) or 0) for k in kinds)
        n += sum(int(added.get(k) or 0) for k in kinds)
        n += sum(int(existing.get(k) or 0) for k in kinds)
        if n <= 0 and not unresolved:
            _dbg("write_prepare", op="add", reason="noop_response", bucket=bucket_name, rows=len(payload_rows))
        return n

    _dbg(
        "write_start",
        op="remove" if unwatch else "add",
        movies=len(body.get("movies") or []),
        shows_nested=len(body.get("shows_nested") or []),
        shows_plain=len(body.get("shows_plain") or []),
        chunk_size=chunk_size,
    )

    stages: list[tuple[str, str]] = [
        ("movies", "movies"),
        ("shows_nested", "shows"),
        ("shows_plain", "shows"),
    ]

    for body_key, bucket in stages:
        rows = body.get(body_key) or []
        if not rows:
            continue

        stage = "" if body_key == bucket else body_key

        for part in _chunk(rows, chunk_size):
            payload = {bucket: part}
            url = URL_REMOVE if unwatch else URL_UPSERT
            attempt = 0
            backoff = delay_ms

            while True:
                r = request_with_retries(
                    sess,
                    "POST",
                    url,
                    params={"apikey": apikey},
                    json=payload,
                    timeout=tmo,
                    max_retries=rr,
                )

                if r.status_code in (200, 201, 204):
                    d: dict[str, Any]
                    if r.status_code == 204 or not (r.text or "").strip():
                        d = {}
                    else:
                        try:
                            d = r.json()
                        except Exception:
                            d = {}
                    n = _apply_success(part, stage or bucket, d, is_unwatch=unwatch)
                    if r.status_code == 204 and unwatch and n <= 0:
                        n = len(part)
                    ok += n

                    time.sleep(max(0.0, delay_ms / 1000.0))
                    break

                if r.status_code in (429, 503):
                    _warn(
                        "rate_limit",
                        op="remove" if unwatch else "add",
                        bucket=bucket,
                        stage=stage or bucket,
                        status=r.status_code,
                        attempt=attempt,
                        backoff_ms=backoff,
                        body=(r.text or '')[:180],
                    )
                    time.sleep(min(max_backoff_ms, backoff) / 1000.0)
                    attempt += 1
                    backoff = min(max_backoff_ms, int(backoff * 1.6) + 200)
                    if attempt <= 4:
                        continue

                if (
                    r.status_code == 500
                    and bucket == "shows"
                    and body_key == "shows_plain"
                    and len(part) > 1
                ):
                    preview = [id_minimal({"type": "show", "ids": (x.get("ids") or {})}) for x in part[:3]]
                    _warn(
                        "write_failed",
                        op="remove" if unwatch else "add",
                        bucket=bucket,
                        stage=body_key,
                        status=500,
                        reason="retry_split",
                        rows=len(part),
                        preview=preview,
                    )

                    for single in part:
                        r2 = request_with_retries(
                            sess,
                            "POST",
                            url,
                            params={"apikey": apikey},
                            json={bucket: [single]},
                            timeout=tmo,
                            max_retries=rr,
                        )
                        if r2.status_code in (200, 201, 204):
                            d2: dict[str, Any]
                            if r2.status_code == 204 or not (r2.text or "").strip():
                                d2 = {}
                            else:
                                try:
                                    d2 = r2.json()
                                except Exception:
                                    d2 = {}
                            n2 = _apply_success([single], body_key, d2, is_unwatch=unwatch)
                            if r2.status_code == 204 and unwatch and n2 <= 0:
                                n2 = 1
                            ok += n2
                            time.sleep(max(0.0, delay_ms / 1000.0))
                            continue

                        ids2 = single.get("ids") or {}
                        unresolved.append(
                            {
                                "item": id_minimal({"type": "show", "ids": ids2}),
                                "hint": f"http:{r2.status_code}",
                            }
                        )
                    break

                _warn(
                    "write_failed",
                    op="remove" if unwatch else "add",
                    bucket=bucket,
                    stage=stage or bucket,
                    status=r.status_code,
                    body=(r.text or '')[:200],
                )
                for x in part:
                    ids = x.get("ids") or {}
                    t = "show" if bucket == "shows" else "movie"
                    unresolved.append({"item": id_minimal({"type": t, "ids": ids}), "hint": f"http:{r.status_code}"})
                break

    if ok > 0 and not unresolved:
        cache = _load_cache()
        if unwatch:
            for it in accepted:
                if not _sync_visible(it):
                    continue
                base = _base_key(it)
                for k in list(cache.keys()):
                    if str(k).startswith(base + "@"):
                        cache.pop(k, None)
        else:
            for it in accepted:
                if not _sync_visible(it):
                    continue
                ek = _event_key(it) if it.get("watched_at") else None
                if ek:
                    cache[ek] = dict(it)
        _save_cache(cache)

        newest: str | None = None
        for it in accepted:
            if not _sync_visible(it):
                continue
            newest = _max_iso(newest, it.get("watched_at"))
        update_watermark_if_new("history", newest)

    _info("write_done", op="remove" if unwatch else "add", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
    return ok, unresolved


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, items, unwatch=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, items, unwatch=True)
