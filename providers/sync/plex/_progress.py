# /providers/sync/plex/_progress.py
# Plex Module for progress (resume) synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, ids_from_guid, ids_from, minimal as id_minimal

from ._common import (
    episode_rating_key_from_show,
    has_external_ids,
    home_scope_enter,
    home_scope_exit,
    item_guid_candidates,
    plex_cfg_get,
    server_find_rating_key_by_guid,
    make_logger,
    normalize,
)


_dbg, _info, _warn, _error, _log = make_logger("progress")


def _mods_debug() -> bool:
    v = (os.getenv("CW_DEBUG") or "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    v = (os.getenv("CW_PLEX_DEBUG") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _to_int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return int(float(str(v).strip()))
    except Exception:
        return None


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        try:
            if v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return None
    try:
        s = str(v).strip()
        if not s:
            return None
        if s.isdigit():
            return datetime.fromtimestamp(int(s), tz=timezone.utc).isoformat().replace("+00:00", "Z")
        return s
    except Exception:
        return None


def _fetch_resume_rating_keys(srv: Any, *, limit: int = 100) -> set[str]:
    rks: set[str] = set()

    def _q(path: str) -> None:
        key = f"{path}?X-Plex-Container-Start=0&X-Plex-Container-Size={int(limit)}&includeUserState=1"
        root = srv.query(key)  # type: ignore[attr-defined]
        for el in list(root) if root is not None else []:
            a = getattr(el, "attrib", {}) or {}
            rk = a.get("ratingKey") or a.get("key")
            if rk:
                rks.add(str(rk))

    for p in ("/hubs/continueWatching/items", "/library/onDeck", "/library/recentlyViewed"):
        try:
            _q(p)
        except Exception:
            continue

    if _mods_debug():
        _dbg("index_fetch_counts", source="resume", count=len(rks), limit=int(limit))
    return rks


def _fetch_metadata_row(srv: Any, rk: str) -> tuple[Mapping[str, Any] | None, dict[str, str]]:
    try:
        key = f"/library/metadata/{str(rk).strip()}?includeUserState=1&includeGuids=1"
        root = srv.query(key)  # type: ignore[attr-defined]
        rows = list(root) if root is not None else []
        if not rows:
            return None, {}

        el = rows[0]
        a = getattr(el, "attrib", {}) or {}

        ids: dict[str, str] = {}

        try:
            for g in el.findall("./Guid"):  # type: ignore[attr-defined]
                gid = (getattr(g, "attrib", {}) or {}).get("id")
                if gid:
                    for k, v in ids_from_guid(str(gid)).items():
                        if k != "guid" and v:
                            ids[k] = v
        except Exception:
            pass

        # Some agents encode tmdb/imdb in the main guid string.
        g0 = a.get("guid")
        if g0:
            for k, v in ids_from_guid(str(g0)).items():
                if k != "guid" and v:
                    ids[k] = v

        return a, ids
    except Exception:
        return None, {}


def build_index(adapter: Any, **_kwargs: Any) -> Mapping[str, dict[str, Any]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return {}

    _need_scope, did_switch, _aid, _uname = home_scope_enter(adapter)
    try:
        rks = _fetch_resume_rating_keys(srv, limit=150)
        out: dict[str, dict[str, Any]] = {}
        dbg = _mods_debug()

        for rk in sorted(rks):
            a, ext_ids = _fetch_metadata_row(srv, rk)
            if not a:
                continue

            pos_ms = _to_int(a.get("viewOffset"))
            if pos_ms is None or pos_ms <= 0:
                continue

            dur_ms = _to_int(a.get("duration"))
            ts = _iso(a.get("lastViewedAt") or a.get("viewedAt"))

            typ = str(a.get("type") or "movie").lower()
            base: dict[str, Any] = {
                "type": "episode" if typ == "episode" else "movie",
                "title": a.get("title") or a.get("grandparentTitle"),
                "year": _to_int(a.get("year")),
                "ids": {},
            }

            # Keep external IDs when available
            if has_external_ids(ext_ids):
                base["ids"] = dict(ext_ids)
                base["ids"]["plex"] = str(rk)

            if typ == "episode":
                base["series_title"] = a.get("grandparentTitle")
                base["season"] = _to_int(a.get("parentIndex") or a.get("seasonNumber"))
                base["episode"] = _to_int(a.get("index"))
                show_ids: dict[str, str] = {}
                gp = a.get("grandparentGuid")
                if gp:
                    for k, v in ids_from_guid(str(gp)).items():
                        if k != "guid" and v:
                            show_ids[k] = v
                if show_ids:
                    base["show_ids"] = show_ids

            norm = id_minimal(base)
            if ts:
                norm["progress_at"] = ts
            norm["progress_ms"] = int(pos_ms)
            if dur_ms is not None and dur_ms > 0:
                norm["duration_ms"] = int(dur_ms)

            ck = canonical_key(norm)
            if ck:
                out[ck] = norm

            if dbg:
                _dbg(
                    "item",
                    ratingKey=str(rk),
                    type=base.get("type"),
                    chosen_viewOffset=int(pos_ms),
                    chosen_lastViewedAt=ts,
                    ids=dict(base.get("ids") or {}),
                    canonical_key=str(ck),
                )

        _info("index_done", count=len(out))
        return out
    finally:
        home_scope_exit(adapter, did_switch)


def _resolve_rating_key(adapter: Any, it: Mapping[str, Any]) -> str | None:
    # Normalize IDs
    ids = ids_from(it)
    base_rk = (ids.get("plex") or "").strip()
    if base_rk.isdigit():
        return base_rk

    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return None

    kind = str(it.get("type") or "movie").lower()
    if kind == "anime":
        kind = "episode"
    is_episode = kind == "episode"

    # Build GUID candidates from item IDs
    show_ids = it.get("show_ids") if isinstance(it.get("show_ids"), Mapping) else {}
    show_ids = dict(show_ids or {})

    guid_candidates = item_guid_candidates(ids, show_ids, it)

    dbg = _mods_debug()
    if dbg:
        _dbg(
            "write_prepare",
            op="add",
            canonical_key=str(canonical_key(id_minimal(it)) or ""),
            kind=kind,
            ids=dict(ids),
            show_ids=dict(show_ids) if show_ids else {},
            guid_candidates=list(guid_candidates),
            title=str(it.get("title") or ""),
            series_title=str(it.get("series_title") or ""),
            season=it.get("season"),
            episode=it.get("episode"),
        )

    # GUID lookup on the server.
    rk = server_find_rating_key_by_guid(srv, guid_candidates)
    if dbg:
        _dbg("resolve_hit" if rk else "resolve_miss", source="guid", rating_key=str(rk or ""))
    if rk:
        try:
            obj = srv.fetchItem(int(rk))  # type: ignore[attr-defined]
            otype = str(getattr(obj, "type", "") or "").lower()
            if not is_episode and otype == "movie":
                return str(rk)
            if is_episode:
                if otype == "episode":
                    return str(rk)
                if otype in ("show", "season"):
                    season = it.get("season")
                    episode = it.get("episode")
                    rk_ep = episode_rating_key_from_show(obj, season, episode)
                    if rk_ep:
                        return rk_ep
        except Exception:
            # If fetchItem fails, still try to use rk for movies.
            return str(rk) if (not is_episode) else None

    strict = bool(plex_cfg_get(adapter, "strict_id_matching", False))
    if strict:
        return None

    # Title fallback
    title = str(it.get("title") or "").strip()
    series_title = str(it.get("series_title") or "").strip()
    query_title = series_title if is_episode and series_title else title
    if not query_title:
        return None

    season = it.get("season")
    episode = it.get("episode")
    year = it.get("year")

    hits: list[Any] = []
    try:
        mediatype = "episode" if is_episode else "movie"
        hits = list(srv.search(query_title, mediatype=mediatype) or [])  # type: ignore[attr-defined]
    except Exception:
        hits = []

    if is_episode and not hits:
        try:
            hits = list(srv.search(query_title, mediatype="show") or [])  # type: ignore[attr-defined]
        except Exception:
            hits = []
    if not hits:
        if dbg:
            _dbg("resolve_miss", source="title", query_title=str(query_title))
        return None

    if dbg:
        _dbg("resolve_hit", source="title", query_title=str(query_title), hits=len(hits))

    def _score(obj: Any) -> int:
        sc = 0
        try:
            otype = str(getattr(obj, "type", "") or "").lower()
            if is_episode:
                if otype == "episode":
                    sc += 4
                elif otype in ("show", "season"):
                    sc += 2
                t0 = (getattr(obj, "grandparentTitle", None) or getattr(obj, "title", None) or "").strip().lower()
            else:
                if otype == "movie":
                    sc += 4
                t0 = (getattr(obj, "title", None) or "").strip().lower()

            if t0 and t0 == query_title.lower():
                sc += 3

            if not is_episode and year is not None and getattr(obj, "year", None) == year:
                sc += 2

            if is_episode and otype == "episode":
                s_ok = season is None or getattr(obj, "seasonNumber", None) == season or getattr(obj, "parentIndex", None) == season
                e_ok = episode is None or getattr(obj, "index", None) == episode
                if s_ok:
                    sc += 1
                if e_ok:
                    sc += 1

            # Prefer exact external ID matches if present.
            meta = normalize(obj) or {}
            mid = dict((meta.get("ids") or {}) if isinstance(meta.get("ids"), Mapping) else {})
            for k in ("tmdb", "imdb", "tvdb"):
                if ids.get(k) and mid.get(k) and str(ids[k]) == str(mid[k]):
                    sc += 6
                if show_ids.get(k) and mid.get(k) and str(show_ids[k]) == str(mid[k]):
                    sc += 3
        except Exception:
            pass
        return sc

    best = max(hits, key=_score)
    try:
        otype = str(getattr(best, "type", "") or "").lower()
        if not is_episode:
            rk2 = getattr(best, "ratingKey", None)
            return str(rk2) if rk2 else None
        if otype == "episode":
            rk2 = getattr(best, "ratingKey", None)
            return str(rk2) if rk2 else None
        if otype in ("show", "season"):
            rk_ep = episode_rating_key_from_show(best, season, episode)
            return rk_ep
    except Exception:
        return None
    return None


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return 0, [{"item": dict(x), "hint": "not_configured"} for x in (items or [])]

    _need_scope, did_switch, _aid, _uname = home_scope_enter(adapter)
    try:
        ok = 0
        unresolved: list[dict[str, Any]] = []

        for it in items or []:
            it0 = dict(it or {})
            ms = it0.get("progress_ms") or it0.get("viewOffset") or it0.get("progress")
            ms_i = _to_int(ms)
            if ms_i is None or ms_i <= 0:
                unresolved.append({"item": it0, "hint": "missing_progress"})
                if _mods_debug():
                    _dbg("add.unresolved", hint="missing_progress", canonical_key=str(canonical_key(id_minimal(it0)) or ""), ids=dict(ids_from(it0)))
                continue

            rk = _resolve_rating_key(adapter, it0)
            if not rk:
                unresolved.append({"item": it0, "hint": "not_found"})
                if _mods_debug():
                    _dbg("resolve_miss", hint="not_found", canonical_key=str(canonical_key(id_minimal(it0)) or ""), ids=dict(ids_from(it0)))
                continue

            try:
                obj = srv.fetchItem(int(rk))  # type: ignore[attr-defined]
                obj.updateProgress(int(ms_i), state="stopped")
                ok += 1
            except Exception as e:
                if _mods_debug():
                    _warn("write_failed", op="add", rating_key=str(rk), canonical_key=str(canonical_key(id_minimal(it0)) or ""), error=str(e))
                unresolved.append({"item": it0, "hint": f"exception:{e}"})

        _info("write_done", op="add", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
        return ok, unresolved
    finally:
        home_scope_exit(adapter, did_switch)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    unresolved = [{"item": dict(x), "hint": "not_supported"} for x in (items or [])]
    _info("write_skipped", op="remove", reason="not_supported", unresolved=len(unresolved))
    return 0, unresolved
