# /providers/sync/plex/_progress.py
# Plex Module for progress (resume) synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, ids_from_guid, ids_from, minimal as id_minimal
from cw_platform.value_coercion import coerce_bool
from providers.sync._progress_policy import decide_progress_write, select_progress_record

from ._common import (
    active_pms_token,
    episode_rating_key_from_show,
    has_external_ids,
    home_scope_enter,
    home_scope_exit,
    item_guid_candidates,
    plex_cfg_get,
    plex_feature_cfg,
    plex_feature_library_ids,
    raise_home_scope_not_applied,
    server_find_rating_key_by_guid,
    make_logger,
    minimal_from_history_row,
    normalize,
    unresolved_home_scope_not_applied,
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


def _epoch(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v if v.tzinfo else v.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    try:
        text = str(v).strip()
        if not text:
            return None
        if text.replace(".", "", 1).isdigit():
            value = float(text)
            return value / 1000.0 if value > 10_000_000_000 else value
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _same_plex_endpoint() -> bool:
    src = str(os.getenv("CW_PAIR_SRC") or "").upper().strip()
    dst = str(os.getenv("CW_PAIR_DST") or "").upper().strip()
    src_instance = str(os.getenv("CW_PAIR_SRC_INSTANCE") or "default").strip().lower() or "default"
    dst_instance = str(os.getenv("CW_PAIR_DST_INSTANCE") or "default").strip().lower() or "default"
    return src == dst == "PLEX" and src_instance == dst_instance


def _currently_playing(
    srv: Any,
    rating_key: str,
    *,
    account_id: int | None = None,
    username: str | None = None,
) -> bool:
    try:
        for session in srv.sessions() or []:  # type: ignore[attr-defined]
            if str(getattr(session, "ratingKey", "") or "") != str(rating_key):
                continue
            if account_id is not None:
                session_account_id = getattr(session, "accountID", None) or getattr(session, "userID", None)
                if session_account_id is not None and str(session_account_id) != str(account_id):
                    continue
            if username:
                names = getattr(session, "usernames", None) or []
                if isinstance(names, str):
                    names = [names]
                user = getattr(session, "user", None)
                user_name = getattr(user, "title", None) or getattr(user, "username", None)
                normalized_names = {str(value).strip().lower() for value in [*names, user_name] if value}
                if normalized_names and username.strip().lower() not in normalized_names:
                    continue
            return True
    except Exception:
        pass
    return False


def _timeline_progress(adapter: Any, srv: Any, rating_key: str, progress_ms: int, duration_ms: int) -> None:
    client_id = str(
        getattr(getattr(adapter, "cfg", None), "client_id", None)
        or os.getenv("PLEX_CLIENT_IDENTIFIER")
        or os.getenv("CW_PLEX_CID")
        or "crosswatch"
    )
    session = getattr(srv, "_session", None)
    post = getattr(session, "post", None)
    if not callable(post):
        raise RuntimeError("plex_timeline_post_unavailable")
    srv.query(
        "/:/timeline",
        method=post,
        params={
            "ratingKey": str(rating_key),
            "key": f"/library/metadata/{rating_key}",
            "identifier": "com.plexapp.plugins.library",
            "state": "stopped",
            "time": max(0, int(progress_ms)),
            "duration": max(0, int(duration_ms)),
            "X-Plex-Client-Identifier": client_id,
        },
    )


def _progress_write_options(adapter: Any) -> tuple[bool, int]:
    progress_cfg = plex_feature_cfg(adapter, "progress")
    replay_enabled = coerce_bool(progress_cfg.get("replay_enabled", False))
    try:
        tolerance = max(0, int(progress_cfg.get("timestamp_tolerance_seconds", 30)))
    except (TypeError, ValueError):
        tolerance = 30
    return replay_enabled, tolerance


def _truthy_attr(obj: Any, name: str) -> bool:
    try:
        value = getattr(obj, name, False)
        return bool(value() if callable(value) else value)
    except Exception:
        return False


def _fetch_resume_rating_keys(srv: Any, *, limit: int = 100) -> set[str]:
    return set(_fetch_resume_items(srv, page_size=limit))


def _library_id(value: Any) -> str | None:
    source = getattr(value, "attrib", None) if not isinstance(value, Mapping) else value
    source = source if isinstance(source, Mapping) else {}
    for key in ("librarySectionID", "sectionID", "librarySectionId", "sectionId", "library_id"):
        raw = source.get(key)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return None


def _page_total(root: Any) -> int | None:
    attributes = getattr(root, "attrib", {}) or {}
    return _to_int(attributes.get("totalSize") or attributes.get("totalRecordCount"))


def _library_sort_key(section: tuple[str, int]) -> tuple[int, str]:
    library_id = section[0]
    return (0, f"{int(library_id):020d}") if library_id.isdigit() else (1, library_id)


def _progress_sections(srv: Any, allowed: set[str]) -> list[tuple[str, int]]:
    root = srv.query("/library/sections")  # type: ignore[attr-defined]
    rows = list(root) if root is not None else []
    sections: list[tuple[str, int]] = []
    found: set[str] = set()
    for element in rows:
        attributes = getattr(element, "attrib", {}) or {}
        library_id = str(attributes.get("key") or attributes.get("ratingKey") or "").strip()
        library_type = str(attributes.get("type") or "").strip().lower()
        if not library_id or library_type not in {"movie", "show"}:
            continue
        if allowed and library_id not in allowed:
            continue
        found.add(library_id)
        sections.append((library_id, 1 if library_type == "movie" else 4))
    missing = sorted(allowed - found)
    if missing:
        raise RuntimeError(f"plex_progress_libraries_not_found:{','.join(missing)}")
    return sorted(sections, key=_library_sort_key)


def _row_with_ids(element: Any, library_id: str) -> tuple[dict[str, Any], dict[str, str]]:
    attributes = getattr(element, "attrib", {}) or {}
    row = dict(attributes)
    # Plex section rows do not reliably repeat the owning section ID.
    row["librarySectionID"] = library_id
    ids: dict[str, str] = {}
    guid_rows: list[dict[str, str]] = []
    try:
        for guid in element.findall("./Guid"):  # type: ignore[attr-defined]
            guid_id = (getattr(guid, "attrib", {}) or {}).get("id")
            if not guid_id:
                continue
            guid_rows.append({"id": str(guid_id)})
            for id_type, id_value in ids_from_guid(str(guid_id)).items():
                if id_type != "guid" and id_value:
                    ids[id_type] = id_value
    except Exception:
        pass
    if guid_rows:
        row["Guid"] = guid_rows
    main_guid = attributes.get("guid")
    if main_guid:
        for id_type, id_value in ids_from_guid(str(main_guid)).items():
            if id_type != "guid" and id_value:
                ids[id_type] = id_value
    return row, ids


def _fetch_resume_items(
    srv: Any,
    *,
    page_size: int = 100,
    allowed_library_ids: set[str] | None = None,
) -> dict[str, tuple[dict[str, Any], dict[str, str]]]:
    started = time.perf_counter()
    items: dict[str, tuple[dict[str, Any], dict[str, str]]] = {}
    allowed = set(allowed_library_ids or set())
    sections = _progress_sections(srv, allowed)

    def _q(library_id: str, plex_type: int) -> None:
        start = 0
        seen_pages: set[tuple[str, ...]] = set()
        while True:
            path = f"/library/sections/{library_id}/all"
            root = srv.query(  # type: ignore[attr-defined]
                path,
                params={
                    "type": int(plex_type),
                    "viewOffset>>": 0,
                    "X-Plex-Container-Start": start,
                    "X-Plex-Container-Size": int(page_size),
                    "includeUserState": 1,
                    "includeGuids": 1,
                },
            )
            rows = list(root) if root is not None else []
            signature = tuple(
                str((getattr(el, "attrib", {}) or {}).get("ratingKey") or (getattr(el, "attrib", {}) or {}).get("key") or "")
                for el in rows
            )
            if rows and signature in seen_pages:
                _warn("pagination_repeated_page", source=path, library_id=library_id, start_index=start)
                break
            seen_pages.add(signature)
            for el in rows:
                a = getattr(el, "attrib", {}) or {}
                rk = a.get("ratingKey") or a.get("key")
                if not rk:
                    continue
                progress_ms = _to_int(a.get("viewOffset"))
                if progress_ms is None or progress_ms <= 0:
                    continue
                if (_to_int(a.get("viewCount")) or 0) > 0:
                    continue
                key_id = str(rk)
                row, ids = _row_with_ids(el, library_id)
                if key_id in items:
                    _dbg("duplicate_progress_item", provider_item_id=key_id, source_library_id=library_id)
                    continue
                items[key_id] = (row, ids)
            start += len(rows)
            total = _page_total(root)
            if not rows or len(rows) < page_size or (total is not None and start >= total):
                break

    for library_id, plex_type in sections:
        _q(library_id, plex_type)

    if _mods_debug():
        _dbg(
            "index_fetch_counts",
            source="selected_libraries",
            count=len(items),
            libraries=len(sections),
            page_size=int(page_size),
            elapsed_ms=int((time.perf_counter() - started) * 1000),
        )
    return items


def build_index(adapter: Any, **_kwargs: Any) -> Mapping[str, dict[str, Any]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return {}

    need_scope, did_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        if need_scope and not did_switch:
            raise_home_scope_not_applied("progress", sel_aid, sel_uname)

        allowed = plex_feature_library_ids(adapter, "progress")
        resume_items = _fetch_resume_items(srv, page_size=150, allowed_library_ids=allowed)
        if not allowed:
            _dbg("library_scope_not_configured")
        out: dict[str, dict[str, Any]] = {}
        dbg = _mods_debug()
        token = active_pms_token(adapter)

        for rk in sorted(resume_items):
            a, ext_ids = resume_items[rk]
            library_id = _library_id(a)
            if allowed and not library_id:
                _dbg("missing_library_id", provider_item_id=str(rk), allowed_library_ids=sorted(allowed), item_title=str(a.get("title") or ""), media_type=str(a.get("type") or ""), provider_ids=dict(ext_ids))
                continue
            if allowed and library_id not in allowed:
                _dbg("outside_library_scope", provider_item_id=str(rk), source_library_id=library_id, allowed_library_ids=sorted(allowed), item_title=str(a.get("title") or ""), media_type=str(a.get("type") or ""), provider_ids=dict(ext_ids))
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

            base["ids"]["plex"] = str(rk)

            # Keep external IDs when available
            if has_external_ids(ext_ids):
                base["ids"].update(dict(ext_ids))
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

            enriched = minimal_from_history_row(a, token=token, allow_discover=True)
            if isinstance(enriched, Mapping):
                enriched_ids = enriched.get("ids") if isinstance(enriched.get("ids"), Mapping) else {}
                if enriched_ids:
                    base["ids"].update({str(k): v for k, v in enriched_ids.items() if v})
                    base["ids"]["plex"] = str(rk)
                enriched_show_ids = enriched.get("show_ids") if isinstance(enriched.get("show_ids"), Mapping) else {}
                if typ == "episode" and enriched_show_ids:
                    base.setdefault("show_ids", {})
                    base["show_ids"].update({str(k): v for k, v in enriched_show_ids.items() if v})
                for key_name in ("title", "series_title", "year", "season", "episode"):
                    if enriched.get(key_name) is not None and base.get(key_name) in (None, ""):
                        base[key_name] = enriched.get(key_name)

            norm = id_minimal(base)
            if library_id:
                norm["library_id"] = library_id
            if ts:
                norm["progress_at"] = ts
            norm["progress_ms"] = int(pos_ms)
            if dur_ms is not None and dur_ms > 0:
                norm["duration_ms"] = int(dur_ms)

            ck = canonical_key(norm)
            selected = norm
            if ck:
                selected, action = select_progress_record(out.get(ck), norm)
                out[ck] = selected
            else:
                action = "missing_canonical_key"

            if dbg:
                _dbg(
                    "item",
                    action=action,
                    ratingKey=str((selected.get("ids") or {}).get("plex") or rk),
                    type=selected.get("type") or base.get("type"),
                    chosen_viewOffset=int(selected.get("progress_ms") or pos_ms),
                    chosen_lastViewedAt=selected.get("progress_at") or ts,
                    ids=dict(selected.get("ids") or base.get("ids") or {}),
                    canonical_key=str(ck),
                )

        _info("index_done", count=len(out), allowed_library_ids=sorted(allowed), scope_enabled=bool(allowed))
        return out
    finally:
        home_scope_exit(adapter, did_switch)


def _resolve_rating_key(adapter: Any, it: Mapping[str, Any]) -> str | None:
    setattr(adapter, "_plex_progress_last_resolve_hint", None)
    allowed = plex_feature_library_ids(adapter, "progress")
    outside_scope_seen = False

    def _allowed_obj(obj: Any, method: str) -> bool:
        nonlocal outside_scope_seen
        if not allowed:
            return True
        lid = _library_id({
            "librarySectionID": getattr(obj, "librarySectionID", None),
            "sectionID": getattr(obj, "sectionID", None),
            "librarySectionId": getattr(obj, "librarySectionId", None),
            "sectionId": getattr(obj, "sectionId", None),
        })
        if lid in allowed:
            return True
        outside_scope_seen = True
        _dbg("target_candidate_outside_library_scope", provider_item_id=str(getattr(obj, "ratingKey", "") or ""), source_library_id=lid, allowed_library_ids=sorted(allowed), resolution_method=method, item_title=str(getattr(obj, "title", "") or ""), media_type=str(getattr(obj, "type", "") or ""), canonical_key=str(canonical_key(id_minimal(it)) or ""), provider_ids=dict(it.get("ids") or {}), show_ids=dict(it.get("show_ids") or {}), season=it.get("season"), episode=it.get("episode"))
        return False

    # Normalize IDs
    ids = ids_from(it)
    base_rk = (ids.get("plex") or "").strip()
    if base_rk.isdigit():
        srv0 = getattr(getattr(adapter, "client", None), "server", None)
        try:
            direct_obj = srv0.fetchItem(int(base_rk)) if srv0 else None  # type: ignore[attr-defined]
        except Exception:
            direct_obj = None
        if direct_obj is not None and _allowed_obj(direct_obj, "direct_plex_rating_key"):
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
                if _allowed_obj(obj, "exact_external_guid"):
                    return str(rk)
            if is_episode:
                if otype == "episode":
                    if _allowed_obj(obj, "exact_external_guid"):
                        return str(rk)
                if otype in ("show", "season"):
                    season = it.get("season")
                    episode = it.get("episode")
                    rk_ep = episode_rating_key_from_show(obj, season, episode)
                    if rk_ep:
                        try:
                            ep_obj = srv.fetchItem(int(rk_ep))  # type: ignore[attr-defined]
                        except Exception:
                            ep_obj = None
                        if ep_obj is not None and _allowed_obj(ep_obj, "show_guid_episode_number"):
                            return rk_ep
        except Exception:
            pass

    # A global GUID lookup can return a duplicate from an excluded section.
    if allowed and guid_candidates:
        try:
            library = getattr(srv, "library", None)
            for section_id in sorted(allowed):
                section = library.sectionByID(int(section_id)) if library else None
                if section is None:
                    continue
                for guid in guid_candidates:
                    try:
                        matches = list(section.search(guid=guid) or [])
                    except Exception:
                        matches = []
                    for obj in matches:
                        otype = str(getattr(obj, "type", "") or "").lower()
                        if not _allowed_obj(obj, "exact_external_guid_scoped_search"):
                            continue
                        if not is_episode and otype == "movie":
                            candidate = getattr(obj, "ratingKey", None)
                            if candidate:
                                return str(candidate)
                        if is_episode and otype == "episode":
                            candidate = getattr(obj, "ratingKey", None)
                            if candidate:
                                return str(candidate)
                        if is_episode and otype in ("show", "season"):
                            candidate = episode_rating_key_from_show(obj, it.get("season"), it.get("episode"))
                            if candidate:
                                return str(candidate)
        except Exception:
            pass

    strict = bool(plex_cfg_get(adapter, "strict_id_matching", False))
    if strict:
        if outside_scope_seen:
            setattr(adapter, "_plex_progress_last_resolve_hint", "outside_library_scope")
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
        if outside_scope_seen:
            setattr(adapter, "_plex_progress_last_resolve_hint", "outside_library_scope")
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

    scoped_hits = [obj for obj in hits if _allowed_obj(obj, "title_fallback")]
    if not scoped_hits:
        if outside_scope_seen:
            setattr(adapter, "_plex_progress_last_resolve_hint", "outside_library_scope")
        return None
    best = max(scoped_hits, key=_score)
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
    if outside_scope_seen:
        setattr(adapter, "_plex_progress_last_resolve_hint", "outside_library_scope")
    return None


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return 0, [{"item": dict(x), "hint": "not_configured"} for x in (items or [])]

    need_scope, did_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        if need_scope and not did_switch:
            unresolved = unresolved_home_scope_not_applied(items, sel_aid, sel_uname)
            _info("write_skipped", op="add", reason="home_scope_not_applied", selected=(sel_aid or sel_uname), unresolved=len(unresolved))
            return 0, unresolved

        applied = 0
        unresolved: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []

        for it in items or []:
            it0 = dict(it or {})
            ms = it0.get("progress_ms") or it0.get("viewOffset") or it0.get("progress")
            ms_i = _to_int(ms)
            if ms_i is None or ms_i <= 0:
                entry = {"status": "unresolved", "reason": "missing_progress", "item": it0}
                unresolved.append(entry)
                results.append(entry)
                if _mods_debug():
                    _dbg("add.unresolved", hint="missing_progress", canonical_key=str(canonical_key(id_minimal(it0)) or ""), ids=dict(ids_from(it0)))
                continue

            rk = _resolve_rating_key(adapter, it0)
            if not rk:
                reason = str(getattr(adapter, "_plex_progress_last_resolve_hint", "") or "not_found")
                entry = {"status": "unresolved", "reason": reason, "item": it0}
                unresolved.append(entry)
                results.append(entry)
                if _mods_debug():
                    _dbg("resolve_miss", hint="not_found", canonical_key=str(canonical_key(id_minimal(it0)) or ""), ids=dict(ids_from(it0)))
                continue

            try:
                obj = srv.fetchItem(int(rk))  # type: ignore[attr-defined]
                replay_enabled, drift = _progress_write_options(adapter)
                watched = bool(
                    _truthy_attr(obj, "isWatched")
                    or _truthy_attr(obj, "isPlayed")
                    or int(getattr(obj, "viewCount", 0) or 0) > 0
                )
                source_timestamp = it0.get("progress_at") or it0.get("lastViewedAt")
                target_timestamp = getattr(obj, "lastViewedAt", None) or getattr(obj, "viewedAt", None) or getattr(obj, "updatedAt", None)
                target_progress = _to_int(getattr(obj, "viewOffset", None))
                duration = _to_int(getattr(obj, "duration", None)) or _to_int(it0.get("duration_ms")) or 0
                decision = decide_progress_write(
                    active_session=_currently_playing(srv, rk, account_id=sel_aid, username=sel_uname),
                    source_timestamp=source_timestamp,
                    target_timestamp=target_timestamp,
                    source_progress_ms=ms_i,
                    source_duration_ms=it0.get("duration_ms") or duration,
                    target_progress_ms=target_progress,
                    target_duration_ms=duration,
                    target_watched=watched,
                    same_origin=_same_plex_endpoint(),
                    replay_enabled=replay_enabled,
                    timestamp_tolerance_seconds=drift,
                )
                context = {
                    "provider": "plex", "provider_instance": os.getenv("CW_PAIR_DST_INSTANCE") or "default",
                    "remote_item_id": str(rk), "library_id": _library_id(obj) or it0.get("library_id"),
                    "source_timestamp": source_timestamp, "target_timestamp": target_timestamp,
                    "source_progress": ms_i, "target_progress": target_progress, "reason": decision.reason,
                }
                if not decision.apply:
                    results.append({"status": "skipped", **context})
                    _dbg("write_skipped", **{key: value for key, value in context.items() if key != "provider"})
                    continue
                if decision.unwatch_first:
                    mark_unplayed = getattr(obj, "markUnplayed", None) or getattr(obj, "markUnwatched", None)
                    if callable(mark_unplayed):
                        mark_unplayed()
                    else:
                        srv.query(f"/:/unscrobble?key={rk}&identifier=com.plexapp.plugins.library")  # type: ignore[attr-defined]
                _timeline_progress(adapter, srv, rk, int(ms_i), duration)
                applied += 1
                results.append({"status": "applied", **context})
            except Exception as e:
                if _mods_debug():
                    _warn("write_failed", op="add", rating_key=str(rk), canonical_key=str(canonical_key(id_minimal(it0)) or ""), error=str(e))
                entry = {"status": "failed", "reason": "target_state_or_write_failed", "remote_item_id": str(rk), "item": it0, "hint": f"exception:{e}"}
                unresolved.append(entry)
                results.append(entry)

        setattr(adapter, "_progress_write_results", results)
        _info("write_done", op="add", ok=len(unresolved) == 0, applied=applied, skipped=sum(1 for row in results if row.get("status") == "skipped"), unresolved=sum(1 for row in results if row.get("status") == "unresolved"), failed=sum(1 for row in results if row.get("status") == "failed"))
        return applied, unresolved
    finally:
        home_scope_exit(adapter, did_switch)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return 0, [{"item": dict(x), "hint": "not_configured"} for x in (items or [])]

    need_scope, did_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        if need_scope and not did_switch:
            unresolved = unresolved_home_scope_not_applied(items, sel_aid, sel_uname)
            _info("write_skipped", op="remove", reason="home_scope_not_applied", selected=(sel_aid or sel_uname), unresolved=len(unresolved))
            return 0, unresolved

        ok = 0
        unresolved: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []

        for it in items or []:
            it0 = dict(it or {})
            rk = _resolve_rating_key(adapter, it0)
            if not rk:
                entry = {"status": "unresolved", "reason": "not_found", "item": it0}
                unresolved.append(entry)
                results.append(entry)
                if _mods_debug():
                    _dbg("resolve_miss", hint="not_found", canonical_key=str(canonical_key(id_minimal(it0)) or ""), ids=dict(ids_from(it0)))
                continue

            try:
                obj = srv.fetchItem(int(rk))  # type: ignore[attr-defined]
                duration = _to_int(getattr(obj, "duration", None)) or _to_int(it0.get("duration_ms")) or 0
                _timeline_progress(adapter, srv, rk, 0, duration)
                ok += 1
                results.append({"status": "applied", "provider": "plex", "provider_instance": os.getenv("CW_PAIR_DST_INSTANCE") or "default", "remote_item_id": str(rk), "library_id": _library_id(obj) or it0.get("library_id"), "source_progress": 0, "target_progress": _to_int(getattr(obj, "viewOffset", None)), "reason": "clear_progress"})
            except Exception as e:
                if _mods_debug():
                    _warn("write_failed", op="remove", rating_key=str(rk), canonical_key=str(canonical_key(id_minimal(it0)) or ""), error=str(e))
                entry = {"status": "failed", "reason": "clear_progress_failed", "item": it0, "remote_item_id": str(rk), "hint": f"exception:{e}"}
                unresolved.append(entry)
                results.append(entry)

        setattr(adapter, "_progress_write_results", results)
        _info("write_done", op="remove", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved), mode="clear_resume")
        return ok, unresolved
    finally:
        home_scope_exit(adapter, did_switch)
