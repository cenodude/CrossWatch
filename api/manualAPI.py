# /api/manualAPI.py
# CrossWatch - Manual history marking API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import threading
import time
from datetime import date as dt_date, datetime, timezone
from typing import Any, cast

import requests
from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse

from cw_platform.access_policy import filter_instances_for_user, request_user
from cw_platform.modules_registry import load_sync_ops, sync_provider_names
from cw_platform.provider_instances import build_provider_config_view, list_instance_ids, normalize_instance_id

router = APIRouter(prefix="/api/manual", tags=["manual"])

_LOG = logging.getLogger("crosswatch.api.manual")

MANUAL_EPISODES_MAX = 500
EPISODE_LIST_PROVIDERS = frozenset({"SIMKL"})
_EPISODE_LIST_TTL = 3600
_EPISODE_LIST_CACHE: dict[tuple[str, str, int], tuple[float, list[dict[str, Any]]]] = {}
_EPISODE_LIST_LOCK = threading.Lock()


def _tmdb_api_key(cfg: dict[str, Any]) -> str:
    def _pick_from_block(blk: Any) -> str:
        if not isinstance(blk, dict):
            return ""
        k = str(blk.get("api_key") or "").strip()
        if k:
            return k
        insts = blk.get("instances")
        if isinstance(insts, dict):
            for v in insts.values():
                kk = str((v or {}).get("api_key") or "").strip() if isinstance(v, dict) else ""
                if kk:
                    return kk
        return ""

    for key in ("tmdb", "tmdb_sync"):
        found = _pick_from_block(cfg.get(key))
        if found:
            return found
    return ""


def _normalize_media_type(value: Any) -> str:
    t = str(value or "").strip().lower()
    if t in {"tv", "show", "shows", "series", "anime"}:
        return "show"
    return "movie"


def _parse_manual_date(value: Any) -> str | None:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return dt_date.fromisoformat(s).isoformat()
    except Exception:
        return None


def _iso_noon_utc(date_str: str) -> str:
    d = dt_date.fromisoformat(date_str)
    return datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _tmdb_release_date(cfg: dict[str, Any], media_type: str, tmdb_id: Any) -> str | None:
    api_key = _tmdb_api_key(cfg)
    if not api_key:
        return None

    typ = "tv" if _normalize_media_type(media_type) == "show" else "movie"
    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/{typ}/{tmdb_id}",
            params={"api_key": api_key},
            timeout=8,
        )
        r.raise_for_status()
        data = r.json() or {}
    except Exception:
        return None

    raw = data.get("first_air_date") if typ == "tv" else data.get("release_date")
    return _parse_manual_date(raw)


def _manual_watched_at(cfg: dict[str, Any], media_type: str, tmdb_id: Any, mode: Any, custom_date: Any) -> tuple[str | None, str | None]:
    selected = str(mode or "today").strip().lower()
    if selected == "today":
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"), None
    if selected == "custom":
        picked = _parse_manual_date(custom_date)
        if not picked:
            return None, "invalid_custom_date"
        return _iso_noon_utc(picked), None
    if selected == "release":
        release_date = _tmdb_release_date(cfg, media_type, tmdb_id)
        if not release_date:
            return None, "release_date_unavailable"
        return _iso_noon_utc(release_date), None
    return None, "invalid_date_mode"


def _manual_rating(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        rating = int(str(value).strip())
    except Exception:
        return None
    return rating if 1 <= rating <= 10 else None


def _manual_external_ids(media_type: str, tmdb_id: Any) -> dict[str, Any]:
    try:
        from .metaAPI import _tmdb_external_ids

        raw = _tmdb_external_ids("tv" if _normalize_media_type(media_type) == "show" else "movie", tmdb_id) or {}
    except Exception:
        raw = {}

    out: dict[str, Any] = {}
    tmdb_s = str(tmdb_id or "").strip()
    if tmdb_s:
        out["tmdb"] = int(tmdb_s) if tmdb_s.isdigit() else tmdb_s

    for key in ("imdb", "tvdb"):
        val = raw.get(key)
        if val in (None, ""):
            continue
        s = str(val).strip()
        if not s:
            continue
        out[key] = int(s) if (key != "imdb" and s.isdigit()) else s
    return out


def _int_or_none(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _manual_episodes(value: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    for raw in value if isinstance(value, list) else []:
        if not isinstance(raw, dict):
            continue
        season = _int_or_none(raw.get("season"))
        episode = _int_or_none(raw.get("episode"))
        if season is None or episode is None or season < 0 or episode < 1 or (season, episode) in seen:
            continue
        seen.add((season, episode))
        out.append({"season": season, "episode": episode, "air_date": _parse_manual_date(raw.get("air_date"))})
    out.sort(key=lambda row: (row["season"], row["episode"]))
    return out


def _manual_episode_item(show: dict[str, Any], row: dict[str, Any], watched_at: str) -> dict[str, Any]:
    item: dict[str, Any] = {
        "type": "episode",
        "title": show.get("title") or "",
        "series_title": show.get("title") or "",
        "show_ids": dict(show.get("ids") or {}),
        "ids": {},
        "season": row["season"],
        "episode": row["episode"],
        "watched_at": watched_at,
    }
    if show.get("year") not in (None, ""):
        item["year"] = show["year"]
    return item


def _split_source(value: Any) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw or raw.lower() == "tmdb":
        return "", ""
    provider, _, instance = raw.partition(":")
    return provider.strip().upper(), normalize_instance_id(instance or "default")


def _season_row(season: int, name: Any, episodes: list[dict[str, Any]] | None, count: Any = None) -> dict[str, Any]:
    label = str(name or "").strip() or ("Specials" if season == 0 else f"Season {season}")
    total = len(episodes) if episodes is not None else (_int_or_none(count) or 0)
    return {"season": season, "name": label, "episode_count": total, "episodes": episodes}


def _tmdb_season_list(tmdb_id: int) -> list[dict[str, Any]] | None:
    from .metaAPI import _cfg_ui_locale, _tmdb_provider

    provider = _tmdb_provider()
    if provider is None:
        return None
    data = provider._get(f"https://api.themoviedb.org/3/tv/{tmdb_id}", {"language": _cfg_ui_locale() or "en-US"}, quiet_404=True) or {}
    rows = []
    for raw in data.get("seasons") or []:
        season = _int_or_none(raw.get("season_number")) if isinstance(raw, dict) else None
        if season is None or not (_int_or_none(raw.get("episode_count")) or 0):
            continue
        rows.append(_season_row(season, raw.get("name"), None, raw.get("episode_count")))
    rows.sort(key=lambda row: (row["season"] == 0, row["season"]))
    return rows


def _tmdb_season_episodes(tmdb_id: int, season: int) -> list[dict[str, Any]] | None:
    from .metaAPI import _cfg_ui_locale, _tmdb_provider

    provider = _tmdb_provider()
    if provider is None:
        return None
    data = provider._get(f"https://api.themoviedb.org/3/tv/{tmdb_id}/season/{season}", {"language": _cfg_ui_locale() or "en-US"}, quiet_404=True) or {}
    rows = []
    for raw in data.get("episodes") or []:
        episode = _int_or_none(raw.get("episode_number")) if isinstance(raw, dict) else None
        if episode is None or episode < 1:
            continue
        rows.append({"episode": episode, "name": str(raw.get("name") or ""), "air_date": _parse_manual_date(raw.get("air_date")) or ""})
    return rows


def _simkl_episode_rows(payload: Any) -> list[dict[str, Any]]:
    seasons: dict[int, list[dict[str, Any]]] = {}
    for raw in payload if isinstance(payload, list) else []:
        if not isinstance(raw, dict):
            continue
        kind = str(raw.get("type") or "episode").strip().lower()
        season = _int_or_none(raw.get("season"))
        episode = _int_or_none(raw.get("episode"))
        if kind == "special" and season is None:
            season = 0
        if season is None or episode is None or season < 0 or episode < 1:
            continue
        seasons.setdefault(season, []).append({
            "episode": episode,
            "name": str(raw.get("title") or ""),
            "air_date": _parse_manual_date(str(raw.get("date") or "")[:10]) or "",
        })
    rows = []
    for season, episodes in seasons.items():
        unique = {row["episode"]: row for row in episodes}
        rows.append(_season_row(season, "", [unique[key] for key in sorted(unique)]))
    rows.sort(key=lambda row: (row["season"] == 0, row["season"]))
    return rows


def _simkl_episode_list(cfg_view: dict[str, Any], tmdb_id: int) -> list[dict[str, Any]] | None:
    from providers.sync.simkl._common import build_headers
    from providers.sync.simkl._history import BASE, simkl_api_params_from_headers

    headers = build_headers(cfg_view)
    simkl_id = ""
    with requests.Session() as session:
        response = session.get(
            f"{BASE}/search/id",
            headers=headers,
            params=simkl_api_params_from_headers(headers, tmdb=str(tmdb_id), type="show"),
            timeout=10,
        )
        if not response.ok:
            return None
        found = response.json() if (response.text or "").strip() else []
        for row in found if isinstance(found, list) else []:
            if not isinstance(row, dict):
                continue
            raw_node = row.get("show")
            node: dict[str, Any] = raw_node if isinstance(raw_node, dict) else row
            if str(row.get("type") or node.get("type") or "").strip().lower() not in {"", "tv", "show"}:
                continue
            raw_ids = node.get("ids")
            ids: dict[str, Any] = raw_ids if isinstance(raw_ids, dict) else {}
            simkl_id = str(ids.get("simkl") or ids.get("simkl_id") or "").strip()
            if simkl_id:
                break
        if not simkl_id.isdigit():
            return None
        response = session.get(
            f"{BASE}/tv/episodes/{simkl_id}",
            headers=headers,
            params=simkl_api_params_from_headers(headers, extended="full"),
            timeout=15,
        )
        if not response.ok:
            return None
        return _simkl_episode_rows(response.json() if (response.text or "").strip() else [])


def _provider_episode_list(cfg: dict[str, Any], provider: str, instance: str, tmdb_id: int) -> list[dict[str, Any]] | None:
    key = (provider, instance, tmdb_id)
    now = time.monotonic()
    with _EPISODE_LIST_LOCK:
        hit = _EPISODE_LIST_CACHE.get(key)
        if hit and now - hit[0] < _EPISODE_LIST_TTL:
            return hit[1]
    rows = _simkl_episode_list(build_provider_config_view(cfg, provider, instance), tmdb_id) if provider == "SIMKL" else None
    if rows:
        with _EPISODE_LIST_LOCK:
            _EPISODE_LIST_CACHE[key] = (now, rows)
    return rows


def _manual_history_targets(cfg: dict[str, Any], user: Any = None) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}

    for provider in sync_provider_names(upper=True):
        ops = load_sync_ops(provider)
        if not ops:
            continue

        try:
            supported = dict(ops.features() or {})
        except Exception:
            supported = {}

        history_ok = bool(supported.get("history"))
        ratings_ok = bool(supported.get("ratings"))
        watchlist_ok = bool(supported.get("watchlist"))
        if not history_ok and not ratings_ok and not watchlist_ok:
            continue

        try:
            instances = filter_instances_for_user(cfg, user, provider, list_instance_ids(cfg, provider))
        except Exception:
            instances = filter_instances_for_user(cfg, user, provider, ["default"])

        for raw_instance in instances:
            instance = normalize_instance_id(raw_instance)
            cfg_view = build_provider_config_view(cfg, provider, instance)
            try:
                configured = bool(ops.is_configured(cfg_view))
            except Exception:
                configured = False
            if not configured:
                continue

            label = provider.title()
            try:
                label = str(ops.label() or label)
            except Exception:
                pass

            merged[(provider, instance)] = {
                "provider": provider,
                "instance": instance,
                "label": label,
                "display": label if instance == "default" else f"{label} ({instance})",
                "history_enabled": history_ok,
                "ratings_enabled": ratings_ok,
                "watchlist_enabled": watchlist_ok,
                "episode_list": history_ok and provider in EPISODE_LIST_PROVIDERS,
            }

    out = [v for v in merged.values() if bool(v.get("history_enabled") or v.get("watchlist_enabled"))]
    out.sort(key=lambda item: (str(item.get("label") or "").lower(), str(item.get("instance") or "")))
    return out


@router.get("/providers")
def api_manual_providers(request: Request = cast(Request, None)) -> JSONResponse:
    from cw_platform.config_base import load_config

    cfg = load_config() or {}
    return JSONResponse({"ok": True, "providers": _manual_history_targets(cfg, request_user(request))}, status_code=200)


@router.get("/episodes")
def api_manual_episodes(
    request: Request,
    tmdb: int = Query(..., ge=1),
    source: str = Query("tmdb", max_length=120),
    season: int | None = Query(None, ge=0),
) -> JSONResponse:
    from cw_platform.config_base import load_config

    cfg = load_config() or {}
    provider, instance = _split_source(source)
    try:
        if not provider:
            if season is not None:
                episodes = _tmdb_season_episodes(tmdb, season)
                if episodes is None:
                    return JSONResponse({"ok": False, "error": "tmdb_unavailable"}, status_code=200)
                return JSONResponse({"ok": True, "source": "tmdb", "season": season, "episodes": episodes})
            seasons = _tmdb_season_list(tmdb)
            if seasons is None:
                return JSONResponse({"ok": False, "error": "tmdb_unavailable"}, status_code=200)
            return JSONResponse({"ok": True, "source": "tmdb", "seasons": seasons})
        allowed = {
            (str(row.get("provider") or "").upper(), normalize_instance_id(row.get("instance") or "default"))
            for row in _manual_history_targets(cfg, request_user(request))
            if row.get("episode_list")
        }
        if (provider, instance) not in allowed:
            return JSONResponse({"ok": False, "error": "episode_list_not_available"}, status_code=400)
        seasons = _provider_episode_list(cfg, provider, instance, tmdb)
        if not seasons:
            return JSONResponse({"ok": False, "error": "episode_list_not_found"}, status_code=200)
        return JSONResponse({"ok": True, "source": f"{provider}:{instance}", "seasons": seasons})
    except Exception:
        _LOG.exception("manual episode list failed for %s", source)
        return JSONResponse({"ok": False, "error": "episode_list_failed"}, status_code=200)


@router.post("/watched")
def api_manual_watched(payload: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> JSONResponse:
    from cw_platform.config_base import load_config

    cfg = load_config() or {}
    item = payload.get("item") or {}
    selected_targets = payload.get("providers") or []

    media_type = _normalize_media_type(item.get("type") or item.get("media_type"))
    tmdb_id = item.get("tmdb") or item.get("tmdb_id") or (item.get("ids") or {}).get("tmdb")
    title = str(item.get("title") or item.get("name") or "").strip()
    year = item.get("year")

    if tmdb_id in (None, ""):
        return JSONResponse({"ok": False, "error": "missing_tmdb_id"}, status_code=400)
    if not isinstance(selected_targets, list) or not selected_targets:
        return JSONResponse({"ok": False, "error": "missing_providers"}, status_code=400)

    raw_actions = payload.get("actions") or {}
    actions = raw_actions if isinstance(raw_actions, dict) else {}
    do_history = bool(actions.get("history", True))
    do_watchlist = bool(actions.get("watchlist"))
    do_rating = bool(actions.get("rating"))
    if not (do_history or do_watchlist or do_rating):
        return JSONResponse({"ok": False, "error": "missing_actions"}, status_code=400)

    raw_episodes = payload.get("episodes")
    episodes = _manual_episodes(raw_episodes) if media_type == "show" and do_history else []
    if raw_episodes and media_type == "show" and do_history and not episodes:
        return JSONResponse({"ok": False, "error": "invalid_episodes"}, status_code=400)
    if len(episodes) > MANUAL_EPISODES_MAX:
        return JSONResponse({"ok": False, "error": "too_many_episodes"}, status_code=400)
    source_provider, source_instance = _split_source(payload.get("episode_source"))
    if episodes and source_provider:
        picked = {
            (str(raw.get("provider") or "").strip().upper(), normalize_instance_id(raw.get("instance") or raw.get("provider_instance") or "default"))
            for raw in selected_targets
            if isinstance(raw, dict)
        }
        if picked != {(source_provider, source_instance)}:
            return JSONResponse({"ok": False, "error": "episode_source_mismatch"}, status_code=400)

    date_mode = str(payload.get("date_mode") or "today").strip().lower()
    watched_at, dt_error = _manual_watched_at(
        cfg,
        media_type,
        tmdb_id,
        payload.get("date_mode"),
        payload.get("watched_on"),
    )
    if not watched_at and episodes and dt_error == "release_date_unavailable" and all(row["air_date"] for row in episodes):
        watched_at = _iso_noon_utc(min(row["air_date"] for row in episodes))
    if not watched_at:
        return JSONResponse({"ok": False, "error": dt_error or "invalid_watched_date"}, status_code=400)

    raw_rating = payload.get("rating")
    rating = _manual_rating(raw_rating)
    if do_rating and rating is None:
        return JSONResponse({"ok": False, "error": "missing_rating"}, status_code=400)
    if raw_rating not in (None, "") and rating is None:
        return JSONResponse({"ok": False, "error": "invalid_rating"}, status_code=400)

    available = _manual_history_targets(cfg, request_user(request))
    target_map = {
        (str(it.get("provider") or "").upper(), normalize_instance_id(it.get("instance") or "default")): it
        for it in available
    }

    ids = _manual_external_ids(media_type, tmdb_id)
    item_payload: dict[str, Any] = {
        "type": media_type,
        "title": title,
        "ids": ids,
        "watched_at": watched_at,
    }
    if year not in (None, ""):
        item_payload["year"] = year

    history_items = [
        _manual_episode_item(
            item_payload,
            row,
            _iso_noon_utc(row["air_date"]) if date_mode == "release" and row["air_date"] else watched_at,
        )
        for row in episodes
    ] or [item_payload]

    results: list[dict[str, Any]] = []
    success_count = 0

    for raw in selected_targets:
        if not isinstance(raw, dict):
            continue

        provider = str(raw.get("provider") or "").strip().upper()
        instance = normalize_instance_id(raw.get("instance") or raw.get("provider_instance") or "default")
        target = target_map.get((provider, instance))
        if not target:
            results.append({"provider": provider, "instance": instance, "ok": False, "error": "provider_not_allowed"})
            continue

        ops = load_sync_ops(provider)
        if not ops:
            results.append({"provider": provider, "instance": instance, "ok": False, "error": "provider_unavailable"})
            continue

        cfg_view = build_provider_config_view(cfg, provider, instance)
        history_res: dict[str, Any] | None = None
        history_skipped: str | None = None
        if do_history and bool(target.get("history_enabled")):
            try:
                hr = ops.add(cfg_view, history_items, feature="history")
                history_res = dict(hr) if isinstance(hr, dict) else {"ok": bool(hr)}
            except Exception:
                _LOG.exception("manual history add failed for %s:%s", provider, instance)
                history_res = {"ok": False, "error": "history_add_failed"}
        elif do_history:
            history_skipped = "history_not_supported"

        watchlist_res: dict[str, Any] | None = None
        watchlist_skipped: str | None = None
        if do_watchlist:
            if bool(target.get("watchlist_enabled")):
                try:
                    wr = ops.add(cfg_view, [item_payload], feature="watchlist")
                    watchlist_res = dict(wr) if isinstance(wr, dict) else {"ok": bool(wr)}
                except Exception:
                    _LOG.exception("manual watchlist add failed for %s:%s", provider, instance)
                    watchlist_res = {"ok": False, "error": "watchlist_add_failed"}
            else:
                watchlist_skipped = "watchlist_not_supported"

        rating_res: dict[str, Any] | None = None
        rating_skipped: str | None = None
        if do_rating:
            if bool(target.get("ratings_enabled")):
                rating_payload = dict(item_payload)
                rating_payload["rating"] = rating
                rating_payload["rated_at"] = watched_at
                try:
                    rr = ops.add(cfg_view, [rating_payload], feature="ratings")
                    rating_res = dict(rr) if isinstance(rr, dict) else {"ok": bool(rr)}
                except Exception:
                    _LOG.exception("manual rating add failed for %s:%s", provider, instance)
                    rating_res = {"ok": False, "error": "rating_add_failed"}
            else:
                rating_skipped = "ratings_not_supported"

        history_ok = bool(history_res is None or bool(history_res.get("ok")) or history_skipped)
        watchlist_ok = bool(watchlist_res is None or bool(watchlist_res.get("ok")) or watchlist_skipped)
        rating_ok = bool(rating_res is None or bool(rating_res.get("ok")) or rating_skipped)
        ok = history_ok and watchlist_ok and rating_ok
        if ok:
            success_count += 1

        entry: dict[str, Any] = {"provider": provider, "instance": instance, "ok": ok}
        if history_res is not None:
            entry["history"] = history_res
        if history_skipped:
            entry["history_skipped"] = history_skipped
        if watchlist_res is not None:
            entry["watchlist"] = watchlist_res
        if watchlist_skipped:
            entry["watchlist_skipped"] = watchlist_skipped
        if rating_res is not None:
            entry["rating"] = rating_res
        if rating_skipped:
            entry["rating_skipped"] = rating_skipped
        results.append(entry)

    return JSONResponse(
        {
            "ok": success_count > 0 and all(bool(it.get("ok")) for it in results),
            "watched_at": watched_at,
            "episodes": len(episodes),
            "results": results,
        },
        status_code=200,
    )
