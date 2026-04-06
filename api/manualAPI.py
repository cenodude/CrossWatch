# /api/manualAPI.py
# CrossWatch - Manual history marking API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import date as dt_date, datetime, timezone
from typing import Any

import requests
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from cw_platform.modules_registry import load_sync_ops
from cw_platform.provider_instances import build_provider_config_view, list_instance_ids, normalize_instance_id

router = APIRouter(prefix="/api/manual", tags=["manual"])


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


def _manual_history_targets(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}

    for provider in (
        "PLEX",
        "SIMKL",
        "ANILIST",
        "TRAKT",
        "TMDB",
        "JELLYFIN",
        "EMBY",
        "MDBLIST",
        "CROSSWATCH",
    ):
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
            instances = list_instance_ids(cfg, provider)
        except Exception:
            instances = ["default"]

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
            }

    out = [v for v in merged.values() if bool(v.get("history_enabled") or v.get("watchlist_enabled"))]
    out.sort(key=lambda item: (str(item.get("label") or "").lower(), str(item.get("instance") or "")))
    return out


@router.get("/providers")
def api_manual_providers() -> JSONResponse:
    from cw_platform.config_base import load_config

    cfg = load_config() or {}
    return JSONResponse({"ok": True, "providers": _manual_history_targets(cfg)}, status_code=200)


@router.post("/watched")
def api_manual_watched(payload: dict[str, Any] = Body(...)) -> JSONResponse:
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

    watched_at, dt_error = _manual_watched_at(
        cfg,
        media_type,
        tmdb_id,
        payload.get("date_mode"),
        payload.get("watched_on"),
    )
    if not watched_at:
        return JSONResponse({"ok": False, "error": dt_error or "invalid_watched_date"}, status_code=400)

    raw_rating = payload.get("rating")
    rating = _manual_rating(raw_rating)
    if do_rating and rating is None:
        return JSONResponse({"ok": False, "error": "missing_rating"}, status_code=400)
    if raw_rating not in (None, "") and rating is None:
        return JSONResponse({"ok": False, "error": "invalid_rating"}, status_code=400)

    available = _manual_history_targets(cfg)
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
                hr = ops.add(cfg_view, [item_payload], feature="history")
                history_res = dict(hr) if isinstance(hr, dict) else {"ok": bool(hr)}
            except Exception as exc:
                history_res = {"ok": False, "error": str(exc)}
        elif do_history:
            history_skipped = "history_not_supported"

        watchlist_res: dict[str, Any] | None = None
        watchlist_skipped: str | None = None
        if do_watchlist:
            if bool(target.get("watchlist_enabled")):
                try:
                    wr = ops.add(cfg_view, [item_payload], feature="watchlist")
                    watchlist_res = dict(wr) if isinstance(wr, dict) else {"ok": bool(wr)}
                except Exception as exc:
                    watchlist_res = {"ok": False, "error": str(exc)}
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
                except Exception as exc:
                    rating_res = {"ok": False, "error": str(exc)}
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
            "results": results,
        },
        status_code=200,
    )
