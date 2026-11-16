# /providers/sync/emby/_continue.py
# EMBY Continue Watching row read and hide-from-resume writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping

from providers.sync._continue_watching import SHOWN

from . import _progress
from ._common import chunked, item_rows, make_logger

_dbg, _info, _warn, _error = make_logger("continue_watching")


def _epoch(value: Any) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _series_by_item(http: Any, uid: str, item_ids: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for batch in chunked(item_ids, 100):
        response = http.get(f"/Users/{uid}/Items", params={"Ids": ",".join(batch), "Fields": "SeriesId"})
        for row in item_rows(response, "emby_continue_series"):
            item_id = str(row.get("Id") or "").strip()
            series_id = str(row.get("SeriesId") or "").strip()
            if item_id and series_id:
                out[item_id] = series_id
    return out


def _resume_row(http: Any, uid: str) -> tuple[set[str], set[str]]:
    response = http.get(
        f"/Users/{uid}/Items/Resume",
        params={"Recursive": True, "MediaTypes": "Video", "Limit": 500, "Fields": "SeriesId", "EnableUserData": False},
    )
    movies: set[str] = set()
    series: set[str] = set()
    for row in item_rows(response, "emby_continue_row"):
        kind = str(row.get("Type") or "").lower()
        if kind == "movie" and row.get("Id"):
            movies.add(str(row["Id"]))
        elif kind == "episode" and row.get("SeriesId"):
            series.add(str(row["SeriesId"]))
    return movies, series


def _played_after(http: Any, uid: str, series_id: str, since: float) -> bool:
    response = http.get(
        f"/Users/{uid}/Items",
        params={
            "ParentId": series_id,
            "Recursive": True,
            "IncludeItemTypes": "Episode",
            "Filters": "IsPlayed",
            "SortBy": "DatePlayed",
            "SortOrder": "Descending",
            "Limit": 1,
            "Fields": "UserData",
            "EnableUserData": True,
        },
    )
    for row in item_rows(response, "emby_continue_played"):
        user_data = row.get("UserData")
        played = _epoch(user_data.get("LastPlayedDate")) if isinstance(user_data, Mapping) else None
        return played is not None and played > since
    return False


def view(adapter: Any) -> dict[str, dict[str, Any]]:
    http = getattr(adapter, "client", None)
    uid = str(getattr(getattr(adapter, "cfg", None), "user_id", "") or "")
    if not http or not uid:
        raise RuntimeError("emby_continue_not_configured")
    index = {str(k): dict(v) for k, v in dict(_progress.build_index(adapter)).items()}
    episodes = sorted({str(it.get("emby_item_id")) for it in index.values()
                       if it.get("type") == "episode" and it.get("emby_item_id")})
    series_of = _series_by_item(http, uid, episodes) if episodes else {}
    movies, series = _resume_row(http, uid)
    later_played: dict[str, bool] = {}
    for item in index.values():
        item_id = str(item.get("emby_item_id") or "")
        if item.get("type") == "movie":
            item[SHOWN] = item_id in movies if item_id else None
            continue
        series_id = series_of.get(item_id)
        if not series_id:
            item[SHOWN] = None
            continue
        item["_emby_series_id"] = series_id
        if series_id in series:
            item[SHOWN] = True
            continue
        since = _epoch(item.get("progress_at"))
        if since is None:
            item[SHOWN] = None
            continue
        if series_id not in later_played:
            later_played[series_id] = _played_after(http, uid, series_id, since)
        item[SHOWN] = None if later_played[series_id] else False
    _info("view_done", items=len(index), row_movies=len(movies), row_series=len(series),
          hidden=sum(1 for it in index.values() if it.get(SHOWN) is False))
    return index


def hide(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    http = getattr(adapter, "client", None)
    uid = str(getattr(getattr(adapter, "cfg", None), "user_id", "") or "")
    if not http or not uid:
        raise RuntimeError("emby_continue_not_configured")
    confirmed: list[str] = []
    errors = 0
    rows = list(items)
    for item in rows:
        item_id = str(item.get("emby_item_id") or "")
        if not item_id:
            errors += 1
            continue
        response = http.post(f"/Users/{uid}/Items/{item_id}/HideFromResume", params={"Hide": "true"})
        status = getattr(response, "status_code", 0)
        if status in (200, 204):
            confirmed.append(str(item.get("_cw_key") or ""))
        else:
            errors += 1
            _warn("hide_failed", provider_item_id=item_id, status=status)
    return {"attempted": len(rows), "confirmed_keys": [k for k in confirmed if k], "errors": errors}
