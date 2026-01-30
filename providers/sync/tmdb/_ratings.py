# /providers/sync/_mod_ratings.py
# CrossWatch - TMDb ratings adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import minimal as id_minimal

from .._log import log as cw_log

from ._common import (
    as_int,
    fetch_external_ids,
    iso_z_from_tmdb,
    key_of,
    pick_media_type,
    resolve_tmdb_id,
    tmdb_id_from_item,
    unresolved_item,
    year_from_date,
)


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("TMDB", "ratings", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("TMDB", "ratings", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("TMDB", "ratings", "warn", msg, **fields)


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    client = adapter.client
    account_id = client.account_id()
    prog = adapter.progress_factory("ratings")
    out: dict[str, dict[str, Any]] = {}

    def pull(path: str, *, typ: str) -> None:
        page = 1
        total_pages = 1
        while page <= total_pages:
            r = client.get(
                f"{client.BASE}/account/{account_id}/{path}",
                params=client._user_params({"page": page, "sort_by": "created_at.asc"}),
            )
            if not (200 <= r.status_code < 300):
                _warn("fetch_failed", path=path, page=page, status=r.status_code)
                raise RuntimeError(f"TMDb ratings fetch failed ({path}) ({r.status_code})")
            data = r.json() if (r.text or "").strip() else {}
            if not isinstance(data, Mapping):
                data = {}
            total_pages = as_int(data.get("total_pages")) or total_pages
            raw_results = data.get("results")
            results: list[Any] = raw_results if isinstance(raw_results, list) else []
            _dbg("page", path=path, typ=typ, page=page, total_pages=total_pages, batch=len(results))

            for raw in results:
                if not isinstance(raw, Mapping):
                    continue
                tmdb_id = as_int(raw.get("id"))
                rating = raw.get("rating")
                if tmdb_id is None or rating is None:
                    continue
                try:
                    rating_i = int(round(float(rating)))
                except Exception:
                    continue
                rated_at = iso_z_from_tmdb(raw.get("created_at"))

                if typ == "movie":
                    title = raw.get("title") or raw.get("original_title")
                    year = year_from_date(raw.get("release_date"))
                    ids = fetch_external_ids(adapter, kind="movie", tmdb_id=int(tmdb_id))
                    ids.setdefault("tmdb", int(tmdb_id))
                    item = {"type": "movie", "title": title, "year": year, "ids": ids}
                elif typ == "tv":
                    title = raw.get("name") or raw.get("original_name")
                    year = year_from_date(raw.get("first_air_date"))
                    ids = fetch_external_ids(adapter, kind="tv", tmdb_id=int(tmdb_id))
                    ids.setdefault("tmdb", int(tmdb_id))
                    item = {"type": "show", "title": title, "year": year, "ids": ids}
                else:
                    show_id = as_int(raw.get("show_id") or raw.get("series_id"))
                    season = as_int(raw.get("season_number"))
                    episode = as_int(raw.get("episode_number"))
                    if show_id is None or season is None or episode is None:
                        continue
                    title = raw.get("name") or raw.get("title")
                    year = year_from_date(raw.get("air_date"))
                    show_ids = {"tmdb": show_id}
                    show_ext = fetch_external_ids(adapter, kind="tv", tmdb_id=int(show_id))
                    if show_ext:
                        show_ids.update(show_ext)

                    item = {
                        "type": "episode",
                        "title": title,
                        "year": year,
                        "season": season,
                        "episode": episode,
                        "ids": {"tmdb": tmdb_id},
                        "show_ids": show_ids,
                    }

                item["rating"] = max(1, min(10, int(rating_i)))
                if rated_at:
                    item["rated_at"] = rated_at
                mini = id_minimal(item)
                out[key_of(mini)] = mini

            prog.tick(len(out), total=None)
            page += 1

    _info("build_index", account_id=account_id)
    pull("rated/movies", typ="movie")
    pull("rated/tv", typ="tv")
    pull("rated/tv/episodes", typ="episode")
    prog.done(ok=True)
    _info("build_index_done", count=len(out))
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    client = adapter.client
    unresolved: list[dict[str, Any]] = []
    ok = 0
    for it in items or []:
        if not isinstance(it, Mapping):
            continue
        rating = it.get("rating")
        if rating is None:
            unresolved.append(unresolved_item(it, "missing_rating"))
            continue
        try:
            val_f = float(rating)
        except Exception:
            unresolved.append(unresolved_item(it, "missing_rating"))
            continue
        val = float(max(1, min(10, int(round(val_f)))))

        typ = pick_media_type(it)
        if typ == "movie":
            tid = resolve_tmdb_id(adapter, it, want="movie")
            if tid is None:
                unresolved.append(unresolved_item(it, "missing_tmdb_id"))
                continue
            url = f"{client.BASE}/movie/{tid}/rating"
            r = client.post(url, params=client._user_params(), json={"value": val})
        elif typ == "tv":
            tid = resolve_tmdb_id(adapter, it, want="tv")
            if tid is None:
                unresolved.append(unresolved_item(it, "missing_tmdb_id"))
                continue
            url = f"{client.BASE}/tv/{tid}/rating"
            r = client.post(url, params=client._user_params(), json={"value": val})
        elif typ == "episode":
            show_ids = it.get("show_ids") if isinstance(it.get("show_ids"), Mapping) else None
            show_id = tmdb_id_from_item(show_ids or {}) if show_ids else None
            if show_id is None:
                show_id = resolve_tmdb_id(adapter, it, want="tv")
            if show_id is None:
                unresolved.append(unresolved_item(it, "missing_show_tmdb_id"))
                continue
            season = as_int(it.get("season") or it.get("season_number"))
            episode = as_int(it.get("episode") or it.get("episode_number"))
            if season is None or episode is None:
                unresolved.append(unresolved_item(it, "missing_season_episode"))
                continue
            url = f"{client.BASE}/tv/{int(show_id)}/season/{season}/episode/{episode}/rating"
            r = client.post(url, params=client._user_params(), json={"value": val})
        else:
            unresolved.append(unresolved_item(it, f"unsupported_type:{typ}"))
            continue

        if 200 <= r.status_code < 300:
            ok += 1
        else:
            unresolved.append(unresolved_item(it, f"http_{r.status_code}"))

    _info("add_done", applied=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    client = adapter.client
    unresolved: list[dict[str, Any]] = []
    ok = 0
    for it in items or []:
        if not isinstance(it, Mapping):
            continue

        typ = pick_media_type(it)
        if typ == "movie":
            tid = resolve_tmdb_id(adapter, it, want="movie")
            if tid is None:
                unresolved.append(unresolved_item(it, "missing_tmdb_id"))
                continue
            url = f"{client.BASE}/movie/{tid}/rating"
            r = client.delete(url, params=client._user_params())
        elif typ == "tv":
            tid = resolve_tmdb_id(adapter, it, want="tv")
            if tid is None:
                unresolved.append(unresolved_item(it, "missing_tmdb_id"))
                continue
            url = f"{client.BASE}/tv/{tid}/rating"
            r = client.delete(url, params=client._user_params())
        elif typ == "episode":
            show_ids = it.get("show_ids") if isinstance(it.get("show_ids"), Mapping) else None
            show_id = tmdb_id_from_item(show_ids or {}) if show_ids else None
            if show_id is None:
                show_id = resolve_tmdb_id(adapter, it, want="tv")
            if show_id is None:
                unresolved.append(unresolved_item(it, "missing_show_tmdb_id"))
                continue
            season = as_int(it.get("season") or it.get("season_number"))
            episode = as_int(it.get("episode") or it.get("episode_number"))
            if season is None or episode is None:
                unresolved.append(unresolved_item(it, "missing_season_episode"))
                continue
            url = f"{client.BASE}/tv/{int(show_id)}/season/{season}/episode/{episode}/rating"
            r = client.delete(url, params=client._user_params())
        else:
            unresolved.append(unresolved_item(it, f"unsupported_type:{typ}"))
            continue

        if 200 <= r.status_code < 300:
            ok += 1
        else:
            unresolved.append(unresolved_item(it, f"http_{r.status_code}"))

    _info("remove_done", applied=ok, unresolved=len(unresolved))
    return ok, unresolved
