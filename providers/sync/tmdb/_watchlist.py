# /providers/sync/_mod_watchlist.py
# CrossWatch - TMDb watchlist adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import minimal as id_minimal

from .._log import log as cw_log

from ._common import (
    as_int,
    fetch_external_ids,
    key_of,
    pick_watchlist_media_type,
    resolve_tmdb_id,
    unresolved_item,
    year_from_date,
)


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("TMDB", "watchlist", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("TMDB", "watchlist", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("TMDB", "watchlist", "warn", msg, **fields)


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    client = adapter.client
    account_id = client.account_id()
    prog = adapter.progress_factory("watchlist")
    out: dict[str, dict[str, Any]] = {}

    def pull(kind: str) -> None:
        page = 1
        total_pages = 1
        while page <= total_pages:
            r = client.get(
                f"{client.BASE}/account/{account_id}/watchlist/{kind}",
                params=client._user_params({"page": page, "sort_by": "created_at.asc"}),
            )
            if not (200 <= r.status_code < 300):
                _warn("fetch_failed", kind=kind, page=page, status=r.status_code)
                raise RuntimeError(f"TMDb watchlist fetch failed ({kind}) ({r.status_code})")
            data = r.json() if (r.text or "").strip() else {}
            if not isinstance(data, Mapping):
                data = {}
            total_pages = as_int(data.get("total_pages")) or total_pages
            results_any = data.get("results")
            results = results_any if isinstance(results_any, list) else []
            _dbg("page", kind=kind, page=page, total_pages=total_pages, batch=len(results))

            for raw in results:
                if not isinstance(raw, Mapping):
                    continue
                tmdb_id = as_int(raw.get("id"))
                if tmdb_id is None:
                    continue
                if kind == "movies":
                    title = raw.get("title") or raw.get("original_title")
                    year = year_from_date(raw.get("release_date"))
                    ids = fetch_external_ids(adapter, kind="movie", tmdb_id=int(tmdb_id))
                    ids.setdefault("tmdb", int(tmdb_id))
                    item = {"type": "movie", "title": title, "year": year, "ids": ids}
                else:
                    title = raw.get("name") or raw.get("original_name")
                    year = year_from_date(raw.get("first_air_date"))
                    ids = fetch_external_ids(adapter, kind="tv", tmdb_id=int(tmdb_id))
                    ids.setdefault("tmdb", int(tmdb_id))
                    item = {"type": "show", "title": title, "year": year, "ids": ids}

                mini = id_minimal(item)
                out[key_of(mini)] = mini

            prog.tick(len(out), total=None)
            page += 1

    _info("build_index", account_id=account_id)
    pull("movies")
    pull("tv")
    prog.done(ok=True)
    _info("build_index_done", count=len(out))
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    client = adapter.client
    account_id = client.account_id()
    unresolved: list[dict[str, Any]] = []
    ok = 0
    for it in items or []:
        if not isinstance(it, Mapping):
            continue
        media_type = pick_watchlist_media_type(it)
        want = "tv" if media_type == "tv" else "movie"
        tid = resolve_tmdb_id(adapter, it, want=want)
        if tid is None:
            unresolved.append(unresolved_item(it, "missing_tmdb_id"))
            continue
        body = {"media_type": media_type, "media_id": int(tid), "watchlist": True}
        r = client.post(
            f"{client.BASE}/account/{account_id}/watchlist",
            params=client._user_params(),
            json=body,
        )
        if 200 <= r.status_code < 300:
            ok += 1
        else:
            unresolved.append(unresolved_item(it, f"http_{r.status_code}"))
    _info("add_done", applied=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    client = adapter.client
    account_id = client.account_id()
    unresolved: list[dict[str, Any]] = []
    ok = 0
    for it in items or []:
        if not isinstance(it, Mapping):
            continue
        media_type = pick_watchlist_media_type(it)
        want = "tv" if media_type == "tv" else "movie"
        tid = resolve_tmdb_id(adapter, it, want=want)
        if tid is None:
            unresolved.append(unresolved_item(it, "missing_tmdb_id"))
            continue
        body = {"media_type": media_type, "media_id": int(tid), "watchlist": False}
        r = client.post(
            f"{client.BASE}/account/{account_id}/watchlist",
            params=client._user_params(),
            json=body,
        )
        if 200 <= r.status_code < 300:
            ok += 1
        else:
            unresolved.append(unresolved_item(it, f"http_{r.status_code}"))
    _info("remove_done", applied=ok, unresolved=len(unresolved))
    return ok, unresolved
