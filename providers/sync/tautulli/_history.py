# providers/sync/tautulli/_history.py
# CrossWatch - Tautulli history sync (read-only)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from cw_platform.id_map import canonical_key, ids_from_guid


def _cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    cfg = getattr(adapter, "cfg", None) or {}
    cur: Any = cfg
    for part in str(key).split("."):
        if not isinstance(cur, Mapping):
            return default
        cur = cur.get(part)
        if cur is None:
            return default
    return cur


def _to_int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v.is_integer() else None
    s = str(v).strip()
    if not s or not s.isdigit():
        return None
    return int(s)


def _as_epoch(v: Any) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, (int, float)):
            x = int(v)
            if x > 10_000_000_000:
                x = int(x / 1000)
            return x if x > 0 else None
        s = str(v).strip()
        if not s:
            return None
        if s.isdigit():
            x = int(s)
            if x > 10_000_000_000:
                x = int(x / 1000)
            return x if x > 0 else None
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None


def _iso_z(epoch: int | None) -> str | None:
    if not epoch:
        return None
    try:
        return datetime.fromtimestamp(int(epoch), tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _collect_guids(obj: Any) -> list[str]:
    out: list[str] = []

    def _add(v: Any) -> None:
        if not v:
            return
        if isinstance(v, str):
            s = v.strip()
            if s and s.lower() not in ("none", "null"):
                out.append(s)
        elif isinstance(v, Mapping):
            _add(v.get("id") or v.get("guid") or v.get("url"))
        elif isinstance(v, (list, tuple)):
            for it in v:
                _add(it)

    if isinstance(obj, Mapping):
        for k, v in obj.items():
            if "guid" in str(k).lower():
                _add(v)
    return out


def _row_ids(adapter: Any, row: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    ids: dict[str, str] = {}
    show_ids: dict[str, str] = {}

    rk = row.get("rating_key") or row.get("ratingKey")
    if rk:
        ids["plex"] = str(rk)

    gp_rk = row.get("grandparent_rating_key") or row.get("grandparentRatingKey")
    if gp_rk:
        show_ids["plex"] = str(gp_rk)

    for g in _collect_guids(row):
        ids.update(ids_from_guid(g))

    need_meta = not any(k in ids for k in ("imdb", "tmdb", "tvdb", "trakt", "simkl"))
    meta_fallback = bool(_cfg_get(adapter, "tautulli.history.metadata_fallback", True))

    if need_meta and meta_fallback and rk and hasattr(getattr(adapter, "client", None), "call"):
        try:
            meta = adapter.client.call("get_metadata", rating_key=str(rk)) or {}
            for g in _collect_guids(meta):
                ids.update(ids_from_guid(g))
        except Exception:
            pass

    for g in _collect_guids({k: row.get(k) for k in ("grandparent_guid", "grandparent_guids") if row.get(k)}):
        show_ids.update(ids_from_guid(g))

    return {k: v for k, v in ids.items() if v}, {k: v for k, v in show_ids.items() if v}


def build_index(adapter: Any, *, per_page: int = 100, max_pages: int = 5000) -> dict[str, dict[str, Any]]:
    client = getattr(adapter, "client", None)
    if not client:
        return {}

    user_id = str(_cfg_get(adapter, "tautulli.history.user_id", "") or "").strip()
    cfg_per_page = max(1, min(500, int(_cfg_get(adapter, "tautulli.history.per_page", per_page) or per_page)))
    cfg_max_pages = int(_cfg_get(adapter, "tautulli.history.max_pages", max_pages) or max_pages)
    if cfg_max_pages <= 0:
        cfg_max_pages = max_pages

    out: dict[str, dict[str, Any]] = {}
    start = 0
    pages = 0

    while True:
        pages += 1
        if pages > cfg_max_pages:
            break

        params: dict[str, Any] = {"start": start, "length": cfg_per_page, "order_column": "date", "order_dir": "desc"}
        if user_id:
            params["user_id"] = user_id

        payload = client.call("get_history", **params) or {}
        rows: list[dict[str, Any]] = []
        total: int | None = None

        if isinstance(payload, Mapping):
            if isinstance(payload.get("data"), list):
                rows = list(payload.get("data") or [])
            elif isinstance(payload.get("data"), Mapping):
                blk = payload.get("data") or {}
                if isinstance(blk.get("data"), list):
                    rows = list(blk.get("data") or [])
                total = blk.get("recordsFiltered") or blk.get("recordsTotal")
            total = total or payload.get("recordsFiltered") or payload.get("recordsTotal")

        if not rows:
            break

        for row in rows:
            if not isinstance(row, Mapping):
                continue

            mtype = (row.get("media_type") or row.get("mediaType") or "").lower()
            if mtype not in ("movie", "episode"):
                continue

            ts = _as_epoch(row.get("date") or row.get("started") or row.get("time"))
            watched_at = _iso_z(ts)
            if not watched_at:
                continue

            ids, show_ids = _row_ids(adapter, row)

            if mtype == "movie":
                item: dict[str, Any] = {
                    "type": "movie",
                    "title": row.get("title") or row.get("full_title"),
                    "year": row.get("year"),
                    "ids": ids,
                    "watched_at": watched_at,
                }
            else:
                season_i = _to_int(row.get("parent_media_index") or row.get("season"))
                episode_i = _to_int(row.get("media_index") or row.get("episode"))
                if season_i is None or episode_i is None:
                    continue

                item = {
                    "type": "episode",
                    "ids": ids,
                    "show_ids": show_ids,
                    "series_title": row.get("grandparent_title") or row.get("grandparentTitle"),
                    "title": row.get("title") or row.get("full_title"),
                    "season": season_i,
                    "episode": episode_i,
                    "watched_at": watched_at,
                }

            ck = canonical_key(item)
            if ck and ck not in out:
                out[ck] = item

        start += cfg_per_page
        if total is not None:
            try:
                if start >= int(total):
                    break
            except Exception:
                pass
        if len(rows) < cfg_per_page:
            break

    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    _ = (adapter, items, dry_run)
    return {"ok": False, "error": "Tautulli is read-only (history can only be sourced).", "count": 0}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    _ = (adapter, items, dry_run)
    return {"ok": False, "error": "Tautulli is read-only (history can only be sourced).", "count": 0}
