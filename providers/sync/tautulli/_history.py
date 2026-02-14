# providers/sync/tautulli/_history.py
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
import re
from typing import Any

from cw_platform.id_map import canonical_key, ids_from_guid

from ._common import make_logger


_EXT_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl")
_log = make_logger("history")


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


def _to_int_total(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    if not s:
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    return int(digits) if digits else None


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


def _clean_guid(s: str) -> str:
    x = (s or "").strip()
    if not x:
        return ""
    return x.split("?", 1)[0].split("#", 1)[0].strip()


def _ids_from_plex_guid(s: str) -> dict[str, str]:
    g = _clean_guid(s)
    if not g:
        return {}
    out: dict[str, str] = {}
    m = re.search(r"(?:com\.plexapp\.agents\.)?imdb://(tt\d+)", g, re.IGNORECASE)
    if m:
        out["imdb"] = m.group(1)
    m = re.search(r"(?:com\.plexapp\.agents\.)?(?:themoviedb|tmdb)://(\d+)", g, re.IGNORECASE)
    if m:
        out["tmdb"] = m.group(1)
    m = re.search(r"(?:com\.plexapp\.agents\.)?thetvdb://(\d+)", g, re.IGNORECASE)
    if m:
        out["tvdb"] = m.group(1)
    return out


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


def _has_any_ids(ids: Mapping[str, str], show_ids: Mapping[str, str]) -> bool:
    if any(ids.get(k) for k in _EXT_ID_KEYS):
        return True
    if any(show_ids.get(k) for k in _EXT_ID_KEYS):
        return True
    return bool(ids.get("plex") or show_ids.get("plex"))


def _has_ext_ids(ids: Mapping[str, str]) -> bool:
    return any(ids.get(k) for k in _EXT_ID_KEYS)


def _merge_ids(dst: dict[str, str], src: Mapping[str, str]) -> None:
    for k, v in (src or {}).items():
        if v and not dst.get(k):
            dst[k] = str(v)


def _first_text(*vals: Any) -> str | None:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("none", "null"):
            return s
    return None


def _series_title(row: Mapping[str, Any], meta: Mapping[str, Any] | None, show_meta: Mapping[str, Any] | None) -> str | None:
    m = meta if isinstance(meta, Mapping) else {}
    sm = show_meta if isinstance(show_meta, Mapping) else {}
    return _first_text(
        row.get("grandparent_title"),
        row.get("grandparentTitle"),
        row.get("grandparent_name"),
        row.get("grandparentName"),
        m.get("grandparent_title"),
        m.get("grandparentTitle"),
        m.get("grandparent_name"),
        m.get("grandparentName"),
        sm.get("title"),
        sm.get("original_title"),
        sm.get("grandparent_title"),
        sm.get("grandparentTitle"),
    )


def _row_ids(row: Mapping[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    ids: dict[str, str] = {}
    show_ids: dict[str, str] = {}

    rk = row.get("rating_key") or row.get("ratingKey")
    if rk:
        ids["plex"] = str(rk)

    gp_rk = row.get("grandparent_rating_key") or row.get("grandparentRatingKey")
    if gp_rk:
        show_ids["plex"] = str(gp_rk)

    for g in _collect_guids(row):
        gg = _clean_guid(g)
        if gg.lower().startswith("plex://") and "guid" not in ids:
            ids["guid"] = gg
        try:
            ids.update(ids_from_guid(gg))
        except Exception:
            pass
        ids.update(_ids_from_plex_guid(gg))

    for g in _collect_guids({k: row.get(k) for k in ("grandparent_guid", "grandparent_guids") if row.get(k)}):
        gg = _clean_guid(g)
        if gg.lower().startswith("plex://") and "guid" not in show_ids:
            show_ids["guid"] = gg
        try:
            show_ids.update(ids_from_guid(gg))
        except Exception:
            pass
        show_ids.update(_ids_from_plex_guid(gg))

    for k in _EXT_ID_KEYS:
        if k in ids and k not in show_ids:
            show_ids[k] = ids[k]

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

    _log("index start", per_page=cfg_per_page, max_pages=cfg_max_pages, has_user_id=bool(user_id))

    out: dict[str, dict[str, Any]] = {}
    meta_cache: dict[str, Mapping[str, Any] | None] = {}
    start = 0
    pages = 0
    prev_sig = ""
    repeats = 0

    rows_seen = 0
    rows_kept = 0
    skipped_type = 0
    skipped_no_time = 0
    skipped_no_ids = 0

    def _page_sig(rows: list[dict[str, Any]]) -> str:
        if not rows:
            return ""
        first = rows[0] if isinstance(rows[0], Mapping) else {}
        last = rows[-1] if isinstance(rows[-1], Mapping) else {}
        fk = first.get("row_id") or first.get("reference_id") or first.get("rating_key") or ""
        lk = last.get("row_id") or last.get("reference_id") or last.get("rating_key") or ""
        ft = first.get("date") or ""
        lt = last.get("date") or ""
        return f"{fk}:{ft}|{lk}:{lt}|{len(rows)}"

    def _get_meta(rating_key: str) -> Mapping[str, Any] | None:
        rk = str(rating_key or "").strip()
        if not rk:
            return None
        if rk in meta_cache:
            return meta_cache[rk]
        try:
            meta = client.call("get_metadata", rating_key=rk) or None
        except Exception as e:
            _log("metadata fetch failed", level="debug", rating_key=rk, error=str(e))
            meta = None
        meta_cache[rk] = meta if isinstance(meta, Mapping) else None
        return meta_cache[rk]

    while True:
        pages += 1
        if pages > cfg_max_pages:
            _log("max pages reached", level="warn", pages=pages, max_pages=cfg_max_pages)
            break

        params: dict[str, Any] = {"start": start, "length": cfg_per_page, "order_column": "date", "order_dir": "desc"}
        if user_id:
            params["user_id"] = user_id

        payload = client.call("get_history", **params) or {}
        rows: list[dict[str, Any]] = []
        total: Any = None

        if isinstance(payload, Mapping):
            if isinstance(payload.get("data"), list):
                rows = list(payload.get("data") or [])
                total = payload.get("recordsFiltered") or payload.get("recordsTotal")
            elif isinstance(payload.get("data"), Mapping):
                blk = payload.get("data") or {}
                if isinstance(blk.get("data"), list):
                    rows = list(blk.get("data") or [])
                total = blk.get("recordsFiltered") or blk.get("recordsTotal")

        if not rows:
            break

        sig = _page_sig(rows)
        if sig and sig == prev_sig:
            repeats += 1
            if repeats >= 2:
                _log("page repeat", level="warn", pages=pages, start=start, sig=sig)
                break
        else:
            repeats = 0
            prev_sig = sig

        for row in rows:
            if not isinstance(row, Mapping):
                continue
            rows_seen += 1

            mtype = (row.get("media_type") or row.get("mediaType") or "").lower()
            if mtype not in ("movie", "episode"):
                skipped_type += 1
                continue

            ts = _as_epoch(row.get("date") or row.get("started") or row.get("time"))
            watched_at = _iso_z(ts)
            if not watched_at:
                skipped_no_time += 1
                continue

            ids, show_ids = _row_ids(row)
            ep_meta: Mapping[str, Any] | None = None
            show_meta: Mapping[str, Any] | None = None
            if not _has_any_ids(ids, show_ids):
                skipped_no_ids += 1
                continue

            if ids.get("plex") and (not _has_ext_ids(ids) or (mtype == "episode" and not _has_ext_ids(show_ids))):
                meta = _get_meta(ids["plex"])
                if isinstance(meta, Mapping):
                    ep_meta = meta
                    m_ids, m_show_ids = _row_ids(meta)
                    _merge_ids(ids, m_ids)
                    _merge_ids(show_ids, m_show_ids)

            if mtype == "episode" and show_ids.get("plex") and not _has_ext_ids(show_ids):
                show_meta = _get_meta(show_ids["plex"])
                if isinstance(show_meta, Mapping):
                    sm_ids, sm_show_ids = _row_ids(show_meta)
                    _merge_ids(show_ids, sm_ids)
                    _merge_ids(show_ids, sm_show_ids)

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
                ids_out = show_ids if _has_ext_ids(show_ids) else ids
                item = {
                    "type": "episode",
                    "ids": ids_out,
                    "title": row.get("title") or row.get("full_title"),
                    "year": row.get("year"),
                    "season": season_i,
                    "episode": episode_i,
                    "watched_at": watched_at,
                }
                st = _series_title(row, ep_meta, show_meta)
                if st:
                    item["series_title"] = st
                if _has_ext_ids(show_ids):
                    item["show_ids"] = show_ids

            ck = canonical_key(item)
            if ck and ck not in out:
                out[ck] = item
                rows_kept += 1

        start += cfg_per_page
        total_i = _to_int_total(total)
        if total_i is not None and start >= total_i:
            break
        if len(rows) < cfg_per_page:
            break

    _log(
        "index done",
        count=len(out),
        pages=pages,
        rows_seen=rows_seen,
        rows_kept=rows_kept,
        skipped_type=skipped_type,
        skipped_no_time=skipped_no_time,
        skipped_no_ids=skipped_no_ids,
        meta_cache=len(meta_cache),
    )
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    _ = (adapter, items, dry_run)
    _log("write not supported", level="warn", op="add")
    return {"ok": False, "error": "Tautulli is read-only (history can only be sourced).", "count": 0}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    _ = (adapter, items, dry_run)
    _log("write not supported", level="warn", op="remove")
    return {"ok": False, "error": "Tautulli is read-only (history can only be sourced).", "count": 0}
