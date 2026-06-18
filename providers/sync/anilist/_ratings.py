# /providers/sync/anilist/_ratings.py
# AniList ratings sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.anime_mapping import AnimeMappingService
from cw_platform.anime_mapping.service import (
    PAIR_FEATURE_OPTIONS_KEY,
    mapped_or_default_media_type,
    mapping_enabled_for_feature,
    runtime_pair_feature_options,
)
from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "ratings", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "ratings", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "ratings", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "ratings", "error", msg, **fields)


GQL_LIST_RATINGS = """
query ($userId: Int!, $type: MediaType!) {
  MediaListCollection(userId: $userId, type: $type) {
    lists {
      entries {
        id
        mediaId
        status
        score(format: POINT_10)
        updatedAt
        createdAt
        media {
          id
          idMal
          title { romaji english native }
          format
          seasonYear
          startDate { year }
        }
      }
    }
  }
}
""".strip()

GQL_MEDIA_BY_MAL = """
query ($idMal: Int!, $type: MediaType!) {
  Media(idMal: $idMal, type: $type) { id idMal }
}
""".strip()

GQL_SEARCH = """
query ($search: String!, $page: Int = 1) {
  Page(page: $page, perPage: 10) {
    media(search: $search, type: ANIME) {
      id
      idMal
      format
      seasonYear
      startDate { year }
      title { romaji english native }
    }
  }
}
""".strip()

GQL_SAVE_RATING = """
mutation ($mediaId: Int!, $scoreRaw: Int!) {
  SaveMediaListEntry(mediaId: $mediaId, scoreRaw: $scoreRaw) {
    id
    mediaId
    score(format: POINT_10)
  }
}
""".strip()


def _to_int(v: Any) -> int | None:
    try:
        if v is None or isinstance(v, bool):
            return None
        if isinstance(v, int):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _to_float(v: Any) -> float | None:
    try:
        if v is None or isinstance(v, bool):
            return None
        s = str(v).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _rating_1_10(v: Any) -> int | None:
    f = _to_float(v)
    if f is None or f <= 0:
        return None
    if 10 < f <= 100:
        f = f / 10.0
    n = int(f + 0.5)
    return n if 1 <= n <= 10 else None


def _score_raw(v: Any) -> int | None:
    rating = _rating_1_10(v)
    if rating is None:
        return None
    return max(0, min(100, int(rating) * 10))


def _pick_title(t: Mapping[str, Any] | None) -> str:
    if not isinstance(t, Mapping):
        return ""
    return str(t.get("english") or t.get("romaji") or t.get("native") or "").strip()


def _media_type(media: Mapping[str, Any]) -> str:
    fmt = str(media.get("format") or "").strip().upper()
    return "movie" if fmt == "MOVIE" else "show"


def _iso_from_epoch(v: Any) -> str | None:
    n = _to_int(v)
    if not n:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(n)))


def _unresolved_item(item: Mapping[str, Any], reason: str) -> dict[str, Any]:
    out = dict(id_minimal(item))
    out["reason"] = reason
    return out


def _mapping_enabled(adapter: Any) -> bool:
    try:
        cfg = getattr(adapter, "raw_cfg", None)
        if not isinstance(cfg, Mapping):
            return False
        if PAIR_FEATURE_OPTIONS_KEY in cfg:
            opts = runtime_pair_feature_options(cfg, "ratings")
            return bool(opts.get("use_anime_mapping", False))
        return mapping_enabled_for_feature(cfg, "ratings")
    except Exception:
        return False


def _mapping_ready(adapter: Any) -> bool:
    if not _mapping_enabled(adapter):
        return False
    try:
        svc = AnimeMappingService(getattr(adapter, "raw_cfg", None))
        ready = bool(svc.ready())
        if not ready and not getattr(adapter, "_cw_anime_ratings_mapping_unavailable_logged", False):
            setattr(adapter, "_cw_anime_ratings_mapping_unavailable_logged", True)
            _warn("anime_mapping_unavailable", fallback="title_resolve")
        return ready
    except Exception as e:
        if not getattr(adapter, "_cw_anime_ratings_mapping_unavailable_logged", False):
            setattr(adapter, "_cw_anime_ratings_mapping_unavailable_logged", True)
            _warn("anime_mapping_unavailable", fallback="title_resolve", error_type=e.__class__.__name__)
        return False


def _anime_only_sync(adapter: Any) -> bool:
    try:
        cfg = getattr(adapter, "raw_cfg", None)
        if not isinstance(cfg, Mapping) or PAIR_FEATURE_OPTIONS_KEY not in cfg:
            return False
        opts = runtime_pair_feature_options(cfg, "ratings")
        return bool(opts.get("anime_only_sync", False) and _mapping_ready(adapter))
    except Exception:
        return False


def _has_mapping_detail(item: Mapping[str, Any]) -> bool:
    detail = item.get("detail") if isinstance(item.get("detail"), Mapping) else {}
    amap = detail.get("anime_mapping") if isinstance(detail, Mapping) else {}
    return isinstance(amap, Mapping) and any(bool(v) for v in amap.values())


def _anime_enrich(adapter: Any, item: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(item or {})
    try:
        if not _mapping_ready(adapter):
            return out
        media_type = mapped_or_default_media_type(out)
        svc = AnimeMappingService(getattr(adapter, "raw_cfg", None))
        enriched = svc.enrich_ids(out.get("ids") if isinstance(out.get("ids"), Mapping) else {}, media_type=media_type)
        if not isinstance(enriched, Mapping):
            return out
        if enriched.get("ids"):
            ids0 = out.get("ids") if isinstance(out.get("ids"), Mapping) else {}
            out["ids"] = enriched["ids"]
            if dict(out.get("ids") or {}) != dict(ids0 or {}):
                out["_cw_anime_mapping"] = True
                _dbg("mapping_enriched", ids_before=len(ids0 or {}), ids_after=len(out.get("ids") or {}), title=str(out.get("title") or ""))
        detail_raw = out.get("detail")
        detail: dict[str, Any] = dict(detail_raw) if isinstance(detail_raw, Mapping) else {}
        enriched_detail = enriched.get("detail")
        if isinstance(enriched_detail, Mapping):
            for k, v in enriched_detail.items():
                detail[str(k)] = v
        if detail:
            out["detail"] = detail
    except Exception as e:
        _dbg("mapping_enrich_failed", error_type=e.__class__.__name__)
    return out


def _tick(prog: Any, value: int, total: int | None = None, *, force: bool = False) -> None:
    if prog is None:
        return
    try:
        if total is not None:
            prog.tick(value, total=total, force=force)
        else:
            prog.tick(value)
    except Exception:
        pass


def _done(prog: Any, *, ok: bool, total: int) -> None:
    if prog is None:
        return
    done = getattr(prog, "done", None)
    if not callable(done):
        return
    try:
        done(ok=ok, total=total)
    except Exception:
        pass


def _item_from_entry(adapter: Any, entry: Mapping[str, Any]) -> dict[str, Any] | None:
    media = entry.get("media")
    if not isinstance(media, Mapping):
        return None
    mid = _to_int(media.get("id") or entry.get("mediaId"))
    if not mid:
        return None
    score = _rating_1_10(entry.get("score"))
    if score is None:
        return None
    title = _pick_title(media.get("title") if isinstance(media.get("title"), Mapping) else None)
    if not title:
        return None
    year = _to_int(media.get("seasonYear"))
    if year is None:
        sd = media.get("startDate")
        if isinstance(sd, Mapping):
            year = _to_int(sd.get("year"))
    ids: dict[str, Any] = {"anilist": int(mid)}
    mal = _to_int(media.get("idMal"))
    if mal:
        ids["mal"] = int(mal)
    item: dict[str, Any] = {
        "type": _media_type(media),
        "title": title,
        "year": int(year or 0),
        "ids": ids,
        "rating": int(score),
        "anilist": {
            "list_entry_id": int(_to_int(entry.get("id")) or 0),
            "status": str(entry.get("status") or ""),
        },
    }
    rated_at = _iso_from_epoch(entry.get("updatedAt") or entry.get("createdAt"))
    if rated_at:
        item["rated_at"] = rated_at
    return _anime_enrich(adapter, item)


def _resolve_media_id(adapter: Any, item: Mapping[str, Any]) -> tuple[int | None, dict[str, Any]]:
    ids_raw = item.get("ids")
    ids = ids_raw if isinstance(ids_raw, Mapping) else {}
    mid = _to_int(ids.get("anilist"))
    if mid:
        return int(mid), {"anilist_id": int(mid), "method": "direct_id"}

    mal = _to_int(ids.get("mal"))
    if mal:
        try:
            data = adapter.client.gql(
                GQL_MEDIA_BY_MAL,
                {"idMal": int(mal), "type": "ANIME"},
                feature="ratings:resolve",
                tolerate_errors=True,
            )
            media = (data or {}).get("Media")
            aid = _to_int(media.get("id")) if isinstance(media, Mapping) else None
            if aid:
                return int(aid), {"anilist_id": int(aid), "mal": int(mal), "method": "mal_lookup"}
        except Exception as e:
            _dbg("resolve_miss", method="mal_lookup", error_type=e.__class__.__name__, mal=int(mal))

    if _anime_only_sync(adapter):
        return None, {"anime_only_unmapped": True}

    title = str(item.get("title") or "").strip()
    if not title:
        return None, {"missing_title": True}

    year = _to_int(item.get("year"))
    want_kind = str(item.get("type") or "").strip().lower()
    try:
        data = adapter.client.gql(
            GQL_SEARCH,
            {"search": title, "page": 1},
            feature="ratings:search",
            tolerate_errors=True,
        )
    except Exception as e:
        _dbg("resolve_miss", method="search", error_type=e.__class__.__name__, title=title)
        return None, {"search_failed": True}

    page = (data or {}).get("Page")
    rows = page.get("media") if isinstance(page, Mapping) else None
    if not isinstance(rows, list):
        return None, {"no_results": True}

    best_id: int | None = None
    best_score = -999
    for cand in rows:
        if not isinstance(cand, Mapping):
            continue
        cid = _to_int(cand.get("id"))
        if not cid:
            continue
        ctitle = _pick_title(cand.get("title") if isinstance(cand.get("title"), Mapping) else None)
        if not ctitle:
            continue
        score = 0
        if ctitle.casefold() == title.casefold():
            score += 60
        elif title.casefold() in ctitle.casefold() or ctitle.casefold() in title.casefold():
            score += 25
        c_year = _to_int(cand.get("seasonYear"))
        if c_year is None:
            sd = cand.get("startDate")
            if isinstance(sd, Mapping):
                c_year = _to_int(sd.get("year"))
        if year and c_year:
            score += 15 if int(year) == int(c_year) else -15
        fmt = str(cand.get("format") or "").strip().upper()
        if want_kind == "movie":
            score += 8 if fmt == "MOVIE" else -10
        elif want_kind in ("show", "series", "tv", "anime"):
            score += 5 if fmt != "MOVIE" else -4
        if score > best_score:
            best_score = score
            best_id = int(cid)

    if best_id and best_score >= 35:
        _dbg("resolve_hit", anilist_id=int(best_id), score=int(best_score), title=title, year=year)
        return int(best_id), {"anilist_id": int(best_id), "method": "search"}

    _dbg("resolve_miss", reason="low_score" if best_id else "no_results", best_score=int(best_score), title=title, year=year)
    return None, {"no_match": True}


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("ratings") if callable(prog_mk) else None
    viewer = adapter.client.viewer()
    user_id = viewer.get("id") if isinstance(viewer, dict) else None
    if not user_id:
        return {}

    t0 = time.time()
    data = adapter.client.gql(
        GQL_LIST_RATINGS,
        {"userId": int(user_id), "type": "ANIME"},
        feature="ratings:index",
    )
    _dbg("index_fetch_counts", source="live", dur_ms=int((time.time() - t0) * 1000))

    mlc = (data or {}).get("MediaListCollection")
    lists = mlc.get("lists") if isinstance(mlc, Mapping) else None
    if not isinstance(lists, list):
        return {}

    out: dict[str, dict[str, Any]] = {}
    done = 0
    for lst in lists:
        if not isinstance(lst, Mapping):
            continue
        entries = lst.get("entries")
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            item = _item_from_entry(adapter, entry)
            if not item:
                continue
            mini = id_minimal(item)
            key = canonical_key(mini)
            if not key:
                continue
            out[key] = mini
            done += 1
            _tick(prog, done)

    _done(prog, ok=True, total=len(out))
    _info("index_done", count=len(out), source="live")
    return out


def add_detailed(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lst = list(items or [])
    if not lst:
        return {"ok": True, "count": 0, "confirmed": 0, "confirmed_keys": [], "skipped_keys": [], "unresolved": []}

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("ratings", total=len(lst)) if callable(prog_mk) else None

    ok = 0
    unresolved: list[dict[str, Any]] = []
    confirmed_keys: list[str] = []
    skipped_keys: list[str] = []
    seen_ok: set[str] = set()
    seen_skip: set[str] = set()
    skipped = 0
    try:
        opts = runtime_pair_feature_options(getattr(adapter, "raw_cfg", {}) if isinstance(getattr(adapter, "raw_cfg", {}), Mapping) else {}, "ratings")
        _dbg(
            "runtime_options",
            use_anime_mapping=bool(opts.get("use_anime_mapping", False)),
            anime_only_sync=bool(opts.get("anime_only_sync", False)),
        )
    except Exception:
        pass

    for i, it in enumerate(lst, start=1):
        enriched = _anime_enrich(adapter, it)
        mapped_item = bool(enriched.get("_cw_anime_mapping") or _has_mapping_detail(enriched))
        mini = id_minimal(enriched)
        if mapped_item:
            mini["_cw_anime_mapping"] = True
        src_key = adapter.key_of(mini) or ""
        score_raw = _score_raw(mini.get("rating"))
        if score_raw is None:
            unresolved.append(_unresolved_item(mini, "missing_or_invalid_rating"))
            _tick(prog, i, total=len(lst))
            continue

        mid, meta = _resolve_media_id(adapter, mini)
        if not mid:
            reason = "anime_only_unmapped" if meta.get("anime_only_unmapped") else "not_anime_or_no_match"
            _dbg("write_item_skipped", op="add", title=str(mini.get("title") or ""), year=_to_int(mini.get("year")), reason=reason)
            if meta.get("anime_only_unmapped"):
                if src_key and src_key not in seen_skip:
                    skipped_keys.append(src_key)
                    seen_skip.add(src_key)
                skipped += 1
            else:
                unresolved.append(_unresolved_item(mini, reason))
            _tick(prog, i, total=len(lst))
            continue

        try:
            adapter.client.gql(
                GQL_SAVE_RATING,
                {"mediaId": int(mid), "scoreRaw": int(score_raw)},
                feature="ratings:add",
            )
            ok += 1
            if src_key and src_key not in seen_ok:
                confirmed_keys.append(src_key)
                seen_ok.add(src_key)
            _dbg("write_item_done", op="add", anilist_id=int(mid), rating=int(score_raw / 10), source=str(meta.get("method") or ""))
        except Exception as e:
            _warn("write_failed", op="add", error_type=e.__class__.__name__, anilist_id=int(mid))
            unresolved.append(_unresolved_item(mini, f"write_failed:{e.__class__.__name__}"))
        _tick(prog, i, total=len(lst))

    _done(prog, ok=len(unresolved) == 0, total=len(lst))
    _info("write_done", op="add", ok=len(unresolved) == 0, applied=ok, skipped=skipped, unresolved=len(unresolved))
    return {
        "ok": True,
        "count": int(ok),
        "confirmed": int(ok),
        "confirmed_keys": confirmed_keys,
        "skipped": int(skipped),
        "skipped_keys": skipped_keys,
        "unresolved": unresolved,
    }


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    res = add_detailed(adapter, items)
    count = int((res or {}).get("confirmed", (res or {}).get("count", 0)) or 0)
    unresolved = (res or {}).get("unresolved") or []
    return count, list(unresolved) if isinstance(unresolved, list) else []


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    lst = list(items or [])
    if not lst:
        return 0, []

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("ratings", total=len(lst)) if callable(prog_mk) else None

    ok = 0
    unresolved: list[dict[str, Any]] = []
    for i, it in enumerate(lst, start=1):
        enriched = _anime_enrich(adapter, it)
        mini = id_minimal(enriched)
        mid, meta = _resolve_media_id(adapter, mini)
        if not mid:
            if meta.get("anime_only_unmapped"):
                _dbg("write_item_skipped", op="remove", title=str(mini.get("title") or ""), reason="anime_only_unmapped")
            else:
                unresolved.append(_unresolved_item(mini, "not_anime_or_no_match"))
            _tick(prog, i, total=len(lst))
            continue
        try:
            adapter.client.gql(
                GQL_SAVE_RATING,
                {"mediaId": int(mid), "scoreRaw": 0},
                feature="ratings:remove",
            )
            ok += 1
            _dbg("write_item_done", op="remove", anilist_id=int(mid))
        except Exception as e:
            _warn("write_failed", op="remove", error_type=e.__class__.__name__, anilist_id=int(mid))
            unresolved.append(_unresolved_item(mini, f"write_failed:{e.__class__.__name__}"))
        _tick(prog, i, total=len(lst))

    _done(prog, ok=len(unresolved) == 0, total=len(lst))
    _info("write_done", op="remove", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
    return ok, unresolved
