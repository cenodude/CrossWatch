# services/export.py
# CrossWatch - Export scrobbled data to various formats
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import csv
import io
import re
import time
from collections.abc import Mapping
from datetime import datetime
from functools import lru_cache
from typing import Any, Callable, Iterable, cast

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from cw_platform.access_policy import managed_profile_instances, request_user, user_can_access_instance
from cw_platform.config_base import CONFIG as CONFIG_DIR, load_config
from cw_platform.history_events import base_key_from_history_event, history_epoch_from_key, is_history_event_key
from cw_platform.modules_registry import load_sync_ops
from cw_platform.provider_instances import sanitize_instance_label

router = APIRouter(prefix="/api", tags=["export"])


_EXPORT_STATE_FEATURES = {"watchlist", "history", "ratings"}


def _state_features_for_export(feature: str | None = None) -> set[str]:
    feat = str(feature or "").strip().lower()
    if feat == "combined":
        return {"history", "ratings"}
    if feat in _EXPORT_STATE_FEATURES:
        return {feat}
    return set(_EXPORT_STATE_FEATURES)


def _load_state(features: set[str] | None = None) -> dict[str, Any]:
    try:
        from cw_platform.orchestrator._state_store import StateStore

        state = StateStore(CONFIG_DIR).load_state_features(features or _EXPORT_STATE_FEATURES)
    except Exception:
        state = {}
    return state if isinstance(state, dict) else {"providers": {}}


def _providers_in_state(s: dict[str, Any]) -> list[str]:
    return sorted((s.get("providers") or {}).keys())


def _load_config_safe() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}


def _allowed_provider_map(cfg: Mapping[str, Any], request: Request | None) -> dict[str, list[str]] | None:
    user = request_user(request)
    if not user or bool(user.get("is_admin")):
        return None
    return managed_profile_instances(cfg, user)


def _export_instance_for_request(cfg: Mapping[str, Any], request: Request | None, provider: str, provider_instance: str) -> str:
    inst = str(provider_instance or "all").strip() or "all"
    user = request_user(request)
    if not user or bool(user.get("is_admin")):
        return inst
    allowed = managed_profile_instances(cfg, user).get(str(provider or "").strip().upper()) or []
    if inst.lower() in {"all", "any", "*"}:
        if allowed:
            return allowed[0]
        raise HTTPException(403, "profile_scope_denied")
    if not user_can_access_instance(cfg, user, provider, inst):
        raise HTTPException(403, "profile_scope_denied")
    return inst


def _cfg_block_for_provider(cfg: dict[str, Any], provider: str) -> dict[str, Any]:
    p = str(provider or "").strip().lower()
    keys = [p]
    if p == "tmdb":
        keys.insert(0, "tmdb_sync")
    if p in {"cw", "crosswatch"}:
        keys.insert(0, "crosswatch")
    for key in keys:
        blk = cfg.get(key)
        if isinstance(blk, dict):
            return blk
    return {}


def _configured_instance_label(cfg: dict[str, Any], provider: str, instance_id: str, fallback: str) -> str:
    inst = str(instance_id or "").strip()
    blk = _cfg_block_for_provider(cfg, provider)
    if not inst or inst.lower() == _DEFAULT_INSTANCE:
        ib = blk
    else:
        insts = blk.get("instances")
        ib = insts.get(inst) if isinstance(insts, dict) else None
    label = sanitize_instance_label((ib or {}).get("label") if isinstance(ib, dict) else "")
    if label:
        return label if not inst or inst.lower() == _DEFAULT_INSTANCE else f"{fallback} - {label}"
    return "Default" if not inst or inst.lower() == _DEFAULT_INSTANCE else fallback


_DEFAULT_INSTANCE = "default"
_MEDIA_TYPES = ("movie", "show", "season", "episode")
_MEDIA_TYPE_ALIASES = {
    "movies": "movie",
    "film": "movie",
    "films": "movie",
    "tv": "show",
    "series": "show",
    "shows": "show",
    "seasons": "season",
    "episodes": "episode",
}
_DEFAULT_MEDIA_TYPES = ("movie",)
_LETTERBOXD_MAX_BYTES = 1_000_000

def _prov_block(s: dict[str, Any], provider: str) -> dict[str, Any]:
    p = (s.get("providers") or {}).get(provider)
    return p if isinstance(p, dict) else {}

def _iter_provider_instance_blocks(s: dict[str, Any], provider: str) -> Iterable[tuple[str, dict[str, Any]]]:
    p = _prov_block(s, provider)
    if not p:
        return
    yield _DEFAULT_INSTANCE, p
    insts = p.get("instances")
    if isinstance(insts, dict):
        for inst_id, blk in insts.items():
            if isinstance(blk, dict):
                yield str(inst_id), blk

def _feature_items(block: dict[str, Any], feature: str) -> dict[str, Any]:
    try:
        b = block[feature]["baseline"]["items"]  # type: ignore[index]
    except Exception:
        return {}
    return b if isinstance(b, dict) else {}

def _item_score(it: dict[str, Any]) -> int:
    ids = _norm_ids(it.get("ids") or {})
    return sum(1 for v in ids.values() if v)

def _merge_best(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    if not a:
        return dict(b or {})
    if not b:
        return dict(a or {})
    sa, sb = _item_score(a), _item_score(b)
    best = a if sa >= sb else b
    other = b if best is a else a
    out = dict(best)
    for k, v in (other or {}).items():
        if k not in out or out[k] in (None, "", [], {}):
            out[k] = v
    if isinstance(best.get("ids"), dict) or isinstance(other.get("ids"), dict):
        ids = dict(other.get("ids") or {})
        ids.update(dict(best.get("ids") or {}))
        out["ids"] = ids
    return out

def _items_bucket(s: dict[str, Any], provider: str, feature: str, instance_id: str | None = None) -> dict[str, Any]:
    inst = str(instance_id or "").strip()
    inst_lc = inst.lower()
    if not inst or inst_lc in {"all", "any", "*"}:
        merged: dict[str, Any] = {}
        for _iid, blk in _iter_provider_instance_blocks(s, provider):
            items = _feature_items(blk, feature)
            if not items:
                continue
            for k, it in items.items():
                prev = merged.get(str(k))
                merged[str(k)] = _merge_best(prev or {}, (it or {}) if isinstance(it, dict) else {})
        return merged
    if inst_lc == "default":
        return _feature_items(_prov_block(s, provider), feature)
    pblk = _prov_block(s, provider)
    insts = pblk.get("instances")
    if isinstance(insts, dict):
        blk = insts.get(inst)
        if isinstance(blk, dict):
            return _feature_items(blk, feature)
    return {}


@lru_cache(maxsize=64)
def _provider_rewatch_read_supported(provider: str) -> bool:
    ops = load_sync_ops(str(provider or "").upper())
    caps = ops.capabilities() if ops and callable(getattr(ops, "capabilities", None)) else {}
    hist = caps.get("history") if isinstance(caps, Mapping) else None
    rewatches = hist.get("rewatches") if isinstance(hist, Mapping) else None
    return bool(isinstance(rewatches, Mapping) and rewatches.get("read"))


def _event_sort_value(key: str, item: dict[str, Any]) -> tuple[int, str]:
    epoch = history_epoch_from_key(key)
    if epoch is None:
        raw = str(item.get("watched_at") or item.get("watchedAt") or item.get("viewed_at") or "")
        if raw:
            try:
                epoch = int(datetime.fromisoformat(raw.replace("Z", "+00:00").replace(" ", "T")).timestamp())
            except Exception:
                epoch = 0
        else:
            epoch = 0
    return int(epoch or 0), str(key or "")


def _collapse_history_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for raw_key, raw_item in (bucket or {}).items():
        key = str(raw_key)
        item = dict(raw_item or {}) if isinstance(raw_item, dict) else {}
        base = base_key_from_history_event(key) if is_history_event_key(key) else key
        prev = out.get(base)
        if not prev or _event_sort_value(key, item) >= _event_sort_value(str(prev.get("_cw_export_source_key") or base), prev):
            item["_cw_export_source_key"] = key
            out[base] = item
    return out


def _history_items_bucket(
    s: dict[str, Any],
    provider: str,
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> dict[str, Any]:
    bucket = _items_bucket(s, provider, "history", instance_id=instance_id)
    if include_rewatches and _provider_rewatch_read_supported(provider):
        return bucket
    return _collapse_history_bucket(bucket)


def _combined_items_bucket(
    s: dict[str, Any],
    provider: str,
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> dict[str, Any]:
    history = _history_items_bucket(s, provider, instance_id=instance_id, include_rewatches=include_rewatches)
    ratings = _items_bucket(s, provider, "ratings", instance_id=instance_id)
    merged: dict[str, dict[str, Any]] = {str(k): dict(v or {}) for k, v in history.items()}
    ratings_by_base = {base_key_from_history_event(str(k)): dict(v or {}) for k, v in ratings.items()}
    if include_rewatches and _provider_rewatch_read_supported(provider):
        for key, history_item in list(merged.items()):
            rating_item = ratings_by_base.get(base_key_from_history_event(key))
            if not rating_item:
                continue
            out = _merge_best(history_item, rating_item)
            for fld in ("watched_at", "watchedAt", "viewed_at"):
                if history_item.get(fld):
                    out[fld] = history_item[fld]
            for fld in ("rating", "user_rating", "rated_at"):
                if rating_item.get(fld) not in (None, ""):
                    out[fld] = rating_item[fld]
            merged[key] = out

    for k, rating_item in ratings.items():
        key = str(k)
        base = base_key_from_history_event(key)
        if include_rewatches and _provider_rewatch_read_supported(provider) and any(base_key_from_history_event(hk) == base for hk in merged):
            continue
        src = dict(rating_item or {})
        if key not in merged:
            merged[key] = src
            continue

        history_item = merged[key]
        out = _merge_best(history_item, src)
 
        for fld in ("watched_at", "watchedAt", "viewed_at"):
            if history_item.get(fld):
                out[fld] = history_item[fld]
        for fld in ("rating", "user_rating", "rated_at"):
            if src.get(fld) not in (None, ""):
                out[fld] = src[fld]
        merged[key] = out
    return merged


def _feature_bucket(
    s: dict[str, Any],
    provider: str,
    feature: str,
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> dict[str, Any]:
    if feature == "combined":
        return _combined_items_bucket(s, provider, instance_id=instance_id, include_rewatches=include_rewatches)
    if feature == "history":
        return _history_items_bucket(s, provider, instance_id=instance_id, include_rewatches=include_rewatches)
    return _items_bucket(s, provider, feature, instance_id=instance_id)

def _iter_items(
    s: dict[str, Any],
    provider: str,
    feature: str,
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> Iterable[tuple[str, dict[str, Any]]]:
    b = _feature_bucket(s, provider, feature, instance_id=instance_id, include_rewatches=include_rewatches)
    for k, it in (b or {}).items():
        yield str(k), (it or {})


def _norm_ids(ids: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(ids, dict):
        return out
    v = ids.get("imdb")
    if v:
        m = re.search(r"(\d+)", str(v))
        if m:
            out["imdb"] = f"tt{m.group(1)}"
    for ns in ("tmdb", "tvdb", "trakt", "simkl", "mdblist"):
        v = ids.get(ns)
        if v is None:
            continue
        m = re.search(r"(\d+)", str(v))
        out[ns] = m.group(1) if m else str(v)
    if ids.get("slug"):
        out["slug"] = str(ids["slug"])
    return out


def _pick_title(it: dict[str, Any]) -> str:
    return str(it.get("title") or it.get("name") or it.get("series_title") or it.get("show_title") or "")


def _pick_year(it: dict[str, Any]) -> str:
    for k in ("year", "release_year", "first_air_year", "movie_year", "show_year"):
        v = it.get(k)
        if v:
            return str(v)
    for k in ("first_aired", "released", "air_date", "release_date"):
        v = it.get(k)
        if isinstance(v, str):
            m = re.search(r"\b(19|20)\d{2}\b", v)
            if m:
                return m.group(0)
    return ""


def _norm_media_type(value: Any) -> str:
    t = str(value or "").strip().lower()
    return _MEDIA_TYPE_ALIASES.get(t, t)


def _parse_media_types(raw: str | None) -> tuple[str, ...]:
    if raw is None:
        return _DEFAULT_MEDIA_TYPES
    vals = []
    for tok in str(raw).split(","):
        norm = _norm_media_type(tok)
        if norm in _MEDIA_TYPES and norm not in vals:
            vals.append(norm)
    return tuple(vals) if vals else _DEFAULT_MEDIA_TYPES


def _row_base(it: dict[str, Any]) -> tuple[str, str, str, str, dict[str, str]]:
    t = _norm_media_type(it.get("type") or "")
    title = _pick_title(it)
    year = _pick_year(it)
    ids = _norm_ids(it.get("ids") or {})
    watched = (it.get("watched_at") or it.get("watchedAt") or it.get("viewed_at") or it.get("rated_at") or "") or ""
    return t, title, year, watched, ids


def _match_query(key: str, it: dict[str, Any], q: str) -> bool:
    if not q:
        return True
    q = q.strip().lower()
    if not q:
        return True
    _, title, year, _, ids = _row_base(it)
    hay = " ".join(
        filter(
            None,
            [
                key.lower(),
                title.lower(),
                str(year or "").lower(),
                str(it.get("series_title") or "").lower(),
                *(f"{k}:{v}".lower() for k, v in (ids or {}).items()),
            ],
        )
    )
    tokens = [tok for tok in re.split(r"\s+", q) if tok]
    return all(tok in hay for tok in tokens)


def _filter_keys(
    s: dict[str, Any],
    provider: str,
    feature: str,
    q: str,
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> list[str]:
    return _filter_keys_with_media(
        s,
        provider,
        feature,
        q,
        _DEFAULT_MEDIA_TYPES,
        instance_id=instance_id,
        include_rewatches=include_rewatches,
    )


def _filter_keys_with_media(
    s: dict[str, Any],
    provider: str,
    feature: str,
    q: str,
    media_types: tuple[str, ...],
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> list[str]:
    keys: list[str] = []
    for k, it in _iter_items(s, provider, feature, instance_id=instance_id, include_rewatches=include_rewatches):
        t, *_ = _row_base(it)
        if t in media_types and _match_query(k, it, q):
            keys.append(k)
    return keys


def _csv_response(filename: str, header: list[str] | None, rows: Iterable[list[str]]) -> Response:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if header:
        w.writerow(header)
    for r in rows:
        w.writerow([str(x) if x is not None else "" for x in r])
    data = buf.getvalue().encode("utf-8")
    return Response(
        content=data,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
        },
    )


def _rating_1_10(val: Any) -> str:
    try:
        f = float(val)
        if f <= 0:
            return ""
        if f > 10:
            f = 10
        return str(int(f) if f.is_integer() else f)
    except Exception:
        return ""


def _title_type_for_imdb(t: str) -> str:
    t = (t or "").lower()
    return "movie" if t == "movie" else "tvSeries"


def _letterboxd_date(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    # Letterboxd accepts YYYY-MM-DD, but many providers store ISO timestamps
    m = re.match(r"^(\d{4}-\d{2}-\d{2})", raw)
    if m:
        return m.group(1)
    for fmt in ("%Y/%m/%d", "%d-%m-%Y", "%Y%m%d"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def _rating_raw(it: dict[str, Any]) -> str:
    return str(it.get("rating") or it.get("user_rating") or "")


def _actual_watched_at(it: dict[str, Any]) -> str:
    return str(it.get("watched_at") or it.get("watchedAt") or it.get("viewed_at") or "")


def _yamtrack_media_type(t: str) -> str:
    return "tv" if t == "show" else t


def _yamtrack_title(it: dict[str, Any], t: str) -> str:
    if t in {"show", "season", "episode"}:
        return str(it.get("series_title") or it.get("show_title") or it.get("title") or it.get("name") or "")
    return _pick_title(it)


def _yamtrack_primary_tmdb_id(it: dict[str, Any], t: str, ids: dict[str, str]) -> str:
    if t in {"season", "episode"}:
        show_ids = _norm_ids(it.get("show_ids") or {})
        return show_ids.get("tmdb", "")
    return ids.get("tmdb", "")


def _yamtrack_status(feature: str, t: str, actual_watched: str) -> str:
    if t == "episode":
        return ""
    if feature == "watchlist":
        return "Planning"
    if feature in {"history", "combined"} and actual_watched:
        return "Completed"
    return ""


def _yamtrack_progress(it: dict[str, Any], t: str) -> str:
    if t == "episode":
        return ""
    v = it.get("progress")
    if v in (None, ""):
        return ""
    return str(v)


def _letterboxd_rows(
    feature: str,
    items: Iterable[tuple[str, dict[str, Any]]],
    *,
    include_watched_date: bool = True,
) -> tuple[list[str], list[list[str]]]:
    if feature == "watchlist":
        header = ["imdbID", "tmdbID", "Title", "Year"]
        rows = [
            [ids.get("imdb", ""), ids.get("tmdb", ""), title, year]
            for _k, it in items
            for t, title, year, _wd, ids in [_row_base(it)]
            if t == "movie"
        ]
        return header, rows
    if feature == "history":
        header = ["imdbID", "tmdbID", "Title", "Year"]
        if include_watched_date:
            header.append("WatchedDate")
        rows = [
            [
                ids.get("imdb", ""),
                ids.get("tmdb", ""),
                title,
                year,
                *([_letterboxd_date(_actual_watched_at(it))] if include_watched_date else []),
            ]
            for _k, it in items
            for t, title, year, _watched, ids in [_row_base(it)]
            if t == "movie"
        ]
        return header, rows
    if feature == "ratings":
        header = ["imdbID", "tmdbID", "Title", "Year", "Rating"]
        rows = [
            [ids.get("imdb", ""), ids.get("tmdb", ""), title, year, _rating_raw(it)]
            for _k, it in items
            for t, title, year, _wd, ids in [_row_base(it)]
            if t == "movie"
        ]
        return header, rows
    if feature == "combined":
        header = ["imdbID", "tmdbID", "Title", "Year", "Rating"]
        if include_watched_date:
            header.append("WatchedDate")
        rows = [
            [
                ids.get("imdb", ""),
                ids.get("tmdb", ""),
                title,
                year,
                _rating_raw(it),
                *([_letterboxd_date(_actual_watched_at(it))] if include_watched_date else []),
            ]
            for _k, it in items
            for t, title, year, _watched, ids in [_row_base(it)]
            if t == "movie"
        ]
        return header, rows
    raise HTTPException(400, "Unsupported feature for Letterboxd")


def _csv_bytes(header: list[str] | None, rows: Iterable[list[str]]) -> int:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if header:
        w.writerow(header)
    for r in rows:
        w.writerow([str(x) if x is not None else "" for x in r])
    return len(buf.getvalue().encode("utf-8"))


def _target_caps(fmt: str) -> dict[str, Any]:
    caps: dict[str, dict[str, Any]] = {
        "letterboxd": {
            "features": {"watchlist", "history", "ratings", "combined"},
            "media_types": {"movie"},
            "label": "Letterboxd",
        },
        "imdb": {
            "features": {"watchlist"},
            "media_types": set(_MEDIA_TYPES),
            "label": "IMDb",
        },
        "justwatch": {
            "features": {"watchlist", "history", "ratings"},
            "media_types": set(_MEDIA_TYPES),
            "label": "JustWatch",
        },
        "yamtrack": {
            "features": {"watchlist", "history", "combined"},
            "media_types": set(_MEDIA_TYPES),
            "label": "Yamtrack",
        },
        "tmdb": {
            "features": {"watchlist", "ratings"},
            "media_types": set(_MEDIA_TYPES),
            "label": "TMDB",
        },
    }
    return caps.get(fmt, {"features": set(), "media_types": set(), "label": fmt})


def _validate_items(
    fmt: str,
    feature: str,
    items: list[tuple[str, dict[str, Any]]],
    *,
    include_watched_date: bool = True,
) -> tuple[list[tuple[str, dict[str, Any]]], list[str], dict[str, int]]:
    caps = _target_caps(fmt)
    warnings: list[str] = []
    stats = {
        "matched_total": len(items),
        "unsupported_media_total": 0,
        "missing_identity_total": 0,
        "invalid_watched_date_total": 0,
    }
    allowed_media = caps["media_types"]
    exportable: list[tuple[str, dict[str, Any]]] = []

    for key, it in items:
        t, title, _year, _watched, ids = _row_base(it)
        actual_watched = _actual_watched_at(it)
        if allowed_media and t not in allowed_media:
            stats["unsupported_media_total"] += 1
            continue
        if not title and not ids:
            stats["missing_identity_total"] += 1
            continue
        if (
            fmt == "letterboxd"
            and include_watched_date
            and feature in {"history", "combined"}
            and actual_watched
            and not _letterboxd_date(actual_watched)
        ):
            stats["invalid_watched_date_total"] += 1
        exportable.append((key, it))

    label = caps["label"]
    if stats["unsupported_media_total"]:
        warnings.append(f"{label} skipped {stats['unsupported_media_total']} row(s) with unsupported media types.")
    if stats["missing_identity_total"]:
        warnings.append(f"Skipped {stats['missing_identity_total']} row(s) without a usable title or ID.")
    if stats["invalid_watched_date_total"]:
        warnings.append(
            f"{stats['invalid_watched_date_total']} row(s) had watched dates that could not be normalized to YYYY-MM-DD."
        )
    if fmt == "letterboxd":
        header, rows = _letterboxd_rows(feature, exportable, include_watched_date=include_watched_date)
        if _csv_bytes(header, rows) > _LETTERBOXD_MAX_BYTES:
            warnings.append("Letterboxd import files over 1 MB may need to be split before upload.")
    return exportable, warnings, stats


def _build_letterboxd(
    provider: str,
    feature: str,
    s: dict[str, Any],
    keys: list[str],
    instance_id: str | None = None,
    *,
    include_watched_date: bool = True,
    include_rewatches: bool = True,
) -> Response:
    src_items = [
        (k, it)
        for k, it in _iter_items(s, provider, feature, instance_id=instance_id, include_rewatches=include_rewatches)
        if (not keys or k in keys)
    ]
    header, rows = _letterboxd_rows(feature, src_items, include_watched_date=include_watched_date)
    ts = time.strftime("%Y%m%d")
    return _csv_response(f"letterboxd_{feature}_{provider.lower()}_{ts}.csv", header, rows)


def _build_imdb(
    provider: str,
    feature: str,
    s: dict[str, Any],
    keys: list[str],
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> Response:
    if feature != "watchlist":
        raise HTTPException(400, "IMDb export supports watchlist only")
    header = ["const"]
    rows: list[list[str]] = []
    for k, it in _iter_items(s, provider, "watchlist", instance_id=instance_id, include_rewatches=include_rewatches):
        if keys and k not in keys:
            continue
        _, _, _, _, ids = _row_base(it)
        if ids.get("imdb"):
            rows.append([ids["imdb"]])
    ts = time.strftime("%Y%m%d")
    return _csv_response(f"imdb_watchlist_{provider.lower()}_{ts}.csv", header, rows)


def _build_justwatch(
    provider: str,
    feature: str,
    s: dict[str, Any],
    keys: list[str],
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> Response:
    header = ["tmdbID", "imdbID", "Title", "Year", "Type"]
    rows: list[list[str]] = []
    for k, it in _iter_items(s, provider, feature, instance_id=instance_id, include_rewatches=include_rewatches):
        if keys and k not in keys:
            continue
        t, title, year, _wd, ids = _row_base(it)
        rows.append([ids.get("tmdb", ""), ids.get("imdb", ""), title, year, t])
    ts = time.strftime("%Y%m%d")
    return _csv_response(f"justwatch_{feature}_{provider.lower()}_{ts}.csv", header, rows)


def _build_yamtrack(
    provider: str,
    feature: str,
    s: dict[str, Any],
    keys: list[str],
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> Response:
    header = [
        "media_id",
        "source",
        "media_type",
        "title",
        "image",
        "season_number",
        "episode_number",
        "score",
        "progress",
        "status",
        "start_date",
        "end_date",
        "notes",
        "progressed_at",
    ]
    rows: list[list[str]] = []
    for k, it in _iter_items(s, provider, feature, instance_id=instance_id, include_rewatches=include_rewatches):
        if keys and k not in keys:
            continue
        t, title, _year, _fallback_date, ids = _row_base(it)
        actual_watched = _actual_watched_at(it)
        if feature == "combined" and not actual_watched:
            # Yamtrack has no clean native representation for rating-only rows
            continue
        tmdb_id = _yamtrack_primary_tmdb_id(it, t, ids)
        source = "tmdb" if tmdb_id else ""
        media_type = _yamtrack_media_type(t)
        native_title = _yamtrack_title(it, t) or title
        score = "" if t == "episode" else _rating_1_10(it.get("rating") or it.get("user_rating") or "")
        season_number = str(it.get("season") or "")
        episode_number = str(it.get("episode") or "")
        end_date = actual_watched if feature in {"history", "combined"} else ""
        progressed_at = actual_watched if feature in {"history", "combined"} else ""
        rows.append(
            [
                tmdb_id,
                source,
                media_type,
                native_title,
                "",
                season_number,
                episode_number,
                score,
                _yamtrack_progress(it, t),
                _yamtrack_status(feature, t, actual_watched),
                "",
                end_date,
                "",
                progressed_at,
            ]
        )
    ts = time.strftime("%Y%m%d")
    return _csv_response(f"yamtrack_{feature}_{provider.lower()}_{ts}.csv", header, rows)

def _tmdb_build_imdb_v3(
    provider: str,
    feature: str,
    s: dict[str, Any],
    keys: list[str],
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> Response:
    ts = time.strftime("%Y%m%d")
    if feature == "watchlist":
        header = [
            "Position",
            "Const",
            "Created",
            "Modified",
            "Description",
            "Title",
            "URL",
            "Title Type",
            "IMDb Rating",
            "Runtime (mins)",
            "Year",
            "Genres",
            "Num Votes",
            "Release Date",
            "Directors",
            "Your Rating",
            "Date Rated",
        ]
        rows: list[list[str]] = []
        pos = 0
        for k, it in _iter_items(s, provider, "watchlist", instance_id=instance_id, include_rewatches=include_rewatches):
            if keys and k not in keys:
                continue
            t, title, year, _wd, ids = _row_base(it)
            imdb = ids.get("imdb")
            if not imdb:
                continue
            pos += 1
            url = f"https://www.imdb.com/title/{imdb}/"
            rows.append(
                [
                    str(pos),
                    imdb,
                    "",
                    "",
                    "",
                    title,
                    url,
                    _title_type_for_imdb(t),
                    "",
                    "",
                    year,
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                ]
            )
        return _csv_response(f"tmdb_imdbv3_watchlist_{provider.lower()}_{ts}.csv", header, rows)
    if feature == "ratings":
        header = [
            "Const",
            "Your Rating",
            "Date Rated",
            "Title",
            "URL",
            "Title Type",
            "IMDb Rating",
            "Runtime (mins)",
            "Year",
            "Genres",
            "Num Votes",
            "Release Date",
            "Directors",
        ]
        rows: list[list[str]] = []
        for k, it in _iter_items(s, provider, "ratings", instance_id=instance_id, include_rewatches=include_rewatches):
            if keys and k not in keys:
                continue
            t, title, year, watched, ids = _row_base(it)
            imdb = ids.get("imdb")
            if not imdb:
                continue
            rating = _rating_1_10(it.get("rating") or it.get("user_rating") or "")
            url = f"https://www.imdb.com/title/{imdb}/"
            date_rated = (it.get("rated_at") or watched or "") or ""
            rows.append(
                [
                    imdb,
                    rating,
                    date_rated,
                    title,
                    url,
                    _title_type_for_imdb(t),
                    "",
                    "",
                    year,
                    "",
                    "",
                    "",
                    "",
                ]
            )
        return _csv_response(f"tmdb_imdbv3_ratings_{provider.lower()}_{ts}.csv", header, rows)
    raise HTTPException(400, "TMDB supports watchlist and ratings only")


def _tmdb_build_trakt_v2(
    provider: str,
    feature: str,
    s: dict[str, Any],
    keys: list[str],
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> Response:
    header = [
        "rated_at",
        "type",
        "title",
        "year",
        "trakt_rating",
        "trakt_id",
        "imdb_id",
        "tmdb_id",
        "tvdb_id",
        "season",
        "episode",
        "show_title",
        "show_year",
        "show_trakt_id",
        "show_imdb_id",
        "show_tmdb_id",
        "show_tvdb_id",
        "episode_imdb_id",
        "episode_tmdb_id",
        "episode_tvdb_id",
        "genres",
        "rating",
    ]
    ts = time.strftime("%Y%m%d")
    rows: list[list[str]] = []
    src = "ratings" if feature == "ratings" else "watchlist"
    for k, it in _iter_items(s, provider, src, instance_id=instance_id, include_rewatches=include_rewatches):
        if keys and k not in keys:
            continue
        t, title, year, watched, ids = _row_base(it)
        rating = _rating_1_10(it.get("rating") or it.get("user_rating") or "")
        rows.append(
            [
                watched if feature == "ratings" else "",
                (t or "movie").lower(),
                title,
                year,
                "",
                ids.get("trakt", ""),
                ids.get("imdb", ""),
                ids.get("tmdb", ""),
                ids.get("tvdb", ""),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                rating if feature == "ratings" else "",
            ]
        )
    return _csv_response(f"tmdb_traktv2_{src}_{provider.lower()}_{ts}.csv", header, rows)


def _tmdb_build_simkl_v1(
    provider: str,
    feature: str,
    s: dict[str, Any],
    keys: list[str],
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> Response:
    header = [
        "SIMKL_ID",
        "Title",
        "Type",
        "Year",
        "Watchlist",
        "LastEpWatched",
        "WatchedDate",
        "Rating",
        "Memo",
        "TVDB",
        "TMDB",
        "IMDB",
    ]
    ts = time.strftime("%Y%m%d")
    rows: list[list[str]] = []
    src = "ratings" if feature == "ratings" else "watchlist"
    for k, it in _iter_items(s, provider, src, instance_id=instance_id, include_rewatches=include_rewatches):
        if keys and k not in keys:
            continue
        t, title, year, watched, ids = _row_base(it)
        rating = _rating_1_10(it.get("rating") or it.get("user_rating") or "")
        rows.append(
            [
                ids.get("simkl", ""),
                title,
                (t or "movie").capitalize(),
                year,
                "1" if src == "watchlist" else "",
                "",
                watched if src == "ratings" else "",
                rating if src == "ratings" else "",
                "",
                ids.get("tvdb", ""),
                ids.get("tmdb", ""),
                ids.get("imdb", ""),
            ]
        )
    return _csv_response(f"tmdb_simklv1_{src}_{provider.lower()}_{ts}.csv", header, rows)


def _build_tmdb(
    provider: str,
    feature: str,
    s: dict[str, Any],
    keys: list[str],
    instance_id: str | None = None,
    *,
    include_rewatches: bool = True,
) -> Response:
    p = provider.upper().strip()
    if p == "TRAKT":
        return _tmdb_build_trakt_v2(provider, feature, s, keys, instance_id=instance_id, include_rewatches=include_rewatches)
    if p == "SIMKL":
        return _tmdb_build_simkl_v1(provider, feature, s, keys, instance_id=instance_id, include_rewatches=include_rewatches)
    return _tmdb_build_imdb_v3(provider, feature, s, keys, instance_id=instance_id, include_rewatches=include_rewatches)


_BUILDERS: dict[str, Callable[..., Response]] = {
    "letterboxd": _build_letterboxd,
    "imdb": _build_imdb,
    "justwatch": _build_justwatch,
    "yamtrack": _build_yamtrack,
    "tmdb": _build_tmdb,
}


@router.get("/export/options", response_class=JSONResponse)
def api_export_options(request: Request = cast(Request, None)) -> dict[str, Any]:
    s = _load_state(_EXPORT_STATE_FEATURES)
    cfg = _load_config_safe()
    provs = _providers_in_state(s)
    allowed_map = _allowed_provider_map(cfg, request)
    if allowed_map is not None:
        allowed_providers = set(allowed_map.keys())
        provs = [provider for provider in provs if str(provider or "").strip().upper() in allowed_providers]
    features = ["watchlist", "history", "ratings", "combined"]

    def insts_for(p: str) -> list[dict[str, str]]:
        blk = _prov_block(s, p)
        if not blk:
            return []
        out: list[dict[str, str]] = [{"id": "default", "label": "Default"}]
        insts = blk.get("instances")
        if isinstance(insts, dict):
            for inst_id in sorted(insts.keys(), key=lambda x: str(x)):
                iid = str(inst_id)
                out.append({"id": iid, "label": _configured_instance_label(cfg, p, iid, iid)})
        if allowed_map is not None:
            allowed = set(allowed_map.get(str(p or "").strip().upper()) or [])
            out = [row for row in out if str(row.get("id") or "default") in allowed]
        return out

    instances: dict[str, list[dict[str, str]]] = {p: insts_for(p) for p in provs}
    counts: dict[str, dict[str, int]] = {
        p: {f: len(_feature_bucket(s, p, f, instance_id="all") or {}) for f in features} for p in provs
    }
    counts_instances: dict[str, dict[str, dict[str, int]]] = {}
    for p in provs:
        counts_instances[p] = {}
        for inst in instances.get(p) or []:
            iid = inst.get("id") or "default"
            counts_instances[p][iid] = {f: len(_feature_bucket(s, p, f, instance_id=iid) or {}) for f in features}

    formats = {
        "watchlist": ["letterboxd", "imdb", "justwatch", "yamtrack", "tmdb"],
        "history": ["letterboxd", "justwatch", "yamtrack"],
        "ratings": ["letterboxd", "tmdb"],
        "combined": ["letterboxd", "yamtrack"],
    }
    labels = {
        "letterboxd": "Letterboxd",
        "imdb": "IMDb (list)",
        "justwatch": "JustWatch",
        "yamtrack": "Yamtrack",
        "tmdb": "TMDB (Auto: IMDb/Trakt/SIMKL)",
    }
    capabilities = {
        fmt: {
            "features": sorted(_target_caps(fmt)["features"]),
            "media_types": sorted(_target_caps(fmt)["media_types"]),
        }
        for fmt in labels
    }
    rewatches = {p: _provider_rewatch_read_supported(p) for p in provs}
    return {
        "providers": provs,
        "instances": instances,
        "counts": counts,
        "counts_instances": counts_instances,
        "formats": formats,
        "labels": labels,
        "capabilities": capabilities,
        "rewatches": rewatches,
        "media_types": list(_MEDIA_TYPES),
        "default_media_types": list(_DEFAULT_MEDIA_TYPES),
    }


@router.get("/export/sample", response_class=JSONResponse)
def api_export_sample(
    provider: str = Query("", description="TRAKT|PLEX|EMBY|JELLYFIN|SIMKL|MDBLIST|CROSSWATCH"),
    provider_instance: str = Query("all", description="default|all|<instance_id>"),
    feature: str = Query("watchlist", pattern="^(watchlist|history|ratings|combined)$"),
    format: str = Query("letterboxd", pattern="^(letterboxd|imdb|justwatch|yamtrack|tmdb)$"),
    media_types: str = Query("movie", description="CSV of movie,show,season,episode"),
    include_watched_date: bool = Query(True, description="Letterboxd only: include WatchedDate for history exports"),
    include_rewatches: bool = Query(True, description="Keep separate history events when supported by the source provider"),
    limit: int = Query(25, ge=1, le=250),
    q: str = Query("", description="case-insensitive multi-token contains"),
    request: Request = cast(Request, None),
) -> dict[str, Any]:
    feature = feature.lower().strip()
    q = q if isinstance(q, str) else ""
    limit = limit if isinstance(limit, int) else 25
    s = _load_state(_state_features_for_export(feature))
    provider = (provider or "").upper().strip()
    cfg = _load_config_safe()
    inst = _export_instance_for_request(cfg, request, provider, provider_instance)
    fmt = format.lower().strip()
    media = _parse_media_types(media_types)
    rewatch_supported = _provider_rewatch_read_supported(provider)
    rewatch_mode = bool(include_rewatches and rewatch_supported)
    if provider and provider in _providers_in_state(s):
        keys = _filter_keys_with_media(
            s,
            provider,
            feature,
            q,
            media,
            instance_id=inst,
            include_rewatches=rewatch_mode,
        )
        bucket = _feature_bucket(s, provider, feature, instance_id=inst, include_rewatches=rewatch_mode)
        matched = [(k, bucket.get(k, {})) for k in keys]
        exportable, warnings, validation = _validate_items(
            fmt,
            feature,
            matched,
            include_watched_date=include_watched_date,
        )
    else:
        exportable = []
        warnings = []
        validation = {
            "matched_total": 0,
            "unsupported_media_total": 0,
            "missing_identity_total": 0,
            "invalid_watched_date_total": 0,
        }

    items: list[dict[str, Any]] = []
    for i, (k, it) in enumerate(exportable):
        t, title, year, watched, ids = _row_base(it)
        items.append(
            {
                "key": k,
                "type": t,
                "title": title,
                "year": year,
                "watched_at": watched,
                "ids": ids,
                "rating": it.get("rating") or it.get("user_rating"),
            }
        )
        if i + 1 >= limit:
            break
    return {
        "items": items,
        "total": len(exportable),
        "matched_total": validation["matched_total"],
        "dropped_total": validation["unsupported_media_total"] + validation["missing_identity_total"],
        "warnings": warnings,
        "validation": validation,
        "media_types": list(media),
        "rewatches_supported": rewatch_supported,
        "include_rewatches": rewatch_mode,
    }


@router.get("/export/file")
def api_export_file(
    provider: str = Query("", description="TRAKT|PLEX|EMBY|JELLYFIN|SIMKL|MDBLIST|CROSSWATCH"),
    provider_instance: str = Query("all", description="default|all|<instance_id>"),
    feature: str = Query("watchlist", pattern="^(watchlist|history|ratings|combined)$"),
    format: str = Query("letterboxd", pattern="^(letterboxd|imdb|justwatch|yamtrack|tmdb)$"),
    media_types: str = Query("movie", description="CSV of movie,show,season,episode"),
    include_watched_date: bool = Query(True, description="Letterboxd only: include WatchedDate for history exports"),
    include_rewatches: bool = Query(True, description="Keep separate history events when supported by the source provider"),
    q: str = Query("", description="optional search filter (server-side)"),
    ids: str = Query("", description="optional CSV of keys to include (overrides q)"),
    request: Request = cast(Request, None),
) -> Response:
    provider_in = (provider or "").upper().strip()
    provider_eff = provider_in or "TRAKT"
    feature = feature.lower().strip()
    q = q if isinstance(q, str) else ""
    s = _load_state(_state_features_for_export(feature))
    fmt = format.lower().strip()
    cfg = _load_config_safe()
    inst = _export_instance_for_request(cfg, request, provider_eff, provider_instance)
    media = _parse_media_types(media_types)
    rewatch_mode = bool(include_rewatches and _provider_rewatch_read_supported(provider_eff))

    if fmt not in _BUILDERS:
        raise HTTPException(400, "Unknown format")
    if feature not in ("watchlist", "ratings", "history", "combined"):
        raise HTTPException(400, "Unsupported feature")
    if feature not in _target_caps(fmt)["features"]:
        raise HTTPException(400, f"{_target_caps(fmt)['label']} does not support {feature} exports")

    if ids.strip():
        requested_keys = [k.strip() for k in ids.split(",") if k.strip()]
        bucket = _feature_bucket(s, provider_eff, feature, instance_id=inst, include_rewatches=rewatch_mode)
        items = [
            (k, bucket.get(k, {}))
            for k in requested_keys
            if k in bucket and _row_base(bucket.get(k, {}))[0] in media
        ]
    else:
        requested_keys = (
            _filter_keys_with_media(
                s,
                provider_eff,
                feature,
                q,
                media,
                instance_id=inst,
                include_rewatches=rewatch_mode,
            )
            if provider_eff in _providers_in_state(s)
            else []
        )
        bucket = _feature_bucket(s, provider_eff, feature, instance_id=inst, include_rewatches=rewatch_mode)
        items = [(k, bucket.get(k, {})) for k in requested_keys]

    exportable, _warnings, _validation = _validate_items(
        fmt,
        feature,
        items,
        include_watched_date=include_watched_date,
    )
    keys = [k for k, _it in exportable]
    if fmt == "letterboxd":
        return _build_letterboxd(
            provider_eff,
            feature,
            s,
            keys,
            inst,
            include_watched_date=include_watched_date,
            include_rewatches=rewatch_mode,
        )
    return _BUILDERS[fmt](provider_eff, feature, s, keys, inst, include_rewatches=rewatch_mode)
