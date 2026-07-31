# providers/sync/stremio/_ratings.py
# CrossWatch Stremio ratings functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from cw_platform.id_map import ids_from, minimal as id_minimal
from providers.auth._auth_STREMIO import StremioAuthError
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import canonical_item_key, configured_block, imdb_id, imdb_ids_from_item, is_capture_mode, stremio_profile_id, tmdb_metadata_provider

LIKES_URL = "https://likes.stremio.com/api/send"
LIKES_STATUS_URL = "https://likes.stremio.com/api/get_status"
STATE_DIR = Path("/config/.cw_state")


def _safe_scope(value: str) -> str:
    out = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in str(value or ""))
    while "__" in out:
        out = out.replace("__", "_")
    return (out.strip("_ ") or "unscoped")[:96]


def _cache_path(adapter: Any) -> Path:
    scope = _safe_scope(os.getenv("CW_PAIR_KEY") or os.getenv("CW_PAIR_SCOPE") or os.getenv("CW_SYNC_PAIR") or os.getenv("CW_PAIR") or "unscoped")
    profile = _safe_scope(stremio_profile_id(adapter))
    return STATE_DIR / f"stremio_ratings.{profile}.{scope}.json"


def _load_cache(adapter: Any) -> dict[str, dict[str, Any]]:
    if is_capture_mode():
        return {}
    try:
        data = json.loads(_cache_path(adapter).read_text("utf-8") or "{}")
    except Exception:
        return {}
    rows = data.get("items") if isinstance(data, Mapping) else None
    return {str(k): dict(v) for k, v in (rows or {}).items() if isinstance(v, Mapping)}


def _save_cache(adapter: Any, rows: Mapping[str, Mapping[str, Any]]) -> None:
    if is_capture_mode():
        return
    try:
        path = _cache_path(adapter)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"items": dict(rows)}, ensure_ascii=False, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception:
        pass


def _rating_number(value: Any) -> float | None:
    try:
        number = float(str(value).strip())
    except Exception:
        return None
    if 10 < number <= 100:
        number /= 10.0
    return number if 0 < number <= 10 else None


def _float_setting(value: Any, default: float) -> float:
    try:
        out = float(str(value).strip())
    except Exception:
        return default
    return out if 0 <= out <= 10 else default


def _rating_thresholds(adapter: Any) -> tuple[float, float]:
    block = configured_block(getattr(adapter, "config", None), getattr(adapter, "instance_id", "default"))
    raw = block.get("ratings")
    ratings = raw if isinstance(raw, Mapping) else {}
    liked_min = _float_setting(ratings.get("liked_min"), 6.0)
    loved_min = _float_setting(ratings.get("loved_min"), 8.0)
    if loved_min < liked_min:
        loved_min = liked_min
    return liked_min, loved_min


def _status_for_rating(adapter: Any, value: Any) -> str | None:
    number = _rating_number(value)
    if number is None:
        return None
    liked_min, loved_min = _rating_thresholds(adapter)
    if liked_min <= number < loved_min:
        return "liked"
    if loved_min <= number <= 10.0:
        return "loved"
    return None


def _media_type(item: Mapping[str, Any]) -> str | None:
    typ = str(item.get("type") or id_minimal(item).get("type") or "").strip().lower()
    if typ == "movie":
        return "movie"
    if typ in {"show", "series", "tv"}:
        return "series"
    return None


def _enriched_item(adapter: Any, item: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(item)
    if imdb_id(imdb_ids_from_item(out).get("imdb")):
        return out
    provider = tmdb_metadata_provider(adapter)
    if provider is None:
        return out
    media_type = _media_type(out)
    if media_type is None:
        return out
    ids = ids_from(out)
    lookup = {k: str(ids.get(k) or "").strip() for k in ("tmdb", "imdb", "tvdb") if str(ids.get(k) or "").strip()}
    title = str(out.get("title") or out.get("series_title") or out.get("show_title") or "").strip()
    if title:
        lookup["title"] = title
    if out.get("year"):
        lookup["year"] = str(out.get("year"))
    if not lookup:
        return out
    try:
        detail = provider.fetch(entity="tv" if media_type == "series" else "movie", ids=lookup, need={"poster": False, "backdrop": False, "ids": True})
    except Exception:
        return out
    ids_raw = detail.get("ids") if isinstance(detail, Mapping) else None
    ids_map: Mapping[str, Any] = ids_raw if isinstance(ids_raw, Mapping) else {}
    found = imdb_id(ids_map.get("imdb"))
    if found:
        merged = dict(ids)
        merged["imdb"] = found
        out["ids"] = merged
    return out


def _media_id(item: Mapping[str, Any]) -> str | None:
    for field in ("_stremio_id", "stremio_id", "content_id"):
        direct = str(item.get(field) or "").strip()
        found = imdb_id(direct)
        if found:
            return found
    imdb = imdb_id(imdb_ids_from_item(item).get("imdb"))
    if imdb:
        return imdb
    return None


def _response_detail(resp: Any) -> str:
    try:
        data = resp.json()
    except Exception:
        data = None
    if isinstance(data, Mapping):
        for key in ("message", "error", "detail"):
            value = data.get(key)
            if value:
                return str(value)[:180]
    return str(getattr(resp, "text", "") or "")[:180]


def _remote_status(adapter: Any, media_id: str, media_type: str) -> str | None:
    resp = adapter.client.session.get(
        LIKES_STATUS_URL,
        params={"authToken": adapter.client.auth_key(), "mediaId": media_id, "mediaType": media_type},
        headers={"Accept": "application/json"},
        timeout=20,
    )
    if resp.status_code in (401, 403):
        raise StremioAuthError("Stremio rejected the credentials", reason="invalid_credentials", status_code=resp.status_code, endpoint="likes/get_status")
    if resp.status_code >= 400:
        raise StremioAuthError("Stremio rating status failed", reason="request_failed", detail=_response_detail(resp), status_code=resp.status_code, endpoint="likes/get_status")
    try:
        data = resp.json()
    except Exception:
        return None
    if not isinstance(data, Mapping):
        return None
    value = data.get("status")
    return str(value).strip().lower() if value else None


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    return _load_cache(adapter)


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, clear_missing=False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, clear_missing=True, dry_run=dry_run)


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, clear_missing: bool, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    skipped: list[str] = []
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    cache = _load_cache(adapter)
    attempted = 0
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(raw)
        item = _enriched_item(adapter, raw)
        media_type = _media_type(item)
        media_id = _media_id(item)
        status = None if clear_missing else _status_for_rating(adapter, item.get("rating"))
        if not media_type or not media_id:
            entry = {"status": "unresolved", "reason": "stremio_rating_id_missing", "item": id_minimal(item), "canonical_key": key}
            unresolved.append(entry)
            results.append(entry)
            continue
        if not clear_missing and status is None:
            skipped.append(key)
            if not dry_run:
                cached = id_minimal(item)
                cached["rating"] = item.get("rating")
                if item.get("rated_at"):
                    cached["rated_at"] = item.get("rated_at")
                cached["_stremio_reaction"] = None
                cached["_stremio_skip_reason"] = "below_threshold"
                cache[key] = cached
            results.append({"status": "skipped", "reason": "stremio_rating_below_threshold", "item": id_minimal(item), "canonical_key": key})
            continue
        attempted += 1
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(item), "canonical_key": key})
            continue
        try:
            if not clear_missing:
                existing = _remote_status(adapter, media_id, media_type)
                if existing == status:
                    skipped.append(key)
                    cached = id_minimal(item)
                    cached["rating"] = item.get("rating")
                    if item.get("rated_at"):
                        cached["rated_at"] = item.get("rated_at")
                    cached["_stremio_reaction"] = status
                    cache[key] = cached
                    results.append({"status": "skipped", "reason": "stremio_rating_already_set", "item": id_minimal(item), "canonical_key": key})
                    continue
            resp = adapter.client.session.post(
                LIKES_URL,
                json={"authToken": adapter.client.auth_key(), "mediaId": media_id, "mediaType": media_type, "status": status},
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                timeout=20,
            )
            if resp.status_code in (401, 403):
                raise StremioAuthError("Stremio rejected the credentials", reason="invalid_credentials", status_code=resp.status_code, endpoint="likes/send")
            if resp.status_code >= 400:
                raise StremioAuthError("Stremio rating write failed", reason="request_failed", detail=_response_detail(resp), status_code=resp.status_code, endpoint="likes/send")
        except StremioAuthError as exc:
            reason = str(getattr(exc, "reason", "") or "stremio_rating_write_failed")
            entry = {"status": "failed", "reason": reason, "item": id_minimal(item), "canonical_key": key}
            detail = str(getattr(exc, "detail", "") or "").strip()
            if detail:
                entry["hint"] = detail
            unresolved.append(entry)
            results.append(entry)
            continue
        except Exception:
            entry = {"status": "failed", "reason": "stremio_rating_write_failed", "item": id_minimal(item), "canonical_key": key}
            unresolved.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        if clear_missing:
            cache.pop(key, None)
        else:
            cached = id_minimal(item)
            cached["rating"] = item.get("rating")
            if item.get("rated_at"):
                cached["rated_at"] = item.get("rated_at")
            cached["_stremio_reaction"] = status
            cache[key] = cached
        results.append({"status": "applied", "item": id_minimal(item), "canonical_key": key})
    if (confirmed or skipped) and not dry_run:
        _save_cache(adapter, cache)
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved, canonical_item_key), unresolved=unresolved, results=results, attempted=attempted, skipped=len(skipped), skipped_keys=skipped)
