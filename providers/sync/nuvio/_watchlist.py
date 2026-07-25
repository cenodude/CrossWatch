# providers/sync/nuvio/_watchlist.py
# CrossWatch Nuvio Library watchlist functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import canonical_key, ids_from, minimal as id_minimal

from providers.sync._log import log as cw_log

from ._common import canonical_item_key, epoch_ms, library_lock, make_item, payload_item_key, pull_library_rows, resolve_content_id_for_item, rpc, selected_profile_id

_METADATA_FIELDS = (
    "name",
    "poster",
    "poster_shape",
    "background",
    "description",
    "release_info",
    "imdb_rating",
    "genres",
    "addon_base_url",
    "added_at",
)


def _info(event: str, **fields: Any) -> None:
    cw_log("NUVIO", "watchlist", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("NUVIO", "watchlist", "warn", event, **fields)


def _item_from_row(row: Mapping[str, Any]) -> tuple[str | None, dict[str, Any] | None, str | None]:
    ctype = str(row.get("content_type") or "").strip().lower()
    if ctype not in {"movie", "series", "show"}:
        return None, None, "nuvio_id_missing"
    item = make_item(content_id=row.get("content_id"), content_type="series" if ctype in {"series", "show"} else "movie", title=row.get("name") or row.get("title"), year=row.get("release_info"))
    if not item:
        return None, None, "nuvio_id_missing"
    if item.get("type") == "episode":
        return None, None, "nuvio_id_missing"
    if ctype in {"series", "show"}:
        item["type"] = "show"
        item.pop("season", None)
        item.pop("episode", None)
    item["_nuvio_content_id"] = str(row.get("content_id") or "").strip()
    item["_nuvio_profile_id"] = row.get("profile_id")
    for field in _METADATA_FIELDS:
        if field in row:
            item[f"_nuvio_{field}"] = row.get(field)
    key = canonical_item_key(item)
    if not key or key == "unknown:":
        return None, None, "nuvio_id_missing"
    return key, item, None


def _remote_for_row(row: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in _METADATA_FIELDS:
        value = row.get(field)
        if value not in (None, ""):
            out[field] = value
    out["content_id"] = str(row.get("content_id") or "").strip()
    out["content_type"] = str(row.get("content_type") or "movie").strip().lower() or "movie"
    if not out.get("name"):
        out["name"] = str(row.get("title") or "Untitled").strip() or "Untitled"
    out["poster_shape"] = str(out.get("poster_shape") or "POSTER").strip().upper() or "POSTER"
    if not isinstance(out.get("genres"), list):
        out["genres"] = []
    return out


def _tmdb_metadata_api_key(adapter: Any) -> str:
    cfg = getattr(adapter, "config", {}) or {}
    if not isinstance(cfg, Mapping):
        return ""
    tmdb_obj = cfg.get("tmdb")
    tmdb: Mapping[str, Any] = tmdb_obj if isinstance(tmdb_obj, Mapping) else {}
    metadata_obj = cfg.get("metadata")
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    return str(tmdb.get("api_key") or metadata.get("tmdb_api_key") or "").strip()


def _tmdb_metadata_locale(adapter: Any) -> str:
    cfg = getattr(adapter, "config", {}) or {}
    if not isinstance(cfg, Mapping):
        return "en-US"
    metadata_obj = cfg.get("metadata")
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    ui_obj = cfg.get("ui")
    ui: Mapping[str, Any] = ui_obj if isinstance(ui_obj, Mapping) else {}
    return str(metadata.get("locale") or ui.get("locale") or "en-US").strip() or "en-US"


def _image_url(meta: Mapping[str, Any], kind: str) -> str:
    images_obj = meta.get("images")
    images: Mapping[str, Any] = images_obj if isinstance(images_obj, Mapping) else {}
    rows = images.get(kind)
    if not isinstance(rows, list):
        return ""
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        url = str(row.get("url") or "").strip()
        if url:
            return url
    return ""


def _tmdb_enrichment(adapter: Any, item: Mapping[str, Any], *, content_id: str, content_type: str) -> dict[str, Any]:
    if not content_id.startswith("tmdb:"):
        return {}
    tmdb_id = content_id.split(":", 1)[1].strip()
    if not tmdb_id.isdigit():
        return {}
    api_key = _tmdb_metadata_api_key(adapter)
    if not api_key:
        return {}

    locale = _tmdb_metadata_locale(adapter)
    ids = ids_from(item)
    try:
        from api.metaAPI import get_meta

        data = get_meta(
            api_key,
            "tv" if content_type == "series" else "movie",
            tmdb_id,
            ".cache",
            need={
                "title": True,
                "year": True,
                "overview": True,
                "genres": True,
                "poster": True,
                "backdrop": True,
            },
            locale=locale,
            title=str(item.get("title") or item.get("series_title") or item.get("name") or "").strip() or None,
            year=item.get("year") or item.get("series_year"),
            imdb_id=ids.get("imdb"),
            tvdb_id=ids.get("tvdb"),
        )
    except Exception as exc:
        _warn("tmdb_enrichment_failed", error_type=exc.__class__.__name__)
        return {}
    if not isinstance(data, Mapping):
        return {}

    out: dict[str, Any] = {}
    title = data.get("title")
    if title:
        out["name"] = str(title)
    poster = _image_url(data, "poster")
    if poster:
        out["poster"] = poster
    backdrop = _image_url(data, "backdrop")
    if backdrop:
        out["background"] = backdrop
    overview = str(data.get("overview") or "").strip()
    if overview:
        out["description"] = overview
    year = data.get("year")
    if year not in (None, ""):
        out["release_info"] = str(year)
    genres = [str(g) for g in (data.get("genres") or []) if str(g or "").strip()]
    if genres:
        out["genres"] = genres
    return out


def _remote_for_item(adapter: Any, item: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    content_id = resolve_content_id_for_item(adapter, item)
    if not content_id:
        return None, "nuvio_id_missing"
    typ = str(item.get("type") or "").strip().lower()
    if typ in {"episode", "season"}:
        return None, "nuvio_id_missing"
    content_type = "series" if typ in {"show", "series", "tv"} else "movie"
    title = str(item.get("title") or item.get("series_title") or item.get("name") or "Untitled").strip() or "Untitled"
    out: dict[str, Any] = {
        "content_id": content_id,
        "content_type": content_type,
        "name": title,
        "poster_shape": "POSTER",
        "genres": [],
    }
    year = item.get("year") or item.get("series_year")
    if year not in (None, ""):
        out["release_info"] = str(year)
    added_at = epoch_ms(item.get("listed_at") or item.get("added_at") or item.get("watched_at"))
    if added_at is not None:
        out["added_at"] = int(added_at)
    enriched = _tmdb_enrichment(adapter, item, content_id=content_id, content_type=content_type)
    for key, value in enriched.items():
        if out.get(key) in (None, "", []) and value not in (None, "", []):
            out[key] = value
    return out, None


def _verify_key_for_item(adapter: Any, item: Mapping[str, Any]) -> str:
    content_id = resolve_content_id_for_item(adapter, item)
    if not content_id:
        return canonical_item_key(item)
    typ = str(item.get("type") or "").strip().lower()
    content_type = "series" if typ in {"show", "series", "tv"} else "movie"
    return payload_item_key({"content_id": content_id, "content_type": content_type}) or canonical_item_key(item)


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    rows = pull_library_rows(adapter)
    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    for row in rows:
        key, item, reason = _item_from_row(row)
        if not key or not item:
            if reason:
                skipped += 1
                _warn("row_skipped", reason=reason)
            continue
        out[key] = item
    _info("index_done", count=len(out), rows=len(rows), skipped=skipped)
    return out


def _pull_remote_by_key(adapter: Any) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    rows = pull_library_rows(adapter)
    items: dict[str, dict[str, Any]] = {}
    remote: dict[str, dict[str, Any]] = {}
    for row in rows:
        key, item, _reason = _item_from_row(row)
        if key and item:
            items[key] = item
            remote[key] = _remote_for_row(row)
    return items, remote


def _merge_metadata(existing: Mapping[str, Any] | None, incoming: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(existing or {})
    for key, value in incoming.items():
        if key in {"content_id", "content_type"}:
            out[key] = value
            continue
        if out.get(key) in (None, "", []) and value not in (None, "", []):
            out[key] = value
    if not out.get("name"):
        out["name"] = "Untitled"
    if not out.get("poster_shape"):
        out["poster_shape"] = "POSTER"
    if not isinstance(out.get("genres"), list):
        out["genres"] = []
    return out


def _result(ok: bool, count: int, attempted: int, confirmed: list[str], unresolved: list[dict[str, Any]], results: list[dict[str, Any]], skipped: int, **extra: Any) -> dict[str, Any]:
    out = {
        "ok": bool(ok),
        "count": int(count),
        "attempted": int(attempted),
        "confirmed_keys": list(dict.fromkeys(k for k in confirmed if k)),
        "unresolved": unresolved,
        "unresolved_keys": list(dict.fromkeys(canonical_key((u.get("item") or {}) if isinstance(u, Mapping) else {}) for u in unresolved)),
        "results": results,
        "skipped": int(skipped),
        "errors": sum(1 for u in unresolved if str(u.get("status") or "") == "failed"),
    }
    out.update(extra)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    src = [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]
    with library_lock(adapter):
        current, remote = _pull_remote_by_key(adapter)
        unresolved: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []
        changed = False
        attempted = 0
        pending_keys: list[str] = []
        verify_keys: list[str] = []
        pending_items: list[dict[str, Any]] = []

        for item in src:
            key = canonical_item_key(item)
            payload, reason = _remote_for_item(adapter, item)
            if payload is None:
                entry = {"status": "unresolved", "reason": reason or "nuvio_id_missing", "item": id_minimal(item)}
                unresolved.append(entry)
                results.append(entry)
                continue
            verify_key = payload_item_key(payload) or key
            attempted += 1
            existing = remote.get(verify_key) or remote.get(key)
            merged = _merge_metadata(existing, payload)
            if existing != merged:
                remote.pop(key, None)
                remote[verify_key] = merged
                changed = True
            pending_keys.append(key)
            verify_keys.append(verify_key)
            pending_items.append(item)
            results.append({"status": "pending" if not dry_run else "dry_run", "item": id_minimal(item), "canonical_key": key})

        if dry_run:
            return _result(len(unresolved) == 0, attempted, attempted, [], unresolved, results, 0, dry_run=True)
        if changed:
            try:
                rpc(adapter, "sync_push_library", {"p_profile_id": selected_profile_id(adapter), "p_items": list(remote.values())})
            except Exception:
                return _result(False, 0, attempted, [], [{"status": "failed", "reason": "nuvio_library_replace_failed"}], results, 0)

        after = build_index(adapter)
        confirmed = [key for key, verify_key in zip(pending_keys, verify_keys) if verify_key in after]
        unresolved.extend({"status": "failed", "reason": "nuvio_library_verification_failed", "item": id_minimal(item), "canonical_key": key} for item, key, verify_key in zip(pending_items, pending_keys, verify_keys) if verify_key not in after)
        ok = len(unresolved) == 0
        _info("write_done", op="add", ok=ok, attempted=attempted, confirmed=len(confirmed), unresolved=len(unresolved))
        return _result(ok, len(confirmed), attempted, confirmed, unresolved, results, 0)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    src = [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]
    with library_lock(adapter):
        current, remote = _pull_remote_by_key(adapter)
        unresolved: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []
        attempted = 0
        skipped = 0
        pending_keys: list[str] = []
        verify_keys: list[str] = []
        pending_items: list[dict[str, Any]] = []

        for item in src:
            key = canonical_item_key(item)
            verify_key = _verify_key_for_item(adapter, item)
            if not key or key == "unknown:":
                entry = {"status": "unresolved", "reason": "nuvio_id_missing", "item": id_minimal(item)}
                unresolved.append(entry)
                results.append(entry)
                continue
            if verify_key not in remote:
                skipped += 1
                results.append({"status": "skipped", "reason": "already_absent", "item": id_minimal(item), "canonical_key": key})
                continue
            attempted += 1
            remote.pop(verify_key, None)
            pending_keys.append(key)
            verify_keys.append(verify_key)
            pending_items.append(item)
            results.append({"status": "pending" if not dry_run else "dry_run", "item": id_minimal(item), "canonical_key": key})

        if dry_run:
            return _result(len(unresolved) == 0, attempted, attempted, [], unresolved, results, skipped, dry_run=True)
        if attempted:
            try:
                rpc(adapter, "sync_push_library", {"p_profile_id": selected_profile_id(adapter), "p_items": list(remote.values())})
            except Exception:
                return _result(False, 0, attempted, [], [{"status": "failed", "reason": "nuvio_library_replace_failed"}], results, skipped)

        after = build_index(adapter)
        confirmed: list[str] = []
        for item, key, verify_key in zip(pending_items, pending_keys, verify_keys):
            if key and verify_key not in after:
                confirmed.append(key)
            elif verify_key in current:
                unresolved.append({"status": "failed", "reason": "nuvio_library_verification_failed", "item": id_minimal(item), "canonical_key": key})
        ok = len(unresolved) == 0
        _info("write_done", op="remove", ok=ok, attempted=attempted, confirmed=len(confirmed), skipped=skipped, unresolved=len(unresolved))
        return _result(ok, len(confirmed), attempted, confirmed, unresolved, results, skipped)
