# api/trackerAPI.py
# CrossWatch - Local tracker browsing and editing API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from cw_platform.config_base import CONFIG, load_config
from cw_platform.history_events import history_epoch_from_item
from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.provider_instances import get_provider_block, list_instance_ids, normalize_instance_id, sanitize_instance_label
from cw_platform.tracker_store import (
    FEATURES,
    ChangeBatch,
    TrackerConflict,
    TrackerStore,
    TrackerUnreadable,
    tracker_context,
    tracker_roots,
)
from cw_platform.value_coercion import coerce_bool
from services import tracker_check, tracker_migration

router = APIRouter(prefix="/api/tracker", tags=["tracker"])

_DATE_FIELDS = ("watched_at", "rated_at", "progress_at", "collected_at", "listed_at", "added_at")
_EDIT_DATE = {"history": "watched_at", "ratings": "rated_at", "progress": "progress_at", "collection": "collected_at"}
_EDIT_IDS = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")
_IDENTITY = ("ids", "show_ids", "title", "year")
_IMDB = re.compile(r"^tt\d{5,10}$")
_NUMERIC = re.compile(r"^\d{1,12}$")
_MAX_DELETE = 1000


def _cfg() -> dict[str, Any]:
    try:
        cfg = load_config() or {}
    except Exception:
        cfg = {}
    return cfg if isinstance(cfg, dict) else {}


def _instances(cfg: Mapping[str, Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for raw in list_instance_ids(cfg, "crosswatch") or ["default"]:
        inst = normalize_instance_id(raw)
        if any(row["id"] == inst for row in out):
            continue
        block = get_provider_block(cfg, "crosswatch", inst) if inst != "default" else {}
        label = sanitize_instance_label((block or {}).get("label")) if inst != "default" else ""
        out.append({"id": inst, "label": label or ("Default" if inst == "default" else inst)})
    return out or [{"id": "default", "label": "Default"}]


def _map(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") and not isinstance(value, bool) else None
    except (TypeError, ValueError):
        return None


def _art(item: Mapping[str, Any]) -> dict[str, str] | None:
    typ = str(item.get("type") or "").lower()
    ids = _map(item.get("ids"))
    show_ids = _map(item.get("show_ids"))
    if typ == "movie":
        tmdb = _text(ids.get("tmdb"))
        return {"kind": "movie", "tmdb": tmdb} if tmdb else None
    tmdb = _text(show_ids.get("tmdb")) or (_text(ids.get("tmdb")) if typ == "show" else None)
    return {"kind": "tv", "tmdb": tmdb} if tmdb else None


def _iso_ts(seconds: Any) -> str | None:
    try:
        value = float(seconds)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def _check_row(entry: tuple[str, dict[str, Any], int] | None) -> dict[str, Any] | None:
    if entry is None:
        return None
    code, detail, checked_ns = entry
    return {
        "code": code,
        "kind": detail.get("kind"),
        "tmdb": detail.get("tmdb"),
        "title": detail.get("title"),
        "year": detail.get("year"),
        "season": detail.get("season"),
        "episode": detail.get("episode"),
        "source": detail.get("source"),
        "fetched_at": _iso_ts(detail.get("fetched_at")),
        "checked_at": _iso_ts(checked_ns / 1_000_000_000),
    }


def _tmdb_key(cfg: Mapping[str, Any]) -> str:
    tmdb = _map(cfg.get("tmdb"))
    meta = _map(cfg.get("metadata"))
    return str(tmdb.get("api_key") or meta.get("tmdb_api_key") or "").strip()


def _tmdb_provider() -> Any:
    from providers.metadata._meta_TMDB import TmdbProvider

    return TmdbProvider(lambda: _cfg(), lambda _: None)


def _row(key: str, item: Any, feature: str | None = None) -> dict[str, Any]:
    data = item if isinstance(item, Mapping) else {}
    typ = str(data.get("type") or "").lower() or None
    return {
        "key": key,
        "type": typ,
        "title": _text(data.get("title")),
        "year": _int(data.get("year")),
        "series_title": _text(data.get("series_title")),
        "series_year": _int(data.get("series_year")),
        "season": _int(data.get("season")),
        "episode": _int(data.get("episode")),
        "rating": data.get("rating") if isinstance(data.get("rating"), (int, float)) and not isinstance(data.get("rating"), bool) else None,
        "progress_percent": data.get("progress_percent") if isinstance(data.get("progress_percent"), (int, float)) else None,
        "date": next((str(data[field]) for field in _DATE_FIELDS if data.get(field)), None),
        "date_field": _EDIT_DATE.get(feature or ""),
        "ids": dict(_map(data.get("ids"))),
        "show_ids": dict(_map(data.get("show_ids"))),
        "art": _art(data),
    }


def _error(code: str, status: int, **extra: Any) -> JSONResponse:
    return JSONResponse({"ok": False, "error": code, **extra}, status_code=status)


def _unreadable() -> JSONResponse:
    return _error("tracker_unreadable", 503)


def _not_migrating() -> None:
    if tracker_migration.busy():
        raise HTTPException(status_code=503, detail="tracker_migrating")


_READY = [Depends(_not_migrating)]


def _sync_running() -> bool:
    try:
        from api import syncAPI

        return bool(syncAPI._is_sync_running())
    except Exception:
        return False


def _delete_effects(cfg: Mapping[str, Any], inst: str) -> dict[str, dict[str, list[dict[str, str]]]]:
    from cw_platform.orchestrator._pairs import _feature_list_for_pair
    from cw_platform.orchestrator._pairs_utils import resolve_flags

    out: dict[str, dict[str, list[dict[str, str]]]] = {feature: {"spreads": [], "returns": []} for feature in FEATURES}
    sync_cfg = _map(cfg.get("sync"))
    for pair in cfg.get("pairs") or []:
        if not isinstance(pair, Mapping) or not coerce_bool(pair.get("enabled", True), True):
            continue
        src, dst = str(pair.get("source") or "").upper().strip(), str(pair.get("target") or "").upper().strip()
        si, ti = normalize_instance_id(pair.get("source_instance")), normalize_instance_id(pair.get("target_instance"))
        cw_src = src == "CROSSWATCH" and si == inst
        cw_dst = dst == "CROSSWATCH" and ti == inst
        if cw_src == cw_dst:
            continue
        other, other_inst = (dst, ti) if cw_src else (src, si)
        target = {"provider": other, "instance": other_inst}
        two_way = "two" in str(pair.get("mode") or "").lower()
        fmap = _map(pair.get("features"))
        for feature in _feature_list_for_pair(pair):
            if feature not in out:
                continue
            flags = resolve_flags(fmap.get(feature), sync_cfg)
            if (cw_src or two_way) and flags["allow_removals"]:
                bucket = out[feature]["spreads"]
            elif (cw_dst or two_way) and flags["allow_adds"]:
                bucket = out[feature]["returns"]
            else:
                continue
            if target not in bucket:
                bucket.append(target)
    return out


def _iso(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("invalid_date")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid_date")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    if parsed.year < 1900 or parsed > datetime.now(timezone.utc) + timedelta(days=1):
        raise ValueError("invalid_date")
    return parsed.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _id_map(current: Any, changes: Any) -> dict[str, Any]:
    if not isinstance(changes, Mapping):
        raise ValueError("invalid_ids")
    out = dict(current) if isinstance(current, Mapping) else {}
    for name, raw in changes.items():
        key = str(name).strip().lower()
        if key not in _EDIT_IDS:
            raise ValueError("invalid_ids")
        value = str(raw if raw is not None else "").strip()
        if not value:
            out.pop(key, None)
            continue
        if key == "imdb":
            value = value.lower()
        if not (_IMDB if key == "imdb" else _NUMERIC).match(value):
            raise ValueError("invalid_ids")
        out[key] = value
    return out


def _edited(feature: str, item: Mapping[str, Any], changes: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(item)
    typ = str(item.get("type") or "").lower()
    child = typ in ("episode", "season")
    for field, value in changes.items():
        if field == "date":
            date_field = _EDIT_DATE.get(feature)
            if not date_field:
                raise ValueError("invalid_date")
            out[date_field] = _iso(value)
        elif field == "rating":
            rating = _int(value)
            if feature != "ratings" or rating is None or not 1 <= rating <= 10 or str(value).strip() != str(rating):
                raise ValueError("invalid_rating")
            out["rating"] = rating
        elif field == "progress_percent":
            try:
                pct = float(value)
            except (TypeError, ValueError):
                raise ValueError("invalid_progress")
            if feature != "progress" or not 0 <= pct <= 100:
                raise ValueError("invalid_progress")
            out["progress_percent"] = round(pct, 3)
            duration = _int(item.get("duration_ms"))
            if duration and duration > 0:
                out["progress_ms"] = int(duration * pct / 100)
        elif field == "ids":
            ids = _id_map(item.get("ids"), value)
            if not ids and not child:
                raise ValueError("invalid_ids")
            out["ids"] = ids
        elif field == "show_ids":
            if not child:
                raise ValueError("invalid_ids")
            show_ids = _id_map(item.get("show_ids"), value)
            if show_ids:
                out["show_ids"] = show_ids
            else:
                out.pop("show_ids", None)
        elif field == "title":
            title = str(value or "").strip()
            if not title or len(title) > 300:
                raise ValueError("invalid_title")
            out["series_title" if child else "title"] = title
        elif field == "year":
            year = _int(value)
            if value not in (None, "") and (year is None or not 1870 <= year <= 2100):
                raise ValueError("invalid_year")
            name = "series_year" if child else "year"
            if year is None:
                out.pop(name, None)
            else:
                out[name] = year
        else:
            raise ValueError("unknown_field")
    return out


def _item_key(feature: str, key: str, before: Mapping[str, Any], after: Mapping[str, Any]) -> str:
    identity = any(before.get(field) != after.get(field) for field in _IDENTITY)
    redated = feature == "history" and before.get("watched_at") != after.get("watched_at")
    if not identity and not redated:
        return key
    old_base, sep, suffix = key.partition("@")
    if feature == "history":
        base = canonical_key(id_minimal(after))
        if sep and suffix.isdigit():
            suffix = str(history_epoch_from_item(after) or suffix)
    else:
        base = canonical_key(after)
    if not base or base.startswith("unknown:") or not identity:
        base = old_base
    return f"{base}{sep}{suffix}" if sep else base


def _store(provider_instance: Any) -> tuple[dict[str, Any], str, TrackerStore]:
    cfg = _cfg()
    inst, root = tracker_context(cfg, CONFIG, provider_instance)
    return cfg, inst, TrackerStore(root)


def _pair_scoped() -> bool:
    from providers.sync.crosswatch._common import pair_scoped

    return pair_scoped()


def _write_guard(*features: Any) -> JSONResponse | None:
    if any(feature not in FEATURES for feature in features):
        return _error("unknown_feature", 400)
    if _pair_scoped():
        return _error("pair_scoped", 409)
    if _sync_running():
        return _error("sync_running", 409)
    return None


def _changed_fields(before: Any, after: Any) -> list[str]:
    a = before if isinstance(before, Mapping) else {}
    b = after if isinstance(after, Mapping) else {}
    return sorted(field for field in set(a) | set(b) if a.get(field) != b.get(field))


def _change(batch: ChangeBatch) -> dict[str, Any]:
    items = []
    for old_key, new_key, before, after in batch.rows:
        shown = after if isinstance(after, Mapping) else before
        items.append({
            "key": new_key or old_key,
            "old_key": old_key,
            "row": _row(new_key or old_key, shown, batch.feature),
            "fields": _changed_fields(before, after) if before is not None and after is not None else [],
            "before": _row(old_key, before, batch.feature) if isinstance(before, Mapping) else None,
            "after": _row(new_key or old_key, after, batch.feature) if isinstance(after, Mapping) else None,
        })
    return {
        "batch": batch.batch,
        "feature": batch.feature,
        "action": batch.action,
        "count": batch.count,
        "at": datetime.fromtimestamp(batch.created_ns / 1_000_000_000, timezone.utc).isoformat().replace("+00:00", "Z"),
        "undone": batch.undone_ns is not None,
        "undoable": batch.undone_ns is None and batch.action != "undo",
        "undo_of": batch.undo_of,
        "items": items,
    }


@router.get("/summary", dependencies=_READY)
def tracker_summary(provider_instance: str | None = Query("default")) -> Any:
    cfg, inst, store = _store(provider_instance)
    try:
        counts = store.counts()
        attention = store.attention_counts()
    except TrackerUnreadable:
        return _unreadable()
    try:
        effects = _delete_effects(cfg, inst)
    except Exception:
        effects = {feature: {"spreads": [], "returns": []} for feature in FEATURES}
    return {
        "ok": True,
        "provider_instance": inst,
        "instances": _instances(cfg),
        "features": list(FEATURES),
        "counts": counts,
        "attention": attention,
        "check": tracker_check.status(store),
        "tmdb_key": bool(_tmdb_key(cfg)),
        "delete_effects": effects,
    }


@router.get("/items", dependencies=_READY)
def tracker_items(
    feature: str = Query("history"),
    provider_instance: str | None = Query("default"),
    q: str = Query("", max_length=200),
    media: str = Query(""),
    sort: str = Query("recent"),
    offset: int = Query(0, ge=0),
    limit: int = Query(60, ge=1, le=200),
    attention: bool = Query(False),
) -> Any:
    if feature not in FEATURES:
        return _error("unknown_feature", 400)
    _, inst, store = _store(provider_instance)
    try:
        total, rows = store.query(
            feature,
            text=q,
            media=media if media in ("movie", "show") else "",
            sort=sort if sort in ("recent", "oldest", "title") else "recent",
            offset=offset,
            limit=limit,
            attention=attention,
        )
        checks = store.checks_for(feature, [key for key, _ in rows])
    except TrackerUnreadable:
        return _unreadable()
    items = []
    for key, item in rows:
        row = _row(key, item, feature)
        row["check"] = _check_row(checks.get(key))
        items.append(row)
    return {
        "ok": True,
        "provider_instance": inst,
        "feature": feature,
        "total": total,
        "offset": offset,
        "items": items,
    }


@router.post("/edit", dependencies=_READY)
def tracker_edit(payload: dict[str, Any] = Body(...)) -> Any:
    feature = str(payload.get("feature") or "")
    key = str(payload.get("key") or "")
    changes = payload.get("changes")
    blocked = _write_guard(feature)
    if blocked is not None:
        return blocked
    if not key or not isinstance(changes, Mapping) or not changes:
        return _error("no_changes", 400)
    _, inst, store = _store(payload.get("provider_instance"))
    try:
        current = store.item(feature, key)
        if not isinstance(current, Mapping):
            return _error("not_found", 404)
        try:
            updated = _edited(feature, current, changes)
        except ValueError as exc:
            return _error(str(exc), 400)
        if updated == dict(current):
            return _error("no_changes", 400)
        new_key = _item_key(feature, key, current, updated)
        batch = store.change_items(feature, [(key, new_key, updated)], action="edit", expected={key: dict(current)})
    except TrackerUnreadable:
        return _unreadable()
    except KeyError:
        return _error("not_found", 404)
    except TrackerConflict as exc:
        return _error(exc.code, 409, key=exc.key)
    return {"ok": True, "provider_instance": inst, "batch": batch, "key": new_key, "item": _row(new_key, updated, feature)}


@router.post("/delete", dependencies=_READY)
def tracker_delete(payload: dict[str, Any] = Body(...)) -> Any:
    feature = str(payload.get("feature") or "")
    blocked = _write_guard(feature)
    if blocked is not None:
        return blocked
    raw = payload.get("keys")
    keys = list(dict.fromkeys(str(k) for k in raw if str(k or "").strip())) if isinstance(raw, list) else []
    if not keys:
        return _error("no_changes", 400)
    if len(keys) > _MAX_DELETE:
        return _error("too_many", 400, limit=_MAX_DELETE)
    _, inst, store = _store(payload.get("provider_instance"))
    try:
        batch = store.change_items(feature, [(key, None, None) for key in keys], action="delete")
    except TrackerUnreadable:
        return _unreadable()
    except KeyError as exc:
        return _error("not_found", 404, key=str(exc.args[0]) if exc.args else None)
    return {"ok": True, "provider_instance": inst, "batch": batch, "count": len(keys)}


@router.post("/undo", dependencies=_READY)
def tracker_undo(payload: dict[str, Any] = Body(...)) -> Any:
    blocked = _write_guard()
    if blocked is not None:
        return blocked
    batch = str(payload.get("batch") or "").strip()
    if not batch:
        return _error("not_found", 404)
    _, inst, store = _store(payload.get("provider_instance"))
    try:
        undo_batch = store.undo(batch)
    except TrackerUnreadable:
        return _unreadable()
    except KeyError:
        return _error("not_found", 404)
    except TrackerConflict as exc:
        return _error(exc.code, 409, key=exc.key)
    return {"ok": True, "provider_instance": inst, "batch": undo_batch, "undone": batch}


@router.get("/changes", dependencies=_READY)
def tracker_changes(
    provider_instance: str | None = Query("default"),
    offset: int = Query(0, ge=0),
    limit: int = Query(30, ge=1, le=100),
) -> Any:
    _, inst, store = _store(provider_instance)
    try:
        total, batches = store.change_log(offset=offset, limit=limit)
    except TrackerUnreadable:
        return _unreadable()
    return {"ok": True, "provider_instance": inst, "total": total, "offset": offset, "changes": [_change(b) for b in batches]}


@router.get("/check", dependencies=_READY)
def tracker_check_status(provider_instance: str | None = Query("default")) -> Any:
    _, inst, store = _store(provider_instance)
    return {"ok": True, "provider_instance": inst, "job": tracker_check.status(store)}


@router.post("/check", dependencies=_READY)
def tracker_check_start(payload: dict[str, Any] = Body(default={})) -> Any:
    cfg, inst, store = _store(payload.get("provider_instance"))
    if not _tmdb_key(cfg):
        return _error("tmdb_key_missing", 400)
    job = tracker_check.start(store, _tmdb_provider(), instance=inst)
    if job.get("error"):
        return _error(str(job["error"]), 409)
    return {"ok": True, "provider_instance": inst, "job": job}


@router.post("/check/cancel", dependencies=_READY)
def tracker_check_cancel(payload: dict[str, Any] = Body(default={})) -> Any:
    _, inst, store = _store(payload.get("provider_instance"))
    return {"ok": True, "provider_instance": inst, "cancelled": tracker_check.cancel(store)}


@router.get("/check/current")
def tracker_check_current() -> Any:
    cfg = _cfg()
    labels = {row["id"]: row["label"] for row in _instances(cfg)}
    migration = tracker_migration.status()
    if migration is not None:
        migration = {**migration, "label": labels.get(str(migration.get("instance") or ""), migration.get("instance"))}
    if tracker_migration.busy():
        return {"ok": True, "job": None, "migration": migration}
    latest: dict[str, Any] | None = None
    for inst, root in tracker_roots(cfg, CONFIG):
        try:
            job = tracker_check.status(TrackerStore(root))
        except Exception:
            continue
        if job.get("state") in (None, "idle"):
            continue
        job = {**job, "instance": inst, "label": labels.get(inst, inst)}
        if job.get("state") == "running":
            return {"ok": True, "job": job, "migration": migration}
        if latest is None or float(job.get("finished_at") or 0) > float(latest.get("finished_at") or 0):
            latest = job
    return {"ok": True, "job": latest, "migration": migration}
