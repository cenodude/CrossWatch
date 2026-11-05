# api/trackerAPI.py
# CrossWatch - Local tracker browsing and editing API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
import re
import uuid
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from cw_platform.access_policy import request_user, user_can_access_instance
from cw_platform.config_base import CONFIG, load_config
from cw_platform.history_events import history_epoch_from_item
from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.provider_instances import get_provider_block, list_instance_ids, normalize_instance_id, sanitize_instance_label
from cw_platform.tracker_store import (
    ATTENTION_CODES,
    FEATURES,
    NOTE_STATUSES,
    NOTES,
    ChangeBatch,
    TrackerConflict,
    TrackerStore,
    TrackerUnreadable,
    tracker_context,
    tracker_roots,
)
from cw_platform.value_coercion import coerce_bool
from services import tracker_check, tracker_import, tracker_migration

router = APIRouter(prefix="/api/tracker", tags=["tracker"])

_DATE_FIELDS = ("watched_at", "rated_at", "progress_at", "collected_at", "listed_at", "added_at")
_EDIT_DATE = {"history": "watched_at", "ratings": "rated_at", "progress": "progress_at", "collection": "collected_at"}
_EDIT_IDS = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")
_IDENTITY = ("ids", "show_ids", "title", "year")
_IMDB = re.compile(r"^tt\d{5,10}$")
_NUMERIC = re.compile(r"^\d{1,12}$")
_MAX_DELETE = 1000
_NOTE_KEY = re.compile(r"^(show|movie):(tmdb|imdb|title|key):.{1,500}$")
_FIELD_ID = re.compile(r"^[a-z0-9]{1,16}$")
_FIELD_TYPES = ("text", "number", "date", "bool", "choice")
_MAX_FIELDS = 20
_MAX_TAGS = 30


def _cfg() -> dict[str, Any]:
    try:
        cfg = load_config() or {}
    except Exception:
        cfg = {}
    return cfg if isinstance(cfg, dict) else {}


def _instances(cfg: Mapping[str, Any], user: Mapping[str, Any] | None = None) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for raw in list_instance_ids(cfg, "crosswatch") or ["default"]:
        inst = normalize_instance_id(raw)
        if any(row["id"] == inst for row in out) or not user_can_access_instance(cfg, user, "CROSSWATCH", inst):
            continue
        block = get_provider_block(cfg, "crosswatch", inst) if inst != "default" else {}
        label = sanitize_instance_label((block or {}).get("label")) if inst != "default" else ""
        out.append({"id": inst, "label": label or ("Default" if inst == "default" else inst)})
    return out if out or user else [{"id": "default", "label": "Default"}]


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

    out: dict[str, dict[str, list[dict[str, str]]]] = {feature: {"spreads": [], "returns": [], "adds": []} for feature in FEATURES}
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
            if (cw_src or two_way) and flags["allow_adds"] and target not in out[feature]["adds"]:
                out[feature]["adds"].append(target)
            if (cw_src or two_way) and flags["allow_removals"]:
                bucket = out[feature]["spreads"]
            elif (cw_dst or two_way) and flags["allow_adds"]:
                bucket = out[feature]["returns"]
            else:
                continue
            if target not in bucket:
                bucket.append(target)
    return out


def _rating(value: Any) -> int | float:
    if isinstance(value, bool):
        raise ValueError("invalid_rating")
    try:
        rating = float(value)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("invalid_rating")
    if not 0.5 <= rating <= 10 or not (rating * 2).is_integer():
        raise ValueError("invalid_rating")
    return int(rating) if rating.is_integer() else rating


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
            if feature != "ratings":
                raise ValueError("invalid_rating")
            out["rating"] = _rating(value)
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


def _user(request: Request | None) -> Mapping[str, Any] | None:
    user = request_user(request) if request is not None else None
    return None if not user or bool(user.get("is_admin")) else user


def _store(provider_instance: Any, request: Request | None = None, *, fallback: bool = False) -> tuple[dict[str, Any], str, TrackerStore]:
    cfg = _cfg()
    user = _user(request)
    inst, root = tracker_context(cfg, CONFIG, provider_instance)
    if inst != normalize_instance_id(provider_instance) and not fallback:
        raise HTTPException(status_code=404, detail="unknown_instance")
    if not user_can_access_instance(cfg, user, "CROSSWATCH", inst):
        allowed = _instances(cfg, user)
        if not fallback or not allowed:
            raise HTTPException(status_code=403, detail="profile_scope_denied")
        inst, root = tracker_context(cfg, CONFIG, allowed[0]["id"])
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


def _title_key(ident: Mapping[str, Any]) -> str:
    if ident.get("show"):
        return "show:" + (f"tmdb:{ident['tmdb']}" if ident.get("tmdb") else f"title:{str(ident.get('title') or '').lower()}")
    for name in ("tmdb", "imdb"):
        if ident.get(name):
            return f"movie:{name}:{ident[name]}"
    return f"movie:key:{ident.get('base') or ''}"


def _note_identity(key: str) -> dict[str, Any]:
    kind, _, rest = key.partition(":")
    name, _, value = rest.partition(":")
    show = kind == "show"
    return {
        "show": show,
        "tmdb": value if name == "tmdb" else "",
        "imdb": value if name == "imdb" and not show else "",
        "title": value if name == "title" else "",
        "base": value if name == "key" and not show else "",
    }


def _note_row(key: str, note: Any) -> dict[str, Any]:
    data = note if isinstance(note, Mapping) else {}
    tmdb = _text(data.get("tmdb"))
    show = data.get("type") == "show"
    return {
        "key": key,
        "type": "show" if show else "movie",
        "title": _text(data.get("title")),
        "year": _int(data.get("year")),
        "ids": {"tmdb": tmdb} if tmdb else {},
        "art": {"kind": "tv" if show else "movie", "tmdb": tmdb} if tmdb else None,
    }


def _note_out(key: str, note: Mapping[str, Any] | None, updated: int | None = None) -> dict[str, Any] | None:
    if note is None:
        return None
    out = {**_note_row(key, note), **{k: note.get(k) for k in ("tmdb", "status", "started", "finished", "notes", "tags", "fields")}}
    if updated:
        out["updated"] = datetime.fromtimestamp(updated / 1_000_000_000, timezone.utc).isoformat().replace("+00:00", "Z")
    return out


def _day(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        raise ValueError("invalid_note_date")


def _field_value(spec: Mapping[str, Any], raw: Any) -> Any:
    typ = spec.get("type")
    if raw is None or raw == "":
        return None
    if typ == "bool":
        return coerce_bool(raw)
    if typ == "date":
        return _day(raw)
    if typ == "number":
        try:
            value = float(raw)
        except (TypeError, ValueError):
            raise ValueError("invalid_field_value")
        if not math.isfinite(value):
            raise ValueError("invalid_field_value")
        return int(value) if value.is_integer() else value
    text = " ".join(str(raw).split()) if typ == "choice" else str(raw).strip()
    if len(text) > 500 or (typ == "choice" and text not in (spec.get("choices") or [])):
        raise ValueError("invalid_field_value")
    return text or None


def _clean_note(payload: Mapping[str, Any], fields: list[dict[str, Any]]) -> dict[str, Any] | None:
    status = str(payload.get("status") or "")
    if status and status not in NOTE_STATUSES:
        raise ValueError("invalid_status")
    started, finished = _day(payload.get("started")), _day(payload.get("finished"))
    if started and finished and finished < started:
        raise ValueError("finished_before_started")
    body = str(payload.get("notes") or "").strip()
    if len(body) > 10_000:
        raise ValueError("notes_too_long")
    raw_tags = payload.get("tags") or []
    if not isinstance(raw_tags, list):
        raise ValueError("invalid_tags")
    tags: dict[str, str] = {}
    for raw in raw_tags:
        tag = " ".join(str(raw or "").split())
        if not tag:
            continue
        if len(tag) > 40 or "," in tag:
            raise ValueError("invalid_tags")
        tags.setdefault(tag.lower(), tag)
    if len(tags) > _MAX_TAGS:
        raise ValueError("too_many_tags")
    given = _map(payload.get("fields"))
    values = {}
    for spec in fields:
        value = _field_value(spec, given.get(spec["id"]))
        if value is not None:
            values[spec["id"]] = value
    if not (status or started or finished or body or tags or values):
        return None
    return {"status": status or None, "started": started, "finished": finished, "notes": body or None, "tags": list(tags.values()), "fields": values}


def _clean_fields(raw: Any, current: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or len(raw) > _MAX_FIELDS:
        raise ValueError("too_many_fields")
    known = {f.get("id"): f.get("type") for f in current}
    out: list[dict[str, Any]] = []
    for row in raw:
        if not isinstance(row, Mapping):
            raise ValueError("invalid_field")
        name = " ".join(str(row.get("name") or "").split())
        typ = str(row.get("type") or "")
        if not name or len(name) > 40 or typ not in _FIELD_TYPES or any(f["name"].lower() == name.lower() for f in out):
            raise ValueError("invalid_field")
        fid = str(row.get("id") or "")
        if not _FIELD_ID.match(fid) or known.get(fid) != typ or any(f["id"] == fid for f in out):
            fid = uuid.uuid4().hex[:8]
        spec: dict[str, Any] = {"id": fid, "name": name, "type": typ}
        if typ == "choice":
            raw_choices = row.get("choices")
            if not isinstance(raw_choices, list):
                raw_choices = []
            choices = list(dict.fromkeys(" ".join(str(c).split()) for c in raw_choices if str(c).strip()))
            if not choices or len(choices) > 30 or any(len(c) > 40 for c in choices):
                raise ValueError("invalid_field")
            spec["choices"] = choices
        out.append(spec)
    return out


def _note_follow(key: str, before: Mapping[str, Any], new_key: str, after: Mapping[str, Any]) -> dict[str, Any] | None:
    old_ident, new_ident = _identity_of(before, key), _identity_of(after, new_key)
    old, new = _title_key(old_ident), _title_key(new_ident)
    if old == new:
        return None
    child = str(after.get("type") or "").lower() in ("episode", "season")
    return {
        "old": old,
        "new": new,
        "ident": {k: v for k, v in old_ident.items() if k in ("show", "tmdb", "imdb", "title", "base")},
        "meta": {
            "type": "show" if new_ident["show"] else "movie",
            "title": str((after.get("series_title") if child else after.get("title")) or ""),
            "year": _int(after.get("series_year") if child else after.get("year")),
            "tmdb": new_ident.get("tmdb") or None,
        },
    }


def _change(batch: ChangeBatch) -> dict[str, Any]:
    items = []
    notes = batch.feature == NOTES
    for old_key, new_key, before, after in batch.rows:
        shown = after if isinstance(after, Mapping) else before
        row = (lambda k, d: _note_row(k, d)) if notes else (lambda k, d: _row(k, d, batch.feature))
        items.append({
            "key": new_key or old_key,
            "old_key": old_key,
            "row": row(new_key or old_key, shown),
            "fields": _changed_fields(before, after) if before is not None and after is not None else [],
            "before": row(old_key, before) if isinstance(before, Mapping) else None,
            "after": row(new_key or old_key, after) if isinstance(after, Mapping) else None,
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
def tracker_summary(request: Request, provider_instance: str | None = Query("default")) -> Any:
    cfg, inst, store = _store(provider_instance, request, fallback=True)
    with store.session():
        try:
            counts = store.counts()
            attention = store.attention_counts()
            health = store.health_counts()
            notes = store.note_summary()
            fields = store.note_fields()
        except TrackerUnreadable:
            return _unreadable()
        check, imported = tracker_check.status(store), _import_status(store)
    try:
        effects = _delete_effects(cfg, inst)
    except Exception:
        effects = {feature: {"spreads": [], "returns": [], "adds": []} for feature in FEATURES}
    return {
        "ok": True,
        "provider_instance": inst,
        "instances": _instances(cfg, _user(request)),
        "features": list(FEATURES),
        "counts": counts,
        "attention": attention,
        "health": health,
        "notes": notes,
        "note_fields": fields,
        "note_statuses": list(NOTE_STATUSES),
        "check": check,
        "import": imported,
        "tmdb_key": bool(_tmdb_key(cfg)),
        "delete_effects": effects,
    }


@router.get("/items", dependencies=_READY)
def tracker_items(
    request: Request,
    feature: str = Query("history"),
    provider_instance: str | None = Query("default"),
    q: str = Query("", max_length=200),
    media: str = Query(""),
    sort: str = Query("recent"),
    offset: int = Query(0, ge=0),
    limit: int = Query(60, ge=1, le=200),
    attention: bool = Query(False),
    health: str = Query(""),
    month: str = Query("", pattern=r"^(\d{4}-(0[1-9]|1[0-2]))?$"),
) -> Any:
    if feature not in FEATURES:
        return _error("unknown_feature", 400)
    _, inst, store = _store(provider_instance, request)
    try:
        with store.session():
            total, rows = store.query(
                feature,
                text=q,
                media=media if media in ("movie", "show") else "",
                sort=sort if sort in ("recent", "oldest", "title") else "recent",
                offset=offset,
                limit=limit,
                attention=attention,
                health=health if health in ("ok", "attention", "unchecked") else "",
                until=_month_end(month),
            )
            checks = store.checks_for(feature, [key for key, _ in rows])
            ratings = {}
            if feature != "ratings":
                for key, item in rows:
                    matches = store.related(**_identity_of(item, key), feature="ratings")
                    candidates = [data for _, _, data in matches if data.get("type") in ("movie", "show", "anime") or (
                        data.get("type") == item.get("type") and data.get("season") == item.get("season") and data.get("episode") == item.get("episode")
                    )]
                    candidates.sort(key=lambda data: (data.get("type") == item.get("type"), str(data.get("rated_at") or "")), reverse=True)
                    ratings[key] = candidates[0].get("rating") if candidates else None
    except TrackerUnreadable:
        return _unreadable()
    items = []
    for key, item in rows:
        row = _row(key, item, feature)
        row["user_rating"] = row["rating"] if feature == "ratings" else ratings.get(key)
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


def _month_end(month: str) -> str:
    if not month:
        return ""
    year, mon = (int(part) for part in month.split("-"))
    return f"{year + mon // 12:04d}-{mon % 12 + 1:02d}-01"


@router.get("/months", dependencies=_READY)
def tracker_months(
    request: Request,
    feature: str = Query("history"),
    provider_instance: str | None = Query("default"),
    q: str = Query("", max_length=200),
    media: str = Query(""),
) -> Any:
    if feature not in FEATURES:
        return _error("unknown_feature", 400)
    _, inst, store = _store(provider_instance, request)
    try:
        months = store.months(feature, text=q, media=media if media in ("movie", "show") else "")
    except TrackerUnreadable:
        return _unreadable()
    return {"ok": True, "provider_instance": inst, "feature": feature, "months": months}


@router.post("/edit", dependencies=_READY)
def tracker_edit(request: Request, payload: dict[str, Any] = Body(...)) -> Any:
    feature = str(payload.get("feature") or "")
    key = str(payload.get("key") or "")
    changes = payload.get("changes")
    blocked = _write_guard(feature)
    if blocked is not None:
        return blocked
    if not key or not isinstance(changes, Mapping) or not changes:
        return _error("no_changes", 400)
    _, inst, store = _store(payload.get("provider_instance"), request)
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
        batch = store.change_items(
            feature, [(key, new_key, updated)], action="edit", expected={key: dict(current)},
            follow=_note_follow(key, current, new_key, updated),
        )
    except TrackerUnreadable:
        return _unreadable()
    except KeyError:
        return _error("not_found", 404)
    except TrackerConflict as exc:
        return _error(exc.code, 409, key=exc.key)
    return {"ok": True, "provider_instance": inst, "batch": batch, "key": new_key, "item": _row(new_key, updated, feature)}


_IDENTITY_FIELDS = ("type", "title", "year", "season", "episode", "series_title", "series_year", "ids", "show_ids", "part")


@router.post("/replace", dependencies=_READY)
def tracker_replace(request: Request, payload: dict[str, Any] = Body(...)) -> Any:
    feature = str(payload.get("feature") or "")
    key = str(payload.get("key") or "")
    corrected = payload.get("item")
    blocked = _write_guard(feature)
    if blocked is not None:
        return blocked
    if not key or not isinstance(corrected, Mapping) or not isinstance(corrected.get("ids"), Mapping):
        return _error("invalid_ids", 400)
    _, inst, store = _store(payload.get("provider_instance"), request)
    try:
        current = store.item(feature, key)
        if not isinstance(current, Mapping):
            return _error("not_found", 404)
        updated = {k: v for k, v in current.items() if k not in _IDENTITY_FIELDS}
        updated.update({k: corrected[k] for k in _IDENTITY_FIELDS if corrected.get(k) not in (None, "", {})})
        if updated == dict(current):
            return _error("no_changes", 400)
        new_key = _item_key(feature, key, current, updated)
        batch = store.change_items(
            feature, [(key, new_key, updated)], action="edit", expected={key: dict(current)},
            follow=_note_follow(key, current, new_key, updated),
        )
    except TrackerUnreadable:
        return _unreadable()
    except KeyError:
        return _error("not_found", 404)
    except TrackerConflict as exc:
        return _error(exc.code, 409, key=exc.key)
    return {"ok": True, "provider_instance": inst, "batch": batch, "key": new_key, "item": _row(new_key, updated, feature)}


_CHILD_TYPES = ("episode", "season")


def _retitled(data: Mapping[str, Any], item: Mapping[str, Any]) -> dict[str, Any]:
    typ = str(data.get("type") or "").lower()
    target = str(item.get("type") or "").lower()
    from_child = target in _CHILD_TYPES
    show_like = from_child or target in ("show", "anime")
    ids = {k: v for k, v in _map(item.get("show_ids") if from_child else item.get("ids")).items() if v not in (None, "")}
    title = _text(item.get("series_title") if from_child else item.get("title"))
    year = _int(item.get("series_year") if from_child else item.get("year"))
    if typ not in _CHILD_TYPES and typ not in ("show", "anime"):
        if show_like:
            return dict(data)
        out = {k: v for k, v in data.items() if k not in _IDENTITY_FIELDS}
        out.update({k: item[k] for k in _IDENTITY_FIELDS if item.get(k) not in (None, "", {})})
        return out
    if not show_like:
        return dict(data)
    out = dict(data)
    child = typ in _CHILD_TYPES
    if ids:
        out["show_ids" if child else "ids"] = ids
    if title:
        out["series_title" if child else "title"] = title
    if year is not None:
        out["series_year" if child else "year"] = year
    return out


@router.post("/edit-title", dependencies=_READY)
def tracker_edit_title(request: Request, payload: dict[str, Any] = Body(...)) -> Any:
    blocked = _write_guard()
    if blocked is not None:
        return blocked
    changes = payload.get("changes")
    item = _map(payload.get("item"))
    if isinstance(changes, Mapping):
        if not changes:
            return _error("no_changes", 400)
        if set(changes) - {"title", "year", "ids"}:
            return _error("unknown_field", 400)
    elif not isinstance(item.get("ids") or item.get("show_ids"), Mapping):
        return _error("invalid_ids", 400)
    show = str(payload.get("show") or "")
    feature = str(payload.get("feature") or "history")
    key = str(payload.get("key") or "")
    if not show and (feature not in FEATURES or not key):
        return _error("not_found", 404)
    _, inst, store = _store(payload.get("provider_instance"), request)
    try:
        with store.mutation():
            if show:
                ident = _show_identity(show)
            else:
                found = store.item(feature, key)
                if not isinstance(found, Mapping):
                    return _error("not_found", 404)
                ident = _identity_of(found, key)
            rows: list[tuple[str, str, str, dict[str, Any], Mapping[str, Any]]] = []
            for name, item_key, data in store.related(**ident, limit=None):
                if isinstance(changes, Mapping):
                    per = dict(changes)
                    if "ids" in per and str(data.get("type") or "").lower() in _CHILD_TYPES:
                        per["show_ids"] = per.pop("ids")
                    updated = _edited(name, data, per)
                else:
                    updated = _retitled(data, item)
                if updated != data:
                    rows.append((name, item_key, _item_key(name, item_key, data, updated), updated, data))
            if not rows:
                return _error("no_changes", 400)
            name, k, nk, u, before = rows[0]
            batch = store.apply(
                [(name, k, nk, u) for name, k, nk, u, _ in rows], action="edit",
                expected={(name, k): data for name, k, _, _, data in rows}, follow=_note_follow(k, before, nk, u),
            )
    except ValueError as exc:
        return _error(str(exc), 400)
    except TrackerUnreadable:
        return _unreadable()
    except KeyError:
        return _error("not_found", 404)
    except TrackerConflict as exc:
        return _error(exc.code, 409, key=exc.key)
    anchor = next((r for r in rows if r[0] == feature and r[1] == key), rows[0])
    moved = _identity_of(anchor[3], anchor[2])
    return {
        "ok": True, "provider_instance": inst, "batch": batch, "count": len(rows),
        "feature": anchor[0], "key": anchor[2],
        "show_key": (f"tmdb:{moved['tmdb']}" if moved.get("tmdb") else f"title:{str(moved.get('title') or '').lower()}") if moved["show"] else None,
    }


@router.post("/delete", dependencies=_READY)
def tracker_delete(request: Request, payload: dict[str, Any] = Body(...)) -> Any:
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
    _, inst, store = _store(payload.get("provider_instance"), request)
    try:
        batch = store.change_items(feature, [(key, None, None) for key in keys], action="delete")
    except TrackerUnreadable:
        return _unreadable()
    except KeyError as exc:
        return _error("not_found", 404, key=str(exc.args[0]) if exc.args else None)
    return {"ok": True, "provider_instance": inst, "batch": batch, "count": len(keys)}


@router.post("/undo", dependencies=_READY)
def tracker_undo(request: Request, payload: dict[str, Any] = Body(...)) -> Any:
    batch = str(payload.get("batch") or "").strip()
    if not batch:
        return _error("not_found", 404)
    _, inst, store = _store(payload.get("provider_instance"), request)
    blocked = _error("pair_scoped", 409) if _pair_scoped() else None
    if blocked is None and store.batch_features(batch) != {NOTES}:
        blocked = _write_guard()
    if blocked is not None:
        return blocked
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
    request: Request,
    provider_instance: str | None = Query("default"),
    offset: int = Query(0, ge=0),
    limit: int = Query(30, ge=1, le=100),
    kind: str = Query("", pattern=r"^(|logged|edited|deleted|notes|undone)$"),
    q: str = Query("", max_length=200),
) -> Any:
    _, inst, store = _store(provider_instance, request)
    try:
        with store.session():
            total, batches = store.change_log(offset=offset, limit=limit, kind=kind, text=q)
            every = store.change_count() if kind or q.strip() else total
    except TrackerUnreadable:
        return _unreadable()
    return {
        "ok": True, "provider_instance": inst, "total": total, "all": every, "offset": offset,
        "changes": [_change(b) for b in batches],
    }


@router.get("/check", dependencies=_READY)
def tracker_check_status(request: Request, provider_instance: str | None = Query("default")) -> Any:
    _, inst, store = _store(provider_instance, request)
    return {"ok": True, "provider_instance": inst, "job": tracker_check.status(store)}


@router.post("/check", dependencies=_READY)
def tracker_check_start(request: Request, payload: dict[str, Any] = Body(default={})) -> Any:
    cfg, inst, store = _store(payload.get("provider_instance"), request)
    if not _tmdb_key(cfg):
        return _error("tmdb_key_missing", 400)
    job = tracker_check.start(store, _tmdb_provider(), instance=inst)
    if job.get("error"):
        return _error(str(job["error"]), 409)
    return {"ok": True, "provider_instance": inst, "job": job}


@router.post("/check/cancel", dependencies=_READY)
def tracker_check_cancel(request: Request, payload: dict[str, Any] = Body(default={})) -> Any:
    _, inst, store = _store(payload.get("provider_instance"), request)
    return {"ok": True, "provider_instance": inst, "cancelled": tracker_check.cancel(store)}


@router.get("/check/current")
def tracker_check_current(request: Request) -> Any:
    cfg = _cfg()
    user = _user(request)
    labels = {row["id"]: row["label"] for row in _instances(cfg, user)}
    migration = tracker_migration.status()
    if migration is not None:
        migration = {**migration, "label": labels.get(str(migration.get("instance") or ""), migration.get("instance"))}
    if tracker_migration.busy():
        return {"ok": True, "job": None, "migration": migration, "import": None}
    latest: dict[str, Any] | None = None
    for inst, root in tracker_roots(cfg, CONFIG):
        if inst not in labels:
            continue
        try:
            job = tracker_check.status(TrackerStore(root))
        except Exception:
            continue
        if job.get("state") in (None, "idle"):
            continue
        job = {**job, "instance": inst, "label": labels.get(inst, inst)}
        if job.get("state") == "running":
            latest = job
            break
        if latest is None or float(job.get("finished_at") or 0) > float(latest.get("finished_at") or 0):
            latest = job
    return {"ok": True, "job": latest, "migration": migration if user is None else None, "import": _latest_import(cfg, labels)}


def _import_run(job: Mapping[str, Any]) -> Mapping[str, Any] | None:
    from api import syncAPI

    return syncAPI.import_run_snapshot(str(job.get("run_id") or ""))


def _import_status(store: TrackerStore) -> dict[str, Any]:
    job = tracker_import.status(store)
    return tracker_import.status(store, _import_run(job)) if job.get("state") == "running" else job


def _latest_import(cfg: Mapping[str, Any], labels: Mapping[str, str]) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for inst, root in tracker_roots(cfg, CONFIG):
        if inst not in labels:
            continue
        try:
            job = _import_status(TrackerStore(root))
        except Exception:
            continue
        if job.get("state") in (None, "idle"):
            continue
        job = {**job, "instance": inst, "label": labels.get(inst, inst)}
        if job.get("state") == "running":
            return job
        if latest is None or float(job.get("finished_at") or 0) > float(latest.get("finished_at") or 0):
            latest = job
    return latest


@router.get("/import/sources", dependencies=_READY)
def tracker_import_sources(request: Request) -> Any:
    cfg = _cfg()
    user = _user(request)
    sources = []
    for row in tracker_import.sources(cfg):
        instances = [i for i in row["instances"] if user_can_access_instance(cfg, user, row["id"], i["id"])]
        if instances:
            sources.append({**row, "instances": instances})
    return {"ok": True, "sources": sources, "instances": _instances(cfg, user), "busy": _sync_running(), "can_create": user is None}


@router.get("/import", dependencies=_READY)
def tracker_import_status(request: Request, provider_instance: str | None = Query("default")) -> Any:
    _, inst, store = _store(provider_instance, request)
    return {"ok": True, "provider_instance": inst, "job": _import_status(store)}


@router.post("/import", dependencies=_READY)
def tracker_import_start(request: Request, payload: dict[str, Any] = Body(default={})) -> Any:
    blocked = _write_guard()
    if blocked is not None:
        return blocked
    cfg, inst, store = _store(payload.get("provider_instance"), request)
    if inst != normalize_instance_id(payload.get("provider_instance")):
        return _error("unknown_instance", 400)
    if not user_can_access_instance(cfg, _user(request), payload.get("source"), payload.get("source_instance")):
        return _error("profile_scope_denied", 403)
    features = payload.get("features")
    try:
        from api import syncAPI

        job = tracker_import.start(
            cfg, store, source=payload.get("source"), source_instance=payload.get("source_instance"), instance=inst,
            features=features if isinstance(features, list) else [], launch=syncAPI.start_import_run,
            dropped=coerce_bool(payload.get("dropped")),
        )
    except ValueError as exc:
        return _error(str(exc), 400)
    if not job.get("ok"):
        return _error(str(job.get("error") or "sync_running"), 409)
    return {"ok": True, "provider_instance": inst, "job": tracker_import.status(store)}


@router.post("/import/cancel", dependencies=_READY)
def tracker_import_cancel(request: Request, payload: dict[str, Any] = Body(default={})) -> Any:
    from api import syncAPI

    _, inst, store = _store(payload.get("provider_instance"), request)
    job = tracker_import.status(store)
    cancelled = job.get("state") == "running" and syncAPI.cancel_import_run(str(job.get("run_id") or ""))
    return {"ok": True, "provider_instance": inst, "cancelled": cancelled}


_ADD_FEATURES = ("history", "watchlist", "ratings", "collection")
_MAX_ADD = 200


def _identity_of(item: Mapping[str, Any], key: str) -> dict[str, Any]:
    typ = str(item.get("type") or "").lower()
    child = typ in ("episode", "season")
    show = child or typ in ("show", "anime")
    ids = _map(item.get("show_ids") if child else item.get("ids"))
    title = str((item.get("series_title") if child else item.get("title")) or "")
    return {
        "show": show,
        "tmdb": str(ids.get("tmdb") or ""),
        "imdb": "" if show else str(ids.get("imdb") or ""),
        "title": title,
        "base": "" if show else key.split("@", 1)[0],
    }


def _show_identity(show_key: str) -> dict[str, Any]:
    kind, _, value = show_key.partition(":")
    return {"show": True, "tmdb": value if kind == "tmdb" else "", "title": value if kind == "title" else ""}


def _when(row: Mapping[str, Any]) -> str:
    return str(row.get("date") or "")


@router.get("/title", dependencies=_READY)
def tracker_title(
    request: Request,
    provider_instance: str | None = Query("default"),
    feature: str = Query("history"),
    key: str = Query("", max_length=600),
    show: str = Query("", max_length=600),
    note: str = Query("", max_length=600),
) -> Any:
    _, inst, store = _store(provider_instance, request)
    if note and not _NOTE_KEY.match(note):
        return _error("invalid_note_key", 400)
    if not show and not note and feature not in FEATURES:
        return _error("unknown_feature", 400)
    try:
        with store.session():
            anchor: Mapping[str, Any] | None = None
            if note:
                ident = _note_identity(note)
            elif show:
                ident = _show_identity(show)
            else:
                found = store.item(feature, key)
                if not isinstance(found, Mapping):
                    return _error("not_found", 404)
                anchor = found
                ident = _identity_of(found, key)
            related = store.related(**ident)
            checks: dict[str, dict[str, Any]] = {}
            for name in FEATURES:
                keys = [k for f, k, _ in related if f == name]
                if keys:
                    checks[name] = store.checks_for(name, keys)
            note_key = _title_key(ident)
            saved = store.note(note_key)
    except TrackerUnreadable:
        return _unreadable()
    if note and not related and saved is None:
        return _error("not_found", 404)
    rows: dict[str, list[dict[str, Any]]] = {name: [] for name in FEATURES}
    for name, item_key, data in related:
        row = _row(item_key, data, name)
        row["feature"] = name
        row["check"] = _check_row(checks.get(name, {}).get(item_key))
        rows[name].append(row)
    head_source = anchor or next((data for _, _, data in related), {})
    head = _row(key or show or note, head_source, feature)
    if not head_source and saved is not None:
        head.update({k: v for k, v in _note_row(note_key, saved).items() if k != "key"})
    is_show = bool(ident["show"])
    if is_show:
        head.update(type="show", title=head.get("series_title") or head.get("title"), year=head.get("series_year") or head.get("year"))
        if ident.get("tmdb"):
            head["art"] = {"kind": "tv", "tmdb": ident["tmdb"]}
            head["ids"] = {**head.get("show_ids", {}), "tmdb": ident["tmdb"]}
        else:
            head["ids"] = head.get("show_ids") or head.get("ids") or {}
    plays = sorted(rows["history"], key=_when, reverse=True)
    seasons: dict[int, dict[int, str]] = {}
    for row in plays:
        if row.get("type") == "episode" and row.get("season") is not None and row.get("episode") is not None:
            seasons.setdefault(int(row["season"]), {}).setdefault(int(row["episode"]), _when(row))
    attention = [row for name in FEATURES for row in rows[name] if (row.get("check") or {}).get("code") in ATTENTION_CODES]
    return {
        "ok": True,
        "provider_instance": inst,
        "show": is_show,
        "show_key": (f"tmdb:{ident['tmdb']}" if ident.get("tmdb") else f"title:{str(ident.get('title') or '').lower()}") if is_show else None,
        "title": head,
        "plays": plays,
        "ratings": rows["ratings"],
        "watchlist": rows["watchlist"],
        "collection": rows["collection"],
        "progress": rows["progress"],
        "attention": attention,
        "seasons": [{"season": s, "episodes": [{"episode": e, "watched_at": w} for e, w in sorted(eps.items())]} for s, eps in sorted(seasons.items())],
        "note_key": note_key,
        "note": _note_out(note_key, saved),
    }


@router.get("/shows", dependencies=_READY)
def tracker_shows(
    request: Request,
    provider_instance: str | None = Query("default"),
    q: str = Query("", max_length=200),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    hide_dropped: bool = Query(False),
) -> Any:
    _, inst, store = _store(provider_instance, request)
    try:
        with store.session():
            hidden = {k[5:] for k in store.status_keys("dropped") if k.startswith("show:")} if hide_dropped else set()
            total, shows = store.show_summaries(text=q, offset=offset, limit=limit, exclude=hidden)
            notes = store.notes_for(f"show:{show['key']}" for show in shows)
    except TrackerUnreadable:
        return _unreadable()
    for show in shows:
        kind, _, value = show["key"].partition(":")
        show["art"] = {"kind": "tv", "tmdb": value} if kind == "tmdb" and value else None
        show["year"] = _int(show.get("year"))
        saved = notes.get(f"show:{show['key']}")
        show["status"] = saved.get("status") if saved else None
        show["tags"] = saved.get("tags") if saved else []
    return {"ok": True, "provider_instance": inst, "total": total, "offset": offset, "shows": shows}


@router.get("/notes", dependencies=_READY)
def tracker_notes(
    request: Request,
    provider_instance: str | None = Query("default"),
    status: str = Query(""),
    tag: str = Query("", max_length=40),
    q: str = Query("", max_length=200),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
) -> Any:
    if status and status not in NOTE_STATUSES:
        return _error("invalid_status", 400)
    _, inst, store = _store(provider_instance, request)
    try:
        with store.session():
            total, rows = store.notes(status=status, tag=tag, text=q, offset=offset, limit=limit)
            summary = store.note_summary()
    except TrackerUnreadable:
        return _unreadable()
    return {
        "ok": True,
        "provider_instance": inst,
        "total": total,
        "offset": offset,
        "notes": [_note_out(key, note, updated) for key, note, updated in rows],
        "summary": summary,
    }


@router.post("/note", dependencies=_READY)
def tracker_note_save(request: Request, payload: dict[str, Any] = Body(...)) -> Any:
    key = str(payload.get("key") or "")
    if not _NOTE_KEY.match(key):
        return _error("invalid_note_key", 400)
    if _pair_scoped():
        return _error("pair_scoped", 409)
    _, inst, store = _store(payload.get("provider_instance"), request)
    ident = _note_identity(key)
    try:
        current = store.note(key)
        clean = _clean_note(payload, store.note_fields())
        title = " ".join(str(payload.get("title") or (current or {}).get("title") or ident["title"] or "").split())[:300]
        year = _int(payload.get("year"))
        note = None if clean is None else {
            "type": "show" if ident["show"] else "movie",
            "title": title,
            "year": year if year and 1870 <= year <= 2100 else None,
            "tmdb": ident["tmdb"] or None,
            **clean,
        }
        expected = payload.get("expected", ...)
        batch = store.save_note(key, note, expected=expected if expected is None or isinstance(expected, Mapping) else ...)
    except ValueError as exc:
        return _error(str(exc), 400)
    except TrackerUnreadable:
        return _unreadable()
    except TrackerConflict as exc:
        return _error(exc.code, 409, key=exc.key)
    return {"ok": True, "provider_instance": inst, "key": key, "batch": batch, "note": _note_out(key, store.note(key))}


@router.post("/note-fields", dependencies=_READY)
def tracker_note_fields(request: Request, payload: dict[str, Any] = Body(...)) -> Any:
    if _pair_scoped():
        return _error("pair_scoped", 409)
    _, inst, store = _store(payload.get("provider_instance"), request)
    try:
        fields = _clean_fields(payload.get("fields"), store.note_fields())
        store.save_note_fields(fields)
    except ValueError as exc:
        return _error(str(exc), 400)
    except TrackerUnreadable:
        return _unreadable()
    return {"ok": True, "provider_instance": inst, "fields": fields}


def _show_detail(cfg: Mapping[str, Any], tmdb: str) -> Mapping[str, Any] | None:
    if not _tmdb_key(cfg) or not tmdb.isdecimal():
        return None
    try:
        found = _tmdb_provider().lookup("show", tmdb, max_age=7 * 86_400, timeout=10, max_retries=1)
    except Exception:
        return None
    return found.data if isinstance(found.data, Mapping) else None


def _season_counts(detail: Mapping[str, Any], *, specials: bool = False) -> list[tuple[int, int, str]]:
    out = []
    for row in detail.get("seasons") or []:
        if not isinstance(row, Mapping):
            continue
        season, count = _int(row.get("season_number")), _int(row.get("episode_count"))
        if season is None or not count or (season == 0 and not specials):
            continue
        out.append((season, count, str(row.get("name") or (f"Season {season}" if season else "Specials"))))
    return sorted(out)


@router.get("/show-progress", dependencies=_READY)
def tracker_show_progress(request: Request, provider_instance: str | None = Query("default"), tmdb: str = Query("", max_length=20)) -> Any:
    cfg, inst, store = _store(provider_instance, request)
    detail = _show_detail(cfg, tmdb.strip())
    if detail is None:
        return {"ok": True, "available": False}
    try:
        related = store.related(show=True, tmdb=tmdb.strip())
    except TrackerUnreadable:
        return _unreadable()
    counts = {season: count for season, count, _ in _season_counts(detail)}
    watched = {
        (int(data["season"]), int(data["episode"]))
        for name, _, data in related
        if name == "history" and _int(data.get("season")) and _int(data.get("episode"))
    }
    seen = {pair for pair in watched if pair[1] <= counts.get(pair[0], 0)}
    upcoming = _map(detail.get("next_episode_to_air"))
    upcoming_pair = (_int(upcoming.get("season_number")), _int(upcoming.get("episode_number"))) if upcoming else None
    order = [(season, episode) for season in sorted(counts) for episode in range(1, counts[season] + 1)]
    latest = max(seen) if seen else None
    after = [pair for pair in order if latest is None or pair > latest]
    nxt = after[0] if after else None
    if nxt and upcoming_pair and all(upcoming_pair) and nxt >= upcoming_pair:
        nxt = None
    status = str(detail.get("status") or "")
    return {
        "ok": True,
        "available": True,
        "provider_instance": inst,
        "total": sum(counts.values()),
        "watched": len(seen),
        "next": {"season": nxt[0], "episode": nxt[1]} if nxt else None,
        "next_air": {"season": upcoming_pair[0], "episode": upcoming_pair[1], "date": upcoming.get("air_date")} if upcoming_pair and all(upcoming_pair) else None,
        "status": status,
        "completed": nxt is None and not upcoming_pair and status in ("Ended", "Canceled") and len(seen) >= sum(counts.values()) > 0,
        "seasons": [{"season": season, "episodes": count} for season, count in sorted(counts.items())],
    }


@router.get("/seasons", dependencies=_READY)
def tracker_seasons(request: Request, provider_instance: str | None = Query("default"), tmdb: str = Query("", max_length=20)) -> Any:
    cfg, _, _ = _store(provider_instance, request)
    if not _tmdb_key(cfg):
        return _error("tmdb_key_missing", 400)
    detail = _show_detail(cfg, tmdb.strip())
    if detail is None:
        return _error("not_found", 404)
    return {"ok": True, "seasons": [{"season": s, "episodes": c, "name": n} for s, c, n in _season_counts(detail, specials=True)]}


@router.get("/stats", dependencies=_READY)
def tracker_stats(request: Request, provider_instance: str | None = Query("default")) -> Any:
    _, inst, store = _store(provider_instance, request)
    now = datetime.now(timezone.utc)
    try:
        out = store.history_stats(now)
    except TrackerUnreadable:
        return _unreadable()
    days = set(out.pop("days"))
    day = now.date()
    if day.isoformat() not in days:
        day -= timedelta(days=1)
    streak = 0
    while day.isoformat() in days:
        streak += 1
        day -= timedelta(days=1)
    return {"ok": True, "provider_instance": inst, **out, "streak": streak}


def _new_item(feature: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    typ = str(raw.get("type") or "").lower()
    if typ not in ("movie", "show", "episode"):
        raise ValueError("invalid_type")
    if feature == "history" and typ == "show":
        raise ValueError("invalid_type")
    ids = _id_map({}, {k: v for k, v in _map(raw.get("ids")).items() if k in _EDIT_IDS})
    item: dict[str, Any] = {"type": typ, "ids": ids}
    title = str((raw.get("series_title") if typ == "episode" else raw.get("title")) or "").strip()
    if not title or len(title) > 300:
        raise ValueError("invalid_title")
    year = _int(raw.get("series_year") if typ == "episode" else raw.get("year"))
    if year is not None and not 1870 <= year <= 2100:
        raise ValueError("invalid_year")
    if typ == "episode":
        show_ids = _id_map({}, {k: v for k, v in _map(raw.get("show_ids")).items() if k in _EDIT_IDS})
        season, episode = _int(raw.get("season")), _int(raw.get("episode"))
        if not show_ids or season is None or season < 0 or not episode or episode < 1:
            raise ValueError("invalid_episode")
        item.update(show_ids=show_ids, series_title=title, season=season, episode=episode, title=f"S{season:02d}E{episode:02d}")
        if year is not None:
            item["series_year"] = year
    else:
        if not ids:
            raise ValueError("invalid_ids")
        item["title"] = title
        if year is not None:
            item["year"] = year
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    if feature == "history":
        item["watched_at"] = _iso(raw.get("watched_at") or now)
    elif feature == "ratings":
        rating = _rating(raw.get("rating"))
        item.update(rating=rating, rated_at=now)
    elif feature == "collection":
        item["collected_at"] = now
    return item


def _new_key(feature: str, item: Mapping[str, Any], event_mode: bool) -> str:
    if feature != "history":
        return canonical_key(item)
    base = canonical_key(id_minimal(item))
    return f"{base}@{history_epoch_from_item(item)}" if event_mode else base


@router.post("/add", dependencies=_READY)
def tracker_add(request: Request, payload: dict[str, Any] = Body(...)) -> Any:
    feature = str(payload.get("feature") or "")
    blocked = _write_guard(feature)
    if blocked is not None:
        return blocked
    if feature not in _ADD_FEATURES:
        return _error("unknown_feature", 400)
    listed = payload.get("items")
    raw_items: list[Any] = listed if isinstance(listed, list) else [payload.get("item")]
    rows = [raw for raw in raw_items if isinstance(raw, Mapping)]
    if not rows or len(rows) != len(raw_items) or len(rows) > _MAX_ADD:
        return _error("too_many" if len(rows) > _MAX_ADD else "no_changes", 400)
    try:
        items = [_new_item(feature, raw) for raw in rows]
    except ValueError as exc:
        return _error(str(exc), 400)
    unlist = str(payload.get("remove_from_watchlist") or "")
    _, inst, store = _store(payload.get("provider_instance"), request)
    try:
        event_mode = feature == "history" and store.history_event_mode()
        changes: list[tuple[str, str | None, str | None, Mapping[str, Any] | None]] = []
        keys: list[str] = []
        for item in items:
            key = _new_key(feature, item, event_mode)
            if not key or key.startswith("unknown:") or key in keys:
                continue
            current = store.item(feature, key)
            if isinstance(current, Mapping):
                if feature == "history" and str(item["watched_at"]) > str(current.get("watched_at") or ""):
                    changes.append((feature, key, key, {**current, "watched_at": item["watched_at"]}))
                elif feature == "ratings" and current.get("rating") != item["rating"]:
                    changes.append((feature, key, key, {**current, "rating": item["rating"], "rated_at": item["rated_at"]}))
                else:
                    continue
            else:
                changes.append((feature, None, key, item))
            keys.append(key)
        if unlist and isinstance(store.item("watchlist", unlist), Mapping):
            changes.append(("watchlist", unlist, None, None))
        if not changes:
            return _error("already_there", 409)
        batch = store.apply(changes, action="add")
    except TrackerUnreadable:
        return _unreadable()
    except KeyError:
        return _error("not_found", 404)
    except TrackerConflict as exc:
        return _error(exc.code, 409, key=exc.key)
    return {"ok": True, "provider_instance": inst, "batch": batch, "count": len(keys), "key": keys[0] if keys else None}
