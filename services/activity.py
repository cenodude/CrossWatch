# /services/activity.py
# CrossWatch - Local Recent Activity Log
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from _logging import log as BASE_LOG

LOG = BASE_LOG.child("ACTIVITY")
LOG_VERSION = 1
DEFAULT_LIMIT = 1000
LOG_FILE_NAME = "activity_history.json"
GROUP_WINDOW_SECONDS = 120
_LOCK = threading.RLock()


def state_dir() -> Path:
    base = Path("/config/.cw_state") if Path("/config/config.json").exists() else Path(".cw_state")
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return base


def activity_path() -> Path:
    return state_dir() / LOG_FILE_NAME


def _now() -> int:
    return int(time.time())


def _as_int(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool):
            return None
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except Exception:
        return None


def _compact_dict(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, Any] = {}
    for k, v in value.items():
        key = str(k or "").strip()
        if not key or v in (None, ""):
            continue
        if isinstance(v, (str, int, float, bool)):
            out[key] = v
    return out


def _nested_dict(item: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = item.get(key)
    if not isinstance(value, Mapping):
        return {}
    return {str(k): v for k, v in value.items()}


def _nested_text(item: Mapping[str, Any], key: str, *fields: str) -> str:
    nested = _nested_dict(item, key)
    for field in fields:
        value = str(nested.get(field) or "").strip()
        if value:
            return value
    return ""


def _item_media_type(item: Mapping[str, Any]) -> str:
    media_type = str(item.get("type") or item.get("media_type") or "").strip().lower()
    if media_type in {"movie", "episode"}:
        return media_type
    if isinstance(item.get("episode"), Mapping):
        return "episode"
    if isinstance(item.get("movie"), Mapping):
        return "movie"
    return media_type


def _item_title(item: Mapping[str, Any], media_type: str) -> Any:
    if media_type == "episode":
        return (
            item.get("series_title")
            or item.get("show_title")
            or _nested_text(item, "show", "title", "name")
            or _nested_text(item, "series", "title", "name")
            or item.get("title")
        )
    return item.get("title") or item.get("name") or _nested_text(item, "movie", "title", "name")


def _item_year(item: Mapping[str, Any], media_type: str) -> Any:
    if item.get("year") or item.get("series_year"):
        return item.get("year") or item.get("series_year")
    if media_type == "episode":
        return _nested_dict(item, "show").get("year") or _nested_dict(item, "series").get("year")
    return _nested_dict(item, "movie").get("year")


def _item_season(item: Mapping[str, Any]) -> Any:
    season = item.get("season")
    if isinstance(season, Mapping):
        return season.get("number")
    return season or _nested_dict(item, "episode").get("season")


def _item_episode(item: Mapping[str, Any]) -> Any:
    episode = item.get("episode")
    if isinstance(episode, Mapping):
        return episode.get("number")
    return episode or item.get("number")


def _item_activity_ids(item: Mapping[str, Any], media_type: str) -> dict[str, Any]:
    if media_type == "episode":
        show_ids = _compact_dict(item.get("show_ids"))
        for key in ("show", "series"):
            nested_ids = _compact_dict(_nested_dict(item, key).get("ids"))
            if nested_ids:
                show_ids = {**nested_ids, **show_ids}
        if show_ids:
            return show_ids
    ids = _compact_dict(item.get("ids"))
    if not ids and media_type == "movie":
        ids = _compact_dict(_nested_dict(item, "movie").get("ids"))
    return ids


def _read_payload() -> dict[str, Any]:
    path = activity_path()
    if not path.exists():
        return {"v": LOG_VERSION, "items": []}
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw) if raw.strip() else {}
    except Exception:
        return {"v": LOG_VERSION, "items": []}
    if isinstance(data, list):
        return {"v": LOG_VERSION, "items": [x for x in data if isinstance(x, dict)]}
    if isinstance(data, dict):
        items = data.get("items")
        return {"v": LOG_VERSION, "items": [x for x in (items or []) if isinstance(x, dict)]}
    return {"v": LOG_VERSION, "items": []}


def _write_payload(payload: Mapping[str, Any]) -> None:
    path = activity_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    tmp = path.with_name(f"{path.name}.{time.time_ns()}.{threading.get_ident()}.tmp")
    data = {"v": LOG_VERSION, "items": list(payload.get("items") or [])}
    tmp.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)


def _event_id(item: Mapping[str, Any]) -> str:
    stable = {
        "kind": item.get("kind"),
        "method": item.get("method"),
        "source": item.get("source"),
        "source_instance": item.get("source_instance"),
        "target": item.get("target"),
        "target_instance": item.get("target_instance"),
        "media_type": item.get("media_type"),
        "title": item.get("title"),
        "year": item.get("year"),
        "season": item.get("season"),
        "episode": item.get("episode"),
        "status": item.get("status"),
        "event": item.get("event"),
        "watched_at": item.get("watched_at"),
        "ids": item.get("ids"),
    }
    raw = json.dumps(stable, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def _target_ref(item: Mapping[str, Any]) -> dict[str, str]:
    return {
        "target": str(item.get("target") or "").strip().lower(),
        "target_instance": str(item.get("target_instance") or "default").strip() or "default",
    }


def _target_key(target: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(target.get("target") or "").strip().lower(),
        str(target.get("target_instance") or "default").strip() or "default",
    )


def _group_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    ids = item.get("ids") if isinstance(item.get("ids"), Mapping) else {}
    ids_key = json.dumps(ids, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return (
        str(item.get("kind") or "").strip().lower(),
        str(item.get("method") or "").strip().lower(),
        str(item.get("event") or "").strip().lower(),
        str(item.get("status") or "").strip().lower(),
        str(item.get("source") or "").strip().lower(),
        str(item.get("source_instance") or "default").strip() or "default",
        str(item.get("media_type") or "").strip().lower(),
        str(item.get("title") or "").strip().lower(),
        _as_int(item.get("year")),
        _as_int(item.get("season")),
        _as_int(item.get("episode")),
        _as_int(item.get("progress")),
        str(item.get("account") or "").strip().lower(),
        ids_key,
    )


def _event_ts(item: Mapping[str, Any]) -> int:
    return _as_int(item.get("captured_at")) or _as_int(item.get("watched_at")) or 0


def _group_route_fanout(items: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    open_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}
    seen_targets: dict[int, set[tuple[str, str]]] = {}

    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        key = _group_key(item)
        ts = _event_ts(item)
        group = open_by_key.get(key)
        group_ts = _event_ts(group) if group else 0
        if group is None or abs(ts - group_ts) > GROUP_WINDOW_SECONDS:
            target = _target_ref(item)
            item["targets"] = [target] if target["target"] else []
            groups.append(item)
            open_by_key[key] = item
            seen_targets[id(item)] = {_target_key(target)} if target["target"] else set()
            continue

        target = _target_ref(item)
        if target["target"]:
            target_key = _target_key(target)
            targets_seen = seen_targets.setdefault(id(group), set())
            if target_key not in targets_seen:
                group.setdefault("targets", []).append(target)
                targets_seen.add(target_key)

    return groups


def add_event(event: Mapping[str, Any], *, limit: int = DEFAULT_LIMIT) -> dict[str, Any] | None:
    media_type = str(event.get("media_type") or "").strip().lower()
    if media_type not in {"movie", "episode"}:
        return None

    captured_at = _as_int(event.get("captured_at")) or _now()
    item: dict[str, Any] = {
        "kind": str(event.get("kind") or "activity").strip().lower() or "activity",
        "method": str(event.get("method") or "").strip().lower(),
        "event": str(event.get("event") or "").strip().lower(),
        "status": str(event.get("status") or "ok").strip().lower() or "ok",
        "source": str(event.get("source") or "").strip().lower(),
        "source_instance": str(event.get("source_instance") or "default").strip() or "default",
        "target": str(event.get("target") or "").strip().lower(),
        "target_instance": str(event.get("target_instance") or "default").strip() or "default",
        "media_type": media_type,
        "title": str(event.get("title") or "").strip(),
        "year": _as_int(event.get("year")),
        "season": _as_int(event.get("season")) if media_type == "episode" else None,
        "episode": _as_int(event.get("episode")) if media_type == "episode" else None,
        "progress": _as_int(event.get("progress")),
        "account": str(event.get("account") or "").strip(),
        "watched_at": _as_int(event.get("watched_at")) or captured_at,
        "captured_at": captured_at,
        "ids": _compact_dict(event.get("ids")),
    }

    if not item["title"]:
        item["title"] = "Untitled"

    item["id"] = str(event.get("id") or "").strip() or _event_id(item)

    with _LOCK:
        payload = _read_payload()
        items = [x for x in list(payload.get("items") or []) if isinstance(x, dict)]
        items = [x for x in items if str(x.get("id") or "") != item["id"]]
        items.insert(0, item)
        cap = max(1, int(limit or DEFAULT_LIMIT))
        payload["items"] = items[:cap]
        _write_payload(payload)
    return item


def record_scrobble_event(
    ev: Any,
    *,
    source: str,
    target: str,
    status: str = "ok",
    target_instance: str = "default",
    source_instance: str = "default",
    progress: Any = None,
    captured_at: int | None = None,
) -> dict[str, Any] | None:
    action = str(getattr(ev, "action", "") or "").strip().lower()
    if action != "stop":
        return None
    media_type = str(getattr(ev, "media_type", "") or "").strip().lower()
    if media_type not in {"movie", "episode"}:
        return None
    return add_event(
        {
            "kind": "scrobble",
            "method": "watcher",
            "event": "scrobble_stop",
            "status": status,
            "source": source,
            "source_instance": source_instance,
            "target": target,
            "target_instance": target_instance,
            "media_type": media_type,
            "title": getattr(ev, "title", None),
            "year": getattr(ev, "year", None),
            "season": getattr(ev, "season", None),
            "episode": getattr(ev, "number", None),
            "progress": progress if progress is not None else getattr(ev, "progress", None),
            "account": getattr(ev, "account", None),
            "watched_at": captured_at or _now(),
            "captured_at": captured_at or _now(),
            "ids": getattr(ev, "ids", None) or {},
        }
    )


def list_events(
    *,
    limit: int = 100,
    offset: int = 0,
    media_type: str = "all",
    status: str = "all",
    kind: str = "all",
    query: str = "",
    since: int | None = None,
    group_routes: bool = True,
) -> dict[str, Any]:
    with _LOCK:
        items = [x for x in list(_read_payload().get("items") or []) if isinstance(x, dict)]

    mt = str(media_type or "all").strip().lower()
    st = str(status or "all").strip().lower()
    kd = str(kind or "all").strip().lower()
    q = str(query or "").strip().lower()
    since_ts = _as_int(since)

    def keep(item: Mapping[str, Any]) -> bool:
        if since_ts is not None:
            ts = _as_int(item.get("captured_at")) or _as_int(item.get("watched_at")) or 0
            if ts < since_ts:
                return False
        if mt in {"movie", "episode"} and str(item.get("media_type") or "").lower() != mt:
            return False
        if st in {"ok", "failed", "error"}:
            want = "failed" if st == "error" else st
            got = str(item.get("status") or "").lower()
            if got != want:
                return False
        if kd != "all" and str(item.get("kind") or "").strip().lower() != kd:
            return False
        if q:
            hay = " ".join(
                str(item.get(k) or "")
                for k in ("title", "source", "target", "account", "media_type", "event", "method")
            ).lower()
            hay += " " + " ".join(
                str(item.get(k) or "")
                for k in ("source_instance", "target_instance")
            ).lower()
            if q not in hay:
                return False
        return True

    filtered = [x for x in items if keep(x)]
    display_items = _group_route_fanout(filtered) if group_routes else filtered
    start = max(0, int(offset or 0))
    cap = max(1, min(500, int(limit or 100)))
    page = display_items[start:start + cap]
    return {
        "ok": True,
        "items": page,
        "total": len(display_items),
        "offset": start,
        "limit": cap,
        "has_more": start + cap < len(display_items),
        "path": str(activity_path()),
    }


def clear_events(*, kind: str | None = None) -> dict[str, Any]:
    path = activity_path()
    existed = path.exists()
    wanted = str(kind or "").strip().lower()
    with _LOCK:
        try:
            if not wanted:
                path.unlink(missing_ok=True)
                return {"ok": True, "path": str(path), "existed": bool(existed), "removed": 1 if existed else 0}

            payload = _read_payload()
            items = [x for x in list(payload.get("items") or []) if isinstance(x, dict)]
            kept = [x for x in items if str(x.get("kind") or "").strip().lower() != wanted]
            removed = len(items) - len(kept)
            if kept:
                _write_payload({"v": LOG_VERSION, "items": kept})
            else:
                path.unlink(missing_ok=True)
            return {
                "ok": True,
                "path": str(path),
                "existed": bool(existed),
                "kind": wanted,
                "removed": removed,
                "remaining": len(kept),
            }
        except Exception as e:
            LOG.error(f"failed to clear activity log: {type(e).__name__}: {e}")
            return {"ok": False, "error": "clear_activity_failed"}


def clear_scrobble_events() -> dict[str, Any]:
    return clear_events(kind="scrobble")


def record_history_sync_items(
    items: Iterable[Mapping[str, Any]],
    *,
    source: str,
    target: str,
    status: str = "ok",
    target_instance: str = "default",
    source_instance: str = "default",
) -> None:
    for item in items or []:
        if not isinstance(item, Mapping):
            continue
        media_type = _item_media_type(item)
        if media_type not in {"movie", "episode"}:
            continue
        add_event(
            {
                "kind": "history_sync",
                "event": "history_add",
                "status": status,
                "source": source,
                "source_instance": source_instance,
                "target": target,
                "target_instance": target_instance,
                "media_type": media_type,
                "title": _item_title(item, media_type),
                "year": _item_year(item, media_type),
                "season": _item_season(item),
                "episode": _item_episode(item),
                "progress": 100,
                "watched_at": item.get("watched_at"),
                "ids": _item_activity_ids(item, media_type),
            }
        )
