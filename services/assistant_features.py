# services/assistant_features.py
# CrossWatch - Read-only backup, tracker and playlist status for built-in chat and MCP
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import Response

from _logging import _redact_log_text
from services.assistant_diagnostics import _fields, admin

MAX_ROWS = 10
MAX_TEXT = 500

TOOLS = {
    "get_backup_status": (
        "Read Backup & Restore: backup settings, schedule, recent backup jobs with errors, local backups and pending restores. "
        "Destination addresses, accounts and passwords are hidden. Does not create, upload or restore a backup.",
        {"type": "object", "properties": {}},
    ),
    "get_tracker_status": (
        "Read the CrossWatch Tracker for one tracker instance: item counts, items needing attention, metadata check, imports "
        "and restore points per feature. Does not edit, check or restore anything.",
        {"type": "object", "properties": {"provider_instance": {"type": "string", "description": "Tracker instance id. Defaults to default."}}},
    ),
    "get_playlists_status": (
        "Read Playlists: playlist sync setups with their last result, errors and warnings, connected playlists and recent activity. "
        "Does not sync or change playlists.",
        {"type": "object", "properties": {}},
    ),
    "get_progress_status": (
        "Read Continue watching: which media servers are included, provider errors and the in-progress items with their progress, "
        "source and allowed actions. Reads the media servers like the Continue watching page does. Does not remove or change progress.",
        {"type": "object", "properties": {
            "search": {"type": "string", "maxLength": 200, "description": "Title to look for."},
            "provider": {"type": "string", "description": "Provider such as PLEX, JELLYFIN or EMBY."},
            "media_type": {"type": "string", "enum": ["movie", "episode", "anime_episode"]},
            "limit": {"type": "integer", "minimum": 1, "maximum": 50, "description": "Defaults to 25."},
        }},
    ),
    "get_maintenance_status": (
        "Read the current or last maintenance job from Maintenance tools, such as a database health check, cleanup or rebuild, "
        "with its state, progress and result. Does not start or stop a job.",
        {"type": "object", "properties": {}},
    ),
}


def _text(value: Any) -> str:
    return _redact_log_text(str(value or ""))[:MAX_TEXT]


def _scalars(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    return {k: _text(v) if isinstance(v, str) else v for k, v in data.items() if isinstance(v, (str, int, float, bool, type(None)))}


def _texts(values: Any) -> list[str]:
    return [_text(v) for v in values[:MAX_ROWS]] if isinstance(values, list) else []


def _payload(value: Any) -> Any:
    if isinstance(value, Response):
        if value.status_code >= 400:
            try:
                detail = json.loads(bytes(value.body)).get("error")
            except (ValueError, AttributeError):
                detail = ""
            raise HTTPException(value.status_code, str(detail or "Not available"))
        return json.loads(bytes(value.body) or b"null")
    return value


def _backups() -> dict[str, Any]:
    from api.backupsAPI import api_backups_schedule_get
    from services import backup_manager
    from services.backups import list_backups

    plan = backup_manager.plan()
    destination = plan.get("destination") or {}
    status = backup_manager.status()
    local = sorted(list_backups(), key=lambda row: float(row.get("mtime") or 0), reverse=True)
    schedule = _payload(api_backups_schedule_get())
    jobs = []
    for job in (status.get("jobs") or [])[:MAX_ROWS]:
        row = _fields(job, ("id", "stage", "trigger", "created_at", "updated_at", "attempts", "retry_at", "local_status", "remote_status", "local_saved_at", "remote_verified_at", "bytes", "total_bytes"))
        for key in ("error", "warning"):
            if job.get(key):
                row[key] = _text(job[key])
        jobs.append(row)
    return {
        "settings": {
            **_fields(plan, ("configured", "encrypt_local", "has_password", "remote_keep", "key_source", "rclone_available", "capture_count")),
            "components": plan.get("components") or [],
            "trackers": plan.get("trackers"),
            "destination": _fields(destination, ("kind", "vendor", "has_password", "allow_http")),
            "rclone_error": _text(plan.get("rclone_error")),
        },
        "schedule": {**_scalars(schedule.get("schedule")), "advanced_enabled": bool(schedule.get("advanced_enabled"))},
        "jobs": jobs,
        "local_backups": {
            "total": len(local),
            "newest": [{**_fields(row, ("path", "size", "mtime", "encrypted", "trigger", "recognized")), "components": row.get("components")} for row in local[:MAX_ROWS]],
        },
        "pending_restore": bool(status.get("pending_restore")),
        "last_restore": _scalars(status.get("restore")),
        "caution": "Jobs are newest first. A backup only counts as safe when its local or remote status is ok or verified.",
    }


def _tracker(request: Request, args: dict[str, Any]) -> dict[str, Any]:
    from api import trackerAPI
    from cw_platform.tracker_store import FEATURES

    trackerAPI._not_migrating()
    instance = str(args.get("provider_instance") or "default")[:64]
    summary = _payload(trackerAPI.tracker_summary(request, provider_instance=instance))
    _, inst, store = trackerAPI._store(summary.get("provider_instance") or instance, request)
    points = {}
    with store.session():
        for feature in FEATURES:
            rows = sorted(store.restore_points(feature), key=lambda point: point.mtime, reverse=True)
            points[feature] = {"total": len(rows), "newest": [{"name": p.name, "created": p.mtime, "size": p.size} for p in rows[:3]]}
    return {
        "provider_instance": inst,
        "instances": summary.get("instances") or [],
        "counts": summary.get("counts") or {},
        "attention": summary.get("attention") or {},
        "health": summary.get("health") or {},
        "metadata_check": _scalars(summary.get("check")),
        "import": _scalars(summary.get("import")),
        "tmdb_key": bool(summary.get("tmdb_key")),
        "restore_points": points,
    }


def _playlists() -> dict[str, Any]:
    from cw_platform.config_base import load_config
    from services import playlists

    cfg = load_config() or {}
    overview = playlists.overview(cfg)
    mappings = []
    for row in playlists.list_mappings(cfg)[:25]:
        result = row.get("last_result") if isinstance(row.get("last_result"), dict) else {}
        mappings.append({
            **_fields(row, ("id", "enabled", "assigned_pair", "source_endpoint")),
            "target_endpoints": [str(x) for x in row.get("target_endpoints") or []][:MAX_ROWS],
            "last_result": {
                **_fields(result, ("ok", "finished_at", "added", "updated", "removed", "skipped", "unresolved_count", "capacity_error")),
                "errors": _texts(result.get("errors")),
                "warnings": _texts(result.get("warnings")),
            },
        })
    endpoints = [_fields(row, ("id", "provider", "provider_label", "instance", "name", "playlist_name", "membership", "item_count", "last_synced")) for row in playlists.list_endpoints(cfg)[:25]]
    activity = [_fields(row, ("ts", "type", "status", "label", "details", "provider", "instance", "added", "updated", "removed", "skipped", "unresolved")) for row in playlists.activity(cfg, limit=15)]
    return {
        "overview": {**_fields(overview, ("total_mappings", "enabled_mappings", "endpoints", "last_sync_epoch", "unresolved")), "warnings": _texts(overview.get("warnings"))},
        "mappings": mappings,
        "endpoints": endpoints,
        "activity": activity,
    }


def _progress(request: Request, args: dict[str, Any]) -> dict[str, Any]:
    from api.playbackProgressAPI import _playback_user_filter
    from services.playback_progress import get_service

    try:
        limit = max(1, min(50, int(args.get("limit", 25))))
    except (TypeError, ValueError, OverflowError):
        limit = 25
    media_type = str(args.get("media_type") or "")
    service = get_service()
    user_filter = _playback_user_filter(request, "")
    settings = service.settings(user_filter=user_filter)
    data = service.items(
        provider=str(args.get("provider") or "").upper()[:40] or None,
        media_type=media_type if media_type in ("movie", "episode", "anime_episode") else None,
        search=str(args.get("search") or "")[:200] or None,
        page_size=limit,
        user_filter=user_filter,
        progressive=True,
    )
    fields = ("provider", "instance_id", "instance_label", "media_type", "title", "series_title", "episode_title", "season", "episode", "year",
              "progress_percent", "remaining_seconds", "duration_seconds", "progress_at", "updated_at", "source_app", "source_device",
              "can_remove_progress", "can_mark_watched", "can_update_progress")
    return {
        "settings": {
            **_fields(settings, ("grouping", "provider_timeout_seconds")),
            "profiles": [_fields(row, ("provider", "provider_label", "instance_id", "instance_label", "configured", "read", "included", "reason")) for row in settings.get("profiles") or []][:25],
        },
        "summary": _scalars(data.get("summary")),
        "total": data.get("total"),
        "items": [{**_fields(row, fields), "ids": row.get("ids") if isinstance(row.get("ids"), dict) else {}} for row in data.get("items") or []][:limit],
        "errors": [_fields(row, ("provider", "instance_id", "error_code", "message", "retryable", "remote_status")) for row in data.get("errors") or []][:MAX_ROWS],
        "still_loading": [_fields(row, ("provider", "instance_id")) for row in data.get("loading") or []],
        "caution": "Servers listed in still_loading did not answer in time, so their items may be missing or older.",
    }


def _maintenance() -> dict[str, Any]:
    from pathlib import Path

    from api.maintenanceAPI import _cw
    from services.maintenance_jobs import status

    _, config_dir, *_ = _cw()
    job = status(Path(config_dir))
    result = job.get("result") if isinstance(job.get("result"), dict) else {}
    details = {key: _scalars(value) for key, value in result.items() if isinstance(value, dict)}
    return {**_scalars(job), "result": {**_scalars(result), **details}}


def run(name: str, request: Request, args: dict[str, Any]) -> Any:
    admin(request)
    if name == "get_progress_status":
        return _progress(request, args)
    if name == "get_maintenance_status":
        return _maintenance()
    if name == "get_backup_status":
        return _backups()
    if name == "get_tracker_status":
        return _tracker(request, args)
    if name == "get_playlists_status":
        return _playlists()
    raise HTTPException(400, "Unknown tool")
