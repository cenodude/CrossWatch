# services/notification_center.py
# CrossWatch - Server-side notification list for the notification menu and push
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import calendar
import json
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from fastapi import HTTPException

from cw_platform import notification_store as store
from cw_platform.access_policy import clean_managed_permissions, managed_profile_instances, request_user
from cw_platform.provider_instances import provider_display_key

PROFILE_SOURCES = {"sync-reviews", "month-reviews", "cw-wrapped"}
ACTIVE_POLL = 3
IDLE_POLL = 10
REVIEW_CHECK_SECONDS = 10

_review_checked: dict[tuple[str, str, str, str, str], float] = {}
_review_lock = threading.Lock()


@dataclass
class Context:
    request: Any
    cfg: Mapping[str, Any]
    user_id: str
    scope: str
    view_profile: str = ""
    user: Mapping[str, Any] | None = None
    admin: bool = True
    write: bool = True
    tracker: bool = True
    signals: dict[str, Any] = field(default_factory=dict)


def context(request: Any, cfg: Mapping[str, Any], user_id: str, scope: str, view_profile: str = "") -> Context:
    user = request_user(request)
    admin = not isinstance(user, Mapping) or bool(user.get("is_admin"))
    perms = clean_managed_permissions(user.get("permissions")) if isinstance(user, Mapping) else {}
    write = admin or bool(perms.get("write"))
    tracker = admin
    if not admin and write:
        try:
            tracker = bool(managed_profile_instances(cfg, user).get(provider_display_key("CROSSWATCH")))
        except Exception:
            tracker = False
    return Context(request, cfg, user_id, scope, view_profile, user if isinstance(user, Mapping) else None, admin, write, tracker)


def notification_key(source: str, item: Mapping[str, Any]) -> str:
    return json.dumps([source, item.get("id") or item.get("href"), item.get("revision") or ""], separators=(",", ":"), ensure_ascii=False)


def _map(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _key(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _js(value: Any) -> str:
    if value is None or value is False:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _num(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _count(value: Any) -> str:
    number = _num(value)
    return f"{int(number):,}" if number.is_integer() else f"{number:,}"


def _plural(value: Any, one: str, many: str) -> str:
    return f"{_count(value)} {one if _num(value) == 1 else many}"


def _mb(value: Any) -> str:
    size = _num(value)
    text = f"{size / 1e6:,.1f}" if size < 1e7 else f"{size / 1e6:,.0f}"
    return text[:-2] if text.endswith(".0") else text


def _where(job: Mapping[str, Any]) -> str:
    inst = job.get("instance")
    return f" ({job.get('label') or inst})" if inst and inst != "default" else ""


def update_items(status: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not status or status.get("unavailable"):
        return []
    latest = str(status.get("latest") or status.get("latest_version") or "").strip()
    latest = latest[1:] if latest[:1] in ("v", "V") else latest
    current = str(status.get("current") or status.get("current_version") or "").strip()
    current = current[1:] if current[:1] in ("v", "V") else current
    available = status.get("available", status.get("update_available"))
    if not available or not latest:
        return []
    return [{
        "id": latest, "title": f"CrossWatch v{latest} is available",
        "detail": f"Installed version: v{current}" if current else "A new CrossWatch release is available.",
        "icon": "system_update_alt", "action": "View release notes",
        "href": f"https://github.com/cenodude/CrossWatch/releases/tag/v{quote(latest, safe='')}",
    }]


def _duration(seconds: float) -> str:
    return f"{round(seconds / 60)} minutes" if seconds >= 120 else f"{int(seconds)} seconds"


def _size(value: float) -> str:
    mb = 1024 * 1024
    return f"{value / (1024 * mb):.1f} GB" if value >= 1024 * mb else f"{round(value / mb)} MB"


def database_items(status: Mapping[str, Any] | None, *, managed: bool) -> list[dict[str, Any]]:
    if not status:
        return []
    out: list[dict[str, Any]] = []
    if status.get("busy"):
        operation = status.get("operation") or ""
        out.append({
            "id": f"busy:{operation}:{_js(status.get('since') or '')}", "title": "Database busy",
            "detail": f"A database write ({operation or 'unknown'}) has been running for {_duration(_num(status.get('seconds')))}. Pages can load slowly until it finishes.",
            "icon": "hourglass_top", "time": status.get("since") or 0, "quiet": True,
            "href": "#" if managed else "/#settings/maintenance", "action": "Got it" if managed else "Open maintenance",
        })
    if status.get("network_filesystem") and not managed:
        out.append({
            "id": f"network:{status.get('filesystem') or ''}", "title": "Database on a network share",
            "detail": f"The local database is on {status.get('filesystem') or 'a network filesystem'}. SQLite locking is unreliable there, which can make CrossWatch slow or stuck. Move /config to local storage.",
            "icon": "lan", "href": "/#settings/maintenance", "action": "Open maintenance",
        })
    reclaimable = _num(status.get("reclaimable_bytes"))
    if status.get("reclaim_suggested") and not managed:
        out.append({
            "id": f"reclaim:{int(reclaimable // (100 * 1024 * 1024))}", "title": "Database can be compacted",
            "detail": f"{_size(reclaimable)} of the {_size(_num(status.get('file_bytes')))} local database is unused space. Run Optimize database under Events in Maintenance to free it.",
            "icon": "compress", "href": "/#settings/maintenance", "action": "Open maintenance", "quiet": True,
        })
    return out


_MAINTENANCE_TITLES = {
    "events-optimize": "Optimize database", "events-health": "Archive health check",
    "database-health": "Database health check", "state-file": "Compact sync state",
    "state-file-prune": "Prune stale state", "events-rebuild": "Rebuild event archive",
    "events-purge": "Clear event data", "state": "Rebuild sync state",
    "database-rebuild": "Rebuild local database",
}
_MAINTENANCE_STEPS = {
    "queued": "Waiting to start", "checkpoint_before": "Preparing database",
    "vacuum": "Reclaiming unused space", "analyze": "Updating database statistics",
    "optimize": "Optimizing queries", "checkpoint_after": "Finishing database writes",
}


def maintenance_items(job: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(job, Mapping) or not job.get("id"):
        return []
    state = job.get("state")
    running = state in ("queued", "running")
    failed = state in ("failed", "interrupted")
    result = _map(job.get("result"))
    issues = result.get("healthy") is False or result.get("checkpoint_pending") or _map(result.get("pair_scopes")).get("deferred") or result.get("warnings")
    if running:
        detail = f"{_MAINTENANCE_STEPS.get(_key(job.get('step')), 'Running in the background')}. You can leave this page."
    elif failed:
        detail = "Maintenance failed. Open Maintenance for details."
    else:
        detail = "Finished with issues that need attention." if issues else "Maintenance completed successfully."
    if state == "interrupted":
        detail = "The maintenance worker stopped before recording a result. Check before retrying."
    if result.get("error") == "checkpoint_busy":
        detail = "The database is busy. Optimization could not start."
    label = "running" if running else "stopped" if failed else "needs attention" if issues else "completed"
    return [{
        "id": job["id"], "revision": "running" if running else f"{state}:{'issues' if issues else 'done'}",
        "title": f"{_MAINTENANCE_TITLES.get(_key(job.get('action')), 'Database maintenance')} - {label}",
        "detail": detail, "icon": "sync" if running else "error_outline" if failed or issues else "check_circle",
        "href": "/#maintenance", "action": "View progress" if running else "View result", "time": job.get("started_at"), "quiet": running, "active": running,
    }]


_CHECK_ERRORS = {
    "tmdb_key_missing": "Add a TMDB API key in Settings and run it again.",
    "tmdb_key_invalid": "TMDB rejected the API key. Check it in Settings.",
    "tmdb_unreachable": "TMDB could not be reached. Try again later.",
}


def _storage_item(job: Mapping[str, Any]) -> dict[str, Any] | None:
    if not job.get("id"):
        return None
    where = _where(job)
    base = {"id": f"storage:{job['id']}", "href": "/#tracker", "action": "Open tracker", "time": job.get("finished_at") or job.get("started_at")}
    saved_bytes = _num(job.get("bytes_saved"))
    saved = f"{_mb(abs(saved_bytes))} MB {'more' if saved_bytes < 0 else 'less'} snapshot data"
    if job.get("state") == "completed":
        return {**base, "revision": "completed", "icon": "check_circle", "title": f"Tracker storage - completed{where}",
                "detail": f"Snapshot processing completed: {saved}. Restore points remain available."}
    if job.get("state") == "failed":
        return {**base, "revision": "failed", "icon": "error_outline", "title": f"Tracker storage - needs attention{where}",
                "detail": "Storage maintenance stopped. Saved restore points are kept. CrossWatch will retry; see the log for details."}
    detail = f"{_count(job.get('done'))} of {_plural(job.get('total'), 'snapshot', 'snapshots')} compressed; {saved}. You can keep using the tracker."
    waiting = job.get("phase") == "waiting"
    reason = job.get("reason")
    if waiting:
        detail = {
            "disk_space": "More free disk space is needed to continue storage maintenance. Completed work is kept; CrossWatch will retry.",
            "wal_pressure": "Waiting for database readers to release the write-ahead log before continuing compression. CrossWatch will retry.",
            "busy": "The database is busy. Storage maintenance will retry when it is available.",
        }.get(_key(reason), "Snapshots compressed. Waiting for an idle window to compact the database.")
    elif job.get("phase") == "compacting":
        detail = "Reclaiming unused database space. Tracker writes may briefly wait."
    return {**base, "revision": f"waiting:{reason}" if waiting else "running", "icon": "database",
            "title": f"Tracker storage - {'waiting' if waiting else 'running'}{where}", "detail": detail,
            "quiet": not (waiting and reason == "disk_space"), "active": not waiting}


def _check_item(job: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(job, Mapping) or not job.get("id"):
        return None
    state = job.get("state")
    busy = state == "running"
    attention = int(_num(job.get("attention")))
    where = _where(job)
    title, detail, icon, href, action = "Metadata check", "", "fact_check", "/#tracker", "Open tracker"
    if busy:
        title += f" - running{where}"
        detail = f"{_count(job.get('done'))} of {_plural(job.get('total'), 'record', 'records')} checked."
        icon, action = "sync", "View progress"
    elif state == "completed":
        title += f" - needs attention{where}" if attention else f" - completed{where}"
        detail = f"{_plural(attention, 'record needs', 'records need')} attention." if attention else "Every record matches TMDB."
        icon = "error_outline" if attention else "check_circle"
        if attention:
            href, action = "/#tracker?attention=1", "Review"
    elif state == "cancelled":
        title += f" - cancelled{where}"
        detail = "Stopped before the end. Results so far are kept."
        icon = "block"
    else:
        title += f" - stopped{where}"
        detail = "CrossWatch restarted during the check. Run it again." if state == "interrupted" else _CHECK_ERRORS.get(_key(job.get("error")), "The check stopped. Run it again from the Tracker.")
        icon = "error_outline"
    return {"id": job["id"], "revision": "running" if busy else f"{state}:{attention}", "title": title, "detail": detail, "icon": icon,
            "href": href, "action": action, "time": job.get("finished_at") or job.get("started_at"), "quiet": busy, "active": busy}


def _import_item(job: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(job, Mapping) or not job.get("id") or job.get("state") == "idle":
        return None
    source_inst = job.get("source_instance")
    origin = f"{job.get('source_label') or job.get('source') or 'a service'}"
    if source_inst and source_inst != "default":
        origin += f" ({job.get('source_instance_label') or source_inst})"
    inst = job.get("instance")
    into = f" into {job.get('label') or inst}" if inst and inst != "default" else ""
    base = {"id": f"import:{job['id']}", "href": "/#tracker", "action": "Open tracker", "time": job.get("finished_at") or job.get("started_at")}
    state = job.get("state")
    if state == "running":
        if job.get("phase") == "writing" and job.get("total"):
            step = f"Writing {_count(job.get('done'))} of {_count(job.get('total'))}."
        else:
            step = f"Reading {origin} and matching IDs." if job.get("phase") == "reading" else "Starting."
        return {**base, "revision": "running", "icon": "download", "action": "View progress", "title": f"Import - running{into}",
                "detail": f"From {origin}. {step}", "quiet": True, "active": True}
    added = int(_num(job.get("added")))
    if state == "completed":
        missed = f" {_plural(job.get('unresolved'), 'record', 'records')} could not be matched." if _num(job.get("unresolved")) else ""
        return {**base, "revision": f"completed:{added}", "icon": "check_circle", "title": f"Import - completed{into}",
                "detail": f"{_plural(added, 'record', 'records')} added from {origin}.{missed}"}
    if state == "cancelled":
        return {**base, "revision": "cancelled", "icon": "block", "title": f"Import - stopped{into}",
                "detail": f"{_plural(added, 'record was', 'records were')} added from {origin} before it stopped."}
    return {**base, "revision": state, "icon": "error_outline", "title": f"Import - did not finish{into}",
            "detail": "CrossWatch restarted during the import. Run it again from the Tracker." if state == "interrupted" else "The import stopped with an error. See the sync log for details."}


def _migration_item(move: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(move, Mapping) or not move.get("id"):
        return None
    results = _list(move.get("results"))
    failures = _list(move.get("errors"))

    def name(inst: Any) -> str:
        return f" {inst}" if inst and inst != "default" else ""

    base = {"id": f"move:{move['id']}", "href": "/#tracker", "action": "Open tracker", "time": move.get("finished_at") or move.get("started_at")}
    if move.get("state") == "running":
        many = f" · tracker {move.get('instance_index')} of {move.get('instance_count')}" if _num(move.get("instance_count") or 1) > 1 else ""
        size = f" ({_mb(move.get('bytes_done'))} of {_mb(move.get('bytes_total'))} MB)" if move.get("bytes_total") else ""
        label = "" if move.get("instance") == "default" else move.get("label") or move.get("instance")
        return {**base, "revision": "running", "icon": "database", "action": "View progress", "title": "Tracker upgrade - running", "quiet": True, "active": True,
                "detail": f"Moving the tracker{name(label)} to its database: {_count(move.get('files_done'))} of {_count(move.get('files_total'))} files{size}{many}. Syncs wait until this is done."}
    if failures:
        first = _map(failures[0])
        if first.get("reason") == "backup_pending":
            return {**base, "revision": "backup_pending", "icon": "error_outline", "title": "Tracker upgrade - backup needs attention",
                    "detail": "The database import completed, but moving the original JSON files to the backup did not finish. Check disk access and restart CrossWatch to retry the move."}
        moved = f" {_plural(len(results), 'other tracker was', 'other trackers were')} moved." if results else ""
        return {**base, "revision": f"failed:{len(failures)}", "icon": "error_outline", "title": "Tracker upgrade - needs attention",
                "detail": f"{first.get('file') or 'A file'} could not be read, so the tracker{name(first.get('instance'))} still uses its old files. Fix or remove that file and restart CrossWatch.{moved}"}
    rows = [row for row in results if isinstance(row, Mapping)]
    counts = ""
    if any("snapshots_backup_only" in row and row.get("snapshots_backup_only") is not None for row in rows):
        imported = sum(_num(row.get("restore_points")) for row in rows)
        backup = sum(_num(row.get("snapshots_backup_only")) for row in rows)
        counts = f" {_plural(imported, 'snapshot', 'snapshots')} imported; {_plural(backup, 'snapshot', 'snapshots')} kept only in the backup."
    if len(rows) > 1:
        detail = f"{len(rows)} trackers now use a database. Their original files are in a json-backup folder."
    else:
        detail = f"The tracker now uses a database. The original files are in {(rows[0].get('backup') if rows else None) or 'a json-backup folder'}."
    return {**base, "revision": "completed", "icon": "check_circle", "title": "Tracker upgrade - completed", "detail": detail + counts}


def tracker_items(value: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    data = _map(value)
    storage = _list(data.get("storage"))
    items = [_migration_item(data.get("migration")), *(_storage_item(job) for job in storage if isinstance(job, Mapping)),
             _import_item(data.get("import")), _check_item(data.get("job"))]
    return [item for item in items if item]


def plex_recovery_items(job: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(job, Mapping) or not job:
        return []
    status = job.get("status")
    auto = _map(job.get("auto_match"))
    reading = status == "reading"
    matching = auto.get("status") == "running"
    failed = status in ("error", "cancelled")
    imported = int(_num(job.get("imported")))
    if reading:
        detail = "Scanning older history"
    elif failed:
        detail = str(job.get("message") or "")
    elif auto.get("id"):
        detail = f"Auto match: {auto.get('done')} / {auto.get('total')} titles checked" if matching else str(auto.get("message") or "")
    elif imported:
        detail = f"{imported} {'item' if imported == 1 else 'items'} imported into CrossWatch"
    else:
        detail = "Ready to review. Nothing has been imported."
    return [{
        "id": job.get("id"), "revision": f"{status}:{imported}:{auto.get('id') or ''}:{auto.get('status') or ''}",
        "title": "Plex history recovery", "detail": detail,
        "icon": "sync" if reading or matching else "error_outline" if failed or auto.get("status") == "paused" else "history",
        "href": "/#import_export?recovery=1", "action": "View progress" if reading or matching else "Open recovery",
        "quiet": reading or matching, "active": reading or matching,
    }]


_REVIEW_STATES = {
    "reading": ("Preparing review", "View progress", "sync"),
    "applying": ("Applying changes", "View progress", "sync"),
    "review": ("Ready to review", "Resume review", "fact_check"),
    "complete": ("Finished", "View report", "description"),
    "error": ("Needs attention", "Open review", "error_outline"),
}


def sync_review_items(sessions: list[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    out = []
    for session in sessions or []:
        pair = _map(session.get("pair"))

        def endpoint(side: str) -> str:
            inst = pair.get(f"{side}_instance")
            return f"{pair.get(side) or 'Provider'}{f' · {inst}' if inst and inst != 'default' else ''}"

        label = f"{endpoint('source')} {'↔' if pair.get('mode') == 'two-way' else '→'} {endpoint('target')}"
        status, action, icon = _REVIEW_STATES.get(_key(session.get("status")), _REVIEW_STATES["error"])
        out.append({
            "id": session.get("id"), "revision": f"{session.get('status')}:{_js(session.get('planned_at') or '')}",
            "title": pair.get("name") or label, "detail": status, "icon": icon,
            "href": f"/#interactive_sync?session={quote(str(session.get('id') or ''), safe='')}",
            "action": action, "time": session.get("planned_at"), "quiet": session.get("status") in ("reading", "applying"),
            "active": session.get("status") in ("reading", "applying"),
        })
    return out


def review_item(row: Mapping[str, Any]) -> dict[str, Any]:
    yearly = row["kind"] == "wrapped"
    period = str(row["period"])
    parameter = "year" if yearly else "month"
    if yearly:
        title = f"Your {period} CrossWatch Wrapped is ready"
    else:
        title = f"Your {calendar.month_name[int(period[5:7])]} review is ready"
    return {
        "id": json.dumps([row["instance"], period], separators=(",", ":"), ensure_ascii=False),
        "revision": str(row.get("ready_revision") or period), "title": title, "detail": f"{row['label']} · {period[:4]}",
        "icon": "auto_awesome", "action": "View review",
        "href": f"#tracker?instance={quote(str(row['instance']), safe='')}&{parameter}={quote(period, safe='')}",
        "time": row.get("ready_at"),
    }


def _handler(fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except HTTPException as exc:
        if exc.status_code in (400, 401, 403, 404, 409, 503):
            return None
        raise


def _updates(ctx: Context) -> list[dict[str, Any]]:
    from api.versionAPI import get_version

    return update_items(get_version())


def _database(ctx: Context) -> list[dict[str, Any]]:
    from services.health_details import database_status

    return database_items(database_status(), managed=not ctx.admin)


def _maintenance(ctx: Context) -> list[dict[str, Any]]:
    if not ctx.admin:
        return []
    from api.maintenanceAPI import maintenance_job_status

    return maintenance_items(maintenance_job_status())


def _tracker(ctx: Context) -> list[dict[str, Any]]:
    if not ctx.tracker:
        return []
    from api.trackerAPI import tracker_check_current

    data = _handler(lambda: tracker_check_current(ctx.request))
    if not isinstance(data, Mapping):
        return []
    ctx.signals["tracker_migration"] = _map(data.get("migration")).get("state")
    return tracker_items(data)


def _plex_recovery(ctx: Context) -> list[dict[str, Any]]:
    if not ctx.write:
        return []
    from services.plex_history_recovery import active

    data = _handler(lambda: active(ctx.request))
    return plex_recovery_items(data.get("job")) if isinstance(data, Mapping) else []


def _sync_reviews(ctx: Context) -> list[dict[str, Any]]:
    if not ctx.write:
        return []
    from api.interactiveSyncAPI import reviews

    data = _handler(lambda: reviews(ctx.request, user_profile=ctx.view_profile))
    return sync_review_items(data.get("sessions")) if isinstance(data, Mapping) else []


def _review_ready(ctx: Context, row: Mapping[str, Any]) -> str | None:
    from api.trackerAPI import _store
    from services import tracker_migration
    from services.tracker_month import get_report

    if tracker_migration.busy():
        return None
    try:
        cfg, _inst, tracker = _store(row["instance"], ctx.request)
    except HTTPException:
        return ""
    yearly = row["kind"] == "wrapped"
    try:
        data = get_report(tracker, cfg, row["period"], yearly=yearly)
    except ValueError:
        return ""
    if data.get("disabled"):
        return ""
    if not data.get("report") or data.get("pending") or data.get("stale") or data.get("error"):
        return None
    period = str(data.get("year") if yearly else data.get("month") or row["period"])
    return _js(data.get("generated") or period)


def _reviews(ctx: Context, kind: str) -> list[dict[str, Any]]:
    if not ctx.tracker:
        return []
    out = []
    for row in store.review_waits(ctx.user_id, ctx.scope):
        if row["kind"] != kind:
            continue
        if row["ready_revision"] is None:
            key = (ctx.user_id, ctx.scope, kind, row["instance"], row["period"])
            now = time.monotonic()
            with _review_lock:
                if now - _review_checked.get(key, -REVIEW_CHECK_SECONDS) < REVIEW_CHECK_SECONDS:
                    continue
                _review_checked[key] = now
            revision = _review_ready(ctx, row)
            if revision == "":
                store.review_dropped(ctx.user_id, ctx.scope, kind, row["instance"], row["period"])
                continue
            if revision is None:
                if time.time() - float(row["created_at"] or 0) > store.WAIT_SECONDS:
                    store.review_dropped(ctx.user_id, ctx.scope, kind, row["instance"], row["period"])
                continue
            store.review_ready(ctx.user_id, ctx.scope, kind, row["instance"], row["period"], revision)
            row = {**row, "ready_revision": revision, "ready_at": time.time()}
        out.append(review_item(row))
    return out


SOURCES: tuple[tuple[str, Callable[[Context], list[dict[str, Any]]], str], ...] = (
    ("sync-reviews", _sync_reviews, "Could not load sync reviews."),
    ("month-reviews", lambda ctx: _reviews(ctx, "month"), ""),
    ("cw-wrapped", lambda ctx: _reviews(ctx, "wrapped"), ""),
    ("plex-recovery", _plex_recovery, "Could not load Plex recovery."),
    ("tracker-check", _tracker, "Could not refresh the tracker status."),
    ("maintenance", _maintenance, "Could not refresh maintenance status."),
    ("database", _database, ""),
    ("updates", _updates, ""),
)


def collect(ctx: Context, *, include: Callable[[str], bool] | None = None) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    errors: list[str] = []
    for name, source, message in SOURCES:
        if include is not None and not include(name):
            continue
        try:
            found = source(ctx)
        except Exception:
            if message:
                errors.append(message)
            continue
        scope = "profile" if name in PROFILE_SOURCES else "account"
        for item in found:
            row = {**item, "source": name, "scope": scope, "quiet": bool(item.get("quiet")), "active": bool(item.get("active"))}
            row["key"] = notification_key(name, row)
            items.append(row)
    active = any(item["active"] for item in items)
    return {"items": items, "errors": errors, "poll": ACTIVE_POLL if active else IDLE_POLL, "signals": dict(ctx.signals)}
