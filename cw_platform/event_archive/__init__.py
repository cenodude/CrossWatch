# cw_platform/event_archive/__init__.py
# CrossWatch - SQLite-backed
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from .db import events_db_path, connect, get_conn, close_conn, EventArchiveError
from .schema import SCHEMA_VERSION, apply_schema
from .recorder import (
    make_event,
    compute_event_hash,
    record_events,
    record_run_started,
    record_run_finished,
    RunRecorder,
)
from .audit import record_audit
from .importer import import_all
from .scrobble_recorder import record_watch, record_webhook
from .query import recent, search, by_item, by_run, status, acknowledge, unacknowledge
from .groups import (
    correlate,
    group_hash,
    list_groups,
    list_tree,
    get_group,
    group_events,
    acknowledge_group,
    unacknowledge_group,
)
from .context import build_context, build_group_context
from .maintenance import health, optimize, rebuild, boot_check, purge
from .stats import statistics
from .activity import Scope as CalendarScope, UNRESTRICTED as CALENDAR_ALL, calendar, day_runs, invalidate as invalidate_calendar
from .run_scope import backfill_run_pairs, record_run_pairs, scope_key

__all__ = [
    "events_db_path",
    "connect",
    "get_conn",
    "close_conn",
    "EventArchiveError",
    "SCHEMA_VERSION",
    "apply_schema",
    "make_event",
    "compute_event_hash",
    "record_events",
    "record_audit",
    "record_run_started",
    "record_run_finished",
    "RunRecorder",
    "import_all",
    "record_watch",
    "record_webhook",
    "recent",
    "search",
    "by_item",
    "by_run",
    "status",
    "acknowledge",
    "unacknowledge",
    "correlate",
    "group_hash",
    "list_groups",
    "list_tree",
    "get_group",
    "group_events",
    "acknowledge_group",
    "unacknowledge_group",
    "build_context",
    "build_group_context",
    "health",
    "optimize",
    "rebuild",
    "boot_check",
    "purge",
    "statistics",
    "CalendarScope",
    "CALENDAR_ALL",
    "calendar",
    "day_runs",
    "invalidate_calendar",
    "backfill_run_pairs",
    "record_run_pairs",
    "scope_key",
]
