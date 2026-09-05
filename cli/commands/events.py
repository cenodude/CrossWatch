# /cli/commands/events.py
# CrossWatch - CLI events archive commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import typer
from rich.text import Text

from .._context import Ctx
from .._errors import EXIT_NOT_FOUND, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text, fmt_duration, fmt_ts, rows_from_payload

events_app = typer.Typer(help="Search and inspect the events archive.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    return rows_from_payload(payload, *keys)


def _when(row: dict[str, Any]) -> str:
    for key in ("ts", "created_at", "timestamp", "at", "occurred_at"):
        if row.get(key):
            return fmt_ts(row.get(key))
    return "-"


EVENT_COLUMNS = [
    ("WHEN", _when),
    ("TYPE", lambda r: str(r.get("event_type") or r.get("type") or "-")),
    ("PROVIDER", lambda r: str(r.get("provider") or r.get("destination_provider") or "-")),
    ("FEATURE", lambda r: str(r.get("feature") or "-")),
    ("TITLE", lambda r: str(r.get("title") or r.get("item_title") or r.get("message") or "-")[:44]),
]

GROUP_COLUMNS = [
    ("ID", lambda r: str(r.get("id") or r.get("group_id") or "-")),
    ("WHEN", _when),
    ("TYPE", lambda r: str(r.get("event_type") or r.get("type") or "-")),
    ("PROVIDER", lambda r: str(r.get("provider") or "-")),
    ("COUNT", lambda r: str(r.get("count") or r.get("events") or 0)),
    ("ACK", lambda r: state_text(coerce_bool(r.get("acknowledged"), False), on="yes", off="no")),
    ("WHAT", lambda r: str(r.get("title") or r.get("message") or "-")[:38]),
]


def _filters(
    query: str,
    event_type: str,
    provider: str,
    feature: str,
    domain: str,
    limit: int,
    offset: int,
    visibility: str,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if query.strip():
        params["q"] = query.strip()
    if event_type.strip():
        params["event_type"] = event_type.strip()
    if provider.strip():
        params["provider"] = provider.strip()
    if feature.strip():
        params["feature"] = feature.strip()
    if domain.strip():
        params["domain"] = domain.strip()
    if visibility.strip():
        params["visibility"] = visibility.strip()
    return params



DAY = 86400
RANGES = {"3m": 98, "6m": 189, "1y": 371}
METRICS = ("changes", "runs", "failures")
LEVEL_COLORS = ("#2d333b", "#0e4429", "#006d32", "#26a641", "#39d353")
FAIL_COLOR = "#f85149"
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _tz_offset() -> int:
    off = datetime.now().astimezone().utcoffset()
    return int(off.total_seconds()) if off else 0


def _day_index(epoch: float, tz: int) -> int:
    return int((float(epoch) + tz) // DAY)


def _index_date(index: int, tz: int) -> date:
    return datetime.fromtimestamp(int(index) * DAY, tz=timezone.utc).date()


def _index_bounds(index: int, tz: int) -> tuple[int, int]:
    since = int(index) * DAY - tz
    return since, since + DAY


def _parse_day(text: str, tz: int) -> int:
    raw = str(text or "").strip().lower()
    today = _day_index(time.time(), tz)
    if raw in ("", "today"):
        return today
    if raw == "yesterday":
        return today - 1
    try:
        parsed = date.fromisoformat(raw)
    except ValueError:
        raise CLIError(f"Cannot read {text!r} as a date", hint="Use YYYY-MM-DD, today or yesterday") from None
    return int(datetime(parsed.year, parsed.month, parsed.day, tzinfo=timezone.utc).timestamp() // DAY)


def _range_days(window: str) -> int:
    raw = str(window or "").strip().lower()
    if raw in RANGES:
        return RANGES[raw]
    digits = raw[:-1] if raw.endswith("d") else raw
    try:
        days = int(digits)
    except ValueError:
        raise CLIError(f"Unknown range {window!r}", hint="Use 3m, 6m, 1y or a day count like 60") from None
    return max(7, min(400, days))


def _metric_value(row: dict[str, Any], metric: str) -> int:
    if metric == "runs":
        return int(row.get("runs") or 0)
    if metric == "failures":
        return int(row.get("failed") or 0)
    return int(row.get("changes") or 0)


def _levels(values: list[int]) -> list[int]:
    hot = sorted(v for v in values if v > 0)
    if not hot:
        return [1, 2, 3, 4]
    def at(p: float) -> int:
        return hot[min(len(hot) - 1, int(p * (len(hot) - 1)))]
    steps: list[int] = []
    prev = 0
    for p in (0.25, 0.5, 0.75, 0.92):
        nxt = max(prev + 1, at(p))
        steps.append(nxt)
        prev = nxt
    return steps


def _level_of(value: int, steps: list[int]) -> int:
    if value <= 0:
        return 0
    for i, step in enumerate(steps):
        if value <= step:
            return i + 1
    return 4


def _glyphs(encoding: str) -> tuple[str, ...]:
    try:
        "■".encode(encoding or "utf-8")
    except Exception:
        return (".", "-", "+", "*", "#")
    return ("■",) * 5


def _heatmap(days_map: dict[int, dict[str, Any]], first: int, last: int, metric: str, glyphs: tuple[str, ...]) -> list[Text]:
    weeks = ((last - first) // 7) + 1
    steps = _levels([_metric_value(r, metric) for r in days_map.values()])

    slots = [" "] * weeks
    month_of = [_index_date(first + w * 7, 0).month for w in range(weeks)]
    col = 0
    while col < weeks:
        month = month_of[col]
        end = col
        while end < weeks and month_of[end] == month:
            end += 1
        name = MONTHS[month - 1]
        if end - col >= 3 and col + len(name) <= weeks:
            for i, char in enumerate(name):
                slots[col + i] = char
        col = end
    lines = [Text("     " + "".join(slots), style="cw.muted")]

    for row in range(7):
        line = Text(f"{WEEKDAYS[row] if row in (0, 2, 4) else '   '}  ", style="cw.muted")
        for w in range(weeks):
            index = first + w * 7 + row
            if index > last:
                line.append(" ")
                continue
            entry = days_map.get(index)
            value = _metric_value(entry, metric) if entry else 0
            level = _level_of(value, steps)
            color = FAIL_COLOR if entry and int(entry.get("failed") or 0) and metric != "runs" else LEVEL_COLORS[level]
            line.append(glyphs[level], style=color)
        lines.append(line)

    legend = Text("     Less ", style="cw.muted")
    for level, color in enumerate(LEVEL_COLORS):
        legend.append(glyphs[level] + " ", style=color)
    legend.append("More", style="cw.muted")
    lines.append(legend)
    return lines


def _routes(run: dict[str, Any]) -> str:
    seen: list[str] = []
    for pair in run.get("pair_rows") or []:
        if not isinstance(pair, dict):
            continue
        label = f"{pair.get('src_provider') or '?'} -> {pair.get('dst_provider') or '?'}"
        if label not in seen:
            seen.append(label)
    return ", ".join(seen) if seen else "-"


def _features(run: dict[str, Any]) -> str:
    seen: list[str] = []
    for pair in run.get("pair_rows") or []:
        if not isinstance(pair, dict):
            continue
        feature = str(pair.get("feature") or "").strip()
        if feature and feature not in seen:
            seen.append(feature)
    return ", ".join(seen) if seen else "-"


def _counts(run: dict[str, Any]) -> str:
    bits = []
    for key, sign in (("added", "+"), ("removed", "-"), ("updated", "~")):
        value = int(run.get(key) or 0)
        if value:
            bits.append(f"{sign}{value}")
    return " ".join(bits) if bits else "0"


RUN_COLUMNS = [
    ("TIME", lambda r: datetime.fromtimestamp(int(r.get("started_at") or 0)).strftime("%H:%M") if r.get("started_at") else "-"),
    ("STATUS", lambda r: "error" if int(r.get("errors") or 0) else str(r.get("status") or "-")),
    ("ROUTE", _routes),
    ("FEATURES", _features),
    ("CHANGES", _counts),
    ("DUR", lambda r: fmt_duration(r.get("duration")) if r.get("duration") else "-"),
]


@events_app.command("status")
def events_status(ctx: typer.Context) -> None:
    """Show the state of the events archive."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/events/status"))
    if state.out.json_mode:
        state.out.data(payload)
        return
    state.out.kv(
        [(key, value) for key, value in payload.items() if not isinstance(value, (dict, list))],
        title="Events archive",
    )


@events_app.command("recent")
def events_recent(
    ctx: typer.Context,
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
    offset: int = typer.Option(0, "--offset", help="Skip this many rows."),
    domain: str = typer.Option("sync", "--domain", "-d", help="sync or scrobble."),
    visibility: str = typer.Option("open", "--visibility", help="open, all or acknowledged."),
    view: str = typer.Option("groups", "--view", help="groups or events."),
) -> None:
    """Show recent activity from the archive."""
    state: Ctx = ctx.obj
    params = {"limit": limit, "offset": offset, "domain": domain, "visibility": visibility, "view": view}
    payload = state.get("/api/events/recent", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "groups", "items", "events")
    columns = GROUP_COLUMNS if view == "groups" else EVENT_COLUMNS
    state.out.records(rows, columns, title=f"Recent {view}", empty="Nothing recorded yet.")


@events_app.command("search")
def events_search(
    ctx: typer.Context,
    query: str = typer.Argument("", help="Free text to search for."),
    event_type: str = typer.Option("", "--type", "-t", help="Event type filter."),
    provider: str = typer.Option("", "--provider", "-p", help="Provider filter."),
    feature: str = typer.Option("", "--feature", help="Feature filter."),
    domain: str = typer.Option("", "--domain", "-d", help="sync or scrobble."),
    visibility: str = typer.Option("", "--visibility", help="open, all or acknowledged."),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
    offset: int = typer.Option(0, "--offset", help="Skip this many rows."),
) -> None:
    """Search events."""
    state: Ctx = ctx.obj
    params = _filters(query, event_type, provider, feature, domain, limit, offset, visibility)
    payload = state.get("/api/events/search", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "events", "items", "results")
    state.out.records(rows, EVENT_COLUMNS, title=f"Events ({len(rows)})", empty="Nothing matched.")


@events_app.command("groups")
def events_groups(
    ctx: typer.Context,
    query: str = typer.Argument("", help="Free text to search for."),
    event_type: str = typer.Option("", "--type", "-t", help="Event type filter."),
    provider: str = typer.Option("", "--provider", "-p", help="Provider filter."),
    feature: str = typer.Option("", "--feature", help="Feature filter."),
    domain: str = typer.Option("", "--domain", "-d", help="sync or scrobble."),
    visibility: str = typer.Option("", "--visibility", help="open, all or acknowledged."),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
    offset: int = typer.Option(0, "--offset", help="Skip this many rows."),
) -> None:
    """List event groups."""
    state: Ctx = ctx.obj
    params = _filters(query, event_type, provider, feature, domain, limit, offset, visibility)
    payload = state.get("/api/events/groups", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "groups", "items")
    state.out.records(rows, GROUP_COLUMNS, title=f"Groups ({len(rows)})", empty="No groups.")


@events_app.command("show")
def events_show(
    ctx: typer.Context,
    group_id: int = typer.Argument(..., help="Group id from 'cw events groups'."),
    limit: int = typer.Option(100, "--limit", "-n", help="Maximum member events."),
) -> None:
    """Show one group and the events inside it."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get(f"/api/events/groups/{group_id}"))
    members = state.get(f"/api/events/groups/{group_id}/events", params={"limit": limit})
    if state.out.json_mode:
        state.out.data({"group": payload, "events": members})
        return
    group = as_dict(payload.get("group")) or payload
    state.out.kv(
        [
            ("Id", str(group.get("id") or group_id)),
            ("Type", str(group.get("event_type") or group.get("type") or "-")),
            ("Provider", str(group.get("provider") or "-")),
            ("Feature", str(group.get("feature") or "-")),
            ("When", _when(group)),
            ("Events", str(group.get("count") or group.get("events") or 0)),
            ("Acknowledged", state_text(coerce_bool(group.get("acknowledged"), False), on="yes", off="no")),
        ],
        title=str(group.get("title") or f"Group {group_id}"),
    )
    rows = _rows(members, "events", "items")
    if rows:
        state.out.print()
        state.out.records(rows, EVENT_COLUMNS, title="Events")


@events_app.command("run")
def events_run(
    ctx: typer.Context,
    run_id: str = typer.Argument(..., help="Run id, see 'cw sync status'."),
    limit: int = typer.Option(200, "--limit", "-n", help="Maximum rows."),
) -> None:
    """Show every event recorded for one sync run."""
    state: Ctx = ctx.obj
    payload = state.get(f"/api/events/run/{run_id}", params={"limit": limit})
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "events", "items")
    state.out.records(rows, EVENT_COLUMNS, title=f"Run {run_id}", empty="Nothing recorded for that run.")


@events_app.command("item")
def events_item(
    ctx: typer.Context,
    item_key: str = typer.Argument(..., help="Item key."),
    limit: int = typer.Option(100, "--limit", "-n", help="Maximum rows."),
) -> None:
    """Show the history of one item."""
    state: Ctx = ctx.obj
    payload = state.get(f"/api/events/item/{item_key}", params={"limit": limit})
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "events", "items")
    state.out.records(rows, EVENT_COLUMNS, title=item_key, empty="No history for that item.")


@events_app.command("stats")
def events_stats(
    ctx: typer.Context,
    window: str = typer.Option("30d", "--range", "-r", help="Time range, for example 7d or 30d."),
) -> None:
    """Show archive statistics."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/events/statistics", params={"range": window}))
    if state.out.json_mode:
        state.out.data(payload)
        return
    state.out.kv(
        [(key, value) for key, value in payload.items() if not isinstance(value, (dict, list))],
        title=f"Events over {window}",
    )
    buckets = payload.get("buckets") or payload.get("series")
    if isinstance(buckets, list) and buckets:
        state.out.print()
        state.out.records(
            [as_dict(b) for b in buckets][:30],
            [
                ("WHEN", lambda r: str(r.get("label") or r.get("bucket") or _when(r))),
                ("COUNT", lambda r: str(r.get("count") or r.get("total") or 0)),
            ],
            title="Buckets",
        )



@events_app.command("calendar")
def events_calendar(
    ctx: typer.Context,
    window: str = typer.Option("1y", "--range", "-r", help="3m, 6m, 1y or a day count."),
    metric: str = typer.Option("changes", "--metric", "-m", help="changes, runs or failures."),
) -> None:
    """Show sync activity per day as a calendar heatmap."""
    state: Ctx = ctx.obj
    if metric not in METRICS:
        raise CLIError(f"Unknown metric {metric!r}", hint="Use changes, runs or failures")
    days = _range_days(window)
    tz = _tz_offset()
    payload = as_dict(state.get("/api/events/calendar", params={"days": days, "tz": tz}))
    if state.out.json_mode:
        state.out.data(payload)
        return
    if payload.get("ok") is False:
        raise CLIError(error_text(payload, "Calendar unavailable"))

    rows = [as_dict(row) for row in (payload.get("days") or [])]
    days_map = {int(row.get("d") or 0): row for row in rows}
    totals = as_dict(payload.get("totals"))
    last = _day_index(time.time(), tz)
    first = last - days + 1
    first -= (_index_date(first, tz).weekday())

    state.out.kv(
        [
            ("Sync runs", str(totals.get("runs") or 0)),
            ("With errors", str(totals.get("failed") or 0)),
            ("Changes", f"{totals.get('changes') or 0} (+{totals.get('added') or 0} / -{totals.get('removed') or 0} / ~{totals.get('updated') or 0})"),
            ("Active days", f"{totals.get('active_days') or 0} of {days}"),
            ("Scope", "current profile" if payload.get("scoped") else "all profiles"),
        ],
        title=f"Sync activity ({metric})",
    )
    state.out.print()
    glyphs = _glyphs(getattr(state.out.console, "encoding", "") or "")
    for line in _heatmap(days_map, first, last, metric, glyphs):
        state.out.print(line)
    if rows:
        newest = _index_date(max(days_map), tz).isoformat()
        state.out.print()
        state.out.print(Text(f"Latest activity {newest}, see: cw events day {newest}", style="cw.muted"))


@events_app.command("day")
def events_day(
    ctx: typer.Context,
    when: str = typer.Argument("today", help="YYYY-MM-DD, today or yesterday."),
    limit: int = typer.Option(200, "--limit", "-n", help="Maximum runs."),
) -> None:
    """Show the sync runs recorded on one day."""
    state: Ctx = ctx.obj
    tz = _tz_offset()
    index = _parse_day(when, tz)
    since, until = _index_bounds(index, tz)
    payload = as_dict(state.get("/api/events/calendar/day", params={"since": since, "until": until, "limit": limit}))
    if state.out.json_mode:
        state.out.data(payload)
        return
    if payload.get("ok") is False:
        raise CLIError(error_text(payload, "Cannot read that day"))

    runs = [as_dict(row) for row in (payload.get("runs") or [])]
    changes = sum(int(row.get("changes") or 0) for row in runs)
    failed = sum(1 for row in runs if int(row.get("errors") or 0))
    label = _index_date(index, tz).isoformat()
    title = f"{label} - {len(runs)} runs, {changes} changes" + (f", {failed} with errors" if failed else "")
    state.out.records(runs, RUN_COLUMNS, title=title, empty="No sync runs on that day.")


@events_app.command("ack")
def events_ack(
    ctx: typer.Context,
    group_id: int = typer.Argument(..., help="Group id."),
    undo: bool = typer.Option(False, "--undo", help="Unacknowledge instead."),
) -> None:
    """Acknowledge a group so it drops out of the open view."""
    state: Ctx = ctx.obj
    state.require_service("Acknowledging events")
    action = "unacknowledge" if undo else "acknowledge"
    result = as_dict(state.post(f"/api/events/groups/{group_id}/{action}"))
    if result.get("ok") is False:
        found = result.get("found")
        if found is False:
            raise CLIError(f"No event group {group_id}", exit_code=EXIT_NOT_FOUND)
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Group {group_id} {'unacknowledged' if undo else 'acknowledged'}.")


@events_app.command("clear")
def events_clear(
    ctx: typer.Context,
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Clear the events archive."""
    state: Ctx = ctx.obj
    state.require_service("Clearing the events archive")
    if not yes and not typer.confirm("Clear the whole events archive?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.post("/api/events/clear"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Clear rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Events archive cleared.")


def register(app: typer.Typer) -> None:
    app.add_typer(events_app, name="events")
