# /cli/commands/events.py
# CrossWatch - CLI events archive commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_NOT_FOUND, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text, fmt_ts, rows_from_payload

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
