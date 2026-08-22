# /cli/commands/insights.py
# CrossWatch - CLI insights, statistics and activity commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._util import as_dict, error_text, fmt_ts, rows_from_payload

activity_app = typer.Typer(help="The recent activity log.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    return rows_from_payload(payload, *keys)


def _flat_kv(payload: dict[str, Any]) -> list[tuple[str, Any]]:
    return [(key, value) for key, value in payload.items() if not isinstance(value, (dict, list))]


ACTIVITY_COLUMNS = [
    ("WHEN", lambda r: fmt_ts(r.get("ts") or r.get("watched_at") or r.get("created_at"))),
    ("TITLE", lambda r: str(r.get("title") or "-")[:44]),
    ("TYPE", lambda r: str(r.get("type") or r.get("media_type") or "-")),
    ("PROVIDER", lambda r: str(r.get("provider") or "-")),
    ("STATUS", lambda r: str(r.get("status") or r.get("action") or "-")),
]


@activity_app.command("recent")
def activity_recent(
    ctx: typer.Context,
    limit: int = typer.Option(20, "--limit", "-n", help="Maximum rows."),
) -> None:
    """Show what happened recently."""
    state: Ctx = ctx.obj
    payload = state.get("/api/activity/recent", params={"limit": limit})
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "items", "activity", "recent")
    state.out.records(rows, ACTIVITY_COLUMNS, title="Recent activity", empty="Nothing recorded.")


@activity_app.command("history")
def activity_history(
    ctx: typer.Context,
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
    offset: int = typer.Option(0, "--offset", help="Skip this many rows."),
    media_type: str = typer.Option("all", "--type", "-t", help="movie, episode or all."),
    status: str = typer.Option("all", "--status", help="Status filter."),
    query: str = typer.Option("", "--search", "-q", help="Free text filter."),
) -> None:
    """Show the activity history."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"limit": limit, "offset": offset, "media_type": media_type, "status": status}
    if query.strip():
        params["q"] = query.strip()
    payload = state.get("/api/activity/history", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "items", "history")
    state.out.records(rows, ACTIVITY_COLUMNS, title=f"History ({len(rows)})", empty="Nothing recorded.")


@activity_app.command("clear")
def activity_clear(
    ctx: typer.Context,
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Clear the activity history."""
    state: Ctx = ctx.obj
    state.require_service("Clearing activity history")
    if not yes and not typer.confirm("Clear the activity history?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete("/api/activity/history"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Clear rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Activity history cleared.")


def register(app: typer.Typer) -> None:
    app.add_typer(activity_app, name="activity")

    @app.command("insights")
    def insights_cmd(
        ctx: typer.Context,
        history: int = typer.Option(3, "--history", help="How many past runs to include."),
        events: bool = typer.Option(True, "--events/--no-events", help="Include event derived numbers."),
    ) -> None:
        """Show the insights the dashboard is built from."""
        state: Ctx = ctx.obj
        params = {"history": history, "include_events": 1 if events else 0}
        payload = as_dict(state.get("/api/insights", params=params))
        if state.out.json_mode:
            state.out.data(payload)
            return
        state.out.kv(_flat_kv(payload), title="Insights")
        for key, value in payload.items():
            block = as_dict(value)
            if block and all(not isinstance(v, (dict, list)) for v in block.values()):
                state.out.print()
                state.out.kv(_flat_kv(block), title=key)

    @app.command("stats")
    def stats_cmd(
        ctx: typer.Context,
        raw: bool = typer.Option(False, "--raw", help="Show the unrolled numbers."),
    ) -> None:
        """Show sync statistics."""
        state: Ctx = ctx.obj
        payload = as_dict(state.get("/api/stats/raw" if raw else "/api/stats"))
        if state.out.json_mode:
            state.out.data(payload)
            return
        state.out.kv(_flat_kv(payload), title="Statistics")
        for key, value in payload.items():
            block = as_dict(value)
            if block and all(not isinstance(v, (dict, list)) for v in block.values()):
                state.out.print()
                state.out.kv(_flat_kv(block), title=key)
