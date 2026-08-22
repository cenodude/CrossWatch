# /cli/commands/maintenance.py
# CrossWatch - CLI maintenance commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_USAGE, CLIError
from .._util import as_dict, error_text

maintenance_app = typer.Typer(help="Database health, caches and housekeeping.", no_args_is_help=True)

CACHES = {
    "all": "/api/maintenance/clear-cache",
    "metadata": "/api/maintenance/clear-metadata-cache",
    "provider-sync": "/api/maintenance/clear-provider-sync-cache",
    "activity-log": "/api/maintenance/clear-activity-log",
    "scrobbles": "/api/maintenance/clear-recent-scrobbles",
    "state": "/api/maintenance/clear-state",
}


def _flat(state: Ctx, payload: Any, title: str) -> None:
    block = as_dict(payload)
    if state.out.json_mode:
        state.out.data(block)
        return
    state.out.kv(
        [(key, value) for key, value in block.items() if not isinstance(value, (dict, list))],
        title=title,
    )
    for key, value in block.items():
        if isinstance(value, dict) and value:
            state.out.print()
            state.out.kv(
                [(k, v) for k, v in value.items() if not isinstance(v, (dict, list))],
                title=key,
            )


@maintenance_app.command("database")
def maintenance_database(ctx: typer.Context) -> None:
    """Check the runtime database."""
    state: Ctx = ctx.obj
    _flat(state, state.post("/api/maintenance/database-health"), "Database health")


@maintenance_app.command("events")
def maintenance_events(
    ctx: typer.Context,
    optimize: bool = typer.Option(False, "--optimize", help="Optimize the archive."),
    rebuild: bool = typer.Option(False, "--rebuild", help="Rebuild the archive index."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Check, optimize or rebuild the events archive."""
    state: Ctx = ctx.obj
    if optimize and rebuild:
        raise CLIError("Pick one of --optimize or --rebuild", exit_code=EXIT_USAGE)
    if rebuild:
        state.require_service("Rebuilding the events archive")
        if not yes and not typer.confirm("Rebuild the events index?", default=False):
            raise CLIError("Cancelled", exit_code=0)
        _flat(state, state.post("/api/maintenance/events-rebuild"), "Events rebuild")
        return
    if optimize:
        state.require_service("Optimizing the events archive")
        _flat(state, state.post("/api/maintenance/events-optimize"), "Events optimize")
        return
    _flat(state, state.post("/api/maintenance/events-health"), "Events health")


@maintenance_app.command("cache")
def maintenance_cache(
    ctx: typer.Context,
    what: str = typer.Argument("all", help=f"One of: {', '.join(sorted(CACHES))}."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Clear a cache."""
    state: Ctx = ctx.obj
    state.require_service("Clearing a cache")
    key = str(what).strip().lower()
    endpoint = CACHES.get(key)
    if endpoint is None:
        raise CLIError(f"Unknown cache '{what}'", hint=f"Try: {', '.join(sorted(CACHES))}", exit_code=EXIT_USAGE)
    if not yes and not typer.confirm(f"Clear the {key} cache?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.post(endpoint))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Clear rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Cleared the {key} cache.")


@maintenance_app.command("provider-cache")
def maintenance_provider_cache(ctx: typer.Context) -> None:
    """Show what the provider cache holds."""
    state: Ctx = ctx.obj
    payload = state.get("/api/maintenance/provider-cache")
    if state.out.json_mode:
        state.out.data(payload)
        return
    block = as_dict(payload)
    rows = block.get("providers") or block.get("items")
    if isinstance(rows, list):
        state.out.records(
            [as_dict(r) for r in rows],
            [
                ("PROVIDER", lambda r: str(r.get("provider") or r.get("name") or "-")),
                ("ENTRIES", lambda r: str(r.get("entries") or r.get("count") or "-")),
                ("SIZE", lambda r: str(r.get("size") or r.get("size_bytes") or "-")),
            ],
            title="Provider cache",
        )
        return
    _flat(state, payload, "Provider cache")


@maintenance_app.command("state-file")
def maintenance_state_file(
    ctx: typer.Context,
    prune: bool = typer.Option(False, "--prune", help="Drop stale entries."),
    compact: bool = typer.Option(False, "--compact", help="Rewrite the file smaller."),
) -> None:
    """Prune or compact state.json."""
    state: Ctx = ctx.obj
    state.require_service("Working on the state file")
    if not prune and not compact:
        raise CLIError("Pick --prune or --compact", exit_code=EXIT_USAGE)
    if prune:
        _flat(state, state.post("/api/maintenance/state-file/prune"), "State file prune")
    if compact:
        _flat(state, state.post("/api/maintenance/state-file/compact"), "State file compact")


@maintenance_app.command("tracker")
def maintenance_tracker(
    ctx: typer.Context,
    instance: str = typer.Option("default", "--instance", "-i", help="Provider instance id."),
    clear: bool = typer.Option(False, "--clear", help="Clear the tracker state."),
    snapshots: bool = typer.Option(False, "--snapshots", help="With --clear, also drop its snapshots."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Show or clear the CrossWatch tracker."""
    state: Ctx = ctx.obj
    if not clear:
        _flat(state, state.get("/api/maintenance/crosswatch-tracker", params={"provider_instance": instance}), "Tracker")
        return
    state.require_service("Clearing the tracker")
    if not yes and not typer.confirm(f"Clear tracker state for {instance}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    body = {"clear_state": True, "clear_snapshots": bool(snapshots), "provider_instance": instance}
    result = as_dict(state.post("/api/maintenance/crosswatch-tracker/clear", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Clear rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Tracker cleared.")


@maintenance_app.command("reset-stats")
def maintenance_reset_stats(
    ctx: typer.Context,
    recalc: bool = typer.Option(False, "--recalc", help="Recalculate instead of only clearing."),
    purge_reports: bool = typer.Option(False, "--purge-reports", help="Also delete sync reports."),
    purge_insights: bool = typer.Option(False, "--purge-insights", help="Also delete insights."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Reset the statistics."""
    state: Ctx = ctx.obj
    state.require_service("Resetting statistics")
    if not yes and not typer.confirm("Reset statistics?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    body = {
        "recalc": bool(recalc),
        "purge_reports": bool(purge_reports),
        "purge_insights": bool(purge_insights),
    }
    _flat(state, state.post("/api/maintenance/reset-stats", json_body=body), "Statistics reset")


@maintenance_app.command("reset-watching")
def maintenance_reset_watching(ctx: typer.Context) -> None:
    """Clear the currently watching card."""
    state: Ctx = ctx.obj
    state.require_service("Resetting currently watching")
    result = as_dict(state.post("/api/maintenance/reset-currently-watching"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Reset rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Currently watching cleared.")


@maintenance_app.command("support")
def maintenance_support(
    ctx: typer.Context,
    scopes: bool = typer.Option(False, "--scopes", help="List what a support bundle can include."),
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
) -> None:
    """Show support diagnostics."""
    state: Ctx = ctx.obj
    if scopes:
        payload = state.get("/api/maintenance/support/scopes")
        if state.out.json_mode:
            state.out.data(payload)
            return
        block = as_dict(payload)
        rows = block.get("scopes") or block.get("items")
        if isinstance(rows, list):
            state.out.table(["SCOPE"], [[str(r)] for r in rows], title="Support scopes")
            return
        _flat(state, payload, "Support scopes")
        return
    params = {"pairs": [p.strip() for p in pairs.split(",") if p.strip()]} if pairs.strip() else None
    _flat(state, state.get("/api/maintenance/support/state", params=params), "Support state")


@maintenance_app.command("restart")
def maintenance_restart(
    ctx: typer.Context,
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Restart the CrossWatch service."""
    state: Ctx = ctx.obj
    state.require_service("Restarting CrossWatch")
    if not yes:
        state.out.warn("This restarts the running service.")
        if not typer.confirm("Restart now?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    try:
        state.post("/api/maintenance/restart")
    except CLIError as err:
        if int(getattr(err, "status", 0) or 0) or "Cannot reach" not in err.message:
            raise
    if state.out.json_mode:
        state.out.data({"ok": True, "restarting": True})
        return
    state.out.success("Restart requested.")


def register(app: typer.Typer) -> None:
    app.add_typer(maintenance_app, name="maintenance")
