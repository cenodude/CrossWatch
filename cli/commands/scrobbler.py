# /cli/commands/scrobbler.py
# CrossWatch - CLI scrobbler management commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_USAGE, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text

scrobbler_app = typer.Typer(help="Scrobbler routes and webhooks.", no_args_is_help=True)
route_app = typer.Typer(help="Scrobble routes.", no_args_is_help=True)
webhook_app = typer.Typer(help="Scrobble webhooks.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    block = as_dict(payload)
    for key in keys:
        found = block.get(key)
        if isinstance(found, list):
            return [as_dict(i) for i in found]
    if isinstance(payload, list):
        return [as_dict(i) for i in payload]
    return []


def _fields(raw: list[str]) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for item in raw or []:
        key, sep, value = str(item).partition("=")
        key = key.strip()
        if not sep or not key:
            raise CLIError(f"--field expects key=value, got '{item}'", exit_code=EXIT_USAGE)
        body[key] = value
    return body


@scrobbler_app.command("overview")
def scrobbler_overview(ctx: typer.Context) -> None:
    """Show the scrobbler setup."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/scrobbler/overview"))
    if state.out.json_mode:
        state.out.data(payload)
        return
    state.out.kv(
        [(key, value) for key, value in payload.items() if not isinstance(value, (dict, list))],
        title="Scrobbler",
    )
    routes = _rows(payload, "routes")
    if routes:
        state.out.print()
        state.out.records(
            routes,
            [
                ("ID", lambda r: str(r.get("id") or r.get("route_id") or "-")),
                ("SOURCE", lambda r: str(r.get("source") or r.get("provider") or "-")),
                ("SINK", lambda r: str(r.get("sink") or "-")),
                ("ENABLED", lambda r: state_text(coerce_bool(r.get("enabled"), False), on="yes", off="no")),
            ],
            title="Routes",
        )


@scrobbler_app.command("event-routes")
def scrobbler_event_routes(ctx: typer.Context) -> None:
    """Show where scrobble events are delivered."""
    state: Ctx = ctx.obj
    payload = state.get("/api/scrobble/event_routes")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "routes", "items")
    state.out.records(
        rows,
        [
            ("EVENT", lambda r: str(r.get("event") or r.get("type") or "-")),
            ("SOURCE", lambda r: str(r.get("source") or r.get("provider") or "-")),
            ("SINK", lambda r: str(r.get("sink") or r.get("target") or "-")),
        ],
        title="Event routes",
        empty="No event routes.",
    )


@route_app.command("add")
def route_add(
    ctx: typer.Context,
    field: list[str] = typer.Option([], "--field", "-F", help="Route setting as key=value. Repeatable."),
) -> None:
    """Add a scrobble route."""
    state: Ctx = ctx.obj
    state.require_service("Adding a scrobble route")
    body = _fields(field)
    if not body:
        raise CLIError("A route needs fields", hint="For example --field source=PLEX --field sink=TRAKT", exit_code=EXIT_USAGE)
    result = as_dict(state.post("/api/scrobbler/routes", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Route added{': ' + str(result.get('id')) if result.get('id') else '.'}")


@route_app.command("set")
def route_set(
    ctx: typer.Context,
    route_id: str = typer.Argument(..., help="Route id."),
    field: list[str] = typer.Option([], "--field", "-F", help="Route setting as key=value. Repeatable."),
) -> None:
    """Update a scrobble route."""
    state: Ctx = ctx.obj
    state.require_service("Updating a scrobble route")
    body = _fields(field)
    if not body:
        raise CLIError("Nothing to change", hint="Pass at least one --field key=value", exit_code=EXIT_USAGE)
    result = as_dict(state.put(f"/api/scrobbler/routes/{route_id}", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Updated route {route_id}")


@route_app.command("delete")
def route_delete(
    ctx: typer.Context,
    route_id: str = typer.Argument(..., help="Route id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a scrobble route."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a scrobble route")
    if not yes and not typer.confirm(f"Delete route {route_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/scrobbler/routes/{route_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted route {route_id}")


@webhook_app.command("urls")
def webhook_urls(ctx: typer.Context) -> None:
    """Show the webhook URLs to paste into your media server."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/webhooks/urls"))
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = [[key, str(value)] for key, value in payload.items() if not isinstance(value, (dict, list))]
    state.out.table(["NAME", "URL"], rows, title="Webhook URLs", empty="No webhook URLs yet.")


@webhook_app.command("regenerate")
def webhook_regenerate(
    ctx: typer.Context,
    profile: str = typer.Option("", "--profile", help="Regenerate one profile instead of all."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Roll the webhook tokens. Existing URLs stop working."""
    state: Ctx = ctx.obj
    state.require_service("Regenerating webhooks")
    if not yes:
        state.out.warn("Existing webhook URLs will stop working.")
        if not typer.confirm("Regenerate?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    if profile.strip():
        result = as_dict(state.post("/api/scrobbler/webhooks/profile/regenerate", json_body={"profile": profile.strip()}))
    else:
        result = as_dict(state.post("/api/webhooks/regenerate"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Webhook URLs regenerated.")


@webhook_app.command("cleanup-legacy")
def webhook_cleanup(
    ctx: typer.Context,
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Strip legacy inherited webhook sinks."""
    state: Ctx = ctx.obj
    state.require_service("Cleaning legacy webhooks")
    if not yes and not typer.confirm("Remove legacy webhook settings?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.post("/api/scrobbler/webhooks/cleanup-legacy"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Legacy webhook settings removed.")


def register(app: typer.Typer) -> None:
    scrobbler_app.add_typer(route_app, name="route")
    scrobbler_app.add_typer(webhook_app, name="webhook")
    app.add_typer(scrobbler_app, name="scrobbler")
