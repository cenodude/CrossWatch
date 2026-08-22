# /cli/commands/playlist.py
# CrossWatch - CLI playlist commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_USAGE, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text, fmt_ts

playlist_app = typer.Typer(help="Playlist endpoints, mappings and rulesets.", no_args_is_help=True)
endpoint_app = typer.Typer(help="Playlist endpoints.", no_args_is_help=True)
mapping_app = typer.Typer(help="Playlist mappings.", no_args_is_help=True)
ruleset_app = typer.Typer(help="Playlist rulesets.", no_args_is_help=True)


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


@playlist_app.command("overview")
def playlist_overview(ctx: typer.Context) -> None:
    """Show the playlist setup."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/playlists/overview"))
    if state.out.json_mode:
        state.out.data(payload)
        return
    state.out.kv(
        [(key, value) for key, value in payload.items() if not isinstance(value, (dict, list))],
        title="Playlists",
    )


@playlist_app.command("providers")
def playlist_providers(ctx: typer.Context) -> None:
    """Show which providers can hold playlists."""
    state: Ctx = ctx.obj
    payload = state.get("/api/playlists/providers")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "providers", "items")
    if rows:
        state.out.records(
            rows,
            [
                ("PROVIDER", lambda r: str(r.get("name") or r.get("provider") or "-")),
                ("INSTANCE", lambda r: str(r.get("instance") or "-")),
            ],
            title="Playlist providers",
        )
        return
    block = as_dict(payload)
    names = block.get("providers")
    if isinstance(names, list):
        state.out.table(["PROVIDER"], [[str(n)] for n in names], title="Playlist providers")
        return
    state.out.kv([(k, v) for k, v in block.items() if not isinstance(v, (dict, list))], title="Playlist providers")


@playlist_app.command("resources")
def playlist_resources(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
) -> None:
    """List the playlists a provider holds."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"provider": provider.strip().upper()}
    if instance.strip():
        params["instance"] = instance.strip()
    payload = state.get("/api/playlists/resources", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "resources", "items", "playlists")
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or r.get("key") or "-")[:24]),
            ("NAME", lambda r: str(r.get("name") or r.get("title") or "-")[:40]),
            ("ITEMS", lambda r: str(r.get("count") or r.get("items") or "-")),
        ],
        title=f"{provider.upper()} playlists",
        empty="No playlists there.",
    )


@playlist_app.command("activity")
def playlist_activity(ctx: typer.Context) -> None:
    """Show recent playlist runs."""
    state: Ctx = ctx.obj
    payload = state.get("/api/playlists/activity")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "activity", "items", "runs")
    state.out.records(
        rows,
        [
            ("WHEN", lambda r: fmt_ts(r.get("ts") or r.get("created_at") or r.get("finished_at"))),
            ("MAPPING", lambda r: str(r.get("mapping") or r.get("mapping_id") or "-")[:24]),
            ("RESULT", lambda r: str(r.get("result") or r.get("status") or "-")),
            ("CHANGES", lambda r: str(r.get("changes") or r.get("added") or "-")),
        ],
        title="Playlist activity",
        empty="No playlist runs yet.",
    )


@endpoint_app.command("list")
def endpoint_list(ctx: typer.Context) -> None:
    """List playlist endpoints."""
    state: Ctx = ctx.obj
    payload = state.get("/api/playlists/endpoints")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "endpoints", "items")
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or "-")),
            ("NAME", lambda r: str(r.get("name") or r.get("label") or "-")[:32]),
            ("PROVIDER", lambda r: str(r.get("provider") or "-")),
            ("ENABLED", lambda r: state_text(coerce_bool(r.get("enabled"), True), on="yes", off="no")),
        ],
        title="Playlist endpoints",
        empty="No endpoints configured.",
    )


@endpoint_app.command("add")
def endpoint_add(
    ctx: typer.Context,
    field: list[str] = typer.Option([], "--field", "-F", help="Endpoint setting as key=value. Repeatable."),
) -> None:
    """Add a playlist endpoint."""
    state: Ctx = ctx.obj
    state.require_service("Adding a playlist endpoint")
    body = _fields(field)
    if not body:
        raise CLIError("An endpoint needs fields", hint="For example --field provider=PLEX --field name=Favourites", exit_code=EXIT_USAGE)
    result = as_dict(state.post("/api/playlists/endpoints", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Endpoint added{': ' + str(result.get('id')) if result.get('id') else '.'}")


@endpoint_app.command("sync")
def endpoint_sync(
    ctx: typer.Context,
    endpoint_id: str = typer.Argument(..., help="Endpoint id."),
) -> None:
    """Refresh one endpoint from its provider."""
    state: Ctx = ctx.obj
    state.require_service("Syncing a playlist endpoint")
    result = as_dict(state.post(f"/api/playlists/endpoints/{endpoint_id}/sync"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Endpoint {endpoint_id} synced.")


@endpoint_app.command("delete")
def endpoint_delete(
    ctx: typer.Context,
    endpoint_id: str = typer.Argument(..., help="Endpoint id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a playlist endpoint."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a playlist endpoint")
    if not yes and not typer.confirm(f"Delete endpoint {endpoint_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/playlists/endpoints/{endpoint_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted endpoint {endpoint_id}")


@mapping_app.command("list")
def mapping_list(
    ctx: typer.Context,
    pair: str = typer.Option("", "--pair", "-p", help="Only mappings for this pair id."),
) -> None:
    """List playlist mappings."""
    state: Ctx = ctx.obj
    path = f"/api/playlists/pairs/{pair.strip()}/mappings" if pair.strip() else "/api/playlists/mappings"
    payload = state.get(path)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "mappings", "items")
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or "-")),
            ("NAME", lambda r: str(r.get("name") or r.get("label") or "-")[:30]),
            ("SOURCE", lambda r: str(r.get("source") or "-")),
            ("TARGET", lambda r: str(r.get("target") or "-")),
            ("ENABLED", lambda r: state_text(coerce_bool(r.get("enabled"), True), on="yes", off="no")),
        ],
        title="Playlist mappings",
        empty="No mappings configured.",
    )


@mapping_app.command("add")
def mapping_add(
    ctx: typer.Context,
    field: list[str] = typer.Option([], "--field", "-F", help="Mapping setting as key=value. Repeatable."),
) -> None:
    """Add a playlist mapping."""
    state: Ctx = ctx.obj
    state.require_service("Adding a playlist mapping")
    body = _fields(field)
    if not body:
        raise CLIError("A mapping needs fields", hint="See 'cw playlist endpoint list' for ids", exit_code=EXIT_USAGE)
    result = as_dict(state.post("/api/playlists/mappings", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Mapping added{': ' + str(result.get('id')) if result.get('id') else '.'}")


@mapping_app.command("run")
def mapping_run(
    ctx: typer.Context,
    mapping_id: str = typer.Argument(..., help="Mapping id."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report what would change, change nothing."),
) -> None:
    """Run one playlist mapping."""
    state: Ctx = ctx.obj
    state.require_service("Running a playlist mapping")
    result = as_dict(state.post(f"/api/playlists/mappings/{mapping_id}/run", params={"dry_run": dry_run}))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Run rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success("Dry run finished." if dry_run else "Mapping run finished.")
    for key in ("added", "removed", "updated", "skipped", "errors"):
        if key in result:
            state.out.info(f"{key}: {result.get(key)}")


@mapping_app.command("preview")
def mapping_preview(
    ctx: typer.Context,
    mapping_id: str = typer.Argument(..., help="Mapping id."),
) -> None:
    """Preview what a mapping would produce."""
    state: Ctx = ctx.obj
    payload = state.post(f"/api/playlists/mappings/{mapping_id}/preview")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "items", "preview")
    state.out.records(
        rows[:50],
        [
            ("TITLE", lambda r: str(r.get("title") or r.get("key") or "-")[:44]),
            ("TYPE", lambda r: str(r.get("type") or "-")),
            ("ACTION", lambda r: str(r.get("action") or r.get("change") or "-")),
        ],
        title=f"Preview {mapping_id}",
        empty="Nothing would change.",
    )


@mapping_app.command("result")
def mapping_result(
    ctx: typer.Context,
    mapping_id: str = typer.Argument(..., help="Mapping id."),
) -> None:
    """Show the last result for a mapping."""
    state: Ctx = ctx.obj
    payload = state.get(f"/api/playlists/mappings/{mapping_id}/result")
    if state.out.json_mode:
        state.out.data(payload)
        return
    block = as_dict(payload)
    state.out.kv(
        [(key, value) for key, value in block.items() if not isinstance(value, (dict, list))],
        title=f"Mapping {mapping_id}",
    )


@mapping_app.command("delete")
def mapping_delete(
    ctx: typer.Context,
    mapping_id: str = typer.Argument(..., help="Mapping id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a playlist mapping."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a playlist mapping")
    if not yes and not typer.confirm(f"Delete mapping {mapping_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/playlists/mappings/{mapping_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted mapping {mapping_id}")


@ruleset_app.command("list")
def ruleset_list(ctx: typer.Context) -> None:
    """List playlist rulesets."""
    state: Ctx = ctx.obj
    payload = state.get("/api/playlists/rulesets")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "rulesets", "items")
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or "-")),
            ("NAME", lambda r: str(r.get("name") or "-")[:34]),
            ("RULES", lambda r: str(len(r.get("rules") or []) or r.get("rule_count") or 0)),
        ],
        title="Rulesets",
        empty="No rulesets.",
    )


@ruleset_app.command("show")
def ruleset_show(ctx: typer.Context, ruleset_id: str = typer.Argument(..., help="Ruleset id.")) -> None:
    """Show one ruleset."""
    state: Ctx = ctx.obj
    payload = state.get(f"/api/playlists/rulesets/{ruleset_id}")
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(
        Syntax(_json.dumps(payload, indent=2, default=str), "json", theme="ansi_dark", background_color="default")
    )


@ruleset_app.command("delete")
def ruleset_delete(
    ctx: typer.Context,
    ruleset_id: str = typer.Argument(..., help="Ruleset id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a ruleset."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a ruleset")
    if not yes and not typer.confirm(f"Delete ruleset {ruleset_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/playlists/rulesets/{ruleset_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted ruleset {ruleset_id}")


def register(app: typer.Typer) -> None:
    playlist_app.add_typer(endpoint_app, name="endpoint")
    playlist_app.add_typer(mapping_app, name="mapping")
    playlist_app.add_typer(ruleset_app, name="ruleset")
    app.add_typer(playlist_app, name="playlist")
