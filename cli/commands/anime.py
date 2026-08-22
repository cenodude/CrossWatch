# /cli/commands/anime.py
# CrossWatch - CLI anime mapping commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_USAGE, CLIError
from .._util import as_dict, error_text, fmt_ts

anime_app = typer.Typer(help="Anime id mapping and custom overrides.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    block = as_dict(payload)
    for key in keys:
        found = block.get(key)
        if isinstance(found, list):
            return [as_dict(i) for i in found]
    if isinstance(payload, list):
        return [as_dict(i) for i in payload]
    return []


@anime_app.command("status")
def anime_status(ctx: typer.Context) -> None:
    """Show the mapping index state."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/anime-mapping/status"))
    if state.out.json_mode:
        state.out.data(payload)
        return
    state.out.kv(
        [
            ("Status", str(payload.get("status") or "-")),
            ("Schema", str(payload.get("schema_version") or payload.get("expected_schema_version") or "-")),
            ("Sources", f"{int(payload.get('source_count') or 0):,}"),
            ("Edges", f"{int(payload.get('edge_count') or 0):,}"),
            ("Updated", fmt_ts(payload.get("updated_at") or payload.get("built_at"))),
            ("Enabled", str(payload.get("enabled"))),
        ],
        title="Anime mapping",
    )
    if payload.get("message"):
        state.out.info(str(payload.get("message")))


@anime_app.command("update")
def anime_update(
    ctx: typer.Context,
    rebuild: bool = typer.Option(False, "--rebuild", help="Rebuild the index instead of fetching an update."),
) -> None:
    """Fetch new mapping data, or rebuild the index."""
    state: Ctx = ctx.obj
    state.require_service("Updating the anime mapping")
    path = "/api/anime-mapping/rebuild-index" if rebuild else "/api/anime-mapping/update"
    result = as_dict(state.post(path))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Update rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success("Index rebuilt." if rebuild else "Mapping updated.")
    for key in ("source_count", "edge_count", "status"):
        if key in result:
            state.out.info(f"{key}: {result.get(key)}")


@anime_app.command("overrides")
def anime_overrides(
    ctx: typer.Context,
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
) -> None:
    """List custom mapping overrides."""
    state: Ctx = ctx.obj
    payload = state.get("/api/anime-mapping/overrides")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "overrides", "rules", "items")
    state.out.records(
        rows[: max(1, limit)],
        [
            ("ID", lambda r: str(r.get("id") or r.get("rule_id") or "-")),
            (
                "MATCH",
                lambda r: f"{r.get('match_provider') or '-'}:{r.get('match_id') or '-'}",
            ),
            (
                "TARGET",
                lambda r: f"{r.get('target_namespace') or '-'}:{r.get('target_id') or '-'}",
            ),
            ("TITLE", lambda r: str(r.get("title") or "-")[:28]),
            ("NOTE", lambda r: str(r.get("note") or r.get("comment") or "-")[:24]),
        ],
        title=f"Overrides ({len(rows)})",
        empty="No custom overrides.",
    )


@anime_app.command("add-override")
def anime_add_override(
    ctx: typer.Context,
    field: list[str] = typer.Option(
        [],
        "--field",
        "-F",
        help="Rule field as key=value. Needs match_provider, match_id, target_namespace and target_id.",
    ),
) -> None:
    """Add a custom mapping override."""
    state: Ctx = ctx.obj
    state.require_service("Adding an override")
    body: dict[str, Any] = {}
    for item in field:
        key, sep, value = str(item).partition("=")
        if not sep or not key.strip():
            raise CLIError(f"--field expects key=value, got '{item}'", exit_code=EXIT_USAGE)
        body[key.strip()] = value
    if not body:
        raise CLIError(
            "An override needs at least one field",
            hint="For example --field match_provider=tvdb --field match_id=81472 --field target_namespace=anidb --field target_id=4563",
            exit_code=EXIT_USAGE,
        )
    result = as_dict(state.post("/api/anime-mapping/overrides", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Override rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Override added{': ' + str(result.get('id')) if result.get('id') else '.'}")


@anime_app.command("delete-override")
def anime_delete_override(
    ctx: typer.Context,
    rule_id: str = typer.Argument(..., help="Rule id from 'cw anime overrides'."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a custom mapping override."""
    state: Ctx = ctx.obj
    state.require_service("Deleting an override")
    if not yes and not typer.confirm(f"Delete override {rule_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/anime-mapping/overrides/{rule_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted {rule_id}")


@anime_app.command("export")
def anime_export(ctx: typer.Context) -> None:
    """Print the overrides as JSON."""
    state: Ctx = ctx.obj
    payload = state.get("/api/anime-mapping/overrides/export")
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    state.out.raw(_json.dumps(payload, indent=2, default=str))


@anime_app.command("search")
def anime_search(
    ctx: typer.Context,
    query: str = typer.Argument(..., help="Title to look up on SIMKL."),
    limit: int = typer.Option(25, "--limit", "-n", help="Maximum rows."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
) -> None:
    """Search SIMKL for an anime, to build a mapping."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"q": query, "limit": limit}
    if instance.strip():
        params["instance"] = instance.strip()
    payload = state.get("/api/anime-mapping/simkl/search", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "results", "items")
    state.out.records(
        rows,
        [
            ("TITLE", lambda r: str(r.get("title") or "-")[:44]),
            ("YEAR", lambda r: str(r.get("year") or "-")),
            ("TYPE", lambda r: str(r.get("type") or "-")),
            ("IDS", lambda r: ", ".join(f"{k}:{v}" for k, v in as_dict(r.get("ids")).items())[:44]),
        ],
        title=f"SIMKL results for {query}",
        empty="Nothing found.",
    )


def register(app: typer.Typer) -> None:
    app.add_typer(anime_app, name="anime")
