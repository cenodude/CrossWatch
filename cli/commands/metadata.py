# /cli/commands/metadata.py
# CrossWatch - CLI metadata and manual entry commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_USAGE, CLIError
from .._util import as_dict, error_text

metadata_app = typer.Typer(help="Look up titles and ids.", no_args_is_help=True)
manual_app = typer.Typer(help="Record a watch by hand.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    block = as_dict(payload)
    for key in keys:
        found = block.get(key)
        if isinstance(found, list):
            return [as_dict(i) for i in found]
    if isinstance(payload, list):
        return [as_dict(i) for i in payload]
    return []


def _ids(row: dict[str, Any]) -> str:
    ids = as_dict(row.get("ids"))
    if not ids:
        ids = {k: v for k, v in row.items() if k in ("imdb", "tmdb", "tvdb", "trakt", "simkl", "anidb")}
    return ", ".join(f"{k}:{v}" for k, v in ids.items() if v)[:50] or "-"


@metadata_app.command("search")
def metadata_search(
    ctx: typer.Context,
    query: str = typer.Argument(..., help="Title to search for."),
    media_type: str = typer.Option("movie", "--type", "-t", help="movie or tv."),
    year: int = typer.Option(0, "--year", "-y", help="Release year."),
    limit: int = typer.Option(10, "--limit", "-n", help="Maximum rows."),
) -> None:
    """Search for a title."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"q": query, "typ": media_type, "limit": limit}
    if year:
        params["year"] = year
    payload = state.get("/api/metadata/search", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "results", "items")
    state.out.records(
        rows,
        [
            ("TITLE", lambda r: str(r.get("title") or r.get("name") or "-")[:40]),
            ("YEAR", lambda r: str(r.get("year") or "-")),
            ("TYPE", lambda r: str(r.get("type") or media_type)),
            ("IDS", _ids),
        ],
        title=f"Results for {query}",
        empty="Nothing found.",
    )


@metadata_app.command("resolve")
def metadata_resolve(
    ctx: typer.Context,
    ids: list[str] = typer.Argument(..., help="Ids as key=value, for example imdb=tt0111161."),
    media_type: str = typer.Option("movie", "--type", "-t", help="movie or tv."),
) -> None:
    """Resolve ids to a canonical item."""
    state: Ctx = ctx.obj
    parsed: dict[str, Any] = {}
    for item in ids:
        key, sep, value = str(item).partition("=")
        if not sep or not key.strip():
            raise CLIError(f"Ids are key=value, got '{item}'", exit_code=EXIT_USAGE)
        parsed[key.strip()] = value
    payload = state.post("/api/metadata/resolve", json_body={"ids": parsed, "type": media_type})
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(
        Syntax(_json.dumps(payload, indent=2, default=str), "json", theme="ansi_dark", background_color="default")
    )


@metadata_app.command("providers")
def metadata_providers(ctx: typer.Context) -> None:
    """Show which metadata providers are available."""
    state: Ctx = ctx.obj
    payload = state.get("/api/metadata/providers")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "providers", "items")
    if rows:
        state.out.records(
            rows,
            [
                ("PROVIDER", lambda r: str(r.get("name") or r.get("provider") or "-")),
                ("ENABLED", lambda r: str(r.get("enabled"))),
                ("NOTE", lambda r: str(r.get("note") or r.get("label") or "-")[:44]),
            ],
            title="Metadata providers",
        )
        return
    block = as_dict(payload)
    state.out.kv([(k, v) for k, v in block.items() if not isinstance(v, (dict, list))], title="Metadata providers")


@manual_app.command("providers")
def manual_providers(ctx: typer.Context) -> None:
    """Show which providers accept a manual watch."""
    state: Ctx = ctx.obj
    payload = state.get("/api/manual/providers")
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
            title="Manual entry targets",
        )
        return
    block = as_dict(payload)
    names = block.get("providers")
    if isinstance(names, list):
        state.out.table(["PROVIDER"], [[str(n)] for n in names], title="Manual entry targets")
        return
    state.out.kv([(k, v) for k, v in block.items() if not isinstance(v, (dict, list))], title="Manual entry targets")


@manual_app.command("watched")
def manual_watched(
    ctx: typer.Context,
    field: list[str] = typer.Option([], "--field", "-F", help="Item detail as key=value. Repeatable."),
    provider: str = typer.Option("", "--provider", "-p", help="Where to record it."),
    watched_at: str = typer.Option("", "--at", help="ISO timestamp. Defaults to now."),
) -> None:
    """Record a watch by hand."""
    state: Ctx = ctx.obj
    state.require_service("Recording a manual watch")
    body: dict[str, Any] = {}
    for item in field:
        key, sep, value = str(item).partition("=")
        if not sep or not key.strip():
            raise CLIError(f"--field expects key=value, got '{item}'", exit_code=EXIT_USAGE)
        body[key.strip()] = value
    if not body:
        raise CLIError(
            "Nothing to record",
            hint="For example --field imdb=tt0111161 --field type=movie",
            exit_code=EXIT_USAGE,
        )
    if provider.strip():
        body["provider"] = provider.strip().upper()
    if watched_at.strip():
        body["watched_at"] = watched_at.strip()
    result = as_dict(state.post("/api/manual/watched", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success("Recorded.")


def register(app: typer.Typer) -> None:
    app.add_typer(metadata_app, name="metadata")
    app.add_typer(manual_app, name="manual")
