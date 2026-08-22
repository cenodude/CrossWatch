# /cli/commands/watchlist.py
# CrossWatch - CLI unified watchlist commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._util import as_dict, error_text, rows_from_payload

watchlist_app = typer.Typer(help="The unified watchlist across providers.", no_args_is_help=True)


def _rows(payload: Any) -> list[dict[str, Any]]:
    return rows_from_payload(payload, "items", "watchlist", "rows", "results")


def _providers(row: dict[str, Any]) -> str:
    found = row.get("providers") or row.get("sources")
    if isinstance(found, dict):
        return ", ".join(sorted(str(k) for k, v in found.items() if v))
    if isinstance(found, list):
        return ", ".join(str(v) for v in found)
    return str(found or "-")


@watchlist_app.command("list")
def watchlist_list(
    ctx: typer.Context,
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows. 0 means everything."),
    media_type: str = typer.Option("", "--type", "-t", help="movie or tv."),
    query: str = typer.Option("", "--search", "-q", help="Filter titles locally."),
    overview: bool = typer.Option(False, "--overview", help="Include the overview text."),
) -> None:
    """List everything on the watchlist."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"limit": limit, "overview": "short" if overview else "none"}
    payload = state.get("/api/watchlist", params=params)
    rows = _rows(payload)
    if media_type.strip():
        want = media_type.strip().lower()
        rows = [r for r in rows if str(r.get("type") or r.get("media_type") or "").lower() == want]
    if query.strip():
        needle = query.strip().lower()
        rows = [r for r in rows if needle in str(r.get("title") or "").lower()]
    if state.out.json_mode:
        state.out.data(rows)
        return
    columns = [
        ("KEY", lambda r: str(r.get("key") or r.get("id") or "-")[:28]),
        ("TITLE", lambda r: str(r.get("title") or "-")[:44]),
        ("TYPE", lambda r: str(r.get("type") or r.get("media_type") or "-")),
        ("YEAR", lambda r: str(r.get("year") or "-")),
        ("PROVIDERS", _providers),
    ]
    if overview:
        columns.append(("OVERVIEW", lambda r: str(r.get("overview") or "-")[:60]))
    state.out.records(rows, columns, title=f"Watchlist ({len(rows)})", empty="Watchlist is empty.")


@watchlist_app.command("remove")
def watchlist_remove(
    ctx: typer.Context,
    keys: list[str] = typer.Argument(..., help="Item keys from 'cw watchlist list'."),
    provider: str = typer.Option("ALL", "--provider", "-p", help="Remove from one provider, or ALL."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Remove items from the watchlist."""
    state: Ctx = ctx.obj
    state.require_service("Removing from the watchlist")
    wanted = [k.strip() for k in keys if k.strip()]
    if not wanted:
        raise CLIError("Nothing to remove")
    if not yes:
        state.out.warn(f"About to remove {len(wanted)} item(s) from {provider}.")
        if not typer.confirm("Continue?", default=False):
            raise CLIError("Cancelled", exit_code=0)

    body: dict[str, Any] = {"keys": wanted, "provider": provider}
    if instance.strip():
        body["provider_instance"] = instance.strip()
    path = "/api/watchlist/delete_batch" if len(wanted) > 1 else "/api/watchlist/delete"
    if len(wanted) == 1:
        body["key"] = wanted[0]
    result = as_dict(state.post(path, json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Remove rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Removed {len(wanted)} item(s).")


def register(app: typer.Typer) -> None:
    app.add_typer(watchlist_app, name="watchlist")
