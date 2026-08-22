# /cli/commands/editor.py
# CrossWatch - CLI editor commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_USAGE, CLIError
from .._util import as_dict, error_text

editor_app = typer.Typer(help="Inspect and adjust stored items.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    block = as_dict(payload)
    for key in keys:
        found = block.get(key)
        if isinstance(found, list):
            return [as_dict(i) for i in found]
    if isinstance(payload, list):
        return [as_dict(i) for i in payload]
    return []


@editor_app.command("list")
def editor_list(
    ctx: typer.Context,
    kind: str = typer.Option("watchlist", "--kind", "-k", help="watchlist, ratings, history or playlists."),
    provider: str = typer.Option("", "--provider", "-p", help="Limit to one provider."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
    source: str = typer.Option("state", "--source", help="state or snapshot."),
    snapshot: str = typer.Option("", "--snapshot", help="Snapshot path when source is snapshot."),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
) -> None:
    """List what the editor sees."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"kind": kind, "source": source}
    if provider.strip():
        params["provider"] = provider.strip().upper()
    if instance.strip():
        params["provider_instance"] = instance.strip()
    if snapshot.strip():
        params["snapshot"] = snapshot.strip()
    payload = state.get("/api/editor", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "items", "rows")
    state.out.records(
        rows[: max(1, limit)],
        [
            ("KEY", lambda r: str(r.get("key") or r.get("id") or "-")[:28]),
            ("TITLE", lambda r: str(r.get("title") or "-")[:40]),
            ("TYPE", lambda r: str(r.get("type") or "-")),
            ("YEAR", lambda r: str(r.get("year") or "-")),
            ("PROVIDER", lambda r: str(r.get("provider") or "-")),
        ],
        title=f"Editor: {kind} ({len(rows)})",
        empty="Nothing stored for that view.",
    )


@editor_app.command("providers")
def editor_providers(
    ctx: typer.Context,
    kind: str = typer.Option("watchlist", "--kind", "-k", help="Which feature to ask about."),
) -> None:
    """Show which providers the editor can send to."""
    state: Ctx = ctx.obj
    payload = state.get("/api/editor/send/providers", params={"kind": kind})
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
            title="Send targets",
        )
        return
    block = as_dict(payload)
    names = block.get("providers")
    if isinstance(names, list):
        state.out.table(["PROVIDER"], [[str(n)] for n in names], title="Send targets")
        return
    state.out.kv([(k, v) for k, v in block.items() if not isinstance(v, (dict, list))], title="Send targets")


@editor_app.command("send")
def editor_send(
    ctx: typer.Context,
    keys: list[str] = typer.Argument(..., help="Item keys."),
    provider: str = typer.Option(..., "--provider", "-p", help="Where to send them."),
    kind: str = typer.Option("watchlist", "--kind", "-k", help="Which feature."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Send stored items to a provider."""
    state: Ctx = ctx.obj
    state.require_service("Sending items")
    wanted = [k.strip() for k in keys if k.strip()]
    if not wanted:
        raise CLIError("Nothing to send", exit_code=EXIT_USAGE)
    if not yes:
        state.out.warn(f"This writes {len(wanted)} item(s) to {provider.upper()}.")
        if not typer.confirm("Continue?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    body: dict[str, Any] = {"keys": wanted, "provider": provider.strip().upper(), "kind": kind}
    if instance.strip():
        body["provider_instance"] = instance.strip()
    result = as_dict(state.post("/api/editor/send", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Send rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Sent {len(wanted)} item(s) to {provider.upper()}.")


@editor_app.command("export")
def editor_export(ctx: typer.Context) -> None:
    """Print the manual editor state as JSON."""
    state: Ctx = ctx.obj
    payload = state.get("/api/editor/state/manual/export")
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    state.out.raw(_json.dumps(payload, indent=2, default=str))


@editor_app.command("sources")
def editor_sources(ctx: typer.Context) -> None:
    """Show which providers the editor can read state from."""
    state: Ctx = ctx.obj
    payload = state.get("/api/editor/state/providers")
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
                ("ITEMS", lambda r: str(r.get("count") or "-")),
            ],
            title="State sources",
        )
        return
    block = as_dict(payload)
    names = block.get("providers")
    if isinstance(names, list):
        state.out.table(["PROVIDER"], [[str(n)] for n in names], title="State sources")
        return
    state.out.kv([(k, v) for k, v in block.items() if not isinstance(v, (dict, list))], title="State sources")


def register(app: typer.Typer) -> None:
    app.add_typer(editor_app, name="editor")
