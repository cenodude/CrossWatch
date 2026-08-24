# /cli/commands/editor.py
# CrossWatch - CLI editor commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_USAGE, CLIError
from .._util import as_dict, error_text, rows_from_payload

editor_app = typer.Typer(help="Inspect and adjust stored items.", no_args_is_help=True)


def _row_key(row: dict[str, Any]) -> str:
    return str(row.get("key") or row.get("id") or "").strip()


def _id_value(row: dict[str, Any], name: str) -> str:
    value = row.get(name)
    if value not in (None, ""):
        return str(value)
    ids = row.get("ids")
    if isinstance(ids, dict):
        value = ids.get(name)
        if value not in (None, ""):
            return str(value)
    return "-"


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    return rows_from_payload(payload, *keys)


@editor_app.command("list")
def editor_list(
    ctx: typer.Context,
    kind: str = typer.Option("watchlist", "--kind", "-k", help="watchlist, ratings, history, progress, playlists or collection."),
    provider: str = typer.Option("", "--provider", "-p", help="Limit to one provider."),
    instance: str = typer.Option("", "--instance", "-i", "--profile", help="Provider instance/profile id."),
    source: str = typer.Option("state", "--source", help="state/current, manual or playlist."),
    snapshot: str = typer.Option("", "--snapshot", "--endpoint", help="Playlist endpoint id when source is playlist."),
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
    workspace = as_dict(payload)
    chosen_provider = str(workspace.get("provider") or provider or "-")
    chosen_instance = str(workspace.get("provider_instance") or instance or "-")
    if chosen_provider != "-":
        for row in rows:
            row.setdefault("provider", chosen_provider)
            row.setdefault("provider_instance", chosen_instance)
    state.out.kv(
        [
            ("Source", str(workspace.get("source") or source)),
            ("Kind", str(workspace.get("kind") or kind)),
            ("Provider", chosen_provider),
            ("Profile", chosen_instance),
            ("Rows", str(len(rows))),
        ],
        title="Editor workspace",
    )
    if rows:
        state.out.print()
    state.out.records(
        rows[: max(1, limit)],
        [
            ("KEY", lambda r: (_row_key(r) or "-")[:28]),
            ("TYPE", lambda r: str(r.get("type") or "-")),
            ("TITLE", lambda r: str(r.get("title") or r.get("name") or "-")[:40]),
            ("TMDB", lambda r: _id_value(r, "tmdb")),
            ("YEAR", lambda r: str(r.get("year") or "-")),
            ("PROVIDER", lambda r: str(r.get("provider") or "-")),
        ],
        title=f"Editor: {source}/{kind} ({len(rows)})",
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
                ("PROFILE", lambda r: str(r.get("instance") or r.get("provider_instance") or "-")),
                ("DISPLAY", lambda r: str(r.get("display") or r.get("label") or "-")),
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
    instance: str = typer.Option("", "--instance", "-i", "--profile", help="Target provider instance/profile id."),
    source: str = typer.Option("state", "--source", help="state/current, manual or playlist."),
    source_provider: str = typer.Option("", "--source-provider", help="Provider to read stored items from."),
    source_instance: str = typer.Option("", "--source-instance", "--source-profile", help="Source provider instance/profile id."),
    snapshot: str = typer.Option("", "--snapshot", "--endpoint", help="Playlist endpoint id when source is playlist."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without writing."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Send stored items to a provider."""
    state: Ctx = ctx.obj
    state.require_service("Sending items")
    wanted = [k.strip() for k in keys if k.strip()]
    if not wanted:
        raise CLIError("Nothing to send", exit_code=EXIT_USAGE)
    params: dict[str, Any] = {"kind": kind, "source": source}
    if source_provider.strip():
        params["provider"] = source_provider.strip().upper()
    if source_instance.strip():
        params["provider_instance"] = source_instance.strip()
    if snapshot.strip():
        params["snapshot"] = snapshot.strip()
    current = state.get("/api/editor", params=params)
    rows = _rows(current, "items", "rows")
    by_key = {_row_key(row): row for row in rows if _row_key(row)}
    selected = [by_key[key] for key in wanted if key in by_key]
    missing = [key for key in wanted if key not in by_key]
    if missing:
        raise CLIError(
            f"{len(missing)} item(s) were not found in that editor view",
            hint=f"Missing: {', '.join(missing[:6])}",
            exit_code=EXIT_USAGE,
        )
    if not yes:
        state.out.warn(f"This writes {len(wanted)} item(s) to {provider.upper()}.")
        if not typer.confirm("Continue?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    body: dict[str, Any] = {
        "items": selected,
        "providers": [{"provider": provider.strip().upper(), "instance": instance.strip() or "default"}],
        "kind": kind,
        "dry_run": dry_run,
    }
    result = as_dict(state.post("/api/editor/send", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Send rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    verb = "Would send" if dry_run else "Sent"
    sent = int(result.get("sent") or len(selected))
    state.out.success(f"{verb} {sent} item(s) to {provider.upper()}.")


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
def editor_sources(
    ctx: typer.Context,
    kind: str = typer.Option("watchlist", "--kind", "-k", help="Which feature to count."),
) -> None:
    """Show which providers the editor can read state from."""
    state: Ctx = ctx.obj
    payload = state.get("/api/editor/state/providers")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "providers", "items")
    if rows:
        for row in rows:
            provider_name = str(row.get("name") or row.get("provider") or "").strip()
            if not provider_name:
                continue
            try:
                detail = as_dict(state.get("/api/editor", params={"kind": kind, "source": "state", "provider": provider_name.upper()}))
            except CLIError:
                continue
            if row.get("count") in (None, ""):
                row["count"] = detail.get("count")
            if row.get("provider_instance") in (None, "") and row.get("instance") in (None, ""):
                row["provider_instance"] = detail.get("provider_instance")
        state.out.records(
            rows,
            [
                ("PROVIDER", lambda r: str(r.get("name") or r.get("provider") or "-")),
                ("PROFILE", lambda r: str(r.get("instance") or r.get("provider_instance") or "-")),
                ("ITEMS", lambda r: str(r.get("count") or "-")),
            ],
            title=f"State sources: {kind}",
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
