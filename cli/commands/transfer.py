# /cli/commands/transfer.py
# CrossWatch - CLI import and export commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path
from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_NOT_FOUND, EXIT_USAGE, CLIError
from .._util import as_dict, error_text, rows_from_payload

export_app = typer.Typer(help="Export your data out of CrossWatch.", no_args_is_help=True)
import_app = typer.Typer(help="Import data into CrossWatch.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    return rows_from_payload(payload, *keys)


def _options(state: Ctx, path: str, title: str) -> None:
    payload = as_dict(state.get(path))
    if state.out.json_mode:
        state.out.data(payload)
        return
    for key, value in payload.items():
        if isinstance(value, list) and value:
            state.out.table([key.upper()], [[str(v)] for v in value], title=key)
            state.out.print()
        elif not isinstance(value, dict):
            state.out.kv([(key, value)])
    if not payload:
        state.out.info(f"No {title} reported.")


@export_app.command("options")
def export_options(ctx: typer.Context) -> None:
    """Show what can be exported and in which formats."""
    state: Ctx = ctx.obj
    _options(state, "/api/export/options", "export options")


@export_app.command("preview")
def export_preview(
    ctx: typer.Context,
    provider: str = typer.Option(..., "--provider", "-p", help="Provider to export from."),
    feature: str = typer.Option("watchlist", "--feature", "-F", help="watchlist, ratings, history or collection."),
    export_format: str = typer.Option("letterboxd", "--format", help="Output format."),
    media_types: str = typer.Option("movie", "--types", help="Comma separated media types."),
    instance: str = typer.Option("all", "--instance", "-i", help="Provider instance id."),
) -> None:
    """Show a sample of what an export would contain."""
    state: Ctx = ctx.obj
    params = {
        "provider": provider.strip().upper(),
        "feature": feature,
        "format": export_format,
        "media_types": media_types,
        "provider_instance": instance,
    }
    payload = state.get("/api/export/sample", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    block = as_dict(payload)
    sample = block.get("sample") or block.get("preview") or block.get("text")
    if isinstance(sample, str):
        state.out.raw(sample)
        return
    rows = _rows(payload, "items", "rows", "sample")
    if rows:
        keys = list(rows[0])[:5]
        state.out.records(rows[:20], [(k.upper(), k) for k in keys], title="Export sample")
        return
    state.out.kv([(k, v) for k, v in block.items() if not isinstance(v, (dict, list))], title="Export sample")


@export_app.command("file")
def export_file(
    ctx: typer.Context,
    output: Path = typer.Argument(..., help="Where to write the export."),
    provider: str = typer.Option(..., "--provider", "-p", help="Provider to export from."),
    feature: str = typer.Option("watchlist", "--feature", "-F", help="watchlist, ratings, history or collection."),
    export_format: str = typer.Option("letterboxd", "--format", help="Output format."),
    media_types: str = typer.Option("movie", "--types", help="Comma separated media types."),
    instance: str = typer.Option("all", "--instance", "-i", help="Provider instance id."),
) -> None:
    """Write an export to a file."""
    state: Ctx = ctx.obj
    state.require_service("Exporting")
    params = {
        "provider": provider.strip().upper(),
        "feature": feature,
        "format": export_format,
        "media_types": media_types,
        "provider_instance": instance,
    }
    payload = state.get("/api/export/file", params=params)
    if isinstance(payload, str):
        text = payload
    else:
        import json as _json

        text = _json.dumps(payload, indent=2, default=str)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(text, encoding="utf-8")
    if state.out.json_mode:
        state.out.data({"ok": True, "path": str(output), "bytes": len(text.encode("utf-8"))})
        return
    state.out.success(f"Wrote {len(text.encode('utf-8')):,} bytes to {output}")


@import_app.command("options")
def import_options(ctx: typer.Context) -> None:
    """Show what can be imported."""
    state: Ctx = ctx.obj
    _options(state, "/api/import/options", "import options")


@import_app.command("preview")
def import_preview(
    ctx: typer.Context,
    file: Path = typer.Argument(..., help="File to import."),
    source: str = typer.Option("auto", "--source", "-s", help="Source format, or auto."),
    target: str = typer.Option("default", "--target", "-t", help="Target instance id."),
    limit: int = typer.Option(20, "--limit", "-n", help="Maximum preview rows."),
) -> None:
    """Upload a file and show what would be imported."""
    state: Ctx = ctx.obj
    state.require_service("Previewing an import")
    if not file.is_file():
        raise CLIError(f"No such file: {file}", exit_code=EXIT_NOT_FOUND)
    client = state.stream_client()
    try:
        with file.open("rb") as handle:
            response = client.session.post(
                client.base_url + "/api/import/preview",
                files={"file": (file.name, handle)},
                data={"source": source, "target_instance": target},
                timeout=client.timeout,
                verify=not client.insecure,
            )
    finally:
        client.close()
    if response.status_code >= 400:
        raise CLIError(f"Import preview failed with HTTP {response.status_code}: {response.text[:200]}")
    payload = as_dict(response.json())
    import_id = str(payload.get("import_id") or payload.get("id") or "")
    if state.out.json_mode:
        state.out.data(payload)
        return
    state.out.kv(
        [
            ("Import id", import_id or "-"),
            ("Source", str(payload.get("source") or source)),
            ("Rows", str(payload.get("total") or payload.get("count") or "-")),
        ],
        title=f"Preview of {file.name}",
    )
    rows = _rows(payload, "items", "rows", "preview")
    if not rows and import_id:
        rows = _rows(state.get(f"/api/import/preview/{import_id}", params={"limit": limit}), "items", "rows")
    if rows:
        state.out.print()
        state.out.records(
            rows[: max(1, limit)],
            [
                ("TITLE", lambda r: str(r.get("title") or "-")[:40]),
                ("YEAR", lambda r: str(r.get("year") or "-")),
                ("TYPE", lambda r: str(r.get("type") or "-")),
                ("STATUS", lambda r: str(r.get("status") or "-")),
            ],
            title="Rows",
        )
    if import_id:
        state.out.print()
        state.out.info(f"Commit it with 'cw import commit {import_id}'.")


@import_app.command("commit")
def import_commit(
    ctx: typer.Context,
    import_id: str = typer.Argument(..., help="Import id from 'cw import preview'."),
    features: str = typer.Option("watchlist", "--features", "-F", help="Comma separated features."),
    target: str = typer.Option("default", "--target", "-t", help="Target instance id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Apply a previewed import."""
    state: Ctx = ctx.obj
    state.require_service("Committing an import")
    if not yes:
        state.out.warn("This writes to your providers.")
        if not typer.confirm(f"Commit import {import_id}?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    body = {
        "import_id": import_id,
        "features": [f.strip() for f in features.split(",") if f.strip()],
        "target_instance": target,
    }
    result = as_dict(state.post("/api/import/commit", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Import rejected"), exit_code=EXIT_USAGE)
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success("Import committed.")
    for key in ("added", "updated", "skipped", "duplicate", "exists", "errors"):
        if key in result:
            state.out.info(f"{key}: {result.get(key)}")


def register(app: typer.Typer) -> None:
    app.add_typer(export_app, name="export")
    app.add_typer(import_app, name="import")
