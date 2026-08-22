# /cli/commands/capture.py
# CrossWatch - CLI capture and snapshot commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._util import as_dict, error_text, fmt_ts

capture_app = typer.Typer(help="Captures, the rollback tool for watchlist, ratings and history.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    block = as_dict(payload)
    for key in keys:
        found = block.get(key)
        if isinstance(found, list):
            return [as_dict(i) for i in found]
    if isinstance(payload, list):
        return [as_dict(i) for i in payload]
    return []


def _size(value: Any) -> str:
    try:
        size = float(value or 0)
    except Exception:
        return "-"
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return "-"


CAPTURE_COLUMNS = [
    ("PATH", lambda r: str(r.get("path") or r.get("id") or r.get("name") or "-")),
    ("PROVIDER", lambda r: str(r.get("provider") or "-")),
    ("FEATURE", lambda r: str(r.get("feature") or "-")),
    ("WHEN", lambda r: fmt_ts(r.get("created_at") or r.get("ts") or r.get("mtime"))),
    ("ITEMS", lambda r: str(r.get("count") or r.get("items") or "-")),
    ("SIZE", lambda r: _size(r.get("size") or r.get("size_bytes"))),
]


@capture_app.command("list")
def capture_list(ctx: typer.Context) -> None:
    """List captures."""
    state: Ctx = ctx.obj
    payload = state.get("/api/snapshots/list")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "snapshots", "items", "captures")
    state.out.records(rows, CAPTURE_COLUMNS, title=f"Captures ({len(rows)})", empty="No captures yet.")


@capture_app.command("manifest")
def capture_manifest(ctx: typer.Context) -> None:
    """Show the capture manifest."""
    state: Ctx = ctx.obj
    payload = state.get("/api/snapshots/manifest")
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(
        Syntax(_json.dumps(payload, indent=2, default=str), "json", theme="ansi_dark", background_color="default")
    )


@capture_app.command("create")
def capture_create(
    ctx: typer.Context,
    provider: str = typer.Option("", "--provider", "-p", help="Limit to one provider."),
    feature: str = typer.Option("", "--feature", "-F", help="Limit to one feature."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Wait for the capture to finish."),
    timeout: float = typer.Option(600.0, "--timeout", help="How long to wait, in seconds."),
) -> None:
    """Take a capture."""
    state: Ctx = ctx.obj
    state.require_service("Taking a capture")
    body: dict[str, Any] = {}
    if provider.strip():
        body["provider"] = provider.strip().upper()
    if feature.strip():
        body["feature"] = feature.strip().lower()
    if instance.strip():
        body["instance"] = instance.strip()

    result = as_dict(state.post("/api/snapshots/create", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Capture rejected"))

    progress_id = str(result.get("progress_id") or result.get("id") or "")
    if not wait or not progress_id:
        if state.out.json_mode:
            state.out.data(result)
            return
        state.out.success("Capture started." if progress_id else "Capture done.")
        return

    deadline = time.time() + max(10.0, timeout)
    last = ""
    while time.time() < deadline:
        time.sleep(2.0)
        progress = as_dict(state.get(f"/api/snapshots/capture-progress/{progress_id}"))
        status = str(progress.get("status") or progress.get("state") or "").lower()
        note = str(progress.get("message") or progress.get("step") or "")
        if note and note != last:
            state.out.info(note)
            last = note
        if status in ("done", "finished", "complete", "ok"):
            if state.out.json_mode:
                state.out.data(progress)
                return
            state.out.success("Capture finished.")
            return
        if status in ("error", "failed"):
            raise CLIError(error_text(progress, "Capture failed"))
    state.out.warn("Still running, stopped waiting.")


@capture_app.command("delete")
def capture_delete(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Capture path from 'cw capture list'."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a capture."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a capture")
    if not yes and not typer.confirm(f"Delete capture {path}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.post("/api/snapshots/delete", json_body={"path": path}))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted {path}")


@capture_app.command("restore")
def capture_restore(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Capture path from 'cw capture list'."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report what would change, change nothing."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Restore a capture back to its provider."""
    state: Ctx = ctx.obj
    state.require_service("Restoring a capture")
    if not dry_run and not yes:
        state.out.warn("This writes to the provider.")
        if not typer.confirm(f"Restore {path}?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    body = {"path": path, "dry_run": bool(dry_run)}
    result = as_dict(state.post("/api/snapshots/restore", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Restore rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(("Dry run finished." if dry_run else "Restore finished."))
    for key in ("added", "removed", "updated", "skipped", "errors"):
        if key in result:
            state.out.info(f"{key}: {result.get(key)}")


@capture_app.command("read")
def capture_read(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Capture path."),
) -> None:
    """Print the contents of a capture."""
    state: Ctx = ctx.obj
    payload = state.get("/api/snapshots/read", params={"path": path})
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(
        Syntax(_json.dumps(payload, indent=2, default=str), "json", theme="ansi_dark", background_color="default")
    )


@capture_app.command("diff")
def capture_diff(
    ctx: typer.Context,
    first: str = typer.Argument(..., help="Older capture path."),
    second: str = typer.Argument(..., help="Newer capture path."),
    feature: str = typer.Option("", "--feature", "-F", help="Limit to one feature."),
    kind: str = typer.Option("all", "--kind", help="all, added or removed."),
    limit: int = typer.Option(100, "--limit", "-n", help="Maximum rows."),
) -> None:
    """Compare two captures."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"a": first, "b": second, "limit": limit, "kind": kind}
    path = "/api/snapshots/diff"
    if feature.strip():
        params["feature"] = feature.strip()
        path = "/api/snapshots/diff/extended"
    payload = state.get(path, params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    block = as_dict(payload)
    summary = as_dict(block.get("summary")) or block
    state.out.kv(
        [
            ("Added", str(summary.get("added") or 0)),
            ("Removed", str(summary.get("removed") or 0)),
            ("Changed", str(summary.get("changed") or summary.get("updated") or 0)),
        ],
        title="Diff",
    )
    rows = _rows(payload, "items", "changes", "rows")
    if rows:
        state.out.print()
        state.out.records(
            rows[: max(1, limit)],
            [
                ("CHANGE", lambda r: str(r.get("kind") or r.get("change") or "-")),
                ("TITLE", lambda r: str(r.get("title") or r.get("key") or "-")[:48]),
                ("TYPE", lambda r: str(r.get("type") or "-")),
                ("FEATURE", lambda r: str(r.get("feature") or "-")),
            ],
            title="Changes",
        )


@capture_app.command("clear")
def capture_clear(
    ctx: typer.Context,
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete every capture."""
    state: Ctx = ctx.obj
    state.require_service("Clearing captures")
    if not yes and not typer.confirm("Delete every capture?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.post("/api/snapshots/clear"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Clear rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Captures cleared.")


def register(app: typer.Typer) -> None:
    app.add_typer(capture_app, name="capture")
