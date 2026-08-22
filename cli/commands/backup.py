# /cli/commands/backup.py
# CrossWatch - CLI backup commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._util import as_dict, error_text, fmt_ts

backup_app = typer.Typer(help="CrossWatch tracker backups.", no_args_is_help=True)


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


@backup_app.command("list")
def backup_list(ctx: typer.Context) -> None:
    """List backups."""
    state: Ctx = ctx.obj
    payload = state.get("/api/backups/list")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "backups", "items", "files")
    state.out.records(
        rows,
        [
            ("PATH", lambda r: str(r.get("path") or r.get("name") or "-")),
            ("WHEN", lambda r: fmt_ts(r.get("created_at") or r.get("mtime") or r.get("ts"))),
            ("SIZE", lambda r: _size(r.get("size") or r.get("size_bytes"))),
            ("KIND", lambda r: str(r.get("kind") or r.get("type") or "-")),
        ],
        title=f"Backups ({len(rows)})",
        empty="No backups yet.",
    )


@backup_app.command("create")
def backup_create(
    ctx: typer.Context,
    note: str = typer.Option("", "--note", "-m", help="Label for this backup."),
) -> None:
    """Take a backup now."""
    state: Ctx = ctx.obj
    state.require_service("Taking a backup")
    body: dict[str, Any] = {}
    if note.strip():
        body["note"] = note.strip()
    result = as_dict(state.post("/api/backups/create", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Backup rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Backup created{': ' + str(result.get('path')) if result.get('path') else '.'}")


@backup_app.command("validate")
def backup_validate(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Backup path from 'cw backup list'."),
) -> None:
    """Check a backup can be restored."""
    state: Ctx = ctx.obj
    result = as_dict(state.post("/api/backups/validate", json_body={"path": path}))
    if state.out.json_mode:
        state.out.data(result)
        return
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Backup is not valid"))
    state.out.success("Backup looks valid.")
    for key, value in result.items():
        if key != "ok" and not isinstance(value, (dict, list)):
            state.out.info(f"{key}: {value}")


@backup_app.command("restore")
def backup_restore(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Backup path from 'cw backup list'."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Restore a backup."""
    state: Ctx = ctx.obj
    state.require_service("Restoring a backup")
    if not yes:
        state.out.warn("This overwrites current data.")
        if not typer.confirm(f"Restore {path}?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.post("/api/backups/restore", json_body={"path": path}))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Restore rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success("Backup restored.")


@backup_app.command("delete")
def backup_delete(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Backup path."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a backup."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a backup")
    if not yes and not typer.confirm(f"Delete {path}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.post("/api/backups/delete", json_body={"path": path}))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted {path}")


@backup_app.command("schedule")
def backup_schedule(
    ctx: typer.Context,
    enable: bool = typer.Option(None, "--enable/--disable", help="Turn scheduled backups on or off."),
    every_hours: int = typer.Option(0, "--every-hours", help="Interval in hours."),
) -> None:
    """Show or change the backup schedule."""
    state: Ctx = ctx.obj
    if enable is None and not every_hours:
        payload = state.get("/api/backups/schedule")
        if state.out.json_mode:
            state.out.data(payload)
            return
        block = as_dict(payload)
        state.out.kv(
            [(key, value) for key, value in block.items() if not isinstance(value, (dict, list))],
            title="Backup schedule",
        )
        return

    state.require_service("Changing the backup schedule")
    body: dict[str, Any] = dict(as_dict(state.get("/api/backups/schedule")))
    if enable is not None:
        body["enabled"] = bool(enable)
    if every_hours:
        body["every_n_hours"] = int(every_hours)
    result = as_dict(state.post("/api/backups/schedule", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Schedule rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Backup schedule updated.")


@backup_app.command("retention")
def backup_retention(
    ctx: typer.Context,
    keep: int = typer.Argument(..., help="How many backups to keep."),
) -> None:
    """Set how many backups to keep and prune the rest."""
    state: Ctx = ctx.obj
    state.require_service("Changing backup retention")
    result = as_dict(state.post("/api/backups/retention", json_body={"keep": int(keep)}))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Retention rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Keeping {keep} backups.")


def register(app: typer.Typer) -> None:
    app.add_typer(backup_app, name="backup")
