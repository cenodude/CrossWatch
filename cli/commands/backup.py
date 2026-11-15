# /cli/commands/backup.py
# CrossWatch - CLI backup commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._util import as_dict, error_text, fmt_ts, rows_from_payload
from .backup_settings import read_secret, register_settings

backup_app = typer.Typer(help="CrossWatch backups, destination copies and recovery.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    return rows_from_payload(payload, *keys)


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
    managed: bool = typer.Option(False, "--managed", help="Run the saved backup plan, including encryption and destination copies."),
) -> None:
    """Create a local backup, or use --managed to run the saved backup plan."""
    state: Ctx = ctx.obj
    state.require_service("Taking a backup")
    body: dict[str, Any] = {}
    if managed:
        if note.strip():
            raise CLIError("Managed backups use the saved plan and do not accept --note.")
        body["managed"] = True
    if note.strip():
        body["label"] = note.strip()
    result = as_dict(state.post("/api/backups/create", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Backup rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    if managed:
        state.out.success(f"Backup queued: {as_dict(result.get('job')).get('id', '')}")
        state.out.info("Follow progress with cw backup status --watch.")
        return
    backup = as_dict(result.get("backup"))
    path = str(backup.get("path") or result.get("path") or "")
    state.out.success(f"Backup created{': ' + path if path else '.'}")


@backup_app.command("validate")
def backup_validate(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Backup path from 'cw backup list'."),
    password_file: Path | None = typer.Option(None, "--password-file", help="Read the archive password from a local file."),
    ask_password: bool = typer.Option(False, "--ask-password", help="Prompt for the archive password."),
) -> None:
    """Check a backup can be restored."""
    state: Ctx = ctx.obj
    body = {"path": path, "password": read_secret(password_file, ask_password)}
    result = as_dict(state.post("/api/backups/validate", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Backup is not valid"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success("Backup looks valid.")
    validation = as_dict(result.get("validation"))
    if validation.get("application_ready") is False:
        state.out.warn(str(validation.get("application_error") or "Application restoration needs the original configuration key."))
    manifest = as_dict(validation.get("manifest"))
    sections = validation.get("available_sections") or []
    if sections:
        state.out.info("Selective recovery (--only): " + ", ".join(str(section) for section in sections))
    state.out.records(_rows(manifest, "trackers"), [("TRACKER", "id"), ("LABEL", "label")], title="Trackers in backup", empty="No trackers in this backup.")
    for key, value in validation.items():
        if key != "ok" and not isinstance(value, (dict, list)):
            state.out.info(f"{key}: {value}")


@backup_app.command("restore")
def backup_restore(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Backup path from 'cw backup list'."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
    password_file: Path | None = typer.Option(None, "--password-file", help="Read the archive password from a local file."),
    ask_password: bool = typer.Option(False, "--ask-password", help="Prompt for the archive password."),
    new_tracker: bool = typer.Option(False, "--new-tracker", help="Recover a tracker into a new instance without sync pairs."),
    only: str = typer.Option("", "--only", help="Restore only mappings, anime_rules or captures."),
    instance: str = typer.Option("", "--instance", help="Tracker ID recorded in the backup, including default."),
    label: str = typer.Option("Recovered tracker", "--label", help="Label for the recovered tracker."),
) -> None:
    """Restore a backup."""
    state: Ctx = ctx.obj
    state.require_service("Restoring a backup")
    if only and (new_tracker or only not in {"mappings", "anime_rules", "captures"}):
        raise CLIError("Use --only mappings, anime_rules or captures without --new-tracker.")
    if new_tracker and not instance:
        raise CLIError("Choose --instance from the tracker IDs shown by cw backup validate.")
    if instance and not new_tracker:
        raise CLIError("Use --new-tracker with --instance.")
    if not new_tracker and not yes:
        state.out.warn(f"This replaces only the selected {only} data." if only else "This overwrites current data.")
        if not typer.confirm(f"Restore {path}?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    body = {"path": path, "password": read_secret(password_file, ask_password), "confirm": True, "restart": not new_tracker,
            "mode": "new_tracker" if new_tracker else only or "replace", "instance": instance, "label": label}
    result = as_dict(state.post("/api/backups/restore", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Restore rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(str(as_dict(result.get("result")).get("message") or "Tracker recovered.") if new_tracker else "Backup prepared. CrossWatch will restore it during restart.")


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
    enable: bool | None = typer.Option(None, "--enable/--disable", help="Turn scheduled backups on or off."),
    every_hours: int = typer.Option(0, "--every-hours", hidden=True),
    at: str = typer.Option("", "--at", help="Backup time as HH:MM in the CW scheduling timezone."),
    day: list[int] | None = typer.Option(None, "--day", min=1, max=7, help="Repeat for weekdays, Monday=1 through Sunday=7."),
    keep: int | None = typer.Option(None, "--keep", min=1, help="Number of automatic local backups to keep."),
    managed: bool | None = typer.Option(None, "--managed/--legacy", help="Use the saved backup plan or the legacy scope."),
) -> None:
    """Show or change the backup schedule."""
    state: Ctx = ctx.obj
    if every_hours:
        raise CLIError("Backup schedules use a daily time. Use --at HH:MM and optional --day 1 through 7.")
    if at and not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", at):
        raise CLIError("Use --at HH:MM, for example 03:00.")
    if enable is None and not at and not day and keep is None and managed is None:
        payload = state.get("/api/backups/schedule")
        if state.out.json_mode:
            state.out.data(payload)
            return
        block = as_dict(as_dict(payload).get("schedule")) or as_dict(payload)
        state.out.kv(
            [(key, value) for key, value in block.items() if not isinstance(value, dict)],
            title="Backup schedule",
        )
        return

    state.require_service("Changing the backup schedule")
    body: dict[str, Any] = dict(as_dict(as_dict(state.get("/api/backups/schedule")).get("schedule")))
    body["enabled"] = bool(body.get("enabled", body.get("active", False)))
    if enable is not None:
        body["enabled"] = bool(enable)
        body["active"] = bool(enable)
    if at:
        body["at"] = at
    if day:
        body["days"] = sorted(set(day))
    if keep is not None:
        body.update(max_backups=keep, auto_delete_old=True, retention_days=0)
    if managed is not None:
        body["managed"] = managed
    if body.get("managed"):
        plan = as_dict(as_dict(state.get("/api/backups/plan")).get("plan"))
        if not plan.get("configured"):
            raise CLIError("Run cw backup configure before scheduling the saved plan.")
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
    keep: int = typer.Argument(..., min=1, help="How many automatic local backups to keep."),
) -> None:
    """Prune automatic local backups. Manual, imported and safety backups are kept."""
    state: Ctx = ctx.obj
    state.require_service("Changing backup retention")
    result = as_dict(state.post("/api/backups/retention", json_body={"max_backups": int(keep)}))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Retention rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Automatic local backups pruned to {keep}. Active backups remain protected.")


register_settings(backup_app)


def register(app: typer.Typer) -> None:
    app.add_typer(backup_app, name="backup")
