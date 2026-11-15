# /cli/commands/backup_settings.py
# CrossWatch - CLI backup plans, destinations and transfer status
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from enum import Enum
from pathlib import Path
from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._util import as_dict, error_text, fmt_ts, rows_from_payload


class Content(str, Enum):
    application = "application"
    trackers = "trackers"
    captures = "captures"


class Destination(str, Enum):
    none = "none"
    onedrive = "onedrive"
    dropbox = "dropbox"
    webdav = "webdav"
    folder = "folder"


class Cloud(str, Enum):
    onedrive = "onedrive"
    dropbox = "dropbox"


def read_secret(path: Path | None, prompt: bool = False, *, label: str = "Backup password", confirm: bool = False) -> str:
    if path is not None and prompt:
        raise CLIError("Choose a password file or a password prompt, not both.")
    if path is not None:
        try:
            value = path.read_text(encoding="utf-8").rstrip("\r\n")
        except (OSError, UnicodeError) as exc:
            raise CLIError("Could not read the secret file.") from exc
        if not value:
            raise CLIError("The secret file is empty.")
        return value
    return str(typer.prompt(label, hide_input=True, confirmation_prompt=confirm)) if prompt else ""


def _post(state: Ctx, path: str, body: dict[str, Any]) -> dict[str, Any]:
    state.require_service("Changing backups")
    result = as_dict(state.post("/api/backups/" + path, json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Backup request failed"))
    return result


def _show(state: Ctx, result: dict[str, Any], message: str) -> None:
    if state.out.json_mode:
        state.out.data(result)
    else:
        state.out.success(message)


def backup_plan(ctx: typer.Context) -> None:
    """Show saved contents, encryption, destination and available trackers."""
    state: Ctx = ctx.obj
    result = as_dict(state.get("/api/backups/plan"))
    if state.out.json_mode:
        state.out.data(result)
        return
    plan = as_dict(result.get("plan"))
    state.out.kv([(key, value) for key, value in plan.items() if key not in {"available_trackers", "destination"}], title="Backup plan")
    state.out.kv(list(as_dict(plan.get("destination")).items()), title="Destination")
    state.out.records(rows_from_payload(plan, "available_trackers"), [("ID", "id"), ("LABEL", "label")], title="Available trackers")


def backup_configure(
    ctx: typer.Context,
    component: list[Content] | None = typer.Option(None, "--component", help="Repeat to select application, trackers or captures. Replaces the saved selection."),
    tracker: list[str] | None = typer.Option(None, "--tracker", help="Repeat to select tracker IDs from cw backup plan."),
    all_trackers: bool = typer.Option(False, "--all-trackers", help="Include all trackers."),
    encrypt: bool | None = typer.Option(None, "--encrypt/--no-encrypt", help="Encrypt local backups. Destination copies are always encrypted."),
    password_file: Path | None = typer.Option(None, "--password-file", help="Read a new backup password from a local file."),
    set_password: bool = typer.Option(False, "--set-password", help="Prompt for a new backup password."),
    password_saved: bool = typer.Option(False, "--password-saved", help="Confirm that the new password is saved outside CW."),
    destination: Destination | None = typer.Option(None, "--destination", help="Choose the destination for encrypted copies."),
    connection: str = typer.Option("", "--connection", help="Connected account ID from cloud-connect."),
    folder: str = typer.Option("", "--folder", help="Relative backup folder on the cloud or WebDAV destination."),
    mount: str = typer.Option("", "--mount", help="Absolute mounted folder path inside the CW container."),
    server: str = typer.Option("", "--server", help="WebDAV server URL."),
    username: str = typer.Option("", "--username", help="WebDAV username."),
    app_password_file: Path | None = typer.Option(None, "--app-password-file", help="Read the WebDAV app password from a local file."),
    ask_app_password: bool = typer.Option(False, "--ask-app-password", help="Prompt for the WebDAV app password."),
    vendor: str = typer.Option("", "--vendor", help="WebDAV vendor: nextcloud, owncloud or other."),
    allow_http: bool | None = typer.Option(None, "--allow-http/--https-only", help="Allow HTTP for a trusted local WebDAV server."),
    remote_keep: int | None = typer.Option(None, "--remote-keep", min=1, max=365, help="Automatic destination copies to keep."),
) -> None:
    """Save backup settings shared with the web interface."""
    state: Ctx = ctx.obj
    state.require_service("Configuring backups")
    if tracker and all_trackers:
        raise CLIError("Choose --tracker or --all-trackers.")
    current = as_dict(as_dict(state.get("/api/backups/plan")).get("plan"))
    body = {key: current[key] for key in ("components", "trackers", "encrypt_local", "destination", "remote_keep") if key in current}
    if component:
        body["components"] = [value.value for value in component]
    if tracker or all_trackers:
        if "trackers" not in body.get("components", []):
            raise CLIError("Include --component trackers when selecting tracker instances.")
        body["trackers"] = None if all_trackers else tracker
    if encrypt is not None:
        body["encrypt_local"] = encrypt
    password = read_secret(password_file, set_password, confirm=True)
    if password:
        if len(password) < 12:
            raise CLIError("Use at least 12 characters for the backup password.")
        if not password_saved and set_password:
            password_saved = typer.confirm("Have you saved this password outside CW?", default=False)
        if not password_saved:
            raise CLIError("Confirm the password is saved outside CW with --password-saved.")
        body.update(password=password, password_saved=True)
    dest: dict[str, Any] = dict(as_dict(body.get("destination")))
    if destination is not None and destination.value != dest.get("kind"):
        dest = {"kind": destination.value}
    kind = dest.get("kind", "none")
    if connection and kind not in {"onedrive", "dropbox"}:
        raise CLIError("Use --connection with OneDrive or Dropbox.")
    if mount and kind != "folder":
        raise CLIError("Use --mount with --destination folder.")
    if folder and kind not in {"onedrive", "dropbox", "webdav"}:
        raise CLIError("Use --folder with a cloud or WebDAV destination.")
    if (server or username or vendor or app_password_file is not None or ask_app_password or allow_http is not None) and kind != "webdav":
        raise CLIError("WebDAV options require --destination webdav.")
    for key, value in (("connection", connection), ("folder", folder), ("path", mount), ("url", server), ("username", username), ("vendor", vendor)):
        if value:
            dest[key] = value
    if kind == "webdav":
        secret = read_secret(app_password_file, ask_app_password, label="WebDAV app password")
        if secret:
            dest["password"] = secret
        if allow_http is not None:
            dest["allow_http"] = allow_http
    dest.pop("has_password", None)
    body["destination"] = dest
    if remote_keep is not None:
        body["remote_keep"] = remote_keep
    if (body.get("encrypt_local") or kind != "none") and not (password or current.get("has_password")):
        raise CLIError("Set a backup password with --set-password or --password-file before enabling encryption or destination copies.")
    result = _post(state, "plan", body)
    _show(state, result, "Backup settings saved. Run cw backup create --managed to use them.")


def backup_status(
    ctx: typer.Context,
    job: str = typer.Option("", "--job", help="Show one backup job."),
    watch: bool = typer.Option(False, "--watch", help="Refresh until the selected jobs finish. Ctrl+C stops watching."),
) -> None:
    """Show backup progress, errors and restore results."""
    state: Ctx = ctx.obj
    while True:
        result = as_dict(state.get("/api/backups/status"))
        rows = rows_from_payload(result, "jobs")
        if job:
            rows = [row for row in rows if row.get("id") == job]
            if not rows:
                raise CLIError("Backup job not found.")
        if state.out.json_mode:
            state.out.data({**result, "jobs": rows})
        else:
            state.out.records(rows, [("JOB", "id"), ("STAGE", "stage"), ("LOCAL", "local_status"), ("DESTINATION", "remote_status"),
                                     ("BYTES", lambda row: f"{row.get('bytes', 0)}/{row.get('total_bytes', 0)}"), ("ERROR", "error"),
                                     ("RETRY", lambda row: fmt_ts(row.get("retry_at")) if row.get("retry_at") else "")], title="Backup status")
            restore = as_dict(result.get("restore"))
            if restore:
                state.out.kv(list(restore.items()), title="Restore result")
            if result.get("pending_restore"):
                state.out.info("A restore is waiting for restart.")
        if not watch or not any(row.get("stage") not in {"complete", "failed"} or row.get("retry_at") for row in rows):
            return
        time.sleep(2)


def backup_retry(ctx: typer.Context, job: str = typer.Argument(..., help="Failed job ID from cw backup status.")) -> None:
    """Retry a failed backup job."""
    state: Ctx = ctx.obj
    _show(state, _post(state, "retry", {"id": job}), "Backup queued for retry.")


def backup_test_destination(ctx: typer.Context) -> None:
    """Test the saved destination with an upload, download and checksum check."""
    state: Ctx = ctx.obj
    _show(state, _post(state, "destination/test", {}), "Destination connection verified.")


def backup_remote_list(ctx: typer.Context) -> None:
    """List backups at the saved destination."""
    state: Ctx = ctx.obj
    result = as_dict(state.get("/api/backups/remote"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Could not list destination backups"))
    if state.out.json_mode:
        state.out.data(result)
    else:
        state.out.records(rows_from_payload(result, "backups"), [("PATH", "path"), ("BYTES", "size"), ("CREATED", "created_at"), ("ENCRYPTED", "encrypted")], title="Destination backups")


def backup_remote_download(ctx: typer.Context, path: str = typer.Argument(..., help="Path from remote-list.")) -> None:
    """Download a destination backup into CW for validation and recovery."""
    state: Ctx = ctx.obj
    result = _post(state, "remote/download", {"path": path})
    _show(state, result, "Downloaded to CW: " + str(as_dict(result.get("backup")).get("path") or ""))


def _show_connection(state: Ctx, result: dict[str, Any]) -> None:
    if state.out.json_mode:
        state.out.data(result)
        return
    value = as_dict(result.get("connection"))
    state.out.kv([(key, value.get(key)) for key in ("id", "kind", "status", "question")], title="Cloud connection")
    if value.get("status") == "connected":
        state.out.info(f"Use cw backup configure --destination {value['kind']} --connection {value['id']} to save this destination.")
    else:
        if value.get("question") == "config_token":
            state.out.info(f'Run rclone authorize "{value["kind"]}" on a computer with rclone 1.76.0 or newer and a browser.')
        state.out.records(rows_from_payload(value, "choices"), [("VALUE", "value"), ("LABEL", "label")], empty="")
        state.out.info(f"Continue with cw backup cloud-continue {value['id']}.")


def backup_cloud_connect(
    ctx: typer.Context,
    provider: Cloud = typer.Argument(...),
    step: bool = typer.Option(False, "--step", help="Start authorization without interactive prompts. Use cloud-continue for each step."),
) -> None:
    """Connect OneDrive or Dropbox using rclone authorization."""
    state: Ctx = ctx.obj
    result = _post(state, "cloud/connect", {"kind": provider.value})
    if step or state.out.json_mode:
        _show_connection(state, result)
        return
    while True:
        _show_connection(state, result)
        value = as_dict(result.get("connection"))
        if value.get("status") == "connected":
            return
        question = value.get("question")
        if question == "config_token":
            answer = str(typer.prompt("Paste the authorization result", hide_input=True))
        elif question == "config_driveid":
            answer = str(typer.prompt("Drive value"))
        elif question == "config_drive_ok":
            if not typer.confirm("Use the selected drive?", default=False):
                raise CLIError("Connection cancelled.", exit_code=0)
            answer = "true"
        elif question == "config_type":
            answer = "onedrive"
        else:
            raise CLIError("Unsupported authorization step. Start the connection again.")
        result = _post(state, "cloud/continue", {"id": value["id"], "answer": answer})


def backup_cloud_continue(
    ctx: typer.Context,
    connection: str = typer.Argument(..., help="Connection ID from cloud-connect."),
    answer_file: Path | None = typer.Option(None, "--answer-file", help="Read the rclone authorization result from a local file."),
    choice: str = typer.Option("", "--choice", help="Drive value, onedrive account type, or true to confirm the drive. Do not pass tokens here."),
) -> None:
    """Continue a cloud connection with a hidden prompt, token file or drive choice."""
    state: Ctx = ctx.obj
    state.require_service("Connecting cloud backups")
    if choice and answer_file is not None:
        raise CLIError("Choose --choice or --answer-file.")
    if not choice and answer_file is None and state.out.json_mode:
        raise CLIError("Use --answer-file or --choice for noninteractive authorization.")
    answer = choice or read_secret(answer_file, answer_file is None, label="Authorization result or drive choice")
    _show_connection(state, _post(state, "cloud/continue", {"id": connection, "answer": answer}))


def backup_recovery_key(ctx: typer.Context, file: Path = typer.Argument(..., help="New local file for the generated backup password. Existing files are never overwritten.")) -> None:
    """Generate a recovery key and save it to a local file."""
    state: Ctx = ctx.obj
    result = _post(state, "recovery-key", {})
    try:
        fd = os.open(file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(str(result["key"]) + "\n")
    except OSError as exc:
        raise CLIError("Could not create the recovery key file. Choose a new writable path.") from exc
    _show(state, {"ok": True, "path": str(file)}, "Recovery key saved. Use configure --password-file with this file to set it as the backup password.")


def register_settings(app: typer.Typer) -> None:
    for name, command in (
        ("plan", backup_plan), ("configure", backup_configure), ("status", backup_status), ("retry", backup_retry),
        ("test-destination", backup_test_destination), ("remote-list", backup_remote_list), ("remote-download", backup_remote_download),
        ("cloud-connect", backup_cloud_connect), ("cloud-continue", backup_cloud_continue), ("recovery-key", backup_recovery_key),
    ):
        app.command(name)(command)
