# /cli/commands/progress.py
# CrossWatch - CLI playback progress commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._util import as_dict, error_text, fmt_ts

progress_app = typer.Typer(help="Unfinished playback across providers.", no_args_is_help=True)


def _rows(payload: Any) -> list[dict[str, Any]]:
    block = as_dict(payload)
    for key in ("items", "rows", "results"):
        found = block.get(key)
        if isinstance(found, list):
            return [as_dict(i) for i in found]
    if isinstance(payload, list):
        return [as_dict(i) for i in payload]
    return []


def _percent(row: dict[str, Any]) -> str:
    value = row.get("progress")
    if value is None:
        value = row.get("percent")
    if value is None:
        return "-"
    try:
        return f"{float(value):.0f}%"
    except Exception:
        return "-"


@progress_app.command("list")
def progress_list(
    ctx: typer.Context,
    provider: str = typer.Option("", "--provider", "-p", help="Limit to one provider."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
    media_type: str = typer.Option("", "--type", "-t", help="movie or episode."),
    minimum: float = typer.Option(0.0, "--min", help="Only items at or above this percent."),
    maximum: float = typer.Option(0.0, "--max", help="Only items at or below this percent."),
    limit: int = typer.Option(50, "--limit", "-n", help="Maximum rows."),
) -> None:
    """List unfinished playback."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {}
    if provider.strip():
        params["provider"] = provider.strip().upper()
    if instance.strip():
        params["instance_id"] = instance.strip()
    if media_type.strip():
        params["media_type"] = media_type.strip().lower()
    if minimum:
        params["progress_min"] = minimum
    if maximum:
        params["progress_max"] = maximum
    payload = state.get("/api/playback_progress/items", params=params or None)
    rows = _rows(payload)
    if state.out.json_mode:
        state.out.data(rows)
        return
    state.out.records(
        rows[: max(1, limit)],
        [
            ("KEY", lambda r: str(r.get("key") or r.get("id") or "-")[:26]),
            ("TITLE", lambda r: str(r.get("title") or "-")[:40]),
            ("TYPE", lambda r: str(r.get("media_type") or r.get("type") or "-")),
            ("PROVIDER", lambda r: str(r.get("provider") or "-")),
            ("PROGRESS", _percent),
            ("WATCHED", lambda r: fmt_ts(r.get("updated_at") or r.get("last_played_at") or r.get("ts"))),
        ],
        title=f"In progress ({len(rows)})",
        empty="Nothing part watched.",
    )


@progress_app.command("providers")
def progress_providers(ctx: typer.Context) -> None:
    """Show which providers report playback progress."""
    state: Ctx = ctx.obj
    payload = state.get("/api/playback_progress/providers", params={"user_profile": ""})
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload) or [as_dict(p) for p in (as_dict(payload).get("providers") or [])]
    state.out.records(
        rows,
        [
            ("PROVIDER", lambda r: str(r.get("provider") or r.get("name") or "-")),
            ("INSTANCE", lambda r: str(r.get("instance") or r.get("instance_id") or "-")),
            ("ITEMS", lambda r: str(r.get("count") or r.get("items") or "-")),
        ],
        title="Progress providers",
        empty="No providers report progress.",
    )


@progress_app.command("settings")
def progress_settings(ctx: typer.Context) -> None:
    """Show the playback progress settings."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/playback_progress/settings", params={"user_profile": ""}))
    if state.out.json_mode:
        state.out.data(payload)
        return
    state.out.kv(
        [(key, value) for key, value in payload.items() if not isinstance(value, (dict, list))],
        title="Playback progress settings",
    )


def _act(state: Ctx, path: str, keys: list[str], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    body: dict[str, Any] = {"keys": keys}
    if len(keys) == 1:
        body["key"] = keys[0]
    body.update(extra or {})
    result = as_dict(state.post(path, json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    return result


@progress_app.command("watched")
def progress_watched(
    ctx: typer.Context,
    keys: list[str] = typer.Argument(..., help="Item keys from 'cw progress list'."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Mark part watched items as fully watched."""
    state: Ctx = ctx.obj
    state.require_service("Marking playback watched")
    wanted = [k.strip() for k in keys if k.strip()]
    if not yes and not typer.confirm(f"Mark {len(wanted)} item(s) watched?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = _act(state, "/api/playback_progress/actions/mark_watched", wanted)
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Marked {len(wanted)} item(s) watched.")


@progress_app.command("remove")
def progress_remove(
    ctx: typer.Context,
    keys: list[str] = typer.Argument(..., help="Item keys from 'cw progress list'."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Drop playback progress for these items."""
    state: Ctx = ctx.obj
    state.require_service("Removing playback progress")
    wanted = [k.strip() for k in keys if k.strip()]
    if not yes and not typer.confirm(f"Remove progress for {len(wanted)} item(s)?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = _act(state, "/api/playback_progress/actions/remove", wanted)
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Removed progress for {len(wanted)} item(s).")


@progress_app.command("set")
def progress_set(
    ctx: typer.Context,
    key: str = typer.Argument(..., help="Item key."),
    percent: float = typer.Argument(..., help="New progress percent."),
) -> None:
    """Set the progress percent for one item."""
    state: Ctx = ctx.obj
    state.require_service("Updating playback progress")
    result = _act(state, "/api/playback_progress/actions/update_progress", [key], {"progress": float(percent)})
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"{key} is now at {percent:g}%")


def register(app: typer.Typer) -> None:
    app.add_typer(progress_app, name="progress")
