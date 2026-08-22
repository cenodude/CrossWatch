# /cli/commands/instance.py
# CrossWatch - CLI provider instance and user profile commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_NOT_FOUND, EXIT_USAGE, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text

instance_app = typer.Typer(help="Provider instances, for multi server setups.", no_args_is_help=True)
profile_app = typer.Typer(help="User profiles.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    block = as_dict(payload)
    for key in keys:
        found = block.get(key)
        if isinstance(found, list):
            return [as_dict(i) for i in found]
    if isinstance(payload, list):
        return [as_dict(i) for i in payload]
    return []


def _fields(raw: list[str]) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for item in raw or []:
        key, sep, value = str(item).partition("=")
        key = key.strip()
        if not sep or not key:
            raise CLIError(f"--field expects key=value, got '{item}'", exit_code=EXIT_USAGE)
        body[key] = value
    return body


@instance_app.command("list")
def instance_list(
    ctx: typer.Context,
    provider: str = typer.Argument("", help="Optional provider to limit to."),
    configured: bool = typer.Option(False, "--configured", help="Only instances that are configured."),
) -> None:
    """List provider instances."""
    state: Ctx = ctx.obj
    if provider.strip():
        payload = state.get(f"/api/provider-instances/{provider.strip().upper()}")
    else:
        payload = state.get("/api/provider-instances", params={"configured_only": configured})
    if state.out.json_mode:
        state.out.data(payload)
        return

    block = as_dict(payload)
    rows: list[list[Any]] = []
    if provider.strip():
        for item in _rows(payload, "instances", "items"):
            rows.append(
                [
                    provider.strip().upper(),
                    str(item.get("id") or item.get("instance_id") or "-"),
                    str(item.get("label") or item.get("name") or "-"),
                    state_text(coerce_bool(item.get("configured"), False), on="yes", off="no"),
                ]
            )
    else:
        source = as_dict(block.get("providers")) or block
        for name in sorted(source):
            entry = source.get(name)
            items = entry if isinstance(entry, list) else as_dict(entry).get("instances") or []
            for item in items:
                item = as_dict(item)
                rows.append(
                    [
                        str(name).upper(),
                        str(item.get("id") or item.get("instance_id") or "-"),
                        str(item.get("label") or item.get("name") or "-"),
                        state_text(coerce_bool(item.get("configured"), False), on="yes", off="no"),
                    ]
                )
    state.out.table(
        ["PROVIDER", "INSTANCE", "LABEL", "CONFIGURED"],
        rows,
        title="Provider instances",
        empty="No instances beyond the defaults.",
    )


@instance_app.command("add")
def instance_add(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    field: list[str] = typer.Option([], "--field", "-F", help="Instance setting as key=value. Repeatable."),
) -> None:
    """Add the next instance for a provider."""
    state: Ctx = ctx.obj
    state.require_service("Adding a provider instance")
    result = as_dict(state.post(f"/api/provider-instances/{provider.strip().upper()}/next", json_body=_fields(field)))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Added {provider.upper()} instance {result.get('instance_id') or result.get('id') or ''}".strip())


@instance_app.command("set")
def instance_set(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    instance_id: str = typer.Argument(..., help="Instance id."),
    field: list[str] = typer.Option([], "--field", "-F", help="Instance setting as key=value. Repeatable."),
) -> None:
    """Update one provider instance."""
    state: Ctx = ctx.obj
    state.require_service("Updating a provider instance")
    body = _fields(field)
    if not body:
        raise CLIError("Nothing to change", hint="Pass at least one --field key=value", exit_code=EXIT_USAGE)
    path = f"/api/provider-instances/{provider.strip().upper()}/{instance_id.strip()}"
    result = as_dict(state.request("PATCH", path, json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Updated {provider.upper()} instance {instance_id}")


@instance_app.command("delete")
def instance_delete(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    instance_id: str = typer.Argument(..., help="Instance id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a provider instance."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a provider instance")
    if not yes and not typer.confirm(f"Delete {provider.upper()} instance {instance_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    path = f"/api/provider-instances/{provider.strip().upper()}/{instance_id.strip()}"
    result = as_dict(state.delete(path))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"), exit_code=EXIT_NOT_FOUND)
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted {provider.upper()} instance {instance_id}")


@profile_app.command("list")
def profile_list(ctx: typer.Context) -> None:
    """List user profiles."""
    state: Ctx = ctx.obj
    payload = state.get("/api/user-profiles")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "profiles", "items")
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or r.get("profile_id") or "-")),
            ("NAME", lambda r: str(r.get("name") or r.get("label") or "-")),
            ("PAIRS", lambda r: str(r.get("pair_count") or len(r.get("pairs") or []) or 0)),
            ("READ ONLY", lambda r: state_text(coerce_bool(r.get("read_only"), False), on="yes", off="no")),
        ],
        title="User profiles",
        empty="No profiles configured.",
    )


@profile_app.command("show")
def profile_show(ctx: typer.Context, profile_id: str = typer.Argument(..., help="Profile id.")) -> None:
    """Show one user profile."""
    state: Ctx = ctx.obj
    payload = state.get(f"/api/user-profiles/{profile_id}")
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(
        Syntax(_json.dumps(payload, indent=2, default=str), "json", theme="ansi_dark", background_color="default")
    )


@profile_app.command("create")
def profile_create(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Profile name."),
    field: list[str] = typer.Option([], "--field", "-F", help="Extra setting as key=value. Repeatable."),
) -> None:
    """Create a user profile."""
    state: Ctx = ctx.obj
    state.require_service("Creating a user profile")
    body = {"name": name, **_fields(field)}
    result = as_dict(state.post("/api/user-profiles", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Created profile {result.get('id') or name}")


@profile_app.command("set")
def profile_set(
    ctx: typer.Context,
    profile_id: str = typer.Argument(..., help="Profile id."),
    field: list[str] = typer.Option([], "--field", "-F", help="Setting as key=value. Repeatable."),
) -> None:
    """Update a user profile."""
    state: Ctx = ctx.obj
    state.require_service("Updating a user profile")
    body = _fields(field)
    if not body:
        raise CLIError("Nothing to change", hint="Pass at least one --field key=value", exit_code=EXIT_USAGE)
    result = as_dict(state.put(f"/api/user-profiles/{profile_id}", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Updated profile {profile_id}")


@profile_app.command("delete")
def profile_delete(
    ctx: typer.Context,
    profile_id: str = typer.Argument(..., help="Profile id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a user profile."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a user profile")
    if not yes and not typer.confirm(f"Delete profile {profile_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/user-profiles/{profile_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"), exit_code=EXIT_NOT_FOUND)
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted profile {profile_id}")


def register(app: typer.Typer) -> None:
    app.add_typer(instance_app, name="instance")
    app.add_typer(profile_app, name="user-profile")
