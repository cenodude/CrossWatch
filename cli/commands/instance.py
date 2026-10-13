# /cli/commands/instance.py
# CrossWatch - CLI provider instance, user profile and managed user commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import secrets
from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_NOT_FOUND, EXIT_USAGE, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text, rows_from_payload

instance_app = typer.Typer(help="Provider instances, for multi server setups.", no_args_is_help=True)
profile_app = typer.Typer(help="User profiles.", no_args_is_help=True)
user_app = typer.Typer(help="Managed users and their access level.", no_args_is_help=True)

ACCESS_LEVELS: dict[str, dict[str, bool]] = {
    "readonly": {"dashboard": True, "watchlist": True, "playback": True, "write": False, "monitor": False},
    "full": {"dashboard": True, "watchlist": True, "playback": True, "write": True, "monitor": False},
    "monitor": {"dashboard": False, "watchlist": False, "playback": False, "write": False, "monitor": True},
}


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    return rows_from_payload(payload, *keys)


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


def _access_level(user: dict[str, Any]) -> str:
    if user.get("protected") or user.get("is_admin"):
        return "admin"
    perms = as_dict(user.get("permissions"))
    if perms.get("monitor") is True:
        return "monitor"
    return "full" if perms.get("write") is True else "readonly"


def _access_permissions(level: str) -> dict[str, bool]:
    key = str(level or "").strip().lower()
    if key not in ACCESS_LEVELS:
        raise CLIError(f"Unknown access level '{level}'", hint=f"Use one of: {', '.join(ACCESS_LEVELS)}", exit_code=EXIT_USAGE)
    return dict(ACCESS_LEVELS[key])


def _managed_users(state: Ctx) -> list[dict[str, Any]]:
    return _rows(state.get("/api/app-auth/users"), "items")


def _find_user(state: Ctx, wanted: str) -> dict[str, Any]:
    needle = str(wanted or "").strip()
    users = [u for u in _managed_users(state) if not u.get("protected")]
    by_id = [u for u in users if str(u.get("id") or "") == needle.lower()]
    if len(by_id) == 1:
        return by_id[0]
    by_name = [u for u in users if str(u.get("username") or "").casefold() == needle.casefold()]
    if len(by_name) == 1:
        return by_name[0]
    raise CLIError(f"No managed user '{wanted}'", hint="See 'cw user list'.", exit_code=EXIT_NOT_FOUND)


def _resolve_profile(state: Ctx, wanted: str) -> str:
    needle = str(wanted or "").strip()
    rows = _rows(state.get("/api/user-profiles"), "profiles", "items")
    for row in rows:
        if str(row.get("id") or "") == needle.lower():
            return str(row.get("id"))
    named = [row for row in rows if str(row.get("label") or row.get("name") or "").casefold() == needle.casefold()]
    if len(named) == 1:
        return str(named[0].get("id") or "")
    raise CLIError(f"No user profile '{wanted}'", hint="See 'cw user-profile list'.", exit_code=EXIT_NOT_FOUND)


def _new_password(password: str, ask: bool) -> str:
    if password and ask:
        raise CLIError("Use either --password or --ask-password", exit_code=EXIT_USAGE)
    if ask:
        return str(typer.prompt("Password", hide_input=True, confirmation_prompt=True) or "")
    return str(password or "")


@user_app.command("list")
def user_list(ctx: typer.Context) -> None:
    """List the administrator and managed users."""
    state: Ctx = ctx.obj
    state.require_service("Listing users")
    rows = _managed_users(state)
    if state.out.json_mode:
        state.out.data(rows)
        return
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or "-")),
            ("USERNAME", lambda r: str(r.get("username") or "-")),
            ("ACCESS", _access_level),
            ("PROFILE", lambda r: str(r.get("profile_id") or "-")),
            ("ENABLED", lambda r: state_text(coerce_bool(r.get("enabled"), True), on="yes", off="no")),
        ],
        title="Users",
        empty="No users yet.",
    )


@user_app.command("create")
def user_create(
    ctx: typer.Context,
    username: str = typer.Argument(..., help="Username."),
    profile: str = typer.Option(..., "--profile", "-p", help="User profile id or name the user belongs to."),
    access: str = typer.Option("readonly", "--access", "-a", help="Access level: readonly, full or monitor."),
    password: str = typer.Option("", "--password", help="Login password. Left out, a random one is set and the account is token only."),
    ask_password: bool = typer.Option(False, "--ask-password", help="Prompt for the login password."),
) -> None:
    """Create a managed user."""
    state: Ctx = ctx.obj
    state.require_service("Creating a user")
    permissions = _access_permissions(access)
    chosen = _new_password(password, ask_password)
    generated = not chosen
    body = {
        "username": username,
        "profile_id": _resolve_profile(state, profile),
        "password": chosen or secrets.token_urlsafe(24),
        "permissions": permissions,
    }
    result = as_dict(state.post("/api/app-auth/users", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    user = as_dict(result.get("user"))
    if state.out.json_mode:
        state.out.data({"ok": True, "user": user, "password_generated": generated})
        return
    state.out.success(f"Created user {user.get('username') or username} ({_access_level(user)})")
    if _access_level(user) == "monitor":
        state.out.info("Monitor accounts are token only and cannot sign in to the web UI.")
    elif generated:
        state.out.info("No login password was given, so a random one was set. Use 'cw user set --ask-password' to allow web sign-in.")


@user_app.command("set")
def user_set(
    ctx: typer.Context,
    user: str = typer.Argument(..., help="User id or username."),
    access: str = typer.Option("", "--access", "-a", help="Access level: readonly, full or monitor."),
    profile: str = typer.Option("", "--profile", "-p", help="User profile id or name."),
    username: str = typer.Option("", "--username", help="New username."),
    password: str = typer.Option("", "--password", help="New login password."),
    ask_password: bool = typer.Option(False, "--ask-password", help="Prompt for the new login password."),
    enabled: bool | None = typer.Option(None, "--enable/--disable", help="Enable or disable the user."),
) -> None:
    """Update a managed user."""
    state: Ctx = ctx.obj
    state.require_service("Updating a user")
    body: dict[str, Any] = {}
    if access:
        body["permissions"] = _access_permissions(access)
    chosen = _new_password(password, ask_password)
    if chosen:
        body["password"] = chosen
    if username:
        body["username"] = username
    if enabled is not None:
        body["enabled"] = bool(enabled)
    if not body and not profile:
        raise CLIError("Nothing to change", hint="Pass --access, --profile, --username, --password or --enable/--disable", exit_code=EXIT_USAGE)
    target = _find_user(state, user)
    if profile:
        body["profile_id"] = _resolve_profile(state, profile)
    result = as_dict(state.put(f"/api/app-auth/users/{target.get('id')}", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    updated = as_dict(result.get("user"))
    if state.out.json_mode:
        state.out.data({"ok": True, "user": updated})
        return
    state.out.success(f"Updated user {updated.get('username') or target.get('username')} ({_access_level(updated)})")


@user_app.command("delete")
def user_delete(
    ctx: typer.Context,
    user: str = typer.Argument(..., help="User id or username."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a managed user."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a user")
    target = _find_user(state, user)
    label = str(target.get("username") or target.get("id") or user)
    if not yes and not typer.confirm(f"Delete user {label}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/app-auth/users/{target.get('id')}"))
    if result.get("ok") is False or result.get("deleted") is False:
        raise CLIError(error_text(result, "Delete rejected"), exit_code=EXIT_NOT_FOUND)
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted user {label}")


def register(app: typer.Typer) -> None:
    app.add_typer(instance_app, name="instance")
    app.add_typer(profile_app, name="user-profile")
    app.add_typer(user_app, name="user")
