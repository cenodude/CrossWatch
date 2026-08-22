# /cli/commands/config.py
# CrossWatch - CLI configuration commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_NOT_FOUND, EXIT_USAGE, CLIError
from .._settings import config_dir
from .._util import dotted_get, dotted_payload, error_text, parse_value, split_path

config_app = typer.Typer(help="Read and change CrossWatch settings.", no_args_is_help=True)


def _load(state: Ctx) -> dict[str, Any]:
    payload = state.get("/api/config")
    if not isinstance(payload, dict):
        raise CLIError("Config endpoint returned an unexpected payload")
    if payload.get("ok") is False:
        raise CLIError(error_text(payload, "Cannot read config"))
    return payload


def _save(state: Ctx, body: dict[str, Any]) -> None:
    result = state.post("/api/config", json_body=body)
    if isinstance(result, dict) and result.get("ok") is False:
        raise CLIError(error_text(result, "Config save rejected"))


def _render_json(state: Ctx, value: Any) -> None:
    if state.out.json_mode:
        state.out.data(value)
        return
    if state.out.plain or not isinstance(value, (dict, list)):
        state.out.raw(value if isinstance(value, str) else json.dumps(value, indent=2, default=str))
        return
    from rich.syntax import Syntax

    state.out.console.print(Syntax(json.dumps(value, indent=2, default=str), "json", theme="ansi_dark", background_color="default"))


@config_app.command("show")
def config_show(
    ctx: typer.Context,
    path: str = typer.Argument("", help="Optional dotted path, for example sync or scrobble.watch."),
) -> None:
    """Print the configuration, or one subtree of it. Secrets stay masked."""
    state: Ctx = ctx.obj
    cfg = _load(state)
    value = cfg if not path.strip() else dotted_get(cfg, path, _MISSING)
    if value is _MISSING:
        raise CLIError(f"No config at '{path}'", exit_code=EXIT_NOT_FOUND)
    _render_json(state, value)


@config_app.command("get")
def config_get(ctx: typer.Context, path: str = typer.Argument(..., help="Dotted path, for example sync.anime.enabled.")) -> None:
    """Print a single config value."""
    state: Ctx = ctx.obj
    value = dotted_get(_load(state), path, _MISSING)
    if value is _MISSING:
        raise CLIError(f"No config at '{path}'", exit_code=EXIT_NOT_FOUND)
    if state.out.json_mode:
        state.out.data({"path": path, "value": value})
        return
    if isinstance(value, (dict, list)):
        _render_json(state, value)
        return
    state.out.raw("" if value is None else ("true" if value is True else "false" if value is False else str(value)))


@config_app.command("set")
def config_set(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Dotted path, for example sync.anime.enabled."),
    value: str = typer.Argument(..., help="New value. Booleans, numbers and null are detected automatically."),
    as_json: bool = typer.Option(False, "--json", "-j", help="Parse the value as JSON (use for lists and objects)."),
) -> None:
    """Change one config value."""
    state: Ctx = ctx.obj
    parsed = parse_value(value, as_json=as_json)
    cfg = _load(state)
    current = dotted_get(cfg, path, _MISSING)
    if current is not _MISSING and isinstance(current, (dict, list)) and not as_json:
        raise CLIError(
            f"'{path}' holds a {type(current).__name__}; refusing to overwrite it with a scalar",
            hint="Pass --json and a JSON value if that is really what you want.",
            exit_code=EXIT_USAGE,
        )
    _save(state, dotted_payload(path, parsed))
    if state.out.json_mode:
        state.out.data({"ok": True, "path": path, "value": parsed, "previous": None if current is _MISSING else current})
        return
    before = "unset" if current is _MISSING else json.dumps(current, default=str)
    state.out.success(f"{path}: {before} -> {json.dumps(parsed, default=str)}")


@config_app.command("unset")
def config_unset(
    ctx: typer.Context,
    path: str = typer.Argument(..., help="Dotted path to remove."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Remove a config key."""
    state: Ctx = ctx.obj
    cfg = _load(state)
    if dotted_get(cfg, path, _MISSING) is _MISSING:
        raise CLIError(f"No config at '{path}'", exit_code=EXIT_NOT_FOUND)
    if len(split_path(path)) == 1 and not yes:
        state.out.warn(f"'{path}' is a top level config section.")
        if not typer.confirm(f"Remove the whole '{path}' section?", default=False):
            raise CLIError("Cancelled", exit_code=0)

    result = state.post("/api/config/unset", json_body={"paths": [path]})
    if not isinstance(result, dict) or result.get("ok") is not True:
        error = str((result or {}).get("error") or "unknown error")
        if error == "protected_path":
            raise CLIError(
                f"'{path}' is protected and cannot be removed from the CLI",
                hint="Authentication settings are managed in the UI or with 'cw auth token'.",
            )
        if error == "not_found":
            raise CLIError(f"No config at '{path}'", exit_code=EXIT_NOT_FOUND)
        raise CLIError(f"Could not remove '{path}': {error}")

    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Removed {path}")


@config_app.command("edit")
def config_edit(
    ctx: typer.Context,
    editor: str = typer.Option("", "--editor", help="Editor command. Defaults to $VISUAL, then $EDITOR, then vi."),
) -> None:
    """Open the configuration in an editor and save what you write back."""
    state: Ctx = ctx.obj
    cfg = _load(state)
    original = json.dumps(cfg, indent=2, default=str)
    command = editor.strip() or os.getenv("VISUAL") or os.getenv("EDITOR") or ("notepad" if os.name == "nt" else "vi")

    handle = tempfile.NamedTemporaryFile("w", suffix=".cw-config.json", delete=False, encoding="utf-8")
    temp = Path(handle.name)
    try:
        handle.write(original)
        handle.close()
        try:
            subprocess.call([*command.split(), str(temp)])
        except FileNotFoundError as exc:
            raise CLIError(f"Editor '{command}' not found", hint="Set $EDITOR or pass --editor.") from exc
        edited = temp.read_text(encoding="utf-8")
    finally:
        try:
            temp.unlink()
        except Exception:
            pass

    if edited.strip() == original.strip():
        state.out.info("No changes.")
        return
    try:
        parsed = json.loads(edited)
    except Exception as exc:
        raise CLIError(f"That is not valid JSON: {exc}", hint="Nothing was saved.") from exc
    if not isinstance(parsed, dict):
        raise CLIError("The config must stay a JSON object", hint="Nothing was saved.")
    _save(state, parsed)
    state.out.success("Configuration saved.")


@config_app.command("meta")
def config_meta(ctx: typer.Context) -> None:
    """Show the config schema metadata the UI uses."""
    state: Ctx = ctx.obj
    _render_json(state, state.get("/api/config/meta"))


@config_app.command("migrate")
def config_migrate(
    ctx: typer.Context,
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Migrate the config to the current schema. Takes a backup first."""
    state: Ctx = ctx.obj
    if not yes and not typer.confirm("Migrate the configuration now?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = state.post("/api/config/migrate")
    if isinstance(result, dict) and result.get("ok") is False:
        raise CLIError(error_text(result, "Migration failed"))
    if state.out.json_mode:
        state.out.data(result)
        return
    block = result if isinstance(result, dict) else {}
    state.out.success("Configuration migrated.")
    if block.get("backup"):
        state.out.info(f"Backup: {block.get('backup')}")
    changed = block.get("changed") or block.get("forced") or []
    if isinstance(changed, list) and changed:
        state.out.info(f"{len(changed)} path(s) changed")


@config_app.command("path")
def config_path(ctx: typer.Context) -> None:
    """Show where the configuration and runtime data live."""
    state: Ctx = ctx.obj
    base = config_dir()
    data = {
        "config_dir": str(base),
        "config_file": str(base / "config.json"),
        "state_dir": str(base / ".cw_state"),
        "database": str(base / ".cw_databases" / "crosswatch.sqlite3"),
        "cache_dir": str(base / "cache"),
        "endpoint": state.url,
    }
    if state.out.json_mode:
        state.out.data(data)
        return
    state.out.kv(list(data.items()), title="Paths")


class _Missing:
    def __repr__(self) -> str:
        return "<missing>"


_MISSING = _Missing()


def register(app: typer.Typer) -> None:
    app.add_typer(config_app, name="config")
