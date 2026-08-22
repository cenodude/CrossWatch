# /cli/commands/watcher.py
# CrossWatch - CLI scrobble watcher commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, strip_ansi

watcher_app = typer.Typer(help="Control the scrobble watcher.", no_args_is_help=True)


def _status(state: Ctx) -> dict[str, Any]:
    payload = state.get("/api/watch/status")
    return as_dict(payload)


def _render(state: Ctx, payload: dict[str, Any]) -> None:
    if state.out.json_mode:
        state.out.data(payload)
        return
    groups = [g for g in (payload.get("groups") or []) if isinstance(g, dict)]
    routes = [r for r in (payload.get("routes") or []) if isinstance(r, dict)]
    state.out.kv(
        [
            ("State", state_text(coerce_bool(payload.get("alive"), False), on="running", off="stopped")),
            ("Provider", str(payload.get("provider") or "-")),
            ("Groups", str(len(groups))),
            ("Routes", str(len(routes))),
            ("Sinks", ", ".join(str(s) for s in (payload.get("sinks") or [])) or "-"),
        ],
        title="Watcher",
    )
    if routes:
        state.out.print()
        state.out.table(
            ["ROUTE", "SOURCE", "SINK", "STATE", "DETAIL"],
            [
                [
                    str(r.get("id") or r.get("route_id") or "-"),
                    str(r.get("provider") or r.get("source") or "-"),
                    str(r.get("sink") or "-"),
                    state_text(coerce_bool(r.get("running"), False), on="running", off="stopped"),
                    str(r.get("status") or r.get("detail") or r.get("error") or "")[:44],
                ]
                for r in routes
            ],
            title="Routes",
        )


@watcher_app.command("status")
def watcher_status(ctx: typer.Context) -> None:
    """Show whether the watcher is running and which routes it serves."""
    state: Ctx = ctx.obj
    _render(state, _status(state))


@watcher_app.command("start")
def watcher_start(ctx: typer.Context) -> None:
    """Start the watcher from the saved configuration."""
    state: Ctx = ctx.obj
    state.require_service("Starting the watcher")
    payload = state.post("/api/watch/start")
    if not state.out.json_mode:
        state.out.success("Watcher started.")
        state.out.print()
    _render(state, as_dict(payload))


@watcher_app.command("stop")
def watcher_stop(ctx: typer.Context) -> None:
    """Stop the watcher."""
    state: Ctx = ctx.obj
    state.require_service("Stopping the watcher")
    payload = state.post("/api/watch/stop")
    if not state.out.json_mode:
        state.out.success("Watcher stopped.")
        state.out.print()
    _render(state, as_dict(payload))


@watcher_app.command("restart")
def watcher_restart(ctx: typer.Context) -> None:
    """Reload the watcher configuration and restart its routes."""
    state: Ctx = ctx.obj
    state.require_service("Restarting the watcher")
    payload = state.post("/api/watch/refresh")
    if not state.out.json_mode:
        state.out.success("Watcher reloaded.")
        state.out.print()
    _render(state, as_dict(payload))


@watcher_app.command("now")
def watcher_now(ctx: typer.Context) -> None:
    """Show what is playing right now."""
    state: Ctx = ctx.obj
    payload = state.get("/api/watch/currently_watching")
    if state.out.json_mode:
        state.out.data(payload)
        return
    items = payload if isinstance(payload, list) else (payload or {}).get("items") or []
    items = [i for i in items if isinstance(i, dict)]
    if not items and isinstance(payload, dict) and payload.get("title"):
        items = [payload]
    state.out.table(
        ["TITLE", "TYPE", "PROVIDER", "USER", "PROGRESS", "STATE"],
        [
            [
                str(i.get("title") or i.get("name") or "-")[:52],
                str(i.get("type") or i.get("media_type") or "-"),
                str(i.get("provider") or i.get("server") or "-"),
                str(i.get("username") or i.get("user") or "-"),
                f"{int(float(i.get('progress') or 0))}%" if i.get("progress") is not None else "-",
                str(i.get("state") or i.get("status") or "-"),
            ]
            for i in items
        ],
        title="Currently watching",
        empty="Nothing is playing.",
    )


@watcher_app.command("logs")
def watcher_logs(
    ctx: typer.Context,
    lines: int = typer.Option(200, "--lines", "-n", min=1, max=3000, help="How many lines to show."),
    tags: str = typer.Option("", "--tags", help="Comma separated watcher log tags."),
) -> None:
    """Show recent watcher log output."""
    state: Ctx = ctx.obj
    state.require_service("Reading watcher logs")
    params: dict[str, Any] = {"tail": lines}
    if tags.strip():
        params["tags"] = tags.strip()
    payload = state.get("/api/logs/watcher", params=params)
    if not isinstance(payload, dict):
        raise CLIError("Watcher log endpoint returned an unexpected payload")
    if state.out.json_mode:
        state.out.data(payload)
        return
    for line in payload.get("lines") or []:
        state.out.raw(strip_ansi(str(line)))


def register(app: typer.Typer) -> None:
    app.add_typer(watcher_app, name="watcher")
