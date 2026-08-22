# /cli/commands/scheduler.py
# CrossWatch - CLI scheduler commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text, fmt_rel, fmt_ts

scheduler_app = typer.Typer(help="Inspect and drive the sync scheduler.", no_args_is_help=True)


def _status(state: Ctx) -> dict[str, Any]:
    payload = state.get("/api/scheduling/status")
    return as_dict(payload)


def _config(state: Ctx) -> dict[str, Any]:
    payload = state.get("/api/scheduling")
    return as_dict(payload)


def _mode(scfg: dict[str, Any]) -> str:
    advanced = as_dict(scfg.get("advanced"))
    if coerce_bool(advanced.get("enabled"), False):
        return "advanced"
    mode = str(scfg.get("mode") or "").strip()
    if mode:
        return mode
    every = scfg.get("every_n_hours") or scfg.get("interval_hours")
    return f"every {every}h" if every else "simple"


def _render(state: Ctx, status: dict[str, Any]) -> None:
    if state.out.json_mode:
        state.out.data(status)
        return
    scfg = as_dict(status.get("config"))
    nxt = int(status.get("next_run_at") or 0)
    warnings = status.get("scheduling_warnings") or []
    state.out.kv(
        [
            ("Enabled", state_text(coerce_bool(scfg.get("enabled"), False), on="yes", off="no")),
            ("Worker", state_text(coerce_bool(status.get("running"), False), on="running", off="stopped")),
            ("Mode", _mode(scfg)),
            ("Next run", f"{fmt_ts(nxt)}  ({fmt_rel(nxt)})" if nxt else "not scheduled"),
            ("Last run", fmt_ts(status.get("last_run_at")) if status.get("last_run_at") else "-"),
            ("Warnings", str(len(warnings)) if warnings else "none"),
        ],
        title="Scheduler",
    )
    if warnings:
        state.out.print()
        rows = []
        for item in warnings:
            if isinstance(item, dict):
                rows.append([str(item.get("code") or item.get("id") or "-"), str(item.get("message") or item.get("text") or "-")])
            else:
                rows.append(["-", str(item)])
        state.out.table(["CODE", "WARNING"], rows, title="Scheduling warnings")


@scheduler_app.command("status")
def scheduler_status(ctx: typer.Context) -> None:
    """Show scheduler state and the next planned run."""
    state: Ctx = ctx.obj
    _render(state, _status(state))


@scheduler_app.command("next")
def scheduler_next(ctx: typer.Context) -> None:
    """Show only the next planned run."""
    state: Ctx = ctx.obj
    payload = state.get("/api/scheduling/next")
    nxt = int((payload or {}).get("next_run_at") or 0)
    if state.out.json_mode:
        state.out.data(as_dict(payload))
        return
    if not nxt:
        state.out.info("No run is scheduled.")
        return
    state.out.raw(f"{fmt_ts(nxt)}  ({fmt_rel(nxt)})")


@scheduler_app.command("run-now")
def scheduler_run_now(ctx: typer.Context) -> None:
    """Ask the scheduler to fire its job immediately."""
    state: Ctx = ctx.obj
    state.require_service("Triggering the scheduler")
    payload = state.post("/api/scheduling/trigger_now")
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Trigger failed"))
    if state.out.json_mode:
        state.out.data(payload if isinstance(payload, dict) else {"ok": True})
        return
    if isinstance(payload, dict) and payload.get("triggered") is False:
        state.out.warn("Scheduler accepted the request but did not fire; is it enabled?")
        return
    state.out.success("Scheduler fired.")


@scheduler_app.command("replan")
def scheduler_replan(ctx: typer.Context) -> None:
    """Recompute the next run time from the current configuration."""
    state: Ctx = ctx.obj
    state.require_service("Replanning the scheduler")
    payload = state.post("/api/scheduling/replan_now")
    if not state.out.json_mode:
        state.out.success("Replanned.")
        state.out.print()
    _render(state, as_dict(payload))


@scheduler_app.command("enable")
def scheduler_enable(ctx: typer.Context) -> None:
    """Turn the scheduler on."""
    _set_enabled(ctx, True)


@scheduler_app.command("disable")
def scheduler_disable(ctx: typer.Context) -> None:
    """Turn the scheduler off."""
    _set_enabled(ctx, False)


def _set_enabled(ctx: typer.Context, enabled: bool) -> None:
    state: Ctx = ctx.obj
    state.require_service("Changing the scheduler")
    scfg = dict(_config(state))
    scfg["enabled"] = bool(enabled)
    payload = state.post("/api/scheduling", json_body=scfg)
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Scheduler update rejected"))
    nxt = int((payload or {}).get("next_run_at") or 0)
    if state.out.json_mode:
        state.out.data({"ok": True, "enabled": enabled, "next_run_at": nxt})
        return
    state.out.success(f"Scheduler {'enabled' if enabled else 'disabled'}.")
    if enabled and nxt:
        state.out.info(f"Next run {fmt_ts(nxt)} ({fmt_rel(nxt)})")


@scheduler_app.command("stop")
def scheduler_stop(ctx: typer.Context) -> None:
    """Stop the scheduler worker without changing the saved configuration."""
    state: Ctx = ctx.obj
    state.require_service("Stopping the scheduler")
    payload = state.post("/api/scheduling/stop")
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Stop failed"))
    if state.out.json_mode:
        state.out.data(payload if isinstance(payload, dict) else {"ok": True})
        return
    state.out.success("Scheduler worker stopped.")


@scheduler_app.command("show")
def scheduler_show(ctx: typer.Context) -> None:
    """Print the raw scheduling configuration."""
    state: Ctx = ctx.obj
    scfg = _config(state)
    if state.out.json_mode:
        state.out.data(scfg)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(Syntax(_json.dumps(scfg, indent=2, default=str), "json", theme="ansi_dark", background_color="default"))


def register(app: typer.Typer) -> None:
    app.add_typer(scheduler_app, name="scheduler")
