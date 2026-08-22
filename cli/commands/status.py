# /cli/commands/status.py
# CrossWatch - CLI status, version and health commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, fmt_rel, fmt_ts, pair_features, pair_label


def _safe(ctx: Ctx, path: str, default: Any = None, *, params: dict[str, Any] | None = None) -> Any:
    try:
        return ctx.get(path, params=params)
    except CLIError as err:
        if int(getattr(err, "status", 0) or 0) in (401, 403):
            raise
        return default


def _provider_rows(providers: dict[str, Any]) -> list[list[Any]]:
    rows: list[list[Any]] = []
    for name in sorted(providers):
        block = providers.get(name)
        if not isinstance(block, dict):
            continue
        summary = as_dict(block.get("instances_summary"))
        total = int(summary.get("total") or 0)
        ok = int(summary.get("ok") or 0)
        instances = f"{ok}/{total}" if total else "-"
        rows.append(
            [
                name,
                state_text(block.get("connected"), on="connected", off="down"),
                instances,
                str(block.get("used_by") or "-"),
                str(block.get("reason") or "")[:52],
            ]
        )
    return rows


def _collect(ctx: Ctx, *, fresh: bool = False) -> dict[str, Any]:
    status = ctx.get("/api/status", params={"fresh": 1} if fresh else None)
    status = as_dict(status)
    pairs = _safe(ctx, "/api/pairs", []) or []
    if not isinstance(pairs, list):
        pairs = []
    run = _safe(ctx, "/api/run/cancel", {}) or {}
    sched = _safe(ctx, "/api/scheduling/status", {}) or {}
    watch = _safe(ctx, "/api/watch/status", {}) or {}
    version = _safe(ctx, "/api/version", {}) or {}
    enabled = [p for p in pairs if isinstance(p, dict) and p.get("enabled", True) is not False]
    return {
        "mode": ctx.mode,
        "url": ctx.url,
        "version": str(version.get("current") or version.get("version") or "unknown"),
        "can_run": coerce_bool(status.get("can_run"), False),
        "sync_running": coerce_bool(run.get("running"), False),
        "cancel_requested": coerce_bool(run.get("cancel_requested"), False),
        "run_id": str(run.get("run_id") or ""),
        "pairs_total": len(pairs),
        "pairs_enabled": len(enabled),
        "scheduler_enabled": coerce_bool((sched.get("config") or {}).get("enabled"), False),
        "scheduler_running": coerce_bool(sched.get("running"), False),
        "next_run_at": int(sched.get("next_run_at") or 0),
        "watcher_alive": coerce_bool(watch.get("alive"), False),
        "watcher_groups": len(watch.get("groups") or []),
        "watcher_sinks": list(watch.get("sinks") or []),
        "providers": as_dict(status.get("providers")),
        "pairs": pairs,
    }


def register(app: typer.Typer) -> None:
    @app.command("status")
    def status_cmd(
        ctx: typer.Context,
        providers: bool = typer.Option(True, "--providers/--no-providers", help="Include the provider connection table."),
        pairs: bool = typer.Option(True, "--pairs/--no-pairs", help="Include the sync pair table."),
        fresh: bool = typer.Option(False, "--fresh", "-f", help="Re-probe providers instead of using the cached status."),
    ) -> None:
        """Show engine, provider, pair, scheduler and watcher state."""
        state: Ctx = ctx.obj
        data = _collect(state, fresh=fresh)
        out = state.out
        if out.json_mode:
            out.data(data)
            return

        out.kv(
            [
                ("CrossWatch", data["version"]),
                ("Endpoint", f"{data['url']}  ({data['mode']})"),
                ("Sync", state_text(data["sync_running"], on="running", off="idle")),
                ("Run id", data["run_id"] or "-"),
                ("Pairs", f"{data['pairs_enabled']} enabled / {data['pairs_total']} total"),
                ("Ready to sync", state_text(data["can_run"], on="yes", off="no")),
                (
                    "Scheduler",
                    state_text(data["scheduler_enabled"], on="enabled", off="disabled").append(
                        f"  next {fmt_ts(data['next_run_at'])} ({fmt_rel(data['next_run_at'])})"
                        if data["next_run_at"]
                        else ""
                    ),
                ),
                (
                    "Watcher",
                    state_text(data["watcher_alive"], on="running", off="stopped").append(
                        f"  {data['watcher_groups']} group(s) -> {', '.join(data['watcher_sinks']) or 'no sinks'}"
                    ),
                ),
            ],
            title="CrossWatch status",
        )

        if providers and data["providers"]:
            out.print()
            out.table(
                ["PROVIDER", "STATE", "INSTANCES", "USED BY", "REASON"],
                _provider_rows(data["providers"]),
                title="Providers",
            )

        if pairs and data["pairs"]:
            out.print()
            out.table(
                ["ID", "PAIR", "STATE", "FEATURES"],
                [
                    [
                        str(p.get("id") or "")[:12],
                        pair_label(p),
                        state_text(p.get("enabled", True) is not False, on="enabled", off="disabled"),
                        ", ".join(pair_features(p)) or "-",
                    ]
                    for p in data["pairs"]
                    if isinstance(p, dict)
                ],
                title="Pairs",
            )

    @app.command("version")
    def version_cmd(
        ctx: typer.Context,
        check: bool = typer.Option(False, "--check", "-c", help="Ask upstream whether a newer release exists."),
    ) -> None:
        """Show the CrossWatch version and provider module versions."""
        state: Ctx = ctx.obj
        version = _safe(state, "/api/version/check" if check else "/api/version", {}) or {}
        modules = _safe(state, "/api/modules/versions", {}) or {}
        if state.out.json_mode:
            state.out.data({"version": version, "modules": modules})
            return
        state.out.kv(
            [
                ("Version", str(version.get("current") or "unknown")),
                ("Latest", str(version.get("latest") or "-")),
                ("Update available", state_text(coerce_bool(version.get("update_available"), False), on="yes", off="no")),
                ("Endpoint", f"{state.url}  ({state.mode})"),
            ],
            title="CrossWatch",
        )
        rows: list[list[Any]] = []
        groups = as_dict(modules.get("groups"))
        if groups:
            for group, members in sorted(groups.items()):
                if isinstance(members, dict):
                    for name, value in sorted(members.items()):
                        rows.append([str(group), str(name), str(value)])
            if rows:
                state.out.print()
                state.out.table(["GROUP", "MODULE", "VERSION"], rows, title="Modules")
            return
        flat = modules.get("flat") if isinstance(modules.get("flat"), dict) else modules
        if isinstance(flat, dict):
            for name, value in sorted(flat.items()):
                if not isinstance(value, (dict, list)):
                    rows.append([str(name), str(value)])
        if rows:
            state.out.print()
            state.out.table(["MODULE", "VERSION"], rows, title="Modules")

    @app.command("health")
    def health_cmd(ctx: typer.Context) -> None:
        """Check that the service (or local install) answers."""
        state: Ctx = ctx.obj
        payload = state.get("/api/health")
        if state.out.json_mode:
            state.out.data(payload if isinstance(payload, dict) else {"raw": payload})
            return
        if isinstance(payload, dict):
            state.out.kv(
                [("Endpoint", state.url), ("Mode", state.mode), *[(k, v) for k, v in payload.items() if not isinstance(v, (dict, list))]],
                title="Health",
            )
        else:
            state.out.success(str(payload))
