# /cli/commands/maintenance.py
# CrossWatch - CLI maintenance commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
import uuid
from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_USAGE, CLIError
from .._util import as_dict, error_text

maintenance_app = typer.Typer(help="Database health, caches and housekeeping.", no_args_is_help=True)

CACHES = {
    "all": "/api/maintenance/clear-cache",
    "metadata": "/api/maintenance/clear-metadata-cache",
    "provider-sync": "/api/maintenance/clear-provider-sync-cache",
    "activity-log": "/api/maintenance/clear-activity-log",
    "scrobbles": "/api/maintenance/clear-recent-scrobbles",
    "state": "/api/maintenance/clear-state",
}

PROVIDER_CLEANUP_FEATURES = ("watchlist", "ratings", "history", "progress", "collection")

MAINTENANCE_TOOLS = [
    ("Sync", "Rebuild sync state", "cw maintenance cache state --yes"),
    ("Sync", "Retry provider items", "cw maintenance cache all --yes"),
    ("Playback", "Clear currently playing", "cw maintenance reset-watching"),
    ("Playback", "Clear Recent Scrobbles", "cw maintenance cache scrobbles --yes"),
    ("Reports & Metadata", "Rebuild statistics", "cw maintenance reset-stats --purge-reports --purge-insights --yes"),
    ("Reports & Metadata", "Refresh artwork & metadata", "cw maintenance cache metadata --yes"),
    ("Events", "Health check", "cw maintenance events"),
    ("Events", "Optimize archive", "cw maintenance events --optimize"),
    ("Events", "Clear archive", "cw maintenance events --rebuild --yes"),
    ("Sync State", "Database health", "cw maintenance database"),
    ("Sync State", "Compact sync state", "cw maintenance state-file --compact"),
    ("Sync State", "Prune stale state", "cw maintenance state-file --prune"),
    ("Archive & Recovery", "CW tracker archive", "cw maintenance tracker --clear --snapshots --yes"),
    ("Captures", "Clear all captures", "cw capture clear --yes"),
    ("Provider Cleanup", "Clear provider data", "cw maintenance provider-cleanup --provider PLEX --feature watchlist --yes"),
    ("Support", "Support diagnostics", "cw maintenance support"),
    ("Danger zone", "Factory reset", "cw maintenance factory-reset --confirm RESET"),
]


def _flat(state: Ctx, payload: Any, title: str) -> None:
    block = as_dict(payload)
    if state.out.json_mode:
        state.out.data(block)
        return
    state.out.kv(
        [(key, value) for key, value in block.items() if not isinstance(value, (dict, list))],
        title=title,
    )
    for key, value in block.items():
        if isinstance(value, dict) and value:
            state.out.print()
            state.out.kv(
                [(k, v) for k, v in value.items() if not isinstance(v, (dict, list))],
                title=key,
            )


def _split_cleanup_features(values: list[str], *, all_features: bool = False) -> list[str]:
    if all_features:
        return list(PROVIDER_CLEANUP_FEATURES)
    out: list[str] = []
    for value in values:
        for chunk in str(value or "").split(","):
            feature = chunk.strip().lower()
            if not feature:
                continue
            if feature not in PROVIDER_CLEANUP_FEATURES:
                raise CLIError(
                    f"Unknown cleanup feature '{feature}'",
                    hint=f"Try: {', '.join(PROVIDER_CLEANUP_FEATURES)}",
                    exit_code=EXIT_USAGE,
                )
            if feature not in out:
                out.append(feature)
    return out


def _provider_targets(payload: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for provider in as_dict(payload).get("providers") or []:
        if not isinstance(provider, dict) or provider.get("configured") is False:
            continue
        pid = str(provider.get("id") or "").strip().upper()
        label = str(provider.get("label") or pid or "-")
        raw_features = provider.get("features")
        features: dict[str, Any] = raw_features if isinstance(raw_features, dict) else {}
        cleanup_features = [feature for feature in PROVIDER_CLEANUP_FEATURES if features.get(feature)]
        instances = provider.get("instances") if isinstance(provider.get("instances"), list) else []
        if not instances:
            instances = [{"id": "default", "label": "Default", "configured": True}]
        for instance in instances:
            if not isinstance(instance, dict) or instance.get("configured") is False:
                continue
            rows.append(
                {
                    "provider": pid or label,
                    "profile": str(instance.get("id") or "default"),
                    "label": str(instance.get("label") or instance.get("id") or "Default"),
                    "features": ", ".join(cleanup_features) if cleanup_features else "-",
                }
            )
    return rows


def _cleanup_result_rows(payload: Any) -> list[dict[str, Any]]:
    block = as_dict(payload)
    result = as_dict(block.get("result"))
    progress = as_dict(block.get("progress"))
    results = (
        as_dict(result.get("results"))
        or as_dict(progress.get("cleanup_results"))
        or as_dict(progress.get("results"))
        or as_dict(block.get("results"))
    )
    rows = []
    for feature, item in results.items():
        row = as_dict(item)
        row.setdefault("feature", str(feature))
        rows.append(row)
    return rows


def _print_cleanup_result(state: Ctx, payload: Any) -> None:
    if state.out.json_mode:
        state.out.data(payload)
        return
    block = as_dict(payload)
    progress = as_dict(block.get("progress"))
    result = as_dict(block.get("result"))
    message = (
        str(progress.get("message") or "")
        or str(result.get("message") or "")
        or "Provider cleanup complete."
    )
    rows = _cleanup_result_rows(payload)
    state.out.success(message)
    if rows:
        state.out.print()
        state.out.records(
            rows,
            [
                ("FEATURE", lambda r: str(r.get("feature") or "-")),
                ("REMOVED", lambda r: str(r.get("removed") or 0)),
                ("FOUND", lambda r: str(r.get("count") or 0)),
                ("LEFT", lambda r: str(r.get("remaining") or 0)),
                ("STATUS", lambda r: "skipped" if r.get("skipped") else ("ok" if r.get("ok", True) else "failed")),
            ],
            title="Provider cleanup",
        )


@maintenance_app.command("database")
def maintenance_database(ctx: typer.Context) -> None:
    """Check the runtime database."""
    state: Ctx = ctx.obj
    _flat(state, state.post("/api/maintenance/database-health"), "Database health")


@maintenance_app.command("events")
def maintenance_events(
    ctx: typer.Context,
    optimize: bool = typer.Option(False, "--optimize", help="Optimize the archive."),
    rebuild: bool = typer.Option(False, "--rebuild", help="Rebuild the archive index."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Check, optimize or rebuild the events archive."""
    state: Ctx = ctx.obj
    if optimize and rebuild:
        raise CLIError("Pick one of --optimize or --rebuild", exit_code=EXIT_USAGE)
    if rebuild:
        state.require_service("Rebuilding the events archive")
        if not yes and not typer.confirm("Rebuild the events index?", default=False):
            raise CLIError("Cancelled", exit_code=0)
        _flat(state, state.post("/api/maintenance/events-rebuild"), "Events rebuild")
        return
    if optimize:
        state.require_service("Optimizing the events archive")
        _flat(state, state.post("/api/maintenance/events-optimize"), "Events optimize")
        return
    _flat(state, state.post("/api/maintenance/events-health"), "Events health")


@maintenance_app.command("cache")
def maintenance_cache(
    ctx: typer.Context,
    what: str = typer.Argument("all", help=f"One of: {', '.join(sorted(CACHES))}."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Clear a cache."""
    state: Ctx = ctx.obj
    state.require_service("Clearing a cache")
    key = str(what).strip().lower()
    endpoint = CACHES.get(key)
    if endpoint is None:
        raise CLIError(f"Unknown cache '{what}'", hint=f"Try: {', '.join(sorted(CACHES))}", exit_code=EXIT_USAGE)
    if not yes and not typer.confirm(f"Clear the {key} cache?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.post(endpoint))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Clear rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Cleared the {key} cache.")


@maintenance_app.command("provider-cache")
def maintenance_provider_cache(ctx: typer.Context) -> None:
    """Show what the provider cache holds."""
    state: Ctx = ctx.obj
    payload = state.get("/api/maintenance/provider-cache")
    if state.out.json_mode:
        state.out.data(payload)
        return
    block = as_dict(payload)
    rows = block.get("providers") or block.get("items")
    if isinstance(rows, list):
        state.out.records(
            [as_dict(r) for r in rows],
            [
                ("PROVIDER", lambda r: str(r.get("provider") or r.get("name") or "-")),
                ("ENTRIES", lambda r: str(r.get("entries") or r.get("count") or "-")),
                ("SIZE", lambda r: str(r.get("size") or r.get("size_bytes") or "-")),
            ],
            title="Provider cache",
        )
        return
    _flat(state, payload, "Provider cache")


@maintenance_app.command("tools")
def maintenance_tools(ctx: typer.Context) -> None:
    """Show Maintenance modal actions and their CLI commands."""
    state: Ctx = ctx.obj
    if state.out.json_mode:
        state.out.data(
            {
                "tools": [
                    {"category": category, "action": action, "command": command}
                    for category, action, command in MAINTENANCE_TOOLS
                ]
            }
        )
        return
    state.out.records(
        [{"category": category, "action": action, "command": command} for category, action, command in MAINTENANCE_TOOLS],
        [
            ("CATEGORY", lambda r: str(r.get("category") or "-")),
            ("ACTION", lambda r: str(r.get("action") or "-")),
            ("COMMAND", lambda r: str(r.get("command") or "-")),
        ],
        title="Maintenance tools",
    )


@maintenance_app.command("provider-cleanup")
def maintenance_provider_cleanup(
    ctx: typer.Context,
    provider: str = typer.Option("", "--provider", "-p", help="Provider to clear, for example PLEX."),
    instance: str = typer.Option("default", "--instance", "-i", help="Provider profile/instance id."),
    feature: list[str] = typer.Option([], "--feature", "-F", help="Data to clear. Repeat or comma-separate."),
    all_features: bool = typer.Option(False, "--all", help="Clear watchlist, ratings, history, progress and collection."),
    targets: bool = typer.Option(False, "--targets", help="Show provider cleanup targets."),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Wait for cleanup to finish."),
    timeout: float = typer.Option(900.0, "--timeout", help="How long to wait, in seconds."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Clear provider watchlist, ratings, history, progress or collection by profile."""
    state: Ctx = ctx.obj
    if targets or not provider.strip():
        payload = state.get("/api/snapshots/manifest")
        if state.out.json_mode:
            state.out.data(payload)
            return
        rows = _provider_targets(payload)
        state.out.records(
            rows,
            [
                ("PROVIDER", lambda r: str(r.get("provider") or "-")),
                ("PROFILE", lambda r: str(r.get("profile") or "-")),
                ("NAME", lambda r: str(r.get("label") or "-")),
                ("CAN CLEAR", lambda r: str(r.get("features") or "-")),
            ],
            title="Provider cleanup targets",
            empty="No configured provider cleanup targets.",
        )
        return

    state.require_service("Clearing provider data")
    features = _split_cleanup_features(feature, all_features=all_features)
    if not features:
        raise CLIError(
            "Pick at least one cleanup feature",
            hint="Use --feature watchlist, --feature ratings, --feature history, --feature progress, --feature collection or --all.",
            exit_code=EXIT_USAGE,
        )
    pid = provider.strip().upper()
    inst = instance.strip() or "default"
    if not yes and not typer.confirm(f"Clear {', '.join(features)} from {pid}#{inst}?", default=False):
        raise CLIError("Cancelled", exit_code=0)

    progress_id = f"provider-cleanup-{uuid.uuid4()}"
    result = as_dict(
        state.post(
            "/api/snapshots/tools/clear",
            json_body={
                "provider": pid,
                "instance": inst,
                "features": features,
                "progress_id": progress_id,
                "background": True,
            },
        )
    )
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Provider cleanup rejected"))
    if not wait:
        if state.out.json_mode:
            state.out.data(result)
            return
        state.out.success(f"Provider cleanup started ({progress_id}).")
        return

    deadline = time.time() + max(10.0, timeout)
    last = ""
    while time.time() < deadline:
        time.sleep(1.0)
        progress_payload = as_dict(state.get(f"/api/snapshots/capture-progress/{progress_id}"))
        progress = as_dict(progress_payload.get("progress")) or progress_payload
        if progress.get("ok") is False and progress.get("done"):
            raise CLIError(error_text(progress, "Provider cleanup failed"))
        note = str(progress.get("message") or progress.get("stage") or "")
        if note and note != last:
            state.out.info(note)
            last = note
        if progress.get("done"):
            _print_cleanup_result(state, {"progress": progress, "start": result})
            return
    state.out.warn("Still running, stopped waiting.")


@maintenance_app.command("factory-reset")
def maintenance_factory_reset(
    ctx: typer.Context,
    confirm: str = typer.Option("", "--confirm", help="Type RESET to confirm."),
    restart: bool = typer.Option(False, "--restart", help="Restart the service after reset."),
) -> None:
    """Return CrossWatch to a clean local install."""
    state: Ctx = ctx.obj
    state.require_service("Factory reset")
    if str(confirm or "").strip().upper() != "RESET":
        raise CLIError(
            "Factory reset requires confirmation",
            hint="Run with --confirm RESET. This deletes local state and backs up config.json.",
            exit_code=EXIT_USAGE,
        )
    result = as_dict(state.post("/api/maintenance/reset-all-default", json_body={"restart": bool(restart)}))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Factory reset rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success("Factory reset complete.")
    state.out.kv(
        [
            ("Config backup", result.get("backup") or "-"),
            ("Removed files", len(result.get("removed_files") or [])),
            ("Removed dirs", len(result.get("removed_dirs") or [])),
            ("Restart", "scheduled" if result.get("restart_scheduled") else "no"),
        ],
        title="Factory reset",
    )


@maintenance_app.command("state-file")
def maintenance_state_file(
    ctx: typer.Context,
    prune: bool = typer.Option(False, "--prune", help="Drop stale entries."),
    compact: bool = typer.Option(False, "--compact", help="Rewrite the file smaller."),
) -> None:
    """Prune or compact state.json."""
    state: Ctx = ctx.obj
    state.require_service("Working on the state file")
    if not prune and not compact:
        raise CLIError("Pick --prune or --compact", exit_code=EXIT_USAGE)
    if prune:
        _flat(state, state.post("/api/maintenance/state-file/prune"), "State file prune")
    if compact:
        _flat(state, state.post("/api/maintenance/state-file/compact"), "State file compact")


@maintenance_app.command("tracker")
def maintenance_tracker(
    ctx: typer.Context,
    instance: str = typer.Option("default", "--instance", "-i", help="Provider instance id."),
    clear: bool = typer.Option(False, "--clear", help="Clear the tracker state."),
    snapshots: bool = typer.Option(False, "--snapshots", help="With --clear, also drop its snapshots."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Show or clear the CrossWatch tracker."""
    state: Ctx = ctx.obj
    if not clear:
        _flat(state, state.get("/api/maintenance/crosswatch-tracker", params={"provider_instance": instance}), "Tracker")
        return
    state.require_service("Clearing the tracker")
    if not yes and not typer.confirm(f"Clear tracker state for {instance}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    body = {"clear_state": True, "clear_snapshots": bool(snapshots), "provider_instance": instance}
    result = as_dict(state.post("/api/maintenance/crosswatch-tracker/clear", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Clear rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Tracker cleared.")


@maintenance_app.command("reset-stats")
def maintenance_reset_stats(
    ctx: typer.Context,
    recalc: bool = typer.Option(False, "--recalc", help="Recalculate instead of only clearing."),
    purge_reports: bool = typer.Option(False, "--purge-reports", help="Also delete sync reports."),
    purge_insights: bool = typer.Option(False, "--purge-insights", help="Also delete insights."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Reset the statistics."""
    state: Ctx = ctx.obj
    state.require_service("Resetting statistics")
    if not yes and not typer.confirm("Reset statistics?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    body = {
        "recalc": bool(recalc),
        "purge_reports": bool(purge_reports),
        "purge_insights": bool(purge_insights),
    }
    _flat(state, state.post("/api/maintenance/reset-stats", json_body=body), "Statistics reset")


@maintenance_app.command("reset-watching")
def maintenance_reset_watching(ctx: typer.Context) -> None:
    """Clear the currently watching card."""
    state: Ctx = ctx.obj
    state.require_service("Resetting currently watching")
    result = as_dict(state.post("/api/maintenance/reset-currently-watching"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Reset rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success("Currently watching cleared.")


@maintenance_app.command("support")
def maintenance_support(
    ctx: typer.Context,
    scopes: bool = typer.Option(False, "--scopes", help="List what a support bundle can include."),
    pairs: str = typer.Option("", "--pairs", "-p", help="Limit to these pair ids, comma separated."),
) -> None:
    """Show support diagnostics."""
    state: Ctx = ctx.obj
    if scopes:
        payload = state.get("/api/maintenance/support/scopes")
        if state.out.json_mode:
            state.out.data(payload)
            return
        block = as_dict(payload)
        rows = block.get("scopes") or block.get("items")
        if isinstance(rows, list):
            state.out.table(["SCOPE"], [[str(r)] for r in rows], title="Support scopes")
            return
        _flat(state, payload, "Support scopes")
        return
    params = {"pairs": [p.strip() for p in pairs.split(",") if p.strip()]} if pairs.strip() else None
    _flat(state, state.get("/api/maintenance/support/state", params=params), "Support state")


@maintenance_app.command("restart")
def maintenance_restart(
    ctx: typer.Context,
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Restart the CrossWatch service."""
    state: Ctx = ctx.obj
    state.require_service("Restarting CrossWatch")
    if not yes:
        state.out.warn("This restarts the running service.")
        if not typer.confirm("Restart now?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    try:
        state.post("/api/maintenance/restart")
    except CLIError as err:
        if int(getattr(err, "status", 0) or 0) or "Cannot reach" not in err.message:
            raise
    if state.out.json_mode:
        state.out.data({"ok": True, "restarting": True})
        return
    state.out.success("Restart requested.")


def register(app: typer.Typer) -> None:
    app.add_typer(maintenance_app, name="maintenance")
