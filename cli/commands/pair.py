# /cli/commands/pair.py
# CrossWatch - CLI sync pair commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_USAGE, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text, find_pair, pair_features, pair_label

pair_app = typer.Typer(help="Inspect and toggle sync pairs.", no_args_is_help=True)


def _pairs(state: Ctx) -> list[dict[str, Any]]:
    payload = state.get("/api/pairs")
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Cannot read pairs"))
    if not isinstance(payload, list):
        return []
    return [p for p in payload if isinstance(p, dict)]


def _set_enabled(state: Ctx, target: str, enabled: bool) -> dict[str, Any]:
    pair = find_pair(_pairs(state), target)
    pair_id = str(pair.get("id") or "")
    result = state.put(f"/api/pairs/{pair_id}", json_body={"enabled": bool(enabled)})
    if isinstance(result, dict) and result.get("ok") is False:
        raise CLIError(error_text(result, "Update rejected"))
    return pair


@pair_app.command("list")
def pair_list(
    ctx: typer.Context,
    enabled_only: bool = typer.Option(False, "--enabled", help="Only show enabled pairs."),
) -> None:
    """List configured sync pairs."""
    state: Ctx = ctx.obj
    pairs = _pairs(state)
    if enabled_only:
        pairs = [p for p in pairs if p.get("enabled", True) is not False]
    if state.out.json_mode:
        state.out.data(pairs)
        return
    state.out.table(
        ["ID", "PAIR", "MODE", "STATE", "FEATURES"],
        [
            [
                str(p.get("id") or ""),
                pair_label(p),
                str(p.get("mode") or "one-way"),
                state_text(p.get("enabled", True) is not False, on="enabled", off="disabled"),
                ", ".join(pair_features(p)) or "-",
            ]
            for p in pairs
        ],
        title="Sync pairs",
        empty="No pairs configured yet.",
    )


@pair_app.command("create")
def pair_create(
    ctx: typer.Context,
    source: str = typer.Argument(..., help="Source provider, for example PLEX."),
    target: str = typer.Argument(..., help="Target provider, for example TRAKT."),
    mode: str = typer.Option("one-way", "--mode", "-m", help="one-way or two-way."),
    feature: list[str] = typer.Option([], "--feature", "-F", help="Enable a feature. Repeatable."),
    source_instance: str = typer.Option("", "--source-instance", help="Source instance id."),
    target_instance: str = typer.Option("", "--target-instance", help="Target instance id."),
    profile: str = typer.Option("", "--profile", help="User profile id."),
    disabled: bool = typer.Option(False, "--disabled", help="Create it switched off."),
) -> None:
    """Create a sync pair."""
    state: Ctx = ctx.obj
    wanted = str(mode).strip().lower()
    if wanted not in ("one-way", "two-way"):
        raise CLIError(f"Unknown mode '{mode}'", hint="Use one-way or two-way.", exit_code=EXIT_USAGE)

    body: dict[str, Any] = {
        "source": str(source).strip().upper(),
        "target": str(target).strip().upper(),
        "mode": wanted,
        "enabled": not disabled,
    }
    if feature:
        body["features"] = {str(f).strip().lower(): {"enable": True} for f in feature if str(f).strip()}
    if source_instance.strip():
        body["source_instance"] = source_instance.strip()
    if target_instance.strip():
        body["target_instance"] = target_instance.strip()
    if profile.strip():
        body["profile_id"] = profile.strip()

    result = as_dict(state.post("/api/pairs", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Pair was rejected"))
    pair_id = str(result.get("id") or "")
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"Created {body['source']} {'<->' if wanted == 'two-way' else '->'} {body['target']} ({pair_id})")
    try:
        created = find_pair(_pairs(state), pair_id)
    except CLIError:
        return
    enabled = pair_features(created)
    if enabled:
        state.out.info(f"Features on: {', '.join(enabled)}")
    else:
        state.out.info(f"No features enabled. Turn one on with 'cw pair feature {pair_id} watchlist on'.")


@pair_app.command("reorder")
def pair_reorder(
    ctx: typer.Context,
    pair_ids: list[str] = typer.Argument(..., help="Pair ids in the order you want them."),
) -> None:
    """Set the order pairs run in."""
    state: Ctx = ctx.obj
    pairs = _pairs(state)
    resolved = [str(find_pair(pairs, p).get("id") or "") for p in pair_ids]
    known = {str(p.get("id") or "") for p in pairs}
    missing = sorted(known - set(resolved))
    if missing:
        raise CLIError(
            "Every pair must be listed when reordering",
            hint=f"Missing: {', '.join(missing)}",
            exit_code=EXIT_USAGE,
        )
    result = as_dict(state.post("/api/pairs/reorder", json_body=resolved))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Reorder rejected"))
    if state.out.json_mode:
        state.out.data({"ok": True, "order": resolved})
        return
    state.out.success("Order updated.")


@pair_app.command("show")
def pair_show(ctx: typer.Context, pair_id: str = typer.Argument(..., help="Pair id or prefix.")) -> None:
    """Show one pair in full."""
    state: Ctx = ctx.obj
    pair = find_pair(_pairs(state), pair_id)
    if state.out.json_mode:
        state.out.data(pair)
        return
    state.out.kv(
        [
            ("Id", str(pair.get("id") or "")),
            ("Route", pair_label(pair)),
            ("Mode", str(pair.get("mode") or "one-way")),
            ("Enabled", state_text(pair.get("enabled", True) is not False, on="yes", off="no")),
            ("Source instance", str(pair.get("source_instance") or "default")),
            ("Target instance", str(pair.get("target_instance") or "default")),
            ("Profile", str(pair.get("profile_id") or "-")),
        ],
        title=pair_label(pair),
    )
    features = as_dict(pair.get("features"))
    rows: list[list[Any]] = []
    for name in sorted(features):
        block = features.get(name)
        if isinstance(block, dict):
            enabled = coerce_bool(block.get("enable", block.get("enabled")), False)
            extras = {k: v for k, v in block.items() if k not in ("enable", "enabled")}
            rows.append([name, state_text(enabled, on="on", off="off"), json.dumps(extras, default=str) if extras else "-"])
        else:
            rows.append([name, state_text(coerce_bool(block, False), on="on", off="off"), "-"])
    if rows:
        state.out.print()
        state.out.table(["FEATURE", "STATE", "OPTIONS"], rows, title="Features")


@pair_app.command("enable")
def pair_enable(ctx: typer.Context, pair_id: str = typer.Argument(..., help="Pair id or prefix.")) -> None:
    """Enable a pair."""
    state: Ctx = ctx.obj
    pair = _set_enabled(state, pair_id, True)
    if state.out.json_mode:
        state.out.data({"ok": True, "id": pair.get("id"), "enabled": True})
        return
    state.out.success(f"Enabled {pair_label(pair)} ({pair.get('id')})")


@pair_app.command("disable")
def pair_disable(ctx: typer.Context, pair_id: str = typer.Argument(..., help="Pair id or prefix.")) -> None:
    """Disable a pair."""
    state: Ctx = ctx.obj
    pair = _set_enabled(state, pair_id, False)
    if state.out.json_mode:
        state.out.data({"ok": True, "id": pair.get("id"), "enabled": False})
        return
    state.out.success(f"Disabled {pair_label(pair)} ({pair.get('id')})")


@pair_app.command("feature")
def pair_feature(
    ctx: typer.Context,
    pair_id: str = typer.Argument(..., help="Pair id or prefix."),
    feature: str = typer.Argument(..., help="Feature name, for example watchlist, ratings, history, playlists."),
    value: str = typer.Argument(..., help="on or off."),
) -> None:
    """Turn one feature of a pair on or off."""
    state: Ctx = ctx.obj
    pair = find_pair(_pairs(state), pair_id)
    features = dict(pair.get("features") or {})
    key = str(feature).strip().lower()
    if key not in features:
        known = ", ".join(sorted(features)) or "none"
        raise CLIError(f"Pair has no feature '{key}'", hint=f"Known features: {known}", exit_code=EXIT_USAGE)
    flag = coerce_bool(value, False)
    block = features.get(key)
    if isinstance(block, dict):
        block = dict(block)
        block["enable"] = flag
        features[key] = block
    else:
        features[key] = flag
    result = state.put(f"/api/pairs/{pair.get('id')}", json_body={"features": features})
    if isinstance(result, dict) and result.get("ok") is False:
        raise CLIError(error_text(result, "Update rejected"))
    if state.out.json_mode:
        state.out.data({"ok": True, "id": pair.get("id"), "feature": key, "enabled": flag})
        return
    state.out.success(f"{pair_label(pair)}: {key} is now {'on' if flag else 'off'}")


@pair_app.command("delete")
def pair_delete(
    ctx: typer.Context,
    pair_id: str = typer.Argument(..., help="Pair id or prefix."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a pair."""
    state: Ctx = ctx.obj
    pair = find_pair(_pairs(state), pair_id)
    if not yes:
        state.out.print(f"About to delete [cw.accent]{pair_label(pair)}[/] ({pair.get('id')})")
        if not typer.confirm("Delete this pair?", default=False):
            raise CLIError("Cancelled", exit_code=0)
    result = state.delete(f"/api/pairs/{pair.get('id')}")
    if isinstance(result, dict) and result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data({"ok": True, "deleted": pair.get("id")})
        return
    state.out.success(f"Deleted {pair_label(pair)} ({pair.get('id')})")


def register(app: typer.Typer) -> None:
    app.add_typer(pair_app, name="pair")
