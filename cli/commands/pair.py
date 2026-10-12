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
from .._util import FALSE_WORDS, TRUE_WORDS, as_dict, coerce_bool, error_text, find_pair, pair_features, pair_label

pair_app = typer.Typer(help="Inspect and toggle sync pairs.", no_args_is_help=True)

BOOL_KEYS = ("add", "remove", "rewatches")
SETTING_KEYS = (*BOOL_KEYS, "remove_mode", "libraries")
REMOVE_MODES = {"source_deletes": "source_deletes", "source": "source_deletes", "mirror": "mirror"}
REMOVE_MODE_FEATURES = ("watchlist", "ratings", "history", "progress", "collection")
LIBRARY_FEATURES = ("history", "ratings", "progress", "collection")
LIBRARY_PROVIDERS = ("PLEX", "JELLYFIN", "EMBY", "KODI")


def _pairs(state: Ctx) -> list[dict[str, Any]]:
    payload = state.get("/api/pairs")
    if isinstance(payload, dict) and payload.get("ok") is False:
        raise CLIError(error_text(payload, "Cannot read pairs"))
    if not isinstance(payload, list):
        return []
    return [p for p in payload if isinstance(p, dict)]


def _two_way(pair: dict[str, Any]) -> bool:
    return str(pair.get("mode") or "").strip().lower().startswith("two")


def _sync_defaults(state: Ctx) -> dict[str, Any]:
    try:
        cfg = as_dict(state.get("/api/config"))
    except Exception:
        return {}
    return as_dict(cfg.get("sync"))


def _mirror_warnings(pair: dict[str, Any], sync: dict[str, Any] | None = None, only: str = "") -> list[str]:
    pair_id = str(pair.get("id") or "")
    features = as_dict(pair.get("features"))
    defaults = sync or {}
    global_mode = str(defaults.get("one_way_remove_mode") or "").strip().lower()
    out: list[str] = []
    for name in sorted(features):
        if name not in REMOVE_MODE_FEATURES or (only and name != only):
            continue
        block = as_dict(features.get(name))
        stored = str(block.get("remove_mode") or "").strip().lower()
        if (stored or global_mode) != "mirror":
            continue
        removing = block.get("remove")
        if removing is None:
            removing = defaults.get("enable_remove")
        origin = "stored on this pair" if stored == "mirror" else "inherited from sync.one_way_remove_mode"
        reset = f"cw pair set {pair_id} {name}.remove_mode source_deletes"
        if _two_way(pair):
            if stored == "mirror":
                out.append(
                    f"{name}: mirror mode is stored on this pair. It is not used while the pair is two-way, "
                    f"but applies again if the pair becomes one-way. Reset it with '{reset}'."
                )
        elif coerce_bool(removing, False):
            out.append(
                f"{name}: Remove runs in mirror mode ({origin}). The target is made to match the source, "
                f"so everything missing from the source is removed from the target. Use '{reset}' to only remove what was deleted at the source."
            )
        else:
            out.append(
                f"{name}: mirror mode is {origin} while Remove is off. Turning Remove on would make the target match "
                f"the source and can remove many items. Reset it with '{reset}'."
            )
    return out


def _strict_bool(key: str, value: str) -> bool:
    low = str(value or "").strip().lower()
    if low in TRUE_WORDS:
        return True
    if low in FALSE_WORDS:
        return False
    raise CLIError(f"'{value}' is not a valid value for {key}", hint="Use on or off.", exit_code=EXIT_USAGE)


def _library_values(raw: Any) -> list[str]:
    values = raw.split(",") if isinstance(raw, str) else raw
    if not isinstance(values, (list, tuple)):
        raise CLIError("Library ids must be a list", hint="Example: PLEX=1,2", exit_code=EXIT_USAGE)
    out: list[str] = []
    for value in values:
        text = str(value).strip()
        if text and text not in out:
            out.append(text)
    return out


def _libraries(pair: dict[str, Any], feature: str, block: dict[str, Any], value: str) -> dict[str, list[str]]:
    if feature not in LIBRARY_FEATURES:
        raise CLIError(
            f"Library scope is not available for {feature}",
            hint=f"Features with library scope: {', '.join(LIBRARY_FEATURES)}",
            exit_code=EXIT_USAGE,
        )
    text = str(value or "").strip()
    endpoints = {str(pair.get(side) or "").strip().upper() for side in ("source", "target")}
    allowed = sorted(endpoints & set(LIBRARY_PROVIDERS))

    def endpoint(key: Any, *, check: bool = True) -> str:
        name, _, instance = str(key).strip().partition("#")
        if check and name.upper() not in allowed:
            raise CLIError(
                f"'{name}' has no library scope in this pair",
                hint=f"Providers with library scope here: {', '.join(allowed) or 'none'}",
                exit_code=EXIT_USAGE,
            )
        return f"{name.upper()}#{instance.strip()}" if instance.strip() else name.upper()

    current = block.get("libraries")
    kept: dict[str, Any] = {endpoint(key, check=False): raw for key, raw in current.items()} if isinstance(current, dict) else {}
    incoming: Any = {}
    if text.startswith("{"):
        try:
            incoming = json.loads(text)
        except Exception as exc:
            raise CLIError(f"Not valid JSON: {text}", exit_code=EXIT_USAGE) from exc
        if not isinstance(incoming, dict):
            raise CLIError("Libraries must be a JSON object of provider to library ids", exit_code=EXIT_USAGE)
        kept = {}
    elif "=" in text:
        provider, _, ids = text.partition("=")
        incoming = {provider: ids}
    else:
        raise CLIError(
            "Libraries need a provider",
            hint="Use PLEX=1,2 for one provider, PLEX= to clear it, or a JSON object to replace them all.",
            exit_code=EXIT_USAGE,
        )
    merged = {**kept, **{endpoint(key): raw for key, raw in incoming.items()}}
    out: dict[str, list[str]] = {}
    for key, raw in merged.items():
        values = _library_values(raw)
        if values:
            out[key] = values
    return out


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
    add_only: bool = typer.Option(False, "--add-only", help="Every enabled feature adds only: add on, remove off."),
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
    names = [str(f).strip().lower() for f in feature if str(f).strip()]
    if add_only:
        body["features"] = {name: {"enable": True, "add": True, "remove": False} for name in names or ["watchlist"]}
    elif names:
        body["features"] = {name: {"enable": True} for name in names}
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
    warnings = _mirror_warnings(pair, _sync_defaults(state))
    if state.out.json_mode:
        state.out.data({**pair, "warnings": warnings})
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
    for warning in warnings:
        state.out.warn(warning)


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
    feature: str = typer.Argument(..., help="Feature name, for example watchlist, ratings, history, playlists, collection."),
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


@pair_app.command("set")
def pair_set(
    ctx: typer.Context,
    pair_id: str = typer.Argument(..., help="Pair id or prefix."),
    setting: str = typer.Argument(..., help="<feature>.<key>. Keys: add, remove, remove_mode, rewatches, libraries."),
    value: str = typer.Argument(..., help="on or off, source_deletes or mirror, or PROVIDER=id,id for libraries."),
) -> None:
    """Set one option of a pair feature, for example history.remove off."""
    state: Ctx = ctx.obj
    pair = find_pair(_pairs(state), pair_id)
    features = dict(pair.get("features") or {})
    name, _, key = str(setting).strip().lower().partition(".")
    if not name or key not in SETTING_KEYS:
        raise CLIError(
            f"Unknown setting '{setting}'",
            hint=f"Use <feature>.<key> with one of: {', '.join(SETTING_KEYS)}",
            exit_code=EXIT_USAGE,
        )
    if name not in features:
        known = ", ".join(sorted(features)) or "none"
        raise CLIError(f"Pair has no feature '{name}'", hint=f"Known features: {known}", exit_code=EXIT_USAGE)
    current = features.get(name)
    block: dict[str, Any] = dict(current) if isinstance(current, dict) else {"enable": coerce_bool(current, False)}

    stored: Any
    if key == "rewatches" and name != "history":
        raise CLIError("Rewatches only apply to history", exit_code=EXIT_USAGE)
    if key in BOOL_KEYS:
        stored = _strict_bool(key, value)
        block[key] = stored
    elif key == "remove_mode":
        if name not in REMOVE_MODE_FEATURES:
            raise CLIError(f"Remove mode is not available for {name}", exit_code=EXIT_USAGE)
        stored = REMOVE_MODES.get(str(value or "").strip().lower())
        if stored is None:
            raise CLIError(f"Unknown remove mode '{value}'", hint="Use source_deletes or mirror.", exit_code=EXIT_USAGE)
        block[key] = stored
    else:
        stored = _libraries(pair, name, block, value)
        if stored:
            block[key] = stored
        else:
            block.pop(key, None)

    features[name] = block
    result = state.put(f"/api/pairs/{pair.get('id')}", json_body={"features": features})
    if isinstance(result, dict) and result.get("ok") is False:
        raise CLIError(error_text(result, "Update rejected"))
    warnings = _mirror_warnings({**pair, "features": features}, _sync_defaults(state), only=name)
    if state.out.json_mode:
        state.out.data({"ok": True, "id": pair.get("id"), "feature": name, "key": key, "value": stored, "warnings": warnings})
        return
    shown = ("on" if stored else "off") if isinstance(stored, bool) else (json.dumps(stored) if isinstance(stored, dict) else stored)
    state.out.success(f"{pair_label(pair)}: {name}.{key} is now {shown}")
    for warning in warnings:
        state.out.warn(warning)


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
