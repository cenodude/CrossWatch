# /cli/commands/playlist.py
# CrossWatch - CLI playlist commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_NOT_FOUND, EXIT_USAGE, CLIError
from .._render import state_text
from .._util import as_dict, coerce_bool, error_text, fmt_ts, parse_value, rows_from_payload

playlist_app = typer.Typer(help="Playlist endpoints, mappings and rulesets.", no_args_is_help=True)
resource_app = typer.Typer(help="Provider playlist resources.", no_args_is_help=True)
endpoint_app = typer.Typer(help="Playlist endpoints.", no_args_is_help=True)
mapping_app = typer.Typer(help="Playlist mappings.", no_args_is_help=True)
ruleset_app = typer.Typer(help="Playlist rulesets.", no_args_is_help=True)


def _rows(payload: Any, *keys: str) -> list[dict[str, Any]]:
    return rows_from_payload(payload, *keys)


def _fields(raw: list[str]) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for item in raw or []:
        key, sep, value = str(item).partition("=")
        key = key.strip()
        if not sep or not key:
            raise CLIError(f"--field expects key=value, got '{item}'", exit_code=EXIT_USAGE)
        body[key] = parse_value(value)
    return body


def _put_if(text: str, body: dict[str, Any], key: str, value: Any) -> None:
    if str(text or "").strip():
        body[key] = value


def _find_id(items: list[dict[str, Any]], needle: str, label: str) -> dict[str, Any]:
    wanted = str(needle or "").strip()
    if not wanted:
        raise CLIError(f"A {label} id is required", exit_code=EXIT_USAGE)
    exact = [item for item in items if str(item.get("id") or "") == wanted]
    if exact:
        return exact[0]
    partial = [item for item in items if str(item.get("id") or "").lower().startswith(wanted.lower())]
    if len(partial) == 1:
        return partial[0]
    if len(partial) > 1:
        ids = ", ".join(str(item.get("id") or "") for item in partial[:6])
        raise CLIError(f"{label.title()} id '{wanted}' is ambiguous", hint=f"Matches: {ids}", exit_code=EXIT_USAGE)
    raise CLIError(f"No {label} matches '{wanted}'", exit_code=EXIT_NOT_FOUND)


def _endpoint_items(state: Ctx) -> list[dict[str, Any]]:
    return _rows(state.get("/api/playlists/endpoints"), "endpoints", "items")


def _mapping_items(state: Ctx) -> list[dict[str, Any]]:
    return _rows(state.get("/api/playlists/mappings"), "mappings", "items")


def _endpoint_label(endpoint: Any) -> str:
    ep = endpoint if isinstance(endpoint, dict) else {}
    provider = str(ep.get("provider") or "").upper()
    inst = str(ep.get("instance") or "default")
    name = str(ep.get("playlist_name") or ep.get("name") or ep.get("playlist_id") or "-")
    return f"{provider}:{inst} {name}" if provider else name


def _mapping_source(mapping: dict[str, Any]) -> str:
    src = mapping.get("source")
    if isinstance(src, dict):
        return _endpoint_label(src)
    return str(mapping.get("source_endpoint") or mapping.get("source") or "-")


def _mapping_targets(mapping: dict[str, Any]) -> str:
    targets = mapping.get("targets")
    if isinstance(targets, list) and targets:
        return ", ".join(_endpoint_label(t) for t in targets if isinstance(t, dict))
    target = mapping.get("target")
    if isinstance(target, dict):
        return _endpoint_label(target)
    raw = mapping.get("target_endpoints")
    if isinstance(raw, list):
        return ", ".join(str(x) for x in raw)
    return str(raw or mapping.get("target") or "-")


def _mapping_payload(mapping: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(mapping.get("id") or ""),
        "name": str(mapping.get("name") or ""),
        "source_endpoint": str(mapping.get("source_endpoint") or ""),
        "target_endpoints": list(mapping.get("target_endpoints") or []),
        "ruleset_id": str(mapping.get("ruleset_id") or ""),
        "membership": str(mapping.get("membership") or "managed_only"),
        "order": str(mapping.get("order") or "ignore"),
        "enabled": coerce_bool(mapping.get("enabled"), True),
        "allow_mass_delete": coerce_bool(mapping.get("allow_mass_delete"), False),
    }


def _run_result(payload: dict[str, Any]) -> dict[str, Any]:
    result = payload.get("result")
    return dict(result) if isinstance(result, dict) else payload


def _print_playlist_counts(state: Ctx, result: dict[str, Any]) -> None:
    for key in ("planned_additions", "planned_removals", "added", "removed", "reordered", "skipped", "unresolved_count", "errors"):
        if key in result:
            state.out.info(f"{key}: {result.get(key)}")


def _load_json_file(path: str) -> dict[str, Any]:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        raise CLIError(f"Cannot read JSON from {path}: {exc}", exit_code=EXIT_USAGE) from exc
    if not isinstance(data, dict):
        raise CLIError("JSON file must contain an object", exit_code=EXIT_USAGE)
    return data


def _short_name(value: str, fallback: str) -> str:
    text = str(value or fallback).strip()
    cleaned = "".join(ch for ch in text if ch.isalnum() or ch in " _.'-&()").strip()
    if not cleaned or not cleaned[0].isalnum():
        cleaned = fallback
    return cleaned[:10] or fallback[:10]


def _resource_rows(state: Ctx, provider: str, instance: str) -> list[dict[str, Any]]:
    params = {"provider": provider.strip().upper()}
    if instance.strip():
        params["instance"] = instance.strip()
    return _rows(state.get("/api/playlists/resources", params=params), "resources", "items", "playlists")


def _choose_resource(state: Ctx, provider: str, instance: str, prompt: str) -> str:
    rows = _resource_rows(state, provider, instance)
    state.out.table(
        ["#", "ID", "NAME", "WRITE"],
        [
            [
                str(idx),
                str(row.get("id") or "-")[:24],
                str(row.get("name") or row.get("title") or "-")[:40],
                state_text(coerce_bool(row.get("writable") or row.get("can_add") or row.get("can_remove"), False), on="yes", off="no"),
            ]
            for idx, row in enumerate(rows, start=1)
        ],
        title=f"{provider.upper()} playlists",
        empty="No playlists found.",
    )
    answer = str(typer.prompt(prompt) or "").strip()
    if answer.isdigit():
        pos = int(answer)
        if 1 <= pos <= len(rows):
            return str(rows[pos - 1].get("id") or "")
    return answer


def _post_endpoint(state: Ctx, body: dict[str, Any]) -> dict[str, Any]:
    result = as_dict(state.post("/api/playlists/endpoints", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Endpoint rejected"))
    return as_dict(result.get("endpoint"))


def _post_mapping(state: Ctx, body: dict[str, Any]) -> dict[str, Any]:
    result = as_dict(state.post("/api/playlists/mappings", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Mapping rejected"))
    return result


@playlist_app.command("overview")
def playlist_overview(ctx: typer.Context) -> None:
    """Show the playlist setup."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/playlists/overview"))
    if state.out.json_mode:
        state.out.data(payload)
        return
    state.out.kv(
        [(key, value) for key, value in payload.items() if not isinstance(value, (dict, list))],
        title="Playlists",
    )


@playlist_app.command("providers")
def playlist_providers(ctx: typer.Context) -> None:
    """Show which providers can hold playlists."""
    state: Ctx = ctx.obj
    payload = state.get("/api/playlists/providers")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "providers", "items")
    if rows:
        state.out.records(
            rows,
            [
                ("PROVIDER", lambda r: str(r.get("name") or r.get("provider") or "-")),
                ("INSTANCE", lambda r: str(r.get("instance") or "-")),
            ],
            title="Playlist providers",
        )
        return
    block = as_dict(payload)
    names = block.get("providers")
    if isinstance(names, list):
        state.out.table(["PROVIDER"], [[str(n)] for n in names], title="Playlist providers")
        return
    state.out.kv([(k, v) for k, v in block.items() if not isinstance(v, (dict, list))], title="Playlist providers")


@playlist_app.command("resources")
def playlist_resources(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
) -> None:
    """List the playlists a provider holds."""
    state: Ctx = ctx.obj
    params: dict[str, Any] = {"provider": provider.strip().upper()}
    if instance.strip():
        params["instance"] = instance.strip()
    payload = state.get("/api/playlists/resources", params=params)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "resources", "items", "playlists")
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or r.get("key") or "-")[:24]),
            ("NAME", lambda r: str(r.get("name") or r.get("title") or "-")[:40]),
            ("KIND", lambda r: str(r.get("kind") or r.get("playlist_type") or "-")),
            ("WRITE", lambda r: state_text(coerce_bool(r.get("writable") or r.get("can_add") or r.get("can_remove"), False), on="yes", off="no")),
            ("ITEMS", lambda r: str(r.get("count") or r.get("item_count") or r.get("items") or "-")),
        ],
        title=f"{provider.upper()} playlists",
        empty="No playlists there.",
    )


@resource_app.command("list")
def resource_list(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
) -> None:
    """List provider playlist resources."""
    playlist_resources(ctx, provider=provider, instance=instance)


@resource_app.command("create")
def resource_create(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    name: str = typer.Option(..., "--name", "-n", help="Playlist name."),
    media_type: str = typer.Option("playlist", "--type", help="Playlist media type."),
    instance: str = typer.Option("default", "--instance", "-i", help="Provider instance id."),
) -> None:
    """Create a playlist at a provider."""
    state: Ctx = ctx.obj
    state.require_service("Creating a provider playlist")
    result = as_dict(
        state.post(
            "/api/playlists/resources",
            json_body={"provider": provider.strip().upper(), "instance": instance.strip() or "default", "name": name, "media_type": media_type},
        )
    )
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Create rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    resource = as_dict(result.get("resource"))
    state.out.success(f"Created {_endpoint_label(resource)} ({resource.get('id') or result.get('playlist_id') or '-'})")


@resource_app.command("rename")
def resource_rename(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    playlist_id: str = typer.Argument(..., help="Provider playlist id."),
    name: str = typer.Option(..., "--name", "-n", help="New playlist name."),
    instance: str = typer.Option("default", "--instance", "-i", help="Provider instance id."),
) -> None:
    """Rename a provider playlist."""
    state: Ctx = ctx.obj
    state.require_service("Renaming a provider playlist")
    result = as_dict(
        state.patch(
            f"/api/playlists/resources/{playlist_id}",
            json_body={"provider": provider.strip().upper(), "instance": instance.strip() or "default", "name": name},
        )
    )
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rename rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    resource = as_dict(result.get("resource"))
    state.out.success(f"Renamed playlist {playlist_id} to {resource.get('name') or name}.")


@resource_app.command("delete")
def resource_delete(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name."),
    playlist_id: str = typer.Argument(..., help="Provider playlist id."),
    instance: str = typer.Option("default", "--instance", "-i", help="Provider instance id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a provider playlist."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a provider playlist")
    if not yes and not typer.confirm(f"Delete {provider.upper()} playlist {playlist_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(
        state.delete(
            f"/api/playlists/resources/{playlist_id}",
            params={"provider": provider.strip().upper(), "instance": instance.strip() or "default"},
        )
    )
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted {provider.upper()} playlist {playlist_id}.")


@playlist_app.command("activity")
def playlist_activity(ctx: typer.Context) -> None:
    """Show recent playlist runs."""
    state: Ctx = ctx.obj
    payload = state.get("/api/playlists/activity")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "activity", "items", "runs")
    state.out.records(
        rows,
        [
            ("WHEN", lambda r: fmt_ts(r.get("ts") or r.get("created_at") or r.get("finished_at"))),
            ("MAPPING", lambda r: str(r.get("mapping") or r.get("mapping_id") or "-")[:24]),
            ("RESULT", lambda r: str(r.get("result") or r.get("status") or "-")),
            ("CHANGES", lambda r: str(r.get("changes") or r.get("added") or "-")),
        ],
        title="Playlist activity",
        empty="No playlist runs yet.",
    )


@playlist_app.command("setup")
def playlist_setup(
    ctx: typer.Context,
    source_provider: str = typer.Option("", "--source-provider", help="Source provider."),
    source_instance: str = typer.Option("default", "--source-instance", help="Source provider instance."),
    source_playlist: str = typer.Option("", "--source-playlist", help="Source provider playlist id."),
    target_provider: str = typer.Option("", "--target-provider", help="Target provider."),
    target_instance: str = typer.Option("default", "--target-instance", help="Target provider instance."),
    target_playlist: list[str] = typer.Option([], "--target-playlist", help="Target provider playlist id. Repeatable."),
    create_target: str = typer.Option("", "--create-target", help="Create this target playlist on first run."),
    name: str = typer.Option("", "--name", "-n", help="Mapping name."),
    ruleset: str = typer.Option("", "--ruleset", "-r", help="Ruleset id, for example trakt_free_account."),
    membership: str = typer.Option("managed_only", "--membership", help="managed_only or mirror."),
    order: str = typer.Option("ignore", "--order", help="ignore or preserve."),
    run_now: bool = typer.Option(False, "--run-now", help="Run the mapping after setup."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview the mapping after setup instead of running."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Guided setup for a playlist mapping."""
    state: Ctx = ctx.obj
    state.require_service("Setting up playlists")

    src_provider = source_provider.strip().upper() or str(typer.prompt("Source provider", default="TRAKT") or "TRAKT").strip().upper()
    src_instance = source_instance.strip() or "default"
    src_playlist = source_playlist.strip() or _choose_resource(state, src_provider, src_instance, "Source playlist id or number")

    dst_provider = target_provider.strip().upper() or str(typer.prompt("Target provider", default="PLEX") or "PLEX").strip().upper()
    dst_instance = target_instance.strip() or "default"
    targets = [str(t).strip() for t in target_playlist if str(t).strip()]
    pending_name = create_target.strip()
    if not targets and not pending_name:
        state.out.print("Choose an existing target playlist, or enter +Name to create it on first run.")
        answer = _choose_resource(state, dst_provider, dst_instance, "Target playlist id, number, or +Name")
        if answer.startswith("+"):
            pending_name = answer[1:].strip()
        else:
            targets = [answer]
    if targets and pending_name:
        raise CLIError("Use either --target-playlist or --create-target, not both.", exit_code=EXIT_USAGE)
    if not targets and not pending_name:
        raise CLIError("A target playlist or --create-target name is required", exit_code=EXIT_USAGE)

    map_name = _short_name(name or pending_name or src_playlist, "Playlist")
    src_endpoint_body = {
        "name": _short_name(f"{map_name}Src", "Src"),
        "provider": src_provider,
        "instance": src_instance,
        "playlist_id": src_playlist,
    }
    target_endpoint_bodies: list[dict[str, Any]] = []
    if pending_name:
        target_endpoint_bodies.append(
            {
                "name": _short_name(f"{map_name}Dst", "Dst"),
                "provider": dst_provider,
                "instance": dst_instance,
                "playlist_id": "",
                "playlist_name": pending_name,
                "pending_create": {"name": pending_name, "media_type": "playlist"},
            }
        )
    else:
        for idx, playlist_id in enumerate(targets, start=1):
            target_endpoint_bodies.append(
                {
                    "name": _short_name(f"{map_name}{idx}", "Target"),
                    "provider": dst_provider,
                    "instance": dst_instance,
                    "playlist_id": playlist_id,
                }
            )

    mapping_body = {
        "name": map_name,
        "source_endpoint": "<created>",
        "target_endpoints": ["<created>"],
        "ruleset_id": ruleset.strip(),
        "membership": membership.strip().lower() or "managed_only",
        "order": order.strip().lower() or "ignore",
        "enabled": True,
    }
    if not yes:
        state.out.kv(
            [
                ("Source", f"{src_provider}:{src_instance} {src_playlist}"),
                ("Target", f"{dst_provider}:{dst_instance} {pending_name or ', '.join(targets)}"),
                ("Mapping", map_name),
                ("Ruleset", ruleset.strip() or "direct"),
                ("Membership", mapping_body["membership"]),
                ("Order", mapping_body["order"]),
            ],
            title="Playlist setup",
        )
        if not typer.confirm("Create endpoint(s) and mapping?", default=True):
            raise CLIError("Cancelled", exit_code=0)

    src_endpoint = _post_endpoint(state, src_endpoint_body)
    target_endpoints = [_post_endpoint(state, body) for body in target_endpoint_bodies]
    mapping_body["source_endpoint"] = str(src_endpoint.get("id") or "")
    mapping_body["target_endpoints"] = [str(ep.get("id") or "") for ep in target_endpoints if ep.get("id")]
    result = _post_mapping(state, mapping_body)
    mapping = as_dict(result.get("mapping"))
    mapping_id = str(mapping.get("id") or "")

    if state.out.json_mode:
        state.out.data({"ok": True, "source_endpoint": src_endpoint, "target_endpoints": target_endpoints, **result})
        return

    state.out.success(f"Playlist mapping created: {mapping_id or '-'}")
    if result.get("pair_id"):
        state.out.info(f"Managed pair: {result.get('pair_id')}")

    if dry_run and mapping_id:
        mapping_preview(ctx, mapping_id)
    if run_now and mapping_id:
        mapping_run(ctx, mapping_id, dry_run=False)


@endpoint_app.command("list")
def endpoint_list(ctx: typer.Context) -> None:
    """List playlist endpoints."""
    state: Ctx = ctx.obj
    payload = state.get("/api/playlists/endpoints")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "endpoints", "items")
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or "-")),
            ("NAME", lambda r: str(r.get("name") or r.get("label") or "-")[:32]),
            ("PROVIDER", lambda r: str(r.get("provider") or "-")),
            ("ENABLED", lambda r: state_text(coerce_bool(r.get("enabled"), True), on="yes", off="no")),
        ],
        title="Playlist endpoints",
        empty="No endpoints configured.",
    )


@endpoint_app.command("add")
def endpoint_add(
    ctx: typer.Context,
    provider: str = typer.Argument("", help="Provider name, for example PLEX."),
    playlist_id: str = typer.Argument("", help="Provider playlist id. Omit with --create."),
    name: str = typer.Option("", "--name", "-n", help="Short endpoint name."),
    instance: str = typer.Option("default", "--instance", "-i", help="Provider instance id."),
    create: str = typer.Option("", "--create", help="Create this provider playlist and bind it."),
    media_type: str = typer.Option("", "--type", help="Created playlist media type."),
    field: list[str] = typer.Option([], "--field", "-F", help="Endpoint setting as key=value. Repeatable."),
) -> None:
    """Add a playlist endpoint."""
    state: Ctx = ctx.obj
    state.require_service("Adding a playlist endpoint")
    body = _fields(field)
    _put_if(provider, body, "provider", provider.strip().upper())
    _put_if(playlist_id, body, "playlist_id", playlist_id.strip())
    _put_if(name, body, "name", name.strip())
    _put_if(instance, body, "instance", instance.strip() or "default")
    if create.strip():
        body["create"] = True
        body["create_name"] = create.strip()
        body.setdefault("name", create.strip()[:10])
    _put_if(media_type, body, "media_type", media_type.strip().lower())
    if not body:
        raise CLIError("An endpoint needs fields", hint="For example --field provider=PLEX --field name=Favourites", exit_code=EXIT_USAGE)
    result = as_dict(state.post("/api/playlists/endpoints", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    endpoint = as_dict(result.get("endpoint"))
    state.out.success(f"Endpoint {'created' if result.get('created') else 'saved'}: {endpoint.get('id') or '-'}")


@endpoint_app.command("edit")
def endpoint_edit(
    ctx: typer.Context,
    endpoint_id: str = typer.Argument(..., help="Endpoint id or unique prefix."),
    name: str = typer.Option("", "--name", "-n", help="Endpoint name."),
    provider: str = typer.Option("", "--provider", help="Provider name."),
    playlist_id: str = typer.Option("", "--playlist", help="Provider playlist id."),
    playlist_name: str = typer.Option("", "--playlist-name", help="Provider playlist display name."),
    instance: str = typer.Option("", "--instance", "-i", help="Provider instance id."),
    media_type: str = typer.Option("", "--type", help="Playlist media type."),
    field: list[str] = typer.Option([], "--field", "-F", help="Endpoint setting as key=value. Repeatable."),
) -> None:
    """Edit a playlist endpoint."""
    state: Ctx = ctx.obj
    state.require_service("Editing a playlist endpoint")
    endpoint = _find_id(_endpoint_items(state), endpoint_id, "endpoint")
    body = {**endpoint, **_fields(field)}
    body["id"] = str(endpoint.get("id") or "")
    _put_if(name, body, "name", name.strip())
    _put_if(provider, body, "provider", provider.strip().upper())
    _put_if(playlist_id, body, "playlist_id", playlist_id.strip())
    _put_if(playlist_name, body, "playlist_name", playlist_name.strip())
    _put_if(instance, body, "instance", instance.strip() or "default")
    _put_if(media_type, body, "media_type", media_type.strip().lower())
    result = as_dict(state.post("/api/playlists/endpoints", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Update rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    saved = as_dict(result.get("endpoint"))
    state.out.success(f"Endpoint saved: {saved.get('id') or endpoint.get('id')}")


@endpoint_app.command("sync")
def endpoint_sync(
    ctx: typer.Context,
    endpoint_id: str = typer.Argument(..., help="Endpoint id."),
) -> None:
    """Refresh one endpoint from its provider."""
    state: Ctx = ctx.obj
    state.require_service("Syncing a playlist endpoint")
    result = as_dict(state.post(f"/api/playlists/endpoints/{endpoint_id}/sync"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Endpoint {endpoint_id} synced.")


@endpoint_app.command("delete")
def endpoint_delete(
    ctx: typer.Context,
    endpoint_id: str = typer.Argument(..., help="Endpoint id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a playlist endpoint."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a playlist endpoint")
    if not yes and not typer.confirm(f"Delete endpoint {endpoint_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/playlists/endpoints/{endpoint_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted endpoint {endpoint_id}")


@mapping_app.command("list")
def mapping_list(
    ctx: typer.Context,
    pair: str = typer.Option("", "--pair", "-p", help="Only mappings for this pair id."),
) -> None:
    """List playlist mappings."""
    state: Ctx = ctx.obj
    path = f"/api/playlists/pairs/{pair.strip()}/mappings" if pair.strip() else "/api/playlists/mappings"
    payload = state.get(path)
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "mappings", "items")
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or "-")),
            ("NAME", lambda r: str(r.get("name") or r.get("label") or "-")[:30]),
            ("SOURCE", _mapping_source),
            ("TARGET", _mapping_targets),
            ("PAIR", lambda r: str(r.get("assigned_pair") or "-")),
            ("ENABLED", lambda r: state_text(coerce_bool(r.get("enabled"), True), on="yes", off="no")),
        ],
        title="Playlist mappings",
        empty="No mappings configured.",
    )


@mapping_app.command("add")
def mapping_add(
    ctx: typer.Context,
    source: str = typer.Option("", "--source", "-s", help="Source endpoint id."),
    target: list[str] = typer.Option([], "--target", "-t", help="Target endpoint id. Repeatable."),
    name: str = typer.Option("", "--name", "-n", help="Mapping name."),
    ruleset: str = typer.Option("", "--ruleset", "-r", help="Ruleset id for partitioned or bidirectional mappings."),
    membership: str = typer.Option("", "--membership", help="managed_only or mirror."),
    order: str = typer.Option("", "--order", help="ignore or preserve."),
    disabled: bool = typer.Option(False, "--disabled", help="Create disabled."),
    allow_mass_delete: bool = typer.Option(False, "--allow-mass-delete", help="Allow mirror-style removals at larger scale."),
    field: list[str] = typer.Option([], "--field", "-F", help="Mapping setting as key=value. Repeatable."),
) -> None:
    """Add a playlist mapping."""
    state: Ctx = ctx.obj
    state.require_service("Adding a playlist mapping")
    body = _fields(field)
    _put_if(source, body, "source_endpoint", source.strip())
    if target:
        body["target_endpoints"] = [str(t).strip() for t in target if str(t).strip()]
    _put_if(name, body, "name", name.strip())
    _put_if(ruleset, body, "ruleset_id", ruleset.strip())
    _put_if(membership, body, "membership", membership.strip().lower())
    _put_if(order, body, "order", order.strip().lower())
    if disabled:
        body["enabled"] = False
    if allow_mass_delete:
        body["allow_mass_delete"] = True
    if not body:
        raise CLIError("A mapping needs fields", hint="See 'cw playlist endpoint list' for ids", exit_code=EXIT_USAGE)
    result = as_dict(state.post("/api/playlists/mappings", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    mapping = as_dict(result.get("mapping"))
    state.out.success(f"Mapping {'created' if result.get('created') else 'saved'}: {mapping.get('id') or '-'}")
    if result.get("pair_id"):
        state.out.info(f"Managed pair: {result.get('pair_id')}")


@mapping_app.command("edit")
def mapping_edit(
    ctx: typer.Context,
    mapping_id: str = typer.Argument(..., help="Mapping id or unique prefix."),
    source: str = typer.Option("", "--source", "-s", help="Source endpoint id."),
    target: list[str] = typer.Option([], "--target", "-t", help="Replace target endpoint list. Repeatable."),
    name: str = typer.Option("", "--name", "-n", help="Mapping name."),
    ruleset: str | None = typer.Option(None, "--ruleset", "-r", help="Ruleset id. Pass empty to clear."),
    membership: str = typer.Option("", "--membership", help="managed_only or mirror."),
    order: str = typer.Option("", "--order", help="ignore or preserve."),
    allow_mass_delete: bool | None = typer.Option(None, "--allow-mass-delete/--no-allow-mass-delete", help="Allow or block larger removals."),
    field: list[str] = typer.Option([], "--field", "-F", help="Mapping setting as key=value. Repeatable."),
) -> None:
    """Edit a playlist mapping."""
    state: Ctx = ctx.obj
    state.require_service("Editing a playlist mapping")
    mapping = _find_id(_mapping_items(state), mapping_id, "mapping")
    body = {**_mapping_payload(mapping), **_fields(field)}
    _put_if(source, body, "source_endpoint", source.strip())
    if target:
        body["target_endpoints"] = [str(t).strip() for t in target if str(t).strip()]
    _put_if(name, body, "name", name.strip())
    if ruleset is not None:
        body["ruleset_id"] = str(ruleset or "").strip()
    _put_if(membership, body, "membership", membership.strip().lower())
    _put_if(order, body, "order", order.strip().lower())
    if allow_mass_delete is not None:
        body["allow_mass_delete"] = bool(allow_mass_delete)
    result = as_dict(state.post("/api/playlists/mappings", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Update rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    saved = as_dict(result.get("mapping"))
    state.out.success(f"Mapping saved: {saved.get('id') or mapping.get('id')}")
    if result.get("pair_id"):
        state.out.info(f"Managed pair: {result.get('pair_id')}")


@mapping_app.command("enable")
def mapping_enable(ctx: typer.Context, mapping_id: str = typer.Argument(..., help="Mapping id or unique prefix.")) -> None:
    """Enable a playlist mapping."""
    _set_mapping_enabled(ctx, mapping_id, True)


@mapping_app.command("disable")
def mapping_disable(ctx: typer.Context, mapping_id: str = typer.Argument(..., help="Mapping id or unique prefix.")) -> None:
    """Disable a playlist mapping."""
    _set_mapping_enabled(ctx, mapping_id, False)


def _set_mapping_enabled(ctx: typer.Context, mapping_id: str, enabled: bool) -> None:
    state: Ctx = ctx.obj
    state.require_service("Changing a playlist mapping")
    mapping = _find_id(_mapping_items(state), mapping_id, "mapping")
    body = _mapping_payload(mapping)
    body["enabled"] = bool(enabled)
    result = as_dict(state.post("/api/playlists/mappings", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Update rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success(f"{'Enabled' if enabled else 'Disabled'} mapping {body['id']}.")


@mapping_app.command("run")
def mapping_run(
    ctx: typer.Context,
    mapping_id: str = typer.Argument(..., help="Mapping id."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report what would change, change nothing."),
) -> None:
    """Run one playlist mapping."""
    state: Ctx = ctx.obj
    state.require_service("Running a playlist mapping")
    result = as_dict(state.post(f"/api/playlists/mappings/{mapping_id}/run", params={"dry_run": dry_run}))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Run rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    state.out.success("Dry run finished." if dry_run else "Mapping run finished.")
    _print_playlist_counts(state, _run_result(result))


@mapping_app.command("preview")
def mapping_preview(
    ctx: typer.Context,
    mapping_id: str = typer.Argument(..., help="Mapping id."),
) -> None:
    """Preview what a mapping would produce."""
    state: Ctx = ctx.obj
    payload = state.post(f"/api/playlists/mappings/{mapping_id}/preview")
    if state.out.json_mode:
        state.out.data(payload)
        return
    preview = as_dict(as_dict(payload).get("preview"))
    state.out.kv(
        [(key, value) for key, value in preview.items() if not isinstance(value, (dict, list))],
        title=f"Preview {mapping_id}",
    )


@mapping_app.command("result")
def mapping_result(
    ctx: typer.Context,
    mapping_id: str = typer.Argument(..., help="Mapping id."),
) -> None:
    """Show the last result for a mapping."""
    state: Ctx = ctx.obj
    payload = state.get(f"/api/playlists/mappings/{mapping_id}/result")
    if state.out.json_mode:
        state.out.data(payload)
        return
    block = _run_result(as_dict(payload))
    state.out.kv(
        [(key, value) for key, value in block.items() if not isinstance(value, (dict, list))],
        title=f"Mapping {mapping_id}",
    )


@mapping_app.command("delete")
def mapping_delete(
    ctx: typer.Context,
    mapping_id: str = typer.Argument(..., help="Mapping id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a playlist mapping."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a playlist mapping")
    if not yes and not typer.confirm(f"Delete mapping {mapping_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/playlists/mappings/{mapping_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted mapping {mapping_id}")


@ruleset_app.command("list")
def ruleset_list(ctx: typer.Context) -> None:
    """List playlist rulesets."""
    state: Ctx = ctx.obj
    payload = state.get("/api/playlists/rulesets")
    if state.out.json_mode:
        state.out.data(payload)
        return
    rows = _rows(payload, "rulesets", "items")
    state.out.records(
        rows,
        [
            ("ID", lambda r: str(r.get("id") or "-")),
            ("NAME", lambda r: str(r.get("name") or "-")[:34]),
            ("RULES", lambda r: str(len(r.get("rules") or []) or r.get("rule_count") or 0)),
        ],
        title="Rulesets",
        empty="No rulesets.",
    )


@ruleset_app.command("show")
def ruleset_show(ctx: typer.Context, ruleset_id: str = typer.Argument(..., help="Ruleset id.")) -> None:
    """Show one ruleset."""
    state: Ctx = ctx.obj
    payload = state.get(f"/api/playlists/rulesets/{ruleset_id}")
    if state.out.json_mode:
        state.out.data(payload)
        return
    import json as _json

    from rich.syntax import Syntax

    state.out.console.print(
        Syntax(_json.dumps(payload, indent=2, default=str), "json", theme="ansi_dark", background_color="default")
    )


@ruleset_app.command("add")
def ruleset_add(
    ctx: typer.Context,
    file: str = typer.Argument(..., help="JSON file containing a ruleset object."),
) -> None:
    """Add or update a ruleset from JSON."""
    state: Ctx = ctx.obj
    state.require_service("Saving a ruleset")
    result = as_dict(state.post("/api/playlists/rulesets", json_body=_load_json_file(file)))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Ruleset rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    ruleset = as_dict(result.get("ruleset"))
    state.out.success(f"Ruleset {'created' if result.get('created') else 'saved'}: {ruleset.get('id') or '-'}")


@ruleset_app.command("validate")
def ruleset_validate(
    ctx: typer.Context,
    file: str = typer.Argument(..., help="JSON file containing a ruleset object."),
) -> None:
    """Validate a ruleset JSON file."""
    state: Ctx = ctx.obj
    result = as_dict(state.post("/api/playlists/rulesets/validate", json_body=_load_json_file(file)))
    if state.out.json_mode:
        state.out.data(result)
        return
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Ruleset is invalid"), exit_code=EXIT_USAGE)
    ruleset = as_dict(result.get("ruleset"))
    state.out.success(f"Ruleset is valid: {ruleset.get('id') or ruleset.get('name') or file}")


@ruleset_app.command("clone")
def ruleset_clone(
    ctx: typer.Context,
    ruleset_id: str = typer.Argument(..., help="Ruleset id."),
    name: str = typer.Option("", "--name", "-n", help="Name for the cloned ruleset."),
) -> None:
    """Clone a ruleset."""
    state: Ctx = ctx.obj
    state.require_service("Cloning a ruleset")
    body = {"name": name} if name.strip() else {}
    result = as_dict(state.post(f"/api/playlists/rulesets/{ruleset_id}/clone", json_body=body))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Clone rejected"))
    if state.out.json_mode:
        state.out.data(result)
        return
    ruleset = as_dict(result.get("ruleset"))
    state.out.success(f"Ruleset cloned: {ruleset.get('id') or '-'}")


@ruleset_app.command("delete")
def ruleset_delete(
    ctx: typer.Context,
    ruleset_id: str = typer.Argument(..., help="Ruleset id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Delete a ruleset."""
    state: Ctx = ctx.obj
    state.require_service("Deleting a ruleset")
    if not yes and not typer.confirm(f"Delete ruleset {ruleset_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/playlists/rulesets/{ruleset_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Delete rejected"))
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Deleted ruleset {ruleset_id}")


def register(app: typer.Typer) -> None:
    playlist_app.add_typer(resource_app, name="resource")
    playlist_app.add_typer(endpoint_app, name="endpoint")
    playlist_app.add_typer(mapping_app, name="mapping")
    playlist_app.add_typer(ruleset_app, name="ruleset")
    app.add_typer(playlist_app, name="playlist")
