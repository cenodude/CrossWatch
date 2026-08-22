# /cli/commands/auth.py
# CrossWatch - CLI provider logins and API token management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import typer

from .._context import Ctx
from .._errors import EXIT_NOT_FOUND, EXIT_USAGE, CLIError
from .._render import state_text
from .._settings import settings_path, update_settings
from .._util import as_dict, coerce_bool, error_text, fmt_rel, fmt_ts

auth_app = typer.Typer(help="Connect providers and manage CLI API tokens.", no_args_is_help=True)
token_app = typer.Typer(help="Create and revoke API tokens for the CLI.", no_args_is_help=True)


@dataclass(frozen=True)
class Endpoints:
    submit: str = ""
    submit_starts: bool = False
    start: str = ""
    poll: str = ""
    finish: str = ""
    cancel: str = ""
    disconnect: str = ""
    needs_origin: bool = False


ENDPOINTS: dict[str, Endpoints] = {
    "PLEX": Endpoints(start="/api/plex/pin/new", disconnect="/api/plex/token/delete"),
    "TRAKT": Endpoints(
        submit="/api/trakt/pin/new",
        submit_starts=True,
        start="/api/trakt/pin/new",
        disconnect="/api/trakt/token/delete",
    ),
    "SIMKL": Endpoints(
        start="/api/simkl/pin/start",
        poll="/api/simkl/pin/poll",
        cancel="/api/simkl/pin/cancel",
        disconnect="/api/simkl/token/delete",
    ),
    "ANILIST": Endpoints(
        submit="/api/anilist/save",
        start="/api/anilist/authorize",
        needs_origin=True,
        disconnect="/api/anilist/token/delete",
    ),
    "MDBLIST": Endpoints(
        submit="/api/mdblist/save",
        start="/api/mdblist/device/start",
        poll="/api/mdblist/device/poll",
        disconnect="/api/mdblist/disconnect",
    ),
    "PUNCHPLAY": Endpoints(
        start="/api/punchplay/device/start",
        poll="/api/punchplay/device/poll",
        cancel="/api/punchplay/device/cancel",
        disconnect="/api/punchplay/disconnect",
    ),
    "BINGEBASE": Endpoints(
        start="/api/bingebase/device/start",
        poll="/api/bingebase/device/poll",
        cancel="/api/bingebase/device/cancel",
        disconnect="/api/bingebase/disconnect",
    ),
    "NUVIO": Endpoints(
        start="/api/nuvio/device/start",
        poll="/api/nuvio/device/poll",
        finish="/api/nuvio/device/finish",
        disconnect="/api/nuvio/disconnect",
    ),
    "JELLYFIN": Endpoints(submit="/api/jellyfin/login", disconnect="/api/jellyfin/token/delete"),
    "EMBY": Endpoints(submit="/api/emby/login", disconnect="/api/emby/token/delete"),
    "KODI": Endpoints(submit="/api/kodi/connect", disconnect="/api/kodi/disconnect"),
    "STREMIO": Endpoints(submit="/api/stremio/connect", disconnect="/api/stremio/disconnect"),
    "FLOPPY": Endpoints(submit="/api/floppy/save", disconnect="/api/floppy/disconnect"),
    "SCROB": Endpoints(submit="/api/scrob/save", disconnect="/api/scrob/disconnect"),
    "TAUTULLI": Endpoints(submit="/api/tautulli/save", disconnect="/api/tautulli/disconnect"),
    "PUBLICMETADB": Endpoints(submit="/api/publicmetadb/save", disconnect="/api/publicmetadb/disconnect"),
    "TMDB": Endpoints(submit="/api/tmdb_sync/save", disconnect="/api/tmdb_sync/disconnect"),
    "CROSSWATCH": Endpoints(submit="/api/crosswatch/connect", disconnect="/api/crosswatch/disconnect"),
}

AUTHORIZED = {"authorized", "ok", "connected", "success", "complete", "done"}
PENDING = {"pending", "waiting", "authorization_pending", "slow_down", "", "polling"}


@dataclass
class Manifest:
    name: str
    label: str
    flow: str
    notes: str = ""
    verify_url: str = ""
    fields: list[dict[str, Any]] = field(default_factory=list)

    @property
    def endpoints(self) -> Endpoints:
        return ENDPOINTS.get(self.name, Endpoints())

    @property
    def supported(self) -> bool:
        endpoints = self.endpoints
        return bool(endpoints.submit or endpoints.start)


def _manifests(state: Ctx) -> list[Manifest]:
    payload = state.get("/api/auth/providers")
    if not isinstance(payload, list):
        raise CLIError("The provider manifest endpoint returned an unexpected payload")
    out: list[Manifest] = []
    for raw in payload:
        block = as_dict(raw)
        name = str(block.get("name") or "").strip().upper()
        if not name:
            continue
        fields = [as_dict(f) for f in (block.get("fields") or []) if isinstance(f, dict)]
        out.append(
            Manifest(
                name=name,
                label=str(block.get("label") or name),
                flow=str(block.get("flow") or ""),
                notes=str(block.get("notes") or ""),
                verify_url=str(block.get("verify_url") or ""),
                fields=fields,
            )
        )
    return sorted(out, key=lambda m: m.name)


def _manifest(state: Ctx, provider: str) -> Manifest:
    want = str(provider or "").strip().upper()
    if not want:
        raise CLIError("A provider name is required", exit_code=EXIT_USAGE)
    catalogue = _manifests(state)
    for item in catalogue:
        if item.name == want:
            return item
    known = ", ".join(m.name.lower() for m in catalogue)
    raise CLIError(f"Unknown provider '{provider}'", hint=f"Known providers: {known}", exit_code=EXIT_USAGE)


def _public(manifest: Manifest) -> dict[str, Any]:
    return {
        "name": manifest.name,
        "label": manifest.label,
        "flow": manifest.flow,
        "supported": manifest.supported,
        "fields": manifest.fields,
        "verify_url": manifest.verify_url,
        "notes": manifest.notes,
    }


@auth_app.command("setup")
def app_setup(
    ctx: typer.Context,
    username: str = typer.Option("admin", "--username", "-u", help="CrossWatch admin username."),
    password: str = typer.Option("", "--password", "-p", help="CrossWatch admin password. Prompts when omitted."),
    create_token: bool = typer.Option(True, "--token/--no-token", help="Create and save a CLI API token after setup."),
    token_name: str = typer.Option("CLI", "--token-name", help="Label for the generated CLI token."),
    save: bool = typer.Option(True, "--save/--no-save", help="Store the generated token in the CLI settings file."),
) -> None:
    """Set up CrossWatch app authentication."""
    state: Ctx = ctx.obj
    pwd = password
    if not pwd:
        pwd = str(typer.prompt("Password", hide_input=True, confirmation_prompt=True) or "")
    payload = as_dict(state.post("/api/app-auth/credentials", json_body={"enabled": True, "username": username, "password": pwd}))
    if payload.get("ok") is False:
        raise CLIError(error_text(payload, "Authentication setup failed"))

    raw = ""
    entry: dict[str, Any] = {}
    stored = ""
    if create_token:
        token_payload = as_dict(state.post("/api/app-auth/tokens", json_body={"name": token_name, "expires_days": 0}))
        if not token_payload.get("token"):
            raise CLIError(error_text(token_payload, "Token creation failed"))
        raw = str(token_payload.get("token") or "")
        entry = as_dict(token_payload.get("entry"))
        if save:
            update_settings(token=raw, url=state.url)
            stored = str(settings_path())

    if state.out.json_mode:
        state.out.data({"ok": True, "enabled": True, "token": raw, "entry": entry, "saved_to": stored})
        return
    rows: list[tuple[str, Any]] = [("Auth", "enabled"), ("Username", username)]
    if create_token:
        rows.extend(
            [
                ("Token", raw),
                ("Token id", str(entry.get("id") or "-")),
                ("Saved to", stored or "not saved"),
            ]
        )
    state.out.kv(rows, title="CrossWatch auth setup")
    if raw:
        state.out.warn("This is the only time the token is shown. Store it somewhere safe.")


def _nest(pairs: dict[str, Any], *, drop_prefix: bool) -> dict[str, Any]:
    body: dict[str, Any] = {}
    for key, value in pairs.items():
        parts = [p for p in str(key).split(".") if p]
        if drop_prefix:
            parts = parts[1:]
        if not parts:
            continue
        node = body
        for part in parts[:-1]:
            nxt = node.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                node[part] = nxt
            node = nxt
        node[parts[-1]] = value
    return body


def _collect_fields(manifest: Manifest, supplied: dict[str, str], *, interactive: bool) -> dict[str, Any]:
    known = {str(f.get("key") or "").strip() for f in manifest.fields if f.get("key")}
    known |= {key.split(".", 1)[-1] for key in known}
    unknown = sorted(k for k in supplied if k not in known)
    if unknown:
        raise CLIError(
            f"{manifest.label} has no field named: {', '.join(unknown)}",
            hint=f"Known fields: {', '.join(sorted(str(f.get('key')) for f in manifest.fields)) or 'none'}",
            exit_code=EXIT_USAGE,
        )

    values: dict[str, Any] = {}
    leftover = dict(supplied)

    for spec in manifest.fields:
        key = str(spec.get("key") or "").strip()
        if not key:
            continue
        short = key.split(".", 1)[-1]
        given = leftover.pop(key, None)
        if given is None:
            given = leftover.pop(short, None)
        kind = str(spec.get("type") or "text").strip().lower()
        required = coerce_bool(spec.get("required"), False)

        if given is not None:
            values[key] = coerce_bool(given, False) if kind == "bool" else given
            continue
        if not interactive:
            if required:
                raise CLIError(
                    f"{manifest.label} needs '{key}' and prompting is disabled",
                    hint=f"Pass it with --field {key}=VALUE",
                    exit_code=EXIT_USAGE,
                )
            continue

        label = str(spec.get("label") or short)
        if kind == "bool":
            values[key] = typer.confirm(label, default=True)
            continue
        answer = str(typer.prompt(label, hide_input=(kind == "password"), default="", show_default=False) or "").strip()
        if answer:
            values[key] = answer
        elif required:
            raise CLIError(f"{manifest.label} needs '{key}'", exit_code=EXIT_USAGE)

    return values


def _parse_field_args(raw: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in raw or []:
        key, sep, value = str(item).partition("=")
        key = key.strip()
        if not sep or not key:
            raise CLIError(f"--field expects key=value, got '{item}'", exit_code=EXIT_USAGE)
        out[key] = value
    return out


def _connected(state: Ctx, provider: str) -> bool:
    try:
        status = as_dict(state.get("/api/status", params={"fresh": 1}))
    except CLIError:
        return False
    if coerce_bool(status.get(f"{provider.lower()}_connected"), False):
        return True
    block = as_dict(as_dict(status.get("providers")).get(provider.upper()))
    return coerce_bool(block.get("connected"), False)


def _rejected(result: Any) -> str:
    block = as_dict(result)
    if block.get("ok") is False:
        return str(block.get("error") or block.get("status") or "rejected")
    return ""


def _show_code(state: Ctx, manifest: Manifest, started: dict[str, Any]) -> bool:
    code = str(started.get("user_code") or started.get("code") or started.get("pin") or "").strip()
    url = str(
        started.get("verification_url")
        or started.get("verificationUrl")
        or started.get("verification_uri")
        or started.get("authorize_url")
        or started.get("url")
        or manifest.verify_url
        or ""
    ).strip()
    expires = started.get("expiresIn") or started.get("expires_in") or 0
    if not code and not url:
        return False
    rows: list[tuple[str, Any]] = []
    if code:
        rows.append(("Code", code))
    if url:
        rows.append(("Open", url))
    if expires:
        rows.append(("Expires", f"in {int(expires)}s"))
    state.out.kv(rows, title=f"Authorize {manifest.label}")
    return True


def _wait(state: Ctx, manifest: Manifest, instance: str, timeout: float, interval: float) -> bool:
    endpoints = manifest.endpoints
    deadline = time.time() + max(30.0, timeout)
    state.out.info("Waiting for authorization... (Ctrl-C to stop)")
    while time.time() < deadline:
        time.sleep(interval)
        if endpoints.poll:
            try:
                result = as_dict(state.post(endpoints.poll, params={"instance": instance}))
            except CLIError:
                continue
            status = str(result.get("status") or "").strip().lower()
            if not status and result.get("ok") and result.get("access_token"):
                status = "authorized"
            if status in AUTHORIZED or coerce_bool(result.get("authorized"), False):
                if endpoints.finish:
                    state.post(endpoints.finish, params={"instance": instance})
                return True
            if status and status not in PENDING:
                raise CLIError(f"{manifest.label} authorization failed: {status}")
        if _connected(state, manifest.name):
            return True
    return False


@auth_app.command("providers")
def auth_providers(
    ctx: typer.Context,
    show_all: bool = typer.Option(False, "--all", "-a", help="Include providers the CLI cannot connect."),
) -> None:
    """List every provider, how it authenticates and what it needs."""
    state: Ctx = ctx.obj
    catalogue = _manifests(state)
    if not show_all:
        catalogue = [m for m in catalogue if m.supported]
    if state.out.json_mode:
        state.out.data([_public(m) for m in catalogue])
        return
    state.out.table(
        ["PROVIDER", "FLOW", "NEEDS", "CLI LOGIN"],
        [
            [
                m.name,
                m.flow or "-",
                ", ".join(str(f.get("key") or "").split(".", 1)[-1] for f in m.fields) or "nothing",
                state_text(m.supported, on="yes", off="no"),
            ]
            for m in catalogue
        ],
        title="Authentication providers",
    )
    state.out.info("Run 'cw auth show <provider>' for details, then 'cw auth login <provider>'.")


@auth_app.command("show")
def auth_show(ctx: typer.Context, provider: str = typer.Argument(..., help="Provider name.")) -> None:
    """Show how one provider authenticates and which fields it needs."""
    state: Ctx = ctx.obj
    manifest = _manifest(state, provider)
    if state.out.json_mode:
        state.out.data(_public(manifest))
        return
    state.out.kv(
        [
            ("Provider", manifest.name),
            ("Label", manifest.label),
            ("Flow", manifest.flow or "-"),
            ("CLI login", state_text(manifest.supported, on="supported", off="use the web UI")),
            ("Verify at", manifest.verify_url or "-"),
            ("Connected", state_text(_connected(state, manifest.name), on="yes", off="no")),
        ],
        title=manifest.label,
    )
    if manifest.notes:
        state.out.print()
        state.out.info(manifest.notes)
    if not manifest.fields:
        return
    state.out.print()
    state.out.table(
        ["FIELD", "LABEL", "TYPE", "REQUIRED"],
        [
            [
                str(f.get("key") or ""),
                str(f.get("label") or "-"),
                str(f.get("type") or "text"),
                state_text(coerce_bool(f.get("required"), False), on="yes", off="no"),
            ]
            for f in manifest.fields
        ],
        title="Fields",
    )
    example = " ".join(f"--field {f.get('key')}=..." for f in manifest.fields if coerce_bool(f.get("required"), False))
    if example:
        state.out.print()
        state.out.info(f"cw auth login {manifest.name.lower()} {example}")


@auth_app.command("list")
def auth_list(
    ctx: typer.Context,
    fresh: bool = typer.Option(False, "--fresh", "-f", help="Re-probe every provider instead of using the cached status."),
) -> None:
    """Show which providers are connected."""
    state: Ctx = ctx.obj
    params = {"fresh": 1} if fresh else None
    providers = as_dict(as_dict(state.get("/api/status", params=params)).get("providers"))
    if state.out.json_mode:
        state.out.data(providers)
        return
    rows: list[list[Any]] = []
    for name in sorted(providers):
        block = as_dict(providers.get(name))
        summary = as_dict(block.get("instances_summary"))
        rows.append(
            [
                name,
                state_text(block.get("connected"), on="connected", off="not connected"),
                f"{int(summary.get('ok') or 0)}/{int(summary.get('total') or 0)}" if summary.get("total") else "-",
                str(block.get("reason") or "")[:44],
            ]
        )
    state.out.table(
        ["PROVIDER", "STATE", "INSTANCES", "REASON"],
        rows,
        title="Provider connections",
        empty="No providers configured yet. Run 'cw auth providers' to see the options.",
    )
    if not fresh:
        state.out.info("Status is cached; pass --fresh to re-probe.")


@auth_app.command("login")
def auth_login(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider name. See 'cw auth providers'."),
    fields: list[str] = typer.Option([], "--field", "-F", help="Supply a field as key=value. Repeatable."),
    instance: str = typer.Option("default", "--instance", "-i", help="Provider instance id."),
    interactive: bool = typer.Option(True, "--interactive/--non-interactive", help="Prompt for missing fields."),
    wait: bool = typer.Option(True, "--wait/--no-wait", help="Wait for a device code to be authorized."),
    timeout: float = typer.Option(300.0, "--timeout", help="How long to wait, in seconds."),
    interval: float = typer.Option(3.0, "--interval", help="Seconds between authorization checks."),
) -> None:
    """Connect a provider, whichever way that provider authenticates."""
    state: Ctx = ctx.obj
    state.require_service("Provider login")
    manifest = _manifest(state, provider)
    endpoints = manifest.endpoints
    if not manifest.supported:
        raise CLIError(
            f"{manifest.label} cannot be connected from the CLI",
            hint=f"Its '{manifest.flow}' flow needs the web UI.",
            exit_code=EXIT_USAGE,
        )

    values = _collect_fields(manifest, _parse_field_args(fields), interactive=interactive)
    params = {"instance": instance}
    started: dict[str, Any] | None = None

    if values or (endpoints.submit and not endpoints.start):
        if endpoints.submit:
            result = state.post(endpoints.submit, params=params, json_body=_nest(values, drop_prefix=True))
            rejected = _rejected(result)
            if rejected:
                raise CLIError(f"{manifest.label} rejected those details: {rejected}")
            if endpoints.submit_starts:
                started = as_dict(result)
        else:
            state.post("/api/config", json_body=_nest(values, drop_prefix=False))

    if started is None and endpoints.start:
        body: dict[str, Any] = {"origin": state.url} if endpoints.needs_origin else {}
        result = state.post(endpoints.start, params=params, json_body=body)
        rejected = _rejected(result)
        if rejected:
            raise CLIError(
                f"{manifest.label} refused to start a login: {rejected}",
                hint=f"Check its fields with 'cw auth show {manifest.name.lower()}'.",
            )
        started = as_dict(result)

    if started is None or not _show_code(state, manifest, started):
        connected = _connected(state, manifest.name)
        if state.out.json_mode:
            state.out.data({"ok": connected, "provider": manifest.name, "instance": instance})
            return
        if connected:
            state.out.success(f"{manifest.label} connected.")
            return
        state.out.warn(f"{manifest.label} accepted the details but does not report as connected yet.")
        raise typer.Exit(1)

    if not wait:
        state.out.info("Finish in your browser, then run 'cw auth list' to confirm.")
        return

    try:
        ok = _wait(state, manifest, instance, timeout, interval)
    except KeyboardInterrupt:
        if endpoints.cancel:
            try:
                state.post(endpoints.cancel, params=params)
            except CLIError:
                pass
        raise CLIError("Login cancelled", exit_code=0) from None

    if state.out.json_mode:
        state.out.data({"ok": ok, "provider": manifest.name, "instance": instance})
        return
    if ok:
        state.out.success(f"{manifest.label} connected.")
        return
    state.out.warn(f"{manifest.label} was not authorized before the timeout.")
    raise typer.Exit(1)


@auth_app.command("logout")
def auth_logout(
    ctx: typer.Context,
    provider: str = typer.Argument(..., help="Provider to disconnect."),
    instance: str = typer.Option("default", "--instance", "-i", help="Provider instance id."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Disconnect a provider and drop its stored credentials."""
    state: Ctx = ctx.obj
    state.require_service("Provider logout")
    manifest = _manifest(state, provider)
    endpoint = manifest.endpoints.disconnect
    if not endpoint:
        raise CLIError(f"{manifest.label} has no disconnect endpoint", exit_code=EXIT_NOT_FOUND)
    if not yes and not typer.confirm(f"Disconnect {manifest.label} ({instance})?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = state.post(endpoint, params={"instance": instance})
    rejected = _rejected(result)
    if rejected:
        raise CLIError(f"{manifest.label} could not be disconnected: {rejected}")
    if state.out.json_mode:
        state.out.data(as_dict(result) or {"ok": True})
        return
    state.out.success(f"{manifest.label} disconnected.")


@token_app.command("list")
def token_list(ctx: typer.Context) -> None:
    """List API tokens."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/app-auth/tokens"))
    tokens = [as_dict(t) for t in (payload.get("tokens") or []) if isinstance(t, dict)]
    if state.out.json_mode:
        state.out.data(tokens)
        return
    state.out.table(
        ["ID", "NAME", "PREFIX", "USER", "CREATED", "EXPIRES", "LAST USED"],
        [
            [
                str(t.get("id") or ""),
                str(t.get("name") or "-"),
                str(t.get("prefix") or "-") + "...",
                str(t.get("username") or t.get("user_id") or "-"),
                fmt_ts(t.get("created_at")),
                fmt_ts(t.get("expires_at")) if t.get("expires_at") else "never",
                fmt_rel(t.get("last_used_at")) if t.get("last_used_at") else "never",
            ]
            for t in tokens
        ],
        title="API tokens",
        empty="No API tokens yet. Create one with 'cw auth token create --local'.",
    )


@token_app.command("create")
def token_create(
    ctx: typer.Context,
    name: str = typer.Option("CLI", "--name", "-n", help="Label shown in the token list."),
    expires_days: int = typer.Option(0, "--expires-days", help="Expire after N days. 0 means never."),
    save: bool = typer.Option(True, "--save/--no-save", help="Store the token in the CLI settings file."),
) -> None:
    """Create an API token. Use --local from the CrossWatch host or container for CLI-only setup."""
    state: Ctx = ctx.obj
    payload = as_dict(state.post("/api/app-auth/tokens", json_body={"name": name, "expires_days": int(expires_days)}))
    if not payload.get("token"):
        raise CLIError(f"Token creation failed: {payload.get('error') or 'unknown error'}")
    raw = str(payload.get("token"))
    entry = as_dict(payload.get("entry"))
    stored = ""
    if save:
        update_settings(token=raw, url=state.url)
        stored = str(settings_path())
    if state.out.json_mode:
        state.out.data({"token": raw, "entry": entry, "saved_to": stored})
        return
    state.out.kv(
        [
            ("Token", raw),
            ("Id", str(entry.get("id") or "-")),
            ("Name", str(entry.get("name") or name)),
            ("Expires", fmt_ts(entry.get("expires_at")) if entry.get("expires_at") else "never"),
            ("Saved to", stored or "not saved"),
        ],
        title="New API token",
    )
    state.out.warn("This is the only time the token is shown. Store it somewhere safe.")


@token_app.command("revoke")
def token_revoke(
    ctx: typer.Context,
    token_id: str = typer.Argument(..., help="Token id from 'cw auth token list'."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Do not ask for confirmation."),
) -> None:
    """Revoke an API token."""
    state: Ctx = ctx.obj
    if not yes and not typer.confirm(f"Revoke token {token_id}?", default=False):
        raise CLIError("Cancelled", exit_code=0)
    result = as_dict(state.delete(f"/api/app-auth/tokens/{token_id}"))
    if result.get("ok") is False:
        raise CLIError(error_text(result, "Token not found"), exit_code=EXIT_NOT_FOUND)
    if state.out.json_mode:
        state.out.data(result or {"ok": True})
        return
    state.out.success(f"Revoked {token_id}")


@token_app.command("use")
def token_use(
    ctx: typer.Context,
    token: str = typer.Argument(..., help="An existing API token to store for future commands."),
    url: str = typer.Option("", "--url", help="Also store this base URL."),
) -> None:
    """Save a token (and optionally a URL) in the CLI settings file."""
    state: Ctx = ctx.obj
    if not token.strip().startswith("cwt_"):
        raise CLIError("That does not look like a CrossWatch API token", hint="Tokens start with cwt_.", exit_code=EXIT_USAGE)
    update_settings(token=token.strip(), url=(url.strip() or state.url))
    if state.out.json_mode:
        state.out.data({"ok": True, "saved_to": str(settings_path())})
        return
    state.out.success(f"Saved to {settings_path()}")


@token_app.command("whoami")
def token_whoami(ctx: typer.Context) -> None:
    """Show which identity the current token maps to."""
    state: Ctx = ctx.obj
    payload = as_dict(state.get("/api/app-auth/tokens/whoami"))
    if state.out.json_mode:
        state.out.data(payload)
        return
    if not payload.get("auth_required"):
        state.out.kv([("Endpoint", state.url), ("Auth", "not enabled on this instance")], title="Identity")
        return
    user = as_dict(payload.get("user"))
    state.out.kv(
        [
            ("Endpoint", state.url),
            ("User", str(user.get("username") or user.get("display_name") or "-")),
            ("Role", "admin" if user.get("is_admin") else "user"),
            ("Profile", str(user.get("profile_id") or "-")),
            ("Authenticated by", str(payload.get("auth_kind") or "-")),
            ("Token", str(payload.get("token_name") or payload.get("token_id") or "-")),
        ],
        title="Identity",
    )


def register(app: typer.Typer) -> None:
    auth_app.add_typer(token_app, name="token")
    app.add_typer(auth_app, name="auth")
