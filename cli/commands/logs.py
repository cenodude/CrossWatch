# /cli/commands/logs.py
# CrossWatch - CLI log commands
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
import time

import typer

from .._context import Ctx
from .._errors import CLIError
from .._util import is_log_control, strip_ansi

logs_app = typer.Typer(help="Read the CrossWatch log stream.", no_args_is_help=True)

COMMON_TAGS = ("SYNC", "SCHED", "WATCH", "WATCHM", "SCROBBLE", "WEBHOOK", "TRAKT", "SIMKL", "PLEX", "JELLYFIN", "EMBY", "TRBL")


@logs_app.command("tail")
def logs_tail(
    ctx: typer.Context,
    tag: str = typer.Option("SYNC", "--tag", "-t", help=f"Log channel. Common ones: {', '.join(COMMON_TAGS)}."),
    lines: int = typer.Option(200, "--lines", "-n", min=1, max=5000, help="How many past lines to show."),
    follow: bool = typer.Option(False, "--follow", "-f", help="Keep printing new lines until interrupted."),
    grep: str = typer.Option("", "--grep", "-g", help="Only print lines matching this regular expression."),
    timeout: float = typer.Option(0.0, "--timeout", help="Stop following after this many seconds (0 = no limit)."),
) -> None:
    """Show recent log output, optionally following it live."""
    state: Ctx = ctx.obj
    channel = tag.strip().upper() or "SYNC"
    pattern = re.compile(grep) if grep.strip() else None

    if not follow:
        payload = state.get("/api/logs/dump", params={"channel": channel, "n": lines})
        if not isinstance(payload, dict):
            raise CLIError("Log endpoint returned an unexpected payload")
        if state.out.json_mode:
            state.out.data(payload)
            return
        for line in payload.get("lines") or []:
            text = strip_ansi(str(line))
            if is_log_control(text):
                continue
            if pattern is None or pattern.search(text):
                state.out.raw(text)
        return

    state.require_service("Following logs")
    deadline = time.time() + timeout if timeout > 0 else 0.0
    client = state.stream_client()
    try:
        for event, data in client.stream_sse(
            "/api/logs/stream", params={"tag": channel, "plain": "true", "tail": lines}
        ):
            if event in ("ping", "scope"):
                if deadline and time.time() > deadline:
                    return
                continue
            text = strip_ansi(data)
            if text.strip() and not is_log_control(text) and (pattern is None or pattern.search(text)):
                state.out.raw(text)
            if deadline and time.time() > deadline:
                return
    except KeyboardInterrupt:
        return
    finally:
        client.close()


@logs_app.command("channels")
def logs_channels(ctx: typer.Context) -> None:
    """List the log channels the CLI knows about."""
    state: Ctx = ctx.obj
    if state.out.json_mode:
        state.out.data(list(COMMON_TAGS))
        return
    state.out.table(["CHANNEL"], [[t] for t in COMMON_TAGS], title="Log channels")
    state.out.info("Any tag the engine emits works, these are just the usual ones.")


def register(app: typer.Typer) -> None:
    app.add_typer(logs_app, name="logs")
