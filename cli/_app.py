# /cli/_app.py
# CrossWatch - CLI application, argument handling and error reporting
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import sys
from typing import Any

import typer

from ._context import Ctx
from ._errors import EXIT_ERROR, EXIT_USAGE, CLIError
from ._render import Output

PROG = "cw"
HELP = """CrossWatch command line.

Talks to the running CrossWatch service over its API, and falls back to the
local install for read-only and repair commands when the service is down.
"""

app = typer.Typer(
    name=PROG,
    help=HELP,
    add_completion=True,
    no_args_is_help=True,
    rich_markup_mode="rich",
    context_settings={"help_option_names": ["-h", "--help"]},
)


def get_ctx(ctx: typer.Context) -> Ctx:
    state = ctx.obj
    if not isinstance(state, Ctx):
        state = Ctx.build()
        ctx.obj = state
    return state


PARAM_TO_OPTION = {
    "url": "url",
    "token": "token",
    "output": "output",
    "local": "local",
    "insecure": "insecure",
    "http_timeout": "timeout",
    "quiet": "quiet",
}


def _given_on_the_command_line(ctx: typer.Context, name: str) -> bool:
    try:
        source = ctx.get_parameter_source(name)
    except Exception:
        return False
    return source is not None and getattr(source, "name", "DEFAULT") != "DEFAULT"


@app.callback()
def main_callback(
    ctx: typer.Context,
    url: str = typer.Option("", "--url", "-U", help="CrossWatch base URL. Defaults to CW_URL, then the local install.", envvar="CW_URL"),
    token: str = typer.Option("", "--token", "-T", help="API token. Defaults to CW_TOKEN or the saved token.", envvar="CW_TOKEN"),
    output: str = typer.Option("auto", "--output", "-o", help="Output format: auto, table, json, yaml, plain."),
    local: bool = typer.Option(False, "--local", "-L", help="Work directly against this install, never the HTTP API."),
    insecure: bool = typer.Option(False, "--insecure", "-k", help="Skip TLS verification (self-signed certificates)."),
    http_timeout: float = typer.Option(30.0, "--http-timeout", help="HTTP timeout in seconds."),
    no_color: bool = typer.Option(False, "--no-color", help="Disable colored output."),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-essential output."),
) -> None:
    if output not in ("auto", "table", "json", "yaml", "plain"):
        raise CLIError(f"Unknown output format '{output}'", hint="Use auto, table, json, yaml or plain.", exit_code=EXIT_USAGE)

    given = {
        "url": url,
        "token": token,
        "output": output,
        "local": local,
        "insecure": insecure,
        "http_timeout": http_timeout,
        "quiet": quiet,
    }
    explicit = {name for name in given if _given_on_the_command_line(ctx, name)}
    if _given_on_the_command_line(ctx, "no_color"):
        explicit.add("no_color")

    inherited = ctx.obj if isinstance(ctx.obj, Ctx) else None
    if inherited is not None and not explicit:
        return

    settings: dict[str, Any] = dict(inherited.options) if inherited is not None else {}
    for name in explicit:
        if name == "no_color":
            settings["color"] = not no_color
        else:
            settings[PARAM_TO_OPTION[name]] = given[name]

    ctx.obj = Ctx.build(**settings) if inherited is not None else Ctx.build(
        url=url,
        token=token,
        timeout=http_timeout,
        insecure=insecure,
        local=local,
        output=output,
        color=not no_color,
        quiet=quiet,
    )


def _register_all() -> None:
    from .commands import (
        analyzer,
        anime,
        auth,
        backup,
        capture,
        config,
        editor,
        events,
        insights,
        instance,
        logs,
        maintenance,
        metadata,
        pair,
        playlist,
        progress,
        scheduler,
        scrobbler,
        shell,
        status,
        sync,
        transfer,
        watcher,
        watchlist,
    )

    for module in (
        status,
        pair,
        sync,
        config,
        auth,
        watcher,
        scheduler,
        logs,
        analyzer,
        events,
        capture,
        backup,
        watchlist,
        progress,
        editor,
        playlist,
        transfer,
        metadata,
        anime,
        instance,
        scrobbler,
        insights,
        maintenance,
        shell,
    ):
        module.register(app)


_register_all()


GLOBAL_VALUE_FLAGS = {"--url", "-U", "--token", "-T", "--output", "-o", "--http-timeout"}
GLOBAL_BOOL_FLAGS = {"--local", "-L", "--insecure", "-k", "--no-color", "--quiet", "-q"}


def hoist_globals(args: list[str]) -> list[str]:
    leading: list[str] = []
    rest: list[str] = []
    index = 0
    while index < len(args):
        token = args[index]
        if token == "--":
            rest.extend(args[index:])
            break
        head = token.split("=", 1)[0]
        if head in GLOBAL_VALUE_FLAGS:
            leading.append(token)
            if "=" not in token and index + 1 < len(args):
                index += 1
                leading.append(args[index])
            index += 1
            continue
        if head in GLOBAL_BOOL_FLAGS:
            leading.append(token)
            index += 1
            continue
        rest.append(token)
        index += 1
    return leading + rest


def click_exception_types() -> tuple[type[BaseException], ...]:
    found: list[type[BaseException]] = []
    modules: list[Any] = []
    try:
        from typer._click import exceptions as typer_exceptions

        modules.append(typer_exceptions)
    except Exception:
        pass
    try:
        import click.exceptions as click_exceptions_module

        modules.append(click_exceptions_module)
    except Exception:
        pass
    for module in modules:
        for name in ("ClickException", "Exit", "Abort", "UsageError"):
            candidate = getattr(module, name, None)
            if isinstance(candidate, type) and issubclass(candidate, BaseException):
                found.append(candidate)
    return tuple(found) or (SystemExit,)


def _handle_click(err: BaseException, reporter: Output) -> int:
    names = {klass.__name__ for klass in type(err).__mro__}
    if "Exit" in names:
        return int(getattr(err, "exit_code", 0) or 0)
    if "Abort" in names:
        reporter.warn("Aborted")
        return 130
    show = getattr(err, "show", None)
    if callable(show):
        try:
            show()
        except Exception:
            reporter.error(str(err))
    else:
        reporter.error(str(err))
    if "UsageError" in names:
        return EXIT_USAGE
    return int(getattr(err, "exit_code", EXIT_ERROR) or EXIT_ERROR)


def _invoke(args: list[str], state: Ctx | None = None) -> int:
    reporter = state.out if state is not None else Output()
    try:
        if state is None:
            app(args=args, prog_name=PROG, standalone_mode=False)
        else:
            app(args=args, prog_name=PROG, standalone_mode=False, obj=state)
    except CLIError as err:
        reporter.error(err.message, err.hint)
        return err.exit_code
    except click_exception_types() as err:
        return _handle_click(err, reporter)
    except KeyboardInterrupt:
        reporter.warn("Interrupted")
        return 130
    except BrokenPipeError:
        return 0
    return 0


def main(argv: list[str] | None = None) -> int:
    return _invoke(hoist_globals(list(sys.argv[1:] if argv is None else argv)))


def run_isolated(ctx_state: Ctx, argv: list[str]) -> int:
    return _invoke(hoist_globals(list(argv)), ctx_state)


def _short_help(command: Any) -> str:
    try:
        return (command.get_short_help_str(limit=70) or "").strip()
    except Exception:
        return (getattr(command, "help", "") or "").strip().splitlines()[0] if getattr(command, "help", "") else ""


def _subcommands(command: Any) -> dict[str, Any]:
    commands = getattr(command, "commands", None)
    return commands if isinstance(commands, dict) else {}


def describe_commands() -> list[tuple[str, str]]:
    root = typer.main.get_command(app)
    return [(name, _short_help(command)) for name, command in sorted(_subcommands(root).items())]


def describe_group(name: str) -> list[tuple[str, str]]:
    root = typer.main.get_command(app)
    group = _subcommands(root).get(name)
    if group is None:
        return []
    return [(sub, _short_help(command)) for sub, command in sorted(_subcommands(group).items())]
