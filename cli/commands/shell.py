# /cli/commands/shell.py
# CrossWatch - CLI interactive shell
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

import typer

from .._context import Ctx
from .._errors import CLIError
from .._settings import cli_home

GROUPS = (
    "pair",
    "sync",
    "config",
    "auth",
    "watcher",
    "scheduler",
    "logs",
    "analyzer",
    "events",
    "capture",
    "backup",
    "watchlist",
    "progress",
    "editor",
    "playlist",
    "export",
    "import",
    "metadata",
    "manual",
    "anime",
    "instance",
    "user-profile",
    "scrobbler",
    "activity",
    "maintenance",
)
LEAVE = {"exit", "end", "quit", "up", "..", "back"}
BANNER = """CrossWatch interactive shell.

  ?              list what you can run here
  <group>        step into a group, for example: sync
  exit / end     leave the group, or quit at the top
  !<command>     run a command at the top level from inside a group
"""


def _history_file() -> Path:
    home = cli_home()
    home.mkdir(parents=True, exist_ok=True)
    return home / "history"


def _setup_readline(completer: Any) -> Any:
    try:
        import readline as _readline
    except Exception:
        return None
    readline: Any = _readline
    try:
        readline.read_history_file(str(_history_file()))
    except Exception:
        pass
    try:
        readline.set_completer(completer)
        readline.set_completer_delims(" \t\n")
        readline.parse_and_bind("tab: complete")
    except Exception:
        pass
    return readline


def _save_history(readline: Any) -> None:
    if readline is None:
        return
    try:
        readline.set_history_length(1000)
        readline.write_history_file(str(_history_file()))
    except Exception:
        pass


class Shell:
    def __init__(self, state: Ctx) -> None:
        self.state = state
        self.group = ""
        self._root_cache: list[tuple[str, str]] = []
        self._group_cache: dict[str, list[tuple[str, str]]] = {}

    def root_commands(self) -> list[tuple[str, str]]:
        if not self._root_cache:
            from .._app import describe_commands

            self._root_cache = describe_commands()
        return self._root_cache

    def group_commands(self, name: str) -> list[tuple[str, str]]:
        if name not in self._group_cache:
            from .._app import describe_group

            self._group_cache[name] = describe_group(name)
        return self._group_cache[name]

    def prompt(self) -> str:
        return f"cw({self.group})> " if self.group else "cw> "

    def complete(self, text: str, index: int) -> str | None:
        pool = [name for name, _ in (self.group_commands(self.group) if self.group else self.root_commands())]
        pool = pool + sorted(LEAVE) + ["?", "help"]
        matches = [c for c in pool if c.startswith(text)]
        return matches[index] if index < len(matches) else None

    def show_help(self) -> None:
        out = self.state.out
        if self.group:
            rows = [[name, doc] for name, doc in self.group_commands(self.group)]
            out.table(["COMMAND", "WHAT IT DOES"], rows, title=f"cw {self.group}")
            out.info("Type 'exit' to go back, or !<command> to run something at the top level.")
            return
        rows = [[name, doc] for name, doc in self.root_commands()]
        out.table(["COMMAND", "WHAT IT DOES"], rows, title="cw")
        out.info("Type a group name to step into it, for example: sync")

    def dispatch(self, argv: list[str]) -> int:
        from .._app import run_isolated

        return run_isolated(self.state, argv)

    def handle(self, raw: str) -> bool:
        line = raw.strip()
        if not line:
            return True
        if line in ("?", "help") or line.startswith("? "):
            self.show_help()
            return True
        if line.lower() in LEAVE:
            if self.group:
                self.group = ""
                return True
            return False
        if line.lower() in ("clear", "cls"):
            self.state.out.console.clear()
            return True

        try:
            argv = shlex.split(line)
        except ValueError as exc:
            self.state.out.error(f"Cannot parse that line: {exc}")
            return True
        if not argv:
            return True

        if argv[0].startswith("!"):
            argv[0] = argv[0][1:]
            argv = [a for a in argv if a]
            if argv:
                self.dispatch(argv)
            return True

        if argv[0] == "help" and len(argv) > 1:
            target = ([self.group] if self.group else []) + argv[1:] + ["--help"]
            self.dispatch(target)
            return True

        if not self.group and argv[0] in GROUPS and len(argv) == 1:
            self.group = argv[0]
            return True

        if self.group:
            known = {name for name, _ in self.group_commands(self.group)}
            if argv[0] in known:
                self.dispatch([self.group, *argv])
                return True
            if argv[0] in GROUPS and len(argv) == 1:
                self.group = argv[0]
                return True
            self.dispatch(argv)
            return True

        self.dispatch(argv)
        return True

    def run(self) -> None:
        out = self.state.out
        out.print(f"[cw.accent]CrossWatch[/] {self.state.url}  ([cw.muted]{self.state.mode}[/])")
        out.print(BANNER)
        readline = _setup_readline(self.complete)
        try:
            while True:
                try:
                    line = input(self.prompt())
                except EOFError:
                    out.print()
                    break
                except KeyboardInterrupt:
                    out.print()
                    continue
                if not self.handle(line):
                    break
        finally:
            _save_history(readline)
        out.info("Bye.")


def register(app: typer.Typer) -> None:
    @app.command("shell")
    def shell_cmd(ctx: typer.Context) -> None:
        """Start an interactive CrossWatch session with grouped contexts."""
        state: Ctx = ctx.obj
        if state.out.json_mode:
            raise CLIError("The shell needs a human readable output format", hint="Drop -o json.")
        Shell(state).run()
