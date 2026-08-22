# /cli/_render.py
# CrossWatch - CLI output rendering
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import sys
from typing import Any, Iterable, Sequence

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.theme import Theme

CW_THEME = Theme(
    {
        "cw.accent": "bold #b026ff",
        "cw.key": "dim",
        "cw.ok": "bold green",
        "cw.warn": "bold yellow",
        "cw.err": "bold red",
        "cw.muted": "dim",
        "cw.on": "green",
        "cw.off": "red",
    }
)

MODES = ("auto", "table", "json", "yaml", "plain")


class Output:
    def __init__(self, mode: str = "auto", *, color: bool = True, quiet: bool = False) -> None:
        self.mode = mode if mode in MODES else "auto"
        self.quiet = bool(quiet)
        force = None if color else False
        self.console = Console(theme=CW_THEME, no_color=not color, force_terminal=force, soft_wrap=False)
        self.err_console = Console(theme=CW_THEME, no_color=not color, stderr=True)

    @property
    def json_mode(self) -> bool:
        return self.mode in ("json", "yaml")

    @property
    def plain(self) -> bool:
        return self.mode == "plain"

    def data(self, payload: Any) -> None:
        if self.mode == "yaml":
            sys.stdout.write(_to_yaml(payload) + "\n")
        else:
            sys.stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        sys.stdout.flush()

    def print(self, *args: Any, **kwargs: Any) -> None:
        if self.quiet or self.json_mode:
            return
        self.console.print(*args, **kwargs)

    def rule(self, title: str) -> None:
        if self.quiet or self.json_mode or self.plain:
            return
        self.console.rule(Text(title, style="cw.accent"))

    def raw(self, text: str) -> None:
        sys.stdout.write(str(text) + "\n")
        sys.stdout.flush()

    def success(self, message: str) -> None:
        if self.quiet or self.json_mode:
            return
        self.console.print(f"[cw.ok]OK[/] {message}")

    def info(self, message: str) -> None:
        if self.quiet or self.json_mode:
            return
        self.console.print(f"[cw.muted]{message}[/]")

    def warn(self, message: str) -> None:
        if self.json_mode:
            return
        self.err_console.print(f"[cw.warn]![/] {message}")

    def error(self, message: str, hint: str = "") -> None:
        self.err_console.print(f"[cw.err]x[/] {message}")
        if hint:
            self.err_console.print(f"  [cw.muted]{hint}[/]")

    def table(
        self,
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
        *,
        title: str = "",
        empty: str = "Nothing to show",
    ) -> None:
        materialised = [list(r) for r in rows]
        if self.plain:
            for row in materialised:
                self.raw("\t".join("" if c is None else str(c) for c in row))
            return
        if not materialised:
            self.info(empty)
            return
        table = Table(
            title=Text(title, style="cw.accent") if title else None,
            title_justify="left",
            header_style="cw.accent",
            box=None,
            pad_edge=False,
            show_edge=False,
            expand=False,
        )
        for name in columns:
            table.add_column(str(name), overflow="fold")
        for row in materialised:
            table.add_row(*[_cell(c) for c in row])
        self.console.print(table)

    def records(
        self,
        items: Sequence[Any],
        columns: Sequence[tuple[str, Any]],
        *,
        title: str = "",
        empty: str = "Nothing to show",
    ) -> None:
        rows = []
        for item in items:
            source = item if isinstance(item, dict) else {}
            row = []
            for _, key in columns:
                row.append(key(source) if callable(key) else source.get(key))
            rows.append(row)
        self.table([head for head, _ in columns], rows, title=title, empty=empty)

    def kv(self, pairs: Sequence[tuple[str, Any]], *, title: str = "") -> None:
        if self.plain:
            for key, value in pairs:
                self.raw(f"{key}\t{_plain(value)}")
            return
        table = Table(box=None, show_header=False, pad_edge=False, show_edge=False)
        table.add_column(style="cw.key", no_wrap=True)
        table.add_column(overflow="fold")
        for key, value in pairs:
            table.add_row(str(key), _cell(value))
        if title:
            self.console.print(Panel(table, title=Text(title, style="cw.accent"), title_align="left", border_style="cw.muted"))
        else:
            self.console.print(table)


def _plain(value: Any) -> str:
    if isinstance(value, Text):
        return value.plain
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def _cell(value: Any) -> Any:
    if isinstance(value, Text):
        return value
    if isinstance(value, bool):
        return Text("yes" if value else "no", style="cw.on" if value else "cw.off")
    if value is None:
        return Text("-", style="cw.muted")
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value) or Text("-", style="cw.muted")
    text = str(value)
    return text if text else Text("-", style="cw.muted")


def state_text(value: Any, *, on: str = "on", off: str = "off") -> Text:
    flag = bool(value)
    return Text(on if flag else off, style="cw.on" if flag else "cw.off")


def _to_yaml(payload: Any, indent: int = 0) -> str:
    pad = "  " * indent
    if isinstance(payload, dict):
        if not payload:
            return pad + "{}"
        lines = []
        for key, value in payload.items():
            if isinstance(value, (dict, list)) and value:
                lines.append(f"{pad}{key}:")
                lines.append(_to_yaml(value, indent + 1))
            else:
                lines.append(f"{pad}{key}: {_yaml_scalar(value)}")
        return "\n".join(lines)
    if isinstance(payload, list):
        if not payload:
            return pad + "[]"
        lines = []
        for item in payload:
            if isinstance(item, (dict, list)) and item:
                rendered = _to_yaml(item, indent + 1).lstrip()
                lines.append(f"{pad}- {rendered}")
            else:
                lines.append(f"{pad}- {_yaml_scalar(item)}")
        return "\n".join(lines)
    return pad + _yaml_scalar(payload)


def _yaml_scalar(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if text == "" or any(ch in text for ch in ":#\n\"'") or text.strip() != text:
        return json.dumps(text)
    return text
