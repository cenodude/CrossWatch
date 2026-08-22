# /cli/__init__.py
# CrossWatch - Command line package
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

__all__ = ["main"]


def main(argv: list[str] | None = None) -> int:
    from ._app import main as _main

    return _main(argv)
