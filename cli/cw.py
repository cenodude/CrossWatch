#!/usr/bin/env python3
# /cli/cw.py
# CrossWatch - Command line entry point
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from cli._app import app, main  # noqa: E402

__all__ = ["app", "main"]

if __name__ == "__main__":
    raise SystemExit(main())
