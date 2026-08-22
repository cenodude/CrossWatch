# /cli/__main__.py
# CrossWatch - Entry point for python -m cli
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import sys

from ._app import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
