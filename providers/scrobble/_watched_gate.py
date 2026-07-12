# providers/scrobble/_watched_gate.py
# CrossWatch - shared scrobble watched-gate policy
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations


def resolve_stop_action(progress: float, watched_at: float) -> str:
    try:
        return "stop" if float(progress) >= float(watched_at) else "pause"
    except (TypeError, ValueError):
        return "stop"
