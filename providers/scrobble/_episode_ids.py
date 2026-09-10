# providers/scrobble/_episode_ids.py
# CrossWatch - Episode Provider Identity
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

PUBLIC_IDS = ("tmdb", "imdb", "tvdb")


def episode_ids(ids: Mapping[str, Any], show_ids: Mapping[str, Any] | None = None) -> dict[str, str]:
    out: dict[str, str] = {}
    for namespace in PUBLIC_IDS:
        value = ids.get(f"{namespace}_episode") or ids.get(namespace)
        known_show_ids = (ids.get(f"{namespace}_show"), (show_ids or {}).get(namespace))
        if value and str(value) not in {str(show_id) for show_id in known_show_ids if show_id}:
            out[namespace] = str(value)
    return out
