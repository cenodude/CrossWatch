# cw_platform/orchestration/_enrich.py
# Enrichment utilities for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Mapping, Sequence
from typing import Any
from ..id_map import minimal

def has_ids_for(provider: str | None, ids: Mapping[str, Any] | None) -> bool:
    p = (provider or "").upper()
    if p == "TRAKT": keys = ("trakt", "tmdb", "imdb", "tvdb")
    elif p == "SIMKL": keys = ("imdb", "tmdb", "tvdb", "slug")
    elif p == "PLEX": keys = ("plex", "guid", "imdb", "tmdb", "tvdb", "trakt")
    elif p == "JELLYFIN": keys = ("jellyfin", "imdb", "tmdb", "tvdb", "slug")
    else: keys = ("tmdb", "imdb", "tvdb", "trakt", "slug")
    return any((ids or {}).get(k) for k in keys)

def write_skipped(
    items: Sequence[Mapping[str, Any]],
    *,
    feature: str,
    dst: str,
) -> None:
    try:
        from pathlib import Path
        import json, time
        base = Path("/config/.cw_state")
        base.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()),
            "feature": feature,
            "dst": dst,
            "count": len(items),
            "items": [minimal(x) for x in items],
        }
        (base / f"skipped_{dst.lower()}_{feature}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            "utf-8",
        )
    except Exception:
        pass