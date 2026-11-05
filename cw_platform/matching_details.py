# cw_platform/matching_details.py
# CrossWatch - Bounded Match Evidence
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

CANDIDATE_LIMIT = 10
ID_NAMES = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "mal", "anilist")


def match_details(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping) or not isinstance(value.get("candidates"), list):
        return {}
    rows = value["candidates"]
    candidates = []
    for row in rows[:CANDIDATE_LIMIT]:
        if not isinstance(row, Mapping):
            continue
        candidate: dict[str, Any] = {}
        for field in ("local_id", "type", "title", "year", "season", "episode"):
            item = row.get(field)
            if isinstance(item, (str, int, float)) and not isinstance(item, bool):
                candidate[field] = str(item)[:300]
        for field in ("ids", "show_ids"):
            ids = row.get(field)
            if isinstance(ids, Mapping):
                candidate[field] = {key: str(ids[key])[:128] for key in ID_NAMES
                                    if isinstance(ids.get(key), (str, int)) and not isinstance(ids[key], bool)}
        if candidate:
            candidates.append(candidate)
    total = value.get("candidate_count")
    total = max(len(rows), total) if isinstance(total, int) and not isinstance(total, bool) else len(rows)
    return {"candidates": candidates, "candidate_count": total,
            "truncated": total > len(candidates)}
