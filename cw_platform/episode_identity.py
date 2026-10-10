# cw_platform/episode_identity.py
# CrossWatch - Episodes that share a number but carry different episode identifiers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections import Counter
from collections.abc import Mapping

NAMESPACES = ("tvdb", "imdb", "tmdb")


def episode_ids(item):
    if not isinstance(item, Mapping) or str(item.get("type") or "").lower() != "episode":
        return {}
    ids, show = item.get("ids"), item.get("show_ids")
    if not isinstance(ids, Mapping) or not isinstance(show, Mapping) or not show:
        return {}
    found = {ns: str(ids[ns]).strip().lower() for ns in NAMESPACES if ids.get(ns) not in (None, "")}
    return {ns: value for ns, value in found.items() if value != str(show.get(ns) or "").strip().lower()}


def label(item):
    title = str(item.get("series_title") or item.get("title") or "").strip()
    try:
        code = f"S{int(item['season']):02d}E{int(item['episode']):02d}"
    except (KeyError, TypeError, ValueError):
        code = ""
    return " ".join(part for part in (title, code) if part)


def unique_ids(index):
    seen = Counter(pair for item in index.values() for pair in episode_ids(item).items())
    return lambda item: {ns: value for ns, value in episode_ids(item).items() if seen[(ns, value)] == 1}


def conflicts(left, right):
    rows = []
    ours, others = unique_ids(left), unique_ids(right)
    for key, item in left.items():
        if key not in right:
            continue
        mine, theirs = ours(item), others(right[key])
        shared = mine.keys() & theirs.keys()
        if shared and all(mine[ns] != theirs[ns] for ns in shared):
            rows.append((key, item, right[key]))
    return rows


def summary(rows):
    examples = ", ".join(dict.fromkeys(label(item) or key for key, item, _ in rows[:5]))
    count = len(rows)
    return (f"{count} episode{'' if count == 1 else 's'} share a season and episode number on both providers but carry "
            f"different episode IDs, so they may be different episodes ({examples}{', ...' if count > 5 else ''}). "
            "Check the numbering, then add episode corrections or an episode group.")
