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


def code(item):
    try:
        return f"S{int(item['season']):02d}E{int(item['episode']):02d}"
    except (KeyError, TypeError, ValueError):
        return ""


def label(item):
    title = str(item.get("series_title") or item.get("title") or "").strip()
    return " ".join(part for part in (title, code(item)) if part)


def unique_ids(index):
    seen = Counter(pair for item in index.values() for pair in episode_ids(item).items())
    return lambda item: {ns: value for ns, value in episode_ids(item).items() if seen[(ns, value)] == 1}


def conflicts(left, right):
    rows = []
    ours, others = unique_ids(left), unique_ids(right)
    places = [{pair: key for key, item in index.items() for pair in ids(item).items()}
              for index, ids in ((left, ours), (right, others))]
    for key, item in left.items():
        if key not in right:
            continue
        mine, theirs = ours(item), others(right[key])
        shared = mine.keys() & theirs.keys()
        if not shared or any(mine[ns] == theirs[ns] for ns in shared):
            continue
        moved = any(places[1].get(pair) not in (None, key) for pair in mine.items()) or any(
            places[0].get(pair) not in (None, key) for pair in theirs.items())
        if moved:
            rows.append((key, item, right[key]))
    return rows


def summary(rows):
    shows = {}
    for key, item, _ in rows:
        title = str(item.get("series_title") or item.get("title") or "").strip()
        shows.setdefault(title, []).append(code(item) or key)
    parts = [" ".join(part for part in (title, ", ".join(sorted(codes)[:6]) + (", ..." if len(codes) > 6 else "")) if part)
             for title, codes in list(shows.items())[:3]]
    examples = "; ".join(parts) + ("; ..." if len(shows) > 3 else "")
    count = len(rows)
    return (f"{count} episode{'' if count == 1 else 's'} share a season and episode number on both providers but carry "
            f"different episode IDs, so they may be different episodes ({examples}). "
            "Use Fix matching in the Analyzer to see what differs.")
