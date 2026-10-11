# services/movie_matching.py
# CrossWatch - Diagnosis for a movie that does not match between two providers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from typing import Any

from fastapi import HTTPException

from cw_platform.id_map import ID_KEYS

SHARED = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "mdblist")
COMMON = ("imdb", "tmdb", "tvdb")


def _name(value):
    return re.sub(r"\W+", "", str(value or "")).casefold()


def _year(value):
    text = str(value or "")[:4]
    return int(text) if text.isdigit() else None


def _same_film(left, right):
    if not _name(left.get("title")) or _name(left.get("title")) != _name(right.get("title")):
        return False
    first, second = _year(left.get("year")), _year(right.get("year"))
    return first is None or second is None or abs(first - second) <= 1


def _ids(item):
    return {key: str(value) for key, value in (item.get("ids") or {}).items() if key in ID_KEYS and value not in (None, "")}


def _conflict(mine, theirs):
    return any(mine[key] != theirs[key] for key in mine.keys() & theirs.keys() if key in SHARED)


def _candidate(found):
    return dict(title=found.get("title") or "", year=_year(found.get("year")), ids=_ids(found))


def diagnose(cfg, row, destination_items, *, by_id=None, search=None):
    from services.catalog_lookup import LABELS, movie_by_id
    from services.interactive_sync_mapping import search_candidates

    item = row["item"]
    if item.get("type") != "movie":
        raise HTTPException(400, "Matching review is available for movies here")
    by_id = by_id or movie_by_id
    search = search or search_candidates
    provider, instance = str(row["provider"]).upper(), row.get("instance") or "default"
    label = LABELS.get(provider, provider.title())
    mine = _ids(item)
    checked: list[str] = []
    result: dict[str, Any] = dict(ok=True, destination=label, checked=checked, candidates=[], candidate=None)

    twins = [other for other in destination_items if other.get("type") == "movie" and _same_film(item, other)
             and not set(mine.items()) & set(_ids(other).items())]
    checked.append(f"{label} items already synced")
    if len(twins) == 1:
        clash = _conflict(mine, _ids(twins[0]))
        return {**result, "status": "twin", "candidate": _candidate(twins[0]), "message":
                f"{label} already has a film with this title and year under different IDs"
                + (", and the IDs disagree. Check that it is the same film before matching." if clash else ". It is very likely the same film.")}

    found = by_id(cfg, provider, instance, mine)
    if found != "unsupported":
        checked.append(f"{label} catalogue by ID")
    if isinstance(found, dict):
        theirs = _ids(found)
        if not _same_film(item, found) and _name(item.get("title")) and _name(found.get("title")):
            return {**result, "status": "different", "candidate": _candidate(found), "message":
                    f"{label} resolves these IDs to a different title, “{found.get('title')}”. One of the providers has a wrong ID. Check before matching."}
        common = {key: value for key, value in theirs.items() if key in COMMON}
        differing = sorted(key.upper() for key, value in common.items() if key in mine and mine[key] != value)
        if differing:
            return {**result, "status": "conflict", "candidate": dict(_candidate(found), ids={**mine, **common}), "message":
                    f"{label} lists a different {' and '.join(differing)} ID for this film. Using its IDs should let it match."}
        if set(common) - set(mine):
            return {**result, "status": "known", "candidate": dict(_candidate(found), ids={**mine, **common}), "message":
                    f"{label} knows this film and lists IDs the source does not have. Adding them should let it match."}
        return {**result, "status": "recognised", "message":
                f"{label} recognises these IDs, so this is not an ID problem. Check the reason the provider reported and run the pair again."}

    hits = []
    for catalog in ("destination", "tmdb"):
        try:
            rows = search(cfg, row, str(item.get("title") or ""), catalog=catalog).get("results") or []
        except HTTPException:
            continue
        checked.append(f"{label} search by title" if catalog == "destination" else "TMDb search by title")
        hits = [found for found in rows if found.get("type", "movie") == "movie" and _same_film(item, found)
                and not found.get("mapping_unavailable")]
        if hits:
            break
    if len(hits) == 1:
        clash = _conflict(mine, _ids(hits[0]))
        return {**result, "status": "candidate", "candidate": _candidate(hits[0]), "message":
                "Found one film with this title and year"
                + (", but its IDs disagree with the source. Check that it is the same film before matching." if clash
                   else (f". {label} did not recognise the source IDs." if found is None else "."))}
    if len(hits) > 1:
        return {**result, "status": "ambiguous", "candidates": [_candidate(found) for found in hits[:5]], "message":
                "Several films share this title and year. Pick the right one with Edit mapping."}
    if len(checked) == 1:
        return {**result, "status": "unknown", "message":
                f"{label} cannot be searched from CrossWatch and no TMDb metadata key is set, so this film could not be looked up."}
    return {**result, "status": "not_found", "message":
            f"No film with this title and year was found. {label} may not list it yet."}
