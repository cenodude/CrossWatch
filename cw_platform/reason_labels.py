# cw_platform/reason_labels.py
# CrossWatch - Reason Labels
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

_PLEX_HISTORY_MESSAGES = {
    "in_catalog_watched": "Plex catalog already has this item marked watched.",
    "in_catalog_unwatched": "Plex catalog has this item, but it is not marked watched yet.",
    "not_in_plex_catalog": "Plex could not find this item in the selected libraries.",
    "show_matched_episode_missing": "Plex found the show, but not this season and episode in the selected libraries.",
    "resolve_ambiguous": "Plex found multiple possible matches and skipped the item to avoid writing to the wrong media.",
    "confirmed_watched_exact_date": "Plex confirmed this item as watched with a matching watched date.",
    "confirmed_watched_date_mismatch": "Plex confirmed this item as watched, but the watched date differs from the source date.",
    "confirmed_watched_no_date": "Plex confirmed this item as watched, but Plex did not expose a watched date to compare.",
    "write_failed": "Plex write failed before the watched state could be confirmed.",
    "scrobble_failed": "Plex scrobble failed before the watched state could be confirmed.",
    "missing_watched_at": "The source item has no watched date, so CrossWatch cannot backdate the Plex history write.",
    "home_scope_not_applied": "Plex Home scope was required but could not be applied.",
}

_MEDIA_SERVER_LIBRARY_MESSAGES = {
    "not_in_library": "The media server library does not contain this item.",
    "not_found": "The media server could not find a matching library item.",
}

_SIMKL_HISTORY_MESSAGES = {
    "simkl_not_found:episodes": "SIMKL could not find this episode with the normal history payload.",
    "simkl_not_found:anime_retry:episodes": "SIMKL could not find this episode with the anime retry payload.",
    "simkl_anime_retry_unmapped:episodes": "CrossWatch confirmed this is SIMKL anime, but could not map the source season and episode to SIMKL's native anime episode number.",
    "simkl_write_response_ambiguous:add_count": "SIMKL accepted the request but did not report enough detail to safely confirm the write.",
    "simkl_write_response_ambiguous:anime_retry_count": "SIMKL accepted the anime retry request but did not report enough episode detail to safely confirm the write.",
    "simkl_write_response_malformed:json": "SIMKL returned a response CrossWatch could not parse.",
}

_DIRECT_LABELS = {
    "not_in_plex_catalog": "Not in library",
    "not_in_library": "Not in library",
    "not_in_catalog": "Not in library",
    "simkl_not_found:anime_retry:episodes": "SIMKL anime episode not found",
    "simkl_not_found:episodes": "SIMKL episode not found",
    "simkl_anime_retry_unmapped:episodes": "SIMKL anime episode mapping missing",
    "simkl_write_response_ambiguous:add_count": "SIMKL write response ambiguous",
    "simkl_write_response_ambiguous:anime_retry_count": "SIMKL anime retry response ambiguous",
    "simkl_write_response_malformed:json": "SIMKL malformed response",
}

TRACKER_TO_MEDIA_SERVER_MESSAGE = (
    "Tracker to media server syncs can have expected gaps because trackers store history "
    "for items that are not present in your media server libraries."
)


def _code(raw: Any) -> str:
    return str(raw or "").strip()


def _base(provider: Any) -> str:
    return str(provider or "").split("@", 1)[0].split("#", 1)[0].split(":", 1)[0].strip().upper()


def friendly_reason(raw: Any) -> str:
    code = _code(raw)
    low = code.lower()
    if not low:
        return "unresolved"
    if low in _DIRECT_LABELS:
        return _DIRECT_LABELS[low]
    if low.startswith("simkl_write_failed:"):
        return "SIMKL request failed"
    if low.startswith("simkl_write_response_malformed:"):
        return "SIMKL malformed response"
    if low.startswith("simkl_write_response_ambiguous:"):
        return "SIMKL write response ambiguous"
    if "episode_missing" in low:
        return "Show found - episode missing"
    if "ambiguous" in low:
        return "Ambiguous match"
    if "missing_watched_at" in low:
        return "Missing watched date"
    if any(t in low for t in ("missing_id", "no_id", "unmatched", "no_confirmations", "provider_unresolved", "id_mismatch")):
        return "ID mismatch"
    if "provider_down" in low or "unavailable" in low:
        return "provider unavailable"
    if "not_removed" in low:
        return "not removed"
    if "exception" in low:
        return "error"
    if "failed" in low:
        return "request failed"
    if "fallback" in low or "unconfirmed" in low:
        return "unconfirmed"
    return code


def reason_message(raw: Any, *, provider: Any = None, feature: Any = None) -> str:
    code = _code(raw)
    if not code:
        return ""
    low = code.lower()
    provider_base = _base(provider)
    feature_key = str(feature or "").strip().lower()
    if provider_base == "PLEX" and feature_key == "history" and low in _PLEX_HISTORY_MESSAGES:
        msg = _PLEX_HISTORY_MESSAGES[low]
        if low == "not_in_plex_catalog":
            return f"{msg} {TRACKER_TO_MEDIA_SERVER_MESSAGE}"
        return msg
    if provider_base in {"PLEX", "EMBY", "JELLYFIN"} and low in _MEDIA_SERVER_LIBRARY_MESSAGES:
        return _MEDIA_SERVER_LIBRARY_MESSAGES[low]
    if provider_base == "SIMKL" and feature_key == "history":
        if low in _SIMKL_HISTORY_MESSAGES:
            return _SIMKL_HISTORY_MESSAGES[low]
        if low.startswith("simkl_write_failed:"):
            return "SIMKL request failed before the history write could be confirmed."
        if low.startswith("simkl_write_response_malformed:"):
            return "SIMKL returned a response CrossWatch could not parse."
        if low.startswith("simkl_write_response_ambiguous:"):
            return "SIMKL did not report enough detail to safely confirm the history write."
    if low == "missing_ids_for_key":
        return "The item is missing IDs needed for a safe provider match."
    if low in {"apply:add:failed", "two:apply:add:failed"}:
        return "The provider did not confirm this write."
    return ""
