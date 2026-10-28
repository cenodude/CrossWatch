# cw_platform/reason_labels.py
# CrossWatch - Reason Labels
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

_PLEX_HISTORY_MESSAGES = {
    "plex_discover_scope_unavailable": "Plex library scope could not be checked completely. Cloud fallback was deferred; retry when the server is available.",
    "plex_write_exception": "Plex history write failed unexpectedly. Check the write_failed log for the exception type and code location.",
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
    "plex_discover_disabled": "Plex Discover watched-state fallback is not enabled for this pair.",
    "plex_discover_not_found": "Plex Discover could not match this movie or episode by its external IDs.",
    "plex_discover_ambiguous_match": "Plex Discover returned conflicting matches. No watched state was written.",
    "plex_discover_account_token_required": "Plex rejected or is missing the account token. Reconnect the Plex account before retrying.",
    "plex_discover_endpoint_unavailable": "Plex could not find the cloud endpoint. Check the connection before retrying.",
    "plex_discover_manual_review": "The cloud play remained unidentified for one hour after the first completed identification scan. Automatic retries stopped. Inspect Plex history; the journal remains locked against repeat scrobbles. There is currently no supported reset action.",
    "plex_discover_account_mismatch": "The Plex account token does not belong to the selected user. No cloud write was made.",
    "plex_discover_account_unsupported": "Plex Discover fallback does not support managed accounts or switched Plex users.",
    "plex_discover_outside_library_scope": "This item exists outside the selected Plex libraries, so the cloud fallback was skipped.",
    "plex_discover_date_pending": "The Plex cloud watch date still needs repair. Retry with Discover fallback enabled; an ambiguous play will not be edited.",
    "plex_discover_rate_limited": "Plex cloud writes are rate limited. Retry after the limit expires; pending date updates are retained.",
    "plex_discover_request_failed": "Plex cloud recovery could not finish. Pending date updates are retained for retry.",
    "plex_discover_invalid_response": "Plex returned an unexpected cloud response. Retry after checking the account connection.",
    "plex_discover_watch_state_unavailable": "Plex did not expose this item's cloud watched state. No new play was written.",
    "plex_discover_history_unavailable": "Plex cloud history is unavailable. Pending date updates are retained for retry.",
    "plex_discover_history_order_unavailable": "Plex cloud history was not ordered newest-first or contained an invalid date. Cloud writes are paused for one hour; pending dates are retained.",
    "plex_discover_history_incomplete": "The cloud history scan reached its batch limit or returned incomplete pagination. Account cloud writes are paused for one hour; pending dates are retained.",
    "plex_discover_catalog_incomplete": "Plex's cloud episode catalog could not be read completely. No new play was written.",
}

_MEDIA_SERVER_LIBRARY_MESSAGES = {
    "not_in_library": "The media server library does not contain this item.",
    "not_found": "The media server could not find a matching library item.",
}

_SILO_MESSAGES = {
    "ignored_by_server": "Silo ignored this watch because the title was unwatched in Silo after this watch date.",
    "missing_duration": "Silo has no runtime for this item, so the watch cannot be written.",
}

_SIMKL_HISTORY_MESSAGES = {
    "simkl_not_found:episodes": "SIMKL could not find this episode with the normal history payload.",
    "simkl_not_found:anime:episodes": "SIMKL could not find this episode with the native anime history payload.",
    "simkl_not_found:anime:shows": "SIMKL could not find this anime entry with the native anime history payload.",
    "simkl_anime_unmapped:episodes": "CrossWatch could not map the source season and episode to the native SIMKL anime episode number.",
    "simkl_write_response_ambiguous:anime_count": "SIMKL accepted the anime request but did not report enough episode detail to safely confirm the write.",
    "simkl_not_found:anime_retry:episodes": "SIMKL could not find this episode with the anime retry payload.",
    "simkl_anime_retry_unmapped:episodes": "CrossWatch confirmed this is SIMKL anime, but could not map the source season and episode to SIMKL's native anime episode number.",
    "simkl_write_response_ambiguous:add_count": "SIMKL accepted the request but did not report enough detail to safely confirm the write.",
    "simkl_write_response_ambiguous:anime_retry_count": "SIMKL accepted the anime retry request but did not report enough episode detail to safely confirm the write.",
    "simkl_write_response_malformed:json": "SIMKL returned a response CrossWatch could not parse.",
}

_DIRECT_LABELS = {
    "path_match": "Matched by file path",
    "path_ambiguous": "Multiple file path matches",
    "path_id_conflict": "File path matches but IDs disagree",
    "not_in_plex_catalog": "Not in library",
    "not_in_library": "Not in library",
    "ignored_by_server": "Ignored by server",
    "missing_duration": "Missing runtime",
    "not_in_catalog": "Not in library",
    "plex_discover_date_pending": "Plex cloud date pending",
    "plex_discover_manual_review": "Plex cloud play needs manual review",
    "plex_discover_rate_limited": "Plex cloud rate limit",
    "plex_discover_not_found": "Not found in Plex Discover",
    "plex_discover_request_failed": "Plex cloud write incomplete",
    "simkl_not_found:anime_retry:episodes": "SIMKL anime episode not found",
    "simkl_not_found:anime:episodes": "SIMKL anime episode not found",
    "simkl_not_found:anime:shows": "SIMKL anime entry not found",
    "simkl_anime_unmapped:episodes": "SIMKL anime episode mapping missing",
    "simkl_write_response_ambiguous:anime_count": "SIMKL anime response ambiguous",
    "simkl_not_found:episodes": "SIMKL episode not found",
    "simkl_anime_retry_unmapped:episodes": "SIMKL anime episode mapping missing",
    "simkl_write_response_ambiguous:add_count": "SIMKL write response ambiguous",
    "simkl_write_response_ambiguous:anime_retry_count": "SIMKL anime retry response ambiguous",
    "simkl_write_response_malformed:json": "SIMKL malformed response",
    "not_mapped": "No anime mapping for this target",
    "not_anime_or_no_mapping": "Not anime or no anime mapping",
    "anime_mapping_unavailable": "Anime mapping not installed",
    "missing_media_identity": "Missing show IDs",
    "media_not_found": "Not found on target",
    "anime_media_type_mismatch": "Anime type mismatch",
    "episode_out_of_range": "Episode out of range",
    "duplicate_stop": "Already recorded on target",
    "duplicate": "Already recorded on target",
    "pro_required": "Rewatch needs a paid plan on target",
    "rewatch_unconfirmed": "Rewatch not confirmed by target",
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
    if provider_base in {"PLEX", "EMBY", "JELLYFIN", "KODI", "SILO"} and low in _MEDIA_SERVER_LIBRARY_MESSAGES:
        return _MEDIA_SERVER_LIBRARY_MESSAGES[low]
    if provider_base == "SILO" and low in _SILO_MESSAGES:
        return _SILO_MESSAGES[low]
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
