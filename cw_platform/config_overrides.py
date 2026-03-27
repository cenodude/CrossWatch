# cw_platform/config_overrides.py
# configuration overrides for migration compatibility and other non-default settings
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

MIGRATION_OVERRIDE_KEYS: tuple[str, ...] = (
    # Plex
    "plex.watchlist_page_size",
    "plex.watchlist_query_limit",
    "plex.watchlist_write_delay_ms",
    "plex.watchlist_guid_priority",

    # Simkl
    "simkl.rate_limit.post_per_sec",
    "simkl.rate_limit.get_per_sec",
    "simkl.watchlist_batch_size",
    "simkl.ratings_chunk_size",
    "simkl.history_chunk_size",

    # MDBList
    "mdblist.rate_limit.post_per_sec",
    "mdblist.rate_limit.get_per_sec",
    "mdblist.watchlist_shadow_ttl_hours",
    "mdblist.watchlist_shadow_validate",
    "mdblist.watchlist_page_size",
    "mdblist.watchlist_batch_size",
    "mdblist.watchlist_freeze_details",
    "mdblist.ratings_per_page",
    "mdblist.ratings_max_pages",
    "mdblist.ratings_chunk_size",
    "mdblist.ratings_write_delay_ms",
    "mdblist.ratings_max_backoff_ms",
    "mdblist.ratings_since",
    "mdblist.history_per_page",
    "mdblist.history_max_pages",
    "mdblist.history_chunk_size",
    "mdblist.history_write_delay_ms",
    "mdblist.history_max_backoff_ms",
    "mdblist.history_since",

    # Tautulli
    "tautulli.history.per_page",
    "tautulli.history.max_pages",

    # Trakt
    "trakt.rate_limit.get_per_sec",
    "trakt.rate_limit.post_per_sec",
    "trakt.watchlist_batch_size",
    "trakt.ratings_per_page",
    "trakt.ratings_max_pages",
    "trakt.ratings_chunk_size",
    "trakt.history_per_page",
    "trakt.history_max_pages",
    "trakt.history_chunk_size",

    # Jellyfin
    "jellyfin.watchlist.watchlist_query_limit",
    "jellyfin.watchlist.watchlist_write_delay_ms",
    "jellyfin.watchlist.watchlist_guid_priority",
    "jellyfin.history.history_query_limit",
    "jellyfin.history.history_write_delay_ms",
    "jellyfin.history.history_guid_priority",
    "jellyfin.ratings.ratings_query_limit",

    # Emby
    "emby.watchlist.watchlist_query_limit",
    "emby.watchlist.watchlist_write_delay_ms",
    "emby.watchlist.watchlist_guid_priority",
    "emby.history.history_query_limit",
    "emby.history.history_write_delay_ms",
    "emby.history.history_guid_priority",
    "emby.ratings.ratings_query_limit",

    # Runtime
    "runtime.snapshot_ttl_sec",
    "runtime.apply_chunk_size",
    "runtime.apply_chunk_pause_ms",
    "runtime.apply_chunk_size_by_provider",
)
