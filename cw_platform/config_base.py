# cw_platform/config_base.py
# configuration management base.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def CONFIG_BASE() -> Path:
    env = os.getenv("CONFIG_BASE")
    if env:
        return Path(env)

    if Path("/app").exists():
        # In container image mount /config as a writable volume
        return Path("/config")
    return Path(__file__).resolve().parents[1]


CONFIG: Path = CONFIG_BASE()
CONFIG.mkdir(parents=True, exist_ok=True)

# Default config
DEFAULT_CFG: dict[str, Any] = {
    # --- Providers -----------------------------------------------------------
    "plex": {
        "server_url": "",                               # http(s)://host:32400 (required for sync & watcher).
        "verify_ssl": False,                            # Verify TLS certificates
        "account_token": "",                            # Plex token (a.k.a. authentication token).
        "client_id": "",                                # Set by PIN login; reused for headers.
        "machine_id": "",                               # PMS machineIdentifier (UUID).
        "username": "",                                 # Preferred Plex Home user/profile.
        "account_id": "",                               # Server-local accountID (int) for the selected user. 
        "home_pin": "",                                 # Plex Home PIN for the selected profile (optional).
        "timeout": 10.0,                                # Optional HTTP timeout (seconds).
        "max_retries": 3,                               # Optional retry budget.
        "fallback_GUID": False,                         # Opt-in GUID/Discover fallback for missing library items (history/ratings)

        "scrobble": {
            "libraries": [],                            # Whitelist of library IDs for scrobble (webhook/watch); empty = all
        },

        "history": {
            "libraries": [],                            # Whitelist of library GUIDs; empty = all
            "include_marked_watched": True,             # Include items manually marked as watched in Plex
        },
        "ratings": {
            "libraries": [],                            # Whitelist of library GUIDs; empty = all
        },

        # Ratings / History
        "rating_workers": 12,                           # Parallel workers for Plex ratings indexing. 12–16 is ideal on a local NAS.
        "history_workers": 12,                          # Parallel workers for Plex history indexing. 12–16 is ideal on a local NAS.

        # Watchlist via Discover (with PMS fallback toggle)
        "watchlist_allow_pms_fallback": False,          # Allow PMS watchlist fallback when needed. Keep False for strict Discover-only behavior.
        "watchlist_page_size": 100,                     # Discover page size (100-200). Higher = faster, but more risk of 504 timeouts.
        "watchlist_query_limit": 25,                    # Max Discover search results per query (10–25). Lower = faster, 25 = safer.
        "watchlist_write_delay_ms": 0,                  # Optional pacing between Discover writes; set 50–150 if you hit 429/5xx.
        "watchlist_title_query": True,                  # Use title/slug tokens for Discover candidate fetching (Discover is text-only).
        "watchlist_use_metadata_match": True,           # Try METADATA /library/metadata/matches with imdb-/tmdb-/tvdb- first; fallback to Discover.
        "watchlist_guid_priority": [                    # GUID resolution order (first match wins).
            "tmdb", "imdb", "tvdb",
            "agent:themoviedb:en", "agent:themoviedb", "agent:imdb"
        ],
    },

    "simkl": {
        "access_token": "",                             # OAuth2 access token
        "refresh_token": "",                            # OAuth2 refresh token
        "token_expires_at": 0,                          # Epoch when access_token expires
        "client_id": "",                                # From your Simkl app
        "client_secret": "",                            # From your Simkl app
        "date_from": "",                                # YYYY-MM-DD (optional start date for full sync)
    },
    
    "anilist": {
        "client_id": "",                                # From your AniList app
        "client_secret": "",                            # From your AniList app
        "access_token": "",                             # OAuth access token (saved after auth)
        "user": {},                                     # Viewer object (id/name)
    },

    "mdblist": {
        "api_key": "",                                  # Your MDBList API key
        "timeout": 10,                                  # HTTP timeout (seconds)
        "max_retries": 3,                               # Retry budget

        # Watchlist
        "watchlist_shadow_ttl_hours": 0,                # Shadow TTL (hours); 0 = disabled
        "watchlist_shadow_validate": True,              # Validate shadow on every run
        "watchlist_page_size": 200,                     # GET page size for /watchlist/items
        "watchlist_batch_size": 100,                    # Batch size for add/remove writes
        "watchlist_freeze_details": True,               # Store extra details for "not_found" freezes

        # Ratings
        "ratings_per_page": 200,                        # Items per page when indexing
        "ratings_max_pages": 50,                        # Max pages to fetch (safety cap)
        "ratings_chunk_size": 25,                       # Batch size for POST/REMOVE
        "ratings_write_delay_ms": 600,                  # Optional pacing between writes
        "ratings_max_backoff_ms": 8000,                 # Max backoff time for retries
        "ratings_since": "1970-01-01T00:00:00Z",        # First-run baseline; watermark overrides after

        # History
        "history_per_page": 1000,                       # Items per page for /sync/watched delta
        "history_max_pages": 250,                       # Max pages to fetch (safety cap)
        "history_chunk_size": 25,                       # Batch size for watched/unwatched writes
        "history_write_delay_ms": 600,                  # Optional pacing between writes
        "history_max_backoff_ms": 8000,                 # Max backoff time for retries
        "history_since": "1970-01-01T00:00:00Z"         # First-run baseline; watermark overrides after
    },
    
     "tautulli": {
         "server_url": "",                              # http(s)://host:8181
         "api_key": "",                                 # Tautulli API key
         "verify_ssl": False,                           # Verify TLS certificates
         "timeout": 10.0,                               # HTTP timeout (seconds)
         "max_retries": 3,                              # Retry budget
         "history": {
             "user_id": "",                             # Optional user filter
             "per_page": 100,                           # Tautulli history page size
             "max_pages": 5000                          # Safety cap
         },
     },

    "trakt": {
        "client_id": "",                                # From your Trakt app
        "client_secret": "",                            # From your Trakt app
        "access_token": "",                             # OAuth2 access token
        "refresh_token": "",                            # OAuth2 refresh token
        "scope": "public",                              # OAuth2 scope (usually "public" or "private")
        "token_type": "Bearer",                         # OAuth2 token type (usually "Bearer")
        "expires_at": 0,                                # Epoch when access_token expires

        "timeout": 10,                                  # HTTP timeout (seconds)
        "max_retries": 5,                               # Retry budget for API calls (429/5xx backoff)

        # Watchlist
        "watchlist_use_etag": True,                     # Use ETag + local shadow to skip unchanged lists
        "watchlist_shadow_ttl_hours": 168,              # Refresh ETag baseline weekly even if 304s keep coming
        "watchlist_batch_size": 100,                    # Chunk size for add/remove to avoid 429/rate spikes
        "watchlist_log_rate_limits": True,              # Log X-RateLimit-* and Retry-After when present
        "watchlist_freeze_details": True,               # Persist last status & ids in freeze store for debugging

        # Ratings
        "ratings_per_page": 100,                        # Items per page when indexing (10–100; clamped to 100)
        "ratings_max_pages": 50,                        # Max pages per type; raise if you have >2k ratings/type
        "ratings_chunk_size": 100,                      # Batch size for POST/REMOVE

        # History
        "history_per_page": 100,                        # Max allowed by Trakt; fastest without spamming
        "history_max_pages": 10000,                     # Safety cap for huge libraries; lower to bound runtime
        "history_unresolved": False,                    # bool, default false (enable the freeze file)
        "history_number_fallback": False,               # episode number fallback (no S/E-based resolution when episode IDs are missing)
        "history_collection": False,                    # mirroring history adds into your Trakt Collection

        "_pending_device": {
            "user_code": "",                            # Temporary device code state for PIN login
            "device_code": "",                          # Temporary device code state for PIN login
            "verification_url": "https://trakt.tv/activate",
            "interval": 5,                              # Polling interval (seconds)
            "expires_at": 0,                            # Epoch when device_code expires
            "created_at": 0,                            # Epoch when device_code was created
        },
    },

    "tmdb": {"api_key": ""},

    "jellyfin": {
        "server": "",                                   # http(s)://host:port (required)
        "access_token": "",                             # Jellyfin access token (required)
        "user_id": "",                                  # Jellyfin userId (required)
        "device_id": "crosswatch",                      # Client device id
        "username": "",                                 # Optional (login username)
        "user": "",                                     # Optional (display name; hydrated after auth)
        "verify_ssl": False,                            # Verify TLS certificates
        "timeout": 15.0,                                # HTTP timeout (seconds)
        "max_retries": 3,                               # Retry budget for API calls

        "scrobble": {
            "libraries": []                             # whitelist of library GUIDs; empty = all
        },

        # Watchlist settings
        "watchlist": {
            "mode": "favorites",                        # "favorites" | "playlist" | "collections"
            "playlist_name": "Watchlist",               # used when mode == "playlist"
            "watchlist_query_limit": 25,                # batch size
            "watchlist_write_delay_ms": 0,              # delay between writes
            "watchlist_guid_priority": [                # id match order
                "tmdb", "imdb", "tvdb",
                "agent:themoviedb:en", "agent:themoviedb", "agent:imdb"
            ]
        },

        # History settings
        "history": {
            "history_query_limit": 25,                  # batch size
            "history_write_delay_ms": 0,                # delay between writes
            "history_guid_priority": [                  # id match order
                "tmdb", "imdb", "tvdb",
                "agent:themoviedb:en", "agent:themoviedb", "agent:imdb"
            ],
            "libraries": []                             # whitelist of library GUIDs (from /api/jellyfin/libraries.key); empty = all
        },

        # Ratings settings
        "ratings": {
            "ratings_query_limit": 2000,                # ratings query limit, default 2000
            "libraries": []                             # whitelist of library GUIDs; empty = all
        },
    },

    "emby": {
        "server": "",                                   # http(s)://host:port (required)
        "access_token": "",                             # Emby access token (required)
        "user_id": "",                                  # Emby userId (required)
        "device_id": "crosswatch",                      # Client device id
        "username": "",                                 # Optional (login username)
        "user": "",                                     # Optional (display name; hydrated after auth)
        "verify_ssl": False,                            # Verify TLS certificates
        "timeout": 15.0,                                # HTTP timeout (seconds)
        "max_retries": 3,                               # Retry budget for API calls

        "scrobble": {
            "libraries": []                             # whitelist of library GUIDs; empty = all
        },

        # Watchlist settings
        "watchlist": {
            "mode": "favorites",                        # "favorites" | "playlist" | "collections"
            "playlist_name": "Watchlist",               # used when mode == "playlist"
            "watchlist_query_limit": 25,                # batch size
            "watchlist_write_delay_ms": 0,              # delay between writes
            "watchlist_guid_priority": [                # id match order
                "tmdb", "imdb", "tvdb",
                "agent:themoviedb:en", "agent:themoviedb", "agent:imdb"
            ]
        },

        # History settings
        "history": {
            "history_query_limit": 25,                  # batch size
            "history_write_delay_ms": 0,                # delay between writes
            "history_guid_priority": [                  # id match order
                "tmdb", "imdb", "tvdb",
                "agent:themoviedb:en", "agent:themoviedb", "agent:imdb"
            ],
            "libraries": []                             # whitelist of library GUIDs (from /api/emby/libraries.key); empty = all
        },

        # Ratings settings
        "ratings": {
            "ratings_query_limit": 2000,                # ratings query limit, default 2000
            "libraries": []                             # whitelist of library GUIDs; empty = all
        },
    },

    "crosswatch": {
        "root_dir":         "/config/.cw_provider",    # Root folder for local provider state
        "enabled":          True,                      # Enable/disable CrossWatch as sync provider
        "retention_days":   30,                        # Snapshot retention in days; 0 = keep forever
        "auto_snapshot":    True,                      # Take snapshot before mutating main JSONs
        "max_snapshots":    64,                        # Max snapshots per feature; 0 = unlimited
        "restore_watchlist": "latest",                 # "", "latest", or specific snapshot name/stem
        "restore_history": "latest",                   # "", "latest", or specific snapshot name/stem
        "restore_ratings": "latest"                    # "", "latest", or specific snapshot name/stem
    },

    # --- Sync / Orchestrator -------------------------------------------------
    "sync": {
        # Global write gates (pair/feature settings will override these by design):
        "enable_add": True,                             # Allow additions by default
        "enable_remove": False,                         # Safer default: do not remove items unless explicitly enabled

        # Execution behavior:
        "verify_after_write": False,                    # When supported, re-check destination after writes
        "dry_run": False,                               # Plan and log only; do not perform writes
        "drop_guard": False,                            # Guard against sudden inventory shrink (protects from bad/suspect snapshots)
        "allow_mass_delete": True,                      # If False, block large delete plans (e.g., >~10% of baseline)
        "tombstone_ttl_days": 1,                        # How long “observed deletes” (tombstones) stay valid
        "include_observed_deletes": True,               # If False, skip processing “observed deletes” for this run. Delta-trackers (SIMKL) will be turned off to prevent accidental removals

        # Optional high-level two-way defaults (pairs always remain the source of truth for mode):
        "bidirectional": {
            "enabled": False,
            "mode": "two-way",                          # Placeholder default; pairs decide final mode per connection
            "source_of_truth": "",                      # Optional: pick one side as tie-breaker if you enforce strict authority
        },

        # Blackbox (including flapper protection)
        "blackbox": {
            "enabled": True,                            # Turn off to fully disable blackbox logic
            "promote_after": 1,                         # Promote an item to blackbox after N consecutive unresolved/fail events
            "unresolved_days": 0,                       # Minimum unresolved age (days) before it counts (0 = immediate)
            "pair_scoped": True,                        # Track per source-target pair to avoid blocking the same title elsewhere
            "cooldown_days": 30,                        # Auto-prune/decay blackbox entries after this cooldown period
            "block_adds": True,                         # When blackboxed, block planned ADDs for that item
            "block_removes": True,                      # When blackboxed, block planned REMOVEs for that item
        },
    },

    # --- Runtime / Diagnostics ----------------------------------------------
    "runtime": {
        "debug": False,                                 # Extra verbose logging (debug level)
        "debug_http": False,                            # Extra verbose HTTP logging (uvicorn access log)
        "debug_mods": False,                            # Extra verbode MODS logging for Synchronization Providers
        "state_dir": "",                                # Optional override for state dir (defaults to CONFIG/state)  - this will break container setups!
        "telemetry": {"enabled": True},                 # Usage stats

        # progress + stability knobs
        "snapshot_ttl_sec": 300,                        # Reuse snapshots within 5 min
        "apply_chunk_size": 100,                        # Sweet spot for apply chunking
        "apply_chunk_pause_ms": 50,                     # Small pause between chunks

        # suspect guard (shrinking inventories protection)
        "suspect_min_prev": 20,                         # Minimum previous size to enable suspect guard
        "suspect_shrink_ratio": 0.10,                   # Shrink ratio to trigger suspect guard
    },

    # --- Metadata (TMDb resolver) -------------------------------------------
    "metadata": {
        "locale": "en-US",                              # e.g. "en-US" / "nl-NL"
        "ttl_hours": 6,                                 # Coarse cache TTL
    },

    # --- Scrobble ------------------------------------------------------------
    "scrobble": {
        "enabled": False,                               # Master toggle
        "mode": "watch",                                # "watch" | "webhook"
        "delete_plex": False,                           # Old name but still valid. Auto-remove movies from all your Watchlists, for all media servers
        "delete_plex_types": ["movie"],                 # Old name but still valid. Movie/show/episode

        # Watcher settings
        "watch": {
            "autostart": False,                         # Start watcher on boot if enabled+mode=watch
            "provider": "plex",                         # Active watcher either "plex|emby|Jellyfin" (default: "plex")
            "sink": "",                                 # "trakt" | "simkl" | "mdblist"
            "plex_simkl_ratings": False,                # Watch mode: forward Plex ratings to SIMKL
            "plex_trakt_ratings": False,                # Watch mode: forward Plex ratings to Trakt
            "plex_mdblist_ratings": False,              # Watch mode: forward Plex ratings to MDblist
            "pause_debounce_seconds": 5,                # Ignore micro-pauses just after start
            "suppress_start_at": 99,                    # Kill near-end "start" flaps (credits)
            "filters": {
                "username_whitelist": [],               # ["name", "id:123", "uuid:abcd…"]
                "server_uuid": ""                       # Restrict to a specific server
            }
        },

        # Webhook settings
        "webhook": {
            "pause_debounce_seconds": 5,                # Ignore micro-pauses
            "suppress_start_at": 99,                    # Suppress near-end "start" flaps (credits)
            "suppress_autoplay_seconds": 15,            # Plex autoplay when set on 10 sec (increase a few sec)
            "probe_session_progress": True,             # Call GET /status/sessions on your Plex server and match the item by ratingKey/sessionKey
            "plex_trakt_ratings": False,                # Watch mode: forward Plex ratings to Trakt
            # Plex-only filters
            "filters_plex": {
                "username_whitelist": [],               # Restrict accepted Account.title values (empty = allow all)
                "server_uuid": ""                       # Restrict to a specific server
            }
        },

        # Trakt sink rules (progress decisions) used by Trakt|SIMKL|MDblist
        "trakt": {
            "progress_step": 5,                         # Send scrobble progress in % steps (1=every %, 5=every 5%)
            "stop_pause_threshold": 85,                 # <85% STOP-send as PAUSE (your “watched” bar)
            "force_stop_at": 95,                        # ≥85% always STOP (bypass debounces)
            "regress_tolerance_percent": 5,             # Small progress regress is tolerated
        }
    },

    # --- Scheduling ----------------------------------------------------------
    "scheduling": {
        "enabled": False,                               # Master toggle for periodic runs
        "mode": "hourly",                               # "hourly" or "daily"
        "every_n_hours": 2,                             # When mode=hourly, run every N hours (1–12)
        "daily_time": "03:30",                          # When mode=daily, run at this time (HH:MM, 24h)
    },

    # --- User Interface ------------------------------------------------------
    "ui": {
        "show_watchlist_preview": True,                 # Show Watchlist Preview card on Main tab
        "show_playingcard": True,                       # Show Now Playing card on Main tab
    },

    # --- Pairs (UI-driven) ---------------------------------------------------
    "pairs": [],
}


# Helpers: paths, IO, merging, normalization
def _cfg_file() -> Path:
    return CONFIG / "config.json"


def config_path() -> Path:
    return _cfg_file()


def _read_json(p: Path) -> dict[str, Any]:
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json_atomic(p: Path, data: dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    import os as _os, time as _time, secrets, threading

    suffix = f".{_time.time_ns()}.{_os.getpid()}.{threading.get_ident()}.{secrets.token_hex(4)}.tmp"
    tmp = p.with_suffix(suffix)

    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.write("\n")
    tmp.replace(p)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)  # type: ignore[assignment]
        else:
            out[k] = v
    return out


# Feature normalization
_ALLOWED_RATING_TYPES: list[str] = ["movies", "shows", "seasons", "episodes"]
_ALLOWED_RATING_MODES: list[str] = ["only_new", "from_date", "all"]


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Iterable):
        return [str(x) for x in value if isinstance(x, (str, int, float))]
    return []


def _normalize_ratings_feature(val: dict[str, Any]) -> dict[str, Any]:
    v: dict[str, Any] = dict(val or {})
    v["enable"] = bool(v.get("enable", False))
    v["add"] = bool(v.get("add", False))
    v["remove"] = bool(v.get("remove", False))

    # types
    raw_types = _as_list(v.get("types"))
    types = [str(t).strip().lower() for t in raw_types]
    if "all" in types:
        types = list(_ALLOWED_RATING_TYPES)
    else:
        types = [t for t in _ALLOWED_RATING_TYPES if t in types]
        if not types:
            types = ["movies", "shows"]
    v["types"] = types

    mode = str(v.get("mode", "only_new")).strip().lower()
    if mode not in _ALLOWED_RATING_MODES:
        mode = "only_new"
    v["mode"] = mode

    from_date = str(v.get("from_date", "") or "").strip()
    if mode != "from_date":
        from_date = ""
    v["from_date"] = from_date

    return v


def _normalize_features_map(features: dict[str, Any] | None) -> dict[str, Any]:
    f: dict[str, Any] = dict(features or {})
    for name, val in list(f.items()):
        if isinstance(val, bool):
            f[name] = {"enable": bool(val), "add": bool(val), "remove": False}
            continue

        if isinstance(val, dict):
            v: dict[str, Any] = dict(val)
            v.setdefault("enable", True)
            v.setdefault("add", True)
            v.setdefault("remove", False)

            # Ratings has extra fields - normalize carefully
            if name == "ratings":
                v = _normalize_ratings_feature(v)
            f[name] = v
            continue

        # Unknown
        f[name] = {"enable": False, "add": False, "remove": False}
    return f


# Public API
def load_config() -> dict[str, Any]:
    p = _cfg_file()
    user_cfg: dict[str, Any] = {}
    if p.exists():
        try:
            user_cfg = _read_json(p)
        except Exception:
            user_cfg = {}

    cfg = _deep_merge(DEFAULT_CFG, user_cfg)
    pairs = cfg.get("pairs")
    if isinstance(pairs, list):
        for it in pairs:
            if isinstance(it, dict):
                it["features"] = _normalize_features_map(it.get("features"))  # type: ignore[arg-type]
    return cfg


def save_config(cfg: dict[str, Any]) -> None:
    data: dict[str, Any] = dict(cfg or {})
    pairs = data.get("pairs")
    if isinstance(pairs, list):
        for it in pairs:
            if isinstance(it, dict):
                it["features"] = _normalize_features_map(it.get("features"))  # type: ignore[arg-type]

    _write_json_atomic(_cfg_file(), data)
