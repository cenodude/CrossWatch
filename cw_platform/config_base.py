# cw_platform/config_base.py
# configuration management base.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
import os
import secrets
import base64
import hashlib
from datetime import datetime
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast


def _current_version_norm() -> str:
    try:
        from api.versionAPI import CURRENT_VERSION as _V
        raw = str(_V)
    except Exception:
        raw = (os.getenv("APP_VERSION") or "v0.7.0").strip()
    return raw[1:] if raw.lower().startswith("v") else raw


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

_ENC_PREFIX = "enc:v1:"

def _config_key_file() -> Path:
    return CONFIG / ".cw_master_key"

def _normalize_fernet_key(raw: str | bytes) -> bytes:
    data = raw.encode("utf-8") if isinstance(raw, str) else bytes(raw)
    data = data.strip()
    if not data:
        raise ValueError("Empty config key")

    try:
        decoded = base64.urlsafe_b64decode(data)
        if len(decoded) == 32:
            return data
    except Exception:
        pass

    return base64.urlsafe_b64encode(hashlib.sha256(data).digest())


def _load_config_key(*, create: bool) -> bytes | None:
    for env_key in ("CW_CONFIG_KEY", "CROSSWATCH_CONFIG_KEY"):
        raw = (os.getenv(env_key) or "").strip()
        if raw:
            return _normalize_fernet_key(raw)

    key_path = _config_key_file()
    if key_path.exists():
        return _normalize_fernet_key(key_path.read_text(encoding="utf-8"))

    if not create:
        return None

    try:
        from cryptography.fernet import Fernet
    except Exception as e:
        raise RuntimeError("Missing dependency: cryptography is required for encrypted config support") from e

    key = Fernet.generate_key()
    key_path.parent.mkdir(parents=True, exist_ok=True)
    key_path.write_text(key.decode("ascii"), encoding="utf-8")
    try:
        os.chmod(key_path, 0o600)
    except Exception:
        pass
    return key


def _get_cipher(*, create: bool):
    key = _load_config_key(create=create)
    if not key:
        return None
    try:
        from cryptography.fernet import Fernet
    except Exception as e:
        raise RuntimeError("Missing dependency: cryptography is required for encrypted config support") from e
    return Fernet(key)


def _encrypt_secret(value: str) -> str:
    s = str(value or "")
    if not s or s.startswith(_ENC_PREFIX):
        return s

    cipher = _get_cipher(create=True)
    if cipher is None:
        return s

    token = cipher.encrypt(s.encode("utf-8")).decode("ascii")
    return f"{_ENC_PREFIX}{token}"


def _decrypt_secret(value: Any) -> Any:
    if not isinstance(value, str) or not value.startswith(_ENC_PREFIX):
        return value

    cipher = _get_cipher(create=False)
    if cipher is None:
        raise RuntimeError(
            f"Encrypted config detected but no key is available. Expected {_config_key_file()} "
            "or env CW_CONFIG_KEY/CROSSWATCH_CONFIG_KEY"
        )

    token = value[len(_ENC_PREFIX):].strip()
    try:
        return cipher.decrypt(token.encode("ascii")).decode("utf-8")
    except Exception as e:
        raise RuntimeError("Encrypted config detected but decryption failed. Check the config key.") from e


def _is_sensitive_path(path: tuple[str, ...]) -> bool:
    if not path:
        return False

    clean: list[str] = [
        str(part or "").strip().lower()
        for part in path
        if str(part or "").strip()
    ]
    if not clean:
        return False

    if len(clean) >= 2 and clean[0] == "security" and clean[1] == "webhook_ids":
        return True

    leaf = clean[-1]
    exact = {
        "api_key", "apikey",
        "access_token", "refresh_token",
        "client_secret",
        "account_token", "pms_token", "home_pin",
        "session_id",
        "token_hash", "salt", "hash",
        "device_code",
        "_pending_request_token",
        "request_token",
        "token",
        "password",
        "secret",
        "webhook_secret",
    }
    if leaf in exact:
        return True

    if leaf.endswith("_token") and leaf not in {"token_endpoint", "token_url"}:
        return True

    return False


def _transform_secret_tree(obj: Any, *, decrypt: bool, path: tuple[str, ...] = ()) -> Any:
    if isinstance(obj, dict):
        return {k: _transform_secret_tree(v, decrypt=decrypt, path=path + (str(k),)) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_transform_secret_tree(v, decrypt=decrypt, path=path + (str(i),)) for i, v in enumerate(obj)]

    if isinstance(obj, str) and _is_sensitive_path(path):
        return _decrypt_secret(obj) if decrypt else _encrypt_secret(obj)

    return obj

# Default config
DEFAULT_CFG: dict[str, Any] = {
    # --- Providers -----------------------------------------------------------
    "plex": {
        "server_url": "",                               # http(s)://host:32400 (required for sync & watcher).
        "verify_ssl": False,                            # Verify TLS certificates
        "account_token": "",                            # Plex token (a.k.a. authentication token).
        "pms_token": "",                                # PMS resource token for the selected server
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
        "watchlist_use_metadata_match": True,           # Try METADATA /library/metadata/matches with tmdb-/imdb-/tvdb- first; fallback to Discover.
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

        # Rate limits
        "rate_limit": {
            "post_per_sec": 1,
            "get_per_sec": 10,
        },
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

        # Rate limits 
        "rate_limit": {
            "post_per_sec": 1,
            "get_per_sec": 10,
        },

        # Watchlist
        "watchlist_shadow_ttl_hours": 0,                # Shadow TTL (hours); 0 = disabled
        "watchlist_shadow_validate": True,              # Validate shadow on every run
        "watchlist_page_size": 200,                     # GET page size for /watchlist/items
        "watchlist_batch_size": 100,                    # Batch size for add/remove writes
        "watchlist_freeze_details": True,               # Store extra details for "not_found" freezes

        # Ratings
        "ratings_per_page": 200,                        # Items per page when indexing
        "ratings_max_pages": 50,                        # Max pages to fetch (safety cap)
        "ratings_chunk_size": 500,                      # Batch size for POST/REMOVE
        "ratings_write_delay_ms": 600,                  # Optional pacing between writes
        "ratings_max_backoff_ms": 8000,                 # Max backoff time for retries
        "ratings_since": "1900-01-01T00:00:02Z",        # First-run baseline; watermark overrides after

        # History
        "history_per_page": 1000,                       # Items per page for /sync/watched delta
        "history_max_pages": 250,                       # Max pages to fetch (safety cap)
        "history_chunk_size": 500,                      # Batch size for watched/unwatched writes
        "history_write_delay_ms": 600,                  # Optional pacing between writes
        "history_max_backoff_ms": 8000,                 # Max backoff time for retries
        "history_since": "1900-01-01T00:00:02Z"         # First-run baseline; watermark overrides after
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

        # Rate limits
        "rate_limit": {
            "get_per_sec": 3.33,
            "post_per_sec": 1,
        },

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

        "_pending_device": {
            "user_code": "",                            # Temporary device code state for PIN login
            "device_code": "",                          # Temporary device code state for PIN login
            "verification_url": "https://trakt.tv/activate",
            "interval": 5,                              # Polling interval (seconds)
            "expires_at": 0,                            # Epoch when device_code expires
            "created_at": 0,                            # Epoch when device_code was created
        },
    },
"tmdb_sync": {                                          # Tracker / sync adapter auth (TMDb v3)
    "api_key": "",                                      # v3 API Key (required)
    "session_id": "",                                   # v3 Session ID (filled after Connect)
    "account_id": "",                                   # v3 Account ID (auto-filled)
    "_pending_request_token": "",                       # Temporary token waiting for approval
    "_pending_created_at": 0,                           # Epoch when request token was created
    "timeout": 15.0,
    "max_retries": 3,
},
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
    
    # --- Meta Providers ------------------------------------------------------
    
    "tmdb": {"api_key": ""},                            # Metadata resolver (TMDb)

    # --- Sync / Orchestrator -------------------------------------------------
    "sync": {
        # Global write gates (pair/feature settings will override these by design):
        "enable_add": True,                             # Allow additions by default
        "enable_remove": False,                         # Safer default: do not remove items unless explicitly enabled
        "one_way_remove_mode": "source_deletes",        # "source_deletes" | "mirror" (mirror = destructive; use with care)


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

        # progress
        "snapshot_ttl_sec": 300,                        # Reuse snapshots within 5 min
        "apply_chunk_size": 100,                        # Sweet spot for apply chunking
        "apply_chunk_pause_ms": 50,                     # Small pause between chunks
        "apply_chunk_size_by_provider": {               # SIMKL/TRAKT/MDBLIST/ANILIST/TMDB/TAUTULLI/TMDB/PLEX/JELLYFIN/EMBY overrides
            "SIMKL": 500,
            "MDBLIST": 500
        },
        
        # suspect guard (shrinking inventories protection)
        "suspect_min_prev": 20,                         # Minimum previous size to enable suspect guard
        "suspect_shrink_ratio": 0.10,                   # Shrink ratio to trigger suspect guard
    },

    # --- Metadata (TMDb resolver) -------------------------------------------
    "metadata": {
        "locale": "en-US",                              # example: "en-US" / "nl-NL"
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
            "routes": [],                               # Route-based config (preferred); empty = migrate legacy keys
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
            "progress_step": 25,                        # Send scrobble progress in % steps
            "stop_pause_threshold": 80,                 # <80% STOP-send as PAUSE (your “watched” bar)
            "force_stop_at": 95,                        # ≥95% always STOP
            "regress_tolerance_percent": 5,             # Small progress regress is tolerated
        }
    },

    # --- Scheduling ----------------------------------------------------------
    "scheduling": {
        "enabled": False,                               # Standard scheduler master toggle
        "mode": "every_n_hours",                        # "hourly" | "every_n_hours" | "daily_time" | "custom_interval"
        "every_n_hours": 12,                            # When mode=every_n_hours, run every N hours (2+ recommended)
        "daily_time": "03:30",                          # When mode=daily_time, run at this time (HH:MM, 24h)
        "custom_interval_minutes": 60,                  # When mode=custom_interval, run every N minutes (minimum 15)
        "advanced": {
            "enabled": False,                           # Advanced scheduler master toggle
            "jobs": [],
        },
    },

    # --- User Interface ------------------------------------------------------
    "ui": {
        "show_watchlist_preview": True,                 # Show Watchlist Preview card on Main tab
        "show_playingcard": True,                       # Show Now Playing card on Main tab
        "show_AI": True,                                # Show ASK AI from GitBook
        "protocol": "http",                             # "http" | "https" (HTTPS uses a self-signed cert by default)
        "tls": {
            "self_signed": True,                        # Auto-generate a self-signed certificate when missing
            "hostname": "localhost",                    # Used for certificate CN/SAN
            "valid_days": 825,                          # Certificate validity (days)
            "cert_file": "",                            # Optional override path to a PEM cert
            "key_file": "",                             # Optional override path to a PEM key
        },
    },

    # --- Local UI Authentication --------------------------------------------
    "app_auth": {
        "enabled": False,
        "username": "",
        "reset_required": False,
        "remember_session_enabled": False,
        "remember_session_days": 30,
        "password": {
            "scheme": "pbkdf2_sha256",
            "iterations": 260000,
            "salt": "",
            "hash": "",
        },
        "session": {
            "token_hash": "",
            "expires_at": 0,
        },
        "sessions": [],
        "last_login_at": 0,
    },

    # --- Pairs (UI-driven) ---------------------------------------------------
    "pairs": [],
}


def redact_config(cfg: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = copy.deepcopy(cfg or {})
    MASK = "••••••••"

    # Provider-specific secret fields
    provider_secret_keys: dict[str, set[str]] = {
        "plex": {"account_token", "pms_token", "home_pin", "webhook_secret"},
        "simkl": {"access_token", "refresh_token", "client_secret"},
        "anilist": {"access_token", "client_secret"},
        "mdblist": {"api_key"},
        "tautulli": {"api_key"},
        "trakt": {"access_token", "refresh_token", "client_secret"},
        "jellyfin": {"access_token", "api_key", "password"},
        "emby": {"access_token", "api_key", "password"},
        "tmdb": {"api_key"},
        "tmdb_sync": {"api_key", "session_id", "_pending_request_token"},
    }

    def _mask_leaf(d: dict[str, Any], key: str) -> None:
        v = d.get(key)
        if v is None:
            return
        s = str(v).strip()
        if not s or s == MASK:
            return
        d[key] = MASK

    for provider, keys in provider_secret_keys.items():
        blk = out.get(provider)
        if not isinstance(blk, dict):
            continue

        for k in keys:
            _mask_leaf(blk, k)

        insts = blk.get("instances")
        if isinstance(insts, dict):
            for inst in insts.values():
                if isinstance(inst, dict):
                    for k in keys:
                        _mask_leaf(inst, k)

    # App UI auth secrets.
    a = out.get("app_auth")
    if isinstance(a, dict):
        pwd = a.get("password")
        if isinstance(pwd, dict):
            if pwd.get("hash"):
                pwd["hash"] = MASK
            if pwd.get("salt"):
                pwd["salt"] = MASK

        sess = a.get("session")
        if isinstance(sess, dict) and sess.get("token_hash"):
            sess["token_hash"] = MASK

        sessions = a.get("sessions")
        if isinstance(sessions, list):
            for s in sessions:
                if isinstance(s, dict) and s.get("token_hash"):
                    s["token_hash"] = MASK

    # Webhook URL tokens
    sec = out.get("security")
    if isinstance(sec, dict):
        wh = sec.get("webhook_ids")
        if isinstance(wh, dict):
            for k in list(wh.keys()):
                _mask_leaf(wh, k)

    return out


# Helpers: paths, IO, merging, normalization
def _cfg_file() -> Path:
    return CONFIG / "config.json"


def config_path() -> Path:
    return _cfg_file()


def backup_config_file() -> Path | None:
    src = _cfg_file()
    if not src.exists():
        return None

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    dst = src.with_name(f"{src.name}.backup_{ts}")
    i = 1
    while dst.exists():
        dst = src.with_name(f"{src.name}.backup_{ts}_{i}")
        i += 1

    import shutil

    shutil.copy2(src, dst)
    return dst


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

def _path_parts(path: str | Iterable[str]) -> list[str]:
    if isinstance(path, str):
        return [part.strip() for part in path.split(".") if str(part).strip()]
    return [str(part).strip() for part in path if str(part).strip()]


def _get_nested_value(src: dict[str, Any], path: str | Iterable[str]) -> tuple[bool, Any]:
    parts = _path_parts(path)
    if not parts:
        return False, None

    cur: Any = src
    for part in parts:
        if not isinstance(cur, dict) or part not in cur:
            return False, None
        cur = cur[part]
    return True, cur


def _set_nested_value(dst: dict[str, Any], path: str | Iterable[str], value: Any) -> None:
    parts = _path_parts(path)
    if not parts:
        return

    cur: dict[str, Any] = dst
    for part in parts[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            cur[part] = nxt
        cur = cast(dict[str, Any], nxt)

    cur[parts[-1]] = copy.deepcopy(value)


def apply_default_overrides(
    cfg: dict[str, Any],
    override_keys: Iterable[str],
) -> tuple[dict[str, Any], list[str]]:
    data = copy.deepcopy(dict(cfg or {}))
    applied: list[str] = []

    for key in override_keys:
        parts = _path_parts(key)
        if not parts:
            continue

        found, value = _get_nested_value(DEFAULT_CFG, parts)
        if not found:
            continue

        dotted = ".".join(parts)
        _set_nested_value(data, parts, value)
        applied.append(dotted)

    return data, applied

def apply_migration_overrides(cfg: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    try:
        from .config_overrides import MIGRATION_OVERRIDE_KEYS
    except Exception:
        MIGRATION_OVERRIDE_KEYS = ()

    return apply_default_overrides(cfg, MIGRATION_OVERRIDE_KEYS)


# Feature normalization
_ALLOWED_RATING_TYPES: list[str] = ["movies", "shows", "seasons", "episodes"]
_ALLOWED_RATING_MODES: list[str] = ["only_new", "from_date", "all"]
_ALLOWED_UI_PROTOCOLS: list[str] = ["http", "https"]


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

            # Ratings has extra fields
            if name == "ratings":
                v = _normalize_ratings_feature(v)
            f[name] = v
            continue

        # Unknown
        f[name] = {"enable": False, "add": False, "remove": False}
    return f


def _ensure_dict(parent: dict[str, Any], key: str) -> dict[str, Any]:
    v = parent.get(key)
    if isinstance(v, dict):
        return cast(dict[str, Any], v)
    d: dict[str, Any] = {}
    parent[key] = d
    return d


def _normalize_tmdb_sync(cfg: dict[str, Any]) -> None:
    t0 = cfg.get("tmdb_sync")
    if isinstance(t0, dict):
        t = t0
    else:
        t = {}
        cfg["tmdb_sync"] = t

    # TMDb sync v3 config
    t["api_key"] = str(t.get("api_key") or "").strip()
    t["session_id"] = str(t.get("session_id") or "").strip()
    t["account_id"] = str(t.get("account_id") or "").strip()
    t["_pending_request_token"] = str(t.get("_pending_request_token") or "").strip()
    try:
        t["_pending_created_at"] = int(t.get("_pending_created_at") or 0)
    except Exception:
        t["_pending_created_at"] = 0

    try:
        t["timeout"] = float(t.get("timeout", 15.0) or 15.0)
    except Exception:
        t["timeout"] = 15.0
    try:
        t["max_retries"] = int(t.get("max_retries", 3) or 3)
    except Exception:
        t["max_retries"] = 3

    # Guard: if api_key is empty, clear derived/session state
    if not t["api_key"]:
        t["session_id"] = ""
        t["account_id"] = ""
        t["_pending_request_token"] = ""
        t["_pending_created_at"] = 0


def _normalize_trakt(cfg: dict[str, Any]) -> None:
    t0 = cfg.get("trakt")
    if isinstance(t0, dict):
        t = t0
    else:
        t = {}
        cfg["trakt"] = t

    rl0 = t.get("rate_limit")
    if isinstance(rl0, dict):
        rl = rl0
    else:
        rl = {}
        t["rate_limit"] = rl

    def _rate(name: str, default: float, *, max_v: float = 1000.0) -> float:
        v = rl.get(name, default)
        try:
            f = float(v)
        except Exception:
            f = float(default)
        if f < 0:
            f = 0.0
        if f > max_v:
            f = max_v
        return f

    # Allow 0 to disable throttling.
    post_rps = _rate("post_per_sec", 1.0)
    get_rps = _rate("get_per_sec", 3.33)
    rl["post_per_sec"] = int(post_rps) if float(post_rps).is_integer() else float(post_rps)
    rl["get_per_sec"] = int(get_rps) if float(get_rps).is_integer() else float(get_rps)


def _normalize_simkl(cfg: dict[str, Any]) -> None:
    s0 = cfg.get("simkl")
    if isinstance(s0, dict):
        s = s0
    else:
        s = {}
        cfg["simkl"] = s

    rl0 = s.get("rate_limit")
    if isinstance(rl0, dict):
        rl = rl0
    else:
        rl = {}
        s["rate_limit"] = rl

    def _rate(name: str, default: float, *, max_v: float = 1000.0) -> float:
        v = rl.get(name, default)
        try:
            f = float(v)
        except Exception:
            f = float(default)
        if f < 0:
            f = 0.0
        if f > max_v:
            f = max_v
        return f

    # Allow 0 to disable throttling.
    post_rps = _rate("post_per_sec", 1.0)
    get_rps = _rate("get_per_sec", 10.0)
    rl["post_per_sec"] = int(post_rps) if float(post_rps).is_integer() else float(post_rps)
    rl["get_per_sec"] = int(get_rps) if float(get_rps).is_integer() else float(get_rps)


def _normalize_mdblist(cfg: dict[str, Any]) -> None:
    m0 = cfg.get("mdblist")
    if isinstance(m0, dict):
        m = m0
    else:
        m = {}
        cfg["mdblist"] = m

    rl0 = m.get("rate_limit")
    if isinstance(rl0, dict):
        rl = rl0
    else:
        rl = {}
        m["rate_limit"] = rl

    def _rate(name: str, default: float, *, max_v: float = 1000.0) -> float:
        v = rl.get(name, default)
        try:
            f = float(v)
        except Exception:
            f = float(default)
        if f < 0:
            f = 0.0
        if f > max_v:
            f = max_v
        return f

    # Allow 0 to disable throttling.
    post_rps = _rate("post_per_sec", 1.0)
    get_rps = _rate("get_per_sec", 10.0)
    rl["post_per_sec"] = int(post_rps) if float(post_rps).is_integer() else float(post_rps)
    rl["get_per_sec"] = int(get_rps) if float(get_rps).is_integer() else float(get_rps)

def _is_hhmm(v: str) -> bool:
    s = (v or "").strip()
    if len(s) != 5 or s[2] != ":":
        return False
    hh, mm = s[:2], s[3:]
    if not hh.isdigit() or not mm.isdigit():
        return False
    try:
        h = int(hh)
        m = int(mm)
    except Exception:
        return False
    return 0 <= h <= 23 and 0 <= m <= 59


def _normalize_scheduling(cfg: dict[str, Any]) -> None:
    s = _ensure_dict(cfg, "scheduling")
    s["enabled"] = bool(s.get("enabled", False))

    mode_raw = str(s.get("mode", "every_n_hours") or "every_n_hours").strip().lower()
    if mode_raw in {"disabled", "off", "none"}:
        mode = "disabled"
    elif mode_raw in {"hourly", "every_hour"}:
        mode = "hourly"
        s["every_n_hours"] = 1
    elif mode_raw in {"daily", "daily_at", "daily_time"}:
        mode = "daily_time"
    elif mode_raw in {"custom", "custom_interval", "custom_minutes", "interval"}:
        mode = "custom_interval"
    elif mode_raw == "every_n_hours":
        mode = "every_n_hours"
    else:
        mode = "every_n_hours"
    s["mode"] = mode

    try:
        n = int(s.get("every_n_hours", 2) or 2)
    except Exception:
        n = 2
    if n < 1:
        n = 1
    if mode == "every_n_hours" and n <= 1:
        mode = "hourly"
        s["mode"] = mode
    s["every_n_hours"] = n

    t = str(s.get("daily_time", "03:30") or "03:30").strip()
    if not _is_hhmm(t):
        t = "03:30"
    s["daily_time"] = t

    try:
        custom_minutes = int(s.get("custom_interval_minutes", s.get("custom_minutes", 60)) or 60)
    except Exception:
        custom_minutes = 60
    if custom_minutes < 15:
        custom_minutes = 15
    s["custom_interval_minutes"] = custom_minutes

    adv = _ensure_dict(s, "advanced")
    adv["enabled"] = bool(adv.get("enabled", False))

    jobs0 = adv.get("jobs")
    jobs: list[dict[str, Any]] = []
    if isinstance(jobs0, list):
        for it in jobs0:
            if isinstance(it, dict):
                jobs.append(dict(it))

    out: list[dict[str, Any]] = []
    for i, j in enumerate(jobs):
        jid = str(j.get("id") or "").strip()
        if not jid:
            jid = f"job_{i+1}"
        j["id"] = jid

        pair_id = j.get("pair_id")
        if pair_id is None:
            j["pair_id"] = None
        else:
            s_pair = str(pair_id).strip()
            j["pair_id"] = s_pair or None

        at = str(j.get("at") or "").strip()
        if at and not _is_hhmm(at):
            at = ""
        j["at"] = at or None

        after = j.get("after")
        if after is None:
            j["after"] = None
        else:
            a = str(after).strip()
            if a and not _is_hhmm(a):
                a = ""
            j["after"] = a or None

        days0 = j.get("days")
        days: list[int] = []
        if isinstance(days0, list):
            for d in days0:
                try:
                    di = int(d)
                except Exception:
                    continue
                if di < 1 or di > 7:
                    continue
                if di not in days:
                    days.append(di)
        j["days"] = days

        j["active"] = bool(j.get("active", True))
        out.append(j)

    adv["jobs"] = out


def _normalize_ui(cfg: dict[str, Any]) -> None:
    ui = _ensure_dict(cfg, "ui")

    ui["show_watchlist_preview"] = bool(ui.get("show_watchlist_preview", True))
    ui["show_playingcard"] = bool(ui.get("show_playingcard", True))
    ui["show_AI"] = bool(ui.get("show_AI", True))

    protocol = str(ui.get("protocol", "http") or "http").strip().lower()
    if protocol not in _ALLOWED_UI_PROTOCOLS:
        protocol = "http"
    ui["protocol"] = protocol

    tls = _ensure_dict(ui, "tls")
    tls["self_signed"] = bool(tls.get("self_signed", True))

    hostname = str(tls.get("hostname", "localhost") or "localhost").strip()
    tls["hostname"] = hostname or "localhost"

    try:
        valid_days = int(tls.get("valid_days", 825) or 825)
    except Exception:
        valid_days = 825
    if valid_days < 1:
        valid_days = 1
    if valid_days > 3650:
        valid_days = 3650
    tls["valid_days"] = valid_days

    tls["cert_file"] = str(tls.get("cert_file", "") or "").strip()
    tls["key_file"] = str(tls.get("key_file", "") or "").strip()


def _normalize_app_auth(cfg: dict[str, Any]) -> None:
    a = _ensure_dict(cfg, "app_auth")
    raw_enabled = bool(a.get("enabled", False))
    a["username"] = str(a.get("username", "") or "").strip()
    a["reset_required"] = bool(a.get("reset_required", False))
    a["remember_session_enabled"] = bool(a.get("remember_session_enabled", False))

    try:
        remember_days = int(a.get("remember_session_days", 30) or 30)
    except Exception:
        remember_days = 30
    if remember_days < 1:
        remember_days = 1
    if remember_days > 365:
        remember_days = 365
    a["remember_session_days"] = remember_days

    plex_sso = _ensure_dict(a, "plex_sso")
    plex_sso["enabled"] = bool(plex_sso.get("enabled", False))
    plex_sso["client_id"] = str(plex_sso.get("client_id", "") or "").strip()
    plex_sso["linked_plex_account_id"] = str(plex_sso.get("linked_plex_account_id", "") or "").strip()
    plex_sso["linked_username"] = str(plex_sso.get("linked_username", "") or "").strip()
    plex_sso["linked_email"] = str(plex_sso.get("linked_email", "") or "").strip()
    plex_sso["linked_thumb"] = str(plex_sso.get("linked_thumb", "") or "").strip()
    try:
        plex_sso["linked_at"] = int(plex_sso.get("linked_at", 0) or 0)
    except Exception:
        plex_sso["linked_at"] = 0
    if not plex_sso["linked_plex_account_id"]:
        plex_sso["enabled"] = False
        plex_sso["linked_username"] = ""
        plex_sso["linked_email"] = ""
        plex_sso["linked_thumb"] = ""
        plex_sso["linked_at"] = 0

    pwd = _ensure_dict(a, "password")
    pwd["scheme"] = str(pwd.get("scheme", "pbkdf2_sha256") or "pbkdf2_sha256").strip() or "pbkdf2_sha256"
    try:
        pwd["iterations"] = int(pwd.get("iterations", 260_000) or 260_000)
    except Exception:
        pwd["iterations"] = 260_000
    pwd["salt"] = str(pwd.get("salt", "") or "").strip()
    pwd["hash"] = str(pwd.get("hash", "") or "").strip()

    has_configured_credentials = bool(a["username"] and pwd["salt"] and pwd["hash"])

    # Auth is mandatory
    if not raw_enabled and has_configured_credentials:
        a["reset_required"] = True
        a["remember_session_enabled"] = bool(a.get("remember_session_enabled", False))

    a["enabled"] = True

    sess = _ensure_dict(a, "session")
    sess["token_hash"] = str(sess.get("token_hash", "") or "").strip()
    try:
        sess["expires_at"] = int(sess.get("expires_at", 0) or 0)
    except Exception:
        sess["expires_at"] = 0

    sessions = a.get("sessions")
    a["sessions"] = sessions if isinstance(sessions, list) else []
    try:
        a["last_login_at"] = int(a.get("last_login_at", 0) or 0)
    except Exception:
        a["last_login_at"] = 0

    if a["reset_required"]:
        sess = _ensure_dict(a, "session")
        sess["token_hash"] = ""
        sess["expires_at"] = 0
        a["sessions"] = []
        a["last_login_at"] = 0


# Public API
def _new_webhook_id() -> str:
    return secrets.token_urlsafe(24)


def _ensure_webhook_ids(cfg: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    sec = cfg.setdefault("security", {})
    if not isinstance(sec, dict):
        cfg["security"] = {}
        sec = cfg["security"]
    wh = sec.setdefault("webhook_ids", {})
    if not isinstance(wh, dict):
        sec["webhook_ids"] = {}
        wh = sec["webhook_ids"]

    changed = False
    for k in ("plextrakt", "jellyfintrakt", "embytrakt", "plexwatcher"):
        v = wh.get(k)
        if not isinstance(v, str) or len(v.strip()) < 16:
            wh[k] = _new_webhook_id()
            changed = True

    return cfg, changed

def load_config() -> dict[str, Any]:
    p = _cfg_file()
    first_run = not p.exists()
    user_cfg: dict[str, Any] = {}
    if p.exists():
        try:
            user_cfg = _transform_secret_tree(_read_json(p), decrypt=True)
        except Exception as e:
            raise RuntimeError(f"Invalid config file: {p}") from e

    cfg = _deep_merge(DEFAULT_CFG, user_cfg)
    cfg.setdefault("version", _current_version_norm())
    _normalize_tmdb_sync(cfg)
    _normalize_trakt(cfg)
    _normalize_simkl(cfg)
    _normalize_mdblist(cfg)
    _normalize_scheduling(cfg)
    _normalize_app_auth(cfg)
    pairs = cfg.get("pairs")
    if isinstance(pairs, list):
        for it in pairs:
            if isinstance(it, dict):
                it["features"] = _normalize_features_map(it.get("features"))  # type: ignore[arg-type]
    _normalize_ui(cfg)

    # First-run marker for welcome/setup
    if first_run:
        try:
            ui = cfg.get("ui")
            if isinstance(ui, dict):
                ui.setdefault("_autogen", True)
        except Exception:
            pass

    # Ensure webhook URL tokens exist
    try:
        cfg, wh_changed = _ensure_webhook_ids(cfg)
        if wh_changed:
            try:
                save_config(cfg)
            except Exception:
                pass
    except Exception:
        pass

    # Scrobble watcher: migrate legacy provider/sink into route mode when routes is empty.
    try:
        from providers.scrobble.routes import ensure_routes

        cfg, migrated = ensure_routes(cfg)
        if migrated:
            try:
                save_config(cfg)
            except Exception:
                pass
    except Exception:
        pass

    # Migrate legacy global Trakt history_collection into pair-scoped overrides (pairs > global).
    migrated_trakt_collection = False
    try:
        t = cfg.get("trakt")
        if isinstance(t, dict) and ("history_collection" in t or "history_collection_types" in t):
            enabled = bool(t.pop("history_collection", False))
            raw_types = t.pop("history_collection_types", None)
            allowed = {"movies", "shows"}
            types: list[str] = []
            if isinstance(raw_types, str):
                types = [x.strip().lower() for x in raw_types.split(",") if x and x.strip()]
            elif isinstance(raw_types, list):
                types = [str(x).strip().lower() for x in raw_types if str(x).strip()]
            types = [x for x in types if x in allowed]
            if enabled and not types:
                types = ["movies"]

            pairs = cfg.get("pairs")
            if isinstance(pairs, list):
                for p in pairs:
                    if not isinstance(p, dict):
                        continue
                    src = str(p.get("source") or "").upper().strip()
                    dst = str(p.get("target") or "").upper().strip()
                    if "TRAKT" not in {src, dst}:
                        continue
                    prov = p.get("providers")
                    if not isinstance(prov, dict):
                        prov = {}
                        p["providers"] = prov
                    trp = prov.get("trakt")
                    if not isinstance(trp, dict):
                        trp = {}
                        prov["trakt"] = trp
                    if "history_collection" not in trp:
                        trp["history_collection"] = enabled
                        migrated_trakt_collection = True
                    if enabled and types and "history_collection_types" not in trp:
                        trp["history_collection_types"] = types
                        migrated_trakt_collection = True
            else:
                migrated_trakt_collection = True
    except Exception:
        pass
    if migrated_trakt_collection:
        try:
            save_config(cfg)
        except Exception:
            pass

    return cfg


def save_config(cfg: dict[str, Any]) -> None:
    data: dict[str, Any] = dict(cfg or {})
    data["version"] = _current_version_norm()
    _normalize_tmdb_sync(data)
    _normalize_trakt(data)
    _normalize_simkl(data)
    _normalize_mdblist(data)
    _normalize_scheduling(data)
    _normalize_app_auth(data)
    _normalize_ui(data)
    pairs = data.get("pairs")
    if isinstance(pairs, list):
        for it in pairs:
            if isinstance(it, dict):
                it["features"] = _normalize_features_map(it.get("features"))  # type: ignore[arg-type]

    _write_json_atomic(_cfg_file(), cast(dict[str, Any], _transform_secret_tree(data, decrypt=False)))
