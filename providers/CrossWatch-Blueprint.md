# CrossWatch – Technical Blueprint

_Last generated: 2025-09-05 19:22 UTC_

## 0) TL;DR
- **Purpose:** Lightweight web UI + API to control and observe Plex ⇄ SIMKL watchlist syncing.
- **Stack:** FastAPI + vanilla JS/CSS. Providers for Plex, SIMKL, TMDb. No DB; JSON files in a configurable `CONFIG_BASE`.
- **How it runs:** `python3 crosswatch.py` (or `uvicorn crosswatch:app --host 0.0.0.0 --port 8787`).
- **Where things live:** see the tree below; API in `crosswatch.py`, UI in `assets/`, providers in `providers/`.
- **You can skim the Communication Matrix** to understand who talks to who.

## 1) Repository Structure
```
_FastAPI.py
_logging.py
_scheduling.py
_statistics.py
_watchlist.py
assets
  PLEX.svg
  SIMKL.svg
  __init__.py
  background.svg
  connections.overlay.js
  crosswatch.css
  crosswatch.js
crosswatch.py
cw_platform
  __init__.py
  config_base.py
  manager.py
  metadata.py
  orchestrator.py
providers
  __init__.py
  auth
    __init__.py
    _auth_PLEX.py
    _auth_SIMKL.py
    _auth_base.py
    registry.py
  metadata
    _meta_TMDB.py
    registry.py
  sync
    __init__.py
    _mod_PLEX.py
    _mod_SIMKL.py
    _mod_base.py
```

## 2) Runtime Architecture (bird’s‑eye)
- **Browser UI (assets/)** serves a single HTML (from `_FastAPI.get_index_html`) with `crosswatch.css` and `crosswatch.js`.
- **FastAPI app (`crosswatch.py`)** serves static assets, exposes REST endpoints for status/config/auth/sync/scheduling/stats/watchlist.
- **Providers (`providers/*`)**:
  - `auth` → device‑PIN (Plex) and OAuth (SIMKL).
  - `sync` → source/target adapters with a common base.
  - `metadata` → TMDb enrichment (images, titles, etc.).
- **State/Config** are JSON files in `CONFIG_BASE` (see `cw_platform/config_base.py`). Examples: `config.json`, `state.json`, `statistics.json`, `tombstones.json`, `profiles.json`, `last_sync.json`, `watchlist_hide.json`.
- **Scheduler (`_scheduling.py`)** triggers periodic syncs; **Stats (`_statistics.py`)** aggregates counters and timelines; **Logging (`_logging.py`)** prints structured logs.
- **Default port:** `8787` (see `crosswatch.main`).

## 3) Communication Matrix
| From | To | Protocol | Why |
|---|---|---|---|
| Web UI (`assets/*.js`) | FastAPI (`/api/*`) | HTTP (fetch) | Read status, change config, start sync, auth flows, list providers, view stats/watchlist |
| FastAPI | Providers (auth/sync/metadata) | In‑process Python calls | Perform provider‑specific logic |
| Providers | External services | HTTPS | Plex PIN API, SIMKL OAuth/Token, TMDb API, Plex & SIMKL data endpoints |
| FastAPI | Filesystem (`CONFIG_BASE`) | File I/O (JSON) | Persist `config.json`, `state.json`, `statistics.json`, etc. |
| Scheduler | FastAPI/Providers | Direct calls | Kick off background sync jobs |

### Key JSON Files
- `config.json` – user settings, auth tokens, pairs, scheduling.
- `state.json` – last known state/snapshots used by the orchestrator.
- `statistics.json` – counters + time series for UI.
- `tombstones.json` – guard deletes / conflict safety.
- `profiles.json` – stored accounts/profiles when applicable.
- `last_sync.json` – last sync metadata.
- `watchlist_hide.json` – UI local hide/overlay set.

## 4) REST API Surface (discovered from decorators)
| Method | Path | Handler |
|---|---|---|
| POST | `/api/metadata/resolve` | `api_metadata_resolve` |
| GET | `/api/update` | `api_update` |
| GET | `/api/version` | `get_version` |
| GET | `/api/version/check` | `api_version_check` |
| GET | `/api/insights` | `api_insights` |
| GET | `/api/stats/raw` | `api_stats_raw` |
| GET | `/api/stats` | `api_stats` |
| GET | `/api/logs/stream` | `api_logs_stream_initial` |
| GET | `/api/watchlist` | `api_watchlist` |
| DELETE | `/api/watchlist/{key}` | `api_watchlist_delete` |
| GET | `/favicon.svg` | `favicon_svg` |
| GET | `/favicon.ico` | `favicon_ico` |
| GET | `/` | `index` |
| GET | `/api/status` | `api_status` |
| GET | `/api/config` | `api_config` |
| POST | `/api/config` | `api_config_save` |
| POST | `/api/plex/pin/new` | `api_plex_pin_new` |
| POST | `/api/simkl/authorize` | `api_simkl_authorize` |
| GET | `/callback` | `oauth_simkl_callback` |
| POST | `/api/run` | `api_run_sync` |
| GET | `/api/run/summary` | `api_run_summary` |
| GET | `/api/run/summary/file` | `api_run_summary_file` |
| GET | `/api/run/summary/stream` | `api_run_summary_stream` |
| GET | `/api/state/wall` | `api_state_wall` |
| GET | `/art/tmdb/{typ}/{tmdb_id}` | `api_tmdb_art` |
| GET | `/api/tmdb/meta/{typ}/{tmdb_id}` | `api_tmdb_meta_path` |
| GET | `/api/scheduling` | `api_sched_get` |
| POST | `/api/scheduling` | `api_sched_post` |
| GET | `/api/scheduling/status` | `api_sched_status` |
| POST | `/api/troubleshoot/clear-cache` | `api_trbl_clear_cache` |
| POST | `/api/troubleshoot/reset-stats` | `api_trbl_reset_stats` |
| POST | `/api/troubleshoot/reset-state` | `api_trbl_reset_state` |
| GET | `/api/auth/providers` | `api_auth_providers` |
| GET | `/api/auth/providers/html` | `api_auth_providers_html` |
| GET | `/api/metadata/providers` | `api_metadata_providers` |
| GET | `/api/metadata/providers/html` | `api_metadata_providers_html` |
| GET | `/api/sync/providers` | `api_sync_providers` |
| GET | `/api/pairs` | `api_pairs_list` |
| POST | `/api/pairs` | `api_pairs_add` |
| PUT | `/api/pairs/{pair_id}` | `api_pairs_update` |
| DELETE | `/api/pairs/{pair_id}` | `api_pairs_delete` |

> Groupings (human‑readable): status/version (`/api/status`, `/api/version*`), config (`/api/config`), auth (`/api/auth/*`, `/api/plex/pin/new`, `/api/simkl/authorize`, `/callback`), sync (`/api/run`, `/api/pairs*`, `/api/sync/providers`), metadata (`/api/metadata/*`, `/api/tmdb/*`), scheduling (`/api/scheduling*`), stats/logs (`/api/stats*`, `/api/logs/*`), troubleshoot (`/api/troubleshoot/*`), watchlist (`/api/watchlist*`).

## 5) Dependencies
**Python runtime packages (seen in imports):** `fastapi`, `uvicorn`, `requests`, `plexapi`, `packaging`, `pydantic` plus stdlib (`asyncio`, `threading`, `pathlib`, etc.).

**External services:**
- **Plex** – device PIN flow (`https://plex.tv/api/v2/pins`), account & watchlist endpoints. 
- **SIMKL** – OAuth authorize/token (`simkl.com` / `api.simkl.com`).
- **TMDb** – images & metadata (`image.tmdb.org/t/p`, REST API requires API key).

**Environment:**
- `CONFIG_BASE` (optional) – where JSON state lives. Defaults: `/config` when containerized; otherwise repo root.
- `TZ` – app uses UTC in JSON (`...Z`). Your browser/UI can render local time.

## 6) How Things Work (short flows)
- **Open UI** → GET `/` → returns inline HTML from `_FastAPI.get_index_html` that links `assets/`.
- **List providers** → UI calls `/api/auth/providers` and `/api/metadata/providers` to render capabilities; `/api/sync/providers` to show sync modules.
- **Plex login** → POST `/api/plex/pin/new` → app polls Plex PIN status and persists token into `config.json`.
- **SIMKL login** → POST `/api/simkl/authorize` → browser opens SIMKL → redirect to `/callback` → token exchange, persist.
- **Run sync** → POST `/api/run` → orchestrator reads `config.json` + `state.json`, loads providers, performs PLEX→SIMKL / SIMKL→PLEX (depending on pair mode), updates `state.json`, `statistics.json`, `last_sync.json`.
- **Scheduler** → `/api/scheduling` GET/POST to view/change; background thread schedules `POST /api/run` equivalent.
- **Troubleshoot** → clear caches or reset JSON state files via `/api/troubleshoot/*`.

## 7) Files & Scripts – Responsibilities
Below is a per‑file summary so future ChatGPTs (and humans) don’t get lost.

### `crosswatch.py`
- **Imports:** _FastAPI, __future__, _scheduling, _statistics, _watchlist, asyncio, contextlib, cw_platform, dataclasses, datetime, fastapi, functools, importlib, inspect, json, os, packaging, pathlib, pkgutil, providers, pydantic, re, requests, secrets, shlex, shutil, socket, sys, threading, time, typing, urllib, uuid, uvicorn
- **Classes:** MetadataResolveIn, PairIn, _UIHostLogger
- **Top functions:** _json_safe, _cfg_pairs, _gen_id, api_metadata_resolve, api_update, _norm, _cached_latest_release, _ttl_marker, _is_update_available, get_version, _ver_tuple, api_version_check, _read_json, _write_json, load_config, save_config, _is_placeholder, _escape_html, strip_ansi, ansi_to_html, _append_log, _sync_progress_ui, _run_pairs_thread, _summary_reset, _summary_set, _summary_set_timeline, _summary_snapshot, _parse_sync_line, _load_hide_set, refresh_wall
- **Role:**

  FastAPI application: routes, static file serving, orchestrating providers, config/state I/O, version check, logs stream, and main entrypoint (port 8787).


### `_FastAPI.py`
- **Imports:** —
- **Classes:** —
- **Top functions:** get_index_html
- **Role:**

  Returns fully inlined HTML for the Web UI; links into `/assets/*`. Keep self‑contained so backend stays Python‑only.


### `_logging.py`
- **Imports:** __future__, datetime, json, sys, threading, typing
- **Classes:** Logger
- **Top functions:** __init__, set_level, enable_color, enable_time, enable_json, set_context, get_context, bind, child, level_name, _fmt_text, _write_sinks, debug, info, warn, warning, error, success, __call__
- **Role:**

  (General helper / module.)


### `_scheduling.py`
- **Imports:** __future__, datetime, threading, time, typing
- **Classes:** SyncScheduler
- **Top functions:** merge_defaults, compute_next_run, __init__, _get_sched_cfg, _set_sched_cfg, ensure_defaults, status, start, stop, refresh, _loop
- **Role:**

  Small configurable scheduler. Computes next run from simple rules (disabled/hourly/daily/cron‑like), runs a worker in a thread, exposes status helpers.


### `_statistics.py`
- **Imports:** __future__, cw_platform, datetime, json, pathlib, re, threading, time, typing
- **Classes:** Stats
- **Top functions:** _read_json, _write_json_atomic, __init__, _load, _save, _title_of, _year_of, _fallback_key, _extract_ids, _canon_from_ids, _aliases, _build_union_map, _counts_by_source, _totals_from_events, _ensure_counters, _count_at, refresh_from_state, record_event, record_summary, reset, overview, primary_key, ensure_bucket
- **Role:**

  Aggregates counters and rolling samples from `state.json`; records add/remove events; exposes `/api/stats` & raw view.


### `_watchlist.py`
- **Imports:** __future__, cw_platform, datetime, json, pathlib, plexapi, typing
- **Classes:** —
- **Top functions:** _load_hide_set, _save_hide_set, _pick_added, _iso_to_epoch, _norm_guid, _guid_variants_from_key_or_item, _extract_plex_identifiers, build_watchlist, delete_watchlist_item
- **Role:**

  Plex‑only watchlist read/hide overlay storage (`watchlist_hide.json`).


### `assets/__init__.py`
- **Imports:** —
- **Classes:** —
- **Top functions:** —
- **Role:**

  Static UI assets: CSS theme, JS to call `/api/*`, overlay for connections board, and SVG brand icons.


### `cw_platform/config_base.py`
- **Imports:** os, pathlib
- **Classes:** —
- **Top functions:** CONFIG_BASE
- **Role:**

  Platform helpers. `config_base.py` resolves `CONFIG_BASE` path with sane defaults.


### `cw_platform/manager.py`
- **Imports:** __future__, _logging, cw_platform, importlib, json, os, pathlib, pkgutil, providers, typing
- **Classes:** PlatformManager
- **Top functions:** __init__, _discover_providers, providers_list, auth_start, auth_finish, auth_refresh, auth_disconnect, _caps, sync_options, _read_profiles, _write_profiles, sync_profiles, sync_profiles_save
- **Role:**

  Platform helpers. `config_base.py` resolves `CONFIG_BASE` path with sane defaults.


### `cw_platform/metadata.py`
- **Imports:** __future__, _logging, importlib, pkgutil, providers, typing
- **Classes:** MetadataManager
- **Top functions:** __init__, _discover, resolve, _merge
- **Role:**

  Platform helpers. `config_base.py` resolves `CONFIG_BASE` path with sane defaults.


### `cw_platform/orchestrator.py`
- **Imports:** __future__, cw_platform, json, pathlib, providers, requests, time, typing
- **Classes:** Orchestrator
- **Top functions:** _count_types, _safe_get, _normalize_title_year, _sig, _collect_ids, _canonical_key, _items_by_type_from_keys, _payload_from_snapshot, _keyset, _sigset, _index_for, _diff_additions, _deletes_payload_from_snapshot, load_snapshot, snapshot_to_index, build_state, write_state, load_tombstones, save_tombstones, prune_tombstones, mark_tombstones, filter_additions_with_tombstones, _tmdb_find_by_imdb, _tmdb_search, _ensure_tmdb_ids, _union_preserve, _add_to, _remove_from, minimal, maybe_key
- **Role:**

  Platform helpers. `config_base.py` resolves `CONFIG_BASE` path with sane defaults.


### `cw_platform/__init__.py`
- **Imports:** —
- **Classes:** —
- **Top functions:** —
- **Role:**

  Platform helpers. `config_base.py` resolves `CONFIG_BASE` path with sane defaults.


### `providers/__init__.py`
- **Imports:** —
- **Classes:** —
- **Top functions:** —
- **Role:**

  (General helper / module.)


### `providers/auth/registry.py`
- **Imports:** __future__, importlib, inspect, pathlib, pkgutil, providers, typing
- **Classes:** —
- **Top functions:** _iter_auth_modules, _provider_from_module, auth_providers_manifests, _module_html, auth_providers_html
- **Role:**

  Auth abstraction (`_auth_base.py`) plus implementations: Plex device‑PIN and SIMKL OAuth. `registry.py` discovers `_auth_*` at runtime.


### `providers/auth/_auth_base.py`
- **Imports:** __future__, _logging, dataclasses, typing
- **Classes:** AuthManifest, AuthStatus, AuthProvider
- **Top functions:** manifest, capabilities, get_status, start, finish, refresh, disconnect
- **Role:**

  Auth abstraction (`_auth_base.py`) plus implementations: Plex device‑PIN and SIMKL OAuth. `registry.py` discovers `_auth_*` at runtime.


### `providers/auth/_auth_PLEX.py`
- **Imports:** __future__, _auth_base, _logging, requests, time, typing
- **Classes:** PlexAuth
- **Top functions:** html, manifest, capabilities, get_status, start, finish, refresh, disconnect
- **Role:**

  Auth abstraction (`_auth_base.py`) plus implementations: Plex device‑PIN and SIMKL OAuth. `registry.py` discovers `_auth_*` at runtime.


### `providers/auth/_auth_SIMKL.py`
- **Imports:** __future__, _auth_base, _logging, requests, time, typing
- **Classes:** SimklAuth
- **Top functions:** html, manifest, capabilities, get_status, start, finish, refresh, disconnect
- **Role:**

  Auth abstraction (`_auth_base.py`) plus implementations: Plex device‑PIN and SIMKL OAuth. `registry.py` discovers `_auth_*` at runtime.


### `providers/auth/__init__.py`
- **Imports:** —
- **Classes:** —
- **Top functions:** —
- **Role:**

  Auth abstraction (`_auth_base.py`) plus implementations: Plex device‑PIN and SIMKL OAuth. `registry.py` discovers `_auth_*` at runtime.


### `providers/metadata/registry.py`
- **Imports:** __future__, importlib, inspect, pkgutil, providers, typing
- **Classes:** —
- **Top functions:** _iter_meta_modules, _provider_from_module, metadata_providers_manifests, _module_html, metadata_providers_html
- **Role:**

  TMDb enrichment. Fetches posters/backdrops and caches results; raises if API key missing.


### `providers/metadata/_meta_TMDB.py`
- **Imports:** __future__, _logging, hashlib, requests, time, typing
- **Classes:** TmdbProvider
- **Top functions:** build, html, __init__, _apikey, _get, _safe_int_year, _images, fetch
- **Role:**

  TMDb enrichment. Fetches posters/backdrops and caches results; raises if API key missing.


### `providers/sync/_mod_base.py`
- **Imports:** __future__, _logging, dataclasses, enum, typing
- **Classes:** Logger, SyncStatus, SyncContext, ProgressEvent, SyncResult, ModuleCapabilities, ModuleInfo, ModuleError, RecoverableModuleError, ConfigError, SyncModule
- **Top functions:** __call__, set_context, get_context, bind, child, __init__, validate_config, run_sync, get_status, cancel, set_logger, reconfigure
- **Role:**

  Sync module base and concrete adapters for PLEX and SIMKL; shared enums/context, progress events, and merge logic live in `_mod_base.py`.


### `providers/sync/_mod_PLEX.py`
- **Imports:** __future__, _logging, _mod_base, json, pathlib, plexapi, re, requests, threading, time, typing
- **Classes:** _NullLogger, _LoggerAdapter, PLEXModule
- **Top functions:** _extract_ids_from_guid_strings, _plex_headers, _discover_get, _discover_metadata_by_ratingkey, plex_fetch_watchlist_items_via_discover, plex_fetch_watchlist_items_via_plexapi, plex_fetch_watchlist_items, plex_item_to_ids, item_libtype, resolve_discover_item, plex_add_by_ids, plex_remove_by_ids, gather_plex_rows, build_index, debug, info, warn, warning, error, set_context, get_context, bind, child, __init__, __call__, set_context, get_context, bind, child, _same
- **Role:**

  Sync module base and concrete adapters for PLEX and SIMKL; shared enums/context, progress events, and merge logic live in `_mod_base.py`.


### `providers/sync/_mod_SIMKL.py`
- **Imports:** __future__, _mod_base, json, requests, time, typing
- **Classes:** SIMKLModule
- **Top functions:** simkl_headers, _read_as_list, ids_from_simkl_item, build_index_from_simkl, simkl_ptw_full, add, _get, __init__, supported_features, validate_config, reconfigure, set_logger, get_status, cancel, simkl_add_to_ptw, simkl_remove_from_ptw, run_sync, _normalize, emit
- **Role:**

  Sync module base and concrete adapters for PLEX and SIMKL; shared enums/context, progress events, and merge logic live in `_mod_base.py`.


### `providers/sync/__init__.py`
- **Imports:** —
- **Classes:** —
- **Top functions:** —
- **Role:**

  Sync module base and concrete adapters for PLEX and SIMKL; shared enums/context, progress events, and merge logic live in `_mod_base.py`.


## 8) REST Endpoints – What they’re for (human version)
- **Status & Version**: `/api/status`, `/api/version`, `/api/version/check`.
- **Config**: `/api/config` GET/POST to retrieve/change `config.json` (providers, pairs, scheduling).
- **Auth**: `/api/auth/providers`, `/api/plex/pin/new`, `/api/simkl/authorize`, `/callback`.
- **Sync**: `/api/run` (start), `/api/pairs*` (CRUD pairs), `/api/sync/providers` (feature matrix).
- **Watchlist**: `/api/watchlist` GET, and DELETE `/api/watchlist/{key}` to unhide.
- **Metadata**: `/api/metadata/resolve`, `/api/tmdb/meta/{typ}/{tmdb_id}`.
- **Scheduling**: `/api/scheduling*` to manage scheduler and `/api/scheduling/status`.
- **Stats & Logs**: `/api/stats`, `/api/stats/raw`, `/api/logs/stream`.
- **Troubleshoot**: reset or clear via `/api/troubleshoot/*`.

## 9) Data Model (pragmatic)
- **Pairs**: list of sync relationships (e.g., `PLEX → SIMKL`, mode one‑way or two‑way, add/remove flags).
- **Auth**: provider‑scoped tokens under distinct keys (`plex.account_token`, `simkl.access_token`, etc.).
- **State**: canonical item keys and snapshots to compute diffs and detect add/remove.
- **Stats**: time‑bucketed counters; totals per source and per type.

## 10) Building & Running
- **Local**: `pip install fastapi uvicorn requests plexapi packaging pydantic` then `python3 crosswatch.py`.
- **Container**: If `/app` exists, `CONFIG_BASE` defaults to `/config`.
- **Port**: 8787 by default. Change with `python3 -c "import crosswatch; crosswatch.main(port=3000)"` or expose via your process manager.

## 11) Conventions & Style
- **Timestamps** stored as ISO 8601 UTC (`...Z`). Render local time in the UI.
- **Atomic writes** to JSON (write to `.tmp` then replace).
- **Registries** discover providers dynamically by filename pattern (`_auth_*`, `_meta_*`, `_mod_*`).
- **Logs** use short tags: `[i]`, `[!]`, `[✓]` etc. JSON stream optional.

## 12) Known Ports & Callbacks
- Web UI/API: **8787** (default).
- SIMKL OAuth redirect: **`/callback`** (must match SIMKL app config).

## 13) What to edit when you add a provider
1. Create `providers/auth/_auth_NEW.py` and expose a Provider implementing `AuthProvider`.
2. Create `providers/sync/_mod_NEW.py` implementing `SyncModule`.
3. Optionally add `providers/metadata/_meta_NEW.py`.
4. They’ll be auto‑discovered by the registries; UI lists them via `/api/*/providers`.

## 14) Appendix A – Raw Endpoint List
(Same as above, but easy to copy/paste.)

| Method | Path | Handler |
|---|---|---|
| POST | `/api/metadata/resolve` | `api_metadata_resolve` |
| GET | `/api/update` | `api_update` |
| GET | `/api/version` | `get_version` |
| GET | `/api/version/check` | `api_version_check` |
| GET | `/api/insights` | `api_insights` |
| GET | `/api/stats/raw` | `api_stats_raw` |
| GET | `/api/stats` | `api_stats` |
| GET | `/api/logs/stream` | `api_logs_stream_initial` |
| GET | `/api/watchlist` | `api_watchlist` |
| DELETE | `/api/watchlist/{key}` | `api_watchlist_delete` |
| GET | `/favicon.svg` | `favicon_svg` |
| GET | `/favicon.ico` | `favicon_ico` |
| GET | `/` | `index` |
| GET | `/api/status` | `api_status` |
| GET | `/api/config` | `api_config` |
| POST | `/api/config` | `api_config_save` |
| POST | `/api/plex/pin/new` | `api_plex_pin_new` |
| POST | `/api/simkl/authorize` | `api_simkl_authorize` |
| GET | `/callback` | `oauth_simkl_callback` |
| POST | `/api/run` | `api_run_sync` |
| GET | `/api/run/summary` | `api_run_summary` |
| GET | `/api/run/summary/file` | `api_run_summary_file` |
| GET | `/api/run/summary/stream` | `api_run_summary_stream` |
| GET | `/api/state/wall` | `api_state_wall` |
| GET | `/art/tmdb/{typ}/{tmdb_id}` | `api_tmdb_art` |
| GET | `/api/tmdb/meta/{typ}/{tmdb_id}` | `api_tmdb_meta_path` |
| GET | `/api/scheduling` | `api_sched_get` |
| POST | `/api/scheduling` | `api_sched_post` |
| GET | `/api/scheduling/status` | `api_sched_status` |
| POST | `/api/troubleshoot/clear-cache` | `api_trbl_clear_cache` |
| POST | `/api/troubleshoot/reset-stats` | `api_trbl_reset_stats` |
| POST | `/api/troubleshoot/reset-state` | `api_trbl_reset_state` |
| GET | `/api/auth/providers` | `api_auth_providers` |
| GET | `/api/auth/providers/html` | `api_auth_providers_html` |
| GET | `/api/metadata/providers` | `api_metadata_providers` |
| GET | `/api/metadata/providers/html` | `api_metadata_providers_html` |
| GET | `/api/sync/providers` | `api_sync_providers` |
| GET | `/api/pairs` | `api_pairs_list` |
| POST | `/api/pairs` | `api_pairs_add` |
| PUT | `/api/pairs/{pair_id}` | `api_pairs_update` |
| DELETE | `/api/pairs/{pair_id}` | `api_pairs_delete` |
