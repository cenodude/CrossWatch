# CrossWatch Developer Guide

This project is organized around a **providers-first** architecture.
See `providers/` for all pluggable integrations: auth, sync, metadata.

## Project Structure
```
root/
├── crosswatch.py                 # main FastAPI app + all API endpoints
├── _FastAPI.py                   # serves web UI index.html
├── _logging.py                   # central log helper
├── _statistics.py                # Stats model + file ops
├── _watchlist.py                 # helpers to build/remove watchlist items
├── _scheduling.py                # scheduler wrapper for sync jobs
│
├── assets/                       # static frontend bundle
│   ├── crosswatch.css
|   ├── crosswatch.js             # main js for 99% of the code
│   └── connections.overlay.js    # some additional JS script for settings >> Synchronization Providers
│
├── cw_platform/                  # platform-level managers
│   ├── manager.py                # PlatformManager: unified auth + sync profiles
│   ├── orchestrator.py           # Orchestrator: runs pairs, writes state.json
│   └── metadata.py               # MetadataManager: providers.metadata wrapper
│
└── providers/                    # pluggable integrations
    ├── auth/                     # authentication providers
    │   ├── __init__.py
    │   ├── _auth_base.py
    │   ├── _auth_PLEX.py
    │   └── _auth_SIMKL.py
    │
    ├── sync/                     # sync modules - must also have mod in /auth
    │   ├── __init__.py
    │   ├── _mod_base.py
    │   ├── _mod_PLEX.py
    │   └── _mod_SIMKL.py
    │
    └── metadata/                 # metadata providers (e.g. TMDb)
        ├── __init__.py
        └── _meta_TMDB.py
```

## Extension Points
- **Auth providers**: implement `AuthProvider` in `providers/auth/_auth_NAME.py`
- **Sync modules**: implement `SyncModule` in `providers/sync/_mod_NAME.py`
- **Metadata providers**: implement `build()` + `fetch()` in `providers/metadata/_meta_NAME.py`

## Orchestration
- `PlatformManager` discovers auth providers and manages sync profiles.
- `Orchestrator` maps `(source, target)` to sync modules and runs them.
- `MetadataManager` resolves images/details from configured sources.

## Logging
Use the flexible logger everywhere:
```python
from _logging import log
log("Fetched 42 posters", level="INFO", module="META", extra={"provider":"TMDB"})
```

## Scheduling
Scheduler runs profiles using PlatformManager + Orchestrator.
Configure `config.json → scheduling` (enabled, mode, daily_time, profile_id, only_when_connected).

# CrossWatch Providers

All pluggable integrations for CrossWatch live under the `providers/` namespace.

## Structure

```
providers/
  auth/        # Authentication providers (connect to external accounts)
  sync/        # Synchronization modules (move data between providers)
  metadata/    # Metadata sources (fetch posters, info, runtime, etc.)
```

## Auth Providers

- Location: `providers/auth/_auth_NAME.py`
- Must implement the `AuthProvider` protocol from `_auth_base.py`
- Export an instance as `PROVIDER`
- Define:
  - `manifest()` → UI metadata (label, flow type, fields, actions)
  - `capabilities()` → supported features
  - `get_status(config)`
  - `start(config, payload, save_config)`
  - `finish(config, payload, save_config)`
  - `refresh(config, save_config)`
  - `disconnect(config, save_config)`

## Sync Modules

- Location: `providers/sync/_mod_NAME.py`
- Must implement the `SyncModule` protocol from `_mod_base.py`
- Define:
  - `run_sync(ctx: SyncContext) -> SyncResult`
  - Optional: `supported_features()`
- Registered in `platform/orchestrator.py` by mapping `(source, target)` → `ModuleClass`

## Metadata Providers

- Location: `providers/metadata/_meta_NAME.py`
- Must expose either:
  - `PROVIDER = ProviderInstance` **or**
  - `def build(load_cfg, save_cfg) -> ProviderInstance`
- Define:
  - `manifest()` (label, supported entities, supported assets, needs API key?)
  - `capabilities()`
  - `fetch(entity, ids, locale, need) -> dict`

Returned metadata should be normalized to include:

```json
{
  "ids": {"tmdb": "123", "imdb": "tt123"},
  "title": "...",
  "overview": "...",
  "year": 2024,
  "images": {
    "poster": [{"url": "...", "w": 1000, "h": 1500, "lang": "en"}],
    "backdrop": [...]
  }
}
```

## Adding a New Provider

1. Create a new `_auth_NAME.py`, `_mod_NAME.py`, or `_meta_NAME.py` file in the proper folder.
2. Follow the conventions described above.
3. Test with `PlatformManager` or `MetadataManager`.
4. Update `orchestrator.py` if adding new sync pairs.

# CrossWatch Backend — `crosswatch.py` + `_FastAPI.py`

## TL;DR
- **`crosswatch.py`** is the backend Python server. It runs Uvicorn on **`http://0.0.0.0:8787`** and exposes all API endpoints listed below.
- **`_FastAPI.py`** serves the web UI shell and routes the root (`/`) to the HTML that references the static assets:
  - **`/assets/crosswatch.js`**
  - **`/assets/crosswatch.css`**

This single doc combines both files’ roles and the available API surface for quick reference.

---

## What each file does

### `crosswatch.py` (API server)
- Starts the FastAPI application and Uvicorn.
- Hosts all API routes under `http://0.0.0.0:8787`.
- Handles sync runs, scheduling, state, providers, pairs, TMDb art/meta, troubleshooting, logs, and status/version/config endpoints.
- Streams logs and run summaries via Server‑Sent Events where applicable.
- Intended to be the only process you need to start for the backend.

**Run options**
```bash
# Simple
python crosswatch.py

# Or explicitly via Uvicorn (equivalent bind)
uvicorn crosswatch:app --host 0.0.0.0 --port 8787
```

### `_FastAPI.py` (Web UI + static references)
- Renders the root HTML (served at `/`) for the web interface.
- References the UI assets: `/assets/crosswatch.js` `/assets/connections.overlay.js` and `/assets/crosswatch.css`.
- Keeps UI generation self‑contained; the API endpoints themselves live in `crosswatch.py`.

---

## Endpoint Inventory (served by `crosswatch.py`)
Base URL: `http://0.0.0.0:8787`

### Metadata
- **POST `/api/metadata/resolve`** → Resolve metadata (posters, year, runtime) via MetadataManager.

### Updates & Version
- **GET `/api/update`** → Check latest GitHub release.  
- **GET `/api/version`** → Return current + latest version.  
- **GET `/api/version/check`** → Simple version check (numeric tuple).

### Insights & Stats
- **GET `/api/insights`** → Return statistics.json series, sync history, estimated watchtime.  
- **GET `/api/stats/raw`** → Raw contents of `statistics.json`.  
- **GET `/api/stats`** → Current overview stats (added/removed counts).

### Logs
- **GET `/api/logs/stream`** → Server‑sent event (SSE) stream of logs.

### Watchlist
- **GET `/api/watchlist`** → Preview combined Plex+SIMKL watchlist.  
- **DELETE `/api/watchlist/{key}`** → Remove entry from state.json + providers.

### Root / UI
- **GET `/`** → Web UI index.

### Status & Config
- **GET `/api/status`** → Connection status Plex/SIMKL.  
- **GET `/api/config`** → Get config.json.  
- **POST `/api/config`** → Save config.json.

### Auth / OAuth
- **POST `/api/plex/pin/new`** → Start Plex PIN login.  
- **POST `/api/simkl/authorize`** → Start SIMKL OAuth.  
- **GET `/callback`** → SIMKL OAuth callback.

### Run & Summary
- **POST `/api/run`** → Trigger manual sync run.  
- **GET `/api/run/summary`** → Current summary snapshot.  
- **GET `/api/run/summary/file`** → Download last run summary.  
- **GET `/api/run/summary/stream`** → Live run summary stream.

### State & Wall
- **GET `/api/state/wall`** → Watchlist preview (wall) from state.json.

### TMDb
- **GET `/art/tmdb/{typ}/{tmdb_id}`** → Serve cached poster/backdrop.  
- **GET `/api/tmdb/meta/{typ}/{tmdb_id}`** → Fetch metadata for movie/tv.

### Scheduling
- **GET `/api/scheduling`** → Get scheduling config.  
- **POST `/api/scheduling`** → Save scheduling config.  
- **GET `/api/scheduling/status`** → Scheduler status.

### Troubleshooting
- **POST `/api/troubleshoot/reset-stats`** → Clear statistics.json.  
- **POST `/api/troubleshoot/clear-cache`** → Empty cache dir.  
- **POST `/api/troubleshoot/reset-state`** → Ask orchestrator to rebuild state.json.

### Providers
- **GET `/api/auth/providers`** → List auth providers.  
- **GET `/api/auth/providers/html`** → Auth providers HTML.  
- **GET `/api/metadata/providers`** → List metadata providers.  
- **GET `/api/metadata/providers/html`** → Metadata providers HTML.  
- **GET `/api/sync/providers`** → List sync modules + capabilities.

### Pairs
- **GET `/api/pairs`** → List sync pairs.  
- **POST `/api/pairs`** → Add new pair.  
- **PUT `/api/pairs/{pair_id}`** → Update pair.  
- **DELETE `/api/pairs/{pair_id}`** → Delete pair.

---

## Quick checks (curl)

```bash
# Health / status
curl -s http://0.0.0.0:8787/api/status | jq

# Version
curl -s http://0.0.0.0:8787/api/version | jq

# Providers (use these for UI dropdowns)
curl -s http://0.0.0.0:8787/api/sync/providers | jq

# Pairs list
curl -s http://0.0.0.0:8787/api/pairs | jq

# Start a sync
curl -s -X POST http://0.0.0.0:8787/api/run | jq

# Watch run summary stream (SSE) with curl
curl -N http://0.0.0.0:8787/api/run/summary/stream
```

---

## Notes
- UI should consume **`/api/sync/providers`** for the dynamic provider list.
- Pairs CRUD exists; when updating the UI, bind to these routes.
- TMDb routes provide poster/art and metadata enrichment.
- Scheduling endpoints control the job runner and status banners in the UI.
- Troubleshoot endpoints reset stats/state/cache when needed.

Keep it simple: start `crosswatch.py`, open the web UI at `/`, and the rest of the app works via the endpoints above.
