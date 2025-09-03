# CrossWatch Developer Guide

This project is organized around a **providers-first** architecture.
See `providers/` for all pluggable integrations: auth, sync, metadata.

## Project Structure
```
root/
├── crosswatch.py
├── _FastAPI.py
├── _logging.py
├── _statistics.py
├── _watchlist.py
├── _scheduling.py
│
├── assets/
│   ├── crossover.css
│   └── crossover.js
│
├── cw_platform/
│   ├── manager.py        # unified auth + sync profiles
│   ├── orchestrator.py   # runs sync profiles via providers.sync modules
│   └── metadata.py       # MetadataManager (providers.metadata)
│
└── providers/
    ├── auth/
    │   ├── __init__.py
    │   ├── _auth_base.py
    │   ├── _auth_PLEX.py
    │   └── _auth_SIMKL.py
    │
    ├── sync/
    │   ├── __init__.py
    │   ├── _mod_base.py
    │   ├── _mod_PLEX.py
    │   └── _mod_SIMKL.py
    │
    └── metadata/
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
- References the UI assets: `/assets/crosswatch.js` and `/assets/crosswatch.css`.
- Keeps UI generation self‑contained; the API endpoints themselves live in `crosswatch.py`.

---

## Endpoint Inventory (served by `crosswatch.py`)

> Base URL: `http://0.0.0.0:8787`

### Root / UI
- **GET** `/` — Index (serves the web UI)

### Status & Version
- **GET** `/api/status` — Api Status
- **GET** `/api/version` — Get Version
- **GET** `/api/version/check` — Api Version Check
- **GET** `/api/update` — Api Update

### Insights & Stats
- **GET** `/api/insights` — Api Insights
- **GET** `/api/stats/raw` — Api Stats Raw
- **GET** `/api/stats` — Api Stats

### Logs & Streams
- **GET** `/api/logs/stream` — Api Logs Stream Initial
- **GET** `/api/run/summary/stream` — Api Run Summary Stream

### Config
- **GET** `/api/config` — Api Config
- **POST** `/api/config` — Api Config Save

### Auth (Plex / SIMKL / OAuth)
- **POST** `/api/plex/pin/new` — Api Plex Pin New
- **POST** `/api/simkl/authorize` — Api Simkl Authorize
- **GET** `/callback` — Oauth Simkl Callback

### Run & Summary
- **POST** `/api/run` — Api Run Sync
- **GET** `/api/run/summary` — Api Run Summary
- **GET** `/api/run/summary/file` — Api Run Summary File

### State & Wall
- **GET** `/api/state/wall` — Api State Wall

### Watchlist
- **GET** `/api/watchlist` — Api Watchlist
- **DELETE** `/api/watchlist/{key}` — Api Watchlist Delete

### TMDb / Art / Metadata
- **GET** `/art/tmdb/{typ}/{tmdb_id}` — Api Tmdb Art
- **GET** `/api/tmdb/meta/{typ}/{tmdb_id}` — Api Tmdb Meta

### Scheduling
- **GET** `/api/scheduling` — Api Sched Get
- **POST** `/api/scheduling` — Api Sched Post
- **GET** `/api/scheduling/status` — Api Sched Status

### Troubleshooting
- **POST** `/api/troubleshoot/reset-stats` — Api Trbl Reset Stats
- **POST** `/api/troubleshoot/clear-cache` — Api Trbl Clear Cache
- **POST** `/api/troubleshoot/reset-state` — Api Trbl Reset State

### Providers
- **GET** `/api/auth/providers` — Api Auth Providers
- **GET** `/api/auth/providers/html` — Api Auth Providers Html
- **GET** `/api/metadata/providers` — Api Metadata Providers
- **GET** `/api/metadata/providers/html` — Api Metadata Providers Html
- **GET** `/api/sync/providers` — Api Sync Providers

### Pairs
- **GET** `/api/pairs` — Api Pairs List
- **POST** `/api/pairs` — Api Pairs Add
- **PUT** `/api/pairs/{pair_id}` — Api Pairs Update
- **DELETE** `/api/pairs/{pair_id}` — Api Pairs Delete

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
