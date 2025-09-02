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
├── platform/
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
