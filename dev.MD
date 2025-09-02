# CrossWatch Developer Guide

Welcome to **CrossWatch** — a modular system for synchronizing watchlists and metadata
across providers such as Plex and SIMKL.

This guide is for developers who want to understand the **project structure**,
**modular architecture**, and how to extend or debug the system.

---

## 📂 Project Structure

```
root/
├── crosswatch.py         # FastAPI backend entrypoint
├── _FastAPI.py           # HTML template (injects assets, containers)
├── _logging.py           # Central logging utilities (ANSI colors, JSON sink)
├── _statistics.py        # Sync statistics engine (diffs, counters, events)
├── _TMDB.py              # TMDb API integration (posters, metadata)
├── _watchlist.py         # Watchlist utilities (grid, Plex API helpers)
│
├── assets/
│   ├── crossover.css     # Frontend styles
│   └── crossover.js      # Frontend logic (auth cards, run sync, status)
│
├── auth/
│   ├── __init__.py
│   ├── _auth_base.py     # AuthProvider protocol + AuthStatus/AuthManifest
│   ├── _auth_PLEX.py     # Plex device PIN auth flow
│   └── _auth_SIMKL.py    # SIMKL OAuth auth flow
│
├── modules/
│   ├── _mod_base.py      # SyncModule protocol + SyncContext/SyncResult
│   ├── _mod_PLEX.py      # Plex sync implementation
│   └── _mod_SIMKL.py     # SIMKL sync implementation
│
└── platform/
    ├── manager.py        # PlatformManager: unified auth + sync profiles
    └── orchestrator.py   # Orchestrator: runs sync profiles via modules
```

---

## 🧩 Core Concepts

### PlatformManager
- Discovers **auth providers** from `auth/_auth_*.py`
- Provides:
  - `/api/platform/providers` → manifests + status + capabilities
  - `/api/platform/auth/*` → start/finish/refresh/disconnect
  - `/api/platform/sync/options` → compute valid feature sets
  - `/api/platform/sync/profiles` → save/load sync profiles

### Orchestrator
- Reads sync profiles from PlatformManager
- Maps `(source, target)` → module class (e.g. PLEX→SIMKL)
- Runs selected features (`watchlist`, `ratings`, `watched`, etc.)
- Persists snapshots to config/state for delta sync

### Modules (`/modules`)
- Each `_mod_*.py` implements the `SyncModule` protocol
- Defines:
  - `run_sync(SyncContext)` → executes sync
  - `supported_features()` (optional) → declare capabilities

### Auth Providers (`/auth`)
- Each `_auth_*.py` implements the `AuthProvider` protocol
- Defines:
  - `manifest()` → label, flow type (`oauth`, `device_pin`, etc.), fields, actions
  - `capabilities()` → what features the provider supports
  - `get_status()`, `start()`, `finish()`, `refresh()`, `disconnect()`

---

## 🖥️ Frontend (assets/)
- `crossover.js` dynamically renders **auth cards** and wires buttons to `/api/platform/*`
- `crossover.css` styles the cards, badges, and sync grid
- UI is **provider-agnostic**: adding a new `_auth_*.py` automatically shows up

---

## 🪵 Logging
- Use `from _logging import log`
- Flexible API:
  ```python
  log("message", level="INFO", module="AUTH", extra={"provider": "SIMKL"})
  ```
- Supports ANSI colors (stdout) and optional JSON logs

---

## 📊 Statistics
- `_statistics.py` compares old/new states
- Tracks counts (added, removed, updated) and timeline events
- Feeds into insights dashboard in the web UI

---

## 🌐 External APIs
- **Plex**: Device PIN login, watchlist, collections
- **SIMKL**: OAuth2 login, watchlist, ratings, watched
- **TMDb**: Metadata enrichment (art, runtime, tagline, etc.)

---

## 🚀 Development Workflow
1. Clone repo and build dev container:
   ```bash
   docker build -t crosswatch-dev .
   docker run --rm -it -p 8787:8787 -v $PWD/config:/config crosswatch-dev
   ```
2. Visit `http://localhost:8787` to open the web UI.
3. Add providers under **Settings → Authentication**.
4. Create sync profiles under **Settings → Sync Profiles**.
5. Run sync via the **Run** button or API:
   ```bash
   curl -X POST http://localhost:8787/api/platform/sync/run -d '{"id":"PLEX→SIMKL"}'
   ```

---

## 🧑‍💻 Extending CrossWatch

### Add a new auth provider
1. Create `auth/_auth_NEWPROVIDER.py`
2. Export `PROVIDER = NewProviderAuth()`
3. Implement `AuthProvider` methods
4. Add `capabilities()` so sync engine knows what features are available

### Add a new sync module
1. Create `modules/_mod_NEWPROVIDER.py`
2. Implement `SyncModule` protocol with `run_sync()`
3. Register it in `platform/orchestrator.py`:
   ```python
   self.registry[("PLEX", "NEWPROVIDER")] = NewProviderModule
   ```

---

## ✅ Conventions
- **File naming**: `_auth_NAME.py`, `_mod_NAME.py`
- **Logger**: always use `_logging.log` instead of `print`
- **Profiles**: saved under `/config/profiles.json`
- **State**: snapshots persisted to `/config/state.json`
- **Config**: tokens, keys in `/config/config.json`

---

Happy hacking!  
_The CrossWatch Team_
