<p align="center">
  <img src="/images/CrossWatch-transparant.png" alt="CrossWatch" width="500">
</p>

# CrossWatch

**CrossWatch** is a lightweight web-based control panel and sync engine that keeps your **Plex**,**SIMKL** and **TRAKT**  in sync.  
It provides a clean UI to link accounts, configure sync pairs, run them manually or on schedule, and track stats/history.

---

## 🚀 Features

- Sync watchlists (one-way or two-way)
- Live Scrobbling (Plex → Trakt)
- Sync Ratings (one-way or two-way - currently disabled in alpha version)
- Sync Watch history (one-way or two-way - currently disabled in alpha version)
- Sync Playlists (one-way or two-way - currently disabled in alpha version)

- Simple web UI — no external DB, just JSON state files
- Rich metadata & posters via TMDb
- Stats, history, and live logs built-in
- Headless scheduling of sync runs

---

## 🐳 Run as Container

```bash
docker run -d   --name crosswatch   -p 8787:8787   -v /path/to/config:/config   -e TZ=Europe/Amsterdam   ghcr.io/cenodude/crosswatch:latest
```

> The container exposes the web UI at:  
> 👉 http://localhost:8787

By default `CONFIG_BASE` will be `/config` inside the container.  
Your `config.json`, `state.json`, `statistics.json`, etc. will all be stored there.

---

## 🧩 Architecture

- **FastAPI** backend (`crosswatch.py`) at port `8787`
- **Vanilla JS/CSS** UI served from `/assets/`
- Pluggable **providers**:  
  - `auth` (Plex device PIN, SIMKL OAuth)
  - `sync` (PLEX ⇄ SIMKL)
  - `metadata` (TMDb enrichment)
- All state/config stored as JSON in `CONFIG_BASE`

---


## ⚡ Local Development

```bash
git clone https://github.com/cenodude/CrossWatch.git
cd CrossWatch
pip install fastapi uvicorn requests plexapi packaging pydantic
python3 crosswatch.py
```

Then open:  
📍 http://localhost:8787

---

## 📋 Usage

1. Open the web UI
2. Connect at least two Authentication providers, Plex, SIMKL and/or TRakt under
3. Create one or more **Sync Pairs** (e.g. Plex → SIMKL or two-way)
4. Click **Synchronize** to start, or enable scheduling in **Settings**
5. Track stats, logs, and history from the UI

---

## 🛠 Troubleshooting

- Clear cache: `POST /api/troubleshoot/clear-cache`
- Reset stats: `POST /api/troubleshoot/reset-stats`
- Reset state: `POST /api/troubleshoot/reset-state`

---
# 🎬 Live Scrobbling (Plex → Trakt)

CrossWatch can **scrobble your real-time Plex playback to Trakt** — so episodes and movies you watch are instantly marked as “Watching” or “Watched” on Trakt.

### How it works
- A background **watcher** connects to your Plex Media Server (via WebSocket).
- Every play/pause/stop is converted into a **ScrobbleEvent**.
- The event is enriched with TMDb/Tvdb/IMDb IDs and sent to **Trakt’s `/scrobble` API**.
- Built-in **deduplication, retries, and fallbacks** ensure stable reporting.

## 📎 API Reference

The backend exposes a REST API at `http://localhost:8787`.  
Main routes include:

- `/api/status`, `/api/version`, `/api/config`
- `/api/auth/providers`, `/api/plex/pin/new`, `/api/simkl/authorize`
- `/api/sync/providers`, `/api/pairs`, `/api/run`
- `/api/watchlist`, `/api/metadata/resolve`, `/api/scheduling`
- `/api/stats`, `/api/logs/stream`, `/api/troubleshoot/*`

---

## ⚖️ License

MIT © [cenodude](https://github.com/cenodude)
