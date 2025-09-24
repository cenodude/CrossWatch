<!-- Logo centered -->
<p align="center">
  <img src="images/CrossWatch.png" alt="CrossWatch" width="480">
</p>

<!-- Click-to-zoom screenshots (links to full size) -->
<p align="center">
  <a href="images/screenshot1.jpg">
    <img src="images/screenshot1.jpg" alt="CrossWatch — Screenshot 1" width="32%">
  </a>
  <a href="images/screenshot2.jpg">
    <img src="images/screenshot2.jpg" alt="CrossWatch — Screenshot 2" width="32%">
  </a>
  <a href="images/screenshot3.jpg">
    <img src="images/screenshot3.jpg" alt="CrossWatch — Screenshot 3" width="32%">
  </a>
</p>

<p align="center"><sub>Click any screenshot to view it full size.</sub></p>


**CrossWatch** is a lightweight synchronization engine that keeps your Plex, Jellyfin, Simkl, and Trakt in sync.  It runs locally with a clean web UI to link accounts, configure sync pairs, run them manually or on schedule, and track stats/history.  It also fully replaces my previous project Plex2SIMKL, with a more modular architecture and broader multi-provider support.

CrossWatch aims to become a one-for-all synchronization system for locally hosted environments. Its modular architecture allows new providers to be added easily. This approach keeps the system maintainable, testable, and easy to extend as new platforms emerge.


---

<div align="center">

⚠️ **EARLY DEVELOPMENT**  
This project is unstable and may break.  
**Always back up your data before use.**

</div>

---

<h2 align="center">🚀 Features</h2>

<div align="center">
  <table style="display:inline-block;text-align:left;">
    <tr>
      <td valign="top">
        <ul>
          <li>Sync watchlists (one-/two-way)</li>
          <li>Live scrobbling (Plex → Trakt)</li>
          <li>Sync ratings (one-/two-way)</li>
          <li>Sync watch history (one-/two-way — disabled)</li>
          <li>Sync playlists (one-/two-way — disabled)</li>
        </ul>
      </td>
      <td valign="top">
        <ul>
          <li>Simple web UI — JSON state</li>
          <li>TMDb metadata & posters</li>
          <li>Stats, history, live logs</li>
          <li>Headless scheduled runs</li>
          <li>Trackers: SIMKL, Trakt</li>
          <li>Media servers: Plex, Jellyfin (experimental)</li>
        </ul>
      </td>
    </tr>
  </table>
</div>

---

## 🐳 Run as Container

```bash
docker run -d   --name crosswatch   -p 8787:8787   -v /path/to/config:/config   -e TZ=Europe/Amsterdam   ghcr.io/cenodude/crosswatch:latest
```

or

```bash
# docker-compose.yml
services:
  crosswatch:
    image: ghcr.io/cenodude/crosswatch:latest
    container_name: crosswatch
    ports:
      - "8787:8787"          # host:container
    environment:
      - TZ=Europe/Amsterdam
    volumes:
      - /path/to/config:/config
    restart: unless-stopped
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
  - `auth` (Plex device PIN, SIMKL OAuth, TRAKT)
  - `sync` (PLEX ⇄ SIMKL ⇄ TRAKT)
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

Open **Settings → Troubleshoot** to access three quick-fix actions:

- **Clear cache** — Purges cached data so fresh metadata is fetched next time.  
  `POST /api/troubleshoot/clear-cache`
- **Reset stats** — Resets usage/summary counters used for insights.  
  `POST /api/troubleshoot/reset-stats`
- **Reset state** — Reinitializes app state (filters, view prefs, local UI). Linked accounts are not touched.  
  `POST /api/troubleshoot/reset-state`

---
# 🎬 Live Scrobbling (Plex → Trakt)

CrossWatch can **scrobble your real-time Plex playback to Trakt** — so episodes and movies you watch are instantly marked as “Watching” or “Watched” on Trakt.

### How it works
- A background **watcher** connects to your Plex Media Server (via WebSocket).
- Every play/pause/stop is converted into a **ScrobbleEvent**.
- The event is enriched with TMDb/Tvdb/IMDb IDs and sent to **Trakt’s `/scrobble` API**.
- Built-in **deduplication, retries, and fallbacks** ensure stable reporting.

---

## ⚖️ License

MIT © [cenodude](https://github.com/cenodude)
