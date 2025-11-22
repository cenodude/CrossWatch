# CrossWatch

<!-- Logo centered -->
<p align="center">
  <img src="images/CrossWatch.png" alt="CrossWatch" width="480">
</p>

<!-- Click-to-zoom screenshots (links to full size) -->
<p align="center">
  <a href="images/screenshot1.jpg">
    <img src="images/screenshot1.jpg" alt="CrossWatch — Screenshot 1" width="24%">
  </a>
  <a href="images/screenshot2.jpg">
    <img src="images/screenshot2.jpg" alt="CrossWatch — Screenshot 2" width="24%">
  </a>
  <a href="images/screenshot3.jpg">
    <img src="images/screenshot3.jpg" alt="CrossWatch — Screenshot 3" width="24%">
  </a>
  <a href="images/screenshot4.jpg">
    <img src="images/screenshot4.jpg" alt="CrossWatch — Screenshot 4" width="24%">
  </a>
</p>

<!-- Caption directly under screenshots -->
<p align="center"><sub>Click any screenshot to view it full size.</sub></p>
<p align="center">
  <img width="788" height="236" alt="image" src="https://github.com/user-attachments/assets/a678ebee-d750-45d2-8dab-a8550f913670" />
</p>

<!-- Download + Wiki + Best Practices (badge-style, GitHub-friendly) -->
<p align="center">
  <a href="https://github.com/cenodude/CrossWatch/releases/latest">
    <img
      alt="Latest Release"
      src="https://img.shields.io/github/v/release/cenodude/CrossWatch?display_name=release&sort=semver&logo=github&label=Latest%20Release&style=for-the-badge">
  </a>
  <a href="https://github.com/cenodude/CrossWatch/wiki/Best-Practices">
    <img
      alt="Must-read: Best Practices"
      src="https://img.shields.io/badge/Best%20Practices-Must%20read!-d93c4a?style=for-the-badge&logo=gitbook">
  </a>
  <a href="https://github.com/cenodude/CrossWatch/wiki">
  </a>
</p>

<!-- Nudge -->
<p align="center">
  <sub>At minimum, read the <a href="https://github.com/cenodude/CrossWatch/wiki/Best-Practices"><strong>Best Practices</strong></a> page before enabling two-way sync or server→server writes.</sub>
</p>

**CrossWatch** is a lightweight synchronization engine that keeps your **Plex, Jellyfin, Emby, Simkl, Trakt and Mdblist** in sync. It runs locally with a clean web UI to link accounts, configure sync pairs, run them manually or on schedule, and track stats/history.

## Why is CrossWatch different? (in a nutshell)
- One brain for all your media syncs.
- Multi-server (Plex, Jellyfin, Emby) and multi-tracker (Trakt, SIMKL, Mdblist) in one tool.
  - No API? Use the **Exporter** to dump Watchlist/History/Ratings CSVs (TMDb, Letterboxd, etc.).
- Sync between media servers: Plex, Jellyfin, Emby - Sync between trackers: SIMKL, TRAKT - or Sync from/to media servers and trackers.
  - Also great for backups or to keep your media servers in sync.
- Beautiful UI, rich debug logs, and lots of sensible toggles.
- Simple **and** advanced scheduling for real freedom.
- Unified, visual Watchlist across providers — filter, search, bulk remove, etc.
- **Back-to-the-Future (Fallback GUID)**  
  - Revives old items that left your library but still hide in your server DB, hello, ancient Plex memories.
- **Intelligent Webhooks** (Plex / Jellyfin / Emby) -> Trakt
  - Plex autoplay quarantine: skip credits without losing “now playing” on Trakt.
  - Many (advanced) filters, multi-ID matching, hardened STOP/PAUSE, etc.
- **Watcher** (Plex / Emby) -> Trakt and/or SIMKL
  - Plugin-free, subscription-free; just works.
  - Same guardrails for clean history; smart regress/duplicate suppression.
  - Multi-ID matching with layered fallbacks.
- **Watchlist Auto-Remove**
  - Clears items from your Watchlist after a **verified finish**
  - Watchlists only, your libraries stay untouched.
  - Type-aware: movies by default; shows/episodes optional.
- **Analyzer**
  - Finds items **stuck** between providers.
  - Shows **Issues: N** per your sync pairs with concrete fix hints.

<h2 align="center">🚀 Features</h2>
<div align="center">
  <table style="display:inline-block;text-align:left;">
    <tr>
      <td valign="top">
        <ul style="margin:0;padding-left:1.1em">
          <li>Sync watchlists (one-/two-way)</li>
          <li>Live scrobble (Plex/Jellyfin/Emby → Trakt)</li>
          <li>Sync ratings (one-/two-way)</li>
          <li>Sync watch history (one-/two-way)</li>
          <li>Sync playlists (one-/two-way — disabled)</li>
          <li>Auto-remove from watchlist after finish</li>
        </ul>
      </td>
      <td valign="top">
        <ul style="margin:0;padding-left:1.1em">
          <li>Analyzer - finds broken or missing matches/IDs</li>
          <li>Exporter - CSV files for popular service</li>
          <li>Stats, history, live logs</li>
          <li>Headless scheduled runs</li>
          <li><strong>Trackers:</strong>
            <img alt="SIMKL" src="https://img.shields.io/badge/SIMKL-0AAEEF?labelColor=1f2328" />
            &nbsp;<img alt="Trakt" src="https://img.shields.io/badge/Trakt-ED1C24?logo=trakt&logoColor=white&labelColor=1f2328" />
            &nbsp;<img alt="MDBList" src="https://img.shields.io/badge/MDBList-3B73B9?labelColor=1f2328" />
          </li>
          <li><strong>Media servers:</strong>
            <img alt="Plex" src="https://img.shields.io/badge/Plex-FFA620?logo=plex&logoColor=black&labelColor=1f2328" />
            &nbsp;<img alt="Jellyfin" src="https://img.shields.io/badge/Jellyfin-946AD9?logo=jellyfin&logoColor=white&labelColor=1f2328" />
            &nbsp;<img alt="Emby" src="https://img.shields.io/badge/Emby-52B54B?logo=emby&logoColor=white&labelColor=1f2328" />
          </li>
        </ul>
      </td>
    </tr>
  </table>
</div>

## ⬇️ Download

- **Docker (recommended):**
  ```bash
  docker pull ghcr.io/cenodude/crosswatch:latest
  ```
- **Prebuilt releases:**  
  Get the latest builds and assets here → **[Releases ▸](https://github.com/cenodude/CrossWatch/releases/latest)**

<sub>Tip: use <code>:latest</code> for stable, or a specific tag like <code>:v0.2.x</code>.</sub>

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

By default <code>CONFIG_BASE</code> will be <code>/config</code> inside the container.  
Your <code>config.json</code>, <code>state.json</code>, <code>statistics.json</code>, etc. will all be stored there.

---

## 📋 Usage

1. Open the web UI  
2. Connect at least two authentication providers — Plex, Jellyfin, <b>Emby</b>, SIMKL and/or TRAKT  
3. Create one or more <b>Sync Pairs</b> (e.g. Plex → SIMKL or two-way) and/or enable Scrobble  
4. Click <b>Synchronize</b> to start, or enable scheduling in <b>Settings</b>  
5. Track stats, logs, and history from the UI

---

## 🎬 Live Scrobbling (Plex/Jellyfin/Emby → Trakt and/or SIMKL)
CrossWatch can <b>scrobble your real-time Plex, Jellyfin, and Emby playback to Trakt</b> — so episodes and movies you watch are instantly marked as “Watching” or “Watched” on Trakt.  
Have Plex Pass / Emby Premiere? Prefer <b>Webhook</b>. No Pass/Premiere? Use the <b>Watcher</b>. Jellyfin users: use <b>Webhook</b>.

---

## Architecture
<p align="center">
<img width="268" height="408" alt="image" src="https://github.com/user-attachments/assets/c3461874-e1e3-4b3a-b4ed-c4fab0e7bc5b" />
</p>

