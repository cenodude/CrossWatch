# CrossWatch
<p align="center">
  <img src="images/CrossWatch.png" alt="CrossWatch" width="480">
</p>

<p align="center">
  <a href="images/screenshot1.jpg">
    <img src="images/screenshot1.jpg" alt="CrossWatch - Screenshot 1" width="24%">
  </a>
  <a href="images/screenshot2.jpg">
    <img src="images/screenshot2.jpg" alt="CrossWatch - Screenshot 2" width="24%">
  </a>
  <a href="images/screenshot3.jpg">
    <img src="images/screenshot3.jpg" alt="CrossWatch - Screenshot 3" width="24%">
  </a>
  <a href="images/screenshot4.jpg">
    <img src="images/screenshot4.jpg" alt="CrossWatch - Screenshot 4" width="24%">
  </a>
</p>

<p align="center"><sub>Click any screenshot to view it full size.</sub></p>
<p align="center">
  <img width="783" height="225" alt="image" src="https://github.com/user-attachments/assets/4337e4b3-b641-4826-9460-d4be441ddc14" />
</p>
<p align="center">
  <a href="https://github.com/cenodude/CrossWatch/releases/latest">
    <img
      alt="Latest Release"
      src="https://img.shields.io/github/v/release/cenodude/CrossWatch?display_name=release&sort=semver&logo=github&label=Latest%20Release&style=for-the-badge">
  </a>
  <a href="https://github.com/cenodude/CrossWatch/wiki/Getting-Started">
    <img
      alt="Must-read: Quick Start"
      src="https://img.shields.io/badge/Quick%20Start-Must%20read!-d93c4a?style=for-the-badge&logo=gitbook">
  </a>
  <a href="https://github.com/cenodude/CrossWatch/wiki">
  </a>
</p>

<p align="center">
  <sub>At minimum, read the <a href="https://github.com/cenodude/CrossWatch/wiki/Best-Practices"><strong>Best Practices</strong></a> before enabling two-way sync or media server to media server writes.</sub>
</p>

**CrossWatch** is a synchronization engine that keeps your **Plex, Jellyfin, Emby, SIMKL, Trakt and MDBlist** in sync.  
It runs locally with a web UI where you link accounts, define sync pairs, run them manually or on a schedule, and review stats and history.  
CrossWatch also includes its own tracker to keep your data safe with snapshots.  

Supported: **Movies** and **TV shows / episodes / Seasons**
Not supported: **Anime** (not yet) and **Multi-users/servers**

## CrossWatch in a nutshell:
- **One brain for all your media syncs** A single place to configure and understand everything.
- **Multi-server** (Plex, Jellyfin, Emby) and multi-tracker (Trakt, SIMKL, MDBlist) in one tool.  
- **Flexible sync directions** Between media server. Between trackers. Or from/to media servers and trackers.  
- **Simple and advanced scheduling** From “run once a day” to more detailed, time-based pair schedules
- **Internal CrossWatch Tracker** Keeps snapshots/backups of your Watchlist, History and Ratings from your media servers and trackers.
- **Unified Watchlist across providers** View all watchlist items in one place, with filter, search, bulk-remove and more.
- **Back-to-the-Future (Fallback GUID)** Revives items that left your Plex library but still exist in your server database.
- **Webhooks** (Plex / Jellyfin / Emby to Trakt)  
- **Watcher** (Plex / Emby to Trakt and/or SIMKL) Plugin-free and subscription-free.
- **Watchlist Auto-Remove** Clears items from your Watchlist after a verified finish. 
- **Analyzer** Finds items that are **stuck** or inconsistent between providers.
- **Editor** Inspect and adjust CrossWatch data. Add or block items. Example: tell Plex to stop sending movie X because you do not want it.
- **Player card** (Webhooks and Watcher) Shows what you are currently watching in real time while Webhooks or Watcher are active.
<p align="center">
  <img alt="image"
       src="https://github.com/user-attachments/assets/86098e05-7250-4e66-9ac5-cc75623d9920"
       style="max-width: 100%; height: auto;" />
</p>

<h2>Features</h2>

<div align="center">
  <table style="display:inline-block; text-align:left; border:0 !important; border-collapse:collapse !important;">
    <tr>
      <td valign="top" style="border:0 !important; padding-right:24px;">
        <ul style="margin:0; padding-left:1.1em;">
          <li>Sync watchlists (one-/two-way)</li>
          <li>Live scrobble (Plex/Jellyfin/Emby to Trakt)</li>
          <li>Sync ratings (one-/two-way)</li>
          <li>Sync watch history (one-/two-way)</li>
          <li>Keep snapshots with CrossWatch tracker</li>
          <li>Auto-remove from watchlist after finish</li>
        </ul>
      </td>
      <td valign="top" style="border:0 !important;">
        <ul style="margin:0; padding-left:1.1em;">
          <li>Analyzer - finds broken or missing matches/IDs</li>
          <li>Exporter - CSV files for popular service</li>
          <li>Now Playing card, Stats, history, live logs</li>
          <li>Headless scheduled runs</li>
          <li><strong>Trackers:</strong>
            <img alt="CrossWatch" src="https://img.shields.io/badge/CrossWatch-7C5CFF?labelColor=1f2328&logoColor=white" />
            &nbsp;<img alt="SIMKL" src="https://img.shields.io/badge/SIMKL-0AAEEF?labelColor=1f2328" />
            &nbsp;<img alt="Trakt" src="https://img.shields.io/badge/Trakt-ED1C24?labelColor=1f2328" />
            &nbsp;<img alt="MDBList" src="https://img.shields.io/badge/MDBList-3B73B9?labelColor=1f2328" />
          </li>
          <li><strong>Media servers:</strong>
            <img alt="Plex" src="https://img.shields.io/badge/Plex-E08A00?logo=plex&logoColor=white&labelColor=1f2328" />
            &nbsp;<img alt="Jellyfin" src="https://img.shields.io/badge/Jellyfin-946AD9?logo=jellyfin&logoColor=white&labelColor=1f2328" />
            &nbsp;<img alt="Emby" src="https://img.shields.io/badge/Emby-52B54B?logo=emby&logoColor=white&labelColor=1f2328" />
          </li>
        </ul>
      </td>
    </tr>
  </table>
</div>

## Download

- **Docker:**
  ```bash
  docker pull ghcr.io/cenodude/crosswatch:latest
  ```
- **Prebuilt releases:**  
  Get the latest builds and assets here: **[Releases ▸](https://github.com/cenodude/CrossWatch/releases/latest)**

<sub>Tip: use <code>:latest</code> for stable, or a specific tag like <code>:v0.2.x</code>.</sub>

---

## Run as Container

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
> http://localhost:8787

By default <code>CONFIG_BASE</code> will be <code>/config</code> inside the container.  
Your <code>config.json</code>, <code>state.json</code>, <code>statistics.json</code>, etc. will all be stored there.

---

## Usage

1. Open the web UI  
2. Connect at least one authentication providers such as: Plex, Jellyfin, Emby, SIMKL, TRAKT etc.
3. Create one or more <b>Sync Pairs</b> (for example: Plex to SIMKL or two-way) AND/OR enable Scrobble  
4. Click <b>Synchronize</b> to start, or enable scheduling in <b>Settings</b>  
5. Track stats, logs, and history from the UI

---

## Live Scrobbling (Plex/Jellyfin/Emby to Trakt and/or SIMKL)
CrossWatch can <b>scrobble your real-time Plex, Jellyfin, and Emby playback to Trakt</b>, so episodes and movies you watch are instantly marked as “Watching” or “Watched” on Trakt/SIMKL.  
Have Plex Pass / Emby Premiere? you can use <b>Webhook</b>. No Pass/Premiere? Use the <b>Watcher</b>. Jellyfin users: use <b>Webhook</b>. 
Personally, I prefer the watcher because it’s real-time.

---

## Security

CrossWatch is NOT meant to be exposed directly to the public internet.
    During the current development stage there is also no authentication built in, so treat it as a LAN/VPN-only tool.

- Do **NOT** port-forward `8787` from your router or expose the web UI directly to WAN. 
- Run CrossWatch on your **local network** only, or access it via:
  - a **VPN** (WireGuard, Tailscale, etc.)
- Anyone who can reach the web UI can change sync pairs, tokens and settings, which may:
  - delete or corrupt watch history / ratings / watchlists,
  - cause unwanted writes between servers/trackers,
  - leak information about your media libraries and accounts.
