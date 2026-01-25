# docs

<div align="center"><img src="../.gitbook/assets/CrossWatch (1).png" alt="CrossWatch" width="480"></div>

<p align="center"><a href="https://github.com/cenodude/CrossWatch">Join us on GitHub</a></p>

[![CrossWatch - Screenshot 1](<../.gitbook/assets/screenshot1 (1).jpg>) ](images/screenshot1.jpg)[![CrossWatch - Screenshot 2](<../.gitbook/assets/screenshot2 (1).jpg>) ](images/screenshot2.jpg)[![CrossWatch - Screenshot 3](<../.gitbook/assets/screenshot3 (1).jpg>) ](images/screenshot3.jpg)[![CrossWatch - Screenshot 4](<../.gitbook/assets/screenshot4 (1).jpg>)](images/screenshot4.jpg)

<p align="center"><sub>Click any screenshot to view it full size.</sub></p>

<p align="center"><a href="https://github.com/cenodude/CrossWatch/releases/latest"><img src="https://img.shields.io/github/v/release/cenodude/CrossWatch?display_name=release&#x26;sort=semver&#x26;logo=github&#x26;label=Latest%20Release&#x26;style=for-the-badge" alt="Latest Release"> </a><a href="https://github.com/cenodude/CrossWatch/pkgs/container/crosswatch"><img src="https://img.shields.io/badge/dynamic/json?url=https://ghcr-badge.elias.eu.org/api/cenodude/CrossWatch/crosswatch&#x26;query=%24.downloadCount&#x26;style=for-the-badge&#x26;logo=github&#x26;label=GHCR%20Pulls" alt="GHCR Pulls"> </a><a href="https://github.com/cenodude/CrossWatch/wiki/Getting-Started"><img src="https://img.shields.io/badge/Quick%20Start-Must%20read!-d93c4a?style=for-the-badge&#x26;logo=gitbook" alt="Must-read: Quick Start"></a><br><a href="https://hub.docker.com/r/cenodude/crosswatch"><img src="https://img.shields.io/docker/pulls/cenodude/crosswatch?style=for-the-badge&#x26;logo=docker&#x26;label=Docker%20Pulls" alt="Docker Pulls"> </a><a href="https://hub.docker.com/r/cenodude/crosswatch"><img src="https://img.shields.io/docker/image-size/cenodude/crosswatch/latest?style=for-the-badge&#x26;logo=docker&#x26;label=Image%20Size" alt="Image Size"> </a><a href="https://hub.docker.com/r/cenodude/crosswatch/tags"><img src="https://img.shields.io/docker/v/cenodude/crosswatch?sort=semver&#x26;style=for-the-badge&#x26;logo=docker&#x26;label=Docker%20Version" alt="Docker Version"></a></p>

**CrossWatch/CW** is a synchronization engine that keeps your **Plex, Jellyfin, Emby, SIMKL, Trakt, AniList, MDBList and Tautulli** in sync. It runs locally with a web UI where you link accounts, define sync pairs, run them manually or on a schedule, and review stats and history. CW also includes its own tracker to keep your data safe with snapshots.

Supported: **Movies** and **TV shows / episodes / Seasons**\
Supported: **Plex, Emby, Jellyfin, MDBList, Tautulli, AniList, Trakt, SIMKL and CW internal tracker**\
NOT supported: **Multi-users/servers**

### CW in a nutshell:

* **One brain for all your media syncs** A single place to configure and understand everything.
* **Multi-server** (Plex, Jellyfin, Emby) and multi-tracker (Trakt, SIMKL, MDBList, AniList) in one tool.
* **Flexible sync directions** Between media server. Between trackers. Or from/to media servers and trackers.
* **Simple and advanced scheduling** From “run once a day” to more detailed, time-based pair schedules
* **Internal CW Tracker** Keeps snapshots/backups from your media servers and trackers.
* **Unified Watchlist across providers** View all watchlist items in one place.
* **Back-to-the-Future (Fallback GUID)** Revives items that left your Plex library but still exist in your server database.
* **Watcher** (Plex / Emby / Jellyfin to Trakt/SIMKL/MDBList. Realtime, Plugin-free and subscription-free.
* **Watchlist Auto-Remove** Clears items from your Watchlist after a verified finish.
* **Analyzer** Finds items that are **stuck** or inconsistent between providers.
* **Editor** Inspect and adjust your items and Add or block items. Example: tell Plex to stop sending movie X.
* **Player card** (Webhooks and Watcher) Shows what you are currently watching in real time while Webhooks or Watcher are active.

<div align="center"><img src="https://github.com/user-attachments/assets/86098e05-7250-4e66-9ac5-cc75623d9920" alt="image"></div>

### Features

<table data-header-hidden><thead><tr><th valign="top"></th><th valign="top"></th></tr></thead><tbody><tr><td valign="top"><p>Core features</p><ul><li>Sync watchlists (one-/two-way)</li><li>Live scrobble (Plex/Jellyfin/Emby to Trakt/SIMKL/MDBList)</li><li>Sync ratings (one-/two-way)</li><li>Sync history (one-/two-way)</li><li>Keep snapshots with CW tracker</li><li>Auto-remove from watchlist after finish</li></ul></td><td valign="top"><p>Tools &#x26; modes</p><ul><li>Analyzer: finds broken or missing matches/IDs</li><li>Exporter: CSV files for popular services</li><li>Editor: Edit and adjust your items</li><li>Now Playing card, Stats, history, live logs</li><li>Headless scheduled runs</li></ul><p>Trackers<br><img src="https://img.shields.io/badge/CrossWatch-7C5CFF?labelColor=1f2328&#x26;logoColor=white" alt="CrossWatch">  <img src="https://img.shields.io/badge/SIMKL-0AAEEF?labelColor=1f2328" alt="SIMKL">  <img src="https://img.shields.io/badge/AniList-02A9FF?labelColor=1f2328" alt="AniList">  <img src="https://img.shields.io/badge/Trakt-ED1C24?labelColor=1f2328" alt="Trakt">  <img src="https://img.shields.io/badge/MDBList-3B73B9?labelColor=1f2328" alt="MDBList"></p><p>Media servers<br><img src="https://img.shields.io/badge/Plex-E08A00?logo=plex&#x26;logoColor=white&#x26;labelColor=1f2328" alt="Plex">  <img src="https://img.shields.io/badge/Jellyfin-946AD9?logo=jellyfin&#x26;logoColor=white&#x26;labelColor=1f2328" alt="Jellyfin">  <img src="https://img.shields.io/badge/Emby-52B54B?logo=emby&#x26;logoColor=white&#x26;labelColor=1f2328" alt="Emby"></p><p>Others<br><img src="https://img.shields.io/badge/Tautulli-FF5C5C?labelColor=1f2328" alt="Tautulli"></p></td></tr></tbody></table>
