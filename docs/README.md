<div align="center"><img src="images/CrossWatch.png" alt="CrossWatch" width="480"></div>

<!-- Screenshots row  -->
<p align="center">
  <a href="images/screenshot1.jpg">
    <img src="images/screenshot1.jpg" alt="CrossWatch - Screenshot 1" width="180" style="border-radius:10px; margin:6px;">
  </a>
  <a href="images/screenshot2.jpg">
    <img src="images/screenshot2.jpg" alt="CrossWatch - Screenshot 2" width="180" style="border-radius:10px; margin:6px;">
  </a>
  <a href="images/screenshot3.jpg">
    <img src="images/screenshot3.jpg" alt="CrossWatch - Screenshot 3" width="180" style="border-radius:10px; margin:6px;">
  </a>
  <a href="images/screenshot4.jpg">
    <img src="images/screenshot4.jpg" alt="CrossWatch - Screenshot 4" width="180" style="border-radius:10px; margin:6px;">
  </a>
</p>

<p align="center" style="font-size:14px;">
  <b>⭐ Star this repository to get updates</b>
</p>

<div align="center"><img src="https://github.com/user-attachments/assets/7f4976b2-b6d7-4c69-9e1e-1612d0288a9f" alt="image" height="314" width="776"></div>

<p align="center"><a href="https://github.com/cenodude/CrossWatch/releases/latest"><img src="https://img.shields.io/github/v/release/cenodude/CrossWatch?display_name=release&#x26;sort=semver&#x26;logo=github&#x26;label=Latest%20Release&#x26;style=for-the-badge" alt="Latest Release"> </a><a href="https://github.com/cenodude/CrossWatch/pkgs/container/crosswatch"><img src="https://img.shields.io/badge/dynamic/json?url=https://ghcr-badge.elias.eu.org/api/cenodude/CrossWatch/crosswatch&#x26;query=%24.downloadCount&#x26;style=for-the-badge&#x26;logo=github&#x26;label=GHCR%20Pulls" alt="GHCR Pulls"> </a><a href="https://github.com/cenodude/CrossWatch/wiki/Getting-Started"><img src="https://img.shields.io/badge/Quick%20Start-Must%20read!-d93c4a?style=for-the-badge&#x26;logo=gitbook" alt="Must-read: Quick Start"></a><br><a href="https://hub.docker.com/r/cenodude/crosswatch"><img src="https://img.shields.io/docker/pulls/cenodude/crosswatch?style=for-the-badge&#x26;logo=docker&#x26;label=Docker%20Pulls" alt="Docker Pulls"> </a><a href="https://hub.docker.com/r/cenodude/crosswatch"><img src="https://img.shields.io/docker/image-size/cenodude/crosswatch/latest?style=for-the-badge&#x26;logo=docker&#x26;label=Image%20Size" alt="Image Size"> </a><a href="https://hub.docker.com/r/cenodude/crosswatch/tags"><img src="https://img.shields.io/docker/v/cenodude/crosswatch?sort=semver&#x26;style=for-the-badge&#x26;logo=docker&#x26;label=Docker%20Version" alt="Docker Version"></a></p>

**CrossWatch/CW** is a synchronization engine that keeps your **Plex, Jellyfin, Emby, SIMKL, Trakt, AniList, MDBList and Tautulli** in sync. It runs locally with a web UI where you link accounts, define sync pairs, run them manually or on a schedule, and review stats and history. CW also includes its own tracker to keep your data safe with snapshots.

Supported: **Movies** and **TV shows / episodes / Seasons**\
Supported: **Plex, Emby, Jellyfin, MDBList, Tautulli, AniList, Trakt, SIMKL and CW internal tracker**\
NOT supported: **Multi-users/servers**

<img
  align="right"
  src="https://github.com/user-attachments/assets/f219a392-839f-4ced-a263-1c745fbdf999"
  alt="CrossWatch mobile"
  width="170"
  style="max-width:170px; height:auto; margin:0 0 12px 16px;"
/>

### CW in a nutshell:
* **One brain for all your media syncs** A single place to configure and understand everything.
* **Multi media-server** and **multi tracker** support, in just one tool.
* **Mobile-friendly overview** that prioritizes only the essentials
* **Flexible sync directions** Between media server and trackers.
* **Simple and advanced scheduling** From “run once a day” to more detailed pair schedules
* **Internal CW Tracker** Keeps snapshots/backups from your media servers and trackers.
* **Unified Watchlist across providers** View all watchlist items in one place.
* **Fallback GUID** Revives items that left your Plex library but still exist in your server database.
* **Watcher** (Plex/Emby/Jellyfin to Trakt/SIMKL/MDBList) Plugin-free and subscription-free.
* **Watchlist Auto-Remove** Clears items from your Watchlist after a verified finish.
* **Analyzer** Finds items that are **stuck** or inconsistent between providers.
* **Editor** Inspect and adjust your items and add or block items.
* **Player card** Shows what you are currently watching in real time.

<!-- Features (no header row, titles visible, no "empty grid") -->
<table width="100%" border="0" cellspacing="0" cellpadding="0" style="border:0; border-collapse:collapse;">
  <tr>
    <td valign="top" width="50%" style="border:0; padding-right:18px;">
      <h4 style="margin:0 0 8px 0;">Core features</h4>
      <ul>
        <li>Sync watchlists (one-/two-way)</li>
        <li>Live scrobble (Plex/Jellyfin/Emby to Trakt/SIMKL/MDBList)</li>
        <li>Sync ratings (one-/two-way)</li>
        <li>Sync history (one-/two-way)</li>
        <li>Keep snapshots with CW tracker</li>
        <li>Auto-remove from watchlist after finish</li>
      </ul>
    </td>
    <td valign="top" width="50%" style="border:0; padding-left:18px;">
      <h4 style="margin:0 0 8px 0;">Tools &amp; modes</h4>
      <ul>
        <li>Analyzer: finds broken or missing matches/IDs</li>
        <li>Exporter: CSV files for popular services</li>
        <li>Editor: Edit and adjust your items</li>
        <li>Now Playing card, Stats, history, live logs</li>
        <li>Headless scheduled runs</li>
      </ul>
      <p style="margin:10px 0 6px 0;"><b>Trackers</b><br>
        <img src="https://img.shields.io/badge/CrossWatch-7C5CFF?labelColor=1f2328&amp;logoColor=white" alt="CrossWatch">
        <img src="https://img.shields.io/badge/SIMKL-0AAEEF?labelColor=1f2328" alt="SIMKL">
        <img src="https://img.shields.io/badge/AniList-02A9FF?labelColor=1f2328" alt="AniList">
        <img src="https://img.shields.io/badge/Trakt-ED1C24?labelColor=1f2328" alt="Trakt">
        <img src="https://img.shields.io/badge/MDBList-3B73B9?labelColor=1f2328" alt="MDBList">
      </p>
      <p style="margin:10px 0 6px 0;"><b>Media servers</b><br>
        <img src="https://img.shields.io/badge/Plex-E08A00?logo=plex&amp;logoColor=white&amp;labelColor=1f2328" alt="Plex">
        <img src="https://img.shields.io/badge/Jellyfin-946AD9?logo=jellyfin&amp;logoColor=white&amp;labelColor=1f2328" alt="Jellyfin">
        <img src="https://img.shields.io/badge/Emby-52B54B?logo=emby&amp;logoColor=white&amp;labelColor=1f2328" alt="Emby">
      </p>
      <p style="margin:10px 0 0 0;"><b>Others</b><br>
        <img src="https://img.shields.io/badge/Tautulli-FF5C5C?labelColor=1f2328" alt="Tautulli">
      </p>
    </td>
  </tr>
</table>
