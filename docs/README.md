
<div align="center"><img src="images/CrossWatch.png" alt="CrossWatch" width="480"></div>

<p align="center">
  <a class="cw-cta" href="https://github.com/cenodude/CrossWatch">
    <span class="cw-cta-icon" aria-hidden="true"></span>
    Join us on GitHub
  </a>
</p>

<!-- Screenshots (small thumbs, click to zoom) -->
<div class="cw-shotgrid is-small" align="center">
  <a href="images/screenshot1.jpg" data-cw-gallery="cw-screens" aria-label="Open screenshot 1">
    <img src="images/screenshot1.jpg" alt="CrossWatch - Screenshot 1" width="180" loading="lazy" decoding="async">
  </a>
  <a href="images/screenshot2.jpg" data-cw-gallery="cw-screens" aria-label="Open screenshot 2">
    <img src="images/screenshot2.jpg" alt="CrossWatch - Screenshot 2" width="180" loading="lazy" decoding="async">
  </a>
  <a href="images/screenshot3.jpg" data-cw-gallery="cw-screens" aria-label="Open screenshot 3">
    <img src="images/screenshot3.jpg" alt="CrossWatch - Screenshot 3" width="180" loading="lazy" decoding="async">
  </a>
  <a href="images/screenshot4.jpg" data-cw-gallery="cw-screens" aria-label="Open screenshot 4">
    <img src="images/screenshot4.jpg" alt="CrossWatch - Screenshot 4" width="180" loading="lazy" decoding="async">
  </a>
</div>

<p align="center"><sub>Click any screenshot to view it full size.</sub></p>


<img
  alt="providers"
  src="https://github.com/user-attachments/assets/fa833ac6-ff96-440d-bfc0-5f749120af8c"
  width="900"
/>

<p align="center"><a href="https://github.com/cenodude/CrossWatch/releases/latest"><img src="https://img.shields.io/github/v/release/cenodude/CrossWatch?display_name=release&#x26;sort=semver&#x26;logo=github&#x26;label=Latest%20Release&#x26;style=for-the-badge" alt="Latest Release"> </a><a href="https://github.com/cenodude/CrossWatch/pkgs/container/crosswatch"><img src="https://img.shields.io/badge/dynamic/json?url=https://ghcr-badge.elias.eu.org/api/cenodude/CrossWatch/crosswatch&#x26;query=%24.downloadCount&#x26;style=for-the-badge&#x26;logo=github&#x26;label=GHCR%20Pulls" alt="GHCR Pulls"> </a><a href="https://github.com/cenodude/CrossWatch/wiki/Getting-Started"><img src="https://img.shields.io/badge/Quick%20Start-Must%20read!-d93c4a?style=for-the-badge&#x26;logo=gitbook" alt="Must-read: Quick Start"></a><br><a href="https://hub.docker.com/r/cenodude/crosswatch"><img src="https://img.shields.io/docker/pulls/cenodude/crosswatch?style=for-the-badge&#x26;logo=docker&#x26;label=Docker%20Pulls" alt="Docker Pulls"> </a><a href="https://hub.docker.com/r/cenodude/crosswatch"><img src="https://img.shields.io/docker/image-size/cenodude/crosswatch/latest?style=for-the-badge&#x26;logo=docker&#x26;label=Image%20Size" alt="Image Size"> </a><a href="https://hub.docker.com/r/cenodude/crosswatch/tags"><img src="https://img.shields.io/docker/v/cenodude/crosswatch?sort=semver&#x26;style=for-the-badge&#x26;logo=docker&#x26;label=Docker%20Version" alt="Docker Version"></a></p>
<p align="center">
  <a href="https://www.crosswatch.app/" style="margin: 0 6px;">
    <img alt="Website" src="https://img.shields.io/badge/Website-crosswatch.app-B026FF?style=for-the-badge">
  </a>
  <a href="https://wiki.crosswatch.app/" style="margin: 0 6px;">
    <img alt="Wiki" src="https://img.shields.io/badge/Wiki-wiki.crosswatch.app-B026FF?style=for-the-badge">
  </a>
</p>

**CrossWatch/CW** is a synchronization engine that keeps your **Plex, Jellyfin, Emby, SIMKL, Trakt, AniList, TMDb, MDBList and Tautulli** in sync. It runs locally with a web UI where you link accounts, define sync pairs, run them manually or on a schedule, and review stats and history. CW also includes its own tracker to keep your data safe with snapshots.

Supported: **Movies** and **TV shows / episodes / Seasons**\
Supported: **Plex, Emby, Jellyfin, MDBList, Tautulli, AniList, Trakt, SIMKL, TMDb and CW internal tracker**\
NOT supported: **Multi-users/servers**

### CW in a nutshell:
* **One brain for all your media syncs** A single place to configure everything.
* **Multi media-server** and **multi tracker** support, in just one tool.
* **Mobile-friendly overview** that prioritizes only the essentials
* **Flexible sync directions** Between media server and trackers.
* **Simple and advanced scheduling** From standard to more detailed pair schedules
* **Internal CW Tracker** Keeps snapshots/backups from your media servers and trackers.
* **Unified Watchlist across providers** View all watchlist items in one place.
* **Fallback GUID** Revives old items from  your Plex library.
* **Watcher** (Plex/Emby/Jellyfin to Trakt/SIMKL/MDBList) subscription-free.
* **Watchlist Auto-Remove** Clears items from your Watchlist after a verified finish.
* **Analyzer** Finds items that are **stuck** or inconsistent between providers.
* **Editor** Inspect and adjust your items and add or block items.
* **Player card** Shows what you are currently watching in real time.
* **Snapshosts** Rollback tool for provider watchlist, ratings, and history

<div align="center"><img src="https://github.com/user-attachments/assets/86098e05-7250-4e66-9ac5-cc75623d9920" alt="image"></div>

### Features

<table data-header-hidden>
  <thead>
    <tr>
      <th valign="top"></th>
      <th valign="top"></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td valign="top">
        <p>Core features</p>
        <ul>
          <li>Sync watchlists (one-/two-way)</li>
          <li>Live scrobble (Plex/Jellyfin/Emby to Trakt/SIMKL/MDBList)</li>
          <li>Sync ratings (one-/two-way)</li>
          <li>Sync history (one-/two-way)</li>
          <li>Keep snapshots with CW tracker</li>
          <li>Auto-remove from watchlist after finish</li>
        </ul>
      </td>
      <td valign="top">
        <p>Tools &#x26; modes</p>
        <ul>
          <li>Analyzer: finds broken or missing matches/IDs</li>
          <li>Exporter: CSV files for popular services</li>
          <li>Editor: Edit and adjust your items</li>
          <li>Now Playing card, Stats, history, live logs</li>
          <li>Headless scheduled runs</li>
        </ul>

        <p>Trackers<br>
          <img src="https://img.shields.io/badge/CrossWatch-7C5CFF?labelColor=1f2328&#x26;logoColor=white" alt="CrossWatch">
          <img src="https://img.shields.io/badge/SIMKL-0AAEEF?labelColor=1f2328" alt="SIMKL">
          <img src="https://img.shields.io/badge/AniList-02A9FF?labelColor=1f2328" alt="AniList">
          <img src="https://img.shields.io/badge/Trakt-ED1C24?labelColor=1f2328" alt="Trakt">
          <img src="https://img.shields.io/badge/MDBList-3B73B9?labelColor=1f2328" alt="MDBList">
          <img src="https://img.shields.io/badge/TMDb-01B4E4?labelColor=1f2328&#x26;logo=themoviedatabase&#x26;logoColor=white" alt="TMDb">
        </p>

        <p>Media servers<br>
          <img src="https://img.shields.io/badge/Plex-E08A00?logo=plex&#x26;logoColor=white&#x26;labelColor=1f2328" alt="Plex">
          <img src="https://img.shields.io/badge/Jellyfin-946AD9?logo=jellyfin&#x26;logoColor=white&#x26;labelColor=1f2328" alt="Jellyfin">
          <img src="https://img.shields.io/badge/Emby-52B54B?logo=emby&#x26;logoColor=white&#x26;labelColor=1f2328" alt="Emby">
        </p>

        <p>Others<br>
          <img src="https://img.shields.io/badge/Tautulli-FF5C5C?labelColor=1f2328" alt="Tautulli">
        </p>
      </td>
    </tr>
  </tbody>
</table>
