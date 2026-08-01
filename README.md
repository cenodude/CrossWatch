<img width="1672" height="941" alt="image (1)" src="https://github.com/user-attachments/assets/567e8347-580a-4ea2-a239-d304fe08fc7d" />

</center>

<p align="center" style="font-size:14px;">
<b>⭐ Star this repository to get updates</b><br>
</p>


<p align="center">
  <a href="https://github.com/cenodude/CrossWatch/releases/latest">
    <img src="https://img.shields.io/github/v/release/cenodude/CrossWatch?display_name=release&amp;sort=semver&amp;logo=github&amp;label=Latest%20Release&amp;style=for-the-badge" alt="Latest Release">
  </a>
  <a href="https://github.com/cenodude/CrossWatch/pkgs/container/crosswatch">
    <img src="https://img.shields.io/badge/dynamic/json?url=https://ghcr-badge.elias.eu.org/api/cenodude/CrossWatch/crosswatch&amp;query=%24.downloadCount&amp;style=for-the-badge&amp;logo=github&amp;label=GHCR%20Pulls" alt="GHCR Pulls">
  </a>
  <a href="https://wiki.crosswatch.app/getting-started/first-time-setup">
    <img src="https://img.shields.io/badge/Quick%20Start-Must%20read!-d93c4a?style=for-the-badge&amp;logo=gitbook" alt="Must-read: Quick Start">
  </a>
  <br>
  <a href="https://hub.docker.com/r/cenodude/crosswatch">
    <img src="https://img.shields.io/docker/pulls/cenodude/crosswatch?style=for-the-badge&amp;logo=docker&amp;label=Docker%20Pulls" alt="Docker Pulls">
  </a>
  <a href="https://hub.docker.com/r/cenodude/crosswatch">
    <img src="https://img.shields.io/docker/image-size/cenodude/crosswatch/latest?style=for-the-badge&amp;logo=docker&amp;label=Image%20Size" alt="Image Size">
  </a>
  <a href="https://hub.docker.com/r/cenodude/crosswatch/tags">
    <img src="https://img.shields.io/docker/v/cenodude/crosswatch?sort=semver&amp;style=for-the-badge&amp;logo=docker&amp;label=Docker%20Version" alt="Docker Version">
  </a>
</p>
<p align="center">

  <a href="https://www.crosswatch.app/" style="margin: 0 6px;">
    <img alt="Website" src="https://img.shields.io/badge/Website-crosswatch.app-B026FF?style=for-the-badge">
  </a>
  <a href="https://wiki.crosswatch.app/" style="margin: 0 6px;">
    <img alt="Wiki" src="https://img.shields.io/badge/Wiki-wiki.crosswatch.app-B026FF?style=for-the-badge">
  </a>
</p>


**CrossWatch (CW)** is a synchronization engine that keeps your **Plex, Jellyfin, Emby, SIMKL, Floppy, Trakt, AniList, TMDb, MDBList, PublicMetaDB, Tautulli, Kodi, Nuvio, Stremio and CW local tracker** in sync. It runs locally with a web UI where you link accounts, define sync pairs, run them manually or on a schedule, and review stats and history. CW also includes its own tracker to keep your data safe with snapshots. With Profiles, you can manage separate sync setups for yourself and for friends or family too, with their own servers and/or tracker API's.

### CW in a nutshell:
* **One brain for all your media syncs** A single place to configure everything.
* **Be your own Sync Hub** Create profiles for seperate media servers/users/trackers.
* **Multi media-server** and **multi tracker** support with profiles.
* **Synchronization**
  * Watchlists, Ratings, History and Progress
  * Anime ID mapping (powered AniBridge) for AniList matching across providers.
* **Scrobble (tracks your activity)**
  * **Watcher** (Plex/Emby/Jellyfin/Kodi to Trakt/SIMKL/MDBList/Floppy/CW)
    * Does not require Plex Pass or Emby Premiere. Yay!
  * **Webhooks** (Plex/Emby/Jellyfin to Trakt/SIMKL/MDBList)
* **Tools**
  * Analyzer: Finds items that are **stuck** or inconsistent between providers.
  * Playback Progress Manager: View and edit unfinished playback sessions across providers.
  * Editor: Inspect and adjust your items and add or block items.
  * Events Viewer: Search and inspect sync runs.
  * Captures: Rollback tool for provider watchlist, ratings, and history.

And much more...such as:
* Simple and advanced scheduling: From standard to more detailed pair schedules
* CW Tracker Keeps snapshots/backups from your media servers and trackers.
* Unified Watchlist: View all watchlist items in one place.
* Player card: Shows what you are currently watching in real time.
* Fallback GUID: Revives old items from  your Plex library.

### Download
[![Guide: Installation](https://img.shields.io/badge/Guide-INSTALLATION-0d6efd?style=for-the-badge)](https://wiki.crosswatch.app/getting-started/installation)


*   **Docker:**

    ```bash
    docker pull ghcr.io/cenodude/crosswatch:latest
    ```

### Run as Container

```bash
docker run -d \
  --name crosswatch \
  -p 8787:8787 \
  -v crosswatch_config:/config \
  -e TZ=Europe/Amsterdam \
  --restart unless-stopped \
  ghcr.io/cenodude/crosswatch:latest
```

or

```bash
services:
  crosswatch:
    image: ghcr.io/cenodude/crosswatch:latest
    container_name: crosswatch
    ports:
      - "8787:8787"
    environment:
      TZ: Europe/Amsterdam
    volumes:
      - type: volume
        source: crosswatch_config
        target: /config
    restart: unless-stopped

volumes:
  crosswatch_config:
```

> The container exposes the web UI at:\
> http://localhost:8787

## Sponsors

<div align="center">

<a href="https://www.buymeacoffee.com/cenodude">
  <img alt="Buy Me a Coffee" src="https://img.shields.io/badge/Buy%20Me%20a%20Coffee-support-ffdd00?style=for-the-badge&logo=buy-me-a-coffee&logoColor=000000">
</a><center><br>
Every cent goes to the <b>ALS Foundation</b> in the Netherlands</center>
<br/>
<br/>

<a href="https://www.gitbook.com/">
  <img alt="GitBook" src="https://img.shields.io/badge/GitBook-sponsored-3884ff?style=for-the-badge&logo=gitbook&logoColor=white">
</a>

</div>

<p align="center">
  Huge thanks to our sponsors for keeping this project moving.
</p>


