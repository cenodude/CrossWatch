
<img width="1468" height="795" alt="overview (1)" src="https://github.com/user-attachments/assets/d4afb18c-b655-4aa7-ba2b-da10f3cef89a" />
</center>

<p align="center" style="font-size:14px;">
<b>⭐ Star this repository to get updates</b><br>
</p>
<img width="1566" height="631" alt="providers1 (1)" src="https://github.com/user-attachments/assets/fa833ac6-ff96-440d-bfc0-5f749120af8c" />
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
<img
  align="right"
  src="https://github.com/user-attachments/assets/f219a392-839f-4ced-a263-1c745fbdf999"
  alt="CrossWatch mobile"
  width="170"
  style="max-width:170px; height:auto; margin:0 0 12px 16px;"
/>

**CrossWatch/CW** is a synchronization engine that keeps your **Plex, Jellyfin, Emby, SIMKL, Trakt, AniList, TMDb, MDBList and Tautulli** in sync. It runs locally with a web UI where you link accounts, define sync pairs, run them manually or on a schedule, and review stats and history. CW also includes its own tracker to keep your data safe with snapshots. With Profiles, you can manage separate sync setups for yourself and for friends or family too, with their own servers and/or tracker API's.

### CW in a nutshell:
* **One brain for all your media syncs** A single place to configure everything.
* **Be your own Sync Hub** Create profiles for seperate media servers/users/trackers.
* **Multi media-server** and **multi tracker** support with profiles.
* **Synchronization**
  * Watchlists, ratings and History
  * Progress sync your progress status between Plex, Emby and Jellyfin.
* **Scrobble (tracks your activity)**
  * **Watcher** (Plex/Emby/Jellyfin to Trakt/SIMKL/MDBList)
    * Does not require any Plex Pass, Emby Premiere,etc.  
  * **Webhooks** (Plex/Emby/Jellyfin to Trakt)
  * **Watchlist Auto-Remove** Clears items from your Watchlist after a verified finish.
* **Tools**
  * Analyzer: Finds items that are **stuck** or inconsistent between providers.
  * Editor: Inspect and adjust your items and add or block items.
  * Captures: Rollback tool for provider watchlist, ratings, and history.

And much more...such as:
* Simple and advanced scheduling: From standard to more detailed pair schedules
* CW Tracker Keeps snapshots/backups from your media servers and trackers.
* Unified Watchlist: View all watchlist items in one place.
* Player card: Shows what you are currently watching in real time.
* Fallback GUID: Revives old items from  your Plex library.



### Download
[![Guide: Container Installation](https://img.shields.io/badge/Guide-Container%20Installation-2ea44f?style=for-the-badge)](https://wiki.crosswatch.app/getting-started/container-installation)
[![Guide: Docker Setup](https://img.shields.io/badge/Guide-Docker%20Setup-0d6efd?style=for-the-badge)](https://wiki.crosswatch.app/getting-started/docker-setup)


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


