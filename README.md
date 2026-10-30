# CrossWatch

**CrossWatch (CW)** tracks what you watch through Watcher and syncs your watchlists, ratings, history, playback progress and collections across supported media servers and trackers. **Watcher doesn't need Plex Pass or Emby Premiere.**

Set up multiple Watcher routes for your servers and users, and sync pairs between media servers, between trackers, or from a server to a tracker. CW runs locally, with a web UI and a CLI to manage it all.

<p align="center" style="font-size:14px;">
<b>⭐ Star this repository to get updates</b><br>
</p>

<p align="center">
  <a href="https://github.com/cenodude/CrossWatch/releases/latest">
    <img src="https://img.shields.io/github/v/release/cenodude/CrossWatch?display_name=release&amp;sort=semver&amp;logo=github&amp;label=Latest%20Release&amp;style=for-the-badge" alt="Latest Release">
  </a>
  <a href="https://github.com/cenodude/CrossWatch/pkgs/container/crosswatch">
    <img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/cenodude/CrossWatch/badges/ghcr-pulls.json&amp;style=for-the-badge" alt="GHCR Pulls">
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
  <a href="https://www.reddit.com/r/CrossWatchApp/" style="margin: 0 6px;">
    <img alt="Reddit" src="https://img.shields.io/badge/Reddit-r%2FCrossWatchApp-ff4500?style=for-the-badge&logo=reddit&logoColor=white">
  </a>
</p>

<img width="1672" height="941" alt="CrossWatch web UI overview" src="https://github.com/user-attachments/assets/06355278-8cdc-4a13-95ae-89f924fd0975" />

## Why CW?

**Watcher doesn't need Plex Pass or Emby Premiere.** Set up multiple routes for your servers and users, and send playback activity to their chosen supported destinations.

You can also keep media servers in sync with each other, connect them to trackers, or sync between trackers. Choose what each pair syncs: watch history, playback progress, watchlists, ratings or collections, depending on provider support. Run pairs manually or on a schedule.

Manage it all from one place: accounts, Watcher routes, syncs, current playback and a combined watchlist. Profiles keep your setup separate from those of friends and family.

Keep your data with you, without being tied to one tracker. Sync it across the services you use and keep local snapshots in the CW Tracker.

When you need more control, Interactive Sync lets you review changes before applying them. The Analyzer helps you check missing items, and saved mappings let you fix wrong matches, anime episode numbering, and split or combined episodes for future syncs.

## Features

- Watcher with multiple routes for different servers and users. **No Plex Pass or Emby Premiere needed.**
- Server-to-server, server-to-tracker and tracker-to-tracker sync, where supported.
- Watchlists, ratings, history, playback progress and collections. Rewatches too, for supported trackers.
- Multiple accounts, separate profiles and managed users who can handle their own pairs, routes and schedule.
- Interactive Sync to review additions, updates and removals before applying them.
- Webhook scrobbling for supported providers.
- Custom mappings for wrong matches, anime IDs, and split or combined episodes.
- A local CW Tracker with snapshots of your server and tracker data.
- Optional AI help: ask CrossWatch advice with your own AI model. It can suggest. You can also give AI apps like Claude, Codex or Cursor read-only access. Off until you set it up.

## Supported integrations

CW works with Plex, Jellyfin, Emby, Silo, SIMKL, Floppy, FlickList, Trakt, WeTrakr, AniList, MyAnimeList (MAL), Kitsu, TMDb, MDBList, PublicMetaDB, PunchPlay, BingeBase, Scrob, Tautulli, Tracearr, Kodi, Nuvio, Stremio and its own local CW Tracker.

Not every integration supports every feature. Some sync lists and history; others report playback activity. Setup details are in the [wiki](https://wiki.crosswatch.app/).

## Installation

The examples below use Docker. See the [installation guide](https://wiki.crosswatch.app/getting-started/installation) for more details.

### Docker Compose

Save this as `compose.yaml`:

```yaml
services:
  crosswatch:
    image: ghcr.io/cenodude/crosswatch:latest
    container_name: crosswatch
    ports:
      - "8787:8787"
    environment:
      TZ: Europe/Amsterdam
    volumes:
      - crosswatch_config:/config
    restart: unless-stopped

volumes:
  crosswatch_config:
```

Change `TZ` to your timezone, then run this from the same folder:

```bash
docker compose up -d
```

### Docker run

Or run it directly from Bash:

```bash
docker run -d \
  --name crosswatch \
  -p 8787:8787 \
  -v crosswatch_config:/config \
  -e TZ=Europe/Amsterdam \
  --restart unless-stopped \
  ghcr.io/cenodude/crosswatch:latest
```

Both examples store persistent data in a Docker volume mounted at `/config`. Keep that volume when replacing or updating the container.

### Your first sync

Open [http://localhost:8787](http://localhost:8787). If Docker is running on another machine, use that machine's address with port `8787`.

1. Connect the accounts you want to use.
2. Create a sync pair: choose which services to connect and what to sync.
3. Run the pair and check the results. Use Interactive Sync if you'd like to review changes before applying them.
4. Add a schedule if you want it to run automatically.

See the [first-time setup guide](https://wiki.crosswatch.app/getting-started/first-time-setup) for the full setup.

## Scrobbling

**Watcher** reports playback from Plex, Emby, Jellyfin, Silo, Kodi and Scrob to supported trackers. No Plex Pass or Emby Premiere needed for Watcher.

You can also use **webhooks** from Plex, Emby and Jellyfin. Anime ID mapping is available for Watcher and webhooks with supported providers.

### Kodi

The [CrossWatch for Kodi add-on](https://github.com/crosswatch-app/kodi-addon) reports playback and household viewers. It also supports ratings directly in the add-on. Set up a Watcher route per viewer and they each get their own history, even when sharing a Kodi profile.

## Mappings

Sometimes the same episode has a different number on another service, or a title gets matched to the wrong movie. You can correct titles, IDs, seasons and episodes, then save the mapping for future syncs. Split and combined episodes are supported, and you can import or export your mappings.

Anime ID mapping, powered by AniBridge and animeApi, supports AniList, MyAnimeList, Kitsu, SIMKL and CW Tracker pairs.

## Tools

Use the **Analyzer** when something hasn't synced as expected. You can compare snapshots, find missing items and watch time differences, fix wrong matches, check pending retries and release blocked items.

| Tool | What you can do |
| --- | --- |
| Editor | Inspect, add or block items, manage saved mappings, and handle split or combined episodes. |
| Sync topology advisor | See your sync routes and spot overlapping paths. |
| Playback Progress Manager | View and edit unfinished playback sessions across providers. |
| Import and Export | Import tracker files or export your data. |
| Events and Logs | Look through sync history, investigate problems, and filter logs by pair or run. |
| Captures | Roll back provider watchlists, ratings and history. |

There's also a unified watchlist, a player card showing what's playing, and notifications for sync progress and pending reviews. Schedules can be simple or configured per pair.

For Plex, you can import history for removed items into your CW Tracker.

## Running without the web UI

Add `-e CW_CLI_ONLY=1` to the Docker command, or `CW_CLI_ONLY: "1"` under `environment` in Compose. The API, CLI, webhooks and auth callbacks keep working.

## Run once and exit

For cron, Kubernetes CronJobs or another external scheduler, use `run-once`. Set up your accounts and sync pairs first:

```bash
docker run --rm \
  -v crosswatch_config:/config \
  -e TZ=Europe/Amsterdam \
  ghcr.io/cenodude/crosswatch:latest \
  run-once
```

Use the volume that contains your existing configuration. If you set up CW with Compose, its volume name normally includes the Compose project name; replace `crosswatch_config` above with that volume's actual name.

To run specific pairs, use their IDs, numbers or unique prefixes. Replace the example IDs below with your own:

```bash
docker run --rm \
  -v crosswatch_config:/config \
  ghcr.io/cenodude/crosswatch:latest \
  run-once --pair pair_07c3 --pair pair_91ab
```

The command exits with `0` on success and `1` if the sync reports errors or cancellation. Add `--fail-on-unresolved` if you also want unmatched items to fail the job.

## Help and feedback

Setup docs are in the [wiki](https://wiki.crosswatch.app/). For questions and discussion, there's [r/CrossWatchApp](https://www.reddit.com/r/CrossWatchApp/).

Please report bugs in [Issues](https://github.com/cenodude/CrossWatch/issues), with steps to reproduce them and what went wrong.

Read [CONTRIBUTING.md](CONTRIBUTING.md) before sending a PR. For security reports, see [SECURITY.md](SECURITY.md). The license is in [LICENSE](LICENSE).

## Support and sponsors

<p align="center">
  <a href="https://www.buymeacoffee.com/cenodude">
    <img alt="Buy Me a Coffee" src="https://img.shields.io/badge/Buy%20Me%20a%20Coffee-support-ffdd00?style=for-the-badge&amp;logo=buy-me-a-coffee&amp;logoColor=000000">
  </a>
</p>

Every cent donated through Buy Me a Coffee goes directly to the **ALS Foundation in the Netherlands**.

<p align="center">
  <a href="https://www.gitbook.com/">
    <img alt="GitBook" src="https://img.shields.io/badge/GitBook-sponsored-3884ff?style=for-the-badge&amp;logo=gitbook&amp;logoColor=white">
  </a>
</p>

Thanks to our sponsors for supporting CW.
