## Pick your path

| I want… | Go here | What you’ll set up |
|---|---|---|
| **Sync data** (watchlist / ratings / history) | [Scenario 1 - Syncing](#scenario-1---syncing) | Auth providers, Metadata (optional), Pairs |
| **Real-time scrobbling** (play/pause/stop to Trakt/SIMKL) | [Scenario 2 - Webhook or Watcher](#scenario-2---webhook-or-watcher) | Auth providers , Metadata (optional), Scrobbler - webhooks or watcher |

---

## Run CrossWatch

### Docker (quick run)

```bash
docker run -d \
  --name crosswatch \
  -p 8787:8787 \
  -v /path/to/config:/config \
  -e TZ=Europe/Amsterdam \
  ghcr.io/cenodude/crosswatch:latest
```

Open the UI:  
**http://localhost:8787**

> [!WARNING]
> **Security:** CrossWatch has **no built-in authentication**.  
> Do not expose port **8787** to the public internet. Use LAN only, a VPN, or a reverse proxy with auth and HTTPS.

### Docker Compose (cleaner)

```yaml
services:
  crosswatch:
    image: ghcr.io/cenodude/crosswatch:latest
    container_name: crosswatch
    ports:
      - "8787:8787"
    environment:
      - TZ=Europe/Amsterdam
    volumes:
      - /path/to/config:/config
    restart: unless-stopped
```

## Scenario 1 - Syncing

> Goal: keep **watchlist / ratings / history** aligned between providers.

### 1) Configure authentication providers
Go to **Settings - Authentication** and connect what you need, for example:
- Plex / Jellyfin / Emby (media server)
- Trakt / SIMKL / etc. (tracker)

### 2) Configure pairs
Go to **Pairs** and create **one pair** first.

**Golden rule:** start **one-way**, default settings for Globals and Providers, **one feature** such as Watchlist, Ratings or History.  
For the Globals and Providers the default settings are suitable for most users. Don't change them if you dont know what you're doing.

**Recommended first run setup**
- Mode: **One-way**
- Feature: pick **one**
  - Watchlist
  - Ratings
  - History
- Sync from your media server to your tracker. For now ignore the internal CrossWatch tracker. That is for later.

> [!TIP]
> Dry run lets you preview the plan. Use **Analyzer** for troubleshooting. Use **Editor** for manual policy and blocking.

### 3) Configure a metadata provider (highly recommended!)
Go to **Settings - Metadata** and add **TMDb** (API key).  
This improves matching, reduces missing peer and shows some nice artwork.....just do it!

### 4) Sync
Run Sync. Review results. Fix IDs where needed. Repeat.

**Do not go from 0 to everything.** Scale it up step by step.

## Scenario 2 - Webhook or Watcher

### Scrobbling mode (real-time)

> [!NOTE]
> **As of v0.6.3, the legacy (“traditional”) webhooks are deprecated and no longer maintained or supported.**
> Use the **Watcher** instead.
>
> The Watcher fully replaces webhooks and is the recommended path going forward: it’s more reliable, more capable,
> and easier to operate than the old webhook setup.
>
> If you’re still on legacy webhooks, expect **no fixes** and **no support** for webhook-related issues, migrate to the Watcher now!


### 1) Configure authentication providers
You need:
- **Trakt and or SIMKL** for Watcher scrobbling
- **Trakt** for Webhook scrobbling
- Plex / Jellyfin / Emby

### 2) Choose your mode

| Server | Best choice | Why |
|---|---|---|
| **Jellyfin** | **Watcher** | Best practice |
| **Plex** | **Watcher** | Best practice and no Plex Pass needed |
| **Emby** | **Watcher** | Best practice and no Emby Premiere needed |

> [!NOTE]
> CrossWatch blocks enabling **Watcher** and **Webhook** at the same time. Pick one scrobble mode.
> Prefer the **Watcher** when available.  
> That means **Plex** and **Emby** and **Jellyfin** use the watcher, the traditional webhooks will be obsolute in the future.

### 3) Enable the mode in CrossWatch
Go to **Settings - Scrobbler**:
- Enable **Watcher** or **Webhook**
- Optional:
  - **Filtering**. Use this if multiple users share your media server.
  - **Auto remove from Watchlist** when a movie is finished
  - **Autostart**. Watcher only. 
  - **Ratings** enable ratings to be sent to Trakt and/or SIMKL and/or MDBList (only for Plex) when using watcher you need to add a webhook helper for ratings!

### 4a) If you picked Webhook
CrossWatch exposes endpoints like:
- Plex: `http://YOUR_HOST:8787/webhook/plextrakt`
- Jellyfin: `http://YOUR_HOST:8787/webhook/jellyfintrakt`
- Emby: `http://YOUR_HOST:8787/webhook/embytrakt`

For Plex you are done. For Jellyfin/Emby read the Webhook wiki since it requires specific configuration:  
https://github.com/cenodude/CrossWatch/wiki/Webhook

### 4b) If you picked Watcher
You are done. if you enable ratings for Plex then also include the webhook helper in your Plex instance `http://YOUR_HOST:8787/webhook/plexwatcher`
