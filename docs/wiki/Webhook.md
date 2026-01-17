> [!NOTE]
> **As of v0.6.3, the legacy (“traditional”) webhooks are deprecated and no longer maintained or supported.**
> Use the **Watcher** instead.
>
> The Watcher fully replaces webhooks and is the recommended path going forward: it’s more reliable, more capable,
> and easier to operate than the old webhook setup.
>
> If you’re still on legacy webhooks, expect **no fixes** and **no support** for webhook-related issues, migrate to the Watcher now!


## What it does
- Listens on three URLs:  
  - **Plex →** `http://YOUR_HOST:8787/webhook/plextrakt`  
  - **Jellyfin →** `http://YOUR_HOST:8787/webhook/jellyfintrakt`  
  - **Emby →** `http://YOUR_HOST:8787/webhook/embytrakt`
- **Auto-remove from Watchlist** When you finish a movie, CrossWatch will automatically remove that title from your configured Watchlists. It’s currently movies-only. It honors your filters (username/server). If the movie isn’t on your Watchlist, nothing happens, your libraries and other services remain untouched.
- **Ratings** are supported for the Plex webhook and when enabled, we’ll send ratings to Trakt. Movies, shows, seasons, and episodes are supported.

## Quick start
1. **Link Trakt in CrossWatch** (Settings - Authentication - Trakt).
2. **Enable “Webhook” mode** in CrossWatch - Settings - **Scrobble → Webhook**.
3. **Auto-remove from Watchlist** - Enable or disable. CrossWatch will automatically remove that title from your Watchlist. 
4. **Ratings** only for Plex, we’ll send ratings to Trakt. Movies, shows, seasons, and episodes are supported.
5. **Copy the endpoints** shown in the UI and paste them into your server:
   - **Plex**: *Plex Account → Webhooks* (Plex Pass required).
   - **Jellyfin**: see **Configure Jellyfin** below.
   - **Emby**: see **Configure Emby** below.

> **User filters (Jellyfin/Emby):** Prefer whitelisting users in the server’s Webhook/Notification settings. CrossWatch also supports an optional username allow‑list, but source‑side filtering is cleaner.

## Configure Jellyfin → `/webhook/jellyfintrakt`
Requires the **Webhook** plugin.
**Steps (Server Dashboard):**  
1. **Dashboard → Plugins → Webhook → Add Generic Destination**  
2. **Webhook URL:** `http://<your-crosswatch-host>/webhook/jellyfintrakt`  
3. **Notification types (enable):**
   - Playback Start
   - Playback Stop
   - Playback Progress — mid‑play updates; optional, will increase some network traffic
4. **Users:** select the Jellyfin user(s) to scrobble.
5. **Send all properties (ignores template):** **ON** (recommended).
6. **Request headers:** add one header — **Content-Type: application/json**
7. **Save**.

## Configure Emby → `/webhook/embytrakt`
Requires the **Webhook** plugin and **Emby Premiere**.

**Steps (Server Dashboard):**  
1. **Dashboard → Plugins → Catalog → Webhook** (install/enable if needed)  
2. **User Preferences → Notifations → Add Notification → Webhooks**  
3. **Name** CrossWatch 
4. **Webhook URL:** `http://<your-crosswatch-host>/webhook/embytrakt`  
5. **Request content type:** application/json
6. **Events - Playback**
   - Start, Pause, Unpause, Stop
7. **Limit User Events:** select the Emby user(s) to scrobble.
8. **Add Notification**.


## UI fields

### Endpoints
- Three boxes (Plex/Jellyfin/Emby) with **Copy**. Paste each into its server’s webhook config.

### Plex specifics
These options are only valid for the Plex webhook.

Filters (Plex only)
- **Username whitelist** – only scrobble events from listed Plex usernames.  
- **Server UUID** – restrict to one Plex server. **Fetch** can auto-fill it from incoming events.

Plex options
- **Enable ratings** - When enabled, we’ll send ratings to Trakt. Movies, shows, seasons, and episodes are supported.

### Advanced

<img width="752" height="102" alt="image" src="https://github.com/user-attachments/assets/e63e0158-fec1-4458-9a2a-10e6640cb762" />

Pause debounce (sec) (default **5**)
- Ignores rapid, duplicate **pause** events within N seconds.
- Mapping: scrobble.webhook.pause_debounce_seconds

Suppress start @ (%) (default **99**)
- If a **play/resume** arrives at or above this %, **don’t send /scrobble/start**.
- Avoids noisy “starts” near end credits.
- Mapping: scrobble.webhook.suppress_start_at

Regress tol % (default **5**)
- Anti-rollback clamp: if new progress drops by > this %, keep the last higher value.
- Protects against 0%/backseek glitches. (Not applied on a true fresh start.)
- Mapping: scrobble.trakt.regress_tolerance_percent

Stop pause ≥ (%) (default **80**)
- When a **STOP** comes in **below** this %, treat it as **PAUSE** (don’t mark watched).
- “Minimum progress before a STOP is even considered.”
- Mapping: scrobble.trakt.stop_pause_threshold

Force stop @ (%) (default **80**)
- At or above this %, a **STOP** is sent as **/scrobble/stop** → marks watched.
- Between the two thresholds, STOPs are still demoted to PAUSE.
- Mapping: scrobble.trakt.force_stop_at

Result: STOP <80% → PAUSE; STOP ≥80% → STOP (watched). No “start” spam near the end, small backseek glitches are clamped.

## ID handling
- **Plex:** reads GUIDs in payload (IMDb/TMDb/TVDb). If missing, falls back to **Trakt title+year** search; episodes can resolve via **show IDs** and episode numbers.
- **Jellyfin:** uses `ProviderIds` (and common flattened keys like `Provider_tmdb/imdb/tvdb` if present). Episodes carry season/episode; show‑scoped resolution is supported.
- **Emby:** same approach as Jellyfin — reads `ProviderIds` and episode context when available; falls back to title+year when IDs are missing.

## Notes
- **Plex Pass** is required for Plex Webhooks. No Plex Pass? Use the **Watcher** instead.
- **Emby Premiere** is required for Emby Webhooks. No Premiere? Use the **Watcher** instead.
- For Jellyfin/Emby, user filtering need to be configured **in the server’s Webhook/Notification settings**.