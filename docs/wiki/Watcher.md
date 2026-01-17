Real-time scrobbling **without Plex Pass or Emby Premiere**. The Watcher attaches directly to your server (Plex Alerts / Emby Sessions / Jellyfin Sessions) and maps play/pause/stop to Trakt and/or SIMKL and/or MDBList.

## What it does
- **Plex:** connects via `AlertListener` and consumes real-time “Playing” alerts.
- **Emby:** polls `/Sessions?ActiveWithinSeconds=15` to detect active playback and status. Updated each 15 seconds
- **Jellyfin:** polls `/Sessions?ActiveWithinSeconds=15` to detect active playback and status. Updated each 15 seconds
- **Auto-remove from Watchlist** – Enable or disable. CrossWatch will automatically remove that title from your Watchlist. 
- **Ratings** Only for Plex Watcher; When enabled, we’ll send ratings to Trakt and/or SIMKL and/or MDBlist but requires an additional webhook helper!

## Quick start
1. **Link Trakt and/or SIMKL and/or MDBList** in CrossWatch (Settings - Authentication - Trakt and/or SIMKL and/or MDBList).
2. **Configure Plex and/or Emby and/or Jellyfin** (Settings - Authentication).
3. Go to **Settings - Scrobbler - Watcher** and **Enable** it.
4. For **Jellyfin and Emby** users, configure Trakt (even when not using Trakt actively!)
   The Watcher includes a fallback that will try to resolve missing episode IDs via Trakt when it can’t retrieve them from your media server. Without Trakt its unable to fetch the correct IDs.
4. (Optional) Toggle **Autostart** so Watcher runs automatically when CrossWatch starts.
5. (Optional) Toggle **Auto-remove from Watchlist** - CrossWatch will automatically remove that title from your Watchlist. 
6. (Optional) Toggle **Ratings** Only for **Plex watcher**; When enabled, we’ll send ratings to Trakt and/or SIMKL but requires an additional webhook helper!

> Default scrobble works without Plex Pass or Emby Premiere. But if you want ratings (Plex only option), you’ll need the webhook helper, which means Plex Pass.

## UI fields

<img width="761" height="77" alt="image" src="https://github.com/user-attachments/assets/cf8b942d-e8cf-40b9-8d3c-5a7456669f28" />

- **sink** - Trakt, SIMKL, MDBlist or any combination 
- **provider** - Plex or Emby or Jellyfin

>When using Trakt you can see realtime what you're watching in Trakt app or website.
>When using SIMKL you can see you stopped/paused sessions at the [SIMKL website](https://simkl.com/7649769/history/playback-progress-manager/)
>Manage your playback progress pause\stop data for unfinished watches. At the moment SIMKL does not provide realtime status in SIMKL app or website.
>When using MDBList realtime playback progress is shown on the main page.

### Options
- **Auto-remove from Watchlist** When you finish a movie, CrossWatch will automatically remove that title from your configured Watchlists. It’s currently movies-only. It honors your filters (username/server). If the movie isn’t on your Watchlist, nothing happens, your libraries and other services remain untouched.
- **Ratings** are supported for the Plex watcher only, when enabled, we’ll send ratings to Trakt and/or SIMKL.
  - This option **needs an extra webhook helper**. Add the following webhook to your Plex instance. `http://YOUR_HOST:8787/webhook/plexwatcher` 

### Filters
- **Username whitelist** - only scrobble listed users. Supports plain names and IDs: `id:<accountID>` or `uuid:<accountUUID>`.
- **Server UUID** - Plex only: restrict to one server.
- **User UUID** - Embby/Jellyfin restrict to one user.

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
