# User Interface

- **Main:** run syncs, see progress, open details/logs. A floating **Save** appears when settings changed. Top‑right refresh rechecks status.
- **Watchlist:** preview grid (when available - requires metadata provider and watchlist syncs).
- **Settings:** configure everything:
  - Authentication Providers (Plex, Jellyfin, SIMKL, Trakt)
  - Synchronization Providers (Pairs) - you only will see the providers that are configured in Authentication Providers
  - Metadata Providers (TMDb key)
  - Scheduling (periodic runs)
  - Scrobbler (Plex watcher to Trakt) - requires active Authentication providers (Plex and Trakt)
  - Maintenance (Debug, Clear State/Cache, Reset Statistics)
- **About** — version and links.

<img width="1121" height="403" alt="image" src="https://github.com/user-attachments/assets/951452b7-5dd1-4b97-933c-c8af7041ea71" />

# Interface Settings
## What these settings control
- **Pairs & modes** - Pick which providers sync, the direction (one-way or two-way), and which features are active.
- **Runtime controls** - Debug level, write batching, and cache windows for faster runs and better diagnostics.
- **Provider scoping** - Server URLs, users, and libraries for platforms like Plex and Jellyfin.

## Pairs & features
- **Pair** - Source to Target (e.g., Plex to Jellyfin).
- **Mode** - One-way (target follows source) or Two-way (conflicts resolved by policy).
- **Features** – Enable per pair: Watchlist, History, Ratings, Playlists.
  - For each feature, choose whether **Add** and/or **Remove** is allowed.

<img width="783" height="225" alt="image" src="https://github.com/user-attachments/assets/4337e4b3-b641-4826-9460-d4be441ddc14" />
<img width="830" height="580" alt="image" src="https://github.com/user-attachments/assets/ac81493d-445f-45f2-aead-7234a217ecef" />

## Important settings
- **Debug level** - level of logs
- **Snapshot cache** - How long we reuse “present” data before re-reading.
- **Apply batch size** - How many items we write per batch.
- **Pause between batches** - Short delay to keep providers happy.
- **Pair mode** - One-way or two-way sync.
- **Feature toggles** - Enable Watchlist/History/Ratings/Playlists and pick add/remove behavior.
- **Server URL / User / Library** – Scope for providers like Plex/Jellyfin.

## Pair Config modal
Controls per-pair options like mode, enabled features, and add/remove behavior. Inputs are pre-validated; some panels show previews where helpful.

## Saving & applying
- Changes are saved from the UI and used on the next run.
- If a run is already in progress, some settings take effect on the following run.