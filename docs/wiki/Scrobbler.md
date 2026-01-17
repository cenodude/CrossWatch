## What it is

Live progress tracking. While you watch, the app can report:
- **Now watching** / progress updates
- **Paused** / **Stopped**
- **Completed** when enough is watched

This keeps your “Up Next” and history correct across services and devices.

> **Note (Plex users):** I strongly recommend using the **Plex Watcher** instead of the **Webhook**.  
> The Watcher runs in real time and handles tricky Plex behaviors, like **Skip Credits**, **Autoplay**, and similar playback issues far more reliably than webhook events.


## How it works
1. Detect playback from the source.
2. Map the item to canonical IDs.
3. Send periodic progress (“heartbeats”).
4. Mark as completed once thresholds are met.
5. Retry and queue if the network is flaky.

## What counts as “watched”
- **Thresholds** – Minimum time and/or percent (e.g., ~80%).
- **Credits handling** – Optional grace so skipping credits still counts.
- **Minimum duration** – Ignore very short clips unless enabled.

**Scrobble settings**
- Completion thresholds; heartbeat interval; offline queue & flush; per-provider on/off.

**Scrobble troubleshooting**
- **Never completes** → Lower the threshold or ensure total duration is available.
- **Duplicates** → Reduce heartbeat frequency; verify ID mapping.
- **Progress stuck** → Ensure the player reports positions.
- **Offline periods** → Enable queue/flush.