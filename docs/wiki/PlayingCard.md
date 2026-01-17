A small card at the bottom of the screen that shows **what you’re watching right now** poster, title, progress bar, and how much time is left.

## What it does
- Shows the current **movie or episode** with poster, year and SxxExx (for shows).
- Live **progress bar** with percentage and “time left”.
- Shows where it comes from: **Plex, Emby, or Jellyfin** (Watcher / webhooks).
- Pulls in **overview, rating and links** to TMDb/IMDb when possible.
- Hides itself when nothing is playing or the session is old.
- You can temporarily hide it with **Hide**, or turn it off in **Settings - UI - Playing Card**.

<img width="790" height="190" alt="image" src="https://github.com/user-attachments/assets/a48216f7-258f-4d7f-b4b6-3655b3837b76" />

## What it needs
- A configured **Plex, Emby or Jellyfin** server (Settings - Authentication).
- A configured **Trakt and/or SIMKL** 
- At least one of:
  - **Jellyfin Webhook** with **Playback Progress** turned on.
  - **Watcher** enabled for **Plex** and/or **Emby**.

Real-time progress works only with:
- Jellyfin Webhook (Playback Progress enabled).
- Plex / Emby Watcher.

Plain Plex/Emby webhooks don’t send live progress. The Playing Card will still show and update on new events (play / pause / stop), just **without a ticking real-time bar**.
