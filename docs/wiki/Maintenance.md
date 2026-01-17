# Maintenance

The **Maintenance** section is where you control logging, run safe cleanup tools, and restart the CrossWatch service.  
You’ll find it under **Settings → Maintenance**.

---

## Debug level

The **Debug** dropdown controls how much detail CrossWatch writes to its logs:

- **off** – Normal mode. Only warnings and important messages.
- **on** – Extra information about sync runs and provider calls.
- **mods** – Same as **on**, plus additional debug from provider modules.
- **full** – Maximum detail. Useful when troubleshooting with logs, but can make logs large and noisy.

You can change this at any time. It does **not** affect your libraries or sync results – only how much information is recorded in the logs. 

---

## Maintenance tools

Click **Maintenance Tools** to open a compact dialog with one-click cleanup actions. These actions are designed to be **safe** for your media libraries, but they **cannot be undone**, so use them on purpose. 

<img  alt="image" src="https://github.com/user-attachments/assets/0a50f20f-0d96-465f-9d64-dcc12683ea12" />

The dialog shows:
- **Tracker root** – Where the CrossWatch tracker files live (local snapshots and state).
- **Provider cache** – Where provider-specific cache and shadow files live.

Then you get several actions:

### Clear state

<img alt="image" src="https://github.com/user-attachments/assets/e64da9af-acd5-4ad6-8ad1-9d516832d434" />

- **What it does**
  - Deletes the orchestrator **`state.json`** file.
  - On the next sync, CrossWatch rebuilds its internal state from all configured providers from scratch.
- **When to use it**
  - If sync plans look wrong or stuck.
  - After big manual changes in multiple providers and you want CrossWatch to “re-learn” everything cleanly.
- **What it does *not* do**
  - Does *not* remove anything from Plex, Jellyfin, Trakt, SIMKL, etc.
  - Does *not* delete CrossWatch tracker snapshots.

---

### Clear provider cache

<img alt="image" src="https://github.com/user-attachments/assets/41551b64-c212-4c03-86a7-a41ff37bb83d" />

- **What it does**
  - Clears provider cache and shadow files under **`/config/.cw_state`**.
  - Forces CrossWatch to refetch provider health and “unresolved” items on the next runs.
- **When to use it**
  - If provider health / unresolved counts never change even after fixes.
  - If CrossWatch seems to reuse very old data for a provider.
- **What it does *not* do**
  - Does *not* touch your libraries or watch history on any provider.

---
### Remove metadata cache

<img alt="image" src="https://github.com/user-attachments/assets/c68eff65-d82b-4ef0-8ad2-4228cbe404ec" />

- **What it does**
  - Deletes cached posters, artwork and metadata under `/config/cache`.
  - Forces CrossWatch to re-fetch artwork and meta from providers the next time items are viewed or processed.
  - Can free up disk space if the cache has grown large over time.

- **When to use it**
  - If posters, artwork or metadata look wrong, corrupt, or badly outdated.
  - After changing provider settings, themes, or artwork rules and you want everything rebuilt cleanly.
  - When troubleshooting weird visual glitches that might be caused by stale cached images.

- **What it does *not* do**
  - Does not delete any movies/shows or change provider libraries (Plex, Jellyfin, etc.).
  - Does not touch CrossWatch tracker files (`watchlist.json`, `history.json`, `ratings.json`) or provider cache under `/config/.cw_state`.
  - Does not affect Kometa / Plex overlays or other assets stored outside `/config/cache`.

---
### Clear CrossWatch tracker

<img  alt="image" src="https://github.com/user-attachments/assets/a2629f57-b0e4-493e-860c-4a62489a035b" />

The CrossWatch tracker is a **local backup tracker** that stores snapshots and current state for:

- Watchlist
- History
- Ratings 

These files live under something like **`/config/.cw_provider`** and include:

- `watchlist.json`
- `history.json`
- `ratings.json`
- Optional snapshot files per feature

In the dialog you have two checkboxes:

- **Tracker state files** – remove current tracker state (`watchlist.json`, `history.json`, `ratings.json`).
- **All snapshots** – remove all stored snapshot files.

**What it does**

- With **Tracker state files** checked:
  - Empties the local tracker state so CrossWatch can rebuild it from live providers on the next sync.
- With **All snapshots** checked:
  - Deletes all existing snapshots (backups) of tracker data.

You can choose one or both options. If both are unchecked, the action will not run. 

**What it does *not* do**

- Never touches your real Plex/Jellyfin/Trakt/SIMKL libraries.
- Only cleans local tracking and snapshot files inside the CrossWatch container.

---

### Reset statistics

<img  alt="image" src="https://github.com/user-attachments/assets/53a55680-1900-4370-b663-6ae20738d5d7" />

- **What it does**
  - Clears stats, reports and insights caches.
  - Next time stats are loaded, CrossWatch recomputes everything from a clean state.
- **When to use it**
  - If Dashboard statistics look obviously wrong or outdated.
  - After large changes plus cleanup, to get a fresh set of numbers.
- **What it does *not* do**
  - Does not change provider data or tracker snapshots.

---

### Reset currently playing

<img alt="image" src="https://github.com/user-attachments/assets/0cb4d80b-f4ce-4b19-a96c-041058d61746" />

- **What it does**
  - Clears **`currently_watching.json`** so stuck “currently playing” entries disappear.
- **When to use it**
  - If something in the UI or logs keeps saying a title is “currently playing” while nothing is actually playing.
- **What it does *not* do**
  - Does not remove any watched history on your media servers or on Trakt/SIMKL.
  - Only clears CrossWatch’s own “now playing” tracking.

---

## Restart CrossWatch

The **Restart CrossWatch** button restarts the CrossWatch service (container/app) from inside the UI.

- **What it does**
  - Restarts the backend so it reloads configuration and code.
  - Can fix rare cases where the UI looks stuck or a background task does not respond.
- **What to keep in mind**
  - Avoid pressing it while a sync is currently running – wait for the run to finish if possible.
  - Your settings, provider connections and tracker data are not reset by this action.

Use this as a “soft reboot” of CrossWatch itself, instead of restarting the whole Docker stack manually.
