The CrossWatch tracker is your local safety net. It can take snapshots of your Watchlist, History and Ratings from services like Plex, Trakt, MDBlist, SIMKL, Jellyfin and Emby and store them safely on disk inside CrossWatch. If something goes wrong, such as a bad sync, a wrong setting, or you change provide you can point a sync back from CrossWatch to restore that older state into your services.

## Capabilities

- **Internal backup provider** (no external account, no login needed)
- Lives entirely on disk under your CrossWatch config:
  - Main data files in: `/config/.cw_provider/`
  - Snapshots in: `/config/.cw_provider/snapshots/`
- **Supported features**:
  - **Watchlist**
  - **History**
  - **Ratings**
- **Bidirectional**:
  - You can **backup from** Plex / Emby / Jellyfin / Trakt / SIMKL / MDBList → CrossWatch
  - You can **restore to** any of those providers ← CrossWatch
- **Per‑feature restore**:
  - Separate snapshot selection for **Watchlist**, **History**, and **Ratings**

CrossWatch tracker behaves like “your own tracking service”, but everything is stored locally as JSON files in your config folder.

## How it works

### 1. Where CrossWatch stores your data

When CrossWatch tracker is enabled, it keeps three main data files:
- `/config/.cw_provider/watchlist.json`
- `/config/.cw_provider/history.json`
- `/config/.cw_provider/ratings.json`

Each file contains the **current state** of that feature, normalized into a common format so CrossWatch can talk to different providers.

In addition, CrossWatch creates **snapshot files** in:
- `/config/.cw_provider/snapshots/`

For each feature you’ll see files like:
- `20251126T234204Z-watchlist.json`
- `20251126T234204Z-history.json`
- `20251126T234204Z-ratings.json`

The timestamp (`YYYYMMDDThhmmssZ`) tells you **when** the snapshot was taken.

### 2. When backups happen
CrossWatch tracker creates / updates data in two moments:

1. **When it is a sync target**  
   - Example: Pair `TRAKT → CROSSWATCH` (Watchlist enabled).  
   - During the run, CrossWatch receives “add/remove” operations and updates its own JSON files.
   - Before writing changes, it can make a **snapshot** of the previous state (if auto snapshots are enabled).

2. **When you change settings (restore selection)**  
   - Changing the selected snapshot for a feature does **not** create new data, but it changes **which snapshot is used** as the “current state” for that feature.

> Important: CrossWatch does **not** run anything by itself. It is used through normal sync pairs, exactly like other providers.

### 3. Snapshots & retention
Every time CrossWatch needs to mutate its main JSON files, it can:

- Take a **snapshot** of the current file into `/config/.cw_provider/snapshots/`
- Keep snapshots for a certain number of days (`retention_days`)
- Limit the total number of snapshots (`max_snapshots`)

Old snapshots are cleaned up automatically based on these settings.

---
## Configuration & UI

### Where to find the settings

<img alt="image" src="https://github.com/user-attachments/assets/330a1c4d-6a40-443d-92fa-cb4088da80dd" />

Open the **Settings** section in the app and look for:
- **Settings → CrossWatch Tracker**

You’ll see two groups:

1. **Basic behaviour**
2. **Restore snapshots**

### Basic behaviour

These fields control when and how CrossWatch stores your data:

- **Enabled**
  - Turns CrossWatch on or off as a sync provider.
  - When disabled, you can’t use it in sync pairs.

- **Retention (days)**
  - How long snapshots should be kept.
  - `0` = keep snapshots forever (until manually removed).

- **Auto snapshot**
  - **On**: Before changing the main files, CrossWatch saves a timestamped snapshot.
  - **Off**: No automatic snapshot; only the main JSON files are updated.

- **Max snapshots per feature**
  - Hard cap on how many snapshots to keep **per feature** (watchlist, history, ratings).
  - `0` = unlimited.

### Restore snapshots (per feature)
Below the basic behaviour, you’ll see three dropdowns:

- **Watchlist snapshot** 
- **History snapshot** 
- **Ratings snapshot** 

Each dropdown offers:
- `Latest (default)` — uses the **most recent snapshot** file for that feature.
- A list of all snapshot files under `/config/.cw_provider/snapshots/`, for example:
  - `20251126T234204Z-watchlist.json`
  - `20251126T234204Z-history.json`
  - `20251126T234204Z-ratings.json`

You can pick **different snapshots** per feature. For example:
- Watchlist → `Latest (default)`  
- History → `20251126T234204Z-history.json`  
- Ratings → `Latest (default)`

When you click **Save settings**, the chosen snapshots are stored in `config.json` under:

```jsonc
"crosswatch": {
  "enabled": true,
  "retention_days": 30,
  "auto_snapshot": true,
  "max_snapshots": 64,
  "restore_watchlist": "latest",
  "restore_history": "latest",
  "restore_ratings": "latest"
}
```

---

## What “restore” actually does

This part is important:
> Selecting a snapshot **does not immediately overwrite other providers**.  
> It only tells CrossWatch which data it should present as its current state.

In practice:
- The selected snapshot is **copied into the main JSON** for that feature.
- When another provider syncs **from CrossWatch**, it sees the data from that snapshot.
- Nothing is pushed anywhere until you run a sync where CrossWatch tracker is the **source**.

Think of it like this:
- CrossWatch = local backup “server”
- Snapshot selection = which backup you want to expose
- Sync pair = how you push that backup back into Plex / Trakt / SIMKL / etc.

---
## Example use cases

### Use case 1 – Undo a bad ratings sync

Scenario:
- You accidentally synced wrong ratings from Provider A to Provider B.
- Luckily, CrossWatch tracker was a target earlier and holds a good backup of your ratings.

Steps:
1. Open **Settings → CrossWatch Tracker**.
2. Under **Restore snapshots → Ratings snapshot**, choose a snapshot from **before** the bad sync, e.g.:  
   `20251126T120000Z-ratings.json`
3. Click **Save settings**.
4. Create or edit a pair where:
   - **Source** = CrossWatch tracker
   - **Target** = Provider B (e.g., Trakt)
   - Only **Ratings** is enabled.
5. Run the sync (ideally first as **Dry Run** if available).

Result:
- Provider B’s ratings are overwritten with the snapshot stored in CrossWatch from that earlier moment.

### Use case 2 – Roll back only the watchlist, not history

Scenario:
- You cleaned your watchlist in a way you regret, but you want to keep your play history and ratings as they are now.

Steps:
1. In **Settings → CrossWatch Tracker**:
   - **Watchlist snapshot**: pick an older snapshot (e.g. `20251120T090000Z-watchlist.json`)
   - **History snapshot**: leave as `Latest (default)`
   - **Ratings snapshot**: leave as `Latest (default)`
2. Save settings.
3. Use a sync pair:
   - **Source** = CrossWatch
   - **Target** = your main tracker (e.g., SIMKL or Trakt)
   - Enable **Watchlist** only.
4. Run the sync.

Result:
- Watchlist is restored to the chosen snapshot state.
- History and ratings remain unaffected because you did not sync those features.

### Use case 3 – Move your “old state” to a different provider

Scenario:
- You previously used Provider A.
- You want to move that “old” state into a completely different Provider B, using CrossWatch as a bridge.

Steps:
1. Make sure you already have a snapshot in `/config/.cw_provider/snapshots/` from the time you liked your library.
2. Set per‑feature snapshots in **Settings → CrossWatch Tracker** to those dates.
3. Create a pair:
   - **Source** = CrossWatch
   - **Target** = Provider B
   - Enable the features you care about (Watchlist, History, Ratings).
4. Run the sync.

Result:
- Provider B gets the “old” state that was backed up into CrossWatch at that snapshot time.

### Use case 4 – Safe experimentation

Scenario:
- You want to experiment with aggressive sync rules, but you’re nervous about losing your data.

Steps:
1. Ensure **CrossWatch** is enabled and **Auto snapshot** is turned **On**.
2. Run a sync with CrossWatch as **target**, so it has a very recent copy of everything.
3. Then experiment with your provider → provider syncs (e.g., Trakt ↔ SIMKL).
4. If something goes wrong, pick the last “known good” snapshots in CrossWatch and use it as a **source** to restore.

Result:
- CrossWatch acts as a safety net; you can always go back to a previous snapshot as long as it exists on disk.

---
## Tips & limitations
- CrossWatch is **local only**:
  - No online login.
  - Your backups live in `/config/.cw_provider/` and will be lost if you remove that folder.
- Snapshots are feature‑specific:
  - Watchlist, History, and Ratings each have their own files and restore selection.
- Switching snapshot selection does **not** automatically push changes to other providers:
  - You must use a sync pair with CrossWatch as **source** to apply the restore to Plex/Trakt/Emby/etc.
- If you set **Retention (days)** to a low value and run many syncs, older snapshots will be cleaned up automatically.
- This does **NOT** replace the backups provided by SIMKL or TRAKT. Think of CrossWatch as an extra local safety net on top of what those services already offer.

For most people, a good starting configuration is:
- CrossWatch **Enabled**
- **Auto snapshot**: On
- **Retention**: 30–90 days
- **Max snapshots per feature**: 64
- Restore options: leave on **Latest (default)** until you intentionally want to roll back.
