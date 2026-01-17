# CrossWatch editor

The CrossWatch editor lets you inspect and adjust data that CrossWatch stores inside the container. It has two modes, each with its own rules.

## Quick start

1. Open CrossWatch, go to **Editor**
2. Pick **Data**: **CW Tracker** or **Current State**
3. Pick **Kind**: watchlist / history / ratings
4. In **CW Tracker**, pick **Snapshot**. In **Current State**, pick **Provider**
5. Make changes, click **Save**

## Modes at a glance

| Mode | What you edit | Where it lives | Backup you should use | Best for |
|---|---|---|---|---|
| **CW Tracker** | CrossWatch local tracker rows | tracker state files inside the container | Tracker ZIP export | fixing tracker data, insights cleanup |
| **Current State** | manual sync policy on top of provider baselines | `/config/state.manual.json`, applied into `/config/state.json` | Policy export | blocking items, manual adds that should survive rebuilds |

## CW Tracker mode

CW Tracker is the classic tracker editor. It edits CrossWatch’s local tracker files used for backups and insights.

### What you can do

1. Fix wrong title, year, mapping
2. Adjust ratings
3. Fix watched timestamps
4. Remove junk rows from tracker data
5. Import, export tracker state between instances

### Requirements

CW Tracker mode needs tracker enabled, and at least one sync run, otherwise there is no tracker data or snapshots to show.

### Snapshot selector

Snapshot lets you view **Latest** or an older snapshot.

Saving always writes to the current state file for that kind, it does not modify old snapshots.

## Current State mode

Current State edits the orchestrator snapshot file at `/config/state.json`, but it does it safely by keeping your manual changes in a separate file.

### What you are really editing

1. Provider baselines are read from `/config/state.json`
2. Your manual changes are stored in `/config/state.manual.json`
3. The editor applies your policy back into `state.json`, so a fresh `state.json` can be “retrained” from policy

### Provider selector

In this mode the snapshot dropdown becomes **Provider**: Plex, SIMKL, Trakt, MDBList, Jellyfin, Emby, and any other providers present in your state.

### Import datasets (populate `state.json`)

In **Current State**, you can import live datasets directly from a provider (without setting up a sync pair first). This populates the provider baseline inside `state.json`, so you can immediately review, edit, and block items before you run any syncs.

How to import:

1. Set **Data** to **Current State**
2. Expand **Import datasets**
3. Pick a **Provider**
4. Choose which datasets to import: **Watchlist**, **History**, **Ratings**
5. Choose **Replace baseline** (overwrite that provider’s baseline for selected datasets) or **Merge**
6. Click **Import**

Notes:

- The provider dropdown only shows providers that are available/configured.
- The dataset list only shows what that provider supports.
- Import runs against live provider APIs. It can take a while; the editor shows a progress indicator while it’s running.
- Import updates the baseline in `state.json`. Your policy still lives in `state.manual.json`.

### Blocking items

Blocking is a provider global rule, meaning: if you block something on a provider, it will not be synced out from that provider.

How to block, step by step:

1. Set **Data** to **Current State**
2. Pick **Kind**
3. Pick **Provider**
4. Find the item, click the **trash icon** on the baseline row
5. Click **Save**

Click the trash icon again to unblock, then save.

Important: blocking is policy. The orchestrator must apply policy during planning and execution for it to affect sync.

### Bulk policy

Bulk policy is a faster way to block/unblock whole media types (per provider) in **Current State**.

1. Set **Data** to **Current State**
2. Expand **Bulk policy**
3. Choose a type (movie/show/etc.)
4. Click **Block all** or **Unblock all**
5. Click **Save**

### Manual adds

Manual adds let you introduce items that should be treated as present on the selected provider, even if the baseline snapshot does not include them yet.

1. Click **Add row**
2. Fill a unique **Key**
3. Set **Type**: movie, show, episode
4. Fill **IDs** if possible (IMDb or TMDB strongly recommended)
5. Click **Save**

If you only provide title and year, matching can be weaker, and targets may not be writable without IDs.

### Episodes show name

For episode rows, the editor shows the **series name** under the episode title when it is available in the data.

## Filtering, sorting, types

### Filter

The filter searches across key, title, type, year, IDs, and series title for episodes.

### Type chips

You can toggle Movies, Shows, Episodes. At least one type must stay enabled.

### Sorting

You can sort by Key, Type, Title, Extra. Extra sorting depends on the kind.

## Table columns

1. Action, trash icon
2. Key
3. Type
4. Title
5. Year
6. IMDb
7. TMDB
8. Trakt
9. Extra

Extra behavior:

1. Ratings: rating picker
2. History: watched_at date and time editor
3. Watchlist: no extra field

## Saving

### CW Tracker save

Save overwrites the current tracker state file for the selected kind and creates a snapshot if snapshotting is enabled.

### Current State save

Save writes policy to `/config/state.manual.json`, then applies it into `/config/state.json`.

Provider baselines are not overwritten by this save (except when you explicitly use **Import datasets**).

## Backup and restore

### CW Tracker backup, restore

Tracker backup exports a ZIP containing current tracker state files and all tracker snapshots.

Tracker import accepts:

1. A ZIP created by the tracker export
2. A JSON state file for a kind
3. A JSON snapshot file for a kind

### Current State policy backup, restore

Policy backup exists so you do not lose blocks and manual adds when `state.json` is replaced.

1. **Policy Download** exports the policy JSON
2. **Policy Import** merges an imported policy into the current policy by default

If you start with a fresh `state.json`, import your policy backup to bring back your blocks and manual adds.

## File locations

1. Current State snapshot: `/config/state.json`
2. Current State policy: `/config/state.manual.json`

Tracker files and snapshots live under the CrossWatch tracker folder inside the container, and are easiest to move via the Tracker ZIP export.

## Safety notes

1. CW Tracker edits affect local tracker data, insights, and anything that reads the tracker
2. Current State policy can affect sync behavior once the orchestrator uses it: blocking can prevent writes, manual adds can force planned writes
3. Import datasets overwrites/merges provider baselines in `state.json` (selected provider + selected datasets)
4. If you break something: restore from backup, or remove policy entries and save again
