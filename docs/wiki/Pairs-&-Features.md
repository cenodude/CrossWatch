This modal defines what a **pair** does. Left = source, right = target. Enable only what you need.

## Header
- **Enabled** — master switch for this pair.
- **Source / Target** — provider pickers.
- **Mode** — `One‑way` or `Two‑way` (only if both sides expose `capabilities.bidirectional=true`).

## Tabs
Tabs appear only when **both** providers support the feature.

## Pair Config

Options are grouped by the selected **feature**.

### Globals
Core safety and behavior toggles applied to the current pair.

| Control | What it does | Notes |
|---|---|---|
| **Dry run** | Simulates the run — **no writes** happen. | Ideal for first-time testing. |
| **Verify after write** | Re-checks a small sample after writes. | Adds a little runtime; improves confidence. |
| **Drop guard** | Protects against empty source snapshots. | Prevents wiping the target if the source momentarily reports 0 items. |
| **Allow mass delete** | Permits bulk removals when required. | Use with care. |
| **Tombstone retention (days)** | How long to keep delete markers. | Avoids re-adding recently removed items. |
| **Include observed deletes** | Treats provider-observed deletes as intentional. | Helps reflect removals done outside the app. |

**Blackbox** (advanced staging)
- **Enabled** — Turn the Blackbox staging area on.
- **Pair-scoped** — Keep Blackbox state isolated per pair.
- **Promote after (days)** — Move items out of Blackbox after N days.
- **Unresolved days** — How long to keep items with missing IDs/details.
- **Cooldown (days)** — Grace period after churn to prevent flip-flopping.

> Blackbox is a **quarantine/staging area** for uncertain changes (e.g., unresolved IDs or rapid changes). It reduces flapping and gives providers time to converge before promoting changes.

---

### Watchlist
Basic enablement and behavior for “plan-to-watch” items.

| Control | What it does |
|---|---|
| **Enable** | Turns watchlist sync on/off for this pair. |
| **Add** | Adds items missing on the target. |
| **Remove** | Removes items not present on the source. |

**Jellyfin/Emby specifics** (no native watchlist)
- **Mode** – Choose **Favorites**, **Playlist**, or **Collections** as the watchlist mechanism.
- **Name** – For playlist/collection mode, set the name (default “Watchlist”).

> Tip: Prefer **Favorites** or **Collections**

---

### Ratings
Control rating synchronization and scope.

| Control | What it does |
|---|---|
| **Enable** | Turns ratings sync on/off. |
| **Add / Update** | Adds new ratings or updates existing ones on the target. |
| **Remove (clear)** | Clears ratings on the target. |

**Scope**
- **All** – Movies, shows, and episodes.
- Or pick specific types: **Movies**, **Shows**, **Episodes**.

**History window**
- **Mode** – **All** (everything) or **From a date**.
- **From date** – Only sync ratings from this date onward (useful for backfills).

**Heads-up for some providers**
- On certain services, rating a **movie** can also mark it **completed** and surface it in “recently watched” or “my list”. Prefer narrow windows when backfilling large libraries.

---

### History
Add-only synchronization of plays.

| Control | What it does |
|---|---|
| **Enable** | Turns history sync on/off. |
| **Add** | Adds missing plays to the target. |
| **Remove** | Disabled to protect your watch history. |

> Deletions are deliberately disabled to avoid accidental loss of history.


---

### When a section is disabled
All dependent controls are visually disabled to prevent accidental edits.

## UI troubleshooting
- **Turned Debug to Full but logs are quiet** → Some flags apply on the next run or after a service restart.
- **Pair modal doesn’t save** → Check your connection to the service and try again.
- **Errors after saving** → Review your server URL/user/library scope and try a small dry-run first.
