This module provides fast synchronization between a Plex Media Server and other provider(s) in CrossWatch. It can read your Plex state (history, ratings, watchlist), and where enabled, it can write back to Plex (scrobble or unscrobble, rate or unrate, add or remove from the watchlist).  
It is designed around **present-state indices** and resilient ID resolution, with an optional, **experimental** GUID fallback.

## Capabilities

- **Features**: Watchlist (via Plex Discover), History (PMS), Ratings (PMS). Playlists are currently disabled in the manifest.
- **Bidirectional**: Supports two-way sync where applicable; writes are supported for ratings, history, and watchlist.
- **Health checks**: Probes Discover and PMS endpoints and reports per-feature readiness.

## How it works (high level)

1. **Connect**
   - Uses a single instrumented HTTP session and an optional PMS connection (base URL plus token). Also scopes the active Plex user/profile and is Plex Home-aware.

2. **Build indices (present state)**
   - **Watchlist**: pulled from Plex Discover, writes also go through Discover, with an optional PMS fallback path. Uses “GUID-first, ID-true matching (tmdb/imdb/tvdb)” rather than fuzzy titles.
   - **History**:
     - Base layer is PMS `/history`, filtered to a user (by `accountID` or username), then normalized.
     - When `plex.history.include_marked_watched = true`, a second layer is added from the Plex **library watched state** (items with `isWatched`/`viewCount > 0`) and merged into the index as extra “watched” entries.
   - **Ratings**: iterates libraries (movies, shows, seasons, episodes), fetches only items with `userRating`, then normalizes.

3. **Apply plan**
   - The orchestrator computes adds and removes and calls feature writers (scrobble or unscrobble, rate or unrate, add or remove watchlist entries).

4. **Freeze unresolved**
   - Items that cannot be matched or written are *frozen* into a local JSON “unresolved” file. This prevents repeated noisy retries. Per-feature files live under `/config/.cw_state/`.

## Configuration

### Connection & Identity

- **Token / Account** – Plex account token used for Discover and PMS.  
  If `baseurl` is set, the module also binds to your PMS with that token.  
  User scope is resolved automatically and persisted (username plus `accountID`).

### Feature toggles (UI / config keys)

- **Workers**: `plex.rating_workers`, `plex.history_workers` (1–64).
- **Library filters**: limit scanning to specific library section IDs via `plex.ratings.libraries`, `plex.history.libraries`.

- **History filters**
  - `plex.history_ignore_local_guid` and `plex.history_ignore_guid_prefixes` (for example `local://`) to skip local-only metadata.
  - `plex.history_require_external_ids` to require imdb/tmdb/tvdb.
  - `plex.history.include_marked_watched`
    - `false` (default): history is built from **Plex play history only**.
    - `true`: history index = **play history + library items marked as watched** (`isWatched` / `viewCount > 0`), as long as Plex provides a usable timestamp (`lastViewedAt` / `viewedAt`).
    - This affects **adds only**; unmarking an item as watched in Plex does not trigger a remove on connected services.

- **Watchlist options**
  - `plex.watchlist_allow_pms_fallback` for optional PMS fallback if a Discover write fails.
  - `plex.watchlist_query_limit` for search breadth, `plex.watchlist_write_delay_ms` for rate-limit friendly writes.

- **Experimental: `plex.fallback_GUID`**  
  Enables GUID fallback when PMS hydration fails or when external IDs are missing. See **Fallback GUID** below.

- **Marked Watched**
  Enables manual watched items (movies/shows) 

> **History vs “marked watched” in Plex**  
> Plex has two separate concepts:
> - **Play history**: actual play events recorded by Plex.
> - The **“watched” checkmark** in the UI (mark/Unmark as Watched)
>
> By default, CrossWatch uses both **play history** and **marked watched** for the History feature.  
> If you disable the **Marked Watched** toggle, CrossWatch will only treat play history items.
>
> This is **add-only**:
> - When something becomes watched in Plex, CrossWatch can sync that out.
> - When you later *unmark* it as watched in Plex, CrossWatch will **not** automatically “unwatch” or remove that play on Trakt/SIMKL. Watched still wins if provider disagree.

> [!IMPORTANT]
> ***Marked Watched** only works for **Plex database (PMS) owner** or **Plex Home Users**.
> If you need to retrieve/sync data for a **specific Plex user such as friends**, **do NOT enable** these options.
> Fallback GUID is only for the (PMS) owner. Only enable ONE-TIME then disable. Dont enable it for Home Users nor Friends.
> <img alt="image" src="https://github.com/user-attachments/assets/1860d596-33d1-450b-a3b3-412d00b5901b" />
## Feature details

### History

**Indexing**

- Base layer:
  - Fetch rows from PMS history, filter to the chosen user (`accountID` when available, otherwise username match), de-duplicate, and sort by time.
  - Parallel-fetch metadata for missing keys, then normalize to a minimal form that includes:
    - `type`, `title` or `series_title`, `year`
    - external IDs (imdb/tmdb/tvdb/trakt) and library id
    - for episodes: `season`, `episode`, plus `show_ids`.
  - Optional filters can drop items with local GUIDs or lacking external IDs, depending on your config.

- Optional **Marked Watched** layer:
  - When `plex.history.include_marked_watched = true`:
    - Scan movie and show libraries for items (movies / episodes) where Plex reports `isWatched` or `viewCount > 0`.
    - Only include these items if Plex also exposes a usable timestamp (`lastViewedAt` / `viewedAt`).
    - Normalize them in the same way as regular history rows and merge into the history index, as additional “watched at T” entries.
  - This is one-way: it lets CrossWatch *discover more watched items*, especially those bulk-marked as watched and missing play events.

**Writes**

- **Add** means “scrobble at a specific timestamp” and requires `watched_at`.
- **Remove** means “unscrobble”.
- There is currently **no automatic “unwatch” propagation** from Plex’s “unmark as watched” actions. If you unmark something in Plex, CrossWatch will not remove the matching plays automatically.

### Ratings

**Indexing**

- Walk movie and show libraries, include shows, seasons, and episodes.
- For each item, fetch by `ratingKey` and keep only entries with `userRating`, carrying `rated_at`.
- When external IDs are missing and GUID fallback is enabled, the module tries to *enrich* IDs (see **Fallback GUID**).

**Writes**

- **Add** uses `/:/rate?rating=1..10`.
- **Remove** uses rating `0` to clear.

### Watchlist

**Discover-first**

- Resolve items by GUID or external IDs (tmdb/imdb/tvdb) using Plex Discover.
- Perform `add` or `remove` via Discover actions. There is no fuzzy title matching.

**Optional PMS fallback**

- If enabled, builds a GUID index from your PMS libraries and uses it when Discover fails.

## Experimental: Fallback GUID

### What it is

Sometimes Plex cannot fully identify something you watched or rated, for example when the item was removed from your PMS or a lookup returns **404**.  
**Fallback GUID** tries extra ways to figure out what that item is, so your sync can still work.

> Enable with `plex.fallback_GUID = true`, or toggle it in the UI (Edit Pairs → Providers).  
> **Only use this if you accept the trade-offs below.**

### When it kicks in

- **History**: when a history row’s `ratingKey` cannot be hydrated from PMS, the module rebuilds a **minimal** item from the raw history row and tries to **enrich** it with external IDs (IMDb/TMDb/TVDb).
- **Ratings**: when a rated item has **no external IDs**, the module tries to **enrich** its IDs before using it in sync.

### What it tries (in this order)

1. **Plex Metadata by ratingKey**, asks the Plex *Metadata* service (not PMS) for external IDs. A `404` here is a miss.
2. **Episodes: also the show**, for episodes it also hydrates the **show** external IDs via `grandparentRatingKey`.
3. **Plex Discover title search**, if IDs are still missing and Discover access is allowed, searches Plex **Discover** by *title plus year* to fill gaps. For episodes, this may also fill **season**, **episode**, and series title.

The goal is to build a minimal record from the history row and then **progressively enrich** it, so your downstream sync has usable IDs.

### Memo / Neg-Cache: `plex_fallback_memo.json`

To avoid repeating expensive fallback work and noisy logs, the module keeps a small memo or negative cache on disk when GUID fallback is used.

- **Path**: `/config/.cw_state/plex_fallback_memo.json`
- **Keys**: deterministic string derived from the history row, `type|guid|parentGuid|grandparentGuid` in lower-case, with empty components allowed.
- **Values**:
  - A **minimal normalized item** (dict) when enrichment succeeded. This contains `type`, `title` or `series_title`, `year`, `ids`, and for episodes `show_ids`, `season`, `episode` when known.
  - The sentinel string `__NOHIT__` when enrichment failed, which prevents repeated attempts on the same orphan.
- **Read path**: on each fallback attempt the key is checked first. If it maps to `__NOHIT__` the attempt is skipped; if it maps to a dict, that cached result is used.
- **Write path**: after an attempt, the result (success or no-hit) is saved under the key.
- **Resetting**: delete the file, or remove specific keys, to force the module to try again for those items.
- **Safety**: the memo only affects fallback discovery or hydration paths; normal PMS-resolved items are not touched.

> Tip: if you want **strict PMS-backed IDs** and no fallback at all, leave `plex.fallback_GUID` off. If you turn it on, the memo keeps future syncs fast and quiet by remembering both successes and known misses.

## Performance
- History and Ratings use thread pools; tune with `plex.history_workers` and `plex.rating_workers` (default around 12, hard-clamped to 1–64).
- Watchlist requests support retries, small write delays, and a bounded Discover query size via `watchlist_query_limit`.

## Unresolved queues (freeze files)

Per-feature “frozen” items are stored under `/config/.cw_state/`, so the module does not spam repeated failures:

- `plex_watchlist.unresolved.json` for watchlist issues.
- `plex_history.unresolved.json` for history.
- `plex_ratings.unresolved.json` for ratings.

Each entry records the minimal item, first and last attempt timestamps, reason(s), and attempt count.
