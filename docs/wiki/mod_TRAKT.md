This module integrates **Trakt** with CrossWatch’s sync engine. It can **index** your current state on Trakt (watchlist, ratings, history) and **apply changes** (add/remove history plays, upsert/unrate ratings, add/remove watchlist items). It is designed to be **idempotent**, **rate‑limit aware**, and **observable** via structured logs and per‑feature progress updates.

## Capabilities
- **Bidirectional sync** (engine decides direction per pair).
- **Present‑state indexing** for:
  - **Watchlist**: GET `/sync/watchlist`
  - **Ratings**: GET `/sync/ratings/{movies|shows|seasons|episodes}`
  - **History**: GET `/sync/history/{movies|episodes}` (indexed as events with `watched_at`)
- **Writes**:
  - **Watchlist**: POST `/sync/watchlist`, POST `/sync/watchlist/remove`
  - **Ratings**: POST `/sync/ratings`, POST `/sync/ratings/remove` (1–10 scale)
  - **History**: POST `/sync/history`, POST `/sync/history/remove` (multiple plays supported via distinct `watched_at` timestamps)
- **Provides IDs**: normalization exposes `ids` with possible keys: `trakt`, `imdb`, `tmdb`, `tvdb`, `slug`.
- **Instrumentation**: all HTTP calls go through a retry wrapper that emits `{"event":"api:hit", ...}`. Feature progress is reported to the orchestrator (`snapshot:progress`, etc.).


## Data model
All features use a **minimal item** representation to keep payloads small and deterministic. Canonical keys are derived from `type` + IDs.

Common ID fields:
- `ids.trakt`, `ids.imdb`, `ids.tmdb`, `ids.tvdb`, and for watchlist/history also `ids.slug` when present.
- Episodes can be addressed directly via `ids.*` **or** through show scope (`show_ids` + `season` + `episode`) when building history payloads.

## Watchlist
**Index**
- GET `/sync/watchlist` returns mixed movies/shows/seasons/episodes.
- Normalization maps rows to minimal items; a **shadow file** caches the last ETag to avoid redundant downloads:
  - `/.cw_state/trakt_watchlist.shadow.json`

**Add / Remove**
- POST `/sync/watchlist` with a minimal **batched** body (see _common helpers for `build_watchlist_body`).
- POST `/sync/watchlist/remove` for removals.
- The module **does not freeze** on obviously transient API statuses; unresolved entries are recorded and may be retried.

**Unresolved tracking**
- Persisted at `/.cw_state/trakt_watchlist.unresolved.json` with reasons and attempt counters.
- Successful writes **thaw** matching keys automatically.

## Ratings
**Index**
- Fetches from four endpoints and **merges** by canonical key, preferring entries with more external IDs and newer `rated_at`.
- Results are cached to speed up subsequent runs:
  - `/.cw_state/trakt_ratings.index.json`

**Upsert / Unrate**
- Ratings must be integers **1–10**.
- POST `/sync/ratings` for upsert; POST `/sync/ratings/remove` to clear.
- Batch payloads are chunked for reliability.

**Unresolved tracking**
- Items that fail validation (missing IDs, bad rating) are frozen in memory with hints and will be retried after manual fix.

## History
**Index (event stream)**
- GET `/sync/history/movies` and `/sync/history/episodes`. The module surfaces events keyed by `canonical_key@epoch` and keeps ordering `DESC` by `watched_at`. `since` and `limit` can be used upstream to trim the working set.

**Add**
- POST `/sync/history`
- Accepts mixed inputs:
  1) Movies/Episodes with concrete `ids` → `{"movies":[{"ids":...,"watched_at":...}], "episodes":[...]}`  
  2) Episodes by **show scope** →  
     `{"shows":[{"ids":{...}, "seasons":[{"number":1,"episodes":[{"number":3,"watched_at":"..."}]}]}]}`
- Multiple plays are supported by providing multiple distinct `watched_at` timestamps.

**Remove**
- POST `/sync/history/remove` with the same addressing forms.

**Unresolved tracking**
- Persisted at `/.cw_state/trakt_history.unresolved.json`. The module only freezes on **permanent** validation failures (missing ids, missing `watched_at`, invalid scope), not on transient HTTP errors.

### Advanced (Trakt • History)
> Both options are **off by default** and hydrate from config.

#### 1) Add collections to Trakt
- **What it does:** When syncing History **to Trakt**, also writes the item’s collection membership (from your media server) into Trakt.
- **Enable/disable (UI):** Pair → **History → Advanced** → toggle **“Add collections to Trakt.”**
  Set `true` to enable, `false` to disable.
- **Scope:** Applies during History **Add/Remove** flows when Trakt is the target. Can increase write volume on large libraries.

#### 2) Number Fallback (History)
- **What it does:** If episode **IDs are missing** but we have **show IDs + season/episode numbers**, the sync posts via the **numbers payload** (`shows → seasons → episodes`) instead of resolving per-episode IDs first.  
  No title conversions. No digits↔words tricks.
- **Enable/disable (UI):** Pair → **History → Advanced** → toggle **“Number Fallback.”**
---
## Logging & Observability
- Feature‑scoped logs appear as:
  - `[TRAKT:watchlist] ...`
  - `[TRAKT:ratings] ...`
  - `[TRAKT:history] ...`

## Rate limits & Retries
- All calls go through a shared `request_with_retries` helper with **exponential backoff** and **idempotent** retries on server/network errors.
- Trakt returns standard `X-RateLimit-*` headers; these are captured for metrics and can be surfaced by the orchestrator as needed.
- When the OAuth token is about to expire, the client proactively **refreshes** via the configured auth provider and replays the request.

## Edge cases & Behavior
- **De‑duplication**: The engine dedups requests by canonical key during a single write batch.
- **Validation**: The module rejects ratings outside `1–10`, and history events missing `watched_at` or identifiers.
- **Timestamps**: `watched_at` is normalized to **ISO‑8601 Z**. Integers are accepted as epoch seconds or milliseconds.
- **Conflicts**: On history add, if the destination already has the same or a newer event, Trakt will ignore the duplicate; this is expected and safe.