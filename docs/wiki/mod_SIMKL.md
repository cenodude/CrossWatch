CrossWatch integrates with **SIMKL** It covers capabilities, configuration, rate‑limit hygiene, delta syncing via `date_from` watermarks, and per‑feature behavior (Watchlist, Ratings, History).

> **Please be kind to SIMKL’s API.** SIMKL purposefully limits load to keep the service reliable. **Do not schedule this provider more than once every 12 hours (daily is even better).** The module is built to minimize requests, but aggressive scheduling can still create unnecessary load.

## Capabilities
- **Features:** Watchlist, Ratings, History (playlists are not supported).
- **Auth:** Requires both **API Key** (`simkl-api-key`) and **Access Token** (Bearer).
- **Delta sync:** SIMKL endpoints support `date_from`; the provider maintains **watermarks** per feature and uses SIMKL **activities** to skip no‑op runs.
- **Conservative calls:** Activities gate + shadow caches + watermarks drastically reduce API usage.
- **IDs:** Matches using `simkl`, `imdb`, `tmdb`, `tvdb`; episodes include `show_ids` when needed.

## Configuration
You can configure SIMKL either through your app config or environment variables.

### Required
- **API Key / Client ID**: `simkl.api_key` (alias: `client_id`)
- **Access Token**: `simkl.access_token`

### Optional
- **Timeout / Retries**: `simkl.timeout`, `simkl.max_retries`  
- **Global baseline**: `SIMKL_DATE_FROM` (ISO 8601, e.g. `2020-01-01T00:00:00Z`)  
- **Per‑feature baselines**: `SIMKL_WATCHLIST_DATE_FROM`, `SIMKL_RATINGS_DATE_FROM`, `SIMKL_HISTORY_DATE_FROM`, or even more granular keys such as `SIMKL_WATCHLIST:movies_DATE_FROM` if you expose them in your environment (see “Watermarks & date_from” below).
- **Watchlist shadow tuning**:  
  - `CW_SIMKL_SHADOW_TTL` (seconds, default `300`) — reuse cached list if activities unchanged and cache is fresh  
  - `CW_SIMKL_WATCHLIST_CLEAR=1` — clear watchlist shadow on next run  
  - `CW_SIMKL_FORCE_PRESENT` (`movies|shows|all|true|1`) — force “present” fetch path (debug/diagnostics)

## Rate‑Limit Hygiene (Important)
- SIMKL is a **conservatively‑provisioned** API. **Schedule runs at most every 12 hours**; once a day is ideal.
- The module surfaces **`Retry-After`** when available from health probes; honor it in your scheduler.
- Activities gating (see below) and watermarks help minimize calls, but **scheduling cadence is still your responsibility**.

## Delta Sync Model: `date_from` + Watermarks
SIMKL supports (or requires) **delta windows** via the `date_from` query parameter. This module keeps a JSON **watermark store** at:

```
/config/.cw_state/simkl.watermarks.json
```
Each feature (and sometimes per‑bucket) has its own watermark key, for example:
- `watchlist`, `watchlist:movies`, `watchlist:shows`
- `ratings:movies`, `ratings:shows``
- `history:movies`, `history:shows`

### How the module chooses `date_from`
1. **Watermark** (most recent successful timestamp)
2. **Env per‑feature** (e.g. `SIMKL_RATINGS_DATE_FROM`)
3. **Env global** (`SIMKL_DATE_FROM`)
4. **Config** (`simkl.date_from`)
5. **Hard default** (`1970-01-01T00:00:00Z`) - this is only used if there are issues and one-time. 

The selected value is normalized to ISO‑Z (`YYYY-MM-DDTHH:MM:SSZ`). Watermarks **only move forward**.

### Activities Gate (No‑op Skips)
Before building an index, the provider calls **`/sync/activities`** and compares the latest “changed at” timestamps to the saved watermarks. If nothing moved forward, the run **short‑circuits** (no network listing calls), keeping API usage minimal.

## Per‑Feature Behavior

### 1) Watchlist
- **Read endpoints:**  
  - `/sync/all-items/{bucket}/plantowatch` with `extended=full|ids_only`  
- **Buckets:** `movies`, `shows`, _there is currently no Anime support included._
- **Delta:** Uses `date_from` from watermarks to incrementally fetch *additions*.  
  - If activities report **removals**, the provider rebuilds the bucket using `ids_only` (cheap) and updates the watermark.
- **Shadow cache:** `/config/.cw_state/simkl.watchlist.shadow.json` keeps the last merged snapshot. If activities are unchanged and cache is fresh (<= TTL), it’s reused without calling the API.  
- **Write endpoints:**  
  - Add: `/sync/add-to-list`  
  - Remove: `/sync/history/remove`
- **Unresolved stash:** `/config/.cw_state/simkl.watchlist.unresolved.json` to avoid hammering problematic items.
- **IDs sent:** Uses best available among `simkl`, `imdb`, `tmdb`, `tvdb` (episodes also provide `show_ids` when necessary).

### 2) Ratings
- **Read endpoints:** `/sync/ratings/{movies|shows}?date_from=...`
- **Activities gate:** If latest rated timestamps haven’t advanced beyond the watermarks, the run is a no‑op.
- **Write endpoints:**  
  - Add/Update: `/sync/ratings` (1–10 scale; `0` means “remove”)  
  - Remove: `/sync/ratings/remove`
- **Write‑through shadow:** `/config/.cw_state/simkl.ratings.shadow.json` keeps locally written ratings so your next read reflects them even before SIMKL’s deltas catch up.
- **Unresolved stash:** `/config/.cw_state/simkl_ratings.unresolved.json`
- **IDs:** `simkl`, `imdb`, `tmdb`, `tvdb`.

### 3) History
- **Read endpoints:**  
  - `/sync/all-items/movies?date_from=...&extended=full`  
  - `/sync/all-items/episodes?date_from=...&extended=full&episode_watched_at=yes`
- **Activities gate:** Compares last watched/completed timestamps for movies/shows to per‑bucket watermarks; if unchanged → no‑op.
- **Write endpoints:**  
  - Add (scrobble): `/sync/history`  
  - Remove (unscrobble): `/sync/history/remove`
- **Unresolved stash:** `/config/.cw_state/simkl_history.unresolved.json`

## Health Checks
- A single **core probe** is used: `POST /sync/activities`. It exercises authentication and availability in one call and surfaces any **Retry‑After** and basic rate headers so your scheduler can back off if needed.
## Matching & Normalization
- The module builds an internal **minimal** record with title/year/type and an `ids` map.  
- For episodes, `show_ids` are propagated so that writes are fully qualified.  
- Canonical keys are stable across features, enabling safe merging and shadow reconciliation.

## Scheduling & Best Practices
- **Run at most twice per day** (every 12h). Prefer **once daily**.  
- Avoid back‑to‑back runs; rely on the **activities gate** for no‑op detection.  
- Keep the shadow TTL at the default unless you have a strong reason to reduce it.

## State & Files

All state files live under `/config/.cw_state`:
- `simkl.watermarks.json` — per‑feature `date_from` watermarks
- `simkl.watchlist.shadow.json` — latest watchlist snapshot (+ timestamp)
- `simkl.ratings.shadow.json` — write‑through cache for ratings
- `simkl.watchlist.unresolved.json`, `simkl_ratings.unresolved.json`, `simkl_history.unresolved.json` — unresolved queues

These files are safe to keep across runs and help the module minimize calls and retries.