> **⚠️ Important:** MDBList currently provides **no backup/import/export** for user data. Use writes and deletes **with caution** Use the CW Tracker to keep some snapshots.

This module integrates **MDBList** with CrossWatch’s sync engine. It can **index** MDBList state (**watchlist**, **ratings**) and **apply changes** (add/remove watchlist items, upsert/unrate ratings). It’s idempotent, rate‑limit aware, and emits structured logs + progress ticks.
  
## Capabilities
- **Bidirectional sync** (engine decides direction per pair).
- **Present‑state indexing** for:
  - **Watchlist** – `GET /watchlist/items`
  - **Ratings** – `GET /sync/ratings` (paginated; **movies**, **shows**, **seasons**, **episodes**).
- **Writes**:
  - **Watchlist** – `POST /watchlist/items/add`, `POST /watchlist/items/remove`.
  - **Ratings** – `POST /sync/ratings`, `POST /sync/ratings/remove` (1–10 scale; supports movies/shows/seasons/episodes).
- **IDs normalized**: `imdb`, `tmdb`, `tvdb`. Watchlist writes send **IMDb/TMDb** only.
- **Instrumentation**: all HTTP calls use a retry wrapper; snapshot progress emits per‑feature ticks.

> **Update (2025‑11‑11):** MDBList fixed a server‑side bug where **show‑level ratings** were ignored. Show ratings now index and upsert correctly.

---
## Data model
Canonical minimal items keyed by external IDs:

- **Common IDs**: `imdb`, `tmdb`, `tvdb` (plus `mdblist` when present).
- **Watchlist item** → `{ "type": "movie|show", "ids": {...}, "title": "...", "year": 2024 }` (title/year when derivable).
- **Rating items** (normalized):
  - Movie → `{ "type": "movie", "ids": {...}, "rating": 1..10, "rated_at": "ISO-8601Z?" }`
  - Show → `{ "type": "show", "ids": {...}, "rating": 1..10, "rated_at": "ISO-8601Z?" }`
  - Season → `{ "type": "season", "ids": {...(tmdb|tvdb or show ids)}, "season": N, "rating": 1..10, "rated_at": "?" }`
  - Episode → `{ "type": "episode", "ids": {...(tmdb|tvdb or show ids)}, "season": S, "number": E, "rating": 1..10, "rated_at": "?" }`

---
## Watchlist

### Index
- `GET /watchlist/items` with `limit`, `offset`, `unified=1`.
- Rows normalize to minimal items; keys prefer `imdb|tmdb|tvdb|mdblist`, fallback to `title|year` only if needed.
- **Shadow cache**: `/config/.cw_state/mdblist_watchlist.shadow.json` (TTL in hours; optional live validation).
- **Unresolved**: `/config/.cw_state/mdblist_watchlist.unresolved.json` freezes repeated failures (`not_found`, invalid ids). Successful runs thaw matches.

### Add / Remove
- **Endpoints:** `POST /watchlist/items/add` and `POST /watchlist/items/remove`.
- **Payload:** 
  ```json
  { "movies": [{ "imdb": "tt123", "tmdb": 123 }], "shows": [{ "imdb": "tt456", "tmdb": 456 }] }
  ```
- Only **IMDb/TMDb** are sent for writes. Results aggregate `added/existing` or `deleted/removed`. Shadow is **busted** after successful writes.

---
## Ratings

### Index
- `GET /sync/ratings` (paginated). MDBList returns:
  - `movies[]`
  - `shows[]` (each may include `seasons[].episodes[]`)
  - Top‑level `seasons[]` and `episodes[]` (server convenience)
- The module flattens all four into a keyed map, keeping the newest `rated_at` per key.
- Cache: `/config/.cw_state/mdblist_ratings.index.json` for faster subsequent runs.
- Page size: `ratings_per_page` (default 200).

### Upsert / Unrate
- **Endpoints:** `POST /sync/ratings` and `POST /sync/ratings/remove`.
- **Supported kinds:** movies, shows, seasons, episodes.
- **Rules:** rating must be **1–10**; optional `rated_at` respected.
- **IDs:**
  - Show‑level: send `imdb|tmdb|tvdb` on the **show**.
  - Season‑level: send `tmdb|tvdb` on season if available; otherwise the **show** ids plus `number` are accepted.
  - Episode‑level: send `tmdb|tvdb` on episode if available; otherwise **show** ids with `season` + `number`.
- Batches chunked via `ratings_chunk_size` with adaptive delay `ratings_write_delay_ms` and backoff on `429/503` up to `ratings_max_backoff_ms`.
- Cache is updated or pruned after successful writes.

---
## Logging & Observability
- Feature logs: `[MDBLIST:watchlist] …` / `[MDBLIST:ratings] …` when `CW_DEBUG` or `CW_MDBLIST_DEBUG` are set.
- Health: pings endpoints to report `status`, `latency_ms`, rate‑limit headers, and `retry_after` on throttle.
- Progress ticks emitted via the snapshot progress factory.

---
## Rate limits & Retries
- All requests use the shared `request_with_retries` helper (timeouts + exponential backoff).
- `X-RateLimit-*` and `Retry-After` are honored. Ratings writes back off on `429/503` and resume chunking.

---
## Edge cases & Behavior
- **Missing API key** → init fails, health `down`.
- **IDs** → watchlist writes require **IMDb/TMDb**; TVDb is read for normalization but not sent for watchlist writes.
- **De‑duplication** → ratings index keeps the newest `rated_at` per key; write batches de‑dupe by chunk.
- **Timestamps** → `rated_at` normalized to ISO‑8601 Z when provided.
- **Scope** → **History** and **Playlists** are not implemented for MDBList.

---
## Safety note
MDBList offers **no data export/import or backup tools**. Use the CW Tracker so that you have some snapshots.