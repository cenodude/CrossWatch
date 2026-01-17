## Capabilities
- **Bidirectional provider** with feature support:
  - **Watchlist** (favorites, playlist, or collection modes)
  - **Ratings** (0–10 scale, accepts 0–5 stars input)
  - **History** (watched events for movies & episodes)
  - **Playlists**: *not implemented*
- **Present-state indexing** (no full GUID authority — we prefer external IDs when available)
- **Per‑feature knobs** for query limits, write delays, and ID priority

_Source of truth: provider manifest & config plumbing._

## How it works

### Normalization & IDs
The adapter normalizes Jellyfin items to a minimal, provider‑agnostic shape (`type`, `title`, `year`, `ids`, and where relevant `show_ids`, `season`, `episode`). It collects IDs from Jellyfin’s `ProviderIds` and related fields where possible. When show context exists (episodes/seasons), the module tries to add `show_ids` alongside item IDs. External IDs (tmdb/imdb/tvdb) are preferred when present.

### Watchlist

The module supports **three modes**:
1. **Favorites** (default) — reads/writes the user’s `IsFavorite` flag on movies and series.
2. **Playlist** — targets a user playlist by name (default: `Watchlist`) and treats items in that playlist as watchlist content. Be aware that the playlists can only hold Movies and Episodes (no shows!)
3. **Collection** — targets a specific collection (collection lookup/creation is handled internally).

Advice: use Favorites or Collections

Reads use the `/Users/{userId}/Items` route with filters and sorting for favorites; playlist/collection modes enumerate their items and normalize each entry. Writes set/unset favorites (or add/remove from the chosen playlist/collection). A small, configurable **write delay** can be applied to avoid rate spikes; after writes, the module re-fetches a single item to **verify** the new state before reporting success.

### Ratings
- Ratings are read and written on the **0–10** scale. If you provide **0–5** stars, they’re **up‑scaled** (×2) during writes.
- **Upsert** and **unrate** are supported:
  - Upsert: setting a numeric rating will create or update the user rating.
  - Unrate: setting `null` (or removing) clears the rating for the item.
- During reads, the module paginates through user items that have ratings; during writes, it posts the numeric value and (optionally) verifies.

### History
- Reads recent **Movie** and **Episode** plays via `/Users/{userId}/Items` with `IsPlayed` filters, sorted by `DateLastPlayed`.
- Each play is converted into a normalized **event** with `watched_at` (ISO‑8601 Z), and for episodes, best‑effort `show_ids` enrichment.
- The module maintains a small **shadow** area for backfills to prevent short‑term gaps from being dropped when merging across providers.

> Note: Jellyfin does not expose unique “play event IDs” like some cloud providers. We approximate event identity by the canonical item key + timestamp during indexing.

### Important keys
- **Watchlist**:
  - `mode`: one of `favorites`, `playlist`, or `collection`.
  - `playlist_name`: only for `playlist` mode.
  - `watchlist_query_limit`: cap for read queries.
  - `watchlist_write_delay_ms`: small delay before writes.
  - `watchlist_guid_priority`: ID preference order when reconciling items across providers.
- **History**:
  - `history_query_limit`, `history_write_delay_ms` analogous to watchlist.
  - `history_guid_priority`: ID preference order for history rows.
  - `history_libraries`: optional include‑list of library IDs (strings).
- **Ratings**:
  - `ratings_query_limit`: client/UI hint for paging upper bounds.

> The UI mirrors these settings under **Settings → Synchronization Providers → Pairs (with Jellyfin)**.

---
## Health checks & diagnostics

- **Ping**: `GET /System/Ping`
- **System info**: `GET /System/Info`
- **User probe**: `GET /Users/{userId}`

The module persists a tiny **health shadow** JSON to assist troubleshooting. 

Log lines are prefixed with `[JELLYFIN:feature]` (e.g., `[JELLYFIN:ratings] …`).

---
## Unresolved / Shadow stores
- **Ratings unresolved**: `/config/.cw_state/jellyfin_ratings.unresolved.json`
- **History shadow (merge‑only)**: kept in memory by the module and/or persisted by the orchestrator

These help avoid infinite retries and provide a place to inspect items that could not be written or verified.

---
## Known limitations & notes
- Jellyfin’s local metadata sometimes lacks external IDs; cross‑provider matching may rely on titles/years or `show_ids` hints for episodes.
- Some write paths do **read‑back verification**. If your server is under heavy load or using slow storage, consider increasing write delays.
- The adapter assumes **user‑scoped** operations. If syncing for multiple users, create separate configs/pairs (one `user_id` per pair).
- Playlists as “watchlists” currently use a single, named playlist. Collections mode is supported but depends on consistent server permissions.


