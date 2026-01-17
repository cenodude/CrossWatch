## Capabilities
- **Bidirectional provider** (module-level) with feature toggles:
  - **Watchlist**: (favorites, playlist, or collection modes)
  - **History**: (implemented)
  - **Ratings**: (currently **disabled**)
  - **Playlists**: (currently **disabled**)
- **Present‑state indexing** (no full GUID authority — prefers external IDs where available)
- **Per‑feature knobs** for query limits, write delays, ID priority, and optional library scoping

_Source of truth: provider manifest, config dataclass, and feature modules._

---
## How it works

### Normalization & IDs
Items are normalized to a minimal, provider‑agnostic shape: `{type, title, year, ids}` with episode extras (`series_title`, `season`, `episode`) when applicable. IDs are collected from Emby’s `ProviderIds` (IMDB/TMDB/TVDB) and the Emby item `Id` is stored as `ids.emby`. External IDs are preferred for cross‑provider matching; a priority list determines which to use first.

### Watchlist
Three supported modes:
1. **Favorites** (default) uses the user’s `IsFavorite` flag on Movies and Series.
2. **Playlist** targets a user playlist by name (default: `Watchlist`) and treats its entries as watchlist items. Movies and Episodes are supported; Series are normalized via their episode’s series context where applicable. **Warning** dont use Playlist unless you have a specific goal.
3. **Collection** — targets a named collection as your watchlist. Creation is handled automatically when missing (best‑effort seed for folder creation).

Reads enumerate the chosen container (favorites / playlist / collection). Writes set/unset favorites (or add/remove playlist/collection entries). The module can apply a small **write delay** for stability and performs lightweight **read‑back verification** for favorites.

### History
Reads recent **Movie** and **Episode** plays using `IsPlayed` filters, turning each into a normalized **event** with `watched_at` (ISO‑8601 Z). Writes mark items played with an optional timestamp; the code tries the dedicated “played” endpoint and falls back to a user‑data write when needed. A small in‑module **shadow** helps merge short‑term gaps across providers. Feature flag is off by default while this stabilizes.

---
### Important keys
- **Top‑level**: `server`, `access_token`, `user_id`, `device_id`, `timeout`, `max_retries`, `verify_ssl`.
- **Watchlist**:
  - `mode`: `favorites` | `playlist` | `collection(s)`.
  - `playlist_name`: used by playlist/collection modes.
  - `watchlist_query_limit`, `watchlist_write_delay_ms`: tuning.
  - `watchlist_guid_priority`: ID preference order during reconciliation.
- **History**:
  - `history_query_limit`, `history_write_delay_ms`, `history_guid_priority`.
  - `libraries`: optionally scope to specific library IDs.
  - `force_overwrite`, `backdate`, `backdate_tolerance_s`: write semantics.
- **Ratings**:
  - `ratings_query_limit`, `libraries`.
  - `ratings_like_threshold`: convert numeric inputs to thumbs on writes.

> The UI mirrors these under **Settings → Synchronization Providers → Pairs (with Emby)**.

---
## Health checks & diagnostics

- **Ping**: `GET /System/Ping`
- **System info**: `GET /System/Info`
- **User probe**: `GET /Users/{userId}`

A tiny **health shadow** JSON is persisted to aid troubleshooting. Log lines are prefixed with `[EMBY:feature]` (e.g., `[EMBY:watchlist] …`).

---
## Unresolved / Shadow stores
- **Watchlist unresolved**: `/config/.cw_state/emby_watchlist.unresolved.json`
- **Ratings unresolved**: `/config/.cw_state/emby_ratings.unresolved.json`
- **History unresolved**: `/config/.cw_state/emby_history.unresolved.json`
- **History shadow (merge‑only)**: `/config/.cw_state/emby_history.shadow.json`
- **Health shadow**: `/config/.cw_state/emby.health.shadow.json`

These avoid infinite retries and expose items that could not be written or verified.

---
## Known limitations & notes
- Local Emby metadata can lack reliable external IDs; matching may fall back to titles/years or series hints for episodes.
- Some write paths use **read‑back verification**. Under heavy load or slow storage, consider a small write delay.
- The adapter assumes **user‑scoped** operations; for multiple users, configure separate pairs (`user_id` per pair).
- The provider index prefers **external IDs** and is cached briefly to reduce pressure on the server.
