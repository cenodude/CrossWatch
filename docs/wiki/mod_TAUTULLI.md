# TAUTULLI (History Import)

> **⚠️ Important:** **Tautulli is not a tracker and not a media server.**  
> CrossWatch uses it as a **read-only import source** to pull *existing* watch history from a long-running Tautulli database.  
> **Do NOT use Tautulli to “sync” into a media server or external tracker** (Plex / Jellyfin / Trakt / SIMKL / etc.).  

> The supported use cases are:
> - Sync Tautulli history into the **CrossWatch Tracker** (so you have snapshots and can sync *from the Tracker* later).
> - Import Tautulli history as a **dataset in the Editor** so you can review/edit/clean it.

This module integrates **Tautulli** with CrossWatch’s sync engine as an **importer**. It can **index History** from Tautulli and produce CrossWatch-normalized items. **Writes are disabled** by design.

## Capabilities
- **Source-only / Read-only**
- **One-way only**
- **History indexing**:
  - `GET /api/v2?cmd=get_history` (paginated)
  - Optional metadata enrichment: `GET /api/v2?cmd=get_metadata&rating_key=...`
- **No writes**
  - `add()` / `remove()` always return an error (“read-only”).


## Data model

The module outputs CrossWatch “minimal items” keyed by normalized IDs.

### Movie
```json
{
  "type": "movie",
  "ids": { "imdb": "tt123", "tmdb": "456", "tvdb": "0", "plex": "12345", "guid": "plex://movie/..." },
  "title": "…",
  "year": 2024,
  "watched_at": "2025-12-30T23:54:11Z"
}
```

### Episode
```json
{
  "type": "episode",
  "ids": { "imdb": "ttSHOW", "tmdb": "157741", "tvdb": "397424", "plex": "40759", "guid": "plex://show/..." },
  "title": "Episode title",
  "year": 2025,
  "season": 2,
  "episode": 4,
  "watched_at": "2025-12-30T23:54:11Z",

  "series_title": "Show title (when available)",
  "show_ids": { "imdb": "ttSHOW", "tmdb": "157741", "tvdb": "397424", "plex": "40759", "guid": "plex://show/..." }
}
```

Notes:
- Canonical keys prefer external IDs (`imdb`/`tmdb`/`tvdb`) and include season/episode for episodes.
- Tautulli history is indexed as a **de-duplicated snapshot**: newest entry wins per canonical key.
- `series_title` is derived from Tautulli’s “grandparent” title when present (or from metadata fallback).

## How indexing works

1. Pull history pages from `get_history` until:
   - `max_pages` reached, or
   - returned rows < `per_page`, or
   - total indicates end, or
   - repeated pages detected (safety stop).
2. Keep only `media_type in {movie, episode}`.
3. Extract IDs from:
   - `guid` / `guids` (and `grandparent_guid(s)` for episodes),
   - plus Plex/Tautulli keys (`rating_key`, `grandparent_rating_key`).
4. If an item only has Plex ids (or show ids are missing for episodes), try `get_metadata(rating_key)` to enrich external IDs.
5. Emit normalized items keyed by canonical id.

## Recommended workflows

### A) Import to CrossWatch Tracker (recommended)
Use the sync pair from Tautulli to Crosswatch to import history into the **CW Tracker** first.
- You get snapshots.
- You can then use the Editor to make your adjustments

### B) Import directly to Editor dataset
Import history as an Editor dataset if you want to:
- fix wrong matches,
- remove junk plays,
- merge duplicates,
- correct show titles/years,
before doing anything else.
