CrossWatch integrates with **AniList** via its **GraphQL API**. This provider is currently focused on **Anime Watchlist** (AniList “Planning”) and is meant to bridge anime items into a multi-provider world where most systems prefer **IMDb/TMDB** over **MAL/AniList** IDs.

> [!IMPORTANT]  
> CrossWatch will never be 100% optimized for anime as the main focus remains **Movies / Shows**.  
> Combining both worlds in one tool is… difficult. Most providers want **IMDB/TMDB**, but AniList speaks **MAL/AniList**.  
> Bridging helps a lot, but **exotic anime titles may still fail**.<br><br>
> CrossWatch uses TMDB for metadata, but TMDB’s anime coverage isn’t complete. Expect many missing titles and wrong/mismatched metadata for Anime titles.<br><br>
> When used in a pair (for example, SIMKL to AniList), CrossWatch will try to map each item to the correct IMDb ID so it stays compatible with other providers. If no mapping is possible, or if you import an AniList dataset into the Editor, the item may only have a MAL ID. Other providers can’t use MAL IDs, which can result in duplicates (multiple entries for the same title).

## In a nutshell
- **Anime-compatible providers:** **SIMKL** and **AniList (Watchlist)**
- Default setups for **Plex** / **Jellyfin** etc don’t natively understand anime IDs (MAL / AniList), so CrossWatch does bridging and normalization.
  - Result: most things work, but **exotic anime titles may still fail**.

## Capabilities
- **Features:** Watchlist 
- **Auth:** **Access Token** (Bearer)
- **Bidirectional:** Add + Remove supported for Watchlist
- **Sync model:** **present snapshot**
- **IDs:** Prefers `anilist` and `mal`; uses shadow state to keep stable cross-provider matching

> [!IMPORTANT]  
> **Be kind to AniList’s API.** This module is lightweight (usually one index query), but bulk adds can generate extra lookups/searches.  
> **Daily is plenty** for normal usage; avoid hammering it unless you’re debugging.
> Currently AniList does not provide token refresh, each token is valid for one (1) year. Every year you need to configure AniList in auth providers again to receive a new token.

## Sync Model
AniList Watchlist is handled as a **present-state snapshot**:
- No watermarks and no `date_from`.
- Each `build_index(watchlist)` fetches the current AniList “Planning” list and builds the index.

## Shadow State (Bridging + Stability)
To survive imperfect matching between providers, AniList maintains a local shadow map:

```
/config/.cw_state/anilist_watchlist_shadow.json
```

It stores things like:
- `anilist_id`, optional `mal`
- `list_entry_id`
- `source_ids` (IDs from the originating provider key)
- `ignored` + `ignore_reason` for “not anime / no match” items

This shadow lets CrossWatch:
- reuse stable mappings on later runs
- avoid repeatedly searching titles that never match
- keep keys stable across multi-provider sync paths

## Per-Feature Behavior

### 1) Watchlist (AniList “Planning”)
**What it syncs**
- AniList **ANIME** entries with status **PLANNING** (no manga).

**Read (index)**
- Primary: `MediaListCollection(userId, type=ANIME, status=PLANNING)`
- Fallback: fetch full `MediaListCollection(userId, type=ANIME)` and filter to `PLANNING` locally

**Write**
- **Add:** `SaveMediaListEntry(mediaId, status=PLANNING)`
- **Remove:** `DeleteMediaListEntry(id)` (by list entry id)

**Resolve strategy (when adding/removing)**
1. Use `ids.anilist` if present
2. Else use `ids.mal` → resolve via `Media(idMal, type=ANIME)`
3. Else title search (`Page { media(search: ...) }`) + scoring (title/year/format); requires a strong match to proceed

**Ignored / non-anime / no-match**
- Items that can’t be resolved are marked **ignored** in the shadow with reason `not_anime_or_no_match`, so the module stops retrying them every run.

## Health Checks
A single probe is used:
- `query { Viewer { id } }`

It verifies auth + availability and returns latency plus any rate-limit headers it can see.

## Best Practices
- **Normal use:** run **daily**.
- Use **separate pairs** for Anime, and use pair-level library whitelisting to minimize non-anime items.
