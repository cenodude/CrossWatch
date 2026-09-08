# Interactive Sync mapping search

Editor, Interactive Sync and Analyzer use the same mapping workspace. Interactive Sync and Analyzer default to **This sync pair**. Editor defaults to **All pairs using this provider instance**, and its mapping scope picker can open a specific pair. Pair corrections take precedence over shared corrections for the same original item. Existing saved mappings remain shared.

Editor corrections stay pending until **Save changes**. Watched dates, ratings and progress remain editable with the normal Editor controls. Shared Editor corrections search TMDb metadata; selecting a pair enables that pair's destination catalogs. The Saved mappings view shows each correction's scope and opens the matching Editor view.

Search and automatic suggestions only appear for a configured destination instance. The server checks availability again on every search. Missing connections cannot borrow credentials from another instance. Manual mapping remains available.

| Destination | Search source |
| --- | --- |
| Plex | Plex Discover catalog with the configured account token |
| Jellyfin, Emby | Configured server library, scoped to the configured user |
| Kodi | Configured library through read-only JSON-RPC queries |
| Scrob | Scrob media search |
| Trakt | Trakt catalog |
| MDBList | MDBList catalog |
| SIMKL | SIMKL movie, TV and anime catalogs |
| Punchplay | Punchplay public catalog |
| TMDb Sync | TMDb catalog using that sync instance's API key |
| Floppy | Floppy's TMDb catalog search |
| FlickList, PublicMetaDB, Nuvio, Stremio | TMDb metadata fallback when a metadata API key is configured |

Plex instances with only a server token can use the metadata fallback. The fallback is labelled **TMDb metadata** and does not check destination availability. It uses the metadata settings key, separate from TMDb Sync credentials.

Direct search is not currently integrated for FlickList, PublicMetaDB, Nuvio or Stremio. PublicMetaDB's external API documents ID lookup, but no title-search endpoint. No public FlickList search contract was verified. Nuvio and Stremio use addon catalogs; this implementation does not search installed addons. BingeBase is excluded because it has no CrossWatch sync provider.

Suggestions require one exact title match, with a compatible year when known. They remain editable drafts. The user reviews show IDs and season/episode numbering, then saves the batch with one recalculation. **Suggest season and episode** resolves original TVDB/IMDb episode IDs through TMDb, or compares unique episode titles and compatible air dates against the chosen TMDb show. It updates show IDs and coordinates together as drafts. It requires a configured destination plus a TMDb metadata key (or the selected TMDb Sync instance key). It uses TMDb numbering; it does not infer destination-specific episode orders. Conflicting IDs, generic episode titles and ambiguous matches stay unchanged with a reason shown on each row. Lookups share a request cache within each batch and have request/time limits. Ambiguous matches are left for manual review. Search does not mark anything watched or apply sync changes.

MDBList search results are enriched with missing TMDb and IMDb IDs through its media-info batch endpoint, using the same configured destination instance. Results are joined by provider ID rather than response order. A result without a verified TMDb ID remains visible with an explanation, but cannot be selected or used by Auto match. Incomplete lookups are not cached, so Find matches can retry. Selecting a complete result updates the checked drafts' titles and IDs immediately; season and episode numbers remain unchanged until edited or suggested separately.

## Blocked items

The Editor's **Mappings & blocks** window manages saved corrections and block rules under the same provider, profile, feature and pair scope. **Blocked items** lists existing exclusions, including keys no longer present in current state. Select a source and feature to add a block by item key, or choose **Unblock** to remove a rule. These actions save immediately for future syncs; save or discard any pending Editor edits first.

Blocks automatically created to suppress a mapping's original identity remain attached to that correction. Manage them by editing or removing the mapping. An explicit block on the corrected item can be added or removed independently. Pair rules remain isolated from other pairs, and shared rules apply to every pair using that provider instance.

The separate **Manual overrides** source has been removed from the Editor. Existing saved selections open **Current State** instead, and existing corrections and blocks remain in storage. Correction editing still uses **Save changes**. Policy backups include both corrections and blocks.

## API references

- [Punchplay search contract](https://docs.punchplay.tv/api-reference/public-catalog/search-movies-shows-or-anime)
- [Floppy search contract tests](https://github.com/dannyvfilms/Floppy/blob/latest/src/api/tests/test_search.py)
- [Scrob media routes](https://github.com/ellite/scrob/blob/main/backend/routers/media.py)
- [Emby library API](https://dev.emby.media/doc/restapi/Browsing-the-Library.html)
- [Kodi JSON-RPC examples](https://kodi.wiki/view/JSON-RPC_API/Examples)
- [TMDb TV search](https://developer.themoviedb.org/reference/search-tv)
- [MDBList API](https://docs.mdblist.com/docs/api)
- [SIMKL API](https://github.com/SIMKL/API/blob/master/apiary.apib)
- [Trakt API](https://trakt.docs.apiary.io/)
- [PublicMetaDB external API](https://publicmetadb.com/api-docs)
- [Stremio catalog protocol](https://stremio.github.io/stremio-addon-sdk/protocol.html)
- [Nuvio backend and default catalog configuration](https://github.com/NuvioMedia/self-host/blob/main/.env.example)
