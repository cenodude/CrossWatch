## What they do
- **Index** – Read current state for each feature (watchlist, history, ratings, playlists).
- **Apply** – Add/remove items to match the Orchestrator’s plan.
- **Normalize** – Map to canonical IDs for cross-provider matches.
- **Stabilize** – Use shadows/tombstones and caching to avoid flapping.

## Contract
- **Index** returns a normalized snapshot for a feature.
- **Apply** performs writes and reports successes/failures.
- Providers aim to be **idempotent** and resilient (retries/backoff).

## Feature semantics
- **Watchlist** – Set-like; add/remove as needed.
- **History** – Time-aware; may include play timestamps; deduplicated.
- **Ratings** – Typically 1–10; removing may affect list membership on some services.
- **Playlists** – Ordered; may need to create and then append unique items.

## settings for stability
- **Snapshot cache** for present reads.
- **Batch size & pause** for writes.
- **Shadows/Tombstones** to prevent flip-flopping.
- **Force fresh reads** when activities move.

**Sync troubleshooting**
- **Adds keep reappearing** → Ensure the shadow is enabled where external IDs are missing (e.g., Jellyfin).
- **Recent changes not syncing** → Lower the cache window or force a fresh present read.
- **429/5xx errors** → Smaller batches, small pause, enable retries.
- **Mismatches** → Prefer exact IDs (IMDb/TMDB/TVDB).