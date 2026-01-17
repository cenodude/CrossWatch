## What it is
The Orchestrator is the brain of the operation. It reads your settings, asks each provider what you have, computes what needs to change, and applies those changes safely.

## Responsibilities
- Load and validate configuration (pairs, features, runtime flags).
- Build **sync pairs** (e.g., Plex → Jellyfin) with feature scopes (watchlist, history, ratings, playlists).
- Query provider modules for **present snapshots** (what exists right now).
- Compute a **plan** (adds/removes per side) by diffing normalized items.
- **Apply** the plan in chunks with retries and safety checks.
- Emit **insights** and logs (changes, failures, durations).

## Key concepts
- **Pair** – A source and a target (one-way or two-way).
- **Feature** – Watchlist, history, ratings, playlists.
- **Present snapshot** – The provider’s current state used for planning.
- **Plan** – The adds/removes needed to reconcile A and B.
- **Shadow / Tombstone** – Local notes to avoid duplicates and remember intentional removals.
- **Chunking** – Batch writes to avoid timeouts and rate limits.

## How a run works
1. **Initialize** – Read your settings; apply flags (debug, batching, cache).
2. **Index** – For each enabled feature, get current snapshots from providers.
3. **Plan** – Build deltas: what to add/remove on each side; resolve conflicts in two-way mode.
4. **Apply** – Write in **chunks**; update shadows/tombstones for stability.
5. **Verify & summarize** – Optional spot-checks and a final summary (`+X / -Y` per feature).

## Modes
- **One-way (A → B)** – A is the source of truth; B follows.
- **Two-way (A ↔ B)** – Both sides contribute; a conflict policy decides.

## Configuration that matters
- Cache window for “present” reads.
- Batch size and pause for writes.
- Debug level (module and network detail when needed).
- Where to store run state (shadows, tombstones).

## Performance & reliability
- Chunking avoids rate limits/timeouts.
- Snapshots are cached briefly to speed up re-runs.
- Retries/backoff handle transient errors.
- Shadows/tombstones stabilize providers that lag or lack external IDs.

## What you’ll see during a run
- Clear start/finish markers for each feature/pair.
- Planned vs applied counts.
- Warnings for items that couldn’t be matched or written (with reasons).
- A simple end summary.