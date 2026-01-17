> My personal best-practice playbook. Use this as your default baseline; deviate only with intent.

## Suggested Default Setup
- **Recommended syncs:** Media server to tracker
- **Snapshots:** use the internal [CrossWatch Tracker](https://github.com/cenodude/CrossWatch/wiki/mod_CROSSWATCH) to keep snapshots of your data.
- **Watchlist:** Start **one-way** (server to tracker). Verify, then flip to **two-way** if stable.
- **History:** Run **one-way** once to seed. Then **disable History** and use **Webhook/Watcher** for new items.
- **Ratings:** Keep **one-way** (server to tracker).
- **Maintenance:** If you change modes/direction, **reset state & statistics**, run once, review results.
- **Alternative syncs (advanced):**
  - **Directions:**
    - Tracker ↔ tracker (SIMKL from/to Trakt) - Possible
    - Media server ↔ media server (Plex from/to Emby from/to Jellyfin) -   Possible, but expect quirks
    - Tracker to media server: **Not recommended** - Media-server backups are tricky—proceed only if you’re 100% sure and know what you're doing!

*If in doubt, keep it one-way*

## Generic
- **Conflicts:** **Source wins**. The configured **Source** in the pair is the system of record, no committee meetings, Source decides.
  - If **both changed** the same item since last sync → keep **Source** state; mirror to Target.
  - **Add vs Remove:** if Source removed and Target added → **remove** (Source wins).
- **Watchlist:** start **one-way**. If it looks sane after a few runs, switch to **two-way**.
- **History & Ratings:** keep **one-way**. Two-way is possible but rarely worth the noise. Your default should be Media server → Tracker.
- **Stability:** don’t keep tweaking pairs. If you do, run **Maintenance → Reset state & statistics**.
- **Scope:** only enable what you actually need, not everything CrossWatch can do.

## Trackers vs. Media Servers: Different Jobs, Different Flows
A **tracker** (SIMKL, Trakt, MDBlist) is your *taste profile*: what you watched, when you watched it, and how you rated it, across apps and devices, forever. A **media server** (Plex, Jellyfin, Emby) is your *jukebox*: it scans files, matches metadata, plays stuff for whoever’s on the couch today, and occasionally rematches when metadata shifts.

These are **not** the same job.

## Why **Media Server to Tracker** makes perfect sense
- The server knows what **actually played** at home.
- The tracker turns that into a **portable history and recommendations**.
- Multiple servers? Doesn’t matter, **one tracker** keeps the full picture.
- Ratings entered on the server can feed the tracker’s **global ecosystem**.

## Why **Tracker to Media Server** can backfires for history/ratings and therefore not recommended
- The server’s library **changes** (rematches, editions, specials), so pushed history/ratings **drift or vanish**.
- **Superset → subset problem (missing items on the server):**
  A tracker knows **everything you watch** (cinema, Netflix, flights, friends’ TV), a **superset** of titles. Your media server only knows **what’s on disk**, a **subset**.  
  Syncing **Tracker → Media Server** tries to write history/ratings for items the server **doesn’t have** (streaming-only, not yet added, different cuts/editions). Results:
  - **Ghost history:** “watched” appears later when you finally add the file, out of context.
  - **Flip-flops:** items come/go with rescans; history toggles unpredictably.

## Why **Media Server to  Media Server** is fragile (and when it’s okay)

- **Backup first.** Make a backup of each server’s **database/app data** before you sync (Plex, Jellyfin, Emby). Test you can **restore** it. If you can’t restore, don’t do two-way. Risks: lost/overwritten watched history, ratings, and lists if IDs shift.
- **Not actually the same library.** Different folders, editions/cuts, partial seasons, extras, 4K vs 1080p. A “mirror” is a myth; things **drift** as soon as paths or editions differ.
- **ID mismatches = chaos.** Plex GUIDs, Jellyfin/Emby IDs, TMDB/TVDB/IMDb remaps, season/episode offsets, specials vs. regular seasons. Small mismatch → **big, silent mis-writes** to the wrong item.
- **Cascade mistakes.** One bad match on Server A copies to B; later rematch on A makes B look “wrong,” you “fix” B, then A flips back. Hello **yo-yo**.

> [!TIP]
> Prefer **one-way** sync with a single source of truth. Two-way only if you’ve backed up, tested restore, **and accept the risks.**

---
## 1) Watchlist Sync
- **Start simple:** configure **one-way** from your tracker → server (or vice versa). Verify adds/removes are expected.
- **Graduate to two-way** only when:
  - Both sides show **stable IDs** (IMDb/TMDb/TVDB) for the same items.
  - A **few clean runs** show no surprise re-adds/removes.
  - You’ve tested both **adds and deletes** in each direction.
- **Conflict policy:** decide your **source-of-truth** up front. If both changed, which wins?
- **Quarantine / grace:** consider a short quarantine for new/just-removed items to avoid churn during premieres and metadata flips.

## 2) History & Ratings
- Default to **one-way** (**Media server → tracker**).
- For History and Ratings, **don’t enable Remove** it only makes sense in rare, specific cases.
- **Two-way** is technically possible but often **creates log noise** and “flip-flops” when providers disagree on episodes, cuts, or rating scales.
- If you must run two-way:
  - Pick a **source-of-truth** for conflicts.
  - Align **rating scales** (e.g., 1–10 vs. hearts/stars) and round consistently.
  - Expect **more chatter** in logs, this is normal.

## 3) Pairs & Configuration Hygiene
- **Do not** constantly change pair modes, directions, or features.
- If you do change them, run **Maintenance → Reset state & statistics** so deltas are recalculated cleanly.

## 4) Keep It Small and Simple
- Start with **one provider pair** and **one feature**.
- Add more **only when the current setup is boringly reliable**.
- Fancy features can wait. Predictability beats clever.

## 5) Scheduling & Runs
- **Avoid overlaps:** don’t schedule new runs if a previous one is still working.
- Run watchlist more frequently than history/ratings if needed; they don’t need the same cadence.

## 6) Logging & Debugging
- Stay in **normal logs** for day-to-day.
- Turn on **debug** only while investigating.

## 7) First-Run & Change Checklist
Before enabling two-way or after a major change:
- A clean **one-way** run shows expected **adds/removes** only.
- No repeated re-adds (ID mapping is stable).

## 8) When to Reset State
- You altered **library filters/whitelists** or provider credentials.
- You merged/split pairs or introduced new features to an existing pair.
> Resetting clears the planner’s memory so the next run rebuilds deltas from zero. With delta providers (e.g., SIMKL), avoid frequent resets, they defeat incremental sync and carry a possible risk of an API ban.

## 9) When **NOT** to Reset State
- You changed pair **direction** or **mode** (one-way ⇄ two-way).