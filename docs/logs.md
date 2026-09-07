# Logs

Open **View details** on Main and choose the expand icon beside Copy to open the Logs page. The selected Sync, Watcher or Debug tab follows you to the page. Events also offers **View logs** for a sync run.

Search across the available log, filter by level or provider, or select the error and warning counts to focus on problems. Expand a line to see the surrounding messages. Scrolling up pauses Follow live; **Resume live** returns to the latest output. The More menu offers line wrapping, copying and downloading the filtered or full log.

Choose a **Sync pair** to see that pair's Sync or Debug messages, including within a run containing several pairs. **Latest run** opens its most recent recorded run and clears the text, level and provider filters. Pair cards also have a **Latest logs** icon. Pair selection is retained when switching between Sync and Debug, and applies to search, counts, surrounding context and exports. General run messages without a pair remain available under All pairs. Pinning or deleting still acts on the entire saved log.

Pair attribution is recorded for new logs; existing development logs are not backfilled. Watcher activity uses its separate continuous log.

**Saved logs** contains each sync run and daily Watcher and Debug logs. Logs are saved on the server and survive restarts. Capture begins with this update; earlier in-memory logs cannot be recovered. The archive contains the messages emitted by the existing logging configuration, so it cannot recover debug messages that were never emitted.

Unpinned logs expire after seven days. A shared 100 MB log-data budget removes the oldest completed, unpinned logs sooner when necessary. **Keep this log** protects a log from automatic cleanup. If active and pinned logs fill the budget, capture stops accepting new lines until space is available; affected logs show a notice. SQLite metadata and journal files use some additional disk space.

You can download or delete individual completed logs, or clear completed, unpinned logs in the current channel. Clearing the quick live view does not delete the saved archive. Watcher and Debug follow their existing administrator-only access; sync logs require access to every provider instance involved in the run, including the selected profile's restrictions.

The archive is stored in `logs.db` beside the CrossWatch SQLite database. Keep that directory on persistent storage when running CrossWatch in a container.
