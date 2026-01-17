## Configuration

CrossWatch reads and writes a single `config.json`. Most settings are editable in the UI (**Settings**). This page documents the schema.

### Location
- **Docker**: `/config/config.json`
- **Bare‑metal/dev**: project root (auto‑resolved)

### Top‑level layout
```jsonc
{
  "plex": {}, "simkl": {}, "trakt": {}, "tmdb": {"api_key": ""}, "jellyfin": {},
  "emby": {}, "sync": {}, "runtime": {}, "metadata": {}, "scrobble": {},
  "scheduling": {}, "pairs": [], "features": {}
}
```

### Provider essentials

#### Plex
- Auth/server: `server_url`, `account_token`, `client_id`, `machine_id`, `username`, `account_id`, `verify_ssl`
- Parallelism: `rating_workers`, `history_workers`
- Watchlist: `watchlist_allow_pms_fallback`, `watchlist_page_size`, `watchlist_query_limit`, `watchlist_write_delay_ms`, `watchlist_title_query`, `watchlist_use_metadata_match`, `watchlist_guid_priority`
- History/Ratings libraries: `history.libraries`, `ratings.libraries`
- Misc: `timeout`, `max_retries`, `fallback_GUID` (ID fallback resolver), `_cloud.account_id`

#### Trakt
- OAuth: `client_id`, `client_secret`, `access_token`, `refresh_token`, `expires_at`, `scope`, `token_type`
- Watchlist: `watchlist_use_etag`, `watchlist_shadow_ttl_hours`, `watchlist_batch_size`, `watchlist_log_rate_limits`, `watchlist_freeze_details`
- Ratings: `ratings_per_page`, `ratings_max_pages`, `ratings_chunk_size`
- History: `history_per_page`, `history_max_pages`, `history_unresolved`, `history_number_fallback`, `history_collection`
- Network: `timeout`, `max_retries`

#### SIMKL
- OAuth: `client_id`, `client_secret`, `access_token`, `refresh_token`, `token_expires_at`
- Internal anchors: `date_from` (used by incremental syncs)

#### Jellyfin / Emby
- Auth/server: `server`, `access_token`, `user_id`, `verify_ssl`, `timeout`, `max_retries`
- Watchlist: `watchlist.mode` (`favorites|playlist|collection`), `playlist_name`, `watchlist_query_limit`, `watchlist_write_delay_ms`, `watchlist_guid_priority`
- History: `history_query_limit`, `history_write_delay_ms`, `history_guid_priority`, `history.libraries`
- Ratings: `ratings_query_limit`, `ratings.libraries`

#### TMDB
- `api_key` (optional but very helpful for GUID/metadata lookups)

### Global sync (`sync.*`)
- Gates: `enable_add` (default **true**), `enable_remove` (default **false**)
- Safety: `verify_after_write`, `dry_run`, `drop_guard`, `allow_mass_delete`,
  `tombstone_ttl_days`, `include_observed_deletes`
- Two‑way defaults: `bidirectional.enabled`, `bidirectional.mode`, `bidirectional.source_of_truth`
- **Blackbox**: `enabled`, `promote_after`, `unresolved_days`, `pair_scoped`, `cooldown_days`, `block_adds`, `block_removes`
- Run‑time heuristics: `runtime.suspect_min_prev`, `runtime.suspect_shrink_ratio`

### Runtime & metadata
- Debug: `runtime.debug`, `debug_http`, `debug_mods`
- Performance: `snapshot_ttl_sec`, `apply_chunk_size`, `apply_chunk_pause_ms`
- Telemetry/state: `state_dir`, `telemetry.enabled`
- Metadata cache: `metadata.locale`, `metadata.ttl_hours`

### Scrobbler
- Switch: `scrobble.enabled`
- Modes: `mode` = `watch` (poll/watch provider) or `webhook` (receive events)
- Deletion guard: `delete_plex`, `delete_plex_types`
- **watch**: `autostart`, `provider`, `pause_debounce_seconds`, `suppress_start_at`, `filters` (e.g. `username_whitelist`, `server_uuid`, `user_id`)
- **webhook**: `pause_debounce_seconds`, `suppress_start_at`, `suppress_autoplay_seconds`,
  `probe_session_progress`, provider‑specific `filters_*`, `post_stop_play_guard_seconds`,
  `start_guard_min_progress`, `guard_autoplay_seconds`, `cancel_checkin_on_stop`, `anti_autoplay_seconds`
- **Trakt sink** thresholds: `stop_pause_threshold`, `force_stop_at`, `regress_tolerance_percent`, `complete_at`

### Scheduling
- Switch: `scheduling.enabled`
- Simple: `mode` = `hourly | every_n_hours | daily_time`, plus `every_n_hours` or `daily_time` (HH:MM, 24h)
- Advanced: `advanced.enabled` with `jobs[]` (id, pair_id, at/after/days, active)

### Pairs
Each pair declares a **source → target** and per‑feature actions.
```jsonc
{
  "source": "PLEX",
  "target": "TRAKT",
  "mode": "one-way",      // or "two-way" (when supported)
  "enabled": true,
  "features": {
    "watchlist": { "enable": true, "add": true, "remove": true },
    "ratings":   { "enable": false, "add": false, "remove": false, "types": ["movies","shows","episodes"], "mode": "all", "from_date": "" },
    "history":   { "enable": false, "add": false, "remove": false },
    "playlists": { "enable": false, "add": true, "remove": false }
  }
}
```
> Tip: keep `remove=false` until you’ve verified adds look sane. Your current pairs run **PLEX → SIMKL** and **PLEX → TRAKT** for *watchlist* only.

### Features
Top‑level toggles for app modules.
- `features.watch.enabled`: enable the watch UI/components.

---

## Notes
- Keep a copy of `config.json` before big changes.
- Use TMDB key for better GUID matching; it pays off in fewer “unresolved” items.