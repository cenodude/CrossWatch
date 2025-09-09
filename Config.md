# CrossWatch Configuration Reference (Full)

This document describes all configuration options, their defaults, and scope.

---

## plex
```json
"plex": {
  "server_url": "",
  "account_token": "",
  "client_id": "",
  "username": "",
  "servers": {
    "machine_ids": []
  }
}
```
- **server_url** *(string)*: Direct URL of Plex server (optional, autodetect normally).
- **account_token** *(string)*: Plex account token (from PIN auth).
- **client_id** *(string)*: CrossWatch client identifier.
- **username** *(string)*: Plex account username.
- **servers.machine_ids** *(list)*: Explicit server machine IDs.

Scope: provider-specific (Plex).

---

## simkl
```json
"simkl": {
  "access_token": "",
  "refresh_token": "",
  "token_expires_at": 0,
  "client_id": "",
  "client_secret": "",
  "date_from": ""
}
```
- **access_token / refresh_token**: OAuth tokens.
- **token_expires_at** *(epoch)*: expiry time of access token.
- **client_id / client_secret**: SIMKL app credentials.
- **date_from** *(string)*: Starting date for imports.

Scope: provider-specific (SIMKL).

---

## trakt
```json
"trakt": {
  "client_id": "",
  "client_secret": "",
  "_pending_device": {...}
}
```
- **client_id / client_secret**: Trakt app credentials.
- **_pending_device** *(object)*: Device auth flow details (temporary).

Scope: provider-specific (Trakt).

---

## auth.trakt
```json
"auth": {
  "trakt": {
    "access_token": "",
    "refresh_token": "",
    "scope": "public",
    "token_type": "Bearer",
    "expires_at": 0
  }
}
```
OAuth token store for Trakt.

---

## tmdb
```json
"tmdb": {
  "api_key": "YOUR_KEY"
}
```
- **api_key** *(string)*: TMDB API key.

Scope: global.

---

## sync
```json
"sync": {
  "enable_add": true,
  "enable_remove": true,
  "verify_after_write": false,
  "drop_guard": false,
  "allow_mass_delete": true,
  "tombstone_ttl_days": 30,
  "include_observed_deletes": true,
  "bidirectional": {
    "enabled": true,
    "mode": "",
    "source_of_truth": ""
  }
}
```
- **enable_add** *(bool, default true)*: Allow add ops. Global.
- **enable_remove** *(bool, default true)*: Allow remove ops. Global.
- **verify_after_write** *(bool, default false)*: Verify after writes. Global.
- **drop_guard** *(bool, default false)*: Prevent accidental wipe when provider returns empty. Global.
- **allow_mass_delete** *(bool, default true)*: Block large deletes when false. Global.
- **tombstone_ttl_days** *(int, default 30)*: TTL for tombstones. Global.
- **include_observed_deletes** *(bool, default true)*: If true, deletes propagate. Global.
- **bidirectional.enabled** *(bool, default true)*: enable two-way sync.
- **bidirectional.mode** *(string)*: "mirror", "two-way".
- **bidirectional.source_of_truth** *(string)*: Provider that wins conflicts.

---

## features
```json
"features": {
  "watch": {...},
  "webhook": {...},
  "scrobble": {...}
}
```
- **watch.enabled** *(bool)*: Enable live watch detection (plex websockets).
  - **allow_self_signed** *(bool)*: Accept self-signed SSL.
  - **reconnect_backoff_min_seconds / max_seconds** *(int)*: Reconnect timings.
- **webhook.enabled** *(bool)*: Enable webhook listening.
- **scrobble.enabled** *(bool)*: Enable live scrobbling.
  - **providers.trakt.enabled** *(bool)*: Enable Trakt scrobble.
  - **filters.username_whitelist** *(list)*: Only events from these usernames.
  - **filters.server_uuid** *(string)*: Restrict to server UUID.

Scope: feature-specific.

---

## runtime
```json
"runtime": {
  "debug": true,
  "state_dir": "",
  "telemetry": { "enabled": true }
}
```
- **debug** *(bool)*: Verbose logging.
- **state_dir** *(string)*: Directory for state persistence.
- **telemetry.enabled** *(bool)*: Enable/disable telemetry.

Scope: global.

---

## scheduling
```json
"scheduling": {
  "enabled": false,
  "mode": "hourly",
  "every_n_hours": 2,
  "daily_time": "03:30"
}
```
- **enabled** *(bool)*: Enable scheduled sync.
- **mode** *(string)*: "hourly" or "daily".
- **every_n_hours** *(int)*: Interval in hourly mode.
- **daily_time** *(string)*: HH:MM time in daily mode.

Scope: global scheduler.

---

## batches
```json
"batches": [
  {
    "name": "Default Batch",
    "enabled": true,
    "pair_ids": [],
    "id": "batch_default"
  }
]
```
Define groups of sync pairs that run together.

---

## pairs
```json
"pairs": [
  {
    "source": "PLEX",
    "target": "SIMKL",
    "mode": "two-way",
    "enabled": true,
    "features": {
      "watchlist": { "enable": true, "add": true, "remove": true }
    },
    "id": "pair_xxx"
  }
]
```
- **source/target**: Provider codes.
- **mode** *(string)*: "mirror", "two-way".
- **enabled** *(bool)*: Enable this pair.
- **features** *(object)*: Enable/disable specific sync features (watchlist, ratings, history, playlists).
- **id** *(string)*: Unique pair ID.

Scope: per-pair.

---

## ui
```json
"ui": {
  "providers_overlay_style": "dark"
}
```
UI preferences.

---

# Summary of Global vs Feature-specific

**Global (apply everywhere):**
- `sync.*` (all keys)
- `tmdb.api_key`
- `runtime.*`
- `scheduling.*`

**Feature-specific:**
- `features.watch`, `features.webhook`, `features.scrobble`
- `pairs[].features`
- `provider` sections (plex/simkl/trakt) — only affect that provider.

---
