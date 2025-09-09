# CrossWatch → Trakt Live Scrobbler — Handover

_Last updated: 2025‑09‑07_

## What this adds
Real‑time “**Now Watching**” scrobbling from **Plex** to **Trakt** using two input modes:

1. **Watch‑mode (recommended)** — a WebSocket listener to Plex Media Server (PMS).  
   - Converts `PlaySessionStateNotification` events into Trakt `/scrobble/start|pause|stop`.
   - Enriches missing episode info by querying Plex metadata (`/library/metadata/{ratingKey}`).
   - Robust ID resolution (TMDb/Tvdb/IMDb), retry/backoff on 5xx/timeouts, and de‑duplication to avoid spam.
2. **Webhook‑mode (optional)** — accepts Plex Webhooks at `POST /webhook/trakt` (multipart or JSON).

You can run watch‑mode only, webhook only, or both. All are feature‑flagged in `config.json`.

---

## File map (what was added/changed)

```
providers/
  scrobble/
    plex/
      watch.py        # WebSocket watcher → builds ScrobbleEvents, dedup, dispatch
    trakt/
      sink.py         # Trakt client + scrobble body builder, retries/fallbacks
    scrobble.py       # Event model, dispatcher, Plex metadata enrichment helpers
crosswatch.py         # Lifespan startup (auto-start watcher), /debug/watch/* endpoints, logger filter
```

### Roles in one line
- `watch.py` — connects to PMS WS, turns frames into normalized events, dedups, dispatches.
- `scrobble.py` — shared types + logic: ID extraction, progress calc, metadata enrichment.
- `trakt/sink.py` — posts to Trakt (`/scrobble/*`) with retries and smart episode handling.
- `crosswatch.py` — starts watcher on app boot (lifespan), exposes status/start/stop + logs, filters DEBUG by config.

---

## How it works (flow)

### 1) Watch‑mode (recommended)
- The watcher connects to `ws(s)://<PMS>/:/websockets/notifications?X-Plex-Token=...` using your **Plex account token**.
- For every `PlaySessionStateNotification`:
  1. Build a `ScrobbleEvent` (`start|pause|stop`, movie/episode, progress %, ids).
  2. If season/number missing for episodes, call PMS `GET /library/metadata/{ratingKey}` to enrich.
  3. **De‑dup** repeated `start/pause/stop` bursts for the same `sessionKey`.
  4. Dispatch to sinks (currently **Trakt**).

### 2) Webhook‑mode (optional)
- Route: `POST /webhook/trakt` (accepts `multipart/form-data` with `payload` file/field or raw JSON).
- (Optional) Validates `X-Plex-Signature` if you configure a `plex.webhook_secret`.
- Converts payload to the same `ScrobbleEvent` and dispatches to sinks.

### 3) Trakt scrobbling
- **Movies**: uses known IDs if present; otherwise title/year fallback.
- **Episodes**: prefers **show.ids + season/number**; falls back to `episode.ids` if needed.
- Retries on **502/503/504** and network timeouts with short backoff.
- On **404/422**, performs a search and retries with found IDs.

---

## Configuration

`config.json` is the single source of truth. The app reads `/config/config.json` (exposed at `GET /api/config`).  
You can update it via `POST /api/config` with the full JSON body.

```jsonc
{
  "plex": {
    "server_url": "http://<PMS-IP>:32400",
    "account_token": "YOUR_PLEX_TOKEN",
    "username": "YourPlexUser",
    "server_uuid": ""              // optional: accept only this PMS UUID
  },
  "trakt": {
    "client_id": "…",
    "client_secret": "…"
  },
  "auth": {
    "trakt": {
      "access_token": "…",
      "refresh_token": "…",
      "scope": "public",
      "token_type": "bearer",
      "expires_at": 0
    }
  },
  "features": {
    "watch": {
      "enabled": true,             // start WS watcher on boot (recommended)
      "options": {
        "allow_self_signed": true, // allow wss self-signed (LAN)
        "reconnect_backoff_min_seconds": 2,
        "reconnect_backoff_max_seconds": 60
      }
    },
    "webhook": {
      "enabled": false             // optional: accept POST /webhook/trakt
    },
    "scrobble": {
      "enabled": true,             // dispatch events to sinks
      "providers": {
        "trakt": { "enabled": true }
      },
      "filters": {
        "username_whitelist": ["YourPlexUser"], // required Plex account(s)
        "server_uuid": ""          // optional: enforce PMS UUID match
      }
    }
  },
  "runtime": {
    "debug": false                 // if true, DEBUG logs are shown
  }
}
```

### Defaults in code (safe)
Factory defaults ship with **all features disabled**:
```jsonc
"features": {
  "watch":   { "enabled": false, "options": { "allow_self_signed": false, "reconnect_backoff_min_seconds": 2, "reconnect_backoff_max_seconds": 60 }},
  "webhook": { "enabled": false },
  "scrobble": {
    "enabled": false,
    "providers": { "trakt": { "enabled": false }},
    "filters": { "username_whitelist": [], "server_uuid": "" }
  }
}
```

---

## Running

### Docker
- Ensure a **single worker** (watcher uses one background thread). The entrypoint runs `uvicorn` with 1 worker → OK.
- Confirm config: `curl -s http://127.0.0.1:8787/api/config`

### Bare uvicorn
```bash
uvicorn crosswatch:app --host 0.0.0.0 --port 8787 --workers 1
```

### Startup behavior
- Uses **FastAPI lifespan**. On boot it:
  - Runs `_on_startup()` if present.
  - **Starts the watcher** if `features.watch.enabled = true`.
  - On shutdown, stops the watcher.

---

## Endpoints (ops)

### Watch control
```
GET  /debug/watch/status    → {"has_watch":true|false,"alive":true|false,"stop_set":bool}
POST /debug/watch/start     → {"ok":true,"alive":true|false}
POST /debug/watch/stop      → {"ok":true,"alive":false}
```

### Logs
```
GET  /api/logs/dump?channel=TRAKT&n=80
GET  /api/logs/stream?channel=TRAKT
```

### Config
```
GET  /api/config
POST /api/config   (body = full JSON to replace config; returns new config)
```

### Webhook (optional if enabled)
```
POST /webhook/trakt
  Content-Type: application/json  OR  multipart/form-data (field "payload" with JSON)
```

---

## Quick tests

### 1) Is watcher healthy?
```bash
curl -s http://127.0.0.1:8787/debug/watch/status
# {"has_watch":true,"alive":true,"stop_set":false}
```

### 2) Live logs
```bash
curl -N 'http://127.0.0.1:8787/api/logs/stream?channel=TRAKT'
# look for: "connect …", "connected", then "dispatch start/pause/stop …"
```

### 3) Webhook (if enabled)
```bash
# movie
curl -s 'http://127.0.0.1:8787/webhook/trakt'   -H 'Content-Type: application/json'   --data '{"event":"media.play","Metadata":{"type":"movie","title":"Ping","duration":600000,"guid":"imdb://tt0878804"},"viewOffset":60000}'
```

---

## Behavior & safeguards

- **De‑duplication**:  
  - Only one `start` per session until a `pause` or `stop`.  
  - Repeated `pause` within 5s and ~same progress → ignored.  
  - Repeated `stop` within 5s → ignored.  
  - (Optional tweak available: allow a new `start` after reconnect/time threshold.)

- **Retry/backoff**:  
  - `502/503/504` and network timeouts are retried with short delays.  
  - Search fallback for `404/422` to map titles to proper IDs.

- **ID resolution preference**:  
  - For **shows/episodes**: prefer **TMDb** → **TVDb** → **IMDb**.  
  - For **movies**: any of `imdb/tmdb` works; title/year fallback exists.

- **Filters**:  
  - If `username_whitelist` is non‑empty, only these Plex accounts are scrobbled.  
  - If `server_uuid` is set, only events from that PMS are accepted.

- **Security**:  
  - Webhook can stay LAN‑only. Use `plex.webhook_secret` to validate signatures (optional).  
  - Watcher uses your Plex **account token**; treat it like a secret.

---

## Troubleshooting

- **`/debug/watch/status` shows `alive:false`**  
  - Missing `plex.account_token` or PMS not reachable (`server_url`).  
  - Verify with logs dump and `curl -I <server_url>` from the container.

- **Frequent WS reconnects**  
  - Connect directly to PMS; avoid reverse proxies that time out WebSockets.  
  - Keep a single worker.

- **Scrobble 502/timeout**  
  - Cloudflare hiccup; backoff will retry. Check later for `201`.

- **Scrobble 404 for episodes**  
  - Means bad/missing IDs. Enrichment + fallback should handle it; verify `parentIndex/index` & GUIDs are present.

- **No DEBUG lines**  
  - Set `"runtime.debug": true`; takes effect within ~2s.

---

## Dependencies

- `fastapi`, `uvicorn[standard]`
- `requests`
- `websocket-client`
- `python-multipart` (for webhook form parsing)

---

## FAQ (short)

**Do I need Plex Webhooks?**  
No. Watch‑mode doesn’t need them. Webhook is an optional alternative.

**Which Plex token should I use?**  
Your **Plex account token** is fine.

**Multiple users?**  
Use `features.scrobble.filters.username_whitelist` to limit scrobbling to specific Plex accounts.

---

## Pointers in code

- Start/stop watcher: `providers/scrobble/plex/watch.py :: WatchService`
- Event normalization: `providers/scrobble/scrobble.py :: from_plex_pssn / from_plex_webhook`
- Trakt posting/retries: `providers/scrobble/trakt/sink.py :: TraktSink.send`
- App lifecycle & endpoints: `crosswatch.py` (lifespan + `/debug/watch/*` + logs API)
- Logger & DEBUG gating: `_UIHostLogger` in `crosswatch.py`
