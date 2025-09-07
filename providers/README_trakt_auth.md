# CrossWatch – Adding a New Authentication Provider (Playbook)
This is the blueprint I followed to add **Trakt**. Use it to add the next provider with the same UX (Plex-style device/PIN or SIMKL-style OAuth).

---

## 0) What you’re adding (mental model)

- **Auth provider module** in `providers/auth/_auth_<PROVIDER>.py` that exposes a standard interface via `PROVIDER` (manifest + actions).
- **Registry** auto-discovers providers and renders their **HTML snippet** into Settings via `/api/auth/providers/html`.
- **Frontend JS** hydrates values from `/api/config`, wires buttons, shows hints, and triggers flows.
- **Backend route(s)** (optional) to start/poll a device flow if you want the Plex-like UX (`/api/<prov>/pin/new`).
- **Config** stores app keys under `<prov>.*` and tokens under `auth.<prov>.*`.

Keep the Plex and SIMKL approaches as canonical references:
- Plex = **device code** flow (opens verify page, shows PIN, server polls).
- SIMKL = **regular OAuth** (client id/secret + redirect, user redirected, no polling).

---

## 1) Files to create / touch

### Create
- `providers/auth/_auth_TRAKT.py`  
  Your provider module. For Trakt we implemented **device code** flow.

### Likely touch
- `providers/auth/registry.py`  
  (Usually no change, the registry auto-imports `PROVIDER` from your module.)
- `crosswatch.py`  
  Add an API endpoint if you want a Plex-style “Get Code” button:
  - `POST /api/trakt/pin/new` → starts device flow, spawns server-side polling.
- `static/crosswatch.js`  
  - Bind copy buttons.
  - Implement `requestTraktPin()` (Plex-like).
  - Implement `hydrateAuthFromConfig()` and `updateTraktHint()` integration.
- `static/crosswatch.css` (optional)
  - Brand color and header border accent (like Plex/SIMKL/TMDB).

You **don’t** need to hard-edit `_FastAPI.py` or templates if your provider exposes `html()` and the registry endpoint `/api/auth/providers/html` is already present (it is).

---

## 2) Provider module contract

Each module must export `PROVIDER` with these methods:

```py
class Provider:
    name = "TRAKT"      # machine id, used in registry
    label = "Trakt"     # display label

    def manifest(self) -> dict: ...
    def html(self, cfg: dict | None = None) -> str: ...
    def start(self, cfg: dict | None = None, *, redirect_uri: str|None = None) -> dict: ...
    def finish(self, cfg: dict | None = None, **kwargs) -> dict: ...
    def refresh(self, cfg: dict | None = None) -> dict: ...
    def disconnect(self, cfg: dict | None = None) -> dict: ...
```

### Manifest rules
Used by `/api/auth/providers` and the HTML endpoint:

```py
{
  "name": "TRAKT",
  "label": "Trakt",
  "flow": "device_pin" | "oauth",
  "fields": [
    # show field editors in Settings (client id/secret etc)
    {"key": "trakt.client_id", "label": "Client ID", "type": "text", "required": true},
    {"key": "trakt.client_secret", "label": "Client Secret", "type": "password", "required": true},
  ],
  "actions": {
    "start": true,
    "finish": true,        # server poller calls this repeatedly for device flow
    "refresh": true,
    "disconnect": true
  },
  "verify_url": "https://trakt.tv/activate",   # optional
  "notes": "Short help text shown in the Settings card."
}
```

### HTML rules
- Return a full `<div class="section" id="sec-<lower>"> ...` block.
- Have predictable element IDs so JS can wire events:
  - Inputs: `trakt_client_id`, `trakt_client_secret`, `trakt_token`, `trakt_pin`.
  - Buttons: `btn-copy-trakt-token`, `btn-copy-trakt-pin`, a button that calls `requestTraktPin()`.

### Config rules
- App keys go in `cfg["trakt"]["client_id"]`, `cfg["trakt"]["client_secret"]`.
- Tokens go in `cfg["auth"]["trakt"]["access_token" | "refresh_token" | "expires_at" | ...]`.
- For a device flow, stash pending state under `cfg["trakt"]["_pending_device"]`.

---

## 3) Trakt (device code) specifics

API endpoints used:
- `POST https://api.trakt.tv/oauth/device/code`  
  payload: `{"client_id": "<cid>"}`  
  → returns `device_code`, `user_code`, `verification_url`, `expires_in`, `interval`
- `POST https://api.trakt.tv/oauth/device/token`  
  payload: `{"code": "<device_code>", "client_id":"<cid>", "client_secret":"<secret>"}`  
  → returns `access_token`, `refresh_token`, `expires_in`, etc.
- `POST https://api.trakt.tv/oauth/token` (refresh)  
  payload: `{"grant_type":"refresh_token","refresh_token":"...","client_id":"...","client_secret":"..."}`

We mirrored Plex:
- The UI shows **Get Code** → JS calls `/api/trakt/pin/new`.
- Server calls `PROVIDER.start()` → saves `_pending_device` and returns `user_code` etc.
- Server **spawns a thread** that repeatedly calls `PROVIDER.finish()` until token is issued or timeout.
- UI optionally polls (we kept server-side polling like Plex).

> Trakt still needs **Client ID & Secret**, even for device code. We show a yellow hint until both are filled.

---

## 4) `_auth_TRAKT.py` highlights (what to copy next time)

- **Headers**: include `trakt-api-version: 2`, `Accept: application/json`.
- **start()**  
  - Requires `client_id`; fetch device code, store under `cfg.trakt._pending_device`.
- **finish()**  
  - Exchanges `device_code` → tokens, writes `cfg.auth.trakt.*`, clears pending on success.
  - Returns statuses like `authorization_pending`, `expired_token`, `slow_down`, `ok`.
- **refresh()**  
  - Exchanges `refresh_token` for new tokens, updates `expires_at`.
- **html()**  
  - Renders:
    - Client ID/Secret fields (with `oninput="updateTraktHint()"` and `onchange="saveSetting(...)"`)
    - A **hint** block when ID/Secret are missing, including a **Copy Redirect URL** button with a fixed `urn:ietf:wg:oauth:2.0:oob`
    - Current token, Link code (PIN), and a **Get Code** button that calls `requestTraktPin()`

---

## 5) `crosswatch.py` (server) – the small API you add

Plex-style endpoint (+ background poll):

```py
@app.post("/api/trakt/pin/new")
def api_trakt_pin_new():
    from providers.auth._auth_TRAKT import PROVIDER as TRAKT
    cfg = load_config()
    # start() returns and also persists pending info
    info = TRAKT.start(cfg)
    save_config(cfg)

    device_code = info["device_code"]
    user_code   = info["user_code"]
    verification_url = info.get("verification_url") or "https://trakt.tv/activate"
    expires_epoch = int(time.time()) + int(info.get("expires_in", 600))

    def waiter(dc: str):
        # poll until success or timeout
        deadline = time.time() + 600
        while time.time() < deadline:
            c = load_config()
            if (c.get("auth", {}).get("trakt", {}).get("access_token")):
                _append_log("TRAKT", "Token acquired.")
                return
            TRAKT.finish(c, device_code=dc)
            save_config(c)
            time.sleep(max(1, info.get("interval", 5)))

    threading.Thread(target=waiter, args=(device_code,), daemon=True).start()

    return {
        "ok": True,
        "user_code": user_code,
        "verification_url": verification_url,
        "expiresIn": max(0, expires_epoch - int(time.time()))
    }
```

---

## 6) `crosswatch.js` (frontend) – minimal hooks

### 6.1 Mount provider HTML and wire buttons
```js
async function mountAuthProviders() {
  const res = await fetch("/api/auth/providers/html");
  if (!res.ok) return;
  const html = await res.text();
  const slot = document.getElementById("auth-providers");
  if (slot) slot.innerHTML = html;

  // Plex copy buttons (existing)
  document.getElementById("btn-copy-plex-pin")?.addEventListener("click", (e) =>
    copyInputValue("plex_pin", e.currentTarget));
  document.getElementById("btn-copy-plex-token")?.addEventListener("click", (e) =>
    copyInputValue("plex_token", e.currentTarget));

  // Trakt copy buttons
  document.getElementById("btn-copy-trakt-pin")?.addEventListener("click", (e) =>
    copyInputValue("trakt_pin", e.currentTarget));
  document.getElementById("btn-copy-trakt-token")?.addEventListener("click", (e) =>
    copyInputValue("trakt_token", e.currentTarget));

  // Trakt hint bindings
  document.getElementById("trakt_client_id")?.addEventListener("input", updateTraktHint);
  document.getElementById("trakt_client_secret")?.addEventListener("input", updateTraktHint);

  await hydrateAuthFromConfig();  // put cfg values into inputs
  updateTraktHint();
  setTimeout(updateTraktHint, 0);
  requestAnimationFrame(updateTraktHint);
}

document.addEventListener("DOMContentLoaded", () => {
  try { mountAuthProviders(); } catch(_) {}
});
```

### 6.2 Hydration + Hint
```js
async function hydrateAuthFromConfig() {
  const r = await fetch("/api/config", { cache: "no-store" });
  if (!r.ok) return;
  const cfg = await r.json();

  setValIfExists("trakt_client_id",     (cfg.trakt?.client_id || "").trim());
  setValIfExists("trakt_client_secret", (cfg.trakt?.client_secret || "").trim());
  setValIfExists("trakt_token",
    cfg.auth?.trakt?.access_token || cfg.trakt?.access_token || "");

  updateTraktHint();
}

function updateTraktHint() {
  try {
    const cid  = document.getElementById("trakt_client_id")?.value?.trim();
    const secr = document.getElementById("trakt_client_secret")?.value?.trim();
    const hint = document.getElementById("trakt_hint");
    if (!hint) return;
    hint.classList.toggle("hidden", !!(cid && secr));
  } catch(_) {}
}
```

### 6.3 Device flow (Plex-like)
```js
async function requestTraktPin() {
  let win = null;
  try { win = window.open("https://trakt.tv/activate", "_blank"); } catch(_) {}

  try {
    const resp = await fetch("/api/trakt/pin/new", { method: "POST" });
    const data = await resp.json();
    if (!data || data.ok === false) throw new Error(data?.error || "PIN request failed");

    const pin = data.user_code || "";
    document.getElementById("trakt_pin")?.setAttribute("value", pin);
    document.getElementById("trakt_msg")?.classList.remove("hidden");

    try { await navigator.clipboard.writeText(pin); } catch(_){}

    // Optionally, poll config to reflect token when server finishes
    setTimeout(refreshTraktTokenUI, 3000);
  } catch (e) {
    console.warn("trakt pin fetch failed", e);
  }

  try { win?.focus(); } catch(_) {}
}

async function refreshTraktTokenUI() {
  try {
    const r = await fetch("/api/config", { cache: "no-store" });
    if (!r.ok) return;
    const cfg = await r.json();
    const token = cfg?.auth?.trakt?.access_token || "";
    if (token) {
      setValIfExists("trakt_token", token);
      document.getElementById("trakt_msg")?.classList.remove("hidden");
    }
  } catch(_) {}
}

// export legacy globals if needed
try { Object.assign(window, { requestTraktPin }); } catch(_){}
```

---

## 7) CSS (brand color + header bar)

Add variables and header accent, mirroring others:

```css
:root {
  --plex:  #e5a00d;
  --simkl: #00b7eb;
  --tmdb:  #01d277;
  --trakt: #ed1c24;   /* NEW */
}

/* Card header border */
#sec-trakt > .head {
  border-left: 3px solid var(--trakt);
  padding-left: 10px;
}
```

...

