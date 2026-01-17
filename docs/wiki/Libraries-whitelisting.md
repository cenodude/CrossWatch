# Whitelisting

Whitelisting tells CrossWatch **which libraries to touch** for each provider and feature.

We have two layers:
1. **Server-level whitelist** - lives in provider settings; applies to **all pairs** that use that provider.
2. **Pair-level whitelist** - lives in the pair modal; applies only to **that specific pair**.

If you never touch any whitelist, CrossWatch will try to see **all libraries** it can reach for a provider.

**IMPORTANT:** Server-level is maintained by the orchestrator and pair-level by the sync modules/adapters. Some features such as the dashboard depend on orchestrator `state.json`. So when using pair-level whitelisting, it can affect these functions.

## Why whitelisting exists
- You don’t want CrossWatch to crawl **20 libraries** if you only care about *Movies* and *TV-Shows*.
- Some libraries (e.g. *Live TV*, *Music*, *Photos*) are noise for history/ratings/scrobble.
- Large libraries cost time and API calls.
- You dont want to scrobble some of your libraries.

Whitelists keep CrossWatch focused and predictable.

## 1. Server-level whitelisting (provider settings)
Server-level whitelists live in **Settings - Authentication Providers** **for Plex, Emby and Jellyfin.**

They define, per authentication provider and per feature:
- **History** (H)
- **Ratings** (R)
- **Scrobble** (S)

<img alt="image" src="https://github.com/user-attachments/assets/e4337adc-2b45-40d4-826a-1a58f7862f59" />

### What “Scrobble” whitelisting does
Scrobble whitelisting controls which libraries are allowed to generate **real-time scrobbles** via:
- **Watchers** (polling/stream watchers)
- **Webhooks** (server push events)

If the Scrobble whitelist is empty, CrossWatch scrobbles **everything** (default behavior).

### How to set server-level whitelists

For **Plex / Jellyfin / Emby**:
1. Go to **Settings - Authentication Providers - [Plex | Jellyfin | Emby] - Settings**.
2. Make sure the server / user is connected.
3. Click **Load Libraries**.
4. Use the chips under **History (H)**, **Ratings (R)**, and the **Kermit-the-Frog green Scrobble (S)** to pick the libraries you want.
5. Click **Save**.

What the values mean (per feature):
- `libraries` **empty**  
  - *No server-level filtering*. All visible libraries for that provider can be used.
- `libraries` contains IDs (e.g. `["19","3"]`)  
  - Only those libraries are allowed for that provider/feature.

These whitelists are used by the orchestrator and will follow the selections for the pair UI.

> Note (Emby/Jellyfin): These servers use **virtual “Views”**. CrossWatch resolves items to the correct View before applying the whitelist, so scrobbles don’t leak from other libraries.

## 2. Pair-level whitelisting (per sync pair)

Pair-level whitelists live inside the **Pair configuration modal**.

You’ll see a block like:

> **Pair library whitelist**  
> Empty = use server-level whitelist.

With rows for:
- **History**
- **Ratings**

and chips for each library (Movies, Kids, etc.).

### About Scrobble and Pair-level
Scrobble whitelisting is **server-level only**.  
(Scrobble is an event-driven feature via watchers/webhooks, not a pair-driven sync feature.)

### How to use pair-level whitelists
1. Go to **Pairs**.
2. Click **Edit** on the pair you care about (e.g. `PLEX to SIMKL`).
3. Click on **Providers** and then **Plex, Emby or Jellyfin**.
4. Click **Load Libraries** if it isn’t populated already.
5. For **History** and/or **Ratings**, click the chips to toggle them **on/off**.
6. Save the pair.

Pair-level selection is stored inside the pair config and only applies to that one pair.

## 3. How server- and pair-level whitelists interact

Think of it as:
> **Server-level = maximum allowed scope** (orchestrator)  
> **Pair-level = per-pair subset** (sync module/adapter)


## 4. Troubleshooting
**I still see insight dashboard has a large amount of ratings/history.**
- The Insights dashboard is mainly built around the orchestrator instead of individual sync modules/adapters, so this affects what it shows. For example, if you have no whitelist at server level but do have a whitelist in a pair, the Insights dashboard may still show all items.
- Best is to match server-level whitelisting with your pair-level whitelisting as close as possible.

**I have whitelisting in a pair, but it doesn’t seem to work?**
- Check your `config.json` for that pair and make sure you have a `libraries` block in **history** and/or **ratings**, for example:
  ```json
  "pairs": [
    {
      "source": "PLEX",
      "target": "SIMKL",
      "features": {
        "history": {
          "enable": true,
          "add": true,
          "remove": false,
          "libraries": {
            "PLEX": [
              "9"
            ]
          }
        }
      }
    }
  ]
  ```

**Scrobble ignores my library selection (Plex/Emby/Jellyfin).**
- Confirm you selected libraries under **Scrobble (S)** in **Settings - Authentication Providers - [Provider] - Settings**.
- Re-run scrobble and check logs: CrossWatch logs whether an event was blocked by scrobble whitelisting.

**I want to reset and let CrossWatch see everything again.**
- In provider settings, clear all selected libraries for that feature so `libraries` becomes `[]`.
- In each pair, clear the Pair whitelist for that feature (no chips selected).
- Save and re-run.

Whitelisting is optional, but once you have large or messy setups it’s a good idea to scope things down here first, instead of hunting issues after the fact. If you don’t have pair-specific requirements, use **server-level whitelisting only**. Using pair-level whitelisting can reduce dashboard accuracy because Insights depends on orchestrator state.
