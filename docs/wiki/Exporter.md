# What it is, what you need, how to use it
Exporter converts your runtime data (`state.json`) into clean, import‑friendly **CSV files** for popular services. It does **not** change data on any service; it just prepares files you can import elsewhere. CrossWatch remains a **synchronizer**, not a media tracker.

---

## What does it actually do?
- **Reads** items from `state.json` per provider & feature (*watchlist*, *history*, *ratings*).
- **Filters** via search (title / year / id), supports select‑all or per‑item selection.
- **Exports** to CSV formats that downstream tools accept:
  - **TMDB** (auto‑dialect: IMDb v3 / Trakt v2 / SIMKL v1)
  - **Letterboxd**, **IMDb (list)**, **JustWatch**, **Yamtrack**
- **Skips unmatchable rows** when required (e.g., TMDB import without IMDb IDs), so your imports don’t fail.
- **Respects** your choices: provider, feature, format, and selection.

> Heads-up: TMDB accepts **Watchlist** and **Ratings** imports; **History** is not supported by TMDB and is intentionally blocked.

---

## What do I need?
- A valid `state.json` (runtime state). If it’s missing, Exporter opens safely with an empty table.
- For TMDB imports, items ideally have **IMDb IDs**. Items without an IMDb ID are skipped to avoid bad matches.
- No additional API keys are required for exporting.

---

## How to use the UI
- Open **Exporter** (next to Analyzer).
- Choose a **Provider** (Trakt, Plex, Emby, Jellyfin, SIMKL).
- Choose a **Feature**: **Watchlist**, **History**, or **Ratings**.
- Choose a **Format** (e.g., **TMDB**, **Letterboxd**, **IMDb (list)**).
- Use **Search** to narrow by title/year/ids (e.g., `imdb:tt1234567` or `2024`).
- Toggle **Select all (filtered)** *or* tick items manually; the **counter** shows what will be exported.
- Click **Export** to download the CSV.

---

## Where do these files go?
- **TMDB**: Use TMDB’s import screen. Exporter auto‑picks the CSV layout TMDB recognizes:
  - From **TRAKT** → **Trakt v2** CSV
  - From **SIMKL** → **SIMKL v1** CSV
  - From everything else → **IMDb v3** CSV
  > Note: TMDB only supports watchlist & ratings imports.

- **Letterboxd**: Import on Letterboxd (watchlist/history/ratings). Watchlist export is movies only by design (or rather by letterboxd design)
- **IMDb (list)**: Creates a list; works well for watchlists. However, there is NO official import provided by IMDb
- **JustWatch / Yamtrack**: CSVs intended for general workflows and other tools that accept similar layouts.

---

## Formats & destinations
| Format                         | Best for         | Notes                                                                 |
|--------------------------------|------------------|-----------------------------------------------------------------------|
| **TMDB (auto dialect)**        | TMDB import      | Watchlist & Ratings only. Picks IMDb v3, Trakt v2, or SIMKL v1 as needed. |
| **Letterboxd**                 | Lbx import       | Watchlist (movies only), History (with dates), Ratings (1–10).       |
| **IMDb (list)**                | IMDb lists       | Watchlist only; there is no official IMDB import                      |
| **JustWatch**                  | Aggregator tools | Generic IDs + metadata (no official JustWatch CSV import guaranteed). |
| **Yamtrack**                   | Custom workflows | Superset with provider/feature info for scripting/analytics.          |

---

## Troubleshooting
- **No `state.json`** → You’ll see an empty table and a disabled Export button (no error).
- **TMDB import failed** → Ensure the file uses the exact layout (Exporter handles this automatically) and that rows have **IMDb IDs**.
- **TMDB “history”** → Not supported by TMDB; export ratings or watchlist instead.
- **Zero items in preview** → Check your Provider/Feature and Search filter.
- **Export button disabled** → Nothing is selected; toggle **Select all (filtered)** or pick items manually.

---

## Notes
Exporter is a convenience tool. CrossWatch’s main job is **synchronization**—Exporter just helps you move lists between services when you need it.