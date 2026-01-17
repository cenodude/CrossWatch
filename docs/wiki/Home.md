**CrossWatch** is a synchronization engine that keeps your **Plex, Jellyfin, Emby, Simkl, Trakt and MDBlist** in sync. It runs locally with a web UI to link accounts, configure sync pairs, run them manually or on schedule, and track stats/history. 

<h2>Features</h2>

<div align="center">

<table border="0" cellspacing="0" cellpadding="10">
  <tr>
    <td valign="top" align="left" width="520">

<b>Core features</b>

<ul>
  <li>Sync watchlists (one-/two-way)</li>
  <li>Live scrobble (Plex/Jellyfin/Emby → Trakt/SIMKL/MDBList)</li>
  <li>Sync ratings (one-/two-way)</li>
  <li>Sync watch history (one-/two-way)</li>
  <li>Keep snapshots with CW tracker</li>
  <li>Auto-remove from watchlist after finish</li>
</ul>
    </td>
    <td valign="top" align="left" width="520">

<b>Tools & modes</b>

<ul>
  <li>Analyzer: finds broken or missing matches/IDs</li>
  <li>Exporter: CSV files for popular services</li>
  <li>Editor: Edit and adjust your items</li>
  <li>Now Playing card, Stats, history, live logs</li>
  <li>Headless scheduled runs</li>
</ul>

<b>Trackers</b><br/>
<img alt="CrossWatch" src="https://img.shields.io/badge/CrossWatch-7C5CFF?labelColor=1f2328&logoColor=white" />
&nbsp;<img alt="SIMKL" src="https://img.shields.io/badge/SIMKL-0AAEEF?labelColor=1f2328" />
&nbsp;<img alt="AniList" src="https://img.shields.io/badge/AniList-02A9FF?labelColor=1f2328" />
&nbsp;<img alt="Trakt" src="https://img.shields.io/badge/Trakt-ED1C24?labelColor=1f2328" />
&nbsp;<img alt="MDBList" src="https://img.shields.io/badge/MDBList-3B73B9?labelColor=1f2328" />


<b>Media servers</b><br/>
<img alt="Plex" src="https://img.shields.io/badge/Plex-E08A00?logo=plex&logoColor=white&labelColor=1f2328" />
&nbsp;<img alt="Jellyfin" src="https://img.shields.io/badge/Jellyfin-946AD9?logo=jellyfin&logoColor=white&labelColor=1f2328" />
&nbsp;<img alt="Emby" src="https://img.shields.io/badge/Emby-52B54B?logo=emby&logoColor=white&labelColor=1f2328" />

<b>Others</b><br/>
<img alt="Tautulli" src="https://img.shields.io/badge/Tautulli-FF5C5C?labelColor=1f2328" />
    </td>
  </tr>
</table>

</div>

## Limitations
CrossWatch is lightweight: no database, JSON files only. That keeps it simple but caps scale.
- **Not for very large libraries.** JSON state and provider rate limits become the bottleneck.
- **Soft size guidance** (varies by hardware; SSD + 2 GB RAM recommended for):
  - **Watchlist:** stable up to ~2,500 items total across providers.
  - **History (plays):** stable up to ~10,000 play events total.
  - **Ratings:** stable up to ~10,000 ratings total.
- Above these ranges you may see slower planning, large state files, and long first-run times.
- Heavy backfills (full history/ratings) can take hours. Prefer incremental windows when possible.
- **Ratings** can take a long time; i don’t cache ratings (by design).
- **Only for Movies and Shows** currently there is no Anime support.

## Known Issues
- Some UI sections don’t auto-refresh reliably. When in doubt, **Ctrl+F5** for a clean reload.
- Stale UI data after long runs: **Ctrl+F5** again.
- **Statistics** may not always display the exact numbers yet.

## Next steps
- Read the wiki: [[Getting Started]]
- File issues here: https://github.com/cenodude/CrossWatch/issues  
- Back up before experiments. Don’t say I didn’t warn you. 😉
