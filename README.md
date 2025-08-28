# CrossWatch (Tauri v2 + React) — Fixed

## Dev
```powershell
pnpm install
pnpm tauri:dev
```

- Vite dev port: 5174 (adjusts tauri.conf.json accordingly)
- Valid Windows icon at `src-tauri/icons/icon.ico` (BMP-based, no PNG CRC issues)
- Settings-first UI: sync.mode, runtime.debug, Plex & SIMKL auth
- Python sidecar: runs `resources/python/plex_simkl_watchlist_sync.py --sync`

If you want to bundle a venv, wire `cmd_run_sync` to that python path.
