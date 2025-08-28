# Plex ⇄ SIMKL Desktop (Tauri v2 + React) — Settings-first

## Dev
```powershell
pnpm install
pnpm tauri:dev
```

## Build
```powershell
pnpm tauri:build
```

## Settings
- Edit all config in-app (sync mode, debug)
- Plex: manual token OR PIN flow
- SIMKL: enter client_id/secret → Connect SIMKL (local callback)
- Config stored at OS app config dir (or ./config/config.json)
