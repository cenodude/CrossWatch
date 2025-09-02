#!/usr/bin/env bash
# Dev-oriented sync runner: uses PlatformManager + Orchestrator (no legacy CLI).
set -Euo pipefail

log(){ echo "[$(date -Iseconds)] $*"; }

: "${RUNTIME_DIR:=/config}"
: "${WEBINTERFACE:=yes}"
: "${PROFILE_ID:=}"

mkdir -p "${RUNTIME_DIR}"

if [[ "${WEBINTERFACE,,}" == "yes" ]]; then
  log "[SKIP] WEBINTERFACE=yes → web mode, not running sync here."
  exit 0
fi

# Run a single profile via Python so cron/CLI can use it even without the web server up.
python - <<'PY'
import os, json, sys, time
try:
    from platform.manager import PlatformManager
    from platform.orchestrator import Orchestrator
except Exception as e:
    print(f"[RUN] Missing platform modules: {e}")
    sys.exit(1)

def load_config():
    import json
    for p in ("/config/config.json","/app/config.json"):
        try:
            return json.load(open(p,"r",encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_config(cfg):
    try:
        with open("/config/config.json","w",encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        print(f"[RUN] Failed to save config: {e}")

pm = PlatformManager(load_config, save_config, profiles_path=None)
# inject default path if None
from pathlib import Path
pm.sync.profiles_path = Path(os.environ.get("RUNTIME_DIR","/config")) / "profiles.json" if hasattr(pm, "sync") else Path("/config/profiles.json")
# Fallback: PlatformManager without SyncManager in consolidated version
if not hasattr(pm, "sync_profiles"):
    # Consolidated PlatformManager API from previous steps
    pass

# Pick profile
profs = pm.sync_profiles()
pid = os.environ.get("PROFILE_ID","") or next((p.get("id") for p in profs if p.get("id")=="PLEX→SIMKL"), None) or (profs[0]["id"] if profs else None)
if not pid:
    print("[RUN] No profiles available.")
    sys.exit(0)

orc = Orchestrator(load_config, save_config, pm)
rep = orc.run_profile(pid)
print("[RUN] Report:", json.dumps(rep, indent=2))
PY
