#!/usr/bin/env bash
set -Euo pipefail
: "${RUNTIME_DIR:=/config}"
: "${PROFILE_ID:=}"

python - <<'PY'
import os, json, sys
from pathlib import Path

def load_config():
    for p in ("/config/config.json","/app/config.json"):
        try:
            return json.load(open(p,"r",encoding="utf-8"))
        except Exception:
            pass
    return {}

def save_config(cfg):
    Path("/config").mkdir(parents=True, exist_ok=True)
    with open("/config/config.json","w",encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

try:
    from platform.manager import PlatformManager
    from platform.orchestrator import Orchestrator
except Exception as e:
    print(f"[RUN] missing platform modules: {e}")
    sys.exit(1)

pm = PlatformManager(load_config, save_config, profiles_path=Path(os.environ.get("RUNTIME_DIR","/config")) / "profiles.json")
profs = pm.sync_profiles()
pid = os.environ.get("PROFILE_ID") or next((p.get("id") for p in profs if p.get("id")=="PLEX→SIMKL"), None) or (profs[0]["id"] if profs else None)
if not pid:
    print("[RUN] no profiles available"); sys.exit(0)

orc = Orchestrator(load_config, save_config, pm)
rep = orc.run_profile(pid)
print(json.dumps(rep, indent=2))
PY
