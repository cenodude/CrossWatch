#!/usr/bin/env bash
set -euo pipefail

log(){ echo "[$(date -Iseconds)] $*"; }

# Defaults (cron-safe)
: "${RUNTIME_DIR:=/config}"
: "${SYNC_CMD:=python /app/plex_simkl_watchlist_sync.py --sync}"
: "${WEBINTERFACE:=yes}"          # default ON; web mode overrules cron
: "${PATH:=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin}"

mkdir -p "$RUNTIME_DIR"

# If web UI is enabled, do not run any sync from cron.
if [[ "${WEBINTERFACE,,}" == "yes" ]]; then
  log "[SKIP] WEBINTERFACE=yes → skipping cron sync."
  exit 0
fi

# --- TOKEN CHECK (prefers /config, falls back to /app) ---
MISSING="$(python - <<'PY'
import json, sys, os

def load(p):
    try:
        with open(p,'r',encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

cfg = load('/config/config.json') or load('/app/config.json')
missing = []
if not cfg:
    missing.append('config.json:missing_or_unreadable')
else:
    plex = cfg.get('plex', {}) if isinstance(cfg, dict) else {}
    simk = cfg.get('simkl', {}) if isinstance(cfg, dict) else {}
    if not plex.get('account_token'):
        missing.append('plex.account_token')
    if not simk.get('access_token'):
        missing.append('simkl.access_token')

print(' '.join(missing))
PY
)"

if [ -n "$MISSING" ]; then
  log "[SKIP] Missing required fields: $MISSING → aborting sync"
  exit 0
fi
# --- END TOKEN CHECK ---

# Simple lock to prevent overlapping runs
LOCKDIR="$RUNTIME_DIR/.sync.lock"
if mkdir "$LOCKDIR" 2>/dev/null; then
  trap 'rmdir "$LOCKDIR"' EXIT INT TERM
else
  log "[SKIP] Another run is in progress (lock: $LOCKDIR)"
  exit 0
fi

log "[RUN] cd $RUNTIME_DIR && ${SYNC_CMD}"
cd "$RUNTIME_DIR"

# Use sh -lc to allow complex SYNC_CMD strings with quotes/pipes/etc.
sh -lc "${SYNC_CMD}"
RET=$?

if [ $RET -eq 0 ]; then
  log "[RUN] done."
else
  log "[RUN] finished with exit code $RET."
fi

exit $RET
