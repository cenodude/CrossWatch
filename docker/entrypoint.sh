#!/usr/bin/env bash
set -euo pipefail

log(){ echo "[$(date -Iseconds)] $*"; }

# ---------- Defaults ----------
: "${TZ:=Europe/Amsterdam}"
: "${RUNTIME_DIR:=/config}"
: "${CRON_SCHEDULE:=0 0 * * *}"     # empty = run once
: "${PUID:=1000}"
: "${PGID:=1000}"
: "${SYNC_CMD:=python /app/plex_simkl_watchlist_sync.py --sync}"
: "${INIT_CMD:=python /app/plex_simkl_watchlist_sync.py --init-simkl redirect --bind 0.0.0.0:8787}"
: "${LOCK_FILE:=/var/lock/plex-simkl.lock}"
: "${WEBINTERFACE:=yes}"            # web wins by default
: "${WEB_HOST:=0.0.0.0}"
: "${WEB_PORT:=8787}"

# ---------- Sanity on PUID/PGID ----------
case "$PUID" in (*[!0-9]*|'') PUID=1000;; esac
case "$PGID" in (*[!0-9]*|'') PGID=1000;; esac

# ---------- FS prep ----------
mkdir -p "$RUNTIME_DIR" /var/log /var/lock
: > /var/log/cron.log

# ---------- Timezone ----------
if [ -f "/usr/share/zoneinfo/${TZ}" ]; then
  ln -snf "/usr/share/zoneinfo/${TZ}" /etc/localtime
  echo "${TZ}" > /etc/timezone
fi

# ---------- User/group ----------
if ! getent group "${PGID}" >/dev/null 2>&1; then
  groupadd -g "${PGID}" appgroup
fi
if ! id -u "${PUID}" >/dev/null 2>&1; then
  useradd -u "${PUID}" -g "${PGID}" -M -s /usr/sbin/nologin appuser
fi
chown -R "${PUID}:${PGID}" "$RUNTIME_DIR" /var/log/cron.log /var/lock

# ---------- CONFIG/STATES (no gating!) ----------
# Create a starter config/state so the webapp can write to them, but don't *check* anything here.
if [ ! -f "$RUNTIME_DIR/config.json" ] && [ -f "/app/config.example.json" ]; then
  cp /app/config.example.json "$RUNTIME_DIR/config.json"
  chown "${PUID}:${PGID}" "$RUNTIME_DIR/config.json"
  log "[ENTRYPOINT] Created $RUNTIME_DIR/config.json from template"
fi
ln -sf "$RUNTIME_DIR/config.json" /app/config.json
log "[ENTRYPOINT] Linked $RUNTIME_DIR/config.json -> /app/config.json"

# Merge ENV → config.json (optional helper; harmless if file missing/invalid)
python - <<'PY'
import json, os
p='/app/config.json'
try:
    cfg=json.load(open(p,'r',encoding='utf-8'))
except Exception:
    cfg={}
cfg.setdefault('plex',{}); cfg.setdefault('simkl',{})
upd=[]
def set_if(env, path):
    v=os.getenv(env,'').strip()
    if not v: return
    d=cfg
    *ps,k=path.split('.')
    for x in ps: d=d.setdefault(x,{})
    d[k]=v; upd.append(path)
set_if('PLEX_ACCOUNT_TOKEN','plex.account_token')
set_if('SIMKL_CLIENT_ID','simkl.client_id')
set_if('SIMKL_CLIENT_SECRET','simkl.client_secret')
if upd:
    json.dump(cfg,open(p,'w',encoding='utf-8'),indent=2)
    print("[ENTRYPOINT] Applied ENV → config.json for: " + ", ".join(upd))
PY

# State file (for web UI previews etc.)
if [ ! -f "$RUNTIME_DIR/state.json" ]; then
  : > "$RUNTIME_DIR/state.json"
  chown "${PUID}:${PGID}" "$RUNTIME_DIR/state.json"
  log "[ENTRYPOINT] Created empty $RUNTIME_DIR/state.json"
fi
ln -sf "$RUNTIME_DIR/state.json" /app/state.json
log "[ENTRYPOINT] Linked $RUNTIME_DIR/state.json -> /app/state.json"

# ---------- WEB MODE (web overrules everything) ----------
if [[ "${WEBINTERFACE,,}" == "yes" ]]; then
  log "[ENTRYPOINT] WEBINTERFACE=yes → launching web UI on ${WEB_HOST}:${WEB_PORT} (skipping OAuth/config checks & cron)"
  exec su -s /bin/sh -c "python /app/webapp.py --host ${WEB_HOST} --port ${WEB_PORT}" appuser
fi

# ---------- NON-WEB MODE: First-run / OAuth check ----------
MISSING_FIELDS="$(python - <<'PY'
import json,sys
try:
    cfg=json.load(open('/app/config.json','r',encoding='utf-8'))
except Exception:
    print('config.json:unreadable'); sys.exit(0)
miss=[]
if not (cfg.get('plex') or {}).get('account_token'): miss.append('plex.account_token')
if not (cfg.get('simkl') or {}).get('client_id'):    miss.append('simkl.client_id')
if not (cfg.get('simkl') or {}).get('client_secret'):miss.append('simkl.client_secret')
print(' '.join(miss))
PY
)"

if python - <<'PY'
import json,sys
try:
    cfg=json.load(open('/app/config.json','r',encoding='utf-8'))
    sys.exit(0 if cfg.get('simkl',{}).get('access_token') else 1)
except Exception:
    sys.exit(1)
PY
then
  log "[ENTRYPOINT] SIMKL access_token found → continue normal run"
else
  if [ -n "${MISSING_FIELDS}" ]; then
    log "[INIT] No SIMKL access_token AND missing required credentials → not starting OAuth"
    log "[INIT] Missing: ${MISSING_FIELDS}"
    log "[INIT] Please add credentials to ${RUNTIME_DIR}/config.json and restart."
    exit 0
  fi
  log "[INIT] No SIMKL access_token; starting OAuth flow on 0.0.0.0:8787"
  cd "$RUNTIME_DIR"
  exec su -s /bin/sh -c "${INIT_CMD} && echo '[INIT] Done. Restart container to start normal syncs.'" appuser
fi

# ---------- RUN-ONCE MODE ----------
if [ -z "${CRON_SCHEDULE}" ]; then
  log "[ENTRYPOINT] Run once"
  exec su -s /bin/sh -c "flock -n ${LOCK_FILE} -c '/usr/local/bin/run-sync.sh'" appuser
fi

# ---------- CRON MODE ----------
{
  echo 'SHELL=/bin/sh'
  echo 'PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
  echo "${CRON_SCHEDULE} appuser flock -n ${LOCK_FILE} -c '/usr/local/bin/run-sync.sh' >> /var/log/cron.log 2>&1"
} > /etc/cron.d/plex-simkl
chmod 0644 /etc/cron.d/plex-simkl
log "[ENTRYPOINT] Cron scheduled: ${CRON_SCHEDULE}"
exec cron -f
