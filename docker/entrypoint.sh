#!/usr/bin/env bash
# Dev-first entrypoint: start web UI; if it fails, drop to an interactive shell.
set -Euo pipefail

log(){ echo "[${TZ:-UTC}][$(date -Iseconds)] $*"; }

: "${TZ:=Europe/Amsterdam}"
: "${RUNTIME_DIR:=/config}"
: "${WEB_HOST:=0.0.0.0}"
: "${WEB_PORT:=8787}"
: "${WEBINTERFACE:=yes}"
: "${PUID:=1000}"
: "${PGID:=1000}"
: "${DEV_SHELL_ON_FAIL:=yes}"

# Timezone
if [ -f "/usr/share/zoneinfo/${TZ}" ]; then
  ln -snf "/usr/share/zoneinfo/${TZ}" /etc/localtime && echo "${TZ}" > /etc/timezone
fi

# User/group for dev-mounted volumes
if ! getent group "${PGID}" >/dev/null 2>&1; then groupadd -g "${PGID}" appgroup; fi
if ! id -u "${PUID}" >/dev/null 2>&1; then useradd -u "${PUID}" -g "${PGID}" -M -s /bin/bash appuser; fi
mkdir -p "${RUNTIME_DIR}"
chown -R "${PUID}:${PGID}" "${RUNTIME_DIR}" || true

# Link config/state in working dir for code that expects them
if [ ! -f "${RUNTIME_DIR}/config.json" ] && [ -f "/app/config.example.json" ]; then
  cp /app/config.example.json "${RUNTIME_DIR}/config.json" || true
fi
ln -sf "${RUNTIME_DIR}/config.json" /app/config.json || true
[ -f "${RUNTIME_DIR}/state.json" ] || : > "${RUNTIME_DIR}/state.json"
ln -sf "${RUNTIME_DIR}/state.json" /app/state.json || true
chown "${PUID}:${PGID}" "${RUNTIME_DIR}/config.json" "${RUNTIME_DIR}/state.json" || true

# If WEBINTERFACE is disabled, run sync-once via script and then hold shell (dev default behavior)
if [[ "${WEBINTERFACE,,}" != "yes" ]]; then
  log "[ENTRY] WEBINTERFACE=no → running one sync via run-sync.sh"
  /usr/local/bin/run-sync.sh || true
  log "[ENTRY] Sync finished."
  exec bash
fi

# --- WEB MODE: start the FastAPI app (modular platform) ---
log "[ENTRY] Starting web UI (crosswatch.py) on ${WEB_HOST}:${WEB_PORT}"

# Prefer uvicorn module if available
set +e
su -s /bin/bash -c "uvicorn crosswatch:app --host ${WEB_HOST} --port ${WEB_PORT} --reload" appuser
ret=$?
set -e

if [ $ret -ne 0 ]; then
  log "[ENTRY] Web failed with exit code $ret."
  if [[ "${DEV_SHELL_ON_FAIL,,}" == "yes" ]]; then
    log "[ENTRY] Dropping to interactive shell (DEV_SHELL_ON_FAIL=yes)."
    exec bash
  else
    exit $ret
  fi
fi
