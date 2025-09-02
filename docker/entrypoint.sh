#!/usr/bin/env bash
set -Eeuo pipefail

# Defaults
WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-8787}"
APP_USER="${APP_USER:-appuser}"
APP_GROUP="${APP_GROUP:-appuser}"
APP_DIR="${APP_DIR:-/app}"
RUNTIME_DIR="${RUNTIME_DIR:-/config}"
RELOAD_FLAG=""

if [[ "${RELOAD:-no}" == "yes" ]]; then
  RELOAD_FLAG="--reload"
fi

# Ensure runtime directory exists
mkdir -p "${RUNTIME_DIR}" || true
chown -R "${APP_USER}:${APP_GROUP}" "${RUNTIME_DIR}" || true

cd "${APP_DIR}"

echo "[ENTRYPOINT] CrossWatch starting on ${WEB_HOST}:${WEB_PORT} (reload=${RELOAD:-no})"
# Prefer module invocation so Python path resolves cleanly
exec su -s /bin/bash -c "uvicorn crosswatch:app --host ${WEB_HOST} --port ${WEB_PORT} ${RELOAD_FLAG}" "${APP_USER}"
