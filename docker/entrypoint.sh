#!/usr/bin/env bash
# Entrypoint for CrossWatch container

set -Eeuo pipefail

WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-8787}"
APP_USER="${APP_USER:-appuser}"
APP_GROUP="${APP_GROUP:-appuser}"
APP_UID="${APP_UID:-1000}"
APP_GID="${APP_GID:-1000}"
APP_DIR="${APP_DIR:-/app}"
RUNTIME_DIR="${RUNTIME_DIR:-/config}"

export PYTHONPATH="${APP_DIR}:${PYTHONPATH:-}"

ensure_user() {
  # Create runtime user/group only when running as root
  if [[ "$(id -u)" -ne 0 ]]; then return 0; fi
  getent group "${APP_GROUP}" >/dev/null 2>&1 || groupadd -g "${APP_GID}" "${APP_GROUP}"
  id -u "${APP_USER}" >/dev/null 2>&1 || useradd -m -u "${APP_UID}" -g "${APP_GROUP}" -s /bin/bash "${APP_USER}"
}

prep_runtime() {
  # Ensure runtime directory exists and is writable
  mkdir -p "${RUNTIME_DIR}" || true
  if [[ "$(id -u)" -eq 0 && -w "$(dirname "${RUNTIME_DIR}")" ]]; then
    chown -R "${APP_USER}:${APP_GROUP}" "${RUNTIME_DIR}" || true
  fi
}

run_as() {
  # Execute given command as app user (root drops privileges)
  if [[ "$(id -u)" -eq 0 ]]; then
    exec su -s /bin/bash -c "$*" "${APP_USER}"
  else
    exec bash -lc "$*"
  fi
}

main() {
  ensure_user
  prep_runtime
  cd "${APP_DIR}"

  echo "[ENTRYPOINT] CrossWatch on ${WEB_HOST}:${WEB_PORT} (reload=${RELOAD:-no}) as $(id -un)"

  if [[ "$#" -gt 0 ]]; then
    # Run a custom command
    run_as "$*"
  else
    # Default: start FastAPI server (reload mode if requested)
    if [[ "${RELOAD:-no}" == "yes" ]]; then
      run_as "watchmedo auto-restart --pattern='*.py' --recursive -- python -m crosswatch --host ${WEB_HOST} --port ${WEB_PORT}"
    else
      run_as "python -m crosswatch --host ${WEB_HOST} --port ${WEB_PORT}"
    fi
  fi
}

main "$@"
