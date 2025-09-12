#!/usr/bin/env bash
set -Eeuo pipefail

WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-8787}"
APP_USER="${APP_USER:-appuser}"
APP_GROUP="${APP_GROUP:-appuser}"
APP_UID="${APP_UID:-1000}"
APP_GID="${APP_GID:-1000}"
APP_DIR="${APP_DIR:-/app}"
RUNTIME_DIR="${RUNTIME_DIR:-/config}"
RELOAD_FLAG=""
[[ "${RELOAD:-no}" == "yes" ]] && RELOAD_FLAG="--reload"

export PYTHONPATH="${APP_DIR}:${PYTHONPATH:-}"

ensure_user() {
  # Only root can create/chown users
  if [[ "$(id -u)" -ne 0 ]]; then
    return 0
  fi
  getent group "${APP_GROUP}" >/dev/null 2>&1 || groupadd -g "${APP_GID}" "${APP_GROUP}"
  if ! id -u "${APP_USER}" >/dev/null 2>&1; then
    useradd -m -u "${APP_UID}" -g "${APP_GROUP}" -s /bin/bash "${APP_USER}"
  fi
}

prep_runtime() {
  mkdir -p "${RUNTIME_DIR}" || true
  # chown only if root and not bind-mounted read-only
  if [[ "$(id -u)" -eq 0 && -w "$(dirname "${RUNTIME_DIR}")" ]]; then
    chown -R "${APP_USER}:${APP_GROUP}" "${RUNTIME_DIR}" || true
  fi
}

run_as() {
  if [[ "$(id -u)" -eq 0 ]]; then
    exec su -s /bin/bash -c "$*" "${APP_USER}"
  else
    # Already non-root (Kubernetes/Podman rootless, etc.)
    exec bash -lc "$*"
  fi
}

main() {
  ensure_user
  prep_runtime
  cd "${APP_DIR}"

  echo "[ENTRYPOINT] CrossWatch on ${WEB_HOST}:${WEB_PORT} (reload=${RELOAD:-no}) as $(id -un)"

  if [[ "$#" -gt 0 ]]; then
    # Custom command path
    run_as "$*"
  else
    # Default server (use crosswatch.py:main() so startup info prints)
    if [[ "${RELOAD:-no}" == "yes" ]]; then
      run_as "watchmedo auto-restart --pattern='*.py' --recursive -- python -m crosswatch --host ${WEB_HOST} --port ${WEB_PORT}"
    else
      run_as "python -m crosswatch --host ${WEB_HOST} --port ${WEB_PORT}"
    fi
  fi
}

main "$@"
