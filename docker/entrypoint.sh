#!/usr/bin/env bash
# Entrypoint for CrossWatch container

set -Eeuo pipefail

WEB_HOST="${WEB_HOST:-0.0.0.0}"
WEB_PORT="${WEB_PORT:-8787}"
DEFAULT_APP_USER="appuser"
DEFAULT_APP_GROUP="appuser"
APP_USER_INPUT="${APP_USER:-}"
APP_GROUP_INPUT="${APP_GROUP:-}"
APP_UID_INPUT="${APP_UID:-}"
APP_GID_INPUT="${APP_GID:-}"
APP_USER="${APP_USER_INPUT:-${DEFAULT_APP_USER}}"
APP_GROUP="${APP_GROUP_INPUT:-${DEFAULT_APP_GROUP}}"
APP_UID="${APP_UID_INPUT:-1000}"
APP_GID="${APP_GID_INPUT:-1000}"
APP_DIR="${APP_DIR:-/app}"
RUNTIME_DIR="${RUNTIME_DIR:-/config}"

export PYTHONPATH="${APP_DIR}:${PYTHONPATH:-}"

is_uint() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

die() {
  echo "[ENTRYPOINT] ERROR: $*" >&2
  exit 1
}

warn() {
  echo "[ENTRYPOINT] WARNING: $*" >&2
}

name_for_gid() {
  getent group "$1" | cut -d: -f1 || true
}

name_for_uid() {
  getent passwd "$1" | cut -d: -f1 || true
}

gid_for_group() {
  getent group "$1" | cut -d: -f3 || true
}

uid_for_user() {
  getent passwd "$1" | cut -d: -f3 || true
}

normalize_app_identity() {
  is_uint "${APP_UID}" || die "APP_UID must be numeric, got '${APP_UID}'"
  is_uint "${APP_GID}" || die "APP_GID must be numeric, got '${APP_GID}'"

  if [[ -n "${APP_USER_INPUT}" && "${APP_USER_INPUT}" =~ ^[0-9]+$ ]]; then
    [[ -z "${APP_UID_INPUT}" || "${APP_UID_INPUT}" == "${APP_USER_INPUT}" ]] \
      || warn "APP_USER is numeric but APP_UID is also set; using APP_UID=${APP_UID}"
    [[ -n "${APP_UID_INPUT}" ]] || APP_UID="${APP_USER_INPUT}"
    APP_USER="${DEFAULT_APP_USER}"
  fi

  if [[ -n "${APP_GROUP_INPUT}" && "${APP_GROUP_INPUT}" =~ ^[0-9]+$ ]]; then
    [[ -z "${APP_GID_INPUT}" || "${APP_GID_INPUT}" == "${APP_GROUP_INPUT}" ]] \
      || warn "APP_GROUP is numeric but APP_GID is also set; using APP_GID=${APP_GID}"
    [[ -n "${APP_GID_INPUT}" ]] || APP_GID="${APP_GROUP_INPUT}"
    APP_GROUP="${DEFAULT_APP_GROUP}"
  fi
}

ensure_group() {
  local current_gid existing_group

  if getent group "${APP_GROUP}" >/dev/null 2>&1; then
    current_gid="$(gid_for_group "${APP_GROUP}")"
    if [[ "${current_gid}" != "${APP_GID}" ]]; then
      existing_group="$(name_for_gid "${APP_GID}")"
      if [[ -n "${existing_group}" ]]; then
        warn "Requested GID ${APP_GID} already belongs to group '${existing_group}'; using it instead of '${APP_GROUP}'"
        APP_GROUP="${existing_group}"
        return 0
      fi
      groupmod -g "${APP_GID}" "${APP_GROUP}"
    fi
    return 0
  fi

  existing_group="$(name_for_gid "${APP_GID}")"
  if [[ -n "${existing_group}" ]]; then
    if [[ "${existing_group}" == "${DEFAULT_APP_GROUP}" ]]; then
      groupmod -n "${APP_GROUP}" "${existing_group}"
    else
      warn "Requested GID ${APP_GID} already belongs to group '${existing_group}'; using it instead of '${APP_GROUP}'"
      APP_GROUP="${existing_group}"
    fi
  else
    groupadd -g "${APP_GID}" "${APP_GROUP}"
  fi
}

ensure_user_account() {
  local current_gid current_uid existing_user home_dir

  if getent passwd "${APP_USER}" >/dev/null 2>&1; then
    current_uid="$(uid_for_user "${APP_USER}")"
    if [[ "${current_uid}" != "${APP_UID}" ]]; then
      existing_user="$(name_for_uid "${APP_UID}")"
      [[ -z "${existing_user}" ]] \
        || die "User '${APP_USER}' has UID ${current_uid}, but requested UID ${APP_UID} already belongs to '${existing_user}'"
      usermod -u "${APP_UID}" "${APP_USER}"
    fi
  else
    existing_user="$(name_for_uid "${APP_UID}")"
    if [[ -n "${existing_user}" ]]; then
      if [[ "${existing_user}" == "${DEFAULT_APP_USER}" ]]; then
        home_dir="/home/${APP_USER}"
        usermod -l "${APP_USER}" -d "${home_dir}" -m "${existing_user}"
      else
        die "Cannot create user '${APP_USER}' with UID ${APP_UID}; UID already belongs to '${existing_user}'"
      fi
    else
      useradd -m -u "${APP_UID}" -g "${APP_GROUP}" -s /bin/bash "${APP_USER}"
    fi
  fi

  current_gid="$(id -g "${APP_USER}")"
  if [[ "${current_gid}" != "${APP_GID}" ]]; then
    usermod -g "${APP_GROUP}" "${APP_USER}"
  fi
}

ensure_user() {
  # Create runtime user/group only when running as root
  if [[ "$(id -u)" -ne 0 ]]; then return 0; fi
  normalize_app_identity
  ensure_group
  ensure_user_account
}

prep_runtime() {
  mkdir -p "${RUNTIME_DIR}" \
    || die "Cannot create runtime directory '${RUNTIME_DIR}'"

  if [[ "$(id -u)" -eq 0 ]]; then
    chown -R "${APP_USER}:${APP_GROUP}" "${RUNTIME_DIR}" \
      || die "Cannot set ownership of runtime directory '${RUNTIME_DIR}' to ${APP_USER}:${APP_GROUP}"

    if ! setpriv --reuid="${APP_UID}" --regid="${APP_GID}" --init-groups \
        /usr/bin/test -w "${RUNTIME_DIR}" \
      || ! setpriv --reuid="${APP_UID}" --regid="${APP_GID}" --init-groups \
        /usr/bin/test -x "${RUNTIME_DIR}"; then
      die "Runtime directory '${RUNTIME_DIR}' is not writable by ${APP_USER}:${APP_GROUP}"
    fi
  elif [[ ! -w "${RUNTIME_DIR}" || ! -x "${RUNTIME_DIR}" ]]; then
    die "Runtime directory '${RUNTIME_DIR}' is not writable by $(id -un):$(id -gn)"
  fi
}

run_as() {
  if [[ "$(id -u)" -eq 0 ]]; then
    exec setpriv \
      --reuid="${APP_UID}" \
      --regid="${APP_GID}" \
      --init-groups \
      "$@"
  else
    exec "$@"
  fi
}

run_identity() {
  if [[ "$(id -u)" -eq 0 ]]; then
    echo "${APP_USER}:${APP_GROUP} (${APP_UID}:${APP_GID})"
  else
    echo "$(id -un):$(id -gn) ($(id -u):$(id -g))"
  fi
}

main() {
  ensure_user
  prep_runtime
  cd "${APP_DIR}"

  echo "[ENTRYPOINT] CrossWatch on ${WEB_HOST}:${WEB_PORT} (reload=${RELOAD:-no}) as $(run_identity)"

  if [[ "$#" -gt 0 ]]; then
    # Run a custom command
    run_as "$@"
  else
    # Default: start FastAPI server (reload mode if requested)
    if [[ "${RELOAD:-no}" == "yes" ]]; then
      run_as watchmedo auto-restart \
        --pattern="*.py" \
        --recursive \
        -- \
        python -m crosswatch \
        --host "${WEB_HOST}" \
        --port "${WEB_PORT}"
    else
      run_as python -m crosswatch --host "${WEB_HOST}" --port "${WEB_PORT}"
    fi
  fi
}

main "$@"
