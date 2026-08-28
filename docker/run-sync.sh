#!/usr/bin/env bash
# Compatibility wrapper for one-shot container syncs.

set -Eeuo pipefail

export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"
export APP_DIR="${APP_DIR:-/app}"
export PYTHONPATH="${APP_DIR}:${PYTHONPATH:-}"
export CW_CLI_HOME="${CW_CLI_HOME:-${RUNTIME_DIR:-/config}/.cw_cli}"

exec /usr/local/bin/cw sync once "$@"
