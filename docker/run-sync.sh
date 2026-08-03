#!/usr/bin/env bash
# Run all configured sync pairs using the Orchestrator
# Exits non-zero on failure and prints full output

set -Eeuo pipefail
export PYTHONUNBUFFERED=1
export PYTHONPATH="${PYTHONPATH:-/app}"

python - <<'PY'
import sys, json, traceback
from cw_platform.config_base import load_config
from cw_platform.orchestrator import Orchestrator

def coerce_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}

try:
    cfg = load_config() or {}
    runtime_cfg = cfg.get("runtime") or {}
    sync_cfg = cfg.get("sync") or {}
    write_state_json = coerce_bool(
        sync_cfg.get("write_state_json", runtime_cfg.get("write_state_json", True)),
        True,
    )
    orc = Orchestrator(cfg)
    result = orc.run_pairs(write_state_json=write_state_json)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    sys.exit(int(result.get("exit_code", 0)) if isinstance(result, dict) else 0)
except Exception as e:
    print(f"[RUN] Sync failed: {e}", file=sys.stderr)
    traceback.print_exc()
    sys.exit(1)
PY
