#!/usr/bin/env bash
# Run all configured sync pairs using the Orchestrator
# Exits non-zero on failure and prints full output

set -Eeuo pipefail
export PYTHONUNBUFFERED=1
export PYTHONPATH="${PYTHONPATH:-/app}"

python - <<'PY'
import sys, json, traceback
from cw_platform.orchestrator import Orchestrator

try:
    orc = Orchestrator()
    result = orc.run_pairs(write_state_json=True)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    sys.exit(int(result.get("exit_code", 0)) if isinstance(result, dict) else 0)
except Exception as e:
    print(f"[RUN] Sync failed: {e}", file=sys.stderr)
    traceback.print_exc()
    sys.exit(1)
PY
