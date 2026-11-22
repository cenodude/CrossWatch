# /cw_platform/anime_mapping/rebuild_process.py
# CrossWatch - Anime Mapping Index Rebuild Process
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from cw_platform.config_base import CONFIG_BASE

from . import cw_mappings, storage

RESULT_PREFIX = "CW_ANIME_REBUILD_RESULT "
TIMEOUT_SECONDS = 1800
_ROOT = Path(__file__).resolve().parents[2]


class RebuildError(RuntimeError):
    pass


def _child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["CONFIG_BASE"] = str(CONFIG_BASE())
    env["PYTHONPATH"] = os.pathsep.join(p for p in (str(_ROOT), env.get("PYTHONPATH", "")) if p)
    return env


def _read_result(stdout: str) -> dict[str, Any] | None:
    for line in reversed((stdout or "").splitlines()):
        if line.startswith(RESULT_PREFIX):
            try:
                payload = json.loads(line[len(RESULT_PREFIX):])
            except ValueError:
                return None
            return payload if isinstance(payload, dict) else None
    return None


def rebuild_index(release_tag: str = "v3") -> dict[str, Any]:
    tag = storage.normalize_release_tag(release_tag)
    cmd = [sys.executable, "-m", __name__, tag, str(cw_mappings.BUNDLED_PATH)]
    storage.close_readers()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(_ROOT),
            env=_child_env(),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise RebuildError("Anime mapping index rebuild timed out") from exc
    except (OSError, ValueError):
        return storage.rebuild_sqlite_from_mappings(release_tag=tag)
    finally:
        storage.close_readers()
    payload = _read_result(proc.stdout)
    if payload is None:
        raise RebuildError(f"Anime mapping index rebuild exited with code {proc.returncode}")
    if not payload.get("ok"):
        raise RebuildError(f"{payload.get('error_type') or 'Error'}: {payload.get('error') or 'rebuild failed'}")
    result = payload.get("result")
    return result if isinstance(result, dict) else {}


def _main(argv: list[str]) -> int:
    tag = argv[0] if argv else "v3"
    if len(argv) > 1 and argv[1]:
        cw_mappings.BUNDLED_PATH = Path(argv[1])
    try:
        out: dict[str, Any] = {"ok": True, "result": storage.rebuild_sqlite_from_mappings(release_tag=tag)}
        code = 0
    except Exception as exc:
        out = {"ok": False, "error_type": exc.__class__.__name__, "error": str(exc)}
        code = 1
    sys.stdout.write(RESULT_PREFIX + json.dumps(out, default=str) + "\n")
    sys.stdout.flush()
    return code


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
