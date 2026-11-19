# services/pair_state_maintenance.py
# CrossWatch - Daily cleanup of obsolete sync pair state
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from cw_platform.local_db.operation_lock import exclusive_operation

_LOG = logging.getLogger("crosswatch.pair_state")

STARTUP_DELAY = 1200
INTERVAL = 86400
RETRY = 900

_LOCK = threading.Lock()
_STOP = threading.Event()
_THREAD: threading.Thread | None = None


ConfigSource = Mapping[str, Any] | Callable[[], Mapping[str, Any] | None] | None


def run_once(config: ConfigSource, config_dir: Path | str, *, prune: bool = True) -> dict[str, Any] | None:
    from services.pair_state import adopt_legacy_scopes, prune_pair_scopes

    try:
        operation = exclusive_operation(config_dir)
        operation.__enter__()
    except RuntimeError as exc:
        if str(exc) == "database_operation_busy":
            return None
        raise
    try:
        cfg = config() if callable(config) else config
        if not isinstance(cfg, Mapping):
            return None
        pairs = cfg.get("pairs")
        if not isinstance(pairs, list) or not pairs:
            return {"skipped": "no_pairs"}
        if not prune:
            return {"adopted": adopt_legacy_scopes(config_dir, cfg)}
        return prune_pair_scopes(config_dir, cfg)
    finally:
        operation.__exit__(None, None, None)


def _loop(load_config: Callable[[], Mapping[str, Any] | None], config_dir: Path | str) -> None:
    try:
        run_once(load_config, config_dir, prune=False)
    except Exception:
        _LOG.exception("Pair state scope update could not run")
    delay = STARTUP_DELAY
    while not _STOP.wait(delay):
        try:
            delay = INTERVAL if run_once(load_config, config_dir) is not None else RETRY
        except Exception:
            _LOG.exception("Pair state cleanup could not run")
            delay = INTERVAL


def start(load_config: Callable[[], Mapping[str, Any] | None], config_dir: Path | str) -> bool:
    global _THREAD
    with _LOCK:
        if _THREAD is not None and _THREAD.is_alive():
            return False
        _STOP.clear()
        _THREAD = threading.Thread(target=_loop, args=(load_config, config_dir), name="pair-state-cleanup", daemon=True)
        _THREAD.start()
        return True


def stop() -> None:
    _STOP.set()
    thread = _THREAD
    if thread is not None:
        thread.join(timeout=2)
