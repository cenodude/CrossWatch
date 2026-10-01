# cw_platform/token_refresh.py
# CrossWatch - Background refresh of provider access tokens
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
import logging
import threading
from typing import Any

_LOG = logging.getLogger("crosswatch.token_refresh")

INTERVAL_S = 3600
PROVIDERS: dict[str, str] = {
    "SIMKL": "providers.auth._auth_SIMKL",
    "TRAKT": "providers.auth._auth_TRAKT",
}

_worker: threading.Thread | None = None
_worker_lock = threading.Lock()
_stop = threading.Event()


def refresh_all() -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for name, module_path in PROVIDERS.items():
        try:
            result: Any = importlib.import_module(module_path).refresh_all()
        except Exception as e:
            _LOG.error("%s token refresh failed: %s", name, type(e).__name__)
            continue
        if isinstance(result, dict) and result:
            out[name] = {str(inst): str(status) for inst, status in result.items()}
    return out


def _loop() -> None:
    while not _stop.is_set():
        try:
            refresh_all()
        except Exception as e:
            _LOG.error("token refresh worker error: %s", type(e).__name__)
        _stop.wait(INTERVAL_S)


def start_worker() -> bool:
    global _worker
    with _worker_lock:
        if _worker is not None and _worker.is_alive():
            return False
        _stop.clear()
        _worker = threading.Thread(target=_loop, name="token-refresh", daemon=True)
        _worker.start()
        return True


def stop_worker() -> None:
    _stop.set()
