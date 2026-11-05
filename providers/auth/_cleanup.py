# providers/auth/_cleanup.py
# CrossWatch - Best-effort remote cleanup after local disconnect
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import threading
from typing import Any, Callable


def cleanup_remote(callback: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
    def run() -> None:
        try:
            callback(*args, **kwargs)
        except Exception:
            logging.getLogger(__name__).warning("Remote disconnect cleanup failed")

    try:
        threading.Thread(target=run, daemon=True, name="auth-disconnect").start()
    except RuntimeError:
        logging.getLogger(__name__).warning("Remote disconnect cleanup unavailable")
