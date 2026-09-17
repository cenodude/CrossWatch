# cw_platform/memory_release.py
# CrossWatch - post-sync cache release and heap trimming
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import ctypes
import ctypes.util
import gc
import sys
import threading
from collections.abc import Callable
from typing import Any

_LOCK = threading.Lock()
_CACHES: dict[str, Callable[[], Any]] = {}
_LIBC: Any = None
_LIBC_LOADED = False


def register_cache(name: str, clear: Callable[[], Any]) -> None:
    with _LOCK:
        _CACHES[name] = clear


def clear_caches() -> None:
    with _LOCK:
        clears = list(_CACHES.values())
    for clear in clears:
        try:
            clear()
        except Exception:
            pass


def _libc() -> Any:
    global _LIBC, _LIBC_LOADED
    if _LIBC_LOADED:
        return _LIBC
    _LIBC_LOADED = True
    if not sys.platform.startswith("linux"):
        return None
    try:
        lib = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
        _LIBC = lib if hasattr(lib, "malloc_trim") else None
    except Exception:
        _LIBC = None
    return _LIBC


def release_memory() -> None:
    gc.collect()
    lib = _libc()
    if lib is None:
        return
    try:
        lib.malloc_trim(0)
    except Exception:
        pass
