# services/__init__.py
from __future__ import annotations

from typing import Callable
from fastapi import FastAPI

from . import watchlist, scheduling, statistics, editor, analyzer, export, importer, snapshots, authPlex, authOidc

SERVICE_MODULES = (watchlist, scheduling, statistics, editor, analyzer, export, importer)

__all__ = [
    "watchlist",
    "scheduling",
    "statistics",
    "editor",
    "analyzer",
    "export",
    "importer",
    "snapshots",
    "authPlex",
    "authOidc",
    "register",
]

def register(app: FastAPI, load_config: Callable[[], dict]) -> None:
    for mod in SERVICE_MODULES:
        fn = getattr(mod, "register", None)
        if callable(fn):
            fn(app, load_config)
