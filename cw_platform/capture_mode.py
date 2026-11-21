# cw_platform/capture_mode.py
# CrossWatch - Capture mode scoped to the operation and provider that asked for it
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Executor
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TypeVar

_T = TypeVar("_T")
_R = TypeVar("_R")
_TRUTHY = ("1", "true", "yes", "y", "on")


@dataclass(frozen=True)
class CaptureScope:
    active: bool
    provider: str = ""
    instance: str = ""
    feature: str = ""
    capture_id: str = ""
    progress_id: str = ""


_SCOPE: ContextVar[CaptureScope | None] = ContextVar("cw_capture_scope", default=None)


@contextmanager
def capture(provider: str, instance: str = "", feature: str = "", capture_id: str = "", progress_id: str = "") -> Iterator[None]:
    scope = CaptureScope(
        True,
        str(provider or "").strip().upper(),
        str(instance or "").strip(),
        str(feature or "").strip().lower(),
        str(capture_id or "").strip(),
        str(progress_id or "").strip(),
    )
    token = _SCOPE.set(scope)
    try:
        yield
    finally:
        _SCOPE.reset(token)


@contextmanager
def writes() -> Iterator[None]:
    token = _SCOPE.set(CaptureScope(False))
    try:
        yield
    finally:
        _SCOPE.reset(token)


def current() -> CaptureScope | None:
    scope = _SCOPE.get()
    if scope is not None:
        return scope if scope.active else None
    if str(os.getenv("CW_CAPTURE_MODE") or "").strip().lower() not in _TRUTHY:
        return None
    return CaptureScope(
        True,
        str(os.getenv("CW_CAPTURE_PROVIDER") or "").strip().upper(),
        str(os.getenv("CW_CAPTURE_INSTANCE") or "").strip(),
        str(os.getenv("CW_CAPTURE_FEATURE") or "").strip().lower(),
        str(os.getenv("CW_CAPTURE_ID") or "").strip(),
        str(os.getenv("CW_CAPTURE_PROGRESS_ID") or "").strip(),
    )


def active(provider: str) -> bool:
    scope = current()
    if scope is None:
        return False
    return not scope.provider or scope.provider == str(provider or "").strip().upper()


def provider() -> str:
    scope = current()
    return scope.provider if scope is not None else ""


def progress_id() -> str:
    scope = current()
    return scope.progress_id if scope is not None else ""


def scoped_map(executor: Executor, fn: Callable[[_T], _R], items: Iterable[_T]) -> Iterator[_R]:
    scope = _SCOPE.get()

    def run(item: _T) -> _R:
        token = _SCOPE.set(scope)
        try:
            return fn(item)
        finally:
            _SCOPE.reset(token)

    return executor.map(run, items)

