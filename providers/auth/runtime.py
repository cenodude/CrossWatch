# providers/auth/runtime.py
# Generic runtime authentication dispatch for provider API calls
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

import requests


def _key(provider: Any) -> str:
    return str(provider or "").strip().lower().replace("-", "_")


def _backend(provider: Any):
    name = _key(provider)
    if name in {"mdb", "mdb_list", "mdblist"}:
        from providers.sync.mdblist import _auth as mdblist_auth

        return mdblist_auth
    raise NotImplementedError(f"No runtime auth backend for provider: {provider}")


def normalize_auth_method(provider: str, value: Any, block: Mapping[str, Any] | None = None) -> str:
    return _backend(provider).normalize_auth_method(value, block)


def active_method(provider: str, block: Mapping[str, Any] | None) -> str:
    return _backend(provider).active_method(block)


def is_configured(provider: str, block: Mapping[str, Any] | None) -> bool:
    return bool(_backend(provider).is_configured(block))


def status_for_block(provider: str, block: Mapping[str, Any] | None) -> dict[str, Any]:
    return dict(_backend(provider).status_for_block(block))


def set_active_method(provider: str, block: MutableMapping[str, Any], method: str) -> str:
    return str(_backend(provider).set_active_method(block, method))


def clear_oauth(provider: str, block: MutableMapping[str, Any]) -> None:
    _backend(provider).clear_oauth(block)


def start_device_code(provider: str, cfg: dict[str, Any] | None, **kwargs: Any) -> dict[str, Any]:
    return dict(_backend(provider).start_device_code(cfg, **kwargs))


def poll_device_code(provider: str, cfg: dict[str, Any] | None, **kwargs: Any) -> dict[str, Any]:
    return dict(_backend(provider).poll_device_code(cfg, **kwargs))


def refresh_token(provider: str, cfg: dict[str, Any] | None = None, **kwargs: Any) -> dict[str, Any]:
    return dict(_backend(provider).refresh_token(cfg, **kwargs))


def request_with_auth(
    provider: str,
    session: requests.Session,
    method: str,
    url: str,
    *,
    cfg: Mapping[str, Any] | None,
    **kwargs: Any,
) -> requests.Response:
    return _backend(provider).request_with_auth(session, method, url, cfg=cfg, **kwargs)
