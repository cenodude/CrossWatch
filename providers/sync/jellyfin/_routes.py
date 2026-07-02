# /providers/sync/jellyfin/_routes.py
# CrossWatch - Jellyfin Sync Provider - Route Utilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Mapping


def user_params(user_id: str, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
    return {**dict(params or {}), "userId": str(user_id)}


def items(item_id: str | None = None) -> str:
    return f"/Items/{item_id}" if item_id else "/Items"


def favorite(item_id: str) -> str:
    return f"/UserFavoriteItems/{item_id}"


def played(item_id: str) -> str:
    return f"/UserPlayedItems/{item_id}"


def user_data(item_id: str) -> str:
    return f"/UserItems/{item_id}/UserData"
