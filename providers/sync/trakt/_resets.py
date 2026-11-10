# providers/sync/trakt/_resets.py
# CrossWatch - Trakt rewatch reset dates per show.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from .._log import log as cw_log
from ._common import fetch_user_settings, headers_for_adapter
from ._status import read_pages

_NOT_VIP_LOGGED: set[str] = set()


def account_is_vip(adapter: Any) -> bool:
    settings = fetch_user_settings(
        adapter.client.session,
        headers_for_adapter(adapter),
        timeout=float(getattr(adapter.cfg, "timeout", 15.0) or 15.0),
        max_retries=int(getattr(adapter.cfg, "max_retries", 3) or 3),
    )
    if not isinstance(settings, Mapping):
        raise RuntimeError("trakt_rewatch_resets_settings_unavailable")
    user = settings.get("user")
    if not isinstance(user, Mapping) or not isinstance(user.get("vip"), bool):
        raise RuntimeError("trakt_rewatch_resets_settings_unavailable")
    return bool(user.get("vip") or user.get("vip_og") or user.get("vip_ep"))


def load_rewatch_resets(adapter: Any) -> list[dict[str, Any]]:
    if not account_is_vip(adapter):
        token = str(getattr(getattr(adapter, "cfg", None), "access_token", "") or "")
        account = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
        if account not in _NOT_VIP_LOGGED:
            _NOT_VIP_LOGGED.add(account)
            cw_log("TRAKT", "history", "info", "rewatch_resets_skipped", reason="not_vip")
        return []
    out: list[dict[str, Any]] = []
    for row in read_pages(adapter, "/sync/progress/watched", only_rewatching="true"):
        show = row.get("show")
        progress = row.get("progress")
        if not isinstance(show, Mapping) or not isinstance(progress, Mapping):
            raise RuntimeError("trakt_rewatch_resets_invalid_row")
        reset_at = progress.get("reset_at")
        if not reset_at:
            continue
        ids = {str(k): str(v) for k, v in (show.get("ids") or {}).items()
               if k != "slug" and isinstance(v, (str, int)) and not isinstance(v, bool) and str(v).strip()}
        if not ids:
            continue
        out.append({"title": show.get("title"), "year": show.get("year"), "ids": ids, "reset_at": str(reset_at)})
    return out
