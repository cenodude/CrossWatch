# providers/sync/kodi/_history.py
# CrossWatch Kodi history synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ._common import feature_index, operation_result, watched_at_to_kodi


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    return feature_index(adapter, "history")


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    def payload(source: Mapping[str, Any], target: Mapping[str, Any]) -> tuple[dict[str, Any], str | None]:
        context = dict(target)
        context.update(source)
        return {
            "playcount": max(1, int(source.get("playcount") or 1)),
            "lastplayed": watched_at_to_kodi(source.get("watched_at"), item=context),
        }, None

    return operation_result(adapter, "history", "add", items, payload, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    def payload(_source: Mapping[str, Any], _target: Mapping[str, Any]) -> tuple[dict[str, Any], str | None]:
        return {"playcount": 0, "lastplayed": ""}, None

    return operation_result(adapter, "history", "remove", items, payload, dry_run=dry_run)
