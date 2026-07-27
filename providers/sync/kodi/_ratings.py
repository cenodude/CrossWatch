# providers/sync/kodi/_ratings.py
# CrossWatch Kodi ratings synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ._common import feature_index, operation_result, rating_for_write


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    return feature_index(adapter, "ratings")


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    def payload(source: Mapping[str, Any], _target: Mapping[str, Any]) -> tuple[dict[str, Any], str | None]:
        rating = rating_for_write(source)
        if rating is None:
            return {}, "missing_rating"
        return {"userrating": rating}, None

    return operation_result(adapter, "ratings", "add", items, payload, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    def payload(_source: Mapping[str, Any], _target: Mapping[str, Any]) -> tuple[dict[str, Any], str | None]:
        return {"userrating": 0}, None

    return operation_result(adapter, "ratings", "remove", items, payload, dry_run=dry_run)
