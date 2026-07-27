# providers/sync/kodi/_progress.py
# CrossWatch Kodi playback progress synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ._common import feature_index, operation_result, progress_ms_for_write, resume_ms


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    return feature_index(adapter, "progress")


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    def payload(source: Mapping[str, Any], target: Mapping[str, Any]) -> tuple[dict[str, Any], str | None]:
        raw_obj = target.get("_kodi_raw")
        raw: Mapping[str, Any] = raw_obj if isinstance(raw_obj, Mapping) else {}
        _target_pos, target_total, _pct = resume_ms(raw)
        pos, total = progress_ms_for_write(source, target_total)
        if pos is None:
            return {}, "missing_progress"
        return {"resume": {"position": max(0.0, pos / 1000.0), "total": max(0.0, (total or 0) / 1000.0)}}, None

    return operation_result(adapter, "progress", "add", items, payload, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    def payload(_source: Mapping[str, Any], target: Mapping[str, Any]) -> tuple[dict[str, Any], str | None]:
        raw_obj = target.get("_kodi_raw")
        raw: Mapping[str, Any] = raw_obj if isinstance(raw_obj, Mapping) else {}
        _target_pos, target_total, _pct = resume_ms(raw)
        return {"resume": {"position": 0.0, "total": max(0.0, (target_total or 0) / 1000.0)}}, None

    return operation_result(adapter, "progress", "remove", items, payload, dry_run=dry_run)
