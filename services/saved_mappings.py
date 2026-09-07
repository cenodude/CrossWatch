# services/saved_mappings.py
# CrossWatch - readable saved corrections and their original identities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

from cw_platform.local_db.manual_policy import _feature_blocks


def mapping_details(item: Mapping[str, Any]) -> dict[str, Any]:
    return {key: deepcopy(item[key]) for key in (
        "type", "title", "name", "series_title", "show_title", "year", "season", "episode", "ids", "show_ids"
    ) if key in item}


def saved_corrections(policy):
    for provider, instance, feature, block in _feature_blocks(policy):
        records = block.get("mappings") or {}
        blocks = set(block.get("blocks") or [])
        for key, item in ((block.get("adds") or {}).get("items") or {}).items():
            if not isinstance(item, Mapping):
                continue
            record = records.get(key) if isinstance(records, dict) else None
            record = record if isinstance(record, dict) else {}
            yield dict(provider=provider, instance=instance, feature=feature, key=key,
                       corrected=mapping_details(item), original=record.get("original"),
                       original_key=record.get("original_key"), saved_at=record.get("saved_at"),
                       origin=record.get("origin"), excluded=key in blocks)
