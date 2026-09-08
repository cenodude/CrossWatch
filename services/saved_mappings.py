# services/saved_mappings.py
# CrossWatch - readable saved corrections and their original identities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterator, Literal, Mapping, NotRequired, TypedDict

from cw_platform.local_db.manual_policy import _feature_blocks


class SavedRule(TypedDict):
    pair_id: str
    scope: Literal["pair", "shared"]
    provider: str
    instance: str
    feature: str
    key: str
    corrected: dict[str, Any] | None
    original: dict[str, Any] | None
    original_key: str | None
    saved_at: int | float | None
    origin: str | None
    excluded: bool
    entry_type: NotRequired[Literal["mapping", "block"]]
    scope_label: NotRequired[str]


def mapping_details(item: Mapping[str, Any]) -> dict[str, Any]:
    return {key: deepcopy(item[key]) for key in (
        "type", "title", "name", "series_title", "show_title", "year", "season", "episode", "ids", "show_ids"
    ) if key in item}


def saved_corrections(policy: Mapping[str, Any]) -> Iterator[SavedRule]:
    for pair_id, scoped in [("", policy), *(policy.get("pairs") or {}).items()]:
        yield from _scoped_corrections(scoped, pair_id)


def correction_block_keys(block):
    items = (block.get("adds") or {}).get("items") or {}
    return {str(record["original_key"]).lower() for target, record in (block.get("mappings") or {}).items()
            if target in items and record.get("original_key") and record["original_key"] != target}


def saved_blocks(policy: Mapping[str, Any]) -> Iterator[SavedRule]:
    for pair_id, scoped in [("", policy), *(policy.get("pairs") or {}).items()]:
        for provider, instance, feature, block in _feature_blocks(scoped):
            automatic = correction_block_keys(block)
            items = (block.get("adds") or {}).get("items") or {}
            for key in block.get("blocks") or []:
                if key.lower() in automatic:
                    continue
                yield SavedRule(pair_id=pair_id, scope="pair" if pair_id else "shared", provider=provider,
                           instance=instance, feature=feature, key=key, entry_type="block",
                           corrected=mapping_details(items[key]) if key in items else None,
                           original=None, original_key=None, saved_at=None, origin=None, excluded=True)


def _scoped_corrections(policy: Mapping[str, Any], pair_id: str) -> Iterator[SavedRule]:
    for provider, instance, feature, block in _feature_blocks(policy):
        records = block.get("mappings") or {}
        blocks = set(block.get("blocks") or [])
        for key, item in ((block.get("adds") or {}).get("items") or {}).items():
            if not isinstance(item, Mapping):
                continue
            record = records.get(key) if isinstance(records, dict) else None
            record = record if isinstance(record, dict) else {}
            yield SavedRule(pair_id=pair_id, scope="pair" if pair_id else "shared", provider=provider, instance=instance, feature=feature, key=key,
                       corrected=mapping_details(item), original=record.get("original"),
                       original_key=record.get("original_key"), saved_at=record.get("saved_at"),
                       origin=record.get("origin"), excluded=key in blocks)
