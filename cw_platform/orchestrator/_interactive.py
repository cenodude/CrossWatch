# cw_platform/orchestrator/_interactive.py
# CrossWatch - Interactive Sync Plan Review
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import hashlib
import json
import time
from typing import Any
from collections.abc import Callable, MutableMapping

from ..id_map import canonical_key


def fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


@dataclass
class InteractivePlan:
    preview: bool = True
    selected: set[str] = field(default_factory=set)
    choices: dict[str, str] = field(default_factory=dict)
    rows: MutableMapping[str, dict[str, Any]] = field(default_factory=dict)
    conflicts: MutableMapping[str, dict[str, Any]] = field(default_factory=dict)
    record_rows: bool = True
    copy_rows: bool = True
    notices: list[dict[str, Any]] = field(default_factory=list)
    seen: set[str] = field(default_factory=set)
    deferred_removes: list[dict[str, Any]] = field(default_factory=list)
    result: dict[str, Any] | None = None
    valid: Callable[[], bool] | None = None
    config_hash: str = ""
    planned_at: int = field(default_factory=lambda: int(time.time()))
    on_progress: Callable[[Any], None] | None = None
    on_result: Callable[..., None] | None = None

    def conflict(self, feature, key, a, b, left, right, default):
        cid = fingerprint([feature, key, a, b, left, right])
        winner = self.choices.get(cid, default)
        if winner not in (a, b):
            winner = default
        if self.record_rows:
            self.conflicts[cid] = dict(id=cid, feature=feature, key=key, source=a, target=b,
                                       left=deepcopy(left) if self.copy_rows else left,
                                       right=deepcopy(right) if self.copy_rows else right, winner=winner)
        return winner

    def filter(self, feature, provider, instance, operation, items, *, source="", source_instance="default", before=None, scope="", down=False, destination_label=""):
        from ._unresolved import load_unresolved_map

        unresolved = load_unresolved_map(provider, feature, cross_features=False) if self.record_rows and self.preview and items else {}
        valid = self.valid is None or self.valid()
        kept = []
        total = len(items) if hasattr(items, "__len__") else None
        done = 0
        def report():
            if self.on_progress is not None:
                self.on_progress(dict(event="review:plan", feature=feature, provider=provider, done=done, total=total))
        if items:
            report()
        for item in items:
            key = canonical_key(item)
            current = (before or {}).get(key)
            payload = dict(feature=feature, provider=provider, instance=instance, operation=operation,
                           item=item, before=current, scope=scope,
                           source=source, source_instance=source_instance, destination_label=destination_label)
            rid = fingerprint(payload)
            reason = "Provider unavailable" if down else ""
            if key == "unknown:":
                reason = "Missing media identifier"
            known = (unresolved or {}).get(key)
            hint = str((known or {}).get("hint") or (known or {}).get("reason") or "") if isinstance(known, dict) else ""
            if "|title:" in key and not hint:
                hint = "No media ID; provider title resolution is required"
            result = "blocked" if reason else ("unresolved" if hint else operation)
            if self.record_rows:
                self.rows[rid] = dict(deepcopy(payload) if self.copy_rows else payload,
                                      id=rid, key=key, result=result, reason=reason or hint, selectable=not reason)
            if valid and not reason and rid in self.selected:
                self.seen.add(rid)
            if not self.preview and rid in self.selected and not reason and valid:
                kept.append(item)
            elif not self.preview and operation == "remove":
                self.deferred_removes.append(payload)
            done += 1
            if done % 250 == 0:
                report()
        if done:
            report()
        return kept

    def retain_deferred(self, feature, provider, instance, current, previous, key_fn=canonical_key):
        retained = False
        for row in self.deferred_removes:
            if (row["feature"], row["source"], row["source_instance"]) != (feature, provider, instance):
                continue
            key = key_fn(row["item"])
            if key in previous and key not in current:
                current[key] = deepcopy(previous[key])
                retained = True
        return retained


def choose_conflict(ctx, feature, key, a, b, left, right, default):
    review = getattr(ctx, "interactive", None)
    return review.conflict(feature, key, a, b, left, right, default) if review is not None else default
