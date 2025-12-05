# cw_platform/orchestrator/_types.py
# types and protocols for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable, Mapping
from typing import Any, Protocol


class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(
        self,
        cfg: Mapping[str, Any],
        *,
        feature: str,
    ) -> Mapping[str, dict[str, Any]]: ...
    def add(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]: ...
    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]: ...

@dataclass
class ConflictPolicy:
    prefer: str = "source"
