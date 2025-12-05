# cw_platform/orchestrator/_state_store.py
# state store management for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Mapping
from typing import Any

@dataclass
class StateStore:
    base_path: Path

    @property
    def state(self) -> Path:
        return self.base_path / "state.json"

    @property
    def tomb(self) -> Path:
        return self.base_path / "tombstones.json"

    @property
    def last(self) -> Path:
        return self.base_path / "last_sync.json"

    @property
    def hide(self) -> Path:
        return self.base_path / "watchlist_hide.json"

    @property
    def ratings_changes(self) -> Path:
        return self.base_path / "ratings_changes.json"

    def _read(self, p: Path, default: Any) -> Any:
        if not p.exists():
            return default
        try:
            return json.loads(p.read_text("utf-8"))
        except Exception:
            return default

    def _write_atomic(self, p: Path, data: Any) -> None:
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(p)

    def load_state(self) -> dict[str, Any]:
        return self._read(
            self.state,
            {"providers": {}, "wall": [], "last_sync_epoch": None},
        )

    def save_state(self, data: Mapping[str, Any]) -> None:
        self._write_atomic(self.state, data)

    def load_tomb(self) -> dict[str, Any]:
        t = self._read(self.tomb, {"keys": {}, "pruned_at": None})
        if "ttl_sec" not in t:
            t["ttl_sec"] = None
        return t

    def save_tomb(self, data: Mapping[str, Any]) -> None:
        self._write_atomic(self.tomb, data)

    def save_last(self, data: Mapping[str, Any]) -> None:
        self._write_atomic(self.last, data)

    def clear_watchlist_hide(self) -> None:
        try:
            if self.hide.exists():
                self.hide.unlink()
        except Exception:
            try:
                self.hide.write_text("[]", encoding="utf-8")
            except Exception:
                pass

    def save_ratings_changes(self, data: Mapping[str, Any]) -> None:
        try:
            self._write_atomic(self.ratings_changes, data)
        except Exception:
            pass