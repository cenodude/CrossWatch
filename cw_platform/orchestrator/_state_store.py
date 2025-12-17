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
    def policy(self) -> Path:
        return self.base_path / "state.manual.json"

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


    def _merge_policy(self, state: dict[str, Any], policy: Any) -> dict[str, Any]:
        if not isinstance(state, dict):
            state = {"providers": {}, "wall": [], "last_sync_epoch": None}
        provs = state.get("providers")
        if not isinstance(provs, dict):
            provs = {}
            state["providers"] = provs
        if not isinstance(policy, dict):
            return state
        p_provs = policy.get("providers")
        if not isinstance(p_provs, dict):
            return state

        def _merge_feature(p_node: dict[str, Any], feature: str, f_node: Any) -> None:
            if not isinstance(f_node, dict):
                return
            s_node = provs.get(p_node["__prov_key__"])
            if not isinstance(s_node, dict):
                s_node = {}
                provs[p_node["__prov_key__"]] = s_node
            s_manual = s_node.get("manual")
            if not isinstance(s_manual, dict):
                s_manual = {}
                s_node["manual"] = s_manual
            s_feat = s_manual.get(feature)
            if not isinstance(s_feat, dict):
                s_feat = {}
                s_manual[feature] = s_feat

            p_blocks = f_node.get("blocks")
            if isinstance(p_blocks, list):
                s_blocks = s_feat.get("blocks")
                if not isinstance(s_blocks, list):
                    s_blocks = []
                s_feat["blocks"] = list(dict.fromkeys([*s_blocks, *p_blocks]))

            p_adds = f_node.get("adds")
            if isinstance(p_adds, dict):
                p_items = p_adds.get("items")
                if isinstance(p_items, dict):
                    s_adds = s_feat.get("adds")
                    if not isinstance(s_adds, dict):
                        s_adds = {}
                    s_items = s_adds.get("items")
                    if not isinstance(s_items, dict):
                        s_items = {}
                    for k, v in p_items.items():
                        if k not in s_items:
                            s_items[k] = v
                    s_adds["items"] = s_items
                    s_feat["adds"] = s_adds

        for prov, p_node_any in p_provs.items():
            if not isinstance(p_node_any, dict):
                continue
            prov_key = str(prov).upper()
            p_node: dict[str, Any] = dict(p_node_any)
            p_node["__prov_key__"] = prov_key

            manual = p_node.get("manual")
            if isinstance(manual, dict):
                for feature, f_node in manual.items():
                    _merge_feature(p_node, str(feature).lower(), f_node)

            for feature in ("watchlist", "history", "ratings", "playlists"):
                if feature in p_node and isinstance(p_node.get(feature), dict):
                    _merge_feature(p_node, feature, p_node.get(feature))

        return state

    def load_state(self) -> dict[str, Any]:
        state = self._read(
            self.state,
            {"providers": {}, "wall": [], "last_sync_epoch": None},
        )
        policy = self._read(self.policy, {"providers": {}})
        return self._merge_policy(state, policy)

    def save_state(self, data: Mapping[str, Any]) -> None:
        state = dict(data or {})
        policy = self._read(self.policy, {"providers": {}})
        state = self._merge_policy(state, policy)
        self._write_atomic(self.state, state)

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
