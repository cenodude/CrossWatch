# cw_platform/orchestrator/_state_store.py
# state store management for orchestrator.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Mapping
from typing import Any

from ..local_db import last_sync as sqlite_last_sync
from ..local_db import manual_policy as sqlite_manual_policy
from ..local_db import state as sqlite_state
from ..local_db import watchlist_hide as sqlite_watchlist_hide

@dataclass
class StateStore:
    base_path: Path

    @property
    def cw_state_dir(self) -> Path:
        p = self.base_path / ".cw_state"
        try:
            p.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        return p

    @property
    def tomb(self) -> Path:
        return self.cw_state_dir / "tombstones.json"

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
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        tmp = p.with_suffix(p.suffix + ".tmp")
        text = json.dumps(data, ensure_ascii=False, indent=2)
        tmp.write_text(text, "utf-8")
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

            for feature in ("watchlist", "history", "ratings", "progress", "playlists"):
                if feature in p_node and isinstance(p_node.get(feature), dict):
                    _merge_feature(p_node, feature, p_node.get(feature))

        return state

    def load_state(self) -> dict[str, Any]:
        state = sqlite_state.load_state(self.base_path)
        policy = sqlite_manual_policy.load_policy(self.base_path)
        return self._merge_policy(state, policy)

    def _filter_policy_features(self, policy: Mapping[str, Any], features: set[str]) -> dict[str, Any]:
        wanted = {str(feature or "").strip().lower() for feature in features if str(feature or "").strip()}
        out: dict[str, Any] = {"version": policy.get("version", 1), "providers": {}}
        providers = policy.get("providers")
        if not wanted or not isinstance(providers, Mapping):
            return out
        for provider, node in providers.items():
            if not isinstance(node, Mapping):
                continue
            dst: dict[str, Any] = {}
            for feature in wanted:
                if isinstance(node.get(feature), Mapping):
                    dst[feature] = node[feature]
            insts = node.get("instances")
            if isinstance(insts, Mapping):
                kept_insts: dict[str, Any] = {}
                for inst, inst_node in insts.items():
                    if not isinstance(inst_node, Mapping):
                        continue
                    kept = {feature: inst_node[feature] for feature in wanted if isinstance(inst_node.get(feature), Mapping)}
                    if kept:
                        kept_insts[str(inst)] = kept
                if kept_insts:
                    dst["instances"] = kept_insts
            if dst:
                out["providers"][str(provider)] = dst
        return out

    def load_state_features(self, features: set[str] | list[str] | tuple[str, ...]) -> dict[str, Any]:
        wanted = {str(feature or "").strip().lower() for feature in features or [] if str(feature or "").strip()}
        state = sqlite_state.load_state_features(self.base_path, features)
        policy = self._filter_policy_features(sqlite_manual_policy.load_policy(self.base_path), wanted)
        return self._merge_policy(state, policy)

    def provider_feature_counts(self, feature: str = "watchlist") -> dict[str, int]:
        return sqlite_state.provider_feature_counts(self.base_path, feature)

    def provider_names(self, features: set[str] | list[str] | tuple[str, ...] | None = None) -> list[str]:
        return sqlite_state.provider_names(self.base_path, features)

    def feature_inventory(self) -> list[dict[str, Any]]:
        return sqlite_state.feature_inventory(self.base_path)

    def last_sync_epoch(self) -> Any:
        return sqlite_state.last_sync_epoch(self.base_path)

    def save_state(self, data: Mapping[str, Any]) -> None:
        sqlite_state.save_state(self.base_path, data or {})

    def save_feature_baseline(
        self,
        *,
        provider: str,
        feature: str,
        items: Mapping[str, Any],
        instance: str = "default",
        checkpoint: Any = None,
        last_sync_epoch: Any = None,
    ) -> None:
        sqlite_state.save_feature_baseline(
            self.base_path,
            provider=provider,
            instance=instance,
            feature=feature,
            items=items,
            checkpoint=checkpoint,
            last_sync_epoch=last_sync_epoch,
        )

    def save_feature_blocks(
        self,
        blocks: Mapping[tuple[str, str, str], Mapping[str, Any]],
        *,
        last_sync_epoch: Any = None,
    ) -> None:
        sqlite_state.save_feature_blocks(self.base_path, blocks, last_sync_epoch=last_sync_epoch)

    def set_last_sync_epoch(self, value: Any) -> None:
        sqlite_state.set_last_sync_epoch(self.base_path, value)

    def clear_state(self) -> None:
        sqlite_state.clear_state(self.base_path)

    def load_tomb(self) -> dict[str, Any]:
        t = self._read(self.tomb, {"keys": {}, "pruned_at": None})
        if "ttl_sec" not in t:
            t["ttl_sec"] = None
        return t

    def save_tomb(self, data: Mapping[str, Any]) -> None:
        self._write_atomic(self.tomb, data)

    def save_last(self, data: Mapping[str, Any]) -> None:
        sqlite_last_sync.save_last_sync(self.base_path, data or {})

    def clear_watchlist_hide(self) -> None:
        try:
            sqlite_watchlist_hide.clear_hidden(self.base_path)
        except Exception:
            pass

    def save_ratings_changes(self, data: Mapping[str, Any]) -> None:
        try:
            self._write_atomic(self.ratings_changes, data)
        except Exception:
            pass
