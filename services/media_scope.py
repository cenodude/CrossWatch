# services/media_scope.py
# CrossWatch - Shared anime sync scope for media views
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any

from cw_platform.anime_mapping.service import (
    ANIME_ONLY_TARGET_KEYS,
    AnimeMappingService,
    _anime_only_keep,
    anime_mapping_pair_feature_options,
)
from cw_platform.provider_instances import normalize_instance_id


class MediaScope:
    def __init__(self, cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.restricted: set[tuple[str, str, str]] = set()
        unrestricted: set[tuple[str, str, str]] = set()
        for pair in cfg.get("pairs") or []:
            if not isinstance(pair, Mapping) or not pair.get("enabled", True):
                continue
            endpoints = [
                (str(pair.get(side) or "").upper(), normalize_instance_id(pair.get(f"{side}_instance")))
                for side in ("source", "target")
            ]
            anime = any(provider.lower() in ANIME_ONLY_TARGET_KEYS for provider, _ in endpoints)
            for feature, raw in (pair.get("features") or {}).items():
                if feature not in {"watchlist", "ratings", "history", "progress"}:
                    continue
                options = raw if isinstance(raw, Mapping) else {"enable": bool(raw)}
                if not options.get("enable", False):
                    continue
                effective = anime_mapping_pair_feature_options(
                    cfg, options, feature, *(provider for provider, _ in endpoints)
                )
                bucket = self.restricted if anime or effective.get("anime_only_sync") else unrestricted
                bucket.update((provider, instance, feature) for provider, instance in endpoints)
        self.restricted.difference_update(unrestricted)
        self.mapping = AnimeMappingService(cfg)
        self._cache: dict[str, dict[tuple[str, str], bool]] = {}

    def fingerprint(self) -> tuple[Any, ...]:
        from cw_platform.anime_mapping.overrides import overrides_path
        from cw_platform.anime_mapping.storage import paths

        stamps = []
        if self.restricted:
            for path in [paths(self.mapping.release_tag)["db"], overrides_path()]:
                try:
                    stat = path.stat()
                    stamps.append((str(path), stat.st_mtime_ns, stat.st_size))
                except OSError:
                    stamps.append((str(path), 0, 0))
        return (tuple(sorted(self.restricted)), json.dumps(self.cfg.get("anime_mapping") or {}, sort_keys=True), tuple(stamps))

    def active(self, features: Any) -> bool:
        return any(feature in features for _, _, feature in self.restricted)

    def keep(self, raw: Any, provider: str, instance: str, feature: str) -> bool:
        if (provider.upper(), normalize_instance_id(instance), feature) not in self.restricted:
            return True
        if provider.lower() in ANIME_ONLY_TARGET_KEYS:
            return True
        if not isinstance(raw, Mapping):
            return False
        nested = raw.get("item")
        item: Mapping[str, Any] = nested if isinstance(nested, Mapping) else raw
        if str(item.get("simkl_bucket") or "").lower() == "anime":
            return True
        for field in ("ids", "show_ids"):
            ids = item.get(field) or {}
            if isinstance(ids, Mapping) and any(ids.get(key) for key in ("mal", "anilist", "kitsu", "anidb")):
                return True
        for target in ("myanimelist", "anilist", "kitsu"):
            if _anime_only_keep(self.mapping, item, feature == "history" or item.get("type") in {"episode", "season"},
                                self._cache.setdefault(target, {}), target):
                return True
        return False

    def state(self, state: Mapping[str, Any]) -> dict[str, Any]:
        if not state or not self.restricted:
            return dict(state)

        def block(raw: Mapping[str, Any], provider: str, instance: str) -> dict[str, Any]:
            out = dict(raw)
            for prov, inst, feature in self.restricted:
                if (prov, inst) != (provider.upper(), normalize_instance_id(instance)):
                    continue
                if feature == "watchlist" and isinstance(raw.get("items"), Mapping):
                    out["items"] = {
                        key: item for key, item in raw["items"].items()
                        if self.keep(item, provider, instance, feature)
                    }
                node = raw.get(feature)
                if not isinstance(node, Mapping):
                    continue
                baseline = node.get("baseline")
                if not isinstance(baseline, Mapping):
                    continue
                items = baseline.get("items") or {}
                out[feature] = {**node, "baseline": {**baseline, "items": {
                    key: item for key, item in items.items() if self.keep(item, provider, instance, feature)
                }}}
            return out

        providers = {}
        for provider, raw in (state.get("providers") or {}).items():
            if not isinstance(raw, Mapping):
                providers[provider] = raw
                continue
            node = block(raw, provider, "default")
            if isinstance(raw.get("instances"), Mapping):
                node["instances"] = {
                    instance: block(value, provider, instance) if isinstance(value, Mapping) else value
                    for instance, value in raw["instances"].items()
                }
            providers[provider] = node
        return {**state, "providers": providers}

    def tracker(self, items: Mapping[str, Any], feature: str) -> dict[str, Any]:
        return {key: item for key, item in items.items() if self.keep(item, "CROSSWATCH", "default", feature)}

    def totals(self, state: Mapping[str, Any], features: Any, user_filter: Mapping[str, Any]) -> dict[str, int]:
        from services.dashboard_widgets import _endpoint_matches_user, _feature_items, _provider_blocks

        totals = {}
        for feature in features:
            counts = []
            for provider in state.get("providers") or {}:
                counts.append(sum(
                    len(_feature_items(block, feature))
                    for instance, block in _provider_blocks(state, provider)
                    if _endpoint_matches_user(provider, instance, user_filter)
                ))
            totals[feature] = max(counts, default=0)
        return totals
