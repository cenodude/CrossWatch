# cw_platform/pair_scope.py
# CrossWatch - Sync Pair State Identity
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from .provider_instances import build_provider_config_view, normalize_instance_id


def pair_feature_scope(config: Mapping[str, Any], pair: Mapping[str, Any], feature: str, index: int = 0) -> str:
    def settings(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(k): settings(v) for k, v in value.items()
                    if not str(k).startswith("_cw_") and not any(word in str(k).lower() for word in ("token", "password", "secret", "api_key"))
                    and str(k) not in {"auth", "oauth", "label"}}
        if isinstance(value, (list, tuple)):
            return [settings(v) for v in value]
        return value

    endpoints = []
    for side in ("source", "target"):
        provider = str(pair.get(side) or "").upper().strip()
        instance = normalize_instance_id(pair.get(f"{side}_instance"))
        view = build_provider_config_view(dict(config), provider, instance)
        block = dict(view.get(provider.lower()) or {})
        for other in ("watchlist", "history", "ratings", "progress", "collection", "playlists"):
            if other != feature:
                block.pop(other, None)
        endpoints.append([provider, instance, settings(block)])
    payload = {
        "pair": str(pair.get("id") or pair.get("pair_id") or index),
        "profile": pair.get("profile_id"),
        "mode": pair.get("mode", "one-way"),
        "endpoints": endpoints,
        "feature": feature,
        "rules": settings((pair.get("features") or {}).get(feature)),
        "overrides": settings(pair.get("providers") or {}),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return "cw2_" + hashlib.sha256(encoded).hexdigest()
