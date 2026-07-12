# providers/webhooks/config.py
# CrossWatch - Webhook settings resolver
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from cw_platform.provider_instances import normalize_instance_id

_SINKS = {"trakt", "simkl", "mdblist"}


def _dict(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, Mapping) else {}


def _merge(base: Mapping[str, Any] | None, overlay: Mapping[str, Any] | None) -> dict[str, Any]:
    out = copy.deepcopy(dict(base or {}))
    for k, v in dict(overlay or {}).items():
        cur = out.get(k)
        if isinstance(cur, dict) and isinstance(v, Mapping):
            out[k] = _merge(cur, v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def webhook_settings(cfg: Mapping[str, Any] | None, provider: str, provider_instance: Any = None) -> dict[str, Any]:
    sc = _dict(_dict(cfg).get("scrobble"))
    wh = _dict(sc.get("webhook"))
    provider_key = str(provider or "").strip().lower()
    inst = normalize_instance_id(provider_instance)

    out = copy.deepcopy(wh)
    providers = _dict(wh.get("providers"))
    out = _merge(out, _dict(providers.get(provider_key)))

    profiles = _dict(wh.get("profiles"))
    provider_profiles = _dict(profiles.get(provider_key))
    out = _merge(out, _dict(provider_profiles.get(inst)))
    return out


def webhook_sinks(cfg: Mapping[str, Any] | None, provider: str, provider_instance: Any = None) -> list[str]:
    wh = webhook_settings(cfg, provider, provider_instance)
    raw = wh.get("sinks")
    items = raw if isinstance(raw, (list, tuple, set)) else str(raw or "").split(",")
    out: list[str] = []
    for item in items:
        sink = str(item or "").strip().lower()
        if sink in _SINKS and sink not in out:
            out.append(sink)
    return out or ["trakt"]


def apply_webhook_settings(cfg: Mapping[str, Any] | None, provider: str, provider_instance: Any = None) -> dict[str, Any]:
    out = copy.deepcopy(dict(cfg or {}))
    sc = out.setdefault("scrobble", {})
    if not isinstance(sc, dict):
        sc = {}
        out["scrobble"] = sc
    sc["webhook"] = webhook_settings(out, provider, provider_instance)
    return out
