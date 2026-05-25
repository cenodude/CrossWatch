# CrossWatch - Scrobble source toggles
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


SourceMap = dict[str, bool]


def _scrobble_block(cfg_or_scrobble: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not isinstance(cfg_or_scrobble, Mapping):
        return {}
    sc = cfg_or_scrobble.get("scrobble")
    if isinstance(sc, Mapping):
        return sc
    return cfg_or_scrobble


def normalize_source_name(source: Any) -> str:
    raw = str(source or "").strip().lower()
    if raw in {"watch", "watcher"}:
        return "watcher"
    if raw in {"webhook", "webhooks"}:
        return "webhook"
    return raw


def scrobble_sources(cfg_or_scrobble: Mapping[str, Any] | None) -> SourceMap:
    sc = _scrobble_block(cfg_or_scrobble)
    if not bool(sc.get("enabled")):
        return {"webhook": False, "watcher": False}

    sources = sc.get("sources")
    if isinstance(sources, Mapping):
        watcher_value = sources.get("watcher", sources.get("watch", False))
        return {
            "webhook": bool(sources.get("webhook", False)),
            "watcher": bool(watcher_value),
        }

    mode = str(sc.get("mode") or "").strip().lower()
    return {
        "webhook": mode == "webhook",
        "watcher": mode == "watch",
    }


def source_enabled(cfg_or_scrobble: Mapping[str, Any] | None, source: Any) -> bool:
    name = normalize_source_name(source)
    return bool(scrobble_sources(cfg_or_scrobble).get(name, False))


def legacy_mode_for_sources(sources: Mapping[str, Any] | None, fallback: str = "webhook") -> str:
    if not isinstance(sources, Mapping):
        return "watch" if str(fallback or "").strip().lower() == "watch" else "webhook"
    webhook = bool(sources.get("webhook", False))
    watcher = bool(sources.get("watcher", sources.get("watch", False)))
    return "watch" if watcher and not webhook else "webhook"
