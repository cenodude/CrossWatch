# providers/sync/kitsu/_history.py
# CrossWatch - Kitsu watched status and episode history
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
from cw_platform.anime_mapping.service import PAIR_FEATURE_OPTIONS_KEY, runtime_pair_feature_options
from cw_platform.id_map import canonical_key
from ._common import apply_items, episode_items, media_item, number, resolve_target


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    out = {}
    for entry, media in adapter.client.entries():
        attr = entry["attributes"]
        total = number(media["attributes"].get("episodeCount"))
        watched = max(0, number(attr.get("progress")))
        if attr.get("status") == "completed":
            watched = max(watched, total)
        if total:
            watched = min(watched, total)
        movie = str(media["attributes"].get("subtype") or "").lower() == "movie"
        if movie and (watched or attr.get("status") == "completed"):
            items = [media_item(adapter, media)]
        else:
            items = episode_items(adapter.raw_cfg, str(media["id"]), watched, str(media["attributes"].get("canonicalTitle") or ""))
        stamp = attr.get("finishedAt") or attr.get("progressedAt") or attr.get("updatedAt")
        for item in items:
            item["watched"] = True
            if stamp:
                item["watched_at"] = stamp
            out[canonical_key(item)] = item
    return out


def _source_status(adapter: Any, item: Any, attr: Any) -> str | None:
    cfg = adapter.raw_cfg
    if PAIR_FEATURE_OPTIONS_KEY not in cfg or not runtime_pair_feature_options(cfg, "history").get("use_source_status"):
        return None
    status = str(item.get("watch_status") or "").strip().lower()
    if not status:
        status = str(attr.get("status") or "")
    return status if status in ("dropped", "on_hold") else None


def _add(adapter: Any, item: Any, ident: str, episode: int) -> None:
    if item.get("type") not in ("movie", "episode"):
        raise ValueError("unsupported_media_type")
    entry = adapter.client.lookup(ident)
    attr = entry.get("attributes", {}) if entry else {}
    media = adapter.client.media(ident)
    media_attr = media["attributes"]
    movie = str(media_attr.get("subtype") or "").lower() == "movie"
    if (item.get("type") == "movie") != movie:
        raise ValueError("anime_media_type_mismatch")
    total = number(media_attr.get("episodeCount")) or (1 if movie else 0)
    if total and episode > total:
        raise ValueError("episode_out_of_range")
    complete = movie or bool(total and episode >= total and media_attr.get("status") == "finished")
    source_status = None if complete else _source_status(adapter, item, attr)
    progress = number(attr.get("progress"))
    if attr.get("status") == "completed" or progress > episode:
        return
    if progress == episode:
        if source_status and attr.get("status") == "current":
            adapter.client.save(ident, entry, {"status": source_status})
        return
    status = "completed" if complete else "current"
    adapter.client.save(ident, entry, {"progress": episode, "status": status})
    if source_status:
        entry = entry or adapter.client.lookup(ident)
        if not entry:
            raise RuntimeError("Kitsu did not return the updated library entry")
        adapter.client.save(ident, entry, {"status": source_status})


def add(adapter: Any, items: Any) -> dict[str, Any]:
    return apply_items(adapter, "history", items, _add)


def order_removals(cfg: Any, items: Any) -> list[Any]:
    def position(item: Any) -> int:
        try:
            target = resolve_target(cfg, item)
            return target[1] if target else 0
        except Exception:
            return 0

    return sorted(items, key=position, reverse=True)


def _remove(adapter: Any, item: Any, ident: str, episode: int) -> None:
    if item.get("type") not in ("movie", "episode"):
        raise ValueError("unsupported_media_type")
    entry = adapter.client.lookup(ident)
    if not entry:
        return
    attr = entry.get("attributes", {})
    media = adapter.client.media(ident)["attributes"]
    movie = str(media.get("subtype") or "").lower() == "movie"
    if (item.get("type") == "movie") != movie:
        raise ValueError("anime_media_type_mismatch")
    total = number(media.get("episodeCount")) or (1 if movie else 0)
    watched = max(0, number(attr.get("progress")))
    if attr.get("status") == "completed":
        if not total:
            raise ValueError("kitsu_history_unknown_completed_total")
        watched = max(watched, total)
    if not movie and episode < watched:
        raise ValueError("kitsu_history_remove_would_erase_later_episodes")
    if not watched or episode > watched:
        return
    progress = 0 if movie else episode - 1
    adapter.client.save(ident, entry, {"progress": progress, "status": "current", "finishedAt": None})
    if not progress:
        adapter.client.save(ident, entry, {"status": "on_hold"})


def remove(adapter: Any, items: Any) -> dict[str, Any]:
    return apply_items(adapter, "history", order_removals(adapter.raw_cfg, items), _remove)
