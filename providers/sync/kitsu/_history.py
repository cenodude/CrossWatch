# providers/sync/kitsu/_history.py
# CrossWatch - Kitsu watched status and episode history
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
from cw_platform.anime_mapping.service import PAIR_FEATURE_OPTIONS_KEY, runtime_pair_feature_options
from cw_platform.id_map import canonical_key, minimal
from providers.sync._mod_common import build_op_result
from providers.sync._log import log
from ._common import episode_items, media_item, number, resolve_target


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
            item["watch_status"] = {"current": "watching", "planned": "planning"}.get(attr.get("status"), attr.get("status"))
            if stamp:
                item["watched_at"] = stamp
            else:
                item["_cw_watched_state"] = True
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


def _validate_add(item: Any, episode: int, media_attr: Any) -> tuple[bool, int]:
    if item.get("type") not in ("movie", "episode"):
        raise ValueError("unsupported_media_type")
    movie = str(media_attr.get("subtype") or "").lower() == "movie"
    if (item.get("type") == "movie") != movie:
        raise ValueError("anime_media_type_mismatch")
    total = number(media_attr.get("episodeCount")) or (1 if movie else 0)
    if total and episode > total:
        raise ValueError("episode_out_of_range")
    return movie, total


def _add(adapter: Any, item: Any, ident: str, episode: int, entry: Any, media_attr: Any) -> None:
    movie, total = _validate_add(item, episode, media_attr)
    attr = entry.get("attributes", {}) if entry else {}
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
    groups: dict[str, list[tuple[Any, int]]] = {}
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []

    def failed(item: Any, exc: Exception) -> None:
        reason = str(exc) if isinstance(exc, ValueError) else f"request_failed:{type(exc).__name__}"
        unresolved.append({**minimal(item), "reason": reason})

    for item in items:
        try:
            target = resolve_target(adapter.raw_cfg, item)
            if target is None:
                raise ValueError("not_anime_or_no_mapping")
            groups.setdefault(target[0], []).append((item, target[1]))
        except Exception as exc:
            failed(item, exc)

    for ident, group in groups.items():
        try:
            entry = adapter.client.lookup(ident)
            media_attr = adapter.client.media(ident)["attributes"]
        except Exception as exc:
            for item, _ in group:
                failed(item, exc)
            continue
        valid = []
        for item, episode in group:
            try:
                _validate_add(item, episode, media_attr)
                valid.append((item, episode))
            except Exception as exc:
                failed(item, exc)
        if not valid:
            continue
        item, episode = max(valid, key=lambda row: row[1])
        try:
            _add(adapter, item, ident, episode, entry, media_attr)
        except Exception as exc:
            for item, _ in valid:
                failed(item, exc)
            continue
        for item, _ in valid:
            key = canonical_key(item)
            if key and key not in confirmed:
                confirmed.append(key)

    log("KITSU", "history", "info", "write_done", applied=len(confirmed), unresolved=len(unresolved))
    return build_op_result(count=len(confirmed), confirmed_keys=confirmed, unresolved=unresolved,
                           unresolved_keys=[canonical_key(item) for item in unresolved])


def order_removals(cfg: Any, items: Any) -> list[Any]:
    def position(item: Any) -> int:
        try:
            target = resolve_target(cfg, item)
            return target[1] if target else 0
        except Exception:
            return 0

    return sorted(items, key=position, reverse=True)


def remove(adapter: Any, items: Any) -> dict[str, Any]:
    groups: dict[str, list[tuple[Any, int]]] = {}
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []

    def failed(item: Any, exc: Exception) -> None:
        reason = str(exc) if isinstance(exc, ValueError) else f"request_failed:{type(exc).__name__}"
        unresolved.append({**minimal(item), "reason": reason})

    def confirm(item: Any) -> None:
        key = canonical_key(item)
        if key and key not in confirmed:
            confirmed.append(key)

    for item in items:
        try:
            target = resolve_target(adapter.raw_cfg, item)
            if target is None:
                raise ValueError("not_anime_or_no_mapping")
            if item.get("type") not in ("movie", "episode"):
                raise ValueError("unsupported_media_type")
            groups.setdefault(target[0], []).append((item, target[1]))
        except Exception as exc:
            failed(item, exc)

    for ident, group in groups.items():
        try:
            entry = adapter.client.lookup(ident)
            if not entry:
                for item, _ in group:
                    confirm(item)
                continue
            attr = entry.get("attributes", {})
            media = adapter.client.media(ident)["attributes"]
            movie = str(media.get("subtype") or "").lower() == "movie"
            total = number(media.get("episodeCount")) or (1 if movie else 0)
            watched = max(0, number(attr.get("progress")))
            if attr.get("status") == "completed":
                if not total:
                    raise ValueError("kitsu_history_unknown_completed_total")
                watched = max(watched, total)
        except Exception as exc:
            for item, _ in group:
                failed(item, exc)
            continue

        progress = watched
        pending = []
        for item, episode in sorted(group, key=lambda row: row[1], reverse=True):
            if (item.get("type") == "movie") != movie:
                failed(item, ValueError("anime_media_type_mismatch"))
            elif not watched or episode > watched:
                confirm(item)
            elif not movie and episode < progress:
                failed(item, ValueError("kitsu_history_remove_would_erase_later_episodes"))
            else:
                progress = 0 if movie else min(progress, episode - 1)
                pending.append(item)
        if not pending:
            continue
        try:
            adapter.client.save(ident, entry, {"progress": progress, "status": "current", "finishedAt": None})
            if not progress:
                adapter.client.save(ident, entry, {"status": "on_hold"})
        except Exception as exc:
            for item in pending:
                failed(item, exc)
            continue
        for item in pending:
            confirm(item)

    log("KITSU", "history", "info", "write_done", applied=len(confirmed), unresolved=len(unresolved))
    return build_op_result(count=len(confirmed), confirmed_keys=confirmed, unresolved=unresolved,
                           unresolved_keys=[canonical_key(item) for item in unresolved])
