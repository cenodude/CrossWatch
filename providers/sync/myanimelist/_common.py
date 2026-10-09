# providers/sync/myanimelist/_common.py
# CrossWatch - MyAnimeList library and anime mapping helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
from typing import Any

from cw_platform.anime_mapping import AnimeMappingService
from cw_platform.anime_mapping.coordinates import translate
from cw_platform.anime_mapping.episodes import resolve_absolute, resolve_axis_coordinate_map
from cw_platform.anime_mapping.overrides import find_episode_override, find_source_identity_overrides, find_source_overrides
from cw_platform.anime_mapping.storage import normalize_release_tag, query_edges
from cw_platform.id_map import canonical_key, minimal
from providers.sync._mod_common import build_op_result
from providers.sync._log import log


def number(value: Any) -> int:
    try:
        return int(value) if not isinstance(value, bool) else 0
    except (ValueError, TypeError):
        return 0


def release_tag(cfg: Mapping[str, Any]) -> str:
    return normalize_release_tag((cfg.get("anime_mapping") or {}).get("release_tag") or "v3")


def mapping_enabled(cfg: Mapping[str, Any]) -> bool:
    return bool((cfg.get("anime_mapping") or {}).get("enabled"))


def native_target(tag: str, namespace: str, ident: str, episode: int) -> tuple[str, int] | None:
    if namespace == "mal":
        return (str(ident), episode) if number(ident) > 0 and episode > 0 else None
    hits = set()
    for row in query_edges(tag, namespace, ident):
        target_provider = str(row.get("target_provider") or "")
        target_id = str(row.get("target_id") or "")
        if target_provider != "mal":
            continue
        if namespace == "anidb" and str(row.get("source_scope") or "").upper() != "R":
            continue
        mapped = translate(row.get("source_range"), row.get("target_range"), episode)
        if mapped and number(target_id) > 0:
            hits.add((target_id, int(mapped)))
    return hits.pop() if len(hits) == 1 else None


def resolve_target(cfg: Mapping[str, Any], item: Mapping[str, Any]) -> tuple[str, int] | None:
    kind = item.get("type")
    if kind in ("movie", "show"):
        ids = dict(item.get("ids") or {})
        if mapping_enabled(cfg):
            service = AnimeMappingService(cfg)
            ids = service.enrich_ids(ids, media_type=str(kind)).get("ids") or ids
            if not ids.get("mal"):
                ids = service.enrich_ids(ids, media_type=str(kind)).get("ids") or ids
        ident = str(ids.get("mal") or "")
        return (ident, 1) if number(ident) > 0 else None
    if kind != "episode" or not mapping_enabled(cfg):
        return None
    tag = release_tag(cfg)
    ids = dict(item.get("show_ids") or {})
    resolved = resolve_absolute(item, release_tag=tag)
    if resolved and resolved.basis == "user_override":
        return native_target(tag, resolved.namespace, resolved.target_id, resolved.absolute)
    season, episode = number(item.get("season")), number(item.get("episode"))
    if season <= 0 or episode <= 0:
        return None
    for provider in ("tvdb", "tmdb"):
        if not ids.get(provider):
            continue
        hits = set()
        for row in query_edges(tag, provider, str(ids[provider]), scope=f"s{season}"):
            if row.get("source_kind") != "show" or row.get("target_provider") != "mal":
                continue
            mapped = translate(row.get("source_range"), row.get("target_range"), episode)
            if mapped and number(row.get("target_id")) > 0:
                hits.add((str(row["target_id"]), int(mapped)))
        if len(hits) == 1:
            hit = hits.pop()
            return hit if not ids.get("mal") or str(ids["mal"]) == hit[0] else None
        if hits:
            return None
    if resolved:
        return native_target(tag, resolved.namespace, resolved.target_id, resolved.absolute)
    if ids.get("mal") and season == 1 and not any(ids.get(p) for p in ("tvdb", "tmdb")):
        return native_target(tag, "mal", str(ids["mal"]), episode)
    return None


def media_item(adapter: Any, media: Mapping[str, Any]) -> dict[str, Any]:
    attr = media
    kind = "movie" if str(attr.get("media_type") or "").lower() == "movie" else "show"
    item = {"type": kind, "ids": {"mal": str(media["id"])}, "title": attr.get("title") or "",
            "year": number(str(attr.get("start_date") or "")[:4])}
    if mapping_enabled(adapter.raw_cfg):
        item = AnimeMappingService(adapter.raw_cfg).enrich_item(item)
        item["ids"].update(find_source_identity_overrides(item["ids"], media_type=kind))
    return item


def episode_items(cfg: Mapping[str, Any], ident: str, count: int, title: str) -> list[dict[str, Any]]:
    if not mapping_enabled(cfg):
        return []
    tag = release_tag(cfg)
    out = []
    for episode in range(1, count + 1):
        coords = [(hit.provider, hit.ident, hit.season, hit.episode)
                  for hit in find_source_overrides("mal", ident, episode)]
        if not coords:
            coords = [(p, i, s, e) for (p, i), (s, e) in resolve_axis_coordinate_map(
                {"mal": ident}, episode, release_tag=tag).items()]
        for provider, show_id, season, ep in set(coords):
            ruled = find_episode_override({provider: show_id}, season, ep)
            if ruled:
                hit = native_target(tag, ruled.namespace, ruled.target_id, ruled.absolute)
                if hit != (ident, episode):
                    continue
            out.append({"type": "episode", "ids": {}, "show_ids": {provider: show_id},
                        "season": season, "episode": ep, "title": title, "series_title": title})
    return out


def apply_items(adapter: Any, feature: str, items: Any, writer: Any) -> dict[str, Any]:
    confirmed, unresolved = [], []
    resolved = []

    def failed(item: Any, exc: Exception) -> None:
        reason = str(exc) if isinstance(exc, ValueError) else f"request_failed:{type(exc).__name__}"
        unresolved.append({**minimal(item), "reason": reason})

    for item in items:
        try:
            target = resolve_target(adapter.raw_cfg, item)
            if target is None:
                raise ValueError("not_anime_or_no_mapping")
            resolved.append((item, *target))
        except Exception as exc:
            failed(item, exc)
    try:
        batch = adapter.client.library_batch(ident for _, ident, _ in resolved) if resolved else nullcontext()
        with batch:
            for item, ident, episode in resolved:
                try:
                    writer(adapter, item, ident, episode)
                    key = canonical_key(item)
                    if key and key not in confirmed:
                        confirmed.append(key)
                except Exception as exc:
                    failed(item, exc)
    except Exception as exc:
        for item, _, _ in resolved:
            failed(item, exc)
    log("MYANIMELIST", feature, "info", "write_done", applied=len(confirmed), unresolved=len(unresolved))
    return build_op_result(count=len(confirmed), confirmed_keys=confirmed, unresolved=unresolved,
                           unresolved_keys=[canonical_key(item) for item in unresolved])
