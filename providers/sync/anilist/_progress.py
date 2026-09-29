# /providers/sync/anilist/_progress.py
# AniList one-way progress writer
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

from cw_platform.anime_mapping import AnimeMappingService
from cw_platform.anime_mapping.coordinates import translate
from cw_platform.anime_mapping.episodes import resolve_absolute
from cw_platform.anime_mapping.storage import normalize_release_tag, query_edges

from .._log import log as cw_log

GQL_MEDIA_ENTRY = """
query ($id: Int!) {
  Media(id: $id, type: ANIME) {
    id
    episodes
    status
    title { romaji english }
    mediaListEntry { id status progress repeat startedAt { year month day } }
  }
}
""".strip()

GQL_SAVE_PROGRESS = """
mutation ($mediaId: Int!, $status: MediaListStatus, $progress: Int, $startedAt: FuzzyDateInput, $completedAt: FuzzyDateInput) {
  SaveMediaListEntry(mediaId: $mediaId, status: $status, progress: $progress, startedAt: $startedAt, completedAt: $completedAt) {
    id
    status
    progress
    repeat
  }
}
""".strip()

_NATIVE_NAMESPACES = ("anilist", "mal", "anidb")
_ANIDB_REGULAR = "R"


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "progress", "debug", msg, **fields)


def _to_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(str(value).strip())
    except Exception:
        return None


def _clean_ids(value: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, item in dict(value if isinstance(value, Mapping) else {}).items():
        name = str(key or "").strip().lower()
        text = str(item or "").strip()
        if name and text:
            out[name] = text
    return out


def _release_tag(cfg: Mapping[str, Any] | None) -> str:
    block = (cfg or {}).get("anime_mapping") if isinstance(cfg, Mapping) else {}
    return normalize_release_tag((block if isinstance(block, Mapping) else {}).get("release_tag") or "v3")


def native_to_anilist(release_tag: str, namespace: str, ident: str, episode: int) -> tuple[int, int] | None:
    ns = str(namespace or "").strip().lower()
    if ns == "anilist":
        aid = _to_int(ident)
        return (aid, int(episode)) if aid else None
    if ns not in _NATIVE_NAMESPACES:
        return None
    try:
        rows = query_edges(release_tag, ns, str(ident))
    except Exception:
        return None
    hits: set[tuple[int, int]] = set()
    for row in rows:
        if str(row.get("target_provider") or "").strip().lower() != "anilist":
            continue
        if ns == "anidb" and str(row.get("source_scope") or "").strip().upper() != _ANIDB_REGULAR:
            continue
        mapped = translate(row.get("source_range"), row.get("target_range"), episode)
        aid = _to_int(row.get("target_id"))
        if mapped and aid:
            hits.add((aid, int(mapped)))
    return hits.pop() if len(hits) == 1 else None


def aired_to_anilist(release_tag: str, show_ids: Mapping[str, Any], season: int | None, episode: int | None) -> tuple[int, int] | None:
    if season is None or season < 0 or episode is None or episode <= 0:
        return None
    for provider in ("tvdb", "tmdb"):
        ident = str(show_ids.get(provider) or "").strip()
        if not ident:
            continue
        try:
            rows = query_edges(release_tag, provider, ident, scope=f"s{season}")
        except Exception:
            continue
        hits: set[tuple[int, int]] = set()
        for row in rows:
            if str(row.get("source_kind") or "").strip().lower() != "show":
                continue
            if str(row.get("target_provider") or "").strip().lower() != "anilist":
                continue
            mapped = translate(row.get("source_range"), row.get("target_range"), episode)
            aid = _to_int(row.get("target_id"))
            if mapped and aid:
                hits.add((aid, int(mapped)))
        if len(hits) == 1:
            return hits.pop()
        if hits:
            return None
    return None


def aired_entries(release_tag: str, show_ids: Mapping[str, Any], season: int | None = None) -> list[int]:
    if season is not None and season < 0:
        return []
    scope = f"s{season}" if season is not None else None
    out: list[int] = []
    for provider in ("tvdb", "tmdb"):
        ident = str(show_ids.get(provider) or "").strip()
        if not ident:
            continue
        try:
            rows = query_edges(release_tag, provider, ident, scope=scope)
        except Exception:
            continue
        for row in rows:
            if str(row.get("source_kind") or "").strip().lower() != "show":
                continue
            if str(row.get("target_provider") or "").strip().lower() != "anilist":
                continue
            aid = _to_int(row.get("target_id"))
            if aid and aid not in out:
                out.append(aid)
        if out:
            return out
    return out


def _enriched_ids(svc: AnimeMappingService, ids: Mapping[str, Any], media_type: str) -> dict[str, str]:
    base = _clean_ids(ids)
    try:
        enriched = svc.enrich_ids(base, media_type=media_type)
    except Exception:
        return base
    got = _clean_ids(enriched.get("ids") if isinstance(enriched, Mapping) else None)
    return {**base, **got} if got else base


def resolve_target(cfg: Mapping[str, Any] | None, item: Mapping[str, Any]) -> tuple[int, int] | None:
    svc = AnimeMappingService(cfg)
    if not svc.ready():
        return None
    tag = _release_tag(cfg)
    kind = str(item.get("type") or "").strip().lower()
    if kind == "movie":
        ids = _enriched_ids(svc, item.get("ids") or {}, "movie")
        for ns in _NATIVE_NAMESPACES:
            if ids.get(ns):
                hit = native_to_anilist(tag, ns, ids[ns], 1)
                if hit:
                    return hit
        return None
    if kind != "episode":
        return None
    show_ids = _enriched_ids(svc, item.get("show_ids") or {}, "show")
    probe = {**dict(item), "show_ids": show_ids}
    try:
        res = resolve_absolute(probe, release_tag=tag)
    except Exception as exc:
        _dbg("resolve_failed", error_type=exc.__class__.__name__)
        return None
    hit = None
    if res is None or res.basis != "user_override":
        hit = aired_to_anilist(tag, show_ids, _to_int(item.get("season")), _to_int(item.get("episode")))
        own = _to_int(_clean_ids(item.get("show_ids")).get("anilist"))
        if hit and own and hit[0] != own:
            hit = None
    if hit is None and res is not None:
        hit = native_to_anilist(tag, res.namespace, res.target_id, res.absolute)
    _dbg(
        "resolved",
        season=_to_int(item.get("season")),
        episode=_to_int(item.get("episode")),
        namespace=res.namespace if res else None,
        target_id=res.target_id if res else None,
        absolute=res.absolute if res else None,
        anilist_id=hit[0] if hit else None,
        anilist_episode=hit[1] if hit else None,
    )
    return hit


def _fuzzy(day: date) -> dict[str, int]:
    return {"year": day.year, "month": day.month, "day": day.day}


def plan_progress(media: Mapping[str, Any], episode: int, *, today: date | None = None) -> dict[str, Any]:
    entry = media.get("mediaListEntry") if isinstance(media.get("mediaListEntry"), Mapping) else {}
    status = str((entry or {}).get("status") or "").strip().upper()
    current = _to_int((entry or {}).get("progress")) or 0
    total = _to_int(media.get("episodes"))
    target = int(episode)
    if total and total > 0 and target > total:
        target = total
    if status == "COMPLETED":
        return {"skip": "already_completed", "status": status, "progress": current, "total": total}
    if target <= current:
        return {"skip": "not_newer", "status": status, "progress": current, "total": total}
    day = today or date.today()
    variables: dict[str, Any] = {"mediaId": int(media.get("id") or 0), "progress": target}
    if status == "REPEATING":
        variables["status"] = "REPEATING"
    else:
        finished = bool(total and target >= total and str(media.get("status") or "").upper() == "FINISHED")
        variables["status"] = "COMPLETED" if finished else "CURRENT"
        started = (entry or {}).get("startedAt") if isinstance((entry or {}).get("startedAt"), Mapping) else {}
        if not (started or {}).get("year"):
            variables["startedAt"] = _fuzzy(day)
        if finished:
            variables["completedAt"] = _fuzzy(day)
    return {"variables": variables, "status": status, "progress": current, "total": total}


def apply_progress(client: Any, anilist_id: int, episode: int, *, today: date | None = None) -> dict[str, Any]:
    data = client.gql(GQL_MEDIA_ENTRY, {"id": int(anilist_id)}, feature="progress:lookup", tolerate_errors=True)
    media = (data or {}).get("Media")
    if not isinstance(media, Mapping) or not media.get("id"):
        return {"ok": False, "reason": "media_not_found", "anilist_id": int(anilist_id)}
    plan = plan_progress(media, episode, today=today)
    title_block = media.get("title") if isinstance(media.get("title"), Mapping) else {}
    title = str((title_block or {}).get("english") or (title_block or {}).get("romaji") or "")
    base = {
        "anilist_id": int(anilist_id),
        "title": title,
        "episode": int(episode),
        "previous_status": plan.get("status") or None,
        "previous_progress": plan.get("progress"),
        "total": plan.get("total"),
    }
    if plan.get("skip"):
        return {"ok": True, "skipped": True, "reason": plan["skip"], **base}
    saved = client.gql(GQL_SAVE_PROGRESS, plan["variables"], feature="progress:save")
    row = (saved or {}).get("SaveMediaListEntry")
    row = row if isinstance(row, Mapping) else {}
    return {"ok": True, "status": row.get("status"), "progress": row.get("progress"), **base}


__all__ = ["resolve_target", "native_to_anilist", "aired_to_anilist", "aired_entries", "plan_progress", "apply_progress"]
