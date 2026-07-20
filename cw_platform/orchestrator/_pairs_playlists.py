# cw_platform/orchestrator/_pairs_playlists.py
# CrossWatch - Playlist mapping dispatch for the pair orchestrator
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..playlists_runner import PlaylistRunError, resolve_pair_mappings, run_mapping


def _emit(ctx, event: str, **fields: Any) -> None:
    try:
        ctx.emit(event, **fields)
    except Exception:
        pass


def _record_playlist_stats(ctx, src: str, dst: str, totals: Mapping[str, Any]) -> None:
    added = int(totals.get("added") or 0)
    removed = int(totals.get("removed") or 0)
    updated = int(totals.get("updated") or 0)
    if not (added or removed or updated):
        return
    stats = getattr(ctx, "stats", None)
    fn = getattr(stats, "record_feature_totals", None)
    if not callable(fn):
        fn = getattr(getattr(stats, "impl", None), "record_feature_totals", None)
    if not callable(fn):
        return
    try:
        fn(
            "playlists",
            added=added,
            removed=removed,
            updated=updated,
            src=dst or src,
            run_id=f"{src}->{dst}:playlists",
        )
    except Exception:
        pass


def run_playlist_mappings(
    ctx,
    src: str,
    dst: str,
    *,
    fcfg: Mapping[str, Any],
    health_map: Mapping[str, Any] | None = None,
    full_cfg: Mapping[str, Any],
    pair: Mapping[str, Any],
) -> dict[str, Any]:
    totals = {
        "ok": True,
        "added": 0,
        "removed": 0,
        "updated": 0,
        "unresolved": 0,
        "skipped": 0,
        "errors": 0,
        "mappings": 0,
        "warnings": [],
    }

    resolved = resolve_pair_mappings(full_cfg, pair)
    if not resolved:
        _emit(ctx, "playlist:pair", src=src, dst=dst, mappings=0)
        return totals

    providers = ctx.providers or {}
    dry_run = bool(getattr(ctx, "dry_run", False))

    for mapping in resolved:
        mapping_id = str(mapping.get("id") or "")
        try:
            res = run_mapping(
                full_cfg,
                mapping,
                dry_run=dry_run,
                providers=providers,
                emit=ctx.emit,
            )
        except PlaylistRunError as e:
            totals["errors"] += 1
            _emit(ctx, "playlist:mapping:error", src=src, dst=dst, mapping=mapping_id, error=str(e))
            continue
        except Exception as e:
            totals["errors"] += 1
            _emit(ctx, "playlist:mapping:error", src=src, dst=dst, mapping=mapping_id, error=str(e))
            continue

        totals["mappings"] += 1
        totals["added"] += int(res.get("added", 0) or 0)
        totals["removed"] += int(res.get("removed", 0) or 0)
        totals["updated"] += int(res.get("reordered", 0) or 0)
        totals["unresolved"] += int(res.get("unresolved_count", len(res.get("unresolved") or [])) or 0)
        if not res.get("ok", True):
            totals["errors"] += 1
        for w in res.get("warnings") or []:
            if w not in totals["warnings"]:
                totals["warnings"].append(str(w))

    _emit(
        ctx,
        "playlist:pair",
        src=src,
        dst=dst,
        mappings=totals["mappings"],
        added=totals["added"],
        removed=totals["removed"],
        reordered=totals["updated"],
        unresolved=totals["unresolved"],
        errors=totals["errors"],
    )
    _record_playlist_stats(ctx, src, dst, totals)
    return totals
