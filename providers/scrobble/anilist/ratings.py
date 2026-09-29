# providers/scrobble/anilist/ratings.py
# CrossWatch - Plex ratings to AniList scores
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from cw_platform.anime_mapping import AnimeMappingService
from cw_platform.anime_mapping.storage import normalize_release_tag
from cw_platform.config_base import CONFIG_BASE
from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id
from providers.sync._mod_ANILIST import OPS, ANILISTAuthError, ANILISTModule
from providers.sync.anilist._progress import aired_entries, resolve_target
from providers.sync.anilist._ratings import apply_scores, entry_scores, plan_scores, score_raw

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

RATING_LEVELS = ("movie", "show", "season")
_RECORD_LOCK = threading.Lock()


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="ANILIST-SINK")
            return
        except Exception:
            pass
    print(f"[ANILIST-SINK:{lvl}] {msg}")


def _to_int(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool) or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _record_path() -> Path:
    return (CONFIG_BASE() / ".cw_state" / "anilist_show_scores.json").resolve()


def _read_records() -> dict[str, Any]:
    try:
        data = json.loads(_record_path().read_text("utf-8") or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_records(data: Mapping[str, Any]) -> None:
    path = _record_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(json.dumps(dict(data), ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception as exc:
        _log(f"show score record not saved: {exc}", "WARNING")


def _show_key(instance: str, show_ids: Mapping[str, Any]) -> str:
    for provider in ("tvdb", "tmdb"):
        value = str(show_ids.get(provider) or "").strip()
        if value:
            return f"{instance}|{provider}:{value}"
    return ""


def _mapping_ready(cfg: Mapping[str, Any]) -> bool:
    block = cfg.get("anime_mapping") if isinstance(cfg.get("anime_mapping"), Mapping) else {}
    if not (block or {}).get("enabled"):
        return False
    try:
        return bool(AnimeMappingService(cfg).ready())
    except Exception:
        return False


def _targets(cfg: Mapping[str, Any], level: str, md: Mapping[str, Any], ids: Mapping[str, Any], show_ids: Mapping[str, Any]) -> list[int]:
    block = cfg.get("anime_mapping") if isinstance(cfg.get("anime_mapping"), Mapping) else {}
    tag = normalize_release_tag((block or {}).get("release_tag") or "v3")
    if level == "movie":
        hit = resolve_target(cfg, {"type": "movie", "ids": dict(ids)})
        return [hit[0]] if hit else []
    if level == "season":
        season = _to_int(md.get("index") if md.get("index") is not None else md.get("season"))
        if season is None:
            return []
        return aired_entries(tag, show_ids, season)
    return aired_entries(tag, show_ids, None)


def send_plex_rating(
    cfg: Mapping[str, Any],
    instance: Any,
    media_type: str,
    md: Mapping[str, Any],
    ids: Mapping[str, Any],
    show_ids: Mapping[str, Any] | None,
    rating: float | None,
) -> dict[str, Any]:
    level = str(media_type or "").strip().lower()
    if level not in RATING_LEVELS:
        return {"ok": True, "skipped": True, "reason": f"{level or 'unknown'}_not_supported"}
    inst = normalize_instance_id(instance)
    view = build_provider_config_view(dict(cfg or {}), "anilist", inst)
    if not OPS.is_configured(view):
        return {"ok": False, "error": "not_configured"}
    if not _mapping_ready(cfg):
        return {"ok": False, "error": "anime_mapping_unavailable"}
    sids = dict(show_ids or {}) if level != "movie" else {}
    if level == "show" and not sids:
        sids = dict(ids or {})
    targets = _targets(cfg, level, md, ids, sids)
    if not targets:
        return {"ok": True, "skipped": True, "reason": "not_mapped"}
    raw = score_raw(rating)
    key = _show_key(inst, sids) if level == "show" else ""
    title = str(md.get("parentTitle") if level == "season" else md.get("title") or "").strip() or "?"
    try:
        client = ANILISTModule(view).client
        with _RECORD_LOCK:
            records = _read_records() if key else {}
            listed = entry_scores(client, targets)
            writes, owned = plan_scores(level, targets, listed, raw, records.get(key))
            done = apply_scores(client, writes)
            if key:
                if owned:
                    records[key] = {"entries": owned, "score": raw}
                else:
                    records.pop(key, None)
                _write_records(records)
    except ANILISTAuthError:
        _log("AniList rejected the token; reconnect AniList", "ERROR")
        return {"ok": False, "error": "unauthorized"}
    except Exception as exc:
        _log(f"rating update failed level={level} err={exc}", "ERROR")
        return {"ok": False, "error": str(exc)}
    if not listed:
        return {"ok": True, "skipped": True, "reason": "not_on_list", "entries": targets}
    _log(f"{level} rating '{title}' score={raw} -> anilist {sorted(done) or 'no change'} "
         f"(on list {len(listed)}/{len(targets)})", "INFO")
    return {"ok": True, "level": level, "score_raw": raw, "entries": targets, "on_list": sorted(listed), "written": done}


__all__ = ["RATING_LEVELS", "send_plex_rating"]
