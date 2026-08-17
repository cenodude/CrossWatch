# providers/scrobble/_auto_remove_watchlist.py
# CrossWatch - Auto-remove from Watchlist on Scrobble Completion
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

from cw_platform.local_db.ttl_dedupe import once_per_ttl

_TTL_SECONDS = 120


def _norm_ids(ids: dict[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    if not isinstance(ids, dict):
        return out
    for k in ("tmdb", "imdb", "tvdb", "trakt", "slug", "jellyfin", "emby"):
        v = ids.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if k == "imdb" and not s.startswith("tt"):
            s = f"tt{s}" if (s.isdigit() or s) else s
        out[k] = s
    return out


def _dedupe_key(ids: dict[str, str], media_type: str | None, scope: str | None = None) -> str:
    parts = [str(scope or "").strip(), media_type or ""]
    for k in ("tmdb", "imdb", "tvdb", "trakt", "slug", "jellyfin", "emby"):
        if ids.get(k):
            parts.append(f"{k}:{ids[k]}")
    return "|".join(parts)


def _once_per_ttl(key: str) -> bool:
    return once_per_ttl(None, "watchlist_auto_remove", key, ttl_seconds=_TTL_SECONDS)


def _log(msg: str, level: str = "INFO") -> None:
    try:
        from crosswatch import _append_log

        _append_log("TRAKT", f"{level} [WL-AUTO] {msg}")
        if level.upper() != "DEBUG":
            return
    except Exception:
        pass
    if level.upper() == "DEBUG":
        print(f"[WL-AUTO] {level} {msg}")


def _cfg_delete_enabled(cfg: dict[str, Any], media_type: str) -> bool:
    s = cfg.get("scrobble") or {}
    if not s.get("delete_plex"):
        return False
    types = s.get("delete_plex_types") or []
    if isinstance(types, list):
        return (media_type in types) or (media_type.rstrip("s") + "s" in types)
    if isinstance(types, str):
        return media_type in types
    return False


def remove_across_providers_by_ids(
    ids: dict[str, Any] | None,
    media_type: str | None = None,
    scope: str | None = None,
) -> dict[str, Any]:
    norm = _norm_ids(ids)
    if not norm:
        _log("auto-remove skipped: no usable IDs in payload", "DEBUG")
        return {"ok": False, "skipped": "no-ids"}
    dkey = _dedupe_key(norm, media_type, scope=scope)
    if not _once_per_ttl(dkey):
        _log(f"auto-remove deduped (TTL) for {dkey}", "DEBUG")
        return {"ok": True, "skipped": "ttl"}
    try:
        import api.watchlistAPI as WLAPI

        res = WLAPI.remove_across_providers_by_ids(norm, media_type or "", origin=scope)
        ok = bool(res.get("ok")) if isinstance(res, dict) else bool(res)
        if ok:
            _log(f"auto-remove OK ids={norm} media={media_type}")
        else:
            _log(f"auto-remove NOOP ids={norm} media={media_type} → {res}", "DEBUG")
        return res if isinstance(res, dict) else {"ok": ok}
    except Exception as e:
        _log(f"auto-remove failed via _watchlistAPI: {e}", "WARN")
        try:
            from cw_platform.config_base import CONFIG, load_config
            from cw_platform.orchestrator._state_store import StateStore
            from services.watchlist import delete_watchlist_batch

            cfg = load_config()
            st = StateStore(CONFIG).load_state_features({"watchlist"}) or {}
            keys: list[str] = []
            for k in ("tmdb", "imdb", "tvdb", "trakt"):
                v = norm.get(k)
                if v:
                    keys.append(f"{k}:{v}")
            keys = list(dict.fromkeys(keys))
            if not keys:
                return {"ok": False, "error": "no-keys"}
            allowed = None
            if scope:
                from cw_platform.access_policy import origin_owner_instances

                prov_raw, _, inst_raw = str(scope).partition(":")
                if str(prov_raw or "").strip():
                    allowed = origin_owner_instances(cfg, prov_raw, inst_raw)
            res2 = delete_watchlist_batch(keys=keys, prov="ALL", state=st, cfg=cfg, allowed_instances=allowed) or {}
            ok2 = bool(res2.get("ok"))
            if ok2:
                _log(f"fallback delete_watchlist_batch OK ids={norm}")
            else:
                _log(f"fallback delete_watchlist_batch NOOP ids={norm} → {res2}", "DEBUG")
            return res2
        except Exception as e2:
            _log(f"fallback failed: {e2}", "WARN")
            return {"ok": False, "error": str(e2), "ids": norm, "media_type": media_type}


def _extract_evt(evt: Any) -> dict[str, Any]:
    if isinstance(evt, dict):
        return evt
    out: dict[str, Any] = {}
    try:
        mt = getattr(evt, "media_type", None)
        if mt is not None:
            out["media_type"] = mt
    except Exception:
        pass
    try:
        pr = getattr(evt, "progress", None)
        if pr is not None:
            out["progress"] = pr
    except Exception:
        pass
    try:
        ids = getattr(evt, "ids", None)
        if isinstance(ids, dict):
            out["ids"] = ids
    except Exception:
        pass
    return out


def auto_remove_if_config_allows(
    evt: Any,
    cfg: dict[str, Any] | None = None,
    scope: str | None = None,
) -> dict[str, Any] | None:
    try:
        if cfg is None:
            from cw_platform.config_base import load_config as _load_cfg

            cfg = _load_cfg()
    except Exception:
        cfg = cfg or {}

    e = _extract_evt(evt)
    media_type = str((e.get("media_type") or "")).strip().lower()
    if media_type != "movie":
        _log(
            f"auto-remove skipped: media_type={media_type or 'unknown'} (only 'movie' allowed)",
            "DEBUG",
        )
        return None

    if not _cfg_delete_enabled(cfg or {}, "movie"):
        _log("auto-remove disabled by config for type=movie", "DEBUG")
        return None

    try:
        watched_at = float((((cfg.get("scrobble") or {}).get("trakt") or {}).get("watched_at")) or 90.0)
    except Exception:
        watched_at = 90.0

    try:
        prog = float(e.get("progress") or 0.0)
    except Exception:
        prog = 0.0

    if prog < watched_at:
        _log(f"auto-remove skipped due to progress {prog:.1f}% < {watched_at:.1f}%", "DEBUG")
        return None

    ids = e.get("ids") or {}
    if not isinstance(ids, dict) or not ids:
        _log("auto-remove skipped: event has no ids", "DEBUG")
        return None

    _log(f"auto-remove (WL-AUTO) executing for movie ids={ids}", "INFO")
    return remove_across_providers_by_ids(ids, "movie", scope=scope)


def remove_by_ids(ids: dict[str, Any] | None, media_type: str | None = None, scope: str | None = None) -> dict[str, Any]:
    mt = str(media_type or "").strip().lower()
    if mt != "movie":
        _log(
            f"remove_by_ids skipped: media_type={mt or 'unknown'} (only 'movie' allowed)",
            "DEBUG",
        )
        return {"ok": False, "skipped": True, "reason": "not_movie"}
    return remove_across_providers_by_ids(ids or {}, "movie", scope=scope)
