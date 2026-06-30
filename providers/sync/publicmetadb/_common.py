# providers/sync/publicmetadb/_common.py
# PUBLICMETADB Module for common functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Mapping

from cw_platform.anime_mapping.service import mapped_or_default_media_type
from cw_platform.config_base import CONFIG_BASE
from cw_platform.metadata_cache import (
    merge_metadata_cache_payload,
    metadata_cache_path,
    prune_metadata_cache,
    read_metadata_cache,
    write_metadata_cache,
)

from .._log import log as cw_log

STATE_DIR = Path("/config/.cw_state")
STATE_DIR.mkdir(parents=True, exist_ok=True)


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _is_capture_mode() -> bool:
    v = str(os.getenv("CW_CAPTURE_MODE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def state_file(name: str) -> Path:
    scope = _pair_scope()
    safe = _safe_scope(scope) if scope else "unscoped"
    p = Path(name)
    if p.suffix:
        return STATE_DIR / f"{p.stem}.{safe}{p.suffix}"
    return STATE_DIR / f"{name}.{safe}"


def read_json(path: Path) -> dict[str, Any]:
    if _is_capture_mode():
        return {}
    try:
        data = json.loads(path.read_text("utf-8") or "{}")
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def write_json(path: Path, data: Mapping[str, Any], *, indent: int | None = 2, sort_keys: bool = True) -> None:
    if _is_capture_mode():
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(json.dumps(dict(data), ensure_ascii=False, indent=indent, sort_keys=sort_keys), "utf-8")
        os.replace(tmp, path)
    except Exception as e:
        cw_log("PUBLICMETADB", "state", "warn", "state_write_failed", path=str(path), error=str(e))


def cfg_section(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    if isinstance(cfg, dict) and isinstance(cfg.get("publicmetadb"), dict):
        return cfg["publicmetadb"]
    return {}


def as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        s = str(value).strip()
        return int(s) if s else None
    except Exception:
        return None


def media_type_for_item(item: Mapping[str, Any]) -> str:
    probe = item
    if not item.get("type") and item.get("media_type"):
        probe = {**dict(item), "type": item.get("media_type")}
    return "tv" if mapped_or_default_media_type(probe) == "show" else "movie"


def tmdb_id_for_item(item: Mapping[str, Any]) -> int | None:
    ids_obj = item.get("ids")
    ids: Mapping[str, Any] = ids_obj if isinstance(ids_obj, Mapping) else {}
    return as_int(ids.get("tmdb") or item.get("tmdb") or item.get("tmdb_id"))


def _tmdb_metadata_api_key(adapter: Any) -> str:
    cfg = getattr(adapter, "config", {}) or {}
    if not isinstance(cfg, Mapping):
        return ""
    tmdb_obj = cfg.get("tmdb")
    tmdb: Mapping[str, Any] = tmdb_obj if isinstance(tmdb_obj, Mapping) else {}
    metadata_obj = cfg.get("metadata")
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    return str(tmdb.get("api_key") or metadata.get("tmdb_api_key") or "").strip()


def _tmdb_metadata_locale(adapter: Any) -> str | None:
    cfg = getattr(adapter, "config", {}) or {}
    if not isinstance(cfg, Mapping):
        return None
    metadata_obj = cfg.get("metadata")
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    ui_obj = cfg.get("ui")
    ui: Mapping[str, Any] = ui_obj if isinstance(ui_obj, Mapping) else {}
    locale = str(metadata.get("locale") or ui.get("locale") or "").strip()
    return locale or None


def _metadata_cache_enabled(adapter: Any) -> bool:
    cfg = getattr(adapter, "config", {}) or {}
    metadata_obj = cfg.get("metadata") if isinstance(cfg, Mapping) else None
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    return bool(metadata.get("meta_cache_enable", True))


def _metadata_cache_ttl_seconds(adapter: Any) -> int:
    cfg = getattr(adapter, "config", {}) or {}
    metadata_obj = cfg.get("metadata") if isinstance(cfg, Mapping) else None
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    try:
        hours = int(metadata.get("ttl_hours", 720))
    except Exception:
        hours = 720
    return max(1, hours) * 3600


def _metadata_cache_max_mb(adapter: Any) -> int:
    cfg = getattr(adapter, "config", {}) or {}
    metadata_obj = cfg.get("metadata") if isinstance(cfg, Mapping) else None
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    try:
        return max(0, int(metadata.get("meta_cache_max_mb", 0)))
    except Exception:
        return 0


def _metadata_cache_root() -> Path:
    root = CONFIG_BASE() / "cache" / "meta"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _metadata_identity(item: Mapping[str, Any]) -> tuple[str, int] | None:
    typ = str(item.get("type") or "").strip().lower()
    if typ == "episode":
        show_ids_obj = item.get("show_ids")
        show_ids: Mapping[str, Any] = show_ids_obj if isinstance(show_ids_obj, Mapping) else {}
        tmdb = as_int(show_ids.get("tmdb")) or tmdb_id_for_item(item)
        return ("show", tmdb) if tmdb is not None else None
    tmdb = tmdb_id_for_item(item)
    if tmdb is None:
        return None
    return ("show" if typ in ("show", "series", "tv") else "movie", tmdb)


def _needs_metadata(item: Mapping[str, Any]) -> bool:
    typ = str(item.get("type") or "").strip().lower()
    title_key = "series_title" if typ == "episode" else "title"
    return not str(item.get(title_key) or "").strip() or item.get("year") in (None, "")


def _tmdb_metadata_provider(adapter: Any) -> Any:
    provider = getattr(adapter, "_publicmetadb_tmdb_metadata_provider", None)
    if provider is not None:
        return provider

    from providers.metadata._meta_TMDB import TmdbProvider

    cfg = getattr(adapter, "config", {}) or {}
    tmdb_obj = cfg.get("tmdb") if isinstance(cfg, Mapping) else None
    tmdb: Mapping[str, Any] = tmdb_obj if isinstance(tmdb_obj, Mapping) else {}
    metadata_obj = cfg.get("metadata") if isinstance(cfg, Mapping) else None
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    runtime_obj = cfg.get("runtime") if isinstance(cfg, Mapping) else None
    runtime: Mapping[str, Any] = runtime_obj if isinstance(runtime_obj, Mapping) else {}
    provider_cfg = {"tmdb": dict(tmdb), "metadata": dict(metadata), "runtime": dict(runtime)}

    def _load_cfg() -> dict[str, Any]:
        return provider_cfg

    provider = TmdbProvider(_load_cfg, lambda _data: None)
    setattr(adapter, "_publicmetadb_tmdb_metadata_provider", provider)
    return provider


def enrich_index_metadata(
    adapter: Any,
    items: Mapping[str, Mapping[str, Any]],
    *,
    feature: str,
) -> dict[str, dict[str, Any]]:
    """Fill sparse PublicMetaDB index rows from the TMDb metadata provider."""
    out = {str(key): dict(item) for key, item in (items or {}).items() if isinstance(item, Mapping)}
    candidates = [(key, item) for key, item in out.items() if _needs_metadata(item)]
    if not candidates:
        return out

    if not _tmdb_metadata_api_key(adapter):
        logged_obj = getattr(adapter, "_publicmetadb_metadata_unavailable_logged", set())
        logged = logged_obj if isinstance(logged_obj, set) else set()
        if feature not in logged:
            cw_log(
                "PUBLICMETADB",
                feature,
                "warn",
                "metadata_enrichment_unavailable",
                reason="missing_tmdb_metadata_api_key",
                missing=len(candidates),
            )
            logged.add(feature)
            setattr(adapter, "_publicmetadb_metadata_unavailable_logged", logged)
        return out

    cache_obj = getattr(adapter, "_publicmetadb_metadata_cache", None)
    cache: dict[tuple[str, int], Mapping[str, Any] | None]
    if isinstance(cache_obj, dict):
        cache = cache_obj
    else:
        cache = {}
        setattr(adapter, "_publicmetadb_metadata_cache", cache)

    enriched = 0
    unresolved = 0
    disk_hits = 0
    cache_written = False
    provider: Any | None = None
    provider_error: Exception | None = None
    locale = _tmdb_metadata_locale(adapter)
    disk_enabled = _metadata_cache_enabled(adapter)
    disk_root = _metadata_cache_root() if disk_enabled else None
    for key, item in candidates:
        identity = _metadata_identity(item)
        if identity is None:
            unresolved += 1
            continue
        entity, tmdb_id = identity
        if identity not in cache:
            resolved: Mapping[str, Any] | None = None
            disk_path: Path | None = None
            if disk_root is not None:
                disk_path = metadata_cache_path(disk_root, entity, tmdb_id, locale or "en-US")
                persisted = read_metadata_cache(disk_path, ttl_seconds=_metadata_cache_ttl_seconds(adapter))
                needs_title = not str(item.get("series_title" if str(item.get("type") or "").lower() == "episode" else "title") or "").strip()
                needs_year = item.get("year") in (None, "")
                persisted_value: dict[str, Any] | None = dict(persisted) if isinstance(persisted, Mapping) else None
                if persisted_value is not None:
                    persisted_satisfies = not needs_title or bool(str(persisted_value.get("title") or "").strip())
                    if persisted_satisfies and needs_year:
                        persisted_satisfies = "year" in persisted_value
                else:
                    persisted_satisfies = False
                if persisted_satisfies and persisted_value is not None:
                    resolved = persisted_value
                    disk_hits += 1

            if resolved is None and provider_error is None:
                try:
                    active_provider = provider
                    if active_provider is None:
                        active_provider = _tmdb_metadata_provider(adapter)
                        provider = active_provider
                    fetched = active_provider.fetch(
                        entity=entity,
                        ids={"tmdb": str(tmdb_id)},
                        locale=locale,
                        need={"title": True, "year": True},
                    )
                    resolved = fetched if isinstance(fetched, Mapping) and fetched else None
                    if resolved is not None and disk_path is not None:
                        previous = read_metadata_cache(disk_path, ttl_seconds=None)
                        payload = merge_metadata_cache_payload(previous, resolved)
                        payload["locale"] = locale or payload.get("locale") or None
                        if write_metadata_cache(disk_path, payload):
                            cache_written = True
                except Exception as exc:
                    provider_error = exc

            cache[identity] = resolved

        metadata = cache.get(identity)
        if not isinstance(metadata, Mapping):
            unresolved += 1
            continue

        merged = dict(item)
        title = str(metadata.get("title") or "").strip()
        title_key = "series_title" if str(item.get("type") or "").strip().lower() == "episode" else "title"
        changed = False
        if not str(merged.get(title_key) or "").strip() and title:
            merged[title_key] = title
            changed = True
        if merged.get("year") in (None, "") and metadata.get("year") not in (None, ""):
            merged["year"] = metadata.get("year")
            changed = True
        if changed:
            out[key] = merged
            enriched += 1

    if provider_error is not None:
        cw_log(
            "PUBLICMETADB",
            feature,
            "warn",
            "metadata_enrichment_unavailable",
            reason="tmdb_metadata_provider_unavailable",
            error_type=provider_error.__class__.__name__,
            unresolved=unresolved,
        )
    if cache_written and disk_root is not None:
        prune_metadata_cache(disk_root, max_mb=_metadata_cache_max_mb(adapter))

    cw_log(
        "PUBLICMETADB",
        feature,
        "debug",
        "metadata_enrichment_done",
        candidates=len(candidates),
        enriched=enriched,
        unresolved=unresolved,
        cached=len(cache),
        disk_hits=disk_hits,
    )
    return out
