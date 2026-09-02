# providers/scrobble/anime_mapping.py
# CrossWatch - Watcher anime mapping helpers
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
import threading
import time
from typing import Any

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from cw_platform.anime_mapping.episodes import resolve_absolute
from cw_platform.anime_mapping.service import AnimeMappingService


WATCHER_ANIME_MAPPING_SINKS = {"crosswatch", "simkl"}
_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")
_CACHE_TTL_SECONDS = 1800.0
_CACHE_MAX = 512
_CACHE: dict[tuple[Any, ...], tuple[float, dict[str, str] | None, dict[str, Any] | None]] = {}
_CACHE_LOCK = threading.RLock()
_CACHE_MISS = object()
_CACHE_NOOP = object()


def _log(msg: str, level: str = "DEBUG") -> None:
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=level, module="WATCH-ANIME")
            return
        except Exception:
            pass


def sink_name_for_mapping(sink: Any) -> str:
    cls = sink.__class__
    name = str(getattr(cls, "__name__", "") or "").strip().lower()
    module = str(getattr(cls, "__module__", "") or "").strip().lower()
    if name == "simklsink" or ".simkl." in module:
        return "simkl"
    if name == "crosswatchsink" or ".crosswatch." in module:
        return "crosswatch"
    return ""


def _route_option_enabled(cfg: Mapping[str, Any], sink_name: str) -> bool:
    if sink_name not in WATCHER_ANIME_MAPPING_SINKS:
        return False
    sc = cfg.get("scrobble") if isinstance(cfg, Mapping) else {}
    watch = (sc or {}).get("watch") if isinstance(sc, Mapping) else {}
    route_options = (watch or {}).get("route_options") if isinstance(watch, Mapping) else {}
    route_watch = (route_options or {}).get("watch") if isinstance(route_options, Mapping) else {}
    if not (isinstance(route_watch, Mapping) and bool(route_watch.get("anime_mapping"))):
        return False
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    return bool(isinstance(block, Mapping) and block.get("enabled"))


def _clean_ids(value: Mapping[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, item in dict(value or {}).items():
        name = str(key or "").strip().lower()
        text = str(item or "").strip()
        if name and text:
            out[name] = text
    return out


def _event_show_ids(ev: Any) -> dict[str, str]:
    ids = _clean_ids(getattr(ev, "ids", {}) or {})
    out: dict[str, str] = {}
    for key in _ID_KEYS:
        show_key = f"{key}_show"
        if ids.get(show_key):
            out[key] = ids[show_key]
    return out


def _ids_desc(ids: Mapping[str, Any]) -> str:
    clean = _clean_ids(ids)
    for key in ("simkl", "mal", "anilist", "anidb", "kitsu", "tmdb", "tvdb", "imdb", "trakt"):
        if clean.get(key):
            return f"{key}:{clean[key]}"
    return "no ids"


def _event_item(ev: Any) -> dict[str, Any] | None:
    media_type = str(getattr(ev, "media_type", "") or "").strip().lower()
    if media_type == "movie":
        return {
            "type": "movie",
            "title": getattr(ev, "title", None),
            "year": getattr(ev, "year", None),
            "ids": {k: v for k, v in _clean_ids(getattr(ev, "ids", {}) or {}).items() if not k.endswith("_show") and not k.endswith("_episode")},
        }
    if media_type != "episode":
        return None
    show_ids = _event_show_ids(ev)
    if not show_ids:
        return None
    return {
        "type": "episode",
        "title": getattr(ev, "title", None),
        "series_title": getattr(ev, "title", None),
        "year": getattr(ev, "year", None),
        "season": getattr(ev, "season", None),
        "episode": getattr(ev, "number", None),
        "show_ids": show_ids,
    }


def _flatten_ids(item: Mapping[str, Any], ev: Any) -> dict[str, str]:
    ids = _clean_ids(getattr(ev, "ids", {}) or {})
    media_type = str(getattr(ev, "media_type", "") or "").strip().lower()
    if media_type == "episode":
        show_ids = _clean_ids(item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else {})
        for key, value in show_ids.items():
            if key in _ID_KEYS:
                ids[f"{key}_show"] = value
        return ids
    item_ids = _clean_ids(item.get("ids") if isinstance(item.get("ids"), Mapping) else {})
    for key, value in item_ids.items():
        if key in _ID_KEYS:
            ids[key] = value
    return ids


def _copy_anime_raw(item: Mapping[str, Any], ev: Any, sink_name: str) -> dict[str, Any]:
    raw = dict(getattr(ev, "raw", {}) or {})
    raw["_cw_watcher_anime_mapping"] = sink_name
    for key in ("_cw_anime_map", "_anime_absolute", "_simkl_episode_number", "simkl_bucket", "anime_type"):
        value = item.get(key)
        if value not in (None, ""):
            raw[key] = value
    return raw


def _cache_key(ev: Any, sink_name: str, cfg: Mapping[str, Any]) -> tuple[Any, ...]:
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    release_tag = str((block if isinstance(block, Mapping) else {}).get("release_tag") or "v3")
    ids = _clean_ids(getattr(ev, "ids", {}) or {})
    show_ids = tuple(sorted((key, value) for key, value in ids.items() if key.endswith("_show")))
    item_ids = tuple(sorted((key, value) for key, value in ids.items() if not key.endswith("_show") and not key.endswith("_episode")))
    return (
        release_tag,
        str(sink_name or "").strip().lower(),
        str(getattr(ev, "media_type", "") or "").strip().lower(),
        show_ids,
        item_ids,
        getattr(ev, "season", None),
        getattr(ev, "number", None),
    )


def _cache_get(key: tuple[Any, ...]) -> object | tuple[dict[str, str], dict[str, Any]]:
    with _CACHE_LOCK:
        row = _CACHE.get(key)
        if row is None:
            return _CACHE_MISS
        ts, ids, raw = row
        if time.time() - ts > _CACHE_TTL_SECONDS:
            _CACHE.pop(key, None)
            return _CACHE_MISS
        if ids is None:
            return _CACHE_NOOP
        return dict(ids), dict(raw or {})


def _cache_set(key: tuple[Any, ...], ids: Mapping[str, Any], raw: Mapping[str, Any]) -> None:
    with _CACHE_LOCK:
        if len(_CACHE) >= _CACHE_MAX:
            oldest = min(list(_CACHE), key=lambda k: _CACHE[k][0])
            _CACHE.pop(oldest, None)
        _CACHE[key] = (time.time(), _clean_ids(ids), dict(raw or {}))


def _cache_set_noop(key: tuple[Any, ...]) -> None:
    with _CACHE_LOCK:
        if len(_CACHE) >= _CACHE_MAX:
            oldest = min(list(_CACHE), key=lambda k: _CACHE[k][0])
            _CACHE.pop(oldest, None)
        _CACHE[key] = (time.time(), None, None)


def _apply_episode_mapping(item: dict[str, Any], cfg: Mapping[str, Any]) -> dict[str, Any]:
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    release_tag = str((block if isinstance(block, Mapping) else {}).get("release_tag") or "v3")
    show_ids = _clean_ids(item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else {})
    try:
        enriched = AnimeMappingService(cfg).enrich_ids(show_ids, media_type="show")
    except Exception as exc:
        _log(f"show id enrichment skipped: {exc.__class__.__name__}")
        enriched = {}
    enriched_ids = _clean_ids(enriched.get("ids") if isinstance(enriched.get("ids"), Mapping) else show_ids)
    if enriched_ids:
        item["show_ids"] = enriched_ids
    detail = enriched.get("detail") if isinstance(enriched, Mapping) else None
    if isinstance(detail, Mapping):
        item["detail"] = dict(detail)
    try:
        res = resolve_absolute(item, release_tag=release_tag)
    except Exception as exc:
        _log(f"episode absolute mapping skipped: {exc.__class__.__name__}")
        return item
    if res is None:
        return item
    show_ids = _clean_ids(item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else {})
    show_ids.setdefault(res.namespace, str(res.target_id))
    item["show_ids"] = show_ids
    item["_cw_anime_map"] = {
        "absolute": res.absolute,
        "namespace": res.namespace,
        "target_id": res.target_id,
        "entry": res.entry,
        "release_tag": release_tag,
    }
    item["_anime_absolute"] = res.absolute
    item["_simkl_episode_number"] = res.absolute
    item["simkl_bucket"] = "anime"
    return item


def maybe_enrich_event_for_sink(ev: Any, sink_name: str, cfg: Mapping[str, Any]) -> Any:
    sink = str(sink_name or "").strip().lower()
    if not _route_option_enabled(cfg, sink):
        return ev
    item = _event_item(ev)
    if item is None:
        return ev
    key = _cache_key(ev, sink, cfg)
    cached = _cache_get(key)
    if cached is _CACHE_NOOP:
        return ev
    if isinstance(cached, tuple):
        ids_cached, raw_cached = cached
        return replace(ev, ids=ids_cached, raw={**dict(getattr(ev, "raw", {}) or {}), **raw_cached})
    media_type = str(getattr(ev, "media_type", "") or "").strip().lower()
    try:
        if media_type == "episode":
            mapped = _apply_episode_mapping(dict(item), cfg)
        else:
            mapped = AnimeMappingService(cfg).enrich_item(item)
    except Exception as exc:
        _log(f"watcher anime mapping skipped: {exc.__class__.__name__}")
        return ev
    new_ids = _flatten_ids(mapped, ev)
    old_ids = _clean_ids(getattr(ev, "ids", {}) or {})
    has_meta = any(mapped.get(key) not in (None, "") for key in ("_cw_anime_map", "_anime_absolute", "_simkl_episode_number", "simkl_bucket", "anime_type"))
    if sink == "simkl" and media_type == "episode" and not has_meta:
        _cache_set_noop(key)
        return ev
    if new_ids == old_ids and not has_meta:
        _cache_set_noop(key)
        return ev
    raw = _copy_anime_raw(mapped, ev, sink)
    if new_ids == old_ids and raw == (getattr(ev, "raw", {}) or {}):
        _cache_set_noop(key)
        return ev
    amap = mapped.get("_cw_anime_map")
    if media_type == "episode" and isinstance(amap, Mapping):
        mapped_show_ids = mapped.get("show_ids")
        show_ids: Mapping[str, Any] = mapped_show_ids if isinstance(mapped_show_ids, Mapping) else {}
        season = getattr(ev, "season", None)
        number = getattr(ev, "number", None)
        _log(
            "mapped "
            f"{sink} episode {_ids_desc(show_ids)} "
            f"S{season}E{number} -> {amap.get('namespace')}:{amap.get('target_id')} "
            f"absolute={amap.get('absolute')}"
        )
    raw_delta = {k: v for k, v in raw.items() if (getattr(ev, "raw", {}) or {}).get(k) != v}
    _cache_set(key, new_ids, raw_delta)
    return replace(ev, ids=new_ids, raw=raw)
