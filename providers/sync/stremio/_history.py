# providers/sync/stremio/_history.py
# CrossWatch Stremio history functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import base64
import copy
import math
import zlib
from collections.abc import Iterable, Mapping
from typing import Any

import requests

from cw_platform.id_map import ID_KEYS, coalesce_ids, minimal as id_minimal
from providers.auth._auth_STREMIO import StremioAuthError
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import (
    canonical_item_key,
    datastore_get,
    datastore_put,
    default_record,
    episode_agrees,
    epoch_ms,
    imdb_ids_from_item,
    imdb_id,
    ids_from_stremio_record,
    is_capture_mode,
    iso_from_epoch_ms,
    item_from_episode,
    item_from_movie_record,
    library_records,
    native_record_ids,
    now_iso,
    now_ms,
    positive_int,
    season_int,
    read_cinemeta_entry,
    read_drop_summary,
    read_progress,
    record_read_drop,
    record_id,
    reset_read_drop_report,
    state_of,
    stremio_id_namespace,
    stremio_id_for_item,
    stremio_write_id_for_item,
    poster_url_from_item,
    tmdb_metadata_provider,
    update_cinemeta_entry,
    video_id_for_episode,
)
from providers.sync._log import log as cw_log

CINEMETA_BASE = "https://v3-cinemeta.strem.io"
CINEMETA_VIDEO_FIELDS = ("id", "season", "episode", "name", "title", "released", "thumbnail")
RECENT_EPISODE_MS = 14 * 24 * 60 * 60 * 1000


class EpisodeIndexUnavailable(ValueError):
    pass


def _bits_from_bytes(raw: bytes, length: int) -> list[bool]:
    return [bool(raw[i // 8] & (1 << (i % 8))) if i // 8 < len(raw) else False for i in range(max(0, length))]


def _bytes_from_bits(bits: list[bool]) -> bytes:
    raw = bytearray(max(0, math.ceil(len(bits) / 8)))
    for i, value in enumerate(bits):
        if value:
            raw[i // 8] |= 1 << (i % 8)
    return bytes(raw)


def _parse_serialized(value: str) -> tuple[str, int, bytes]:
    parts = str(value or "").split(":")
    if len(parts) < 3:
        raise ValueError("malformed_watched_bitfield")
    payload = parts[-1]
    anchor_length = int(parts[-2])
    anchor_id = ":".join(parts[:-2])
    if not anchor_id or anchor_length < 0:
        raise ValueError("malformed_watched_bitfield")
    return anchor_id, anchor_length, zlib.decompress(base64.b64decode(payload))


def watched_bits(value: Any, video_ids: list[str]) -> list[bool]:
    raw_value = str(value or "").strip()
    if not raw_value:
        return [False] * len(video_ids)
    anchor_id, anchor_length, packed = _parse_serialized(raw_value)
    try:
        anchor_index = video_ids.index(anchor_id)
    except ValueError:
        return [False] * len(video_ids)
    if anchor_index == anchor_length - 1:
        bits = _bits_from_bytes(packed, len(video_ids))
        return bits[: len(video_ids)] + [False] * max(0, len(video_ids) - len(bits))
    old_bits = _bits_from_bytes(packed, anchor_length)
    offset = (anchor_length - 1) - anchor_index
    return [old_bits[i + offset] if 0 <= i + offset < len(old_bits) else False for i in range(len(video_ids))]


def serialize_watched_bits(bits: list[bool], video_ids: list[str]) -> str:
    last = -1
    for idx, value in enumerate(bits):
        if value:
            last = idx
    if last < 0:
        return ""
    packed = zlib.compress(_bytes_from_bits(bits))
    return f"{video_ids[last]}:{last + 1}:{base64.b64encode(packed).decode('ascii')}"


def decode_watched_episodes(value: Any, video_ids: list[str]) -> set[str]:
    return {video_ids[i] for i, value in enumerate(watched_bits(value, video_ids)) if value}


def watched_anchor(value: Any) -> tuple[str, int] | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        anchor_id, anchor_length, _packed = _parse_serialized(raw)
    except Exception:
        return None
    return anchor_id, anchor_length


def show_id_from_video_id(value: Any) -> str:
    raw = str(value or "").strip()
    parts = raw.split(":")
    if len(parts) < 3:
        return ""
    season = season_int(parts[-2])
    episode = positive_int(parts[-1])
    if season is None or not episode:
        return ""
    return ":".join(parts[:-2]).strip()


def order_matches_anchor(value: Any, video_ids: list[str]) -> bool:
    anchor = watched_anchor(value)
    if anchor is None:
        return False
    anchor_id, anchor_length = anchor
    try:
        return video_ids.index(anchor_id) + 1 == anchor_length
    except ValueError:
        return False


def set_episode_watched_value(value: Any, video_ids: list[str], video_id: str, watched: bool) -> str:
    bits = watched_bits(value, video_ids)
    index = video_ids.index(video_id)
    bits[index] = bool(watched)
    return serialize_watched_bits(bits, video_ids)


def cinemeta_videos(adapter: Any, imdb: str, *, fresh: bool = False, strict: bool = False) -> list[Mapping[str, Any]]:
    cache = getattr(adapter, "_stremio_cinemeta_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(adapter, "_stremio_cinemeta_cache", cache)
    refreshed = getattr(adapter, "_stremio_cinemeta_refreshed", None)
    if not isinstance(refreshed, set):
        refreshed = set()
        setattr(adapter, "_stremio_cinemeta_refreshed", refreshed)
    if fresh and imdb in refreshed:
        return cache.get(imdb) or []
    if not fresh:
        if imdb in cache:
            return cache[imdb]
        stored = read_cinemeta_entry(imdb).get("videos")
        if isinstance(stored, list) and stored:
            cache[imdb] = stored
            return stored
    try:
        resp = requests.get(f"{CINEMETA_BASE}/meta/series/{imdb}.json", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        meta = data.get("meta") if isinstance(data, Mapping) else None
        videos = meta.get("videos") if isinstance(meta, Mapping) else None
        if not isinstance(videos, list):
            raise ValueError("cinemeta_invalid")
    except Exception as exc:
        if fresh and not strict and cache.get(imdb):
            return cache[imdb]
        status = int(getattr(getattr(exc, "response", None), "status_code", 0) or 0)
        if status == 404 or str(exc) == "cinemeta_invalid":
            raise
        raise EpisodeIndexUnavailable("cinemeta_episode_index_unavailable") from exc
    rows: list[Mapping[str, Any]] = [{key: row[key] for key in CINEMETA_VIDEO_FIELDS if key in row}
                                     for row in videos if isinstance(row, Mapping) and str(row.get("id") or "").strip()]
    if not rows and fresh and cache.get(imdb):
        if strict:
            raise ValueError("cinemeta_episode_index_unavailable")
        return cache[imdb]
    cache[imdb] = rows
    if rows:
        refreshed.add(imdb)
        update_cinemeta_entry(imdb, videos=rows)
    return rows


def cinemeta_refreshed(adapter: Any, imdb: str) -> bool:
    return imdb in (getattr(adapter, "_stremio_cinemeta_refreshed", None) or ())


def _cinemeta_videos_for(adapter: Any, imdb: str, watched_value: Any, *, for_write: bool = False) -> list[Mapping[str, Any]]:
    if for_write:
        return cinemeta_videos(adapter, imdb, fresh=True, strict=True)
    videos = cinemeta_videos(adapter, imdb)
    watched = str(watched_value or "").strip()
    if not watched or read_cinemeta_entry(imdb).get("checked_watched") == watched:
        return videos
    try:
        videos = cinemeta_videos(adapter, imdb, fresh=True, strict=True)
    except EpisodeIndexUnavailable:
        raise
    except Exception:
        return videos
    if cinemeta_refreshed(adapter, imdb):
        update_cinemeta_entry(imdb, checked_watched=watched)
    return videos


def _tmdb_season_rows(fetch: Any, tmdb_id: str, season_no: int, prefix: str) -> list[Mapping[str, Any]]:
    base = "https://api.themoviedb.org/3"
    try:
        season_data = fetch(f"{base}/tv/{tmdb_id}/season/{season_no}", {"language": "en-US"})
    except Exception:
        return []
    episodes = season_data.get("episodes") if isinstance(season_data, Mapping) else None
    if not isinstance(episodes, list):
        return []
    rows: list[Mapping[str, Any]] = []
    for episode_row in episodes:
        if not isinstance(episode_row, Mapping):
            continue
        episode_no = positive_int(episode_row.get("episode_number"))
        if not episode_no:
            continue
        rows.append(
            {
                "id": f"{prefix}:{season_no}:{episode_no}",
                "season": season_no,
                "episode": episode_no,
                "title": str(episode_row.get("name") or "").strip(),
                "released": str(episode_row.get("air_date") or "").strip(),
                "thumbnail": "",
            }
        )
    return rows


def tmdb_native_video_orders(adapter: Any, tmdb_id: str, *, video_prefix: str | None = None, fresh: bool = False) -> list[list[Mapping[str, Any]]]:
    cache = getattr(adapter, "_stremio_tmdb_video_cache", None)
    if not isinstance(cache, dict):
        cache = {}
        setattr(adapter, "_stremio_tmdb_video_cache", cache)
    refreshed = getattr(adapter, "_stremio_tmdb_video_refreshed", None)
    if not isinstance(refreshed, set):
        refreshed = set()
        setattr(adapter, "_stremio_tmdb_video_refreshed", refreshed)
    prefix = str(video_prefix or f"tmdb:{tmdb_id}").strip()
    cache_key = f"{tmdb_id}|{prefix}"
    if cache_key in cache and (not fresh or cache_key in refreshed):
        return cache[cache_key]

    provider = tmdb_metadata_provider(adapter)
    if provider is None:
        raise ValueError("tmdb_metadata_provider_missing")
    get = getattr(provider, "_get", None)
    if not callable(get):
        raise ValueError("tmdb_metadata_provider_invalid")
    unavailable = False

    def fetch(url: str, params: Mapping[str, Any]) -> Any:
        nonlocal unavailable
        try:
            return get(url, params, fresh=True) if fresh else get(url, params)
        except Exception as exc:
            if int(getattr(getattr(exc, "response", None), "status_code", 0) or 0) != 404:
                unavailable = True
            raise

    try:
        detail = provider.fetch(entity="tv", ids={"tmdb": str(tmdb_id)}, need={"ids": True}, fresh=fresh, strict=True)
    except Exception as exc:
        raise EpisodeIndexUnavailable("tmdb_episode_index_unavailable") from exc
    meta = detail.get("detail") if isinstance(detail, Mapping) else None
    meta_map = meta if isinstance(meta, Mapping) else {}
    seasons = positive_int(meta_map.get("number_of_seasons"))
    if not seasons:
        raise ValueError("tmdb_show_seasons_missing")

    aired: list[Mapping[str, Any]] = []
    for season_no in range(1, seasons + 1):
        aired.extend(_tmdb_season_rows(fetch, tmdb_id, season_no, prefix))
    if not aired:
        raise (EpisodeIndexUnavailable if unavailable else ValueError)("tmdb_episode_index_unavailable")

    specials = _tmdb_season_rows(fetch, tmdb_id, 0, prefix)
    if fresh and unavailable:
        raise EpisodeIndexUnavailable("tmdb_episode_index_unavailable")
    orders = [aired] if not specials else [aired, specials + aired, aired + specials]
    cache[cache_key] = orders
    if fresh:
        refreshed.add(cache_key)
    return orders


def _native_orders_for(adapter: Any, tmdb: str, show_id: str, watched_value: Any) -> list[list[Mapping[str, Any]]]:
    orders = tmdb_native_video_orders(adapter, tmdb, video_prefix=show_id)
    watched = str(watched_value or "").strip()
    if not watched or any(order_matches_anchor(watched, [str(v.get("id") or "").strip() for v in order]) for order in orders):
        return orders
    key = f"tmdb_{tmdb}"
    if read_cinemeta_entry(key).get("checked_watched") == watched:
        return orders
    try:
        orders = tmdb_native_video_orders(adapter, tmdb, video_prefix=show_id, fresh=True)
    except EpisodeIndexUnavailable:
        raise
    except ValueError:
        return orders
    update_cinemeta_entry(key, checked_watched=watched)
    return orders


def tmdb_native_videos(adapter: Any, tmdb_id: str, *, video_prefix: str | None = None) -> list[Mapping[str, Any]]:
    return tmdb_native_video_orders(adapter, tmdb_id, video_prefix=video_prefix)[0]


_TMDB_RESOLVABLE_ID_KEYS = ("tvdb", "imdb")


def tmdb_id_for_native_ids(adapter: Any, ids: Mapping[str, Any], title: Any = None) -> str:
    lookup = {key: str(ids.get(key) or "").strip() for key in _TMDB_RESOLVABLE_ID_KEYS if str(ids.get(key) or "").strip()}
    if not lookup:
        return ""
    name = str(title or "").strip()
    if name:
        lookup["title"] = name
    provider = tmdb_metadata_provider(adapter)
    if provider is None:
        raise ValueError("tmdb_metadata_provider_missing")
    try:
        detail = provider.fetch(entity="tv", ids=lookup, need={"ids": True})
    except Exception:
        detail = None
    resolved = detail.get("ids") if isinstance(detail, Mapping) else None
    return str((resolved or {}).get("tmdb") or "").strip() if isinstance(resolved, Mapping) else ""


def _video_orders_for_show_id(adapter: Any, show_id: str, record: Mapping[str, Any], watched_value: Any = None, *, for_write: bool = False) -> tuple[list[list[Mapping[str, Any]]], bool]:
    imdb = imdb_id(show_id)
    if imdb:
        return [_cinemeta_videos_for(adapter, imdb, watched_value, for_write=for_write)], False
    ids = ids_from_stremio_record(record) if show_id == record_id(record) else ids_from_stremio_record({"_id": show_id, "type": record.get("type")})
    tmdb = str(ids.get("tmdb") or "").strip()
    if not tmdb:
        tmdb = tmdb_id_for_native_ids(adapter, ids, record.get("name"))
    if tmdb:
        return _native_orders_for(adapter, tmdb, show_id, watched_value), True
    raise ValueError("native_episode_namespace_unsupported")


def video_orders_for_series_record(adapter: Any, record: Mapping[str, Any], watched_value: Any = None, *, for_write: bool = False) -> tuple[list[list[Mapping[str, Any]]], bool]:
    anchor = watched_anchor(watched_value)
    if anchor is not None:
        anchor_show_id = show_id_from_video_id(anchor[0])
        if anchor_show_id:
            return _video_orders_for_show_id(adapter, anchor_show_id, record, watched_value, for_write=for_write)
    return _video_orders_for_show_id(adapter, record_id(record), record, watched_value, for_write=for_write)


def videos_for_series_record(adapter: Any, record: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    orders, _reconstructed = video_orders_for_series_record(adapter, record)
    return orders[0]


def select_video_order(orders: list[list[Mapping[str, Any]]], watched_value: Any, *, require_anchor_match: bool) -> list[Mapping[str, Any]]:
    for order in orders:
        video_ids = [str(v.get("id") or "").strip() for v in order]
        if order_matches_anchor(watched_value, video_ids):
            return order
    if require_anchor_match:
        raise ValueError("native_episode_order_unverified")
    return orders[0]


def parse_movie_history_record(record: Mapping[str, Any]) -> dict[str, Any] | None:
    item = item_from_movie_record(record)
    if not item:
        return None
    state = state_of(record)
    watched = (positive_int(state.get("timesWatched")) or 0) > 0 or (positive_int(state.get("flaggedWatched")) or 0) > 0
    item["watched"] = watched
    watched_at = iso_from_epoch_ms(state.get("lastWatched")) or iso_from_epoch_ms(record.get("_mtime"))
    if watched_at:
        item["watched_at"] = watched_at
    return item


def _image_url(detail: Mapping[str, Any], kind: str) -> str:
    images = detail.get("images")
    image_map = images if isinstance(images, Mapping) else {}
    rows = image_map.get(kind)
    if not isinstance(rows, list):
        return ""
    for row in rows:
        if isinstance(row, Mapping):
            url = str(row.get("url") or "").strip()
            if url:
                return url
    return ""


def _metadata_enriched(adapter: Any, item: Mapping[str, Any], typ: str) -> dict[str, Any]:
    out = dict(item)
    stremio_id = stremio_id_for_item(out)
    if imdb_id(stremio_id):
        if not str(out.get("poster") or out.get("poster_url") or "").strip():
            out["poster"] = poster_url_from_item(out, stremio_id)
        return out
    provider = tmdb_metadata_provider(adapter)
    if provider is None:
        return out
    show_ids_raw = out.get("show_ids")
    show_ids: Mapping[str, Any] = show_ids_raw if isinstance(show_ids_raw, Mapping) else {}
    source: Mapping[str, Any] = show_ids if typ in {"episode", "episodes"} and show_ids else imdb_ids_from_item(out)
    lookup = {k: str(source.get(k) or "").strip() for k in ("tmdb", "imdb", "tvdb") if str(source.get(k) or "").strip()}
    title = str(out.get("series_title") or out.get("show_title") or out.get("title") or "").strip()
    if title:
        lookup["title"] = title
    if out.get("year"):
        lookup["year"] = str(out.get("year"))
    if not lookup:
        return out
    try:
        detail = provider.fetch(entity="tv" if typ in {"episode", "episodes"} else "movie", ids=lookup, need={"poster": True, "backdrop": False, "ids": True})
    except Exception:
        return out
    if not isinstance(detail, Mapping):
        return out
    ids_raw = detail.get("ids")
    ids: Mapping[str, Any] = ids_raw if isinstance(ids_raw, Mapping) else {}
    imdb = imdb_id(ids.get("imdb"))
    if imdb:
        out_ids_raw = out.get("ids")
        out_ids: Mapping[str, Any] = out_ids_raw if isinstance(out_ids_raw, Mapping) else {}
        merged: dict[str, Any] = dict(out_ids)
        merged["imdb"] = imdb
        out["ids"] = merged
        if typ in {"episode", "episodes"}:
            merged_show = dict(show_ids)
            merged_show["imdb"] = imdb
            out["show_ids"] = merged_show
    if not str(out.get("poster") or out.get("poster_url") or "").strip():
        poster = poster_url_from_item(out, imdb) or _image_url(detail, "poster")
        if poster:
            out["poster"] = poster
    return out


_RECONSTRUCTION_FAILURES = {
    "tmdb_metadata_provider_missing",
    "tmdb_metadata_provider_invalid",
    "tmdb_show_seasons_missing",
    "tmdb_episode_index_unavailable",
}
_TMDB_KEY_REASONS = {"bare_numeric_id_unverified", "native_episode_index_unavailable", "native_episode_order_unverified"}
_UNKNOWN_ID_REASONS = {"unsupported_stremio_id", "native_episode_namespace_unsupported", "bare_numeric_id_mismatch"}
_UNKNOWN_EPISODE_WATCHED_AT = "1970-01-01T00:00:01Z"
_STORED_EPISODE_ESTIMATE_SOURCE = "stored_episode_estimate"


def _series_watched_at(record: Mapping[str, Any]) -> tuple[str | None, str]:
    state = state_of(record)
    watched_at = iso_from_epoch_ms(state.get("lastWatched"))
    if watched_at:
        return watched_at, "show_last_watched"
    watched_at = iso_from_epoch_ms(record.get("_mtime"))
    if watched_at:
        return watched_at, "show_record_mtime"
    return None, ""


def _baseline_items(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    return {str(k): dict(v) for k, v in value.items() if isinstance(v, Mapping)}


def _load_stremio_history_baseline(adapter: Any) -> dict[str, dict[str, Any]]:
    injected = getattr(adapter, "_stremio_history_baseline", None)
    if isinstance(injected, Mapping):
        return _baseline_items(injected)
    if is_capture_mode():
        return {}
    if not isinstance(getattr(adapter, "config", None), Mapping):
        return {}
    try:
        from pathlib import Path

        from cw_platform.config_base import CONFIG_BASE
        from cw_platform.orchestrator._state_store import StateStore
        from cw_platform.provider_instances import normalize_instance_id

        state = StateStore(Path(CONFIG_BASE()), pair_scope=adapter.config.get("_cw_pair_scope")).load_state_features({"history"}) or {}
        providers = state.get("providers") if isinstance(state, Mapping) else None
        providers = providers if isinstance(providers, Mapping) else {}
        stremio = providers.get("STREMIO") or providers.get("stremio")
        if not isinstance(stremio, Mapping):
            return {}

        instance_id = normalize_instance_id(getattr(adapter, "instance_id", "default"))
        nodes: list[Mapping[str, Any]] = []
        if instance_id != "default":
            instances = stremio.get("instances")
            if isinstance(instances, Mapping):
                node = instances.get(instance_id) or instances.get(str(instance_id))
                if not isinstance(node, Mapping):
                    for raw_inst, raw_node in instances.items():
                        if normalize_instance_id(raw_inst) == instance_id and isinstance(raw_node, Mapping):
                            node = raw_node
                            break
                if isinstance(node, Mapping):
                    nodes.append(node)
        if instance_id == "default":
            nodes.append(stremio)

        for node in nodes:
            history = node.get("history") if isinstance(node, Mapping) else None
            history = history if isinstance(history, Mapping) else {}
            baseline = history.get("baseline") if isinstance(history, Mapping) else None
            baseline = baseline if isinstance(baseline, Mapping) else {}
            items = baseline.get("items") if isinstance(baseline, Mapping) else None
            found = _baseline_items(items)
            if found:
                return found
    except Exception:
        return {}
    return {}


def _episode_base_key(key: Any) -> str:
    return str(key or "").split("@", 1)[0].strip()


def _episode_fragment(item: Mapping[str, Any]) -> str | None:
    season = season_int(item.get("season"))
    episode = positive_int(item.get("episode"))
    if season is None or not episode:
        return None
    return f"#s{season:02d}e{episode:02d}"


def _episode_date_aliases(item: Mapping[str, Any], key: Any = None) -> set[str]:
    aliases = {str(alias).strip().lower() for alias in (key, _episode_base_key(key)) if str(alias or "").strip()}
    try:
        canonical = canonical_item_key(item)
        if canonical:
            aliases.add(canonical.lower())
    except Exception:
        pass

    fragment = _episode_fragment(item)
    if not fragment:
        return aliases
    for source in (item.get("show_ids"), item.get("ids")):
        if not isinstance(source, Mapping):
            continue
        ids = coalesce_ids(source)
        for id_key in ID_KEYS:
            value = ids.get(id_key)
            if value:
                aliases.add(f"{id_key}:{value}{fragment}".lower())
    return aliases


def _remember_previous_date(out: dict[str, str], alias: str, watched_at: str) -> None:
    alias = str(alias or "").strip().lower()
    if not alias:
        return
    existing = out.get(alias)
    if existing is None or (epoch_ms(watched_at) or 0) < (epoch_ms(existing) or 0):
        out[alias] = watched_at


def _previous_episode_watched_dates(previous: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, item in (previous or {}).items():
        if not isinstance(item, Mapping):
            continue
        if str(item.get("type") or "").strip().lower() not in {"episode", "episodes"}:
            continue
        if item.get("watched") is False:
            continue
        watched_at = iso_from_epoch_ms(item.get("watched_at"))
        if not watched_at or watched_at == _UNKNOWN_EPISODE_WATCHED_AT:
            continue
        for alias in _episode_date_aliases(item, key):
            _remember_previous_date(out, alias, watched_at)
    return out


def _apply_episode_watched_at(item: dict[str, Any], key: str, record: Mapping[str, Any], previous_dates: Mapping[str, str]) -> None:
    previous = next((previous_dates.get(alias) for alias in _episode_date_aliases(item, key) if previous_dates.get(alias)), None)
    if previous:
        item["watched_at"] = previous
        item["_stremio_watched_at_source"] = _STORED_EPISODE_ESTIMATE_SOURCE
        return

    show_ts, ts_source = _series_watched_at(record)
    if show_ts:
        item["watched_at"] = show_ts
        item["_stremio_watched_at_source"] = ts_source
    else:
        item["watched_at"] = _UNKNOWN_EPISODE_WATCHED_AT
        item["_stremio_watched_at_fallback"] = "unknown_episode_watch_time"


def tmdb_ids_for_record(adapter: Any, record: Mapping[str, Any], entity: str) -> dict[str, str]:
    imdb = imdb_id(record_id(record))
    if not imdb or getattr(adapter, "_stremio_tmdb_lookup_failed", False):
        return {}
    find = getattr(tmdb_metadata_provider(adapter), "find_imdb", None)
    if not callable(find):
        return {}
    try:
        found = find(imdb, entity)
    except Exception:
        setattr(adapter, "_stremio_tmdb_lookup_failed", True)
        return {}
    return {str(k): str(v) for k, v in found.items() if str(v or "").strip()} if isinstance(found, Mapping) else {}


def _recently_released(video: Mapping[str, Any]) -> bool:
    released = epoch_ms(video.get("released"))
    return released is not None and now_ms() - released < RECENT_EPISODE_MS


def verified_video_ids(adapter: Any, tmdb_id: str, videos: list[Mapping[str, Any]], watched_ids: set[str], imdb: str = "") -> set[str]:
    fetch = getattr(tmdb_metadata_provider(adapter), "_get", None)
    if not tmdb_id or not callable(fetch):
        return set()
    unavailable = False

    def season_data(url: str, params: Mapping[str, Any], options: Mapping[str, bool]) -> Any:
        nonlocal unavailable
        try:
            return fetch(url, params, quiet_404=True, **options)
        except Exception as exc:
            if int(getattr(getattr(exc, "response", None), "status_code", 0) or 0) != 404:
                unavailable = True
            raise

    def rows_for(season_no: int, **options: bool) -> dict[int | None, Mapping[str, Any]]:
        rows = _tmdb_season_rows(lambda url, params: season_data(url, params, options), tmdb_id, season_no, "")
        return {positive_int(row.get("episode")): row for row in rows}

    stored = read_cinemeta_entry(imdb).get("rechecked") if imdb else None
    rechecked = {str(video_id) for video_id in stored} if isinstance(stored, list) else set()
    seasons: dict[int, dict[int | None, Mapping[str, Any]]] = {}
    refreshed: set[int] = set()
    verified: set[str] = set()
    failed: set[str] = set()
    for video in videos:
        video_id = str(video.get("id") or "").strip()
        season = season_int(video.get("season"))
        episode = positive_int(video.get("episode"))
        if video_id not in watched_ids or season is None or not episode:
            continue
        if season not in seasons:
            seasons[season] = rows_for(season)
        row = seasons[season].get(episode)
        agrees = bool(row and episode_agrees(video, row, seasons[season].values()))
        if not agrees and season not in refreshed and (video_id not in rechecked or _recently_released(video)):
            refreshed.add(season)
            seasons[season] = rows_for(season, fresh=True) or seasons[season]
            row = seasons[season].get(episode)
            agrees = bool(row and episode_agrees(video, row, seasons[season].values()))
        (verified if agrees else failed).add(video_id)
    known = (rechecked | failed) - verified
    if imdb and not unavailable and known != rechecked:
        update_cinemeta_entry(imdb, rechecked=sorted(known))
    return verified


def _requires_for(record: Mapping[str, Any], reason: str | None) -> str | None:
    reason_s = str(reason or "")
    if reason_s in _TMDB_KEY_REASONS and stremio_id_namespace(record_id(record)) in {"tmdb", "tmdb_bare"}:
        return "tmdb_api_key"
    if reason_s in _UNKNOWN_ID_REASONS:
        return "known_native_id"
    return None


def build_index(adapter: Any, **kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    previous_dates = _previous_episode_watched_dates(_load_stremio_history_baseline(adapter))
    reset_read_drop_report(adapter, "history")
    setattr(adapter, "_stremio_tmdb_lookup_failed", False)
    unresolved_titles = 0
    held_episodes = 0
    records = library_records(adapter, incremental=bool(kwargs.get("incremental")))
    tick = read_progress(adapter, "history", len(records))
    for done, record in enumerate(records):
        tick(done)
        typ = str(record.get("type") or "").strip().lower()
        if typ == "movie":
            state = state_of(record)
            watched = (positive_int(state.get("timesWatched")) or 0) > 0 or (positive_int(state.get("flaggedWatched")) or 0) > 0
            ids, drop_reason = native_record_ids(adapter, record)
            if not ids:
                if watched:
                    record_read_drop(adapter, "history", record, drop_reason or "unsupported_stremio_id", requires_id=_requires_for(record, drop_reason))
                continue
            item = parse_movie_history_record(record)
            if not item or not item.get("watched"):
                continue
            item = _metadata_enriched(adapter, item, "movie")
            native = tmdb_ids_for_record(adapter, record, "movie")
            if native:
                item["ids"] = {**item["ids"], **native}
            elif imdb_id(record_id(record)):
                unresolved_titles += 1
            out[canonical_item_key(item)] = item
        elif typ in {"series", "show"}:
            show_id = record_id(record)
            record_state = state_of(record)
            watched_value = record_state.get("watched")
            if not str(watched_value or "").strip():
                continue
            ids, drop_reason = native_record_ids(adapter, record)
            if not ids:
                record_read_drop(adapter, "history", record, drop_reason or "unsupported_stremio_id", requires_id=_requires_for(record, drop_reason))
                continue
            try:
                orders, reconstructed = video_orders_for_series_record(adapter, record, watched_value)
                videos = select_video_order(orders, watched_value, require_anchor_match=reconstructed)
                video_ids = [str(v.get("id") or "").strip() for v in videos]
                watched_ids = decode_watched_episodes(watched_value, video_ids)
                if not watched_ids:
                    raise ValueError("watched_anchor_unmatched")
            except EpisodeIndexUnavailable:
                cw_log("STREMIO", "history", "warn", "index_failed_episode_list_unavailable", record_id=show_id)
                raise
            except Exception as exc:
                reason = str(exc) if isinstance(exc, ValueError) else ""
                if not reason or reason in _RECONSTRUCTION_FAILURES:
                    reason = "cinemeta_episode_index_unavailable" if stremio_id_namespace(show_id) == "imdb" else "native_episode_index_unavailable"
                record_read_drop(adapter, "history", record, reason, requires_id=_requires_for(record, reason), detail=str(exc))
                continue
            by_id = {str(v.get("id") or "").strip(): v for v in videos}
            native = tmdb_ids_for_record(adapter, record, "tv")
            verified = verified_video_ids(adapter, native.get("tmdb", ""), videos, watched_ids, imdb_id(show_id) or "")
            if native:
                held_episodes += len(watched_ids - verified)
            elif imdb_id(show_id):
                unresolved_titles += 1
            for video_id in watched_ids:
                video = by_id.get(video_id) or {}
                item = item_from_episode(show_id, video.get("season"), video.get("episode"), record, video)
                if not item:
                    continue
                item = _metadata_enriched(adapter, item, "episode")
                if video_id in verified:
                    item["show_ids"] = {**item["show_ids"], **native}
                item["watched"] = True
                key = canonical_item_key(item)
                _apply_episode_watched_at(item, key, record, previous_dates)
                out[key] = item
    tick(len(records))
    summary = read_drop_summary(adapter, "history")
    if int(summary.get("dropped") or 0):
        cw_log("STREMIO", "history", "warn", "index_rows_dropped", indexed=len(out), **summary)
    if unresolved_titles or held_episodes:
        cw_log("STREMIO", "history", "info", "index_tmdb_ids_incomplete", unresolved_titles=unresolved_titles, held_episodes=held_episodes,
               lookup_failed=bool(getattr(adapter, "_stremio_tmdb_lookup_failed", False)), tmdb_key=tmdb_metadata_provider(adapter) is not None)
    return out


def _history_ts(item: Mapping[str, Any]) -> str:
    return iso_from_epoch_ms(item.get("watched_at") or item.get("last_watched_at") or item.get("lastWatchedAt")) or now_iso()


def _latest_history_ts(items: Iterable[Mapping[str, Any]]) -> str | None:
    latest: tuple[int, str] | None = None
    for item in items:
        ts = _history_ts(item)
        ms = epoch_ms(ts)
        if ms is None:
            continue
        if latest is None or ms > latest[0]:
            latest = (ms, ts)
    return latest[1] if latest else None


def _unresolved(item: Mapping[str, Any], reason: str, key: str | None = None) -> dict[str, Any]:
    out = {"status": "unresolved", "reason": reason, "item": id_minimal(item)}
    if key:
        out["key"] = key
        out["canonical_key"] = key
    return out


def _api_failure(item: Mapping[str, Any], key: str, exc: StremioAuthError, fallback: str) -> dict[str, Any]:
    entry = {"status": "failed", "reason": str(getattr(exc, "reason", "") or fallback), "item": id_minimal(item), "key": key, "canonical_key": key}
    detail = str(getattr(exc, "detail", "") or "").replace("\n", " ").replace("\r", " ").strip()
    if detail:
        for token in ("authKey", "auth_key", "password"):
            detail = detail.replace(token, f"{token[0]}***")
        entry["detail"] = detail[:180]
    return entry


def _apply_poster(record: dict[str, Any], item: Mapping[str, Any]) -> None:
    poster = poster_url_from_item(item, record.get("_id"))
    if poster and not str(record.get("poster") or "").strip():
        record["poster"] = poster


def _apply_movie(record: dict[str, Any], item: Mapping[str, Any], watched: bool) -> None:
    ts = now_iso()
    state = record.setdefault("state", {})
    _apply_poster(record, item)
    record["_mtime"] = ts
    if watched:
        state["lastWatched"] = _history_ts(item)
        state["timesWatched"] = max(1, positive_int(state.get("timesWatched")) or 0)
        state["flaggedWatched"] = 1
        state["timeOffset"] = 0
    else:
        state["lastWatched"] = ""
        state["timesWatched"] = 0
        state["flaggedWatched"] = 0
        state["timeOffset"] = 0


def _matching_video_id(videos: list[Mapping[str, Any]], item: Mapping[str, Any], show_id: str) -> str:
    video_ids = [str(v.get("id") or "").strip() for v in videos]
    video_id = video_id_for_episode(item, show_id)
    if video_id and video_id in video_ids:
        return video_id
    season = season_int(item.get("season"))
    episode = positive_int(item.get("episode"))
    return next((str(v.get("id") or "").strip() for v in videos if season is not None and season_int(v.get("season")) == season and positive_int(v.get("episode")) == episode), "")


def _refreshed_videos(adapter: Any, record: Mapping[str, Any], watched_value: Any, reconstructed: bool) -> list[Mapping[str, Any]]:
    show_id = record_id(record)
    imdb = imdb_id(show_id)
    if imdb:
        return cinemeta_videos(adapter, imdb, fresh=True)
    tmdb = str(ids_from_stremio_record(record).get("tmdb") or "").strip()
    if not tmdb:
        return []
    orders = tmdb_native_video_orders(adapter, tmdb, video_prefix=show_id, fresh=True)
    return select_video_order(orders, watched_value, require_anchor_match=reconstructed and bool(str(watched_value or "").strip()))


def _apply_episode(adapter: Any, record: dict[str, Any], item: Mapping[str, Any], watched: bool) -> str | None:
    show_id = record_id(record)
    if not show_id or not ids_from_stremio_record(record):
        return "stremio_id_missing"
    state = record.setdefault("state", {})
    try:
        orders, reconstructed = video_orders_for_series_record(adapter, record, state.get("watched"), for_write=True)
        videos = select_video_order(orders, state.get("watched"), require_anchor_match=reconstructed and bool(str(state.get("watched") or "").strip()))
    except ValueError as exc:
        return str(exc) or "stremio_episode_unresolved"
    except Exception:
        return "stremio_episode_unresolved"
    video_id = _matching_video_id(videos, item, show_id)
    if not video_id:
        try:
            videos = _refreshed_videos(adapter, record, state.get("watched"), reconstructed) or videos
        except ValueError:
            pass
        video_id = _matching_video_id(videos, item, show_id)
    if not video_id:
        return "stremio_episode_unresolved"
    video_ids = [str(v.get("id") or "").strip() for v in videos]
    _apply_poster(record, item)
    try:
        state["watched"] = set_episode_watched_value(state.get("watched"), video_ids, video_id, watched)
    except Exception:
        return "stremio_watched_bitfield_malformed"
    record["_mtime"] = now_iso()
    return None


def _apply_series_history_timestamp(record: dict[str, Any], items: Iterable[Mapping[str, Any]], watched: bool) -> None:
    state = record.setdefault("state", {})
    if watched:
        latest = _latest_history_ts(items)
        if latest:
            state["lastWatched"] = latest
            state["timesWatched"] = max(1, positive_int(state.get("timesWatched")) or 0)
            state["flaggedWatched"] = 1
        return
    if not str(state.get("watched") or "").strip():
        state["lastWatched"] = ""
        state["timesWatched"] = 0
        state["flaggedWatched"] = 0


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, True, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, False, dry_run=dry_run)


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], watched: bool, *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    grouped: dict[tuple[str, str], list[tuple[str, dict[str, Any], str]]] = {}
    attempted = 0
    for item in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        key = canonical_item_key(item)
        typ = str(item.get("type") or id_minimal(item).get("type") or "").strip().lower()
        item = _metadata_enriched(adapter, item, typ)
        stremio_id = stremio_write_id_for_item(item)
        if not stremio_id or typ not in {"movie", "episode", "episodes"}:
            entry = _unresolved(item, "stremio_id_missing", key)
            unresolved.append(entry)
            results.append(entry)
            continue
        attempted += 1
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(item), "canonical_key": key})
            continue
        grouped.setdefault((stremio_id, "movie" if typ == "movie" else "series"), []).append((key, item, typ))
    if grouped and not dry_run:
        ids = list(dict.fromkeys(stremio_id for stremio_id, _item_type in grouped))
        try:
            current = {record_id(row): copy.deepcopy(dict(row)) for row in datastore_get(adapter, ids=ids, all_records=False)}
        except StremioAuthError as exc:
            for ops in grouped.values():
                for key, item, _typ in ops:
                    entry = _api_failure(item, key, exc, "stremio_history_write_failed")
                    unresolved.append(entry)
                    results.append(entry)
            return build_op_result(ok=False, count=0, confirmed_keys=[], unresolved_keys=unresolved_keys(unresolved, canonical_item_key), unresolved=unresolved, results=results, attempted=attempted)
        prepared: list[tuple[dict[str, Any], list[tuple[str, dict[str, Any]]]]] = []
        for (stremio_id, item_type), ops in grouped.items():
            record = current.get(stremio_id) or default_record(stremio_id, item_type, ops[0][1])
            applied: list[tuple[str, dict[str, Any]]] = []
            for key, item, typ in ops:
                try:
                    reason = None
                    if typ == "movie":
                        _apply_movie(record, item, watched)
                    else:
                        reason = _apply_episode(adapter, record, item, watched)
                    if reason:
                        raise ValueError(reason)
                except ValueError as exc:
                    entry = _unresolved(item, str(exc) or "stremio_history_write_failed", key)
                    unresolved.append(entry)
                    results.append(entry)
                    continue
                except Exception:
                    entry = {"status": "failed", "reason": "stremio_history_write_failed", "item": id_minimal(item), "key": key, "canonical_key": key}
                    unresolved.append(entry)
                    results.append(entry)
                    continue
                applied.append((key, item))
            if applied:
                if item_type == "series":
                    _apply_series_history_timestamp(record, [item for _key, item in applied], watched)
                prepared.append((record, applied))
        pending = prepared
        if prepared:
            try:
                datastore_put(adapter, [record for record, _ops in prepared])
            except Exception:
                pending = []
                for record, ops in prepared:
                    try:
                        datastore_put(adapter, [record])
                    except StremioAuthError as exc:
                        for key, item in ops:
                            entry = _api_failure(item, key, exc, "stremio_history_write_failed")
                            unresolved.append(entry)
                            results.append(entry)
                        continue
                    except Exception:
                        for key, item in ops:
                            entry = {"status": "failed", "reason": "stremio_history_write_failed", "item": id_minimal(item), "key": key, "canonical_key": key}
                            unresolved.append(entry)
                            results.append(entry)
                        continue
                    pending.append((record, ops))
        for _record, ops in pending:
            for key, item in ops:
                confirmed.append(key)
                results.append({"status": "applied", "item": id_minimal(item), "canonical_key": key})
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved, canonical_item_key), unresolved=unresolved, results=results, attempted=attempted)
