# /services/playback_progress/service.py
# CrossWatch - Playback Progress Service
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, cast

from _logging import log as BASE_LOG
from cw_platform.config_base import load_config, save_config
from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.provider_instances import build_provider_config_view, get_instance_block, get_provider_block, list_instance_ids, normalize_instance_id

from .adapters.base import PlaybackProgressAdapter, configured_label
from .adapters.media_servers import EmbyPlaybackAdapter, JellyfinPlaybackAdapter, PlexPlaybackAdapter
from .adapters.mdblist import MDBListPlaybackAdapter
from .adapters.publicmetadb import PublicMetaDBPlaybackAdapter
from .adapters.simkl import SimklPlaybackAdapter
from .adapters.trakt import TraktPlaybackAdapter
from .models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, clean_mapping, utc_now_iso


LOG = BASE_LOG.child("PLAYBACK")
CACHE_TTL_SECONDS = 60.0
MAX_WORKERS = 6
DEFAULT_PROVIDER_TIMEOUT_SECONDS = 12.0
GROUP_PROGRESS_TOLERANCE = 2.0
PHASE1_PROVIDERS = ("trakt", "simkl", "mdblist", "publicmetadb", "plex", "emby", "jellyfin")
SORT_VALUES = {"last_updated", "progress_high", "progress_low", "remaining_time", "rating_high", "title", "provider"}
LIVE_MEDIA_PROVIDERS = {"plex", "emby", "jellyfin"}
LIVE_ACTIVE_STATES = {"playing", "paused", "buffering"}
LIVE_MAX_AGE_SECONDS = 10 * 60


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], value) if isinstance(value, Mapping) else {}


def _group_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _group_number(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        return str(int(value))
    except Exception:
        return str(value).strip().lower()


def _live_source_provider(value: Any) -> str:
    source = str(value or "").strip().lower()
    return {
        "plextrakt": "plex",
        "embytrakt": "emby",
        "jellyfintrakt": "jellyfin",
    }.get(source, source)


def _live_show_ids(ids: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in ids.items():
        text = str(key or "").strip().lower()
        if not text.endswith("_show") or value in (None, ""):
            continue
        out[text[:-5]] = value
    return out


def _live_item(stream: Mapping[str, Any]) -> dict[str, Any]:
    ids_value = stream.get("ids")
    ids: Mapping[str, Any] = cast(Mapping[str, Any], ids_value) if isinstance(ids_value, Mapping) else {}
    media_type = str(stream.get("media_type") or stream.get("type") or "").strip().lower()
    typ = "episode" if media_type in {"episode", "anime_episode"} else "movie"
    show_ids = _live_show_ids(ids)
    item: dict[str, Any] = {
        "type": typ,
        "title": stream.get("title"),
        "year": stream.get("year"),
        "season": stream.get("season"),
        "episode": stream.get("episode"),
        "ids": {str(k): v for k, v in ids.items() if not str(k).lower().endswith("_show")},
    }
    if show_ids:
        item["show_ids"] = show_ids
    return id_minimal(item)


def _title_key(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _live_match_keys(item: Mapping[str, Any]) -> set[str]:
    keys: set[str] = set()
    ck = str(item.get("canonical_key") or "").strip().lower()
    if not ck:
        try:
            ck = canonical_key(item).strip().lower()
        except Exception:
            ck = ""
    if ck and not ck.startswith("unknown:"):
        keys.add(f"key:{ck}")

    media_type = str(item.get("media_type") or item.get("type") or "").strip().lower()
    typ = "movie" if media_type == "movie" else "episode"
    title = _title_key(item.get("title"))
    series = _title_key(item.get("series_title"))
    season = _group_number(item.get("season"))
    episode = _group_number(item.get("episode"))
    year = _group_number(item.get("year"))
    if typ == "movie" and title:
        keys.add(f"live:movie:{title}:{year}")
    elif typ == "episode" and season and episode:
        for name in {series, title}:
            if name:
                keys.add(f"live:episode:{name}:{season}:{episode}")
    return {key for key in keys if key}


def _live_stream_keys(stream: Mapping[str, Any]) -> set[str]:
    return _live_match_keys(_live_item(stream))


def _live_progress_percent(stream: Mapping[str, Any]) -> float | None:
    raw = stream.get("progress")
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except Exception:
        return None
    return round(max(0.0, min(100.0, value)), 3) if math.isfinite(value) else None


def _live_remaining_seconds(stream: Mapping[str, Any], progress_percent: float | None) -> int | None:
    raw = stream.get("duration_ms")
    if raw in (None, ""):
        return None
    try:
        duration_ms = float(raw)
    except Exception:
        return None
    if not math.isfinite(duration_ms) or duration_ms <= 0 or progress_percent is None:
        return None
    remaining = int(round((duration_ms / 1000.0) * max(0.0, 100.0 - min(progress_percent, 100.0)) / 100.0))
    return remaining if remaining > 0 else None


def _live_rank(stream: Mapping[str, Any]) -> tuple[int, int]:
    state = str(stream.get("state") or "").strip().lower()
    rank = 0 if state == "playing" else 1 if state == "buffering" else 2 if state == "paused" else 3
    try:
        updated = int(stream.get("updated") or 0)
    except Exception:
        updated = 0
    return (rank, -updated)


def _currently_watching_state_file() -> Any:
    try:
        from providers.scrobble.currently_watching import state_file

        return state_file()
    except Exception:
        return None


def _load_live_streams(now: int | None = None) -> list[dict[str, Any]]:
    path = _currently_watching_state_file()
    if path is None:
        return []
    try:
        if not path.exists():
            return []
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw) if raw.strip() else None
    except Exception:
        return []
    if not isinstance(data, Mapping) or int(data.get("v") or 0) != 2 or not isinstance(data.get("streams"), Mapping):
        return []
    now_sec = int(now if now is not None else time.time())
    out: list[dict[str, Any]] = []
    for value in data.get("streams", {}).values():
        if not isinstance(value, Mapping):
            continue
        state = str(value.get("state") or "").strip().lower()
        provider = _live_source_provider(value.get("source"))
        try:
            updated = int(value.get("updated") or 0)
        except Exception:
            updated = 0
        if provider not in LIVE_MEDIA_PROVIDERS or state not in LIVE_ACTIVE_STATES:
            continue
        if updated and now_sec - updated > LIVE_MAX_AGE_SECONDS:
            continue
        stream = dict(value)
        stream["_live_provider"] = provider
        stream["_live_instance_id"] = normalize_instance_id(value.get("provider_instance")) if value.get("provider_instance") else ""
        out.append(stream)
    out.sort(key=_live_rank)
    return out


def _apply_live_overlay(item: dict[str, Any], stream: Mapping[str, Any]) -> None:
    progress = _live_progress_percent(stream)
    remaining = _live_remaining_seconds(stream, progress)
    provider = str(stream.get("_live_provider") or _live_source_provider(stream.get("source")) or "").strip().lower()
    instance_id = normalize_instance_id(stream.get("_live_instance_id") or stream.get("provider_instance"))
    item["live_state"] = str(stream.get("state") or "").strip().lower()
    item["live_source"] = str(stream.get("source") or "").strip()
    item["live_provider"] = provider
    item["live_instance_id"] = instance_id
    item["live_updated"] = int(stream.get("updated") or 0) if str(stream.get("updated") or "").strip() else None
    item["live_started"] = int(stream.get("started") or 0) if str(stream.get("started") or "").strip() else None
    if progress is not None:
        item["live_progress_percent"] = progress
    if remaining is not None:
        item["live_remaining_seconds"] = remaining


def _overlay_live_streams(items: list[dict[str, Any]], streams: list[dict[str, Any]] | None = None) -> None:
    live_streams = streams if streams is not None else _load_live_streams()
    if not live_streams:
        return
    by_key: dict[str, list[dict[str, Any]]] = {}
    for stream in live_streams:
        for key in _live_stream_keys(stream):
            by_key.setdefault(key, []).append(stream)
    if not by_key:
        return
    for item in items:
        provider = str(item.get("provider") or "").strip().lower()
        if provider not in LIVE_MEDIA_PROVIDERS:
            continue
        instance_id = normalize_instance_id(item.get("instance_id"))
        candidates: list[dict[str, Any]] = []
        for key in _live_match_keys(item):
            for stream in by_key.get(key, []):
                live_provider = str(stream.get("_live_provider") or _live_source_provider(stream.get("source")) or "").strip().lower()
                live_instance = normalize_instance_id(stream.get("_live_instance_id") or stream.get("provider_instance"))
                if live_provider and live_provider != provider:
                    continue
                if live_instance != instance_id:
                    continue
                candidates.append(stream)
        if candidates:
            _apply_live_overlay(item, sorted(candidates, key=_live_rank)[0])


def _record_group_keys(item: Mapping[str, Any]) -> list[str]:
    keys: list[str] = []
    key = str(item.get("canonical_key") or "").strip().lower()
    profile = _group_text(normalize_instance_id(item.get("instance_id")))
    if key and not key.startswith("unknown:"):
        keys.append(f"key:{key}:profile:{profile}")
    media_type = _group_text(item.get("media_type"))
    title = _group_text(item.get("title"))
    series = _group_text(item.get("series_title"))
    season = _group_number(item.get("season"))
    episode = _group_number(item.get("episode"))
    year = _group_number(item.get("year"))
    if media_type == "movie":
        fallback = f"fallback:movie:{title}:{year}:profile:{profile}"
    else:
        fallback = f"fallback:episode:{series or title}:{season}:{episode}:profile:{profile}"
    if fallback not in keys:
        keys.append(fallback)
    return keys


def _record_time_value(item: Mapping[str, Any]) -> float:
    try:
        live_updated = int(item.get("live_updated") or 0)
    except Exception:
        live_updated = 0
    if live_updated > 0:
        return float(live_updated)
    dt = _parse_iso(item.get("updated_at") or item.get("progress_at"))
    return dt.timestamp() if dt else 0.0


def _record_order_key(item: Mapping[str, Any]) -> tuple[float, int, str, str, str]:
    artwork = int(not _blank_scalar(item.get("poster_url"))) + int(not _blank_scalar(item.get("backdrop_url")))
    return (
        -_record_time_value(item),
        -artwork,
        str(item.get("provider") or "").strip().lower(),
        normalize_instance_id(item.get("instance_id")),
        str(item.get("remote_id") or "").strip(),
    )


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except Exception:
        return None


def _split_progress_groups(records: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    numeric: list[tuple[float, dict[str, Any]]] = []
    missing: list[dict[str, Any]] = []
    for record in records:
        progress = _float_or_none(record.get("progress_percent"))
        if progress is None:
            missing.append(record)
        else:
            numeric.append((progress, record))

    numeric.sort(key=lambda entry: (entry[0], _record_order_key(entry[1])))
    groups: list[list[dict[str, Any]]] = []
    group_start: float | None = None
    for progress, record in numeric:
        if group_start is None or progress - group_start > GROUP_PROGRESS_TOLERANCE:
            groups.append([record])
            group_start = progress
        else:
            groups[-1].append(record)
    if missing:
        groups.append(sorted(missing, key=_record_order_key))
    return groups


def _remaining_seconds(duration_seconds: Any, progress_percent: Any) -> int | None:
    duration = _float_or_none(duration_seconds)
    progress = _float_or_none(progress_percent)
    if duration is None or progress is None or duration <= 0:
        return None
    remaining = int(round(duration * max(0.0, 100.0 - min(progress, 100.0)) / 100.0))
    return remaining if remaining > 0 else None


def _blank_scalar(value: Any) -> bool:
    return value is None or value == ""


def _rating_value(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        rating = float(value)
        return rating if math.isfinite(rating) and rating > 0 else None
    except Exception:
        return None


def _with_remaining_fallback(item: dict[str, Any]) -> dict[str, Any]:
    item["rating"] = _rating_value(item.get("rating"))
    if not _blank_scalar(item.get("remaining_seconds")):
        return item
    remaining = _remaining_seconds(item.get("duration_seconds"), item.get("progress_percent"))
    if remaining is not None:
        item["remaining_seconds"] = remaining
    return item


def _combine_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(records, key=_record_order_key)
    primary = dict(ordered[0])
    live_records = [record for record in ordered if str(record.get("live_state") or "").strip().lower() in LIVE_ACTIVE_STATES]
    if live_records:
        live = sorted(
            live_records,
            key=lambda record: (
                0 if record.get("live_state") == "playing" else 1 if record.get("live_state") == "buffering" else 2,
                -int(record.get("live_updated") or 0),
            ),
        )[0]
        for key in (
            "live_state",
            "live_source",
            "live_provider",
            "live_instance_id",
            "live_updated",
            "live_started",
            "live_progress_percent",
            "live_remaining_seconds",
        ):
            if key in live:
                primary[key] = live.get(key)
    if _blank_scalar(primary.get("remaining_seconds")):
        primary["remaining_seconds"] = next((record.get("remaining_seconds") for record in ordered if not _blank_scalar(record.get("remaining_seconds"))), None)
    if _blank_scalar(primary.get("duration_seconds")):
        primary["duration_seconds"] = next((record.get("duration_seconds") for record in ordered if not _blank_scalar(record.get("duration_seconds"))), None)
    artwork_records = sorted(
        records,
        key=lambda record: (
            str(record.get("provider") or "").strip().lower(),
            normalize_instance_id(record.get("instance_id")),
            str(record.get("remote_id") or "").strip(),
        ),
    )
    for field in ("poster_url", "backdrop_url"):
        primary[field] = next((record.get(field) for record in artwork_records if not _blank_scalar(record.get(field))), "")
    merged_ids: dict[str, Any] = {}
    for record in ordered:
        ids = record.get("ids")
        if not isinstance(ids, Mapping):
            continue
        for key, value in ids.items():
            if not _blank_scalar(value):
                merged_ids.setdefault(str(key), value)
    if merged_ids:
        primary["ids"] = merged_ids
    primary_meta_value = primary.get("provider_metadata")
    primary_meta: dict[str, Any] = (
        {str(key): value for key, value in primary_meta_value.items()}
        if isinstance(primary_meta_value, Mapping)
        else {}
    )
    if not isinstance(primary_meta.get("show_ids"), Mapping) or not primary_meta.get("show_ids"):
        for record in ordered:
            meta = record.get("provider_metadata")
            show_ids = meta.get("show_ids") if isinstance(meta, Mapping) else None
            if isinstance(show_ids, Mapping) and show_ids:
                primary_meta["show_ids"] = {str(key): value for key, value in show_ids.items()}
                break
    if primary_meta:
        primary["provider_metadata"] = primary_meta
    ratings = [rating for rating in (_rating_value(record.get("rating")) for record in ordered) if rating is not None]
    if ratings:
        primary["rating"] = max(ratings)
    else:
        primary["rating"] = None
    providers: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for record in ordered:
        provider = str(record.get("provider") or "").strip()
        instance_id = normalize_instance_id(record.get("instance_id"))
        key = (provider, instance_id)
        if key in seen:
            continue
        seen.add(key)
        providers.append(
            {
                "provider": provider,
                "provider_label": str(record.get("provider_label") or provider),
                "instance_id": instance_id,
                "instance_label": str(record.get("instance_label") or record.get("provider_label") or provider),
                "can_remove_progress": bool(record.get("can_remove_progress")),
                "can_mark_watched": bool(record.get("can_mark_watched")),
                "can_update_progress": bool(record.get("can_update_progress")),
                "remote_id": str(record.get("remote_id") or ""),
            }
        )
    primary["records"] = ordered
    primary["providers"] = providers
    primary["provider_count"] = len(providers)
    primary["is_combined"] = len(ordered) > 1
    primary["can_remove_progress"] = any(bool(record.get("can_remove_progress")) for record in ordered)
    primary["can_mark_watched"] = any(bool(record.get("can_mark_watched")) for record in ordered)
    primary["can_update_progress"] = any(bool(record.get("can_update_progress")) for record in ordered)
    combined_id = "|".join(
        sorted(
            f"{str(record.get('provider') or '').strip().lower()}:{normalize_instance_id(record.get('instance_id'))}:{str(record.get('remote_id') or record.get('canonical_key') or '').strip()}"
            for record in ordered
        )
    )
    primary["provider"] = "combined" if len(ordered) > 1 else primary.get("provider")
    primary["instance_id"] = "all" if len(ordered) > 1 else primary.get("instance_id")
    primary["remote_id"] = combined_id if len(ordered) > 1 else primary.get("remote_id")
    return primary


def _group_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    identity_groups: dict[str, list[dict[str, Any]]] = {}
    aliases: dict[str, str] = {}
    for item in records:
        keys = _record_group_keys(item)
        group_key = next((aliases[key] for key in keys if key in aliases), keys[0] if keys else "fallback:unknown")
        identity_groups.setdefault(group_key, []).append(item)
        for key in keys:
            aliases[key] = group_key
    return [
        _combine_records(progress_group)
        for identity_group in identity_groups.values()
        for progress_group in _split_progress_groups(identity_group)
    ]


def _validated_progress_percent(value: Any) -> tuple[float | None, str]:
    try:
        progress = float(value)
    except Exception:
        return None, "Progress must be a number."
    if not math.isfinite(progress):
        return None, "Progress must be a finite number."
    if progress < 2 or progress >= 80:
        return None, "Progress must be between 2 and 79 percent. Use Mark as Watched for completed items."
    return round(progress, 2), ""


def _instance_label(cfg: Mapping[str, Any], provider: str, instance_id: str) -> str:
    block = get_provider_block(cfg, provider, instance_id)
    label = configured_label(block, "Default" if instance_id == "default" else instance_id)
    provider_label = {
        "trakt": "Trakt",
        "simkl": "SIMKL",
        "mdblist": "MDBList",
        "publicmetadb": "PublicMetaDB",
        "plex": "Plex",
        "emby": "Emby",
        "jellyfin": "Jellyfin",
    }.get(provider, provider)
    if label.lower() == "default":
        return f"{provider_label} Default"
    if label.lower().startswith(provider_label.lower()):
        return label
    return f"{provider_label} {label}"


def _profile_key(provider: Any, instance_id: Any) -> str:
    return f"{str(provider or '').strip().lower()}:{normalize_instance_id(instance_id)}"


def _path_value(block: Mapping[str, Any], path: str) -> Any:
    value: Any = block
    for part in path.split("."):
        if not isinstance(value, Mapping):
            return None
        value = value.get(part)
    return value


def _profile_has_explicit_identity(cfg: Mapping[str, Any], provider: str, instance_id: Any) -> bool:
    inst = normalize_instance_id(instance_id)
    if inst == "default":
        return True
    raw = get_instance_block(cfg, provider, inst, create=False)
    if not raw:
        return False
    identity_paths = {
        "trakt": ("access_token", "token", "oauth.access_token"),
        "simkl": ("access_token", "token", "oauth.access_token"),
        "mdblist": ("access_token", "api_key", "key"),
        "publicmetadb": ("api_key",),
        "plex": (
            "account_token",
            "token",
            "pms_token",
            "pms.token",
            "pms.x_plex_token",
            "username",
            "account_id",
            "baseurl",
            "server_url",
            "server",
            "server_name",
            "machine_id",
        ),
        "emby": ("access_token", "user_id"),
        "jellyfin": ("access_token", "user_id"),
    }.get(str(provider or "").strip().lower(), ())
    return any(str(_path_value(raw, path) or "").strip() for path in identity_paths)


def _playback_settings(cfg: Mapping[str, Any]) -> Mapping[str, Any]:
    value = cfg.get("playback_progress") if isinstance(cfg, Mapping) else None
    return value if isinstance(value, Mapping) else {}


def _disabled_profiles(cfg: Mapping[str, Any]) -> set[str]:
    raw = _playback_settings(cfg).get("disabled_profiles")
    if not isinstance(raw, list):
        return set()
    return {_profile_key(*(str(item).split(":", 1) if ":" in str(item) else (item, "default"))) for item in raw}


def _profile_included(cfg: Mapping[str, Any], provider: Any, instance_id: Any) -> bool:
    return _profile_key(provider, instance_id) not in _disabled_profiles(cfg)


def _provider_timeout_seconds(cfg: Mapping[str, Any]) -> float:
    try:
        raw = float(_playback_settings(cfg).get("provider_timeout_seconds") or DEFAULT_PROVIDER_TIMEOUT_SECONDS)
    except Exception:
        raw = DEFAULT_PROVIDER_TIMEOUT_SECONDS
    return max(3.0, min(raw, 60.0))


class PlaybackProgressService:
    def __init__(self) -> None:
        self.adapters: dict[str, PlaybackProgressAdapter] = {
            "trakt": TraktPlaybackAdapter(),
            "simkl": SimklPlaybackAdapter(),
            "mdblist": MDBListPlaybackAdapter(),
            "publicmetadb": PublicMetaDBPlaybackAdapter(),
            "plex": PlexPlaybackAdapter(),
            "emby": EmbyPlaybackAdapter(),
            "jellyfin": JellyfinPlaybackAdapter(),
        }
        self._cache: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.RLock()

    def provider_instances(self, cfg: Mapping[str, Any] | None = None) -> list[dict[str, str]]:
        config = cfg or load_config()
        out: list[dict[str, str]] = []
        for provider in PHASE1_PROVIDERS:
            for instance_id in list_instance_ids(config, provider):
                inst = normalize_instance_id(instance_id)
                if not _profile_has_explicit_identity(config, provider, inst):
                    continue
                out.append(
                    {
                        "provider": provider,
                        "instance_id": inst,
                        "instance_label": _instance_label(config, provider, inst),
                    }
                )
        return out

    def capabilities(self, cfg: Mapping[str, Any] | None = None) -> list[PlaybackCapabilities]:
        config = cfg or load_config()
        out: list[PlaybackCapabilities] = []
        for spec in self.provider_instances(config):
            provider = spec["provider"]
            adapter = self.adapters.get(provider)
            if not adapter:
                continue
            config_view = build_provider_config_view(config, provider, spec["instance_id"])
            try:
                cap = adapter.capabilities(config_view, instance_id=spec["instance_id"], instance_label=spec["instance_label"])
                cap.included = _profile_included(config, provider, spec["instance_id"])
                if not cap.included:
                    cap.reason = "Excluded from Playback Progress."
                cached = self._cache.get((provider, spec["instance_id"]))
                if cached:
                    cap.last_refresh = cached.get("refreshed_at")
                    err = cached.get("error")
                    if isinstance(err, Mapping):
                        cap.last_error = str(err.get("message") or err.get("error_code") or "")
                out.append(cap)
            except Exception:
                out.append(
                    PlaybackCapabilities(
                        provider=provider,
                        provider_label=getattr(adapter, "provider_label", provider),
                        instance_id=spec["instance_id"],
                        instance_label=spec["instance_label"],
                        included=_profile_included(config, provider, spec["instance_id"]),
                        configured=False,
                        reason="Capability detection failed.",
                        last_error="Capability detection failed.",
                    )
                )
        return out

    def _cache_key(self, provider: str, instance_id: str) -> tuple[str, str]:
        return (str(provider).lower(), normalize_instance_id(instance_id))

    def invalidate(self, provider: str, instance_id: str) -> None:
        with self._lock:
            self._cache.pop(self._cache_key(provider, instance_id), None)
        LOG.debug(f"cache invalidated provider={provider} instance={normalize_instance_id(instance_id)}")

    def _activity_marker(self, adapter: PlaybackProgressAdapter, config_view: Mapping[str, Any], *, instance_id: str) -> str:
        marker_fn = getattr(adapter, "activity_marker", None)
        if callable(marker_fn):
            try:
                return str(marker_fn(config_view, instance_id=instance_id) or "")
            except TypeError:
                try:
                    return str(marker_fn(config_view) or "")
                except Exception:
                    return ""
            except Exception:
                return ""
        return ""

    def _list_one(
        self,
        cfg: Mapping[str, Any],
        spec: Mapping[str, str],
        force_refresh: bool,
    ) -> PlaybackListResult:
        provider = spec["provider"]
        instance_id = spec["instance_id"]
        adapter = self.adapters[provider]
        config_view = build_provider_config_view(cfg, provider, instance_id)
        cap = adapter.capabilities(config_view, instance_id=instance_id, instance_label=spec["instance_label"])
        if not cap.read:
            return PlaybackListResult(
                ok=False,
                provider=provider,
                instance_id=instance_id,
                error_code="unsupported" if cap.configured else "not_configured",
                message=cap.reason or "Provider does not support playback listing.",
            )

        key = self._cache_key(provider, instance_id)
        now = time.time()
        with self._lock:
            cached = self._cache.get(key)
            if not force_refresh and cached and (now - float(cached.get("ts") or 0)) < CACHE_TTL_SECONDS:
                result = cached.get("result")
                if isinstance(result, PlaybackListResult):
                    LOG.debug(f"cache hit provider={provider} instance={instance_id}")
                    return result

        marker = self._activity_marker(adapter, config_view, instance_id=instance_id)
        with self._lock:
            cached = self._cache.get(key)
            if (
                not force_refresh
                and cached
                and marker
                and marker == cached.get("activity_marker")
            ):
                result = cached.get("result")
                if isinstance(result, PlaybackListResult):
                    LOG.debug(f"activity unchanged provider={provider} instance={instance_id}")
                    return result

        started = time.monotonic()
        result = adapter.list_progress(
            config_view,
            instance_id=instance_id,
            instance_label=spec["instance_label"],
            force_refresh=force_refresh,
        )
        elapsed_ms = int((time.monotonic() - started) * 1000)
        with self._lock:
            if result.ok:
                self._cache[key] = {"ts": now, "result": result, "activity_marker": marker, "refreshed_at": result.refreshed_at}
                LOG.debug(f"provider listed provider={provider} instance={instance_id} items={len(result.items)} elapsed_ms={elapsed_ms}")
            else:
                self._cache[key] = {"ts": now, "result": result, "activity_marker": marker, "error": result.to_error()}
                LOG.warn(f"provider list failed provider={provider} instance={instance_id} error={result.error_code or 'provider_error'} status={result.remote_status or ''} elapsed_ms={elapsed_ms}")
        return result

    def items(
        self,
        *,
        provider: str | None = None,
        instance_id: str | None = None,
        media_type: str | None = None,
        progress_min: float | None = None,
        progress_max: float | None = None,
        age: str | None = None,
        rating_min: float | None = None,
        search: str | None = None,
        sort: str = "last_updated",
        page: int = 1,
        page_size: int = 50,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        cfg = load_config()
        provider_filter = str(provider or "").strip().lower()
        instance_filter = normalize_instance_id(instance_id) if instance_id else ""
        specs = [
            spec
            for spec in self.provider_instances(cfg)
            if (not provider_filter or spec["provider"] == provider_filter)
            and (not instance_filter or spec["instance_id"] == instance_filter)
        ]
        readable_specs: list[dict[str, str]] = []
        capabilities = self.capabilities(cfg)
        cap_by_key = {(cap.provider, cap.instance_id): cap for cap in capabilities}
        for spec in specs:
            cap = cap_by_key.get((spec["provider"], spec["instance_id"]))
            if cap and cap.read and cap.included:
                readable_specs.append(dict(spec))

        LOG.info(
            f"list requested providers={len(readable_specs)} force_refresh={bool(force_refresh)} "
            f"provider_filter={provider_filter or 'all'} instance_filter={instance_filter or 'all'}"
        )
        results: list[PlaybackListResult] = []
        if readable_specs:
            pool = ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(readable_specs)))
            future_specs = {pool.submit(self._list_one, cfg, spec, force_refresh): spec for spec in readable_specs}
            try:
                done, pending = wait(future_specs, timeout=_provider_timeout_seconds(cfg))
                for future in done:
                    try:
                        results.append(future.result())
                    except Exception:
                        results.append(PlaybackListResult(ok=False, provider="unknown", instance_id="", error_code="provider_error", message="Provider request failed.", retryable=True))
                for future in pending:
                    spec = future_specs[future]
                    future.cancel()
                    LOG.warn(
                        f"provider timeout provider={spec['provider']} instance={spec['instance_id']} "
                        f"timeout_s={_provider_timeout_seconds(cfg):g}"
                    )
                    results.append(
                        PlaybackListResult(
                            ok=False,
                            provider=spec["provider"],
                            instance_id=spec["instance_id"],
                            error_code="provider_timeout",
                            message="Provider did not respond quickly enough. It was skipped for this refresh.",
                            retryable=True,
                        )
                    )
            finally:
                pool.shutdown(wait=False, cancel_futures=True)

        errors = [r.to_error() for r in results if not r.ok]
        items = [_with_remaining_fallback(item.to_dict()) for result in results if result.ok for item in result.items]
        _overlay_live_streams(items)
        filtered = self._apply_filters(items, media_type=media_type, progress_min=progress_min, progress_max=progress_max, age=age, rating_min=rating_min, search=search)
        if not provider_filter:
            filtered = _group_records(filtered)
        sorted_items = self._sort(filtered, sort)
        page = max(1, int(page or 1))
        page_size = max(1, min(250, int(page_size or 50)))
        total = len(sorted_items)
        start = (page - 1) * page_size
        end = start + page_size
        LOG.debug(f"list completed total={total} errors={len(errors)} page={page} page_size={page_size}")
        return {
            "items": sorted_items[start:end],
            "page": page,
            "page_size": page_size,
            "total": total,
            "providers": [cap.to_dict() for cap in capabilities],
            "errors": errors,
            "partial": bool(errors and items),
            "refreshed_at": utc_now_iso(),
        }

    def settings(self, cfg: Mapping[str, Any] | None = None) -> dict[str, Any]:
        config = cfg or load_config()
        disabled = _disabled_profiles(config)
        profiles = []
        for cap in self.capabilities(config):
            key = _profile_key(cap.provider, cap.instance_id)
            profiles.append(
                {
                    "key": key,
                    "provider": cap.provider,
                    "provider_label": cap.provider_label,
                    "instance_id": cap.instance_id,
                    "instance_label": cap.instance_label,
                    "configured": cap.configured,
                    "read": cap.read,
                    "included": key not in disabled,
                    "reason": cap.reason,
                }
            )
        return {
            "provider_timeout_seconds": _provider_timeout_seconds(config),
            "disabled_profiles": sorted(disabled),
            "profiles": profiles,
            "refreshed_at": utc_now_iso(),
        }

    def save_settings(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        cfg = load_config()
        known = {_profile_key(spec["provider"], spec["instance_id"]) for spec in self.provider_instances(cfg)}
        disabled: set[str] = set()
        profiles_value = payload.get("profiles")
        if isinstance(profiles_value, list):
            for item in profiles_value:
                if not isinstance(item, Mapping):
                    continue
                key = _profile_key(item.get("provider") or str(item.get("key") or "").split(":", 1)[0], item.get("instance_id") or (str(item.get("key") or "").split(":", 1)[1] if ":" in str(item.get("key") or "") else "default"))
                if key in known and not bool(item.get("included", True)):
                    disabled.add(key)
        else:
            raw = payload.get("disabled_profiles")
            if isinstance(raw, list):
                disabled = {_profile_key(*(str(item).split(":", 1) if ":" in str(item) else (item, "default"))) for item in raw}
                disabled &= known

        block = cfg.setdefault("playback_progress", {})
        if not isinstance(block, dict):
            block = {}
            cfg["playback_progress"] = block
        block["disabled_profiles"] = sorted(disabled)
        if "provider_timeout_seconds" in payload:
            try:
                block["provider_timeout_seconds"] = _provider_timeout_seconds({"playback_progress": {"provider_timeout_seconds": payload.get("provider_timeout_seconds")}})
            except Exception:
                block["provider_timeout_seconds"] = DEFAULT_PROVIDER_TIMEOUT_SECONDS
        save_config(cfg)
        with self._lock:
            self._cache.clear()
        LOG.info(
            f"settings saved disabled_profiles={len(disabled)} "
            f"timeout_s={_provider_timeout_seconds(cfg):g}"
        )
        return {"ok": True, "settings": self.settings(cfg)}

    def _apply_filters(self, items: list[dict[str, Any]], **filters: Any) -> list[dict[str, Any]]:
        media_type = str(filters.get("media_type") or "").strip().lower()
        q = str(filters.get("search") or "").strip().lower()
        age = str(filters.get("age") or "").strip().lower()
        rating_min = filters.get("rating_min")
        progress_min = filters.get("progress_min")
        progress_max = filters.get("progress_max")
        now = datetime.now(timezone.utc)
        out: list[dict[str, Any]] = []
        for item in items:
            if media_type and media_type not in {"all", item.get("media_type")}:
                continue
            progress = item.get("progress_percent")
            if progress_min is not None and (progress is None or float(progress) < float(progress_min)):
                continue
            if progress_max is not None and (progress is None or float(progress) > float(progress_max)):
                continue
            rating = item.get("rating")
            if rating_min is not None and (rating is None or float(rating) < float(rating_min)):
                continue
            if age:
                dt = _parse_iso(item.get("updated_at") or item.get("progress_at"))
                if age == "today":
                    if not dt or dt.date() != now.date():
                        continue
                elif age == "7d":
                    if not dt or dt < now - timedelta(days=7):
                        continue
                elif age == "30d":
                    if not dt or dt < now - timedelta(days=30):
                        continue
                elif age == "older_30d":
                    if not dt or dt >= now - timedelta(days=30):
                        continue
            if q:
                hay = " ".join(
                    str(item.get(key) or "")
                    for key in ("title", "series_title", "episode_title", "provider_label", "instance_label", "source_app", "source_device")
                ).lower()
                if item.get("season") is not None and item.get("episode") is not None:
                    hay += f" s{int(item['season']):02d}e{int(item['episode']):02d}"
                if q not in hay:
                    continue
            out.append(item)
        return out

    def _sort(self, items: list[dict[str, Any]], sort: str) -> list[dict[str, Any]]:
        key = sort if sort in SORT_VALUES else "last_updated"
        def ts(item: Mapping[str, Any]) -> float:
            dt = _parse_iso(item.get("updated_at") or item.get("progress_at"))
            return dt.timestamp() if dt else 0.0
        def progress_low(item: Mapping[str, Any]) -> float:
            progress = item.get("progress_percent")
            return float(progress) if progress is not None else math.inf
        def remaining_time(item: Mapping[str, Any]) -> int:
            remaining = item.get("remaining_seconds")
            return int(remaining) if remaining is not None else 10**12
        if key == "progress_high":
            return sorted(items, key=lambda x: float(x.get("progress_percent") or -1), reverse=True)
        if key == "progress_low":
            return sorted(items, key=progress_low)
        if key == "remaining_time":
            return sorted(items, key=remaining_time)
        if key == "rating_high":
            return sorted(items, key=lambda x: float(x.get("rating") or -1), reverse=True)
        if key == "title":
            return sorted(items, key=lambda x: str(x.get("series_title") or x.get("title") or "").lower())
        if key == "provider":
            return sorted(items, key=lambda x: (str(x.get("provider_label") or ""), str(x.get("instance_label") or ""), str(x.get("title") or "")))
        return sorted(items, key=ts, reverse=True)

    def _adapter_for_action(self, cfg: Mapping[str, Any], provider: str, instance_id: str) -> tuple[PlaybackProgressAdapter | None, dict[str, Any], str]:
        provider_key = str(provider or "").strip().lower()
        inst = normalize_instance_id(instance_id)
        adapter = self.adapters.get(provider_key)
        if not adapter:
            return None, {}, inst
        return adapter, build_provider_config_view(cfg, provider_key, inst), inst

    def remove(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        cfg = load_config()
        provider = str(payload.get("provider") or "").lower()
        instance_id = normalize_instance_id(payload.get("instance_id"))
        record_value = payload.get("record")
        record = _as_mapping(record_value) if isinstance(record_value, Mapping) else payload
        adapter, config_view, inst = self._adapter_for_action(cfg, provider, instance_id)
        if adapter is None:
            LOG.warn(f"remove requested unknown provider={provider} instance={inst}")
            return PlaybackActionResult(False, provider, inst, "remove_progress", error_code="unknown_provider", message="Unknown provider.").to_dict()
        label = _instance_label(cfg, provider, inst)
        LOG.info(f"remove progress requested provider={provider} instance={inst} remote_id={record.get('remote_id') or ''}")
        result = adapter.remove_progress(config_view, record, instance_id=inst, instance_label=label)
        if result.ok:
            self.invalidate(provider, inst)
            LOG.success(f"remove progress completed provider={provider} instance={inst} remote_id={record.get('remote_id') or ''}")
        else:
            LOG.warn(f"remove progress failed provider={provider} instance={inst} error={result.error_code or 'provider_error'}")
        return result.to_dict()

    def mark_watched(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        cfg = load_config()
        provider = str(payload.get("provider") or "").lower()
        instance_id = normalize_instance_id(payload.get("instance_id"))
        record_value = payload.get("record")
        record = _as_mapping(record_value) if isinstance(record_value, Mapping) else payload
        adapter, config_view, inst = self._adapter_for_action(cfg, provider, instance_id)
        if adapter is None:
            LOG.warn(f"mark watched requested unknown provider={provider} instance={inst}")
            return PlaybackActionResult(False, provider, inst, "mark_watched", error_code="unknown_provider", message="Unknown provider.").to_dict()
        label = _instance_label(cfg, provider, inst)
        LOG.info(f"mark watched requested provider={provider} instance={inst} remote_id={record.get('remote_id') or ''}")
        result = adapter.mark_watched(config_view, record, instance_id=inst, instance_label=label, watched_at=str(payload.get("watched_at") or "").strip() or None)
        if result.ok:
            self.invalidate(provider, inst)
            cleanup = self._cleanup_after_mark(adapter, config_view, record, provider, inst, label)
            result.playback_cleanup_result = cleanup.to_dict()
            self.invalidate(provider, inst)
            LOG.success(f"mark watched completed provider={provider} instance={inst} remote_id={record.get('remote_id') or ''}")
        else:
            LOG.warn(f"mark watched failed provider={provider} instance={inst} error={result.error_code or 'provider_error'}")
        return result.to_dict()

    def _cleanup_after_mark(
        self,
        adapter: PlaybackProgressAdapter,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        provider: str,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackActionResult:
        if provider == "plex":
            return PlaybackActionResult(True, provider, instance_id, "remove_progress", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""), message="Cleanup skipped because Plex clears resume progress through mark-unwatched.")
        caps = adapter.capabilities(config_view, instance_id=instance_id, instance_label=instance_label)
        if not caps.remove_progress:
            return PlaybackActionResult(True, provider, instance_id, "remove_progress", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""), message="Cleanup skipped because remove progress is unsupported.")
        return adapter.remove_progress(config_view, record, instance_id=instance_id, instance_label=instance_label)

    def bulk(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        action = str(payload.get("action") or "").strip().lower()
        items_value = payload.get("items")
        items: list[Any] = items_value if isinstance(items_value, list) else []
        results: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, Mapping):
                results.append({"ok": False, "operation": action, "error_code": "invalid_item", "message": "Invalid selected item."})
                continue
            record_value = item.get("record")
            record = _as_mapping(record_value) if isinstance(record_value, Mapping) else item
            if action == "remove_progress":
                if not record.get("can_remove_progress"):
                    results.append({"ok": False, "provider": item.get("provider"), "instance_id": item.get("instance_id"), "operation": action, "remote_id": item.get("remote_id"), "canonical_key": item.get("canonical_key"), "error_code": "unsupported", "message": "Remove Progress is unsupported for this record."})
                    continue
                results.append(self.remove(item))
            elif action == "mark_watched":
                if not record.get("can_mark_watched"):
                    results.append({"ok": False, "provider": item.get("provider"), "instance_id": item.get("instance_id"), "operation": action, "remote_id": item.get("remote_id"), "canonical_key": item.get("canonical_key"), "error_code": "unsupported", "message": "Mark as Watched is unsupported for this record."})
                    continue
                results.append(self.mark_watched(item))
            elif action == "update_progress":
                progress, reason = _validated_progress_percent(payload.get("progress_percent"))
                if progress is None:
                    results.append({"ok": False, "provider": item.get("provider"), "instance_id": item.get("instance_id"), "operation": action, "remote_id": item.get("remote_id"), "canonical_key": item.get("canonical_key"), "error_code": "invalid_progress", "message": reason})
                    continue
                if not record.get("can_update_progress"):
                    results.append({"ok": False, "provider": item.get("provider"), "instance_id": item.get("instance_id"), "operation": action, "remote_id": item.get("remote_id"), "canonical_key": item.get("canonical_key"), "error_code": "unsupported", "message": "Edit Progress is unsupported for this record."})
                    continue
                update_item = dict(item)
                update_item["progress_percent"] = progress
                results.append(self.update_progress(update_item))
            else:
                results.append({"ok": False, "operation": action, "error_code": "unsupported_action", "message": "Unsupported bulk action."})
        successful = sum(1 for r in results if r.get("ok"))
        unsupported = sum(1 for r in results if r.get("error_code") == "unsupported")
        failed = sum(1 for r in results if not r.get("ok") and r.get("error_code") != "unsupported")
        LOG.info(f"bulk action completed action={action or 'unknown'} items={len(items)} successful={successful} failed={failed} unsupported={unsupported}")
        return {
            "results": [clean_mapping(r) for r in results],
            "successful": successful,
            "failed": failed,
            "skipped": 0,
            "unsupported": unsupported,
        }

    def update_progress(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        progress, reason = _validated_progress_percent(payload.get("progress_percent"))
        provider = str(payload.get("provider") or "").lower()
        instance_id = normalize_instance_id(payload.get("instance_id"))
        if progress is None:
            LOG.warn(f"update progress rejected provider={provider} instance={instance_id} reason={reason}")
            return PlaybackActionResult(False, provider, instance_id, "update_progress", error_code="invalid_progress", message=reason).to_dict()
        cfg = load_config()
        record_value = payload.get("record")
        record = _as_mapping(record_value) if isinstance(record_value, Mapping) else payload
        adapter, config_view, inst = self._adapter_for_action(cfg, provider, instance_id)
        if adapter is None:
            LOG.warn(f"update progress requested unknown provider={provider} instance={inst}")
            return PlaybackActionResult(False, provider, inst, "update_progress", error_code="unknown_provider", message="Unknown provider.").to_dict()
        if not record.get("can_update_progress"):
            LOG.warn(f"update progress unsupported provider={provider} instance={inst} remote_id={record.get('remote_id') or ''}")
            return PlaybackActionResult(False, provider, inst, "update_progress", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""), error_code="unsupported", message="Edit Progress is unsupported for this record.").to_dict()
        label = _instance_label(cfg, provider, inst)
        LOG.info(f"update progress requested provider={provider} instance={inst} remote_id={record.get('remote_id') or ''} progress={progress:g}")
        result = adapter.update_progress(config_view, record, progress, instance_id=inst, instance_label=label)
        if result.ok:
            self.invalidate(provider, inst)
            LOG.success(f"update progress completed provider={provider} instance={inst} remote_id={record.get('remote_id') or ''} progress={progress:g}")
        else:
            LOG.warn(f"update progress failed provider={provider} instance={inst} error={result.error_code or 'provider_error'}")
        return result.to_dict()


_SERVICE = PlaybackProgressService()


def get_service() -> PlaybackProgressService:
    return _SERVICE
