# /services/playback_progress/adapters/media_servers.py
# CrossWatch - Media Server Playback Progress Adapters
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any, Mapping, cast

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.metadata._meta_TMDB import TmdbProvider

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, clean_mapping, utc_now_iso
from .base import PlaybackProgressAdapter, metadata_rating, public_failure, rating_from_sources

try:
    from providers.sync._mod_EMBY import OPS as EMBY_OPS
except Exception:
    EMBY_OPS = None  # type: ignore[assignment]

try:
    from providers.sync._mod_JELLYFIN import OPS as JELLYFIN_OPS
except Exception:
    JELLYFIN_OPS = None  # type: ignore[assignment]

try:
    from providers.sync._mod_PLEX import OPS as PLEX_OPS, PLEXModule
    from providers.sync.plex._progress import _resolve_rating_key as _plex_resolve_rating_key
except Exception:
    PLEX_OPS = None  # type: ignore[assignment]
    PLEXModule = None  # type: ignore[assignment]
    _plex_resolve_rating_key = None  # type: ignore[assignment]


def _int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def _float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _first_str(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], value) if isinstance(value, Mapping) else {}


def _ms_to_seconds(value: Any) -> int | None:
    n = _int(value)
    return int(round(n / 1000.0)) if n and n > 0 else None


def _progress_percent(progress_ms: int | None, duration_ms: int | None) -> float | None:
    if progress_ms is None or duration_ms is None or duration_ms <= 0:
        return None
    return round((float(progress_ms) / float(duration_ms)) * 100.0, 3)


def _progress_ms_for_percent(record: Mapping[str, Any], progress_percent: float) -> tuple[int | None, str]:
    duration_seconds = _int(record.get("duration_seconds"))
    if duration_seconds is None:
        progress_item = _as_mapping(_as_mapping(record.get("provider_metadata")).get("progress_item"))
        duration_seconds = _ms_to_seconds(progress_item.get("duration_ms") or progress_item.get("duration"))
    if duration_seconds is None or duration_seconds <= 0:
        return None, "missing_duration"
    duration_ms = duration_seconds * 1000
    progress_ms = int(round((float(progress_percent) / 100.0) * float(duration_ms)))
    progress_ms = max(1, min(duration_ms - 1, progress_ms))
    return progress_ms, ""


def _history_item(record: Mapping[str, Any], provider: str) -> dict[str, Any]:
    meta = _as_mapping(record.get("provider_metadata"))
    history_value = meta.get("history_item")
    if isinstance(history_value, Mapping):
        out = clean_mapping(history_value)
        remote_id = _first_str(record.get("remote_id"))
        if remote_id:
            ids = out.get("ids")
            if not isinstance(ids, dict):
                ids = {}
                out["ids"] = ids
            ids[provider] = remote_id
        return clean_mapping(out)
    media_type = str(record.get("media_type") or "").lower()
    ids = clean_mapping(record.get("ids") if isinstance(record.get("ids"), Mapping) else {})
    remote_id = _first_str(record.get("remote_id"))
    if remote_id and provider in {"plex", "emby", "jellyfin"}:
        ids.setdefault(provider, remote_id)
    item: dict[str, Any] = {
        "type": "episode" if media_type in {"episode", "anime_episode"} else "movie",
        "title": record.get("episode_title") or record.get("title") or record.get("series_title"),
        "series_title": record.get("series_title"),
        "year": record.get("year"),
        "season": record.get("season"),
        "episode": record.get("episode"),
        "ids": ids,
    }
    show_ids = _as_mapping(meta.get("show_ids"))
    if show_ids:
        item["show_ids"] = clean_mapping(show_ids)
    return clean_mapping(item)


def _successful_write(result: Mapping[str, Any]) -> bool:
    if not bool(result.get("ok")):
        return False
    count = _int(result.get("count"))
    unresolved = result.get("unresolved")
    if count and count > 0:
        return True
    return count == 0 and isinstance(unresolved, list) and len(unresolved) == 0


def _has_metadata_ids(ids: Mapping[str, Any]) -> bool:
    return any(_first_str(ids.get(key)) for key in ("tmdb", "imdb", "tvdb"))


def _metadata_provider(config_view: Mapping[str, Any]) -> TmdbProvider | None:
    tmdb = _as_mapping(config_view.get("tmdb"))
    metadata = _as_mapping(config_view.get("metadata"))
    if not _first_str(tmdb.get("api_key"), metadata.get("tmdb_api_key")):
        return None
    cfg = dict(config_view)
    return TmdbProvider(lambda: cfg, lambda _cfg: None)


def _resolve_with_metadata(
    provider: TmdbProvider | None,
    *,
    entity: str,
    title: str,
    year: Any,
    ids: Mapping[str, Any],
) -> dict[str, Any]:
    if _has_metadata_ids(ids):
        return dict(ids)
    if provider is None or not title:
        return dict(ids)
    lookup_ids = {str(k): str(v) for k, v in ids.items() if v}
    lookup_ids["title"] = title
    if year:
        lookup_ids["year"] = str(year)
    try:
        resolved = provider.fetch(
            entity=entity,
            ids=lookup_ids,
            need={"ids": True, "poster": False, "backdrop": False},
        )
    except Exception:
        return dict(ids)
    resolved_ids = resolved.get("ids") if isinstance(resolved, Mapping) else None
    if not isinstance(resolved_ids, Mapping):
        return dict(ids)
    out = dict(ids)
    out.update({str(k): v for k, v in resolved_ids.items() if v})
    return out


class _MediaServerPlaybackAdapter(PlaybackProgressAdapter):
    provider = ""
    provider_label = ""
    ops: Any = None
    module_cls: Any = None

    def capabilities(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackCapabilities:
        configured = False
        reason = ""
        if self.ops is None:
            reason = f"{self.provider_label} playback support is unavailable in this installation."
        else:
            try:
                configured = bool(self.ops.is_configured(config_view))
            except Exception:
                configured = False
            if not configured:
                reason = f"{self.provider_label} is not connected for this instance."
        can_use = bool(configured and self.ops is not None)
        return PlaybackCapabilities(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            configured=configured,
            read=can_use,
            remove_progress=can_use,
            mark_watched=can_use,
            update_progress=can_use,
            bulk_remove_progress=can_use,
            bulk_mark_watched=can_use,
            bulk_update_progress=can_use,
            supports_movies=True,
            supports_episodes=True,
            supports_anime=False,
            reason=reason,
        )

    def list_progress(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str, force_refresh: bool = False) -> PlaybackListResult:
        if self.ops is None:
            return PlaybackListResult(ok=False, provider=self.provider, instance_id=instance_id, error_code="provider_unavailable", message=f"{self.provider_label} playback support is unavailable.", retryable=False)
        try:
            caps = self.capabilities(config_view, instance_id=instance_id, instance_label=instance_label)
            rows = self.ops.build_index(config_view, feature="progress")
            metadata_provider = _metadata_provider(config_view)
            items = [
                record
                for key, row in (rows or {}).items()
                if isinstance(row, Mapping)
                for record in [self._normalize(str(key), row, metadata_provider, instance_id, instance_label, caps)]
                if record is not None
            ]
            return PlaybackListResult(ok=True, provider=self.provider, instance_id=instance_id, items=items, refreshed_at=utc_now_iso())
        except Exception:
            return PlaybackListResult(ok=False, provider=self.provider, instance_id=instance_id, error_code="provider_error", message=f"{self.provider_label} playback request failed.", retryable=True)

    def _normalize(
        self,
        key: str,
        row: Mapping[str, Any],
        metadata_provider: TmdbProvider | None,
        instance_id: str,
        instance_label: str,
        caps: PlaybackCapabilities,
    ) -> PlaybackRecord | None:
        ids = clean_mapping(row.get("ids") if isinstance(row.get("ids"), Mapping) else {})
        remote_id = _first_str(row.get(f"{self.provider}_item_id"), row.get("_item_id"), row.get("ratingKey"), ids.get(self.provider), row.get("id"))
        if not remote_id:
            remote_id = key if key and not key.startswith("unknown:") else ""
        media_type = str(row.get("type") or "movie").strip().lower()
        if media_type not in {"movie", "episode", "anime_episode"}:
            media_type = "episode" if media_type in {"show", "season"} else "movie"
        title = _first_str(row.get("series_title") if media_type in {"episode", "anime_episode"} else None, row.get("title"))
        episode_title = _first_str(row.get("title")) if media_type in {"episode", "anime_episode"} else ""
        series_title = _first_str(row.get("series_title")) if media_type in {"episode", "anime_episode"} else ""
        show_ids = clean_mapping(row.get("show_ids") if isinstance(row.get("show_ids"), Mapping) else {})
        if media_type in {"episode", "anime_episode"}:
            show_ids = _resolve_with_metadata(metadata_provider, entity="tv", title=series_title or title, year=row.get("year"), ids=show_ids)
            if not _has_metadata_ids(ids) and _has_metadata_ids(show_ids):
                ids = dict(ids)
                for key_name in ("tmdb", "imdb", "tvdb"):
                    if show_ids.get(key_name):
                        ids.setdefault(key_name, show_ids.get(key_name))
        else:
            ids = _resolve_with_metadata(metadata_provider, entity="movie", title=title, year=row.get("year"), ids=ids)
        progress_ms = _int(row.get("progress_ms") or row.get("viewOffset") or row.get("view_offset"))
        duration_ms = _int(row.get("duration_ms") or row.get("duration"))
        progress = _progress_percent(progress_ms, duration_ms)
        duration_seconds = _ms_to_seconds(duration_ms)
        remaining_seconds = _ms_to_seconds((duration_ms - progress_ms) if duration_ms and progress_ms is not None else None)
        item = id_minimal(
            {
                "type": "episode" if media_type in {"episode", "anime_episode"} else "movie",
                "title": episode_title or title,
                "series_title": series_title,
                "year": row.get("year"),
                "season": row.get("season"),
                "episode": row.get("episode"),
                "ids": ids,
                "show_ids": show_ids,
            }
        )
        canonical = canonical_key(item) or key or ""
        rating_ids = show_ids if media_type in {"episode", "anime_episode"} and _has_metadata_ids(show_ids) else ids
        rating_title = (series_title or title) if media_type in {"episode", "anime_episode"} else title
        rating = rating_from_sources(row) or metadata_rating(metadata_provider, media_type=media_type, ids=rating_ids, title=rating_title, year=row.get("year"))
        return PlaybackRecord(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            remote_id=remote_id,
            canonical_key=canonical,
            media_type=media_type,
            title=title,
            episode_title=episode_title,
            series_title=series_title,
            season=_int(row.get("season")),
            episode=_int(row.get("episode")),
            year=_int(row.get("year")),
            ids=ids,
            progress_percent=progress,
            remaining_seconds=remaining_seconds,
            duration_seconds=duration_seconds,
            progress_at=_first_str(row.get("progress_at"), row.get("updated_at")) or None,
            updated_at=_first_str(row.get("progress_at"), row.get("updated_at")) or None,
            rating=rating,
            poster_url=_first_str(row.get("poster"), row.get("poster_url")),
            backdrop_url=_first_str(row.get("backdrop"), row.get("backdrop_url"), row.get("fanart")),
            can_remove_progress=caps.remove_progress,
            can_mark_watched=caps.mark_watched,
            can_update_progress=bool(caps.update_progress and duration_seconds),
            capability_messages=[] if caps.configured else [caps.reason],
            provider_metadata={
                "history_item": clean_mapping(item),
                "show_ids": clean_mapping(show_ids),
                "progress_item": clean_mapping(row),
            },
        )

    def remove_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackActionResult:
        if self.ops is None:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message=f"{self.provider_label} playback support is unavailable.", error_code="provider_unavailable")
        if self.provider == "plex":
            return self._remove_plex_progress(config_view, record, instance_id=instance_id)
        item = _history_item(record, self.provider)
        try:
            result = self.ops.remove(config_view, [item], feature="progress")
            ok = _successful_write(result)
            return PlaybackActionResult(
                ok=ok,
                provider=self.provider,
                instance_id=instance_id,
                operation="remove_progress",
                remote_id=str(record.get("remote_id") or ""),
                canonical_key=str(record.get("canonical_key") or ""),
                message=f"Playback record removed from {self.provider_label}." if ok else f"{self.provider_label} remove progress failed.",
                error_code="" if ok else "progress_remove_failed",
                playback_cleanup_result=clean_mapping(result),
            )
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message=f"{self.provider_label} remove progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

    def _remove_plex_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str) -> PlaybackActionResult:
        if PLEXModule is None:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Plex playback support is unavailable.", error_code="provider_unavailable")
        try:
            module = PLEXModule(config_view)
            item = _history_item(record, self.provider)
            remote_id = str(record.get("remote_id") or "").strip()
            rating_key = remote_id if remote_id.isdigit() else ""
            if not rating_key and _plex_resolve_rating_key is not None:
                rating_key = str(_plex_resolve_rating_key(module, item) or "")
            if not rating_key:
                return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Plex item could not be resolved.", error_code="not_found", remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))
            server = getattr(getattr(module, "client", None), "server", None)
            if not server:
                return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Plex Media Server is not available.", error_code="server_unavailable", retryable=True, remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))
            obj = server.fetchItem(int(rating_key))
            mark_unplayed = getattr(obj, "markUnplayed", None) or getattr(obj, "markUnwatched", None)
            if callable(mark_unplayed):
                mark_unplayed()
            else:
                server.query(f"/:/unscrobble?key={rating_key}&identifier=com.plexapp.plugins.library")
            return PlaybackActionResult(True, self.provider, instance_id, "remove_progress", remote_id=remote_id or rating_key, canonical_key=str(record.get("canonical_key") or ""), message="Playback record removed from Plex.")
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Plex remove progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

    def mark_watched(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
        watched_at: str | None = None,
    ) -> PlaybackActionResult:
        if self.ops is None:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="mark_watched", message=f"{self.provider_label} playback support is unavailable.", error_code="provider_unavailable")
        item = _history_item(record, self.provider)
        item["watched_at"] = watched_at or utc_now_iso()
        try:
            result = self.ops.add(config_view, [item], feature="history")
            ok = _successful_write(result)
            return PlaybackActionResult(
                ok=ok,
                provider=self.provider,
                instance_id=instance_id,
                operation="mark_watched",
                remote_id=str(record.get("remote_id") or ""),
                canonical_key=str(record.get("canonical_key") or ""),
                message=f"Marked watched on {self.provider_label}." if ok else f"{self.provider_label} mark watched failed.",
                error_code="" if ok else "history_failed",
                history_result=clean_mapping(result),
            )
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="mark_watched", message=f"{self.provider_label} mark watched failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

    def update_progress(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        progress_percent: float,
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackActionResult:
        if self.ops is None:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message=f"{self.provider_label} playback support is unavailable.", error_code="provider_unavailable")
        progress_ms, reason = _progress_ms_for_percent(record, progress_percent)
        if progress_ms is None:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message=f"{self.provider_label} update progress failed: {reason}.", error_code=reason or "invalid_record", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
        item = _history_item(record, self.provider)
        item["progress_ms"] = progress_ms
        item["progress_at"] = utc_now_iso()
        try:
            result = self.ops.add(config_view, [item], feature="progress")
            ok = _successful_write(result)
            return PlaybackActionResult(
                ok=ok,
                provider=self.provider,
                instance_id=instance_id,
                operation="update_progress",
                remote_id=str(record.get("remote_id") or ""),
                canonical_key=str(record.get("canonical_key") or ""),
                message=f"Progress updated on {self.provider_label} to {progress_percent:g}%." if ok else f"{self.provider_label} update progress failed.",
                error_code="" if ok else "progress_update_failed",
                playback_cleanup_result=clean_mapping(result),
            )
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message=f"{self.provider_label} update progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))


class PlexPlaybackAdapter(_MediaServerPlaybackAdapter):
    provider = "plex"
    provider_label = "Plex"
    ops = PLEX_OPS
    module_cls = PLEXModule


class EmbyPlaybackAdapter(_MediaServerPlaybackAdapter):
    provider = "emby"
    provider_label = "Emby"
    ops = EMBY_OPS


class JellyfinPlaybackAdapter(_MediaServerPlaybackAdapter):
    provider = "jellyfin"
    provider_label = "Jellyfin"
    ops = JELLYFIN_OPS
