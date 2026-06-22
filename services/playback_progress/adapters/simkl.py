# /services/playback_progress/adapters/simkl.py
# CrossWatch - Simkl Playback Progress Adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import os
from typing import Any, Mapping, cast

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.sync._mod_SIMKL import OPS as SIMKL_OPS, SIMKLModule, __VERSION__ as SIMKL_MODULE_VERSION
from providers.sync.simkl._common import build_headers

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, clean_mapping, utc_now_iso
from .base import PlaybackProgressAdapter, public_failure, rating_from_sources, tmdb_metadata_provider


def _int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
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
        s = str(value).strip()
        if s:
            return s
    return ""


def _duration_seconds_from_sources(*sources: Mapping[str, Any]) -> int | None:
    for source in sources:
        for key in ("runtime_ms", "duration_ms"):
            value = _float(source.get(key))
            if value and value > 0:
                return int(round(value / 1000.0))
        for key in ("runtime_seconds", "duration_seconds"):
            value = _float(source.get(key))
            if value and value > 0:
                return int(round(value))
        for key in ("runtime", "runtime_minutes", "duration_minutes"):
            value = _float(source.get(key))
            if value and value > 0:
                return int(round(value * 60.0))
        duration = _float(source.get("duration"))
        if duration and duration > 0:
            return int(round(duration * 60.0 if duration <= 600 else duration))
    return None


def _remaining_seconds(duration_seconds: int | None, progress_percent: float | None) -> int | None:
    if duration_seconds is None or progress_percent is None or duration_seconds <= 0:
        return None
    remaining = int(round(duration_seconds * max(0.0, 100.0 - min(progress_percent, 100.0)) / 100.0))
    return remaining if remaining > 0 else None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], value) if isinstance(value, Mapping) else {}


def _api_params(api_key: str, **extra: Any) -> dict[str, Any]:
    version = str(os.getenv("APP_VERSION") or SIMKL_MODULE_VERSION or "1.0").strip()
    params: dict[str, Any] = {
        "client_id": api_key,
        "app-name": "crosswatch",
        "app-version": version,
    }
    params.update({k: v for k, v in extra.items() if v is not None})
    return params


def _pick_container(row: Mapping[str, Any]) -> tuple[str, Mapping[str, Any], Mapping[str, Any]]:
    row_type = str(row.get("type") or "").lower()
    episode = _as_mapping(row.get("episode"))
    for key, media_type in (("movie", "movie"), ("show", "episode"), ("anime", "anime_episode")):
        container_value = row.get(key)
        if isinstance(container_value, Mapping):
            return media_type, cast(Mapping[str, Any], container_value), episode
    if row_type in {"movie", "movies"}:
        return "movie", row, {}
    if row_type in {"anime", "anime_episode"}:
        anime = _as_mapping(row.get("anime"))
        return "anime_episode", anime or row, episode
    show = _as_mapping(row.get("show"))
    return "episode", show or row, episode


def _image_url(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("poster", "medium", "large", "small", "url"):
            s = _first_str(value.get(key))
            if s:
                return s
    return ""


def _history_item(record: Mapping[str, Any]) -> dict[str, Any]:
    meta = _as_mapping(record.get("provider_metadata"))
    history_value = meta.get("history_item")
    hist = history_value if isinstance(history_value, Mapping) else None
    if hist:
        return clean_mapping(hist)
    media_type = str(record.get("media_type") or "").lower()
    return clean_mapping(
        {
            "type": "episode" if media_type in {"episode", "anime_episode"} else "movie",
            "title": record.get("episode_title") or record.get("title") or record.get("series_title"),
            "series_title": record.get("series_title"),
            "year": record.get("year"),
            "season": record.get("season"),
            "episode": record.get("episode"),
            "ids": record.get("ids") if isinstance(record.get("ids"), Mapping) else {},
            "show_ids": _as_mapping(meta.get("show_ids")),
        }
    )


def _progress_body_from_record(record: Mapping[str, Any], progress_percent: float) -> dict[str, Any]:
    item = _history_item(record)
    media_type = str(record.get("media_type") or item.get("type") or "").lower()
    ids = clean_mapping(item.get("ids") if isinstance(item.get("ids"), Mapping) else {})
    body: dict[str, Any] = {"progress": progress_percent}
    if media_type == "movie":
        movie: dict[str, Any] = {}
        if ids:
            movie["ids"] = ids
        if item.get("title"):
            movie["title"] = item.get("title")
        if item.get("year") is not None:
            movie["year"] = item.get("year")
        body["movie"] = movie
        return clean_mapping(body)
    show_ids = clean_mapping(item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else {})
    parent = "anime" if media_type == "anime_episode" else "show"
    episode: dict[str, Any] = {}
    if ids:
        episode["ids"] = ids
    if item.get("season") is not None:
        episode["season"] = item.get("season")
    if item.get("episode") is not None:
        episode["number"] = item.get("episode")
    if show_ids:
        body[parent] = {"ids": show_ids}
    body["episode"] = episode
    return clean_mapping(body)


def _metadata_rating_duration(
    provider: Any,
    *,
    media_type: str,
    ids: Mapping[str, Any],
    title: str,
    year: Any = None,
    season: int | None = None,
    episode: int | None = None,
) -> tuple[float | None, int | None]:
    if provider is None:
        return None, None
    lookup_ids = {str(k): str(v) for k, v in ids.items() if v not in (None, "", 0, False)}
    if title:
        lookup_ids["title"] = str(title)
    if year:
        lookup_ids["year"] = str(year)
    if not lookup_ids.get("tmdb") and not lookup_ids.get("imdb") and not lookup_ids.get("title"):
        return None, None
    try:
        detail = provider.fetch(
            entity="movie" if media_type == "movie" else "tv",
            ids=lookup_ids,
            need={"poster": False, "backdrop": False, "ids": True, "score": True, "runtime_minutes": True},
        )
    except Exception:
        return None, None
    if not isinstance(detail, Mapping):
        return None, None
    duration = _int(detail.get("runtime_minutes"))
    if duration is None and media_type != "movie" and season is not None and episode is not None:
        ids_value = detail.get("ids")
        detail_ids = ids_value if isinstance(ids_value, Mapping) else {}
        tmdb = _first_str(detail_ids.get("tmdb"), lookup_ids.get("tmdb"))
        fetch = getattr(provider, "_get", None)
        if callable(fetch) and tmdb:
            try:
                episode_detail = fetch(f"https://api.themoviedb.org/3/tv/{tmdb}/season/{int(season)}/episode/{int(episode)}")
            except Exception:
                episode_detail = {}
            if isinstance(episode_detail, Mapping):
                duration = _int(episode_detail.get("runtime"))
    return rating_from_sources(detail), (duration * 60 if duration else None)


class SimklPlaybackAdapter(PlaybackProgressAdapter):
    provider = "simkl"
    provider_label = "SIMKL"

    def capabilities(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackCapabilities:
        configured = False
        try:
            block = _as_mapping(config_view.get("simkl"))
            configured = bool(
                SIMKL_OPS.is_configured(config_view)
                and str(block.get("api_key") or block.get("client_id") or "").strip()
            )
        except Exception:
            configured = False
        reason = "" if configured else "SIMKL is not connected for this instance or is missing its API key."
        return PlaybackCapabilities(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            configured=configured,
            read=configured,
            remove_progress=configured,
            mark_watched=configured,
            update_progress=configured,
            bulk_remove_progress=configured,
            bulk_mark_watched=configured,
            bulk_update_progress=configured,
            supports_movies=configured,
            supports_episodes=configured,
            supports_anime=configured,
            reason=reason,
        )

    def list_progress(
        self,
        config_view: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
        force_refresh: bool = False,
    ) -> PlaybackListResult:
        try:
            module = SIMKLModule(config_view)
            headers = build_headers(
                {"simkl": {"api_key": module.cfg.api_key, "access_token": module.cfg.access_token}},
                force_refresh=force_refresh,
            )
            caps = self.capabilities(config_view, instance_id=instance_id, instance_label=instance_label)
            response = module.client._request(
                "GET",
                f"{module.client.BASE}/sync/playback",
                headers=headers,
                params=_api_params(module.cfg.api_key, limit=100, hide_watched="true"),
            )
            if not (200 <= response.status_code < 300):
                detail = str(response.text or "").strip()
                status = int(response.status_code)
                message = "SIMKL playback request failed."
                if detail:
                    message = f"{message} {detail}"
                return PlaybackListResult(
                    ok=False,
                    provider=self.provider,
                    instance_id=instance_id,
                    error_code=f"http:{status}",
                    message=message,
                    remote_status=status,
                    retryable=status in {408, 429, 500, 502, 503, 504},
                )
            data = response.json() if (response.text or "").strip() else []
            rows: list[Mapping[str, Any]] = []
            if isinstance(data, list):
                rows.extend([r for r in data if isinstance(r, Mapping)])
            elif isinstance(data, Mapping):
                for key in ("items", "playback", "movies", "episodes", "shows", "anime"):
                    bucket = data.get(key)
                    if isinstance(bucket, list):
                        rows.extend([r for r in bucket if isinstance(r, Mapping)])
            metadata = tmdb_metadata_provider(config_view)
            items = [self._normalize(row, instance_id, instance_label, caps, metadata) for row in rows]
            return PlaybackListResult(ok=True, provider=self.provider, instance_id=instance_id, items=[x for x in items if x], refreshed_at=utc_now_iso())
        except Exception:
            return PlaybackListResult(ok=False, provider=self.provider, instance_id=instance_id, error_code="provider_error", message="SIMKL playback request failed.", retryable=True)

    def _normalize(
        self,
        row: Mapping[str, Any],
        instance_id: str,
        instance_label: str,
        caps: PlaybackCapabilities,
        metadata: Any,
    ) -> PlaybackRecord | None:
        remote = _first_str(row.get("id"), row.get("playback_id"), row.get("session_id"))
        if not remote:
            return None
        media_type, container, episode = _pick_container(row)
        container_ids = clean_mapping(container.get("ids") if isinstance(container.get("ids"), Mapping) else row.get("ids") if isinstance(row.get("ids"), Mapping) else {})
        episode_ids = clean_mapping(episode.get("ids") if isinstance(episode.get("ids"), Mapping) else {})
        if media_type == "movie":
            title = _first_str(container.get("title"), row.get("title"))
            item = id_minimal({"type": "movie", "title": title, "year": container.get("year") or row.get("year"), "ids": container_ids})
            history_item = dict(item)
            key = canonical_key(item) or ""
            series_title = ""
            episode_title = ""
            season = None
            episode_no = None
        else:
            series_title = _first_str(container.get("title"), row.get("series_title"), row.get("show_title"))
            episode_title = _first_str(episode.get("title"), row.get("episode_title"))
            season = _int(episode.get("season") or row.get("season") or row.get("season_number"))
            episode_no = _int(episode.get("episode") or episode.get("number") or row.get("episode") or row.get("episode_number"))
            item = id_minimal(
                {
                    "type": "episode",
                    "title": episode_title or series_title,
                    "series_title": series_title,
                    "year": container.get("year") or row.get("year"),
                    "season": season,
                    "episode": episode_no,
                    "ids": episode_ids or container_ids,
                    "show_ids": container_ids,
                }
            )
            history_item = dict(item)
            key = canonical_key(item) or ""
            title = series_title or episode_title
        poster = _image_url(row.get("poster") or container.get("poster") or container.get("poster_url") or container.get("image"))
        backdrop = _image_url(row.get("backdrop") or row.get("fanart") or container.get("fanart") or container.get("backdrop"))
        progress = _float(row.get("percent") or row.get("progress") or row.get("progress_percent"))
        updated = _first_str(row.get("paused_at"), row.get("watched_at"), row.get("updated_at"), row.get("date"), row.get("last_watched_at")) or None
        remaining = _int(row.get("remaining") or row.get("remaining_seconds"))
        duration = _int(row.get("runtime_seconds") or row.get("duration_seconds")) or _duration_seconds_from_sources(row, container, episode)
        rating_ids = container_ids if media_type == "movie" else (container_ids or episode_ids)
        rating_title = title if media_type == "movie" else series_title or title
        rating = rating_from_sources(row, container, episode)
        if duration is None or rating is None:
            meta_rating, meta_duration = _metadata_rating_duration(
                metadata,
                media_type=media_type,
                ids=rating_ids,
                title=rating_title,
                year=container.get("year") or row.get("year"),
                season=season,
                episode=episode_no,
            )
            if duration is None:
                duration = meta_duration
            if rating is None:
                rating = meta_rating
        if remaining is None and _int(row.get("remaining_mins")) is not None:
            remaining = int(_int(row.get("remaining_mins")) or 0) * 60
        if remaining is None:
            remaining = _remaining_seconds(duration, progress)
        return PlaybackRecord(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            remote_id=remote,
            canonical_key=key,
            media_type=media_type,
            title=title,
            episode_title=episode_title,
            series_title=series_title,
            season=season,
            episode=episode_no,
            year=_int(container.get("year") or row.get("year")),
            ids=episode_ids or container_ids,
            progress_percent=progress,
            remaining_seconds=remaining,
            duration_seconds=duration,
            progress_at=updated,
            updated_at=updated,
            source_app=_first_str(row.get("app"), row.get("source_app"), row.get("application")),
            source_device=_first_str(row.get("device"), row.get("source_device")),
            rating=rating,
            poster_url=poster,
            backdrop_url=backdrop,
            can_remove_progress=caps.remove_progress,
            can_mark_watched=caps.mark_watched,
            can_update_progress=caps.update_progress,
            capability_messages=[] if caps.configured else [caps.reason],
            provider_metadata={"history_item": history_item, "show_ids": container_ids},
        )

    def remove_progress(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackActionResult:
        remote_id = str(record.get("remote_id") or "").strip()
        if not remote_id:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Missing SIMKL playback record id.", error_code="missing_remote_id")
        try:
            module = SIMKLModule(config_view)
            headers = build_headers({"simkl": {"api_key": module.cfg.api_key, "access_token": module.cfg.access_token}}, force_refresh=True)
            response = module.client._request(
                "DELETE",
                f"{module.client.BASE}/sync/playback/{remote_id}",
                headers=headers,
                params=_api_params(module.cfg.api_key),
            )
            if response.status_code in {200, 202, 204, 404}:
                return PlaybackActionResult(
                    ok=True,
                    provider=self.provider,
                    instance_id=instance_id,
                    operation="remove_progress",
                    remote_id=remote_id,
                    canonical_key=str(record.get("canonical_key") or ""),
                    message="Playback record removed.",
                    remote_status=response.status_code,
                )
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="SIMKL remove progress failed.", error_code=f"http:{response.status_code}", remote_status=response.status_code, retryable=response.status_code in {408, 429, 500, 502, 503, 504}, remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="SIMKL remove progress failed.", retryable=True, remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))

    def mark_watched(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
        watched_at: str | None = None,
    ) -> PlaybackActionResult:
        item = _history_item(record)
        if watched_at:
            item["watched_at"] = watched_at
        try:
            result = SIMKL_OPS.add(config_view, [item], feature="history")
            ok = bool(result.get("ok"))
            return PlaybackActionResult(
                ok=ok,
                provider=self.provider,
                instance_id=instance_id,
                operation="mark_watched",
                remote_id=str(record.get("remote_id") or ""),
                canonical_key=str(record.get("canonical_key") or ""),
                message="Marked watched on SIMKL." if ok else "SIMKL mark watched failed.",
                error_code="" if ok else "history_failed",
                history_result=clean_mapping(result),
            )
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="mark_watched", message="SIMKL mark watched failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

    def update_progress(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        progress_percent: float,
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackActionResult:
        try:
            module = SIMKLModule(config_view)
            headers = build_headers({"simkl": {"api_key": module.cfg.api_key, "access_token": module.cfg.access_token}}, force_refresh=True)
            response = module.client._request(
                "POST",
                f"{module.client.BASE}/scrobble/pause",
                headers=headers,
                params=_api_params(module.cfg.api_key),
                json=_progress_body_from_record(record, progress_percent),
            )
            if response.status_code in {200, 201, 202}:
                return PlaybackActionResult(
                    ok=True,
                    provider=self.provider,
                    instance_id=instance_id,
                    operation="update_progress",
                    remote_id=str(record.get("remote_id") or ""),
                    canonical_key=str(record.get("canonical_key") or ""),
                    message=f"Progress updated on SIMKL to {progress_percent:g}%.",
                    remote_status=response.status_code,
                )
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="SIMKL update progress failed.", error_code=f"http:{response.status_code}", remote_status=response.status_code, retryable=response.status_code in {408, 429, 500, 502, 503, 504}, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="SIMKL update progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
