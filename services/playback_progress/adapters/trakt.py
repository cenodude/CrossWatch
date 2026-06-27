# /services/playback_progress/adapters/trakt.py
# CrossWatch - Trakt Playback Progress Adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.sync._mod_TRAKT import OPS as TRAKT_OPS, TRAKTModule

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, clean_mapping, utc_now_iso
from .base import PlaybackProgressAdapter, public_failure, rating_from_sources


def _num(value: Any) -> int | None:
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


def _trakt_image_url(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("//"):
        return f"https:{text}"
    if text.lower().startswith("media.trakt.tv/"):
        return f"https://{text}"
    return text


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


def _media_from_row(row: Mapping[str, Any]) -> tuple[str, Mapping[str, Any], Mapping[str, Any]]:
    row_type = str(row.get("type") or "").lower()
    movie_value = row.get("movie")
    episode_value = row.get("episode")
    show_value = row.get("show")
    movie: Mapping[str, Any] | None = movie_value if isinstance(movie_value, Mapping) else None
    episode: Mapping[str, Any] | None = episode_value if isinstance(episode_value, Mapping) else None
    show_payload: Mapping[str, Any] = show_value if isinstance(show_value, Mapping) else {}
    if movie is not None:
        return "movie", movie, {}
    if episode is not None:
        return "episode", episode, show_payload
    if row_type == "movie":
        return "movie", row, {}
    return "episode", episode or row, show_payload


def _history_item_from_record(record: Mapping[str, Any]) -> dict[str, Any]:
    meta = record.get("provider_metadata") if isinstance(record.get("provider_metadata"), Mapping) else {}
    hist = meta.get("history_item") if isinstance(meta, Mapping) and isinstance(meta.get("history_item"), Mapping) else None
    if hist:
        out = dict(hist)
    else:
        media_type = str(record.get("media_type") or "").lower()
        ids = clean_mapping(record.get("ids") if isinstance(record.get("ids"), Mapping) else {})
        out = {
            "type": "episode" if media_type in {"episode", "anime_episode"} else "movie",
            "title": record.get("episode_title") or record.get("title") or record.get("series_title"),
            "series_title": record.get("series_title") or "",
            "year": record.get("year"),
            "season": record.get("season"),
            "episode": record.get("episode"),
            "ids": ids,
        }
        show_ids = meta.get("show_ids") if isinstance(meta, Mapping) and isinstance(meta.get("show_ids"), Mapping) else None
        if show_ids:
            out["show_ids"] = clean_mapping(show_ids)
    return clean_mapping(out)


def _progress_body_from_record(record: Mapping[str, Any], progress_percent: float) -> dict[str, Any]:
    item = _history_item_from_record(record)
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
    episode: dict[str, Any] = {}
    if ids:
        episode["ids"] = ids
    if item.get("season") is not None:
        episode["season"] = item.get("season")
    if item.get("episode") is not None:
        episode["number"] = item.get("episode")
    if item.get("title"):
        episode["title"] = item.get("title")
    if show_ids:
        body["show"] = {"ids": show_ids}
    body["episode"] = episode
    return clean_mapping(body)


def _module(config_view: Mapping[str, Any]) -> TRAKTModule:
    return TRAKTModule(config_view, connect=False)


class TraktPlaybackAdapter(PlaybackProgressAdapter):
    provider = "trakt"
    provider_label = "Trakt"

    def capabilities(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackCapabilities:
        configured = False
        reason = ""
        try:
            trakt_block = config_view.get("trakt")
            block: Mapping[str, Any] = trakt_block if isinstance(trakt_block, Mapping) else {}
            configured = bool(TRAKT_OPS.is_configured(config_view) and str(block.get("client_id") or block.get("api_key") or "").strip())
        except Exception:
            configured = False
        if not configured:
            reason = "Trakt is not connected for this instance or is missing its client id."
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
            supports_anime=False,
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
            module = _module(config_view)
            rows: list[Mapping[str, Any]] = []
            seen_ids: set[str] = set()
            first_error: tuple[int, str] | None = None
            for kind in ("movies", "episodes"):
                response = module.client.get(f"{module.client.BASE}/sync/playback/{kind}", params={"extended": "full,images"})
                if not (200 <= response.status_code < 300):
                    if first_error is None:
                        first_error = (int(response.status_code), kind)
                    continue
                data = response.json() if (response.text or "").strip() else []
                if not isinstance(data, list):
                    continue
                for row in data:
                    if not isinstance(row, Mapping):
                        continue
                    row_id = str(row.get("id") or "").strip()
                    if row_id and row_id in seen_ids:
                        continue
                    if row_id:
                        seen_ids.add(row_id)
                    rows.append(row)
            if first_error and not rows:
                status, kind = first_error
                return PlaybackListResult(
                    ok=False,
                    provider=self.provider,
                    instance_id=instance_id,
                    error_code=f"http:{status}",
                    message=f"Trakt playback {kind} request failed.",
                    remote_status=status,
                    retryable=status in {408, 429, 500, 502, 503, 504},
                )
            caps = self.capabilities(config_view, instance_id=instance_id, instance_label=instance_label)
            items = [self._normalize(row, instance_id, instance_label, caps) for row in rows]
            return PlaybackListResult(ok=True, provider=self.provider, instance_id=instance_id, items=[x for x in items if x], refreshed_at=utc_now_iso())
        except Exception:
            return PlaybackListResult(
                ok=False,
                provider=self.provider,
                instance_id=instance_id,
                error_code="provider_error",
                message="Trakt playback request failed.",
                retryable=True,
            )

    def _normalize(
        self,
        row: Mapping[str, Any],
        instance_id: str,
        instance_label: str,
        caps: PlaybackCapabilities,
    ) -> PlaybackRecord | None:
        remote = str(row.get("id") or "").strip()
        if not remote:
            return None
        media_type, payload, show = _media_from_row(row)
        ids = clean_mapping(payload.get("ids") if isinstance(payload.get("ids"), Mapping) else {})
        show_ids = clean_mapping(show.get("ids") if isinstance(show.get("ids"), Mapping) else {})
        if media_type == "movie":
            title = str(payload.get("title") or row.get("title") or "").strip()
            item = id_minimal({"type": "movie", "title": title, "year": payload.get("year"), "ids": ids})
            key = canonical_key(item) or ""
            history_item = dict(item)
            episode_title = ""
            series_title = ""
        else:
            episode_title = str(payload.get("title") or "").strip()
            series_title = str(show.get("title") or row.get("series_title") or "").strip()
            title = series_title or episode_title
            item = id_minimal(
                {
                    "type": "episode",
                    "title": episode_title or series_title,
                    "series_title": series_title,
                    "year": show.get("year") or payload.get("year"),
                    "season": payload.get("season"),
                    "episode": payload.get("number") or payload.get("episode"),
                    "ids": ids,
                    "show_ids": show_ids,
                }
            )
            key = canonical_key(item) or ""
            history_item = dict(item)
        images_payload = payload.get("images")
        images: Mapping[str, Any] = images_payload if isinstance(images_payload, Mapping) else {}
        poster = ""
        backdrop = ""
        try:
            poster = _trakt_image_url(((images.get("poster") or []) or [""])[0])
            backdrop = _trakt_image_url(((images.get("fanart") or images.get("background") or []) or [""])[0])
        except Exception:
            poster = ""
            backdrop = ""
        progress = _float(row.get("progress"))
        duration = _duration_seconds_from_sources(payload, show, row)
        updated = str(row.get("paused_at") or row.get("updated_at") or "").strip() or None
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
            season=_num(payload.get("season")),
            episode=_num(payload.get("number") or payload.get("episode")),
            year=_num(payload.get("year") or show.get("year")),
            ids=ids,
            progress_percent=progress,
            remaining_seconds=_remaining_seconds(duration, progress),
            duration_seconds=duration,
            progress_at=updated,
            updated_at=updated,
            rating=rating_from_sources(row, payload, show),
            poster_url=poster,
            backdrop_url=backdrop,
            can_remove_progress=caps.remove_progress,
            can_mark_watched=caps.mark_watched,
            can_update_progress=caps.update_progress,
            capability_messages=[] if caps.configured else [caps.reason],
            provider_metadata={"history_item": history_item, "show_ids": show_ids},
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
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Missing Trakt playback record id.", error_code="missing_remote_id")
        try:
            module = _module(config_view)
            response = module.client.delete(f"{module.client.BASE}/sync/playback/{remote_id}")
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
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Trakt remove progress failed.", error_code=f"http:{response.status_code}", remote_status=response.status_code, retryable=response.status_code in {408, 429, 500, 502, 503, 504}, remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Trakt remove progress failed.", retryable=True, remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))

    def mark_watched(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
        watched_at: str | None = None,
    ) -> PlaybackActionResult:
        item = _history_item_from_record(record)
        if watched_at:
            item["watched_at"] = watched_at
        try:
            result = _module(config_view).add("history", [item])
            ok = bool(result.get("ok"))
            return PlaybackActionResult(
                ok=ok,
                provider=self.provider,
                instance_id=instance_id,
                operation="mark_watched",
                remote_id=str(record.get("remote_id") or ""),
                canonical_key=str(record.get("canonical_key") or ""),
                message="Marked watched on Trakt." if ok else "Trakt mark watched failed.",
                error_code="" if ok else "history_failed",
                history_result=clean_mapping(result),
            )
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="mark_watched", message="Trakt mark watched failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

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
            module = _module(config_view)
            response = module.client.post(f"{module.client.BASE}/scrobble/pause", json=_progress_body_from_record(record, progress_percent))
            if response.status_code in {200, 201, 202}:
                return PlaybackActionResult(
                    ok=True,
                    provider=self.provider,
                    instance_id=instance_id,
                    operation="update_progress",
                    remote_id=str(record.get("remote_id") or ""),
                    canonical_key=str(record.get("canonical_key") or ""),
                    message=f"Progress updated on Trakt to {progress_percent:g}%.",
                    remote_status=response.status_code,
                )
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="Trakt update progress failed.", error_code=f"http:{response.status_code}", remote_status=response.status_code, retryable=response.status_code in {408, 429, 500, 502, 503, 504}, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="Trakt update progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
