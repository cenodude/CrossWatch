# /services/playback_progress/adapters/publicmetadb.py
# CrossWatch - PublicMetaDB Playback Progress Adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any, Mapping, cast

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.metadata._meta_TMDB import TmdbProvider
from providers.sync._mod_PUBLICMETADB import OPS as PUBLICMETADB_OPS, PUBLICMETADBModule

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, clean_mapping, utc_now_iso
from .base import PlaybackProgressAdapter, metadata_rating, public_failure, rating_from_sources


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
        text = str(value).strip()
        if text:
            return text
    return ""


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], value) if isinstance(value, Mapping) else {}


def _nested_maps(row: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = [row]
    for key in ("item", "media", "metadata", "movie", "show", "series", "episode"):
        nested = row.get(key)
        if isinstance(nested, Mapping):
            out.append(nested)
    return out


def _first_nested_str(row: Mapping[str, Any], *keys: str) -> str:
    for source in _nested_maps(row):
        for key in keys:
            value = source.get(key)
            if isinstance(value, Mapping):
                value = value.get("title") or value.get("name") or value.get("original_title") or value.get("original_name")
            text = _first_str(value)
            if text:
                return text
    return ""


def _first_nested_int(row: Mapping[str, Any], *keys: str) -> int | None:
    for source in _nested_maps(row):
        ids = source.get("ids")
        if isinstance(ids, Mapping):
            for key in keys:
                value = _int(ids.get(key))
                if value is not None:
                    return value
        for key in keys:
            value = _int(source.get(key))
            if value is not None:
                return value
    return None


def _metadata_provider(config_view: Mapping[str, Any]) -> TmdbProvider | None:
    tmdb = _as_mapping(config_view.get("tmdb"))
    metadata = _as_mapping(config_view.get("metadata"))
    if not _first_str(tmdb.get("api_key"), metadata.get("tmdb_api_key")):
        return None
    cfg = dict(config_view)
    return TmdbProvider(lambda: cfg, lambda _cfg: None)


def _metadata_title(
    provider: TmdbProvider | None,
    *,
    media_type: str,
    tmdb: int | None,
) -> tuple[str, int | None, int | None]:
    if provider is None or tmdb is None:
        return "", None, None
    try:
        detail = provider.fetch(
            entity="movie" if media_type == "movie" else "tv",
            ids={"tmdb": str(tmdb)},
            need={"poster": False, "backdrop": False, "ids": False},
        )
    except Exception:
        return "", None, None
    if not isinstance(detail, Mapping):
        return "", None, None
    runtime_minutes = _int(detail.get("runtime_minutes"))
    return _first_str(detail.get("title")), _int(detail.get("year")), runtime_minutes * 60 if runtime_minutes else None


def _rows(data: Any) -> list[Mapping[str, Any]]:
    if isinstance(data, list):
        return [row for row in data if isinstance(row, Mapping)]
    if not isinstance(data, Mapping):
        return []
    for key in ("items", "results", "data"):
        value = data.get(key)
        if isinstance(value, list):
            return [row for row in value if isinstance(row, Mapping)]
    return []


def _ms_to_seconds(value: Any) -> int | None:
    ms = _int(value)
    if ms is None or ms <= 0:
        return None
    return max(1, round(ms / 1000))


def _seconds_to_ms(value: Any) -> int | None:
    seconds = _int(value)
    if seconds is None or seconds <= 0:
        return None
    return int(seconds) * 1000


def _history_item(record: Mapping[str, Any]) -> dict[str, Any]:
    meta = _as_mapping(record.get("provider_metadata"))
    history_value = meta.get("history_item")
    hist = history_value if isinstance(history_value, Mapping) else None
    if hist:
        return clean_mapping(hist)

    media_type = str(record.get("media_type") or "").lower()
    ids = clean_mapping(record.get("ids") if isinstance(record.get("ids"), Mapping) else {})
    item: dict[str, Any] = {
        "type": "episode" if media_type in {"episode", "anime_episode"} else "movie",
        "ids": ids,
        "title": record.get("episode_title") or record.get("title") or record.get("series_title"),
        "year": record.get("year"),
    }
    if item["type"] == "episode":
        item["show_ids"] = clean_mapping(_as_mapping(meta.get("show_ids"))) or ids
        item["series_title"] = record.get("series_title")
        item["season"] = record.get("season")
        item["episode"] = record.get("episode")
    return clean_mapping(item)


def _progress_payload_from_record(record: Mapping[str, Any], progress_percent: float) -> tuple[dict[str, Any] | None, str]:
    meta = _as_mapping(record.get("provider_metadata"))
    resume = _as_mapping(meta.get("resume_item"))
    runtime_ms = _int(resume.get("runtime_ms") or resume.get("runtimeMs") or resume.get("duration_ms") or resume.get("durationMs")) or _seconds_to_ms(record.get("duration_seconds"))
    if runtime_ms is None:
        return None, "missing_duration"
    position_ms = max(1, min(runtime_ms - 1, round((float(progress_percent) / 100.0) * float(runtime_ms))))
    media_type = str(record.get("media_type") or "").lower()
    ids = _as_mapping(record.get("ids"))
    show_ids = _as_mapping(meta.get("show_ids")) or ids
    tmdb = _int(show_ids.get("tmdb") if media_type in {"episode", "anime_episode"} else ids.get("tmdb"))
    if tmdb is None:
        tmdb = _int(resume.get("tmdb_id") or resume.get("tmdb"))
    if tmdb is None:
        return None, "missing_tmdb_id"
    if media_type in {"episode", "anime_episode"}:
        season = _int(record.get("season") or resume.get("season") or resume.get("season_number"))
        episode = _int(record.get("episode") or resume.get("episode") or resume.get("episode_number"))
        if season is None or episode is None:
            return None, "missing_episode_numbers"
        return {
            "tmdb_id": int(tmdb),
            "media_type": "tv",
            "season": int(season),
            "episode": int(episode),
            "position_ms": int(position_ms),
            "runtime_ms": int(runtime_ms),
        }, ""
    return {
        "tmdb_id": int(tmdb),
        "media_type": "movie",
        "position_ms": int(position_ms),
        "runtime_ms": int(runtime_ms),
    }, ""


class PublicMetaDBPlaybackAdapter(PlaybackProgressAdapter):
    provider = "publicmetadb"
    provider_label = "PublicMetaDB"

    def capabilities(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackCapabilities:
        configured = False
        try:
            configured = bool(PUBLICMETADB_OPS.is_configured(config_view))
        except Exception:
            configured = False
        reason = "" if configured else "PublicMetaDB is not connected for this instance."
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
            module = PUBLICMETADBModule(config_view)
            page = 1
            per_page = max(1, min(int(getattr(module.cfg, "progress_per_page", 100) or 100), 500))
            max_pages = max(1, int(getattr(module.cfg, "progress_max_pages", 1000) or 1000))
            rows: list[Mapping[str, Any]] = []
            while page <= max_pages:
                response = module.client.get("/api/external/resume", params={"page": page, "perPage": per_page})
                if not (200 <= response.status_code < 300):
                    return PlaybackListResult(
                        ok=False,
                        provider=self.provider,
                        instance_id=instance_id,
                        error_code=f"http:{response.status_code}",
                        message="PublicMetaDB resume request failed.",
                        remote_status=response.status_code,
                        retryable=response.status_code in {408, 429, 500, 502, 503, 504},
                    )
                data = module.client.safe_json(response)
                page_rows = _rows(data)
                if not page_rows:
                    break
                rows.extend(page_rows)
                total_pages = _int(data.get("totalPages") or data.get("total_pages")) if isinstance(data, Mapping) else None
                if total_pages is None or page >= total_pages:
                    break
                page += 1

            caps = self.capabilities(config_view, instance_id=instance_id, instance_label=instance_label)
            metadata = _metadata_provider(config_view)
            items = [self._normalize(row, instance_id, instance_label, caps, metadata) for row in rows]
            return PlaybackListResult(
                ok=True,
                provider=self.provider,
                instance_id=instance_id,
                items=[item for item in items if item],
                refreshed_at=utc_now_iso(),
            )
        except Exception:
            return PlaybackListResult(
                ok=False,
                provider=self.provider,
                instance_id=instance_id,
                error_code="provider_error",
                message="PublicMetaDB resume request failed.",
                retryable=True,
            )

    def _normalize(
        self,
        row: Mapping[str, Any],
        instance_id: str,
        instance_label: str,
        caps: PlaybackCapabilities,
        metadata: TmdbProvider | None,
    ) -> PlaybackRecord | None:
        remote_id = _first_str(row.get("id"), row.get("resume_id"), row.get("_publicmetadb_resume_id"))
        if not remote_id:
            return None

        media = str(row.get("media_type") or row.get("type") or "").strip().lower()
        is_episode = media in {"tv", "show", "series", "episode"}
        tmdb = _first_nested_int(row, "tmdb_id", "tmdb")
        ids = {"tmdb": tmdb} if tmdb is not None else {}
        progress_ms = _int(row.get("position_ms") or row.get("positionMs") or row.get("progress_ms") or row.get("progressMs"))
        runtime_ms = _int(row.get("runtime_ms") or row.get("runtimeMs") or row.get("duration_ms") or row.get("durationMs"))
        progress = _float(row.get("progress") or row.get("progress_percent"))
        if progress is None and progress_ms is not None and runtime_ms:
            progress = round((float(progress_ms) / float(runtime_ms)) * 100.0, 3)
        updated = _first_str(row.get("updated"), row.get("updated_at"), row.get("progress_at"), row.get("last_played")) or None

        if is_episode:
            series_title = _first_nested_str(row, "series_title", "show_title", "show_name", "name", "title")
            episode_title = _first_nested_str(row, "episode_title", "episode_name", "episodeTitle", "episodeName")
            season_no = _int(row.get("season") or row.get("season_number"))
            episode_no = _int(row.get("episode") or row.get("episode_number"))
            year = _int(row.get("year"))
            if not series_title or not runtime_ms:
                meta_title, meta_year, meta_duration = _metadata_title(metadata, media_type="episode", tmdb=tmdb)
                series_title = series_title or meta_title
                year = year or meta_year
                runtime_ms = runtime_ms or (meta_duration * 1000 if meta_duration else None)
            item = id_minimal(
                {
                    "type": "episode",
                    "ids": ids,
                    "show_ids": ids,
                    "series_title": series_title,
                    "title": episode_title or series_title,
                    "season": season_no,
                    "episode": episode_no,
                }
            )
            history_item = dict(item)
            media_type = "episode"
            title = series_title or episode_title or (f"TMDb {tmdb}" if tmdb is not None else "")
        else:
            title = _first_nested_str(row, "title", "name", "original_title", "original_name")
            year = _int(row.get("year"))
            if not title or year is None or not runtime_ms:
                meta_title, meta_year, meta_duration = _metadata_title(metadata, media_type="movie", tmdb=tmdb)
                title = title or meta_title
                year = year or meta_year
                runtime_ms = runtime_ms or (meta_duration * 1000 if meta_duration else None)
            title = title or (f"TMDb {tmdb}" if tmdb is not None else "")
            item = id_minimal({"type": "movie", "ids": ids, "title": title, "year": year})
            history_item = dict(item)
            media_type = "movie"
            series_title = ""
            episode_title = ""
            season_no = None
            episode_no = None

        if progress is None and progress_ms is not None and runtime_ms:
            progress = round((float(progress_ms) / float(runtime_ms)) * 100.0, 3)
        canonical = canonical_key(item) or ""
        duration_seconds = _ms_to_seconds(runtime_ms)
        rating = rating_from_sources(row) or metadata_rating(metadata, media_type=media_type, ids=ids, title=title, year=year)
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
            season=season_no,
            episode=episode_no,
            year=year,
            ids=ids,
            progress_percent=progress,
            remaining_seconds=max(0, _ms_to_seconds(runtime_ms - progress_ms) or 0) if runtime_ms and progress_ms is not None else None,
            duration_seconds=duration_seconds,
            progress_at=updated,
            updated_at=updated,
            source_app=_first_str(row.get("app"), row.get("source_app"), row.get("application")),
            source_device=_first_str(row.get("device"), row.get("source_device")),
            rating=rating,
            poster_url=_first_str(row.get("poster"), row.get("poster_url")),
            backdrop_url=_first_str(row.get("backdrop"), row.get("backdrop_url"), row.get("fanart")),
            can_remove_progress=caps.remove_progress,
            can_mark_watched=caps.mark_watched,
            can_update_progress=bool(caps.update_progress and duration_seconds),
            capability_messages=[] if caps.configured else [caps.reason],
            provider_metadata={
                "history_item": history_item,
                "show_ids": clean_mapping(history_item.get("show_ids") if isinstance(history_item.get("show_ids"), Mapping) else {}),
                "resume_item": clean_mapping(row),
            },
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
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Missing PublicMetaDB resume record id.", error_code="missing_remote_id")
        try:
            module = PUBLICMETADBModule(config_view)
            response = module.client.delete(f"/api/external/resume/{remote_id}")
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
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="PublicMetaDB remove progress failed.", error_code=f"http:{response.status_code}", remote_status=response.status_code, retryable=response.status_code in {408, 429, 500, 502, 503, 504}, remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="PublicMetaDB remove progress failed.", retryable=True, remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))

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
        item["watched_at"] = watched_at or utc_now_iso()
        try:
            result = PUBLICMETADB_OPS.add(config_view, [item], feature="history")
            ok = bool(result.get("ok"))
            return PlaybackActionResult(
                ok=ok,
                provider=self.provider,
                instance_id=instance_id,
                operation="mark_watched",
                remote_id=str(record.get("remote_id") or ""),
                canonical_key=str(record.get("canonical_key") or ""),
                message="Marked watched on PublicMetaDB." if ok else "PublicMetaDB mark watched failed.",
                error_code="" if ok else "history_failed",
                history_result=clean_mapping(result),
            )
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="mark_watched", message="PublicMetaDB mark watched failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

    def update_progress(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        progress_percent: float,
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackActionResult:
        payload, reason = _progress_payload_from_record(record, progress_percent)
        if payload is None:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message=f"PublicMetaDB update progress failed: {reason}.", error_code=reason or "invalid_record", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
        try:
            module = PUBLICMETADBModule(config_view)
            response = module.client.post("/api/external/resume", json=payload)
            if response.status_code in {200, 201, 202}:
                return PlaybackActionResult(
                    ok=True,
                    provider=self.provider,
                    instance_id=instance_id,
                    operation="update_progress",
                    remote_id=str(record.get("remote_id") or ""),
                    canonical_key=str(record.get("canonical_key") or ""),
                    message=f"Progress updated on PublicMetaDB to {progress_percent:g}%.",
                    remote_status=response.status_code,
                )
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="PublicMetaDB update progress failed.", error_code=f"http:{response.status_code}", remote_status=response.status_code, retryable=response.status_code in {408, 429, 500, 502, 503, 504}, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="PublicMetaDB update progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
