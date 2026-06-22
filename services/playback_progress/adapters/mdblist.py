# /services/playback_progress/adapters/mdblist.py
# CrossWatch - MDBList Playback Progress Adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from typing import Any, Mapping, cast

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.sync._mod_MDBLIST import MDBLISTModule, OPS as MDBLIST_OPS

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, clean_mapping, utc_now_iso
from .base import PlaybackProgressAdapter, metadata_rating, public_failure, rating_from_sources, tmdb_metadata_provider


MDBLIST_PLAYBACK_REASON = ""


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


def _ids(obj: Any) -> dict[str, Any]:
    if not isinstance(obj, Mapping):
        return {}
    raw_ids = obj.get("ids")
    raw = raw_ids if isinstance(raw_ids, Mapping) else obj
    if not isinstance(raw, Mapping):
        return {}
    out: dict[str, Any] = {}
    for key in ("imdb", "tmdb", "tvdb", "trakt", "mdblist"):
        value = raw.get(key) or raw.get(f"{key}_id")
        if value is None or value == "":
            continue
        if key in {"tmdb", "tvdb", "trakt"}:
            try:
                out[key] = int(value)
            except Exception:
                continue
        elif key == "imdb":
            text = str(value).strip()
            if text and not text.startswith("tt") and text.isdigit():
                text = f"tt{text}"
            if text:
                out[key] = text
        else:
            out[key] = str(value).strip()
    return out


def _module(config_view: Mapping[str, Any], instance_id: str) -> MDBLISTModule:
    mod = MDBLISTModule(config_view)
    # MDBLISTModule normally chooses its instance from sync-pair env vars. Playback Progress
    # carries the selected instance explicitly, so keep the request auth on that instance.
    mod.instance_id = instance_id
    try:
        mod.client.instance_id = instance_id
    except Exception:
        pass
    return mod


def _history_item(record: Mapping[str, Any]) -> dict[str, Any]:
    meta = _as_mapping(record.get("provider_metadata"))
    history_value = meta.get("history_item")
    hist = history_value if isinstance(history_value, Mapping) else None
    if hist:
        return clean_mapping(hist)
    media_type = str(record.get("media_type") or "").lower()
    ids = clean_mapping(record.get("ids") if isinstance(record.get("ids"), Mapping) else {})
    item: dict[str, Any] = {
        "type": "episode" if media_type == "episode" else "movie",
        "ids": ids,
        "title": record.get("episode_title") or record.get("title") or record.get("series_title"),
        "year": record.get("year"),
    }
    if media_type == "episode":
        item["show_ids"] = clean_mapping(_as_mapping(meta.get("show_ids"))) or ids
        item["series_title"] = record.get("series_title")
        item["season"] = record.get("season")
        item["episode"] = record.get("episode")
    return clean_mapping(item)


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
    show: dict[str, Any] = {}
    if show_ids:
        show["ids"] = show_ids
    if item.get("series_title"):
        show["title"] = item.get("series_title")
    episode: dict[str, Any] = {}
    if item.get("season") is not None:
        episode["season"] = item.get("season")
    if item.get("episode") is not None:
        episode["number"] = item.get("episode")
    if ids:
        episode["ids"] = ids
    body["show"] = show
    body["episode"] = episode
    return clean_mapping(body)


class MDBListPlaybackAdapter(PlaybackProgressAdapter):
    provider = "mdblist"
    provider_label = "MDBList"

    def capabilities(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackCapabilities:
        configured = False
        try:
            configured = bool(MDBLIST_OPS.is_configured(config_view))
        except Exception:
            configured = False
        reason = "" if configured else "MDBList is not connected for this instance."
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
            mod = _module(config_view, instance_id)
            response = mod.client.get(f"{mod.client.BASE}/sync/playback", params={"apikey": mod.cfg.api_key})
            if not (200 <= response.status_code < 300):
                return PlaybackListResult(
                    ok=False,
                    provider=self.provider,
                    instance_id=instance_id,
                    error_code=f"http:{response.status_code}",
                    message="MDBList playback request failed.",
                    remote_status=response.status_code,
                    retryable=response.status_code in {408, 429, 500, 502, 503, 504},
                )
            data = response.json() if (response.text or "").strip() else []
            rows: list[Mapping[str, Any]] = []
            if isinstance(data, list):
                rows = [row for row in data if isinstance(row, Mapping)]
            elif isinstance(data, Mapping):
                for key in ("playback", "items", "sessions", "movies", "episodes"):
                    value = data.get(key)
                    if isinstance(value, list):
                        rows.extend(row for row in value if isinstance(row, Mapping))
            caps = self.capabilities(config_view, instance_id=instance_id, instance_label=instance_label)
            metadata = tmdb_metadata_provider(config_view)
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
                message="MDBList playback request failed.",
                retryable=True,
            )

    def _normalize(
        self,
        row: Mapping[str, Any],
        instance_id: str,
        instance_label: str,
        caps: PlaybackCapabilities,
        metadata: Any,
    ) -> PlaybackRecord | None:
        remote_id = _first_str(row.get("id"), row.get("playback_id"), row.get("session_id"))
        if not remote_id:
            return None
        row_type = str(row.get("type") or "").strip().lower()
        movie = _as_mapping(row.get("movie"))
        show = _as_mapping(row.get("show"))
        episode_obj = _as_mapping(row.get("episode"))
        season_obj = _as_mapping(row.get("season"))
        show_season_obj = _as_mapping(show.get("season"))
        show_episode_obj = _as_mapping(show_season_obj.get("episode"))

        if movie or row_type == "movie":
            payload = movie or row
            ids = _ids(payload)
            title = _first_str(payload.get("title"), payload.get("name"), row.get("title"))
            year = _int(payload.get("year") or row.get("year"))
            item = id_minimal({"type": "movie", "ids": ids, "title": title, "year": year})
            history_item = dict(item)
            media_type = "movie"
            canonical = canonical_key(item) or ""
            series_title = ""
            episode_title = ""
            season_no = None
            episode_no = None
        else:
            show_ids = _ids(show or row)
            ep_ids = _ids(episode_obj)
            series_title = _first_str(show.get("title"), show.get("name"), row.get("series_title"), row.get("show_title"), row.get("title"))
            episode_title = _first_str(episode_obj.get("title"), episode_obj.get("name"), row.get("episode_title"))
            season_no = _int(
                episode_obj.get("season")
                or season_obj.get("number")
                or show_season_obj.get("number")
                or row.get("season")
                or row.get("season_number")
            )
            episode_no = _int(
                episode_obj.get("number")
                or episode_obj.get("episode")
                or show_episode_obj.get("number")
                or show_episode_obj.get("episode")
                or row.get("episode")
                or row.get("episode_number")
            )
            ids = ep_ids or show_ids
            item = id_minimal(
                {
                    "type": "episode",
                    "ids": ids,
                    "show_ids": show_ids or ids,
                    "series_title": series_title,
                    "title": episode_title or series_title,
                    "season": season_no,
                    "episode": episode_no,
                }
            )
            history_item = dict(item)
            media_type = "episode"
            canonical = canonical_key(item) or ""
            title = series_title or episode_title
            year = _int(show.get("year") or row.get("year"))

        updated = _first_str(row.get("paused_at"), row.get("updated_at"), row.get("progress_at")) or None
        progress = _float(row.get("progress") or row.get("progress_percent") or row.get("percent"))
        duration = _duration_seconds_from_sources(row, movie, show, episode_obj)
        remaining = _remaining_seconds(duration, progress)
        rating = rating_from_sources(row, movie, show, episode_obj)
        if rating is None:
            show_rating_ids = clean_mapping(history_item.get("show_ids") if isinstance(history_item.get("show_ids"), Mapping) else {})
            rating_ids = ids if media_type == "movie" else show_rating_ids or ids
            rating_title = title if media_type == "movie" else series_title or title
            rating = metadata_rating(metadata, media_type=media_type, ids=rating_ids, title=rating_title, year=year)
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
            remaining_seconds=remaining,
            duration_seconds=duration,
            progress_at=updated,
            updated_at=updated,
            source_app=_first_str(row.get("app"), row.get("source_app"), row.get("application")),
            source_device=_first_str(row.get("device"), row.get("source_device")),
            rating=rating,
            poster_url=_first_str(row.get("poster"), row.get("poster_url")),
            backdrop_url=_first_str(row.get("backdrop"), row.get("backdrop_url"), row.get("fanart")),
            can_remove_progress=caps.remove_progress,
            can_mark_watched=caps.mark_watched,
            can_update_progress=caps.update_progress,
            capability_messages=[] if caps.configured else [caps.reason],
            provider_metadata={
                "history_item": history_item,
                "show_ids": clean_mapping(history_item.get("show_ids") if isinstance(history_item.get("show_ids"), Mapping) else {}),
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
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="Missing MDBList playback record id.", error_code="missing_remote_id")
        try:
            mod = _module(config_view, instance_id)
            response = mod.client.post(
                f"{mod.client.BASE}/scrobble/clear",
                params={"apikey": mod.cfg.api_key},
                json={"id": int(remote_id) if remote_id.isdigit() else remote_id},
            )
            if response.status_code in {200, 201, 202, 204, 404}:
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
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="MDBList remove progress failed.", error_code=f"http:{response.status_code}", remote_status=response.status_code, retryable=response.status_code in {408, 429, 500, 502, 503, 504}, remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="MDBList remove progress failed.", retryable=True, remote_id=remote_id, canonical_key=str(record.get("canonical_key") or ""))

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
        elif not item.get("watched_at"):
            item["watched_at"] = utc_now_iso()
        try:
            result = MDBLIST_OPS.add(config_view, [item], feature="history")
            ok = bool(result.get("ok"))
            return PlaybackActionResult(
                ok=ok,
                provider=self.provider,
                instance_id=instance_id,
                operation="mark_watched",
                remote_id=str(record.get("remote_id") or ""),
                canonical_key=str(record.get("canonical_key") or ""),
                message="Marked watched on MDBList." if ok else "MDBList mark watched failed.",
                error_code="" if ok else "history_failed",
                history_result=clean_mapping(result),
            )
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="mark_watched", message="MDBList mark watched failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

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
            mod = _module(config_view, instance_id)
            response = mod.client.post(
                f"{mod.client.BASE}/scrobble/pause",
                params={"apikey": mod.cfg.api_key},
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
                    message=f"Progress updated on MDBList to {progress_percent:g}%.",
                    remote_status=response.status_code,
                )
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="MDBList update progress failed.", error_code=f"http:{response.status_code}", remote_status=response.status_code, retryable=response.status_code in {408, 429, 500, 502, 503, 504}, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="MDBList update progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))
