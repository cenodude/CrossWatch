# /services/playback_progress/adapters/flicklist.py
# CrossWatch - FlickList Playback Progress Adapter
from __future__ import annotations

from typing import Any, Mapping, cast

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.sync._mod_FLICKLIST import OPS as FLICKLIST_OPS
from providers.sync.flicklist._common import write_ident

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, clean_mapping, utc_now_iso
from .base import PlaybackProgressAdapter, enrich_parallel, public_failure, tmdb_metadata_provider


def _mapping(value: Any) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], value) if isinstance(value, Mapping) else {}


def _num(value: Any) -> int | None:
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
        return float(str(value).strip())
    except Exception:
        return None


def _image_url(meta: Mapping[str, Any], kind: str) -> str:
    rows = _mapping(meta.get("images")).get(kind)
    if not isinstance(rows, list):
        return ""
    for row in rows:
        if isinstance(row, Mapping) and str(row.get("url") or "").strip():
            return str(row.get("url") or "").strip()
    return ""


def _metadata_detail(provider: Any, *, media_type: str, ids: Mapping[str, Any], show_ids: Mapping[str, Any]) -> dict[str, Any]:
    lookup_ids = show_ids if media_type == "episode" and show_ids else ids
    fetch_ids = {key: str(lookup_ids.get(key) or "").strip() for key in ("tmdb", "imdb", "tvdb") if str(lookup_ids.get(key) or "").strip()}
    if provider is None or not fetch_ids:
        return {}
    try:
        detail = provider.fetch(entity="tv" if media_type == "episode" else "movie", ids=fetch_ids, need={"poster": True, "backdrop": True, "ids": False})
    except Exception:
        return {}
    if not isinstance(detail, Mapping):
        return {}
    out: dict[str, Any] = {}
    title = str(detail.get("title") or "").strip()
    if title:
        out["title"] = title
    year = _num(detail.get("year"))
    if year is not None:
        out["year"] = year
    poster = _image_url(detail, "poster")
    if poster:
        out["poster_url"] = poster
    backdrop = _image_url(detail, "backdrop")
    if backdrop:
        out["backdrop_url"] = backdrop
    return out


def _progress_item(record: Mapping[str, Any]) -> dict[str, Any]:
    meta = _mapping(record.get("provider_metadata"))
    stored = meta.get("progress_item")
    if isinstance(stored, Mapping):
        item = clean_mapping(stored)
    else:
        media_type = str(record.get("media_type") or "").strip().lower()
        ids = clean_mapping(_mapping(record.get("ids")))
        item = {
            "type": "episode" if media_type in {"episode", "anime_episode"} else "movie",
            "ids": ids,
            "title": record.get("episode_title") or record.get("title") or record.get("series_title"),
            "year": record.get("year"),
        }
        if item["type"] == "episode":
            item["show_ids"] = clean_mapping(_mapping(meta.get("show_ids"))) or ids
            item["series_title"] = record.get("series_title")
            item["season"] = record.get("season")
            item["episode"] = record.get("episode")
        item = clean_mapping(item)
    remote_id = str(record.get("remote_id") or "").strip()
    if remote_id:
        item["_flicklist_playback_id"] = remote_id
    return item


def _progress_item_with_percent(record: Mapping[str, Any], progress_percent: float) -> dict[str, Any]:
    item = _progress_item(record)
    item["progress_percent"] = round(max(0.0, min(100.0, float(progress_percent))), 3)
    item["progress_at"] = utc_now_iso()
    return clean_mapping(item)


class FlickListPlaybackAdapter(PlaybackProgressAdapter):
    provider = "flicklist"
    provider_label = "FlickList"
    ops = FLICKLIST_OPS

    def capabilities(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackCapabilities:
        try:
            configured = bool(self.ops.is_configured(config_view))
        except Exception:
            configured = False
        caps = _mapping(self.ops.capabilities())
        progress = _mapping(caps.get("progress"))
        history = _mapping(caps.get("history"))
        types = _mapping(progress.get("types"))
        reason = "" if configured else "FlickList is not connected for this instance."
        return PlaybackCapabilities(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            configured=configured,
            read=bool(configured and progress.get("read")),
            remove_progress=bool(configured and progress.get("remove")),
            mark_watched=bool(configured and history.get("write")),
            update_progress=bool(configured and progress.get("upsert")),
            bulk_remove_progress=bool(configured and progress.get("remove")),
            bulk_mark_watched=bool(configured and history.get("write")),
            bulk_update_progress=bool(configured and progress.get("upsert")),
            supports_movies=bool(configured and types.get("movies")),
            supports_episodes=bool(configured and types.get("episodes")),
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
        caps = self.capabilities(config_view, instance_id=instance_id, instance_label=instance_label)
        if not caps.read:
            return PlaybackListResult(False, self.provider, instance_id, error_code="unsupported" if caps.configured else "not_configured", message=caps.reason or "FlickList does not support playback listing.")
        try:
            index = self.ops.build_index(config_view, feature="progress") or {}
        except Exception:
            return PlaybackListResult(False, self.provider, instance_id, error_code="provider_error", message="FlickList progress request failed.", retryable=True)
        metadata = tmdb_metadata_provider(config_view)
        pending = [(key, item) for key, item in dict(index).items() if isinstance(item, Mapping)]
        items = enrich_parallel(pending, lambda entry: self._record(entry[0], entry[1], instance_id, instance_label, caps, metadata))
        return PlaybackListResult(True, self.provider, instance_id, items=[item for item in items if item], refreshed_at=utc_now_iso())

    def _record(self, key: Any, item: Mapping[str, Any], instance_id: str, instance_label: str, caps: PlaybackCapabilities, metadata: Any = None) -> PlaybackRecord | None:
        mini = id_minimal(item)
        typ = str(mini.get("type") or item.get("type") or "").strip().lower()
        media_type = "episode" if typ == "episode" else "movie"
        ids = clean_mapping(_mapping(mini.get("ids") or item.get("ids")))
        show_ids = clean_mapping(_mapping(mini.get("show_ids") or item.get("show_ids")))
        title = str(mini.get("title") or item.get("title") or item.get("series_title") or "").strip()
        series_title = str(mini.get("series_title") or item.get("series_title") or "").strip()
        needs_metadata = not title or (media_type == "episode" and not series_title) or not str(item.get("poster") or item.get("poster_url") or "").strip()
        resolved = _metadata_detail(metadata, media_type=media_type, ids=ids, show_ids=show_ids) if needs_metadata else {}
        if resolved.get("title"):
            if media_type == "episode" and not series_title:
                series_title = str(resolved["title"])
            if not title:
                title = str(resolved["title"])
        year = _num(mini.get("year") or item.get("year") or resolved.get("year"))
        progress = _float(item.get("progress_percent") or item.get("progress"))
        if progress is not None:
            progress = round(max(0.0, min(100.0, progress)), 3)
        canonical = canonical_key(mini) or str(key or "")
        progress_item = clean_mapping(item)
        can_update = bool(caps.update_progress and progress_item and write_ident(progress_item))
        if not canonical or progress is None:
            return None
        episode_title = title if media_type == "episode" and title != series_title else ""
        return PlaybackRecord(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            remote_id=str(item.get("_flicklist_playback_id") or key or ""),
            canonical_key=canonical,
            media_type=media_type,
            title=series_title or title,
            episode_title=episode_title,
            series_title=series_title,
            season=_num(mini.get("season") or item.get("season")),
            episode=_num(mini.get("episode") or item.get("episode")),
            year=year,
            ids=ids,
            progress_percent=progress,
            progress_at=str(item.get("progress_at") or item.get("updated_at") or "").strip() or None,
            updated_at=str(item.get("updated_at") or item.get("progress_at") or "").strip() or None,
            source_app="FlickList",
            can_remove_progress=caps.remove_progress,
            can_mark_watched=caps.mark_watched,
            can_update_progress=can_update,
            capability_messages=[] if caps.configured else [caps.reason],
            poster_url=str(item.get("poster") or item.get("poster_url") or resolved.get("poster_url") or "").strip(),
            backdrop_url=str(item.get("backdrop") or item.get("backdrop_url") or resolved.get("backdrop_url") or "").strip(),
            provider_metadata={"progress_item": progress_item, "show_ids": show_ids},
        )

    def remove_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackActionResult:
        try:
            result = self.ops.remove(config_view, [_progress_item(record)], feature="progress", dry_run=False) or {}
            ok = bool(result.get("ok"))
            return PlaybackActionResult(ok, self.provider, instance_id, "remove_progress", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""), message="Playback record removed." if ok else "FlickList remove progress failed.", error_code="" if ok else "progress_failed", decision_context=clean_mapping(result))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="remove_progress", message="FlickList remove progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

    def mark_watched(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str, instance_label: str, watched_at: str | None = None) -> PlaybackActionResult:
        item = _progress_item(record)
        item["watched_at"] = watched_at or utc_now_iso()
        try:
            result = self.ops.add(config_view, [item], feature="history", dry_run=False) or {}
            ok = bool(result.get("ok"))
            return PlaybackActionResult(ok, self.provider, instance_id, "mark_watched", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""), message="Marked watched on FlickList." if ok else "FlickList mark watched failed.", error_code="" if ok else "history_failed", history_result=clean_mapping(result))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="mark_watched", message="FlickList mark watched failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))

    def update_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], progress_percent: float, *, instance_id: str, instance_label: str) -> PlaybackActionResult:
        try:
            result = self.ops.add(config_view, [_progress_item_with_percent(record, progress_percent)], feature="progress", dry_run=False) or {}
            ok = bool(result.get("ok"))
            return PlaybackActionResult(ok, self.provider, instance_id, "update_progress", remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""), message=f"Progress updated on FlickList to {progress_percent:g}%." if ok else "FlickList update progress failed.", error_code="" if ok else "progress_failed", decision_context=clean_mapping(result))
        except Exception:
            return public_failure(provider=self.provider, instance_id=instance_id, operation="update_progress", message="FlickList update progress failed.", retryable=True, remote_id=str(record.get("remote_id") or ""), canonical_key=str(record.get("canonical_key") or ""))


__all__ = ["FlickListPlaybackAdapter"]
