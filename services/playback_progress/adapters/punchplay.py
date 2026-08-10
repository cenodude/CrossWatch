# /services/playback_progress/adapters/punchplay.py
# CrossWatch - PunchPlay Playback Progress Adapter
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Mapping, cast

import requests

from cw_platform.id_map import canonical_key, minimal as id_minimal
from providers.sync._mod_PUNCHPLAY import OPS as PUNCHPLAY_OPS
from providers.sync.punchplay import _progress as feat_progress
from providers.sync.punchplay._common import (
    PunchPlayRateLimited,
    URL_IN_PROGRESS_ITEM,
    URL_PLAYBACK,
    error_of,
    punchplay_request,
)

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult, PlaybackRecord, clean_mapping, utc_now_iso
from .base import PlaybackProgressAdapter, enrich_parallel, tmdb_metadata_provider

PROGRESS_ID_FIELD = feat_progress.PROGRESS_ID_FIELD


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
    images = _mapping(meta.get("images"))
    rows = images.get(kind)
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


def _is_placeholder_title(value: Any) -> bool:
    return str(value or "").strip().lower() in {"", "untitled"}


class _Adapter:
    def __init__(self, cfg: Mapping[str, Any], instance_id: str, session: requests.Session) -> None:
        self.config = dict(cfg or {})
        self.instance_id = instance_id
        self.session = session


class PunchPlayPlaybackAdapter(PlaybackProgressAdapter):
    provider = "punchplay"
    provider_label = "PunchPlay"

    def __init__(self) -> None:
        self._session = requests.Session()

    def _client(self, config_view: Mapping[str, Any], instance_id: str) -> _Adapter:
        return _Adapter(config_view, instance_id or "default", self._session)

    def capabilities(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackCapabilities:
        try:
            configured = bool(PUNCHPLAY_OPS.is_configured(config_view))
        except Exception:
            configured = False
        caps = _mapping(PUNCHPLAY_OPS.capabilities())
        progress = _mapping(caps.get("progress"))
        history = _mapping(caps.get("history"))
        types = _mapping(progress.get("types"))
        reason = "" if configured else "PunchPlay is not connected for this instance."
        return PlaybackCapabilities(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            configured=configured,
            read=bool(configured and progress),
            remove_progress=bool(configured and progress.get("remove")),
            mark_watched=bool(configured and history.get("write")),
            update_progress=bool(configured and progress.get("upsert")),
            bulk_remove_progress=bool(configured and progress.get("remove")),
            bulk_mark_watched=bool(configured and history.get("write")),
            bulk_update_progress=bool(configured and progress.get("upsert")),
            supports_movies=bool(configured and types.get("movies")),
            supports_episodes=bool(configured and types.get("episodes")),
            supports_anime=bool(configured),
            reason=reason,
        )

    def list_progress(self, config_view: Mapping[str, Any], *, instance_id: str, instance_label: str, force_refresh: bool = False) -> PlaybackListResult:
        caps = self.capabilities(config_view, instance_id=instance_id, instance_label=instance_label)
        if not caps.read:
            return PlaybackListResult(
                False,
                self.provider,
                instance_id,
                error_code="unsupported" if caps.configured else "not_configured",
                message=caps.reason or "PunchPlay does not support playback listing.",
            )
        try:
            index = PUNCHPLAY_OPS.build_index(config_view, feature="progress") or {}
        except Exception:
            return PlaybackListResult(
                False,
                self.provider,
                instance_id,
                error_code="provider_error",
                message="PunchPlay progress request failed.",
                retryable=True,
            )
        metadata = tmdb_metadata_provider(config_view)
        pending = [(key, item) for key, item in dict(index).items() if isinstance(item, Mapping)]
        items = enrich_parallel(pending, lambda entry: self._record(entry[0], entry[1], instance_id, instance_label, caps, metadata))
        return PlaybackListResult(True, self.provider, instance_id, items=[i for i in items if i], refreshed_at=utc_now_iso())

    def _record(self, key: Any, item: Mapping[str, Any], instance_id: str, instance_label: str, caps: PlaybackCapabilities, metadata: Any = None) -> PlaybackRecord | None:
        mini = id_minimal(item)
        ids = clean_mapping(_mapping(mini.get("ids") or item.get("ids")))
        show_ids = clean_mapping(_mapping(mini.get("show_ids") or item.get("show_ids")))
        media_type = "episode" if str(item.get("type") or "").lower() == "episode" else "movie"
        remote_id = str(item.get(PROGRESS_ID_FIELD) or "").strip()
        title = str(mini.get("title") or item.get("title") or item.get("series_title") or "").strip()
        series_title = str(mini.get("series_title") or item.get("series_title") or "").strip()
        if _is_placeholder_title(title):
            title = ""
        if media_type == "episode" and _is_placeholder_title(series_title):
            series_title = ""
        needs_metadata = (
            not title
            or (media_type == "episode" and not series_title)
            or not str(item.get("poster") or item.get("poster_url") or "").strip()
            or not str(item.get("background") or item.get("backdrop") or item.get("backdrop_url") or "").strip()
        )
        resolved = _metadata_detail(metadata, media_type=media_type, ids=ids, show_ids=show_ids) if needs_metadata else {}
        resolved_title = str(resolved.get("title") or "").strip()
        if resolved_title and (not title or (media_type == "episode" and not series_title)):
            if media_type == "episode" and not series_title:
                series_title = resolved_title
            if not title:
                title = resolved_title
        year = _num(mini.get("year") or item.get("year") or resolved.get("year"))

        duration_ms = _float(item.get("duration_ms"))
        progress_ms = _float(item.get("progress_ms"))
        percent = _float(item.get("progress_percent"))
        if percent is None and progress_ms is not None and duration_ms:
            percent = (progress_ms / duration_ms) * 100.0

        remaining = None
        if duration_ms and progress_ms is not None:
            remaining = max(0, int((duration_ms - progress_ms) / 1000.0))

        return PlaybackRecord(
            provider=self.provider,
            provider_label=self.provider_label,
            instance_id=instance_id,
            instance_label=instance_label,
            remote_id=remote_id,
            canonical_key=str(key or canonical_key(mini) or ""),
            media_type=media_type,
            title=series_title or title,
            episode_title=title if media_type == "episode" and title != series_title else "",
            series_title=series_title,
            season=_num(item.get("season")),
            episode=_num(item.get("episode")),
            year=year,
            ids=ids,
            progress_percent=round(percent, 3) if percent is not None else None,
            remaining_seconds=remaining,
            duration_seconds=int(duration_ms / 1000.0) if duration_ms else None,
            progress_at=item.get("progress_at") or item.get("updated_at"),
            updated_at=item.get("updated_at"),
            source_app=str(item.get("playback_state") or ""),
            can_remove_progress=caps.remove_progress,
            can_mark_watched=caps.mark_watched,
            can_update_progress=bool(caps.update_progress and duration_ms),
            capability_messages=[] if caps.configured else [caps.reason],
            poster_url=str(item.get("poster") or item.get("poster_url") or resolved.get("poster_url") or "").strip(),
            backdrop_url=str(item.get("background") or item.get("backdrop") or item.get("backdrop_url") or resolved.get("backdrop_url") or "").strip(),
            provider_metadata={"progress_item": clean_mapping(item), "show_ids": show_ids},
        )

    def _rate_limited(self, exc: PunchPlayRateLimited, operation: str, instance_id: str, record: Mapping[str, Any]) -> PlaybackActionResult:
        return PlaybackActionResult(
            ok=False,
            provider=self.provider,
            instance_id=instance_id,
            operation=operation,
            remote_id=str(record.get("remote_id") or ""),
            canonical_key=str(record.get("canonical_key") or ""),
            error_code="rate_limited",
            message=f"PunchPlay rate limit reached, retry in {exc.retry_after}s.",
            retryable=True,
            remote_status=429,
        )

    def _failed(self, operation: str, instance_id: str, record: Mapping[str, Any], *, code: str, message: str, status: int | None = None, retryable: bool = False) -> PlaybackActionResult:
        return PlaybackActionResult(
            ok=False,
            provider=self.provider,
            instance_id=instance_id,
            operation=operation,
            remote_id=str(record.get("remote_id") or ""),
            canonical_key=str(record.get("canonical_key") or ""),
            error_code=code,
            message=message,
            retryable=retryable,
            remote_status=status,
        )

    def _applied(self, operation: str, instance_id: str, record: Mapping[str, Any]) -> PlaybackActionResult:
        return PlaybackActionResult(
            ok=True,
            provider=self.provider,
            instance_id=instance_id,
            operation=operation,
            remote_id=str(record.get("remote_id") or ""),
            canonical_key=str(record.get("canonical_key") or ""),
        )

    def _item_from_record(self, record: Mapping[str, Any]) -> dict[str, Any]:
        media_type = str(record.get("media_type") or "movie").lower()
        ids = dict(record.get("ids") or {})
        item: dict[str, Any] = {"type": "episode" if media_type == "episode" else "movie"}
        if media_type == "episode":
            item["show_ids"] = ids
            item["season"] = _num(record.get("season"))
            item["episode"] = _num(record.get("episode"))
            item["series_title"] = str(record.get("series_title") or "")
            item["title"] = str(record.get("episode_title") or record.get("title") or "")
        else:
            item["ids"] = ids
            item["title"] = str(record.get("title") or "")
            year = _num(record.get("year"))
            if year:
                item["year"] = year
        duration = _num(record.get("duration_seconds"))
        if duration:
            item["duration_ms"] = duration * 1000
        return item

    def _send_playback(self, config_view: Mapping[str, Any], instance_id: str, action: str, payload: Mapping[str, Any]) -> Any:
        client = self._client(config_view, instance_id)
        return punchplay_request(client, "POST", URL_PLAYBACK.format(action=action), json=dict(payload), no_wait=True)

    def remove_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str, instance_label: str) -> PlaybackActionResult:
        remote_id = str(record.get("remote_id") or "").strip()
        if not remote_id:
            return self._failed("remove_progress", instance_id, record, code="missing_remote_id", message="PunchPlay in-progress id is unknown; refresh the list first.")
        client = self._client(config_view, instance_id)
        try:
            resp = punchplay_request(client, "DELETE", URL_IN_PROGRESS_ITEM.format(entry_id=remote_id), no_wait=True)
        except PunchPlayRateLimited as exc:
            return self._rate_limited(exc, "remove_progress", instance_id, record)
        except Exception:
            return self._failed("remove_progress", instance_id, record, code="provider_error", message="PunchPlay dismiss request failed.", retryable=True)
        code = int(getattr(resp, "status_code", 0) or 0)
        if 200 <= code < 300 or code == 404:
            return self._applied("remove_progress", instance_id, record)
        return self._failed("remove_progress", instance_id, record, code=error_of(resp) or "provider_error", message="PunchPlay rejected the dismiss request.", status=code, retryable=code >= 500 or code == 429)

    def update_progress(self, config_view: Mapping[str, Any], record: Mapping[str, Any], progress_percent: float, *, instance_id: str, instance_label: str) -> PlaybackActionResult:
        item = self._item_from_record(record)
        percent = max(0.0, min(100.0, float(progress_percent or 0.0)))
        duration_ms = _float(item.get("duration_ms"))
        if duration_ms:
            item["progress_ms"] = int(duration_ms * (percent / 100.0))
        else:
            item["progress_percent"] = percent

        payload = feat_progress._playback_payload(item)
        if payload is None:
            return self._failed("update_progress", instance_id, record, code="missing_ids", message="PunchPlay needs a TMDB, IMDb or TVDB id for this item.")
        payload.update(
            feat_progress._session_ids(
                item,
                str(record.get("canonical_key") or ""),
                device_id=str((_mapping(config_view.get("punchplay")).get("device_id")) or ""),
                instance=instance_id or "default",
                position=payload.get("position_seconds"),
            )
        )
        payload["watched"] = False
        payload["watched_threshold"] = 1.0
        try:
            resp = self._send_playback(config_view, instance_id, "stop", payload)
        except PunchPlayRateLimited as exc:
            return self._rate_limited(exc, "update_progress", instance_id, record)
        except Exception:
            return self._failed("update_progress", instance_id, record, code="provider_error", message="PunchPlay progress update failed.", retryable=True)
        code = int(getattr(resp, "status_code", 0) or 0)
        if 200 <= code < 300:
            return self._applied("update_progress", instance_id, record)
        return self._failed("update_progress", instance_id, record, code=error_of(resp) or "provider_error", message="PunchPlay rejected the progress update.", status=code, retryable=code >= 500 or code == 429)

    def mark_watched(self, config_view: Mapping[str, Any], record: Mapping[str, Any], *, instance_id: str, instance_label: str, watched_at: str | None = None) -> PlaybackActionResult:
        item = self._item_from_record(record)
        duration_ms = _float(item.get("duration_ms"))
        if duration_ms:
            item["progress_ms"] = int(duration_ms)
        item["progress_percent"] = 100.0

        payload = feat_progress._playback_payload(item)
        if payload is None:
            return self._failed("mark_watched", instance_id, record, code="missing_ids", message="PunchPlay needs a TMDB, IMDb or TVDB id for this item.")
        payload.update(
            feat_progress._session_ids(
                item,
                str(record.get("canonical_key") or ""),
                device_id=str((_mapping(config_view.get("punchplay")).get("device_id")) or ""),
                instance=instance_id or "default",
                position=payload.get("position_seconds"),
            )
        )
        payload["watched"] = True
        payload["watched_threshold"] = 0.9
        payload["progress"] = 1.0

        try:
            resp = self._send_playback(config_view, instance_id, "stop", payload)
        except PunchPlayRateLimited as exc:
            return self._rate_limited(exc, "mark_watched", instance_id, record)
        except Exception:
            return self._failed("mark_watched", instance_id, record, code="provider_error", message="PunchPlay watched request failed.", retryable=True)
        code = int(getattr(resp, "status_code", 0) or 0)
        if 200 <= code < 300:
            return self._applied("mark_watched", instance_id, record)
        return self._failed("mark_watched", instance_id, record, code=error_of(resp) or "provider_error", message="PunchPlay rejected the watched request.", status=code, retryable=code >= 500 or code == 429)


__all__ = ["PunchPlayPlaybackAdapter"]
