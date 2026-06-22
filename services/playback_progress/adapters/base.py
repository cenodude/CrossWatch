# /services/playback_progress/adapters/base.py
# CrossWatch - Playback Progress Adapters
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import re
from typing import Any, Mapping

from providers.metadata._meta_TMDB import TmdbProvider

from ..models import PlaybackActionResult, PlaybackCapabilities, PlaybackListResult


class PlaybackProgressAdapter:
    provider = ""
    provider_label = ""

    def capabilities(
        self,
        config_view: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackCapabilities:
        raise NotImplementedError

    def list_progress(
        self,
        config_view: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
        force_refresh: bool = False,
    ) -> PlaybackListResult:
        raise NotImplementedError

    def remove_progress(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackActionResult:
        raise NotImplementedError

    def mark_watched(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        *,
        instance_id: str,
        instance_label: str,
        watched_at: str | None = None,
    ) -> PlaybackActionResult:
        raise NotImplementedError

    def update_progress(
        self,
        config_view: Mapping[str, Any],
        record: Mapping[str, Any],
        progress_percent: float,
        *,
        instance_id: str,
        instance_label: str,
    ) -> PlaybackActionResult:
        raise NotImplementedError


def public_failure(
    *,
    provider: str,
    instance_id: str,
    operation: str,
    message: str,
    error_code: str = "provider_error",
    remote_status: int | None = None,
    retryable: bool = False,
    remote_id: str = "",
    canonical_key: str = "",
) -> PlaybackActionResult:
    return PlaybackActionResult(
        ok=False,
        provider=provider,
        instance_id=instance_id,
        operation=operation,
        remote_id=remote_id,
        canonical_key=canonical_key,
        error_code=error_code,
        message=message,
        remote_status=remote_status,
        retryable=retryable,
    )


def configured_label(block: Mapping[str, Any] | None, fallback: str) -> str:
    if isinstance(block, Mapping):
        for key in ("label", "name", "account_label"):
            value = str(block.get(key) or "").strip()
            if value:
                return value
    return fallback


def rating_from_sources(*sources: Any) -> float | None:
    def _rating(value: Any) -> float | None:
        if isinstance(value, Mapping):
            return rating_from_sources(value)
        if value is None or value == "":
            return None
        try:
            number = float(value)
        except Exception:
            match = re.search(r"-?\d+(?:\.\d+)?", str(value))
            if not match:
                return None
            try:
                number = float(match.group(0))
            except Exception:
                return None
        if number <= 0:
            return None
        if number > 10 and number <= 100:
            number = number / 10.0
        if number > 10:
            return None
        return round(number, 1)

    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for key in (
            "rating",
            "user_rating",
            "score",
            "vote_average",
            "tmdb_rating",
            "imdb_rating",
            "trakt_rating",
            "simkl_rating",
            "mdblist_rating",
            "tmdb",
            "imdb",
            "trakt",
            "simkl",
            "mdblist",
        ):
            value = _rating(source.get(key))
            if value is not None:
                return value
        for key in ("ratings", "scores"):
            nested = source.get(key)
            if isinstance(nested, Mapping):
                value = rating_from_sources(nested)
                if value is not None:
                    return value
    return None


def tmdb_metadata_provider(config_view: Mapping[str, Any]) -> TmdbProvider | None:
    def _as_mapping(value: Any) -> Mapping[str, Any]:
        return value if isinstance(value, Mapping) else {}

    def _first_str(*values: Any) -> str:
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return ""

    tmdb = _as_mapping(config_view.get("tmdb"))
    metadata = _as_mapping(config_view.get("metadata"))
    if not _first_str(tmdb.get("api_key"), metadata.get("tmdb_api_key")):
        return None
    cfg = dict(config_view)
    return TmdbProvider(lambda: cfg, lambda _cfg: None)


def metadata_rating(
    provider: TmdbProvider | None,
    *,
    media_type: str,
    ids: Mapping[str, Any],
    title: str,
    year: Any = None,
) -> float | None:
    if provider is None:
        return None
    lookup_ids = {str(k): str(v) for k, v in ids.items() if v not in (None, "", 0, False)}
    if title:
        lookup_ids["title"] = str(title)
    if year:
        lookup_ids["year"] = str(year)
    if not lookup_ids.get("tmdb") and not lookup_ids.get("imdb") and not lookup_ids.get("title"):
        return None
    try:
        detail = provider.fetch(
            entity="movie" if str(media_type or "").lower() == "movie" else "tv",
            ids=lookup_ids,
            need={"poster": False, "backdrop": False, "ids": False, "score": True},
        )
    except Exception:
        return None
    return rating_from_sources(detail)
