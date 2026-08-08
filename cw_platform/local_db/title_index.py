# cw_platform/local_db/title_index.py
# CrossWatch - title lookups derived from persisted baseline items
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from typing import Any

from .db import get_conn
from .schema import ID_KEYS

_LOOKUP_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl", "slug", "plex", "guid")

_PROVIDER_PRIORITY = {
    "PLEX": 100,
    "TRAKT": 80,
    "SIMKL": 70,
    "JELLYFIN": 60,
    "EMBY": 60,
    "CROSSWATCH": 55,
}
_DEFAULT_PRIORITY = 50


def _priority(provider: Any) -> int:
    return _PROVIDER_PRIORITY.get(str(provider or "").strip().upper(), _DEFAULT_PRIORITY)


def _text(value: Any) -> str:
    return str(value or "").strip()


def _year(value: Any) -> int | None:
    try:
        year = int(value)
    except Exception:
        return None
    return year if 1800 <= year <= 2200 else None


def history_title_rows(base_path: str | os.PathLike[str] | None = None) -> list[dict[str, Any]]:
    conn = get_conn(base_path)
    if conn is None:
        return []

    id_columns = ", ".join(f"b.ids_{key}" for key in ID_KEYS if key in _LOOKUP_ID_KEYS)
    show_id_columns = ", ".join(f"b.show_ids_{key}" for key in ID_KEYS if key in _LOOKUP_ID_KEYS)
    try:
        cursor = conn.execute(
            f"""
            SELECT s.provider, b.item_key, b.base_key, b.media_type,
                   b.title, b.year, b.series_title,
                   {id_columns}, {show_id_columns}
            FROM baseline_items b
            JOIN provider_feature_state s ON s.id = b.provider_state_id
            WHERE s.feature = 'history'
              AND (COALESCE(b.title, '') <> '' OR COALESCE(b.series_title, '') <> '')
            """
        )
        columns = [str(col[0]) for col in cursor.description or []]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except Exception:
        return []


def history_title_maps(
    base_path: str | os.PathLike[str] | None = None,
) -> tuple[dict[str, tuple[str, int | None]], dict[str, tuple[str, int | None]], dict[str, str]]:
    movie_key_map: dict[str, tuple[str, int | None]] = {}
    movie_id_map: dict[str, tuple[str, int | None]] = {}
    show_id_map: dict[str, str] = {}
    key_priority: dict[str, int] = {}
    id_priority: dict[str, int] = {}
    show_priority: dict[str, int] = {}

    lookup_keys = [key for key in ID_KEYS if key in _LOOKUP_ID_KEYS]

    for row in history_title_rows(base_path):
        priority = _priority(row.get("provider"))
        title = _text(row.get("title"))
        series_title = _text(row.get("series_title"))
        year = _year(row.get("year"))
        media_type = _text(row.get("media_type")).lower()

        if title and media_type not in ("episode", "season", "show"):
            value = (title, year)
            for raw_key in (row.get("item_key"), row.get("base_key")):
                key = _text(raw_key).lower()
                if not key:
                    continue
                for candidate in {key, key.split("#", 1)[0]}:
                    if candidate and priority > key_priority.get(candidate, -1):
                        movie_key_map[candidate] = value
                        key_priority[candidate] = priority
            for id_key in lookup_keys:
                raw = _text(row.get(f"ids_{id_key}"))
                if not raw:
                    continue
                candidate = f"{id_key}:{raw.lower()}"
                if priority > id_priority.get(candidate, -1):
                    movie_id_map[candidate] = value
                    id_priority[candidate] = priority

        if series_title:
            for prefix in ("show_ids", "ids"):
                for id_key in lookup_keys:
                    raw = _text(row.get(f"{prefix}_{id_key}"))
                    if not raw:
                        continue
                    candidate = f"{id_key}:{raw.lower()}"
                    if priority > show_priority.get(candidate, -1):
                        show_id_map[candidate] = series_title
                        show_priority[candidate] = priority

    return movie_key_map, movie_id_map, show_id_map
