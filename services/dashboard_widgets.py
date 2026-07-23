# services/dashboard_widgets.py
# CrossWatch - Main dashboard media widgets
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Iterable, Mapping

from services.activity import list_events

try:
    from _logging import log as _cw_log
except Exception:  # pragma: no cover
    _cw_log = None


_DEFAULT_INSTANCE = "default"
_HISTORY_BUCKET_SECONDS = 300
_METADATA_MANAGER: Any | None = None
_METADATA_MANAGER_FAILED = False
_ID_KEYS = (
    "tmdb",
    "imdb",
    "tvdb",
    "trakt",
    "simkl",
    "anilist",
    "mal",
    "kitsu",
    "anidb",
    "plex",
    "jellyfin",
    "mdblist",
    "emby",
    "guid",
    "slug",
)
_SHOW_ID_ALIASES = {
    "tmdb_show": "tmdb",
    "imdb_show": "imdb",
    "tvdb_show": "tvdb",
    "trakt_show": "trakt",
}
_EPISODE_CODE_RE = re.compile(r"^S\d{1,3}E\d{1,4}$", re.IGNORECASE)


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return number if number == number else None


def _nested_dict(item: Mapping[str, Any], key: str) -> dict[str, Any]:
    return _as_dict(item.get(key))


def _merge_ids(*values: Mapping[str, Any]) -> dict[str, Any]:
    ids: dict[str, Any] = {}
    for value in values:
        for key, raw in value.items():
            if raw not in (None, "", 0, False):
                ids.setdefault(str(key), raw)
    return ids


def _iso_epoch(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    text = str(value or "").strip()
    if not text:
        return 0
    if text.isdigit():
        try:
            n = int(text)
            return n // 1000 if len(text) >= 13 else n
        except Exception:
            return 0
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def _ids(item: Mapping[str, Any]) -> dict[str, Any]:
    ids = _as_dict(item.get("ids"))
    typ = _media_type(item)
    if typ == "movie":
        ids = _merge_ids(ids, _as_dict(_nested_dict(item, "movie").get("ids")))
    elif typ == "show":
        ids = _merge_ids(ids, _as_dict(_nested_dict(item, "show").get("ids")), _as_dict(_nested_dict(item, "series").get("ids")))
    elif typ == "episode":
        ids = _merge_ids(ids, _as_dict(_nested_dict(item, "episode").get("ids")))
    elif typ == "season":
        ids = _merge_ids(ids, _as_dict(_nested_dict(item, "season").get("ids")))

    for key in _ID_KEYS:
        value = item.get(key)
        if value not in (None, "", 0, False):
            ids.setdefault(key, value)
    show_ids = _merge_ids(_as_dict(ids.get("show_ids")), _as_dict(item.get("show_ids")))
    for source in (ids, item):
        for alias, key in _SHOW_ID_ALIASES.items():
            value = source.get(alias)
            if value not in (None, "", 0, False):
                show_ids.setdefault(key, value)
    for nested_key in ("show", "series"):
        nested_ids = _as_dict(_nested_dict(item, nested_key).get("ids"))
        show_ids = _merge_ids(show_ids, nested_ids)
    if show_ids:
        ids["show_ids"] = show_ids
    return ids


def _tmdb_id(item: Mapping[str, Any]) -> Any:
    ids = _ids(item)
    raw_show_ids = ids.get("show_ids")
    show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
    if _media_type(item) in {"episode", "season"}:
        return show_ids.get("tmdb") or ids.get("tmdb") or item.get("tmdb") or item.get("tmdb_id")
    return ids.get("tmdb") or show_ids.get("tmdb") or item.get("tmdb") or item.get("tmdb_id")


def _media_type(item: Mapping[str, Any]) -> str:
    raw = str(item.get("type") or item.get("media_type") or item.get("entity") or "").strip().lower()
    if raw in {"episode", "season"}:
        return raw
    if raw in {"tv", "show", "shows", "series", "anime"}:
        return "show"
    if isinstance(item.get("episode"), Mapping):
        return "episode"
    if isinstance(item.get("season"), Mapping):
        return "season"
    if isinstance(item.get("show"), Mapping) or isinstance(item.get("series"), Mapping):
        return "show"
    return "movie"


def _art_type(item: Mapping[str, Any]) -> str:
    return "movie" if _media_type(item) == "movie" else "tv"


def _rating_value(item: Mapping[str, Any]) -> int | None:
    raw = item.get("rating")
    if raw in (None, ""):
        raw = item.get("user_rating")
    if raw in (None, ""):
        return None
    try:
        n = int(round(float(raw)))
    except Exception:
        return None
    return n if 1 <= n <= 10 else None


def _text(value: Any) -> str:
    return str(value or "").strip()


def _norm_text(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().lower()).strip()


def _show_title(item: Mapping[str, Any]) -> str:
    for key in ("series_title", "show_title", "series_name", "show_name"):
        value = _text(item.get(key))
        if value:
            return value
    show = item.get("show") or item.get("series")
    if isinstance(show, Mapping):
        for key in ("title", "name", "series_title", "show_title"):
            value = _text(show.get(key))
            if value:
                return value
    return _text(show) if isinstance(show, str) else ""


def _movie_title(item: Mapping[str, Any]) -> str:
    movie = item.get("movie")
    if isinstance(movie, Mapping):
        for key in ("title", "name"):
            value = _text(movie.get(key))
            if value:
                return value
    return ""


def _season_title(item: Mapping[str, Any]) -> str:
    season = item.get("season")
    if isinstance(season, Mapping):
        return _text(season.get("title") or season.get("name"))
    return ""


def _episode_title(item: Mapping[str, Any]) -> str:
    episode = item.get("episode")
    if isinstance(episode, Mapping):
        return _text(episode.get("title") or episode.get("name"))
    return ""


def _title(item: Mapping[str, Any]) -> str:
    typ = _media_type(item)
    if typ in {"episode", "season"}:
        show = _show_title(item)
        if show:
            return show
    if typ == "movie":
        return _text(item.get("title") or item.get("name") or _movie_title(item) or "Untitled")
    if typ == "show":
        return _text(item.get("title") or item.get("name") or _show_title(item) or "Untitled")
    return _text(item.get("title") or item.get("name") or _season_title(item) or _episode_title(item) or "Untitled")


def _year(item: Mapping[str, Any]) -> int | None:
    year = _as_int(item.get("year") or item.get("release_year") or item.get("series_year"))
    if year is not None:
        return year
    for nested_key in ("movie", "show", "series"):
        nested = _nested_dict(item, nested_key)
        year = _as_int(nested.get("year") or nested.get("release_year") or nested.get("series_year"))
        if year is not None:
            return year
    return None


def _season_number(item: Mapping[str, Any]) -> int | None:
    season = item.get("season")
    if isinstance(season, Mapping):
        season = season.get("number")
    if season in (None, ""):
        season = _nested_dict(item, "episode").get("season")
    return _as_int(season)


def _episode_number(item: Mapping[str, Any]) -> int | None:
    episode = item.get("episode")
    if isinstance(episode, Mapping):
        episode = episode.get("number")
    if episode in (None, ""):
        episode = item.get("number")
    return _as_int(episode)


def _episode_label(item: Mapping[str, Any]) -> str:
    season = _season_number(item)
    episode = _episode_number(item)
    if season is None or episode is None:
        return ""
    return f"S{season:02d}E{episode:02d}"


def _provider_blocks(state: Mapping[str, Any], provider: str) -> Iterable[tuple[str, Mapping[str, Any]]]:
    providers = state.get("providers") if isinstance(state.get("providers"), Mapping) else {}
    block = providers.get(provider) if isinstance(providers, Mapping) else None
    if block is None and isinstance(providers, Mapping):
        wanted = str(provider or "").strip().upper()
        for key, value in providers.items():
            if str(key or "").strip().upper() == wanted:
                block = value
                break
    if not isinstance(block, Mapping):
        return
    yield _DEFAULT_INSTANCE, block
    instances = block.get("instances")
    if isinstance(instances, Mapping):
        for instance_id, instance_block in instances.items():
            if isinstance(instance_block, Mapping):
                yield str(instance_id or _DEFAULT_INSTANCE), instance_block


def _feature_items(block: Mapping[str, Any], feature: str) -> dict[str, Any]:
    try:
        items = block[feature]["baseline"]["items"]  # type: ignore[index]
    except Exception:
        return {}
    return dict(items) if isinstance(items, Mapping) else {}

_RATING_TIME_KEYS = ("rated_at", "ratedAt", "user_rated_at", "user_ratedAt")
_UPDATE_TIME_KEYS = ("updated_at", "updatedAt")

def _rating_time(item: Mapping[str, Any]) -> Any:
    for key in _RATING_TIME_KEYS:
        value = item.get(key)
        if _iso_epoch(value):
            return value
    return ""


def _rating_epoch(item: Mapping[str, Any]) -> int:
    value = _rating_time(item)
    return _iso_epoch(value) if value else 0



def _update_epoch(item: Mapping[str, Any]) -> int:
    for key in _UPDATE_TIME_KEYS:
        value = item.get(key)
        ts = _iso_epoch(value)
        if ts:
            return ts
    return 0

def _unwrap_rating_item(value: Any) -> dict[str, Any]:
    item = _as_dict(value)
    nested = _as_dict(item.get("item"))
    if nested:
        merged = dict(nested)
        for key in ("rating", "user_rating", *_RATING_TIME_KEYS, *_UPDATE_TIME_KEYS):
            if item.get(key) not in (None, ""):
                merged[key] = item[key]
        return merged
    return item


def _unwrap_history_item(value: Any) -> dict[str, Any]:
    item = _as_dict(value)
    nested = _as_dict(item.get("item"))
    if nested:
        merged = dict(nested)
        for key in ("watched_at", "watchedAt", "viewed_at", "captured_at"):
            if item.get(key) not in (None, ""):
                merged[key] = item[key]
        return merged
    return item


def _canonical_key(raw_key: str, item: Mapping[str, Any]) -> str:
    ids = _ids(item)
    for key in ("tmdb", "imdb", "tvdb", "trakt", "simkl", "anilist", "mal"):
        value = ids.get(key)
        if value not in (None, "", 0, False):
            return f"{key}:{value}"
    return str(raw_key or _title(item)).strip().lower()


def _history_key(raw_key: str, item: Mapping[str, Any]) -> str:
    ids = _ids(item)
    raw_show_ids = ids.get("show_ids")
    show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
    identity_ids = show_ids if _media_type(item) in {"episode", "season"} and show_ids else ids
    base = ""
    for key in _ID_KEYS:
        value = identity_ids.get(key)
        if value not in (None, "", 0, False):
            base = f"{key}:{value}"
            break
    if not base:
        base = str(raw_key or _title(item)).strip().lower()
    return ":".join(
        [
            _media_type(item),
            base,
            "" if _season_number(item) is None else str(_season_number(item)),
            "" if _episode_number(item) is None else str(_episode_number(item)),
            str(_history_sort_epoch(item) or _history_key_epoch(raw_key)),
        ]
    )



def _history_sort_epoch(item: Mapping[str, Any]) -> int:
    for key in ("captured_at", "watched_at", "watchedAt", "viewed_at"):
        ts = _iso_epoch(item.get(key))
        if ts:
            return ts
    return 0


def _history_key_epoch(raw_key: str) -> int:
    suffix = str(raw_key or "").rsplit("@", 1)[-1]
    return _iso_epoch(suffix) if suffix != raw_key else 0


def _time_bucket(value: Any) -> int:
    epoch = _as_int(value) or 0
    return int(epoch // _HISTORY_BUCKET_SECONDS) if epoch > 0 else 0


def _episode_still_url(item: Mapping[str, Any], *, size: str) -> str:
    if _media_type(item) != "episode":
        return ""
    tmdb = _tmdb_id(item)
    season = _season_number(item)
    episode = _episode_number(item)
    if tmdb in (None, "", 0, False) or season is None or episode is None:
        return ""
    return f"/art/tmdb/tv/{tmdb}?kind=still&season={season}&episode={episode}&size={size}&artv=2"


def _resolved_art_type(item: Mapping[str, Any], tmdb: Any) -> str:
    requested = _art_type(item)
    if _media_type(item) == "episode":
        return requested
    try:
        from api.metaAPI import _resolve_entity

        ids = _ids(item)
        resolved = _resolve_entity(
            "movie" if requested == "movie" else "show",
            tmdb,
            title=_title(item),
            year=_year(item),
            imdb_id=ids.get("imdb"),
            tvdb_id=ids.get("tvdb"),
        )
    except Exception:
        return requested
    return "movie" if resolved == "movie" else "tv"


def _poster_url(item: Mapping[str, Any], *, size: str = "w342", episode_still: bool = False) -> str:
    if episode_still:
        still = _episode_still_url(item, size=size)
        if still:
            return still
    tmdb = _tmdb_id(item)
    if tmdb in (None, "", 0, False):
        return ""
    return f"/art/tmdb/{_resolved_art_type(item, tmdb)}/{tmdb}?size={size}"


def _grid_art_url(item: Mapping[str, Any], *, size: str = "w300") -> str:
    still = _episode_still_url(item, size=size)
    if still:
        return still
    tmdb = _tmdb_id(item)
    if tmdb in (None, "", 0, False):
        return ""
    return f"/art/tmdb/{_resolved_art_type(item, tmdb)}/{tmdb}?kind=backdrop&size={size}"


def _cover_url(item: Mapping[str, Any], *, size: str = "w342") -> str:
    return _poster_url(item, size=size, episode_still=False)


def _metadata_manager() -> Any | None:
    global _METADATA_MANAGER, _METADATA_MANAGER_FAILED
    if _METADATA_MANAGER is not None:
        return _METADATA_MANAGER
    if _METADATA_MANAGER_FAILED:
        return None
    try:
        import crosswatch as CW  # type: ignore

        manager = getattr(CW, "_METADATA", None)
        if manager is not None:
            _METADATA_MANAGER = manager
            return manager
    except Exception:
        pass
    try:
        from cw_platform.config_base import load_config, save_config
        from cw_platform.metadata import MetadataManager

        _METADATA_MANAGER = MetadataManager(load_config, save_config)
        return _METADATA_MANAGER
    except Exception:
        _METADATA_MANAGER_FAILED = True
        return None


def _metadata_lookup_ids(row: Mapping[str, Any]) -> dict[str, Any]:
    ids = _ids(row)
    out: dict[str, Any] = {}
    is_movie = _art_type(row) == "movie"
    show_ids = ids.get("show_ids") if isinstance(ids.get("show_ids"), Mapping) else {}
    source = show_ids if not is_movie and show_ids else ids
    for key in _ID_KEYS:
        value = source.get(key) if isinstance(source, Mapping) else None
        if value not in (None, "", 0, False):
            out[key] = value
    title = _text(row.get("title"))
    if title:
        out["title"] = title
    year = _as_int(row.get("year"))
    if is_movie and year is not None:
        out["year"] = str(year)
    return out


def _looks_like_episode_code(value: Any) -> bool:
    return bool(_EPISODE_CODE_RE.match(str(value or "").strip()))


def _resolve_episode_show_title(row: dict[str, Any]) -> None:
    if _media_type(row) != "episode":
        return
    if not _looks_like_episode_code(row.get("title")):
        return
    ids = _metadata_lookup_ids(row)
    ids.pop("title", None)
    if not any(ids.get(key) for key in ("tmdb", "imdb", "tvdb", "trakt")):
        return
    manager = _metadata_manager()
    if manager is None:
        return
    try:
        res = manager.resolve(
            entity="show",
            ids=ids,
            need={"title": True, "ids": True},
            strategy="first_success",
        ) or {}
    except Exception:
        return
    title = _text(res.get("title") or res.get("name"))
    if not title or _looks_like_episode_code(title):
        return
    row["title"] = title
    row.setdefault("series_title", title)


def _art_debug(row: dict[str, Any], reason: str, **fields: Any) -> None:
    row["art_reason"] = reason
    try:
        if _cw_log is None:
            return
        _cw_log(
            f"dashboard art {reason}",
            level="DEBUG",
            module="DASH",
            extra={
                "title": row.get("title"),
                "type": row.get("type"),
                "reason": reason,
                **{k: v for k, v in fields.items() if v not in (None, "", [], {})},
            },
        )
    except Exception:
        return


def _resolve_missing_art(
    row: dict[str, Any], *, size: str, episode_still: bool = False, backdrop_fallback: bool = False
) -> None:
    if row.get("poster") or row.get("tmdb"):
        row["art_reason"] = "existing_tmdb"
        return
    ids = _metadata_lookup_ids(row)
    if not ids.get("title") and not any(ids.get(key) for key in ("imdb", "tmdb")):
        _art_debug(row, "missing_lookup_identity")
        return
    manager = _metadata_manager()
    if manager is None:
        _art_debug(row, "metadata_unavailable", lookup_keys=sorted(ids.keys()))
        return
    try:
        res = manager.resolve(
            entity="movie" if _art_type(row) == "movie" else "show",
            ids=ids,
            need={"poster": True, "backdrop": False, "overview": False, "ids": True},
            strategy="first_success",
        ) or {}
    except Exception as exc:
        _art_debug(row, "metadata_error", error=str(exc), lookup_keys=sorted(ids.keys()))
        return
    resolved_ids = res.get("ids") if isinstance(res.get("ids"), Mapping) else {}
    tmdb = resolved_ids.get("tmdb") if isinstance(resolved_ids, Mapping) else None
    if tmdb in (None, "", 0, False):
        _art_debug(row, "metadata_no_tmdb", lookup_keys=sorted(ids.keys()))
        return
    row["tmdb"] = tmdb
    current_ids = dict(row.get("ids") or {}) if isinstance(row.get("ids"), Mapping) else {}
    if _art_type(row) == "movie":
        current_ids.setdefault("tmdb", tmdb)
    else:
        show_ids = dict(current_ids.get("show_ids") or {}) if isinstance(current_ids.get("show_ids"), Mapping) else {}
        show_ids.setdefault("tmdb", tmdb)
        current_ids.setdefault("show_ids", show_ids)
    row["ids"] = current_ids
    row["poster"] = _grid_art_url(row, size=size) if backdrop_fallback else _poster_url(row, size=size, episode_still=episode_still)
    _art_debug(row, "metadata_resolved", tmdb=tmdb)


def _ensure_cover_art(row: dict[str, Any], *, size: str) -> None:
    if row.get("cover"):
        return
    cover = _cover_url(row, size=size)
    if cover:
        row["cover"] = cover


def _resolve_missing_art_rows(
    rows: list[dict[str, Any]],
    *,
    size: str,
    episode_still: bool = False,
    cover_size: str = "w342",
    backdrop_fallback: bool = False,
) -> list[dict[str, Any]]:
    for row in rows:
        _resolve_episode_show_title(row)
        _resolve_missing_art(row, size=size, episode_still=episode_still, backdrop_fallback=backdrop_fallback)
        _ensure_cover_art(row, size=cover_size)
    return rows


def _provider_ref(provider: str, instance: str) -> dict[str, str]:
    return {"provider": str(provider or "").upper(), "instance": str(instance or _DEFAULT_INSTANCE)}


def _sources_from_item(item: Mapping[str, Any], *, default_provider: str = "CROSSWATCH") -> list[dict[str, str]]:
    out: list[dict[str, str]] = []

    direct = item.get("sources")
    if isinstance(direct, list):
        for value in direct:
            provider = str(value or "").strip().upper()
            if provider:
                out.append(_provider_ref(provider, _DEFAULT_INSTANCE))

    by_provider = item.get("sources_by_provider") or item.get("sourcesByProvider")
    if isinstance(by_provider, Mapping):
        for provider, instances in by_provider.items():
            provider_key = str(provider or "").strip().upper()
            if not provider_key:
                continue
            if isinstance(instances, list) and instances:
                for instance in instances:
                    out.append(_provider_ref(provider_key, str(instance or _DEFAULT_INSTANCE)))
            else:
                out.append(_provider_ref(provider_key, _DEFAULT_INSTANCE))

    for key in ("provider", "source", "target"):
        provider = str(item.get(key) or "").strip().upper()
        if provider:
            out.append(_provider_ref(provider, str(item.get(f"{key}_instance") or _DEFAULT_INSTANCE)))

    if not out and default_provider:
        out.append(_provider_ref(default_provider, _DEFAULT_INSTANCE))

    seen: set[tuple[str, str]] = set()
    clean: list[dict[str, str]] = []
    for source in out:
        key = (source["provider"], source["instance"])
        if not key[0] or key in seen:
            continue
        seen.add(key)
        clean.append(source)
    return clean


def _rating_row(raw_key: str, item: Mapping[str, Any], sources: list[dict[str, str]]) -> dict[str, Any] | None:
    rating = _rating_value(item)
    if rating is None:
        return None
    typ = _media_type(item)
    return {
        "key": _canonical_key(raw_key, item),
        "type": typ,
        "art_type": _resolved_art_type(item, _tmdb_id(item)),
        "title": _title(item),
        "year": _year(item),
        "season": _season_number(item),
        "episode": _episode_number(item),
        "episode_label": _episode_label(item),
        "rating": rating,
        "rated_at": str(_rating_time(item) or ""),
        "sort_epoch": _rating_epoch(item),
        "updated_epoch": _update_epoch(item),
        "ids": _ids(item),
        "tmdb": _tmdb_id(item),
        "poster": _grid_art_url(item, size="w300"),
        "cover": _cover_url(item, size="w342"),
        "sources": sources,
    }


def _rating_aliases(row: Mapping[str, Any]) -> list[str]:
    title = _norm_text(row.get("title"))
    if not title:
        return []
    typ = str(row.get("type") or "").strip().lower() or "movie"
    season = _as_int(row.get("season"))
    episode = _as_int(row.get("episode"))
    year = _as_int(row.get("year"))
    aliases = []
    if typ == "episode" and season is not None and episode is not None:
        aliases.append(f"rating|episode|{title}|s{season}|e{episode}")
    elif typ == "season" and season is not None:
        aliases.append(f"rating|season|{title}|s{season}")
    else:
        aliases.append(f"rating|{typ}|{title}|y{year or ''}")
    return aliases


def _copy_richer_media_fields(dst: dict[str, Any], src: Mapping[str, Any]) -> None:
    for key in ("tmdb", "poster", "cover", "ids"):
        if not dst.get(key) and src.get(key):
            dst[key] = src[key]
    if not dst.get("art_type") and src.get("art_type"):
        dst["art_type"] = src["art_type"]
    if not dst.get("year") and src.get("year"):
        dst["year"] = src["year"]


def _merge_sources(dst: dict[str, Any], src: Mapping[str, Any]) -> None:
    dst_sources = dst.setdefault("sources", [])
    for source in src.get("sources") or []:
        if source not in dst_sources:
            dst_sources.append(source)


def _merge_media_row(prev: Mapping[str, Any], row: Mapping[str, Any], *, sort_key: str) -> dict[str, Any]:
    prev_row = dict(prev)
    next_row = dict(row)
    _merge_sources(prev_row, next_row)
    _merge_sources(next_row, prev_row)
    if int(next_row.get(sort_key) or 0) >= int(prev_row.get(sort_key) or 0):
        chosen = next_row
        other = prev_row
    else:
        chosen = prev_row
        other = next_row
    _copy_richer_media_fields(chosen, other)
    return chosen


_RATING_TRACKER_FLAG = "_tracker_rating"


def _merge_rating_row(prev: Mapping[str, Any], row: Mapping[str, Any]) -> dict[str, Any]:
    prev_row = dict(prev)
    next_row = dict(row)
    prev_tracker = bool(prev_row.get(_RATING_TRACKER_FLAG))
    next_tracker = bool(next_row.get(_RATING_TRACKER_FLAG))
    if prev_tracker == next_tracker:
        return _merge_media_row(prev_row, next_row, sort_key="sort_epoch")
    chosen, other = (prev_row, next_row) if prev_tracker else (next_row, prev_row)
    _merge_sources(chosen, other)
    _copy_richer_media_fields(chosen, other)
    if not chosen.get("type") and other.get("type"):
        chosen["type"] = other["type"]
    return chosen


def _tracker_feature_items(kind: str) -> dict[str, Any]:
    try:
        from services.editor import load_state

        data = load_state(kind)  # type: ignore[arg-type]
    except Exception:
        return {}
    items = data.get("items") if isinstance(data, Mapping) else {}
    return dict(items) if isinstance(items, Mapping) else {}


def latest_ratings_widget(
    state: Mapping[str, Any],
    *,
    limit: int = 12,
    tracker_items: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows: dict[str, dict[str, Any]] = {}
    aliases: dict[str, str] = {}

    def put(row: dict[str, Any]) -> None:
        key = str(row["key"])
        match_key = key
        for alias in _rating_aliases(row):
            if alias in aliases:
                match_key = aliases[alias]
                break
        prev = rows.get(match_key)
        if prev:
            rows[match_key] = _merge_rating_row(prev, row)
        else:
            rows[match_key] = row
        for alias in _rating_aliases(rows[match_key]):
            aliases[alias] = match_key

    for raw_key, raw_item in (tracker_items or {}).items():
        item = _unwrap_rating_item(raw_item)
        row = _rating_row(str(raw_key), item, _sources_from_item(item))
        if row:
            row[_RATING_TRACKER_FLAG] = True
            put(row)

    providers = state.get("providers") if isinstance(state.get("providers"), Mapping) else {}
    provider_keys = sorted({str(p).upper() for p in providers.keys()}) if isinstance(providers, Mapping) else []
    for provider in provider_keys:
        for instance, block in _provider_blocks(state, provider):
            for raw_key, raw_item in _feature_items(block, "ratings").items():
                item = _unwrap_rating_item(raw_item)
                row = _rating_row(str(raw_key), item, [_provider_ref(provider, instance)])
                if not row:
                    continue
                put(row)

    items = sorted(
        rows.values(),
        key=lambda x: (
            int(x.get("sort_epoch") or 0),
            int(x.get("updated_epoch") or 0),
            str(x.get("title") or ""),
        ),
        reverse=True,
    )
    cap = max(1, min(int(limit or 12), 24))
    selected = _resolve_missing_art_rows(items[:cap], size="w300", episode_still=True, backdrop_fallback=True)
    for row in selected:
        row.pop(_RATING_TRACKER_FLAG, None)
    return {"ok": True, "items": selected, "total": len(items)}


def _activity_row(event: Mapping[str, Any]) -> dict[str, Any]:
    typ = _media_type(event)
    raw_targets = event.get("targets")
    targets: list[Any] = raw_targets if isinstance(raw_targets, list) else []
    source = _provider_ref(str(event.get("source") or ""), str(event.get("source_instance") or _DEFAULT_INSTANCE))
    target_refs: list[dict[str, str]] = []
    for target in targets:
        if isinstance(target, Mapping):
            target_refs.append(_provider_ref(str(target.get("target") or ""), str(target.get("target_instance") or _DEFAULT_INSTANCE)))
    if not targets and event.get("target"):
        target_refs.append(_provider_ref(str(event.get("target") or ""), str(event.get("target_instance") or _DEFAULT_INSTANCE)))

    seen: set[tuple[str, str]] = set()
    clean_sources: list[dict[str, str]] = []
    clean_targets: list[dict[str, str]] = []
    for endpoint in [source, *target_refs]:
        key = (endpoint["provider"], endpoint["instance"])
        if not key[0] or key in seen:
            continue
        seen.add(key)
        clean_sources.append(endpoint)
        if endpoint is not source:
            clean_targets.append(endpoint)

    return {
        "id": str(event.get("id") or ""),
        "key": _history_key(str(event.get("id") or ""), event),
        "type": typ,
        "art_type": _art_type(event),
        "title": _title(event),
        "year": _year(event),
        "season": _season_number(event),
        "episode": _episode_number(event),
        "episode_label": _episode_label(event),
        "watched_at": _as_int(event.get("watched_at")) or _as_int(event.get("captured_at")) or 0,
        "captured_at": _as_int(event.get("captured_at")) or 0,
        "sort_epoch": _history_sort_epoch(event),
        "status": str(event.get("status") or "").lower(),
        "event": str(event.get("event") or "").lower(),
        "method": str(event.get("method") or "").lower(),
        "ids": _ids(event),
        "tmdb": _tmdb_id(event),
        "poster": _poster_url(event, size="w300", episode_still=True),
        "cover": _cover_url(event, size="w342"),
        "source": source,
        "targets": clean_targets,
        "sources": clean_sources,
    }


def _history_state_row(raw_key: str, item: Mapping[str, Any], sources: list[dict[str, str]]) -> dict[str, Any] | None:
    sort_epoch = _history_sort_epoch(item) or _history_key_epoch(raw_key)
    if sort_epoch <= 0:
        return None
    return {
        "id": str(raw_key or ""),
        "key": _history_key(raw_key, item),
        "type": _media_type(item),
        "art_type": _art_type(item),
        "title": _title(item),
        "year": _year(item),
        "season": _season_number(item),
        "episode": _episode_number(item),
        "episode_label": _episode_label(item),
        "watched_at": sort_epoch,
        "captured_at": 0,
        "sort_epoch": sort_epoch,
        "status": "ok",
        "event": "history_state",
        "method": "sync_state",
        "ids": _ids(item),
        "tmdb": _tmdb_id(item),
        "poster": _poster_url(item, size="w300", episode_still=True),
        "cover": _cover_url(item, size="w342"),
        "sources": sources,
    }


def _history_alias_representatives() -> dict[str, str]:
    try:
        import json

        from cw_platform.config_base import load_config
        from services.analyzer import CWS_DIR, _alias_destination_key, _expected_alias_scopes
    except Exception:
        return {}
    if not CWS_DIR.exists() or not CWS_DIR.is_dir():
        return {}
    try:
        expected = _expected_alias_scopes(load_config() or {})
    except Exception:
        return {}
    if not expected:
        return {}

    out: dict[str, str] = {}
    for path in sorted(CWS_DIR.glob("*history.pair_alias*.json")):
        try:
            doc = json.loads(path.read_text("utf-8"))
        except Exception:
            continue
        if not isinstance(doc, Mapping):
            continue
        if str(doc.get("scope") or "").strip() not in expected:
            continue
        items = doc.get("items")
        if not isinstance(items, Mapping):
            continue
        for src_event_key, rec in items.items():
            if not isinstance(rec, Mapping):
                continue
            dest = _alias_destination_key(rec)
            src_base = str(src_event_key or "").split("@", 1)[0]
            if not dest or not src_base or dest == src_base:
                continue
            out[src_base] = dest
            out[dest] = dest
    return out


def _history_aliases(row: Mapping[str, Any], alias_map: Mapping[str, str] | None = None) -> list[str]:
    rep = (alias_map or {}).get(str(row.get("id") or "").split("@", 1)[0])
    title = _norm_text(row.get("title"))
    if not title:
        return [f"history|xalias|{rep}"] if rep else []
    typ = str(row.get("type") or "").strip().lower() or "movie"
    bucket = _time_bucket(row.get("sort_epoch") or row.get("watched_at"))
    year = _as_int(row.get("year"))
    season = _as_int(row.get("season"))
    episode = _as_int(row.get("episode"))
    aliases: list[str] = []
    if rep:
        aliases.append(f"history|xalias|{rep}")
    if typ == "episode" and season is not None and episode is not None:
        aliases.append(f"history|episode|{title}|s{season}|e{episode}|b{bucket}")
        aliases.append(f"history|episode|{title}|s{season}|e{episode}")
    elif typ == "season" and season is not None:
        aliases.append(f"history|season|{title}|s{season}|b{bucket}")
        aliases.append(f"history|season|{title}|s{season}")
    else:
        aliases.append(f"history|{typ}|{title}|y{year or ''}|b{bucket}")
        if year:
            aliases.append(f"history|{typ}|{title}|y{year}")
    return aliases


def _latest_history_state_rows(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    providers = state.get("providers") if isinstance(state.get("providers"), Mapping) else {}
    provider_keys = sorted({str(p).upper() for p in providers.keys()}) if isinstance(providers, Mapping) else []
    for provider in provider_keys:
        for instance, block in _provider_blocks(state, provider):
            for raw_key, raw_item in _feature_items(block, "history").items():
                item = _unwrap_history_item(raw_item)
                row = _history_state_row(str(raw_key), item, [_provider_ref(provider, instance)])
                if not row:
                    continue
                key = str(row["key"])
                prev = rows.get(key)
                if not prev:
                    rows[key] = row
                    continue
                prev_sources = prev.setdefault("sources", [])
                for src in row.get("sources") or []:
                    if src not in prev_sources:
                        prev_sources.append(src)
                if int(row.get("sort_epoch") or 0) >= int(prev.get("sort_epoch") or 0):
                    row["sources"] = prev_sources
                    rows[key] = row
    return sorted(rows.values(), key=lambda x: int(x.get("sort_epoch") or 0), reverse=True)


def _latest_history_tracker_rows(items: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for raw_key, raw_item in (items or {}).items():
        item = _unwrap_history_item(raw_item)
        row = _history_state_row(str(raw_key), item, _sources_from_item(item))
        if not row:
            continue
        rows[str(row["key"])] = row
    return sorted(rows.values(), key=lambda x: int(x.get("sort_epoch") or 0), reverse=True)


def _merge_history_rows(
    *groups: Iterable[Mapping[str, Any]],
    alias_map: Mapping[str, str] | None = None,
) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    aliases: dict[str, str] = {}
    for group in groups:
        for raw in group:
            row = dict(raw)
            key = str(row.get("key") or row.get("id") or "")
            if not key:
                continue
            match_key = key
            for alias in _history_aliases(row, alias_map):
                if alias in aliases:
                    match_key = aliases[alias]
                    break
            prev = rows.get(match_key)
            if not prev:
                rows[match_key] = row
            else:
                rows[match_key] = _merge_media_row(prev, row, sort_key="sort_epoch")
            for alias in _history_aliases(rows[match_key], alias_map):
                aliases[alias] = match_key
            for alias in _history_aliases(row, alias_map):
                aliases.setdefault(alias, match_key)
    return sorted(rows.values(), key=lambda x: int(x.get("sort_epoch") or 0), reverse=True)


def recent_history_widget(
    state: Mapping[str, Any] | None = None,
    *,
    limit: int = 8,
    tracker_items: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cap = max(1, min(int(limit or 8), 24))
    state_rows = _latest_history_state_rows(state or {})
    tracker_rows = _latest_history_tracker_rows(tracker_items or {})
    rows = _merge_history_rows(state_rows, tracker_rows, alias_map=_history_alias_representatives())
    selected = _resolve_missing_art_rows(rows[:cap], size="w300", episode_still=True)
    return {"ok": True, "items": selected, "total": len(rows)}


def recent_scrobble_widget(*, limit: int = 8) -> dict[str, Any]:
    cap = max(1, min(int(limit or 8), 24))
    payload = list_events(limit=max(cap, 12), offset=0, status="ok", kind="all", group_routes=True)
    rows = [
        _activity_row(item)
        for item in payload.get("items") or []
        if isinstance(item, Mapping) and str(item.get("kind") or "").strip().lower() in {"scrobble", "history_sync"}
    ]
    selected = _resolve_missing_art_rows(rows[:cap], size="w300", episode_still=True)
    return {"ok": True, "items": selected, "total": len(rows)}


def _progress_epoch(item: Mapping[str, Any], raw_key: str = "") -> int:
    for key in (
        "progress_at",
        "updated_at",
        "last_updated",
        "synced_at",
        "captured_at",
        "created_at",
        "watched_at",
    ):
        epoch = _iso_epoch(item.get(key))
        if epoch > 0:
            return epoch
    return _history_key_epoch(raw_key)


def _progress_value(item: Mapping[str, Any]) -> float | None:
    for key in ("progress_percent", "progress", "percent", "position_percent", "resume_percent"):
        value = _as_float(item.get(key))
        if value is not None:
            return max(0.0, min(100.0, round(value, 1)))
    progress_ms = _as_float(item.get("progress_ms") or item.get("viewOffset") or item.get("view_offset"))
    duration_ms = _as_float(item.get("duration_ms") or item.get("duration"))
    if progress_ms is not None and duration_ms and duration_ms > 0:
        return max(0.0, min(100.0, round((progress_ms / duration_ms) * 100.0, 1)))
    return None


def _progress_row(raw_key: str, item: Mapping[str, Any], sources: list[dict[str, str]]) -> dict[str, Any] | None:
    sort_epoch = _progress_epoch(item, raw_key)
    if sort_epoch <= 0:
        return None
    return {
        "id": str(raw_key or ""),
        "key": _history_key(raw_key, item),
        "type": _media_type(item),
        "art_type": _art_type(item),
        "title": _title(item),
        "year": _year(item),
        "season": _season_number(item),
        "episode": _episode_number(item),
        "episode_label": _episode_label(item),
        "progress": _progress_value(item),
        "sort_epoch": sort_epoch,
        "ids": _ids(item),
        "tmdb": _tmdb_id(item),
        "poster": _poster_url(item, size="w300", episode_still=True),
        "cover": _cover_url(item, size="w342"),
        "sources": sources,
    }


def recent_progress_widget(
    state: Mapping[str, Any] | None = None,
    *,
    limit: int = 8,
    tracker_items: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    rows: dict[str, dict[str, Any]] = {}

    def put(row: dict[str, Any]) -> None:
        key = str(row.get("key") or row.get("id") or "")
        if not key:
            return
        prev = rows.get(key)
        rows[key] = _merge_media_row(prev, row, sort_key="sort_epoch") if prev else row

    for raw_key, raw_item in (tracker_items or {}).items():
        item = raw_item if isinstance(raw_item, Mapping) else {}
        row = _progress_row(str(raw_key), item, _sources_from_item(item))
        if row:
            put(row)

    providers = (state or {}).get("providers") if isinstance((state or {}).get("providers"), Mapping) else {}
    provider_keys = sorted({str(p).upper() for p in providers.keys()}) if isinstance(providers, Mapping) else []
    for provider in provider_keys:
        for instance, block in _provider_blocks(state or {}, provider):
            for raw_key, raw_item in _feature_items(block, "progress").items():
                item = raw_item if isinstance(raw_item, Mapping) else {}
                row = _progress_row(str(raw_key), item, [_provider_ref(provider, instance)])
                if row:
                    put(row)

    items = sorted(rows.values(), key=lambda x: int(x.get("sort_epoch") or 0), reverse=True)
    cap = max(1, min(int(limit or 8), 24))
    selected = _resolve_missing_art_rows(items[:cap], size="w300", episode_still=True)
    return {"ok": True, "items": selected, "total": len(items)}


def recent_playlists_widget(*, limit: int = 8) -> dict[str, Any]:
    try:
        from cw_platform.config_base import load_config
        from services import playlists

        rows = playlists.activity(load_config() or {}, limit=max(1, min(int(limit or 8), 24)))
    except Exception:
        rows = []
    return {"ok": True, "items": rows, "total": len(rows)}


def dashboard_widgets_payload(
    state: Mapping[str, Any],
    *,
    history_limit: int = 8,
    ratings_limit: int = 12,
    scrobble_limit: int = 8,
    progress_limit: int = 8,
    playlists_limit: int = 8,
    include: set[str] | None = None,
) -> dict[str, Any]:
    requested = {str(key).strip().lower() for key in include} if include is not None else {
        "history",
        "ratings",
        "scrobble",
        "progress",
        "playlists",
    }
    payload: dict[str, Any] = {"ok": True}
    if "history" in requested:
        payload["recent_history"] = recent_history_widget(
            state,
            limit=history_limit,
            tracker_items=_tracker_feature_items("history"),
        )
    if "scrobble" in requested:
        payload["recent_scrobble"] = recent_scrobble_widget(limit=scrobble_limit)
    if "ratings" in requested:
        payload["latest_ratings"] = latest_ratings_widget(
            state,
            limit=ratings_limit,
            tracker_items=_tracker_feature_items("ratings"),
        )
    if "progress" in requested:
        payload["recent_progress"] = recent_progress_widget(
            state,
            limit=progress_limit,
            tracker_items=_tracker_feature_items("progress"),
        )
    if "playlists" in requested:
        payload["recent_playlists"] = recent_playlists_widget(limit=playlists_limit)
    return payload
