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


def _unwrap_rating_item(value: Any) -> dict[str, Any]:
    item = _as_dict(value)
    nested = _as_dict(item.get("item"))
    if nested:
        merged = dict(nested)
        for key in ("rating", "user_rating", "rated_at", "ratedAt"):
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


def _rating_sort_epoch(item: Mapping[str, Any]) -> int:
    for key in ("rated_at", "ratedAt", "user_rated_at", "user_ratedAt", "updated_at", "updatedAt"):
        ts = _iso_epoch(item.get(key))
        if ts:
            return ts
    return 0


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


def _poster_url(item: Mapping[str, Any], *, size: str = "w342", episode_still: bool = False) -> str:
    if episode_still:
        still = _episode_still_url(item, size=size)
        if still:
            return still
    tmdb = _tmdb_id(item)
    if tmdb in (None, "", 0, False):
        return ""
    return f"/art/tmdb/{_art_type(item)}/{tmdb}?size={size}"


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


def _resolve_missing_art(row: dict[str, Any], *, size: str, episode_still: bool = False) -> None:
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
    row["poster"] = _poster_url(row, size=size, episode_still=episode_still)
    _art_debug(row, "metadata_resolved", tmdb=tmdb)


def _resolve_missing_art_rows(rows: list[dict[str, Any]], *, size: str, episode_still: bool = False) -> list[dict[str, Any]]:
    for row in rows:
        _resolve_missing_art(row, size=size, episode_still=episode_still)
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
        "art_type": _art_type(item),
        "title": _title(item),
        "year": _year(item),
        "season": _season_number(item),
        "episode": _episode_number(item),
        "episode_label": _episode_label(item),
        "rating": rating,
        "rated_at": str(item.get("rated_at") or item.get("ratedAt") or item.get("user_rated_at") or item.get("user_ratedAt") or ""),
        "sort_epoch": _rating_sort_epoch(item),
        "ids": _ids(item),
        "tmdb": _tmdb_id(item),
        "poster": _poster_url(item),
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
    for key in ("tmdb", "poster", "ids"):
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
            rows[match_key] = _merge_media_row(prev, row, sort_key="sort_epoch")
        else:
            rows[match_key] = row
        for alias in _rating_aliases(rows[match_key]):
            aliases[alias] = match_key

    for raw_key, raw_item in (tracker_items or {}).items():
        item = _unwrap_rating_item(raw_item)
        row = _rating_row(str(raw_key), item, _sources_from_item(item))
        if row:
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

    items = sorted(rows.values(), key=lambda x: (int(x.get("sort_epoch") or 0), str(x.get("title") or "")), reverse=True)
    cap = max(1, min(int(limit or 12), 24))
    selected = _resolve_missing_art_rows(items[:cap], size="w342")
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
        "sources": sources,
    }


def _history_aliases(row: Mapping[str, Any]) -> list[str]:
    title = _norm_text(row.get("title"))
    if not title:
        return []
    typ = str(row.get("type") or "").strip().lower() or "movie"
    bucket = _time_bucket(row.get("sort_epoch") or row.get("watched_at"))
    year = _as_int(row.get("year"))
    season = _as_int(row.get("season"))
    episode = _as_int(row.get("episode"))
    aliases: list[str] = []
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


def _merge_history_rows(*groups: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    aliases: dict[str, str] = {}
    for group in groups:
        for raw in group:
            row = dict(raw)
            key = str(row.get("key") or row.get("id") or "")
            if not key:
                continue
            match_key = key
            for alias in _history_aliases(row):
                if alias in aliases:
                    match_key = aliases[alias]
                    break
            prev = rows.get(match_key)
            if not prev:
                rows[match_key] = row
            else:
                rows[match_key] = _merge_media_row(prev, row, sort_key="sort_epoch")
            for alias in _history_aliases(rows[match_key]):
                aliases[alias] = match_key
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
    rows = _merge_history_rows(state_rows, tracker_rows)
    selected = _resolve_missing_art_rows(rows[:cap], size="w300", episode_still=True)
    return {"ok": True, "items": selected, "total": len(rows)}


def recent_scrobble_widget(*, limit: int = 8) -> dict[str, Any]:
    cap = max(1, min(int(limit or 8), 24))
    payload = list_events(limit=max(cap, 12), offset=0, status="ok", kind="scrobble", group_routes=True)
    rows = [_activity_row(item) for item in payload.get("items") or [] if isinstance(item, Mapping)]
    selected = _resolve_missing_art_rows(rows[:cap], size="w300", episode_still=True)
    return {"ok": True, "items": selected, "total": len(rows)}


def dashboard_widgets_payload(
    state: Mapping[str, Any],
    *,
    history_limit: int = 8,
    ratings_limit: int = 12,
    scrobble_limit: int = 8,
) -> dict[str, Any]:
    history_items = _tracker_feature_items("history")
    ratings_items = _tracker_feature_items("ratings")
    return {
        "ok": True,
        "recent_history": recent_history_widget(state, limit=history_limit, tracker_items=history_items),
        "recent_scrobble": recent_scrobble_widget(limit=scrobble_limit),
        "latest_ratings": latest_ratings_widget(state, limit=ratings_limit, tracker_items=ratings_items),
    }
