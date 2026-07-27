# providers/sync/kodi/_common.py
# CrossWatch Kodi sync helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
import time
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cw_platform.config_base import load_config, save_config
from cw_platform.id_map import canonical_key, ids_from, keys_for_item, minimal as id_minimal
from cw_platform.provider_instances import ensure_instance_block, get_provider_block, normalize_instance_id
from providers.auth._auth_KODI import KodiAuthError, jsonrpc_batch_call, jsonrpc_call, verify_connection
from providers.sync._log import log as cw_log

MOVIE_BASE_PROPERTIES = ["uniqueid", "title", "year"]
EPISODE_BASE_PROPERTIES = ["uniqueid", "title", "showtitle", "season", "episode", "tvshowid"]
MOVIE_PROPERTIES = [*MOVIE_BASE_PROPERTIES, "playcount", "lastplayed", "userrating", "resume"]
EPISODE_PROPERTIES = [*EPISODE_BASE_PROPERTIES, "playcount", "lastplayed", "userrating", "resume"]
SHOW_PROPERTIES = ["uniqueid", "title", "year"]
WHITELIST_FEATURES = ("history", "ratings", "progress", "scrobble")


def properties_for_feature(feature: str) -> tuple[list[str], list[str], list[str]]:
    name = str(feature or "").strip().lower()
    if name == "history":
        return (
            [*MOVIE_BASE_PROPERTIES, "playcount", "lastplayed"],
            [*EPISODE_BASE_PROPERTIES, "playcount", "lastplayed"],
            list(SHOW_PROPERTIES),
        )
    if name == "ratings":
        return (
            [*MOVIE_BASE_PROPERTIES, "userrating"],
            [*EPISODE_BASE_PROPERTIES, "userrating"],
            list(SHOW_PROPERTIES),
        )
    if name == "progress":
        return (
            [*MOVIE_BASE_PROPERTIES, "resume"],
            [*EPISODE_BASE_PROPERTIES, "resume"],
            list(SHOW_PROPERTIES),
        )
    return list(MOVIE_PROPERTIES), list(EPISODE_PROPERTIES), list(SHOW_PROPERTIES)


@dataclass(frozen=True)
class KodiConfig:
    server: str
    username: str = ""
    password: str = ""
    verify_ssl: bool = False
    timeout: float = 12.0
    connection_verified: bool = False


def log(feature: str, level: str, event: str, **fields: Any) -> None:
    cw_log("KODI", feature, level, event, **fields)


def pick_instance_id() -> str:
    for key in ("CW_SNAPSHOT_INSTANCE", "CW_INSTANCE_ID", "CW_PROVIDER_INSTANCE", "CW_INSTANCE"):
        value = str(os.getenv(key) or "").strip()
        if value:
            return normalize_instance_id(value)
    for side in ("SRC", "DST"):
        if str(os.getenv(f"CW_PAIR_{side}") or "").upper().strip() == "KODI":
            value = str(os.getenv(f"CW_PAIR_{side}_INSTANCE") or "").strip()
            if value:
                return normalize_instance_id(value)
    return "default"


def configured_block(cfg: Mapping[str, Any] | None, instance_id: Any = "default") -> dict[str, Any]:
    return get_provider_block(dict(cfg or {}), "kodi", normalize_instance_id(instance_id))


def is_configured(cfg: Mapping[str, Any] | None, instance_id: Any = "default") -> bool:
    block = configured_block(cfg, instance_id)
    return bool(str(block.get("server") or "").strip() and block.get("connection_verified") is True)


def make_config(cfg: Mapping[str, Any], instance_id: Any = "default") -> KodiConfig:
    block = configured_block(cfg, instance_id)
    return KodiConfig(
        server=str(block.get("server") or "").strip(),
        username=str(block.get("username") or "").strip(),
        password=str(block.get("password") or ""),
        verify_ssl=bool(block.get("verify_ssl", False)),
        timeout=float(block.get("timeout", 12.0) or 12.0),
        connection_verified=block.get("connection_verified") is True,
    )


def _as_list_str(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        raw = value
    elif value in (None, ""):
        raw = []
    else:
        raw = [value]
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        text = str(item or "").strip()
        if text and text not in seen:
            out.append(text)
            seen.add(text)
    return out


def _feature_block(cfg: Mapping[str, Any], feature: str, instance_id: Any = "default") -> Mapping[str, Any]:
    kodi = configured_block(cfg, instance_id)
    block = kodi.get(str(feature or "").strip().lower())
    return block if isinstance(block, Mapping) else {}


def selected_library_paths(cfg: Mapping[str, Any], feature: str, instance_id: Any = "default") -> list[str]:
    return _as_list_str(_feature_block(cfg, feature, instance_id).get("libraries"))


def blocked_library_paths(cfg: Mapping[str, Any], feature: str, instance_id: Any = "default") -> list[str]:
    block = _feature_block(cfg, feature, instance_id)
    values: list[str] = []
    for key in ("blocked_libraries", "excluded_libraries", "blacklist"):
        values.extend(_as_list_str(block.get(key)))
    return sorted(set(values))


def ensure_whitelist_defaults(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> None:
    cfg2 = cfg or load_config()
    kodi = ensure_instance_block(cfg2, "kodi", normalize_instance_id(instance_id))
    changed = False
    for feature in WHITELIST_FEATURES:
        if not isinstance(kodi.get(feature), dict):
            kodi[feature] = {}
            changed = True
        block = kodi[feature]
        if not isinstance(block.get("libraries"), list):
            block["libraries"] = _as_list_str(block.get("libraries"))
            changed = True
    if changed:
        save_config(cfg2)


def _normalize_path(value: Any) -> str:
    text = str(value or "").strip().replace("\\", "/")
    while text.endswith("/") and len(text) > 1:
        text = text[:-1]
    return text.lower()


def _path_matches(path: Any, roots: Iterable[Any]) -> bool:
    target = _normalize_path(path)
    if not target:
        return False
    for root in roots:
        base = _normalize_path(root)
        if not base:
            continue
        if target == base or target.startswith(base + "/"):
            return True
    return False


def path_allowed(cfg: Mapping[str, Any], feature: str, path: Any, instance_id: Any = "default") -> bool:
    allowed, _, _, _ = path_scope_status(cfg, feature, path, instance_id)
    return allowed


def path_scope_status(
    cfg: Mapping[str, Any],
    feature: str,
    path: Any,
    instance_id: Any = "default",
) -> tuple[bool, str, list[str], list[str]]:
    allowed = selected_library_paths(cfg, feature, instance_id)
    blocked = blocked_library_paths(cfg, feature, instance_id)
    if blocked and _path_matches(path, blocked):
        return False, "blocked_library_scope", allowed, blocked
    if allowed and not _path_matches(path, allowed):
        return False, "outside_library_scope", allowed, blocked
    return True, "", allowed, blocked


def path_filter_for(allowed: Iterable[Any] = (), blocked: Iterable[Any] = ()) -> Mapping[str, Any] | None:
    allow_rules = [{"field": "path", "operator": "startswith", "value": str(path)} for path in _as_list_str(list(allowed))]
    block_rules = [{"field": "path", "operator": "doesnotcontain", "value": str(path)} for path in _as_list_str(list(blocked))]
    rules: list[Mapping[str, Any]] = []
    if len(allow_rules) == 1:
        rules.append(allow_rules[0])
    elif allow_rules:
        rules.append({"or": allow_rules})
    rules.extend(block_rules)
    if not rules:
        return None
    return rules[0] if len(rules) == 1 else {"and": rules}


def to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).strip())
        return number if number == number else None
    except Exception:
        return None


def normalize_uniqueids(uniqueid: Any) -> dict[str, str]:
    raw = uniqueid if isinstance(uniqueid, Mapping) else {}
    out: dict[str, str] = {}
    for key, value in raw.items():
        text = str(value or "").strip()
        if not text:
            continue
        name = "".join(ch for ch in str(key or "").strip().lower() if ch.isalnum())
        if "imdb" in name or text.lower().startswith("tt"):
            out.setdefault("imdb", text)
        elif "tmdb" in name or "themoviedb" in name:
            out.setdefault("tmdb", text)
        elif "tvdb" in name or "thetvdb" in name:
            out.setdefault("tvdb", text)
    return out


def kodi_item_id(row: Mapping[str, Any], media_type: str) -> int | None:
    field = "movieid" if media_type == "movie" else "episodeid"
    return to_int(row.get(field) or row.get("id"))


def resume_ms(row: Mapping[str, Any]) -> tuple[int | None, int | None, float | None]:
    resume = row.get("resume")
    resume = resume if isinstance(resume, Mapping) else {}
    pos = to_float(resume.get("position"))
    total = to_float(resume.get("total"))
    pos_ms = int(round(pos * 1000)) if pos and pos > 0 else None
    total_ms = int(round(total * 1000)) if total and total > 0 else None
    pct = (max(0.0, min(100.0, (pos / total) * 100.0)) if pos and total and total > 0 else None)
    return pos_ms, total_ms, pct


def movie_item(row: Mapping[str, Any]) -> dict[str, Any] | None:
    kid = kodi_item_id(row, "movie")
    if kid is None:
        return None
    ids = normalize_uniqueids(row.get("uniqueid"))
    item: dict[str, Any] = {"type": "movie", "ids": ids, "title": row.get("title"), "year": to_int(row.get("year")), "_kodi_id": kid, "_kodi_type": "movie"}
    item["_kodi_keys"] = sorted(keys_for_item(item))
    return item


def episode_item(row: Mapping[str, Any], show_ids_by_id: Mapping[int, Mapping[str, str]] | None = None, show_year_by_id: Mapping[int, int] | None = None) -> dict[str, Any] | None:
    kid = kodi_item_id(row, "episode")
    if kid is None:
        return None
    tvshow_id = to_int(row.get("tvshowid")) or -1
    show_ids = dict((show_ids_by_id or {}).get(tvshow_id) or {})
    ep_ids = normalize_uniqueids(row.get("uniqueid"))
    item: dict[str, Any] = {
        "type": "episode",
        "ids": ep_ids,
        "show_ids": show_ids or ep_ids,
        "title": row.get("title") or row.get("showtitle"),
        "series_title": row.get("showtitle"),
        "year": to_int(row.get("year")) or (show_year_by_id or {}).get(tvshow_id),
        "season": to_int(row.get("season")),
        "episode": to_int(row.get("episode")),
        "_kodi_id": kid,
        "_kodi_type": "episode",
    }
    item["_kodi_keys"] = sorted(keys_for_item(item))
    return item


def item_key(item: Mapping[str, Any]) -> str:
    return canonical_key(id_minimal(item))


def _has_external_identifiers(item: Mapping[str, Any]) -> bool:
    ids = ids_from(item)
    sources: list[Mapping[str, Any]] = [ids]
    show_ids = item.get("show_ids")
    if isinstance(show_ids, Mapping):
        sources.append(show_ids)
    for src in sources:
        if any(str(src.get(k) or "").strip() for k in ("tmdb", "imdb", "tvdb")):
            return True
    return False


def resolution_keys(item: Mapping[str, Any]) -> set[str]:
    keys = {k for k in keys_for_item(item) | {canonical_key(id_minimal(item))} if k and k != "unknown:"}
    kodi_id = to_int(item.get("_kodi_id"))
    kodi_type = str(item.get("_kodi_type") or item.get("type") or "").strip().lower()
    if kodi_id is not None and kodi_type in {"movie", "episode"}:
        keys.add(f"kodi:{kodi_type}:{kodi_id}")
    if _has_external_identifiers(item):
        return {k for k in keys if "|title:" not in k}
    return {k for k in keys if "|title:" in k} or keys


def watched_at_to_kodi(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return text


def kodi_lastplayed_to_iso(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text or text.startswith("0000-00-00"):
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return text if "T" in text else None


def rating_for_write(item: Mapping[str, Any]) -> int | None:
    number = to_float(item.get("rating") or item.get("userrating"))
    if number is None or number <= 0:
        return None
    return max(1, min(10, int(round(number))))


def progress_ms_for_write(item: Mapping[str, Any], target_duration_ms: int | None = None) -> tuple[int | None, int | None]:
    pos = to_int(item.get("progress_ms") or item.get("progress") or item.get("viewOffset"))
    duration = to_int(item.get("duration_ms")) or target_duration_ms
    if pos is None:
        pct = to_float(item.get("progress_percent") or item.get("percent"))
        if pct is not None and duration and duration > 0:
            pos = int(round((pct / 100.0) * duration))
    if pos is None or pos < 0:
        return None, duration
    return pos, duration


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _progress_signature(item: Mapping[str, Any]) -> tuple[int | None, int | None, float | None]:
    pct = to_float(item.get("progress_percent") or item.get("percent"))
    return (
        to_int(item.get("progress_ms") or item.get("progress") or item.get("viewOffset")),
        to_int(item.get("duration_ms")),
        round(pct, 3) if pct is not None else None,
    )


def _load_kodi_progress_baseline(adapter: Any) -> Mapping[str, Mapping[str, Any]]:
    injected = getattr(adapter, "_kodi_progress_baseline", None)
    if isinstance(injected, Mapping):
        return {str(k): dict(v) for k, v in injected.items() if isinstance(v, Mapping)}
    try:
        from cw_platform.config_base import CONFIG_BASE

        path = Path(CONFIG_BASE()) / "state.json"
        if not path.exists():
            return {}
        state = json.loads(path.read_text("utf-8"))
        providers = state.get("providers") if isinstance(state, Mapping) else None
        providers = providers if isinstance(providers, Mapping) else {}
        kodi = providers.get("KODI") or providers.get("kodi")
        if not isinstance(kodi, Mapping):
            return {}
        instance_id = normalize_instance_id(getattr(adapter, "instance_id", "default"))
        nodes: list[Mapping[str, Any]] = []
        instances = kodi.get("instances")
        if isinstance(instances, Mapping):
            inst_node = instances.get(instance_id) or instances.get(str(instance_id))
            if isinstance(inst_node, Mapping):
                nodes.append(inst_node)
        nodes.append(kodi)
        for node in nodes:
            progress = node.get("progress")
            progress = progress if isinstance(progress, Mapping) else {}
            baseline = progress.get("baseline")
            baseline = baseline if isinstance(baseline, Mapping) else {}
            items = baseline.get("items")
            if isinstance(items, Mapping):
                return {str(k): dict(v) for k, v in items.items() if isinstance(v, Mapping)}
    except Exception:
        return {}
    return {}


def _managed_progress_metadata(prior: Mapping[str, Any] | None, item: Mapping[str, Any], now_iso: str) -> tuple[str, str]:
    if isinstance(prior, Mapping):
        previous = str(prior.get("progress_at") or prior.get("updated_at") or "").strip()
        if previous and _progress_signature(prior) == _progress_signature(item):
            source = str(prior.get("progress_at_source") or "kodi_first_observed").strip()
            return previous, source
        if previous:
            return now_iso, "kodi_resume_changed"
    return now_iso, "kodi_first_observed"


def fetch_libraries_from_cfg(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> list[dict[str, Any]]:
    cfg2 = cfg or load_config()
    ensure_whitelist_defaults(cfg2, instance_id=instance_id)
    kcfg = make_config(cfg2, normalize_instance_id(instance_id))
    if not (kcfg.server and kcfg.connection_verified):
        return []
    try:
        sources = KodiClient(kcfg).get_sources("video")
    except Exception:
        return []

    libs: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source in sources:
        label = str(source.get("label") or "").strip()
        paths = _as_list_str(source.get("paths"))
        file_path = str(source.get("file") or "").strip()
        if file_path:
            paths.insert(0, file_path)
        for path in paths:
            key = str(path).strip()
            if not key or key in seen:
                continue
            seen.add(key)
            title = label or key
            if label and len(paths) > 1:
                title = f"{label} - {key}"
            libs.append({"id": key, "key": key, "title": title, "name": title, "path": key, "type": "source", "sourceid": source.get("sourceid")})
    libs.sort(key=lambda row: str(row.get("title") or row.get("key") or "").lower())
    return libs


class KodiClient:
    def __init__(self, cfg: KodiConfig):
        if not cfg.server:
            raise RuntimeError("Kodi config requires server")
        self.cfg = cfg

    def rpc(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        return jsonrpc_call(
            self.cfg.server,
            method,
            params=params,
            username=self.cfg.username,
            password=self.cfg.password,
            verify_ssl=self.cfg.verify_ssl,
            timeout=self.cfg.timeout,
        )

    def verify(self) -> dict[str, Any]:
        return verify_connection(
            self.cfg.server,
            username=self.cfg.username,
            password=self.cfg.password,
            verify_ssl=self.cfg.verify_ssl,
            timeout=self.cfg.timeout,
        )

    @staticmethod
    def _library_params(properties: list[str], path_filter: Mapping[str, Any] | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {"properties": properties}
        if path_filter:
            params["filter"] = dict(path_filter)
        return params

    def get_sources(self, media: str = "video") -> list[Mapping[str, Any]]:
        result = self.rpc("Files.GetSources", {"media": media})
        rows = (result or {}).get("sources") if isinstance(result, Mapping) else None
        return [row for row in rows or [] if isinstance(row, Mapping)]

    def get_movies(self, properties: list[str] | None = None, path_filter: Mapping[str, Any] | None = None) -> list[Mapping[str, Any]]:
        result = self.rpc("VideoLibrary.GetMovies", self._library_params(properties or MOVIE_PROPERTIES, path_filter))
        rows = (result or {}).get("movies") if isinstance(result, Mapping) else None
        return [row for row in rows or [] if isinstance(row, Mapping)]

    def get_tvshows(self, properties: list[str] | None = None, path_filter: Mapping[str, Any] | None = None) -> list[Mapping[str, Any]]:
        result = self.rpc("VideoLibrary.GetTVShows", self._library_params(properties or SHOW_PROPERTIES, path_filter))
        rows = (result or {}).get("tvshows") if isinstance(result, Mapping) else None
        return [row for row in rows or [] if isinstance(row, Mapping)]

    def get_episodes(self, properties: list[str] | None = None, path_filter: Mapping[str, Any] | None = None) -> list[Mapping[str, Any]]:
        result = self.rpc("VideoLibrary.GetEpisodes", self._library_params(properties or EPISODE_PROPERTIES, path_filter))
        rows = (result or {}).get("episodes") if isinstance(result, Mapping) else None
        return [row for row in rows or [] if isinstance(row, Mapping)]

    def library_rows(
        self,
        feature: str = "",
        path_filter: Mapping[str, Any] | None = None,
    ) -> tuple[list[Mapping[str, Any]], list[Mapping[str, Any]], list[Mapping[str, Any]]]:
        movie_props, episode_props, show_props = properties_for_feature(feature)
        calls: list[tuple[str, Mapping[str, Any] | None]] = [
            ("VideoLibrary.GetMovies", self._library_params(movie_props, path_filter)),
            ("VideoLibrary.GetEpisodes", self._library_params(episode_props, path_filter)),
            ("VideoLibrary.GetTVShows", self._library_params(show_props, path_filter)),
        ]
        try:
            results = jsonrpc_batch_call(
                self.cfg.server,
                calls,
                username=self.cfg.username,
                password=self.cfg.password,
                verify_ssl=self.cfg.verify_ssl,
                timeout=self.cfg.timeout,
            )
            movies = results.get("0:VideoLibrary.GetMovies") or {}
            episodes = results.get("1:VideoLibrary.GetEpisodes") or {}
            tvshows = results.get("2:VideoLibrary.GetTVShows") or {}
            return (
                [row for row in (movies.get("movies") if isinstance(movies, Mapping) else []) or [] if isinstance(row, Mapping)],
                [row for row in (episodes.get("episodes") if isinstance(episodes, Mapping) else []) or [] if isinstance(row, Mapping)],
                [row for row in (tvshows.get("tvshows") if isinstance(tvshows, Mapping) else []) or [] if isinstance(row, Mapping)],
            )
        except KodiAuthError as exc:
            log(str(feature or "library"), "warning", "batch_read_failed", reason=exc.reason, error=str(exc))
        return (
            self.get_movies(movie_props, path_filter),
            self.get_episodes(episode_props, path_filter),
            self.get_tvshows(show_props, path_filter),
        )

    def set_movie(self, movieid: int, payload: Mapping[str, Any]) -> Any:
        params = {"movieid": int(movieid), **dict(payload)}
        return self.rpc("VideoLibrary.SetMovieDetails", params)

    def set_episode(self, episodeid: int, payload: Mapping[str, Any]) -> Any:
        params = {"episodeid": int(episodeid), **dict(payload)}
        return self.rpc("VideoLibrary.SetEpisodeDetails", params)


class LibraryIndex:
    def __init__(self, movies: Iterable[Mapping[str, Any]], episodes: Iterable[Mapping[str, Any]], tvshows: Iterable[Mapping[str, Any]]):
        self.items: list[dict[str, Any]] = []
        show_ids_by_id: dict[int, dict[str, str]] = {}
        show_year_by_id: dict[int, int] = {}
        for row in tvshows:
            show_id = to_int(row.get("tvshowid"))
            ids = normalize_uniqueids(row.get("uniqueid"))
            if show_id is not None and ids:
                show_ids_by_id[show_id] = ids
            year = to_int(row.get("year"))
            if show_id is not None and year:
                show_year_by_id[show_id] = year
        for row in movies:
            item = movie_item(row)
            if item:
                item["_kodi_raw"] = dict(row)
                self.items.append(item)
        for row in episodes:
            item = episode_item(row, show_ids_by_id, show_year_by_id)
            if item:
                item["_kodi_raw"] = dict(row)
                self.items.append(item)
        self.by_key: dict[str, list[dict[str, Any]]] = {}
        for item in self.items:
            kodi_id = to_int(item.get("_kodi_id"))
            kodi_type = str(item.get("_kodi_type") or item.get("type") or "").strip().lower()
            kodi_key = f"kodi:{kodi_type}:{kodi_id}" if kodi_id is not None and kodi_type in {"movie", "episode"} else ""
            for key in sorted(set(keys_for_item(item)) | {item_key(item), kodi_key}):
                if key and key != "unknown:":
                    self.by_key.setdefault(key, []).append(item)

    def resolve(self, item: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str]:
        candidates: list[dict[str, Any]] = []
        seen_ids: set[tuple[str, int]] = set()
        for key in sorted(resolution_keys(item)):
            for candidate in self.by_key.get(key, []):
                ident = (str(candidate.get("_kodi_type") or ""), int(candidate.get("_kodi_id") or 0))
                if ident not in seen_ids:
                    seen_ids.add(ident)
                    candidates.append(candidate)
        if len(candidates) == 1:
            return candidates[0], "ok"
        if not candidates:
            return None, "not_found"
        return None, "ambiguous"


def library_index(adapter: Any, feature: str = "") -> LibraryIndex:
    cache_name = f"_kodi_library_index_{str(feature or 'all').strip().lower() or 'all'}"
    cached = getattr(adapter, cache_name, None)
    if isinstance(cached, LibraryIndex):
        return cached
    client = getattr(adapter, "client", None)
    if client is None:
        raise RuntimeError("missing_kodi_client")
    movie_props, episode_props, show_props = properties_for_feature(feature)
    cfg = getattr(adapter, "config", None)
    cfg_map: Mapping[str, Any] = cfg if isinstance(cfg, Mapping) else {}
    instance_id = getattr(adapter, "instance_id", "default")
    selected_paths = selected_library_paths(cfg_map, feature, instance_id)
    blocked_paths = blocked_library_paths(cfg_map, feature, instance_id)
    pfilter = path_filter_for(selected_paths, blocked_paths)
    if callable(getattr(client, "library_rows", None)):
        try:
            movies, episodes, tvshows = client.library_rows(feature, path_filter=pfilter)
        except TypeError:
            movies, episodes, tvshows = client.library_rows(feature)
    else:
        try:
            movies = client.get_movies(movie_props, pfilter) if pfilter else client.get_movies(movie_props)
        except TypeError:
            movies = client.get_movies()
        try:
            episodes = client.get_episodes(episode_props, pfilter) if pfilter else client.get_episodes(episode_props)
        except TypeError:
            episodes = client.get_episodes()
        try:
            tvshows = client.get_tvshows(show_props, pfilter) if pfilter else client.get_tvshows(show_props)
        except TypeError:
            tvshows = client.get_tvshows()
    if selected_paths or blocked_paths:
        movie_count = len(movies or [])
        episode_count = len(episodes or [])
        tvshow_count = len(tvshows or [])
        log(
            str(feature or "library"),
            "debug",
            "index_fetch_counts",
            source="selected_libraries",
            count=movie_count + episode_count,
            libraries=len(selected_paths),
            blocked_libraries=len(blocked_paths),
            allowed_library_paths=selected_paths,
            blocked_library_paths=blocked_paths,
            movies=movie_count,
            episodes=episode_count,
            tvshows=tvshow_count,
        )
    idx = LibraryIndex(movies, episodes, tvshows)
    setattr(adapter, cache_name, idx)
    return idx


def feature_index(adapter: Any, feature: str) -> dict[str, dict[str, Any]]:
    idx = library_index(adapter, feature)
    out: dict[str, dict[str, Any]] = {}
    prior_progress = _load_kodi_progress_baseline(adapter) if feature == "progress" else {}
    now_iso = _utc_now_iso() if feature == "progress" else ""
    for base in idx.items:
        item = dict(base)
        raw_obj = item.get("_kodi_raw")
        raw: Mapping[str, Any] = raw_obj if isinstance(raw_obj, Mapping) else {}
        if feature == "history":
            playcount = to_int(raw.get("playcount")) or 0
            if playcount <= 0:
                continue
            item["watched"] = True
            item["watched_at"] = kodi_lastplayed_to_iso(raw.get("lastplayed"))
            item["playcount"] = playcount
        elif feature == "ratings":
            rating = rating_for_write(raw)
            if rating is None:
                continue
            item["rating"] = rating
        elif feature == "progress":
            progress_ms, duration_ms, pct = resume_ms(raw)
            if not progress_ms:
                continue
            item["progress_ms"] = progress_ms
            item["duration_ms"] = duration_ms
            if pct is not None:
                item["progress_percent"] = pct
        else:
            continue
        key = item_key(item)
        if feature == "progress":
            progress_at, progress_at_source = _managed_progress_metadata(prior_progress.get(key), item, now_iso)
            item["progress_at"] = progress_at
            item["progress_at_source"] = progress_at_source
        out[key] = item
    log(feature, "info", "index_done", count=len(out))
    return out


def apply_set_details(adapter: Any, target: Mapping[str, Any], payload: Mapping[str, Any]) -> None:
    client = getattr(adapter, "client", None)
    if client is None:
        raise RuntimeError("missing_kodi_client")
    media_type = str(target.get("_kodi_type") or target.get("type") or "").lower()
    kodi_id = int(target.get("_kodi_id") or 0)
    if media_type == "movie":
        client.set_movie(kodi_id, payload)
    elif media_type == "episode":
        client.set_episode(kodi_id, payload)
    else:
        raise RuntimeError("unsupported_media_type")


def operation_result(adapter: Any, feature: str, op: str, items: Iterable[Mapping[str, Any]], payload_for: Any, *, dry_run: bool = False) -> dict[str, Any]:
    from providers.sync._mod_common import build_op_result

    rows = list(items or [])
    if dry_run:
        return build_op_result(ok=True, count=len(rows), dry_run=True)
    idx = library_index(adapter, feature)
    applied = 0
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()

    for source in rows:
        key = item_key(source)
        target, reason = idx.resolve(source)
        if target is None:
            reasons[reason] += 1
            unresolved.append({"key": key, "item": dict(source), "reason": reason})
            results.append({"status": "unresolved", "key": key, "reason": reason})
            continue
        try:
            payload, skip_reason = payload_for(source, target)
            if skip_reason:
                reasons[skip_reason] += 1
                unresolved.append({"key": key, "item": dict(source), "reason": skip_reason})
                results.append({"status": "unresolved", "key": key, "reason": skip_reason})
                continue
            apply_set_details(adapter, target, payload)
            applied += 1
            confirmed.append(key)
            results.append({"status": "applied", "key": key, "remote_item_id": target.get("_kodi_id")})
        except Exception as exc:
            reasons["write_failed"] += 1
            unresolved.append({"key": key, "item": dict(source), "reason": "write_failed", "hint": str(exc)})
            results.append({"status": "failed", "key": key, "reason": "write_failed", "hint": str(exc)})

    log(feature, "info", "write_done", op=op, applied=applied, unresolved=len(unresolved), failed=reasons.get("write_failed", 0))
    return build_op_result(
        ok=not bool(reasons.get("write_failed")),
        count=applied,
        confirmed_keys=confirmed,
        unresolved_keys=[u["key"] for u in unresolved if u.get("key")],
        unresolved=unresolved,
        results=results,
        reason_counts=dict(reasons),
    )


def health_payload(adapter: Any) -> dict[str, Any]:
    start = time.perf_counter()
    try:
        detected = adapter.client.verify()
        version = str(detected.get("jsonrpc_version") or "")
        major = to_int(version.split(".", 1)[0]) if version else None
        details: dict[str, Any] = {"kodi_version": detected.get("kodi_version"), "jsonrpc_version": version}
        status = "ok"
        if major is not None and major > 13:
            status = "compat_warning"
            details["warning"] = "Kodi JSON-RPC major version is newer than the tested v13 API family."
        return {"ok": True, "status": status, "latency_ms": int((time.perf_counter() - start) * 1000), "details": details}
    except KodiAuthError as exc:
        return {"ok": False, "status": exc.reason, "latency_ms": int((time.perf_counter() - start) * 1000), "details": {"reason": exc.reason}}
    except Exception as exc:
        return {"ok": False, "status": "service_unavailable", "latency_ms": int((time.perf_counter() - start) * 1000), "details": {"reason": str(exc)}}
