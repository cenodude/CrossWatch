# cw_platform/path_matching.py
# CrossWatch - Scoped media path matching for local media servers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Iterable
from typing import Any

from .value_coercion import coerce_bool
from .id_map import canonical_key

PROVIDERS = frozenset({"PLEX", "JELLYFIN", "EMBY"})
EXTERNAL_IDS = ("imdb", "tmdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")


def pair_enabled(pair: Mapping[str, Any]) -> bool:
    return coerce_bool(pair.get("path_matching", False)) and all(
        str(pair.get(key) or "").upper() in PROVIDERS for key in ("source", "target")
    )


def media_paths(obj: Any) -> list[str]:
    def get(value: Any, key: str, default: Any = None) -> Any:
        return value.get(key, default) if isinstance(value, Mapping) else getattr(value, key, default)

    values: list[Any] = []
    for field in ("Path", "file", "path"):
        value = get(obj, field)
        if isinstance(value, str):
            values.append(value)
    paths = get(obj, "media_paths", [])
    if isinstance(paths, (list, tuple)):
        values.extend(paths)
    for source in get(obj, "MediaSources", []) or []:
        values.append(get(source, "Path"))
    for media in get(obj, "Media", None) or get(obj, "media", []) or []:
        for part in get(media, "Part", None) or get(media, "parts", []) or []:
            values.append(get(part, "file"))
    return sorted({v.strip() for v in values if isinstance(v, str) and v.strip()})


def path_keys(item: Mapping[str, Any]) -> set[str]:
    kind = str(item.get("type") or "").lower()
    if kind not in {"movie", "episode"}:
        return set()
    suffix = ""
    if kind == "episode":
        try:
            season, episode = int(item["season"]), int(item["episode"])
            if season < 0 or episode < 0:
                return set()
            suffix = f"/{season}/{episode}"
        except (KeyError, TypeError, ValueError):
            return set()
    part = item.get("part")
    if part is not None:
        if isinstance(part, bool) or not isinstance(part, int) or part < 1:
            return set()
        suffix += f"/part:{part}"
    count = 2 if kind == "movie" else 3
    keys = set()
    for path in media_paths(item):
        if "://" in path and not path.lower().startswith(("file://", "smb://", "nfs://")):
            continue
        segments = [p for p in re.split(r"[\\/]+", path.strip()) if p]
        if len(segments) < count or any(p in {".", ".."} or p.endswith(":") for p in segments[-count:]):
            continue
        value = kind + "/" + "/".join(segments[-count:]).casefold() + suffix
        keys.add("path:" + hashlib.sha256(value.encode("utf-8")).hexdigest())
    return keys


def ids_conflict(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    for field in ("ids", "show_ids"):
        a, b = left.get(field) or {}, right.get(field) or {}
        for key in EXTERNAL_IDS:
            av, bv = str(a.get(key) or "").strip().lower(), str(b.get(key) or "").strip().lower()
            if av and bv and av != bv:
                return True
    return False


class PathIndex:
    def __init__(self, rows: Iterable[tuple[str, Mapping[str, Any]]]):
        self.items: dict[str, Mapping[str, Any]] = {}
        self.keys: dict[str, set[str]] = {}
        for native_id, item in rows:
            self.items[native_id] = item
            for key in path_keys(item):
                self.keys.setdefault(key, set()).add(native_id)

    def resolve(self, item: Mapping[str, Any]) -> tuple[str | None, str]:
        hits: set[str] = set()
        for key in path_keys(item):
            hits.update(self.keys.get(key, ()))
        if not hits:
            return None, "path_not_found"
        if len(hits) != 1:
            return None, "path_ambiguous"
        native_id = next(iter(hits))
        if ids_conflict(item, self.items[native_id]):
            return None, "path_id_conflict"
        return native_id, "path_match"


def rekey_by_path(index: Mapping[str, Any], reference: Mapping[str, Any]) -> dict[str, Any]:
    source = PathIndex((str(k), v) for k, v in reference.items() if isinstance(v, Mapping))
    own = PathIndex((str(k), v) for k, v in index.items() if isinstance(v, Mapping))
    out = dict(index)
    for key, item in index.items():
        if key in reference or not isinstance(item, Mapping):
            continue
        target, _ = source.resolve(item)
        unique, _ = own.resolve(source.items[target]) if target else (None, "path_not_found")
        if target and unique == key and target not in out:
            out[target] = out.pop(key)
    return out


class PathAliases:
    def __init__(self, indices: Iterable[Mapping[str, Any]], enabled: bool):
        self.allowed: set[str] = set()
        snapshots = list(indices)
        if not enabled or len(snapshots) != 2:
            return
        left, right = snapshots
        a = PathIndex((canonical_key(v) + "|" + ",".join(sorted(path_keys(v))), v) for v in left.values() if isinstance(v, Mapping))
        b = PathIndex((canonical_key(v) + "|" + ",".join(sorted(path_keys(v))), v) for v in right.values() if isinstance(v, Mapping))
        for key, item in a.items.items():
            target, _ = b.resolve(item)
            if target is None:
                continue
            other = b.items[target]
            reverse, _ = a.resolve(other)
            if reverse == key:
                self.allowed.update(path_keys(item) & path_keys(other))

    def tokens(self, item: Mapping[str, Any]) -> set[str]:
        return path_keys(item) & self.allowed
