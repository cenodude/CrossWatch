# /cw_platform/metadata_cache.py
# CrossWatch - Shared persistent metadata cache
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import unicodedata
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

_LOCALE_RE = re.compile(r"[a-zA-Z0-9]{1,8}(?:-[a-zA-Z0-9]{1,8})*")
_WRITE_LOCK = RLock()
_INDEX_LOCK = RLock()
_INDEX_MEMO: dict[str, tuple[tuple[int, int], dict[str, Any]]] = {}

RESOLUTION_DIRNAME = "resolution"
RESOLUTION_INDEX_NAME = "_index.json"
UNRESOLVED_TTL_SECONDS = 7 * 24 * 3600

_APOSTROPHES = dict.fromkeys(map(ord, "’ʼ‘`´"), "'")
_DASHES = dict.fromkeys(map(ord, "‐‑‒–—―−"), "-")
_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
_SPACE_RE = re.compile(r"\s+")


def normalize_title(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.translate(_APOSTROPHES).translate(_DASHES)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = _PUNCT_RE.sub(" ", text)
    return _SPACE_RE.sub(" ", text).strip().casefold()


def _cache_tmdb_id(value: Any) -> str:
    text = str(value or "").strip()
    if "/" in text or "\\" in text or not text.isascii() or not text.isdecimal():
        raise ValueError("TMDb ID must be a positive integer")
    normalized = str(int(text))
    if normalized == "0":
        raise ValueError("TMDb ID must be a positive integer")
    return normalized


def _cache_locale(value: Any) -> str:
    text = str(value or "en-US").strip()
    if (
        "/" in text
        or "\\" in text
        or len(text) > 64
        or _LOCALE_RE.fullmatch(text) is None
    ):
        raise ValueError("Locale must be a valid language tag")
    return text


def metadata_cache_path(
    cache_root: Path | str,
    entity: str,
    tmdb_id: str | int,
    locale: str | None,
) -> Path:
    cache_id = _cache_tmdb_id(tmdb_id)
    cache_locale = _cache_locale(locale)
    root = os.path.realpath(os.fspath(cache_root))
    media = "movie" if str(entity or "").strip().lower() == "movie" else "show"
    media_root = os.path.realpath(os.path.join(root, media))
    root_prefix = root if root.endswith(os.sep) else root + os.sep
    if not media_root.startswith(root_prefix):
        raise ValueError("Metadata cache media path escaped its root")
    Path(media_root).mkdir(parents=True, exist_ok=True)
    name = f"{cache_id}.{cache_locale}.json"
    path = os.path.realpath(os.path.join(media_root, name))
    media_prefix = media_root if media_root.endswith(os.sep) else media_root + os.sep
    if not path.startswith(media_prefix):
        raise ValueError("Metadata cache path escaped its media root")
    return Path(path)


def read_metadata_cache(
    cache_root: Path | str,
    entity: str,
    tmdb_id: str | int,
    locale: str | None,
    *,
    ttl_seconds: int | None,
) -> dict[str, Any] | None:
    try:
        target = metadata_cache_path(cache_root, entity, tmdb_id, locale)
        data = json.loads(target.read_text("utf-8"))
        if not isinstance(data, dict):
            return None
        if ttl_seconds is not None:
            fetched_at = float(data.get("fetched_at") or 0.0)
            if fetched_at <= 0 or (time.time() - fetched_at) > max(1, int(ttl_seconds)):
                return None
        return data
    except Exception:
        return None


def merge_metadata_cache_payload(
    base: Mapping[str, Any] | None,
    extra: Mapping[str, Any],
) -> dict[str, Any]:
    previous = dict(base or {})
    incoming = dict(extra or {})
    out = {**previous, **incoming}
    for field in ("ids", "detail", "images"):
        old_raw = previous.get(field)
        new_raw = incoming.get(field)
        old_value: dict[str, Any] = dict(old_raw) if isinstance(old_raw, Mapping) else {}
        new_value: dict[str, Any] = dict(new_raw) if isinstance(new_raw, Mapping) else {}
        out[field] = {**old_value, **new_value}
    return out


def write_metadata_cache(
    cache_root: Path | str,
    entity: str,
    tmdb_id: str | int,
    locale: str | None,
    payload: Mapping[str, Any],
) -> bool:
    try:
        target = metadata_cache_path(cache_root, entity, tmdb_id, locale)
        data = dict(payload)
        data["fetched_at"] = time.time()
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        with _WRITE_LOCK:
            tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            tmp.replace(target)
        return True
    except Exception:
        return False


def _resolution_media(value: Any) -> str:
    return "movie" if str(value or "").strip().lower() == "movie" else "show"


def resolution_cache_key(
    tmdb_id: str | int,
    *,
    imdb_id: str | None = None,
    tvdb_id: str | int | None = None,
    title: str | None = None,
    year: int | str | None = None,
) -> str:
    cache_id = _cache_tmdb_id(tmdb_id)
    imdb = str(imdb_id or "").strip().lower()
    tvdb = str(tvdb_id or "").strip().lower()
    if imdb:
        evidence = f"tmdb:{cache_id}|imdb:{imdb}"
    elif tvdb:
        evidence = f"tmdb:{cache_id}|tvdb:{tvdb}"
    else:
        norm_title = normalize_title(title)
        year_text = str(year or "").strip()
        if norm_title:
            evidence = f"tmdb:{cache_id}|title:{norm_title}|year:{year_text}"
        else:
            evidence = f"tmdb:{cache_id}"
    digest = hashlib.sha256(evidence.encode("utf-8")).hexdigest()[:32]
    return f"{cache_id}_{digest}"


def _resolution_root(cache_root: Path | str, *, create: bool = True) -> Path:
    root = os.path.realpath(os.fspath(cache_root))
    target = os.path.realpath(os.path.join(root, RESOLUTION_DIRNAME))
    root_prefix = root if root.endswith(os.sep) else root + os.sep
    if not target.startswith(root_prefix):
        raise ValueError("Resolution cache path escaped its root")
    if create:
        Path(target).mkdir(parents=True, exist_ok=True)
    return Path(target)


def resolution_cache_path(cache_root: Path | str, key: str) -> Path:
    safe_key = str(key or "").strip()
    if not safe_key or not re.fullmatch(r"[A-Za-z0-9_]+", safe_key):
        raise ValueError("Resolution cache key must be alphanumeric")
    base = _resolution_root(cache_root)
    path = os.path.realpath(os.path.join(os.fspath(base), f"{safe_key}.json"))
    base_prefix = os.fspath(base)
    base_prefix = base_prefix if base_prefix.endswith(os.sep) else base_prefix + os.sep
    if not path.startswith(base_prefix):
        raise ValueError("Resolution cache path escaped its root")
    return Path(path)


def read_resolution_cache(
    cache_root: Path | str,
    key: str,
    *,
    unresolved_ttl_seconds: int | None = UNRESOLVED_TTL_SECONDS,
) -> dict[str, Any] | None:
    try:
        data = json.loads(resolution_cache_path(cache_root, key).read_text("utf-8"))
        if not isinstance(data, dict):
            return None
        if str(data.get("status") or "") == "resolved":
            return data
        if unresolved_ttl_seconds is None:
            return data
        resolved_at = float(data.get("resolved_at") or 0.0)
        if resolved_at <= 0 or (time.time() - resolved_at) > max(1, int(unresolved_ttl_seconds)):
            return None
        return data
    except Exception:
        return None


def write_resolution_cache(
    cache_root: Path | str,
    key: str,
    payload: Mapping[str, Any],
) -> bool:
    try:
        target = resolution_cache_path(cache_root, key)
        data = dict(payload)
        data.setdefault("resolved_at", time.time())
        tmp = target.with_suffix(target.suffix + ".tmp")
        with _WRITE_LOCK:
            tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            tmp.replace(target)
        return True
    except Exception:
        return False


def _resolution_index_path(cache_root: Path | str, *, create: bool = True) -> Path:
    return _resolution_root(cache_root, create=create) / RESOLUTION_INDEX_NAME


def _load_resolution_index(cache_root: Path | str) -> dict[str, Any]:
    path = _resolution_index_path(cache_root, create=False)
    memo_key = os.fspath(path)
    try:
        stat = path.stat()
        stamp = (stat.st_mtime_ns, stat.st_size)
    except Exception:
        with _INDEX_LOCK:
            _INDEX_MEMO.pop(memo_key, None)
        return {}

    with _INDEX_LOCK:
        hit = _INDEX_MEMO.get(memo_key)
        if hit is not None and hit[0] == stamp:
            return hit[1]

    try:
        data = json.loads(path.read_text("utf-8"))
    except Exception:
        data = {}
    if not isinstance(data, dict):
        data = {}

    with _INDEX_LOCK:
        _INDEX_MEMO[memo_key] = (stamp, data)
    return data


def read_resolution_index(
    cache_root: Path | str,
    requested_type: str,
    tmdb_id: str | int,
) -> str | None:
    try:
        cache_id = _cache_tmdb_id(tmdb_id)
        entry = _load_resolution_index(cache_root).get(f"{_resolution_media(requested_type)}:{cache_id}")
        if not isinstance(entry, dict) or entry.get("ambiguous"):
            return None
        resolved = str(entry.get("resolved_type") or "").strip().lower()
        return resolved if resolved in {"movie", "show"} else None
    except Exception:
        return None


def write_resolution_index(
    cache_root: Path | str,
    requested_type: str,
    tmdb_id: str | int,
    resolved_type: str,
) -> bool:
    try:
        cache_id = _cache_tmdb_id(tmdb_id)
        resolved = _resolution_media(resolved_type)
        index_key = f"{_resolution_media(requested_type)}:{cache_id}"
        target = _resolution_index_path(cache_root)
        with _WRITE_LOCK:
            try:
                data = json.loads(target.read_text("utf-8"))
            except Exception:
                data = {}
            if not isinstance(data, dict):
                data = {}

            entry = data.get(index_key)
            if isinstance(entry, dict):
                if entry.get("ambiguous"):
                    return True
                if str(entry.get("resolved_type") or "") == resolved:
                    return True
                data[index_key] = {"ambiguous": True}
            else:
                data[index_key] = {"resolved_type": resolved}

            tmp = target.with_suffix(target.suffix + ".tmp")
            tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            tmp.replace(target)
            with _INDEX_LOCK:
                _INDEX_MEMO.pop(os.fspath(target), None)
        return True
    except Exception:
        return False


def prune_metadata_cache(cache_root: Path | str, *, max_mb: int) -> int:
    if int(max_mb or 0) <= 0:
        return 0
    try:
        root = Path(cache_root).resolve()
        files = [
            path
            for path in root.rglob("*.json")
            if path.is_file()
            and not path.is_symlink()
            and RESOLUTION_DIRNAME not in path.relative_to(root).parts
        ]
        total = sum(path.stat().st_size for path in files)
        cap = int(max_mb) * 1024 * 1024
        if total <= cap:
            return 0
        files.sort(key=lambda path: path.stat().st_mtime)
        target = int(cap * 0.9)
        removed = 0
        for path in files:
            try:
                total -= path.stat().st_size
                path.unlink(missing_ok=True)
                removed += 1
            except Exception:
                continue
            if total <= target:
                break
        return removed
    except Exception:
        return 0
