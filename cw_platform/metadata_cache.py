# /cw_platform/metadata_cache.py
# CrossWatch - Shared persistent metadata cache
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

_LOCALE_RE = re.compile(r"[a-zA-Z0-9]{1,8}(?:-[a-zA-Z0-9]{1,8})*")
_WRITE_LOCK = RLock()


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


def prune_metadata_cache(cache_root: Path | str, *, max_mb: int) -> int:
    if int(max_mb or 0) <= 0:
        return 0
    try:
        root = Path(cache_root).resolve()
        files = [path for path in root.rglob("*.json") if path.is_file() and not path.is_symlink()]
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
