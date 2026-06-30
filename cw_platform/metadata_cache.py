# /cw_platform/metadata_cache.py
# CrossWatch - Shared persistent metadata cache
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from threading import RLock
from typing import Any, Mapping

_SAFE_PART_RE = re.compile(r"[^a-zA-Z0-9._-]+")
_WRITE_LOCK = RLock()


def _safe_part(value: Any, *, default: str) -> str:
    text = _SAFE_PART_RE.sub("_", str(value or "").strip()).strip("._-")
    return text or default


def metadata_cache_path(
    cache_root: Path | str,
    entity: str,
    tmdb_id: str | int,
    locale: str | None,
) -> Path:
    root = Path(cache_root).resolve()
    media = "movie" if str(entity or "").strip().lower() == "movie" else "show"
    media_root = (root / media).resolve()
    media_root.relative_to(root)
    media_root.mkdir(parents=True, exist_ok=True)
    name = f"{_safe_part(tmdb_id, default='x')}.{_safe_part(locale or 'en-US', default='en-US')}.json"
    path = (media_root / name).resolve()
    path.relative_to(media_root)
    return path


def read_metadata_cache(path: Path | str, *, ttl_seconds: int | None) -> dict[str, Any] | None:
    try:
        data = json.loads(Path(path).read_text("utf-8"))
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


def write_metadata_cache(path: Path | str, payload: Mapping[str, Any]) -> bool:
    target = Path(path)
    try:
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
