# /cw_platform/anime_mapping/overrides.py
# CrossWatch - Anime Mapping User Overrides
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import re
import tempfile
import threading
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cw_platform.config_base import CONFIG_BASE

MATCH_PROVIDERS = ("tvdb", "tmdb", "imdb", "anidb", "mal", "anilist", "simkl", "kitsu")
TARGET_NAMESPACES = ("anidb", "mal", "anilist", "simkl", "kitsu")
MEDIA_TYPES = ("show", "movie")
_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
_LOCK = threading.Lock()
_MAX_OVERRIDES = 5000
_CACHE: dict[str, Any] = {"key": "", "rows": []}


class OverrideError(ValueError):
    pass


GENERIC_OVERRIDE_ERROR = "Invalid rule"

_SAFE_OVERRIDE_ERRORS: dict[str, str] = {
    text: text
    for text in (
        "media_type must be 'show' or 'movie'",
        f"match_provider must be one of: {', '.join(MATCH_PROVIDERS)}",
        "match_id is required",
        f"target_namespace must be one of: {', '.join(TARGET_NAMESPACES)}",
        "target_id is required",
        "The rule maps an ID to itself",
        "match_season cannot be negative",
        "Movie rules cannot define seasons or episode ranges",
        "An episode rule needs both a first episode and a maps-to number",
        "Episode numbers must be 1 or higher",
        "The last episode cannot be lower than the first",
        "An episode rule needs a season",
        f"At most {_MAX_OVERRIDES} rules are supported",
        "mode must be 'merge' or 'replace'",
        "The file does not contain a list of rules",
        "The file contains no valid rules",
    )
}

IMPORT_MODES = ("merge", "replace")


def safe_override_error(exc: Exception) -> str:
    return _SAFE_OVERRIDE_ERRORS.get(str(exc), GENERIC_OVERRIDE_ERROR)


@dataclass(frozen=True)
class EpisodeOverride:
    absolute: int
    namespace: str
    target_id: str
    rule_id: str


@dataclass(frozen=True)
class SourceOverride:
    provider: str
    ident: str
    season: int
    episode: int
    rule_id: str


def overrides_path() -> Path:
    return (CONFIG_BASE() / ".cw_state" / "anime_mapping_overrides.json").resolve()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(dict(payload or {}), f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass


def _as_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _text(value: Any, limit: int = 200) -> str:
    return str(value or "").strip()[:limit]


def _clean_id(value: Any) -> str:
    text = str(value or "").strip()
    return text if _ID_RE.fullmatch(text) else ""


def normalize_override(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    data = dict(raw or {})

    media_type = _text(data.get("media_type") or "show").lower()
    if media_type not in MEDIA_TYPES:
        raise OverrideError("media_type must be 'show' or 'movie'")

    match_provider = _text(data.get("match_provider")).lower()
    if match_provider not in MATCH_PROVIDERS:
        raise OverrideError(f"match_provider must be one of: {', '.join(MATCH_PROVIDERS)}")

    match_id = _clean_id(data.get("match_id"))
    if not match_id:
        raise OverrideError("match_id is required")

    target_namespace = _text(data.get("target_namespace")).lower()
    if target_namespace not in TARGET_NAMESPACES:
        raise OverrideError(f"target_namespace must be one of: {', '.join(TARGET_NAMESPACES)}")

    target_id = _clean_id(data.get("target_id"))
    if not target_id:
        raise OverrideError("target_id is required")

    if match_provider == target_namespace and match_id == target_id:
        raise OverrideError("The rule maps an ID to itself")

    season = _as_int(data.get("match_season")) if data.get("match_season") not in (None, "") else None
    if season is not None and season < 0:
        raise OverrideError("match_season cannot be negative")

    ep_from = _as_int(data.get("episode_from")) if data.get("episode_from") not in (None, "") else None
    ep_to = _as_int(data.get("episode_to")) if data.get("episode_to") not in (None, "") else None
    ep_start = _as_int(data.get("episode_start_at")) if data.get("episode_start_at") not in (None, "") else None
    has_episode_rule = any(v is not None for v in (ep_from, ep_to, ep_start))

    if media_type == "movie":
        if has_episode_rule or season is not None:
            raise OverrideError("Movie rules cannot define seasons or episode ranges")
    elif has_episode_rule:
        if ep_from is None or ep_start is None:
            raise OverrideError("An episode rule needs both a first episode and a maps-to number")
        if ep_from <= 0 or ep_start <= 0:
            raise OverrideError("Episode numbers must be 1 or higher")
        if ep_to is not None and ep_to < ep_from:
            raise OverrideError("The last episode cannot be lower than the first")
        if season is None:
            raise OverrideError("An episode rule needs a season")

    rule_id = _text(data.get("id"), 64) or f"ovr_{uuid.uuid4().hex[:12]}"
    now = int(time.time())
    return {
        "id": rule_id,
        "enabled": bool(data.get("enabled", True)),
        "media_type": media_type,
        "title": _text(data.get("title")),
        "note": _text(data.get("note"), 500),
        "match_provider": match_provider,
        "match_id": match_id,
        "match_season": season,
        "target_namespace": target_namespace,
        "target_id": target_id,
        "episode_from": ep_from,
        "episode_to": ep_to,
        "episode_start_at": ep_start,
        "created_at": _as_int(data.get("created_at")) or now,
        "updated_at": now,
    }


def _cache_key(path: Path) -> str:
    try:
        st = path.stat()
    except OSError:
        return "absent"
    return f"{path}|{st.st_mtime_ns}|{st.st_size}"


def invalidate_cache() -> None:
    _CACHE["key"] = ""
    _CACHE["rows"] = []


def load_overrides() -> list[dict[str, Any]]:
    path = overrides_path()
    key = _cache_key(path)
    if key == _CACHE["key"]:
        return list(_CACHE["rows"])
    rows = _parse_overrides(path)
    _CACHE["key"] = key
    _CACHE["rows"] = rows
    return list(rows)


def _parse_overrides(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    data = _read_json(path)
    rows = data.get("overrides") if isinstance(data, Mapping) else None
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        try:
            clean = normalize_override(row)
        except OverrideError:
            continue
        clean["updated_at"] = _as_int(row.get("updated_at")) or clean["updated_at"]
        out.append(clean)
    return out


def save_overrides(rows: Sequence[Mapping[str, Any]]) -> None:
    path = overrides_path()
    _write_json_atomic(path, {"overrides": [dict(r) for r in rows], "updated_at": int(time.time())})
    invalidate_cache()


def upsert_override(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    clean = normalize_override(raw)
    with _LOCK:
        rows = load_overrides()
        for i, row in enumerate(rows):
            if row.get("id") == clean["id"]:
                clean["created_at"] = row.get("created_at") or clean["created_at"]
                rows[i] = clean
                save_overrides(rows)
                return clean
        if len(rows) >= _MAX_OVERRIDES:
            raise OverrideError(f"At most {_MAX_OVERRIDES} rules are supported")
        rows.append(clean)
        save_overrides(rows)
    return clean


def delete_override(rule_id: Any) -> bool:
    wanted = _text(rule_id, 64)
    if not wanted:
        return False
    with _LOCK:
        rows = load_overrides()
        kept = [r for r in rows if r.get("id") != wanted]
        if len(kept) == len(rows):
            return False
        save_overrides(kept)
    return True


def export_payload() -> dict[str, Any]:
    return {"overrides": load_overrides(), "updated_at": int(time.time())}


def _rows_from_payload(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, Mapping):
        rows = raw.get("overrides")
        if isinstance(rows, list):
            return list(rows)
    raise OverrideError("The file does not contain a list of rules")


def import_overrides(raw: Any, *, mode: str = "merge") -> dict[str, Any]:
    wanted = str(mode or "merge").strip().lower()
    if wanted not in IMPORT_MODES:
        raise OverrideError("mode must be 'merge' or 'replace'")

    incoming = _rows_from_payload(raw)
    if len(incoming) > _MAX_OVERRIDES:
        raise OverrideError(f"At most {_MAX_OVERRIDES} rules are supported")

    parsed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for index, row in enumerate(incoming):
        if not isinstance(row, Mapping):
            skipped.append({"index": index, "title": "", "reason": GENERIC_OVERRIDE_ERROR})
            continue
        try:
            clean = normalize_override(row)
        except OverrideError as e:
            skipped.append({"index": index, "title": _text(row.get("title"), 64), "reason": safe_override_error(e)})
            continue
        clean["updated_at"] = _as_int(row.get("updated_at")) or clean["updated_at"]
        parsed.append(clean)

    if incoming and not parsed:
        raise OverrideError("The file contains no valid rules")

    added = 0
    updated = 0
    with _LOCK:
        current = load_overrides() if wanted == "merge" else []
        by_id: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for row in current:
            rid = str(row.get("id") or "")
            by_id[rid] = dict(row)
            order.append(rid)
        for clean in parsed:
            rid = str(clean.get("id") or "")
            previous = by_id.get(rid)
            if previous is None:
                order.append(rid)
                added += 1
            else:
                clean["created_at"] = previous.get("created_at") or clean["created_at"]
                updated += 1
            by_id[rid] = clean
        if len(order) > _MAX_OVERRIDES:
            raise OverrideError(f"At most {_MAX_OVERRIDES} rules are supported")
        save_overrides([by_id[rid] for rid in order])

    return {
        "mode": wanted,
        "received": len(incoming),
        "imported": len(parsed),
        "added": added,
        "updated": updated,
        "skipped": skipped[:50],
        "skipped_count": len(skipped),
        "total": len(order),
    }


def _ids_of(item: Mapping[str, Any]) -> dict[str, str]:
    src = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else item.get("ids")
    out: dict[str, str] = {}
    for k, v in (src if isinstance(src, Mapping) else {}).items():
        text = str(v or "").strip()
        if text:
            out[str(k).strip().lower()] = text
    return out


def _matches_ids(row: Mapping[str, Any], ids: Mapping[str, str]) -> bool:
    provider = str(row.get("match_provider") or "")
    value = str(row.get("match_id") or "")
    return bool(value) and str(ids.get(provider) or "") == value


def find_episode_override(
    ids: Mapping[str, str],
    season: Any,
    episode: Any,
    *,
    rows: list[dict[str, Any]] | None = None,
) -> EpisodeOverride | None:
    s_num = _as_int(season)
    e_num = _as_int(episode)
    if s_num is None or e_num is None or e_num <= 0:
        return None
    for row in rows if rows is not None else load_overrides():
        if not row.get("enabled", True) or row.get("media_type") != "show":
            continue
        if not _matches_ids(row, ids):
            continue
        if row.get("match_season") is not None and int(row["match_season"]) != s_num:
            continue
        ep_from = row.get("episode_from")
        ep_start = row.get("episode_start_at")
        if ep_from is None or ep_start is None:
            continue
        ep_to = row.get("episode_to")
        if e_num < int(ep_from) or (ep_to is not None and e_num > int(ep_to)):
            continue
        return EpisodeOverride(
            absolute=int(ep_start) + (e_num - int(ep_from)),
            namespace=str(row.get("target_namespace") or ""),
            target_id=str(row.get("target_id") or ""),
            rule_id=str(row.get("id") or ""),
        )
    return None


def find_source_override(
    namespace: Any,
    target_id: Any,
    absolute: Any,
    *,
    rows: list[dict[str, Any]] | None = None,
) -> SourceOverride | None:
    ns = str(namespace or "").strip().lower()
    tid = str(target_id or "").strip()
    abs_num = _as_int(absolute)
    if not ns or not tid or abs_num is None or abs_num <= 0:
        return None

    found: SourceOverride | None = None
    for row in rows if rows is not None else load_overrides():
        if not row.get("enabled", True) or row.get("media_type") != "show":
            continue
        if str(row.get("target_namespace") or "") != ns or str(row.get("target_id") or "") != tid:
            continue
        ep_from = row.get("episode_from")
        ep_start = row.get("episode_start_at")
        season = row.get("match_season")
        if ep_from is None or ep_start is None or season is None:
            continue
        offset = abs_num - int(ep_start)
        if offset < 0:
            continue
        episode = int(ep_from) + offset
        ep_to = row.get("episode_to")
        if ep_to is not None and episode > int(ep_to):
            continue
        candidate = SourceOverride(
            provider=str(row.get("match_provider") or ""),
            ident=str(row.get("match_id") or ""),
            season=int(season),
            episode=episode,
            rule_id=str(row.get("id") or ""),
        )
        if not candidate.provider or not candidate.ident:
            continue
        if found is None:
            found = candidate
            continue
        if (found.provider, found.ident, found.season, found.episode) != (
            candidate.provider,
            candidate.ident,
            candidate.season,
            candidate.episode,
        ):
            return None
    return found


def find_identity_overrides(
    ids: Mapping[str, str],
    *,
    media_type: str | None = None,
    rows: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    wanted = str(media_type or "").strip().lower()
    out: dict[str, str] = {}
    for row in rows if rows is not None else load_overrides():
        if not row.get("enabled", True):
            continue
        if wanted in MEDIA_TYPES and row.get("media_type") != wanted:
            continue
        if not _matches_ids(row, ids):
            continue
        namespace = str(row.get("target_namespace") or "")
        if namespace and namespace not in out:
            out[namespace] = str(row.get("target_id") or "")
    return out


def override_for_item(item: Mapping[str, Any]) -> EpisodeOverride | None:
    ids = _ids_of(item)
    if not ids:
        return None
    season = item.get("season") if item.get("season") is not None else item.get("season_number")
    episode = item.get("episode") if item.get("episode") is not None else item.get("episode_number")
    return find_episode_override(ids, season, episode)


def stats() -> dict[str, Any]:
    rows = load_overrides()
    enabled = [r for r in rows if r.get("enabled", True)]
    return {
        "total": len(rows),
        "enabled": len(enabled),
        "shows": len([r for r in enabled if r.get("media_type") == "show"]),
        "movies": len([r for r in enabled if r.get("media_type") == "movie"]),
        "episode_rules": len([r for r in enabled if r.get("episode_start_at") is not None]),
        "path": str(overrides_path()),
    }
