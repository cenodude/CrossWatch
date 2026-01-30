# /providers/sync/tmdb/_common.py
# TMDb shared helpers (watchlist + ratings)
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from cw_platform.id_map import canonical_key, ids_from, minimal as id_minimal

STATE_DIR = Path("/config/.cw_state")
STATE_DIR.mkdir(parents=True, exist_ok=True)


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None


def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def state_file(name: str) -> Path:
    scope = _pair_scope()
    safe = _safe_scope(scope) if scope else "unscoped"
    p = Path(name)
    if p.suffix:
        return STATE_DIR / f"{p.stem}.{safe}{p.suffix}"
    return STATE_DIR / f"{name}.{safe}"


def _legacy_path(path: Path) -> Path | None:
    parts = path.stem.split(".")
    if len(parts) < 2:
        return None
    legacy_name = ".".join(parts[:-1]) + path.suffix
    legacy = path.with_name(legacy_name)
    return None if legacy == path else legacy


def _migrate_legacy_json(path: Path) -> None:
    if path.exists() or _pair_scope() is None:
        return
    legacy = _legacy_path(path)
    if not legacy or not legacy.exists():
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_bytes(legacy.read_bytes())
        os.replace(tmp, path)
    except Exception:
        pass


def read_json(path: Path) -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    _migrate_legacy_json(path)
    try:
        return json.loads(path.read_text("utf-8") or "{}")
    except Exception:
        return {}


def write_json(path: Path, data: Mapping[str, Any], *, indent: int | None = 2, sort_keys: bool = True) -> None:
    if _pair_scope() is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        if indent is None:
            tmp.write_text(
                json.dumps(dict(data), ensure_ascii=False, separators=(",", ":"), sort_keys=sort_keys),
                "utf-8",
            )
        else:
            tmp.write_text(
                json.dumps(dict(data), ensure_ascii=False, indent=indent, sort_keys=sort_keys),
                "utf-8",
            )
        os.replace(tmp, path)
    except Exception:
        pass


def now_epoch() -> int:
    return int(time.time())


def as_int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(float(s)) if ("." in s or "e" in s.lower()) else int(s)
    except Exception:
        return None


def _norm_kind(t: Any) -> str:
    s = str(t or "").strip().lower()
    if s in ("tv", "show", "shows", "series"):
        return "tv"
    if s in ("movie", "movies"):
        return "movie"
    if s in ("episode", "episodes"):
        return "episode"
    if s in ("season", "seasons"):
        return "season"
    return s or "movie"


def key_of(item: Mapping[str, Any]) -> str:
    return str(canonical_key(id_minimal(item)) or "").strip()


def year_from_date(s: Any) -> int | None:
    txt = str(s or "").strip()
    if len(txt) >= 4 and txt[:4].isdigit():
        try:
            return int(txt[:4])
        except Exception:
            return None
    return None


def iso_z_from_tmdb(s: Any) -> str | None:
    txt = str(s or "").strip()
    if not txt:
        return None
    if txt.endswith("Z") and "T" in txt:
        return txt
    if txt.endswith(" UTC"):
        try:
            dt = datetime.strptime(txt, "%Y-%m-%d %H:%M:%S UTC").replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return None
    try:
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def tmdb_id_from_item(item: Mapping[str, Any]) -> int | None:
    ids = ids_from(item)
    return as_int(ids.get("tmdb"))


def pick_media_type(item: Mapping[str, Any]) -> str:
    t = _norm_kind(item.get("type"))
    if t == "tv":
        return "tv"
    if t == "season":
        return "season"
    if t == "episode":
        return "episode"
    return "movie"


def pick_watchlist_media_type(item: Mapping[str, Any]) -> str:
    t = _norm_kind(item.get("type"))
    return "tv" if t in ("tv", "season", "episode") else "movie"


def unresolved_item(item: Mapping[str, Any], reason: str) -> dict[str, Any]:
    try:
        k = key_of(item)
    except Exception:
        k = ""
    return {"key": k, "reason": str(reason), "item": id_minimal(item)}


def touch_feature_state(adapter: Any, feature: str, **fields: Any) -> None:
    p = state_file(f"tmdb.{feature}.state.json")
    data = read_json(p)
    data.setdefault("provider", "TMDB")
    data.setdefault("feature", feature)
    data["updated_at"] = now_epoch()
    for k, v in fields.items():
        if v is None:
            continue
        data[k] = v
    write_json(p, data)


def _memo(adapter: Any, name: str) -> dict[str, Any]:
    memo = getattr(adapter, name, None)
    if not isinstance(memo, dict):
        memo = {}
        setattr(adapter, name, memo)
    return memo


def _disk_cache(adapter: Any, name: str, path: Path) -> dict[str, Any]:
    disk = _memo(adapter, name)
    loaded_name = f"{name}_loaded"
    if not bool(getattr(adapter, loaded_name, False)):
        disk.update(read_json(path))
        setattr(adapter, loaded_name, True)
    return disk


def _external_ids_cache_path(feature: str) -> Path:
    return state_file(f"tmdb.external_ids.{feature}.json")


def _find_cache_path() -> Path:
    return state_file("tmdb.find_cache.json")


def external_ids(adapter: Any, tmdb_id: int, *, media_type: str, feature: str) -> dict[str, Any]:
    mkey = f"{media_type}:{int(tmdb_id)}"
    memo = _memo(adapter, "_tmdb_external_ids_memo")
    if mkey in memo and isinstance(memo[mkey], dict):
        return dict(memo[mkey])

    cache_path = _external_ids_cache_path(feature)
    disk = _disk_cache(adapter, f"_tmdb_external_ids_disk_{feature}", cache_path)
    cached = disk.get(mkey)
    if isinstance(cached, Mapping):
        out = dict(cached)
        memo[mkey] = out
        return out

    client = getattr(adapter, "client", None)
    if not client:
        return {}
    try:
        r = client.get(
            f"{client.BASE}/{media_type}/{int(tmdb_id)}/external_ids",
            params=client._params(),
        )
    except Exception:
        return {}
    if not (200 <= r.status_code < 300):
        return {}
    try:
        data = r.json() if (r.text or "").strip() else {}
    except Exception:
        data = {}
    if not isinstance(data, Mapping):
        data = {}

    out: dict[str, Any] = {}
    imdb_id = data.get("imdb_id")
    if isinstance(imdb_id, str) and imdb_id.strip():
        out["imdb"] = imdb_id.strip()
    tvdb_id = as_int(data.get("tvdb_id"))
    if tvdb_id is not None:
        out["tvdb"] = str(tvdb_id)

    memo[mkey] = dict(out)
    disk[mkey] = dict(out)
    write_json(cache_path, disk, indent=2)
    return out


def enrich_ids_dict(adapter: Any, ids: dict[str, Any], *, media_type: str, tmdb_id: int, feature: str) -> None:
    ext = external_ids(adapter, tmdb_id, media_type=media_type, feature=feature)
    imdb = ext.get("imdb")
    if isinstance(imdb, str) and imdb.strip():
        ids["imdb"] = imdb.strip()
    tvdb = ext.get("tvdb")
    if isinstance(tvdb, str) and tvdb.strip():
        ids["tvdb"] = tvdb.strip()


def resolve_tmdb_id(adapter: Any, item: Mapping[str, Any], *, want: str) -> int | None:
    want = _norm_kind(want)
    kind = _norm_kind(item.get("type"))

    if want == "tv" and kind in ("season", "episode"):
        show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
        if show_ids:
            tid = tmdb_id_from_item(show_ids)
            if tid is not None:
                return tid

    tid = tmdb_id_from_item(item)
    if tid is not None:
        return tid

    ids_src: Mapping[str, Any] = item
    if want == "tv" and kind in ("season", "episode"):
        show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
        if show_ids:
            ids_src = show_ids

    ids = ids_from(ids_src)
    imdb = ids.get("imdb")
    tvdb = ids.get("tvdb")
    if not imdb and not tvdb:
        return None

    memo = _memo(adapter, "_tmdb_find_memo")
    cache_path = _find_cache_path()
    disk = _disk_cache(adapter, "_tmdb_find_disk", cache_path)

    if imdb:
        mkey = f"imdb:{imdb}|{want}"
        if mkey in memo:
            return memo[mkey]
        cached = disk.get(mkey)
        if isinstance(cached, int):
            memo[mkey] = cached
            return cached
        tid = _find_by_external(adapter, str(imdb), external_source="imdb_id", want=want)
        memo[mkey] = tid
        if tid is not None:
            disk[mkey] = int(tid)
            write_json(cache_path, disk)
        return tid

    if tvdb:
        mkey = f"tvdb:{tvdb}|{want}"
        if mkey in memo:
            return memo[mkey]
        cached = disk.get(mkey)
        if isinstance(cached, int):
            memo[mkey] = cached
            return cached
        tid = _find_by_external(adapter, str(tvdb), external_source="tvdb_id", want=want)
        memo[mkey] = tid
        if tid is not None:
            disk[mkey] = int(tid)
            write_json(cache_path, disk)
        return tid

    return None


def _find_by_external(adapter: Any, external_id: str, *, external_source: str, want: str) -> int | None:
    client = getattr(adapter, "client", None)
    if not client:
        return None

    url = f"{client.BASE}/find/{external_id}"
    try:
        r = client.get(url, params=client._params({"external_source": external_source}))
    except Exception:
        return None
    if not (200 <= r.status_code < 300):
        return None
    try:
        data = r.json() if (r.text or "").strip() else {}
    except Exception:
        data = {}
    if not isinstance(data, Mapping):
        return None

    want = _norm_kind(want)
    results = data.get("movie_results") if want == "movie" else data.get("tv_results")
    if not isinstance(results, list) or not results:
        return None
    first = results[0] if isinstance(results[0], Mapping) else None
    if not first:
        return None
    return as_int(first.get("id"))


__all__ = [
    "STATE_DIR",
    "state_file",
    "read_json",
    "write_json",
    "now_epoch",
    "as_int",
    "key_of",
    "year_from_date",
    "iso_z_from_tmdb",
    "tmdb_id_from_item",
    "pick_media_type",
    "pick_watchlist_media_type",
    "unresolved_item",
    "touch_feature_state",
    "external_ids",
    "enrich_ids_dict",
    "resolve_tmdb_id",
]