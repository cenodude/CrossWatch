# /providers/sync/trakt/_common.py
# TRAKT Module for common functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from cw_platform.id_map import minimal as id_minimal, canonical_key

from .._mod_common import request_with_retries

# headers
UA = os.environ.get("CW_UA", "CrossWatch/3.0 (Trakt)")

STATE_DIR = Path("/config/.cw_state")
_ACT_MEMO: tuple[float, dict[str, Any] | None] = (0.0, None)


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None




def _is_capture_mode() -> bool:
    v = str(os.getenv("CW_CAPTURE_MODE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


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
    if path.exists():
        return
    if _is_capture_mode() or _pair_scope() is None:
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


def _read_json(path: Path) -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    _migrate_legacy_json(path)
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, data: Mapping[str, Any]) -> None:
    if _pair_scope() is None:
        return
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception:
        pass


def _watermark_path() -> Path:
    return state_file("trakt.watermarks.json")


def load_watermarks() -> dict[str, str]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    raw = _read_json(_watermark_path())
    return {k: str(v) for k, v in (raw or {}).items() if isinstance(k, str) and isinstance(v, str) and v.strip()}


def save_watermark(feature: str, iso_ts: str) -> None:
    if _pair_scope() is None:
        return
    if (_pair_scope() or "").startswith("health:"):
        return
    data = load_watermarks()
    data[str(feature)] = str(iso_ts)
    _write_json(_watermark_path(), data)


def get_watermark(feature: str) -> str | None:
    if _pair_scope() is None:
        return None
    return load_watermarks().get(str(feature))


def update_watermark_if_new(feature: str, iso_ts: str | None) -> str | None:
    if not _iso_ok(iso_ts):
        return get_watermark(feature)
    current = get_watermark(feature)
    new = _max_iso(current, iso_ts)
    if new and new != current:
        save_watermark(feature, new)
    return new


def _iso_ok(value: object) -> bool:
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def _iso_z(value: str | None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("invalid ISO timestamp")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _max_iso(a: str | None, b: str | None) -> str | None:
    if not _iso_ok(a):
        return _iso_z(b) if _iso_ok(b) else None
    if not _iso_ok(b):
        return _iso_z(a)
    a_z = _iso_z(a)
    b_z = _iso_z(b)
    dt_a = datetime.fromisoformat(a_z.replace("Z", "+00:00"))
    dt_b = datetime.fromisoformat(b_z.replace("Z", "+00:00"))
    return _iso_z(a if dt_a >= dt_b else b)


def fetch_last_activities(
    session: Any,
    headers: Mapping[str, str],
    *,
    timeout: float = 15.0,
    max_retries: int = 3,
) -> dict[str, Any] | None:
    global _ACT_MEMO
    now = time.time()
    ts, cached = _ACT_MEMO
    if cached is not None and (now - ts) < 10.0:
        return cached
    url = "https://api.trakt.tv/sync/last_activities"
    try:
        r = request_with_retries(
            session,
            "GET",
            url,
            headers=dict(headers),
            timeout=timeout,
            max_retries=max_retries,
        )
        if 200 <= r.status_code < 300:
            data = r.json() if (r.text or "").strip() else {}
            _ACT_MEMO = (now, data)
            return data
        return None
    except Exception:
        return None


def extract_latest_ts(activities: Mapping[str, Any], paths: Iterable[Iterable[str]]) -> str | None:
    latest: str | None = None
    for path in paths or []:
        cur: Any = activities
        ok = True
        for key in path:
            if isinstance(cur, Mapping) and key in cur:
                cur = cur[key]
            else:
                ok = False
                break
        if ok and _iso_ok(cur):
            latest = _max_iso(latest, str(cur))
    return latest


def update_watermarks_from_last_activities(activities: Mapping[str, Any] | None) -> dict[str, str]:
    if not isinstance(activities, Mapping):
        return load_watermarks()
    hist = extract_latest_ts(activities, (("movies", "watched_at"), ("episodes", "watched_at")))
    rat = extract_latest_ts(
        activities,
        (("movies", "rated_at"), ("shows", "rated_at"), ("seasons", "rated_at"), ("episodes", "rated_at")),
    )
    wlu = extract_latest_ts(activities, (("watchlist", "updated_at"),))
    update_watermark_if_new("history", hist)
    update_watermark_if_new("ratings", rat)
    update_watermark_if_new("watchlist", wlu)
    return load_watermarks()


def build_headers(arg1: Any, access_token: str | None = None) -> dict[str, str]:
    client_id = ""
    token = ""
    if isinstance(arg1, Mapping) and access_token is None:
        t = arg1.get("trakt") or arg1
        client_id = str(t.get("client_id") or "").strip()
        token = str(t.get("access_token") or "").strip()
    else:
        client_id = str(arg1 or "").strip()
        token = str(access_token or "").strip()
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
        "User-Agent": UA,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


# ids / keys
_ALLOWED_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt")


def _fix_imdb(ids: dict[str, Any]) -> None:
    v = str(ids.get("imdb") or "").strip()
    if not v:
        return
    if not v.startswith("tt"):
        digits = "".join(ch for ch in v if ch.isdigit())
        if digits:
            ids["imdb"] = f"tt{digits}"


def normalize_watchlist_row(row: Mapping[str, Any]) -> dict[str, Any]:
    t = str(row.get("type") or "movie").lower()

    if t == "movie":
        payload = row.get("movie") or {}
        ids = dict(payload.get("ids") or {})
        _fix_imdb(ids)
        return id_minimal(
            {
                "type": "movie",
                "title": payload.get("title"),
                "year": payload.get("year"),
                "ids": {k: str(v) for k, v in ids.items() if v},
            }
        )

    if t == "show":
        payload = row.get("show") or {}
        ids = dict(payload.get("ids") or {})
        _fix_imdb(ids)
        return id_minimal(
            {
                "type": "show",
                "title": payload.get("title"),
                "year": payload.get("year"),
                "ids": {k: str(v) for k, v in ids.items() if v},
            }
        )

    if t == "season":
        season = row.get("season") or {}
        show = row.get("show") or {}
        ids = dict(season.get("ids") or {})
        show_ids = dict(show.get("ids") or {})
        _fix_imdb(ids)
        _fix_imdb(show_ids)
        number = season.get("number")
        m = {
            "type": "season",
            "title": show.get("title"),
            "series_title": show.get("title"),
            "year": show.get("year"),
            "season": number,
            "ids": {k: str(v) for k, v in ids.items() if v},
        }
        if show_ids:
            m["show_ids"] = {k: str(v) for k, v in show_ids.items() if v}
        return id_minimal(m)

    if t == "episode":
        episode = row.get("episode") or {}
        show = row.get("show") or {}
        ids = dict(episode.get("ids") or {})
        show_ids = dict(show.get("ids") or {})
        _fix_imdb(ids)
        _fix_imdb(show_ids)
        season_no = episode.get("season")
        episode_no = episode.get("number")
        m = {
            "type": "episode",
            "title": episode.get("title") or show.get("title"),
            "series_title": show.get("title"),
            "year": show.get("year"),
            "season": season_no,
            "episode": episode_no,
            "ids": {k: str(v) for k, v in ids.items() if v},
        }
        if show_ids:
            m["show_ids"] = {k: str(v) for k, v in show_ids.items() if v}
        return id_minimal(m)

    return id_minimal(row)


def normalize(obj: Mapping[str, Any]) -> dict[str, Any]:
    if "ids" in obj and "type" in obj:
        return id_minimal(obj)
    if "type" in obj and any(k in obj for k in ("movie", "show", "season", "episode")):
        return normalize_watchlist_row(obj)
    return id_minimal(obj)


def key_of(item: Mapping[str, Any]) -> str:
    return canonical_key(item)


def ids_for_trakt(item: Mapping[str, Any]) -> dict[str, Any]:
    ids = dict(item.get("ids") or {})
    _fix_imdb(ids)

    def _coerce(key: str, value: Any) -> Any:
        if value is None:
            return None
        if key in ("trakt", "tvdb", "tmdb"):
            # Trakt expects numeric IDs as integers in the sync endpoints.
            try:
                return int(value)
            except Exception:
                s = str(value).strip()
                return int(s) if s.isdigit() else None
        if key == "imdb":
            s = str(value).strip()
            return s or None
        # slug or any other string-ish id
        s = str(value).strip()
        return s or None

    t = str(item.get("type") or "").lower()
    # Episodes and seasons sometimes carry show IDs in the "ids" field.
    # Strip any ids that exactly match show_ids so callers can fall back to show-scoped
    # season/episode numbers.
    if t in ("episode", "season"):
        has_ep_scope = item.get("season") is not None and item.get("episode") is not None
        if t == "episode" and has_ep_scope and not item.get("show_ids"):
            return {}

        show_ids = dict(item.get("show_ids") or {})
        for key in list(ids.keys()):
            if key in show_ids and str(ids.get(key)) == str(show_ids.get(key)):
                ids.pop(key, None)

        out: dict[str, Any] = {}
        for key in ("trakt", "tvdb", "tmdb", "imdb"):
            v = _coerce(key, ids.get(key))
            if v is not None:
                out[key] = v
        return out

    out2: dict[str, Any] = {}
    for k in _ALLOWED_ID_KEYS:
        v = _coerce(k, ids.get(k))
        if v is not None:
            out2[k] = v
    return out2


def pick_trakt_kind(item: Mapping[str, Any]) -> str:
    t = str(item.get("type") or "movie").lower()
    if t == "episode":
        return "episodes"
    if t == "season":
        return "seasons"
    if t in ("show", "series", "tv"):
        return "shows"
    return "movies"


def build_watchlist_body(items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    body: dict[str, Any] = {}
    nested_shows: dict[str, dict[str, Any]] = {}

    def push(bucket: str, obj: dict[str, Any]) -> None:
        body.setdefault(bucket, []).append(obj)

    def show_scope_ids(it: Mapping[str, Any]) -> dict[str, Any]:
        return {
            k: str(v)
            for k, v in dict(it.get("show_ids") or {}).items()
            if k in ("trakt", "slug", "tmdb", "imdb", "tvdb") and v
        }

    def show_key(ids: Mapping[str, Any]) -> str:
        return json.dumps(
            {k: ids.get(k) for k in ("trakt", "slug", "tmdb", "imdb", "tvdb") if ids.get(k)},
            sort_keys=True,
        )

    for it in items or []:
        kind = pick_trakt_kind(it)
        ids = ids_for_trakt(it)
        if kind == "movies":
            if ids:
                push("movies", {"ids": ids})
            continue

        if kind == "shows":
            if ids:
                skey = show_key(ids)
                nested_shows.setdefault(skey, {"ids": ids, "seasons": {}})
            continue

        if kind == "seasons":
            season_no = it.get("season")
            if season_no is None:
                season_no = it.get("number")
            if ids:
                push("seasons", {"ids": ids})
                continue
            show_ids = show_scope_ids(it)
            if show_ids and season_no is not None:
                skey = show_key(show_ids)
                entry = nested_shows.setdefault(skey, {"ids": show_ids, "seasons": {}})
                try:
                    season_i = int(str(season_no))
                except Exception:
                    continue
                entry["seasons"].setdefault(season_i, {"number": season_i})
            continue

        if kind == "episodes":
            season_no = it.get("season")
            if season_no is None:
                season_no = it.get("season_number")
            episode_no = it.get("episode")
            if episode_no is None:
                episode_no = it.get("episode_number")

            show_ids = show_scope_ids(it)
            show_scope_ok = bool(show_ids and season_no is not None and episode_no is not None)
            strong_ids = bool(ids and ("trakt" in ids or "tvdb" in ids))
            use_ids = bool(ids) and (strong_ids or not show_scope_ok)

            # Prefer show-scoped season/episode when weak episode IDs.
            if use_ids:
                push("episodes", {"ids": ids})
                continue

            if show_scope_ok:
                skey = show_key(show_ids)
                entry = nested_shows.setdefault(skey, {"ids": show_ids, "seasons": {}})
                try:
                    season_i = int(str(season_no))
                    episode_i = int(str(episode_no))
                except Exception:
                    continue
                season_entry = entry["seasons"].setdefault(season_i, {"number": season_i, "episodes": []})
                season_entry.setdefault("episodes", []).append({"number": episode_i})
    if nested_shows:
        body.setdefault("shows", []).extend(
            ({"ids": v["ids"], "seasons": list(v["seasons"].values())} if v.get("seasons") else {"ids": v["ids"]})
            for v in nested_shows.values()
            if v.get("ids")
        )
    return body
