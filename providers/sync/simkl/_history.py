# SIMKL Module for history sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from itertools import chain
from typing import Any, Iterable, Mapping, cast

from cw_platform.id_map import minimal as id_minimal

from .._log import log as cw_log
from ._common import (
    build_headers,
    coalesce_date_from,
    fetch_activities,
    extract_latest_ts,
    get_watermark,
    normalize_flat_watermarks,
    key_of as simkl_key_of,
    normalize as simkl_normalize,
    update_watermark_if_new,
    state_file,
    _pair_scope,
    _is_capture_mode,
)

BASE = "https://api.simkl.com"
URL_ALL_ITEMS = f"{BASE}/sync/all-items"
URL_ADD = f"{BASE}/sync/history"
URL_REMOVE = f"{BASE}/sync/history/remove"
URL_TV_EPISODES = f"{BASE}/tv/episodes"
URL_ANIME_EPISODES = f"{BASE}/anime/episodes"


def _unresolved_path() -> str:
    return str(state_file("simkl_history.unresolved.json"))


def _shadow_path() -> str:
    return str(state_file("simkl.history.shadow.json"))


def _show_map_path() -> str:
    return str(state_file("simkl.show.map.json"))


ID_KEYS = ("simkl", "tmdb", "imdb", "tvdb", "trakt", "mal", "anilist", "kitsu", "anidb")

_EP_LOOKUP_MEMO: dict[str, dict[tuple[int, int], dict[str, Any]]] = {}

_ANIME_TVDB_MAP_MEMO: dict[str, str] | None = None
_ANIME_TVDB_MAP_TTL_SEC = 24 * 3600
_ANIME_TVDB_MAP_DATE_FROM = "1900-01-01T00:00:00Z"


def _anime_tvdb_map_path() -> str:
    return str(state_file("simkl.anime.tvdb_map.json"))


def _load_anime_tvdb_map() -> tuple[dict[str, str], int]:
    if _is_capture_mode():
        return {}, 0
    try:
        raw = json.loads(Path(_anime_tvdb_map_path()).read_text("utf-8"))
        mp = dict(raw.get("map") or {})
        updated = int(raw.get("updated_at") or 0)
        return {str(k): str(v) for k, v in mp.items() if k and v}, updated
    except Exception:
        return {}, 0


def _save_anime_tvdb_map(mp: Mapping[str, str]) -> None:
    if _is_capture_mode():
        return
    try:
        payload = {"updated_at": int(time.time()), "map": dict(mp)}
        Path(_anime_tvdb_map_path()).write_text(json.dumps(payload, indent=2, sort_keys=True), "utf-8")
    except Exception:
        pass


def _ensure_anime_tvdb_map(adapter: Any) -> dict[str, str]:
    global _ANIME_TVDB_MAP_MEMO
    if _ANIME_TVDB_MAP_MEMO is not None:
        return _ANIME_TVDB_MAP_MEMO

    mp, updated = _load_anime_tvdb_map()
    if mp and updated and (time.time() - updated) < _ANIME_TVDB_MAP_TTL_SEC:
        _ANIME_TVDB_MAP_MEMO = mp
        return mp

    try:
        rows = _fetch_kind(adapter.client.session, _headers(adapter, force_refresh=True), kind="anime", since_iso=_ANIME_TVDB_MAP_DATE_FROM, timeout=adapter.cfg.timeout)
    except Exception:
        _ANIME_TVDB_MAP_MEMO = mp
        return mp

    built: dict[str, str] = {}
    for row in rows or []:
        show = row.get("show") if isinstance(row, Mapping) else None
        show = show if isinstance(show, Mapping) else row
        ids = dict(show.get("ids") or {}) if isinstance(show, Mapping) else {}
        tvdb = str(ids.get("tvdb") or "").strip()
        if not tvdb:
            continue
        for k in ("tmdb", "imdb", "simkl"):
            v = str(ids.get(k) or "").strip()
            if v:
                built[f"{k}:{v}"] = tvdb
    if built:
        mp = built
        _save_anime_tvdb_map(mp)
    _ANIME_TVDB_MAP_MEMO = mp
    return mp


def _maybe_map_tvdb(adapter: Any, ids: Mapping[str, Any]) -> dict[str, str]:
    out = {k: str(v) for k, v in dict(ids).items() if v}
    if out.get("tvdb"):
        return out
    if not any(out.get(k) for k in ("tmdb", "imdb", "simkl")):
        return out
    mp = _ensure_anime_tvdb_map(adapter)
    for k in ("tmdb", "imdb", "simkl"):
        v = out.get(k)
        if not v:
            continue
        tvdb = mp.get(f"{k}:{v}")
        if tvdb:
            out["tvdb"] = tvdb
            break
    return out


def _dedupe_history_movies(out: dict[str, dict[str, Any]]) -> None:
    if not out:
        return

    bucket_ids: dict[str, dict[str, Any]] = {}
    by_tvdb: dict[str, list[str]] = {}
    by_tmdb: dict[str, list[str]] = {}

    for event_key, item in out.items():
        if not isinstance(item, Mapping):
            continue
        if str(item.get("type") or "").lower() != "movie":
            continue
        bucket_key = event_key.split("@", 1)[0]
        ids = dict(item.get("ids") or {})
        if not ids:
            continue
        if bucket_key in bucket_ids:
            continue
        bucket_ids[bucket_key] = ids
        tvdb = (str(ids.get("tvdb") or "")).strip()
        tmdb = (str(ids.get("tmdb") or "")).strip()
        if tvdb:
            by_tvdb.setdefault(tvdb, []).append(bucket_key)
        if tmdb:
            by_tmdb.setdefault(tmdb, []).append(bucket_key)

    if not bucket_ids:
        return

    drop_buckets: set[str] = set()

    def pick(groups: dict[str, list[str]]) -> None:
        for _gid, keys in groups.items():
            if len(keys) < 2:
                continue
            canonical: str | None = None

            for k in keys:
                ids = bucket_ids.get(k) or {}
                if ids.get("plex") or ids.get("guid"):
                    canonical = k
                    break
            if canonical is None:
                canonical = keys[0]
            for k in keys:
                if k != canonical:
                    drop_buckets.add(k)

    pick(by_tvdb)
    pick(by_tmdb)

    if not drop_buckets:
        return

    to_drop: list[str] = [
        ek for ek in list(out.keys())
        if ek.split("@", 1)[0] in drop_buckets
    ]
    for ek in to_drop:
        out.pop(ek, None)

    _log(f"deduped {len(drop_buckets)} movie buckets ({len(to_drop)} events)")

def _safe_int(value: Any) -> int:
    try:
        n = int(value)
        return n if n > 0 else 0
    except Exception:
        return 0

def _log(msg: str, *, level: str = "debug", **fields: Any) -> None:
    cw_log("SIMKL", "history", level, msg, **fields)


def _now_epoch() -> int:
    return int(time.time())


def _as_epoch(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if isinstance(value, str):
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except Exception:
            return None
    return None


def _as_iso(ts: int) -> str:
    return (
        datetime.fromtimestamp(int(ts), tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _headers(adapter: Any, *, force_refresh: bool = False) -> dict[str, str]:
    return build_headers(
        {"simkl": {"api_key": adapter.cfg.api_key, "access_token": adapter.cfg.access_token}},
        force_refresh=force_refresh,
    )


def _ids_of(obj: Mapping[str, Any]) -> dict[str, Any]:
    ids = dict(obj.get("ids") or {})
    return {k: ids[k] for k in ID_KEYS if ids.get(k)}


def _raw_show_ids(item: Mapping[str, Any]) -> dict[str, Any]:
    return dict(item.get("show_ids") or {})


def _thaw_key(item: Mapping[str, Any]) -> str:
    typ = str(item.get("type") or "").lower()
    return simkl_key_of(item) if typ == "episode" else simkl_key_of(id_minimal(item))


def _episode_lookup(
    session: Any,
    headers: Mapping[str, str],
    *,
    timeout: float,
    show_ids: Mapping[str, Any],
    kind: str,
) -> dict[tuple[int, int], dict[str, Any]]:
    ids = dict(show_ids or {})
    candidates = [
        str(ids.get("simkl") or "").strip(),
        str(ids.get("tvdb") or "").strip(),
        str(ids.get("tmdb") or "").strip(),
        str(ids.get("imdb") or "").strip(),
    ]
    candidates = [c for c in candidates if c]
    if not candidates:
        return {}

    base_url = URL_ANIME_EPISODES if str(kind).lower() == "anime" else URL_TV_EPISODES

    for cand in candidates:
        memo_key = f"{str(kind).lower()}:{cand}"
        if memo_key in _EP_LOOKUP_MEMO:
            return _EP_LOOKUP_MEMO[memo_key]

    def _as_title(v: Any) -> str | None:
        if isinstance(v, str):
            t = v.strip()
            return t or None
        if isinstance(v, Mapping):
            for k in ("en", "title", "name", "original", "en_title"):
                t = _as_title(v.get(k))
                if t:
                    return t
            for vv in v.values():
                t = _as_title(vv)
                if t:
                    return t
        if isinstance(v, list):
            for it in v:
                t = _as_title(it)
                if t:
                    return t
        return None

    def _pick_title(row: Mapping[str, Any]) -> str | None:
        for key in ("title", "name", "en_title", "episode_title", "episodeName", "episodeTitle", "episode_name"):
            t = _as_title(row.get(key))
            if t:
                return t
        ep = row.get("episode")
        if isinstance(ep, Mapping):
            for key in ("title", "name", "en_title", "episode_title"):
                t = _as_title(ep.get(key))
                if t:
                    return t
        for key in ("titles", "names"):
            t = _as_title(row.get(key))
            if t:
                return t
        return None

    def _extract_ids(row: Mapping[str, Any]) -> dict[str, str]:
        raw = row.get("ids")
        d = dict(raw) if isinstance(raw, Mapping) else {}
        if d.get("simkl_id") and not d.get("simkl"):
            d["simkl"] = d["simkl_id"]
        return {k: str(d[k]) for k in ID_KEYS if d.get(k)}

    client_id = str(headers.get("simkl-api-key") or "").strip()

    for cand in candidates:
        out: dict[tuple[int, int], dict[str, Any]] = {}
        url = f"{base_url}/{cand}"
        params: dict[str, str] = {"extended": "full"}
        if client_id:
            params["client_id"] = client_id
        try:
            resp = session.get(url, headers=dict(headers), params=params, timeout=timeout)
            if not resp.ok:
                _log(f"tv/episodes failed id={cand} status={resp.status_code}")
                _EP_LOOKUP_MEMO[memo_key] = {}
                continue
            body = resp.json() if (resp.text or "").strip() else []
        except Exception as exc:
            _log(f"tv/episodes error id={cand}: {exc}")
            _EP_LOOKUP_MEMO[memo_key] = {}
            continue

        if not isinstance(body, list):
            _log(f"tv/episodes non-list id={cand}")
            _EP_LOOKUP_MEMO[memo_key] = {}
            continue
        if not body:
            _log(f"tv/episodes empty id={cand}")
            _EP_LOOKUP_MEMO[memo_key] = {}
            continue

        for row in body:
            if not isinstance(row, Mapping):
                continue
            s_num = _safe_int(row.get("season") or row.get("season_number"))
            e_num = _safe_int(row.get("episode") or row.get("episode_number") or row.get("number"))
            if not s_num or not e_num:
                continue
            out[(s_num, e_num)] = {
                "title": _pick_title(row),
                "ids": _extract_ids(row),
            }

        if out:
            for k in candidates:
                _EP_LOOKUP_MEMO[f"{str(kind).lower()}:{k}"] = out
            _log(f"tv/episodes ok id={cand} episodes={len(out)}")
            return out

        _EP_LOOKUP_MEMO[memo_key] = {}

    return {}

def _show_ids_of_episode(item: Mapping[str, Any]) -> dict[str, Any]:
    show_ids = _raw_show_ids(item)
    return {k: show_ids[k] for k in ID_KEYS if show_ids.get(k)}


def _scope_ids_for_freeze(item: Mapping[str, Any]) -> dict[str, Any]:
    typ = str(item.get("type") or "").lower()
    if typ in ("season", "episode"):
        scoped = _show_ids_of_episode(item)
        if scoped:
            return scoped
    return _ids_of(item)


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


def _load_json(path: str) -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    p = Path(path)
    _migrate_legacy_json(p)
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return {}


def _save_json(path: str, data: Mapping[str, Any]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
    except Exception as exc:
        _log(f"save {Path(path).name} failed: {exc}")


def _is_null_env(row: Any) -> bool:
    return isinstance(row, Mapping) and row.get("type") == "null" and row.get("body") is None


def _load_unresolved() -> dict[str, Any]:
    return _load_json(_unresolved_path())


def _save_unresolved(data: Mapping[str, Any]) -> None:
    _save_json(_unresolved_path(), data)


def _freeze(
    item: Mapping[str, Any],
    *,
    action: str,
    reasons: list[str],
    ids_sent: Mapping[str, Any],
    watched_at: str | None,
) -> None:
    key = _thaw_key(item)
    data = _load_unresolved()
    row = data.get(key) or {
        "feature": "history",
        "action": action,
        "first_seen": _now_epoch(),
        "attempts": 0,
    }
    row.update({"item": id_minimal(item), "last_attempt": _now_epoch()})
    existing_reasons: list[str] = list(row.get("reasons", [])) if isinstance(row.get("reasons"), list) else []
    row["reasons"] = sorted(set(existing_reasons) | set(reasons or []))
    row["ids_sent"] = dict(ids_sent or {})
    if watched_at:
        row["watched_at"] = watched_at
    row["attempts"] = int(row.get("attempts", 0)) + 1
    data[key] = row
    _save_unresolved(data)


def _unfreeze(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for key in set(keys or []):
        if key in data:
            del data[key]
            changed = True
    if changed:
        _save_unresolved(data)


def _shadow_load() -> dict[str, Any]:
    return _load_json(_shadow_path()) or {"events": {}}


def _shadow_save(obj: Mapping[str, Any]) -> None:
    _save_json(_shadow_path(), obj)


def _shadow_item(item: Mapping[str, Any]) -> dict[str, Any]:
    typ = str(item.get("type") or "").lower()
    watched_at = item.get("watched_at") or item.get("watchedAt")
    if typ == "episode":
        ids = dict(item.get("ids") or {})
        show_ids = dict(item.get("show_ids") or {})
        out = {
            "type": "episode",
            "season": item.get("season"),
            "episode": item.get("episode"),
            "title": item.get("title"),
            "year": item.get("year"),
            "ids": ids,
            "show_ids": show_ids,
            "series_title": item.get("series_title"),
            "series_year": item.get("series_year"),
            "watched": item.get("watched"),
        }
        if isinstance(watched_at, str) and watched_at:
            out["watched_at"] = watched_at
        return out
    out = dict(id_minimal(item))
    if isinstance(watched_at, str) and watched_at:
        out.setdefault("watched_at", watched_at)
    return out


def _shadow_put_all(items: Iterable[Mapping[str, Any]]) -> None:
    items_list = list(items or [])
    if not items_list:
        return
    shadow = _shadow_load()
    events: dict[str, Any] = dict(shadow.get("events") or {})
    for item in items_list:
        watched_at = item.get("watched_at") or item.get("watchedAt")
        ts = _as_epoch(watched_at)
        bucket_key = _thaw_key(item)
        if not ts or not bucket_key:
            continue
        iso = watched_at if isinstance(watched_at, str) and watched_at else _as_iso(int(ts))
        shadow_item = _shadow_item(item)
        if isinstance(iso, str) and iso and "watched_at" not in shadow_item:
            shadow_item["watched_at"] = iso
        events[f"{bucket_key}@{ts}"] = {"item": shadow_item, "watched_at": iso}
    shadow["events"] = events
    _shadow_save(shadow)


def _shadow_merge_into(out: dict[str, dict[str, Any]], thaw: set[str]) -> None:
    shadow = _shadow_load()
    events: dict[str, Any] = dict(shadow.get("events") or {})
    if not events:
        return
    changed = False
    merged = 0
    for event_key, record in list(events.items()):
        if event_key in out:
            del events[event_key]
            changed = True
            continue
        item = record.get("item")
        if not isinstance(item, Mapping):
            continue
        item_dict: dict[str, Any] = dict(item)
        if not item_dict.get("watched_at"):
            iso = record.get("watched_at")
            if not (isinstance(iso, str) and iso):
                ts = _safe_int(str(event_key).rsplit("@", 1)[-1])
                iso = _as_iso(ts) if ts else None
            if isinstance(iso, str) and iso:
                item_dict["watched_at"] = iso
                fixed_record = dict(record)
                fixed_item = dict(item)
                fixed_item["watched_at"] = iso
                fixed_record["item"] = fixed_item
                fixed_record["watched_at"] = iso
                events[event_key] = fixed_record
                changed = True
        out[event_key] = item_dict
        thaw.add(_thaw_key(item_dict))
        merged += 1
    if merged:
        _log(f"shadow merged {merged} backfill events")
    if changed or merged:
        _shadow_save({"events": events})


_RESOLVE_CACHE: dict[str, dict[str, str]] = {}

def _load_show_map() -> dict[str, Any]:
    return _load_json(_show_map_path()) or {"map": {}}


def _save_show_map(obj: Mapping[str, Any]) -> None:
    _save_json(_show_map_path(), obj)


def _persist_show_map(key: str, ids: Mapping[str, Any]) -> None:
    ok = {k: str(v) for k, v in ids.items() if k in ("tvdb", "tmdb", "imdb", "simkl") and v}
    if not ok:
        return
    data = _load_show_map()
    mapping: dict[str, Any] = dict(data.get("map") or {})
    if mapping.get(key) == ok:
        return
    mapping[key] = ok
    data["map"] = mapping
    _save_show_map(data)


def _norm_title(value: str | None) -> str:
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())

_SMALL_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "but",
    "by",
    "for",
    "in",
    "nor",
    "of",
    "on",
    "or",
    "per",
    "the",
    "to",
    "vs",
    "via",
    "with",
}


def _slug_to_title(slug: str | None) -> str:
    s = (slug or "").strip().replace("_", "-")
    if not s:
        return ""
    parts = [p for p in s.split("-") if p]
    out: list[str] = []
    for i, p in enumerate(parts):
        w = p.lower()
        if i and w in _SMALL_WORDS:
            out.append(w)
            continue
        out.append(w[:1].upper() + w[1:])
    return " ".join(out)


def _best_ids(obj: Mapping[str, Any]) -> dict[str, str]:
    ids = dict(obj.get("ids") or obj or {})
    return {k: str(ids[k]) for k in ("tvdb", "tmdb", "imdb", "simkl") if ids.get(k)}

def _simkl_resolve_show_via_ids(adapter: Any, ids: Mapping[str, Any]) -> dict[str, str]:
    return {}


def _simkl_search_show(adapter: Any, title: str, year: int | None) -> dict[str, str]:
    if not title or os.getenv("CW_SIMKL_AUTO_RESOLVE", "1") == "0":
        return {}
    session = adapter.client.session
    headers = _headers(adapter, force_refresh=True)
    try:
        resp = session.get(
            f"{BASE}/search/tv",
            headers=headers,
            params={"q": title, "limit": 5, "extended": "full"},
            timeout=adapter.cfg.timeout,
        )
        if not resp.ok:
            return {}
        arr = resp.json() or []
    except Exception:
        return {}
    want = _norm_title(title)
    pick: dict[str, Any] = {}
    best = -1
    if isinstance(arr, list):
        for x in arr:
            show = (x.get("show") if isinstance(x, Mapping) else None) or x
            if not isinstance(show, Mapping):
                continue
            ids = _best_ids(show)
            show_title = (show or {}).get("title") or ""
            show_year = (show or {}).get("year")
            if not ids:
                continue
            score = 0
            if _norm_title(show_title) == want:
                score += 2
            if year and show_year:
                try:
                    if abs(int(show_year) - int(year)) <= 1:
                        score += 1
                except Exception:
                    pass
            if score > best:
                best = score
                pick = {"ids": ids, "title": show_title, "year": show_year}
    raw_ids = pick.get("ids")
    ids_source: Mapping[str, Any]
    if isinstance(raw_ids, Mapping):
        ids_source = raw_ids
    else:
        ids_source = pick
    return _best_ids(ids_source)


def _simkl_resolve_show_via_episode_id(adapter: Any, item: Mapping[str, Any]) -> dict[str, str]:
    return {}


def _resolve_show_ids(adapter: Any, item: Mapping[str, Any], raw_show_ids: Mapping[str, Any]) -> dict[str, str]:
    have = {k: raw_show_ids[k] for k in ("tvdb", "tmdb", "imdb", "simkl") if raw_show_ids.get(k)}
    return {k: str(v) for k, v in have.items()} if have else {}

def _fetch_kind(
    session: Any,
    headers: Mapping[str, str],
    *,
    kind: str,
    since_iso: str,
    timeout: float,
) -> list[dict[str, Any]]:
    ext = "full_anime_seasons" if kind == "anime" else "full"
    params = {"extended": ext, "episode_watched_at": "yes", "date_from": since_iso}
    if kind in {"shows", "anime"}:
        params["include_all_episodes"] = "yes"
    resp = session.get(f"{URL_ALL_ITEMS}/{kind}", headers=headers, params=params, timeout=timeout)
    if not resp.ok:
        _log(f"GET {URL_ALL_ITEMS}/{kind} -> {resp.status_code}")
        return []
    try:
        body = resp.json() or []
    except Exception:
        body = []
    if isinstance(body, list):
        return [x for x in body if not _is_null_env(x)]
    if isinstance(body, Mapping):
        arr = body.get(kind) or body.get("items") or []
        if isinstance(arr, list):
            return [x for x in arr if not _is_null_env(x)]
    return []





def _apply_since_limit(
    out: dict[str, dict[str, Any]],
    *,
    since: int | None,
    limit: int | None,
) -> None:
    if since is not None:
        cutoff = int(since)
        for k in list(out.keys()):
            ts = _safe_int(str(k).rsplit("@", 1)[-1])
            if ts and ts < cutoff:
                out.pop(k, None)

    if limit is None:
        return
    try:
        lim = int(limit)
    except Exception:
        return
    if lim <= 0 or len(out) <= lim:
        return

    scored: list[tuple[int, str]] = []
    for k in out.keys():
        ts = _safe_int(str(k).rsplit("@", 1)[-1])
        scored.append((ts, str(k)))
    scored.sort(reverse=True)
    keep = {k for _ts, k in scored[:lim]}
    for k in list(out.keys()):
        if k not in keep:
            out.pop(k, None)


def build_index(adapter: Any, since: int | None = None, limit: int | None = None) -> dict[str, dict[str, Any]]:
    session = adapter.client.session
    timeout = adapter.cfg.timeout
    normalize_flat_watermarks()
    out: dict[str, dict[str, Any]] = {}
    thaw: set[str] = set()

    acts, _rate = fetch_activities(session, _headers(adapter, force_refresh=True), timeout=timeout)
    act_latest: str | None = None

    if isinstance(acts, Mapping):
        wm = get_watermark("history") or ""
        lm = extract_latest_ts(acts, (("movies", "playback"), ("movies", "all")))
        ls = extract_latest_ts(acts, (("tv_shows", "playback"), ("shows", "playback"), ("tv_shows", "all"), ("shows", "all")))
        la = extract_latest_ts(acts, (("anime", "playback"), ("anime", "all")))
        candidates = [t for t in (lm, ls, la) if isinstance(t, str) and t]
        act_latest = max(candidates) if candidates else None

        unchanged = (lm is None or lm <= wm) and (ls is None or ls <= wm) and (la is None or la <= wm)
        if unchanged:
            _log(f"activities unchanged; history from shadow (m={lm} s={ls} a={la})", level="info")
            _shadow_merge_into(out, thaw)
            _dedupe_history_movies(out)
            _apply_since_limit(out, since=since, limit=limit)
            _unfreeze(thaw)
            try:
                _shadow_put_all(out.values())
            except Exception as exc:
                _log(f"shadow.put index skipped: {exc}")
            _log(f"index size: {len(out)} (shadow)", level="info")
            return out

    headers = _headers(adapter, force_refresh=True)
    added = 0
    latest_ts_movies: int | None = None
    latest_ts_shows: int | None = None
    latest_ts_anime: int | None = None
    cfg_iso = _as_iso(since) if since else None
    df_iso = coalesce_date_from("history", cfg_date_from=cfg_iso)
    if since:
        since_iso = _as_iso(int(since))
        try:
            sm = max(_as_epoch(df_iso) or 0, _as_epoch(since_iso) or 0)
            ss = max(_as_epoch(df_iso) or 0, _as_epoch(since_iso) or 0)
            df_iso = _as_iso(sm)
            df_iso = _as_iso(ss)
        except Exception:
            pass

    movie_rows = _fetch_kind(session, headers, kind="movies", since_iso=df_iso, timeout=timeout)
    movies_cnt = 0
    for row in movie_rows:
        if not isinstance(row, Mapping):
            continue
        watched_at = (row.get("last_watched_at") or row.get("watched_at") or "").strip()
        ts = _as_epoch(watched_at)
        if not ts:
            continue
        movie_norm = simkl_normalize(row)
        if not movie_norm or str(movie_norm.get("type") or "").lower() != "movie":
            continue
        movie_norm["watched"] = True
        movie_norm["watched_at"] = watched_at
        movie_norm["simkl_bucket"] = "movies"
        bucket_key = simkl_key_of(movie_norm)
        event_key = f"{bucket_key}@{ts}"
        if event_key in out:
            continue
        out[event_key] = movie_norm
        thaw.add(bucket_key)
        movies_cnt += 1
        added += 1
        latest_ts_movies = max(latest_ts_movies or 0, ts)
        if limit and added >= limit:
            break

    if not limit or added < limit:
        show_rows = _fetch_kind(session, headers, kind="shows", since_iso=df_iso, timeout=timeout)
        anime_rows = _fetch_kind(session, headers, kind="anime", since_iso=df_iso, timeout=timeout)
        eps_cnt = 0
        for row, row_kind in chain(((r, "shows") for r in show_rows), ((r, "anime") for r in anime_rows)):
            if not isinstance(row, Mapping):
                continue
            show = row.get("show") or row
            if not show:
                continue
            base = simkl_normalize(row)
            show_ids = _ids_of(base) or _ids_of(show)
            if not show_ids:
                continue
            show_title = str(
                base.get("title") or (show.get("title") if isinstance(show, Mapping) else "") or "",
            ).strip()
            show_year = base.get("year") or (show.get("year") if isinstance(show, Mapping) else None)
            series_name: str | None = show_title or (base.get("title") if isinstance(base, Mapping) else None)
            if row_kind == "anime":
                raw_ids = show.get("ids") if isinstance(show, Mapping) else None
                if isinstance(raw_ids, Mapping):
                    slug = raw_ids.get("tvdbslug") or raw_ids.get("trakttvslug")
                    if isinstance(slug, str) and slug:
                        series_name = _slug_to_title(slug) or series_name
            if not (series_name.strip() if isinstance(series_name, str) else ""):
                sid = str(show_ids.get("simkl") or "").strip()
                series_name = f"SIMKL:{sid}" if sid else "Unknown Series"
            if row_kind == "anime":
                at = (show.get("anime_type") or show.get("animeType")) if isinstance(show, Mapping) else None
                anime_type = at.strip().lower() if isinstance(at, str) and at.strip() else None
                if anime_type == "movie":
                    watched_at = (row.get("last_watched_at") or row.get("watched_at") or "").strip()
                    if not watched_at:
                        best_ts = 0
                        best = ""
                        for season in row.get("seasons") or []:
                            season = season if isinstance(season, Mapping) else {}
                            for episode in (season.get("episodes") or []):
                                episode = episode if isinstance(episode, Mapping) else {}
                                wa = (episode.get("watched_at") or episode.get("last_watched_at") or "").strip()
                                ts_wa = _as_epoch(wa)
                                if ts_wa and ts_wa > best_ts:
                                    best_ts = ts_wa
                                    best = wa
                        watched_at = best
                    ts = _as_epoch(watched_at)
                    if ts:
                        movie_item: dict[str, Any] = {
                            "type": "movie",
                            "title": series_name,
                            "year": show_year,
                            "ids": dict(show_ids),
                            "simkl_bucket": "anime",
                            "anime_type": "movie",
                            "watched": True,
                            "watched_at": watched_at,
                        }
                        bucket_key = simkl_key_of(movie_item)
                        event_key = f"{bucket_key}@{ts}"
                        if event_key not in out:
                            out[event_key] = movie_item
                            thaw.add(bucket_key)
                            added += 1
                            latest_ts_anime = max(latest_ts_anime or 0, ts)
                            if limit and added >= limit:
                                break
                    continue

            for season in row.get("seasons") or []:
                season = season if isinstance(season, Mapping) else {}
                s_num_internal = int((season.get("number") or season.get("season") or 0))
                for episode in (season.get("episodes") or []):
                    episode = episode if isinstance(episode, Mapping) else {}
                    e_num_internal = int((episode.get("number") or episode.get("episode") or 0))
                    s_num = s_num_internal
                    e_num = e_num_internal
                    if row_kind == "anime":
                        tvdb_map = episode.get("tvdb")
                        if isinstance(tvdb_map, Mapping):
                            s_m = int(tvdb_map.get("season") or 0)
                            e_m = int(tvdb_map.get("episode") or 0)
                            if s_m >= 1 and e_m >= 1:
                                s_num = s_m
                                e_num = e_m
                    watched_at = (episode.get("watched_at") or episode.get("last_watched_at") or "").strip()
                    ts = _as_epoch(watched_at)
                    if not ts or not s_num or not e_num:
                        continue
                    ep = {
                        "type": "episode",
                        "season": s_num,
                        "episode": e_num,
                        "ids": dict(show_ids),
                        "title": f"S{s_num:02d}E{e_num:02d}",
                        "year": None,
                        "series_title": series_name,
                        "series_year": show_year,
                        "show_ids": dict(show_ids),
                        "watched": True,
                        "watched_at": watched_at,
                        "simkl_bucket": row_kind,
                    }
                    bucket_key = simkl_key_of(ep)
                    event_key = f"{bucket_key}@{ts}"
                    if event_key in out:
                        continue
                    out[event_key] = ep
                    thaw.add(bucket_key)
                    eps_cnt += 1
                    added += 1
                    if row_kind == "anime":
                        latest_ts_anime = max(latest_ts_anime or 0, ts)
                    else:
                        latest_ts_shows = max(latest_ts_shows or 0, ts)
                    if limit and added >= limit:
                        break
                if limit and added >= limit:
                    break
        _shadow_merge_into(out, thaw)
        _dedupe_history_movies(out)
        _log(
            f"movies={movies_cnt} episodes={eps_cnt} from={df_iso}",
            level="info",
        )
    else:
        _shadow_merge_into(out, thaw)
        _dedupe_history_movies(out)
        _log(
            f"movies={movies_cnt} episodes=0 from={df_iso}",
            level="info",
        )

    if act_latest:
        update_watermark_if_new("history", act_latest)

    latest_any = max([t for t in (latest_ts_movies, latest_ts_shows, latest_ts_anime) if isinstance(t, int)], default=None)
    if latest_any is not None:
        update_watermark_if_new("history", _as_iso(latest_any))

    _unfreeze(thaw)
    try:
        _shadow_put_all(out.values())
    except Exception as exc:
        _log(f"shadow.put index skipped: {exc}")
    _log(f"index size: {len(out)}", level="info")
    return out

def _movie_add_entry(item: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = _ids_of(item)
    watched_at = (item.get("watched_at") or item.get("watchedAt") or "").strip()
    if not ids or not watched_at:
        return None
    return {"ids": ids, "watched_at": watched_at}


def _is_anime_like(item: Mapping[str, Any], ids: Mapping[str, Any]) -> bool:
    bucket = str(item.get("simkl_bucket") or "").strip().lower()
    if bucket == "anime":
        return True
    typ = str(item.get("type") or "").lower()
    if typ == "anime":
        return True
    for k in ("mal", "anidb", "anilist", "kitsu"):
        if ids.get(k):
            return True
    return False



def _show_add_entry(adapter: Any, item: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = _ids_of(item)
    if not ids:
        return None
    ids = _maybe_map_tvdb(adapter, ids)
    entry: dict[str, Any] = {"ids": ids}
    if _is_anime_like(item, ids):
        entry["use_tvdb_anime_seasons"] = True
    return entry


def _show_scope_entry(adapter: Any, item: Mapping[str, Any], raw_show_ids: Mapping[str, Any]) -> dict[str, Any] | None:
    show_ids = {k: str(raw_show_ids[k]) for k in ID_KEYS if raw_show_ids.get(k)}
    show_ids = _maybe_map_tvdb(adapter, show_ids)
    if not show_ids:
        return None

    show: dict[str, Any] = {"ids": show_ids}
    if _is_anime_like(item, show_ids):
        show["use_tvdb_anime_seasons"] = True

    show_title = item.get("series_title") or item.get("title")
    if isinstance(show_title, str) and show_title.strip():
        show["title"] = show_title.strip()

    series_year = item.get("series_year") or item.get("year")
    if isinstance(series_year, int):
        show["year"] = series_year
    elif isinstance(series_year, str) and series_year.isdigit():
        show["year"] = int(series_year)

    return show


def _season_add_entry(adapter: Any, item: Mapping[str, Any]) -> tuple[dict[str, Any], int, str] | None:
    show_ids_raw = _raw_show_ids(item)
    if not show_ids_raw:
        return None
    show = _show_scope_entry(adapter, item, show_ids_raw)
    if not show:
        return None

    s_num = _safe_int(item.get("season") or item.get("season_number"))
    watched_at = item.get("watched_at") or item.get("watchedAt")
    if not s_num or not isinstance(watched_at, str) or not watched_at:
        return None
    return show, s_num, watched_at


def _episode_add_entry(adapter: Any, item: Mapping[str, Any]) -> tuple[dict[str, Any], int, int, str] | None:
    show_ids_raw = _show_ids_of_episode(item)
    if not show_ids_raw:
        return None
    show = _show_scope_entry(adapter, item, show_ids_raw)
    if not show:
        return None

    s_num = _safe_int(item.get("season") or item.get("season_number"))
    e_num = _safe_int(item.get("episode") or item.get("episode_number"))
    watched_at = item.get("watched_at") or item.get("watchedAt")
    if not s_num or not e_num or not isinstance(watched_at, str) or not watched_at:
        return None

    return show, s_num, e_num, watched_at


def _merge_show_group(groups: dict[str, dict[str, Any]], show_entry: Mapping[str, Any]) -> dict[str, Any]:
    ids_key = json.dumps(show_entry.get("ids") or {}, sort_keys=True)
    group = groups.setdefault(ids_key, {"ids": dict(show_entry.get("ids") or {}), "seasons": []})
    for key in ("title", "year", "use_tvdb_anime_seasons"):
        value = show_entry.get(key)
        if value not in (None, "", False):
            group[key] = value
    return group


def _merge_show_season(group: dict[str, Any], season_number: int, *, watched_at: str | None = None) -> dict[str, Any]:
    # Ensure seasons list exists and is well-typed for analyzers.
    seasons_obj = group.setdefault("seasons", [])
    if not isinstance(seasons_obj, list):
        seasons_obj = []
        group["seasons"] = seasons_obj
    seasons = cast(list[dict[str, Any]], seasons_obj)
    season: dict[str, Any] | None = next(
        (s for s in seasons if isinstance(s, dict) and s.get("number") == season_number),
        None,
    )
    if season is None:
        season = {"number": season_number}
        seasons.append(season)
    if isinstance(watched_at, str) and watched_at and not season.get("watched_at"):
        season["watched_at"] = watched_at
    return season

def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    timeout = adapter.cfg.timeout
    movies: list[dict[str, Any]] = []
    shows_whole: list[dict[str, Any]] = []
    shows_scoped: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    thaw_keys: list[str] = []
    shadow_events: list[dict[str, Any]] = []
    items_list: list[Mapping[str, Any]] = list(items or [])

    guid_eps = sum(
        1
        for it in items_list
        if str((it.get("ids") or {}).get("guid") or "").startswith("plex://show/")
    )
    guid_mov = sum(
        1
        for it in items_list
        if str((it.get("ids") or {}).get("guid") or "").startswith("plex://movie/")
    )
    _log(f"incoming items={len(items_list)} guid_eps={guid_eps} guid_movies={guid_mov}")

    for item in items_list:
        if isinstance(item, dict):
            item["_adapter"] = adapter

    unresolved_eps_missing = 0

    for item in items_list:
        typ = str(item.get("type") or "").lower()
        bucket = str(item.get("simkl_bucket") or "").strip().lower()
        if typ == "movie" and bucket == "anime":
            entry = _show_add_entry(adapter, item)
            watched_at = str(item.get("watched_at") or "").strip()
            if entry and watched_at:
                entry["watched_at"] = watched_at
            if entry:
                shows_whole.append(entry)
                thaw_keys.append(_thaw_key(item))
                ev = dict(id_minimal(item))
                if watched_at:
                    ev["watched_at"] = watched_at
                    shadow_events.append(ev)
            else:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids_or_watched_at"})
            continue

        if typ == "movie":
            entry = _movie_add_entry(item)
            if entry:
                movies.append(entry)
                thaw_keys.append(_thaw_key(item))
                ev = dict(id_minimal(item))
                ev["watched_at"] = entry.get("watched_at")
                if ev.get("watched_at"):
                    shadow_events.append(ev)
            else:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids_or_watched_at"})
            continue

        if typ == "season":
            packed = _season_add_entry(adapter, item)
            if not packed:
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_season_or_watched_at"},
                )
                continue

            show_entry, s_num, watched_at = packed
            group = _merge_show_group(shows_scoped, show_entry)
            _merge_show_season(group, s_num, watched_at=watched_at)

            thaw_keys.append(_thaw_key(item))
            ev = dict(id_minimal(item))
            ev["watched_at"] = watched_at
            shadow_events.append(ev)
            continue

        if typ == "episode":
            packed = _episode_add_entry(adapter, item)
            if not packed:
                unresolved_eps_missing += 1
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_s/e_or_watched_at"},
                )
                continue

            show_entry, s_num, e_num, watched_at = packed
            group = _merge_show_group(shows_scoped, show_entry)
            season = _merge_show_season(group, s_num)
            season.setdefault("episodes", []).append({"number": e_num, "watched_at": watched_at})

            thaw_keys.append(_thaw_key(item))
            ev = dict(item)
            ev.pop("_adapter", None)
            ev["watched_at"] = watched_at
            shadow_events.append(ev)
            continue

        entry = _show_add_entry(adapter, item)
        if entry:
            shows_whole.append(entry)
            thaw_keys.append(_thaw_key(item))
        else:
            unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})

    _log(
        f"prepared movies={len(movies)} shows_whole={len(shows_whole)} shows_scoped={len(shows_scoped)} "
        f"unresolved_eps_missing={unresolved_eps_missing}",
    )

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies

    shows_payload: list[dict[str, Any]] = []
    if shows_whole:
        shows_payload.extend(shows_whole)
    if shows_scoped:
        shows_payload.extend(list(shows_scoped.values()))
    if shows_payload:
        body["shows"] = shows_payload

    if not body:
        return 0, unresolved

    try:
        resp = session.post(URL_ADD, headers=headers, json=body, timeout=timeout)
        if 200 <= resp.status_code < 300:
            _unfreeze(thaw_keys)
            eps_count = sum(
                len(season.get("episodes", []))
                for group in shows_scoped.values()
                for season in group.get("seasons", [])
            )
            seasons_count = sum(
                1
                for group in shows_scoped.values()
                for season in group.get("seasons", [])
                if season.get("watched_at")
            )

            added_new = {"movies": 0, "shows": 0, "episodes": 0}
            not_found = {"movies": [], "shows": [], "episodes": []}
            try:
                payload = resp.json() if (resp.text or '').strip() else {}
                if isinstance(payload, dict):
                    a = payload.get("added")
                    if isinstance(a, dict):
                        added_new["movies"] = int(a.get("movies") or 0)
                        added_new["shows"] = int(a.get("shows") or 0)
                        added_new["episodes"] = int(a.get("episodes") or 0)
                    nf = payload.get("not_found")
                    if isinstance(nf, dict):
                        not_found["movies"] = list(nf.get("movies") or [])
                        not_found["shows"] = list(nf.get("shows") or [])
                        not_found["episodes"] = list(nf.get("episodes") or [])
            except Exception as exc:
                _log(f"add response parse skipped: {exc}")

            nf_total = len(not_found["movies"]) + len(not_found["shows"]) + len(not_found["episodes"])
            if nf_total:
                _log(
                    f"add not_found: movies={len(not_found['movies'])} shows={len(not_found['shows'])} episodes={len(not_found['episodes'])}",
                )

                for bucket_name in ("movies", "shows", "episodes"):
                    for obj in not_found[bucket_name][:50]:
                        if isinstance(obj, dict):
                            unresolved.append({"item": obj, "hint": f"simkl_not_found:{bucket_name}"})
                        else:
                            unresolved.append({"item": {"raw": obj}, "hint": f"simkl_not_found:{bucket_name}"})

            ok = max(0, len(thaw_keys) - nf_total)
            _log(
                f"add done http:{resp.status_code} sent={len(thaw_keys)} added_new(m/s/e)="
                f"{added_new['movies']}/{added_new['shows']}/{added_new['episodes']} "
                f"movies={len(movies)} shows_payload={len(shows_payload)} seasons={seasons_count} episodes={eps_count} not_found={nf_total}",
            )
            try:
                _shadow_put_all(shadow_events)
            except Exception as exc:
                _log(f"shadow.put skipped: {exc}")
            return ok, unresolved

        _log(f"ADD failed {resp.status_code}: {(resp.text or '')[:200]}")
    except Exception as exc:
        _log(f"ADD error: {exc}")

    for item in items_list:
        ids = _scope_ids_for_freeze(item)
        watched_at = item.get("watched_at") or item.get("watchedAt") or None
        watched_str = watched_at if isinstance(watched_at, str) else None
        if ids:
            _freeze(
                item,
                action="add",
                reasons=["write_failed"],
                ids_sent=ids,
                watched_at=watched_str,
            )
    return 0, unresolved

def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    timeout = adapter.cfg.timeout
    movies: list[dict[str, Any]] = []
    shows_whole: list[dict[str, Any]] = []
    shows_scoped: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    thaw_keys: list[str] = []
    items_list: list[Mapping[str, Any]] = list(items or [])
    for item in items_list:
        typ = str(item.get("type") or "").lower()
        bucket = str(item.get("simkl_bucket") or "").strip().lower()
        if typ == "movie" and bucket == "anime":
            ids = _ids_of(item)
            if not ids:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})
                continue
            shows_whole.append({"ids": ids})
            thaw_keys.append(_thaw_key(item))
            continue
        if typ == "movie":
            ids = _ids_of(item)
            if not ids:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})
                continue
            movies.append({"ids": ids})
            thaw_keys.append(_thaw_key(item))
            continue
        if typ == "season":
            show_ids = _raw_show_ids(item)
            show_entry = _show_scope_entry(adapter, item, show_ids) if show_ids else None
            s_num = int(item.get("season") or item.get("season_number") or 0)
            if not show_entry or not s_num:
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_season"},
                )
                continue
            group = _merge_show_group(shows_scoped, show_entry)
            _merge_show_season(group, s_num)
            thaw_keys.append(_thaw_key(item))
            continue
        if typ == "episode":
            show_ids = _show_ids_of_episode(item)
            s_num = int(item.get("season") or item.get("season_number") or 0)
            e_num = int(item.get("episode") or item.get("episode_number") or 0)
            if not show_ids or not s_num or not e_num:
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_s/e"},
                )
                continue
            show_entry = _show_scope_entry(adapter, item, show_ids)
            if not show_entry:
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_s/e"},
                )
                continue
            group = _merge_show_group(shows_scoped, show_entry)
            season = _merge_show_season(group, s_num)
            season.setdefault("episodes", []).append({"number": e_num})
            thaw_keys.append(_thaw_key(item))
            continue
        ids = _ids_of(item)
        if ids:
            shows_whole.append({"ids": ids})
            thaw_keys.append(_thaw_key(item))
        else:
            unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})
    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    shows_payload: list[dict[str, Any]] = []
    if shows_whole:
        shows_payload.extend(shows_whole)
    if shows_scoped:
        shows_payload.extend(list(shows_scoped.values()))
    if shows_payload:
        body["shows"] = shows_payload
    if not body:
        return 0, unresolved
    try:
        resp = session.post(URL_REMOVE, headers=headers, json=body, timeout=timeout)
        if 200 <= resp.status_code < 300:
            _unfreeze(thaw_keys)
            ok = len(thaw_keys)
            _log(
                f"remove done http:{resp.status_code} sent={len(thaw_keys)} movies={len(movies)} shows_payload={len(shows_payload)}",
            )
            return ok, unresolved
        _log(f"REMOVE failed {resp.status_code}: {(resp.text or '')[:200]}")
    except Exception as exc:
        _log(f"REMOVE error: {exc}")
    for item in items_list:
        ids = _scope_ids_for_freeze(item)
        if ids:
            _freeze(
                item,
                action="remove",
                reasons=["write_failed"],
                ids_sent=ids,
                watched_at=None,
            )
    return 0, unresolved
