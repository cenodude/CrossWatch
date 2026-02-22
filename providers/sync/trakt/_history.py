# /providers/sync/trakt/_history.py
# TRAKT Module for history sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from ._common import (
    build_headers,
    key_of,
    ids_for_trakt,
    pick_trakt_kind,
    fetch_last_activities,
    update_watermarks_from_last_activities,
    extract_latest_ts,
    state_file,
    _pair_scope,
)
from .._mod_common import request_with_retries
from cw_platform.id_map import minimal as id_minimal, canonical_key
from .._log import log as cw_log

BASE = "https://api.trakt.tv"
URL_HIST_MOV = f"{BASE}/sync/history/movies"
URL_HIST_EPI = f"{BASE}/sync/history/episodes"
URL_ADD = f"{BASE}/sync/history"
URL_REMOVE = f"{BASE}/sync/history/remove"
URL_COLL_ADD = f"{BASE}/sync/collection"
RESOLVE_ENABLE = False

def _unresolved_path() -> Path:
    return state_file("trakt_history.unresolved.json")


def _last_limit_path() -> Path:
    return state_file("trakt_last_limit_error.json")


def _cache_path() -> Path:
    return state_file("trakt_history.index.json")


def _bust_index_cache(reason: str) -> None:
    try:
        p = _cache_path()
        if p.exists():
            p.unlink()
            _info("index_cache_bust", reason=reason)
    except Exception as e:
        _warn("index_cache_bust_failed", reason=reason, error=str(e))



def _not_found_count(nf: Any) -> int:
    if not isinstance(nf, dict):
        return 0
    c = 0
    for v in nf.values():
        if isinstance(v, list):
            c += len(v)
    return c

def _record_limit_error(feature: str) -> None:
    if _pair_scope() is None:
        return
    try:
        _last_limit_path().parent.mkdir(parents=True, exist_ok=True)
        tmp = _last_limit_path().with_suffix(".tmp")
        tmp.write_text(
            json.dumps(
                {"feature": feature, "ts": _now_iso()},
                ensure_ascii=False,
                sort_keys=True,
            ),
            "utf-8",
        )
        os.replace(tmp, _last_limit_path())
    except Exception as e:
        _warn("limit_error_save_failed", feature=feature, error=str(e))

_PROVIDER = "TRAKT"
_FEATURE = "history"


def _dbg(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "debug", event, **fields)

def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)

def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)

def _error(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "error", event, **fields)



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
    if _pair_scope() is None:
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


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _iso8601(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None

    epoch: int | None = None
    if s.isdigit() and len(s) >= 13:
        try:
            epoch = int(s) // 1000
        except Exception:
            return None
    elif s.isdigit():
        try:
            epoch = int(s)
        except Exception:
            return None
    else:
        if "T" not in s:
            return None
        try:
            from datetime import datetime
            iso = s
            if iso.endswith("Z"):
                iso = iso.replace("Z", "+00:00")
            else:
                tail = iso[10:]
                if "+" not in tail and "-" not in tail:
                    iso = iso + "+00:00"
            epoch = int(datetime.fromisoformat(iso).timestamp())
        except Exception:
            return None

    if epoch is None:
        return None

    # Trakt is moving watched_at to minute precision (seconds + milliseconds => 00.000Z).
    epoch = (epoch // 60) * 60
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(epoch))


def _as_epoch(iso: str) -> int | None:
    try:
        from datetime import datetime
        s = iso.replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None


def _cfg(adapter: Any) -> Any:
    return getattr(adapter, "cfg", None) or getattr(adapter, "config", {})


def _cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    c = _cfg(adapter)
    try:
        if hasattr(c, key):
            v = getattr(c, key)
            return default if v is None else v
    except Exception:
        pass
    if isinstance(c, Mapping):
        v = c.get(key, default)
        return default if v is None else v
    return default


def _cfg_num(adapter: Any, key: str, default: Any, cast: Callable[[Any], Any] = int) -> Any:
    try:
        v = _cfg_get(adapter, key, default)
        return cast(v)
    except Exception:
        return cast(default)


def _freeze_enabled(adapter: Any) -> bool:
    v = _cfg_get(adapter, "history_unresolved", False)
    try:
        return bool(v)
    except Exception:
        return False


def _history_number_fallback_enabled(adapter: Any) -> bool:
    return True if not RESOLVE_ENABLE else bool(_cfg_get(adapter, "history_number_fallback", False))


def _history_collection_enabled(adapter: Any) -> bool:
    return bool(_cfg_get(adapter, "history_collection", False))


def _history_collection_types(adapter: Any) -> set[str]:
    raw = _cfg_get(adapter, "history_collection_types", None)
    allowed = {"movies", "shows"}
    vals: list[str] = []
    if isinstance(raw, str):
        vals = [x.strip().lower() for x in raw.split(",") if x and x.strip()]
    elif isinstance(raw, list):
        vals = [str(x).strip().lower() for x in raw if str(x).strip()]
    out = {x for x in vals if x in allowed}
    if _history_collection_enabled(adapter) and not out:
        out = {"movies"}
    return out



def _load_unresolved() -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    p = _unresolved_path()
    _migrate_legacy_json(p)
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return {}


def _save_unresolved(data: Mapping[str, Any]) -> None:
    if _pair_scope() is None:
        return
    try:
        _unresolved_path().parent.mkdir(parents=True, exist_ok=True)
        tmp = _unresolved_path().with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, _unresolved_path())
    except Exception as e:
        _warn("unresolved_save_failed", error=str(e))


def _load_cache_doc() -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    try:
        p = _cache_path()
        _migrate_legacy_json(p)
        if not p.exists():
            return {}
        return json.loads(p.read_text("utf-8") or "{}")
    except Exception:
        return {}


def _save_cache_doc(items: Mapping[str, Any], watched_at: str | None) -> None:
    if _pair_scope() is None:
        return
    try:
        _cache_path().parent.mkdir(parents=True, exist_ok=True)
        doc = {"generated_at": _now_iso(), "items": dict(items), "wm": {"watched_at": watched_at or ""}}
        tmp = _cache_path().with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, _cache_path())
    except Exception as e:
        _warn("cache_save_failed", error=str(e))


def _freeze_item_if_enabled(adapter: Any, item: Mapping[str, Any], *, action: str, reasons: list[str]) -> None:
    if not _freeze_enabled(adapter):
        return
    m = id_minimal(item)
    k = key_of(m)
    data = _load_unresolved()
    entry = data.get(k) or {"feature": "history", "action": action, "first_seen": _now_iso(), "attempts": 0}
    entry.update({"item": m, "last_attempt": _now_iso()})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    entry["reasons"] = sorted(rset)
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[k] = entry
    _save_unresolved(data)


def _unfreeze_keys_if_present(adapter: Any, keys: Iterable[str]) -> None:
    if not _freeze_enabled(adapter):
        return
    data = _load_unresolved()
    changed = False
    for k in list(keys or []):
        if k in data:
            del data[k]
            changed = True
    if changed:
        _save_unresolved(data)


def _is_frozen(adapter: Any, item: Mapping[str, Any]) -> bool:
    if not _freeze_enabled(adapter):
        return False
    return key_of(id_minimal(item)) in _load_unresolved()


def _hdr_int(headers: Mapping[str, Any], name: str) -> int | None:
    try:
        for k, v in (headers or {}).items():
            if str(k).lower() == name.lower():
                return int(str(v).strip())
    except Exception:
        return None
    return None


def _preflight_total(
    sess: Any,
    headers: Mapping[str, Any],
    url: str,
    *,
    per_page: int,
    timeout: float,
    max_retries: int,
    max_pages: int | None,
) -> int | None:
    try:
        r = request_with_retries(
            sess,
            "GET",
            url,
            headers=headers,
            params={"page": 1, "limit": per_page},
            timeout=timeout,
            max_retries=max_retries,
        )
        if r.status_code != 200:
            return None
        item_count = _hdr_int(r.headers, "X-Pagination-Item-Count")
        if item_count is None:
            page_count = _hdr_int(r.headers, "X-Pagination-Page-Count")
            limit_hdr = _hdr_int(r.headers, "X-Pagination-Limit") or per_page
            if page_count is not None and limit_hdr:
                item_count = int(page_count) * int(limit_hdr)
        if item_count is None:
            return None
        if max_pages and max_pages > 0:
            item_count = min(item_count, int(max_pages) * int(per_page))
        return int(item_count)
    except Exception:
        return None


def _fetch_history(
    sess: Any,
    headers: Mapping[str, Any],
    url: str,
    *,
    per_page: int,
    max_pages: int,
    timeout: float,
    max_retries: int,
    bump: Callable[[int], None] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    total_pages: int | None = None
    while True:
        r = request_with_retries(
            sess,
            "GET",
            url,
            headers=headers,
            params={"page": page, "limit": per_page},
            timeout=timeout,
            max_retries=max_retries,
        )
        if r.status_code != 200:
            _warn("http_page_failed", url=url, page=page, status=r.status_code)
            break
        if total_pages is None:
            pc = _hdr_int(r.headers, "X-Pagination-Page-Count")
            if pc is not None:
                total_pages = pc
        rows = r.json() or []
        if not rows:
            break
        added = 0
        for row in rows:
            w = row.get("watched_at")
            if not w:
                continue
            typ = (row.get("type") or "").lower()
            if typ == "movie" and isinstance(row.get("movie"), dict):
                mv = row["movie"]
                m = id_minimal(
                    {"type": "movie", "ids": mv.get("ids") or {}, "title": mv.get("title"), "year": mv.get("year")}
                )
                m["watched_at"] = w
                out.append(m)
                added += 1
            elif typ == "episode" and isinstance(row.get("episode"), dict):
                ep = row["episode"]
                show = row.get("show") or {}
                m = id_minimal(
                    {
                        "type": "episode",
                        "ids": ep.get("ids") or {},
                        "show_ids": show.get("ids") or {},
                        "season": ep.get("season"),
                        "episode": ep.get("number"),
                        "series_title": show.get("title"),
                        "title": ep.get("title"),
                    }
                )
                m["watched_at"] = w
                out.append(m)
                added += 1
        if bump and added:
            try:
                bump(added)
            except Exception:
                pass
        page += 1
        if total_pages is not None and page > total_pages:
            break
        if total_pages is None and len(rows) < per_page:
            break
        if max_pages and page > max_pages:
            _warn("page_safety_cap", max_pages=max_pages)
            break
    return out


def build_index(adapter: Any, *, per_page: int = 100, max_pages: int = 100000) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("history") if callable(prog_mk) else None
    sess = adapter.client.session
    headers = build_headers(
        {
            "trakt": {
                "client_id": _cfg_get(adapter, "client_id"),
                "access_token": _cfg_get(adapter, "access_token"),
            }
        }
    )
    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))
    cfg_per_page = int(_cfg_num(adapter, "history_per_page", per_page, int))
    cfg_per_page = max(1, min(100, cfg_per_page))
    cfg_max_pages = int(_cfg_num(adapter, "history_max_pages", max_pages, int))
    if cfg_max_pages <= 0:
        cfg_max_pages = max_pages

    doc = _load_cache_doc()
    cached_items: dict[str, dict[str, Any]] = dict(doc.get("items") or {})
    cached_wm = str((doc.get("wm") or {}).get("watched_at") or "").strip()

    acts = fetch_last_activities(sess, headers, timeout=timeout, max_retries=retries)
    update_watermarks_from_last_activities(acts)
    remote_wm = extract_latest_ts(acts or {}, (("movies", "watched_at"), ("episodes", "watched_at"))) if acts else None

    if cached_items and remote_wm and cached_wm:
        a = _as_epoch(_iso8601(remote_wm) or "")
        b = _as_epoch(_iso8601(cached_wm) or "")
        if a is not None and b is not None and a <= b:
            _info("index_cache_hit", reason="activities_unchanged", count=len(cached_items))
            if prog:
                try:
                    prog.tick(0, total=len(cached_items), force=True)
                    prog.tick(len(cached_items), total=len(cached_items))
                    prog.done(ok=True, total=len(cached_items))
                except Exception:
                    pass
            return cached_items
    elif cached_items and not remote_wm:
        _info("index_cache_hit", reason="activities_unavailable", count=len(cached_items))
        if prog:
            try:
                prog.tick(0, total=len(cached_items), force=True)
                prog.tick(len(cached_items), total=len(cached_items))
                prog.done(ok=True, total=len(cached_items))
            except Exception:
                pass
        return cached_items

    total_mov = _preflight_total(
        sess,
        headers,
        URL_HIST_MOV,
        per_page=cfg_per_page,
        timeout=timeout,
        max_retries=retries,
        max_pages=cfg_max_pages,
    )
    total_epi = _preflight_total(
        sess,
        headers,
        URL_HIST_EPI,
        per_page=cfg_per_page,
        timeout=timeout,
        max_retries=retries,
        max_pages=cfg_max_pages,
    )
    announced_total: int | None = None
    if total_mov is not None and total_epi is not None:
        announced_total = int(total_mov) + int(total_epi)
        if prog:
            try:
                prog.tick(0, total=announced_total, force=True)
            except Exception:
                pass
    done = 0

    def bump(n: int) -> None:
        nonlocal done
        done += int(n or 0)
        if prog:
            try:
                if announced_total is not None:
                    prog.tick(done, total=announced_total)
                else:
                    prog.tick(done)
            except Exception:
                pass

    movies = _fetch_history(
        sess,
        headers,
        URL_HIST_MOV,
        per_page=cfg_per_page,
        max_pages=cfg_max_pages,
        timeout=timeout,
        max_retries=retries,
        bump=bump,
    )
    episodes = _fetch_history(
        sess,
        headers,
        URL_HIST_EPI,
        per_page=cfg_per_page,
        max_pages=cfg_max_pages,
        timeout=timeout,
        max_retries=retries,
        bump=bump,
    )
    idx: dict[str, dict[str, Any]] = {}
    base_keys_to_unfreeze: set[str] = set()
    for m in movies + episodes:
        w = _iso8601(m.get("watched_at"))
        ts = _as_epoch(w) if w else None
        if not ts:
            continue
        if (
            m.get("type") == "episode"
            and isinstance(m.get("show_ids"), dict)
            and m.get("season") is not None
            and m.get("episode") is not None
        ):
            base_key = canonical_key(
                id_minimal(
                    {
                        "type": "episode",
                        "show_ids": m["show_ids"],
                        "season": m["season"],
                        "episode": m["episode"],
                    }
                )
            )
        else:
            base_key = canonical_key(id_minimal(m))
        ek = f"{base_key}@{ts}"
        idx[ek] = m
        base_keys_to_unfreeze.add(base_key)
    _unfreeze_keys_if_present(adapter, base_keys_to_unfreeze)
    if prog:
        try:
            if announced_total is not None:
                prog.done(ok=True, total=announced_total)
            else:
                prog.done(ok=True, total=len(idx))
        except Exception:
            pass
    _info("index_done", count=len(idx), movies=len(movies), episodes=len(episodes), per_page=cfg_per_page, max_pages=cfg_max_pages)
    _save_cache_doc(idx, remote_wm or cached_wm)
    return idx


# resolvers
_SHOW_PATH_CACHE: dict[str, str] = {}
_SEASON_EP_CACHE: dict[str, dict[int, dict[str, str]]] = {}
_EP_RESOLVE_CACHE: dict[str, dict[str, str]] = {}


def _stable_show_key(ids: Mapping[str, Any]) -> str:
    return json.dumps(
        {k: ids.get(k) for k in ("slug", "trakt", "tmdb", "imdb", "tvdb") if ids.get(k)},
        sort_keys=True,
    )


def _pick_show_path_id(ids: Mapping[str, Any]) -> str | None:
    slug = ids.get("slug")
    if slug:
        return str(slug)
    trakt_id = ids.get("trakt")
    if trakt_id:
        return str(trakt_id)
    return None


def _trakt_headers_for(adapter: Any) -> dict[str, str]:
    return build_headers(
        {
            "trakt": {
                "client_id": _cfg_get(adapter, "client_id"),
                "access_token": _cfg_get(adapter, "access_token"),
            }
        }
    )


def _resolve_show_path_id(
    adapter: Any,
    show_ids: Mapping[str, Any],
    *,
    timeout: float,
    retries: int,
) -> str | None:
    if not RESOLVE_ENABLE:
        return _pick_show_path_id(show_ids or {})
    skey = _stable_show_key(show_ids or {})
    if skey in _SHOW_PATH_CACHE:
        return _SHOW_PATH_CACHE[skey]
    path_id = _pick_show_path_id(show_ids or {})
    if path_id:
        _SHOW_PATH_CACHE[skey] = path_id
        return path_id
    sess = adapter.client.session
    headers = _trakt_headers_for(adapter)
    for k in ("tmdb", "imdb", "tvdb"):
        v = (show_ids or {}).get(k)
        if not v:
            continue
        url = f"{BASE}/search/{k}/{v}"
        r = request_with_retries(
            sess,
            "GET",
            url,
            headers=headers,
            params={"type": "show"},
            timeout=timeout,
            max_retries=retries,
        )
        if r.status_code == 200:
            arr = r.json() or []
            for hit in arr:
                show = hit.get("show") or {}
                ids = show.get("ids") or {}
                pid = _pick_show_path_id(ids)
                if pid:
                    _SHOW_PATH_CACHE[skey] = pid
                    return pid
    return None


def _resolve_episode_ids_via_trakt(
    adapter: Any,
    show_ids: Mapping[str, Any],
    season: Any,
    number: Any,
    *,
    timeout: float,
    retries: int,
) -> dict[str, str]:
    if not RESOLVE_ENABLE:
        return {}
    try:
        s = int(season)
        e = int(number)
    except Exception:
        return {}
    path_id = _resolve_show_path_id(adapter, show_ids, timeout=timeout, retries=retries)
    if not path_id:
        return {}
    season_key = f"{path_id}|S{s}"
    if season_key not in _SEASON_EP_CACHE:
        sess = adapter.client.session
        headers = _trakt_headers_for(adapter)
        url = f"{BASE}/shows/{path_id}/seasons/{s}"
        r = request_with_retries(
            sess,
            "GET",
            url,
            headers=headers,
            timeout=timeout,
            max_retries=retries,
        )
        epmap: dict[int, dict[str, str]] = {}
        if r.status_code == 200:
            rows = r.json() or []
            for row in rows:
                num = row.get("number")
                ids = {
                    ik: str(iv)
                    for ik, iv in (row.get("ids") or {}).items()
                    if ik in ("tmdb", "imdb", "tvdb", "trakt") and iv
                }
                if isinstance(num, int) and ids:
                    epmap[num] = ids
        _SEASON_EP_CACHE[season_key] = epmap
    ids = _SEASON_EP_CACHE.get(season_key, {}).get(e)
    if ids:
        return ids
    cache_key = json.dumps({"p": path_id, "s": s, "e": e}, sort_keys=True)
    if cache_key in _EP_RESOLVE_CACHE:
        return dict(_EP_RESOLVE_CACHE[cache_key])
    sess = adapter.client.session
    headers = _trakt_headers_for(adapter)
    url = f"{BASE}/shows/{path_id}/seasons/{s}/episodes/{e}"
    r = request_with_retries(
        sess,
        "GET",
        url,
        headers=headers,
        timeout=timeout,
        max_retries=retries,
    )
    if r.status_code == 200:
        d = r.json() or {}
        ids = {
            ik: str(iv)
            for ik, iv in (d.get("ids") or {}).items()
            if ik in ("tmdb", "imdb", "tvdb", "trakt") and iv
        }
        if ids:
            _EP_RESOLVE_CACHE[cache_key] = ids
            return ids
    return {}


# batching helpers
def _extract_show_ids_for_episode(it: Mapping[str, Any]) -> dict[str, Any]:
    show_ids = dict(it.get("show_ids") or {})
    if not show_ids and (it.get("season") is not None and it.get("episode") is not None):
        show_ids = dict(it.get("ids") or {})
    return {k: show_ids[k] for k in ("trakt", "slug", "tmdb", "imdb", "tvdb") if show_ids.get(k)}


def _batch_add(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    episodes_flat: list[dict[str, Any]] = []
    shows_map: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    accepted_keys: list[str] = []
    accepted_minimals: list[dict[str, Any]] = []

    def _show_key(ids: Mapping[str, Any]) -> str:
        return json.dumps(
            {k: ids[k] for k in ("trakt", "slug", "tmdb", "imdb", "tvdb") if k in ids and ids[k]},
            sort_keys=True,
        )

    for it in items or []:
        if _is_frozen(adapter, it):
            _dbg("skip_frozen", title=id_minimal(it).get("title"))
            continue
        when = _iso8601(it.get("watched_at"))
        if not when:
            unresolved.append({"item": id_minimal(it), "hint": "missing watched_at"})
            _freeze_item_if_enabled(adapter, it, action="add", reasons=["missing-watched_at"])
            continue
        kind = (pick_trakt_kind(it) or "movies").lower()
        if kind == "movies":
            ids = ids_for_trakt(it)
            if not ids:
                unresolved.append({"item": id_minimal(it), "hint": "missing ids"})
                _freeze_item_if_enabled(adapter, it, action="add", reasons=["missing-ids"])
                continue
            movies.append({"ids": ids, "watched_at": when})
            m_min = id_minimal({"type": "movie", "ids": ids})
            accepted_minimals.append(m_min)
            accepted_keys.append(key_of(m_min))
            continue
        season = it.get("season") or it.get("season_number")
        number = it.get("episode") or it.get("episode_number")
        show_ids = _extract_show_ids_for_episode(it)
        ids = ids_for_trakt(it)

        # Prefer show and season and episode payload
        if show_ids and season is not None and number is not None:
            skey = _show_key(show_ids)
            show_entry = shows_map.setdefault(skey, {"ids": show_ids, "seasons": {}})
            seasons = show_entry["seasons"]  # type: ignore[assignment]
            season_entry = seasons.setdefault(int(season), {"number": int(season), "episodes": []})
            season_entry["episodes"].append({"number": int(number), "watched_at": when})
            e_min = id_minimal({"type": "episode", "show_ids": show_ids, "season": int(season), "episode": int(number)})
            accepted_minimals.append(e_min)
            accepted_keys.append(key_of(e_min))
            continue

        if ids:
            episodes_flat.append({"ids": ids, "watched_at": when})
            e_min = id_minimal({"type": "episode", "ids": ids})
            accepted_minimals.append(e_min)
            accepted_keys.append(key_of(e_min))
            continue
        unresolved.append({"item": id_minimal(it), "hint": "episode scope or ids missing"})
        _freeze_item_if_enabled(adapter, it, action="add", reasons=["episode-scope-missing"])

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if episodes_flat:
        body["episodes"] = episodes_flat
    if shows_map:
        body["shows"] = [
            {"ids": v["ids"], "seasons": list(v["seasons"].values())}
            for v in shows_map.values()
        ]
    return body, unresolved, accepted_keys, accepted_minimals


def _batch_remove(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    episodes_flat: list[dict[str, Any]] = []
    shows_map: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    accepted_keys: list[str] = []
    accepted_minimals: list[dict[str, Any]] = []

    def _show_key(ids: Mapping[str, Any]) -> str:
        return json.dumps(
            {k: ids[k] for k in ("trakt", "slug", "tmdb", "imdb", "tvdb") if k in ids and ids[k]},
            sort_keys=True,
        )

    for it in items or []:
        if _is_frozen(adapter, it):
            _dbg("skip_frozen", title=id_minimal(it).get("title"))
            continue
        when = _iso8601(it.get("watched_at"))
        if not when:
            unresolved.append({"item": id_minimal(it), "hint": "missing watched_at"})
            _freeze_item_if_enabled(adapter, it, action="remove", reasons=["missing-watched_at"])
            continue
        kind = (pick_trakt_kind(it) or "movies").lower()
        if kind == "movies":
            ids = ids_for_trakt(it)
            if not ids:
                unresolved.append({"item": id_minimal(it), "hint": "missing ids"})
                _freeze_item_if_enabled(adapter, it, action="remove", reasons=["missing-ids"])
                continue
            movies.append({"ids": ids, "watched_at": when})
            m_min = id_minimal({"type": "movie", "ids": ids})
            accepted_minimals.append(m_min)
            accepted_keys.append(key_of(m_min))
            continue
        season = it.get("season") or it.get("season_number")
        number = it.get("episode") or it.get("episode_number")
        show_ids = _extract_show_ids_for_episode(it)
        ids = ids_for_trakt(it)

        # Prefer show and season and episode payload
        if show_ids and season is not None and number is not None:
            skey = _show_key(show_ids)
            show_entry = shows_map.setdefault(skey, {"ids": show_ids, "seasons": {}})
            seasons = show_entry["seasons"]  # type: ignore[assignment]
            season_entry = seasons.setdefault(int(season), {"number": int(season), "episodes": []})
            season_entry["episodes"].append({"number": int(number), "watched_at": when})
            e_min = id_minimal({"type": "episode", "show_ids": show_ids, "season": int(season), "episode": int(number)})
            accepted_minimals.append(e_min)
            accepted_keys.append(key_of(e_min))
            continue

        if ids:
            episodes_flat.append({"ids": ids, "watched_at": when})
            e_min = id_minimal({"type": "episode", "ids": ids})
            accepted_minimals.append(e_min)
            accepted_keys.append(key_of(e_min))
            continue
        unresolved.append({"item": id_minimal(it), "hint": "episode scope or ids missing"})
        _freeze_item_if_enabled(adapter, it, action="remove", reasons=["episode-scope-missing"])

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if episodes_flat:
        body["episodes"] = episodes_flat
    if shows_map:
        body["shows"] = [
            {"ids": v["ids"], "seasons": list(v["seasons"].values())}
            for v in shows_map.values()
        ]
    return body, unresolved, accepted_keys, accepted_minimals


def _history_body_to_collection(body: Mapping[str, Any], types: set[str]) -> dict[str, Any]:

    out: dict[str, Any] = {}
    if "movies" in types:
        seen_movies: set[str] = set()
        for m in body.get("movies") or []:
            ids = (m or {}).get("ids") or {}
            if not ids:
                continue
            k = json.dumps(ids, sort_keys=True)
            if k in seen_movies:
                continue
            seen_movies.add(k)
            out.setdefault("movies", []).append(
                {
                    "ids": ids,
                    "collected_at": m.get("watched_at") or _now_iso(),
                }
            )

    if "shows" in types:
        seen_eps: set[str] = set()
        for e in body.get("episodes") or []:
            ids = (e or {}).get("ids") or {}
            if not ids:
                continue
            k = json.dumps(ids, sort_keys=True)
            if k in seen_eps:
                continue
            seen_eps.add(k)
            out.setdefault("episodes", []).append(
                {
                    "ids": ids,
                    "collected_at": e.get("watched_at") or _now_iso(),
                }
            )

        shows = body.get("shows") or []
        if shows:
            coll_shows: list[dict[str, Any]] = []
            for sh in shows:
                ids = (sh or {}).get("ids") or {}
                seasons_in = (sh or {}).get("seasons") or []
                if not ids or not seasons_in:
                    continue
                seasons_out: list[dict[str, Any]] = []
                for s in seasons_in:
                    num = s.get("number")
                    eps = s.get("episodes") or []
                    if num is None or not eps:
                        continue
                    eps_out: list[dict[str, Any]] = []
                    for ep in eps:
                        n = ep.get("number")
                        if n is None:
                            continue
                        eps_out.append(
                            {
                                "number": int(n),
                                "collected_at": ep.get("watched_at") or _now_iso(),
                            }
                        )
                    if eps_out:
                        seasons_out.append({"number": int(num), "episodes": eps_out})
                if seasons_out:
                    coll_shows.append({"ids": ids, "seasons": seasons_out})
            if coll_shows:
                out["shows"] = coll_shows
    return out



def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    sess = adapter.client.session
    headers = build_headers(
        {
            "trakt": {
                "client_id": _cfg_get(adapter, "client_id"),
                "access_token": _cfg_get(adapter, "access_token"),
            }
        }
    )
    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))
    body, unresolved, accepted_keys, accepted_minimals = _batch_add(adapter, items)
    if not body:
        return 0, unresolved
    r = request_with_retries(
        sess,
        "POST",
        URL_ADD,
        headers=headers,
        json=body,
        timeout=timeout,
        max_retries=retries,
    )
    ok = 0
    if r.status_code in (200, 201):
        d = r.json() or {}
        added = d.get("added") or {}
        existing = d.get("existing") or {}
        ok = (
            int(added.get("movies") or 0)
            + int(added.get("episodes") or 0)
            + int(existing.get("movies") or 0)
            + int(existing.get("episodes") or 0)
        )
        nf = d.get("not_found") or {}
        for t in ("movies", "episodes"):
            for obj in nf.get(t) or []:
                m = id_minimal(
                    {"type": "movie" if t == "movies" else "episode", "ids": obj.get("ids") or {}}
                )
                unresolved.append({"item": m, "hint": "not_found"})
                _freeze_item_if_enabled(adapter, m, action="add", reasons=["not-found"])
        if _not_found_count(nf) > 0 and ok == 0:
            _bust_index_cache("write:add:not_found")
        if ok > 0:
            _unfreeze_keys_if_present(adapter, accepted_keys)
            _bust_index_cache("write:add")
            if _history_collection_enabled(adapter):
                coll_body = _history_body_to_collection(body, _history_collection_types(adapter))
                if coll_body:
                    try:
                        rc = request_with_retries(
                            sess,
                            "POST",
                            URL_COLL_ADD,
                            headers=headers,
                            json=coll_body,
                            timeout=timeout,
                            max_retries=retries,
                        )
                        if rc.status_code == 420:
                            _warn("collection_limit", status=420)
                            _record_limit_error("collection")
                        elif rc.status_code not in (200, 201):
                            _warn("collection_add_failed", status=rc.status_code, body=((rc.text or "")[:200]))
                    except Exception as e:
                        _warn("collection_add_exception", error=str(e))
        elif not unresolved:
            _warn("write_noop", action="add")
    elif r.status_code == 420:
        _warn("write_limit", action="add", status=420)
        _record_limit_error("history")
        for m in accepted_minimals:
            unresolved.append({"item": m, "hint": "trakt_limit"})
        return 0, unresolved
    else:
        _warn("write_failed", action="add", status=r.status_code, body=((r.text or "")[:200]))
        for m in accepted_minimals:
            _freeze_item_if_enabled(adapter, m, action="add", reasons=[f"http:{r.status_code}"])
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    sess = adapter.client.session
    headers = build_headers(
        {
            "trakt": {
                "client_id": _cfg_get(adapter, "client_id"),
                "access_token": _cfg_get(adapter, "access_token"),
            }
        }
    )
    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))
    body, unresolved, accepted_keys, accepted_minimals = _batch_remove(adapter, items)
    if not body:
        return 0, unresolved
    r = request_with_retries(
        sess,
        "POST",
        URL_REMOVE,
        headers=headers,
        json=body,
        timeout=timeout,
        max_retries=retries,
    )
    ok = 0
    if r.status_code in (200, 201):
        d = r.json() or {}
        deleted = d.get("deleted") or d.get("removed") or {}
        ok = int(deleted.get("movies") or 0) + int(deleted.get("episodes") or 0)
        nf = d.get("not_found") or {}
        for t in ("movies", "episodes"):
            for obj in nf.get(t) or []:
                m = id_minimal(
                    {"type": "movie" if t == "movies" else "episode", "ids": obj.get("ids") or {}}
                )
                unresolved.append({"item": m, "hint": "not_found"})
                _freeze_item_if_enabled(adapter, m, action="remove", reasons=["not-found"])
        if ok > 0:
            _unfreeze_keys_if_present(adapter, accepted_keys)
            _bust_index_cache("write:remove")
    else:
        _warn("write_failed", action="remove", status=r.status_code, body=((r.text or "")[:200]))
        for m in accepted_minimals:
            _freeze_item_if_enabled(adapter, m, action="remove", reasons=[f"http:{r.status_code}"])
    return ok, unresolved
