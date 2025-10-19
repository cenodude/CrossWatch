# /providers/sync/trakt/_history.py
from __future__ import annotations
import os, time, json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Callable

from ._common import (
    build_headers,
    key_of,
    ids_for_trakt,
    pick_trakt_kind,
)
from .._mod_common import request_with_retries

try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

BASE = "https://api.trakt.tv"
URL_HIST_MOV = f"{BASE}/sync/history/movies"
URL_HIST_EPI = f"{BASE}/sync/history/episodes"
URL_ADD      = f"{BASE}/sync/history"
URL_REMOVE   = f"{BASE}/sync/history/remove"

UNRESOLVED_PATH = "/config/.cw_state/trakt_history.unresolved.json"

def _log(msg: str):
    if os.getenv("CW_DEBUG") or os.getenv("CW_TRAKT_DEBUG"):
        print(f"[TRAKT:history] {msg}")

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _iso8601(v: Any) -> Optional[str]:
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    if s.isdigit() and len(s) >= 13:
        try: return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(int(s)//1000))
        except Exception: return None
    if s.isdigit():
        try: return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(int(s)))
        except Exception: return None
    if "T" in s and s.endswith("Z"): return s
    if "T" in s: return s + "Z"
    return None

def _as_epoch(iso: str) -> Optional[int]:
    try:
        s = iso.replace("Z", "+00:00")
        from datetime import datetime
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None

def _cfg(adapter):
    return getattr(adapter, "cfg", None) or getattr(adapter, "config", {})

def _cfg_get(adapter, key: str, default: Any = None) -> Any:
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

def _cfg_num(adapter, key: str, default: Any, cast=int):
    try:
        v = _cfg_get(adapter, key, default)
        return cast(v)
    except Exception:
        return cast(default)

def _freeze_enabled(adapter) -> bool:
    v = _cfg_get(adapter, "history_unresolved", False)
    try:
        return bool(v)
    except Exception:
        return False

def _history_number_fallback_enabled(adapter) -> bool:
    return bool(_cfg_get(adapter, "history_number_fallback", False))

def _load_unresolved() -> Dict[str, Any]:
    try: return json.loads(Path(UNRESOLVED_PATH).read_text("utf-8"))
    except Exception: return {}

def _save_unresolved(data: Mapping[str, Any]) -> None:
    try:
        p = Path(UNRESOLVED_PATH); p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
    except Exception as e:
        _log(f"unresolved.save failed: {e}")

def _freeze_item_if_enabled(adapter, item: Mapping[str, Any], *, action: str, reasons: List[str]) -> None:
    if not _freeze_enabled(adapter): return
    m = id_minimal(item); k = key_of(m)
    data = _load_unresolved()
    entry = data.get(k) or {"feature": "history", "action": action, "first_seen": _now_iso(), "attempts": 0}
    entry.update({"item": m, "last_attempt": _now_iso()})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    entry["reasons"] = sorted(rset)
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[k] = entry
    _save_unresolved(data)

def _unfreeze_keys_if_present(adapter, keys: Iterable[str]) -> None:
    if not _freeze_enabled(adapter): return
    data = _load_unresolved(); changed = False
    for k in list(keys or []):
        if k in data: del data[k]; changed = True
    if changed: _save_unresolved(data)

def _is_frozen(adapter, item: Mapping[str, Any]) -> bool:
    if not _freeze_enabled(adapter): return False
    return key_of(id_minimal(item)) in _load_unresolved()

def _hdr_int(headers: Mapping[str, Any], name: str) -> Optional[int]:
    try:
        for k, v in (headers or {}).items():
            if str(k).lower() == name.lower():
                return int(str(v).strip())
    except Exception:
        return None
    return None

def _preflight_total(sess, headers, url: str, *, per_page: int, timeout: float, max_retries: int, max_pages: Optional[int]) -> Optional[int]:
    try:
        r = request_with_retries(
            sess, "GET", url, headers=headers,
            params={"page": 1, "limit": per_page},
            timeout=timeout, max_retries=max_retries
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
    sess, headers, url: str, *,
    per_page: int, max_pages: int, timeout: float, max_retries: int,
    bump: Optional[Callable[[int], None]] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page = 1
    total_pages: Optional[int] = None

    while True:
        r = request_with_retries(
            sess, "GET", url, headers=headers,
            params={"page": page, "limit": per_page},
            timeout=timeout, max_retries=max_retries
        )
        if r.status_code != 200:
            _log(f"GET {url} p{page} -> {r.status_code}")
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
            if not w: continue
            typ = (row.get("type") or "").lower()

            if typ == "movie" and isinstance(row.get("movie"), dict):
                mv = row["movie"]
                m = id_minimal({"type": "movie", "ids": mv.get("ids") or {}, "title": mv.get("title"), "year": mv.get("year")})
                m["watched_at"] = w
                out.append(m); added += 1

            elif typ == "episode" and isinstance(row.get("episode"), dict):
                ep = row["episode"]; show = row.get("show") or {}
                m = id_minimal({
                    "type": "episode",
                    "ids": ep.get("ids") or {},
                    "show_ids": (show.get("ids") or {}),
                    "season": ep.get("season"),
                    "episode": ep.get("number"),
                    "series_title": show.get("title"),
                })
                m["watched_at"] = w
                out.append(m); added += 1

        if bump and added:
            try: bump(added)
            except Exception: pass

        page += 1
        if total_pages is not None and page > total_pages: break
        if total_pages is None and len(rows) < per_page: break
        if max_pages and page > max_pages:
            _log(f"stopping early at safety cap: max_pages={max_pages}")
            break

    return out

def build_index(adapter, *, per_page: int = 100, max_pages: int = 100000) -> Dict[str, Dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history") if callable(prog_mk) else None

    sess = adapter.client.session
    headers = build_headers({
        "trakt": {
            "client_id": _cfg_get(adapter, "client_id"),
            "access_token": _cfg_get(adapter, "access_token"),
        }
    })

    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))

    cfg_per_page = int(_cfg_num(adapter, "history_per_page", per_page, int))
    cfg_per_page = max(1, min(100, cfg_per_page))
    cfg_max_pages = int(_cfg_num(adapter, "history_max_pages", max_pages, int))
    if cfg_max_pages <= 0: cfg_max_pages = max_pages

    total_mov = _preflight_total(sess, headers, URL_HIST_MOV, per_page=cfg_per_page, timeout=timeout, max_retries=retries, max_pages=cfg_max_pages)
    total_epi = _preflight_total(sess, headers, URL_HIST_EPI, per_page=cfg_per_page, timeout=timeout, max_retries=retries, max_pages=cfg_max_pages)

    announced_total: Optional[int] = None
    if total_mov is not None and total_epi is not None:
        announced_total = int(total_mov) + int(total_epi)
        if prog:
            try: prog.tick(0, total=announced_total, force=True)
            except Exception: pass

    done = 0
    def bump(n: int):
        nonlocal done
        done += int(n or 0)
        if prog:
            try:
                if announced_total is not None: prog.tick(done, total=announced_total)
                else:                             prog.tick(done)
            except Exception:
                pass

    movies   = _fetch_history(sess, headers, URL_HIST_MOV, per_page=cfg_per_page, max_pages=cfg_max_pages, timeout=timeout, max_retries=retries, bump=bump)
    episodes = _fetch_history(sess, headers, URL_HIST_EPI, per_page=cfg_per_page, max_pages=cfg_max_pages, timeout=timeout, max_retries=retries, bump=bump)

    idx: Dict[str, Dict[str, Any]] = {}
    base_keys_to_unfreeze: set[str] = set()
    for m in movies + episodes:
        w = _iso8601(m.get("watched_at"))
        ts = _as_epoch(w) if w else None
        if not ts: continue

        if (m.get("type") == "episode"
            and isinstance(m.get("show_ids"), dict)
            and m.get("season") is not None
            and m.get("episode") is not None):
            base_key = canonical_key(id_minimal({
                "type": "episode",
                "show_ids": m["show_ids"],
                "season": m["season"],
                "episode": m["episode"],
            }))
        else:
            base_key = canonical_key(id_minimal(m))

        ek = f"{base_key}@{ts}"
        idx[ek] = m
        base_keys_to_unfreeze.add(base_key)

    _unfreeze_keys_if_present(adapter, base_keys_to_unfreeze)

    if prog:
        try:
            if announced_total is not None: prog.done(ok=True, total=announced_total)
            else:                             prog.done(ok=True, total=len(idx))
        except Exception:
            pass

    _log(f"index size: {len(idx)} (movies={len(movies)}, episodes={len(episodes)}; per_page={cfg_per_page}, max_pages={cfg_max_pages})")
    return idx

# ── resolvers ─────────────────────────────────────────────────────────────────

_SHOW_PATH_CACHE: Dict[str, str] = {}
_SEASON_EP_CACHE: Dict[str, Dict[int, Dict[str, str]]] = {}
_EP_RESOLVE_CACHE: Dict[str, Dict[str, str]] = {}

def _stable_show_key(ids: Mapping[str, Any]) -> str:
    return json.dumps({k: ids.get(k) for k in ("slug","trakt","imdb","tvdb","tmdb") if ids.get(k)}, sort_keys=True)

def _pick_show_path_id(ids: Mapping[str, Any]) -> Optional[str]:
    slug = ids.get("slug")
    if slug: return str(slug)
    trakt_id = ids.get("trakt")
    if trakt_id: return str(trakt_id)
    return None

def _trakt_headers_for(adapter) -> Dict[str, str]:
    return build_headers({
        "trakt": {
            "client_id": _cfg_get(adapter, "client_id"),
            "access_token": _cfg_get(adapter, "access_token"),
        }
    })

def _resolve_show_path_id(adapter, show_ids: Mapping[str, Any], *, timeout: float, retries: int) -> Optional[str]:
    skey = _stable_show_key(show_ids or {})
    if skey in _SHOW_PATH_CACHE:
        return _SHOW_PATH_CACHE[skey]

    path_id = _pick_show_path_id(show_ids or {})
    if path_id:
        _SHOW_PATH_CACHE[skey] = path_id
        return path_id

    sess = adapter.client.session
    headers = _trakt_headers_for(adapter)

    for k in ("imdb", "tvdb", "tmdb"):
        v = (show_ids or {}).get(k)
        if not v: continue
        url = f"{BASE}/search/{k}/{v}"
        r = request_with_retries(sess, "GET", url, headers=headers, params={"type": "show"}, timeout=timeout, max_retries=retries)
        if r.status_code == 200:
            arr = r.json() or []
            for hit in arr:
                show = hit.get("show") or {}
                ids  = show.get("ids") or {}
                pid = _pick_show_path_id(ids)
                if pid:
                    _SHOW_PATH_CACHE[skey] = pid
                    return pid
    return None

def _resolve_episode_ids_via_trakt(adapter, show_ids: Mapping[str, Any], season: Any, number: Any, *, timeout: float, retries: int) -> Dict[str, str]:
    try:
        s = int(season); e = int(number)
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
        r = request_with_retries(sess, "GET", url, headers=headers, timeout=timeout, max_retries=retries)
        epmap: Dict[int, Dict[str, str]] = {}
        if r.status_code == 200:
            rows = r.json() or []
            for row in rows:
                num = row.get("number")
                ids = {ik: str(iv) for ik, iv in (row.get("ids") or {}).items() if ik in ("imdb","tvdb","trakt","tmdb") and iv}
                if isinstance(num, int) and ids:
                    epmap[num] = ids
        _SEASON_EP_CACHE[season_key] = epmap

    ids = _SEASON_EP_CACHE.get(season_key, {}).get(e)
    if ids: return ids

    cache_key = json.dumps({"p": path_id, "s": s, "e": e}, sort_keys=True)
    if cache_key in _EP_RESOLVE_CACHE:
        return dict(_EP_RESOLVE_CACHE[cache_key])

    sess = adapter.client.session
    headers = _trakt_headers_for(adapter)
    url = f"{BASE}/shows/{path_id}/seasons/{s}/episodes/{e}"
    r = request_with_retries(sess, "GET", url, headers=headers, timeout=timeout, max_retries=retries)
    if r.status_code == 200:
        d = r.json() or {}
        ids = {ik: str(iv) for ik, iv in (d.get("ids") or {}).items() if ik in ("imdb","tvdb","trakt","tmdb") and iv}
        if ids:
            _EP_RESOLVE_CACHE[cache_key] = ids
            return ids
    return {}

# ── batching helpers  ─────────────────────────────────────────────────────────

def _extract_show_ids_for_episode(it: Mapping[str, Any]) -> Dict[str, Any]:
    # Prefer explicit show_ids; otherwise treat item['ids'] (series ids) as show_ids.
    show_ids = dict(it.get("show_ids") or {})
    if not show_ids and (it.get("season") is not None and it.get("episode") is not None):
        show_ids = dict(it.get("ids") or {})
    return {k: show_ids[k] for k in ("trakt","slug","imdb","tmdb","tvdb") if show_ids.get(k)}

def _batch_add(adapter, items: Iterable[Mapping[str, Any]]):
    movies: List[Dict[str, Any]] = []
    episodes_flat: List[Dict[str, Any]] = []
    shows_map: Dict[str, Dict[str, Any]] = {}

    unresolved: List[Dict[str, Any]] = []
    accepted_keys: List[str] = []
    accepted_minimals: List[Dict[str, Any]] = []

    def _show_key(ids: Mapping[str, Any]) -> str:
        return json.dumps({k: ids[k] for k in ("trakt","slug","imdb","tmdb","tvdb") if k in ids and ids[k]}, sort_keys=True)

    for it in items or []:
        if _is_frozen(adapter, it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
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
            accepted_minimals.append(m_min); accepted_keys.append(key_of(m_min))
            continue

        # episodes
        season = it.get("season") or it.get("season_number")
        number = it.get("episode") or it.get("episode_number")
        show_ids = _extract_show_ids_for_episode(it)

        # 1) Try genuine episode-level ids (ids_for_trakt returns {} in the SIMKL case)
        ids = ids_for_trakt(it)
        if ids:
            episodes_flat.append({"ids": ids, "watched_at": when})
            e_min = id_minimal({"type": "episode", "ids": ids})
            accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
            continue

        # 2) Resolve using show ids + S/E
        if show_ids and season is not None and number is not None:
            if _history_number_fallback_enabled(adapter):
                skey = _show_key(show_ids)
                show_entry = shows_map.setdefault(skey, {"ids": show_ids, "seasons": {}})
                seasons = show_entry["seasons"]  # type: ignore[assignment]
                season_entry = seasons.setdefault(int(season), {"number": int(season), "episodes": []})
                season_entry["episodes"].append({"number": int(number), "watched_at": when})
                e_min = id_minimal({"type": "episode", "show_ids": show_ids, "season": int(season), "episode": int(number)})
                accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
            else:
                timeout = float(_cfg_num(adapter, "timeout", 10, float))
                retries = int(_cfg_num(adapter, "max_retries", 3, int))
                rids = _resolve_episode_ids_via_trakt(adapter, show_ids, season, number, timeout=timeout, retries=retries)
                if rids:
                    episodes_flat.append({"ids": rids, "watched_at": when})
                    e_min = id_minimal({"type": "episode", "ids": rids})
                    accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
                else:
                    unresolved.append({"item": id_minimal(it), "hint": "episode ids missing; resolver failed"})
                    _freeze_item_if_enabled(adapter, it, action="add", reasons=["episode-ids-missing"])
            continue

        unresolved.append({"item": id_minimal(it), "hint": "episode scope or ids missing"})
        _freeze_item_if_enabled(adapter, it, action="add", reasons=["episode-scope-missing"])

    body: Dict[str, Any] = {}
    if movies: body["movies"] = movies
    if episodes_flat: body["episodes"] = episodes_flat
    if shows_map:
        body["shows"] = [
            {"ids": v["ids"], "seasons": list(v["seasons"].values())}
            for v in shows_map.values()
        ]
    return body, unresolved, accepted_keys, accepted_minimals

def _batch_remove(adapter, items: Iterable[Mapping[str, Any]]):
    movies: List[Dict[str, Any]] = []
    episodes_flat: List[Dict[str, Any]] = []
    shows_map: Dict[str, Dict[str, Any]] = {}

    unresolved: List[Dict[str, Any]] = []
    accepted_keys: List[str] = []
    accepted_minimals: List[Dict[str, Any]] = []

    def _show_key(ids: Mapping[str, Any]) -> str:
        return json.dumps({k: ids[k] for k in ("trakt","slug","imdb","tmdb","tvdb") if k in ids and ids[k]}, sort_keys=True)

    for it in items or []:
        if _is_frozen(adapter, it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
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
            accepted_minimals.append(m_min); accepted_keys.append(key_of(m_min))
            continue

        season = it.get("season") or it.get("season_number")
        number = it.get("episode") or it.get("episode_number")
        show_ids = _extract_show_ids_for_episode(it)

        ids = ids_for_trakt(it)
        if ids:
            episodes_flat.append({"ids": ids, "watched_at": when})
            e_min = id_minimal({"type": "episode", "ids": ids})
            accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
            continue

        if show_ids and season is not None and number is not None:
            if _history_number_fallback_enabled(adapter):
                skey = _show_key(show_ids)
                show_entry = shows_map.setdefault(skey, {"ids": show_ids, "seasons": {}})
                seasons = show_entry["seasons"]  # type: ignore[assignment]
                season_entry = seasons.setdefault(int(season), {"number": int(season), "episodes": []})
                season_entry["episodes"].append({"number": int(number), "watched_at": when})
                e_min = id_minimal({"type": "episode", "show_ids": show_ids, "season": int(season), "episode": int(number)})
                accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
            else:
                timeout = float(_cfg_num(adapter, "timeout", 10, float))
                retries = int(_cfg_num(adapter, "max_retries", 3, int))
                rids = _resolve_episode_ids_via_trakt(adapter, show_ids, season, number, timeout=timeout, retries=retries)
                if rids:
                    episodes_flat.append({"ids": rids, "watched_at": when})
                    e_min = id_minimal({"type": "episode", "ids": rids})
                    accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
                else:
                    unresolved.append({"item": id_minimal(it), "hint": "episode ids missing; resolver failed"})
                    _freeze_item_if_enabled(adapter, it, action="remove", reasons=["episode-ids-missing"])
            continue

        unresolved.append({"item": id_minimal(it), "hint": "episode scope or ids missing"})
        _freeze_item_if_enabled(adapter, it, action="remove", reasons=["episode-scope-missing"])

    body: Dict[str, Any] = {}
    if movies: body["movies"] = movies
    if episodes_flat: body["episodes"] = episodes_flat
    if shows_map:
        body["shows"] = [
            {"ids": v["ids"], "seasons": list(v["seasons"].values())}
            for v in shows_map.values()
        ]
    return body, unresolved, accepted_keys, accepted_minimals

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    sess = adapter.client.session
    headers = build_headers({
        "trakt": {
            "client_id": _cfg_get(adapter, "client_id"),
            "access_token": _cfg_get(adapter, "access_token"),
        }
    })
    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))

    body, unresolved, accepted_keys, accepted_minimals = _batch_add(adapter, items)
    if not body: return 0, unresolved

    r = request_with_retries(sess, "POST", URL_ADD, headers=headers, json=body, timeout=timeout, max_retries=retries)
    ok = 0
    if r.status_code in (200, 201):
        d = r.json() or {}
        added    = d.get("added")    or {}
        existing = d.get("existing") or {}
        ok = int(added.get("movies") or 0) + int(added.get("episodes") or 0) \
           + int(existing.get("movies") or 0) + int(existing.get("episodes") or 0)
        nf = d.get("not_found") or {}
        for t in ("movies", "episodes"):
            for obj in (nf.get(t) or []):
                m = id_minimal({"type": "movie" if t == "movies" else "episode", "ids": obj.get("ids") or {}})
                unresolved.append({"item": m, "hint": "not_found"})
                _freeze_item_if_enabled(adapter, m, action="add", reasons=["not-found"])
        if ok > 0:
            _unfreeze_keys_if_present(adapter, accepted_keys)
        elif not unresolved:
            _log("ADD returned 200 but nothing added/existing")
    else:
        _log(f"ADD failed {r.status_code}: {(r.text or '')[:200]}")
        for m in accepted_minimals:
            _freeze_item_if_enabled(adapter, m, action="add", reasons=[f"http:{r.status_code}"])
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    sess = adapter.client.session
    headers = build_headers({
        "trakt": {
            "client_id": _cfg_get(adapter, "client_id"),
            "access_token": _cfg_get(adapter, "access_token"),
        }
    })
    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))

    body, unresolved, accepted_keys, accepted_minimals = _batch_remove(adapter, items)
    if not body: return 0, unresolved

    r = request_with_retries(sess, "POST", URL_REMOVE, headers=headers, json=body, timeout=timeout, max_retries=retries)
    ok = 0
    if r.status_code in (200, 201):
        d = r.json() or {}
        deleted = d.get("deleted") or d.get("removed") or {}
        ok = int(deleted.get("movies") or 0) + int(deleted.get("episodes") or 0)
        nf = d.get("not_found") or {}
        for t in ("movies", "episodes"):
            for obj in (nf.get(t) or []):
                m = id_minimal({"type": "movie" if t == "movies" else "episode", "ids": obj.get("ids") or {}})
                unresolved.append({"item": m, "hint": "not_found"})
                _freeze_item_if_enabled(adapter, m, action="remove", reasons=["not-found"])
        if ok > 0:
            _unfreeze_keys_if_present(adapter, accepted_keys)
    else:
        _log(f"REMOVE failed {r.status_code}: {(r.text or '')[:200]}")
        for m in accepted_minimals:
            _freeze_item_if_enabled(adapter, m, action="remove", reasons=[f"http:{r.status_code}"])
    return ok, unresolved
