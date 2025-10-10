# /providers/sync/trakt/_history.py
from __future__ import annotations
import os, time, json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Callable

from ._common import (
    build_headers,
    key_of,                # canonical key (no timestamp)
    ids_for_trakt,         # minimal → {"trakt"|"imdb"|"tmdb"|"tvdb": id}
    pick_trakt_kind,       # "movies" | "episodes" (never bare "shows" for history)
)
from .._mod_common import request_with_retries  # emits {"event":"api:hit", ...}

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

# ── tiny log/helpers ──────────────────────────────────────────────────────────

def _log(msg: str):
    if os.getenv("CW_DEBUG") or os.getenv("CW_TRAKT_DEBUG"):
        print(f"[TRAKT:history] {msg}")

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _iso8601(v: Any) -> Optional[str]:
    # Normalize watched_at to ISO 8601 Z. Accept epoch sec/ms or ISO-like.
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

# ── cfg helpers (support adapter.cfg attr or adapter.config dict) ─────────────

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
    # Gate all freeze/unfreeze on config: trakt.history_unresolved
    v = _cfg_get(adapter, "history_unresolved", False)
    try:
        return bool(v)
    except Exception:
        return False

# ── unresolved (freeze) ───────────────────────────────────────────────────────

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

# ── pagination preflight (totals for progress bar) ────────────────────────────

def _hdr_int(headers: Mapping[str, Any], name: str) -> Optional[int]:
    try:
        for k, v in (headers or {}).items():
            if str(k).lower() == name.lower():
                return int(str(v).strip())
    except Exception:
        return None
    return None

def _preflight_total(sess, headers, url: str, *, per_page: int, timeout: float, max_retries: int, max_pages: Optional[int]) -> Optional[int]:
    # Read pagination headers to announce a fixed total (supports your sync bar).
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

# ── history fetch (movies + episodes) ─────────────────────────────────────────

def _fetch_history(
    sess, headers, url: str, *,
    per_page: int, max_pages: int, timeout: float, max_retries: int,
    bump: Optional[Callable[[int], None]] = None,
) -> List[Dict[str, Any]]:
    # Fetch pages; tick progress per page via 'bump(count)' if provided.
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

# ── index (event-based) with progress-helper ──────────────────────────────────

def build_index(adapter, *, per_page: int = 100, max_pages: int = 100000) -> Dict[str, Dict[str, Any]]:
    """
    Present state as {event_key: minimal}.
    event_key = canonical_key(minimal) + "@" + epoch(watched_at).
    Emits progress with a fixed total (movies+episodes), based on pagination headers.
    """
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
    cfg_per_page = max(1, min(100, cfg_per_page))  # Trakt cap = 100
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

# ── batching helpers  ──────────────────────────────────

def _batch_add(adapter, items: Iterable[Mapping[str, Any]]):
    movies: List[Dict[str, Any]] = []
    episodes_flat: List[Dict[str, Any]] = []
    shows_map: Dict[str, Dict[str, Any]] = {}  # key by stable show-ids JSON

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
        ids = ids_for_trakt(it)
        if ids:
            episodes_flat.append({"ids": ids, "watched_at": when})
            e_min = id_minimal({"type": "episode", "ids": ids})
            accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
            continue

        # optional nested shows if scope present (no resolver, no extra GETs)
        show_ids = dict(it.get("show_ids") or {})
        season = it.get("season") or it.get("season_number")
        number = it.get("episode") or it.get("episode_number")
        if show_ids and season is not None and number is not None:
            skey = _show_key(show_ids)
            show_entry = shows_map.setdefault(skey, {"ids": {k: show_ids[k] for k in ("trakt","slug","imdb","tmdb","tvdb") if show_ids.get(k)}, "seasons": {}})
            seasons = show_entry["seasons"]  # type: ignore[assignment]
            season_entry = seasons.setdefault(int(season), {"number": int(season), "episodes": []})
            season_entry["episodes"].append({"number": int(number), "watched_at": when})
            e_min = id_minimal({"type": "episode", "show_ids": show_ids, "season": int(season), "episode": int(number)})
            accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
        else:
            unresolved.append({"item": id_minimal(it), "hint": "episode scope or ids missing"})
            _freeze_item_if_enabled(adapter, it, action="add", reasons=["episode-scope-missing"])
            continue

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
    """
    Remove specific events (require watched_at). Same structure as add.
    """
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

        ids = ids_for_trakt(it)
        if ids:
            episodes_flat.append({"ids": ids, "watched_at": when})
            e_min = id_minimal({"type": "episode", "ids": ids})
            accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
            continue

        show_ids = dict(it.get("show_ids") or {})
        season = it.get("season") or it.get("season_number")
        number = it.get("episode") or it.get("episode_number")
        if show_ids and season is not None and number is not None:
            skey = _show_key(show_ids)
            show_entry = shows_map.setdefault(skey, {"ids": {k: show_ids[k] for k in ("trakt","slug","imdb","tmdb","tvdb") if show_ids.get(k)}, "seasons": {}})
            seasons = show_entry["seasons"]  # type: ignore[assignment]
            season_entry = seasons.setdefault(int(season), {"number": int(season), "episodes": []})
            season_entry["episodes"].append({"number": int(number), "watched_at": when})
            e_min = id_minimal({"type": "episode", "show_ids": show_ids, "season": int(season), "episode": int(number)})
            accepted_minimals.append(e_min); accepted_keys.append(key_of(e_min))
        else:
            unresolved.append({"item": id_minimal(it), "hint": "episode scope or ids missing"})
            _freeze_item_if_enabled(adapter, it, action="remove", reasons=["episode-scope-missing"])
            continue

    body: Dict[str, Any] = {}
    if movies: body["movies"] = movies
    if episodes_flat: body["episodes"] = episodes_flat
    if shows_map:
        body["shows"] = [
            {"ids": v["ids"], "seasons": list(v["seasons"].values())}
            for v in shows_map.values()
        ]
    return body, unresolved, accepted_keys, accepted_minimals

# ── public API (writes) ───────────────────────────────────────────────────────

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    # Add history events. Multiple plays = multiple entries (distinct watched_at).
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
    # Remove specific history events (require watched_at).
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
