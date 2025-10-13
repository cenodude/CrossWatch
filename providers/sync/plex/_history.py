# /providers/sync/plex/_history.py
from __future__ import annotations
import os, json, re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

from ._utils import resolve_user_scope, patch_history_with_account_id
from ._common import normalize as plex_normalize
try:
    from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from
except Exception:
    from _id_map import canonical_key, minimal as id_minimal, ids_from  # type: ignore

UNRESOLVED_PATH = "/config/.cw_state/plex_history.unresolved.json"

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:history] {msg}")

# ── time helpers ──────────────────────────────────────────────────────────────

def _as_epoch(v: Any) -> Optional[int]:
    if v is None: return None
    if isinstance(v, (int, float)): return int(v)
    if isinstance(v, datetime):
        if v.tzinfo is None: v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp())
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try:
                n = int(s)
                return n // 1000 if len(s) >= 13 else n
            except Exception:
                return None
        try:
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return None
    return None

def _iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")

# ── config helpers ────────────────────────────────────────────────────────────

def _plex_cfg(adapter) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    return cfg.get("plex", {}) if isinstance(cfg, dict) else {}

def _plex_cfg_get(adapter, key: str, default: Any = None) -> Any:
    c = _plex_cfg(adapter)
    v = c.get(key, default) if isinstance(c, dict) else default
    return default if v is None else v

def _get_workers(adapter, cfg_key: str, env_key: str, default: int) -> int:
    try:
        n = int(_plex_cfg_get(adapter, cfg_key, 0) or 0)
    except Exception:
        n = 0
    if n <= 0:
        try:
            n = int(os.environ.get(env_key, str(default)))
        except Exception:
            n = default
    return max(1, min(n, 64))

def _allowed_history_sec_ids(adapter) -> Set[str]:
    """Allowed section ids; empty set = allow all."""
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        arr = ((plex.get("history") or {}).get("libraries") or [])
        return {str(int(x)) for x in arr if str(x).strip()}
    except Exception:
        return set()

def _row_section_id(h) -> Optional[str]:
    for a in ("librarySectionID","sectionID","librarySectionId","sectionId"):
        v = getattr(h, a, None)
        if v is not None:
            try: return str(int(v))
            except Exception: pass
    sk = getattr(h, "sectionKey", None) or getattr(h, "librarySectionKey", None)
    if sk:
        m = re.search(r"/library/sections/(\d+)", str(sk))
        if m: return m.group(1)
    return None

# ── unresolved store ──────────────────────────────────────────────────────────

def _load_unresolved() -> Dict[str, Any]:
    try:
        with open(UNRESOLVED_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _save_unresolved(data: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(UNRESOLVED_PATH), exist_ok=True)
        tmp = UNRESOLVED_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, UNRESOLVED_PATH)
    except Exception as e:
        _log(f"unresolved.save failed: {e}")

def _event_key(it: Mapping[str, Any]) -> str:
    m = id_minimal(it)
    k = canonical_key(m)
    ts = _as_epoch(it.get("watched_at"))
    return f"{k}@{ts}" if ts else k

def _freeze_item(it: Mapping[str, Any], *, action: str, reasons: List[str]) -> None:
    now_iso = _iso(int(datetime.now(timezone.utc).timestamp()))
    key = _event_key(it)
    data = _load_unresolved()
    entry = data.get(key) or {"feature": "history", "action": action, "first_seen": now_iso, "attempts": 0}
    entry.update({"item": id_minimal(it), "watched_at": it.get("watched_at"), "last_attempt": now_iso})
    entry["reasons"] = sorted(set(entry.get("reasons", [])) | set(reasons or []))
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)

def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved(); changed = False
    for k in list(keys or []):
        if k in data:
            del data[k]; changed = True
    if changed: _save_unresolved(data)

def _is_frozen(it: Mapping[str, Any]) -> bool:
    return _event_key(it) in _load_unresolved()

# ── snapshot filters (provider-level) ─────────────────────────────────────────

def _has_external_ids(minimal: Mapping[str, Any]) -> bool:
    ids = minimal.get("ids") or {}
    return bool(ids.get("imdb") or ids.get("tmdb") or ids.get("tvdb") or ids.get("trakt"))

def _guid_from_minimal(minimal: Mapping[str, Any]) -> str:
    ids = minimal.get("ids") or {}
    g = minimal.get("guid") or ids.get("guid") or ids.get("plex_guid")
    return str(g).lower() if g else ""

def _keep_in_snapshot(adapter, minimal: Mapping[str, Any]) -> bool:
    ignore_local = bool(_plex_cfg_get(adapter, "history_ignore_local_guid", False))
    prefixes = _plex_cfg_get(adapter, "history_ignore_guid_prefixes", ["local://"]) or []
    require_ext = bool(_plex_cfg_get(adapter, "history_require_external_ids", False))
    if require_ext and not _has_external_ids(minimal): return False
    if ignore_local:
        guid = _guid_from_minimal(minimal)
        if guid and any(guid.startswith(p.lower()) for p in prefixes): return False
    return True

# ── index (present-state) ─────────────────────────────────────────────────────

_FETCH_CACHE: Dict[str, Dict[str, Any]] = {}

def _fetch_one(srv, rk: str) -> Optional[Dict[str, Any]]:
    try:
        obj = srv.fetchItem(int(rk))
        if not obj: return None
        m = plex_normalize(obj) or {}
        return m if m else None
    except Exception:
        return None

def build_index(adapter, since: Optional[int] = None, limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        _log("no PMS bound (account-only) → empty history index")
        return {}

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history") if callable(prog_mk) else None

    def _int_or_zero(v):
        try: return int(v or 0)
        except Exception: return 0

    acct_id = _int_or_zero(getattr(getattr(adapter, "client", None), "user_account_id", None)) or _int_or_zero(_plex_cfg_get(adapter, "account_id", 0))
    uname = str(_plex_cfg_get(adapter, "username", "") or getattr(getattr(adapter, "client", None), "user_username", "") or "").strip().lower()
    allow = _allowed_history_sec_ids(adapter)

    rows = []
    try:
        if acct_id:
            try: rows = list(srv.history(accountID=int(acct_id)) or [])
            except Exception: rows = list(srv.history() or [])
        else:
            rows = list(srv.history() or [])
    except Exception as e:
        _log(f"history fetch failed: {e}")
        return {}

    def _username_match(h, target_uname: str) -> bool:
        if not target_uname: return True
        try:
            fields = [
                getattr(getattr(h, "Account", None), "title", None),
                getattr(getattr(h, "Account", None), "name", None),
                getattr(h, "account", None),
                getattr(h, "username", None),
            ]
            tl = target_uname.lower()
            return any((str(v).strip().lower() == tl) for v in fields if v)
        except Exception:
            return False

    work: List[Tuple[str, int]] = []
    for h in rows:
        if allow:
            sid = _row_section_id(h)
            if sid and sid not in allow:
                continue

        hid = getattr(h, "accountID", None)
        if acct_id:
            try:
                if hid and int(hid) != int(acct_id): continue
            except Exception:
                pass
        else:
            if uname and not _username_match(h, uname): continue

        ts = (
            _as_epoch(getattr(h, "viewedAt", None)) or
            _as_epoch(getattr(h, "viewed_at", None)) or
            _as_epoch(getattr(h, "lastViewedAt", None))
        )
        if not ts or (since is not None and ts < int(since)): continue

        rk = getattr(h, "ratingKey", None) or getattr(h, "key", None)
        try:
            if rk is not None: work.append((str(int(rk)), int(ts)))
        except Exception:
            continue

    if not work:
        if prog:
            try: prog.done(ok=True, total=0)
            except Exception: pass
        _log("index size: 0 (since/user/library filter or empty history)")
        return {}

    work.sort(key=lambda x: x[1], reverse=True)
    if isinstance(limit, int) and limit > 0: work = work[: int(limit)]
    total = len(work)

    if prog:
        try: prog.tick(0, total=total, force=True)
        except Exception: pass

    unique_rks = sorted({rk for rk, _ in work})
    workers = _get_workers(adapter, "history_workers", "CW_PLEX_HISTORY_WORKERS", 10)
    to_fetch = [rk for rk in unique_rks if rk not in _FETCH_CACHE]

    if to_fetch:
        try:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = {ex.submit(_fetch_one, srv, rk): rk for rk in to_fetch}
                for fut in as_completed(futs):
                    rk = futs[fut]
                    m = fut.result()
                    if rk and m: _FETCH_CACHE[rk] = m
        except Exception as e:
            _log(f"parallel fetch error: {e}")

    out: Dict[str, Dict[str, Any]] = {}
    done = ignored = 0
    for rk_s, ts in work:
        m = _FETCH_CACHE.get(rk_s)
        if not m or not _keep_in_snapshot(adapter, m):
            ignored += int(bool(m)); done += 1
            if prog:
                try: prog.tick(done, total=total)
                except Exception: pass
            continue

        if allow:
            lid = m.get("library_id")
            if lid is not None and str(lid) not in allow:
                done += 1
                if prog:
                    try: prog.tick(done, total=total)
                    except Exception: pass
                continue

        row = dict(m)
        row["watched"] = True
        row["watched_at"] = _iso(ts)
        out[f"{canonical_key(row)}@{ts}"] = row

        done += 1
        if prog:
            try: prog.tick(done, total=total)
            except Exception: pass

    if prog:
        try: prog.done(ok=True, total=total)
        except Exception: pass

    _log(f"index size: {len(out)} (ignored={ignored}, since={since}, scanned={total}, "
         f"workers={workers}, unique={len(unique_rks)}, user={uname or acct_id})")
    return out

# ── add/remove (guarded) ──────────────────────────────────────────────────────

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        unresolved = []
        for it in items or []:
            _freeze_item(it, action="add", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(it), "hint": "no_plex_server"})
        _log("add skipped: no PMS bound")
        return 0, unresolved

    ok = 0
    unresolved: List[Dict[str, Any]] = []

    for it in items or []:
        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        ts = _as_epoch(it.get("watched_at"))
        if not ts:
            _freeze_item(it, action="add", reasons=["missing_watched_at"])
            unresolved.append({"item": id_minimal(it), "hint": "missing_watched_at"})
            continue

        rk = _resolve_rating_key(adapter, it)
        if not rk:
            _freeze_item(it, action="add", reasons=["not_in_library"])
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            continue

        if _scrobble_with_date(srv, rk, ts):
            ok += 1; _unfreeze_keys_if_present([_event_key(it)])
        else:
            _freeze_item(it, action="add", reasons=["scrobble_failed"])
            unresolved.append({"item": id_minimal(it), "hint": "scrobble_failed"})

    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        unresolved = []
        for it in items or []:
            _freeze_item(it, action="remove", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(it), "hint": "no_plex_server"})
        _log("remove skipped: no PMS bound")
        return 0, unresolved

    ok = 0
    unresolved: List[Dict[str, Any]] = []

    for it in items or []:
        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        rk = _resolve_rating_key(adapter, it)
        if not rk:
            _freeze_item(it, action="remove", reasons=["not_in_library"])
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            continue

        if _unscrobble(srv, rk):
            ok += 1; _unfreeze_keys_if_present([_event_key(it)])
        else:
            _freeze_item(it, action="remove", reasons=["unscrobble_failed"])
            unresolved.append({"item": id_minimal(it), "hint": "unscrobble_failed"})

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

# ── resolution + write helpers ────────────────────────────────────────────────

def _resolve_rating_key(adapter, it: Mapping[str, Any]) -> Optional[str]:
    ids = ids_from(it)
    rk = ids.get("plex")
    if rk: return str(rk)

    srv = getattr(adapter.client, "server", None)
    if not srv: return None

    kind = (it.get("type") or "movie").lower()
    is_episode = (kind == "episode")

    title = (it.get("title") or "").strip()
    series_title = (it.get("series_title") or "").strip()
    query_title = series_title if (is_episode and series_title) else title
    if not query_title: return None

    year = it.get("year")
    season = it.get("season") or it.get("season_number")
    episode = it.get("episode") or it.get("episode_number")

    sec_types = ("show",) if is_episode else ("movie",)
    allow = _allowed_history_sec_ids(adapter)

    hits: List[Any] = []
    for sec in adapter.libraries(types=sec_types) or []:
        sid = str(getattr(sec, "key", "")).strip()
        if allow and sid not in allow:
            continue
        try:
            hs = sec.search(title=query_title) or []
            if len(hs) == 1:
                hits.extend(hs); break
            hits.extend(hs)
        except Exception:
            continue

    def _score(obj) -> int:
        sc = 0
        try:
            ot = (getattr(obj, "grandparentTitle", None) if is_episode else getattr(obj, "title", None)) or ""
            if ot.strip().lower() == query_title.lower(): sc += 3
            if (not is_episode) and (year is not None) and (getattr(obj, "year", None) == year): sc += 2
            if is_episode:
                s_ok = (season is None) or (getattr(obj, "seasonNumber", None) == season or getattr(obj, "parentIndex", None) == season)
                e_ok = (episode is None) or (getattr(obj, "index", None) == episode)
                if s_ok and e_ok: sc += 2
            mids = (plex_normalize(obj).get("ids") or {})
            for k in ("imdb", "tmdb", "tvdb"):
                if k in mids and k in ids and mids[k] == ids[k]: sc += 4
        except Exception:
            pass
        return sc

    best, best_score = None, -1
    for h in hits:
        sc = _score(h)
        if sc > best_score:
            best, best_score = h, sc

    rk = getattr(best, "ratingKey", None) if best else None
    return str(rk) if rk else None

def _scrobble_with_date(srv, rating_key: Any, epoch: int) -> bool:
    try:
        url = srv.url("/:/scrobble")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library", "viewedAt": int(epoch)}
        r = srv._session.get(url, params=params, timeout=10)
        return r.ok
    except Exception:
        return False

def _unscrobble(srv, rating_key: Any) -> bool:
    try:
        url = srv.url("/:/unscrobble")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library"}
        r = srv._session.get(url, params=params, timeout=10)
        return r.ok
    except Exception:
        return False
