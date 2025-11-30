# /providers/sync/plex/_history.py
from __future__ import annotations
import os, json, re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

from ._common import (
    normalize as plex_normalize,
    minimal_from_history_row,
    candidate_guids_from_ids,
    section_find_by_guid,
    server_find_rating_key_by_guid,
)

try:
    from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from
except Exception:
    from _id_map import canonical_key, minimal as id_minimal, ids_from  # type: ignore

UNRESOLVED_PATH = "/config/.cw_state/plex_history.unresolved.json"
SHADOW_PATH = "/config/.cw_state/plex_history.shadow.json"

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:history] {msg}")

def _emit(evt: dict) -> None:
    try:
        feature = str(evt.get("feature") or "?")
        head = []
        if "event" in evt: head.append(f"event={evt['event']}")
        if "action" in evt: head.append(f"action={evt['action']}")
        tail = [f"{k}={v}" for k, v in evt.items() if k not in {"feature", "event", "action"}]
        line = " ".join(head + tail)
        print(f"[PLEX:{feature}] {line}", flush=True)
    except Exception:
        pass

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

def _plex_cfg(adapter) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    return cfg.get("plex", {}) if isinstance(cfg, dict) else {}

def _plex_cfg_get(adapter, key: str, default: Any = None) -> Any:
    c = _plex_cfg(adapter)
    v = c.get(key, default) if isinstance(c, dict) else default
    return default if v is None else v

def _history_cfg(adapter) -> Mapping[str, Any]:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        hist = plex.get("history") or {}
        return hist if isinstance(hist, dict) else {}
    except Exception:
        return {}

def _history_cfg_get(adapter, key: str, default: Any = None) -> Any:
    c = _history_cfg(adapter)
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
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        arr = ((plex.get("history") or {}).get("libraries") or [])
        return {str(int(x)) for x in arr if str(x).strip()}
    except Exception:
        return set()

def _row_section_id(h) -> Optional[str]:
    for a in ("librarySectionID", "sectionID", "librarySectionId", "sectionId"):
        v = getattr(h, a, None)
        if v is not None:
            try:
                return str(int(v))
            except Exception:
                pass
    sk = getattr(h, "sectionKey", None) or getattr(h, "librarySectionKey", None)
    if sk:
        m = re.search(r"/library/sections/(\d+)", str(sk))
        if m: return m.group(1)
    return None

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
    k = canonical_key(m) or canonical_key(it) or ""
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

def _load_shadow() -> Dict[str, Any]:
    try:
        with open(SHADOW_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _save_shadow(data: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(SHADOW_PATH), exist_ok=True)
        tmp = SHADOW_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, SHADOW_PATH)
    except Exception:
        pass

def _shadow_add(it: Mapping[str, Any]) -> None:
    try:
        key = _event_key(it)
        if not key:
            return
        data = _load_shadow()
        entry = data.get(key) or {"item": id_minimal(it)}
        entry["item"] = id_minimal(it)
        entry["watched_at"] = it.get("watched_at")
        entry["last_seen"] = _iso(int(datetime.now(timezone.utc).timestamp()))
        if "first_seen" not in entry:
            entry["first_seen"] = entry["last_seen"]
        data[key] = entry
        _save_shadow(data)
    except Exception:
        pass

def _has_external_ids(minimal: Mapping[str, Any]) -> bool:
    ids = minimal.get("ids") or {}
    sids = minimal.get("show_ids") or {}
    return bool(
        ids.get("imdb") or ids.get("tmdb") or ids.get("tvdb") or ids.get("trakt")
        or sids.get("imdb") or sids.get("tmdb") or sids.get("tvdb") or sids.get("trakt")
    )

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

_FETCH_CACHE: Dict[str, Dict[str, Any]] = {}

def _fetch_one(srv, rk: str) -> Optional[Dict[str, Any]]:
    try:
        obj = srv.fetchItem(int(rk))
        if not obj:
            return None
        m = plex_normalize(obj) or {}
        return m if m else None
    except Exception:
        return None

def _is_marked_watched(obj) -> bool:
    # true when Plex library item is 'watched' (flagged in UI)
    try:
        if getattr(obj, "isWatched", None):
            return True
    except Exception:
        pass
    try:
        vc = getattr(obj, "viewCount", None)
        if vc is not None and int(vc) > 0:
            return True
    except Exception:
        pass
    return False

def _last_view_ts(obj) -> Optional[int]:
    for attr in ("lastViewedAt", "viewedAt"):
        try:
            v = getattr(obj, attr, None)
        except Exception:
            v = None
        ts = _as_epoch(v)
        if ts:
            return ts
    return None

def _iter_marked_watched_from_library(
    adapter,
    allow: Set[str],
    since: Optional[int],
) -> List[Tuple[Dict[str, Any], int]]:
    results: List[Tuple[Dict[str, Any], int]] = []
    try:
        sections = adapter.libraries(types=("movie", "show")) or []
    except Exception:
        sections = []

    for sec in sections:
        try:
            sid = str(getattr(sec, "key", "")).strip()
        except Exception:
            sid = ""
        if allow and sid and sid not in allow:
            continue

        stype = (getattr(sec, "type", "") or "").lower()

        # Movies
        if stype == "movie":
            try:
                items = sec.all() or []
            except Exception:
                items = []
            for obj in items:
                try:
                    if not _is_marked_watched(obj):
                        continue

                    ts = _last_view_ts(obj)
                    # require a timestamp
                    if ts is None:
                        continue
                    if since is not None and ts < int(since):
                        continue

                    m = plex_normalize(obj) or {}
                    if not m:
                        continue
                    results.append((m, int(ts)))
                except Exception:
                    continue

        # Shows to episodes
        elif stype == "show":
            try:
                shows = sec.all() or []
            except Exception:
                shows = []
            for show in shows:
                try:
                    eps = show.episodes() or []
                except Exception:
                    eps = []
                for ep in eps:
                    try:
                        if not _is_marked_watched(ep):
                            continue

                        ts = _last_view_ts(ep)
                        # require a timestamp
                        if ts is None:
                            continue
                        if since is not None and ts < int(since):
                            continue

                        m = plex_normalize(ep) or {}
                        if not m:
                            continue
                        results.append((m, int(ts)))
                    except Exception:
                        continue
    return results

def build_index(adapter, since: Optional[int] = None, limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        _log("no PMS bound (account-only) → empty history index")
        return {}

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history") if callable(prog_mk) else None

    fallback_guid = bool(_plex_cfg_get(adapter, "fallback_GUID", False) or _plex_cfg_get(adapter, "fallback_guid", False))
    if fallback_guid:
        _emit({"event": "debug", "msg": "fallback_guid.enabled", "provider": "PLEX", "feature": "history"})

    def _int_or_zero(v):
        try: return int(v or 0)
        except Exception: return 0

    acct_id = _int_or_zero(getattr(getattr(adapter, "client", None), "user_account_id", None)) or _int_or_zero(_plex_cfg_get(adapter, "account_id", 0))
    uname = str(_plex_cfg_get(adapter, "username", "") or getattr(getattr(adapter, "client", None), "user_username", "") or "").strip().lower()
    allow = _allowed_history_sec_ids(adapter)

    rows = []
    try:
        kwargs = {}
        if acct_id:
            kwargs["accountID"] = int(acct_id)
        if since is not None:
            kwargs["mindate"] = datetime.fromtimestamp(int(since), tz=timezone.utc).replace(tzinfo=None)

        rows = list(srv.history(**kwargs) or [])

        if not rows and "accountID" in kwargs:
            _log("no rows with accountID → retry without account scope")
            kwargs.pop("accountID", None)
            rows = list(srv.history(**kwargs) or [])
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

    raw_by_rk: Dict[str, Any] = {}
    orphans: List[Tuple[Any, int]] = []
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
        if rk is None:
            if fallback_guid:
                try: orphans.append((h, int(ts)))
                except Exception: pass
            continue
        try:
            rk_s = str(int(rk))
            work.append((rk_s, int(ts)))
            raw_by_rk[rk_s] = h
        except Exception:
            if fallback_guid:
                try: orphans.append((h, int(ts)))
                except Exception: pass
            continue

    if not work and not (fallback_guid and orphans):
        if prog:
            try: prog.done(ok=True, total=0)
            except Exception: pass
        _log("index size: 0 (since/user/library filter or empty history)")
        return {}

    work.sort(key=lambda x: x[1], reverse=True)
    if isinstance(limit, int) and limit > 0: work = work[: int(limit)]
    total = len(work) + (len(orphans) if fallback_guid else 0)

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

    if fallback_guid:
        misses = [rk for rk in to_fetch if rk not in _FETCH_CACHE]
        for rk in misses:
            _emit({"event": "fallback_guid", "provider": "PLEX", "feature": "history", "action": "try", "rk": rk})
            fb = minimal_from_history_row(raw_by_rk.get(rk), allow_discover=True)
            _emit({"event": "fallback_guid", "provider": "PLEX", "feature": "history", "action": ("ok" if fb else "miss"), "rk": rk})
            if fb:
                _FETCH_CACHE[rk] = fb

    extras: List[Tuple[Dict[str, Any], int]] = []
    if fallback_guid and orphans:
        for row_obj, ts in orphans:
            fb = minimal_from_history_row(row_obj, allow_discover=True)
            if fb:
                extras.append((fb, ts))

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

    if extras:
        for m, ts in extras:
            if isinstance(limit, int) and limit > 0 and len(out) >= int(limit):
                _log(f"index truncated at {limit} (including extras)")
                break

            if not _keep_in_snapshot(adapter, m):
                done += 1
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

    # Shadow entries
    try:
        sh = _load_shadow()
        if sh:
            for ek, entry in list(sh.items()):
                m = entry.get("item") or {}
                ts = _as_epoch(entry.get("watched_at"))
                if not ts:
                    continue
                row = dict(m)
                row["watched"] = True
                row["watched_at"] = _iso(ts)
                k = f"{canonical_key(row)}@{ts}"
                if k not in out:
                    out[k] = row
    except Exception:
        pass

    # hydrate from Plex library watched state (Mark as watched)
    include_marked = bool(_history_cfg_get(adapter, "include_marked_watched", False))
    if include_marked:
        try:
            base_keys: Set[str] = set()
            for row in out.values():
                try:
                    bk = canonical_key(row)
                    if bk:
                        base_keys.add(bk)
                except Exception:
                    continue

            marked = _iter_marked_watched_from_library(adapter, allow, since)
            for m, ts in marked:
                if isinstance(limit, int) and limit > 0 and len(out) >= int(limit):
                    _log(f"index truncated at {limit} (including marked-watched)")
                    break

                # require timestamp from library
                if ts is None:
                    continue

                if not _keep_in_snapshot(adapter, m):
                    continue
                if allow:
                    lid = m.get("library_id")
                    if lid is not None and str(lid) not in allow:
                        continue

                row = dict(m)
                row["watched"] = True

                ts_int = int(ts)
                row["watched_at"] = _iso(ts_int)

                bk = canonical_key(row)
                if bk and bk in base_keys:
                    continue

                out[f"{bk}@{ts_int}"] = row
                if bk:
                    base_keys.add(bk)
        except Exception as e:
            _log(f"marked-watched hydrate failed: {e}")

    if prog:
        try: prog.done(ok=True, total=total)
        except Exception: pass

    _log(f"index size: {len(out)} (ignored={ignored}, since={since}, scanned={total}, "
         f"workers={workers}, unique={len(unique_rks)}, user={uname or acct_id})")
    return out

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
            ok += 1; _unfreeze_keys_if_present([_event_key(it)]); _shadow_add(it)
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

def _find_rk_by_guid_http(srv, guid_list: Iterable[str], allow: Set[str]) -> Optional[str]:
    for g in guid_list or []:
        try:
            r = srv._session.get(
                srv.url("/library/all"),
                params={"guid": g, "X-Plex-Container-Start": 0, "X-Plex-Container-Size": 50},
                timeout=8,
            )
            if not r.ok:
                continue
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.text or "")
            for md in root.findall(".//Video"):
                rk = md.attrib.get("ratingKey")
                sid = md.attrib.get("librarySectionID") or md.attrib.get("sectionID")
                if rk and (not allow or str(sid) in allow):
                    return str(rk)
        except Exception:
            continue
    return None

def _episode_rk_from_show(show_obj, season, episode) -> Optional[str]:
    try:
        eps = []
        try:
            eps = show_obj.episodes() or []
        except Exception:
            eps = []
        for e in eps:
            s_ok = (season is None) or getattr(e, "parentIndex", None) == season or getattr(e, "seasonNumber", None) == season
            e_ok = (episode is None) or getattr(e, "index", None) == episode
            if s_ok and e_ok:
                rk = getattr(e, "ratingKey", None)
                if rk:
                    return str(rk)
    except Exception:
        pass
    try:
        srv = getattr(show_obj, "_server", None) or getattr(show_obj, "server", None)
        sid = getattr(show_obj, "ratingKey", None)
        if srv and sid and hasattr(srv, "_session"):
            r = srv._session.get(
                srv.url(f"/library/metadata/{sid}/children"),
                params={"X-Plex-Container-Start": 0, "X-Plex-Container-Size": 500},
                timeout=8,
            )
            if r.ok:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(r.text or "")
                for ep in root.findall(".//Video"):
                    s_ok = (season is None) or int(ep.attrib.get("parentIndex","0") or "0") == int(season)
                    e_ok = (episode is None) or int(ep.attrib.get("index","0") or "0") == int(episode)
                    if s_ok and e_ok:
                        rk = ep.attrib.get("ratingKey")
                        if rk:
                            return str(rk)
    except Exception:
        pass
    return None

def _resolve_rating_key(adapter, it: Mapping[str, Any]) -> Optional[str]:
    ids = ids_from(it)
    srv = getattr(adapter.client, "server", None)
    if not srv:
        return None

    rk = ids.get("plex") or None
    if rk:
        try:
            if srv.fetchItem(int(rk)):
                return str(rk)
        except Exception:
            pass

    kind = (it.get("type") or "movie").lower()
    is_episode = (kind == "episode")
    title = (it.get("title") or "").strip()
    series_title = (it.get("series_title") or "").strip()
    query_title = series_title if (is_episode and series_title) else title
    if not query_title:
        return None

    year = it.get("year")
    season = it.get("season") or it.get("season_number")
    episode = it.get("episode") or it.get("episode_number")

    sec_types = ("show",) if is_episode else ("movie",)
    allow = _allowed_history_sec_ids(adapter)

    hits: List[Any] = []
    guids = candidate_guids_from_ids(it)
    if guids:
        for sec in adapter.libraries(types=sec_types) or []:
            sid = str(getattr(sec, "key", "")).strip()
            if allow and sid not in allow:
                continue
            obj = section_find_by_guid(sec, guids)
            if obj:
                hits.append(obj)

    if not hits and guids:
        rk_fast = _find_rk_by_guid_http(srv, guids, allow)
        if rk_fast:
            try:
                obj = srv.fetchItem(int(rk_fast))
                if obj:
                    hits.append(obj)
            except Exception:
                pass

    if not hits and guids:
        rk_any = server_find_rating_key_by_guid(srv, guids)
        if rk_any:
            try:
                obj = srv.fetchItem(int(rk_any))
                if obj:
                    sid = str(getattr(obj, "librarySectionID", "") or getattr(obj, "sectionID", "") or "")
                    if not allow or not sid or sid in allow:
                        hits.append(obj)
            except Exception:
                pass

    if not hits:
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

    if not hits:
        try:
            med = "episode" if is_episode else "movie"
            hs = srv.search(query_title, mediatype=med) or []
            for o in hs:
                sid = str(getattr(o, "librarySectionID", "") or getattr(o, "sectionID", "") or "")
                if allow and sid and sid not in allow:
                    continue
                hits.append(o)
        except Exception:
            pass

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

    if not hits:
        return None

    if is_episode:
        ep_hits = [o for o in hits if (getattr(o, "type", "") or "").lower() == "episode"]
        if ep_hits:
            best_ep = max(ep_hits, key=_score)
            rk = getattr(best_ep, "ratingKey", None)
            return str(rk) if rk else None
        show_hits = [o for o in hits if (getattr(o, "type", "") or "").lower() == "show"]
        for sh in show_hits:
            rk2 = _episode_rk_from_show(sh, season, episode)
            if rk2:
                return rk2
        return None

    best = max(hits, key=_score)
    rk = getattr(best, "ratingKey", None)
    return str(rk) if rk else None

def _scrobble_with_date(srv, rating_key: Any, epoch: int) -> bool:
    try:
        try:
            obj = srv.fetchItem(int(rating_key))
            if obj:
                typ = (getattr(obj, "type", "") or "").lower()
                if typ not in ("episode", "movie"):
                    return False
                try:
                    obj.markWatched()
                    return True
                except Exception:
                    pass
        except Exception:
            pass

        url = srv.url("/:/scrobble")
        token = getattr(srv, "token", None) or getattr(srv, "_token", None)
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library", "viewedAt": int(epoch)}
        if token:
            params["X-Plex-Token"] = token
        r = srv._session.get(url, params=params, headers=getattr(srv._session, "headers", None), timeout=10)
        if r.status_code == 401 and token:
            params2 = {"ratingKey": int(rating_key), "identifier": "com.plexapp.plugins.library", "viewedAt": int(epoch), "X-Plex-Token": token}
            r = srv._session.get(url, params=params2, headers=getattr(srv._session, "headers", None), timeout=10)
        if not r.ok:
            print(f"[PLEX:history] scrobble {rating_key} -> {r.status_code}")
        return r.ok
    except Exception as e:
        print(f"[PLEX:history] scrobble exception key={rating_key}: {e}")
        return False

def _unscrobble(srv, rating_key: Any) -> bool:
    try:
        url = srv.url("/:/unscrobble")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library"}
        r = srv._session.get(url, params=params, timeout=10)
        return r.ok
    except Exception:
        return False
