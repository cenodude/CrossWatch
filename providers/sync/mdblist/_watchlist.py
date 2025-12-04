# /providers/sync/mdblist/_watchlist.py
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from .._mod_common import request_with_retries
from cw_platform.id_map import minimal as id_minimal

BASE = "https://api.mdblist.com"
URL_LIST   = f"{BASE}/watchlist/items"
URL_MODIFY = f"{BASE}/watchlist/items/{{action}}"

STATE_DIR = Path("/config/.cw_state"); STATE_DIR.mkdir(parents=True, exist_ok=True)
SHADOW = STATE_DIR / "mdblist_watchlist.shadow.json"
UNRESOLVED_PATH = STATE_DIR / "mdblist_watchlist.unresolved.json"

def _log(msg: str):
    if os.getenv("CW_DEBUG") or os.getenv("CW_MDBLIST_DEBUG"):
        print(f"[MDBLIST:watchlist] {msg}")

def _cfg(adapter) -> Mapping[str, Any]:
    c = getattr(adapter, "config", {}) or {}
    if isinstance(c, dict) and isinstance(c.get("mdblist"), dict):
        return c["mdblist"]
    cfg_obj = getattr(adapter, "cfg", None)
    if cfg_obj:
        try:
            maybe = getattr(cfg_obj, "config", {}) or {}
            if isinstance(maybe, dict) and isinstance(maybe.get("mdblist"), dict):
                return maybe["mdblist"]
        except Exception:
            pass
    return {}

def _cfg_int(d: Mapping[str, Any], key: str, default: int) -> int:
    try: return int(d.get(key, default))
    except Exception: return default

def _cfg_bool(d: Mapping[str, Any], key: str, default: bool) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    if s in ("1","true","yes","on"): return True
    if s in ("0","false","no","off"): return False
    return default

def _shadow_load() -> Dict[str, Any]:
    try: return json.loads(SHADOW.read_text("utf-8"))
    except Exception: return {"ts": 0, "items": {}}

def _shadow_save(items: Mapping[str, Any]) -> None:
    try:
        tmp = SHADOW.with_suffix(".tmp")
        tmp.write_text(json.dumps({"ts": int(time.time()), "items": dict(items)}, ensure_ascii=False), "utf-8")
        os.replace(tmp, SHADOW)
    except Exception:
        pass

def _shadow_bust() -> None:
    try:
        if SHADOW.exists():
            SHADOW.unlink()
            _log("shadow.bust → file removed")
    except Exception:
        pass

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _load_unresolved() -> Dict[str, Any]:
    try: return json.loads(UNRESOLVED_PATH.read_text("utf-8"))
    except Exception: return {}

def _save_unresolved(data: Mapping[str, Any]) -> None:
    try:
        UNRESOLVED_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = UNRESOLVED_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, UNRESOLVED_PATH)
    except Exception as e:
        _log(f"unresolved.save failed: {e}")

def _key_of(obj: Mapping[str, Any]) -> str:
    ids = dict((obj.get("ids") or obj) or {})
    imdb = (ids.get("imdb") or ids.get("imdb_id") or "").strip()
    if imdb: return f"imdb:{imdb}"
    tmdb = ids.get("tmdb") or ids.get("tmdb_id")
    if tmdb: return f"tmdb:{int(tmdb)}"
    tvdb = ids.get("tvdb") or ids.get("tvdb_id")
    if tvdb: return f"tvdb:{int(tvdb)}"
    mdbl = ids.get("mdblist") or ids.get("id")
    if mdbl: return f"mdblist:{mdbl}"
    t = (obj.get("title") or "").strip(); y = obj.get("year")
    return f"title:{t}|year:{y}" if t and y else f"obj:{hash(json.dumps(obj, sort_keys=True)) & 0xffffffff}"

def _freeze_item(item: Mapping[str, Any], *, action: str, reasons: List[str], details: Optional[Mapping[str, Any]] = None) -> None:
    m = id_minimal(item)
    key = _key_of(m)
    data = _load_unresolved()
    entry = data.get(key) or {"feature": "watchlist", "action": action, "first_seen": _now_iso(), "attempts": 0}
    entry.update({"item": m, "last_attempt": _now_iso()})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    entry["reasons"] = sorted(rset)
    if details:
        entry["details"] = {**(entry.get("details") or {}), **details}
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)

def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved(); changed = False
    for k in list(keys or []):
        if k in data:
            del data[k]; changed = True
    if changed: _save_unresolved(data)

def _is_frozen(item: Mapping[str, Any]) -> bool:
    return _key_of(id_minimal(item)) in _load_unresolved()

def _ids_for_mdblist(it: Mapping[str, Any]) -> Dict[str, Any]:
    ids = dict((it.get("ids") or {}))
    if not ids:
        ids = {
            "imdb": it.get("imdb") or it.get("imdb_id"),
            "tmdb": it.get("tmdb") or it.get("tmdb_id"),
            "tvdb": it.get("tvdb") or it.get("tvdb_id"),
        }
    out = {}
    if ids.get("imdb"): out["imdb"] = str(ids["imdb"])
    if ids.get("tmdb"): out["tmdb"] = int(ids["tmdb"])
    if ids.get("tvdb"): out["tvdb"] = int(ids["tvdb"])
    return out

def _pick_kind_from_row(row: Mapping[str, Any]) -> str:
    t = (row.get("mediatype") or row.get("type") or "").strip().lower()
    if t in ("show","tv","series","shows"): return "show"
    return "movie"

def _to_minimal(row: Mapping[str, Any]) -> Dict[str, Any]:
    ids = {
        "imdb":   row.get("imdb_id") or row.get("imdb"),
        "tmdb":   row.get("tmdb_id") or row.get("tmdb"),
        "tvdb":   row.get("tvdb_id") or row.get("tvdb"),
        "mdblist": row.get("id"),
    }
    typ = _pick_kind_from_row(row)
    title = (row.get("title") or row.get("name") or row.get("original_title") or row.get("original_name") or "").strip()
    year = (
        row.get("year")
        or row.get("release_year")
        or (int(str(row.get("release_date"))[:4]) if row.get("release_date") else None)
        or row.get("first_air_year")
        or (int(str(row.get("first_air_date"))[:4]) if row.get("first_air_date") else None)
    )
    m: Dict[str, Any] = {"type": typ, "ids": {k: v for k, v in ids.items() if v}}
    if title:
        m["title"] = title
    if year:
        try: m["year"] = int(year)
        except Exception: pass
    return m

# ---- live-peek helpers (to validate shadow) ----
def _parse_rows_and_total(data: Any) -> Tuple[List[Mapping[str, Any]], Optional[int]]:
    if isinstance(data, dict):
        total = None
        for k in ("total_items", "total", "count", "items_total"):
            try:
                v = int(data.get(k) or 0)
                if v > 0:
                    total = v
                    break
            except Exception:
                pass
        rows: List[Mapping[str, Any]] = []
        if "movies" in data or "shows" in data:
            rows.extend(data.get("movies", []) or [])
            rows.extend(data.get("shows", []) or [])
        else:
            rows = (data.get("results") or data.get("items") or []) or []
        return rows if isinstance(rows, list) else [], total
    if isinstance(data, list):
        return data, None
    return [], None

def _peek_live(adapter, apikey: str, timeout: float, retries: int) -> Tuple[Optional[str], Optional[int]]:
    try:
        r = request_with_retries(
            adapter.client.session, "GET", URL_LIST,
            params={"apikey": apikey, "limit": 1, "offset": 0, "unified": 1},
            timeout=timeout, max_retries=retries
        )
        if r.status_code != 200:
            _log(f"peek failed {r.status_code}")
            return None, None
        rows, total = _parse_rows_and_total(r.json() if (r.text or "").strip() else {})
        if rows:
            try:
                k = _key_of(_to_minimal(rows[0]))
                return k, total
            except Exception:
                return None, total
        return None, total
    except Exception as e:
        _log(f"peek error: {e}")
        return None, None

def build_index(adapter) -> Dict[str, Dict[str, Any]]:
    c = _cfg(adapter)
    ttl_h = _cfg_int(c, "watchlist_shadow_ttl_hours", 24)
    validate_shadow = _cfg_bool(c, "watchlist_shadow_validate", False)
    limit = _cfg_int(c, "watchlist_page_size", 200)
    apikey = str(c.get("api_key") or "").strip()
    if not apikey:
        return dict(_shadow_load().get("items") or {})

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("watchlist") if callable(prog_mk) else None

    sh = _shadow_load()
    # TTL==0 → force live fetch; TTL>0 may use shadow (optionally validated)
    if ttl_h > 0 and sh.get("ts"):
        age = int(time.time()) - int(sh.get("ts", 0))
        cached = dict(sh.get("items") or {})
        if age <= ttl_h * 3600 and cached:
            stale = False
            if validate_shadow:
                k0, total_live = _peek_live(adapter, apikey, adapter.cfg.timeout, adapter.cfg.max_retries)
                cached_count = len(cached)
                if total_live is not None and int(total_live) != cached_count:
                    stale = True
                    _log(f"shadow invalid: live_total={total_live} != cached={cached_count}")
                elif k0 and (k0 not in cached):
                    stale = True
                    _log("shadow invalid: first live item not in cache")
            if not stale:
                if prog:
                    try:
                        total = len(cached)
                        prog.tick(0, total=total, force=True); prog.tick(total, total=total)
                    except Exception:
                        pass
                _unfreeze_keys_if_present(cached.keys())
                return cached
            _log("shadow → rebuild due to validation")

    sess = adapter.client.session
    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries

    collected: Dict[str, Dict[str, Any]] = {}
    offset = 0
    total_tick = 0

    while True:
        params = {"apikey": apikey, "limit": limit, "offset": offset, "unified": 1}
        r = request_with_retries(sess, "GET", URL_LIST, params=params, timeout=timeout, max_retries=retries)
        if r.status_code != 200:
            _log(f"GET offset {offset} failed {r.status_code}")
            break
        data = r.json() if (r.text or "").strip() else {}
        rows, _ = _parse_rows_and_total(data)
        if not rows:
            break
        for row in rows:
            try:
                m = _to_minimal(row)
                collected[_key_of(m)] = m
            except Exception:
                pass
        total_tick += len(rows)
        if prog:
            try:
                prog.tick(total_tick, total=max(total_tick, (offset + len(rows))))
            except Exception:
                pass
        if len(rows) < limit:
            break
        offset += len(rows)

    if collected:
        _shadow_save(collected)
        _unfreeze_keys_if_present(collected.keys())
    if prog:
        try:
            total = len(collected); prog.tick(total, total=total)
        except Exception:
            pass
    _log(f"index size: {len(collected)}")
    return collected

def _chunk(seq: List[Any], n: int) -> Iterable[List[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def _batch_payload(items: Iterable[Mapping[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    accepted: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for it in items or []:
        if _is_frozen(it):
            continue
        ids = _ids_for_mdblist(it)
        if not ids:
            rejected.append({"item": id_minimal(it), "hint": "missing ids"})
            continue
        accepted.append({"type": ("show" if (it.get("type") or "").lower() in ("show","shows","tv","series") else "movie"), "ids": ids})
    return accepted, rejected

def _payload_from_accepted(accepted_slice: List[Dict[str, Any]]) -> Dict[str, Any]:
    movies = [{"imdb": x["ids"].get("imdb"), "tmdb": x["ids"].get("tmdb")} for x in accepted_slice if x["type"] == "movie"]
    shows  = [{"imdb": x["ids"].get("imdb"), "tmdb": x["ids"].get("tmdb")} for x in accepted_slice if x["type"] == "show"]
    movies = [{k: v for k, v in d.items() if v is not None} for d in movies]
    shows  = [{k: v for k, v in d.items() if v is not None} for d in shows]
    payload: Dict[str, Any] = {}
    if movies: payload["movies"] = movies
    if shows:  payload["shows"] = shows
    return payload

def _freeze_not_found(not_found: Mapping[str, Any], *, action: str, unresolved: List[Dict[str, Any]], add_details: bool) -> None:
    for t in ("movies", "shows"):
        for obj in (not_found.get(t) or []):
            ids = {k: v for k, v in dict(obj or {}).items() if k in ("imdb", "tmdb")}
            typ = "movie" if t == "movies" else "show"
            m = id_minimal({"type": typ, "ids": ids})
            unresolved.append({"item": m, "hint": "not_found"})
            _freeze_item(m, action=action, reasons=[f"{action}:not-found"], details={"ids": ids} if add_details else None)

def _write(adapter, action: str, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    c = _cfg(adapter)
    apikey = str(c.get("api_key") or "").strip()
    if not apikey:
        return 0, [{"item": id_minimal(it), "hint": "missing_api_key"} for it in (items or [])]
    batch = _cfg_int(c, "watchlist_batch_size", 100)
    freeze_details = _cfg_bool(c, "watchlist_freeze_details", True)
    sess = adapter.client.session
    accepted, unresolved = _batch_payload(items)
    if not accepted:
        return 0, unresolved
    ok = 0
    for sl in _chunk(accepted, batch):
        payload = _payload_from_accepted(sl)
        if not payload:
            continue
        r = request_with_retries(
            sess, "POST", URL_MODIFY.format(action=action),
            params={"apikey": apikey},
            json=payload,
            timeout=adapter.cfg.timeout, max_retries=adapter.cfg.max_retries
        )
        if r.status_code in (200, 201):
            d = r.json() if (r.text or "").strip() else {}
            added    = d.get("added") or {}
            existing = d.get("existing") or {}
            removed  = d.get("deleted") or d.get("removed") or {}
            if action == "add":
                ok += int(added.get("movies") or 0) + int(added.get("shows") or 0) + int(existing.get("movies") or 0) + int(existing.get("shows") or 0)
            else:
                ok += int(removed.get("movies") or 0) + int(removed.get("shows") or 0)
            nf = d.get("not_found") or {}
            _freeze_not_found(nf, action=action, unresolved=unresolved, add_details=freeze_details)
        else:
            _log(f"{action.upper()} failed {r.status_code}: {r.text[:200] if r.text else ''}")
            for x in sl:
                m = id_minimal({"type": x["type"], "ids": x["ids"]})
                unresolved.append({"item": m, "hint": f"http:{r.status_code}"})
                _freeze_item(m, action=action, reasons=[f"http:{r.status_code}"], details={"status": r.status_code} if freeze_details else None)

    # Bust shadow after successful writes so next read is fresh
    if ok > 0:
        _shadow_bust()

    return ok, unresolved

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    return _write(adapter, "add", items)

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    return _write(adapter, "remove", items)