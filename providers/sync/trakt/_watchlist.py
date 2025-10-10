# /providers/sync/trakt/_watchlist.py
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from ._common import (
    build_headers,
    normalize_watchlist_row,
    key_of,
    ids_for_trakt,
    pick_trakt_kind,
    build_watchlist_body,  # kept for callers; local batching is simple
)
from .._mod_common import request_with_retries
try:
    from cw_platform.id_map import minimal as id_minimal
except Exception:
    from _id_map import minimal as id_minimal  # type: ignore

BASE = "https://api.trakt.tv"
URL_ALL    = f"{BASE}/sync/watchlist"
URL_REMOVE = f"{BASE}/sync/watchlist/remove"

STATE_DIR = Path("/config/.cw_state"); STATE_DIR.mkdir(parents=True, exist_ok=True)
SHADOW = STATE_DIR / "trakt_watchlist.shadow.json"
UNRESOLVED_PATH = STATE_DIR / "trakt_watchlist.unresolved.json"

def _log(msg: str):
    # Enable with CW_DEBUG=1 or CW_TRAKT_DEBUG=1
    if os.getenv("CW_DEBUG") or os.getenv("CW_TRAKT_DEBUG"):
        print(f"[TRAKT:watchlist] {msg}")

# ── config helpers ────────────────────────────────────────────────────────────
def _cfg(adapter) -> Mapping[str, Any]:
    # Prefer adapter.config['trakt'], fallback to adapter.cfg.config['trakt']
    c = getattr(adapter, "config", {}) or {}
    if isinstance(c, dict) and isinstance(c.get("trakt"), dict):
        return c["trakt"]
    cfg_obj = getattr(adapter, "cfg", None)
    if cfg_obj:
        try:
            maybe = getattr(cfg_obj, "config", {}) or {}
            if isinstance(maybe, dict) and isinstance(maybe.get("trakt"), dict):
                return maybe["trakt"]
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

# ── shadow (etag) ─────────────────────────────────────────────────────────────
def _shadow_load() -> Dict[str, Any]:
    try: return json.loads(SHADOW.read_text("utf-8"))
    except Exception: return {"etag": None, "ts": 0, "items": {}}

def _shadow_save(etag: Optional[str], items: Mapping[str, Any]) -> None:
    try:
        tmp = SHADOW.with_suffix(".tmp")
        tmp.write_text(json.dumps({"etag": etag, "ts": int(time.time()), "items": dict(items)}, ensure_ascii=False), "utf-8")
        os.replace(tmp, SHADOW)
    except Exception:
        pass

# ── unresolved (freeze) ───────────────────────────────────────────────────────
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

def _freeze_item(item: Mapping[str, Any], *, action: str, reasons: List[str], details: Optional[Mapping[str, Any]] = None) -> None:
    m = id_minimal(item)
    key = key_of(m)
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
    return key_of(id_minimal(item)) in _load_unresolved()

# ── rate-limit logging ────────────────────────────────────────────────────────
def _log_rate_headers(resp) -> None:
    try:
        r = resp.headers
        remain = r.get("X-RateLimit-Remaining")
        reset  = r.get("X-RateLimit-Reset")
        raf    = r.get("Retry-After")
        if remain or reset or raf:
            _log(f"rate remain={remain or '-'} reset={reset or '-'} retry_after={raf or '-'}")
    except Exception:
        pass

# ── index (progress-aware; fixed total; no prog.done) ─────────────────────────
def build_index(adapter) -> Dict[str, Dict[str, Any]]:
    """
    Present-state as {key: minimal}. Uses optional ETag+shadow; ticks progress.
    """
    cfg = _cfg(adapter)
    use_etag = _cfg_bool(cfg, "watchlist_use_etag", True)
    ttl_h = _cfg_int(cfg, "watchlist_shadow_ttl_hours", 168)
    log_rates = _cfg_bool(cfg, "watchlist_log_rate_limits", True)

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("watchlist") if callable(prog_mk) else None

    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})

    sh = _shadow_load()
    if use_etag and sh.get("etag"):
        # Respect TTL if set (>0). If expired, skip If-None-Match to force 200.
        fresh = True
        if ttl_h > 0 and sh.get("ts"):
            age = int(time.time()) - int(sh.get("ts", 0))
            fresh = age <= ttl_h * 3600
        if fresh:
            headers["If-None-Match"] = sh["etag"]

    r = request_with_retries(
        sess, "GET", URL_ALL, headers=headers,
        timeout=adapter.cfg.timeout, max_retries=adapter.cfg.max_retries
    )
    if log_rates: _log_rate_headers(r)
    etag = r.headers.get("ETag")

    # 304 → reuse shadow
    if r.status_code == 304 and use_etag:
        _log("304 Not Modified → shadow")
        idx = dict(sh.get("items") or {})
        total = len(idx)
        if prog:
            try:
                prog.tick(0, total=total, force=True)
                prog.tick(total, total=total)
            except Exception:
                pass
        _unfreeze_keys_if_present(idx.keys())
        return idx

    # Non-200 → fallback to shadow
    if r.status_code != 200:
        _log(f"GET failed {r.status_code}; using shadow")
        idx = dict(sh.get("items") or {})
        total = len(idx)
        if prog:
            try:
                prog.tick(0, total=total, force=True)
                prog.tick(total, total=total)
            except Exception:
                pass
        _unfreeze_keys_if_present(idx.keys())
        return idx

    # 200 OK
    data = r.json() if (r.text or "").strip() else []
    items = [normalize_watchlist_row(x) for x in (data or []) if isinstance(x, dict)]
    idx = {key_of(m): m for m in items}
    if use_etag:
        _shadow_save(etag, idx)
    _unfreeze_keys_if_present(idx.keys())

    if prog:
        try:
            total = len(idx)
            prog.tick(0, total=total, force=True)
            prog.tick(total, total=total)
        except Exception:
            pass

    _log(f"index size: {len(idx)}")
    return idx

# ── writes ────────────────────────────────────────────────────────────────────
def _batch_payload(items: Iterable[Mapping[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (accepted_items, rejected_items). Accepted: [{'type','ids'}]."""
    accepted: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for it in items or []:
        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue
        ids = ids_for_trakt(it)
        if not ids:
            rejected.append({"item": id_minimal(it), "hint": "missing ids"})
            continue
        accepted.append({"type": ("show" if pick_trakt_kind(it) == "shows" else "movie"), "ids": ids})
    return accepted, rejected

def _freeze_not_found(not_found: Mapping[str, Any], *, action: str, unresolved: List[Dict[str, Any]], add_details: bool) -> None:
    for t in ("movies", "shows"):
        for obj in (not_found.get(t) or []):
            ids = dict(obj.get("ids") or {})
            m = id_minimal({"type": "movie" if t == "movies" else "show", "ids": ids})
            unresolved.append({"item": m, "hint": "not_found"})
            _freeze_item(m, action=action, reasons=[f"{action}:not-found"], details={"ids": ids} if add_details else None)

def _chunk(seq: List[Any], n: int) -> Iterable[List[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def _payload_from_accepted(accepted_slice: List[Dict[str, Any]]) -> Dict[str, Any]:
    movies = [{"ids": x["ids"]} for x in accepted_slice if x["type"] == "movie"]
    shows  = [{"ids": x["ids"]} for x in accepted_slice if x["type"] == "show"]
    payload: Dict[str, Any] = {}
    if movies: payload["movies"] = movies
    if shows:  payload["shows"] = shows
    return payload

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """Add items by IDs only (imdb/tmdb/tvdb/trakt); configurable batching."""
    cfg = _cfg(adapter)
    batch = _cfg_int(cfg, "watchlist_batch_size", 100)
    log_rates = _cfg_bool(cfg, "watchlist_log_rate_limits", True)
    freeze_details = _cfg_bool(cfg, "watchlist_freeze_details", True)

    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})

    accepted, unresolved = _batch_payload(items)
    if not accepted:
        return 0, unresolved

    ok = 0
    for sl in _chunk(accepted, batch):
        payload = _payload_from_accepted(sl)
        if not payload: 
            continue
        r = request_with_retries(
            sess, "POST", URL_ALL, headers=headers, json=payload,
            timeout=adapter.cfg.timeout, max_retries=adapter.cfg.max_retries
        )
        if log_rates: _log_rate_headers(r)

        if r.status_code in (200, 201):
            d = r.json() if (r.text or "").strip() else {}
            added = d.get("added") or {}
            existing = d.get("existing") or {}
            ok += int(added.get("movies") or 0) + int(added.get("shows") or 0) + int(existing.get("movies") or 0) + int(existing.get("shows") or 0)
            nf = d.get("not_found") or {}
            _freeze_not_found(nf, action="add", unresolved=unresolved, add_details=freeze_details)
            if ok == 0 and not unresolved:
                _log("ADD returned 200 but no items were added or existing")
        else:
            _log(f"ADD failed {r.status_code}: {r.text[:180] if r.text else ''}")
            # Conservative: freeze this slice
            for x in sl:
                m = id_minimal({"type": x["type"], "ids": x["ids"]})
                unresolved.append({"item": m, "hint": f"http:{r.status_code}"})
                _freeze_item(m, action="add", reasons=[f"http:{r.status_code}"], details={"status": r.status_code} if freeze_details else None)

    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """Remove items by IDs only; configurable batching."""
    cfg = _cfg(adapter)
    batch = _cfg_int(cfg, "watchlist_batch_size", 100)
    log_rates = _cfg_bool(cfg, "watchlist_log_rate_limits", True)
    freeze_details = _cfg_bool(cfg, "watchlist_freeze_details", True)

    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})

    accepted, unresolved = _batch_payload(items)
    if not accepted:
        return 0, unresolved

    ok = 0
    for sl in _chunk(accepted, batch):
        payload = _payload_from_accepted(sl)
        if not payload:
            continue
        r = request_with_retries(
            sess, "POST", URL_REMOVE, headers=headers, json=payload,
            timeout=adapter.cfg.timeout, max_retries=adapter.cfg.max_retries
        )
        if log_rates: _log_rate_headers(r)

        if r.status_code in (200, 201):
            d = r.json() if (r.text or "").strip() else {}
            deleted = (d.get("deleted") or d.get("removed") or {})
            ok += int(deleted.get("movies") or 0) + int(deleted.get("shows") or 0)
            nf = d.get("not_found") or {}
            _freeze_not_found(nf, action="remove", unresolved=unresolved, add_details=freeze_details)
        else:
            _log(f"REMOVE failed {r.status_code}: {r.text[:180] if r.text else ''}")
            for x in sl:
                m = id_minimal({"type": x["type"], "ids": x["ids"]})
                unresolved.append({"item": m, "hint": f"http:{r.status_code}"})
                _freeze_item(m, action="remove", reasons=[f"http:{r.status_code}"], details={"status": r.status_code} if freeze_details else None)

    return ok, unresolved
