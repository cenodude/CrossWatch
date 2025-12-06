# /providers/sync/mdblist/_watchlist.py
# MDBList watchlist sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from cw_platform.id_map import minimal as id_minimal

from .._mod_common import request_with_retries


BASE = "https://api.mdblist.com"
URL_LIST = f"{BASE}/watchlist/items"
URL_MODIFY = f"{BASE}/watchlist/items/{{action}}"

STATE_DIR = Path("/config/.cw_state")
STATE_DIR.mkdir(parents=True, exist_ok=True)
SHADOW = STATE_DIR / "mdblist_watchlist.shadow.json"
UNRESOLVED_PATH = STATE_DIR / "mdblist_watchlist.unresolved.json"


def _log(msg: str) -> None:
    if os.getenv("CW_DEBUG") or os.getenv("CW_MDBLIST_DEBUG"):
        print(f"[MDBLIST:watchlist] {msg}")


def _cfg(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    if isinstance(cfg, dict) and isinstance(cfg.get("mdblist"), dict):
        return cfg["mdblist"]
    cfg_obj = getattr(adapter, "cfg", None)
    if cfg_obj:
        try:
            runtime_cfg = getattr(cfg_obj, "config", {}) or {}
            if isinstance(runtime_cfg, dict) and isinstance(runtime_cfg.get("mdblist"), dict):
                return runtime_cfg["mdblist"]
        except Exception:
            pass
    return {}


def _cfg_int(data: Mapping[str, Any], key: str, default: int) -> int:
    raw = data.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _cfg_bool(data: Mapping[str, Any], key: str, default: bool) -> bool:
    raw = data.get(key, default)
    if isinstance(raw, bool):
        return raw
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _shadow_load() -> dict[str, Any]:
    try:
        return json.loads(SHADOW.read_text("utf-8"))
    except Exception:
        return {"ts": 0, "items": {}}


def _shadow_save(items: Mapping[str, Any]) -> None:
    try:
        tmp = SHADOW.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"ts": int(time.time()), "items": dict(items)}, ensure_ascii=False),
            "utf-8",
        )
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


def _load_unresolved() -> dict[str, Any]:
    try:
        return json.loads(UNRESOLVED_PATH.read_text("utf-8"))
    except Exception:
        return {}


def _save_unresolved(data: Mapping[str, Any]) -> None:
    try:
        UNRESOLVED_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = UNRESOLVED_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, UNRESOLVED_PATH)
    except Exception as e:
        _log(f"unresolved.save failed: {e}")


def _key_of(obj: Mapping[str, Any]) -> str:
    ids: dict[str, Any] = dict((obj.get("ids") or obj) or {})
    imdb = str(ids.get("imdb") or ids.get("imdb_id") or "").strip()
    if imdb:
        return f"imdb:{imdb}"
    tmdb_val = ids.get("tmdb") or ids.get("tmdb_id")
    if tmdb_val is not None:
        try:
            return f"tmdb:{int(tmdb_val)}"
        except Exception:
            pass
    tvdb_val = ids.get("tvdb") or ids.get("tvdb_id")
    if tvdb_val is not None:
        try:
            return f"tvdb:{int(tvdb_val)}"
        except Exception:
            pass
    mdbl = ids.get("mdblist") or ids.get("id")
    if mdbl:
        return f"mdblist:{mdbl}"
    title = str(obj.get("title") or "").strip()
    year_val = obj.get("year")
    if title and year_val:
        return f"title:{title}|year:{year_val}"
    return f"obj:{hash(json.dumps(obj, sort_keys=True)) & 0xffffffff}"


def _freeze_item(
    item: Mapping[str, Any],
    *,
    action: str,
    reasons: list[str],
    details: Mapping[str, Any] | None = None,
) -> None:
    minimal = id_minimal(item)
    key = _key_of(minimal)
    data = _load_unresolved()
    entry = data.get(key) or {
        "feature": "watchlist",
        "action": action,
        "first_seen": _now_iso(),
        "attempts": 0,
    }
    entry.update({"item": minimal, "last_attempt": _now_iso()})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    entry["reasons"] = sorted(rset)
    if details:
        old_details = entry.get("details") or {}
        entry["details"] = {**old_details, **details}
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)


def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for key in list(keys or []):
        if key in data:
            del data[key]
            changed = True
    if changed:
        _save_unresolved(data)


def _is_frozen(item: Mapping[str, Any]) -> bool:
    return _key_of(id_minimal(item)) in _load_unresolved()


def _ids_for_mdblist(item: Mapping[str, Any]) -> dict[str, Any]:
    ids_raw: dict[str, Any] = dict(item.get("ids") or {})
    if not ids_raw:
        ids_raw = {
            "imdb": item.get("imdb") or item.get("imdb_id"),
            "tmdb": item.get("tmdb") or item.get("tmdb_id"),
            "tvdb": item.get("tvdb") or item.get("tvdb_id"),
        }
    out: dict[str, Any] = {}
    imdb_val = ids_raw.get("imdb")
    if imdb_val:
        out["imdb"] = str(imdb_val)
    tmdb_val = ids_raw.get("tmdb")
    if tmdb_val is not None:
        try:
            out["tmdb"] = int(tmdb_val)
        except Exception:
            pass
    tvdb_val = ids_raw.get("tvdb")
    if tvdb_val is not None:
        try:
            out["tvdb"] = int(tvdb_val)
        except Exception:
            pass
    return out


def _pick_kind_from_row(row: Mapping[str, Any]) -> str:
    t = str(row.get("mediatype") or row.get("type") or "").strip().lower()
    if t in ("show", "tv", "series", "shows"):
        return "show"
    return "movie"


def _to_minimal(row: Mapping[str, Any]) -> dict[str, Any]:
    ids = {
        "imdb": row.get("imdb_id") or row.get("imdb"),
        "tmdb": row.get("tmdb_id") or row.get("tmdb"),
        "tvdb": row.get("tvdb_id") or row.get("tvdb"),
        "mdblist": row.get("id"),
    }
    typ = _pick_kind_from_row(row)
    title = str(
        row.get("title")
        or row.get("name")
        or row.get("original_title")
        or row.get("original_name")
        or ""
    ).strip()
    year = (
        row.get("year")
        or row.get("release_year")
        or (int(str(row.get("release_date"))[:4]) if row.get("release_date") else None)
        or row.get("first_air_year")
        or (int(str(row.get("first_air_date"))[:4]) if row.get("first_air_date") else None)
    )
    minimal: dict[str, Any] = {"type": typ, "ids": {k: v for k, v in ids.items() if v}}
    if title:
        minimal["title"] = title
    if year:
        try:
            minimal["year"] = int(year)
        except Exception:
            pass
    return minimal


def _parse_rows_and_total(data: Any) -> tuple[list[Mapping[str, Any]], int | None]:
    if isinstance(data, dict):
        total: int | None = None
        for key in ("total_items", "total", "count", "items_total"):
            try:
                v = int(data.get(key) or 0)
                if v > 0:
                    total = v
                    break
            except Exception:
                pass
        rows: list[Mapping[str, Any]] = []
        if "movies" in data or "shows" in data:
            rows.extend(data.get("movies", []) or [])
            rows.extend(data.get("shows", []) or [])
        else:
            rows = (data.get("results") or data.get("items") or []) or []
        return rows if isinstance(rows, list) else [], total
    if isinstance(data, list):
        return data, None
    return [], None


def _peek_live(
    adapter: Any,
    apikey: str,
    timeout: float,
    retries: int,
) -> tuple[str | None, int | None]:
    try:
        r = request_with_retries(
            adapter.client.session,
            "GET",
            URL_LIST,
            params={"apikey": apikey, "limit": 1, "offset": 0, "unified": 1},
            timeout=timeout,
            max_retries=retries,
        )
        if r.status_code != 200:
            _log(f"peek failed {r.status_code}")
            return None, None
        rows, total = _parse_rows_and_total(r.json() if (r.text or "").strip() else {})
        if rows:
            try:
                key = _key_of(_to_minimal(rows[0]))
                return key, total
            except Exception:
                return None, total
        return None, total
    except Exception as e:
        _log(f"peek error: {e}")
        return None, None


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    cfg = _cfg(adapter)
    ttl_h = _cfg_int(cfg, "watchlist_shadow_ttl_hours", 24)
    validate_shadow = _cfg_bool(cfg, "watchlist_shadow_validate", False)
    limit = _cfg_int(cfg, "watchlist_page_size", 200)
    apikey = str(cfg.get("api_key") or "").strip()
    if not apikey:
        shadow = _shadow_load()
        return dict(shadow.get("items") or {})
    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("watchlist") if callable(prog_factory) else None
    shadow = _shadow_load()
    if ttl_h > 0 and shadow.get("ts"):
        age = int(time.time()) - int(shadow.get("ts", 0))
        cached: dict[str, dict[str, Any]] = dict(shadow.get("items") or {})
        if age <= ttl_h * 3600 and cached:
            stale = False
            if validate_shadow:
                k0, total_live = _peek_live(
                    adapter,
                    apikey,
                    adapter.cfg.timeout,
                    adapter.cfg.max_retries,
                )
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
                        prog.tick(0, total=total, force=True)
                        prog.tick(total, total=total)
                    except Exception:
                        pass
                _unfreeze_keys_if_present(cached.keys())
                return cached
            _log("shadow → rebuild due to validation")
    sess = adapter.client.session
    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries
    collected: dict[str, dict[str, Any]] = {}
    offset = 0
    total_tick = 0
    while True:
        params = {"apikey": apikey, "limit": limit, "offset": offset, "unified": 1}
        r = request_with_retries(
            sess,
            "GET",
            URL_LIST,
            params=params,
            timeout=timeout,
            max_retries=retries,
        )
        if r.status_code != 200:
            _log(f"GET offset {offset} failed {r.status_code}")
            break
        data = r.json() if (r.text or "").strip() else {}
        rows, _ = _parse_rows_and_total(data)
        if not rows:
            break
        for row in rows:
            try:
                minimal = _to_minimal(row)
                collected[_key_of(minimal)] = minimal
            except Exception:
                pass
        batch_len = len(rows)
        total_tick += batch_len
        if prog:
            try:
                prog.tick(total_tick, total=max(total_tick, offset + batch_len))
            except Exception:
                pass
        if batch_len < limit:
            break
        offset += batch_len
    if collected:
        _shadow_save(collected)
        _unfreeze_keys_if_present(collected.keys())
    if prog:
        try:
            total = len(collected)
            prog.tick(total, total=total)
        except Exception:
            pass
    _log(f"index size: {len(collected)}")
    return collected


def _chunk(seq: list[Any], n: int) -> Iterable[list[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _batch_payload(
    items: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for item in items or []:
        if _is_frozen(item):
            continue
        ids = _ids_for_mdblist(item)
        if not ids:
            rejected.append({"item": id_minimal(item), "hint": "missing ids"})
            continue
        kind = "show" if str(item.get("type") or "").lower() in ("show", "shows", "tv", "series") else "movie"
        accepted.append({"type": kind, "ids": ids})
    return accepted, rejected


def _payload_from_accepted(accepted_slice: list[dict[str, Any]]) -> dict[str, Any]:
    movies = [
        {"imdb": x["ids"].get("imdb"), "tmdb": x["ids"].get("tmdb")}
        for x in accepted_slice
        if x["type"] == "movie"
    ]
    shows = [
        {"imdb": x["ids"].get("imdb"), "tmdb": x["ids"].get("tmdb")}
        for x in accepted_slice
        if x["type"] == "show"
    ]
    movies = [{k: v for k, v in d.items() if v is not None} for d in movies]
    shows = [{k: v for k, v in d.items() if v is not None} for d in shows]
    payload: dict[str, Any] = {}
    if movies:
        payload["movies"] = movies
    if shows:
        payload["shows"] = shows
    return payload


def _freeze_not_found(
    not_found: Mapping[str, Any],
    *,
    action: str,
    unresolved: list[dict[str, Any]],
    add_details: bool,
) -> None:
    for bucket in ("movies", "shows"):
        for obj in not_found.get(bucket) or []:
            ids = {k: v for k, v in dict(obj or {}).items() if k in ("imdb", "tmdb")}
            typ = "movie" if bucket == "movies" else "show"
            minimal = id_minimal({"type": typ, "ids": ids})
            unresolved.append({"item": minimal, "hint": "not_found"})
            details = {"ids": ids} if add_details else None
            _freeze_item(minimal, action=action, reasons=[f"{action}:not-found"], details=details)


def _write(
    adapter: Any,
    action: str,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    cfg = _cfg(adapter)
    apikey = str(cfg.get("api_key") or "").strip()
    if not apikey:
        return 0, [{"item": id_minimal(it), "hint": "missing_api_key"} for it in (items or [])]
    batch = _cfg_int(cfg, "watchlist_batch_size", 100)
    freeze_details = _cfg_bool(cfg, "watchlist_freeze_details", True)
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
            sess,
            "POST",
            URL_MODIFY.format(action=action),
            params={"apikey": apikey},
            json=payload,
            timeout=adapter.cfg.timeout,
            max_retries=adapter.cfg.max_retries,
        )
        if r.status_code in (200, 201):
            d = r.json() if (r.text or "").strip() else {}
            added = d.get("added") or {}
            existing = d.get("existing") or {}
            removed = d.get("deleted") or d.get("removed") or {}
            if action == "add":
                ok += int(added.get("movies") or 0)
                ok += int(added.get("shows") or 0)
                ok += int(existing.get("movies") or 0)
                ok += int(existing.get("shows") or 0)
            else:
                ok += int(removed.get("movies") or 0)
                ok += int(removed.get("shows") or 0)
            nf = d.get("not_found") or {}
            _freeze_not_found(nf, action=action, unresolved=unresolved, add_details=freeze_details)
        else:
            text = (r.text or "")[:200]
            _log(f"{action.upper()} failed {r.status_code}: {text}")
            for x in sl:
                minimal = id_minimal({"type": x["type"], "ids": x["ids"]})
                unresolved.append({"item": minimal, "hint": f"http:{r.status_code}"})
                details = {"status": r.status_code} if freeze_details else None
                _freeze_item(minimal, action=action, reasons=[f"http:{r.status_code}"], details=details)
    if ok > 0:
        _shadow_bust()
    return ok, unresolved


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, "add", items)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, "remove", items)
