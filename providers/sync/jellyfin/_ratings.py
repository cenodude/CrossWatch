# /providers/sync/jellyfin/_ratings.py
from __future__ import annotations
import os, json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from cw_platform.id_map import minimal as id_minimal, canonical_key
from ._common import normalize as jelly_normalize
from ._common import jf_scope_ratings, jf_get_library_roots, jf_resolve_library_id

UNRESOLVED_PATH = "/config/.cw_state/jellyfin_ratings.unresolved.json"
SHADOW_PATH = "/config/.cw_state/jellyfin_ratings.shadow.json"

# --- shadow (presence echo) ---------------------------------------------------

def _shadow_load() -> Dict[str, int]:
    try:
        with open(SHADOW_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
            return {str(k): int(v) for k, v in raw.items()}
    except Exception:
        return {}

def _shadow_save(d: Mapping[str, int]) -> None:
    try:
        os.makedirs(os.path.dirname(SHADOW_PATH), exist_ok=True)
        with open(SHADOW_PATH, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass


# --- logging -----------------------------------------------------------------

def _dbg_on() -> bool:
    return bool(os.environ.get("CW_JELLYFIN_DEBUG") or os.environ.get("CW_DEBUG"))

def _log(msg: str) -> None:
    if _dbg_on():
        print(f"[JELLYFIN:ratings] {msg}")


# --- unresolved store --------------------------------------------------------

def _load() -> Dict[str, Any]:
    try:
        with open(UNRESOLVED_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _save(obj: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(UNRESOLVED_PATH), exist_ok=True)
        with open(UNRESOLVED_PATH, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass

def _freeze(item: Mapping[str, Any], *, reason: str) -> None:
    key = canonical_key(item)
    data = _load()
    ent = data.get(key) or {"feature": "ratings", "attempts": 0}
    ent.update({"hint": id_minimal(item)})
    ent["attempts"] = int(ent.get("attempts", 0)) + 1
    ent["reason"] = reason
    data[key] = ent
    _save(data)

def _thaw_if_present(keys: Iterable[str]) -> None:
    data = _load()
    changed = False
    for k in list(keys or []):
        if k in data:
            data.pop(k, None)
            changed = True
    if changed:
        _save(data)


# --- cfg ---------------------------------------------------------------------

def _limit(adapter) -> int:
    v = getattr(getattr(adapter, "cfg", None), "ratings_query_page", None)
    if v is None:
        v = 500
    try:
        return max(50, int(v))
    except Exception:
        return 500


# --- http helpers ------------------------------------------------------------

def _body_snip(r, n: int = 240) -> str:
    try:
        t = r.text() if callable(getattr(r, "text", None)) else getattr(r, "text", "")
        return (t[:n] + "…") if t and len(t) > n else (t or "no-body")
    except Exception:
        return "no-body"


# --- low-level write ---------------------------------------------------------
# Numeric rating 0..10; accepts 0..5 upscale.

def _rate(http, uid: str, item_id: str, rating: Optional[float]) -> bool:
    try:
        payload: Dict[str, Any] = {}
        if rating is None:
            payload["Rating"] = None
        else:
            r = float(rating)
            if 0.0 <= r <= 5.0:
                r *= 2.0
            r = max(0.0, min(10.0, r))
            payload["Rating"] = round(r, 1)

        r1 = http.post(f"/UserItems/{item_id}/UserData", params={"userId": uid}, json=payload)
        ok = getattr(r1, "status_code", 0) in (200, 204)
        if ok:
            return True

        r2 = http.post(f"/Users/{uid}/Items/{item_id}/UserData", json=payload)
        ok2 = getattr(r2, "status_code", 0) in (200, 204)

        if not ok2 and _dbg_on():
            _log(
                f"write failed user={uid} item={item_id} "
                f"status={getattr(r1,'status_code',None)}/{getattr(r2,'status_code',None)} "
                f"body={_body_snip(r1) if getattr(r1,'status_code',0) not in (200,204) else _body_snip(r2)}"
            )
        return ok2
    except Exception as e:
        if _dbg_on():
            _log(f"write exception item={item_id} err={e!r}")
        return False


# --- index builder -----------------------------------------------------------

def build_index(adapter) -> Dict[str, Dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("ratings") if callable(prog_mk) else None

    http = adapter.client
    uid = adapter.cfg.user_id
    page = _limit(adapter)
    start = 0

    scope_params = jf_scope_ratings(adapter.cfg)
    scope_libs: List[str] = []
    if isinstance(scope_params, Mapping):
        pid = scope_params.get("ParentId")
        if pid:
            scope_libs = [str(pid)]
        else:
            anc = scope_params.get("AncestorIds")
            if isinstance(anc, (list, tuple)):
                scope_libs = [str(x) for x in anc if x]

    roots = jf_get_library_roots(adapter)

    out: Dict[str, Dict[str, Any]] = {}
    total_seen = 0

    while True:
        params: Dict[str, Any] = {
            "userId": uid,
            "recursive": True,
            "includeItemTypes": "Movie,Series,Episode",
            "enableUserData": True,
            "fields": (
                "ProviderIds,ProductionYear,UserData,UserRating,Type,"
                "IndexNumber,ParentIndexNumber,SeriesName,Name,"
                "ParentId,LibraryId,AncestorIds"
            ),
            "startIndex": start,
            "limit": page,
            "enableTotalRecordCount": True,
            "hasUserRating": True,
            "sortBy": "SortName",
            "sortOrder": "Ascending",
        }
        if scope_params:
            params.update(scope_params)

        r = http.get(f"/Users/{uid}/Items", params=params)
        body = r.json() or {}
        rows = body.get("Items") or []
        if not rows:
            break

        for row in rows:
            total_seen += 1
            ud = row.get("UserData") or {}
            rating = row.get("UserRating")
            if rating is None:
                rating = ud.get("Rating")
            try:
                rf = float(rating)
            except (TypeError, ValueError):
                continue
            if rf <= 0.0:
                continue

            try:
                m = jelly_normalize(row)
                lib_id = jf_resolve_library_id(row, roots, scope_libs, http)
                m = dict(m)
                m["library_id"] = lib_id
                m["rating"] = round(rf, 1)

                k = canonical_key(m)
                jf_new = str((m.get("ids") or {}).get("jellyfin") or row.get("Id") or "")

                prev = out.get(k)
                if not prev:
                    out[k] = m
                else:
                    jf_prev = str((prev.get("ids") or {}).get("jellyfin") or "")
                    if jf_new and jf_prev and jf_new < jf_prev:
                        out[k] = m
            except Exception:
                pass

        start += len(rows)
        if prog:
            try:
                prog.tick(total_seen, total=max(total_seen, start))
            except Exception:
                pass
        if len(rows) < page:
            break

    if prog:
        try:
            prog.done(ok=True, total=len(out))
        except Exception:
            pass

    if _dbg_on():
        seen_libs: Dict[str, int] = {}
        for m in out.values():
            lid = m.get("library_id") or "NONE"
            lid_s = str(lid)
            seen_libs[lid_s] = seen_libs.get(lid_s, 0) + 1
        _log(f"library_id distribution: {seen_libs}")

    shadow = _shadow_load()
    if shadow:
        added = 0
        for k in shadow.keys():
            if k not in out:
                out.setdefault(k, {"shadow": True})
                added += 1
        if added:
            _log(f"shadow merged: +{added}")

    _thaw_if_present(out.keys())
    _log(f"index size: {len(out)}")
    return out

# --- writes ------------------------------------------------------------------
def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    ok = 0
    unresolved: List[Dict[str, Any]] = []
    shadow = _shadow_load()

    from ._common import resolve_item_id

    for it in items or []:
        base: Dict[str, Any] = dict(it or {})
        base_ids = base.get("ids") if isinstance(base.get("ids"), dict) else {}
        has_ids = bool(base_ids) and any(v not in (None, "", 0) for v in base_ids.values())

        rating = base.get("rating")
        try:
            rf = float(rating)
        except Exception:
            unresolved.append({"item": id_minimal(base), "hint": "invalid_rating"})
            _freeze(base, reason="invalid_rating")
            continue

        # If ids are weak, normalize to try to fill them.
        m = jelly_normalize(base) if not has_ids else base

        try:
            k = canonical_key(m) or canonical_key(base)
        except Exception:
            k = None
        if not k:
            unresolved.append({"item": id_minimal(base), "hint": "missing_ids_for_key"})
            _freeze(base, reason="missing_ids_for_key")
            continue

        iid = resolve_item_id(adapter, m)
        if not iid:
            unresolved.append({"item": id_minimal(m), "hint": "not_in_library"})
            _freeze(m, reason="resolve_failed")
            continue

        if _rate(http, uid, iid, rf):
            ok += 1
            shadow[k] = int(shadow.get(k, 0)) + 1
            _thaw_if_present([k])
        else:
            unresolved.append({"item": id_minimal(m), "hint": "rate_failed"})
            _freeze(m, reason="write_failed")

    shadow = {k: v for k, v in shadow.items() if v > 0}
    _shadow_save(shadow)

    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    ok = 0
    unresolved: List[Dict[str, Any]] = []
    shadow = _shadow_load()

    from ._common import resolve_item_id

    for it in items or []:
        base: Dict[str, Any] = dict(it or {})
        base_ids = base.get("ids") if isinstance(base.get("ids"), dict) else {}
        has_ids = bool(base_ids) and any(v not in (None, "", 0) for v in base_ids.values())

        m = jelly_normalize(base) if not has_ids else base

        try:
            k = canonical_key(m) or canonical_key(base)
        except Exception:
            k = None
        if not k:
            unresolved.append({"item": id_minimal(base), "hint": "missing_ids_for_key"})
            _freeze(base, reason="missing_ids_for_key")
            continue

        iid = resolve_item_id(adapter, m)
        if not iid:
            unresolved.append({"item": id_minimal(m), "hint": "not_in_library"})
            _freeze(m, reason="resolve_failed")
            continue

        if _rate(http, uid, iid, None):
            ok += 1
            cur = int(shadow.get(k, 0))
            nxt = max(0, cur - 1)
            if nxt > 0:
                shadow[k] = nxt
            else:
                shadow.pop(k, None)
            _thaw_if_present([k])
        else:
            unresolved.append({"item": id_minimal(m), "hint": "clear_failed"})
            _freeze(m, reason="write_failed")

    shadow = {k: v for k, v in shadow.items() if v > 0}
    _shadow_save(shadow)

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved