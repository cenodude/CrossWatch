# /providers/sync/emby/_ratings.py
from __future__ import annotations
import os, json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

from ._common import normalize as emby_normalize
from ._common import emby_scope_ratings
from ._common import resolve_item_id  # resolved once here for parity

UNRESOLVED_PATH = "/config/.cw_state/emby_ratings.unresolved.json"

def _dbg_on() -> bool:
    return bool(os.environ.get("CW_EMBY_DEBUG") or os.environ.get("CW_DEBUG"))

def _log(msg: str) -> None:
    if _dbg_on(): print(f"[EMBY:ratings] {msg}")

# -- unresolved store ----------------------------------------------------------

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
    data = _load(); changed = False
    for k in list(keys or []):
        if k in data:
            data.pop(k, None); changed = True
    if changed:
        _save(data)

# -- cfg -----------------------------------------------------------------------

def _limit(adapter) -> int:
    v = getattr(getattr(adapter, "cfg", None), "ratings_query_limit", None)
    if v is None:
        v = getattr(getattr(adapter, "cfg", None), "ratings_query_page", 500)
    try: return max(50, int(v))
    except Exception: return 500

def _like_threshold(adapter) -> float:
    # Simple numeric→thumbs policy; default 6.0 (>=6 is Like, <6 clears)
    try:
        t = getattr(getattr(adapter, "cfg", None), "ratings_like_threshold", 6.0)
        return float(t)
    except Exception:
        return 6.0

# -- http helpers --------------------------------------------------------------

def _body_snip(r, n: int = 240) -> str:
    try:
        t = r.text if isinstance(getattr(r, "text", None), str) else (r.text() if callable(getattr(r, "text", None)) else "")
        return (t[:n] + "…") if t and len(t) > n else (t or "no-body")
    except Exception:
        return "no-body"
        
# Emby thumbs API:
#   Set    : POST /Users/{UserId}/Items/{ItemId}/Rating?Likes=true|false
#   Clear  : DELETE /Users/{UserId}/Items/{ItemId}/Rating
def _set_like(http, uid: str, item_id: str, *, likes: Optional[bool]) -> bool:
    try:
        if likes is None:
            r = http.delete(f"/Users/{uid}/Items/{item_id}/Rating")
            ok = getattr(r, "status_code", 0) in (200, 204)
            if not ok and _dbg_on(): _log(f"clear failed status={getattr(r,'status_code',None)} body={_body_snip(r)}")
            return ok
        r = http.post(f"/Users/{uid}/Items/{item_id}/Rating", params={"Likes": "true" if likes else "false"})
        ok = getattr(r, "status_code", 0) in (200, 204)
        if not ok and _dbg_on(): _log(f"thumbs write failed status={getattr(r,'status_code',None)} body={_body_snip(r)}")
        return ok
    except Exception as e:
        if _dbg_on(): _log(f"thumbs write exception item={item_id} err={e!r}")
        return False

# -- index (likes only; numeric if present is mapped to 10.0) ------------------

def build_index(adapter) -> Dict[str, Dict[str, Any]]:
    """
    Returns map[key] -> {type,title,year,ids,..., rating}
    For Emby, we index user Likes as rating=10.0 for parity with numeric sinks.
    """
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("ratings") if callable(prog_mk) else None

    http = adapter.client
    uid  = adapter.cfg.user_id
    page = _limit(adapter)
    start = 0

    out: Dict[str, Dict[str, Any]] = {}
    total_seen = 0

    while True:
        params: Dict[str, Any] = {
            "UserId": uid,
            "Recursive": True,
            "IncludeItemTypes": "Movie,Series,Episode",
            "EnableUserData": True,
            "Fields": "ProviderIds,ProductionYear,UserData,UserRating,Type,IndexNumber,ParentIndexNumber,SeriesName,Name,ParentId",
            "StartIndex": start,
            "Limit": page,
            "EnableTotalRecordCount": False,
            "SortBy": "SortName",
            "SortOrder": "Ascending",
        }
        params.update(emby_scope_ratings(adapter.cfg))

        r = http.get(f"/Users/{uid}/Items", params=params)
        body = r.json() or {}
        rows = body.get("Items") or []
        if not rows: break

        for row in rows:
            total_seen += 1
            ud = row.get("UserData") or {}
            liked = ud.get("Likes")
            if liked is None:
                liked = ud.get("Like", None)
                if liked is None:
                    liked = ud.get("IsLiked", None)

            numeric = row.get("UserRating")
            if numeric is None:
                numeric = ud.get("Rating")

            has_like = (liked is True)
            has_numeric = False
            try:
                rf = float(numeric) if numeric is not None else None
                has_numeric = (rf is not None and rf > 0.0)
            except Exception:
                rf = None
                has_numeric = False

            if not (has_like or has_numeric):
                continue

            try:
                m = emby_normalize(row)
                # Normalize to 10pt scale for parity; likes become 10.0
                if has_like:
                    m["rating"] = 10.0
                elif rf is not None:
                    # If server gives 0..5, upscale; if 0..10, leave (best-effort)
                    m["rating"] = float(rf) * (2.0 if float(rf) <= 5.0 else 1.0)

                k = canonical_key(m)
                emby_new = str((m.get("ids") or {}).get("emby") or row.get("Id") or "")
                prev = out.get(k)
                if not prev:
                    out[k] = m
                else:
                    emby_prev = str((prev.get("ids") or {}).get("emby") or "")
                    if emby_new and emby_prev and emby_new < emby_prev:
                        out[k] = m
            except Exception:
                pass

        start += len(rows)
        if prog:
            try: prog.tick(total_seen, total=max(total_seen, start))
            except Exception: pass
        if len(rows) < page: break

    if prog:
        try: prog.done(ok=True, total=len(out))
        except Exception: pass

    _thaw_if_present(out.keys())
    _log(f"index size: {len(out)}")
    return out

# -- writes (numeric -> thumbs policy) ----------------------------------------

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Numeric ratings are mapped to thumbs:
      - rating >= like_threshold → Like
      - rating <  like_threshold → Clear rating
    If an item carries 'liked' bool, it is honored directly.
    """
    http = adapter.client; uid = adapter.cfg.user_id
    thresh = float(_like_threshold(adapter))
    ok = 0; unresolved: List[Dict[str, Any]] = []

    for it in items or []:
        liked_flag = it.get("liked", None)
        rating = it.get("rating", None)

        likes: Optional[bool]
        if isinstance(liked_flag, bool):
            likes = liked_flag
        else:
            try:
                rf = float(rating) if rating is not None else None
                likes = (rf is not None and rf >= thresh)
            except Exception:
                unresolved.append({"item": id_minimal(it), "hint": "invalid_rating"})
                _freeze(it, reason="invalid_rating")
                continue

        iid = resolve_item_id(adapter, it)
        if not iid:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            _freeze(it, reason="resolve_failed")
            continue

        if _set_like(http, uid, iid, likes=True if likes else None):
            ok += 1
            _thaw_if_present([canonical_key(id_minimal(it))])
        else:
            unresolved.append({"item": id_minimal(it), "hint": "rate_failed"})
            _freeze(it, reason="write_failed")

    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    http = adapter.client; uid = adapter.cfg.user_id
    ok = 0; unresolved: List[Dict[str, Any]] = []

    for it in items or []:
        iid = resolve_item_id(adapter, it)
        if not iid:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            _freeze(it, reason="resolve_failed")
            continue

        if _set_like(http, uid, iid, likes=None):
            ok += 1
            _thaw_if_present([canonical_key(id_minimal(it))])
        else:
            unresolved.append({"item": id_minimal(it), "hint": "clear_failed"})
            _freeze(it, reason="write_failed")

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved
