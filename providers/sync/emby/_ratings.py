# /providers/sync/emby/_ratings.py
from __future__ import annotations
import os, json, time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

from ._common import normalize as emby_normalize
from ._common import emby_scope_ratings
from ._common import resolve_item_id

UNRESOLVED_PATH = "/config/.cw_state/emby_ratings.unresolved.json"

# === Debug toggles ===

def _dbg_on() -> bool:
    return bool(os.environ.get("CW_EMBY_DEBUG") or os.environ.get("CW_DEBUG"))

def _log(msg: str) -> None:
    if _dbg_on(): print(f"[EMBY:ratings] {msg}")

# === Unresolved store ===

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

# === Config helpers ===

def _limit(adapter) -> int:
    v = getattr(getattr(adapter, "cfg", None), "ratings_query_limit", None)
    if v is None:
        v = getattr(getattr(adapter, "cfg", None), "ratings_query_page", 500)
    try: return max(50, int(v))
    except Exception: return 500

def _like_threshold(adapter) -> float:
    try:
        t = getattr(getattr(adapter, "cfg", None), "ratings_like_threshold", 6.0)
        return float(t)
    except Exception:
        return 6.0

def _delay_ms(adapter) -> int:
    v = getattr(getattr(adapter, "cfg", None), "ratings_write_delay_ms", 0)
    try: return max(0, int(v))
    except Exception: return 0

def _write_like_enabled(adapter) -> bool:
    try: return bool(getattr(getattr(adapter, "cfg", None), "ratings_write_like", True))
    except Exception: return True

def _write_numeric_enabled(adapter) -> bool:
    try: return bool(getattr(getattr(adapter, "cfg", None), "ratings_write_numeric", True))
    except Exception: return True

# === HTTP helpers ===

def _body_snip(r, n: int = 240) -> str:
    try:
        t = r.text if isinstance(getattr(r, "text", None), str) else (r.text() if callable(getattr(r, "text", None)) else "")
        return (t[:n] + "…") if t and len(t) > n else (t or "no-body")
    except Exception:
        return "no-body"

# === Minimal ID helpers (parity with history) ===

def _series_ids_for(http, series_id: Optional[str]) -> Dict[str, str]:
    sid = (str(series_id or "").strip()) or ""
    if not sid: return {}
    try:
        r = http.get(f"/Items/{sid}", params={"Fields": "ProviderIds,ProductionYear"})
        if getattr(r, "status_code", 0) != 200: return {}
        body = r.json() or {}
        pids = (body.get("ProviderIds") or {})
        out: Dict[str, str] = {}
        for k, v in (pids.items() if isinstance(pids, dict) else []):
            kl = str(k).lower(); sv = str(v).strip()
            if not sv: continue
            if   kl == "imdb": out["imdb"] = sv if sv.startswith("tt") else f"tt{sv}"
            elif kl == "tmdb":
                try: out["tmdb"] = str(int(sv))
                except Exception: pass
            elif kl == "tvdb":
                try: out["tvdb"] = str(int(sv))
                except Exception: pass
        return out
    except Exception:
        return {}

def _item_ids_for(http, item_id: Optional[str]) -> Dict[str, str]:
    iid = (str(item_id or '').strip()) or ''
    if not iid: return {}
    try:
        r = http.get(f"/Items/{iid}", params={"Fields": "ProviderIds,ProductionYear"})
        if getattr(r, "status_code", 0) != 200: return {}
        body = r.json() or {}
        pids = (body.get("ProviderIds") or {})
        out: Dict[str, str] = {}
        for k, v in (pids.items() if isinstance(pids, dict) else []):
            kl = str(k).lower(); sv = str(v).strip()
            if not sv: continue
            if   kl == "imdb": out["imdb"] = sv if sv.startswith("tt") else f"tt{sv}"
            elif kl in ("tmdb", "tvdb"):
                try: out[kl] = str(int(sv))
                except Exception: pass
        return out
    except Exception:
        return {}

# === Emby write helpers (thumbs + numeric) ===

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

def _set_numeric_rating(http, uid: str, item_id: str, *, rating: Optional[float]) -> bool:
    try:
        if rating is None:
            r = http.delete(f"/Users/{uid}/Items/{item_id}/UserRating")
            if getattr(r, "status_code", 0) in (200, 204):
                return True
            r2 = http.post(f"/Users/{uid}/Items/{item_id}/UserData", json={"UserRating": None})
            if getattr(r2, "status_code", 0) in (200, 204):
                return True
            return False

        try:
            rf = float(rating)
        except Exception:
            return False
        if rf < 0: rf = 0.0
        if rf > 10: rf = 10.0
        iv = int(round(rf))

        r = http.post(f"/Users/{uid}/Items/{item_id}/UserRating", params={"userRating": iv})
        if getattr(r, "status_code", 0) in (200, 204):
            return True

        r2 = http.post(f"/Users/{uid}/Items/{item_id}/UserRating", params={"rating": iv})
        if getattr(r2, "status_code", 0) in (200, 204):
            return True

        r3 = http.post(f"/Users/{uid}/Items/{item_id}/UserData", json={"UserRating": rf, "Rating": rf})
        if getattr(r3, "status_code", 0) in (200, 204):
            return True

        return False
    except Exception as e:
        if _dbg_on(): _log(f"numeric write exception item={item_id} err={e!r}")
        return False

# === Index (reads) — likes mapped to 10.0/0.0; numeric normalized to 10-pt ===

def build_index(adapter) -> Dict[str, Dict[str, Any]]:
    """
    Build a map of canonical key -> normalized item containing a `rating` on a 10-pt scale.
    Presence rule:
      - include if UserData.UserRating (or fallback) is set, OR if UserData.Likes is not null.
      - Likes True  -> rating 10.0
      - Likes False -> rating 0.0
      - Otherwise numeric (0..10 as written), <=5 doubled to maintain compatibility with 5-star inputs.
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
            "HasUserData": True,  # safe filter; server may ignore
            "Fields": "ProviderIds,ProductionYear,UserData,UserRating,Type,IndexNumber,ParentIndexNumber,SeriesName,SeriesId,Name,ParentId",
            "StartIndex": start,
            "Limit": page,
            "EnableTotalRecordCount": False,
            "SortBy": "SortName",
            "SortOrder": "Ascending",
        }
        params.update(emby_scope_ratings(adapter.cfg))

        r = http.get(f"/Users/{uid}/Items", params=params)
        try:
            body = r.json() or {}
            rows = body.get("Items") or []
        except Exception:
            rows = []
        if not rows: break

        for row in rows:
            total_seen += 1
            ud = row.get("UserData") or {}

            # --- thumbs present? (true or false both mean "has user data")
            liked = ud.get("Likes", None)
            if liked is None:
                liked = ud.get("Like", None)
            if liked is None:
                liked = ud.get("IsLiked", None)
            has_like = (liked is True) or (liked is False)

            # --- numeric rating: prefer UserData.UserRating, then item-level, then UD.Rating as a last fallback
            numeric = ud.get("UserRating", None)
            if numeric is None:
                numeric = row.get("UserRating", None)
            if numeric is None:
                numeric = ud.get("Rating", None)

            try:
                rf = float(numeric) if numeric is not None else None
                has_numeric = (rf is not None and rf >= 0.0)
            except Exception:
                rf = None
                has_numeric = False

            if not (has_like or has_numeric):
                continue  # nothing rated by this user

            try:
                m = emby_normalize(row)
                typ = (row.get("Type") or "").strip()

                # rating mapping precedence: thumbs (either way) > numeric
                if has_like:
                    # True → 10.0, False → 0.0
                    m["rating"] = 10.0 if (liked is True) else 0.0
                elif rf is not None:
                    m["rating"] = float(rf) * (2.0 if float(rf) <= 5.0 else 1.0)

                if typ == "Movie":
                    ids = dict(m.get("ids") or {})
                    if not any(k in ids for k in ("imdb", "tmdb", "tvdb")):
                        extra = _item_ids_for(http, row.get("Id"))
                        if extra:
                            ids.update(extra); m["ids"] = ids

                elif typ == "Series":
                    ids = dict(m.get("ids") or {})
                    if not any(k in ids for k in ("imdb", "tmdb", "tvdb")):
                        extra = _series_ids_for(http, row.get("Id")) or _item_ids_for(http, row.get("Id"))
                        if extra:
                            ids.update(extra); m["ids"] = ids
                    m["type"] = m.get("type") or "show"

                elif typ == "Episode":
                    sid = row.get("SeriesId") or row.get("ParentId")
                    show_ids = dict(m.get("show_ids") or {})
                    if not any(k in show_ids for k in ("imdb", "tmdb", "tvdb")):
                        show_ids = _series_ids_for(http, sid) or _item_ids_for(http, sid)
                        if show_ids:
                            m["show_ids"] = show_ids

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

# === Apply (writes) — numeric and/or thumbs, threshold policy preserved ===

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Writes numeric and/or thumbs ratings to Emby:
      • If a numeric 'rating' is present and ratings_write_numeric=True → set UserRating (0–10).
      • If a boolean 'liked' is present, honor it directly for thumbs.
      • Otherwise, derive thumbs from numeric via ratings_like_threshold.
      • We only ever write thumbs-up or clear; thumbs-down is represented in reads if present.
    """
    http = adapter.client; uid = adapter.cfg.user_id
    thresh = float(_like_threshold(adapter))
    do_like = _write_like_enabled(adapter)
    do_num  = _write_numeric_enabled(adapter)
    delay   = _delay_ms(adapter)

    try:
        from ._common import provider_index
        provider_index(adapter)
    except Exception:
        pass

    ok = 0
    unresolved: List[Dict[str, Any]] = []

    stats = {
        "numeric_set": 0, "numeric_cleared": 0,
        "thumbs_set": 0, "thumbs_cleared": 0,
        "invalid_rating": 0, "resolve_failed": 0, "write_failed": 0
    }

    for it in items or []:
        liked_flag = it.get("liked", None)
        rating_val = it.get("rating", None)

        likes: Optional[bool] = None
        rf: Optional[float] = None

        if rating_val is not None:
            try:
                rf = float(rating_val)
            except Exception:
                unresolved.append({"item": id_minimal(it), "hint": "invalid_rating"})
                _freeze(it, reason="invalid_rating")
                stats["invalid_rating"] += 1
                continue

        if isinstance(liked_flag, bool):
            likes = liked_flag
        elif rf is not None:
            likes = (rf >= thresh)

        iid = resolve_item_id(adapter, it)
        if not iid:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            _freeze(it, reason="resolve_failed")
            stats["resolve_failed"] += 1
            continue

        wrote = False

        if do_num and (rf is not None or liked_flag is not None):
            num_ok = _set_numeric_rating(http, uid, iid, rating=rf)
            if num_ok:
                wrote = True
                if rf is None:
                    stats["numeric_cleared"] += 1
                else:
                    stats["numeric_set"] += 1

        if do_like and (likes is not None or rf is not None):
            # policy: derive thumbs-up from threshold; if not met, clear
            like_ok = _set_like(http, uid, iid, likes=True if likes else None)
            if like_ok:
                wrote = True
                if likes:
                    stats["thumbs_set"] += 1
                else:
                    stats["thumbs_cleared"] += 1

        if wrote:
            ok += 1
            _thaw_if_present([canonical_key(id_minimal(it))])
        else:
            unresolved.append({"item": id_minimal(it), "hint": "rate_failed"})
            _freeze(it, reason="write_failed")
            stats["write_failed"] += 1

        if delay:
            try: time.sleep(delay / 1000.0)
            except Exception: pass

    _log(
        "add done: "
        f"+{ok} / unresolved {len(unresolved)} | "
        f"numeric_set={stats['numeric_set']} numeric_cleared={stats['numeric_cleared']} "
        f"thumbs_set={stats['thumbs_set']} thumbs_cleared={stats['thumbs_cleared']} "
        f"invalid_rating={stats['invalid_rating']} resolve_failed={stats['resolve_failed']} "
        f"write_failed={stats['write_failed']}"
    )
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

        like_ok = _set_like(http, uid, iid, likes=None)
        num_ok  = _set_numeric_rating(http, uid, iid, rating=None)
        if like_ok or num_ok:
            ok += 1
            _thaw_if_present([canonical_key(id_minimal(it))])
        else:
            unresolved.append({"item": id_minimal(it), "hint": "clear_failed"})
            _freeze(it, reason="write_failed")

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved
