# /providers/sync/plex/_ratings.py
# Plex Module for ratings synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from ._common import normalize as plex_normalize, minimal_from_history_row
from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from

UNRESOLVED_PATH = "/config/.cw_state/plex_ratings.unresolved.json"

def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:ratings] {msg}")


def _emit(evt: dict[str, Any]) -> None:
    try:
        feature = str(evt.get("feature") or "?")
        head: list[str] = []
        if "event" in evt:
            head.append(f"event={evt['event']}")
        if "action" in evt:
            head.append(f"action={evt['action']}")
        tail = [f"{k}={v}" for k, v in evt.items() if k not in {"feature", "event", "action"}]
        line = " ".join(head + tail)
        print(f"[PLEX:{feature}] {line}", flush=True)
    except Exception:
        pass


# Config helpers
def _get_rating_workers(adapter: Any) -> int:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        n = int(plex.get("rating_workers", 0) or 0)
    except Exception:
        n = 0
    if n <= 0:
        try:
            n = int(os.environ.get("CW_PLEX_RATINGS_WORKERS", "12"))
        except Exception:
            n = 12
    return max(1, min(n, 64))


def _allowed_ratings_sec_ids(adapter: Any) -> set[str]:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        arr = ((plex.get("ratings") or {}).get("libraries") or [])
        return {str(int(x)) for x in arr if str(x).strip()}
    except Exception:
        return set()


def _plex_cfg(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    return cfg.get("plex", {}) if isinstance(cfg, dict) else {}


def _plex_cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    c = _plex_cfg(adapter)
    v = c.get(key, default) if isinstance(c, Mapping) else default
    return default if v is None else v


# Helpers for data normalization
def _as_epoch(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
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
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp())
    return None


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _norm_rating(v: Any) -> int | None:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None

    i = int(round(f))

    if i < 0:
        return None
    if i == 0:
        return 0
    if i > 10:
        i = 10
    return i

def _has_ext_ids(m: Mapping[str, Any]) -> bool:
    ids = (m.get("ids") if isinstance(m, Mapping) else None) or {}
    return bool(ids.get("imdb") or ids.get("tmdb") or ids.get("tvdb"))


# Unresolved storage helpers
def _load_unresolved() -> dict[str, Any]:
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
    k = canonical_key(id_minimal(it))
    return k or canonical_key(it) or ""


def _freeze_item(it: Mapping[str, Any], *, action: str, reasons: list[str]) -> None:
    key = _event_key(it)
    data = _load_unresolved()
    now = _iso(int(datetime.now(timezone.utc).timestamp()))
    entry = data.get(key) or {"feature": "ratings", "action": action, "first_seen": now, "attempts": 0}
    entry.update({"item": id_minimal(it), "last_attempt": now})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    entry["reasons"] = sorted(rset)
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)


def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for k in list(keys or []):
        if k in data:
            del data[k]
            changed = True
    if changed:
        _save_unresolved(data)


def _is_frozen(it: Mapping[str, Any]) -> bool:
    return _event_key(it) in _load_unresolved()


# Rating key resolution
def _resolve_rating_key(adapter: Any, it: Mapping[str, Any]) -> str | None:
    ids = ids_from(it)
    srv = getattr(adapter.client, "server", None)
    if not srv:
        return None

    rk = ids.get("plex")
    if rk:
        try:
            if srv.fetchItem(int(rk)):
                return str(rk)
        except Exception:
            pass

    kind = (it.get("type") or "movie").lower()
    is_episode = kind == "episode"
    is_season = kind == "season"

    title = (it.get("title") or "").strip()
    series_title = (it.get("series_title") or "").strip()
    query_title = series_title if (is_episode or is_season) and series_title else title
    if not query_title:
        return None

    year = it.get("year")
    season = it.get("season") or it.get("season_number")
    episode = it.get("episode") or it.get("episode_number")

    allow = _allowed_ratings_sec_ids(adapter)
    sec_types = ("show",) if (is_episode or is_season) else ("movie",)

    hits: list[Any] = []
    for sec in adapter.libraries(types=sec_types) or []:
        sid = str(getattr(sec, "key", "")).strip()
        if allow and sid not in allow:
            continue
        try:
            found = sec.search(title=query_title) or []
            hits.extend(found)
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

    if not hits:
        return None

    def _score(obj: Any) -> int:
        sc = 0
        try:
            ot = (getattr(obj, "grandparentTitle", None) if (is_episode or is_season) else getattr(obj, "title", None)) or ""
            if ot.strip().lower() == query_title.lower():
                sc += 3
            if not (is_episode or is_season) and year is not None and getattr(obj, "year", None) == year:
                sc += 2
            if is_episode:
                s_ok = (season is None) or (
                    getattr(obj, "seasonNumber", None) == season or getattr(obj, "parentIndex", None) == season
                )
                e_ok = (episode is None) or (getattr(obj, "index", None) == episode)
                if s_ok and e_ok:
                    sc += 2
            mids = plex_normalize(obj).get("ids") or {}
            for k in ("imdb", "tmdb", "tvdb"):
                if k in mids and k in ids and mids[k] == ids[k]:
                    sc += 4
        except Exception:
            pass
        return sc

    best = max(hits, key=_score)
    rk2 = getattr(best, "ratingKey", None)
    return str(rk2) if rk2 else None


def _rate(srv: Any, rating_key: Any, rating_1to10: int) -> bool:
    try:
        url = srv.url("/:/rate")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library", "rating": int(rating_1to10)}
        r = srv._session.get(url, params=params, timeout=10)
        return r.ok
    except Exception:
        return False


# Per-item fetch (plexapi)
def _fetch_one_rating(srv: Any, rk: str) -> dict[str, Any] | None:
    try:
        it = srv.fetchItem(int(rk))
    except Exception:
        return None

    r = _norm_rating(getattr(it, "userRating", None))
    if not r or r <= 0:
        return None

    m: dict[str, Any] = plex_normalize(it) or {}
    if not m:
        return None

    m["rating"] = r
    ts = _as_epoch(getattr(it, "lastRatedAt", None))
    if ts:
        m["rated_at"] = _iso(ts)

    t = (getattr(it, "type", None) or m.get("type") or "movie").lower()
    m["type"] = t if t in ("movie", "show", "season", "episode") else "movie"

    try:
        if m["type"] == "season":
            m["series_title"] = getattr(it, "parentTitle", None) or getattr(it, "grandparentTitle", None)
            m["season"] = getattr(it, "index", None)
        elif m["type"] == "episode":
            m["series_title"] = getattr(it, "grandparentTitle", None)
            m["season"] = getattr(it, "parentIndex", None)
            m["episode"] = getattr(it, "index", None)
    except Exception:
        pass

    if m["type"] in ("season", "episode") and not m.get("show_ids"):
        show_ids: dict[str, Any] = {}

        p_guid = getattr(it, "parentGuid", None) if m["type"] == "season" else getattr(it, "grandparentGuid", None)
        if p_guid:
            try:
                show_ids = ids_from(p_guid) or {}
            except Exception:
                show_ids = {}

        if not show_ids:
            prk = getattr(it, "parentRatingKey", None) if m["type"] == "season" else getattr(
                it, "grandparentRatingKey", None
            )
            if prk:
                try:
                    parent_obj = srv.fetchItem(int(prk))
                    parent_norm = plex_normalize(parent_obj) or {}
                    show_ids = (parent_norm.get("ids") or {}) if isinstance(parent_norm, Mapping) else {}
                except Exception:
                    show_ids = {}

        show_ids = {k: v for k, v in show_ids.items() if k in ("imdb", "tmdb", "tvdb") and v}
        if show_ids:
            m["show_ids"] = show_ids

    for key in ("season", "episode"):
        if m.get(key) is not None:
            try:
                m[key] = int(m[key])
            except Exception:
                pass

    if m.get("type") == "season":
        title = (m.get("title") or "").strip().lower()
        s_no = m.get("season")
        if title in ("season", f"season {s_no}".lower() if s_no is not None else "") and m.get("series_title"):
            m.setdefault("title", m["series_title"])

    return m


# Index
def build_index(adapter: Any, limit: int | None = None) -> dict[str, dict[str, Any]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        raise RuntimeError("PLEX server not bound")

    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("ratings") if callable(prog_mk) else None

    plex_cfg = _plex_cfg(adapter)
    if plex_cfg.get("fallback_GUID") or plex_cfg.get("fallback_guid"):
        _emit({"event": "debug", "msg": "fallback_guid.enabled", "provider": "PLEX", "feature": "ratings"})

    out: dict[str, dict[str, Any]] = {}
    added = 0
    scanned = 0

    allow = _allowed_ratings_sec_ids(adapter)
    keys: list[tuple[str, str]] = []

    for sec in adapter.libraries(types=("movie", "show")) or []:
        sid = str(getattr(sec, "key", "")).strip()
        if allow and sid not in allow:
            continue
        stype = (getattr(sec, "type", "") or "").lower()
        if stype == "movie":
            try:
                for mv in (sec.all() or []):
                    rk = getattr(mv, "ratingKey", None)
                    if rk:
                        keys.append(("movie", str(rk)))
            except Exception:
                pass
        else:
            try:
                for sh in (sec.all() or []):
                    rk_s = getattr(sh, "ratingKey", None)
                    if rk_s:
                        keys.append(("show", str(rk_s)))
                    try:
                        for sn in (sh.seasons() or []):
                            rk_sn = getattr(sn, "ratingKey", None)
                            if rk_sn:
                                keys.append(("season", str(rk_sn)))
                    except Exception:
                        pass
                    try:
                        for ep in (sh.episodes() or []):
                            rk_ep = getattr(ep, "ratingKey", None)
                            if rk_ep:
                                keys.append(("episode", str(rk_ep)))
                    except Exception:
                        pass
            except Exception:
                pass

    grand_total = len(keys)
    if prog is not None:
        try:
            prog.tick(0, total=grand_total, force=True)
        except Exception:
            pass

    workers = _get_rating_workers(adapter)
    fallback_guid = bool(
        _plex_cfg_get(adapter, "fallback_GUID", False)
        or _plex_cfg_get(adapter, "fallback_guid", False)
    )

    fb_try = 0
    fb_ok = 0

    def _tick() -> None:
        if prog is not None:
            try:
                prog.tick(scanned, total=grand_total)
            except Exception:
                pass

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_one_rating, srv, rk): (typ, rk) for typ, rk in keys}
        for fut in as_completed(futs):
            typ, rk = futs[fut]
            scanned += 1
            try:
                m = fut.result()
            except Exception:
                m = None

            if m:
                if fallback_guid and not _has_ext_ids(m):
                    fb_try += 1
                    _emit(
                        {
                            "event": "fallback_guid",
                            "provider": "PLEX",
                            "feature": "ratings",
                            "action": "enrich_try",
                            "rk": rk,
                        }
                    )
                    fb = minimal_from_history_row(m, allow_discover=True)
                    ids_fb: dict[str, Any] = {}
                    show_ids_fb: dict[str, Any] = {}
                    if isinstance(fb, Mapping):
                        ids_fb = dict(fb.get("ids") or {})
                        show_ids_fb = dict(fb.get("show_ids") or {})
                    ok_enrich = bool(ids_fb or show_ids_fb)
                    _emit(
                        {
                            "event": "fallback_guid",
                            "provider": "PLEX",
                            "feature": "ratings",
                            "action": "enrich_ok" if ok_enrich else "enrich_miss",
                            "rk": rk,
                        }
                    )
                    if ok_enrich:
                        fb_ok += 1
                        ids0 = dict(m.get("ids") or {})
                        if ids_fb:
                            ids0.update({k: v for k, v in ids_fb.items() if v})
                            m["ids"] = ids0
                        if show_ids_fb:
                            si0 = dict(m.get("show_ids") or {})
                            si0.update({k: v for k, v in show_ids_fb.items() if v})
                            m["show_ids"] = si0

                if typ in ("movie", "show", "season", "episode"):
                    m["type"] = typ
                out[canonical_key(m)] = m
                added += 1

            _tick()
            if limit is not None and added >= limit:
                _log(f"index truncated at {limit}")
                if prog is not None:
                    try:
                        prog.done(ok=True, total=grand_total)
                    except Exception:
                        pass
                return out

    if prog is not None:
        try:
            prog.done(ok=True, total=grand_total)
        except Exception:
            pass

    _log(
        f"index size: {len(out)} (added={added}, scanned={grand_total}, fb_try={fb_try}, fb_ok={fb_ok})"
    )

    # additional debug snapshot - needs to be enabled manually
    if os.getenv("CW_PLEX_SNAPSHOT_DEBUG"):
        try:
            from pathlib import Path

            snap: dict[str, Any] = {}
            for _k, v in out.items():
                base = id_minimal(v)
                base["rating"] = v.get("rating")
                base["rated_at"] = v.get("rated_at")
                key = canonical_key(base)
                if not key:
                    continue
                snap[key] = base

            p = Path("/config/.cw_state/plex.ratings.snapshot.json")
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(
                json.dumps(snap, ensure_ascii=False, indent=2, sort_keys=True),
                "utf-8",
            )
            _log(f"ratings snapshot written: {p}")
        except Exception as exc:
            _log(f"ratings snapshot dump failed: {exc}")

    return out

def _get_existing_rating(srv: Any, rating_key: Any) -> int | None:
    try:
        it = srv.fetchItem(int(rating_key))
    except Exception:
        return None
    return _norm_rating(getattr(it, "userRating", None))


# Add
def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        unresolved: list[dict[str, Any]] = []
        for it in items:
            _freeze_item(it, action="add", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(it), "hint": "no_plex_server"})
        _log("add skipped: no PMS bound")
        return 0, unresolved

    ok = 0
    unresolved: list[dict[str, Any]] = []

    for it in items:
        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        rating = _norm_rating(it.get("rating"))
        if rating is None or rating <= 0:
            _freeze_item(it, action="add", reasons=["missing_or_invalid_rating"])
            unresolved.append({"item": id_minimal(it), "hint": "missing_or_invalid_rating"})
            continue

        rk = _resolve_rating_key(adapter, it)
        if not rk:
            _freeze_item(it, action="add", reasons=["not_in_library"])
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            continue

        existing = _get_existing_rating(srv, rk)
        if existing is not None and existing == rating:
            _log(f"skip rate: same rating for {id_minimal(it).get('title')}")
            _unfreeze_keys_if_present([_event_key(it)])
            continue

        if _rate(srv, rk, rating):
            ok += 1
            _unfreeze_keys_if_present([_event_key(it)])
        else:
            _freeze_item(it, action="add", reasons=["rate_failed"])
            unresolved.append({"item": id_minimal(it), "hint": "rate_failed"})

    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

# Remove
def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        unresolved: list[dict[str, Any]] = []
        for it in items:
            _freeze_item(it, action="remove", reasons=["no_plex_server"])
            unresolved.append({"item": id_minimal(it), "hint": "no_plex_server"})
        _log("remove skipped: no PMS bound")
        return 0, unresolved

    ok = 0
    unresolved: list[dict[str, Any]] = []

    for it in items:
        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        rk = _resolve_rating_key(adapter, it)
        if not rk:
            _freeze_item(it, action="remove", reasons=["not_in_library"])
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            continue

        if _rate(srv, rk, 0):
            ok += 1
            _unfreeze_keys_if_present([_event_key(it)])
        else:
            _freeze_item(it, action="remove", reasons=["clear_failed"])
            unresolved.append({"item": id_minimal(it), "hint": "clear_failed"})

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved