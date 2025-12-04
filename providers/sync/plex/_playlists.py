# /providers/sync/plex/_playlists.py

from __future__ import annotations
import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from ._common import normalize as plex_normalize
from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:playlists] {msg}")

# ── index ─────────────────────────────────────────────────────────────────────

def build_index(adapter, include_names: Optional[Iterable[str]] = None) -> Dict[str, Dict[str, Any]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        _log("no PMS bound (account-only) → empty playlists index")
        return {}

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("playlists") if callable(prog_mk) else None

    names = {n.strip().lower() for n in (include_names or [])}
    out: Dict[str, Dict[str, Any]] = {}
    work: List[Tuple[Any, List[Any]]] = []
    total = 0
    try:
        playlists = list(srv.playlists() or [])
        for pl in playlists:
            if getattr(pl, "playlistType", "video") != "video":
                continue
            title = (getattr(pl, "title", None) or "").strip()
            if names and title.lower() not in names:
                continue
            try:
                items = list(pl.items() or [])
            except Exception:
                items = []
            work.append((pl, items))
            total += len(items)
    except Exception as e:
        _log(f"index error (pre-scan): {e}")
        work, total = [], 0

    # Announce fixed total once
    if prog:
        try:
            prog.tick(0, total=total, force=True)
        except Exception:
            pass

    if total == 0:
        if prog:
            try: prog.done(ok=True, total=0)
            except Exception: pass
        _log("index size: 0")
        return out

    done = 0
    try:
        for pl, items in work:
            pid = str(getattr(pl, "ratingKey", "") or "")
            pname = getattr(pl, "title", None)
            for obj in items:
                m = plex_normalize(obj)
                m["playlist"] = {"id": pid, "name": pname}
                k = canonical_key(m)
                out[f"playlist:{pid}|{k}"] = m
                done += 1
                if prog:
                    try: prog.tick(done, total=total)
                    except Exception: pass
    except Exception as e:
        _log(f"index error (ingest): {e}")

    if prog:
        try: prog.done(ok=True, total=total)
        except Exception: pass

    _log(f"index size: {len(out)}")
    return out


# ── add/remove ────────────────────────────────────────────────────────────────

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        unresolved = [{"item": id_minimal(it), "hint": "no_plex_server"} for it in items]
        if unresolved: _log("add skipped: no PMS bound")
        return 0, unresolved

    ok = 0
    unresolved: List[Dict[str, Any]] = []

    for it in items:
        pl = _find_or_create_playlist(srv, it.get("playlist"))
        if not pl:
            unresolved.append({"item": id_minimal(it), "hint": "playlist_missing"})
            continue

        obj = _resolve_obj(adapter, it)
        if not obj:
            unresolved.append({"item": id_minimal(it), "hint": "item_not_in_library"})
            continue

        try:
            pl.addItems([obj])
            ok += 1
        except Exception as e:
            _log(f"add failed: {e}")
            unresolved.append({"item": id_minimal(it), "hint": "add_failed"})

    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved


def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    srv = getattr(adapter.client, "server", None)
    if not srv:
        unresolved = [{"item": id_minimal(it), "hint": "no_plex_server"} for it in items]
        if unresolved: _log("remove skipped: no PMS bound")
        return 0, unresolved

    ok = 0
    unresolved: List[Dict[str, Any]] = []

    for it in items:
        pl = _find_playlist(srv, it.get("playlist"))
        if not pl:
            unresolved.append({"item": id_minimal(it), "hint": "playlist_not_found"})
            continue

        obj = _resolve_obj(adapter, it)
        if not obj:
            unresolved.append({"item": id_minimal(it), "hint": "item_not_in_library"})
            continue

        try:
            pl.removeItems([obj])
            ok += 1
        except Exception as e:
            _log(f"remove failed: {e}")
            unresolved.append({"item": id_minimal(it), "hint": "remove_failed"})

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved


# ── helpers ───────────────────────────────────────────────────────────────────

def _find_playlist(srv, pinfo: Optional[Mapping[str, Any]]):
    if not isinstance(pinfo, Mapping):
        return None
    pid = str(pinfo.get("id") or "").strip()
    pname = (pinfo.get("name") or "").strip()

    try:
        pls = list(srv.playlists())
    except Exception:
        return None

    if pid:
        for pl in pls:
            if str(getattr(pl, "ratingKey", "")) == pid and getattr(pl, "playlistType", "video") == "video":
                return pl
    if pname:
        for pl in pls:
            if getattr(pl, "playlistType", "video") == "video" and (pl.title or "").strip().lower() == pname.lower():
                return pl
    return None


def _find_or_create_playlist(srv, pinfo: Optional[Mapping[str, Any]]):
    pl = _find_playlist(srv, pinfo)
    if pl:
        return pl
    if not isinstance(pinfo, Mapping):
        return None
    name = (pinfo.get("name") or "").strip()
    if not name:
        return None
    try:
        # Create empty video playlist
        return srv.createPlaylist(title=name, items=[], playlistType="video")
    except Exception:
        return None


def _resolve_obj(adapter, it: Mapping[str, Any]):
    ids = ids_from(it)
    rk = ids.get("plex")
    if rk:
        obj = adapter.client.fetch_by_rating_key(rk)
        if obj:
            return obj

    srv = getattr(adapter.client, "server", None)
    if not srv:
        return None

    kind = (it.get("type") or "movie").lower()
    is_episode = kind == "episode"
    title = (it.get("title") or "").strip()
    series_title = (it.get("series_title") or "").strip()
    query_title = series_title if is_episode and series_title else title
    if not query_title:
        return None

    year = it.get("year")
    season = it.get("season") or it.get("season_number")
    episode = it.get("episode") or it.get("episode_number")

    sec_types = ("show",) if is_episode else ("movie",)
    hits: List[Any] = []
    for sec in adapter.libraries(types=sec_types) or []:
        try:
            found = sec.search(title=query_title) or []
            hits.extend(found)
        except Exception:
            continue

    def _score(obj) -> int:
        sc = 0
        try:
            ot = (getattr(obj, "grandparentTitle", None) if is_episode else getattr(obj, "title", None)) or ""
            if ot.strip().lower() == query_title.lower():
                sc += 3
            if not is_episode and year is not None and getattr(obj, "year", None) == year:
                sc += 2
            if is_episode:
                s_ok = (season is None) or (getattr(obj, "seasonNumber", None) == season or getattr(obj, "parentIndex", None) == season)
                e_ok = (episode is None) or (getattr(obj, "index", None) == episode)
                if s_ok and e_ok:
                    sc += 2
            m = plex_normalize(obj)
            mids = m.get("ids") or {}
            for k in ("imdb", "tmdb", "tvdb"):
                if k in mids and k in ids and mids[k] == ids[k]:
                    sc += 4
        except Exception:
            pass
        return sc

    best = None
    best_score = -1
    for h in hits:
        sc = _score(h)
        if sc > best_score:
            best, best_score = h, sc

    return best
