# /providers/sync/anilist/_watchlist.py
# AniList watchlist sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import minimal as id_minimal


SHADOW_PATH = "/config/.cw_state/anilist_watchlist_shadow.json"

GQL_VIEWER = "query { Viewer { id name } }"

GQL_LIST = """
query ($userId: Int!, $type: MediaType!, $status: MediaListStatus!) {
  MediaListCollection(userId: $userId, type: $type, status: $status) {
    lists {
      entries {
        id
        status
        media {
          id
          idMal
          title { romaji english native }
          format
          seasonYear
          startDate { year }
        }
      }
    }
  }
}
""".strip()

GQL_LIST_FALLBACK = """
query ($userId: Int!, $type: MediaType!) {
  MediaListCollection(userId: $userId, type: $type) {
    lists {
      entries {
        id
        status
        media {
          id
          idMal
          title { romaji english native }
          format
          seasonYear
          startDate { year }
        }
      }
    }
  }
}
""".strip()

GQL_MEDIA_BY_MAL = """
query ($idMal: Int!, $type: MediaType!) {
  Media(idMal: $idMal, type: $type) { id idMal }
}
""".strip()

GQL_ENTRY_BY_MEDIA = """
query ($mediaId: Int!, $userId: Int!) {
  MediaList(mediaId: $mediaId, userId: $userId) { id status }
}
""".strip()

GQL_SAVE_ENTRY = """
mutation ($mediaId: Int!, $status: MediaListStatus!) {
  SaveMediaListEntry(mediaId: $mediaId, status: $status) { id }
}
""".strip()

GQL_DELETE_ENTRY = """
mutation ($id: Int!) {
  DeleteMediaListEntry(id: $id) { deleted }
}
""".strip()

GQL_SEARCH = """
query ($search: String!, $page: Int = 1) {
  Page(page: $page, perPage: 10) {
    pageInfo { hasNextPage }
    media(search: $search, type: ANIME) {
      id
      idMal
      format
      seasonYear
      startDate { year }
      title { romaji english native }
    }
  }
}
""".strip()

_WS = re.compile(r"\s+")


def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_ANILIST_DEBUG"):
        print(f"[ANILIST:watchlist] {msg}")


def _shadow_load() -> dict[str, dict[str, Any]]:
    try:
        with open(SHADOW_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
            return dict(raw) if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _shadow_save(d: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(SHADOW_PATH), exist_ok=True)
        with open(SHADOW_PATH, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2, sort_keys=True)
    except Exception:
        pass


def _pick_title(t: Mapping[str, Any] | None) -> str:
    if not isinstance(t, Mapping):
        return ""
    return str(t.get("english") or t.get("romaji") or t.get("native") or "").strip()


def _norm_title(s: str) -> str:
    s = (s or "").strip().lower()
    s = _WS.sub(" ", s)
    return s


def _to_int(v: Any) -> int | None:
    try:
        if v is None:
            return None
        if isinstance(v, bool):
            return None
        if isinstance(v, int):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _tick(prog: Any, value: int, total: int | None = None, *, force: bool = False) -> None:
    if prog is None:
        return
    try:
        if total is not None:
            prog.tick(value, total=total, force=force)
        else:
            prog.tick(value)
    except Exception:
        pass


def _shadow_rev(shadow: Mapping[str, Any]) -> dict[int, str]:
    rev: dict[int, str] = {}
    for src_key, ent in (shadow or {}).items():
        if not src_key or not isinstance(ent, Mapping):
            continue
        aid = _to_int(ent.get("anilist_id"))
        if not aid or aid in rev:
            continue
        rev[aid] = str(src_key)
    return rev


def _score_candidate(
    *,
    want_title: str,
    want_year: int | None,
    want_kind: str,
    cand_title: str,
    cand_year: int | None,
    cand_format: str,
) -> int:
    wt = _norm_title(want_title)
    ct = _norm_title(cand_title)

    score = 0
    if wt and ct and wt == ct:
        score += 70
    elif wt and ct and (wt in ct or ct in wt):
        score += 20

    if want_year and cand_year:
        if want_year == cand_year:
            score += 30
        else:
            score -= 50

    k = (want_kind or "").lower()
    fmt = (cand_format or "").upper()

    if k == "movie":
        if fmt == "MOVIE":
            score += 5
    else:
        if fmt in ("TV", "TV_SHORT", "ONA", "OVA"):
            score += 5

    return score


def _resolve_media_id(adapter: Any, item: Mapping[str, Any]) -> tuple[int | None, dict[str, Any]]:
    ids = item.get("ids")
    ids = dict(ids) if isinstance(ids, Mapping) else {}

    mid = _to_int(ids.get("anilist"))
    if mid:
        return mid, {"anilist_id": int(mid)}

    mal = _to_int(ids.get("mal"))
    if mal:
        data = adapter.client.gql(
            GQL_MEDIA_BY_MAL,
            {"idMal": int(mal), "type": "ANIME"},
            feature="watchlist:resolve",
            tolerate_errors=True,
        )
        m = (data or {}).get("Media")
        if isinstance(m, Mapping):
            aid = _to_int(m.get("id"))
            if aid:
                meta: dict[str, Any] = {"anilist_id": int(aid)}
                mm = _to_int(m.get("idMal"))
                if mm:
                    meta["mal"] = int(mm)
                return int(aid), meta

    title = str(item.get("title") or "").strip()
    if not title:
        return None, {}

    year = _to_int(item.get("year"))
    kind = str(item.get("type") or "").strip()

    q = adapter.client.gql(
        GQL_SEARCH,
        {"search": title, "page": 1},
        feature="watchlist:search",
        tolerate_errors=True,
    )
    page = (q or {}).get("Page")
    media = page.get("media") if isinstance(page, Mapping) else None
    if not isinstance(media, list) or not media:
        _log(f"resolve miss title={title!r} year={year!r} (no results)")
        return None, {}

    best_id: int | None = None
    best_meta: dict[str, Any] = {}
    best_score = -10_000

    for cand in media:
        if not isinstance(cand, Mapping):
            continue
        cid = _to_int(cand.get("id"))
        if not cid:
            continue

        ctitle = _pick_title(cand.get("title") if isinstance(cand.get("title"), Mapping) else None)
        if not ctitle:
            continue

        c_year = _to_int(cand.get("seasonYear"))
        if c_year is None:
            sd = cand.get("startDate")
            if isinstance(sd, Mapping):
                c_year = _to_int(sd.get("year"))

        c_fmt = str(cand.get("format") or "")
        score = _score_candidate(
            want_title=title,
            want_year=year,
            want_kind=kind,
            cand_title=ctitle,
            cand_year=c_year,
            cand_format=c_fmt,
        )

        if score > best_score:
            best_score = score
            best_id = int(cid)
            best_meta = {"anilist_id": int(cid)}
            cm = _to_int(cand.get("idMal"))
            if cm:
                best_meta["mal"] = int(cm)

    if best_id is None or best_score < 85:
        _log(f"resolve miss title={title!r} year={year!r} best_score={best_score}")
        return None, {}

    _log(f"resolve hit title={title!r} year={year!r} -> id={best_id} score={best_score}")
    return best_id, best_meta


def _unresolved_item(m: Mapping[str, Any], reason: str) -> dict[str, Any]:
    out = dict(id_minimal(m))
    out["_cw_unresolved_reason"] = str(reason or "unknown")
    return out


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("watchlist") if callable(prog_mk) else None

    viewer = adapter.client.viewer()
    user_id = viewer.get("id") if isinstance(viewer, dict) else None
    if not user_id:
        return {}

    shadow = _shadow_load()
    rev = _shadow_rev(shadow)

    t0 = time.time()

    try:
        data = adapter.client.gql(
            GQL_LIST,
            {"userId": int(user_id), "type": "ANIME", "status": "PLANNING"},
            feature="watchlist:index",
        )
    except Exception:
        _log("index planning-query failed; falling back to full list query")
        data = adapter.client.gql(
            GQL_LIST_FALLBACK,
            {"userId": int(user_id), "type": "ANIME"},
            feature="watchlist:index",
        )

    _log(f"index fetched in {int((time.time() - t0) * 1000)}ms")

    mlc = (data or {}).get("MediaListCollection")
    lists = mlc.get("lists") if isinstance(mlc, Mapping) else None
    if not isinstance(lists, list):
        return {}

    out: dict[str, dict[str, Any]] = {}
    live_ids: set[int] = set()

    done = 0
    shadow_changed = False

    for lst in lists:
        if not isinstance(lst, Mapping):
            continue
        entries = lst.get("entries")
        if not isinstance(entries, list):
            continue

        for e in entries:
            if not isinstance(e, Mapping):
                continue
            if str(e.get("status") or "").upper() != "PLANNING":
                continue
            media = e.get("media")
            if not isinstance(media, Mapping):
                continue

            mid = _to_int(media.get("id"))
            if not mid:
                continue
            live_ids.add(int(mid))

            title = _pick_title(media.get("title") if isinstance(media.get("title"), Mapping) else None)
            if not title:
                continue

            year = _to_int(media.get("seasonYear"))
            if year is None:
                sd = media.get("startDate")
                if isinstance(sd, Mapping):
                    year = _to_int(sd.get("year"))

            mal = _to_int(media.get("idMal"))
            entry_id = _to_int(e.get("id"))

            ids: dict[str, Any] = {"anilist": int(mid)}
            if mal:
                ids["mal"] = int(mal)

            item = {
                "type": "anime",
                "title": title,
                "year": int(year or 0),
                "ids": ids,
                "anilist": {"list_entry_id": int(entry_id or 0), "status": "PLANNING"},
            }

            src_key = rev.get(int(mid))
            if src_key:
                item["anilist"]["shadow"] = True
                item["anilist"]["shadow_source_key"] = src_key
                key = src_key

                ent = shadow.get(src_key)
                if isinstance(ent, Mapping):
                    src_ids = ent.get("source_ids")
                    if isinstance(src_ids, Mapping):
                        for ik, iv in src_ids.items():
                            if not ik or iv is None:
                                continue
                            k2 = str(ik).strip()
                            if not k2:
                                continue
                            sv = str(iv).strip()
                            if sv and k2 not in ids:
                                ids[k2] = sv

                if isinstance(ent, Mapping) and entry_id and _to_int(ent.get("list_entry_id")) != int(entry_id):
                    shadow[src_key] = dict(ent)
                    shadow[src_key]["list_entry_id"] = int(entry_id)
                    shadow[src_key]["updated_at"] = int(time.time())
                    shadow_changed = True
            else:
                key = adapter.key_of(item)

            if key:
                out[str(key)] = item

            done += 1
            if done % 250 == 0:
                _tick(prog, done)


    if shadow:
        for src_key, ent in (shadow or {}).items():
            if not src_key or not isinstance(ent, Mapping) or ent.get("ignored") is not True:
                continue
            if str(src_key) in out:
                continue
            src_ids = ent.get("source_ids")
            out[str(src_key)] = {
                "type": str(ent.get("type") or "unknown"),
                "title": str(ent.get("title") or ""),
                "year": int(_to_int(ent.get("year")) or 0),
                "ids": dict(src_ids) if isinstance(src_ids, Mapping) else {},
                "anilist": {"shadow": True, "ignored": True, "reason": str(ent.get("ignore_reason") or "ignored")},
            }

    if shadow:
        for k in list(shadow.keys()):
            ent = shadow.get(k)
            if not isinstance(ent, Mapping):
                shadow.pop(k, None)
                shadow_changed = True
                continue
            if ent.get("ignored") is True:
                continue
            aid = _to_int(ent.get("anilist_id"))
            if not aid or int(aid) not in live_ids:
                shadow.pop(k, None)
                shadow_changed = True

    if shadow_changed:
        _shadow_save(shadow)

    _tick(prog, done, force=True)
    return out


def add_detailed(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lst = list(items or [])
    if not lst:
        return {"ok": True, "count": 0, "confirmed": 0, "confirmed_keys": [], "skipped_keys": [], "unresolved": []}

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("watchlist", total=len(lst)) if callable(prog_mk) else None

    viewer = adapter.client.viewer()
    user_id = viewer.get("id") if isinstance(viewer, dict) else None

    shadow = _shadow_load()
    shadow_changed = False

    ok = 0
    unresolved: list[dict[str, Any]] = []
    confirmed_keys: list[str] = []
    skipped_keys: list[str] = []
    _seen_ok: set[str] = set()
    _seen_skip: set[str] = set()

    for i, it in enumerate(lst, start=1):
        m = id_minimal(it)
        src_key = adapter.key_of(m) or ""
        ent = shadow.get(src_key) if src_key and isinstance(shadow, dict) else None


        if isinstance(ent, Mapping) and ent.get("ignored") is True:
            if src_key and src_key not in _seen_skip:
                skipped_keys.append(src_key)
                _seen_skip.add(src_key)
            _tick(prog, i, total=len(lst))
            continue

        if isinstance(ent, Mapping) and _to_int(ent.get("anilist_id")):
            mid = int(_to_int(ent.get("anilist_id")) or 0) or None
            meta: dict[str, Any] = {"anilist_id": int(mid)} if mid else {}
            if _to_int(ent.get("mal")):
                meta["mal"] = int(_to_int(ent.get("mal")) or 0)
        else:
            mid, meta = _resolve_media_id(adapter, m)

        if not mid:

            if src_key:
                ent2: dict[str, Any] = dict(shadow.get(src_key) or {})
                ent2["ignored"] = True
                ent2["ignore_reason"] = "not_anime_or_no_match"
                ent2["type"] = str(m.get("type") or ent2.get("type") or "")
                src_ids = m.get("ids")
                if isinstance(src_ids, Mapping) and src_ids:
                    ent2["source_ids"] = {
                        str(k): str(v).strip()
                        for k, v in src_ids.items()
                        if k and v is not None and str(v).strip()
                    }
                ent2["title"] = str(m.get("title") or ent2.get("title") or "")
                ent2["year"] = int(_to_int(m.get("year")) or ent2.get("year") or 0)
                ent2["updated_at"] = int(time.time())
                shadow[src_key] = ent2
                shadow_changed = True
            _log(f"skip non-anime/no-match title={str(m.get('title') or '')!r} year={_to_int(m.get('year'))!r}")
            if src_key and src_key not in _seen_skip:
                skipped_keys.append(src_key)
                _seen_skip.add(src_key)
            _tick(prog, i, total=len(lst))
            continue


        if user_id and src_key:
            try:
                data = adapter.client.gql(
                    GQL_ENTRY_BY_MEDIA,
                    {"mediaId": int(mid), "userId": int(user_id)},
                    feature="watchlist:lookup",
                    tolerate_errors=True,
                )
                ml = (data or {}).get("MediaList")
                if isinstance(ml, Mapping):
                    entry_id0 = _to_int(ml.get("id"))
                    status0 = str(ml.get("status") or "").upper().strip()
                    if entry_id0 and status0 == "PLANNING":
                        ent2: dict[str, Any] = dict(shadow.get(src_key) or {})
                        ent2.pop("ignored", None)
                        ent2.pop("ignore_reason", None)
                        ent2["anilist_id"] = int(mid)
                        if _to_int(meta.get("mal")):
                            ent2["mal"] = int(_to_int(meta.get("mal")) or 0)
                        src_ids = m.get("ids")
                        if isinstance(src_ids, Mapping) and src_ids:
                            ent2["source_ids"] = {
                                str(k): str(v).strip()
                                for k, v in src_ids.items()
                                if k and v is not None and str(v).strip()
                            }
                        ent2["list_entry_id"] = int(entry_id0)
                        ent2["type"] = str(m.get("type") or ent2.get("type") or "")
                        ent2["title"] = str(m.get("title") or ent2.get("title") or "")
                        ent2["year"] = int(_to_int(m.get("year")) or ent2.get("year") or 0)
                        ent2["updated_at"] = int(time.time())
                        shadow[src_key] = ent2
                        shadow_changed = True
                        ok += 1
                        if src_key and src_key not in _seen_ok:
                            confirmed_keys.append(src_key)
                            _seen_ok.add(src_key)
                        _log(f"adopt existing planning entry mediaId={mid} src_key={src_key}")
                        _tick(prog, i, total=len(lst))
                        continue
            except Exception:
                pass

        try:
            res = adapter.client.gql(
                GQL_SAVE_ENTRY,
                {"mediaId": int(mid), "status": "PLANNING"},
                feature="watchlist:add",
                tolerate_errors=False,
            )
            entry = (res or {}).get("SaveMediaListEntry")
            entry_id = _to_int(entry.get("id") if isinstance(entry, Mapping) else None)

            ok += 1
            if src_key and src_key not in _seen_ok:
                confirmed_keys.append(src_key)
                _seen_ok.add(src_key)

            if src_key:
                ent2: dict[str, Any] = dict(shadow.get(src_key) or {})
                ent2.pop("ignored", None)
                ent2.pop("ignore_reason", None)
                ent2["anilist_id"] = int(mid)
                if "mal" in meta and _to_int(meta.get("mal")):
                    ent2["mal"] = int(meta["mal"])
                ent2["type"] = str(m.get("type") or ent2.get("type") or "")
                src_ids = m.get("ids")
                if isinstance(src_ids, Mapping) and src_ids:
                    ent2["source_ids"] = {
                        str(k): str(v).strip()
                        for k, v in src_ids.items()
                        if k and v is not None and str(v).strip()
                    }
                if entry_id:
                    ent2["list_entry_id"] = int(entry_id)
                ent2["title"] = str(m.get("title") or ent2.get("title") or "")
                ent2["year"] = int(_to_int(m.get("year")) or ent2.get("year") or 0)
                ent2["updated_at"] = int(time.time())
                shadow[src_key] = ent2
                shadow_changed = True
        except Exception as e:
            unresolved.append(_unresolved_item(m, f"add_failed:{e.__class__.__name__}"))

        _tick(prog, i, total=len(lst))

    if shadow_changed:
        _shadow_save(shadow)

    if unresolved:
        reasons: dict[str, int] = {}
        for u in unresolved:
            r = str(u.get("_cw_unresolved_reason") or "unknown")
            reasons[r] = reasons.get(r, 0) + 1
        _log(f"add unresolved={len(unresolved)} by_reason={reasons}")

    return {
        "ok": True,
        "count": int(ok),
        "confirmed": int(ok),
        "confirmed_keys": list(confirmed_keys),
        "skipped_keys": list(skipped_keys),
        "unresolved": list(unresolved),
    }


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    res = add_detailed(adapter, items)
    count = int((res or {}).get("confirmed", (res or {}).get("count", 0)) or 0)
    unresolved = (res or {}).get("unresolved") or []
    return count, list(unresolved) if isinstance(unresolved, list) else []


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    lst = list(items or [])
    if not lst:
        return 0, []

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("watchlist", total=len(lst)) if callable(prog_mk) else None

    viewer = adapter.client.viewer()
    user_id = viewer.get("id") if isinstance(viewer, dict) else None
    if not user_id:
        return 0, [_unresolved_item(id_minimal(x), "missing_viewer") for x in lst]

    shadow = _shadow_load()
    shadow_changed = False

    ok = 0
    unresolved: list[dict[str, Any]] = []

    for i, it in enumerate(lst, start=1):
        m = id_minimal(it)
        src_key = adapter.key_of(m) or ""
        ent = shadow.get(src_key) if src_key and isinstance(shadow.get(src_key), Mapping) else None
        if isinstance(ent, Mapping) and ent.get("ignored") is True:
            if src_key and src_key in shadow:
                shadow.pop(src_key, None)
                shadow_changed = True
            ok += 1
            _tick(prog, i, total=len(lst))
            continue

        entry_id = None
        aobj = m.get("anilist")
        if isinstance(aobj, Mapping):
            entry_id = _to_int(aobj.get("list_entry_id"))

        if entry_id is None and src_key and isinstance(shadow.get(src_key), Mapping):
            entry_id = _to_int(shadow[src_key].get("list_entry_id"))

        if entry_id is None:
            mid, _meta = _resolve_media_id(adapter, m)
            if mid:
                try:
                    data = adapter.client.gql(
                        GQL_ENTRY_BY_MEDIA,
                        {"mediaId": int(mid), "userId": int(user_id)},
                        feature="watchlist:lookup",
                        tolerate_errors=True,
                    )
                    ml = (data or {}).get("MediaList")
                    if isinstance(ml, Mapping):
                        entry_id = _to_int(ml.get("id"))
                except Exception:
                    entry_id = None

        if not entry_id:
            unresolved.append(_unresolved_item(m, "missing_list_entry_id"))
            _tick(prog, i, total=len(lst))
            continue

        try:
            res = adapter.client.gql(
                GQL_DELETE_ENTRY,
                {"id": int(entry_id)},
                feature="watchlist:remove",
                tolerate_errors=False,
            )
            deleted = ((res or {}).get("DeleteMediaListEntry") or {}).get("deleted")
            if deleted is not True:
                unresolved.append(_unresolved_item(m, "delete_failed"))
            else:
                ok += 1
                if src_key and src_key in shadow:
                    shadow.pop(src_key, None)
                    shadow_changed = True
        except Exception as e:
            unresolved.append(_unresolved_item(m, f"remove_failed:{e.__class__.__name__}"))

        _tick(prog, i, total=len(lst))

    if shadow_changed:
        _shadow_save(shadow)

    return ok, unresolved
