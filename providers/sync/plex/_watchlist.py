# /providers/sync/plex/_watchlist.py
# Plex Module for watchlist synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import random
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import requests

from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from_guid
from .._mod_common import request_with_retries

UNRESOLVED_PATH = "/config/.cw_state/plex_watchlist.unresolved.json"

DISCOVER = "https://discover.provider.plex.tv"
METADATA = "https://metadata.provider.plex.tv"

CLIENT_ID = (
    os.environ.get("CW_PLEX_CID")
    or os.environ.get("PLEX_CLIENT_IDENTIFIER")
    or str(uuid.uuid4())
)


def plex_headers(token: str) -> dict[str, str]:
    return {
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Platform": "CrossWatch",
        "X-Plex-Version": "2.1.0",
        "X-Plex-Client-Identifier": CLIENT_ID,
        "X-Plex-Token": token,
        "Accept": "application/json, application/xml;q=0.9, */*;q=0.5",
    }


# Utils
def _safe_int(v: Any) -> int | None:
    try:
        if v is None:
            return None
        s = str(v).strip()
        return int(s) if s else None
    except Exception:
        return None


def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:watchlist] {msg}")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sleep_ms(ms: int) -> None:
    try:
        if ms and ms > 0:
            time.sleep(ms / 1000.0)
    except Exception:
        pass


def _cfg(adapter: Any) -> dict[str, Any]:
    c = getattr(adapter, "config", {}) or {}
    return c.get("plex", {}) if isinstance(c, dict) else {}


def _cfg_int(d: Mapping[str, Any], key: str, default: int) -> int:
    try:
        return int(d.get(key, default))
    except Exception:
        return default


def _cfg_bool(d: Mapping[str, Any], key: str, default: bool) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _cfg_list(d: Mapping[str, Any], key: str, default: list[str]) -> list[str]:
    v = d.get(key, default)
    if isinstance(v, list):
        return [str(x) for x in v]
    return list(default)


def _xml_meta_attribs(elem: ET.Element) -> dict[str, Any]:
    a = elem.attrib
    row: dict[str, Any] = {
        "type": a.get("type"),
        "title": a.get("title"),
        "year": _safe_int(a.get("year")),
        "guid": a.get("guid"),
        "ratingKey": a.get("ratingKey"),
        "grandparentGuid": a.get("grandparentGuid"),
        "grandparentRatingKey": a.get("grandparentRatingKey"),
        "grandparentTitle": a.get("grandparentTitle"),
        "index": _safe_int(a.get("index")),
        "parentIndex": _safe_int(a.get("parentIndex")),
        "librarySectionID": _safe_int(
            a.get("librarySectionID")
            or a.get("sectionID")
            or a.get("librarySectionId")
            or a.get("sectionId")
        ),
        "Guid": [{"id": (g.attrib.get("id") or "")} for g in elem.findall("./Guid") if g.attrib.get("id")],
    }
    return row


def _xml_to_container(xml_text: str) -> Mapping[str, Any]:
    root = ET.fromstring(xml_text)
    mc_elem = root if root.tag.endswith("MediaContainer") else root.find(".//MediaContainer")
    if mc_elem is None:
        return {"MediaContainer": {"Metadata": [], "SearchResults": []}}

    meta_rows: list[Mapping[str, Any]] = []
    for md in mc_elem.findall("./Metadata"):
        meta_rows.append(_xml_meta_attribs(md))

    sr_list: list[Mapping[str, Any]] = []
    for sr in mc_elem.findall("./SearchResults"):
        sr_obj: dict[str, Any] = {
            "id": sr.attrib.get("id"),
            "title": sr.attrib.get("title"),
            "size": _safe_int(sr.attrib.get("size")),
            "SearchResult": [],
        }
        for it in sr.findall("./SearchResult"):
            md = it.find("./Metadata")
            if md is not None:
                sr_obj["SearchResult"].append({"Metadata": _xml_meta_attribs(md)})
            else:
                md_attr = it.attrib.get("Metadata")
                if md_attr and md_attr != "[object Object]":
                    sr_obj["SearchResult"].append({"Metadata": {"title": md_attr}})
        sr_list.append(sr_obj)

    return {"MediaContainer": {"Metadata": meta_rows, "SearchResults": sr_list}}


# Discover
def ids_from_discover_row(row: Mapping[str, Any]) -> dict[str, str]:
    ids: dict[str, str] = {}
    g = row.get("guid")
    if g:
        ids.update(ids_from_guid(str(g)))
    for gg in (row.get("Guid") or []):
        try:
            gid = gg.get("id") or gg.get("Id") or gg.get("ID")
            if gid:
                ids.update(ids_from_guid(str(gid)))
        except Exception:
            continue
    rk = row.get("ratingKey")
    if rk:
        ids["plex"] = str(rk)
    return {k: v for k, v in ids.items() if v and str(v).strip().lower() not in ("none", "null")}


def hydrate_external_ids(token: str | None, rating_key: str | None) -> dict[str, str]:
    if not token or not rating_key:
        return {}
    rk = str(rating_key)
    url = f"{METADATA}/library/metadata/{rk}"
    try:
        r = requests.get(url, headers=plex_headers(token), timeout=10)
        if r.status_code == 401:
            raise RuntimeError("Unauthorized (bad Plex token)")
        if not r.ok:
            return {}
        ctype = (r.headers.get("content-type") or "").lower()
        ids: dict[str, str] = {}
        if "application/json" in ctype:
            data = r.json()
            mc = data.get("MediaContainer") or data
            md = mc.get("Metadata") or []
            if md and isinstance(md, list):
                for gg in (md[0].get("Guid") or []):
                    gid = gg.get("id")
                    if gid:
                        ids.update(ids_from_guid(str(gid)))
        else:
            cont = _xml_to_container(r.text or "")
            mc = cont.get("MediaContainer") or {}
            md = mc.get("Metadata") or []
            if md and isinstance(md, list):
                for gg in (md[0].get("Guid") or []):
                    gid = gg.get("id")
                    if gid:
                        ids.update(ids_from_guid(str(gid)))
        return {k: v for k, v in ids.items() if v}
    except Exception:
        return {}


def normalize_discover_row(row: Mapping[str, Any], *, token: str | None = None) -> dict[str, Any]:
    t = (row.get("type") or "movie").lower()
    ids = ids_from_discover_row(row)
    if not any(k in ids for k in ("imdb", "tmdb", "tvdb")) and token:
        rk = row.get("ratingKey")
        ids.update(hydrate_external_ids(token, str(rk) if rk else None))
        ids = {k: v for k, v in ids.items() if v}
    base: dict[str, Any] = {
        "type": t,
        "title": row.get("title"),
        "year": row.get("year"),
        "guid": row.get("guid"),
        "ids": ids,
    }
    lid = (
        row.get("library_id")
        or row.get("librarySectionID")
        or row.get("sectionID")
        or row.get("librarySectionId")
        or row.get("sectionId")
    )
    if lid is not None:
        lid_i = _safe_int(lid)
        if lid_i is not None:
            base["library_id"] = lid_i

    if t in ("season", "episode"):
        gp = row.get("grandparentGuid")
        gp_rk = row.get("grandparentRatingKey")
        if gp:
            base["show_ids"] = {k: v for k, v in ids_from_guid(str(gp)).items() if v}
        if gp_rk:
            base.setdefault("show_ids", {})
            base["show_ids"]["plex"] = str(gp_rk)

    if t == "season":
        base["season"] = _safe_int(row.get("index"))
    if t == "episode":
        base["season"] = _safe_int(row.get("parentIndex"))
        base["episode"] = _safe_int(row.get("index"))
        base["series_title"] = row.get("grandparentTitle")

    keep_show_ids = base.get("show_ids") if t in ("season", "episode") else None
    res = id_minimal(base)
    if keep_show_ids:
        res["show_ids"] = keep_show_ids
    if "library_id" in base:
        res["library_id"] = base["library_id"]
    return res


# GUID candidates
def candidate_guids_from_ids(it: Mapping[str, Any]) -> list[str]:
    ids = (it.get("ids") or {}) if isinstance(it.get("ids"), dict) else {}
    out: list[str] = []

    def add(v: str | None) -> None:
        if v:
            vv = str(v)
            if vv and vv not in out:
                out.append(vv)

    imdb = ids.get("imdb")
    tmdb = ids.get("tmdb")
    tvdb = ids.get("tvdb")

    if tmdb:
        add(f"tmdb://{tmdb}")
        add(f"themoviedb://{tmdb}")
        add(f"com.plexapp.agents.themoviedb://{tmdb}?lang=en")
        add(f"com.plexapp.agents.themoviedb://{tmdb}?lang=en-US")
        add(f"com.plexapp.agents.themoviedb://{tmdb}")
        add(str(tmdb))
    if imdb:
        add(f"imdb://{imdb}")
        add(f"com.plexapp.agents.imdb://{imdb}")
        add(str(imdb))
    if tvdb:
        add(f"tvdb://{tvdb}")
        add(f"com.plexapp.agents.thetvdb://{tvdb}")
        add(str(tvdb))
    g = it.get("guid")
    if g:
        add(str(g))
    return out

def _get_container(
    session: Any,
    url: str,
    token: str,
    *,
    timeout: float,
    retries: int,
    params: Mapping[str, Any] | None = None,
    accept_json: bool = False,
) -> Mapping[str, Any] | None:
    try:
        headers = plex_headers(token)
        if accept_json:
            headers = dict(headers)
            headers["Accept"] = "application/json"

        r = request_with_retries(
            session,
            "GET",
            url,
            headers=headers,
            params=(params or {}),
            timeout=timeout,
            max_retries=int(retries),
        )

        ctype = (r.headers.get("content-type") or "").lower()
        body = r.text or ""

        if r.status_code == 401:
            raise RuntimeError("Unauthorized (bad Plex token)")

        if not r.ok:
            try:
                req_url = getattr(r.request, "url", url)
                raw_headers = dict(getattr(r.request, "headers", {}) or {})
                safe_headers: dict[str, str] = {}
                for k, v in raw_headers.items():
                    kl = k.lower()
                    if kl == "x-plex-token":
                        safe_headers[k] = f"<redacted:{str(v)[:5]}...>"
                    else:
                        safe_headers[k] = str(v)
                snippet = body.replace("\n", " ")[:300]
                _log(
                    f"GET {req_url} -> {r.status_code} "
                    f"ctype={ctype or 'n/a'} headers={json.dumps(safe_headers, sort_keys=True)} "
                    f"body={snippet!r}"
                )
            except Exception:
                _log(f"GET {url} -> {r.status_code}")
            return None

        if "application/json" in ctype:
            try:
                return r.json()
            except Exception:
                _log("json parse failed; trying xml")

        if "xml" in ctype or body.lstrip().startswith("<"):
            try:
                return _xml_to_container(body)
            except Exception as e:
                _log(f"xml parse failed: {e}")

        _log(f"unknown payload: ctype={ctype or 'n/a'}")
        return None
    except Exception as e:
        _log(f"GET error: {e}")
        return None

def _iter_meta_rows(container: Mapping[str, Any] | None):
    if not container:
        return
    mc = container.get("MediaContainer") or container
    meta = mc.get("Metadata") if isinstance(mc, Mapping) else None
    if isinstance(meta, list):
        for row in meta:
            if isinstance(row, Mapping):
                yield row

def _iter_search_rows(container: Mapping[str, Any] | None):
    if not container:
        return
    mc = container.get("MediaContainer") or {}
    for sr in (mc.get("SearchResults") or []):
        for it in (sr.get("SearchResult") or []):
            md = it.get("Metadata")
            if isinstance(md, Mapping):
                yield md
            elif isinstance(md, list):
                for m in md:
                    if isinstance(m, Mapping):
                        yield m


# GUID proiority and sorting
def _guid_priority(cfg: Mapping[str, Any]) -> list[str]:
    return _cfg_list(
        cfg,
        "watchlist_guid_priority",
        ["imdb", "tmdb", "tvdb", "agent:themoviedb:en", "agent:themoviedb", "agent:imdb"],
    )


def _sort_guid_candidates(guids: list[str], priority: list[str]) -> list[str]:
    if not guids:
        return []

    def score(g: str) -> tuple[int, int]:
        s = g.lower()
        order: list[int] = []
        for p in priority:
            if p == "imdb" and s.startswith("imdb://"):
                order.append(0)
            elif p == "tmdb" and s.startswith("tmdb://"):
                order.append(1)
            elif p == "tvdb" and s.startswith("tvdb://"):
                order.append(2)
            elif p.startswith("agent:themoviedb") and s.startswith("com.plexapp.agents.themoviedb://"):
                order.append(3 if ":en" in p and "?lang=en" in s else 4)
            elif p == "agent:imdb" and s.startswith("com.plexapp.agents.imdb://"):
                order.append(5)
        return (min(order) if order else 99, len(s))

    return sorted(guids, key=score)


def _clean_query_tokens(*, title: str | None, year: int | None, slug: str | None) -> list[str]:
    out: list[str] = []

    def add(v: str | None) -> None:
        if not v:
            return
        q = str(v).strip()
        if q and q not in out:
            out.append(q)

    if title:
        add(title)
        if year:
            add(f"{title} {year}")
    if slug:
        add(slug.replace("-", " "))
    return out[:8]


def _id_pairs_from_guid(g: str) -> set[tuple[str, str]]:
    s: set[tuple[str, str]] = set()
    try:
        for k, v in (ids_from_guid(g) or {}).items():
            if k in ("tmdb", "imdb", "tvdb") and v:
                s.add((k, str(v)))
    except Exception:
        pass
    return s


# ID resolver via METADATA.matches
def _metadata_match_by_ids(
    session: Any,
    token: str,
    ids: Mapping[str, Any],
    libtype: str,
    year: int | None,
    *,
    timeout: float,
    retries: int,
) -> str | None:
    order = [("imdb", ids.get("imdb")), ("tmdb", ids.get("tmdb")), ("tvdb", ids.get("tvdb"))]
    for key, val in order:
        v = str(val).strip() if val else ""
        if not v:
            continue
        title_param = f"{key}-{v}"
        params: dict[str, Any] = {
            "type": "movie" if libtype == "movie" else "show",
            "title": title_param,
        }
        if isinstance(year, int) and year > 0:
            params["year"] = int(year)
        cont = _get_container(
            session,
            f"{METADATA}/library/metadata/matches",
            token,
            timeout=timeout,
            retries=retries,
            params=params,
            accept_json=True,
        )
        if not cont:
            continue
        for row in _iter_search_rows(cont):
            rk = str(row.get("ratingKey") or "") if isinstance(row, Mapping) else ""
            if not rk:
                continue
            row_ids = ids_from_discover_row(row) if isinstance(row, Mapping) else {}
            if row_ids.get(key) and str(row_ids.get(key)) == v:
                _log(f"resolve rk={rk} via METADATA.matches key={key}")
                return rk
            ext = hydrate_external_ids(token, rk) if rk else {}
            if ext.get(key) and str(ext.get(key)) == v:
                _log(f"resolve rk={rk} via METADATA.matches(hydrate) key={key}")
                return rk
    return None


# Fallback resolver via Discover search
def _discover_resolve_rating_key(
    session: Any,
    token: str,
    guid_candidates: list[str],
    *,
    libtype: str,
    item_ids: Mapping[str, Any] | None = None,
    title: str | None,
    year: int | None,
    slug: str | None,
    timeout: float,
    retries: int,
    query_limit: int,
    allow_title: bool,
    cfg: Mapping[str, Any],
) -> str | None:
    ids: dict[str, Any] = {}
    if isinstance(item_ids, Mapping):
        for k in ("imdb", "tmdb", "tvdb"):
            if item_ids.get(k):
                ids[k] = str(item_ids.get(k))
    for g in guid_candidates or []:
        try:
            for k, v in (ids_from_guid(g) or {}).items():
                if k in ("imdb", "tmdb", "tvdb") and v and k not in ids:
                    ids[k] = str(v)
        except Exception:
            pass

    use_match = _cfg_bool(cfg, "watchlist_use_metadata_match", True)
    if use_match and any(ids.get(k) for k in ("imdb", "tmdb", "tvdb")):
        rk0 = _metadata_match_by_ids(session, token, ids, libtype, year, timeout=timeout, retries=retries)
        if rk0:
            return rk0

    queries = _clean_query_tokens(
        title=(title if allow_title else None),
        year=(year if allow_title else None),
        slug=(slug if allow_title else None),
    )
    if not queries:
        return None

    pri = _guid_priority(cfg)
    targets = [(_g, _id_pairs_from_guid(_g)) for _g in _sort_guid_candidates(guid_candidates or [], pri)]

    def ids_match(rk: str, row: Mapping[str, Any]) -> bool:
        row_ids = ids_from_discover_row(row) if isinstance(row, Mapping) else {}
        row_pairs = {(k, str(v)) for k, v in row_ids.items() if k in ("tmdb", "imdb", "tvdb")}
        g = row.get("guid")
        if g:
            row_pairs |= _id_pairs_from_guid(str(g))
        if row_pairs:
            return any(tgt & row_pairs for _, tgt in targets if tgt)
        ext = hydrate_external_ids(token, rk) or {}
        hyd_pairs = {(k, str(v)) for k, v in ext.items() if k in ("tmdb", "imdb", "tvdb")}
        return bool(hyd_pairs and any(tgt & hyd_pairs for _, tgt in targets if tgt))

    params_common: dict[str, Any] = {
        "limit": 25,
        "searchTypes": "movies,tv",
        "searchProviders": "discover",
        "includeMetadata": 1,
    }

    consecutive_empty = 0
    for q in queries[: max(1, min(query_limit, 50))]:
        cont = _get_container(
            session,
            f"{DISCOVER}/library/search",
            token,
            timeout=timeout,
            retries=retries,
            params={**params_common, "query": q},
            accept_json=True,
        )
        if not cont:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            continue
        any_row = False
        for row in _iter_search_rows(cont):
            any_row = True
            rk = str(row.get("ratingKey") or "") if isinstance(row, Mapping) else ""
            if not rk:
                continue
            if ids_match(rk, row):
                _log(f"resolve rk={rk} via DISCOVER/search query={q}")
                return rk
        if not any_row:
            _log(f"discover.search empty for query={q}")
        _sleep_ms(random.randint(5, 40))
    return None


# Write
def _discover_write_by_rk(
    session: Any,
    token: str,
    rating_key: str,
    action: str,
    *,
    timeout: float,
    retries: int,
    delay_ms: int,
) -> tuple[bool, int, str, bool]:
    if not rating_key:
        return False, 0, "no-ratingKey", False
    path = "addToWatchlist" if action == "add" else "removeFromWatchlist"
    url = f"{DISCOVER}/actions/{path}"
    try:
        _sleep_ms(delay_ms)
        r = request_with_retries(
            session,
            "PUT",
            url,
            headers=plex_headers(token),
            params={"ratingKey": rating_key},
            timeout=timeout,
            max_retries=int(retries),
        )
        status = r.status_code
        body = (r.text or "")[:240]
        already_ok = False
        if not (200 <= status < 300):
            lb = (body or "").lower()
            if action == "add" and (
                "already on the watchlist" in lb
                or "already added" in lb
                or status == 409
            ):
                already_ok = True
            if action == "remove" and (
                "not on the watchlist" in lb
                or "is not on the watchlist" in lb
                or status == 404
            ):
                already_ok = True
        ok = (200 <= status < 300) or already_ok
        transient = status in (408, 429, 500, 502, 503, 504)
        if status == 429:
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    wait = max(0.0, float(ra))
                    _log(f"429 Retry-After={wait}s")
                    time.sleep(min(wait, 5.0))
                except Exception:
                    pass
        _log(f"discover.{action} rk={rating_key} -> {status} {body!r}")
        return ok, status, body, transient
    except Exception as e:
        return False, 0, str(e), True


# Unresolved items store
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


def _freeze_item(
    item: Mapping[str, Any],
    *,
    action: str,
    reasons: list[str],
    guids_tried: list[str],
) -> None:
    key = canonical_key(item)
    data = _load_unresolved()
    now = _now_iso()
    entry = data.get(key) or {"feature": "watchlist", "action": action, "first_seen": now, "attempts": 0}
    entry.update({"item": id_minimal(item), "last_attempt": now})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    gset = set(entry.get("guids_tried", [])) | set((guids_tried or [])[:8])
    entry["reasons"] = sorted(rset)
    entry["guids_tried"] = sorted(gset)
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


def _is_frozen(item: Mapping[str, Any]) -> bool:
    return canonical_key(item) in _load_unresolved()


# PMS GUID index
_GUID_INDEX_MOVIE: dict[str, Any] = {}
_GUID_INDEX_SHOW: dict[str, Any] = {}


def meta_guids(meta_obj: Any) -> list[str]:
    vals: list[str] = []
    try:
        if getattr(meta_obj, "guid", None):
            vals.append(str(meta_obj.guid))
        for gg in getattr(meta_obj, "guids", []) or []:
            gid = getattr(gg, "id", None)
            if gid:
                vals.append(str(gid))
    except Exception:
        pass
    return vals


def _build_guid_index(adapter: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    gi_m: dict[str, Any] = {}
    gi_s: dict[str, Any] = {}
    for sec in adapter.libraries(types=("movie", "show")) or []:
        try:
            for obj in (sec.all() or []):
                try:
                    gset = set(meta_guids(obj))
                    if not gset:
                        continue
                    for g in gset:
                        (gi_m if getattr(sec, "type", "") == "movie" else gi_s)[g] = obj
                except Exception:
                    continue
        except Exception as e:
            _log(f"GUID index build error in '{getattr(sec, 'title', None)}': {e}")
            continue
    _log(f"GUID index: movies={len(gi_m)}, shows={len(gi_s)}")
    return gi_m, gi_s


def _pms_find_in_index(libtype: str, guid_candidates: list[str]) -> Any | None:
    src = _GUID_INDEX_SHOW if libtype == "show" else _GUID_INDEX_MOVIE
    for g in guid_candidates or []:
        if g in src:
            return src[g]
    return None


# Index build
def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    token = getattr(adapter, "cfg", None) and getattr(adapter.cfg, "token", None)
    if not token:
        raise RuntimeError("Plex token is required for watchlist index")

    session = adapter.client.session
    timeout = float(getattr(adapter.cfg, "timeout", 12.0) or 12.0)
    retries = int(getattr(adapter.cfg, "max_retries", 3) or 3)
    cfg = _cfg(adapter)

    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("watchlist") if callable(prog_mk) else None

    page_size = _cfg_int(cfg, "watchlist_page_size", 100)
    base_params: dict[str, Any] = {"includeCollections": 1, "includeExternalMedia": 1}

    out: dict[str, dict[str, Any]] = {}
    done = 0
    total: int | None = None
    start = 0
    raw = 0
    coll = 0
    typ: dict[str, int] = {}

    while True:
        params = dict(base_params)
        params["X-Plex-Container-Start"] = start
        params["X-Plex-Container-Size"] = page_size
        cont = _get_container(
            session,
            f"{DISCOVER}/library/sections/watchlist/all",
            token,
            timeout=timeout,
            retries=retries,
            params=params,
            accept_json=True,
        )

        mc = (cont or {}).get("MediaContainer") if isinstance(cont, Mapping) else None
        if total is None:
            try:
                t = (mc or {}).get("totalSize") or (mc or {}).get("size")
                total = int(t) if t is not None and str(t).isdigit() else None
            except Exception:
                total = None

        rows = list(_iter_meta_rows(cont))
        raw += len(rows)

        if prog is not None and start == 0:
            try:
                prog.tick(0, total=(total if total is not None else 0), force=True)
            except Exception:
                pass

        if not rows:
            break

        stop = False
        for row in rows:
            m = normalize_discover_row(row, token=token)
            k = canonical_key(m)
            if k in out:
                coll += 1
            out[k] = m
            t = (m.get("type") or "movie").lower()
            typ[t] = typ.get(t, 0) + 1
            done += 1
            if prog is not None:
                try:
                    prog.tick(done, total=(total if total is not None else done))
                except Exception:
                    pass
            if total is not None and done >= total:
                stop = True
                break

        if stop:
            break
        if total is None and start > 0 and len(rows) < page_size:
            break
        start += len(rows)

    _unfreeze_keys_if_present(out.keys())
    _log(f"index size: {len(out)} raw={raw} coll={coll} types={typ}")
    return out


# Add
def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    token = getattr(adapter, "cfg", None) and getattr(adapter.cfg, "token", None)
    if not token:
        raise RuntimeError("Plex token is required for watchlist writes")

    session = adapter.client.session
    acct = adapter.account()
    cfg = _cfg(adapter)

    allow_pms = _cfg_bool(cfg, "watchlist_allow_pms_fallback", False)
    pms_first = _cfg_bool(cfg, "watchlist_pms_first", False)
    pms_enabled = allow_pms or pms_first

    timeout = float(getattr(adapter.cfg, "timeout", 12.0) or 12.0)
    retries = int(getattr(adapter.cfg, "max_retries", 3) or 3)

    qlimit = _cfg_int(cfg, "watchlist_query_limit", 25)
    delay_ms = _cfg_int(cfg, "watchlist_write_delay_ms", 0)
    allow_title = _cfg_bool(cfg, "watchlist_title_query", True)

    if pms_enabled and not (_GUID_INDEX_MOVIE or _GUID_INDEX_SHOW):
        gm, gs = _build_guid_index(adapter)
        _GUID_INDEX_MOVIE.update(gm)
        _GUID_INDEX_SHOW.update(gs)

    ok = 0
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()

    for it in items:
        ck = canonical_key(it)
        if ck in seen:
            continue
        seen.add(ck)

        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        guids = _sort_guid_candidates(candidate_guids_from_ids(it), _guid_priority(cfg))
        kind = (it.get("type") or "movie").lower()
        libtype = "show" if kind in ("show", "series", "tv") else "movie"
        title = it.get("title")
        year = it.get("year")
        slug = (it.get("ids") or {}).get("slug") if isinstance(it.get("ids"), dict) else None

        if not (guids or title or slug):
            unresolved.append({"item": id_minimal(it), "hint": "no_external_ids"})
            _freeze_item(it, action="add", reasons=["no-external-ids"], guids_tried=guids)
            continue

        if pms_first and pms_enabled:
            chosen = _pms_find_in_index(libtype, guids)
            if chosen:
                try:
                    chosen.addToWatchlist(account=acct)
                    ok += 1
                    if _is_frozen(it):
                        _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    msg = str(e).lower()
                    if "already on the watchlist" in msg:
                        ok += 1
                        if _is_frozen(it):
                            _unfreeze_keys_if_present([canonical_key(it)])
                        continue
                    _log(f"PMS add failed: {e}")

        rk = _discover_resolve_rating_key(
            session,
            token,
            guids,
            libtype=libtype,
            item_ids=(it.get("ids") or {}),
            title=title,
            year=year,
            slug=slug,
            timeout=timeout,
            retries=retries,
            query_limit=qlimit,
            allow_title=allow_title,
            cfg=cfg,
        )

        if rk:
            ok_flag, status, body, transient = _discover_write_by_rk(
                session,
                token,
                rk,
                action="add",
                timeout=timeout,
                retries=retries,
                delay_ms=delay_ms,
            )
            if ok_flag:
                ok += 1
                if _is_frozen(it):
                    _unfreeze_keys_if_present([canonical_key(it)])
                continue
            if transient:
                unresolved.append({"item": id_minimal(it), "hint": f"discover_transient_{status}"})
                continue
            _log(f"discover.add failed rk={rk} status={status} body={body!r}")

        if not pms_first and pms_enabled:
            chosen = _pms_find_in_index(libtype, guids)
            if chosen:
                try:
                    chosen.addToWatchlist(account=acct)
                    ok += 1
                    if _is_frozen(it):
                        _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    msg = str(e).lower()
                    if "already on the watchlist" in msg:
                        ok += 1
                        if _is_frozen(it):
                            _unfreeze_keys_if_present([canonical_key(it)])
                        continue
                    _log(f"PMS add failed: {e}")
                    unresolved.append({"item": id_minimal(it), "hint": "pms_transient"})
                    continue

        unresolved.append({"item": id_minimal(it), "hint": "discover+library failed"})
        _freeze_item(
            it,
            action="add",
            reasons=[
                "discover:resolve-or-write-failed" if rk else "discover:resolve-empty",
                *(["library:guid-index-miss"] if pms_enabled else []),
            ],
            guids_tried=guids,
        )

    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

# Remove
def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    token = getattr(adapter, "cfg", None) and getattr(adapter.cfg, "token", None)
    if not token:
        raise RuntimeError("Plex token is required for watchlist writes")

    session = adapter.client.session
    acct = adapter.account()
    cfg = _cfg(adapter)

    allow_pms = _cfg_bool(cfg, "watchlist_allow_pms_fallback", False)
    pms_first = _cfg_bool(cfg, "watchlist_pms_first", False)
    pms_enabled = allow_pms or pms_first

    timeout = float(getattr(adapter.cfg, "timeout", 12.0) or 12.0)
    retries = int(getattr(adapter.cfg, "max_retries", 3) or 3)

    qlimit = _cfg_int(cfg, "watchlist_query_limit", 25)
    delay_ms = _cfg_int(cfg, "watchlist_write_delay_ms", 0)
    allow_title = _cfg_bool(cfg, "watchlist_title_query", True)

    if pms_enabled and not (_GUID_INDEX_MOVIE or _GUID_INDEX_SHOW):
        gm, gs = _build_guid_index(adapter)
        _GUID_INDEX_MOVIE.update(gm)
        _GUID_INDEX_SHOW.update(gs)

    ok = 0
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()

    for it in items:
        ck = canonical_key(it)
        if ck in seen:
            continue
        seen.add(ck)

        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        guids = _sort_guid_candidates(candidate_guids_from_ids(it), _guid_priority(cfg))
        kind = (it.get("type") or "movie").lower()
        libtype = "show" if kind in ("show", "series", "tv") else "movie"
        title = it.get("title")
        year = it.get("year")
        slug = (it.get("ids") or {}).get("slug") if isinstance(it.get("ids"), dict) else None

        if not (guids or title or slug):
            unresolved.append({"item": id_minimal(it), "hint": "no_external_ids"})
            _freeze_item(it, action="remove", reasons=["no-external-ids"], guids_tried=guids)
            continue

        if pms_first and pms_enabled:
            chosen = _pms_find_in_index(libtype, guids)
            if chosen:
                try:
                    chosen.removeFromWatchlist(account=acct)
                    ok += 1
                    if _is_frozen(it):
                        _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    msg = str(e).lower()
                    if "not on the watchlist" in msg or "is not on the watchlist" in msg:
                        ok += 1
                        if _is_frozen(it):
                            _unfreeze_keys_if_present([canonical_key(it)])
                        continue
                    _log(f"PMS remove failed: {e}")

        rk = _discover_resolve_rating_key(
            session,
            token,
            guids,
            libtype=libtype,
            item_ids=(it.get("ids") or {}),
            title=title,
            year=year,
            slug=slug,
            timeout=timeout,
            retries=retries,
            query_limit=qlimit,
            allow_title=allow_title,
            cfg=cfg,
        )

        if rk:
            ok_flag, status, body, transient = _discover_write_by_rk(
                session,
                token,
                rk,
                action="remove",
                timeout=timeout,
                retries=retries,
                delay_ms=delay_ms,
            )
            if ok_flag:
                ok += 1
                if _is_frozen(it):
                    _unfreeze_keys_if_present([canonical_key(it)])
                continue
            if transient:
                unresolved.append({"item": id_minimal(it), "hint": f"discover_transient_{status}"})
                continue
            _log(f"discover.remove failed rk={rk} status={status} body={body!r}")

        if not pms_first and pms_enabled:
            chosen = _pms_find_in_index(libtype, guids)
            if chosen:
                try:
                    chosen.removeFromWatchlist(account=acct)
                    ok += 1
                    if _is_frozen(it):
                        _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    msg = str(e).lower()
                    if "not on the watchlist" in msg or "is not on the watchlist" in msg:
                        ok += 1
                        if _is_frozen(it):
                            _unfreeze_keys_if_present([canonical_key(it)])
                        continue
                    _log(f"PMS remove failed: {e}")
                    unresolved.append({"item": id_minimal(it), "hint": "pms_transient"})
                    continue

        unresolved.append({"item": id_minimal(it), "hint": "discover+library failed"})
        _freeze_item(
            it,
            action="remove",
            reasons=[
                "discover:resolve-or-write-failed" if rk else "discover:resolve-empty",
                *(["library:guid-index-miss"] if pms_enabled else []),
            ],
            guids_tried=guids,
        )

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved
