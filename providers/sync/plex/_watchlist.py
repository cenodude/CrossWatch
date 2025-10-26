# /providers/sync/plex/_watchlist.py

from __future__ import annotations
import os, json, time, random, uuid, requests, xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, List

try:
    from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from, ids_from_guid
except Exception:
    from _id_map import canonical_key, minimal as id_minimal, ids_from, ids_from_guid  # type: ignore

from .._mod_common import request_with_retries  # instrumented (api:hit etc.)

UNRESOLVED_PATH = "/config/.cw_state/plex_watchlist.unresolved.json"

DISCOVER = "https://discover.provider.plex.tv"
METADATA = "https://metadata.provider.plex.tv"

CLIENT_ID = (
    os.environ.get("CW_PLEX_CID")
    or os.environ.get("PLEX_CLIENT_IDENTIFIER")
    or str(uuid.uuid4())
)

def plex_headers(token: str) -> Dict[str, str]:
    return {
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Platform": "CrossWatch",
        "X-Plex-Version": "3.1.0",
        "X-Plex-Client-Identifier": CLIENT_ID,
        "X-Plex-Token": token,
        "Accept": "application/json, application/xml;q=0.9, */*;q=0.5",
    }

# ── small utils ───────────────────────────────────────────────────────────────

def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None: return None
        s = str(v).strip()
        return int(s) if s else None
    except Exception:
        return None

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:watchlist] {msg}")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _sleep_ms(ms: int):
    try:
        if ms and ms > 0:
            time.sleep(ms / 1000.0)
    except Exception:
        pass

def _cfg(adapter) -> Dict[str, Any]:
    c = getattr(adapter, "config", {}) or {}
    return c.get("plex", {}) if isinstance(c, dict) else {}

def _cfg_int(d: Mapping[str, Any], key: str, default: int) -> int:
    try: return int(d.get(key, default))
    except Exception: return default

def _cfg_bool(d: Mapping[str, Any], key: str, default: bool) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool): return v
    s = str(v).strip().lower()
    return True if s in ("1","true","yes","on") else False if s in ("0","false","no","off") else default

def _cfg_list(d: Mapping[str, Any], key: str, default: List[str]) -> List[str]:
    v = d.get(key, default)
    if isinstance(v, list):
        return [str(x) for x in v]
    return list(default)

# ── XML helpers ───────────────────────────────────────────────────────────────

def _xml_meta_attribs(elem: ET.Element) -> Dict[str, Any]:
    a = elem.attrib
    row: Dict[str, Any] = {
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
        "librarySectionID": _safe_int(a.get("librarySectionID") or a.get("sectionID") or a.get("librarySectionId") or a.get("sectionId")),
        "Guid": [{"id": (g.attrib.get("id") or "")} for g in elem.findall("./Guid") if g.attrib.get("id")],
    }
    return row

def _xml_to_container(xml_text: str) -> Mapping[str, Any]:
    root = ET.fromstring(xml_text)
    mc_elem = root if root.tag.endswith("MediaContainer") else root.find(".//MediaContainer")
    if mc_elem is None:
        return {"MediaContainer": {"Metadata": [], "SearchResults": []}}

    meta_rows: List[Mapping[str, Any]] = []
    for md in mc_elem.findall("./Metadata"):
        meta_rows.append(_xml_meta_attribs(md))

    sr_list: List[Mapping[str, Any]] = []
    for sr in mc_elem.findall("./SearchResults"):
        sr_obj: Dict[str, Any] = {
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

# ── Discover/METADATA id helpers ──────────────────────────────────────────────

def ids_from_discover_row(row: Mapping[str, Any]) -> Dict[str, str]:
    ids: Dict[str, str] = {}
    g = row.get("guid")
    if g: ids.update(ids_from_guid(str(g)))
    for gg in (row.get("Guid") or []):
        try:
            gid = gg.get("id") or gg.get("Id") or gg.get("ID")
            if gid: ids.update(ids_from_guid(str(gid)))
        except Exception:
            continue
    rk = row.get("ratingKey")
    if rk: ids["plex"] = str(rk)
    return {k: v for k, v in ids.items() if v and str(v).strip().lower() not in ("none","null")}

def hydrate_external_ids(token: Optional[str], rating_key: Optional[str]) -> Dict[str, str]:
    """Fetch imdb/tmdb/tvdb from METADATA by ratingKey."""
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
        ids: Dict[str, str] = {}
        if "application/json" in ctype:
            data = r.json()
            mc = data.get("MediaContainer") or data
            md = (mc.get("Metadata") or [])
            if md and isinstance(md, list):
                for gg in (md[0].get("Guid") or []):
                    gid = gg.get("id")
                    if gid: ids.update(ids_from_guid(str(gid)))
        else:
            cont = _xml_to_container(r.text or "")
            mc = cont.get("MediaContainer") or {}
            md = (mc.get("Metadata") or [])
            if md and isinstance(md, list):
                for gg in (md[0].get("Guid") or []):
                    gid = gg.get("id")
                    if gid: ids.update(ids_from_guid(str(gid)))
        return {k: v for k, v in ids.items() if v}
    except Exception:
        return {}

def normalize_discover_row(row: Mapping[str, Any], *, token: Optional[str] = None) -> Dict[str, Any]:
    """Normalize Discover row; hydrate external IDs if missing."""
    t = (row.get("type") or "movie").lower()
    ids = ids_from_discover_row(row)
    if not any(k in ids for k in ("imdb","tmdb","tvdb")) and token:
        rk = row.get("ratingKey")
        ids.update(hydrate_external_ids(token, str(rk) if rk else None))
        ids = {k: v for k, v in ids.items() if v}
    base: Dict[str, Any] = {
        "type": t,
        "title": row.get("title"),
        "year": row.get("year"),
        "guid": row.get("guid"),
        "ids": ids,
    }
    lid = row.get("library_id") or row.get("librarySectionID") or row.get("sectionID") or row.get("librarySectionId") or row.get("sectionId")
    if lid is not None:
        lid_i = _safe_int(lid)
        if lid_i is not None: base["library_id"] = lid_i

    if t in ("season","episode"):
        gp = row.get("grandparentGuid"); gp_rk = row.get("grandparentRatingKey")
        if gp: base["show_ids"] = {k: v for k, v in ids_from_guid(str(gp)).items() if v}
        if gp_rk:
            base.setdefault("show_ids", {}); base["show_ids"]["plex"] = str(gp_rk)

    if t == "season":
        base["season"] = _safe_int(row.get("index"))
    if t == "episode":
        base["season"] = _safe_int(row.get("parentIndex"))
        base["episode"] = _safe_int(row.get("index"))
        base["series_title"] = row.get("grandparentTitle")

    keep_show_ids = base.get("show_ids") if t in ("season","episode") else None
    res = id_minimal(base)
    if keep_show_ids: res["show_ids"] = keep_show_ids
    if "library_id" in base: res["library_id"] = base["library_id"]
    return res

# ── GUIDs from item (for matching, not for searching) ─────────────────────────

def candidate_guids_from_ids(it: Mapping[str, Any]) -> List[str]:
    ids = (it.get("ids") or {}) if isinstance(it.get("ids"), dict) else {}
    out: List[str] = []
    def add(v: Optional[str]):
        if v:
            v = str(v)
            if v and v not in out: out.append(v)
    imdb = ids.get("imdb"); tmdb = ids.get("tmdb"); tvdb = ids.get("tvdb")
    if tmdb:
        add(f"tmdb://{tmdb}"); add(f"themoviedb://{tmdb}")
        add(f"com.plexapp.agents.themoviedb://{tmdb}?lang=en")
        add(f"com.plexapp.agents.themoviedb://{tmdb}?lang=en-US")
        add(f"com.plexapp.agents.themoviedb://{tmdb}")
        add(str(tmdb))
    if imdb:
        add(f"imdb://{imdb}"); add(f"com.plexapp.agents.imdb://{imdb}")
        add(str(imdb))
    if tvdb:
        add(f"tvdb://{tvdb}"); add(f"com.plexapp.agents.thetvdb://{tvdb}")
        add(str(tvdb))
    g = it.get("guid")
    if g: add(str(g))
    return out

# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get_container(session, url: str, token: str, *, timeout: float, retries: int, params: Optional[Mapping[str, Any]] = None, accept_json: bool = False) -> Optional[Mapping[str, Any]]:
    try:
        headers = plex_headers(token)
        if accept_json:
            headers = dict(headers)
            headers["Accept"] = "application/json"
        r = request_with_retries(
            session, "GET", url,
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

def _iter_meta_rows(container: Optional[Mapping[str, Any]]):
    if not container: return
    mc = container.get("MediaContainer") or container
    meta = mc.get("Metadata") if isinstance(mc, Mapping) else None
    if isinstance(meta, list):
        for row in meta:
            if isinstance(row, Mapping): yield row

def _iter_search_rows(container: Optional[Mapping[str, Any]]):
    if not container: return
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

# ── GUID priority & query building ────────────────────────────────────────────

def _guid_priority(cfg: Mapping[str, Any]) -> List[str]:
    return _cfg_list(cfg, "watchlist_guid_priority",
                     ["imdb","tmdb","tvdb","agent:themoviedb:en","agent:themoviedb","agent:imdb"])

def _sort_guid_candidates(guids: List[str], priority: List[str]) -> List[str]:
    if not guids: return []
    def score(g: str) -> Tuple[int,int]:
        s = g.lower()
        order = []
        for p in priority:
            if p == "imdb" and s.startswith("imdb://"): order.append(0)
            elif p == "tmdb" and s.startswith("tmdb://"): order.append(1)
            elif p == "tvdb" and s.startswith("tvdb://"): order.append(2)
            elif p.startswith("agent:themoviedb") and s.startswith("com.plexapp.agents.themoviedb://"):
                order.append(3 if ":en" in p and "?lang=en" in s else 4)
            elif p == "agent:imdb" and s.startswith("com.plexapp.agents.imdb://"):
                order.append(5)
        return (min(order) if order else 99, len(s))
    return sorted(guids, key=score)

def _clean_query_tokens(*, title: Optional[str], year: Optional[int], slug: Optional[str]) -> List[str]:
    """
    Build Discover text queries. We do NOT include imdb/tmdb numbers: Discover ignores them.
    """
    out: List[str] = []
    def add(v: Optional[str]):
        if not v: return
        q = str(v).strip()
        if q and q not in out: out.append(q)
    if title:
        add(title)
        if year:
            add(f"{title} {year}")
    if slug:
        add(slug.replace("-", " "))
    return out[:8]

def _id_pairs_from_guid(g: str) -> set:
    s = set()
    try:
        for k, v in (ids_from_guid(g) or {}).items():
            if k in ("tmdb","imdb","tvdb") and v: s.add((k, str(v)))
    except Exception:
        pass
    return s

# ── ID-first resolver via METADATA.matches ────────────────────────────────────

def _metadata_match_by_ids(session, token: str, ids: Mapping[str, Any], libtype: str, year: Optional[int], *, timeout: float, retries: int) -> Optional[str]:
    # Priority: IMDB → TMDB → TVDB
    order = [("imdb", ids.get("imdb")), ("tmdb", ids.get("tmdb")), ("tvdb", ids.get("tvdb"))]
    for key, val in order:
        v = str(val).strip() if val else ""
        if not v:
            continue
        title_param = f"{key}-{v}"
        params = {"type": ("movie" if libtype == "movie" else "show"), "title": title_param}
        if isinstance(year, int) and year > 0:
            params["year"] = int(year)
        cont = _get_container(
            session, f"{METADATA}/library/metadata/matches", token,
            timeout=timeout, retries=retries, params=params, accept_json=True
        )
        if not cont:
            continue
        for row in _iter_search_rows(cont):
            rk = str(row.get("ratingKey") or "") if isinstance(row, Mapping) else ""
            if not rk:
                continue
            # Try shallow IDs first
            row_ids = ids_from_discover_row(row) if isinstance(row, Mapping) else {}
            if row_ids.get(key) and str(row_ids.get(key)) == v:
                _log(f"resolve rk={rk} via METADATA.matches key={key}")
                return rk
            # Else hydrate and compare
            ext = hydrate_external_ids(token, rk) if rk else {}
            if ext.get(key) and str(ext.get(key)) == v:
                _log(f"resolve rk={rk} via METADATA.matches(hydrate) key={key}")
                return rk
    return None

# ── Resolver (ID-first; fallback to Discover text) ────────────────────────────

def _discover_resolve_rating_key(
    session,
    token: str,
    guid_candidates: List[str],
    *,
    libtype: str,
    item_ids: Optional[Mapping[str, Any]] = None,
    title: Optional[str],
    year: Optional[int],
    slug: Optional[str],
    timeout: float,
    retries: int,
    query_limit: int,
    allow_title: bool,
    cfg: Mapping[str, Any]
) -> Optional[str]:
    # Collect external IDs from item and candidate GUIDs
    ids: Dict[str, Any] = {}
    if isinstance(item_ids, Mapping):
        for k in ("imdb", "tmdb", "tvdb"):
            if item_ids.get(k):
                ids[k] = str(item_ids.get(k))
    for g in (guid_candidates or []):
        try:
            for k, v in (ids_from_guid(g) or {}).items():
                if k in ("imdb", "tmdb", "tvdb") and v and k not in ids:
                    ids[k] = str(v)
        except Exception:
            pass

    # 1) Exact: metadata.matches by ID (bypasses regional AKA titles)
    use_match = _cfg_bool(cfg, "watchlist_use_metadata_match", True)
    if use_match and any(ids.get(k) for k in ("imdb", "tmdb", "tvdb")):
        rk0 = _metadata_match_by_ids(session, token, ids, libtype, year, timeout=timeout, retries=retries)
        if rk0:
            return rk0

    # 2) Fallback: Discover text search → hydrate → ID compare
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
        row_pairs = {(k, str(v)) for k, v in row_ids.items() if k in ("tmdb","imdb","tvdb")}
        g = row.get("guid")
        if g:
            row_pairs |= _id_pairs_from_guid(str(g))
        if row_pairs:
            return any(tgt & row_pairs for _, tgt in targets if tgt)
        ext = hydrate_external_ids(token, rk) or {}
        hyd_pairs = {(k, str(v)) for k, v in ext.items() if k in ("tmdb","imdb","tvdb")}
        return bool(hyd_pairs and any(tgt & hyd_pairs for _, tgt in targets if tgt))

    params_common = {
        "limit": 25,
        "searchTypes": "movies,tv",
        "searchProviders": "discover",
        "includeMetadata": 1,
    }

    consecutive_empty = 0
    for q in queries[:max(1, min(query_limit, 50))]:
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

# ── Discover write (idempotent) ───────────────────────────────────────────────

def _discover_write_by_rk(session, token: str, rating_key: str, action: str, *, timeout: float, retries: int, delay_ms: int) -> Tuple[bool, int, str, bool]:
    if not rating_key: return (False, 0, "no-ratingKey", False)
    path = "addToWatchlist" if action == "add" else "removeFromWatchlist"
    url = f"{DISCOVER}/actions/{path}"
    try:
        _sleep_ms(delay_ms)
        r = request_with_retries(
            session, "PUT", url,
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
            if action == "add" and ("already on the watchlist" in lb or "already added" in lb or status == 409):
                already_ok = True
            if action == "remove" and ("not on the watchlist" in lb or "is not on the watchlist" in lb or status == 404):
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

# ── unresolved (hard freeze) ──────────────────────────────────────────────────

def _load_unresolved() -> Dict[str, Any]:
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

def _freeze_item(item: Mapping[str, Any], *, action: str, reasons: List[str], guids_tried: List[str]) -> None:
    key = canonical_key(item)
    data = _load_unresolved()
    entry = data.get(key) or {"feature": "watchlist", "action": action, "first_seen": _now_iso(), "attempts": 0}
    entry.update({"item": id_minimal(item), "last_attempt": _now_iso()})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    gset = set(entry.get("guids_tried", [])) | set((guids_tried or [])[:8])
    entry["reasons"] = sorted(rset); entry["guids_tried"] = sorted(gset)
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)

def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved(); changed = False
    for k in list(keys or []):
        if k in data: del data[k]; changed = True
    if changed: _save_unresolved(data)

def _is_frozen(item: Mapping[str, Any]) -> bool:
    return canonical_key(item) in _load_unresolved()

# ── PMS GUID index (optional fallback) ────────────────────────────────────────

_GUID_INDEX_MOVIE: Dict[str, Any] = {}
_GUID_INDEX_SHOW: Dict[str, Any] = {}

def meta_guids(meta_obj) -> List[str]:
    vals: List[str] = []
    try:
        if getattr(meta_obj, "guid", None): vals.append(str(meta_obj.guid))
        for gg in getattr(meta_obj, "guids", []) or []:
            gid = getattr(gg, "id", None)
            if gid: vals.append(str(gid))
    except Exception:
        pass
    return vals

def _build_guid_index(adapter) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    gi_m: Dict[str, Any] = {}
    gi_s: Dict[str, Any] = {}
    for sec in adapter.libraries(types=("movie", "show")) or []:
        try:
            for obj in (sec.all() or []):
                try:
                    gset = set(meta_guids(obj))
                    if not gset: continue
                    for g in gset:
                        (gi_m if sec.type == "movie" else gi_s)[g] = obj
                except Exception:
                    continue
        except Exception as e:
            _log(f"GUID index build error in '{getattr(sec,'title',None)}': {e}")
            continue
    _log(f"GUID index: movies={len(gi_m)}, shows={len(gi_s)}")
    return gi_m, gi_s

def _pms_find_in_index(libtype: str, guid_candidates: List[str]) -> Optional[Any]:
    src = _GUID_INDEX_SHOW if libtype == "show" else _GUID_INDEX_MOVIE
    for g in guid_candidates or []:
        if g in src:
            return src[g]
    return None

# ── per-run hydrate cache ─────────────────────────────────────────────────────

_HYDRATE_CACHE: Dict[str, Dict[str, Any]] = {}

def _hydrate_ids_cached(token: str, rk: str) -> Dict[str, Any]:
    m = _HYDRATE_CACHE.get(rk)
    if m is None:
        try:
            m = hydrate_external_ids(token, rk) or {}
        except Exception:
            m = {}
        _HYDRATE_CACHE[rk] = m
    return m

# ── public API: index ─────────────────────────────────────────────────────────
def build_index(adapter) -> Dict[str, Dict[str, Any]]:
    token = getattr(adapter, "cfg", None) and getattr(adapter.cfg, "token", None)
    if not token:
        raise RuntimeError("Plex token is required for watchlist index")
    session = adapter.client.session
    timeout = float(getattr(adapter.cfg, "timeout", 12.0) or 12.0)
    retries = int(getattr(adapter.cfg, "max_retries", 3) or 3)
    cfg = _cfg(adapter)
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("watchlist") if callable(prog_mk) else None
    page_size = _cfg_int(cfg, "watchlist_page_size", 100)
    base_params = {"includeCollections": 1, "includeExternalMedia": 1}

    out: Dict[str, Dict[str, Any]] = {}
    done = 0; total: Optional[int] = None; start = 0
    raw = 0; coll = 0; typ: Dict[str,int] = {}
    while True:
        params = dict(base_params)
        params["X-Plex-Container-Start"] = start
        params["X-Plex-Container-Size"] = page_size
        params["offset"] = start
        params["limit"] = page_size
        cont = _get_container(session, f"{DISCOVER}/library/sections/watchlist/all", token,
                              timeout=timeout, retries=retries, params=params, accept_json=True)
        mc = (cont or {}).get("MediaContainer") if isinstance(cont, Mapping) else None
        if total is None:
            try:
                t = (mc or {}).get("totalSize") or (mc or {}).get("size")
                total = int(t) if t is not None and str(t).isdigit() else None
            except Exception:
                total = None
        rows = list(_iter_meta_rows(cont)); raw += len(rows)
        if prog and start == 0:
            try: prog.tick(0, total=(total if total is not None else 0), force=True)
            except Exception: pass
        if not rows: break
        stop = False
        for row in rows:
            m = normalize_discover_row(row, token=token)
            k = canonical_key(m)
            if k in out: coll += 1
            out[k] = m
            t = (m.get("type") or "movie").lower(); typ[t] = typ.get(t,0)+1
            done += 1
            if prog:
                try: prog.tick(done, total=(total if total is not None else done))
                except Exception: pass
            if total is not None and done >= total:
                stop = True; break
        if stop: break
        if total is None and len(rows) < page_size: break
        start += len(rows)
    _unfreeze_keys_if_present(out.keys())
    _log(f"index size: {len(out)} raw={raw} coll={coll} types={typ}")
    return out

# ── public API: add/remove ────────────────────────────────────────────────────

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, list]:
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
        _GUID_INDEX_MOVIE.update(gm); _GUID_INDEX_SHOW.update(gs)

    ok = 0
    unresolved = []
    seen = set()

    for it in items:
        ck = canonical_key(it); 
        if ck in seen: continue
        seen.add(ck)

        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        guids = _sort_guid_candidates(candidate_guids_from_ids(it), _guid_priority(cfg))
        kind = (it.get("type") or "movie").lower()
        libtype = "show" if kind in ("show", "series", "tv") else "movie"
        title = it.get("title"); year = it.get("year")
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
                    if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    msg = str(e).lower()
                    if "already on the watchlist" in msg:
                        ok += 1
                        if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                        continue
                    _log(f"PMS add failed: {e}")

        rk = _discover_resolve_rating_key(
            session, token, guids,
            libtype=libtype, item_ids=(it.get("ids") or {}),
            title=title, year=year, slug=slug,
            timeout=timeout, retries=retries, query_limit=qlimit, allow_title=allow_title,
            cfg=cfg
        )

        if rk:
            ok_flag, status, body, transient = _discover_write_by_rk(
                session, token, rk, action="add",
                timeout=timeout, retries=retries, delay_ms=delay_ms
            )
            if ok_flag:
                ok += 1
                if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
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
                    if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    msg = str(e).lower()
                    if "already on the watchlist" in msg:
                        ok += 1
                        if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                        continue
                    _log(f"PMS add failed: {e}")
                    unresolved.append({"item": id_minimal(it), "hint": "pms_transient"})
                    continue

        unresolved.append({"item": id_minimal(it), "hint": "discover+library failed"})
        _freeze_item(
            it, action="add",
            reasons=["discover:resolve-or-write-failed" if rk else "discover:resolve-empty",
                     *(["library:guid-index-miss"] if pms_enabled else [])],
            guids_tried=guids
        )

    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, list]:
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
        _GUID_INDEX_MOVIE.update(gm); _GUID_INDEX_SHOW.update(gs)

    ok = 0
    unresolved = []
    seen = set()

    for it in items:
        ck = canonical_key(it)
        if ck in seen: continue
        seen.add(ck)

        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        guids = _sort_guid_candidates(candidate_guids_from_ids(it), _guid_priority(cfg))
        kind = (it.get("type") or "movie").lower()
        libtype = "show" if kind in ("show", "series", "tv") else "movie"
        title = it.get("title"); year = it.get("year")
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
                    if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    msg = str(e).lower()
                    if "not on the watchlist" in msg or "is not on the watchlist" in msg:
                        ok += 1
                        if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                        continue
                    _log(f"PMS remove failed: {e}")

        rk = _discover_resolve_rating_key(
            session, token, guids,
            libtype=libtype, item_ids=(it.get("ids") or {}),
            title=title, year=year, slug=slug,
            timeout=timeout, retries=retries, query_limit=qlimit, allow_title=allow_title,
            cfg=cfg
        )
        if rk:
            ok_flag, status, body, transient = _discover_write_by_rk(
                session, token, rk, action="remove",
                timeout=timeout, retries=retries, delay_ms=delay_ms
            )
            if ok_flag:
                ok += 1
                if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
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
                    if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                    continue
                except Exception as e:
                    msg = str(e).lower()
                    if "not on the watchlist" in msg or "is not on the watchlist" in msg:
                        ok += 1
                        if _is_frozen(it): _unfreeze_keys_if_present([canonical_key(it)])
                        continue
                    _log(f"PMS remove failed: {e}")
                    unresolved.append({"item": id_minimal(it), "hint": "pms_transient"})
                    continue

        unresolved.append({"item": id_minimal(it), "hint": "discover+library failed"})
        _freeze_item(
            it, action="remove",
            reasons=["discover:resolve-or-write-failed" if rk else "discover:resolve-empty",
                     *(["library:guid-index-miss"] if pms_enabled else [])],
            guids_tried=guids
        )

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved
