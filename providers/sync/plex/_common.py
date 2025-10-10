# /providers/sync/plex/_common.py
from __future__ import annotations
import os, uuid, requests, xml.etree.ElementTree as ET
from typing import Any, Dict, List, Mapping, Optional, Iterable

try:
    from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from_guid
except Exception:  # flat tests
    from _id_map import canonical_key, minimal as id_minimal, ids_from_guid  # type: ignore

_PLEX_CTX: Dict[str, Optional[str]] = {"baseurl": None, "token": None}

def configure_plex_context(*, baseurl: str | None, token: str | None) -> None:
    _PLEX_CTX["baseurl"] = (baseurl.rstrip("/") if isinstance(baseurl, str) else None)
    _PLEX_CTX["token"] = (token or None)

DISCOVER = "https://discover.provider.plex.tv"
METADATA = "https://metadata.provider.plex.tv"

CLIENT_ID = (
    os.environ.get("CW_PLEX_CID")
    or os.environ.get("PLEX_CLIENT_IDENTIFIER")
    or str(uuid.uuid4())
)

def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_PLEX_DEBUG"):
        print(f"[PLEX:common] {msg}")

def plex_headers(token: str) -> Dict[str, str]:
    return {
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Platform": "CrossWatch",
        "X-Plex-Version": "3.1.0",
        "X-Plex-Client-Identifier": CLIENT_ID,
        "X-Plex-Token": token,
        "Accept": "application/json, application/xml;q=0.9, */*;q=0.5",
    }

def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None: return None
        s = str(v).strip()
        return int(s) if s else None
    except Exception:
        return None

def _as_base_url(srv) -> Optional[str]:
    if not srv: return None
    v = getattr(srv, "baseurl", None)
    if isinstance(v, str) and v.startswith(("http://", "https://")): return v.rstrip("/")
    u = getattr(srv, "url", None)
    if callable(u):
        try: u = u()
        except Exception: u = None
    if isinstance(u, str) and u.startswith(("http://", "https://")): return u.rstrip("/")
    return None

def type_of(obj) -> str:
    t = (getattr(obj, "type", None) or "").lower()
    return t if t in ("movie","show","season","episode") else "movie"

def ids_from_obj(obj) -> Dict[str, str]:
    ids: Dict[str, str] = {}
    rk = getattr(obj, "ratingKey", None)
    if rk is not None: ids["plex"] = str(rk)
    g = getattr(obj, "guid", None)
    if g: ids.update(ids_from_guid(str(g)))
    for gg in (getattr(obj, "guids", []) or []):
        val = getattr(gg, "id", None)
        if val: ids.update(ids_from_guid(str(val)))
    return {k: v for k, v in ids.items() if v and str(v).strip().lower() not in ("none","null")}

def show_ids_hint(obj) -> Dict[str, str]:
    out: Dict[str, str] = {}
    gp = getattr(obj, "grandparentGuid", None)
    if gp: out.update(ids_from_guid(str(gp)))
    gp_rk = getattr(obj, "grandparentRatingKey", None)
    if gp_rk: out["plex"] = str(gp_rk)
    return {k: v for k, v in out.items() if v}

_SHOW_PMS_GUID_CACHE: Dict[str, Dict[str, str]] = {}

def _hydrate_show_ids_from_pms(obj) -> Dict[str, str]:
    rk = getattr(obj, "grandparentRatingKey", None)
    if not rk: return {}
    rk = str(rk)
    if rk in _SHOW_PMS_GUID_CACHE: return _SHOW_PMS_GUID_CACHE[rk]
    srv = getattr(obj, "_server", None)
    base = _as_base_url(srv) or _PLEX_CTX["baseurl"]
    token = (getattr(srv, "token", None) or getattr(srv, "_token", None) or _PLEX_CTX["token"])
    if not base or not token:
        _SHOW_PMS_GUID_CACHE[rk] = {}; return {}
    url = f"{base}/library/metadata/{rk}?includeGuids=1"
    try:
        r = requests.get(url, headers={"X-Plex-Token": token, "Accept": "application/json, application/xml;q=0.9, */*;q=0.5"}, timeout=8)
        ids: Dict[str, str] = {}
        if r.ok:
            ctype = (r.headers.get("content-type") or "").lower()
            if "application/json" in ctype:
                data = r.json(); mc = data.get("MediaContainer") or data; md = (mc.get("Metadata") or [])
                if md and isinstance(md, list):
                    for gg in (md[0].get("Guid") or []):
                        gid = gg.get("id"); 
                        if gid: ids.update(ids_from_guid(str(gid)))
            else:
                cont = _xml_to_container(r.text or ""); mc = cont.get("MediaContainer") or {}; md = (mc.get("Metadata") or [])
                if md and isinstance(md, list):
                    for gg in (md[0].get("Guid") or []):
                        gid = gg.get("id")
                        if gid: ids.update(ids_from_guid(str(gid)))
        ids = {k: v for k, v in ids.items() if v}
        _SHOW_PMS_GUID_CACHE[rk] = ids
        return ids
    except Exception as e:
        _log(f"hydrate show via PMS rk={rk} error: {e}")
        _SHOW_PMS_GUID_CACHE[rk] = {}
        return {}

_GUID_CACHE: Dict[str, Dict[str, str]] = {}

def _xml_to_container(xml_text: str) -> Mapping[str, Any]:
    root = ET.fromstring(xml_text)
    mc = root if root.tag.endswith("MediaContainer") else root.find(".//MediaContainer")
    if mc is None: return {"MediaContainer": {"Metadata": []}}
    rows: List[Mapping[str, Any]] = []
    for md in mc.findall("./Metadata"):
        a = md.attrib
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
            "Guid": [{"id": (g.attrib.get("id") or "")} for g in md.findall("./Guid") if g.attrib.get("id")],
        }
        rows.append(row)
    return {"MediaContainer": {"Metadata": rows}}

def hydrate_external_ids(token: Optional[str], rating_key: Optional[str]) -> Dict[str, str]:
    if not token or not rating_key: return {}
    rk = str(rating_key)
    if rk in _GUID_CACHE: return _GUID_CACHE[rk]
    url = f"{METADATA}/library/metadata/{rk}"
    try:
        r = requests.get(url, headers=plex_headers(token), timeout=10)
        if r.status_code == 401: raise RuntimeError("Unauthorized (bad Plex token)")
        if not r.ok:
            _log(f"hydrate {rk} -> {r.status_code}"); _GUID_CACHE[rk] = {}; return {}
        ctype = (r.headers.get("content-type") or "").lower()
        ids: Dict[str, str] = {}
        if "application/json" in ctype:
            data = r.json(); mc = data.get("MediaContainer") or data; md = (mc.get("Metadata") or [])
            if md and isinstance(md, list):
                for gg in (md[0].get("Guid") or []):
                    gid = gg.get("id"); 
                    if gid: ids.update(ids_from_guid(str(gid)))
        else:
            cont = _xml_to_container(r.text or ""); mc = cont.get("MediaContainer") or {}; md = (mc.get("Metadata") or [])
            if md and isinstance(md, list):
                for gg in (md[0].get("Guid") or []):
                    gid = gg.get("id")
                    if gid: ids.update(ids_from_guid(str(gid)))
        ids = {k: v for k, v in ids.items() if v}
        _GUID_CACHE[rk] = ids
        return ids
    except Exception as e:
        _log(f"hydrate error rk={rk}: {e}")
        _GUID_CACHE[rk] = {}
        return {}

def normalize(obj) -> Dict[str, Any]:
    # Add library_id when available; safe for all consumers.
    t = type_of(obj)
    ids = ids_from_obj(obj)
    base: Dict[str, Any] = {
        "type": t,
        "title": getattr(obj, "title", None),
        "year": getattr(obj, "year", None),
        "ids": ids,
        "guid": getattr(obj, "guid", None),
    }
    lid = _safe_int(getattr(obj, "librarySectionID", None) or getattr(obj, "sectionID", None) or getattr(obj, "librarySectionId", None) or getattr(obj, "sectionId", None))
    if lid is not None: base["library_id"] = lid

    if t in ("season","episode"):
        sid = show_ids_hint(obj)
        if sid: base["show_ids"] = sid
        has_ext = lambda m: bool(isinstance(m, dict) and any(m.get(k) for k in ("imdb","tmdb","tvdb")))
        if not has_ext(base.get("show_ids")):
            extra = _hydrate_show_ids_from_pms(obj)
            if extra: base.setdefault("show_ids", {}).update(extra)
        if not has_ext(base.get("show_ids")):
            srv = getattr(obj, "_server", None)
            token = (getattr(srv, "_token", None) or getattr(srv, "token", None) or _PLEX_CTX["token"])
            gp_rk = getattr(obj, "grandparentRatingKey", None)
            if token and gp_rk:
                extra2 = hydrate_external_ids(token, str(gp_rk))
                if extra2: base.setdefault("show_ids", {}).update(extra2)

    if t == "season":
        base["season"] = _safe_int(getattr(obj, "index", None))
    if t == "episode":
        base["season"] = _safe_int(getattr(obj, "seasonNumber", None) if hasattr(obj, "seasonNumber") else getattr(obj, "parentIndex", None))
        base["episode"] = _safe_int(getattr(obj, "index", None))
        base["series_title"] = getattr(obj, "grandparentTitle", None)

    keep_show_ids = base.get("show_ids") if t in ("season","episode") else None
    res = id_minimal(base)
    if keep_show_ids: res["show_ids"] = keep_show_ids
    if "library_id" in base: res["library_id"] = base["library_id"]
    return res

def key_of(obj) -> str:
    return canonical_key(normalize(obj))

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

def rating_key_from_discover_row(row: Mapping[str, Any]) -> Optional[str]:
    rk = row.get("ratingKey")
    return str(rk) if rk is not None else None

def normalize_discover_row(row: Mapping[str, Any], *, token: Optional[str] = None) -> Dict[str, Any]:
    # Mirror normalize(); also pass library_id when available.
    if token is None: token = _PLEX_CTX["token"]
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
        if token and not any(base.get("show_ids", {}).get(k) for k in ("imdb","tmdb","tvdb")):
            extra2 = hydrate_external_ids(token, str(gp_rk) if gp_rk else None)
            if extra2: base["show_ids"].update(extra2)

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

def sort_guid_candidates(guids: List[str]) -> List[str]:
    if not guids: return []
    pri, rest = [], list(guids)
    def pick(prefix, contains=None):
        out = [g for g in rest if (g.startswith(prefix) if contains is None else contains(g))]
        for g in out: rest.remove(g)
        return out
    pri += pick("tmdb://")
    pri += pick("imdb://")
    pri += pick("tvdb://")
    pri += pick("", contains=lambda g: g.startswith("com.plexapp.agents.themoviedb://") and "?lang=en" in g)
    pri += pick("", contains=lambda g: g.startswith("com.plexapp.agents.themoviedb://") and "?lang=en-US" in g)
    pri += pick("com.plexapp.agents.themoviedb://")
    pri += pick("com.plexapp.agents.imdb://")
    return pri + rest

def candidate_guids_from_ids(it: Mapping[str, Any]) -> List[str]:
    ids = (it.get("ids") or {}) if isinstance(it.get("ids"), dict) else {}
    out: List[str] = []
    def add(v: Optional[str]):
        if v and v not in out: out.append(v)
    imdb = ids.get("imdb"); tmdb = ids.get("tmdb"); tvdb = ids.get("tvdb")
    if tmdb:
        add(f"tmdb://{tmdb}"); add(f"themoviedb://{tmdb}")
        add(f"com.plexapp.agents.themoviedb://{tmdb}?lang=en")
        add(f"com.plexapp.agents.themoviedb://{tmdb}?lang=en-US")
        add(f"com.plexapp.agents.themoviedb://{tmdb}")
    if imdb:
        add(f"imdb://{imdb}"); add(f"com.plexapp.agents.imdb://{imdb}")
    if tvdb:
        add(f"tvdb://{tvdb}"); add(f"com.plexapp.agents.thetvdb://{tvdb}")
    g = it.get("guid")
    if g: add(str(g))
    return out

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

def meta_idset(meta_obj) -> set:
    s = set()
    try:
        g = getattr(meta_obj, "guid", None)
        if g:
            for k, v in ids_from_guid(str(g)).items():
                if k in ("imdb","tmdb","tvdb") and v: s.add((k, v))
        for gg in getattr(meta_obj, "guids", []) or []:
            gid = getattr(gg, "id", None)
            if gid:
                for k, v in ids_from_guid(str(gid)).items():
                    if k in ("imdb","tmdb","tvdb") and v: s.add((k, v))
    except Exception:
        pass
    return s

def resolve_discover_metadata(acct, it: Mapping[str, Any]):
    return None

def section_find_by_guid(sec, candidates: Iterable[str]):
    for g in candidates or []:
        try:
            hits = sec.search(guid=g) or []
            if hits: return hits[0]
        except Exception:
            continue
    return None
