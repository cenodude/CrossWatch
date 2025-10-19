# /providers/sync/plex/_common.py
from __future__ import annotations
import os, uuid, time, requests, xml.etree.ElementTree as ET
import re, unicodedata
from typing import Any, Dict, List, Mapping, Optional, Iterable, Set

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
        
def _emit(evt: dict) -> None:
    try:
        feature = str(evt.get("feature") or "common")
        head = []
        if "event"  in evt: head.append(f"event={evt['event']}")
        if "action" in evt: head.append(f"action={evt['action']}")
        tail = [f"{k}={v}" for k, v in evt.items() if k not in {"feature","event","action"}]
        line = " ".join(head + tail)
        print(f"[PLEX:{feature}] {line}", flush=True)
    except Exception:
        pass

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

# --------- server-side GUID search ------------------------------
def server_find_rating_key_by_guid(srv, guids: Iterable[str]) -> Optional[str]:
    base = _as_base_url(srv)
    tok = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    ses = getattr(srv, "_session", None)
    if not (base and ses): return None
    hdrs = dict(getattr(ses, "headers", {}) or {})
    hdrs.update(plex_headers(tok))
    hdrs["Accept"] = "application/json"
    for g in [x for x in (guids or []) if x]:
        try:
            r = ses.get(f"{base}/library/all", params={"guid": g}, headers=hdrs, timeout=8)
            if not r.ok: continue
            j = r.json() if r.headers.get("Content-Type","").startswith("application/json") else {}
            md = (j.get("MediaContainer", {}) or {}).get("Metadata") or []
            if md and isinstance(md, list):
                rk = md[0].get("ratingKey") or md[0].get("ratingkey")
                if rk: return str(rk)
        except Exception:
            pass
    return None

# --- fallback GUID memo/neg-cache --------------------------------------------
_FBGUID_MEMO = {}  # key -> dict (success) or "__NOHIT__"
_FBGUID_NOHIT = "__NOHIT__"
_FBGUID_CACHE_PATH = "/config/.cw_state/plex_fallback_memo.json"

def _fb_key_from_row(row) -> str:
    def g(obj, *names):
        for n in names:
            v = getattr(obj, n, None)
            if v: return str(v).strip().lower()
        if isinstance(obj, dict):
            for n in names:
                v = obj.get(n)
                if v: return str(v).strip().lower()
        return ""

    t  = g(row, "type")
    g0 = g(row, "guid")
    gp = g(row, "parentGuid")
    gg = g(row, "grandparentGuid")
    gprk = g(row, "grandparentRatingKey")
    if not t  and isinstance(row, dict):  t  = str(row.get("type","")).lower()
    if not g0 and isinstance(row, dict): g0 = str(row.get("guid","")).lower()
    if not gp and isinstance(row, dict): gp = str(row.get("parentGuid","")).lower()
    if not gg and isinstance(row, dict): gg = str(row.get("grandparentGuid","")).lower()

    if isinstance(row, dict):
        title = str(row.get("grandparentTitle") or row.get("title") or "").strip().lower()
        year  = row.get("year")
    else:
        title = (getattr(row, "grandparentTitle", None) or getattr(row, "title", None) or "")
        title = str(title).strip().lower()
        year  = getattr(row, "year", None)

    try:
        yv = _year_from_any(year)
        ys = str(yv or "")
    except Exception:
        ys = ""

    if t == "episode":
        s = g(row, "parentIndex")
        e = g(row, "index")
        show_id = gprk or gg or gp or g0 or ""
        parts = ["k2", "ep", show_id, f"s{s}" if s else "", f"e{e}" if e else ""]
        if not show_id:
            parts += [title, ys]
        return "|".join([p for p in parts if p])

    parts = ["k2", (t or "item"), g0, title, ys]
    return "|".join([p for p in parts if p])

def _fb_cache_load() -> dict:
    if _FBGUID_MEMO: return _FBGUID_MEMO
    try:
        import json, os
        if os.path.exists(_FBGUID_CACHE_PATH):
            with open(_FBGUID_CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
                if isinstance(data, dict):
                    _FBGUID_MEMO.update(data)
    except Exception:
        pass
    return _FBGUID_MEMO

def _fb_cache_save() -> None:
    try:
        import json, os
        os.makedirs(os.path.dirname(_FBGUID_CACHE_PATH), exist_ok=True)
        tmp = _FBGUID_CACHE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_FBGUID_MEMO, f, ensure_ascii=False, indent=0, separators=(",",":"))
        os.replace(tmp, _FBGUID_CACHE_PATH)
    except Exception:
        pass

# -- PMS show GUID fetch -------------------------------------------------------
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
_HYDRATE_404: Set[str] = set()

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
    if rk in _HYDRATE_404: return {}
    url = f"{METADATA}/library/metadata/{rk}"
    try:
        r = requests.get(url, headers=plex_headers(token), timeout=10)
        if r.status_code == 401: raise RuntimeError("Unauthorized (bad Plex token)")
        if not r.ok:
            _log(f"hydrate {rk} -> {r.status_code}")
            _emit({"feature":"common","event":"hydrate","action":"miss","rk":rk,"status":r.status_code})
            if r.status_code == 404: _HYDRATE_404.add(rk)
            _GUID_CACHE[rk] = {}; return {}
        else:
            _emit({"feature":"common","event":"hydrate","action":"ok","rk":rk})
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
        _HYDRATE_404.add(rk)
        _GUID_CACHE[rk] = {}
        return {}

def normalize(obj) -> Dict[str, Any]:
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
    # Mirrors normalize(); also passes library_id when available.
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
            if extra2:
                base.setdefault("show_ids", {})
                base["show_ids"].update(extra2)

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

# --- GUID fallback helpers ----------------------------------------------------
def _iso8601_any(v: Any) -> Optional[str]:
    try:
        if v is None: return None
        s = str(v).strip()
        if not s: return None
        if s.isdigit():
            ts = int(s)
            if len(s) >= 13:
                ts //= 1000
            return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(ts))
        if "T" in s:
            return s if s.endswith("Z") else (s + "Z")
        return None
    except Exception:
        return None

def _watched_at_from_row(row: Any) -> Optional[str]:
    v = _row_get(
        row,
        "viewedAt", "viewed_at",
        "lastViewedAt", "last_viewed_at",
        "watchedAt", "watched_at",
        "originallyWatchedAt", "originally_watched_at",
    )
    return _iso8601_any(v)

def _year_from_any(v: Any) -> Optional[int]:
    try:
        if isinstance(v, int): return v
        s = str(v or "").strip()
        if not s: return None
        if s.isdigit() and len(s) in (4, 8):  # "2021" or "20210501"
            return int(s[:4])
        return int(s[:4]) if len(s) >= 4 and s[:4].isdigit() else None
    except Exception:
        return None

def _row_get(row: Any, *names: str) -> Any:
    for n in names:
        if isinstance(row, Mapping) and n in row: return row.get(n)
        if hasattr(row, n): return getattr(row, n)
    return None

def ids_from_history_row(row: Any) -> Dict[str, str]:
    ids: Dict[str, str] = {}
    rk = _row_get(row, "ratingKey", "key")
    if rk is not None: ids["plex"] = str(rk)
    for n in ("guid", "grandparentGuid", "parentGuid"):
        g = _row_get(row, n)
        if g: ids.update(ids_from_guid(str(g)))
    try:
        gg = _row_get(row, "Guid") or []
        if isinstance(gg, list):
            for it in gg:
                gid = (it.get("id") if isinstance(it, Mapping) else None) or getattr(it, "id", None)
                if gid: ids.update(ids_from_guid(str(gid)))
    except Exception:
        pass
    return {k: v for k, v in ids.items() if v and str(v).strip().lower() not in ("none","null")}

def _has_ext_ids(ids: Mapping[str, Any]) -> bool:
    try:
        return any(str(ids.get(k) or "").strip() for k in ("imdb","tmdb","tvdb"))
    except Exception:
        return False

def _build_minimal_from_row(row: Any, ids: Mapping[str, Any]) -> Dict[str, Any]:
    kind = str((_row_get(row, "type") or "movie")).lower()
    is_ep = (kind == "episode")

    title = (
        _row_get(row, "grandparentTitle") if is_ep else
        _row_get(row, "title") or _row_get(row, "originalTitle")
    )
    year = (
        _row_get(row, "year")
        or _row_get(row, "originallyAvailableAt")
        or _row_get(row, "originally_available_at")
        or _row_get(row, "grandparentYear")
    )

    base: Dict[str, Any] = {
        "type": ("episode" if is_ep else "movie"),
        "title": title,
        "year": _year_from_any(year),
        "guid": _row_get(row, "guid") or _row_get(row, "grandparentGuid") or _row_get(row, "parentGuid"),
        "ids": dict(ids or {}),
    }

    wa = _watched_at_from_row(row)
    if wa:
        base["watched_at"] = wa

    if is_ep:
        base["series_title"] = (
            _row_get(row, "grandparentTitle")
            or _row_get(row, "title")
            or _row_get(row, "parentTitle")
        )
        base["season"] = _safe_int(
            _row_get(row, "parentIndex") or _row_get(row, "seasonNumber")
        )
        base["episode"] = _safe_int(_row_get(row, "index"))

        gp = _row_get(row, "grandparentGuid")
        gp_rk = _row_get(row, "grandparentRatingKey")
        sids: Dict[str, Any] = {}
        if gp:
            sids.update({k: v for k, v in ids_from_guid(str(gp)).items() if v})
        if gp_rk:
            sids["plex"] = str(gp_rk)
        if not sids and base.get("season") is not None and base.get("episode") is not None:
            ext = {k: v for k, v in (base.get("ids") or {}).items() if k in ("imdb", "tmdb", "tvdb") and v}
            if ext:
                sids.update(ext)
        if sids:
            base["show_ids"] = sids

    res = id_minimal(base)

    if "show_ids" in base:
        res["show_ids"] = base["show_ids"]
    if "watched_at" in base:
        res["watched_at"] = base["watched_at"]
    if is_ep:
        if base.get("season") is not None:
            res["season"] = base["season"]
        if base.get("episode") is not None:
            res["episode"] = base["episode"]
        if base.get("series_title"):
            res["series_title"] = base["series_title"]

    return res

def _discover_search_title(
    token: str,
    title: str,
    kind: str,
    year: Optional[int],
    limit: int = 15,
    season: Optional[int] = None,
    episode: Optional[int] = None,
) -> Optional[Mapping[str, Any]]:
    try:
        if not title or not token:
            return None

        NUM_TO_WORD = {"1":"one","2":"two","3":"three","4":"four","5":"five","6":"six","7":"seven","8":"eight","9":"nine"}
        WORD_TO_NUM = {v:k for k,v in NUM_TO_WORD.items()}

        def _strip_accents(s: str) -> str:
            return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

        def _remove_parens(s: str) -> str:
            return re.sub(r"\([^)]*\)", " ", s)

        def _fold(s: str) -> str:
            s = _remove_parens(s)
            s = _strip_accents(s)
            s = re.sub(r"[^\w\s&]", " ", s)
            s = re.sub(r"\s+", " ", s).strip()
            return s

        def _to_words_1_9(s: str) -> str:
            return re.sub(r"\b([1-9])\b", lambda m: NUM_TO_WORD.get(m.group(1), m.group(1)), s, flags=re.IGNORECASE)

        def _to_digits_1_9(s: str) -> str:
            return re.sub(r"\b(one|two|three|four|five|six|seven|eight|nine)\b",
                          lambda m: WORD_TO_NUM.get(m.group(0).lower(), m.group(0)),
                          s, flags=re.IGNORECASE)

        def _and_amp_variants(s: str) -> list[str]:
            vs = [s]
            s1 = re.sub(r"\s*&\s*", " and ", s)
            if s1 != s: vs.append(re.sub(r"\s+", " ", s1).strip())
            s2 = re.sub(r"\band\b", "&", s, flags=re.IGNORECASE)
            if s2 != s: vs.append(re.sub(r"\s+", " ", s2).strip())
            return vs

        def _digit_word_variants(s: str) -> list[str]:
            vs = [s]
            sw = _to_words_1_9(s)
            if sw != s: vs.append(sw)
            sd = _to_digits_1_9(s)
            if sd != s: vs.append(sd)
            return list(dict.fromkeys(vs))

        def _variants(s: str) -> list[str]:
            base = (s or "").strip()
            pool = [base]
            no_parens = _remove_parens(base)
            if no_parens and no_parens != base:
                pool.append(re.sub(r"\s+", " ", no_parens).strip())
            pool = sum([_and_amp_variants(x) for x in pool], [])
            pool = sum([_digit_word_variants(x) for x in pool], [])
            folded = [_fold(x) for x in pool]
            pool += folded
            cut = []
            for x in pool:
                p = re.split(r"[:\-–(]", x, 1)[0].strip()
                if p and p not in pool:
                    cut.append(p)
            pool += cut

            if year:
                for x in list(pool):
                    if len(x) < 64:
                        pool.append(f"{x} {year}")
            out = []
            seen = set()
            for x in pool:
                y = re.sub(r"\s+", " ", x).strip()
                if y and y not in seen:
                    seen.add(y); out.append(y)
            return out

        def _collect_rows(j: Mapping[str, Any]) -> list[Mapping[str, Any]]:
            rows: list[Mapping[str, Any]] = []
            mc = (j.get("MediaContainer") or {})
            for bucket in (mc.get("SearchResults") or []):
                for item in (bucket.get("SearchResult") or []):
                    md = item.get("Metadata")
                    if isinstance(md, Mapping):
                        rows.append(md)
                    elif isinstance(md, list):
                        rows.extend([x for x in md if isinstance(x, Mapping)])
            for hub in (mc.get("Hub") or []):
                for md in (hub.get("Metadata") or []):
                    if isinstance(md, Mapping):
                        rows.append(md)
            mds = mc.get("Metadata")
            if isinstance(mds, list):
                rows.extend([x for x in mds if isinstance(x, Mapping)])
            elif isinstance(mds, Mapping):
                rows.append(mds)
            return rows

        def _hdrs() -> Dict[str, str]:
            # Prefer EN for stable IDs/names
            lang = os.environ.get("CW_PLEX_LANG") or "en-US,en;q=0.9"
            h = dict(plex_headers(token))
            h["Accept"] = "application/json"
            h["Accept-Language"] = lang
            h.setdefault("X-Plex-Product", "Plex Web")
            h.setdefault("X-Plex-Platform", "Web")
            return h

        def _search_v2_all(q: str, types: list[str]) -> list[Mapping[str, Any]]:
            rows: list[Mapping[str, Any]] = []
            base_params = {
                "query": q,
                "limit": max(50, int(limit)),
                "searchProviders": "discover",
                "includeMetadata": 1,
                "includeExternalMedia": 1,
            }
            combos = [t for t in types if t] + [""]
            for st in combos:
                params = dict(base_params)
                if st:
                    params["searchTypes"] = st
                try:
                    r = requests.get(f"{DISCOVER}/library/search", headers=_hdrs(), params=params, timeout=7)
                    if r.ok and "json" in (r.headers.get("content-type","").lower()):
                        rows2 = _collect_rows(r.json())
                        if rows2:
                            rows.extend(rows2)
                except Exception:
                    continue
            return rows

        def _search_v1(q: str) -> list[Mapping[str, Any]]:
            try:
                params = {"query": q, "limit": max(50, int(limit)), "includeMeta": 1}
                r = requests.get(f"{DISCOVER}/hubs/search", headers=_hdrs(), params=params, timeout=7)
                if r.ok and "json" in (r.headers.get("content-type", "").lower()):
                    return _collect_rows(r.json())
            except Exception:
                pass
            return []

        def _search_metadata_provider(q: str) -> list[Mapping[str, Any]]:
            try:
                r = requests.get(f"{METADATA}/library/search", headers=_hdrs(), params={"query": q, "limit": max(50, int(limit))}, timeout=7)
                if r.ok and "json" in (r.headers.get("content-type","").lower()):
                    return _collect_rows(r.json())
            except Exception:
                pass
            return []

        def _search_all(q: str) -> list[Mapping[str, Any]]:
            if kind == "movie":
                types = ["movies", "movie"]
                rows = _search_v2_all(q, types)
                if rows: return rows
                rows = _search_v1(q)
                if rows: return rows
                return _search_metadata_provider(q)
            else:
                types = ["episodes", "episode", "shows", "show", "series", "tv"]
                rows = _search_v2_all(q, types)
                if rows: return rows
                rows = _search_v1(q)
                if rows: return rows
                return _search_metadata_provider(q)

        def _titles_key(a: str) -> str:
            s = _remove_parens(a)
            s = _fold(s)
            s = re.sub(r"\s*&\s*", " and ", s, flags=re.IGNORECASE)
            s = _to_words_1_9(s)
            s = re.sub(r"\W+", " ", s).strip().lower()
            return re.sub(r"\s+", " ", s)

        def _titles_equal(a: str, b: str) -> bool:
            return _titles_key(a) == _titles_key(b)

        def _score(md: Mapping[str, Any]) -> int:
            s = 0
            t = (md.get("type") or "").lower()
            if kind == "episode":
                if t == "episode": s += 8
                elif t in ("show","series"): s += 6
            else:
                if t == "movie": s += 8

            mt = (md.get("grandparentTitle") if t == "episode" else (md.get("title") or "")) or ""
            if _titles_equal(mt, title): s += 8
            y = _year_from_any(md.get("year"))
            if kind == "movie":
                if year and y and abs(y - year) <= 1: s += 2
            elif kind == "episode" and t == "episode":
                if year and y and abs(y - year) <= 1: s += 2

            if kind == "episode" and t == "episode":
                si = md.get("parentIndex"); ei = md.get("index")
                if season is not None and si == season: s += 2
                if episode is not None and ei == episode: s += 2
            return s

        best, best_sc, best_t = None, -1, None
        tried = set()
        for q in _variants(title):
            if q in tried: continue
            tried.add(q)
            rows = _search_all(q)
            for md in rows:
                sc = _score(md)
                if sc > best_sc:
                    best, best_sc, best_t = md, sc, (md.get("type") or "").lower()
            if best_sc >= (12 if kind == "movie" else 12):
                break

        if not best:
            return None

        mt = (best.get("grandparentTitle") if best_t == "episode" else (best.get("title") or "")) or ""
        if not _titles_equal(mt, title):
            return None

        if year is not None:
            yb = _year_from_any(best.get("year"))
            if kind == "movie":
                if yb is not None and abs(yb - year) > 1: return None
            elif kind == "episode" and best_t == "episode":
                if yb is not None and abs(yb - year) > 1: return None

        return best

    except Exception:
        return None

#--- GUID fallback (SIMKL-friendly) --------------------------------------------
def minimal_from_history_row(row: Any, *, token: Optional[str] = None, allow_discover: bool = False) -> Optional[Dict[str, Any]]:
    key = _fb_key_from_row(row)
    memo = _fb_cache_load()
    hit = memo.get(key, None)
    if hit == _FBGUID_NOHIT:
        return None
    if isinstance(hit, dict) and hit:
        return dict(hit)

    ids = ids_from_history_row(row)
    kind = str((_row_get(row, "type") or "movie")).lower()
    m = _build_minimal_from_row(row, ids)

    if not _has_ext_ids(m.get("ids", {})):
        rk = m.get("ids", {}).get("plex")
        tok = token or _PLEX_CTX["token"]
        if rk and tok:
            _emit({"feature":"common","event":"fallback_guid","action":"enrich_by_rk_try","rk":str(rk)})
            extra = hydrate_external_ids(tok, str(rk))
            _emit({"feature":"common","event":"fallback_guid","action":("enrich_by_rk_ok" if extra else "enrich_by_rk_miss"),"rk":str(rk)})
            if extra:
                m["ids"].update({k: v for k, v in extra.items() if v})

    if kind == "episode" and not _has_ext_ids(m.get("show_ids", {})):
        tok = token or _PLEX_CTX["token"]
        gp_rk = _row_get(row, "grandparentRatingKey")
        if tok and gp_rk:
            _emit({"feature":"common","event":"fallback_guid","action":"enrich_show_by_rk_try","rk":str(gp_rk)})
            extra2 = hydrate_external_ids(tok, str(gp_rk))
            _emit({"feature":"common","event":"fallback_guid","action":("enrich_show_by_rk_ok" if extra2 else "enrich_show_by_rk_miss"),"rk":str(gp_rk)})
            if extra2:
                m.setdefault("show_ids", {}).update({k: v for k, v in extra2.items() if v})

    if not _has_ext_ids(m.get("ids", {})) and allow_discover:
        tok = token or _PLEX_CTX["token"]
        title = m.get("series_title") if kind == "episode" else m.get("title")
        year = m.get("year")

        _emit({"feature":"common","event":"fallback_guid","action":"discover_try","title":str(title or ""), "kind":kind, "year":year})
        md = _discover_search_title(
            tok,
            str(title or ""),
            kind,
            year,
            season=m.get("season"),
            episode=m.get("episode"),
        )
        _emit({"feature":"common","event":"fallback_guid","action":("discover_ok" if md else "discover_miss"), "title":str(title or ""), "kind":kind, "year":year})

        if md:
            nd = normalize_discover_row(md, token=tok)

            def _pairs(d: Optional[Mapping[str, Any]]) -> set:
                return {(k, v) for (k, v) in (d or {}).items() if k in ("imdb", "tmdb", "tvdb") and v}

            cur_ids, new_ids = _pairs(m.get("ids")), _pairs(nd.get("ids"))
            overlap_ok = (not cur_ids or not new_ids or bool(cur_ids & new_ids))

            if kind == "episode":
                cur_sid, new_sid = _pairs(m.get("show_ids")), _pairs(nd.get("show_ids"))
                if cur_sid and new_sid and not (cur_sid & new_sid):
                    overlap_ok = False

            has_ext = _has_ext_ids(nd.get("ids", {})) or (kind == "episode" and _has_ext_ids(nd.get("show_ids", {})))
            if overlap_ok and has_ext:
                if _has_ext_ids(nd.get("ids", {})):
                    m["ids"].update({k: v for k, v in nd["ids"].items() if v})
                if kind == "episode" and _has_ext_ids(nd.get("show_ids", {})):
                    m.setdefault("show_ids", {}).update({k: v for k, v in nd["show_ids"].items() if v})
                if kind == "episode":
                    if m.get("season") is None: m["season"] = nd.get("season")
                    if m.get("episode") is None: m["episode"] = nd.get("episode")
                    if not m.get("series_title"): m["series_title"] = nd.get("series_title") or nd.get("title")
                if not m.get("title") and nd.get("title"):
                    m["title"] = nd["title"]

    if not (m.get("title") or m.get("series_title")):
        _FBGUID_MEMO[key] = _FBGUID_NOHIT
        _fb_cache_save()
        return None

    if not _has_ext_ids(m.get("ids", {})) and not _has_ext_ids(m.get("show_ids", {})):
        _FBGUID_MEMO[key] = _FBGUID_NOHIT
        _fb_cache_save()
        return None

    _FBGUID_MEMO[key] = dict(m)
    _fb_cache_save()
    return m
