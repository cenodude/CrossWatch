# /providers/sync/plex/_common.py
# Plex Module for common utilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import re
import shutil
import time
from datetime import datetime, timezone
import unicodedata
import uuid
import socket
import xml.etree.ElementTree as ET
from pathlib import Path
from threading import RLock
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

from .._log import log as cw_log

import requests

STATE_DIR = Path("/config/.cw_state")


def _pair_scope() -> str | None:
    for k in ("CW_PAIR_KEY", "CW_PAIR_SCOPE", "CW_SYNC_PAIR", "CW_PAIR"):
        v = os.getenv(k)
        if v and str(v).strip():
            return str(v).strip()
    return None




def _is_capture_mode() -> bool:
    v = str(os.getenv("CW_CAPTURE_MODE") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _safe_scope(value: str) -> str:
    s = "".join(ch if (ch.isalnum() or ch in ("-", "_", ".")) else "_" for ch in str(value))
    s = s.strip("_ ")
    while "__" in s:
        s = s.replace("__", "_")
    return s[:96] if s else "default"


def scope_safe() -> str:
    scope = _pair_scope()
    return _safe_scope(scope) if scope else "unscoped"


def state_file(name: str) -> Path:
    safe = scope_safe()
    p = Path(name)
    scoped = STATE_DIR / (f"{p.stem}.{safe}{p.suffix}" if p.suffix else f"{name}.{safe}")
    legacy = STATE_DIR / (f"{p.stem}{p.suffix}" if p.suffix else name)
    if (not _is_capture_mode()) and (not scoped.exists()) and legacy.exists():
        try:
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            shutil.copy2(legacy, scoped)
        except Exception:
            pass
    return scoped


def read_json(path: Path) -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    try:
        return json.loads(path.read_text("utf-8") or "{}")
    except Exception:
        return {}


def write_json(
    path: Path,
    data: Mapping[str, Any],
    *,
    indent: int = 2,
    sort_keys: bool = True,
    separators: tuple[str, str] | None = None,
) -> None:
    if _pair_scope() is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_text(
            json.dumps(
                dict(data),
                ensure_ascii=False,
                indent=indent,
                sort_keys=sort_keys,
                separators=separators,
            ),
            "utf-8",
        )
        os.replace(tmp, path)
    except Exception:
        pass


try:
    from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from_guid
except ImportError:
    from _id_map import minimal as id_minimal, ids_from_guid  # type: ignore

_PLEX_CTX: dict[str, str | None] = {"baseurl": None, "token": None}


def configure_plex_context(*, baseurl: str | None, token: str | None) -> None:
    _PLEX_CTX["baseurl"] = baseurl.rstrip("/") if isinstance(baseurl, str) else None
    _PLEX_CTX["token"] = token or None


DISCOVER = "https://discover.provider.plex.tv"
METADATA = "https://metadata.provider.plex.tv"


_CLIENT_ID_LOCK: RLock = RLock()
_CLIENT_ID_CACHE: str | None = None


def stable_client_id() -> str:
    global _CLIENT_ID_CACHE
    for k in ("CW_PLEX_CID", "PLEX_CLIENT_IDENTIFIER", "X_PLEX_CLIENT_IDENTIFIER"):
        v = os.environ.get(k)
        if v and v.strip():
            return v.strip()

    base_state_dir = "/config/.cw_state" if os.path.isdir("/config") else ".cw_state"
    cid_dir = os.path.join(base_state_dir, "id")
    try:
        os.makedirs(cid_dir, exist_ok=True)
    except Exception:
        pass

    cid_path = os.path.join(cid_dir, "plex_client_id.txt")
    legacy_path = os.path.join(base_state_dir, "plex_client_id.txt")
    with _CLIENT_ID_LOCK:
        if _CLIENT_ID_CACHE:
            return _CLIENT_ID_CACHE
        try:
            if os.path.isfile(cid_path):
                existing = (Path(cid_path).read_text("utf-8") or "").strip()
                if existing:
                    _CLIENT_ID_CACHE = existing
                    return existing
            if os.path.isfile(legacy_path):
                legacy = (Path(legacy_path).read_text("utf-8") or "").strip()
                if legacy:
                    try:
                        Path(cid_path).write_text(legacy, "utf-8")
                        try:
                            os.remove(legacy_path)
                        except Exception:
                            pass
                    except Exception:
                        pass
                    _CLIENT_ID_CACHE = legacy
                    return legacy
            new_id = f"crosswatch-{uuid.uuid4().hex[:16]}"
            Path(cid_path).write_text(new_id, "utf-8")
            _CLIENT_ID_CACHE = new_id
            return new_id
        except Exception:
            host = socket.gethostname() or "host"
            cid = f"crosswatch-{host}"
            _CLIENT_ID_CACHE = cid
            return cid


def set_client_id(cid: str) -> None:
    global CLIENT_ID, _CLIENT_ID_CACHE
    v = str(cid or "").strip()
    if not v:
        return
    with _CLIENT_ID_LOCK:
        _CLIENT_ID_CACHE = v
    CLIENT_ID = v


CLIENT_ID = stable_client_id()



def make_logger(feature: str):
    feat = str(feature or "common")
    def dbg(event: str, **fields: Any) -> None:
        cw_log("PLEX", feat, "debug", event, **fields)
    def info(event: str, **fields: Any) -> None:
        cw_log("PLEX", feat, "info", event, **fields)
    def warn(event: str, **fields: Any) -> None:
        cw_log("PLEX", feat, "warn", event, **fields)
    def error(event: str, **fields: Any) -> None:
        cw_log("PLEX", feat, "error", event, **fields)
    def log(msg: str) -> None:
        dbg(msg)
    return dbg, info, warn, error, log


# Meta enrichment log control
_META_LOG_LOCK: RLock = RLock()
_META_ENRICH_COUNTS: dict[str, int] = {}
_META_ENRICH_SHOWN: int = 0
_META_ENRICH_SUPPRESSED: int = 0
_META_ENRICH_LAST_FLUSH: float = time.monotonic()


def _meta_enrich_log_mode() -> str:
    v = str(os.getenv("CW_PLEX_META_ENRICH_LOG") or "").strip().lower()
    return v or "summary"

def _meta_enrich_detail_limit() -> int:
    try:
        return max(0, int(os.getenv("CW_PLEX_META_ENRICH_DETAIL_LIMIT") or "25"))
    except Exception:
        return 25

def _meta_enrich_flush_interval() -> float:
    try:
        return max(0.0, float(os.getenv("CW_PLEX_META_ENRICH_FLUSH_S") or "30"))
    except Exception:
        return 30.0

def _meta_enrich_record(action: str) -> None:
    with _META_LOG_LOCK:
        _META_ENRICH_COUNTS[action] = _META_ENRICH_COUNTS.get(action, 0) + 1

def _meta_enrich_note_shown() -> None:
    global _META_ENRICH_SHOWN
    with _META_LOG_LOCK:
        _META_ENRICH_SHOWN += 1

def _meta_enrich_note_suppressed() -> None:
    global _META_ENRICH_SUPPRESSED
    with _META_LOG_LOCK:
        _META_ENRICH_SUPPRESSED += 1

def _meta_enrich_maybe_flush(*, level: str = "debug") -> dict[str, Any] | None:
    global _META_ENRICH_LAST_FLUSH, _META_ENRICH_SHOWN, _META_ENRICH_SUPPRESSED
    interval = _meta_enrich_flush_interval()
    if interval <= 0:
        return None
    now = time.monotonic()
    with _META_LOG_LOCK:
        if not _META_ENRICH_COUNTS:
            _META_ENRICH_LAST_FLUSH = now
            return None
        if (now - _META_ENRICH_LAST_FLUSH) < interval:
            return None
        counts = dict(_META_ENRICH_COUNTS)
        suppressed = int(_META_ENRICH_SUPPRESSED)
        shown = int(_META_ENRICH_SHOWN)
        _META_ENRICH_COUNTS.clear()
        _META_ENRICH_SHOWN = 0
        _META_ENRICH_SUPPRESSED = 0
        _META_ENRICH_LAST_FLUSH = now
    return {
        "feature": "common",
        "event": "meta_enrich",
        "action": "summary",
        "level": level,
        "counts": counts,
        "shown": shown,
        "suppressed": suppressed,
    }

def emit(evt: dict[str, Any], *, default_feature: str = "common") -> None:
    try:
        feat = str(evt.get("feature") or default_feature)
        event = str(evt.get("event") or "event")
        action = evt.get("action")
        fields = {k: v for k, v in evt.items() if k not in {"feature", "event", "action", "level"}}
        if action is not None:
            fields["action"] = action

        if event == "meta_enrich":
            act = str(action or "event")
            if act != "summary":
                _meta_enrich_record(act)
                mode = _meta_enrich_log_mode()

                if mode == "off":
                    _meta_enrich_note_suppressed()
                    summ = _meta_enrich_maybe_flush()
                    if summ:
                        s_feat = str(summ.get("feature") or default_feature)
                        s_event = str(summ.get("event") or "event")
                        s_action = summ.get("action")
                        s_fields = {k: v for k, v in summ.items() if k not in {"feature", "event", "action", "level"}}
                        if s_action is not None:
                            s_fields["action"] = s_action
                        s_level = str(summ.get("level") or "debug")
                        cw_log("PLEX", s_feat, s_level, s_event, **s_fields)
                    return

                if mode == "summary" and not act.endswith("_miss"):
                    _meta_enrich_note_suppressed()
                    summ = _meta_enrich_maybe_flush()
                    if summ:
                        s_feat = str(summ.get("feature") or default_feature)
                        s_event = str(summ.get("event") or "event")
                        s_action = summ.get("action")
                        s_fields = {k: v for k, v in summ.items() if k not in {"feature", "event", "action", "level"}}
                        if s_action is not None:
                            s_fields["action"] = s_action
                        s_level = str(summ.get("level") or "debug")
                        cw_log("PLEX", s_feat, s_level, s_event, **s_fields)
                    return

                if mode == "detail":
                    global _META_ENRICH_SHOWN, _META_ENRICH_SUPPRESSED
                    limit = _meta_enrich_detail_limit()
                    # Rate-limit detail lines per flush interval.
                    with _META_LOG_LOCK:
                        if _META_ENRICH_SHOWN >= limit:
                            _META_ENRICH_SUPPRESSED += 1
                            summ = _meta_enrich_maybe_flush()
                            if summ:
                                s_feat = str(summ.get("feature") or default_feature)
                                s_event = str(summ.get("event") or "event")
                                s_action = summ.get("action")
                                s_fields = {k: v for k, v in summ.items() if k not in {"feature", "event", "action", "level"}}
                                if s_action is not None:
                                    s_fields["action"] = s_action
                                s_level = str(summ.get("level") or "debug")
                                cw_log("PLEX", s_feat, s_level, s_event, **s_fields)
                            return
                        _META_ENRICH_SHOWN += 1
                else:
                    _meta_enrich_note_shown()

        level = str(evt.get("level") or "").strip().lower()
        if level not in ("debug", "info", "warn", "error"):
            if event == "meta_enrich" and str(action or "").endswith("_miss"):
                level = "warn"
            else:
                level = "debug" if event in ("meta_enrich", "hydrate") else "info"

        cw_log("PLEX", feat, level, event, **fields)
    except Exception:
        pass


def _plex_cfg(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    return cfg.get("plex", {}) if isinstance(cfg, dict) else {}


_dbg, _info, _warn, _error, _log = make_logger("common")


def _emit(evt: dict[str, Any]) -> None:
    emit(evt, default_feature="common")


def key_of(item: Mapping[str, Any]) -> str:
    try:
        m = normalize(item)
    except Exception:
        m = id_minimal(item)
    return canonical_key(m) or ""

def plex_headers(
    token: str,
    *,
    product: str = "CrossWatch",
    platform: str = "CrossWatch",
    version: str = "5.0.0",
    client_id: str | None = None,
    accept: str = "application/json, application/xml;q=0.9, */*;q=0.5",
    user_agent: str | None = None,
) -> dict[str, str]:
    cid = str(client_id or CLIENT_ID or "").strip() or stable_client_id()
    headers = {
        "X-Plex-Product": product,
        "X-Plex-Platform": platform,
        "X-Plex-Version": version,
        "X-Plex-Client-Identifier": cid,
        "X-Plex-Token": token,
        "Accept": accept,
    }
    if user_agent:
        headers["User-Agent"] = str(user_agent)
    return headers

def _safe_int(v: Any) -> int | None:
    try:
        if v is None:
            return None
        s = str(v).strip()
        return int(s) if s else None
    except Exception:
        return None


def _as_base_url(srv: Any) -> str | None:
    if not srv:
        return None

    def _root(u: str) -> str | None:
        try:
            p = urlsplit(u)
        except Exception:
            return None
        if not p.scheme or not p.netloc:
            return None
        return f"{p.scheme}://{p.netloc}"

    for attr in ("baseurl", "_baseurl", "serverUrl", "_serverUrl"):
        v = getattr(srv, attr, None)
        if isinstance(v, str):
            out = _root(v)
            if out:
                return out

    u = getattr(srv, "url", None)
    if callable(u):
        for key in ("/", ""):
            try:
                v = u(key, includeToken=False)
            except TypeError:
                try:
                    v = u(key)
                except Exception:
                    v = None
            except Exception:
                v = None
            if isinstance(v, str):
                out = _root(v)
                if out:
                    return out
    return None


def type_of(obj: Any) -> str:
    t = (getattr(obj, "type", None) or "").lower()
    return t if t in ("movie", "show", "season", "episode") else "movie"


def ids_from_obj(obj: Any) -> dict[str, str]:
    ids: dict[str, str] = {}
    rk = getattr(obj, "ratingKey", None)
    if rk is not None:
        ids["plex"] = str(rk)
    g = getattr(obj, "guid", None)
    if g:
        ids.update(ids_from_guid(str(g)))
    for gg in (getattr(obj, "guids", []) or []):
        val = getattr(gg, "id", None)
        if val:
            ids.update(ids_from_guid(str(val)))
    return {k: v for k, v in ids.items() if v and str(v).strip().lower() not in ("none", "null")}


def show_ids_hint(obj: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    gp = getattr(obj, "grandparentGuid", None)
    if gp:
        out.update(ids_from_guid(str(gp)))
    gp_rk = getattr(obj, "grandparentRatingKey", None)
    if gp_rk:
        out["plex"] = str(gp_rk)
    return {k: v for k, v in out.items() if v}


def server_find_rating_key_by_guid(srv: Any, guids: Iterable[str]) -> str | None:
    base = _as_base_url(srv)
    tok = getattr(srv, "token", None) or getattr(srv, "_token", None) or ""
    ses = getattr(srv, "_session", None)
    if not (base and ses):
        return None
    hdrs = dict(getattr(ses, "headers", {}) or {})
    hdrs.update(plex_headers(tok))
    hdrs["Accept"] = "application/json"
    for g in [x for x in (guids or []) if x]:
        try:
            r = ses.get(f"{base}/library/all", params={"guid": g}, headers=hdrs, timeout=8)
            if not r.ok:
                continue
            j = r.json() if r.headers.get("Content-Type", "").startswith("application/json") else {}
            md = (j.get("MediaContainer", {}) or {}).get("Metadata") or []
            if md and isinstance(md, list):
                rk = md[0].get("ratingKey") or md[0].get("ratingkey")
                if rk:
                    return str(rk)
        except Exception:
            pass
    return None


_FBGUID_MEMO: dict[str, Any] = {}
_FBGUID_NOHIT = "__NOHIT__"
def _fbguid_cache_path() -> Path:
    return state_file("plex_fallback_memo.json")



def _fb_key_from_row(row: Any) -> str:
    def g(obj: Any, *names: str) -> str:
        for n in names:
            v = getattr(obj, n, None)
            if v:
                return str(v).strip().lower()
        if isinstance(obj, dict):
            for n in names:
                v = obj.get(n)
                if v:
                    return str(v).strip().lower()
        return ""

    t = g(row, "type")
    g0 = g(row, "guid")
    gp = g(row, "parentGuid")
    gg = g(row, "grandparentGuid")
    gprk = g(row, "grandparentRatingKey")
    if not t and isinstance(row, dict):
        t = str(row.get("type", "")).lower()
    if not g0 and isinstance(row, dict):
        g0 = str(row.get("guid", "")).lower()
    if not gp and isinstance(row, dict):
        gp = str(row.get("parentGuid", "")).lower()
    if not gg and isinstance(row, dict):
        gg = str(row.get("grandparentGuid", "")).lower()
    if isinstance(row, dict):
        title = str(row.get("grandparentTitle") or row.get("title") or "").strip().lower()
        year = row.get("year")
    else:
        title = (getattr(row, "grandparentTitle", None) or getattr(row, "title", None) or "")
        title = str(title).strip().lower()
        year = getattr(row, "year", None)
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


def _fb_cache_load() -> dict[str, Any]:
    if _FBGUID_MEMO:
        return _FBGUID_MEMO
    try:
        data = read_json(_fbguid_cache_path())
        if isinstance(data, dict):
            _FBGUID_MEMO.update(data)
    except Exception:
        pass
    return _FBGUID_MEMO


def _fb_cache_save() -> None:
    try:
        write_json(_fbguid_cache_path(), _FBGUID_MEMO, indent=0, sort_keys=False, separators=(",", ":"))
    except Exception:
        pass

_SHOW_PMS_GUID_CACHE: dict[str, dict[str, str]] = {}
_EP_SHOW_IDS_CACHE: dict[str, dict[str, str]] = {}

def _hydrate_show_ids_from_episode_rk(token: str | None, episode_rk: str | None) -> dict[str, str]:
    if not token or not episode_rk:
        return {}
    rk = str(episode_rk).strip()
    if not rk:
        return {}

    if rk in _EP_SHOW_IDS_CACHE:
        return dict(_EP_SHOW_IDS_CACHE[rk])

    headers = plex_headers(token)
    headers["Accept"] = "application/json, application/xml;q=0.9,*/*;q=0.5"
    base = str(_PLEX_CTX.get("baseurl") or "").strip().rstrip("/")

    def _parse(r: requests.Response) -> dict[str, str]:
        ctype = (r.headers.get("content-type") or "").lower()
        if "application/json" in ctype:
            data = r.json() or {}
            mc = data.get("MediaContainer") or data
        else:
            mc = (_xml_to_container(r.text or "") or {}).get("MediaContainer") or {}
        md = mc.get("Metadata") or []
        if not (isinstance(md, list) and md and isinstance(md[0], Mapping)):
            return {}
        md0 = md[0]
        gp_guid = md0.get("grandparentGuid") or md0.get("grandparent_guid")
        gp_rk = md0.get("grandparentRatingKey") or md0.get("grandparent_rating_key")
        out: dict[str, str] = {}
        if gp_guid:
            out.update({k: v for k, v in ids_from_guid(str(gp_guid)).items() if v})
        if gp_rk:
            out["plex"] = str(gp_rk)
            extra = hydrate_external_ids(token, str(gp_rk))
            if extra:
                out.update({k: v for k, v in extra.items() if v})
        return {k: v for k, v in out.items() if v}

    urls: list[str] = []
    if base:
        urls.append(f"{base}/library/metadata/{rk}")
    urls.append(f"{METADATA}/library/metadata/{rk}")

    for url in urls:
        try:
            r = requests.get(url, headers=headers, params={"includeGuids": 1}, timeout=10)
            if not r.ok:
                continue
            ids = _parse(r)
            _EP_SHOW_IDS_CACHE[rk] = dict(ids)
            return dict(ids)
        except Exception:
            continue

    _EP_SHOW_IDS_CACHE[rk] = {}
    return {}


def _hydrate_show_ids_from_pms(obj: Any) -> dict[str, str]:
    rk = getattr(obj, "grandparentRatingKey", None)
    if not rk:
        return {}
    rk = str(rk)
    if rk in _SHOW_PMS_GUID_CACHE:
        return _SHOW_PMS_GUID_CACHE[rk]
    srv = getattr(obj, "_server", None)
    base = _as_base_url(srv) or _PLEX_CTX["baseurl"]
    token = getattr(srv, "token", None) or getattr(srv, "_token", None) or _PLEX_CTX["token"]
    if not base or not token:
        _SHOW_PMS_GUID_CACHE[rk] = {}
        return {}
    url = f"{base}/library/metadata/{rk}?includeGuids=1"
    try:
        r = requests.get(
            url,
            headers={
                "X-Plex-Token": token,
                "Accept": "application/json, application/xml;q=0.9, */*;q=0.5",
            },
            timeout=8,
        )
        ids: dict[str, str] = {}
        if r.ok:
            ctype = (r.headers.get("content-type") or "").lower()
            if "application/json" in ctype:
                data = r.json()
                mc = data.get("MediaContainer") or data
                md = mc.get("Metadata") or []
                if md and isinstance(md, list):
                    for gg in md[0].get("Guid") or []:
                        gid = gg.get("id")
                        if gid:
                            ids.update(ids_from_guid(str(gid)))
            else:
                cont = _xml_to_container(r.text or "")
                mc = cont.get("MediaContainer") or {}
                md = mc.get("Metadata") or []
                if md and isinstance(md, list):
                    for gg in md[0].get("Guid") or []:
                        gid = gg.get("id")
                        if gid:
                            ids.update(ids_from_guid(str(gid)))
        ids = {k: v for k, v in ids.items() if v}
        _SHOW_PMS_GUID_CACHE[rk] = ids
        return ids
    except Exception as e:
        _warn("hydrate_show_pms_failed", rk=rk, error=str(e))
        _SHOW_PMS_GUID_CACHE[rk] = {}
        return {}


_GUID_CACHE: dict[str, dict[str, str]] = {}
_HYDRATE_404: set[str] = set()
_HYDRATE_LOCK: RLock = RLock()


def _xml_to_container(xml_text: str) -> Mapping[str, Any]:
    def _meta_row(elem: ET.Element) -> dict[str, Any]:
        a = elem.attrib
        return {
            "type": a.get("type"),
            "title": a.get("title"),
            "year": _safe_int(a.get("year")),
            "viewCount": _safe_int(a.get("viewCount")),
            "viewedAt": _safe_int(a.get("viewedAt")),
            "lastViewedAt": _safe_int(a.get("lastViewedAt")),
            "guid": a.get("guid"),
            "ratingKey": a.get("ratingKey"),
            "parentGuid": a.get("parentGuid"),
            "parentRatingKey": a.get("parentRatingKey"),
            "parentTitle": a.get("parentTitle"),
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
            "userRating": a.get("userRating"),
            "lastRatedAt": a.get("lastRatedAt"),
            "Guid": [{"id": (g.attrib.get("id") or "")} for g in elem.findall("./Guid") if g.attrib.get("id")],
        }

    root = ET.fromstring(xml_text)
    mc = root if root.tag.endswith("MediaContainer") else root.find(".//MediaContainer")
    if mc is None:
        return {"MediaContainer": {"Metadata": [], "SearchResults": []}}

    out_mc: dict[str, Any] = {}
    for k in ("totalSize", "size", "offset"):
        if k in mc.attrib:
            out_mc[k] = _safe_int(mc.attrib.get(k))

    rows: list[Mapping[str, Any]] = []
    for elem in list(mc):
        if getattr(elem, "tag", "") == "SearchResults":
            continue
        a = getattr(elem, "attrib", {}) or {}
        if not a:
            continue
        if not (a.get("ratingKey") or a.get("guid") or a.get("type")):
            continue
        rows.append(_meta_row(elem))

    sr_list: list[Mapping[str, Any]] = []
    for sr in mc.findall("./SearchResults"):
        sr_obj: dict[str, Any] = {
            "id": sr.attrib.get("id"),
            "title": sr.attrib.get("title"),
            "size": _safe_int(sr.attrib.get("size")),
            "SearchResult": [],
        }
        for it in sr.findall("./SearchResult"):
            md = it.find("./Metadata")
            if md is not None:
                sr_obj["SearchResult"].append({"Metadata": _meta_row(md)})
                continue
            md_attr = it.attrib.get("Metadata")
            if md_attr and md_attr != "[object Object]":
                sr_obj["SearchResult"].append({"Metadata": {"title": md_attr}})
        sr_list.append(sr_obj)

    out_mc["Metadata"] = rows
    out_mc["SearchResults"] = sr_list
    return {"MediaContainer": out_mc}

def hydrate_external_ids(token: str | None, rating_key: str | None) -> dict[str, str]:
    if not token or not rating_key:
        return {}
    rk = str(rating_key).strip()
    if not rk:
        return {}
    with _HYDRATE_LOCK:
        if rk in _GUID_CACHE:
            return _GUID_CACHE[rk]
        if rk in _HYDRATE_404:
            return {}

    headers = plex_headers(token)
    base = str(_PLEX_CTX.get("baseurl") or "").strip().rstrip("/")

    meta_status: int | None = None
    last_err: str | None = None

    def _parse_response(r: requests.Response) -> dict[str, str]:
        ctype = (r.headers.get("content-type") or "").lower()
        ids: dict[str, str] = {}
        if "application/json" in ctype:
            data = r.json()
            mc = data.get("MediaContainer") or data
            md = mc.get("Metadata") or []
            if md and isinstance(md, list):
                for gg in md[0].get("Guid") or []:
                    gid = gg.get("id")
                    if gid:
                        ids.update(ids_from_guid(str(gid)))
        else:
            cont = _xml_to_container(r.text or "")
            mc = cont.get("MediaContainer") or {}
            md = mc.get("Metadata") or []
            if md and isinstance(md, list):
                for gg in md[0].get("Guid") or []:
                    gid = gg.get("id")
                    if gid:
                        ids.update(ids_from_guid(str(gid)))
        return {k: v for k, v in ids.items() if v}

    urls: list[tuple[str, str]] = []
    if base:
        urls.append((f"{base}/library/metadata/{rk}", "pms"))
    urls.append((f"{METADATA}/library/metadata/{rk}", "meta"))

    for url, kind in urls:
        try:
            r = requests.get(url, headers=headers, params={"includeGuids": 1}, timeout=10)
            if kind == "meta":
                meta_status = r.status_code
            if r.status_code == 401:
                raise RuntimeError("Unauthorized (bad Plex token)")
            if not r.ok:
                if kind == "meta":
                    _dbg("hydrate_miss", rk=rk, status=r.status_code, source=kind)
                    _emit({"feature": "common", "event": "hydrate", "action": "miss", "rk": rk, "status": r.status_code})
                continue
            ids = _parse_response(r)
            with _HYDRATE_LOCK:
                _GUID_CACHE[rk] = ids
            return ids
        except Exception as e:
            last_err = str(e)
            continue

    if meta_status == 404:
        with _HYDRATE_LOCK:
            _HYDRATE_404.add(rk)
            _GUID_CACHE[rk] = {}
        return {}

    with _HYDRATE_LOCK:
        _GUID_CACHE[rk] = {}
    if last_err:
        _warn("hydrate_failed", rk=rk, error=last_err, meta_status=meta_status)
    return {}

def normalize(obj: Any) -> dict[str, Any]:
    t = type_of(obj)
    ids = ids_from_obj(obj)
    base: dict[str, Any] = {
        "type": t,
        "title": getattr(obj, "title", None),
        "year": getattr(obj, "year", None),
        "ids": ids,
        "guid": getattr(obj, "guid", None),
    }
    lid = _safe_int(
        getattr(obj, "librarySectionID", None)
        or getattr(obj, "sectionID", None)
        or getattr(obj, "librarySectionId", None)
        or getattr(obj, "sectionId", None)
    )
    if lid is not None:
        base["library_id"] = lid
    if t in ("season", "episode"):
        sid = show_ids_hint(obj)
        if sid:
            base["show_ids"] = sid

        def has_ext(m: Any) -> bool:
            return bool(isinstance(m, dict) and any(m.get(k) for k in ("tmdb", "imdb", "tvdb")))

        if not has_ext(base.get("show_ids")):
            extra = _hydrate_show_ids_from_pms(obj)
            if extra:
                base.setdefault("show_ids", {}).update(extra)
        if not has_ext(base.get("show_ids")):
            srv = getattr(obj, "_server", None)
            token = getattr(srv, "_token", None) or getattr(srv, "token", None) or _PLEX_CTX["token"]
            gp_rk = getattr(obj, "grandparentRatingKey", None)
            if token and gp_rk:
                extra2 = hydrate_external_ids(token, str(gp_rk))
                if extra2:
                    base.setdefault("show_ids", {}).update(extra2)
    if t == "season":
        base["season"] = _safe_int(getattr(obj, "index", None))
    if t == "episode":
        base["season"] = _safe_int(
            getattr(obj, "seasonNumber", None) if hasattr(obj, "seasonNumber") else getattr(obj, "parentIndex", None)
        )
        base["episode"] = _safe_int(getattr(obj, "index", None))
        base["series_title"] = getattr(obj, "grandparentTitle", None)
    keep_show_ids = base.get("show_ids") if t in ("season", "episode") else None
    res = id_minimal(base)
    if keep_show_ids:
        res["show_ids"] = keep_show_ids
    if "library_id" in base:
        res["library_id"] = base["library_id"]
    return res


def ids_from_discover_row(row: Mapping[str, Any]) -> dict[str, str]:
    ids: dict[str, str] = {}
    g = row.get("guid")
    if g:
        ids.update(ids_from_guid(str(g)))
    for gg in row.get("Guid") or []:
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


def normalize_discover_row(row: Mapping[str, Any], *, token: str | None = None) -> dict[str, Any]:
    if token is None:
        token = _PLEX_CTX["token"]
    t = (row.get("type") or "movie").lower()
    ids = ids_from_discover_row(row)
    if not any(k in ids for k in ("tmdb", "imdb", "tvdb")) and token:
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
        if token and not any(base.get("show_ids", {}).get(k) for k in ("tmdb", "imdb", "tvdb")):
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
    keep_show_ids = base.get("show_ids") if t in ("season", "episode") else None
    res = id_minimal(base)
    if keep_show_ids:
        res["show_ids"] = keep_show_ids
    if "library_id" in base:
        res["library_id"] = base["library_id"]
    return res


def sort_guid_candidates(guids: list[str], *, priority: list[str] | None = None) -> list[str]:
    if not guids:
        return []

    if priority:
        pri = [str(p).strip().lower() for p in priority if str(p).strip()]
        if not pri:
            pri = []

        def score(g: str) -> tuple[int, int]:
            s = g.lower()
            for i, p in enumerate(pri):
                if p == "tmdb" and (s.startswith("tmdb://") or s.startswith("themoviedb://") or s.startswith("com.plexapp.agents.themoviedb://")):
                    return (i, len(s))
                if p == "imdb" and (s.startswith("imdb://") or s.startswith("com.plexapp.agents.imdb://")):
                    return (i, len(s))
                if p == "tvdb" and (s.startswith("tvdb://") or s.startswith("com.plexapp.agents.thetvdb://")):
                    return (i, len(s))
                if p == "agent:themoviedb:en" and s.startswith("com.plexapp.agents.themoviedb://") and "?lang=en" in s:
                    return (i, len(s))
                if p == "agent:themoviedb" and s.startswith("com.plexapp.agents.themoviedb://"):
                    return (i, len(s))
                if p == "agent:imdb" and s.startswith("com.plexapp.agents.imdb://"):
                    return (i, len(s))
                if p == "agent:tvdb" and s.startswith("com.plexapp.agents.thetvdb://"):
                    return (i, len(s))
            return (99, len(s))

        return sorted(list(guids), key=score)

    pri: list[str] = []
    rest = list(guids)

    def pick(prefix: str, contains: Any | None = None) -> list[str]:
        out = [g for g in rest if (g.startswith(prefix) if contains is None else contains(g))]
        for g in out:
            rest.remove(g)
        return out

    # Prefer TMDB first (including common Plex agent GUID variants), then IMDb, then TVDB.
    pri += pick("tmdb://")
    pri += pick("themoviedb://")
    pri += pick("", contains=lambda g: g.startswith("com.plexapp.agents.themoviedb://") and "?lang=en" in g)
    pri += pick("", contains=lambda g: g.startswith("com.plexapp.agents.themoviedb://") and "?lang=en-US" in g)
    pri += pick("com.plexapp.agents.themoviedb://")
    pri += pick("imdb://")
    pri += pick("com.plexapp.agents.imdb://")
    pri += pick("tvdb://")
    pri += pick("", contains=lambda g: g.startswith("com.plexapp.agents.thetvdb://") and "?lang=en" in g)
    pri += pick("", contains=lambda g: g.startswith("com.plexapp.agents.thetvdb://") and "?lang=en-US" in g)
    pri += pick("com.plexapp.agents.thetvdb://")
    return pri + rest

def candidate_guids_from_ids(it: Mapping[str, Any], *, include_raw_ids: bool = False) -> list[str]:
    ids = it.get("ids") or {}
    ids = ids if isinstance(ids, dict) else {}
    out: list[str] = []

    def add(v: str | None) -> None:
        if v and v not in out:
            out.append(v)

    imdb = ids.get("imdb")
    tmdb = ids.get("tmdb")
    tvdb = ids.get("tvdb")
    if tmdb:
        add(f"tmdb://{tmdb}")
        add(f"themoviedb://{tmdb}")
        add(f"com.plexapp.agents.themoviedb://{tmdb}?lang=en")
        add(f"com.plexapp.agents.themoviedb://{tmdb}?lang=en-US")
        add(f"com.plexapp.agents.themoviedb://{tmdb}")
        if include_raw_ids:
            add(str(tmdb))
    if imdb:
        add(f"imdb://{imdb}")
        add(f"com.plexapp.agents.imdb://{imdb}")
        if include_raw_ids:
            add(str(imdb))
    if tvdb:
        add(f"tvdb://{tvdb}")
        add(f"com.plexapp.agents.thetvdb://{tvdb}")
        add(f"com.plexapp.agents.thetvdb://{tvdb}?lang=en")
        add(f"com.plexapp.agents.thetvdb://{tvdb}?lang=en-US")
        if include_raw_ids:
            add(str(tvdb))

    g = it.get("guid")
    if g:
        add(str(g))
    return out

def _iso8601_any(v: Any) -> str | None:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if not s:
            return None
        if s.isdigit():
            ts = int(s)
            if len(s) >= 13:
                ts //= 1000
            return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(ts))
        if "T" in s:
            return s if s.endswith("Z") else s + "Z"
        return None
    except Exception:
        return None


def _watched_at_from_row(row: Any) -> str | None:
    v = _row_get(
        row,
        "viewedAt",
        "viewed_at",
        "lastViewedAt",
        "last_viewed_at",
        "watchedAt",
        "watched_at",
        "originallyWatchedAt",
        "originally_watched_at",
    )
    return _iso8601_any(v)


def _year_from_any(v: Any) -> int | None:
    try:
        if isinstance(v, int):
            return v
        s = str(v or "").strip()
        if not s:
            return None
        if s.isdigit() and len(s) in (4, 8):
            return int(s[:4])
        return int(s[:4]) if len(s) >= 4 and s[:4].isdigit() else None
    except Exception:
        return None


def _row_get(row: Any, *names: str) -> Any:
    for n in names:
        if isinstance(row, Mapping) and n in row:
            return row.get(n)
        if hasattr(row, n):
            return getattr(row, n)
    return None


def ids_from_history_row(row: Any) -> dict[str, str]:
    ids: dict[str, str] = {}
    rk = _row_get(row, "ratingKey", "key")
    if rk is not None:
        ids["plex"] = str(rk)
    for n in ("guid", "grandparentGuid", "parentGuid"):
        g = _row_get(row, n)
        if g:
            ids.update(ids_from_guid(str(g)))
    try:
        gg = _row_get(row, "Guid") or []
        if isinstance(gg, list):
            for it in gg:
                gid = (it.get("id") if isinstance(it, Mapping) else None) or getattr(it, "id", None)
                if gid:
                    ids.update(ids_from_guid(str(gid)))
    except Exception:
        pass
    return {k: v for k, v in ids.items() if v and str(v).strip().lower() not in ("none", "null")}


def _has_ext_ids(ids: Mapping[str, Any]) -> bool:
    try:
        return any(str(ids.get(k) or "").strip() for k in ("tmdb", "imdb", "tvdb"))
    except Exception:
        return False


def _build_minimal_from_row(row: Any, ids: Mapping[str, Any]) -> dict[str, Any]:
    kind = str((_row_get(row, "type") or "movie")).lower()
    is_ep = kind == "episode"
    title = _row_get(row, "grandparentTitle") if is_ep else _row_get(row, "title") or _row_get(row, "originalTitle")
    year = (
        _row_get(row, "year")
        or _row_get(row, "originallyAvailableAt")
        or _row_get(row, "originally_available_at")
        or _row_get(row, "grandparentYear")
    )
    base: dict[str, Any] = {
        "type": "episode" if is_ep else "movie",
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
            _row_get(row, "grandparentTitle") or _row_get(row, "title") or _row_get(row, "parentTitle")
        )
        base["season"] = _safe_int(_row_get(row, "parentIndex") or _row_get(row, "seasonNumber"))
        base["episode"] = _safe_int(_row_get(row, "index"))
        gp = _row_get(row, "grandparentGuid")
        gp_rk = _row_get(row, "grandparentRatingKey")
        sids: dict[str, Any] = {}
        if gp:
            sids.update({k: v for k, v in ids_from_guid(str(gp)).items() if v})
        if gp_rk:
            sids["plex"] = str(gp_rk)
        if not sids and base.get("season") is not None and base.get("episode") is not None:
            ext = {k: v for k, v in (base.get("ids") or {}).items() if k in ("tmdb", "imdb", "tvdb") and v}
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
    token: str | None,
    title: str,
    kind: str,
    year: int | None,
    limit: int = 15,
    season: int | None = None,
    episode: int | None = None,
) -> Mapping[str, Any] | None:
    try:
        if not title or not token:
            return None
        num_to_word = {
            "1": "one",
            "2": "two",
            "3": "three",
            "4": "four",
            "5": "five",
            "6": "six",
            "7": "seven",
            "8": "eight",
            "9": "nine",
        }
        word_to_num = {v: k for k, v in num_to_word.items()}

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
            return re.sub(
                r"\b([1-9])\b",
                lambda m: num_to_word.get(m.group(1), m.group(1)),
                s,
                flags=re.IGNORECASE,
            )

        def _to_digits_1_9(s: str) -> str:
            return re.sub(
                r"\b(one|two|three|four|five|six|seven|eight|nine)\b",
                lambda m: word_to_num.get(m.group(0).lower(), m.group(0)),
                s,
                flags=re.IGNORECASE,
            )

        def _and_amp_variants(s: str) -> list[str]:
            vs = [s]
            s1 = re.sub(r"\s*&\s*", " and ", s)
            if s1 != s:
                vs.append(re.sub(r"\s+", " ", s1).strip())
            s2 = re.sub(r"\band\b", "&", s, flags=re.IGNORECASE)
            if s2 != s:
                vs.append(re.sub(r"\s+", " ", s2).strip())
            return vs

        def _digit_word_variants(s: str) -> list[str]:
            vs = [s]
            sw = _to_words_1_9(s)
            if sw != s:
                vs.append(sw)
            sd = _to_digits_1_9(s)
            if sd != s:
                vs.append(sd)
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
            cut: list[str] = []
            for x in pool:
                p = re.split(r"[:\-–(]", x, 1)[0].strip()
                if p and p not in pool:
                    cut.append(p)
            pool += cut
            if year:
                for x in list(pool):
                    if len(x) < 64:
                        pool.append(f"{x} {year}")
            out: list[str] = []
            seen: set[str] = set()
            for x in pool:
                y = re.sub(r"\s+", " ", x).strip()
                if y and y not in seen:
                    seen.add(y)
                    out.append(y)
            return out

        def _collect_rows(j: Mapping[str, Any]) -> list[Mapping[str, Any]]:
            rows: list[Mapping[str, Any]] = []
            mc = j.get("MediaContainer") or {}
            for bucket in mc.get("SearchResults") or []:
                for item in bucket.get("SearchResult") or []:
                    md = item.get("Metadata")
                    if isinstance(md, Mapping):
                        rows.append(md)
                    elif isinstance(md, list):
                        rows.extend([x for x in md if isinstance(x, Mapping)])
            for hub in mc.get("Hub") or []:
                for md in hub.get("Metadata") or []:
                    if isinstance(md, Mapping):
                        rows.append(md)
            mds = mc.get("Metadata")
            if isinstance(mds, list):
                rows.extend([x for x in mds if isinstance(x, Mapping)])
            elif isinstance(mds, Mapping):
                rows.append(mds)
            return rows

        def _hdrs() -> dict[str, str]:
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
                    r = requests.get(
                        f"{DISCOVER}/library/search",
                        headers=_hdrs(),
                        params=params,
                        timeout=7,
                    )
                    if r.ok and "json" in (r.headers.get("content-type", "").lower()):
                        rows2 = _collect_rows(r.json())
                        if rows2:
                            rows.extend(rows2)
                except Exception:
                    continue
            return rows

        def _search_v1(q: str) -> list[Mapping[str, Any]]:
            try:
                params = {"query": q, "limit": max(50, int(limit)), "includeMeta": 1}
                r = requests.get(
                    f"{DISCOVER}/hubs/search",
                    headers=_hdrs(),
                    params=params,
                    timeout=7,
                )
                if r.ok and "json" in (r.headers.get("content-type", "").lower()):
                    return _collect_rows(r.json())
            except Exception:
                pass
            return []

        def _search_metadata_provider(q: str) -> list[Mapping[str, Any]]:
            try:
                r = requests.get(
                    f"{METADATA}/library/search",
                    headers=_hdrs(),
                    params={"query": q, "limit": max(50, int(limit))},
                    timeout=7,
                )
                if r.ok and "json" in (r.headers.get("content-type", "").lower()):
                    return _collect_rows(r.json())
            except Exception:
                pass
            return []

        def _search_all(q: str) -> list[Mapping[str, Any]]:
            if kind == "movie":
                types = ["movies", "movie"]
                rows = _search_v2_all(q, types)
                if rows:
                    return rows
                rows = _search_v1(q)
                if rows:
                    return rows
                return _search_metadata_provider(q)
            types = ["episodes", "episode", "shows", "show", "series", "tv"]
            rows = _search_v2_all(q, types)
            if rows:
                return rows
            rows = _search_v1(q)
            if rows:
                return rows
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
                if t == "episode":
                    s += 8
                elif t in ("show", "series"):
                    s += 6
            else:
                if t == "movie":
                    s += 8
            mt = (md.get("grandparentTitle") if t == "episode" else (md.get("title") or "")) or ""
            if _titles_equal(mt, title):
                s += 8
            y = _year_from_any(md.get("year"))
            if kind == "movie":
                if year and y and abs(y - year) <= 1:
                    s += 2
            elif kind == "episode" and t == "episode":
                if year and y and abs(y - year) <= 1:
                    s += 2
            if kind == "episode" and t == "episode":
                si = md.get("parentIndex")
                ei = md.get("index")
                if season is not None and si == season:
                    s += 2
                if episode is not None and ei == episode:
                    s += 2
            return s

        best: Mapping[str, Any] | None = None
        best_sc = -1
        best_t: str | None = None
        tried: set[str] = set()
        for q in _variants(title):
            if q in tried:
                continue
            tried.add(q)
            rows = _search_all(q)
            for md in rows:
                sc = _score(md)
                if sc > best_sc:
                    best = md
                    best_sc = sc
                    best_t = (md.get("type") or "").lower()
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
                if yb is not None and abs(yb - year) > 1:
                    return None
            elif kind == "episode" and best_t == "episode":
                if yb is not None and abs(yb - year) > 1:
                    return None
        return best
    except Exception:
        return None


def minimal_from_history_row(
    row: Any,
    *,
    token: str | None = None,
    allow_discover: bool = False,
) -> dict[str, Any] | None:
    key = _fb_key_from_row(row)
    memo = _fb_cache_load()
    hit = memo.get(key, None)
    if hit == _FBGUID_NOHIT and not allow_discover:
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
            _emit(
                {
                    "feature": "common",
                    "event": "meta_enrich",
                    "action": "enrich_by_rk_try",
                    "rk": str(rk),
                }
            )
            extra = hydrate_external_ids(tok, str(rk))
            _emit(
                {
                    "feature": "common",
                    "event": "meta_enrich",
                    "action": "enrich_by_rk_ok" if extra else "enrich_by_rk_miss",
                    "rk": str(rk),
                }
            )
            if extra:
                m["ids"].update({k: v for k, v in extra.items() if v})
                
    if kind == "episode" and not _has_ext_ids(m.get("show_ids", {})):
        tok = token or _PLEX_CTX["token"]
        gp_rk = _row_get(row, "grandparentRatingKey")
        if tok and gp_rk:
            _emit(
                {
                    "feature": "common",
                    "event": "meta_enrich",
                    "action": "enrich_show_by_rk_try",
                    "rk": str(gp_rk),
                }
            )
            extra2 = hydrate_external_ids(tok, str(gp_rk))
            _emit(
                {
                    "feature": "common",
                    "event": "meta_enrich",
                    "action": "enrich_show_by_rk_ok" if extra2 else "enrich_show_by_rk_miss",
                    "rk": str(gp_rk),
                }
            )
            if extra2:
                m.setdefault("show_ids", {}).update({k: v for k, v in extra2.items() if v})
                
        # Plex history rows lack grandparentRatingKey
        if not _has_ext_ids(m.get("show_ids", {})):
            ep_rk = str((m.get("ids") or {}).get("plex") or "").strip()
            if tok and ep_rk:
                extra3 = _hydrate_show_ids_from_episode_rk(tok, ep_rk)
                if extra3:
                    m.setdefault("show_ids", {}).update({k: v for k, v in extra3.items() if v})
                    
    if not _has_ext_ids(m.get("ids", {})) and allow_discover:
        tok = token or _PLEX_CTX["token"]
        title = m.get("series_title") if kind == "episode" else m.get("title")
        year = m.get("year")
        _emit(
            {
                "feature": "common",
                "event": "fallback_guid",
                "action": "discover_try",
                "title": str(title or ""),
                "kind": kind,
                "year": year,
            }
        )
        md = _discover_search_title(
            tok,
            str(title or ""),
            kind,
            year,
            season=m.get("season"),
            episode=m.get("episode"),
        )
        _emit(
            {
                "feature": "common",
                "event": "fallback_guid",
                "action": "discover_ok" if md else "discover_miss",
                "title": str(title or ""),
                "kind": kind,
                "year": year,
            }
        )
        if md:
            nd = normalize_discover_row(md, token=tok)

            def _pairs(d: Mapping[str, Any] | None) -> set[tuple[str, str]]:
                return {(k, v) for (k, v) in (d or {}).items() if k in ("tmdb", "imdb", "tvdb") and v}

            cur_ids = _pairs(m.get("ids"))
            new_ids = _pairs(nd.get("ids"))
            overlap_ok = not cur_ids or not new_ids or bool(cur_ids & new_ids)
            if kind == "episode":
                cur_sid = _pairs(m.get("show_ids"))
                new_sid = _pairs(nd.get("show_ids"))
                if cur_sid and new_sid and not (cur_sid & new_sid):
                    overlap_ok = False
            has_ext = _has_ext_ids(nd.get("ids", {})) or (kind == "episode" and _has_ext_ids(nd.get("show_ids", {})))

            if overlap_ok and has_ext:
                if _has_ext_ids(nd.get("ids", {})):
                    nd_ids = {k: v for k, v in (nd.get("ids", {}) or {}).items() if v}
                    m["ids"].update(nd_ids)
                    if kind == "episode" and not _has_ext_ids(m.get("show_ids", {})) and not _has_ext_ids(nd.get("show_ids", {})):
                        m.setdefault("show_ids", {}).update({k: v for k, v in nd_ids.items() if k in ("tmdb", "imdb", "tvdb")})

                if kind == "episode" and _has_ext_ids(nd.get("show_ids", {})):
                    m.setdefault("show_ids", {}).update({k: v for k, v in (nd.get("show_ids", {}) or {}).items() if v})

                if kind == "episode":
                    if m.get("season") is None:
                        m["season"] = nd.get("season")
                    if m.get("episode") is None:
                        m["episode"] = nd.get("episode")
                    if not m.get("series_title"):
                        m["series_title"] = nd.get("series_title") or nd.get("title")
                if not m.get("title") and nd.get("title"):
                    m["title"] = nd["title"]
                    
    if not (m.get("title") or m.get("series_title")):
        if allow_discover:
            _FBGUID_MEMO[key] = _FBGUID_NOHIT
            _fb_cache_save()
        return None
    
    if not _has_ext_ids(m.get("ids", {})) and not _has_ext_ids(m.get("show_ids", {})):
        if allow_discover:
            _FBGUID_MEMO[key] = _FBGUID_NOHIT
            _fb_cache_save()
        return None
    _FBGUID_MEMO[key] = dict(m)
    _fb_cache_save()
    return m


def home_scope_enter(adapter: Any) -> tuple[bool, bool, int | None, str | None]:
    cli = getattr(adapter, "client", None)
    if not cli:
        return False, False, None, None

    desired_aid = getattr(cli, "selected_account_id", None)
    desired_uname = getattr(cli, "selected_username", None)
    active_aid = getattr(cli, "user_account_id", None)
    active_uname = getattr(cli, "user_username", None)

    def _same_user(aid1: Any, uname1: Any, aid2: Any, uname2: Any) -> bool:
        try:
            if aid1 is not None and aid2 is not None and int(aid1) == int(aid2):
                return True
        except Exception:
            pass
        if uname1 and uname2 and str(uname1).strip().lower() == str(uname2).strip().lower():
            return True
        return False

    need = bool(desired_aid or desired_uname) and not _same_user(desired_aid, desired_uname, active_aid, active_uname)

    try:
        sel_aid = int(desired_aid) if desired_aid is not None else None
    except Exception:
        sel_aid = None
    try:
        sel_uname = (str(desired_uname).strip() or None) if desired_uname is not None else None
    except Exception:
        sel_uname = None

    if not need:
        return False, False, sel_aid, sel_uname

    try:
        if not bool(getattr(cli, "can_home_switch")()):
            return True, False, sel_aid, sel_uname
    except Exception:
        return True, False, sel_aid, sel_uname

    pin = (getattr(getattr(cli, "cfg", None), "home_pin", None) or "").strip() or None
    try:
        ok = bool(
            getattr(cli, "enter_home_user_scope")(
                target_username=(str(desired_uname).strip() if desired_uname else None),
                target_account_id=(int(desired_aid) if desired_aid is not None else None),
                pin=pin,
            )
        )
    except Exception:
        ok = False

    if not ok:
        _warn("home_scope_not_applied", selected=(desired_aid or desired_uname))
    return True, ok, sel_aid, sel_uname


def home_scope_exit(adapter: Any, did_switch: bool) -> None:
    if not did_switch:
        return
    cli = getattr(adapter, "client", None)
    if not cli:
        return
    try:
        getattr(cli, "exit_home_user_scope")()
    except Exception:
        pass


def as_epoch(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        n = int(v)
        return n // 1000 if n >= 10**12 else n
    if isinstance(v, datetime):
        return int(v.timestamp())
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
    return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def iso_from_epoch(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def episode_code(season: Any, episode: Any) -> str | None:
    try:
        s = int(season or 0)
        e = int(episode or 0)
    except Exception:
        return None
    if s <= 0 or e <= 0:
        return None
    return f"S{s:02d}E{e:02d}"


def force_episode_title(row: dict[str, Any]) -> None:
    if (row.get("type") or "").lower() != "episode":
        return
    code = episode_code(row.get("season"), row.get("episode"))
    if code:
        row["title"] = code



class UnresolvedStore:
    def __init__(self, feature: str):
        self.feature = str(feature or "common").strip().lower() or "common"

    def path(self) -> Path:
        return state_file(f"plex_{self.feature}.unresolved.json")

    def load(self) -> dict[str, Any]:
        return dict(read_json(self.path()) or {})

    def save(self, data: Mapping[str, Any]) -> None:
        try:
            write_json(self.path(), data)
        except Exception as e:
            _warn("unresolved_save_failed", feature=self.feature, path=str(self.path()), error=str(e))

    def event_key(self, item: Mapping[str, Any]) -> str:
        try:
            base = canonical_key(id_minimal(item)) or canonical_key(item) or ""
        except Exception:
            base = canonical_key(item) if "canonical_key" in globals() else ""
        if self.feature == "history":
            ts = as_epoch(item.get("watched_at"))
            return f"{base}@{ts}" if (base and ts) else (base or "")
        return base or ""

    def freeze(self, item: Mapping[str, Any], *, action: str, reasons: Iterable[str], extra: Mapping[str, Any] | None = None) -> str:
        key = self.event_key(item)
        if not key:
            return ""
        data = self.load()
        now = now_iso()
        entry = data.get(key) or {"feature": self.feature, "action": action, "first_seen": now, "attempts": 0}
        entry.update({"item": id_minimal(item), "last_attempt": now})
        if self.feature == "history" and item.get("watched_at") is not None:
            entry["watched_at"] = item.get("watched_at")

        rset = set(entry.get("reasons", [])) | set([str(x) for x in (reasons or []) if x])
        entry["reasons"] = sorted(rset)

        if extra:
            if "guids_tried" in extra:
                cur = set(entry.get("guids_tried", []))
                add = [str(x) for x in (extra.get("guids_tried") or []) if x]
                cur |= set(add[:8])
                entry["guids_tried"] = sorted(cur)
            for k, v in extra.items():
                if k == "guids_tried":
                    continue
                entry[k] = v

        entry["attempts"] = int(entry.get("attempts", 0)) + 1
        data[key] = entry
        self.save(data)
        return key

    def unfreeze(self, keys: Iterable[str]) -> int:
        data = self.load()
        changed = 0
        for k in list(keys or []):
            if k in data:
                del data[k]
                changed += 1
        if changed:
            self.save(data)
        return changed

    def is_frozen(self, item: Mapping[str, Any]) -> bool:
        key = self.event_key(item)
        return bool(key) and key in self.load()


def unresolved_store(feature: str) -> UnresolvedStore:
    return UnresolvedStore(feature)
