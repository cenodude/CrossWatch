# providers/scrobble/scrobble.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Literal, Optional, Protocol
import re
import xml.etree.ElementTree as ET

import requests

ScrobbleAction = Literal["start", "pause", "stop"]
MediaType = Literal["movie", "episode"]

_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|thetvdb|tvdb)://(\d+)", re.I)


@dataclass(frozen=True)
class ScrobbleEvent:
    action: ScrobbleAction
    media_type: MediaType
    ids: Dict[str, Any]
    title: Optional[str]
    year: Optional[int]
    season: Optional[int]
    number: Optional[int]
    progress: float  # 0..100
    account: Optional[str]
    server_uuid: Optional[str]
    session_key: Optional[str]
    raw: Optional[Dict[str, Any]] = None


class ScrobbleSink(Protocol):
    def send(self, event: ScrobbleEvent) -> Any: ...


class Dispatcher:
    def __init__(self, sinks: Iterable[ScrobbleSink]):
        self.sinks: List[ScrobbleSink] = list(sinks)

    def send(self, event: ScrobbleEvent) -> List[Any]:
        out: List[Any] = []
        for s in self.sinks:
            try:
                out.append(s.send(event))
            except Exception as e:
                out.append({"ok": False, "error": str(e), "sink": s.__class__.__name__})
        return out


# ---------- ID helpers ----------

def _ids_from_candidates(candidates: Iterable[Any]) -> Dict[str, Any]:
    for c in candidates:
        if not c:
            continue
        s = str(c)
        m = _PAT_IMDB.search(s)
        if m:
            return {"imdb": m.group(1)}
        m = _PAT_TMDB.search(s)
        if m:
            return {"tmdb": int(m.group(1))}
        m = _PAT_TVDB.search(s)
        if m:
            return {"tvdb": int(m.group(1))}
    return {}


def _gather_guid_candidates(md: Dict[str, Any]) -> List[str]:
    cand: List[str] = []
    for k in ("guid", "grandparentGuid", "parentGuid"):
        v = md.get(k)
        if v:
            cand.append(v)
    gi = md.get("Guid") or []
    for g in gi:
        if isinstance(g, dict):
            v = g.get("id")
            if v:
                cand.append(v)
        elif isinstance(g, str):
            cand.append(g)
    seen, out = set(), []
    for v in cand:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _ids_from_metadata(md: Dict[str, Any], media_type: str) -> Dict[str, Any]:
    if media_type == "episode":
        pref = [md.get("grandparentGuid"), md.get("parentGuid")]
        ids = _ids_from_candidates(pref)
        if ids:
            return ids
    return _ids_from_candidates(_gather_guid_candidates(md))


def _progress_from(payload_like: Dict[str, Any]) -> float:
    md = payload_like.get("Metadata") or {}
    vo = payload_like.get("viewOffset") or md.get("viewOffset") or 0
    dur = md.get("duration") or 0
    if not dur:
        return 0.0
    p = max(0.0, min(99.9, float(vo) * 100.0 / float(dur)))
    return round(p, 2)


def _map_event_name(e: str) -> Optional[ScrobbleAction]:
    e = (e or "").lower()
    if e in ("media.play", "media.resume"):
        return "start"
    if e == "media.pause":
        return "pause"
    if e in ("media.stop", "media.scrobble"):
        return "stop"
    return None


# ---------- Plex metadata enrichment ----------

def _load_config() -> Dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        import json
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)


def _plex_get_xml(path: str, cfg: Dict[str, Any]) -> Optional[ET.Element]:
    plex = cfg.get("plex") or {}
    base = (plex.get("server_url") or plex.get("base_url") or "http://127.0.0.1:32400").rstrip("/")
    tok = (plex.get("account_token") or "").strip()
    url = f"{base}{path}"
    if "X-Plex-Token=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}X-Plex-Token={tok}"
    try:
        r = requests.get(url, timeout=8)
        if r.status_code != 200 or not (r.text or "").strip():
            return None
        return ET.fromstring(r.text)
    except Exception:
        return None


def _merge_guid_lists(a: List[Dict[str, Any]] | None, b: List[Dict[str, Any]] | None) -> List[Dict[str, Any]]:
    out = []
    seen = set()
    for lst in (a or []), (b or []):
        for it in lst:
            try:
                v = it.get("id")
            except Exception:
                v = None
            if v and v not in seen:
                seen.add(v)
                out.append({"id": v})
    return out


def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _enrich_md_from_plex(md: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    rk = md.get("ratingKey") or md.get("parentRatingKey") or md.get("grandparentRatingKey")
    if not rk:
        return md
    root = _plex_get_xml(f"/library/metadata/{rk}", cfg)
    if root is None:
        return md

    node = None
    for child in root:
        node = child
        break
    if node is None:
        return md

    at = node.attrib
    md2: Dict[str, Any] = dict(md)

    if not md2.get("type") and at.get("type") in ("movie", "episode"):
        md2["type"] = at.get("type")

    if md2.get("type") == "movie":
        md2.setdefault("title", at.get("title"))
    elif md2.get("type") == "episode":
        md2.setdefault("grandparentTitle", at.get("grandparentTitle") or at.get("title"))
        md2.setdefault("parentIndex", _to_int(at.get("parentIndex")))
        md2.setdefault("index", _to_int(at.get("index")))

    md2.setdefault("year", _to_int(at.get("year")))
    md2.setdefault("duration", _to_int(at.get("duration")))
    md2.setdefault("guid", at.get("guid"))

    guids: List[Dict[str, Any]] = []
    for g in node.findall(".//Guid"):
        gid = g.attrib.get("id")
        if gid:
            guids.append({"id": gid})
    md2["Guid"] = _merge_guid_lists(md.get("Guid"), guids)

    return md2


# ---------- Builders ----------

def from_plex_webhook(payload: Dict[str, Any]) -> Optional[ScrobbleEvent]:
    if not isinstance(payload, dict):
        return None
    event_name = payload.get("event")
    md = payload.get("Metadata") or {}
    action = _map_event_name(event_name or "")
    if not action or not md:
        return None

    media_type = (md.get("type") or "").lower()
    if media_type not in ("movie", "episode"):
        cfg = _load_config()
        md = _enrich_md_from_plex(md, cfg)
        media_type = (md.get("type") or "").lower()
        if media_type not in ("movie", "episode"):
            return None

    ids = _ids_from_metadata(md, media_type)
    title = md.get("title") if media_type == "movie" else md.get("grandparentTitle")
    year = md.get("year")
    season = md.get("parentIndex") if media_type == "episode" else None
    number = md.get("index") if media_type == "episode" else None
    progress = _progress_from(payload)
    account = ((payload.get("Account") or {}).get("title") or None)
    server_uuid = ((payload.get("Server") or {}).get("uuid") or None)
    session_key = str(payload.get("sessionKey") or md.get("sessionKey") or md.get("ratingKey") or "") or None
    return ScrobbleEvent(
        action=action,
        media_type=media_type, ids=ids,
        title=title, year=year, season=season, number=number,
        progress=progress, account=account, server_uuid=server_uuid,
        session_key=session_key, raw=payload,
    )


def from_plex_pssn(notification_frame: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> Optional[ScrobbleEvent]:
    """Build ScrobbleEvent from PlaySessionStateNotification (handles NotificationContainer)."""
    if not isinstance(notification_frame, dict):
        return None

    container = notification_frame.get("NotificationContainer") or notification_frame
    lst = container.get("PlaySessionStateNotification") or container.get("playSessionStateNotification")
    if not isinstance(lst, list) or not lst:
        return None

    n = lst[0]
    state = (n.get("state") or "").lower()
    if state == "playing":
        action: ScrobbleAction = "start"
    elif state == "paused":
        action = "pause"
    elif state == "stopped":
        action = "stop"
    else:
        return None

    md = {
        "type": n.get("type"),
        "title": n.get("title"),
        "year": n.get("year"),
        "duration": n.get("duration"),
        "guid": n.get("guid"),
        "grandparentGuid": n.get("grandparentGuid"),
        "parentGuid": n.get("parentGuid"),
        "grandparentTitle": n.get("grandparentTitle"),
        "parentIndex": n.get("parentIndex"),
        "index": n.get("index"),
        "ratingKey": n.get("ratingKey"),
        "sessionKey": n.get("sessionKey"),
        "Guid": [{"id": n.get("guid")}] if n.get("guid") else [],
        "viewOffset": n.get("viewOffset"),
    }

    media_type = (md.get("type") or "").lower()
    if media_type not in ("movie", "episode") or md.get("parentIndex") is None or md.get("index") is None:
        cfg = _load_config()
        md = _enrich_md_from_plex(md, cfg)
        media_type = (md.get("type") or "").lower()
        if media_type not in ("movie", "episode"):
            return None

    payload_like = {
        "event": {"start": "media.play", "pause": "media.pause", "stop": "media.stop"}[action],
        "Metadata": md,
        "viewOffset": n.get("viewOffset", 0),
    }
    if defaults:
        payload_like["Account"] = {"title": (defaults.get("username") or "")}
        payload_like["Server"] = {"uuid": (defaults.get("server_uuid") or "")}

    return from_plex_webhook(payload_like)


__all__ = [
    "ScrobbleEvent",
    "ScrobbleSink",
    "Dispatcher",
    "from_plex_webhook",
    "from_plex_pssn",
]
