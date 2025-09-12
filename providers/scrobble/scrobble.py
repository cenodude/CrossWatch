from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Literal, Optional, Protocol, Callable, Tuple
import re
import xml.etree.ElementTree as ET
import json
import requests

# ---- ids / types ----
ScrobbleAction = Literal["start", "pause", "stop"]
MediaType = Literal["movie", "episode"]

_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|thetvdb|tvdb)://(\d+)", re.I)

# ---- config helpers ----
def _load_config() -> Dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)

def _plex_base(cfg: Dict[str, Any]) -> str:
    plex = cfg.get("plex") or {}
    base = (plex.get("server_url") or plex.get("base_url") or "http://127.0.0.1:32400").strip().rstrip("/")
    if "://" not in base:
        base = f"http://{base}"
    return base

def _plex_token(cfg: Dict[str, Any]) -> Optional[str]:
    plex = cfg.get("plex") or {}
    return plex.get("account_token") or plex.get("token")

def _plex_get_xml(path: str, cfg: Dict[str, Any]) -> Optional[ET.Element]:
    url = f"{_plex_base(cfg)}{path}"
    headers = {"X-Plex-Token": _plex_token(cfg)} if _plex_token(cfg) else {}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        return ET.fromstring(r.text)
    except Exception:
        return None

# ---- model ----
@dataclass(frozen=True)
class ScrobbleEvent:
    action: ScrobbleAction
    media_type: MediaType
    ids: Dict[str, Any]
    title: Optional[str]
    year: Optional[int]
    season: Optional[int]
    number: Optional[int]
    progress: int
    account: Optional[str]
    server_uuid: Optional[str]
    session_key: Optional[str]
    raw: Dict[str, Any]

class ScrobbleSink(Protocol):
    def send(self, ev: ScrobbleEvent) -> None: ...

# ---- dispatcher with simple filters ----
class Dispatcher:
    def __init__(self, sinks: Iterable[ScrobbleSink], cfg_provider: Callable[[], Dict[str, Any]] = _load_config):
        self._sinks = list(sinks)
        self._cfg_provider = cfg_provider

    def _passes_filters(self, ev: ScrobbleEvent) -> bool:
        cfg = self._cfg_provider() or {}
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl = filt.get("username_whitelist")
        want_server = (filt.get("server_uuid") or (cfg.get("plex") or {}).get("server_uuid"))
        if want_server and ev.server_uuid and str(ev.server_uuid) != str(want_server):
            return False
        if not wl:
            return True
        # title match (space/case-insensitive)
        import re as _re
        def norm(s: str) -> str:
            return _re.sub(r"[^a-z0-9]+", "", (s or "").lower())
        title_ok = any(norm(ev.account or "") == norm(x) for x in (wl if isinstance(wl, list) else [wl]) if not str(x).lower().startswith(("id:", "uuid:")))
        if title_ok:
            return True
        # id:/uuid: support (if present in raw)
        raw = ev.raw or {}
        def find_psn(o):
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(k, str) and k.lower() == "playsessionstatenotification":
                        return v if isinstance(v, list) else [v]
                for v in o.values():
                    r = find_psn(v)
                    if r: return r
            elif isinstance(o, list):
                for v in o:
                    r = find_psn(v)
                    if r: return r
            return None
        n = (find_psn(raw) or [None])[0] or {}
        acc_id = str(n.get("accountID") or "")
        acc_uuid = str(n.get("accountUUID") or "").lower()
        for e in (wl if isinstance(wl, list) else [wl]):
            es = str(e).strip()
            if es.lower().startswith("id:") and acc_id and es.split(":", 1)[1].strip() == acc_id:
                return True
            if es.lower().startswith("uuid:") and acc_uuid and es.split(":", 1)[1].strip().lower() == acc_uuid:
                return True
        return False

    def dispatch(self, ev: ScrobbleEvent) -> None:
        if not self._passes_filters(ev):
            return
        for s in self._sinks:
            try:
                s.send(ev)
            except Exception:
                continue

# ---- id extraction helpers ----
def _ids_from_metadata(md: Dict[str, Any], media_type: str) -> Dict[str, Any]:
    ids: Dict[str, Any] = {}
    guid = (md.get("guid") or "")
    if guid:
        m = _PAT_IMDB.search(guid)
        if m: ids["imdb"] = m.group(1)
        m = _PAT_TMDB.search(guid)
        if m: ids["tmdb"] = int(m.group(1))
        m = _PAT_TVDB.search(guid)
        if m: ids["tvdb"] = int(m.group(1))
    if md.get("ratingKey") and "plex" not in ids:
        ids["plex"] = str(md.get("ratingKey"))
    if media_type == "episode":
        for k in ("grandparentGuid", "parentGuid"):
            g = md.get(k)
            if not g: continue
            m = _PAT_TVDB.search(g)
            if m and "tvdb_show" not in ids: ids["tvdb_show"] = int(m.group(1))
            m = _PAT_TMDB.search(g)
            if m and "tmdb_show" not in ids: ids["tmdb_show"] = int(m.group(1))
            m = _PAT_IMDB.search(g)
            if m and "imdb_show" not in ids: ids["imdb_show"] = m.group(1)
    return ids

def _progress_from(obj: Dict[str, Any]) -> int:
    md = obj.get("Metadata") or {}
    duration = md.get("duration")
    vo = obj.get("viewOffset", md.get("viewOffset"))
    try:
        duration = int(duration) if duration is not None else None
        vo = int(vo) if vo is not None else None
    except Exception:
        duration = None; vo = None
    if not duration or duration <= 0 or vo is None:
        return 0
    pct = int(round(100 * max(0, min(vo, duration)) / float(duration)))
    return max(0, min(100, pct))

def _account_from_session(session_key: Optional[str], cfg: Dict[str, Any]) -> Optional[str]:
    if not session_key:
        return None
    el = _plex_get_xml("/status/sessions", cfg)
    if el is None:
        return None
    for v in el.findall(".//Video"):
        if v.get("sessionKey") == str(session_key):
            u = v.find("User")
            return u.get("title") if u is not None else None
    return None

def _enrich_md_from_plex(md: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    if md.get("ratingKey") and (md.get("type") and md.get("duration") and (md.get("title") or md.get("grandparentTitle"))):
        return md
    rk = md.get("ratingKey")
    if not rk:
        return md
    el = _plex_get_xml(f"/library/metadata/{rk}", cfg)
    if el is None:
        return md
    node = None
    for tag in ("Video", "Movie", "Episode"):
        node = el.find(f".//{tag}")
        if node is not None:
            break
    if node is None:
        return md
    out = dict(md)
    out.setdefault("type", (node.get("type") or "").lower())
    out.setdefault("title", node.get("title"))
    out.setdefault("grandparentTitle", node.get("grandparentTitle"))
    if node.get("year"): out.setdefault("year", int(node.get("year")))
    if node.get("duration"): out.setdefault("duration", int(node.get("duration")))
    out.setdefault("parentIndex", node.get("parentIndex") and int(node.get("parentIndex")))
    out.setdefault("index", node.get("index") and int(node.get("index")))
    out.setdefault("guid", node.get("guid"))
    out.setdefault("grandparentGuid", node.get("grandparentGuid"))
    out.setdefault("parentGuid", node.get("parentGuid"))
    out.setdefault("sessionKey", node.get("sessionKey"))
    return out

# ---- PSN / webhook / flat parsers ----
def _find_psn_list(container: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(container, dict):
        return None
    for k, v in container.items():
        if isinstance(k, str) and k.lower() == "playsessionstatenotification":
            if isinstance(v, list): return v
            return [v] if isinstance(v, dict) else None
    for k, v in container.items():
        if isinstance(k, str) and k.lower() == "notificationcontainer" and isinstance(v, dict):
            sub = _find_psn_list(v)
            if sub: return sub
    return None

def from_plex_webhook(webhook_json: Dict[str, Any]) -> Optional[ScrobbleEvent]:
    if not isinstance(webhook_json, dict):
        return None
    mapping = {
        "media.play": "start",
        "media.resume": "start",
        "media.pause": "pause",
        "media.stop": "stop",
        "media.scrobble": "stop",
    }
    action = mapping.get((webhook_json.get("event") or "").lower())
    if not action:
        return None
    md = webhook_json.get("Metadata") or {}
    cfg = _load_config()
    md = _enrich_md_from_plex(md, cfg)
    media_type = (md.get("type") or "").lower()
    if media_type not in ("movie", "episode"):
        return None
    progress = _progress_from(webhook_json)
    account = ((webhook_json.get("Account") or {}).get("title"))
    server_uuid = ((webhook_json.get("Server") or {}).get("uuid"))
    session_key = str(md.get("sessionKey") or md.get("ratingKey") or "") or None
    return ScrobbleEvent(
        action=action, media_type=media_type, ids=_ids_from_metadata(md, media_type),
        title=md.get("title") if media_type == "movie" else md.get("grandparentTitle"),
        year=md.get("year"),
        season=md.get("parentIndex") if media_type == "episode" else None,
        number=md.get("index") if media_type == "episode" else None,
        progress=progress, account=account, server_uuid=server_uuid,
        session_key=session_key, raw=webhook_json,
    )

def from_plex_pssn(notification_frame: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> Optional[ScrobbleEvent]:
    candidates = [notification_frame]
    nc = notification_frame.get("NotificationContainer")
    if isinstance(nc, dict):
        candidates.append(nc)
    lst = None
    for cand in candidates:
        lst = _find_psn_list(cand)
        if lst: break
    if not lst:
        return None
    n = lst[0]
    state = (n.get("state") or "").lower()
    if state == "playing":
        action: ScrobbleAction = "start"
    elif state == "paused":
        action = "pause"
    elif state in ("stopped", "bufferingstopped"):
        action = "stop"
    else:
        return None
    md = {
        "type": (n.get("type") or "").lower(),
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
        "viewOffset": n.get("viewOffset"),
    }
    cfg = _load_config()
    md = _enrich_md_from_plex(md, cfg)
    media_type = (md.get("type") or "").lower()
    if media_type not in ("movie", "episode"):
        return None
    ev = ScrobbleEvent(
        action=action, media_type=media_type, ids=_ids_from_metadata(md, media_type),
        title=md.get("title") if media_type == "movie" else md.get("grandparentTitle"),
        year=md.get("year"), season=md.get("parentIndex") if media_type == "episode" else None,
        number=md.get("index") if media_type == "episode" else None,
        progress=_progress_from({"Metadata": md, "viewOffset": md.get("viewOffset")}),
        account=str(n.get("account") or n.get("accountID") or (defaults or {}).get("username")) if (n.get("account") or n.get("accountID") or (defaults or {}).get("username")) else None,
        server_uuid=str(n.get("machineIdentifier") or (defaults or {}).get("server_uuid")) if (n.get("machineIdentifier") or (defaults or {}).get("server_uuid")) else None,
        session_key=str(md.get("sessionKey") or md.get("ratingKey") or "") or None,
        raw=notification_frame,
    )
    if not ev.account:
        acc = _account_from_session(ev.session_key, cfg)
        if acc:
            ev = ScrobbleEvent(**{**ev.__dict__, "account": acc})
    return ev

def _find_flat_play_node(container: Any) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    stack: List[Tuple[Any, Any]] = [(container, None)]
    while stack:
        cur, parent = stack.pop()
        if isinstance(cur, dict):
            t = None
            for k, v in cur.items():
                if isinstance(k, str) and k.lower() == "type":
                    t = (v or ""); break
            if isinstance(t, str) and t.lower() in ("playing", "paused", "stopped"):
                return cur, (parent if isinstance(parent, dict) else cur)
            for v in cur.values():
                stack.append((v, cur))
        elif isinstance(cur, list):
            for v in cur:
                stack.append((v, parent))
    return None

def from_plex_flat_playing(payload: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> Optional[ScrobbleEvent]:
    hit = _find_flat_play_node(payload) if isinstance(payload, dict) else None
    if not hit:
        return None
    node, parent = hit
    t = ""
    for k, v in node.items():
        if isinstance(k, str) and k.lower() == "type":
            t = (v or ""); break
    t = t.lower()
    if t not in ("playing", "paused", "stopped"):
        return None
    action: ScrobbleAction = {"playing": "start", "paused": "pause", "stopped": "stop"}[t]
    md = node.get("Metadata") or node.get("metadata") or parent.get("Metadata") or parent.get("metadata") or {}
    if not md:
        md = {
            "type": (node.get("itemType") or node.get("type") or parent.get("itemType") or parent.get("type") or "").lower(),
            "title": node.get("title") or parent.get("title"),
            "year": node.get("year") or parent.get("year"),
            "duration": node.get("duration") or parent.get("duration"),
            "guid": node.get("guid") or parent.get("guid"),
            "grandparentGuid": node.get("grandparentGuid") or parent.get("grandparentGuid"),
            "parentGuid": node.get("parentGuid") or parent.get("parentGuid"),
            "grandparentTitle": node.get("grandparentTitle") or parent.get("grandparentTitle"),
            "parentIndex": node.get("parentIndex") or parent.get("parentIndex"),
            "index": node.get("index") or parent.get("index"),
            "ratingKey": node.get("ratingKey") or parent.get("ratingKey"),
            "sessionKey": node.get("sessionKey") or parent.get("sessionKey"),
            "viewOffset": node.get("viewOffset") or parent.get("viewOffset"),
        }
    cfg = _load_config()
    md = _enrich_md_from_plex(md, cfg)
    media_type = (md.get("type") or "").lower()
    if media_type not in ("movie", "episode"):
        return None
    vo = node.get("viewOffset"); 
    if vo is None: vo = parent.get("viewOffset")
    progress = _progress_from({"Metadata": md, "viewOffset": vo})
    account = ((node.get("Account") or {}).get("title")) or ((parent.get("Account") or {}).get("title")) or (defaults or {}).get("username")
    server_uuid = ((node.get("Server") or {}).get("uuid")) or ((parent.get("Server") or {}).get("uuid")) or (defaults or {}).get("server_uuid")
    session_key = str(node.get("sessionKey") or md.get("sessionKey") or md.get("ratingKey") or "") or None
    ev = ScrobbleEvent(
        action=action, media_type=media_type, ids=_ids_from_metadata(md, media_type),
        title=md.get("title") if media_type == "movie" else md.get("grandparentTitle"),
        year=md.get("year"), season=md.get("parentIndex") if media_type == "episode" else None,
        number=md.get("index") if media_type == "episode" else None,
        progress=progress, account=str(account) if account else None,
        server_uuid=str(server_uuid) if server_uuid else None,
        session_key=session_key, raw=payload,
    )
    if not ev.account:
        acc = _account_from_session(ev.session_key, cfg)
        if acc:
            ev = ScrobbleEvent(**{**ev.__dict__, "account": acc})
    return ev

__all__ = [
    "ScrobbleEvent","ScrobbleSink","Dispatcher",
    "from_plex_webhook","from_plex_pssn","from_plex_flat_playing",
]
