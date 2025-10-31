# _scrobbleAPI.py  — Scrobbler
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from cw_platform.config_base import load_config
from urllib.parse import parse_qs
import urllib.parse, json, threading

try:
    from plexapi.myplex import MyPlexAccount
    HAVE_PLEXAPI = True
except Exception:
    MyPlexAccount = None  # type: ignore
    HAVE_PLEXAPI = False

router = APIRouter(tags=["Scrobbler"])

def _env_logs(request: Request | None = None):
    if request is not None:
        try:
            lb = getattr(request.app.state, "LOG_BUFFERS", None)
            ml = getattr(request.app.state, "MAX_LOG_LINES", None)
            if isinstance(lb, dict) and isinstance(ml, int):
                return lb, ml
        except Exception:
            pass
    try:
        import crosswatch as CW
        return getattr(CW, "LOG_BUFFERS", {}), getattr(CW, "MAX_LOG_LINES", 2000)
    except Exception:
        return {}, 2000

def _debug_on() -> bool:
    try:
        cfg = load_config()
        rt = (cfg.get("runtime") or {})
        return bool(rt.get("debug") or rt.get("debug_mods"))
    except Exception:
        return False

def _watch_kind(w) -> str | None:
    try:
        name = getattr(getattr(w, "__class__", None), "__name__", "") or ""
        n = name.lower()
        if "emby" in n: return "emby"
        if "plex" in n: return "plex"
    except Exception:
        pass
    return None

def _plex_token(cfg: Dict[str, Any]) -> str:
    return ((cfg.get("plex") or {}).get("account_token") or "").strip()

def _plex_client_id(cfg: Dict[str, Any]) -> str:
    return (cfg.get("plex") or {}).get("client_id") or "crosswatch"

def _account(cfg: Dict[str, Any]):
    tok = _plex_token(cfg)
    if not HAVE_PLEXAPI or not tok:
        return None
    try:
        return MyPlexAccount(token=tok)
    except Exception:
        return None

def _resolve_plex_server_uuid(cfg: Dict[str, Any]) -> str:
    plex = cfg.get("plex") or {}
    if plex.get("server_uuid"):
        return str(plex["server_uuid"]).strip()

    acc = _account(cfg)
    if not acc:
        return ""

    host_hint = ""
    base = (plex.get("server_url") or "").strip()
    if base:
        try:
            host_hint = urllib.parse.urlparse(base).hostname or ""
        except Exception:
            host_hint = ""

    try:
        servers = [r for r in acc.resources()
                   if "server" in (r.provides or "") and (r.product or "") == "Plex Media Server"]
        owned = [r for r in servers if getattr(r, "owned", False)]

        def matches_host(res) -> bool:
            if not host_hint:
                return False
            for c in (res.connections or []):
                if host_hint in (c.uri or "") or host_hint == (c.address or ""):
                    return True
            return False

        for res in owned:
            if matches_host(res):
                return res.clientIdentifier or ""
        if owned:
            return owned[0].clientIdentifier or ""
        return servers[0].clientIdentifier if servers else ""
    except Exception:
        return ""

def _fetch_owner_and_managed(cfg: Dict[str, Any]) -> Tuple[Optional[dict], List[dict]]:
    acc = _account(cfg)
    if not acc:
        return None, []
    owner = {
        "id": str(getattr(acc, "id", "") or ""),
        "username": (getattr(acc, "username", "") or getattr(acc, "title", "") or getattr(acc, "email", "") or "").strip(),
        "title": (getattr(acc, "title", "") or getattr(acc, "username", "") or "").strip(),
        "email": (getattr(acc, "email", "") or "").strip(),
        "type": "owner",
    }
    managed: List[dict] = []
    try:
        home = getattr(acc, "home", None)
        if home:
            for u in home.users():
                uid = str(getattr(u, "id", "") or "").strip()
                if not uid:
                    continue
                uname = (getattr(u, "username", "") or getattr(u, "title", "") or "").strip()
                managed.append({
                    "id": uid,
                    "username": uname,
                    "title": (getattr(u, "title", "") or uname).strip(),
                    "email": (getattr(u, "email", "") or "").strip(),
                    "type": "managed",
                })
    except Exception:
        pass
    return owner, managed

def _list_plex_users(cfg: Dict[str, Any]) -> List[dict]:
    users: List[dict] = []
    acc = _account(cfg)
    if acc:
        try:
            for u in acc.users():
                users.append({
                    "id": str(getattr(u, "id", "") or ""),
                    "username": (u.username or u.title or u.email or "").strip(),
                    "title": (u.title or u.username or "").strip(),
                    "email": (getattr(u, "email", "") or "").strip(),
                    "type": "friend",
                })
        except Exception:
            pass
    owner, managed = _fetch_owner_and_managed(cfg)
    if owner:
        users.append(owner)
    users.extend(managed)

    rank = {"owner": 3, "managed": 2, "friend": 1}
    out: Dict[str, dict] = {}
    for u in users:
        uid = str(u.get("id") or "")
        if not uid:
            continue
        cur = out.get(uid)
        if not cur or rank.get(u.get("type", "friend"), 0) >= rank.get(cur.get("type", "friend"), 0):
            out[uid] = u
    return list(out.values())

def _filter_users_with_server_access(cfg: Dict[str, Any], users: List[dict], server_uuid: str) -> List[dict]:
    if not users:
        return users
    allowed = {"owner", "managed", "friend"}
    out = []
    for u in users:
        if u.get("type") in allowed:
            v = dict(u); v["has_access"] = True; out.append(v)
    return out

def _list_pms_servers(cfg: Dict[str, Any]) -> List[dict]:
    acc = _account(cfg)
    if not acc:
        return []
    plex = cfg.get("plex") or {}
    host_hint = ""
    base = (plex.get("server_url") or "").strip()
    if base:
        try:
            host_hint = urllib.parse.urlparse(base).hostname or ""
        except Exception:
            host_hint = ""
    servers = []
    try:
        for r in acc.resources():
            if "server" not in (r.provides or "") or (r.product or "") != "Plex Media Server":
                continue
            conns = []
            for c in (r.connections or []):
                conns.append({
                    "uri": c.uri or "",
                    "address": c.address or "",
                    "port": c.port or "",
                    "protocol": c.protocol or "",
                    "local": bool(getattr(c, "local", False)),
                    "relay": bool(getattr(c, "relay", False)),
                })
            def pick_best() -> str:
                if host_hint:
                    for c in (r.connections or []):
                        if host_hint in (c.uri or "") or host_hint == (c.address or ""):
                            return c.uri or ""
                for c in (r.connections or []):
                    if not c.relay and (c.protocol or "").lower() == "https" and not getattr(c, "local", False):
                        return c.uri or ""
                for c in (r.connections or []):
                    if not c.relay and (c.protocol or "").lower() == "https":
                        return c.uri or ""
                for c in (r.connections or []):
                    if not c.relay:
                        return c.uri or ""
                return (r.connections[0].uri if r.connections else "") or ""
            servers.append({
                "id": r.clientIdentifier or "",
                "name": r.name or r.product or "Plex Media Server",
                "owned": bool(getattr(r, "owned", False)),
                "platform": r.platform or "",
                "product": r.product or "",
                "device": r.device or "",
                "version": r.productVersion or "",
                "connections": conns,
                "best_url": pick_best(),
            })
    except Exception:
        pass
    return servers

@router.get("/api/plex/server_uuid")
def api_plex_server_uuid() -> JSONResponse:
    cfg = load_config()
    uid = _resolve_plex_server_uuid(cfg)
    return JSONResponse({"server_uuid": uid or None}, headers={"Cache-Control": "no-store"})

@router.get("/api/plex/users")
def api_plex_users(
    only_with_server_access: bool = Query(False),
    only_home_or_owner: bool = Query(False)
) -> JSONResponse:
    cfg = load_config()
    users = _list_plex_users(cfg)
    if only_with_server_access:
        server_uuid = _resolve_plex_server_uuid(cfg)
        users = _filter_users_with_server_access(cfg, users, server_uuid)
    if only_home_or_owner:
        users = [u for u in users if u.get("type") in ("owner", "managed")]
    return JSONResponse({"users": users, "count": len(users)}, headers={"Cache-Control": "no-store"})

@router.get("/api/plex/pms")
def api_plex_pms() -> JSONResponse:
    cfg = load_config()
    servers = _list_pms_servers(cfg)
    return JSONResponse({"servers": servers, "count": len(servers)}, headers={"Cache-Control": "no-store"})

@router.get("/debug/watch/logs")
def debug_watch_logs(
    request: Request,
    tail: int = Query(50, ge=1, le=3000),
    tag: str | None = Query(None, description="Single tag"),
    tags: str = Query("*", description="CSV or * for all")
) -> JSONResponse:
    LOG_BUFFERS, MAX = _env_logs(request)
    sel = [t.strip().upper() for t in ( [tag] if tag else tags.split(",") ) if t and t.strip()]
    if sel == ["*"]:
        sel = sorted(LOG_BUFFERS.keys())
    tail = max(1, min(int(tail or 50), int(MAX)))
    merged: list[str] = []
    for t in sel:
        merged.extend(LOG_BUFFERS.get(t, []))
    return JSONResponse({"tags": sel, "tail": tail, "lines": merged[-tail:]}, headers={"Cache-Control": "no-store"})

@router.get("/debug/watch/status")
def debug_watch_status(request: Request):
    w = getattr(request.app.state, "watch", None)
    return {
        "has_watch": bool(w),
        "alive": bool(getattr(w, "is_alive", lambda: False)()),
        "stop_set": bool(getattr(w, "is_stopping", lambda: False)()),
        "provider": _watch_kind(w),
    }

def _ensure_watch_started(request: Request, provider: str | None = None):
    w = getattr(request.app.state, "watch", None)
    if w and getattr(w, "is_alive", lambda: False)():
        if provider:
            want = provider.lower().strip()
            cur = _watch_kind(w)
            if want != cur:
                try:
                    w.stop()
                except Exception:
                    pass
                request.app.state.watch = None
            else:
                return w
        else:
            return w

    cfg = load_config()
    prov = (provider
            or (((cfg.get("scrobble") or {}).get("watch") or {}).get("provider"))
            or "plex").lower().strip()

    watch_cfg = ((cfg.get("scrobble") or {}).get("watch") or {})
    sink_cfg = (watch_cfg.get("sink") or "trakt")
    names = [s.strip().lower() for s in str(sink_cfg).split(",") if s and s.strip()]
    added = set()
    sinks = []
    for name in names or ["trakt"]:
        if name == "trakt" and "trakt" not in added:
            try:
                from providers.scrobble.trakt.sink import TraktSink
                sinks.append(TraktSink())
                added.add("trakt")
            except Exception:
                pass
        elif name == "simkl" and "simkl" not in added:
            try:
                from providers.scrobble.simkl.sink import SimklSink
                sinks.append(SimklSink())
                added.add("simkl")
            except Exception:
                pass
    if not sinks:
        try:
            from providers.scrobble.trakt.sink import TraktSink
            sinks = [TraktSink()]
        except Exception:
            sinks = []

    make_watch = None
    if prov == "emby":
        try:
            from providers.scrobble.emby.watch import make_default_watch as _mk
            make_watch = _mk
        except Exception:
            make_watch = None

    if make_watch is None:
        from providers.scrobble.plex.watch import make_default_watch as _mk
        make_watch = _mk
        prov = "plex"

    w = make_watch(sinks=sinks)
    if hasattr(w, "start_async"):
        w.start_async()
    else:
        threading.Thread(target=w.start, daemon=True).start()

    request.app.state.watch = w
    return w

@router.post("/debug/watch/start")
def debug_watch_start(request: Request, provider: str | None = Query(None)):
    w = _ensure_watch_started(request, provider)
    return {
        "ok": True,
        "alive": bool(getattr(w, "is_alive", lambda: False)()),
        "provider": _watch_kind(w),
    }

@router.post("/debug/watch/stop")
def debug_watch_stop(request: Request):
    w = getattr(request.app.state, "watch", None)
    if w:
        try:
            w.stop()
        except Exception:
            pass
    request.app.state.watch = None
    return {"ok": True, "alive": False}

@router.post("/webhook/jellyfintrakt")
async def webhook_jellyfintrakt(request: Request):
    from crosswatch import _UIHostLogger
    try:
        from providers.webhooks.jellyfintrakt import process_webhook as jf_process_webhook
    except Exception:
        try:
            from crosswatch import process_webhook_jellyfin as jf_process_webhook
        except Exception:
            from jellyfintrakt import process_webhook as jf_process_webhook

    logger = _UIHostLogger("TRAKT", "SCROBBLE")

    def log(msg, level="INFO"):
        try:
            if level.upper() == "DEBUG" and not _debug_on():
                return
            logger(msg, level=level, module="SCROBBLE")
        except Exception:
            pass
        try:
            if level.upper() == "DEBUG" and not _debug_on():
                return
            print(f"[SCROBBLE] {level} {msg}")
        except Exception:
            pass

    raw = await request.body()
    ct = (request.headers.get("content-type") or "").lower()
    log(f"jf-webhook: received | content-type='{ct}' bytes={len(raw)}", "DEBUG")

    payload = {}
    try:
        if "application/x-www-form-urlencoded" in ct:
            d = parse_qs(raw.decode("utf-8", errors="replace"))
            blob = d.get("payload") or d.get("data") or d.get("json")
            payload = json.loads((blob[0] if isinstance(blob, list) else blob) or "{}") if blob else {}
            log("jf-webhook: parsed urlencoded payload", "DEBUG")
        else:
            payload = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
            log("jf-webhook: parsed json payload", "DEBUG")
    except Exception as e:
        snippet = (raw[:200].decode("utf-8", errors="replace") if raw else "<no body>")
        log(f"jf-webhook: failed to parse payload: {e} | body[:200]={snippet}", "ERROR")
        return JSONResponse({"ok": True}, status_code=200)

    md = (payload.get("Item") or payload.get("item") or payload.get("Metadata") or {}) or {}
    event = (payload.get("NotificationType") or payload.get("Event") or "").strip() or "?"
    user = (
        ((payload.get("User") or {}).get("Name"))
        or payload.get("UserName")
        or ((payload.get("Server") or {}).get("UserName"))
        or ""
    ).strip()

    mtype = (md.get("Type") or md.get("type") or "").strip().lower()
    if mtype == "episode":
        series = (md.get("SeriesName") or md.get("SeriesTitle") or "").strip()
        ep_name = (md.get("Name") or md.get("EpisodeTitle") or "").strip()
        season = md.get("ParentIndexNumber") or md.get("SeasonIndexNumber")
        number = md.get("IndexNumber")
        if isinstance(season, int) and isinstance(number, int):
            title = f"{series} S{season:02}E{number:02}" + (f" — {ep_name}" if ep_name else "")
        else:
            title = ep_name or series or "?"
    elif mtype == "movie":
        name = (md.get("Name") or md.get("title") or "").strip()
        year = md.get("ProductionYear") or md.get("year")
        title = f"{name} ({year})" if (name and year) else (name or "?")
    else:
        title = (md.get("Name") or md.get("title") or md.get("SeriesName") or "?")

    log(f"jf-webhook: payload summary event='{event}' user='{user}' media='{title}'", "DEBUG")

    try:
        res = jf_process_webhook(payload=payload, headers=dict(request.headers), raw=raw, logger=log)
    except Exception as e:
        log(f"jf-webhook: process_webhook raised: {e}", "ERROR")
        return JSONResponse({"ok": True, "error": "internal"}, status_code=200)

    if res.get("error"):
        log(f"jf-webhook: result error={res['error']}", "WARN")
    elif res.get("ignored"):
        log("jf-webhook: ignored by filters/rules", "DEBUG")
    elif res.get("debounced"):
        log("jf-webhook: debounced pause", "DEBUG")
    elif res.get("suppressed"):
        log("jf-webhook: suppressed late start", "DEBUG")
    elif res.get("dedup"):
        log("jf-webhook: duplicate event suppressed", "DEBUG")

    log(f"jf-webhook: done action={res.get('action')} status={res.get('status')}", "DEBUG")
    return JSONResponse({"ok": True, **{k: v for k, v in res.items() if k != 'error'}}, status_code=200)

@router.post("/webhook/embytrakt")
async def webhook_embytrakt(request: Request):
    return await webhook_jellyfintrakt(request)

@router.post("/webhook/plextrakt")
async def webhook_trakt(request: Request):
    from crosswatch import _UIHostLogger
    try:
        from providers.scrobble.trakt.webhook import process_webhook
    except Exception:
        from crosswatch import process_webhook

    logger = _UIHostLogger("TRAKT", "SCROBBLE")

    def log(msg, level="INFO"):
        try:
            if level.upper() == "DEBUG" and not _debug_on():
                return
            logger(msg, level=level, module="SCROBBLE")
        except Exception:
            pass
        try:
            if level.upper() == "DEBUG" and not _debug_on():
                return
            print(f"[SCROBBLE] {level} {msg}")
        except Exception:
            pass

    raw = await request.body()
    ct = (request.headers.get("content-type") or "").lower()
    log(f"webhook: received | content-type='{ct}' bytes={len(raw)}", "DEBUG")

    payload = None
    try:
        if "multipart/form-data" in ct:
            form = await request.form()
            part = form.get("payload")
            if part is None:
                raise ValueError("multipart: no 'payload' part")
            if isinstance(part, (bytes, bytearray)):
                payload = json.loads(part.decode("utf-8", errors="replace"))
            elif hasattr(part, "read"):
                data = await part.read()
                payload = json.loads(data.decode("utf-8", errors="replace"))
            else:
                payload = json.loads(str(part))
            log("webhook: parsed multipart payload", "DEBUG")
        elif "application/x-www-form-urlencoded" in ct:
            d = parse_qs(raw.decode("utf-8", errors="replace"))
            if not d.get("payload"):
                raise ValueError("urlencoded: no 'payload' key")
            payload = json.loads(d["payload"][0])
            log("webhook: parsed urlencoded payload", "DEBUG")
        else:
            payload = json.loads(raw.decode("utf-8", errors="replace")) if raw else {}
            log("webhook: parsed json payload", "DEBUG")
    except Exception as e:
        snippet = (raw[:200].decode("utf-8", errors="replace") if raw else "<no body>")
        log(f"webhook: failed to parse payload: {e} | body[:200]={snippet}", "ERROR")
        return JSONResponse({"ok": True}, status_code=200)

    acc = ((payload.get("Account") or {}).get("title") or "").strip()
    srv = ((payload.get("Server") or {}).get("uuid") or "").strip()
    md = payload.get("Metadata") or {}
    title = md.get("title") or md.get("grandparentTitle") or "?"
    log(f"webhook: payload summary user='{acc}' server='{srv}' media='{title}'", "DEBUG")

    try:
        res = process_webhook(payload=payload, headers=dict(request.headers), raw=raw, logger=log)
    except Exception as e:
        log(f"webhook: process_webhook raised: {e}", "ERROR")
        return JSONResponse({"ok": True, "error": "internal"}, status_code=200)

    if res.get("error"):
        log(f"webhook: result error={res['error']}", "WARN")
    elif res.get("ignored"):
        log("webhook: ignored by filters/rules", "DEBUG")
    elif res.get("debounced"):
        log("webhook: debounced pause", "DEBUG")
    elif res.get("suppressed"):
        log("webhook: suppressed late start", "DEBUG")
    elif res.get("dedup"):
        log("webhook: duplicate event suppressed", "DEBUG")

    log(f"webhook: done action={res.get('action')} status={res.get('status')}", "DEBUG")
    return JSONResponse({"ok": True, **{k: v for k, v in res.items() if k != 'error'}}, status_code=200)
